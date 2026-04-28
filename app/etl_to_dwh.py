# app/etl_to_dwh.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, size, split, when, date_format, year, month, dayofmonth, lit, expr,
    to_timestamp, to_date, current_timestamp, crc32, coalesce
)
from pyspark.sql.types import StringType, FloatType, IntegerType
from pyspark.ml import PipelineModel
import re
import sys
import traceback
import clickhouse_connect

# --- КОНФИГУРАЦИЯ ---
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "social_staging"
TABLE_RAW = "raw_data"

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASS = "123"
CLICKHOUSE_DB = "social_dwh"

MODEL_PATH = "/models/sentiment_logreg_model"

# --- СЛОВАРЬ ТЕМ ---
TOPIC_RULES = {
    "Crypto": ["bitcoin", "btc", "eth", "crypto", "blockchain", "wallet", "coin", "nft"],
    "AI & Tech": ["ai", "gpt", "python", "code", "tech", "tesla", "robot", "apple", "google", "microsoft", "dev"],
    "Gaming": ["game", "play", "steam", "xbox", "ps5", "nintendo", "twitch"],
    "Politics": ["president", "vote", "election", "war", "law", "gov", "policy", "news"],
    "Movies & TV": ["movie", "film", "cinema", "watch", "actor", "netflix", "series"]
}

# --- UDF ---
def clean_text_logic(text):
    if not text: return ""
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.lower().strip()

def count_hashtags_logic(text):
    if not text: return 0
    return len([w for w in text.split() if w.startswith('#')])

def determine_topic_logic(text):
    if not text: return "Other"
    text_lower = text.lower()
    for category, keywords in TOPIC_RULES.items():
        for kw in keywords:
            if f" {kw} " in f" {text_lower} ": return category
    return "Other"

clean_text_udf = udf(clean_text_logic, StringType())
count_hashtags_udf = udf(count_hashtags_logic, IntegerType())
determine_topic_udf = udf(determine_topic_logic, StringType())

# --- ФУНКЦИЯ ЗАПИСИ ---
def write_to_clickhouse_pandas(df, table_name):
    row_count = df.count()
    print(f"💾 Converting {row_count} rows to Pandas for insertion into {table_name}...")
    
    if row_count == 0:
        return

    # Spark -> Pandas
    pandas_df = df.toPandas()
    
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, 
            port=CLICKHOUSE_PORT, 
            username=CLICKHOUSE_USER, 
            password=CLICKHOUSE_PASS,
            database=CLICKHOUSE_DB
        )
        
        client.insert_df(table_name, pandas_df)
        print(f"✅ Successfully inserted into {table_name}.")
        
    except Exception as e:
        print(f"❌ ERROR inserting into {table_name}: {e}")

# --- DIMENSIONS ---
def upsert_dim_source(spark, df):
    print("Preparing dim_source...")
    source_df = (df.select(col("source").alias("source_name"))
        .na.fill({"source_name": "unknown"})
        .distinct()
        .withColumn("source_id", expr("abs(crc32(source_name))").cast("Long"))
        .withColumn("source_type", lit("Social Network"))
        .withColumn("load_dt", current_timestamp())
    )
    write_to_clickhouse_pandas(source_df, "dim_source")

def upsert_dim_date(spark, df):
    print("Preparing dim_date...")
    date_df = (df.select("created_ts")
        .where(col("created_ts").isNotNull())
        .withColumn("date_value", to_date(col("created_ts")))
        .select("date_value")
        .distinct()
        .withColumn("date_id", date_format(col("date_value"), "yyyyMMdd").cast("Long"))
        .withColumn("year", year(col("date_value")))
        .withColumn("month", month(col("date_value")))
        .withColumn("day", dayofmonth(col("date_value")))
        .withColumn("load_dt", current_timestamp())
    )
    write_to_clickhouse_pandas(date_df, "dim_date")

def upsert_dim_topic(spark, df):
    print("Preparing dim_topic...")
    topic_df = (df.select("topic_name")
        .distinct()
        .withColumn("topic_id", expr("abs(crc32(topic_name))").cast("Long"))
        .withColumn("load_dt", current_timestamp())
    )
    write_to_clickhouse_pandas(topic_df, "dim_topic")

# --- MAIN ---
def main():
    spark = None
    try:
        print("🚀 Starting ETL Job (Fix Null Dates)...")
        spark = (SparkSession.builder
            .appName("Smart_ETL_ML")
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
            .config("spark.cassandra.connection.host", CASSANDRA_HOST)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # 1. Загрузка модели
        print(f"⏳ Loading model from {MODEL_PATH} ...")
        model = PipelineModel.load(MODEL_PATH)
        
        # 2. Чтение из Cassandra
        print("📥 Reading raw data from Cassandra...")
        raw_df = (spark.read
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", CASSANDRA_KEYSPACE)
            .option("table", TABLE_RAW)
            .load()
        )

        if raw_df.count() == 0:
            print("⚠️ No data in Cassandra.")
            return

        # 3. Предобработка (С ЗАЩИТОЙ ОТ NULL DATETIME)
        print("🧹 Preprocessing data...")
        
        processed_df = (raw_df
            .withColumn("parsed_ts", to_timestamp(col("created_at")))
            
            .withColumn("safe_ts", 
                        when(col("parsed_ts").isNull(), current_timestamp())
                        .otherwise(col("parsed_ts")))
            
            .withColumn("raw_text", col("text"))
            .withColumn("clean_text", clean_text_udf(col("text")))
            .withColumn("word_count", when(col("clean_text") == "", 0).otherwise(size(split(col("clean_text"), " "))))
            .withColumn("hashtags_count", count_hashtags_udf(col("raw_text")))
            .withColumn("topic_name", determine_topic_udf(col("clean_text")))
            .withColumn("text", col("clean_text")) 
            .withColumn("created_ts", col("safe_ts")) # Используем безопасную дату без NULL
        )

        # 4. Dimensions
        upsert_dim_source(spark, processed_df)
        upsert_dim_date(spark, processed_df)
        upsert_dim_topic(spark, processed_df)

        # 5. ML Inference
        print("🤖 Running Inference...")
        predictions = model.transform(processed_df)

        # 6. Facts
        print("🏗️ Building Facts...")
        fact_df = (predictions
            .withColumn("fact_id", expr("uuid()"))
            .withColumn("source_id", expr("abs(crc32(source))").cast("Long"))
            .withColumn("date_value", to_date(col("created_ts")))
            .withColumn("date_id", date_format(col("date_value"), "yyyyMMdd").cast("Long"))
            .withColumn("topic_id", expr("abs(crc32(topic_name))").cast("Long"))
            .withColumn("sentiment_class",
                        when(col("prediction") == 0.0, "negative")
                        .when(col("prediction") == 1.0, "positive")
                        .otherwise("neutral")
            )
            .withColumn("sentiment_score", lit(1.0).cast(FloatType()))
            .select(
                "fact_id", "source_id", "date_id", "topic_id", 
                col("raw_text").alias("text"), "sentiment_class", "sentiment_score",
                col("word_count").cast(IntegerType()), 
                col("hashtags_count").cast(IntegerType()),
                col("created_ts").alias("created_at")
            )
        )

        # 7. Запись
        write_to_clickhouse_pandas(fact_df, "fact_sentiment")
        
        print("🎉 ETL FINISHED SUCCESSFULLY!")

    except Exception as e:
        print("❌ CRASH:")
        traceback.print_exc()
    finally:
        if spark: spark.stop()

if __name__ == "__main__":
    main()