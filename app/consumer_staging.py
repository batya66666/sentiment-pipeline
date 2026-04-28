# app/consumer_staging.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col

# --- КОНФИГУРАЦИЯ ---
KAFKA_SERVER = "kafka:9092"
TOPIC_NAME = "social_data"
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "social_staging"
CASSANDRA_TABLE = "raw_data"

def main():
    # Инициализация Spark с нужными пакетами
    spark = SparkSession.builder \
        .appName("KafkaToCassandra") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Схема данных (JSON), который шлет продюсер
    schema = StructType([
        StructField("source", StringType()),
        StructField("text", StringType()),
        StructField("created_at", StringType())
        
    ])

    # 1. Читаем из Kafka
    print(f"🎧 Listening to Kafka topic: {TOPIC_NAME}...")
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Парсим JSON
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("id", col("text")) 

    # 3. Пишем в Cassandra
    print(f"💾 Writing to Cassandra: {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}...")
    
    query = df_parsed.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", CASSANDRA_KEYSPACE) \
        .option("table", CASSANDRA_TABLE) \
        .option("checkpointLocation", "/tmp/checkpoints/staging_fix") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()