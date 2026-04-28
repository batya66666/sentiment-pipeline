from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when, lower, regexp_replace
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, LongType
import shutil
import os
import json

MODEL_PATH = "/models/sentiment_logreg_model"
DATASET_PATH = "/app/twitter_training.csv"
METRICS_PATH = "/models/metrics.json"

def main():
    spark = SparkSession.builder \
        .appName("TrainSentimentModel") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"--- 1. Loading and Preparing Data ({DATASET_PATH}) ---")
    if not os.path.exists(DATASET_PATH):
        print(f"❌ File not found: {DATASET_PATH}")
        return

    # Schema
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("entity", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("text", StringType(), True)
    ])

    df = spark.read.csv(DATASET_PATH, header=False, schema=schema)
    df = df.dropna(subset=["text", "sentiment"])
    df = df.filter(col("sentiment") != "Irrelevant")

    # Cleaning
    df = df.withColumn("clean_text", lower(col("text")))
    df = df.withColumn("clean_text", regexp_replace("clean_text", r"http\S+", ""))
    df = df.withColumn("clean_text", regexp_replace("clean_text", r"[^a-z\s]", ""))

    # Label mapping
    df = df.withColumn(
        "label",
        when(col("sentiment") == "Negative", 0.0)
        .when(col("sentiment") == "Positive", 1.0)
        .otherwise(2.0)
    ).select("clean_text", "label")

    count = df.count()
    print(f"📌 Dataset size: {count} rows")

    # Pipeline
    tokenizer = RegexTokenizer(inputCol="clean_text", outputCol="words", pattern="\\s+")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=20000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    lr = LogisticRegression(
        maxIter=20,
        regParam=0.01,
        elasticNetParam=0.0,
        featuresCol="features",
        labelCol="label",
        family="multinomial"
    )

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print("\n--- Training Model ---")
    model = pipeline.fit(train_df)

    # Evaluation
    print("\n--- Evaluating Model ---")
    predictions = model.transform(test_df)

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction"
    )

    accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
    precision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
    recall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
    f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

    # Print metrics
    print(f"🎯 Accuracy:  {accuracy:.4f}")
    print(f"🎯 Precision: {precision:.4f}")
    print(f"🎯 Recall:    {recall:.4f}")
    print(f"🎯 F1 Score:  {f1:.4f}")

    # Confusion Matrix
    print("\n--- Confusion Matrix (label × prediction) ---")
    conf_matrix_df = (
        predictions.groupBy("label")
        .pivot("prediction", [0.0, 1.0, 2.0])
        .count()
        .na.fill(0)
        .orderBy("label")
    )

    conf_matrix_df.show()

    # Convert matrix to Python list for JSON
    rows = conf_matrix_df.collect()
    cm_array = []
    for row in rows:
        cm_array.append([int(row[1]), int(row[2]), int(row[3])])

    # Saving model
    print(f"\n💾 Saving model into {MODEL_PATH} ...")
    try:
        if os.path.exists(MODEL_PATH):
            shutil.rmtree(MODEL_PATH)
        model.save(MODEL_PATH)
        print("✅ Model saved successfully!")
    except Exception as e:
        print(f"❌ Failed to save model: {e}")

    # Save metrics
    metrics_data = {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "confusion_matrix": cm_array
    }

    with open(METRICS_PATH, "w") as f:
        json.dump(metrics_data, f)

    print(f"✅ Metrics written to {METRICS_PATH}")

    spark.stop()

if __name__ == "__main__":
    main()
