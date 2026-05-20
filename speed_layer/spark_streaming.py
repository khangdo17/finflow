"""
Entry point for the FinFlow Spark Structured Streaming pipeline.
Reads raw-transactions from Kafka, applies fraud detection rules,
and writes alerts to both Redis (real-time) and Kafka fraud-alerts topic.

Run with:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        speed_layer/spark_streaming.py
"""
import os

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType,
)

from speed_layer.fraud_detector import apply_all_rules
from speed_layer.sink.redis_sink import write_fraud_alerts
from speed_layer.sink.kafka_sink import write_to_kafka

load_dotenv()

# Schema must match generator output field names exactly
TX_SCHEMA = StructType([
    StructField("tx_id",       StringType(),    nullable=False),
    StructField("user_id",     StringType(),    nullable=False),
    StructField("merchant",    StringType(),    nullable=True),
    StructField("amount",      DoubleType(),    nullable=False),
    StructField("currency",    StringType(),    nullable=True),
    StructField("country",     StringType(),    nullable=True),
    StructField("tx_at",       StringType(),    nullable=False),  # ISO string from generator
    StructField("device",      StringType(),    nullable=True),
    StructField("is_fraud",    BooleanType(),   nullable=True),
    StructField("fraud_reason", StringType(),   nullable=True),
])

CHECKPOINT_BASE = "/tmp/finflow/checkpoint"


def build_spark() -> SparkSession:
    """
    Create a local SparkSession with the Kafka connector package.
    shuffle.partitions=4 keeps local-mode overhead low.
    """
    return (
        SparkSession.builder
        .appName("FinFlow-FraudDetector")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .getOrCreate()
    )


def parse_transactions(raw_df: DataFrame) -> DataFrame:
    """
    Deserialise JSON payload from Kafka value bytes, apply TX_SCHEMA,
    cast tx_at from ISO string to TimestampType, and add a 10-second watermark.
    The watermark enables stateful windowed aggregations downstream.
    """
    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json(F.col("json_str"), TX_SCHEMA).alias("tx"))
        .select("tx.*")
        .withColumn("tx_at", F.to_timestamp(F.col("tx_at")))
        .filter(F.col("tx_id").isNotNull())
        .filter(F.col("tx_at").isNotNull())
        .withWatermark("tx_at", "10 seconds")
    )
    return parsed


def run() -> None:
    """
    Main streaming loop:
      1. Read from Kafka raw-transactions topic
      2. Parse and watermark
      3. Apply fraud detection rules (velocity, amount spike, geo anomaly)
      4. Write fraud alerts to Redis (foreachBatch) and Kafka (foreachBatch)
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_in = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "raw-transactions")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Connecting to Kafka at {bootstrap_servers}, topic={topic_in}")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic_in)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "1000")
        .load()
    )

    tx_df = parse_transactions(raw_df)

    fraud_df = apply_all_rules(tx_df)

    # Write to Redis — foreachBatch, checkpointed
    redis_query = (
        fraud_df.writeStream
        .foreachBatch(write_fraud_alerts)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/redis")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Write to Kafka fraud-alerts topic — foreachBatch, checkpointed
    kafka_query = (
        fraud_df.writeStream
        .foreachBatch(write_to_kafka)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/kafka")
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info("Streaming queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
