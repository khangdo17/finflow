"""
Kafka sink for fraud alerts.
Called via foreachBatch from the Spark streaming query.
Publishes serialised fraud alert JSON to the fraud-alerts topic.
Circuit breaker: skips publishing if fraud_rate > 20% in the current batch,
preventing alert storms from overwhelming downstream consumers.
"""
import json
import os
from typing import Any

import redis
from dotenv import load_dotenv
from kafka import KafkaProducer
from loguru import logger

load_dotenv()

CIRCUIT_BREAKER_THRESHOLD = 0.20  # 20% fraud rate triggers circuit breaker


def write_to_kafka(batch_df: Any, batch_id: int) -> None:
    """
    foreachBatch sink: publish each fraud alert to the fraud-alerts Kafka topic.
    Uses kafka-python KafkaProducer (not the Spark Kafka connector) so we can
    apply per-row logic like circuit breaking and custom serialisation.

    Circuit breaker logic:
      If fraud_rate in this batch > 20%, skip publishing and log a warning.
      This prevents a runaway fraud burst from flooding the alerts topic.
    """
    rows = batch_df.collect()
    if not rows:
        return

    total_rows = len(rows)
    # Rows arriving here are already filtered to is_flagged=True by fraud_detector
    fraud_count = total_rows

    # Check circuit breaker state in Redis before publishing
    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
    )
    circuit_tripped = r.exists("fraud:circuit_breaker:triggered")

    if circuit_tripped:
        logger.warning(
            f"Kafka sink SKIPPED — circuit breaker is active "
            f"(batch_id={batch_id}, {fraud_count} alerts suppressed)"
        )
        return

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC_FRAUD_ALERTS", "fraud-alerts")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )

    published = 0
    try:
        for row in rows:
            alert = {
                "tx_id":           row["tx_id"],
                "user_id":         row["user_id"],
                "merchant":        row.get("merchant"),
                "amount":          row.get("amount"),
                "country":         row.get("country"),
                "tx_at":           str(row.get("tx_at")),
                "fraud_score":     int(row.get("fraud_score", 1)),
                "triggered_rules": row.get("triggered_rules", ""),
                "velocity_score":  int(row.get("velocity_score", 0)),
                "amount_spike":    bool(row.get("amount_spike_flag", False)),
                "geo_anomaly":     bool(row.get("geo_anomaly_flag", False)),
            }
            producer.send(topic, key=row["user_id"], value=alert)
            published += 1
    finally:
        producer.flush()
        producer.close()

    logger.info(f"Kafka sink batch_id={batch_id}: published {published} alerts to {topic}")
