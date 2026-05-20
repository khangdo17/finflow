"""
Redis sink for fraud alerts.
Called via foreachBatch from the Spark streaming query.
Uses pipeline() for all writes within a batch to minimise round-trips.

Key schema:
  fraud:alerts              LIST  — LPUSH + LTRIM to 1000 (most-recent first)
  fraud:count:total         STRING — INCR per alert
  fraud:count:user:{uid}    STRING — per-user counter
  fraud:top_users           ZSET  — ZINCRBY by fraud_score
  fraud:circuit_breaker:triggered STRING — SET with ex=300 when fraud_rate > 20%
"""
import json
import os
from typing import Any

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

ALERTS_KEY = "fraud:alerts"
TOTAL_COUNT_KEY = "fraud:count:total"
TOP_USERS_KEY = "fraud:top_users"
CIRCUIT_BREAKER_KEY = "fraud:circuit_breaker:triggered"
ALERTS_MAX_LEN = 1000
CIRCUIT_BREAKER_TTL = 300  # seconds


def _get_redis_client():
    """Create a Redis client from environment variables."""
    import redis as redis_lib
    return redis_lib.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
    )


def write_fraud_alerts(batch_df: Any, batch_id: int) -> None:
    """
    foreachBatch sink: write all fraud alerts in the micro-batch to Redis.
    Uses a single pipeline() per batch to batch commands and reduce latency.
    Triggers circuit breaker if fraud_rate in this batch exceeds 20%.
    """
    rows = batch_df.collect()
    if not rows:
        return

    r = _get_redis_client()
    total_rows = len(rows)
    fraud_rows = [row for row in rows if row.get("is_flagged", False)]
    fraud_count = len(fraud_rows)

    logger.info(f"Redis sink batch_id={batch_id}: {fraud_count}/{total_rows} fraud alerts")

    if not fraud_rows:
        return

    pipe = r.pipeline()

    for row in fraud_rows:
        alert = {
            "tx_id":          row["tx_id"],
            "user_id":        row["user_id"],
            "merchant":       row.get("merchant"),
            "amount":         row.get("amount"),
            "country":        row.get("country"),
            "tx_at":          str(row.get("tx_at")),
            "fraud_score":    int(row.get("fraud_score", 1)),
            "triggered_rules": row.get("triggered_rules", ""),
            "velocity_score": int(row.get("velocity_score", 0)),
            "amount_spike":   bool(row.get("amount_spike_flag", False)),
            "geo_anomaly":    bool(row.get("geo_anomaly_flag", False)),
        }
        alert_json = json.dumps(alert)

        # Push to head of list (most recent first), trim to 1000
        pipe.lpush(ALERTS_KEY, alert_json)

        # Increment counters
        pipe.incr(TOTAL_COUNT_KEY)
        pipe.incr(f"fraud:count:user:{row['user_id']}")

        # Add to sorted set — score is fraud_score so heavier fraudsters rank higher
        pipe.zincrby(TOP_USERS_KEY, float(alert["fraud_score"]), row["user_id"])

    # Trim list to last 1000 entries after all pushes
    pipe.ltrim(ALERTS_KEY, 0, ALERTS_MAX_LEN - 1)

    pipe.execute()

    # Circuit breaker: if fraud_rate in this batch > 20%, set flag with TTL
    fraud_rate = fraud_count / total_rows if total_rows > 0 else 0.0
    if fraud_rate > 0.20:
        r.set(CIRCUIT_BREAKER_KEY, "1", ex=CIRCUIT_BREAKER_TTL)
        logger.warning(
            f"Circuit breaker TRIGGERED — fraud_rate={fraud_rate:.1%} "
            f"(batch_id={batch_id}, will clear in {CIRCUIT_BREAKER_TTL}s)"
        )
