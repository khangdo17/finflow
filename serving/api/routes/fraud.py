"""
Fraud alert routes — reads from Redis speed-layer keys.
GET /fraud/alerts  — latest N fraud alerts from the fraud:alerts LIST
GET /fraud/stats   — aggregate counters + top flagged users from Redis
"""
import json
import os
from typing import Any, Dict, List, Optional

import redis
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query
from loguru import logger
from pydantic import BaseModel

load_dotenv()

router = APIRouter()


class FraudAlert(BaseModel):
    """Pydantic model for a single fraud alert record stored in Redis."""
    tx_id: str
    user_id: str
    merchant: Optional[str] = None
    amount: Optional[float] = None
    country: Optional[str] = None
    tx_at: Optional[str] = None
    fraud_score: int = 0
    triggered_rules: Optional[str] = None
    velocity_score: int = 0
    amount_spike: bool = False
    geo_anomaly: bool = False


def _get_redis() -> redis.Redis:
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
    )


@router.get("/alerts", response_model=List[FraudAlert])
def get_fraud_alerts(
    limit: int = Query(default=50, ge=1, le=1000, description="Number of recent alerts to return"),
) -> List[FraudAlert]:
    """
    Return the most recent fraud alerts from the Redis LIST fraud:alerts.
    Alerts are stored newest-first (LPUSH), so LRANGE 0 N-1 gives the latest.
    """
    r = _get_redis()
    try:
        raw_alerts = r.lrange("fraud:alerts", 0, limit - 1)
    except redis.RedisError as exc:
        logger.error(f"Redis error fetching alerts: {exc}")
        raise HTTPException(status_code=503, detail="Redis unavailable") from exc

    alerts: List[FraudAlert] = []
    for raw in raw_alerts:
        try:
            data = json.loads(raw)
            alerts.append(FraudAlert(**data))
        except Exception as exc:
            logger.warning(f"Skipping malformed alert: {exc}")

    return alerts


@router.get("/trend")
def get_fraud_trend() -> List[Dict[str, Any]]:
    """
    Return fraud alert counts bucketed by hour for the last 24 hours.
    Reads from the fraud:alerts LIST and groups by tx_at hour.
    Returns up to 24 buckets suitable for a time-series line chart.
    """
    r = _get_redis()
    try:
        raw_alerts = r.lrange("fraud:alerts", 0, 999)
    except redis.RedisError as exc:
        logger.error(f"Redis error fetching trend data: {exc}")
        raise HTTPException(status_code=503, detail="Redis unavailable") from exc

    from collections import defaultdict
    from datetime import datetime, timezone, timedelta

    buckets: Dict[str, int] = defaultdict(int)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    for raw in raw_alerts:
        try:
            data = json.loads(raw)
            tx_at_str = data.get("tx_at", "")
            if not tx_at_str:
                continue
            # Parse ISO timestamp; handle with or without timezone
            try:
                tx_at = datetime.fromisoformat(tx_at_str.replace("Z", "+00:00"))
            except ValueError:
                continue
            if tx_at.tzinfo is None:
                tx_at = tx_at.replace(tzinfo=timezone.utc)
            if tx_at < cutoff:
                continue
            hour_key = tx_at.strftime("%Y-%m-%dT%H:00:00")
            buckets[hour_key] += 1
        except Exception as exc:
            logger.warning(f"Skipping alert in trend: {exc}")

    return [{"hour": h, "count": c} for h, c in sorted(buckets.items())]


@router.get("/heatmap")
def get_fraud_heatmap() -> List[Dict[str, Any]]:
    """
    Return transaction volume bucketed by day-of-week and hour-of-day.
    Reads from fraud:alerts LIST. Used to render a 7x24 heatmap.
    """
    r = _get_redis()
    try:
        raw_alerts = r.lrange("fraud:alerts", 0, 999)
    except redis.RedisError as exc:
        logger.error(f"Redis error fetching heatmap data: {exc}")
        raise HTTPException(status_code=503, detail="Redis unavailable") from exc

    from collections import defaultdict
    from datetime import datetime, timezone

    DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    buckets: Dict[str, int] = defaultdict(int)

    for raw in raw_alerts:
        try:
            data = json.loads(raw)
            tx_at_str = data.get("tx_at", "")
            if not tx_at_str:
                continue
            try:
                tx_at = datetime.fromisoformat(tx_at_str.replace("Z", "+00:00"))
            except ValueError:
                continue
            if tx_at.tzinfo is None:
                tx_at = tx_at.replace(tzinfo=timezone.utc)
            key = f"{DAY_NAMES[tx_at.weekday()]}_{tx_at.hour:02d}"
            buckets[key] += 1
        except Exception as exc:
            logger.warning(f"Skipping alert in heatmap: {exc}")

    result = []
    for day in DAY_NAMES:
        for hour in range(24):
            key = f"{day}_{hour:02d}"
            result.append({"day": day, "hour": hour, "count": buckets.get(key, 0)})
    return result


@router.get("/stats")
def get_fraud_stats() -> Dict[str, Any]:
    """
    Return aggregate fraud counters and top flagged users from Redis.
    Reads: fraud:count:total, fraud:top_users (ZSET), circuit_breaker status.
    """
    r = _get_redis()
    try:
        total = int(r.get("fraud:count:total") or 0)
        circuit_breaker_active = bool(r.exists("fraud:circuit_breaker:triggered"))
        # Top 10 users by cumulative fraud_score
        top_raw = r.zrevrange("fraud:top_users", 0, 9, withscores=True)
        top_users = [{"user_id": uid, "total_score": score} for uid, score in top_raw]
    except redis.RedisError as exc:
        logger.error(f"Redis error fetching stats: {exc}")
        raise HTTPException(status_code=503, detail="Redis unavailable") from exc

    return {
        "total_fraud_alerts": total,
        "circuit_breaker_active": circuit_breaker_active,
        "top_flagged_users": top_users,
    }
