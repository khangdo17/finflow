"""
Health check route — pings all three backing services.
GET /health — returns status for Kafka, Postgres, and Redis
Returns 200 even when services are degraded; callers inspect the body for per-service status.
"""
import os
from typing import Any, Dict

import psycopg2
import redis
from dotenv import load_dotenv
from fastapi import APIRouter
from kafka import KafkaAdminClient
from kafka.errors import KafkaError
from loguru import logger

load_dotenv()

router = APIRouter()


def _check_kafka() -> Dict[str, Any]:
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            request_timeout_ms=3000,
        )
        topics = admin.list_topics()
        admin.close()
        return {"status": "ok", "topics": len(topics)}
    except KafkaError as exc:
        logger.warning(f"Kafka health check failed: {exc}")
        return {"status": "error", "detail": str(exc)}
    except Exception as exc:
        logger.warning(f"Kafka health check error: {exc}")
        return {"status": "error", "detail": str(exc)}


def _check_postgres() -> Dict[str, Any]:
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5433")),
            dbname=os.getenv("POSTGRES_DB", "finflow_db"),
            user=os.getenv("POSTGRES_USER", "finflow"),
            password=os.getenv("POSTGRES_PASSWORD", "finflow_pass"),
            connect_timeout=3,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_transactions")
            count = cur.fetchone()[0]
        conn.close()
        return {"status": "ok", "row_count": count}
    except psycopg2.OperationalError as exc:
        logger.warning(f"Postgres health check failed: {exc}")
        return {"status": "error", "detail": str(exc)}


def _check_redis() -> Dict[str, Any]:
    try:
        r = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            decode_responses=True,
            socket_timeout=3,
        )
        r.ping()
        total_alerts = int(r.get("fraud:count:total") or 0)
        return {"status": "ok", "total_fraud_alerts": total_alerts}
    except redis.RedisError as exc:
        logger.warning(f"Redis health check failed: {exc}")
        return {"status": "error", "detail": str(exc)}


@router.get("")
def health_check() -> Dict[str, Any]:
    """
    Ping Kafka, Postgres, and Redis.
    Returns overall status 'ok' only when all three services respond successfully.
    Individual service degradation is surfaced in the response body.
    """
    kafka_status = _check_kafka()
    postgres_status = _check_postgres()
    redis_status = _check_redis()

    all_ok = all(
        s["status"] == "ok"
        for s in [kafka_status, postgres_status, redis_status]
    )

    return {
        "overall": "ok" if all_ok else "degraded",
        "kafka": kafka_status,
        "postgres": postgres_status,
        "redis": redis_status,
    }
