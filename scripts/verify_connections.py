"""
Health check script that verifies connectivity to Kafka, PostgreSQL, and Redis.
Exits with code 1 if any service is unreachable, so make verify fails fast.
"""
import os
import sys

from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def check_kafka() -> bool:
    """Verify Kafka broker is reachable by listing topics."""
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="finflow-health-check")
        topics = admin.list_topics()
        admin.close()
        logger.success(f"Kafka OK — broker={bootstrap_servers}, topics={topics}")
        return True
    except KafkaError as exc:
        logger.error(f"Kafka FAILED — {exc}")
        return False


def check_postgres() -> bool:
    """Verify PostgreSQL is reachable and the finflow_db database exists."""
    import psycopg2

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5433"))
    dbname = os.getenv("POSTGRES_DB", "finflow_db")
    user = os.getenv("POSTGRES_USER", "finflow")
    password = os.getenv("POSTGRES_PASSWORD", "finflow_pass")

    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
        table_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        logger.success(f"Postgres OK — host={host}:{port}, db={dbname}, public tables={table_count}")
        return True
    except Exception as exc:
        logger.error(f"Postgres FAILED — {exc}")
        return False


def check_redis() -> bool:
    """Verify Redis is reachable by sending a PING command."""
    import redis

    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))

    try:
        r = redis.Redis(host=host, port=port, db=db, socket_connect_timeout=5)
        pong = r.ping()
        logger.success(f"Redis OK — host={host}:{port}, ping={pong}")
        return True
    except Exception as exc:
        logger.error(f"Redis FAILED — {exc}")
        return False


def main() -> None:
    """Run all health checks and exit non-zero if any fail."""
    logger.info("Starting FinFlow connectivity checks...")

    results = {
        "kafka": check_kafka(),
        "postgres": check_postgres(),
        "redis": check_redis(),
    }

    passed = sum(results.values())
    total = len(results)
    logger.info(f"Health check complete: {passed}/{total} services OK")

    if not all(results.values()):
        failed = [svc for svc, ok in results.items() if not ok]
        logger.error(f"Failed services: {failed}")
        sys.exit(1)

    logger.success("All services healthy. FinFlow is ready.")


if __name__ == "__main__":
    main()
