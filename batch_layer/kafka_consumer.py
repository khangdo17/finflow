"""
Kafka consumer for FinFlow batch layer.
Reads from raw-transactions topic and bulk-inserts into PostgreSQL.
Uses watermark tracking to skip already-processed records (idempotency).
Batch flushes at 100 records OR every 5 seconds, whichever comes first.
"""
import json
import os
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from loguru import logger

load_dotenv()

BATCH_SIZE = 100
FLUSH_INTERVAL_SECONDS = 5
GROUP_ID = "finflow-batch-consumer"

INSERT_SQL = """
INSERT INTO raw_transactions
    (tx_id, user_id, merchant, amount, currency, country, tx_at, device, is_fraud, fraud_reason)
VALUES %s
ON CONFLICT (tx_id) DO NOTHING
"""


def get_db_connection() -> psycopg2.extensions.connection:
    """Open a Postgres connection using environment variables."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "finflow_db"),
        user=os.getenv("POSTGRES_USER", "finflow"),
        password=os.getenv("POSTGRES_PASSWORD", "finflow_pass"),
    )


def get_watermark(conn: psycopg2.extensions.connection) -> Optional[datetime]:
    """
    Return MAX(tx_at) from raw_transactions so we can skip records already ingested.
    Returns None if the table is empty (first run).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(tx_at) FROM raw_transactions;")
        result = cur.fetchone()[0]
    return result


def flush_batch(
    conn: psycopg2.extensions.connection,
    batch: List[Dict[str, Any]],
    watermark: Optional[datetime],
) -> int:
    """
    Bulk-insert a batch of transaction dicts into raw_transactions.
    Skips records whose tx_at is before or equal to the watermark.
    Returns the number of rows actually inserted.
    """
    if not batch:
        return 0

    rows = []
    skipped = 0
    for tx in batch:
        try:
            tx_at = datetime.fromisoformat(tx["tx_at"])
            # Make both tz-aware for comparison if needed
            if watermark is not None:
                wm = watermark if watermark.tzinfo else watermark.replace(tzinfo=timezone.utc)
                ta = tx_at if tx_at.tzinfo else tx_at.replace(tzinfo=timezone.utc)
                if ta <= wm:
                    skipped += 1
                    continue

            rows.append((
                tx["tx_id"],
                tx["user_id"],
                tx.get("merchant"),
                tx.get("amount"),
                tx.get("currency", "VND"),
                tx.get("country"),
                tx_at,
                tx.get("device"),
                tx.get("is_fraud", False),
                tx.get("fraud_reason"),
            ))
        except (KeyError, ValueError) as exc:
            logger.warning(f"Skipping malformed record: {exc} — {tx}")

    if rows:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, INSERT_SQL, rows)
        conn.commit()

    if skipped:
        logger.debug(f"Skipped {skipped} records behind watermark")

    return len(rows)


def run_consumer() -> None:
    """
    Main consumer loop. Connects to Kafka and Postgres, then polls indefinitely.
    Flushes to Postgres in batches of BATCH_SIZE or every FLUSH_INTERVAL_SECONDS.
    Dead-letters invalid JSON without crashing.
    """
    topic = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "raw-transactions")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: m,  # raw bytes; we decode manually for error handling
        consumer_timeout_ms=FLUSH_INTERVAL_SECONDS * 1000,
    )

    conn = get_db_connection()
    watermark = get_watermark(conn)
    logger.info(f"Watermark loaded: {watermark}")

    batch: List[Dict[str, Any]] = []
    last_flush = time.monotonic()
    total_inserted = 0

    logger.info(f"Consumer started — topic={topic}, group={GROUP_ID}, watermark={watermark}")

    try:
        while True:
            try:
                for message in consumer:
                    try:
                        tx = json.loads(message.value.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                        # Dead-letter: log and skip without crashing
                        logger.error(f"Invalid JSON at offset {message.offset}: {exc}")
                        continue

                    batch.append(tx)

                    now = time.monotonic()
                    time_triggered = (now - last_flush) >= FLUSH_INTERVAL_SECONDS
                    size_triggered = len(batch) >= BATCH_SIZE

                    if size_triggered or time_triggered:
                        inserted = flush_batch(conn, batch, watermark)
                        total_inserted += inserted
                        logger.info(
                            f"Flushed {len(batch)} records → {inserted} inserted "
                            f"(total={total_inserted}, trigger={'size' if size_triggered else 'time'})"
                        )
                        batch.clear()
                        last_flush = time.monotonic()

            except StopIteration:
                # consumer_timeout_ms elapsed — flush whatever is buffered
                if batch:
                    inserted = flush_batch(conn, batch, watermark)
                    total_inserted += inserted
                    logger.info(f"Timeout flush: {inserted} inserted (total={total_inserted})")
                    batch.clear()
                    last_flush = time.monotonic()

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except KafkaError as exc:
        logger.error(f"Kafka error: {exc}")
        raise
    finally:
        if batch:
            flush_batch(conn, batch, watermark)
        consumer.close()
        conn.close()
        logger.info(f"Consumer stopped. Total inserted: {total_inserted}")


if __name__ == "__main__":
    run_consumer()
