"""
Hourly ingest DAG: runs the Kafka consumer batch job and triggers transform_dag on success.
Idempotent via ON CONFLICT DO NOTHING + watermark in the consumer.
Retries twice with 5-minute delay before alerting on failure.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def on_failure_callback(context: dict) -> None:
    """Log failure details; extend here to send Slack/PagerDuty alerts."""
    from loguru import logger

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    exception = context.get("exception")
    logger.error(
        f"DAG failure — dag={dag_id}, task={task_id}, "
        f"execution_date={execution_date}, error={exception}"
    )


def run_kafka_consumer(**context) -> dict:
    """
    Pull one batch from Kafka and insert into raw_transactions.
    Uses internal Docker network: host=postgres, port=5432.
    Overrides env vars so the consumer targets the Airflow-internal Postgres address.
    """
    import json
    import time
    import psycopg2
    import psycopg2.extras
    from kafka import KafkaConsumer
    from loguru import logger

    topic = "raw-transactions"
    bootstrap_servers = "kafka:29092"
    group_id = "finflow-batch-consumer"

    # Internal Docker network connection (not the host-mapped 5433 port)
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="finflow_db",
        user="finflow",
        password="finflow_pass",
    )

    # Get watermark
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(tx_at) FROM raw_transactions;")
        watermark = cur.fetchone()[0]
    logger.info(f"Watermark: {watermark}")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=10_000,  # 10s timeout for batch window
    )

    batch = []
    BATCH_SIZE = 100
    INSERT_SQL = """
        INSERT INTO raw_transactions
            (tx_id, user_id, merchant, amount, currency, country, tx_at, device, is_fraud, fraud_reason)
        VALUES %s
        ON CONFLICT (tx_id) DO NOTHING
    """

    def flush(rows):
        if not rows:
            return 0
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, INSERT_SQL, rows)
        conn.commit()
        return len(rows)

    total = 0
    try:
        for msg in consumer:
            try:
                tx = json.loads(msg.value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.error(f"Dead-letter at offset {msg.offset}: {exc}")
                continue

            # Watermark skip
            if watermark:
                from datetime import timezone
                tx_at = datetime.fromisoformat(tx["tx_at"])
                wm = watermark if watermark.tzinfo else watermark.replace(tzinfo=timezone.utc)
                ta = tx_at if tx_at.tzinfo else tx_at.replace(tzinfo=timezone.utc)
                if ta <= wm:
                    continue

            batch.append((
                tx["tx_id"], tx["user_id"], tx.get("merchant"), tx.get("amount"),
                tx.get("currency", "VND"), tx.get("country"),
                datetime.fromisoformat(tx["tx_at"]),
                tx.get("device"), tx.get("is_fraud", False), tx.get("fraud_reason"),
            ))

            if len(batch) >= BATCH_SIZE:
                total += flush(batch)
                batch.clear()
    except StopIteration:
        pass
    finally:
        total += flush(batch)
        consumer.close()
        conn.close()

    logger.info(f"Ingest complete — total rows inserted: {total}")
    return {"rows_inserted": total}


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="ingest_dag",
    description="Hourly Kafka → Postgres ingest with watermark deduplication",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["batch", "ingest"],
) as dag:

    ingest_task = PythonOperator(
        task_id="run_kafka_consumer",
        python_callable=run_kafka_consumer,
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="transform_dag",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    ingest_task >> trigger_transform
