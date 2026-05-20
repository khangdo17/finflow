"""
Simple 3-task health check DAG for verifying Airflow is operational.
Used to confirm DAG parsing and task execution work end-to-end.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_postgres(**context) -> str:
    """Ping PostgreSQL using internal Docker network address."""
    import psycopg2

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="finflow_db",
        user="finflow",
        password="finflow_pass",
    )
    conn.close()
    return "postgres: OK"


def check_kafka(**context) -> str:
    """Ping Kafka broker using internal Docker network address."""
    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(bootstrap_servers="kafka:29092", client_id="airflow-health")
    topics = admin.list_topics()
    admin.close()
    return f"kafka: OK — topics={topics}"


def summarise(**context) -> None:
    """Log results from upstream tasks."""
    from loguru import logger

    pg_result = context["task_instance"].xcom_pull(task_ids="check_postgres")
    kf_result = context["task_instance"].xcom_pull(task_ids="check_kafka")
    logger.info(f"Health summary — {pg_result} | {kf_result}")


with DAG(
    dag_id="example_health_check",
    description="Simple 3-task DAG to verify Airflow + service connectivity",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "health"],
) as dag:

    task_postgres = PythonOperator(
        task_id="check_postgres",
        python_callable=check_postgres,
    )

    task_kafka = PythonOperator(
        task_id="check_kafka",
        python_callable=check_kafka,
    )

    task_summary = PythonOperator(
        task_id="summarise",
        python_callable=summarise,
    )

    [task_postgres, task_kafka] >> task_summary
