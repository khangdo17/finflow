"""
Observability DAG: runs schema drift detection and freshness checks hourly.
Both tasks run in PARALLEL (no dependency between them) so a slow Postgres
schema query does not delay the freshness check and vice versa.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "finflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="observability_dag",
    description="Hourly schema drift + data freshness monitoring",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["observability", "finflow"],
) as dag:

    def _run_schema_check():
        from observability.schema_check import run_schema_check
        diffs = run_schema_check()
        breaking = [d for d in diffs if d.is_breaking]
        if breaking:
            raise RuntimeError(
                f"Breaking schema changes detected: {[d.source_name for d in breaking]}"
            )

    def _run_freshness_checks():
        from observability.freshness_check import run_freshness_checks
        results = run_freshness_checks()
        stale = [r for r in results if r.status == "stale"]
        if stale:
            # Log but do not raise — Slack alert already sent inside run_freshness_checks()
            from loguru import logger
            logger.warning(f"Stale tables detected: {[r.source_name for r in stale]}")

    schema_check_task = PythonOperator(
        task_id="schema_check",
        python_callable=_run_schema_check,
    )

    freshness_check_task = PythonOperator(
        task_id="freshness_check",
        python_callable=_run_freshness_checks,
    )

    # Run in parallel — no >> dependency between them
    [schema_check_task, freshness_check_task]
