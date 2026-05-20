"""
dbt transformation DAG: triggered by ingest_dag (schedule=None).
Runs in order: staging models → staging tests → mart models → all tests.
Fails fast if any step errors so bad data never reaches marts.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DBT_DIR = "/opt/airflow/dbt/finflow"
DBT_PROFILES_DIR = "/opt/airflow/dbt/finflow"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="transform_dag",
    description="dbt staging → test → marts → test (triggered by ingest_dag)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["batch", "dbt", "transform"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"dbt deps "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"dbt run --select staging "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"dbt test --select staging "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"dbt run --select intermediate marts "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=(
            f"dbt test "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_deps >> dbt_run_staging >> dbt_test_staging >> dbt_run_marts >> dbt_test_all
