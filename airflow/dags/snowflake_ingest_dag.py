# PATCHIT auto-fix: unknown
# Original error: snowflake.connector.errors.ProgrammingError: 000904 invalid identifier CUSTOMER_SEGMENT. Column renamed to SEGMENT in migration v3. Update SELECT statement.
from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def generate_payload(**context) -> None:
    scenario = (context.get("dag_run").conf or {}).get("scenario", "good")
    if scenario == "SNF1_snowpipe_format_break":
        raise ValueError("Simulated delimiter/format mismatch for Snowpipe.")
    if scenario == "SNF2_schema_change":
        raise ValueError("Simulated schema change breaking COPY INTO.")


with DAG(
    dag_id="patchit_snowflake_ingest_dag",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    generate = PythonOperator(task_id="generate_payload", python_callable=generate_payload)

    generate
