from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.email import send_email

from dbt_utils import build_dbt_task

# --- Fix: added default_args with retry policy ---
# Previously missing: tasks failed immediately on transient Snowflake
# connection drops during peak load windows (no retries configured).
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": True,
}

with DAG(
    dag_id="dbt_customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "snowflake", "customers"],
) as dag:
    dbt_deps = build_dbt_task("dbt_deps", "dbt deps")
    dbt_run_stg = build_dbt_task(
        "dbt_run_stg", "dbt run --select path:models/customers/stg"
    )
    dbt_run_int = build_dbt_task(
        "dbt_run_int", "dbt run --select path:models/customers/int"
    )
    dbt_run_gold = build_dbt_task(
        "dbt_run_gold", "dbt run --select path:models/customers/gold"
    )
    dbt_test = build_dbt_task(
        "dbt_test", "dbt test --select path:models/customers"
    )

    dbt_deps >> dbt_run_stg >> dbt_run_int >> dbt_run_gold >> dbt_test
