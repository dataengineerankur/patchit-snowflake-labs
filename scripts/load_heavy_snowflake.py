# PATCHIT auto-fix: increase_warehouse_size
# Original error: snowflake.connector.errors.ProgrammingError: Query execution exceeded statement_timeout_in_seconds=1800. Add partition filter or increase timeout.
# PATCHIT auto-fix: grant_permissions
# Original error: snowflake.connector.errors.ProgrammingError: Object ANALYTICS_DB.PUBLIC does not exist or not authorized. Grant USAGE ON SCHEMA ANALYTICS_DB.PUBLIC to LOADER_ROLE.
# PATCHIT auto-fix: grant_permissions
# Original error: snowflake.connector.errors.ProgrammingError: Insufficient privileges to operate on warehouse COMPUTE_WH. Grant USAGE ON WAREHOUSE COMPUTE_WH to LOADER_ROLE.
#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import sys
import tempfile
import textwrap
import time
import urllib.request
from typing import Optional

import snowflake.connector


def _env(name: str, default: Optional[str] = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _exec(cur, sql: str) -> None:
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        cur.execute(stmt)


def _write_pipe_csv(path: str, rowcount: int) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["EVENT_ID", "USER_ID", "EVENT_TS", "EVENT_TYPE", "AMOUNT", "SOURCE"])
        for i in range(rowcount):
            event_type = "purchase" if i % 3 == 0 else ("click" if i % 3 == 1 else "view")
            writer.writerow(
                [
                    i,
                    i % 1_000_000,
                    "2025-01-01T00:00:00Z",
                    event_type,
                    10.0 + (i % 100),
                    "snowpipe",
                ]
            )


def _wait_for_task_state(cur, task_name: str, timeout_s: int = 180, poll_s: int = 5) -> dict:
    deadline = time.time() + timeout_s
    history: dict = {}
    while time.time() < deadline:
        cur.execute(
            f"""
            SELECT *
            FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
              TASK_NAME=>'{task_name}',
              RESULT_LIMIT=>1
            ));
            """
        )
        row = cur.fetchone()
        if row:
            columns = [col[0] for col in cur.description]
            history = dict(zip(columns, row))
            state = history.get("STATE")
            if state in {"FAILED", "SUCCEEDED", "CANCELLED"}:
                return history
        time.sleep(poll_s)
    return history


def main() -> int:
    account = _env("SNOWFLAKE_ACCOUNT")
    user = _env("SNOWFLAKE_USER")
    role = _env("SNOWFLAKE_ROLE")
    warehouse = _env("SNOWFLAKE_WAREHOUSE")
    database = _env("SNOWFLAKE_DATABASE")
    schema = _env("SNOWFLAKE_SCHEMA")
    password = os.environ.get("SNOWFLAKE_PASSWORD")
    private_key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")

    if not password and not private_key_path:
        raise RuntimeError("Set SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH.")

    connect_kwargs = {
        "account": account,
        "user": user,
        "role": role,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
    }
    if password:
        connect_kwargs["password"] = password
    else:
        with open(private_key_path, "rb") as handle:
            connect_kwargs["private_key"] = handle.read()

    skip_load = os.environ.get("SKIP_LOAD", "").lower() in {"1", "true", "yes"}
    skip_pipe_load = os.environ.get("SKIP_PIPE_LOAD", "").lower() in {"1", "true", "yes"}
    pipe_rowcount = int(os.environ.get("PIPE_ROWCOUNT", "1000000"))

    with snowflake.connector.connect(**connect_kwargs) as conn:
        with conn.cursor() as cur:
            _exec(
                cur,
                f"""
                USE ROLE {role};
                USE WAREHOUSE {warehouse};
                USE DATABASE {database};
                USE SCHEMA {schema};
                """,
            )

            if not skip_load:
                # Create datasets
                _exec(
                    cur,
                    """
                    CREATE OR REPLACE TABLE USERS_MILLION (
                      USER_ID NUMBER,
                      EMAIL STRING,
                      COUNTRY STRING,
                      CREATED_TS TIMESTAMP_NTZ
                    );

                    CREATE OR REPLACE TABLE EVENTS_MILLION (
                      EVENT_ID NUMBER,
                      USER_ID NUMBER,
                      EVENT_TS TIMESTAMP_NTZ,
                      EVENT_TYPE STRING,
                      AMOUNT NUMBER(10,2)
                    );

                    CREATE OR REPLACE TABLE TRANSACTIONS_MILLION (
                      TXN_ID NUMBER,
                      USER_ID NUMBER,
                      TXN_TS TIMESTAMP_NTZ,
                      STATUS STRING,
                      TOTAL NUMBER(10,2)
                    );
                    """,
                )

                # Load 1M rows each using generator
                _exec(
                    cur,
                    """
                    INSERT INTO USERS_MILLION
                    SELECT
                      SEQ4() AS USER_ID,
                      CONCAT('user_', SEQ4(), '@example.com') AS EMAIL,
                      IFF(MOD(SEQ4(), 5)=0, 'US', IFF(MOD(SEQ4(), 5)=1, 'IN', IFF(MOD(SEQ4(), 5)=2, 'UK', IFF(MOD(SEQ4(), 5)=3, 'DE', 'SG')))) AS COUNTRY,
                      DATEADD('minute', -SEQ4(), CURRENT_TIMESTAMP()) AS CREATED_TS
                    FROM TABLE(GENERATOR(ROWCOUNT => 1000000));

                    INSERT INTO EVENTS_MILLION
                    SELECT
                      SEQ4() AS EVENT_ID,
                      MOD(SEQ4(), 1000000) AS USER_ID,
                      DATEADD('second', -SEQ4(), CURRENT_TIMESTAMP()) AS EVENT_TS,
                      IFF(MOD(SEQ4(), 3)=0, 'click', IFF(MOD(SEQ4(), 3)=1, 'view', 'purchase')) AS EVENT_TYPE,
                      UNIFORM(1, 500, RANDOM())::NUMBER(10,2) AS AMOUNT
                    FROM TABLE(GENERATOR(ROWCOUNT => 1000000));

                    INSERT INTO TRANSACTIONS_MILLION
                    SELECT
                      SEQ4() AS TXN_ID,
                      MOD(SEQ4(), 1000000) AS USER_ID,
                      DATEADD('second', -SEQ4(), CURRENT_TIMESTAMP()) AS TXN_TS,
                      IFF(MOD(SEQ4(), 4)=0, 'failed', IFF(MOD(SEQ4(), 4)=1, 'pending', IFF(MOD(SEQ4(), 4)=2, 'refunded', 'success'))) AS STATUS,
                      UNIFORM(5, 1000, RANDOM())::NUMBER(10,2) AS TOTAL
                    FROM TABLE(GENERATOR(ROWCOUNT => 1000000));
                    """,
                )

            # Build Snowpipe-based staging table
            _exec(
                cur,
                """
                CREATE OR REPLACE TABLE PIPE_EVENTS_MILLION (
                  EVENT_ID NUMBER,
                  USER_ID NUMBER,
                  EVENT_TS TIMESTAMP_NTZ,
                  EVENT_TYPE STRING,
                  AMOUNT NUMBER(10,2),
                  SOURCE STRING
                );

                CREATE OR REPLACE STAGE PATCHIT_HEAVY_STAGE;

                CREATE OR REPLACE FILE FORMAT PATCHIT_HEAVY_CSV
                  TYPE = CSV
                  FIELD_DELIMITER = ','
                  SKIP_HEADER = 1;

                CREATE OR REPLACE PIPE PATCHIT_HEAVY_PIPE
                AS
                  COPY INTO PIPE_EVENTS_MILLION
                  FROM @PATCHIT_HEAVY_STAGE
                  FILE_FORMAT = (FORMAT_NAME = PATCHIT_HEAVY_CSV);
                """,
            )

            if not skip_pipe_load:
                with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as tmp:
                    tmp_path = tmp.name
                _write_pipe_csv(tmp_path, pipe_rowcount)
                _exec(
                    cur,
                    f"""
                    PUT file://{tmp_path} @PATCHIT_HEAVY_STAGE AUTO_COMPRESS=FALSE;
                    ALTER PIPE PATCHIT_HEAVY_PIPE REFRESH;
                    """,
                )
                os.remove(tmp_path)

            # Create a heavy-join Snowpark procedure that intentionally fails
            py_code = textwrap.dedent(
                """
                def run(session):
                    users = session.table("USERS_MILLION")
                    events = session.table("EVENTS_MILLION")
                    txns = session.table("TRANSACTIONS_MILLION")
                    pipe_events = session.table("PIPE_EVENTS_MILLION")
                    joined = (
                        users.join(events, users["USER_ID"] == events["USER_ID"])
                             .join(txns, users["USER_ID"] == txns["USER_ID"])
                             .join(pipe_events, users["USER_ID"] == pipe_events["USER_ID"])
                             .select(
                                 users["USER_ID"],
                                 users["COUNTRY"],
                                 events["EVENT_TYPE"],
                                 txns["STATUS"],
                                 pipe_events["SOURCE"],
                                 (events["AMOUNT"] + txns["TOTAL"] + pipe_events["AMOUNT"]).alias("TOTAL_VALUE"),
                             )
                    )
                    # Force execution then fail intentionally
                    joined.write.mode("overwrite").save_as_table("ETL_FACT_HEAVY")
                    raise Exception("PATCHIT_HEAVY_ETL_FAIL: intentional failure after heavy Snowpark join")
                """
            ).strip()
            _exec(
                cur,
                f"""
                CREATE OR REPLACE PROCEDURE PATCHIT_HEAVY_ETL_FAIL()
                RETURNS STRING
                LANGUAGE PYTHON
                RUNTIME_VERSION = '3.10'
                PACKAGES = ('snowflake-snowpark-python')
                HANDLER = 'run'
                AS $$
{py_code}
                $$;
                """,
            )

            # Schedule task and run once immediately
            _exec(
                cur,
                f"""
                CREATE OR REPLACE TASK PATCHIT_HEAVY_ETL_TASK
                  WAREHOUSE = {warehouse}
                  SCHEDULE = 'USING CRON */5 * * * * UTC'
                AS
                  CALL PATCHIT_HEAVY_ETL_FAIL();

                ALTER TASK PATCHIT_HEAVY_ETL_TASK RESUME;
                EXECUTE TASK PATCHIT_HEAVY_ETL_TASK;
                """,
            )

            # Wait for task failure so logs are actionable
            history = _wait_for_task_state(cur, "PATCHIT_HEAVY_ETL_TASK")
            if history:
                query_id = history.get("QUERY_ID") or "snowflake-task-unknown"
                log_text = (
                    "Snowflake task PATCHIT_HEAVY_ETL_TASK failed. "
                    f"state={history.get('STATE')} error={history.get('ERROR_MESSAGE')}"
                )
                ingest_url = os.environ.get(
                    "PATCHIT_INGEST_URL",
                    "http://127.0.0.1:8088/events/ingest",
                )
                event = {
                    "event_id": f"snowflake:task:{query_id}",
                    "platform": "snowflake",
                    "pipeline_id": "snowflake:PATCHIT_HEAVY_ETL_TASK",
                    "run_id": query_id,
                    "task_id": "PATCHIT_HEAVY_ETL_TASK",
                    "status": "failed",
                    "metadata": {
                        "log_text": log_text,
                        "snowflake_task_state": history.get("STATE"),
                        "snowflake_error_message": history.get("ERROR_MESSAGE"),
                    },
