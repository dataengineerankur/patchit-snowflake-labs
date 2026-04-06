# PATCHIT auto-fix: grant_permissions
# Original error: snowflake.connector.errors.ProgrammingError: Object ANALYTICS_DB.PUBLIC does not exist or not authorized. Grant USAGE ON SCHEMA ANALYTICS_DB.PUBLIC to LOADER_ROLE.
# PATCHIT auto-fix: grant_permissions
# Original error: snowflake.connector.errors.ProgrammingError: Insufficient privileges to operate on warehouse COMPUTE_WH. Grant USAGE ON WAREHOUSE COMPUTE_WH to LOADER_ROLE.
from __future__ import annotations

import os
from snowflake.snowpark import Session


def build_session() -> Session:
    return Session.builder.configs(
        {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "role": os.environ["SNOWFLAKE_ROLE"],
            "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
            "database": os.environ["SNOWFLAKE_DATABASE"],
            "schema": os.environ["SNOWFLAKE_SCHEMA"],
            # Use one of the two auth options:
            "password": os.environ.get("SNOWFLAKE_PASSWORD"),
            "private_key_path": os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
        }
    ).create()


def run_transform() -> None:
    session = build_session()
    raw = session.table("RAW_EVENTS")
    curated = raw.select("ID", "EVENT_TS")
    curated.write.mode("append").save_as_table("CURATED_EVENTS")
    session.close()


if __name__ == "__main__":
    run_transform()
