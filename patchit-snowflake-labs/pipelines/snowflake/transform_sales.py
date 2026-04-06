"""
transform_sales.py

Daily sales pipeline — Snowflake ETL transform step.

Reads raw order data from the ORDERS_STAGING table, applies business-rule
transformations, and writes the enriched dataset to the SALES_FACT table.

Schema: SALES_DB.PUBLIC
Task:   DAILY_SALES_PIPELINE >> transform_sales
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

SNOWFLAKE_CONNECTION_PARAMS = {
    "account": "xy12345.us-east-1",
    "database": "SALES_DB",
    "schema": "PUBLIC",
    "warehouse": "COMPUTE_WH",
    "role": "PIPELINE_ROLE",
}


def get_session(connection_params: Optional[dict] = None) -> Session:
    """Return an authenticated Snowpark session."""
    params = connection_params or SNOWFLAKE_CONNECTION_PARAMS
    return Session.builder.configs(params).create()


def extract_orders(session: Session) -> pd.DataFrame:
    """Pull raw orders from staging into a Pandas DataFrame for transformation."""
    logger.info("Extracting raw orders from ORDERS_STAGING ...")
    df = session.sql("SELECT * FROM ORDERS_STAGING").to_pandas()
    logger.info("Extracted %d rows from ORDERS_STAGING.", len(df))
    return df


def validate_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Reject rows missing required business keys."""
    required_cols = ["order_id", "customer_id", "order_date", "total_amount"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"ORDERS_STAGING is missing required columns: {missing}")
    before = len(df)
    df = df.dropna(subset=required_cols)
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %d rows with null required fields.", dropped)
    return df


def transform_orders(df: pd.DataFrame, run_date: Optional[datetime] = None) -> pd.DataFrame:
    """Apply business-rule transformations to the raw orders DataFrame."""
    run_date = run_date or datetime.utcnow()

    # Normalize column names to lowercase
    df.columns = [c.lower() for c in df.columns]

    # Derive revenue tier
    conditions = [
        df["total_amount"] < 100,
        (df["total_amount"] >= 100) & (df["total_amount"] < 1000),
        df["total_amount"] >= 1000,
    ]
    choices = ["low", "medium", "high"]
    df["revenue_tier"] = pd.Series(
        pd.cut(
            df["total_amount"],
            bins=[-float("inf"), 100, 1000, float("inf")],
            labels=choices,
        )
    )

    # Add pipeline metadata
    df["pipeline_run_date"] = run_date.date()
    df["pipeline_version"] = "1.4.2"

    return df


def load_to_fact(session: Session, df: pd.DataFrame, target_table: str = "SALES_FACT") -> None:
    """Write the transformed DataFrame back into Snowflake."""
    logger.info("Loading %d rows into %s ...", len(df), target_table)
    snow_df = session.create_dataframe(df)
    snow_df.write.mode("append").save_as_table(target_table)
    logger.info("Load complete.")


def run_pipeline(connection_params: Optional[dict] = None) -> None:
    """End-to-end entry point: extract → validate → transform → load."""
    session = get_session(connection_params)
    try:
        raw_df = extract_orders(session)
        valid_df = validate_orders(raw_df)
        transformed_df = transform_orders(valid_df)
        load_to_fact(session, transformed_df)
        logger.info("daily_sales_pipeline completed successfully.")
    except Exception:
        logger.exception("daily_sales_pipeline failed.")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_pipeline()

# PATCHIT: increase warehouse to handle timeout
WAREHOUSE_SIZE = 'Medium'  # was: Small
AUTO_SUSPEND = 600  # seconds
