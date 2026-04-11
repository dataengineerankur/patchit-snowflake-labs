"""Customer Lifetime Value computation pipeline.

Fetches order history from Snowflake, computes LTV metrics per customer,
and writes enriched results back to the PATCHIT_LABS.CUSTOMER_LTV table.
"""
from __future__ import annotations

import logging
import math
import os
from datetime import date, datetime
from typing import Any

import snowflake.connector

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.environ.get("SNOWFLAKE_ACCOUNT",   "WBZTWSY-KH99814")
SNOWFLAKE_USER      = os.environ.get("SNOWFLAKE_USER",      "PATCHIT")
SNOWFLAKE_PASSWORD  = os.environ.get("SNOWFLAKE_PASSWORD",  "")
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE",      "ACCOUNTADMIN")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE",  "ANALYTICS")
SNOWFLAKE_SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA",    "PATCHIT_LABS")

# LTV model hyper-parameters
RETENTION_HALFLIFE_DAYS: float = 180.0   # days until retention factor halves
LTV_TIERS = [
    ("PLATINUM", 5_000),
    ("GOLD",     2_000),
    ("SILVER",     500),
    ("BRONZE",       0),
]


# ── Database helpers ─────────────────────────────────────────────────────────

def _connect() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="RAW",
    )


def fetch_customer_order_stats(conn: snowflake.connector.SnowflakeConnection) -> list[dict[str, Any]]:
    """Return per-customer aggregated order statistics for completed orders."""
    query = """
        SELECT
            c.CUSTOMER_ID,
            c.FIRST_NAME,
            c.LAST_NAME,
            c.EMAIL,
            c.CREATED_AT                                         AS customer_since,
            COUNT(DISTINCT o.ORDER_ID)                           AS order_count,
            COALESCE(SUM(oi.LINE_TOTAL), 0.0)                    AS total_revenue,
            COALESCE(AVG(oi.LINE_TOTAL), 0.0)                    AS avg_order_value,
            MIN(o.ORDER_DATE)                                    AS first_order_date,
            MAX(o.ORDER_DATE)                                    AS last_order_date
        FROM   ANALYTICS.RAW.CUSTOMERS  c
        LEFT JOIN ANALYTICS.RAW.ORDERS  o
               ON c.CUSTOMER_ID = o.CUSTOMER_ID
              AND o.ORDER_STATUS = 'completed'
        LEFT JOIN ANALYTICS.RAW.ORDER_ITEMS oi
               ON o.ORDER_ID = oi.ORDER_ID
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY total_revenue DESC
    """
    cur = conn.cursor(snowflake.connector.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows


# ── LTV computation ───────────────────────────────────────────────────────────

def _retention_factor(last_order_date: date | None, reference_date: date) -> float:
    """Exponential retention decay based on recency."""
    if last_order_date is None:
        return 0.0
    days_since = (reference_date - last_order_date).days
    return math.exp(-days_since * math.log(2) / RETENTION_HALFLIFE_DAYS)


def _ltv_tier(ltv_score: float) -> str:
    for tier, threshold in LTV_TIERS:
        if ltv_score >= threshold:
            return tier
    return "BRONZE"


def compute_ltv_record(row: dict[str, Any], reference_date: date) -> dict[str, Any]:
    """Compute LTV metrics for a single customer row.

    Parameters
    ----------
    row:
        A row returned by ``fetch_customer_order_stats``.  Keys are upper-cased
        Snowflake column names (CUSTOMER_ID, ORDER_COUNT, TOTAL_REVENUE, …).
    reference_date:
        The date used as "today" for recency calculations (allows back-testing).

    Returns
    -------
    dict with LTV_SCORE, LTV_TIER, PREDICTED_REVENUE_12M, CHURN_RISK_SCORE.
    """
    customer_id    = row["CUSTOMER_ID"]
    total_revenue  = float(row["TOTAL_REVENUE"] or 0.0)
    order_count    = int(row["ORDER_COUNT"] or 0)
    first_order_dt = row["FIRST_ORDER_DATE"]
    last_order_dt  = row["LAST_ORDER_DATE"]

    if order_count == 0 or first_order_dt is None:
        return {
            "CUSTOMER_ID":          customer_id,
            "LTV_SCORE":            0.0,
            "LTV_TIER":             "BRONZE",
            "PREDICTED_REVENUE_12M": 0.0,
            "CHURN_RISK_SCORE":     1.0,
            "COMPUTED_AT":          datetime.utcnow(),
        }

    # Convert Snowflake date objects to Python date if needed
    if isinstance(first_order_dt, datetime):
        first_order_dt = first_order_dt.date()
    if isinstance(last_order_dt, datetime):
        last_order_dt = last_order_dt.date()

    retention    = _retention_factor(last_order_dt, reference_date)
    churn_risk   = round(1.0 - retention, 4)

    # Tenure in calendar months from first purchase to today
    tenure_months = (
        (reference_date.year - first_order_dt.year) * 12
        + (reference_date.month - first_order_dt.month)
    )

    # Average monthly run-rate — divides total revenue by tenure in months
    avg_monthly_revenue = total_revenue / tenure_months if tenure_months > 0 else total_revenue

    predicted_12m  = round(avg_monthly_revenue * 12.0 * retention, 2)
    ltv_score      = round(predicted_12m * (1 + order_count * 0.02), 2)
    tier           = _ltv_tier(ltv_score)

    return {
        "CUSTOMER_ID":           customer_id,
        "LTV_SCORE":             ltv_score,
        "LTV_TIER":              tier,
        "PREDICTED_REVENUE_12M": predicted_12m,
        "CHURN_RISK_SCORE":      churn_risk,
        "COMPUTED_AT":           datetime.utcnow(),
    }


# ── Write results ─────────────────────────────────────────────────────────────

def upsert_ltv_records(
    conn: snowflake.connector.SnowflakeConnection,
    records: list[dict[str, Any]],
) -> int:
    """Merge LTV records into PATCHIT_LABS.CUSTOMER_LTV. Returns rows merged."""
    if not records:
        return 0

    cur = conn.cursor()
    cur.execute("USE SCHEMA ANALYTICS.PATCHIT_LABS")

    # Create a temp table and bulk-insert, then MERGE
    cur.execute("""
        CREATE OR REPLACE TEMPORARY TABLE tmp_ltv_batch (
            CUSTOMER_ID           VARCHAR(36),
            LTV_SCORE             FLOAT,
            LTV_TIER              VARCHAR(20),
            PREDICTED_REVENUE_12M FLOAT,
            CHURN_RISK_SCORE      FLOAT,
            COMPUTED_AT           TIMESTAMP_NTZ
        )
    """)

    cur.executemany(
        """
        INSERT INTO tmp_ltv_batch VALUES (%s, %s, %s, %s, %s, %s)
        """,
        [
            (
                r["CUSTOMER_ID"], r["LTV_SCORE"], r["LTV_TIER"],
                r["PREDICTED_REVENUE_12M"], r["CHURN_RISK_SCORE"], r["COMPUTED_AT"],
            )
            for r in records
        ],
    )

    cur.execute("""
        MERGE INTO ANALYTICS.PATCHIT_LABS.CUSTOMER_LTV tgt
        USING tmp_ltv_batch src
           ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
        WHEN MATCHED THEN UPDATE SET
            LTV_SCORE             = src.LTV_SCORE,
            LTV_TIER              = src.LTV_TIER,
            PREDICTED_REVENUE_12M = src.PREDICTED_REVENUE_12M,
            CHURN_RISK_SCORE      = src.CHURN_RISK_SCORE,
            COMPUTED_AT           = src.COMPUTED_AT
        WHEN NOT MATCHED THEN INSERT (
            CUSTOMER_ID, LTV_SCORE, LTV_TIER,
            PREDICTED_REVENUE_12M, CHURN_RISK_SCORE, COMPUTED_AT
        ) VALUES (
            src.CUSTOMER_ID, src.LTV_SCORE, src.LTV_TIER,
            src.PREDICTED_REVENUE_12M, src.CHURN_RISK_SCORE, src.COMPUTED_AT
        )
    """)
    merged = cur.rowcount
    cur.close()
    return merged


# ── Main entry-point ──────────────────────────────────────────────────────────

def run(reference_date: date | None = None) -> None:
    """Full pipeline: fetch → compute → upsert."""
    if reference_date is None:
        reference_date = date.today()

    logger.info("Starting Customer LTV pipeline for reference date %s", reference_date)

    conn = _connect()
    try:
        stats = fetch_customer_order_stats(conn)
        logger.info("Fetched %d customer rows", len(stats))

        records = []
        for row in stats:
            try:
                rec = compute_ltv_record(row, reference_date)
                records.append(rec)
            except ZeroDivisionError:
                logger.error(
                    "Division by zero for customer %s — tenure_months=%s",
                    row.get("CUSTOMER_ID"),
                    row.get("FIRST_ORDER_DATE"),
                )
                raise

        merged = upsert_ltv_records(conn, records)
        logger.info("Upserted %d LTV records into CUSTOMER_LTV", merged)

    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
