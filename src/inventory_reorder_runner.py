"""Inventory Reorder Plan computation pipeline.

Reads product sales velocity from Snowflake, applies ABC classification,
computes safety stock and economic reorder quantities, and writes the plan
to PATCHIT_LABS.INVENTORY_REORDER_PLAN.
"""
from __future__ import annotations

import logging
import math
import os
from datetime import date, datetime, timedelta
from typing import Any

import snowflake.connector

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.environ.get("SNOWFLAKE_ACCOUNT",   "WBZTWSY-KH99814")
SNOWFLAKE_USER      = os.environ.get("SNOWFLAKE_USER",      "PATCHIT")
SNOWFLAKE_PASSWORD  = os.environ.get("SNOWFLAKE_PASSWORD",  "")
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE",      "ACCOUNTADMIN")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE",  "ANALYTICS")

# Model parameters
SERVICE_LEVEL_Z         = 1.65   # 95th-percentile z-score for safety stock
DEFAULT_LEAD_TIME_DAYS  = 14.0   # fallback lead time when no history exists
ABC_THRESHOLDS          = (0.80, 0.95)   # cumulative revenue cut-offs


# ── Database helpers ──────────────────────────────────────────────────────────

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


def fetch_sales_velocity(
    conn: snowflake.connector.SnowflakeConnection,
    lookback_days: int = 90,
) -> list[dict[str, Any]]:
    """Return per-product daily sales velocity over the last *lookback_days* days."""
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    query = f"""
        SELECT
            p.PRODUCT_ID,
            p.SKU,
            p.PRODUCT_NAME,
            p.CATEGORY,
            p.PRICE,
            COUNT(DISTINCT o.ORDER_ID)              AS order_count,
            COALESCE(SUM(oi.QUANTITY), 0)           AS total_qty_sold,
            COALESCE(AVG(oi.QUANTITY), 0.0)         AS avg_qty_per_order,
            COALESCE(STDDEV_POP(oi.QUANTITY), 0.0)  AS std_qty_per_order,
            COALESCE(SUM(oi.LINE_TOTAL), 0.0)       AS total_revenue
        FROM   ANALYTICS.RAW.PRODUCTS     p
        LEFT JOIN ANALYTICS.RAW.ORDER_ITEMS oi ON p.PRODUCT_ID = oi.PRODUCT_ID
        LEFT JOIN ANALYTICS.RAW.ORDERS      o  ON oi.ORDER_ID  = o.ORDER_ID
                                               AND o.ORDER_DATE >= '{cutoff}'
                                               AND o.ORDER_STATUS = 'completed'
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY total_revenue DESC
    """
    cur = conn.cursor(snowflake.connector.DictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows


# ── ABC classification ────────────────────────────────────────────────────────

def _assign_abc_class(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add ABC_CLASS field to each row based on cumulative revenue share."""
    total_rev = sum(float(r["TOTAL_REVENUE"] or 0) for r in rows)
    if total_rev == 0:
        for r in rows:
            r["ABC_CLASS"] = "C"
        return rows

    cumulative = 0.0
    for r in sorted(rows, key=lambda x: float(x["TOTAL_REVENUE"] or 0), reverse=True):
        cumulative += float(r["TOTAL_REVENUE"] or 0)
        share = cumulative / total_rev
        if share <= ABC_THRESHOLDS[0]:
            r["ABC_CLASS"] = "A"
        elif share <= ABC_THRESHOLDS[1]:
            r["ABC_CLASS"] = "B"
        else:
            r["ABC_CLASS"] = "C"
    return rows


# ── Reorder computation ───────────────────────────────────────────────────────

def compute_reorder_record(
    row: dict[str, Any],
    lookback_days: int = 90,
    lead_time_override: float | None = None,
) -> dict[str, Any]:
    """Compute reorder metrics for a single product row.

    Parameters
    ----------
    row:
        A row from ``fetch_sales_velocity`` enriched with ABC_CLASS.
    lookback_days:
        Number of days the velocity query covered.
    lead_time_override:
        If provided, use this as avg_lead_time_days instead of the default.
    """
    product_id       = row["PRODUCT_ID"]
    sku              = row["SKU"]
    total_qty        = float(row["TOTAL_QTY_SOLD"] or 0)
    std_qty          = float(row["STD_QTY_PER_ORDER"] or 0.0)
    avg_qty_order    = float(row["AVG_QTY_PER_ORDER"] or 0.0)

    avg_daily_demand = total_qty / lookback_days          # units / day

    lead_time_days   = lead_time_override if lead_time_override is not None \
                       else DEFAULT_LEAD_TIME_DAYS

    # Safety stock: z * σ_demand / σ_lead_time proxy
    # For simplicity we use daily std-dev = std_qty / sqrt(lookback_days)
    daily_std_dev    = std_qty / math.sqrt(lookback_days) if lookback_days > 0 else 0.0

    safety_stock     = math.ceil(SERVICE_LEVEL_Z * daily_std_dev * math.sqrt(lead_time_days))

    reorder_point    = math.ceil(avg_daily_demand * lead_time_days) + safety_stock

    # Economic reorder quantity: Q = sqrt(2 * D * S / H)
    # When demand_std_dev == 0 (perfectly consistent demand), fall back to
    # a simpler formula using avg_qty_per_order as the order quantity unit.
    demand_std_dev   = float(row["STD_QTY_PER_ORDER"] or 0.0)
    reorder_qty      = math.ceil(
        (avg_daily_demand * lead_time_days) / demand_std_dev
    )

    current_stock    = 0   # would come from inventory system in production

    return {
        "PRODUCT_ID":    product_id,
        "SKU":           sku,
        "CURRENT_STOCK": current_stock,
        "REORDER_QTY":   max(1, reorder_qty),
        "REORDER_POINT": max(0, reorder_point),
        "SAFETY_STOCK":  max(0, safety_stock),
        "ABC_CLASS":     row.get("ABC_CLASS", "C"),
        "COMPUTED_AT":   datetime.utcnow(),
    }


# ── Write results ──────────────────────────────────────────────────────────────

def upsert_reorder_plan(
    conn: snowflake.connector.SnowflakeConnection,
    records: list[dict[str, Any]],
) -> int:
    """Merge reorder records into PATCHIT_LABS.INVENTORY_REORDER_PLAN."""
    if not records:
        return 0

    cur = conn.cursor()
    cur.execute("USE SCHEMA ANALYTICS.PATCHIT_LABS")

    cur.execute("""
        CREATE OR REPLACE TEMPORARY TABLE tmp_reorder_batch (
            PRODUCT_ID    VARCHAR(36),
            SKU           VARCHAR(50),
            CURRENT_STOCK INTEGER,
            REORDER_QTY   INTEGER,
            REORDER_POINT INTEGER,
            SAFETY_STOCK  INTEGER,
            ABC_CLASS     VARCHAR(1),
            COMPUTED_AT   TIMESTAMP_NTZ
        )
    """)

    cur.executemany(
        """
        INSERT INTO tmp_reorder_batch VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                r["PRODUCT_ID"], r["SKU"], r["CURRENT_STOCK"],
                r["REORDER_QTY"], r["REORDER_POINT"], r["SAFETY_STOCK"],
                r["ABC_CLASS"], r["COMPUTED_AT"],
            )
            for r in records
        ],
    )

    cur.execute("""
        MERGE INTO ANALYTICS.PATCHIT_LABS.INVENTORY_REORDER_PLAN tgt
        USING tmp_reorder_batch src
           ON tgt.PRODUCT_ID = src.PRODUCT_ID
        WHEN MATCHED THEN UPDATE SET
            CURRENT_STOCK = src.CURRENT_STOCK,
            REORDER_QTY   = src.REORDER_QTY,
            REORDER_POINT = src.REORDER_POINT,
            SAFETY_STOCK  = src.SAFETY_STOCK,
            ABC_CLASS     = src.ABC_CLASS,
            COMPUTED_AT   = src.COMPUTED_AT
        WHEN NOT MATCHED THEN INSERT VALUES (
            src.PRODUCT_ID, src.SKU, src.CURRENT_STOCK,
            src.REORDER_QTY, src.REORDER_POINT, src.SAFETY_STOCK,
            src.ABC_CLASS, src.COMPUTED_AT
        )
    """)
    merged = cur.rowcount
    cur.close()
    return merged


# ── Main entry-point ──────────────────────────────────────────────────────────

def run(lookback_days: int = 90, lead_time_override: float | None = None) -> None:
    """Full pipeline: fetch → classify → compute → upsert."""
    logger.info("Starting Inventory Reorder pipeline (lookback=%d days)", lookback_days)

    conn = _connect()
    try:
        velocity_rows = fetch_sales_velocity(conn, lookback_days=lookback_days)
        logger.info("Fetched velocity data for %d products", len(velocity_rows))

        velocity_rows = _assign_abc_class(velocity_rows)

        records: list[dict[str, Any]] = []
        for row in velocity_rows:
            try:
                rec = compute_reorder_record(row, lookback_days, lead_time_override)
                records.append(rec)
            except ZeroDivisionError:
                logger.error(
                    "Division by zero for product %s (SKU=%s) — demand_std_dev=%s",
                    row.get("PRODUCT_ID"),
                    row.get("SKU"),
                    row.get("STD_QTY_PER_ORDER"),
                )
                raise

        merged = upsert_reorder_plan(conn, records)
        logger.info("Upserted %d reorder records into INVENTORY_REORDER_PLAN", merged)

    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
