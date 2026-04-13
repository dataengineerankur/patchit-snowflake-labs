-- =============================================================================
-- FILE: 02_sp_customer_ltv.sql
-- PURPOSE: Stored procedure — compute per-customer Lifetime Value metrics
-- TARGET: ANALYTICS.PATCHIT_LABS.CUSTOMER_LTV
-- SCHEDULE: Called nightly by TASK_CUSTOMER_LTV (02:00 UTC)
-- =============================================================================

CREATE OR REPLACE PROCEDURE ANALYTICS.PATCHIT_LABS.SP_COMPUTE_CUSTOMER_LTV()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
DECLARE
    -- -----------------------------------------------------------------------
    -- Runtime counters
    -- -----------------------------------------------------------------------
    v_run_ts            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP();
    v_rows_merged       INTEGER        DEFAULT 0;
    v_rows_inserted     INTEGER        DEFAULT 0;
    v_rows_updated      INTEGER        DEFAULT 0;
    v_customer_count    INTEGER        DEFAULT 0;
    v_platinum_count    INTEGER        DEFAULT 0;
    v_gold_count        INTEGER        DEFAULT 0;
    v_silver_count      INTEGER        DEFAULT 0;
    v_bronze_count      INTEGER        DEFAULT 0;
    v_result_msg        VARCHAR;

    -- -----------------------------------------------------------------------
    -- Scalar accumulators used in intermediate steps
    -- -----------------------------------------------------------------------
    v_total_revenue     FLOAT          DEFAULT 0.0;
    v_avg_ltv           FLOAT          DEFAULT 0.0;

    -- Decay constant for exponential retention model:
    --   retention_factor = EXP( -lambda * days_since_last_order )
    -- lambda = LN(2) / 180  →  half-life of 180 days
    v_decay_lambda      FLOAT          DEFAULT 0.003851;   -- LN(2)/180

    -- z-score multiplier for the 95th-percentile churn boundary
    v_z95               FLOAT          DEFAULT 1.645;

BEGIN
    -- =======================================================================
    -- STEP 1: Materialise enriched order metrics into a transient stage table
    --         so that downstream steps can reference it without re-scanning
    --         the fact tables.
    -- =======================================================================
    CREATE OR REPLACE TRANSIENT TABLE ANALYTICS.PATCHIT_LABS._TMP_CUST_METRICS AS
    WITH completed_orders AS (
        -- Only 'completed' orders contribute to recognised revenue
        SELECT
            o.ORDER_ID,
            o.CUSTOMER_ID,
            o.ORDER_DATE,
            o.ORDER_UPDATED_AT
        FROM ANALYTICS.RAW.ORDERS  o
        WHERE o.ORDER_STATUS = 'completed'
    ),
    order_revenue AS (
        -- Line-level rollup per order
        SELECT
            oi.ORDER_ID,
            SUM(oi.LINE_TOTAL)          AS order_revenue,
            SUM(oi.QUANTITY)            AS order_units
        FROM ANALYTICS.RAW.ORDER_ITEMS  oi
        GROUP BY oi.ORDER_ID
    ),
    customer_raw_stats AS (
        -- Per-customer aggregates across all completed orders
        SELECT
            c.CUSTOMER_ID,
            c.FIRST_NAME,
            c.LAST_NAME,
            c.EMAIL,
            c.CREATED_AT                AS customer_since,
            COUNT(DISTINCT co.ORDER_ID) AS order_count,
            SUM(orv.order_revenue)      AS total_revenue,
            AVG(orv.order_revenue)      AS avg_order_value,
            SUM(orv.order_units)        AS total_units,
            MIN(co.ORDER_DATE)          AS first_order_date,
            MAX(co.ORDER_DATE)          AS last_order_date,
            -- Rolling 12-month revenue for recency weighting
            SUM(
                CASE
                    WHEN co.ORDER_DATE >= DATEADD('month', -12, CURRENT_DATE())
                    THEN orv.order_revenue
                    ELSE 0
                END
            )                           AS revenue_last_12m,
            -- Rolling 3-month order count (purchase cadence signal)
            COUNT(
                DISTINCT CASE
                    WHEN co.ORDER_DATE >= DATEADD('month', -3, CURRENT_DATE())
                    THEN co.ORDER_ID
                END
            )                           AS orders_last_3m
        FROM ANALYTICS.RAW.CUSTOMERS c
        LEFT JOIN completed_orders      co  ON co.CUSTOMER_ID = c.CUSTOMER_ID
        LEFT JOIN order_revenue         orv ON orv.ORDER_ID    = co.ORDER_ID
        GROUP BY
            c.CUSTOMER_ID,
            c.FIRST_NAME,
            c.LAST_NAME,
            c.EMAIL,
            c.CREATED_AT
    ),
    recency_features AS (
        -- Recency and tenure derived fields
        SELECT
            crs.*,
            DATEDIFF('day',  last_order_date,  CURRENT_DATE()) AS days_since_last_order,
            DATEDIFF('month', first_order_date, CURRENT_DATE()) AS tenure_months,
            -- Inter-order gap (avg days between consecutive orders, order-level window)
            CASE
                WHEN order_count > 1
                THEN DATEDIFF('day', first_order_date, last_order_date)
                         / NULLIF(order_count - 1, 0)
                ELSE NULL
            END                                                 AS avg_days_between_orders
        FROM customer_raw_stats crs
        WHERE order_count > 0   -- exclude customers with zero completed orders
    ),
    ltv_scored AS (
        SELECT
            rf.*,
            -- ----------------------------------------------------------------
            -- Exponential decay retention factor
            -- Higher recency → lower decay → higher retention
            -- ----------------------------------------------------------------
            EXP( -:v_decay_lambda * days_since_last_order )     AS retention_factor,

            -- ----------------------------------------------------------------
            -- Monthly revenue run-rate
            -- tenure_months is the denominator; equals 0 when the customer's
            -- first (and only) completed order falls within the current
            -- calendar month
            -- ----------------------------------------------------------------
            total_revenue / NULLIF(tenure_months, 0)             AS avg_monthly_revenue,

            -- Frequency score: normalise orders_last_3m on [0,1] scale
            -- capped at 10 orders per quarter as a reasonable upper bound
            LEAST(orders_last_3m / 10.0, 1.0)                  AS frequency_score
        FROM recency_features rf
    ),
    ltv_projected AS (
        SELECT
            ls.*,
            -- Forward revenue projection: avg run-rate × 12 months × retention
            avg_monthly_revenue * 12.0 * retention_factor        AS predicted_revenue_12m,

            -- Composite LTV score blending revenue, retention, and frequency
            (avg_monthly_revenue * 12.0 * retention_factor)
                * (0.7 + 0.3 * frequency_score)                  AS ltv_score,

            -- Churn risk is the complement of the retention factor
            1.0 - retention_factor                               AS churn_risk_score
        FROM ltv_scored ls
    ),
    ltv_tiered AS (
        SELECT
            lp.*,
            CASE
                WHEN ltv_score >= 5000 THEN 'PLATINUM'
                WHEN ltv_score >= 2000 THEN 'GOLD'
                WHEN ltv_score >= 500  THEN 'SILVER'
                ELSE                       'BRONZE'
            END                                                  AS ltv_tier
        FROM ltv_projected lp
    ),
    -- Percentile ranks for secondary enrichment / downstream use
    ltv_ranked AS (
        SELECT
            lt.*,
            PERCENT_RANK() OVER (ORDER BY lt.ltv_score)         AS ltv_pct_rank,
            PERCENT_RANK() OVER (ORDER BY lt.total_revenue)     AS revenue_pct_rank,
            ROW_NUMBER()   OVER (ORDER BY lt.ltv_score DESC)    AS ltv_global_rank
        FROM ltv_tiered lt
    )
    SELECT
        lr.CUSTOMER_ID,
        lr.ltv_score,
        lr.ltv_tier,
        lr.predicted_revenue_12m,
        lr.churn_risk_score,
        lr.total_revenue,
        lr.order_count,
        lr.avg_order_value,
        lr.first_order_date,
        lr.last_order_date,
        lr.days_since_last_order,
        lr.tenure_months,
        lr.avg_monthly_revenue,
        lr.retention_factor,
        lr.frequency_score,
        lr.revenue_last_12m,
        lr.ltv_pct_rank,
        lr.revenue_pct_rank,
        lr.ltv_global_rank,
        :v_run_ts                                               AS computed_at
    FROM ltv_ranked lr;

    -- =======================================================================
    -- STEP 2: Capture row count before merge for summary reporting
    -- =======================================================================
    SELECT COUNT(*) INTO :v_customer_count
    FROM ANALYTICS.PATCHIT_LABS._TMP_CUST_METRICS;

    SELECT
        SUM(CASE WHEN ltv_tier = 'PLATINUM' THEN 1 ELSE 0 END),
        SUM(CASE WHEN ltv_tier = 'GOLD'     THEN 1 ELSE 0 END),
        SUM(CASE WHEN ltv_tier = 'SILVER'   THEN 1 ELSE 0 END),
        SUM(CASE WHEN ltv_tier = 'BRONZE'   THEN 1 ELSE 0 END),
        COALESCE(AVG(ltv_score), 0.0),
        COALESCE(SUM(total_revenue), 0.0)
    INTO
        :v_platinum_count,
        :v_gold_count,
        :v_silver_count,
        :v_bronze_count,
        :v_avg_ltv,
        :v_total_revenue
    FROM ANALYTICS.PATCHIT_LABS._TMP_CUST_METRICS;

    -- =======================================================================
    -- STEP 3: Upsert results into PATCHIT_LABS.CUSTOMER_LTV
    -- =======================================================================
    MERGE INTO ANALYTICS.PATCHIT_LABS.CUSTOMER_LTV  tgt
    USING (
        SELECT
            CUSTOMER_ID,
            ltv_score,
            ltv_tier,
            predicted_revenue_12m,
            churn_risk_score,
            computed_at
        FROM ANALYTICS.PATCHIT_LABS._TMP_CUST_METRICS
    ) src
    ON tgt.CUSTOMER_ID = src.CUSTOMER_ID

    WHEN MATCHED
        AND (
            tgt.LTV_SCORE              <> src.ltv_score
         OR tgt.LTV_TIER               <> src.ltv_tier
         OR tgt.PREDICTED_REVENUE_12M  <> src.predicted_revenue_12m
         OR tgt.CHURN_RISK_SCORE       <> src.churn_risk_score
        )
    THEN UPDATE SET
        tgt.LTV_SCORE             = src.ltv_score,
        tgt.LTV_TIER              = src.ltv_tier,
        tgt.PREDICTED_REVENUE_12M = src.predicted_revenue_12m,
        tgt.CHURN_RISK_SCORE      = src.churn_risk_score,
        tgt.COMPUTED_AT           = src.computed_at

    WHEN NOT MATCHED THEN INSERT (
        CUSTOMER_ID,
        LTV_SCORE,
        LTV_TIER,
        PREDICTED_REVENUE_12M,
        CHURN_RISK_SCORE,
        COMPUTED_AT
    ) VALUES (
        src.CUSTOMER_ID,
        src.ltv_score,
        src.ltv_tier,
        src.predicted_revenue_12m,
        src.churn_risk_score,
        src.computed_at
    );

    -- Capture DML row counts from the MERGE statement
    SELECT
        "number of rows inserted",
        "number of rows updated"
    INTO :v_rows_inserted, :v_rows_updated
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    v_rows_merged := v_rows_inserted + v_rows_updated;

    -- =======================================================================
    -- STEP 4: Cleanup transient staging table
    -- =======================================================================
    DROP TABLE IF EXISTS ANALYTICS.PATCHIT_LABS._TMP_CUST_METRICS;

    -- =======================================================================
    -- STEP 5: Build and return summary message
    -- =======================================================================
    v_result_msg :=
        'SP_COMPUTE_CUSTOMER_LTV completed at ' || TO_VARCHAR(v_run_ts) || ' | '
        || 'customers_processed='     || v_customer_count  || ' | '
        || 'rows_merged='             || v_rows_merged     || ' ('
        || 'inserted='                || v_rows_inserted   || ', '
        || 'updated='                 || v_rows_updated    || ') | '
        || 'tier_breakdown: PLATINUM='|| v_platinum_count  || ' '
        || 'GOLD='                    || v_gold_count      || ' '
        || 'SILVER='                  || v_silver_count    || ' '
        || 'BRONZE='                  || v_bronze_count    || ' | '
        || 'avg_ltv='                 || ROUND(v_avg_ltv, 2)        || ' | '
        || 'total_portfolio_revenue=' || ROUND(v_total_revenue, 2);

    RETURN v_result_msg;

EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS ANALYTICS.PATCHIT_LABS._TMP_CUST_METRICS;
        RAISE;
END;
$$;
