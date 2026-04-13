-- =============================================================================
-- FILE: 03_sp_inventory_reorder.sql
-- PURPOSE: Stored procedure — compute inventory reorder plan per SKU
-- TARGET: ANALYTICS.PATCHIT_LABS.INVENTORY_REORDER_PLAN
-- SCHEDULE: Called nightly by TASK_INVENTORY_REORDER (03:00 UTC)
-- =============================================================================

CREATE OR REPLACE PROCEDURE ANALYTICS.PATCHIT_LABS.SP_COMPUTE_INVENTORY_REORDER(
    p_lead_time_override FLOAT DEFAULT NULL
)
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
DECLARE
    -- -----------------------------------------------------------------------
    -- Runtime counters and control flags
    -- -----------------------------------------------------------------------
    v_run_ts                TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP();
    v_rows_inserted         INTEGER         DEFAULT 0;
    v_rows_updated          INTEGER         DEFAULT 0;
    v_rows_merged           INTEGER         DEFAULT 0;
    v_product_count         INTEGER         DEFAULT 0;
    v_abc_a_count           INTEGER         DEFAULT 0;
    v_abc_b_count           INTEGER         DEFAULT 0;
    v_abc_c_count           INTEGER         DEFAULT 0;
    v_result_msg            VARCHAR;

    -- -----------------------------------------------------------------------
    -- Statistical constants
    -- -----------------------------------------------------------------------
    -- z-score for 95 % service level (standard normal inverse CDF)
    v_z_score               FLOAT           DEFAULT 1.645;

    -- Default lead time in days applied when no override and no history
    v_default_lead_time     FLOAT           DEFAULT 7.0;

    -- Observation windows for velocity calculations
    v_window_30d            INTEGER         DEFAULT 30;
    v_window_60d            INTEGER         DEFAULT 60;
    v_window_90d            INTEGER         DEFAULT 90;

    -- ABC thresholds (cumulative revenue percentiles)
    v_abc_a_pct             FLOAT           DEFAULT 0.80;   -- top 80 % cumulative
    v_abc_b_pct             FLOAT           DEFAULT 0.95;   -- next 15 %

BEGIN
    -- =======================================================================
    -- STEP 1: Compute per-product sales velocity across three rolling windows
    --         and derive demand variability metrics used in safety stock calc.
    -- =======================================================================
    CREATE OR REPLACE TRANSIENT TABLE ANALYTICS.PATCHIT_LABS._TMP_VELOCITY AS
    WITH daily_sales AS (
        -- Explode order_items to daily granularity for each product
        SELECT
            oi.PRODUCT_ID,
            DATE_TRUNC('day', oi.CREATED_AT)     AS sale_date,
            SUM(oi.QUANTITY)                      AS units_sold,
            SUM(oi.LINE_TOTAL)                    AS revenue
        FROM ANALYTICS.RAW.ORDER_ITEMS  oi
        INNER JOIN ANALYTICS.RAW.ORDERS  o
            ON  o.ORDER_ID     = oi.ORDER_ID
            AND o.ORDER_STATUS = 'completed'
        WHERE oi.CREATED_AT >= DATEADD('day', -:v_window_90d, CURRENT_DATE())
        GROUP BY oi.PRODUCT_ID, DATE_TRUNC('day', oi.CREATED_AT)
    ),
    product_windows AS (
        -- Aggregate across each rolling window independently
        SELECT
            ds.PRODUCT_ID,

            -- 30-day window
            SUM(CASE WHEN ds.sale_date >= DATEADD('day', -:v_window_30d, CURRENT_DATE())
                     THEN ds.units_sold ELSE 0 END) / :v_window_30d   AS avg_daily_demand_30d,
            SUM(CASE WHEN ds.sale_date >= DATEADD('day', -:v_window_30d, CURRENT_DATE())
                     THEN ds.revenue    ELSE 0 END)                    AS revenue_30d,

            -- 60-day window
            SUM(CASE WHEN ds.sale_date >= DATEADD('day', -:v_window_60d, CURRENT_DATE())
                     THEN ds.units_sold ELSE 0 END) / :v_window_60d   AS avg_daily_demand_60d,

            -- 90-day window (primary driver for reorder calculations)
            SUM(ds.units_sold) / :v_window_90d                        AS avg_daily_demand_90d,
            SUM(ds.revenue)                                            AS revenue_90d,
            COUNT(DISTINCT ds.sale_date)                               AS active_selling_days
        FROM daily_sales ds
        GROUP BY ds.PRODUCT_ID
    ),
    demand_variability AS (
        -- Standard deviation of daily units sold over the 90-day window
        -- This drives the safety stock buffer calculation
        SELECT
            ds.PRODUCT_ID,
            STDDEV_POP(ds.units_sold)    AS demand_std_dev,
            VAR_POP(ds.units_sold)       AS demand_variance,
            MIN(ds.units_sold)           AS min_daily_units,
            MAX(ds.units_sold)           AS max_daily_units,
            COUNT(*)                     AS observation_days
        FROM daily_sales ds
        GROUP BY ds.PRODUCT_ID
    ),
    simulated_lead_times AS (
        -- In a production system this would join a supplier_lead_times table.
        -- Here we approximate lead time from the gap between order creation
        -- and the order_updated_at timestamp on completed orders.
        -- For products that have no completed orders in the 90-day window
        -- this CTE returns no rows, yielding NULL after the LEFT JOIN.
        SELECT
            oi.PRODUCT_ID,
            AVG(
                DATEDIFF('day', o.ORDER_DATE, o.ORDER_UPDATED_AT)
            )                            AS avg_lead_time_days
        FROM ANALYTICS.RAW.ORDER_ITEMS  oi
        INNER JOIN ANALYTICS.RAW.ORDERS  o
            ON  o.ORDER_ID     = oi.ORDER_ID
            AND o.ORDER_STATUS = 'completed'
        WHERE oi.CREATED_AT >= DATEADD('day', -:v_window_90d, CURRENT_DATE())
        GROUP BY oi.PRODUCT_ID
    )
    SELECT
        pw.PRODUCT_ID,
        pw.avg_daily_demand_30d,
        pw.avg_daily_demand_60d,
        pw.avg_daily_demand_90d,
        pw.revenue_30d,
        pw.revenue_90d,
        pw.active_selling_days,
        COALESCE(dv.demand_std_dev,  0.0)   AS demand_std_dev,
        COALESCE(dv.demand_variance, 0.0)   AS demand_variance,
        COALESCE(dv.min_daily_units, 0.0)   AS min_daily_units,
        COALESCE(dv.max_daily_units, 0.0)   AS max_daily_units,
        COALESCE(dv.observation_days, 0)    AS observation_days,
        -- Lead time: override parameter takes priority, then historical avg,
        -- then the procedure-level default is applied in STEP 2
        COALESCE(
            :p_lead_time_override,
            slt.avg_lead_time_days
        )                                   AS avg_lead_time_days
    FROM product_windows                pw
    LEFT JOIN demand_variability        dv  ON dv.PRODUCT_ID = pw.PRODUCT_ID
    LEFT JOIN simulated_lead_times      slt ON slt.PRODUCT_ID = pw.PRODUCT_ID;

    -- =======================================================================
    -- STEP 2: Enrich with product master data, compute reorder metrics,
    --         and perform ABC classification by cumulative revenue share.
    -- =======================================================================
    CREATE OR REPLACE TRANSIENT TABLE ANALYTICS.PATCHIT_LABS._TMP_REORDER AS
    WITH product_enriched AS (
        SELECT
            p.PRODUCT_ID,
            p.SKU,
            p.PRODUCT_NAME,
            p.CATEGORY,
            p.PRICE,
            -- Proxy current_stock from historical sell-through (no warehouse
            -- management system in scope); in production replace with WMS join
            GREATEST(
                1000.0
                - COALESCE(v.avg_daily_demand_90d * :v_window_90d, 0),
                0
            )                                                   AS current_stock,
            COALESCE(v.avg_daily_demand_30d, 0.0)              AS avg_daily_demand_30d,
            COALESCE(v.avg_daily_demand_60d, 0.0)              AS avg_daily_demand_60d,
            COALESCE(v.avg_daily_demand_90d, 0.0)              AS avg_daily_demand_90d,
            COALESCE(v.revenue_30d,          0.0)              AS revenue_30d,
            COALESCE(v.revenue_90d,          0.0)              AS revenue_90d,
            COALESCE(v.active_selling_days,  0)                AS active_selling_days,
            COALESCE(v.demand_std_dev,       0.0)              AS demand_std_dev,
            COALESCE(v.demand_variance,      0.0)              AS demand_variance,
            COALESCE(v.observation_days,     0)                AS observation_days,
            -- Apply procedure default when all other lead-time sources are NULL
            COALESCE(v.avg_lead_time_days, :v_default_lead_time) AS avg_lead_time_days
        FROM ANALYTICS.RAW.PRODUCTS     p
        LEFT JOIN ANALYTICS.PATCHIT_LABS._TMP_VELOCITY  v
            ON  v.PRODUCT_ID = p.PRODUCT_ID
    ),
    reorder_calcs AS (
        SELECT
            pe.*,
            -- ------------------------------------------------------------------
            -- Safety stock: covers demand variability over the replenishment
            -- lead time at the configured service level (95 %).
            -- Formula: Z × σ_demand × √(lead_time)
            -- ------------------------------------------------------------------
            :v_z_score * pe.demand_std_dev * SQRT(pe.avg_lead_time_days) AS safety_stock,

            -- ------------------------------------------------------------------
            -- Reorder point: expected demand during lead time plus safety buffer
            -- ------------------------------------------------------------------
            (pe.avg_daily_demand_90d * pe.avg_lead_time_days)
                + (:v_z_score * pe.demand_std_dev * SQRT(pe.avg_lead_time_days)) AS reorder_point,

            -- ------------------------------------------------------------------
            -- Reorder quantity: economic order quantity proxy using lead-time
            -- demand scaled by variability coefficient.
            -- For products with perfectly consistent demand (demand_std_dev = 0),
            -- this resolves to division by zero.
            -- ------------------------------------------------------------------
            CEIL(
                (pe.avg_daily_demand_90d * pe.avg_lead_time_days)
                / NULLIF(pe.demand_std_dev, 0)
            )                                                    AS reorder_qty
        FROM product_enriched pe
    ),
    abc_revenue_rank AS (
        -- Rank products by descending 90-day revenue for ABC classification
        SELECT
            rc.*,
            SUM(rc.revenue_90d) OVER ()                         AS total_portfolio_revenue_90d,
            SUM(rc.revenue_90d) OVER (
                ORDER BY rc.revenue_90d DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )                                                   AS cumulative_revenue_90d
        FROM reorder_calcs rc
    ),
    abc_classified AS (
        SELECT
            ar.*,
            ar.cumulative_revenue_90d
                / NULLIF(ar.total_portfolio_revenue_90d, 0)    AS cumulative_revenue_pct,
            CASE
                WHEN (ar.cumulative_revenue_90d
                          / NULLIF(ar.total_portfolio_revenue_90d, 0)) <= :v_abc_a_pct
                THEN 'A'
                WHEN (ar.cumulative_revenue_90d
                          / NULLIF(ar.total_portfolio_revenue_90d, 0)) <= :v_abc_b_pct
                THEN 'B'
                ELSE 'C'
            END                                                 AS abc_class
        FROM abc_revenue_rank ar
    )
    SELECT
        ac.PRODUCT_ID,
        ac.SKU,
        ac.PRODUCT_NAME,
        ac.CATEGORY,
        ac.PRICE,
        ac.current_stock,
        ac.avg_daily_demand_30d,
        ac.avg_daily_demand_60d,
        ac.avg_daily_demand_90d,
        ac.revenue_30d,
        ac.revenue_90d,
        ac.demand_std_dev,
        ac.observation_days,
        ac.avg_lead_time_days,
        ac.safety_stock,
        ac.reorder_point,
        GREATEST(ac.reorder_qty, 1)                             AS reorder_qty,
        ac.abc_class,
        ac.cumulative_revenue_pct,
        :v_run_ts                                               AS computed_at
    FROM abc_classified ac;

    -- =======================================================================
    -- STEP 3: Capture summary counts before merge
    -- =======================================================================
    SELECT COUNT(*) INTO :v_product_count
    FROM ANALYTICS.PATCHIT_LABS._TMP_REORDER;

    SELECT
        SUM(CASE WHEN abc_class = 'A' THEN 1 ELSE 0 END),
        SUM(CASE WHEN abc_class = 'B' THEN 1 ELSE 0 END),
        SUM(CASE WHEN abc_class = 'C' THEN 1 ELSE 0 END)
    INTO :v_abc_a_count, :v_abc_b_count, :v_abc_c_count
    FROM ANALYTICS.PATCHIT_LABS._TMP_REORDER;

    -- =======================================================================
    -- STEP 4: Upsert into PATCHIT_LABS.INVENTORY_REORDER_PLAN
    -- =======================================================================
    MERGE INTO ANALYTICS.PATCHIT_LABS.INVENTORY_REORDER_PLAN  tgt
    USING (
        SELECT
            PRODUCT_ID,
            SKU,
            current_stock,
            reorder_qty,
            reorder_point,
            safety_stock,
            abc_class,
            computed_at
        FROM ANALYTICS.PATCHIT_LABS._TMP_REORDER
    ) src
    ON tgt.PRODUCT_ID = src.PRODUCT_ID

    WHEN MATCHED
        AND (
            tgt.REORDER_QTY   <> src.reorder_qty
         OR tgt.REORDER_POINT <> src.reorder_point
         OR tgt.SAFETY_STOCK  <> src.safety_stock
         OR tgt.ABC_CLASS     <> src.abc_class
         OR tgt.CURRENT_STOCK <> src.current_stock
        )
    THEN UPDATE SET
        tgt.SKU           = src.SKU,
        tgt.CURRENT_STOCK = src.current_stock,
        tgt.REORDER_QTY   = src.reorder_qty,
        tgt.REORDER_POINT = src.reorder_point,
        tgt.SAFETY_STOCK  = src.safety_stock,
        tgt.ABC_CLASS     = src.abc_class,
        tgt.COMPUTED_AT   = src.computed_at

    WHEN NOT MATCHED THEN INSERT (
        PRODUCT_ID,
        SKU,
        CURRENT_STOCK,
        REORDER_QTY,
        REORDER_POINT,
        SAFETY_STOCK,
        ABC_CLASS,
        COMPUTED_AT
    ) VALUES (
        src.PRODUCT_ID,
        src.SKU,
        src.current_stock,
        src.reorder_qty,
        src.reorder_point,
        src.safety_stock,
        src.abc_class,
        src.computed_at
    );

    SELECT
        "number of rows inserted",
        "number of rows updated"
    INTO :v_rows_inserted, :v_rows_updated
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    v_rows_merged := v_rows_inserted + v_rows_updated;

    -- =======================================================================
    -- STEP 5: Cleanup transient staging tables
    -- =======================================================================
    DROP TABLE IF EXISTS ANALYTICS.PATCHIT_LABS._TMP_VELOCITY;
    DROP TABLE IF EXISTS ANALYTICS.PATCHIT_LABS._TMP_REORDER;

    -- =======================================================================
    -- STEP 6: Return execution summary
    -- =======================================================================
    v_result_msg :=
        'SP_COMPUTE_INVENTORY_REORDER completed at ' || TO_VARCHAR(v_run_ts) || ' | '
        || 'lead_time_override='  || COALESCE(TO_VARCHAR(:p_lead_time_override), 'NULL (dynamic)') || ' | '
        || 'products_processed='  || v_product_count  || ' | '
        || 'rows_merged='         || v_rows_merged     || ' ('
        || 'inserted='            || v_rows_inserted   || ', '
        || 'updated='             || v_rows_updated    || ') | '
        || 'abc_breakdown: A='    || v_abc_a_count     || ' '
        || 'B='                   || v_abc_b_count     || ' '
        || 'C='                   || v_abc_c_count;

    RETURN v_result_msg;

EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS ANALYTICS.PATCHIT_LABS._TMP_VELOCITY;
        DROP TABLE IF EXISTS ANALYTICS.PATCHIT_LABS._TMP_REORDER;
        RAISE;
END;
$$;
