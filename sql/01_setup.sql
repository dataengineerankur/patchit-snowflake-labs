-- =============================================================================
-- FILE: 01_setup.sql
-- PURPOSE: Bootstrap ANALYTICS.PATCHIT_LABS schema and output tables
-- ACCOUNT: WBZTWSY-KH99814
-- DATABASE: ANALYTICS
-- =============================================================================

USE DATABASE ANALYTICS;

-- ---------------------------------------------------------------------------
-- Schema
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ANALYTICS.PATCHIT_LABS
    DATA_RETENTION_TIME_IN_DAYS = 14
    COMMENT = 'PatchIt Labs computed analytics outputs — LTV, inventory planning, segmentation';

-- ---------------------------------------------------------------------------
-- CUSTOMER_LTV
-- Stores per-customer lifetime value scores, tier classifications,
-- 12-month forward revenue projections, and churn risk signals.
-- Written by SP_COMPUTE_CUSTOMER_LTV; consumed by BI / downstream ML models.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ANALYTICS.PATCHIT_LABS.CUSTOMER_LTV (
    CUSTOMER_ID         NUMBER(38, 0)   NOT NULL
        COMMENT 'FK → ANALYTICS.RAW.CUSTOMERS.CUSTOMER_ID',
    LTV_SCORE           FLOAT           NOT NULL
        COMMENT 'Composite lifetime-value score (revenue × retention_factor)',
    LTV_TIER            VARCHAR(16)     NOT NULL
        COMMENT 'PLATINUM | GOLD | SILVER | BRONZE',
    PREDICTED_REVENUE_12M FLOAT         NOT NULL
        COMMENT 'Forward 12-month revenue estimate (avg_monthly_revenue × 12 × retention_factor)',
    CHURN_RISK_SCORE    FLOAT           NOT NULL
        COMMENT '0 = no churn risk, 1 = definite churn (1 − retention_factor)',
    COMPUTED_AT         TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'Wall-clock timestamp of last computation run',

    CONSTRAINT PK_CUSTOMER_LTV PRIMARY KEY (CUSTOMER_ID)
)
DATA_RETENTION_TIME_IN_DAYS = 14
COMMENT = 'Customer lifetime-value outputs produced by SP_COMPUTE_CUSTOMER_LTV';

-- ---------------------------------------------------------------------------
-- INVENTORY_REORDER_PLAN
-- Stores per-SKU reorder recommendations: safety stock, reorder point,
-- suggested reorder quantity, and ABC classification.
-- Written by SP_COMPUTE_INVENTORY_REORDER; consumed by procurement workflows.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ANALYTICS.PATCHIT_LABS.INVENTORY_REORDER_PLAN (
    PRODUCT_ID          NUMBER(38, 0)   NOT NULL
        COMMENT 'FK → ANALYTICS.RAW.PRODUCTS.PRODUCT_ID',
    SKU                 VARCHAR(64)     NOT NULL
        COMMENT 'Stock-keeping unit code from ANALYTICS.RAW.PRODUCTS.SKU',
    CURRENT_STOCK       NUMBER(18, 2)   NOT NULL
        COMMENT 'On-hand units at time of computation (sourced from RAW.PRODUCTS)',
    REORDER_QTY         NUMBER(18, 2)   NOT NULL
        COMMENT 'Recommended purchase quantity for the next replenishment cycle',
    REORDER_POINT       FLOAT           NOT NULL
        COMMENT 'Inventory level that triggers a reorder (avg_daily_demand × lead_time + safety_stock)',
    SAFETY_STOCK        FLOAT           NOT NULL
        COMMENT 'Buffer stock to absorb demand variability (z_score × std_dev × SQRT(lead_time))',
    ABC_CLASS           VARCHAR(1)      NOT NULL
        COMMENT 'A = top-20 % revenue, B = next-30 %, C = remaining 50 %',
    COMPUTED_AT         TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'Wall-clock timestamp of last computation run',

    CONSTRAINT PK_INVENTORY_REORDER_PLAN PRIMARY KEY (PRODUCT_ID)
)
DATA_RETENTION_TIME_IN_DAYS = 14
COMMENT = 'Inventory reorder plan produced by SP_COMPUTE_INVENTORY_REORDER';

-- ---------------------------------------------------------------------------
-- Grants (adjust role names to match your account RBAC model)
-- ---------------------------------------------------------------------------
GRANT USAGE  ON SCHEMA  ANALYTICS.PATCHIT_LABS                           TO ROLE TRANSFORMER;
GRANT SELECT ON TABLE   ANALYTICS.PATCHIT_LABS.CUSTOMER_LTV              TO ROLE REPORTER;
GRANT SELECT ON TABLE   ANALYTICS.PATCHIT_LABS.INVENTORY_REORDER_PLAN    TO ROLE REPORTER;
