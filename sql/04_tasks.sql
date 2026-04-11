-- =============================================================================
-- FILE: 04_tasks.sql
-- PURPOSE: Snowflake Tasks — schedule nightly execution of analytics procedures
-- ACCOUNT: WBZTWSY-KH99814
-- DATABASE: ANALYTICS
-- =============================================================================

USE DATABASE ANALYTICS;
USE SCHEMA   PATCHIT_LABS;

-- =============================================================================
-- TASK 1: TASK_CUSTOMER_LTV
-- Fires daily at 02:00 UTC.
-- Calls SP_COMPUTE_CUSTOMER_LTV() to refresh PATCHIT_LABS.CUSTOMER_LTV.
-- =============================================================================
CREATE OR REPLACE TASK ANALYTICS.PATCHIT_LABS.TASK_CUSTOMER_LTV
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 2 * * * UTC'
    COMMENT   = 'Nightly refresh of CUSTOMER_LTV via SP_COMPUTE_CUSTOMER_LTV'
AS
CALL ANALYTICS.PATCHIT_LABS.SP_COMPUTE_CUSTOMER_LTV();

-- Tasks are created in SUSPENDED state by default; resume to activate.
ALTER TASK ANALYTICS.PATCHIT_LABS.TASK_CUSTOMER_LTV RESUME;

-- =============================================================================
-- TASK 2: TASK_INVENTORY_REORDER
-- Fires daily at 03:00 UTC (after LTV task completes).
-- Calls SP_COMPUTE_INVENTORY_REORDER() to refresh INVENTORY_REORDER_PLAN.
-- Runs with default lead-time derivation (p_lead_time_override = NULL).
-- =============================================================================
CREATE OR REPLACE TASK ANALYTICS.PATCHIT_LABS.TASK_INVENTORY_REORDER
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 3 * * * UTC'
    COMMENT   = 'Nightly refresh of INVENTORY_REORDER_PLAN via SP_COMPUTE_INVENTORY_REORDER'
AS
CALL ANALYTICS.PATCHIT_LABS.SP_COMPUTE_INVENTORY_REORDER();

-- Tasks are created in SUSPENDED state by default; resume to activate.
ALTER TASK ANALYTICS.PATCHIT_LABS.TASK_INVENTORY_REORDER RESUME;
