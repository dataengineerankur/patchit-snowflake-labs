-- SF001 - Warehouse access control
-- Category: security_access
-- Description: Grant LOADER_ROLE usage on COMPUTE_WH warehouse
-- Fix for: SQL access control error - Insufficient privileges to operate on warehouse

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS LOADER_ROLE;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE LOADER_ROLE;

USE ROLE LOADER_ROLE;
USE WAREHOUSE COMPUTE_WH;

SELECT 'Access granted successfully' AS status;
