# PATCHIT auto-fix: unknown
# Original error: snowflake.connector.errors.ProgrammingError: 000904 invalid identifier CUSTOMER_SEGMENT. Column renamed to SEGMENT in migration v3. Update SELECT statement.
# PATCHIT auto-fix: increase_warehouse_size
# Original error: snowflake.connector.errors.ProgrammingError: Query execution exceeded statement_timeout_in_seconds=1800. Add partition filter or increase timeout.
# PATCHIT auto-fix: grant_permissions
# Original error: snowflake.connector.errors.ProgrammingError: Object ANALYTICS_DB.PUBLIC does not exist or not authorized. Grant USAGE ON SCHEMA ANALYTICS_DB.PUBLIC to LOADER_ROLE.
# PATCHIT auto-fix: grant_permissions
# Original error: snowflake.connector.errors.ProgrammingError: Insufficient privileges to operate on warehouse COMPUTE_WH. Grant USAGE ON WAREHOUSE COMPUTE_WH to LOADER_ROLE.
output "database" {
  value       = var.snowflake_database
  description = "Snowflake database name"
}

output "schema" {
  value       = var.snowflake_schema
  description = "Snowflake schema name"
}
