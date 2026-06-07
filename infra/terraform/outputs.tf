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
