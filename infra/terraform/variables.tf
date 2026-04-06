# PATCHIT auto-fix: unknown
# Original error: snowflake.connector.errors.ProgrammingError: 000904 invalid identifier CUSTOMER_SEGMENT. Column renamed to SEGMENT in migration v3. Update SELECT statement.
# PATCHIT auto-fix: increase_warehouse_size
# Original error: snowflake.connector.errors.ProgrammingError: Query execution exceeded statement_timeout_in_seconds=1800. Add partition filter or increase timeout.
# PATCHIT auto-fix: grant_permissions
# Original error: snowflake.connector.errors.ProgrammingError: Object ANALYTICS_DB.PUBLIC does not exist or not authorized. Grant USAGE ON SCHEMA ANALYTICS_DB.PUBLIC to LOADER_ROLE.
# PATCHIT auto-fix: grant_permissions
# Original error: snowflake.connector.errors.ProgrammingError: Insufficient privileges to operate on warehouse COMPUTE_WH. Grant USAGE ON WAREHOUSE COMPUTE_WH to LOADER_ROLE.
variable "snowflake_account" {
  type        = string
  description = "Snowflake account identifier"
}

variable "snowflake_user" {
  type        = string
  description = "Snowflake user"
}

variable "snowflake_role" {
  type        = string
  description = "Snowflake role"
}

variable "snowflake_warehouse" {
  type        = string
  description = "Snowflake warehouse name"
}

variable "snowflake_database" {
  type        = string
  description = "Snowflake database name"
}

variable "snowflake_schema" {
  type        = string
  description = "Snowflake schema name"
}

variable "create_database" {
  type        = bool
  description = "Create database if it does not exist"
  default     = true
}

variable "create_schema" {
  type        = bool
  description = "Create schema if it does not exist"
  default     = true
}

variable "create_warehouse" {
  type        = bool
  description = "Create warehouse if it does not exist"
  default     = false
}

variable "create_snowpark_procedure" {
  type        = bool
  description = "Create Snowpark Python stored procedure for transform"
  default     = true
}

variable "enable_apply" {
  type        = bool
  description = "Safety switch. Must be true to create cloud resources."
  default     = false
}
