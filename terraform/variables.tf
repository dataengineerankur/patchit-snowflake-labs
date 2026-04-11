##############################################################################
# variables.tf
# Input variable definitions for the patchit-snowflake-labs Terraform module.
##############################################################################

variable "snowflake_account" {
  description = "Snowflake account identifier in the format <org>-<account> (e.g. WBZTWSY-KH99814)"
  type        = string
  sensitive   = false

  validation {
    condition     = can(regex("^[A-Z0-9]+-[A-Z0-9]+$", upper(var.snowflake_account)))
    error_message = "snowflake_account must match the pattern ORG-ACCOUNT (e.g. WBZTWSY-KH99814)."
  }
}

variable "snowflake_user" {
  description = "Snowflake username used by Terraform to authenticate and provision resources"
  type        = string
  sensitive   = false
}

variable "snowflake_role" {
  description = "Snowflake role assumed by Terraform during provisioning. Must have CREATE SCHEMA / TABLE / PROCEDURE / TASK privileges on the target database."
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_warehouse" {
  description = "Virtual warehouse used for Terraform operations and assigned to scheduled tasks"
  type        = string
  default     = "COMPUTE_WH"
}

variable "snowflake_database" {
  description = "Snowflake database that hosts both the RAW source schema and the PATCHIT_LABS output schema"
  type        = string
  default     = "ANALYTICS"

  validation {
    condition     = length(trimspace(var.snowflake_database)) > 0
    error_message = "snowflake_database must not be empty."
  }
}
