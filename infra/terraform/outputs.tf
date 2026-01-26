output "database" {
  value       = var.snowflake_database
  description = "Snowflake database name"
}

output "schema" {
  value       = var.snowflake_schema
  description = "Snowflake schema name"
}
