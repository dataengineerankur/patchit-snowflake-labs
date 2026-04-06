# PATCHIT auto-fix: grant_permissions
# Original error: snowflake.connector.errors.ProgrammingError: Insufficient privileges to operate on warehouse COMPUTE_WH. Grant USAGE ON WAREHOUSE COMPUTE_WH to LOADER_ROLE.
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.82"
    }
  }
}

provider "snowflake" {
  account  = var.snowflake_account
  user     = var.snowflake_user
  role     = var.snowflake_role
}

# Safety: resources are created only when enable_apply = true.

resource "snowflake_database" "patchit_db" {
  count = var.enable_apply && var.create_database ? 1 : 0
  name  = var.snowflake_database
}

resource "snowflake_schema" "patchit_schema" {
  count    = var.enable_apply && var.create_schema ? 1 : 0
  database = var.snowflake_database
  name     = var.snowflake_schema
}

resource "snowflake_warehouse" "patchit_wh" {
  count = var.enable_apply && var.create_warehouse ? 1 : 0
  name  = var.snowflake_warehouse
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_table" "raw_events" {
  count    = var.enable_apply ? 1 : 0
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "RAW_EVENTS"
  change_tracking = true
  column {
    name = "ID"
    type = "NUMBER"
  }
  column {
    name = "EVENT_TS"
    type = "TIMESTAMP_NTZ"
  }
}

resource "snowflake_table" "curated_events" {
  count    = var.enable_apply ? 1 : 0
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "CURATED_EVENTS"
  column {
    name = "ID"
    type = "NUMBER"
  }
  column {
    name = "EVENT_TS"
    type = "TIMESTAMP_NTZ"
  }
}

resource "snowflake_stage" "landing_stage" {
  count    = var.enable_apply ? 1 : 0
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "LANDING_STAGE"
}

resource "snowflake_file_format" "csv_format" {
  count    = var.enable_apply ? 1 : 0
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "CSV_FORMAT"
  format_type = "CSV"
  field_delimiter = ","
  skip_header = 1
}

resource "snowflake_pipe" "events_pipe" {
  count    = var.enable_apply ? 1 : 0
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "EVENTS_PIPE"
  copy_statement = "COPY INTO ${var.snowflake_database}.${var.snowflake_schema}.RAW_EVENTS FROM @${var.snowflake_database}.${var.snowflake_schema}.LANDING_STAGE FILE_FORMAT=(FORMAT_NAME=${var.snowflake_database}.${var.snowflake_schema}.CSV_FORMAT)"
  auto_ingest = false
  depends_on = [
    snowflake_table.raw_events,
    snowflake_stage.landing_stage,
    snowflake_file_format.csv_format,
  ]
}

resource "snowflake_stream_on_table" "raw_stream" {
  count    = var.enable_apply ? 1 : 0
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "RAW_EVENTS_STREAM"
  table    = "${var.snowflake_database}.${var.snowflake_schema}.RAW_EVENTS"
  depends_on = [snowflake_table.raw_events]
}

resource "snowflake_task" "merge_task" {
  count    = var.enable_apply ? 1 : 0
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "MERGE_CURATED_TASK"
  warehouse = var.snowflake_warehouse
  started  = false
  sql_statement = "INSERT INTO ${var.snowflake_database}.${var.snowflake_schema}.CURATED_EVENTS SELECT ID, EVENT_TS FROM ${var.snowflake_database}.${var.snowflake_schema}.RAW_EVENTS_STREAM"
  depends_on = [
    snowflake_stream_on_table.raw_stream,
    snowflake_table.curated_events,
  ]
}

resource "snowflake_procedure" "snowpark_transform" {
  count    = var.enable_apply && var.create_snowpark_procedure ? 1 : 0
  database = var.snowflake_database
  schema   = var.snowflake_schema
  name     = "PATCHIT_SNOWPARK_TRANSFORM"
  language = "PYTHON"
  handler  = "run"
  return_type = "STRING"
  packages = ["snowflake-snowpark-python"]
  runtime_version = "3.10"
  statement = <<-PYCODE
def run(session):
    raw = session.table("RAW_EVENTS")
    curated = raw.select("ID", "EVENT_TS")
    curated.write.mode("append").save_as_table("CURATED_EVENTS")
    return "ok"
PYCODE
  depends_on = [
    snowflake_table.raw_events,
    snowflake_table.curated_events,
  ]
}
