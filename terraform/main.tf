##############################################################################
# main.tf
# Provisions the ANALYTICS.PATCHIT_LABS schema, tables, stored procedures,
# and scheduled tasks in Snowflake using the Snowflake-Labs provider.
##############################################################################

terraform {
  required_version = ">= 1.3.0"

  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
    }
  }
}

provider "snowflake" {
  account   = var.snowflake_account
  username  = var.snowflake_user
  role      = var.snowflake_role
  warehouse = var.snowflake_warehouse
}

##############################################################################
# Schema
##############################################################################

resource "snowflake_schema" "patchit_labs" {
  database            = var.snowflake_database
  name                = "PATCHIT_LABS"
  comment             = "PatchIt Labs computed analytics outputs — LTV, inventory planning"
  data_retention_days = 14
  is_transient        = false
}

##############################################################################
# Output tables
##############################################################################

resource "snowflake_table" "customer_ltv" {
  database = var.snowflake_database
  schema   = snowflake_schema.patchit_labs.name
  name     = "CUSTOMER_LTV"
  comment  = "Per-customer lifetime value scores and tier classifications"

  column {
    name     = "CUSTOMER_ID"
    type     = "NUMBER(38,0)"
    nullable = false
    comment  = "FK → ANALYTICS.RAW.CUSTOMERS.CUSTOMER_ID"
  }

  column {
    name     = "LTV_SCORE"
    type     = "FLOAT"
    nullable = false
    comment  = "Composite lifetime-value score (revenue × retention_factor)"
  }

  column {
    name     = "LTV_TIER"
    type     = "VARCHAR(16)"
    nullable = false
    comment  = "PLATINUM | GOLD | SILVER | BRONZE"
  }

  column {
    name     = "PREDICTED_REVENUE_12M"
    type     = "FLOAT"
    nullable = false
    comment  = "Forward 12-month revenue estimate"
  }

  column {
    name     = "CHURN_RISK_SCORE"
    type     = "FLOAT"
    nullable = false
    comment  = "0 = no churn risk, 1 = definite churn"
  }

  column {
    name    = "COMPUTED_AT"
    type    = "TIMESTAMP_NTZ"
    nullable = false
    comment  = "Wall-clock timestamp of last computation run"

    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

  primary_key {
    name = "PK_CUSTOMER_LTV"
    keys = ["CUSTOMER_ID"]
  }

  depends_on = [snowflake_schema.patchit_labs]
}

resource "snowflake_table" "inventory_reorder_plan" {
  database = var.snowflake_database
  schema   = snowflake_schema.patchit_labs.name
  name     = "INVENTORY_REORDER_PLAN"
  comment  = "Per-SKU inventory reorder recommendations"

  column {
    name     = "PRODUCT_ID"
    type     = "NUMBER(38,0)"
    nullable = false
    comment  = "FK → ANALYTICS.RAW.PRODUCTS.PRODUCT_ID"
  }

  column {
    name     = "SKU"
    type     = "VARCHAR(64)"
    nullable = false
    comment  = "Stock-keeping unit code"
  }

  column {
    name     = "CURRENT_STOCK"
    type     = "NUMBER(18,2)"
    nullable = false
    comment  = "On-hand units at time of computation"
  }

  column {
    name     = "REORDER_QTY"
    type     = "NUMBER(18,2)"
    nullable = false
    comment  = "Recommended purchase quantity"
  }

  column {
    name     = "REORDER_POINT"
    type     = "FLOAT"
    nullable = false
    comment  = "Inventory level that triggers a reorder"
  }

  column {
    name     = "SAFETY_STOCK"
    type     = "FLOAT"
    nullable = false
    comment  = "Buffer stock to absorb demand variability"
  }

  column {
    name     = "ABC_CLASS"
    type     = "VARCHAR(1)"
    nullable = false
    comment  = "A = top-20% revenue, B = next-30%, C = remaining 50%"
  }

  column {
    name     = "COMPUTED_AT"
    type     = "TIMESTAMP_NTZ"
    nullable = false
    comment  = "Wall-clock timestamp of last computation run"

    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

  primary_key {
    name = "PK_INVENTORY_REORDER_PLAN"
    keys = ["PRODUCT_ID"]
  }

  depends_on = [snowflake_schema.patchit_labs]
}

##############################################################################
# Stored Procedures
##############################################################################

resource "snowflake_procedure" "sp_compute_customer_ltv" {
  database = var.snowflake_database
  schema   = snowflake_schema.patchit_labs.name
  name     = "SP_COMPUTE_CUSTOMER_LTV"
  language = "SQL"

  return_type = "VARCHAR"
  execute_as  = "CALLER"

  statement = file("${path.module}/../sql/02_sp_customer_ltv.sql")

  comment = "Nightly stored procedure: computes per-customer LTV metrics and merges into CUSTOMER_LTV"

  depends_on = [
    snowflake_table.customer_ltv,
  ]
}

resource "snowflake_procedure" "sp_compute_inventory_reorder" {
  database = var.snowflake_database
  schema   = snowflake_schema.patchit_labs.name
  name     = "SP_COMPUTE_INVENTORY_REORDER"
  language = "SQL"

  return_type = "VARCHAR"
  execute_as  = "CALLER"

  arguments {
    name = "P_LEAD_TIME_OVERRIDE"
    type = "FLOAT"
  }

  statement = file("${path.module}/../sql/03_sp_inventory_reorder.sql")

  comment = "Nightly stored procedure: computes per-SKU reorder plan and merges into INVENTORY_REORDER_PLAN"

  depends_on = [
    snowflake_table.inventory_reorder_plan,
  ]
}

##############################################################################
# Tasks
##############################################################################

resource "snowflake_task" "task_customer_ltv" {
  database  = var.snowflake_database
  schema    = snowflake_schema.patchit_labs.name
  name      = "TASK_CUSTOMER_LTV"
  warehouse = var.snowflake_warehouse
  schedule  = "USING CRON 0 2 * * * UTC"
  enabled   = true
  comment   = "Nightly 02:00 UTC — calls SP_COMPUTE_CUSTOMER_LTV"

  sql_statement = "CALL ${var.snowflake_database}.${snowflake_schema.patchit_labs.name}.SP_COMPUTE_CUSTOMER_LTV()"

  depends_on = [snowflake_procedure.sp_compute_customer_ltv]
}

resource "snowflake_task" "task_inventory_reorder" {
  database  = var.snowflake_database
  schema    = snowflake_schema.patchit_labs.name
  name      = "TASK_INVENTORY_REORDER"
  warehouse = var.snowflake_warehouse
  schedule  = "USING CRON 0 3 * * * UTC"
  enabled   = true
  comment   = "Nightly 03:00 UTC — calls SP_COMPUTE_INVENTORY_REORDER"

  sql_statement = "CALL ${var.snowflake_database}.${snowflake_schema.patchit_labs.name}.SP_COMPUTE_INVENTORY_REORDER()"

  depends_on = [snowflake_procedure.sp_compute_inventory_reorder]
}
