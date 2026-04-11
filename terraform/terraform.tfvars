##############################################################################
# terraform.tfvars
# Non-sensitive variable values for the patchit-snowflake-labs workspace.
# DO NOT store passwords, private keys, or OAuth tokens in this file.
# Pass credentials via environment variables:
#   export SNOWFLAKE_PASSWORD=<your-password>
# or via a secrets manager (Vault, AWS Secrets Manager, etc.)
##############################################################################

snowflake_account  = "WBZTWSY-KH99814"
snowflake_user     = "PATCHIT"
snowflake_database = "ANALYTICS"
