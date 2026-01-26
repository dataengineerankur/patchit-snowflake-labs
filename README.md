## Patchit Snowflake Labs

Snowflake ELT lab with Airflow ingestion + Snowpipe + Snowpark + Tasks/Streams and controlled failure drills.

### Variables (set at runtime)

- `WORKSPACE_ROOT="<local folder where repos will live>"`
- `GITHUB_ORIGIN="<optional: remote github org/repo base>"`
- `PATCHIT_CMD="<command to invoke Patchit locally>"`
- `OUTPUT_DIR="<local folder for evidence packs and reports>"`
- `SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_PRIVATE_KEY_PATH` or `SNOWFLAKE_PASSWORD`
- `S3_BUCKET_FOR_SNOWPIPE` (or Azure/GCS equivalent) + credentials

---

### Architecture

Local Airflow DAG:
1. Generates a small daily file (CSV/JSON).
2. Uploads to S3 landing bucket.
3. Triggers Snowpipe ingestion into raw table.
4. Snowpark transformation (Snowpark Python) builds curated table.
5. Task/Stream handles incremental merge.

Terraform deploys Snowflake objects + minimal grants.
Includes:
- tables, stage, file format, pipe
- stream + task for incremental merge

---

### Setup

Create a local `.env` (do not commit):

```bash
cat > .env <<'EOF'
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_ROLE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
SNOWFLAKE_PRIVATE_KEY_PATH=
SNOWFLAKE_PASSWORD=
S3_BUCKET_FOR_SNOWPIPE=
PATCHIT_CMD=
OUTPUT_DIR=
EOF
```

Install Snowpark locally (optional for running the Snowpark script):

```bash
pip install "snowflake-snowpark-python[pandas]"
```

---

### Terraform deploy (do NOT apply without credentials)

```bash
cd infra/terraform
terraform init
terraform plan \
  -var "snowflake_account=${SNOWFLAKE_ACCOUNT}" \
  -var "snowflake_user=${SNOWFLAKE_USER}" \
  -var "snowflake_role=${SNOWFLAKE_ROLE}" \
  -var "snowflake_warehouse=${SNOWFLAKE_WAREHOUSE}" \
  -var "snowflake_database=${SNOWFLAKE_DATABASE}" \
  -var "snowflake_schema=${SNOWFLAKE_SCHEMA}"
```

Apply (only after explicit confirmation + creds):

```bash
terraform apply \
  -var "snowflake_account=${SNOWFLAKE_ACCOUNT}" \
  -var "snowflake_user=${SNOWFLAKE_USER}" \
  -var "snowflake_role=${SNOWFLAKE_ROLE}" \
  -var "snowflake_warehouse=${SNOWFLAKE_WAREHOUSE}" \
  -var "snowflake_database=${SNOWFLAKE_DATABASE}" \
  -var "snowflake_schema=${SNOWFLAKE_SCHEMA}"
```

Destroy:

```bash
terraform destroy \
  -var "snowflake_account=${SNOWFLAKE_ACCOUNT}" \
  -var "snowflake_user=${SNOWFLAKE_USER}" \
  -var "snowflake_role=${SNOWFLAKE_ROLE}" \
  -var "snowflake_warehouse=${SNOWFLAKE_WAREHOUSE}" \
  -var "snowflake_database=${SNOWFLAKE_DATABASE}" \
  -var "snowflake_schema=${SNOWFLAKE_SCHEMA}"
```

---

### Failure drills

Drills are defined in `drills/drills.yaml`.

Run one drill:

```bash
OUTPUT_DIR="<path>" PATCHIT_CMD="<patchit command>" \
./scripts/run_drill.sh SNF1_snowpipe_format_break
```

Run all enabled drills:

```bash
OUTPUT_DIR="<path>" PATCHIT_CMD="<patchit command>" \
./scripts/run_all_drills.sh
```

---

### How PATCHIT is invoked

```bash
${PATCHIT_CMD} \
  --repo "$(pwd)" \
  --platform snowflake \
  --logs "$LOG_PATH" \
  --mode pr_only \
  --evidence_out "$EVIDENCE_PATH"
```

---

### Cost controls / cleanup

- Use smallest warehouse size.
- Disable schedules by default.
- Always `terraform destroy` after validation.

---

### How to onboard a real company later

- Use platform-native eventing (e.g., SQS/SNS/Lambda) instead of polling.
- Keep credentials in a secrets manager.
- Enforce PR-only remediation.

---

### How to run and test (end-to-end)

1) Local smoke test (no cloud required):

```bash
OUTPUT_DIR="$(pwd)/evidence" PATCHIT_CMD="<your patchit cmd>" \
./scripts/run_drill.sh SNF1_snowpipe_format_break
```

2) Run Snowpark transform (requires Snowflake credentials):

```bash
export SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_ROLE \
  SNOWFLAKE_WAREHOUSE SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA
export SNOWFLAKE_PASSWORD # or SNOWFLAKE_PRIVATE_KEY_PATH

python snowpark/transform.py
```

3) Snowpark procedure (deployed via Terraform):

After `terraform apply`, run in Snowflake:

```sql
CALL PATCHIT_SNOWPARK_TRANSFORM();
```

4) Review outputs:
- Evidence: `evidence/SNF1_snowpipe_format_break/evidence_pack.json`
- Logs: `evidence/SNF1_snowpipe_format_break/logs/`

5) Run all enabled drills:

```bash
OUTPUT_DIR="$(pwd)/evidence" PATCHIT_CMD="<your patchit cmd>" \
./scripts/run_all_drills.sh
```
