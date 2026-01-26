#!/usr/bin/env bash
set -euo pipefail

SCENARIO_ID="${1:-unknown}"
OUT_DIR="${2:-./evidence/${SCENARIO_ID}/logs}"
mkdir -p "${OUT_DIR}"

# Placeholder collector for local mode.
echo "[${SCENARIO_ID}] simulated snowflake log" > "${OUT_DIR}/snowflake_run.log"
echo "${OUT_DIR}/snowflake_run.log"
