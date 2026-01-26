#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRILLS_FILE="${REPO_ROOT}/drills/drills.yaml"

current_id=""
while IFS= read -r line; do
  if [[ "${line}" =~ scenario_id:\ (.*)$ ]]; then
    current_id="${BASH_REMATCH[1]}"
  fi
  if [[ "${line}" =~ enabled:\ true ]]; then
    if [[ -n "${current_id}" ]]; then
      "${REPO_ROOT}/scripts/run_drill.sh" "${current_id}"
    fi
  fi
done < "${DRILLS_FILE}"
