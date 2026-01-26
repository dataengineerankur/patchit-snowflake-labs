from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict


def normalize_log(
    *,
    job_id: str,
    run_id: str,
    error_signature: str,
    log_excerpt: str,
    artifacts: list[str] | None = None,
    code_paths: list[str] | None = None,
    config: dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Normalize a Snowflake failure signal into PATCHIT input format."""
    return {
        "platform": "snowflake",
        "job_id": job_id,
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error_signature": error_signature,
        "log_excerpt": log_excerpt,
        "artifacts": artifacts or [],
        "code_paths": code_paths or [],
        "config": config or {},
    }


def write_normalized(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
