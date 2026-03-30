"""
PATCHIT Reconstructed Service — Python 3.12
Loads all core logic from intact cpython-312 pyc modules.
Replaces the accidentally-overwritten app_real.cpython-312.pyc.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── Python path setup (must be first) ────────────────────────────────────────
# __file__ may not exist when loaded via SourcelessFileLoader — derive path via patchit package
def _find_proj_root() -> str:
    import patchit as _pk
    pkg_file = getattr(_pk, "__file__", None) or (
        getattr(_pk, "__spec__", None) and getattr(_pk.__spec__, "origin", None)
    )
    if pkg_file:
        return os.path.dirname(os.path.dirname(pkg_file))
    return os.getcwd()

_PROJ_ROOT = _find_proj_root()
if _PROJ_ROOT not in sys.path:
    sys.path.insert(0, _PROJ_ROOT)

# ── 3.12-pyc meta-path finder ────────────────────────────────────────────────
import importlib.abc as _abc
import importlib.machinery as _mach
import importlib.util as _util

_VER = "cpython-312"


class _PycFinder312(_abc.MetaPathFinder):
    """Load patchit/* modules from __pycache__ cpython-312 pyc files."""

    def find_spec(self, fullname, path, target=None):
        if not path:
            return None
        for entry in path:
            name = fullname.split(".")[-1]
            pyc = os.path.join(entry, "__pycache__", f"{name}.{_VER}.pyc")
            if os.path.isfile(pyc) and os.path.getsize(pyc) > 200:
                ldr = _mach.SourcelessFileLoader(fullname, pyc)
                return _util.spec_from_loader(fullname, ldr, origin=pyc)
        return None


if not any(isinstance(f, _PycFinder312) for f in sys.meta_path):
    sys.meta_path.insert(0, _PycFinder312())

# ardoa → patchit alias (compiled code uses ardoa.* names)
import patchit as _patchit_pkg

if "ardoa" not in sys.modules:
    sys.modules["ardoa"] = _patchit_pkg

# ── PATCHIT 3.12 imports ─────────────────────────────────────────────────────
from patchit.models import UniversalFailureEvent, Platform  # noqa: E402
from patchit.settings import Settings  # noqa: E402
from patchit.telemetry.audit import AuditLogger  # noqa: E402
from patchit.repos.registry import select_repo_context  # noqa: E402
from patchit.context.collector import CodeContextCollector  # noqa: E402
from patchit.context.artifacts import summarize_artifacts  # noqa: E402
from patchit.parsers.log_fetcher import LogFetcher  # noqa: E402
from patchit.parsers.stacktrace import parse_python_traceback as parse_stacktrace  # noqa: E402
from patchit.code_engine.agentic import AgenticCodeEngine  # noqa: E402
from patchit.gitops.pr_creator import PullRequestCreator  # noqa: E402
from patchit.service.ui import render_ui_html, stream_jsonl_sse  # noqa: E402

# ── FastAPI ───────────────────────────────────────────────────────────────────
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger("patchit.service")

# ── Config ────────────────────────────────────────────────────────────────────
_RUNTIME_JSON = os.path.join(_PROJ_ROOT, "var", "config", "runtime.json")
_AUDIT_JSONL   = os.path.join(_PROJ_ROOT, "var", "audit", "patchit_audit.jsonl")
_PATCHES_DIR   = os.path.join(_PROJ_ROOT, "var", "patches")
_REPORTS_DIR   = os.path.join(_PROJ_ROOT, "var", "reports")
_EVIDENCE_DIR  = os.path.join(_PROJ_ROOT, "var", "evidence")


def _load_settings() -> Settings:
    try:
        with open(_RUNTIME_JSON) as f:
            cfg = json.load(f)
        return Settings.model_validate(cfg)
    except Exception as e:
        logger.error("Failed to load runtime.json: %s", e)
        return Settings()


_settings = _load_settings()
_audit = AuditLogger(path=_AUDIT_JSONL)

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="PATCHIT Auto-Remediation", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"ok": True, "version": "0.2.0", "engine": "cpython-312"}


# ── UI ────────────────────────────────────────────────────────────────────────
@app.get("/ui", response_class=HTMLResponse)
@app.get("/ui/{path:path}", response_class=HTMLResponse)
async def ui(path: str = ""):
    try:
        return HTMLResponse(content=render_ui_html())
    except Exception:
        return HTMLResponse(content="<h1>PATCHIT UI</h1>")


# ── Runtime config ────────────────────────────────────────────────────────────
@app.get("/api/runtime_config")
async def runtime_config():
    global _settings
    _settings = _load_settings()
    return json.loads(_settings.model_dump_json())


# ── Audit ─────────────────────────────────────────────────────────────────────
@app.get("/api/audit/recent")
async def audit_recent(limit: int = 50, n: Optional[int] = None):
    """Return recent audit records. Accepts either `limit` or `n` query param."""
    effective_limit = n if n is not None else limit
    records = _audit.recent(n=effective_limit)
    return {"records": records}


@app.get("/api/audit/stream")
async def audit_stream():
    async def event_gen():
        async for chunk in stream_jsonl_sse(_AUDIT_JSONL):
            yield chunk

    return StreamingResponse(event_gen(), media_type="text/event-stream")


# ── Stats (audit analytics) ───────────────────────────────────────────────────
@app.get("/api/stats")
async def get_stats():
    """
    Return aggregate stats from the audit log:
    - total_incidents: unique correlation_ids with event.received
    - success_rate: % of those incidents that got a pr.created event
    - avg_confidence: mean confidence from agent.patch_proposed events
    - platform_breakdown: count by platform from event.received payloads
    - data_available: false if audit log is empty or missing
    """
    audit_path = Path(_AUDIT_JSONL)
    if not audit_path.exists():
        return JSONResponse({
            "total_incidents": 0,
            "success_rate": 0.0,
            "avg_confidence": 0.0,
            "platform_breakdown": {},
            "data_available": False,
        })

    events: list[dict] = []
    try:
        with open(audit_path, "r", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except Exception as exc:
        logger.warning("Failed to read audit log for stats: %s", exc)

    if not events:
        return JSONResponse({
            "total_incidents": 0,
            "success_rate": 0.0,
            "avg_confidence": 0.0,
            "platform_breakdown": {},
            "data_available": False,
        })

    # Track per-correlation state
    incident_correlation_ids: set[str] = set()
    pr_correlation_ids: set[str] = set()
    confidence_scores: list[float] = []
    platform_counts: dict[str, int] = defaultdict(int)

    for evt in events:
        event_type = evt.get("event_type", "")
        correlation_id = evt.get("correlation_id", "")
        payload = evt.get("payload", {})

        if event_type == "event.received":
            incident_correlation_ids.add(correlation_id)
            platform = payload.get("event", {}).get("platform", "unknown")
            platform_counts[platform] += 1

        elif event_type == "pr.created":
            # Only count if this correlation_id had an event.received
            pr_correlation_ids.add(correlation_id)

        elif event_type in ("agent.patch_proposed", "patch.proposed"):
            confidence = payload.get("confidence")
            if confidence is not None:
                try:
                    confidence_scores.append(float(confidence))
                except (TypeError, ValueError):
                    pass

    total_incidents = len(incident_correlation_ids)
    # Only count PRs where the correlation_id also had an event.received
    successful = len(pr_correlation_ids & incident_correlation_ids)

    success_rate = round(successful / total_incidents * 100, 2) if total_incidents > 0 else 0.0
    avg_confidence = (
        round(sum(confidence_scores) / len(confidence_scores), 3)
        if confidence_scores
        else 0.0
    )

    return JSONResponse({
        "total_incidents": total_incidents,
        "success_rate": success_rate,
        "avg_confidence": avg_confidence,
        "platform_breakdown": dict(platform_counts),
        "data_available": True,
    })


# ── Patch / report / evidence file endpoints ──────────────────────────────────
@app.get("/api/patch/{patch_id}.diff")
async def get_patch(patch_id: str):
    path = os.path.join(_PATCHES_DIR, f"{patch_id}.diff")
    if not os.path.isfile(path):
        raise HTTPException(404, "patch not found")
    return PlainTextResponse(open(path).read())


@app.get("/api/report/{report_id}.md")
async def get_report_md(report_id: str):
    path = os.path.join(_REPORTS_DIR, f"{report_id}.md")
    if not os.path.isfile(path):
        raise HTTPException(404, "report not found")
    return PlainTextResponse(open(path).read())


@app.get("/api/report/{report_id}.log")
async def get_report_log(report_id: str):
    path = os.path.join(_REPORTS_DIR, f"{report_id}.log")
    if not os.path.isfile(path):
        raise HTTPException(404, "log not found")
    return PlainTextResponse(open(path).read())


@app.get("/api/evidence/{correlation_id}.json")
async def get_evidence(correlation_id: str):
    path = os.path.join(_EVIDENCE_DIR, f"{correlation_id}.json")
    if not os.path.isfile(path):
        raise HTTPException(404, "evidence not found")
    return JSONResponse(json.loads(open(path).read()))


# ── Mock PR viewer ────────────────────────────────────────────────────────────
@app.get("/mock/pr/{pr_number}")
@app.get("/api/mock/pr/{pr_number}")
async def mock_pr(pr_number: int):
    mock_dir = os.path.join(_PROJ_ROOT, ".mock_github", "pulls")
    path = os.path.join(mock_dir, f"{pr_number}.json")
    if not os.path.isfile(path):
        raise HTTPException(404, "PR not found")
    return JSONResponse(json.loads(open(path).read()))


# ── Poller stub ───────────────────────────────────────────────────────────────
@app.get("/api/poller/status")
async def poller_status():
    return {"enabled": False, "last_poll": None, "message": "Poller not active in 3.12 service"}


@app.post("/api/poller/reset")
async def poller_reset():
    return {"ok": True}


# ── Diff normalizer ───────────────────────────────────────────────────────────
def _normalize_diff(diff_text: str) -> str:
    """
    Ensure multi-file diffs have proper 'diff --git a/X b/X' headers.
    Claude sometimes generates multi-file patches missing these headers,
    causing 'corrupt patch at line N' errors in git apply.
    """
    if not diff_text:
        return diff_text
    lines = diff_text.splitlines(keepends=True)
    result = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # Already has a diff --git header — keep as-is
        if line.startswith("diff --git"):
            result.append(line)
            i += 1
            continue
        # --- a/file line without a preceding diff --git header
        if line.startswith("--- a/") and (not result or not result[-1].startswith("diff --git")):
            path = line[6:].strip()
            result.append(f"diff --git a/{path} b/{path}\n")
            result.append(f"index 0000000..0000000 100644\n")
        result.append(line)
        i += 1
    return "".join(result)


# ── Core: event ingestion + remediation ──────────────────────────────────────
def _run_remediation(event_dict: dict, correlation_id: str) -> None:
    """Background task: run the full remediation pipeline."""
    global _settings
    _settings = _load_settings()
    s = _settings

    try:
        event = UniversalFailureEvent.model_validate(event_dict)

        _audit.write(correlation_id, "event.received", {
            "event": event_dict,
            "agent": {"mode": s.agent_mode},
        })

        # Resolve the correct repo context for this pipeline
        repo_ctx = select_repo_context(settings=s, pipeline_id=event.pipeline_id)
        repo_root = (
            repo_ctx.repo_root_dir if repo_ctx and repo_ctx.repo_root_dir else s.repo_root
        )
        github_repo = (
            repo_ctx.github_repo if repo_ctx and repo_ctx.github_repo else s.github_repo
        )
        github_base_branch = (
            repo_ctx.github_base_branch
            if repo_ctx and repo_ctx.github_base_branch
            else s.github_base_branch
        )

        _audit.write(correlation_id, "repo.selected", {
            "repo_root": repo_root,
            "github_repo": github_repo,
        })

        # ── Stage 1: Fetch logs ───────────────────────────────────────────────
        raw_log = ""
        if event.log_uri:
            try:
                fetcher = LogFetcher()
                raw_log = fetcher.fetch(event.log_uri)
                _audit.write(correlation_id, "log.fetched", {
                    "uri": event.log_uri,
                    "bytes": len(raw_log),
                })
            except Exception as exc:
                logger.warning("Log fetch failed: %s", exc)

        # ── Stage 2: Parse stacktrace ─────────────────────────────────────────
        frames = []
        error_text = raw_log or ""
        try:
            parsed = parse_stacktrace(raw_log)
            frames = parsed.frames if parsed else []
            if parsed and parsed.error_message:
                error_text = parsed.error_message
            _audit.write(correlation_id, "error.parsed", {
                "error_type": type(parsed).__name__ if parsed else "None",
                "frames": len(frames),
            })
        except Exception as exc:
            logger.warning("Stacktrace parse failed: %s", exc)
            _audit.write(correlation_id, "error.parsed", {"error_type": "None", "frames": 0})

        # ── Stage 3: Collect code context ─────────────────────────────────────
        code_context: Dict[str, Any] = {}
        try:
            collector = CodeContextCollector(project_root=repo_root)
            if frames:
                code_context = collector.collect(frames)
            _audit.write(correlation_id, "context.collected", {
                "files": len(code_context),
            })
        except Exception as exc:
            logger.warning("Code context collection failed: %s", exc)
            _audit.write(correlation_id, "context.collected", {"files": 0})

        # Summarize artifacts
        artifact_summaries = []
        try:
            artifact_summaries = summarize_artifacts(event.artifact_uris or [])
        except Exception as exc:
            logger.warning("Artifact summarize failed: %s", exc)

        # Build normalized error dict
        normalized_error = {
            "message": error_text or f"{event.platform} pipeline {event.pipeline_id} failed",
            "type": "pipeline_failure",
            "frames": [f.__dict__ if hasattr(f, "__dict__") else f for f in frames],
        }

        _audit.write(correlation_id, "agent.started", {
            "mode": s.agent_mode,
            "repo_root": repo_root,
            "pipeline_id": event.pipeline_id,
        })

        # ── Stage 4: Agentic patch proposal ──────────────────────────────────
        engine = AgenticCodeEngine(
            mode=s.agent_mode,
            groq_api_key=s.groq_api_key,
            openrouter_api_key=s.openrouter_api_key,
            openrouter_model=getattr(s, "openrouter_model", "anthropic/claude-3.5-sonnet"),
            repo_root=repo_root,
            timeout_s=float(getattr(s, "agent_attempt_timeout_s", 360)),
            github_token=s.github_token,
            github_repo=github_repo,
            github_base_branch=github_base_branch,
        )

        _audit.write(correlation_id, "agent.attempt_started", {
            "mode": s.agent_mode,
            "attempt": 1,
        })

        patch = engine.propose_patch(
            event=event_dict,
            error=normalized_error,
            code_context=code_context,
            artifact_summaries=artifact_summaries,
            codebase_index=None,
        )

        if patch is None:
            _audit.write(correlation_id, "agent.no_patch", {"pipeline_id": event.pipeline_id})
            return

        _audit.write(correlation_id, "agent.patch_proposed", {
            "patch_id": patch.patch_id,
            "title": patch.title,
            "confidence": patch.confidence,
            "files": [f.path for f in (patch.files or [])],
        })
        _audit.write(correlation_id, "patch.saved", {
            "patch_id": patch.patch_id,
            "path": os.path.join(_PATCHES_DIR, f"{patch.patch_id}.diff"),
        })

        # ── Stage 5: Create PR ────────────────────────────────────────────────
        import copy
        pr_cfg = json.loads(s.model_dump_json())
        pr_cfg["github_repo"] = github_repo
        pr_cfg["github_base_branch"] = github_base_branch
        pr_cfg["repo_root"] = repo_root
        pr_cfg["local_sync_repo_path"] = repo_root
        pr_settings = Settings.model_validate(pr_cfg)

        prc = PullRequestCreator(settings=pr_settings)
        pr_result = prc.create(
            title=patch.title,
            body=_build_pr_body(event, patch, correlation_id),
            patch=patch,
            draft=False,
        )

        # Nested pr dict — required by UI's stageMilestones() prEvt.payload.pr.pr_url
        _audit.write(correlation_id, "pr.created", {
            "pr": {
                "pr_url": pr_result.pr_url,
                "pr_number": pr_result.pr_number,
                "branch_name": pr_result.branch_name,
                "mode": pr_result.mode,
            },
            "pipeline_id": event.pipeline_id,
        })

        logger.info("PR created: %s", pr_result.pr_url)

    except Exception as exc:
        import traceback

        _audit.write(correlation_id, "remediation.error", {
            "error": str(exc),
            "traceback": traceback.format_exc(),
        })
        logger.exception("Remediation failed for correlation_id=%s", correlation_id)


def _build_pr_body(event: UniversalFailureEvent, patch: Any, correlation_id: str) -> str:
    files_list = "\n".join(f"- `{f.path}`" for f in (patch.files or []))
    return f"""## Summary

{patch.rationale}

## Changed Files

{files_list or "_(see diff)_"}

## Incident Details

| Field | Value |
|-------|-------|
| Pipeline | `{event.pipeline_id}` |
| Platform | `{event.platform}` |
| Run ID | `{event.run_id}` |
| Correlation ID | `{correlation_id}` |
| Confidence | {patch.confidence:.0%} |

🤖 Generated by [PATCHIT Auto-Remediation](https://github.com/dataengineerankur/patchit-snowflake-labs)
"""


@app.post("/events/ingest")
async def ingest_event(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    correlation_id = _audit.new_correlation_id()

    try:
        UniversalFailureEvent.model_validate(body)
    except Exception as e:
        raise HTTPException(422, f"Invalid event: {e}")

    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _run_remediation, body, correlation_id)

    return {"accepted": True, "correlation_id": correlation_id}
