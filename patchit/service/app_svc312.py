"""PATCHIT FastAPI service for Python 3.12+."""
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import json
import os
from pathlib import Path
from collections import defaultdict

app = FastAPI(title="PATCHIT Auto-Remediation API")


def _load_audit_log():
    """Load and parse patchit_audit.jsonl file."""
    audit_path = Path("var/audit/patchit_audit.jsonl")
    if not audit_path.exists():
        return []
    
    events = []
    try:
        with open(audit_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except Exception:
        pass
    return events


@app.get("/api/stats")
async def get_stats():
    """
    Return audit statistics from patchit_audit.jsonl:
    - total_incidents: count of event.received events
    - success_rate: % of incidents with pr.created events
    - avg_confidence: average confidence from error.classified events
    - platform_breakdown: count by platform from event.received
    """
    events = _load_audit_log()
    
    # Track metrics
    total_incidents = 0
    incidents_with_pr = 0
    confidence_scores = []
    platform_counts = defaultdict(int)
    
    # Map correlation_id to whether it has a pr.created
    correlation_has_pr = set()
    
    # First pass: find all incidents and PRs
    for event in events:
        event_type = event.get("event_type", "")
        correlation_id = event.get("correlation_id", "")
        payload = event.get("payload", {})
        
        if event_type == "event.received":
            total_incidents += 1
            event_data = payload.get("event", {})
            platform = event_data.get("platform", "unknown")
            platform_counts[platform] += 1
        
        elif event_type == "pr.created":
            correlation_has_pr.add(correlation_id)
        
        elif event_type == "error.classified":
            confidence = payload.get("confidence")
            if confidence is not None:
                try:
                    confidence_scores.append(float(confidence))
                except (TypeError, ValueError):
                    pass
    
    # Count incidents that got PRs
    incidents_with_pr = len(correlation_has_pr)
    
    # Calculate success rate
    success_rate = (
        round((incidents_with_pr / total_incidents * 100), 2)
        if total_incidents > 0
        else 0.0
    )
    
    # Calculate average confidence
    avg_confidence = (
        round(sum(confidence_scores) / len(confidence_scores), 3)
        if confidence_scores
        else 0.0
    )
    
    return JSONResponse({
        "total_incidents": total_incidents,
        "success_rate": success_rate,
        "avg_confidence": avg_confidence,
        "platform_breakdown": dict(platform_counts)
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=18088)
