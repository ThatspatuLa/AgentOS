#!/usr/bin/env python3
"""
Zen Multi-LLM Pipeline — Token Tracker
Tracks per-model token usage, costs, and pipeline stage for dashboard display.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

USAGE_FILE = Path.home() / ".hermes" / "sandbox" / "token-usage.json"

# Model pricing (per 1M tokens)
PRICING = {
    "openrouter/owl-alpha": {"in": 0.0, "out": 0.0},  # Free tier
    "openai-codex": {"in": 0.0, "out": 0.0},  # OAuth, no per-token cost
    "ollama/qwen3-coder:30b": {"in": 0.0, "out": 0.0},  # Local, free
    "ollama/nemotron-3-ultra:cloud": {"in": 0.0, "out": 0.0},
    "ollama/minimax-m3:cloud": {"in": 0.279, "out": 1.15},
    "ollama/kimi-k2.6:cloud": {"in": 0.20, "out": 0.50},
    "ollama/deepseek-coder-v2:16b": {"in": 0.14, "out": 0.28},
    "ollama/qwen2.5-coder:14b": {"in": 0.10, "out": 0.30},
}


def load_usage() -> dict:
    """Load existing usage data or return empty structure."""
    if USAGE_FILE.exists():
        try:
            return json.loads(USAGE_FILE.read_text())
        except (json.JSONDecodeError, IOError):
            pass
    return {"runs": [], "totals": {}}


def save_usage(data: dict):
    """Save usage data to disk."""
    USAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    USAGE_FILE.write_text(json.dumps(data, indent=2, default=str))


def log_run(
    task: str,
    stage: str,
    model: str,
    tokens_in: int,
    tokens_out: int,
    result: str = "unknown",
    retries: int = 0,
):
    """Log a single pipeline run."""
    data = load_usage()

    pricing = PRICING.get(model, {"in": 0.0, "out": 0.0})
    cost = (tokens_in / 1e6 * pricing["in"]) + (tokens_out / 1e6 * pricing["out"])

    run = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "task": task,
        "stage": stage,
        "model": model,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "cost": round(cost, 4),
        "result": result,
        "retries": retries,
    }

    data["runs"].append(run)

    # Update totals
    if model not in data["totals"]:
        data["totals"][model] = {"in": 0, "out": 0, "cost": 0, "calls": 0}
    data["totals"][model]["in"] += tokens_in
    data["totals"][model]["out"] += tokens_out
    data["totals"][model]["cost"] = round(data["totals"][model]["cost"] + cost, 4)
    data["totals"][model]["calls"] += 1

    save_usage(data)
    return run


def get_totals() -> dict:
    """Get aggregated totals."""
    data = load_usage()
    return data.get("totals", {})


def get_recent_runs(limit: int = 20) -> list:
    """Get recent pipeline runs."""
    data = load_usage()
    return data.get("runs", [])[-limit:]


def get_summary() -> dict:
    """Get summary for dashboard."""
    data = load_usage()
    totals = data.get("totals", {})

    total_cost = sum(m.get("cost", 0) for m in totals.values())
    total_in = sum(m.get("in", 0) for m in totals.values())
    total_out = sum(m.get("out", 0) for m in totals.values())
    total_calls = sum(m.get("calls", 0) for m in totals.values())

    # Cloud vs local split
    cloud_in = sum(m.get("in", 0) for k, m in totals.items() if not k.startswith("ollama/"))
    cloud_out = sum(m.get("out", 0) for k, m in totals.items() if not k.startswith("ollama/"))
    local_in = sum(m.get("in", 0) for k, m in totals.items() if k.startswith("ollama/"))
    local_out = sum(m.get("out", 0) for k, m in totals.items() if k.startswith("ollama/"))

    return {
        "total_cost": round(total_cost, 4),
        "total_in": total_in,
        "total_out": total_out,
        "total_calls": total_calls,
        "cloud_in": cloud_in,
        "cloud_out": cloud_out,
        "local_in": local_in,
        "local_out": local_out,
        "per_model": totals,
        "recent_runs": data.get("runs", [])[-10:],
    }


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "summary":
        print(json.dumps(get_summary(), indent=2))
    elif len(sys.argv) > 1 and sys.argv[1] == "log":
        # Usage: python token-tracker.py log "task" "stage" "model" tokens_in tokens_out [result] [retries]
        task = sys.argv[2] if len(sys.argv) > 2 else "unknown"
        stage = sys.argv[3] if len(sys.argv) > 3 else "unknown"
        model = sys.argv[4] if len(sys.argv) > 4 else "unknown"
        tokens_in = int(sys.argv[5]) if len(sys.argv) > 5 else 0
        tokens_out = int(sys.argv[6]) if len(sys.argv) > 6 else 0
        result = sys.argv[7] if len(sys.argv) > 7 else "unknown"
        retries = int(sys.argv[8]) if len(sys.argv) > 8 else 0
        run = log_run(task, stage, model, tokens_in, tokens_out, result, retries)
        print(json.dumps(run, indent=2))
    else:
        print("Usage:")
        print("  python token-tracker.py summary")
        print("  python token-tracker.py log <task> <stage> <model> <tokens_in> <tokens_out> [result] [retries]")
