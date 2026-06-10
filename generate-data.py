#!/usr/bin/env python3
"""
Zen Agent OS — Data Generator
Reads real data from Hermes logs and Obsidian vault to produce live dashboard JSON.
Run this before opening agent-os.html to refresh data.
"""

import subprocess
import json
import re
import os
from datetime import datetime, timedelta
from pathlib import Path

HOME = Path.home()
LOG_FILE = HOME / ".hermes/logs/agent.log"
VAULT = HOME / "Obsidian/ZenVault"
SANDBOX_RUNS = VAULT / "Sandbox/Runs"
PROMOTION_LOG = HOME / ".hermes/sandbox/SB_Promotion_Log.md"
RUN_LOG = HOME / ".hermes/sandbox/SB_Run_Log.md"

# OpenRouter pricing (per 1M tokens) — update as needed
PRICING = {
    "openrouter/owl-alpha": {"in": 0.0, "out": 0.0},
    "minimax-m3:cloud": {"in": 0.279, "out": 1.15},
    "qwen3-coder:30b": {"in": 0.08, "out": 0.28},
    "nemotron-3:ultra:cloud": {"in": 0.15, "out": 0.15},
    "kimi-k2.6:cloud": {"in": 0.20, "out": 0.50},
    "deepseek-coder-v2:16b": {"in": 0.14, "out": 0.28},
    "qwen2.5-coder:14b": {"in": 0.10, "out": 0.30},
    "claude-opus-4.8": {"in": 15.0, "out": 75.0},
    "default": {"in": 0.15, "out": 0.30},
}

def get_pricing(model):
    return PRICING.get(model, PRICING["default"])

def parse_agent_log():
    """Parse agent.conversation_loop lines from Hermes log for token usage."""
    if not LOG_FILE.exists():
        return {"today": {}, "yesterday": {}, "total_tokens_in": 0, "total_tokens_out": 0, "total_calls": 0, "today_calls": 0}

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    today_in = 0
    today_out = 0
    today_calls = 0
    yesterday_in = 0
    yesterday_out = 0
    per_session = {}
    total_in = 0
    total_out = 0
    total_calls = 0
    per_model = {}

    try:
        with open(LOG_FILE, "r", errors="replace") as f:
            for line in f:
                if "agent.conversation_loop" not in line:
                    continue
                # Parse date
                date_match = re.match(r"(\d{4}-\d{2}-\d{2})", line)
                if not date_match:
                    continue
                date_str = date_match.group(1)

                # Parse model
                model_match = re.search(r"model=(\S+)", line)
                model = model_match.group(1) if model_match else "unknown"

                # Parse in/out
                in_match = re.search(r"in=(\d+)", line)
                out_match = re.search(r"out=(\d+)", line)
                if not in_match or not out_match:
                    continue

                tokens_in = int(in_match.group(1))
                tokens_out = int(out_match.group(1))

                # Parse session
                session_match = re.search(r"\[([^\]]+)\]", line)
                session = session_match.group(1) if session_match else "unknown"

                if date_str == today:
                    today_in += tokens_in
                    today_out += tokens_out
                    today_calls += 1
                if date_str == yesterday:
                    yesterday_in += tokens_in
                    yesterday_out += tokens_out

                total_in += tokens_in
                total_out += tokens_out
                total_calls += 1

                # Per model
                if model not in per_model:
                    per_model[model] = {"in": 0, "out": 0, "calls": 0}
                per_model[model]["in"] += tokens_in
                per_model[model]["out"] += tokens_out
                per_model[model]["calls"] += 1

                # Per session
                if session not in per_session:
                    per_session[session] = {"in": 0, "out": 0, "calls": 0}
                per_session[session]["in"] += tokens_in
                per_session[session]["out"] += tokens_out
                per_session[session]["calls"] += 1

    except Exception as e:
        print(f"Error parsing log: {e}")

    # Calculate costs
    total_cost = 0
    today_cost = 0
    for model, data in per_model.items():
        pricing = get_pricing(model)
        cost = (data["in"] / 1e6 * pricing["in"]) + (data["out"] / 1e6 * pricing["out"])
        data["cost"] = round(cost, 4)
        total_cost += cost

    # Per-session costs
    for session, data in per_session.items():
        cost = (data["in"] / 1e6 * 0.15) + (data["out"] / 1e6 * 0.30)  # Conservative estimate
        data["cost"] = round(cost, 4)

    today_cost = (today_in / 1e6 * 0.02) + (today_out / 1e6 * 0.05)  # Conservative
    yesterday_cost = (yesterday_in / 1e6 * 0.02) + (yesterday_out / 1e6 * 0.05)

    return {
        "today": {"in": today_in, "out": today_out, "calls": today_calls, "cost": round(today_cost, 4)},
        "yesterday": {"in": yesterday_in, "out": yesterday_out, "cost": round(yesterday_cost, 4)},
        "total_in": total_in,
        "total_out": total_out,
        "total_calls": total_calls,
        "per_model": per_model,
        "per_session": dict(sorted(per_session.items(), key=lambda x: x[1]["in"], reverse=True)[:10]),
    }

def parse_sandbox_data():
    """Read sandbox detail files for prompt pipeline data."""
    if not SANDBOX_RUNS.exists():
        return []

    files = sorted(SANDBOX_RUNS.glob("**/*.md"))
    prompts = []

    for f in files:
        try:
            content = f.read_text(errors="replace")
            if "---" not in content:
                continue

            # Parse frontmatter
            parts = content.split("---")
            if len(parts) < 2:
                continue

            fm = {}
            for line in parts[1].strip().split("\n"):
                if ":" in line:
                    key, _, value = line.partition(":")
                    fm[key.strip()] = value.strip().strip('"')

            prompts.append({
                "id": fm.get("id", f.stem),
                "summary": fm.get("summary", f.stem)[:60],
                "project": fm.get("project", "—"),
                "quality": fm.get("quality", "—"),
                "review_status": fm.get("review_status", "pending"),
                "task_type": fm.get("task_type", "—"),
                "genericness": fm.get("genericness", "—"),
                "failure_mode": fm.get("failure_mode", "none"),
            })
        except Exception:
            pass

    return prompts

def get_promotion_data():
    """Read promotion log."""
    if not PROMOTION_LOG.exists():
        return []
    content = PROMOTION_LOG.read_text(errors="replace")
    # Parse markdown table
    lines = content.split("\n")
    promotions = []
    for line in lines:
        if line.startswith("|") and "Origin" not in line and "---" not in line:
            cells = [c.strip() for c in line.split("|")[1:-1]]
            if len(cells) >= 4:
                promotions.append({
                    "origin": cells[0],
                    "target": cells[1],
                    "path": cells[2] if len(cells) > 2 else "",
                    "date": cells[3] if len(cells) > 3 else "",
                    "status": cells[4] if len(cells) > 4 else "",
                })
    return promotions

def get_run_log_data():
    """Read run log."""
    if not RUN_LOG.exists():
        return []
    content = RUN_LOG.read_text(errors="replace")
    lines = content.split("\n")
    events = []
    for line in lines:
        if line.startswith("|") and "Timestamp" not in line and "---" not in line:
            cells = [c.strip() for c in line.split("|")[1:-1]]
            if len(cells) >= 3:
                events.append({
                    "time": cells[0],
                    "type": cells[1],
                    "details": cells[2],
                })
    return events

def generate_dashboard_data():
    """Generate complete dashboard JSON."""
    token_data = parse_agent_log()
    sandbox_prompts = parse_sandbox_data()
    promotions = get_promotion_data()
    run_log = get_run_log_data()

    # Sandbox stats
    total = len(sandbox_prompts)
    accepted = sum(1 for p in sandbox_prompts if p["review_status"] == "accepted")
    rejected = sum(1 for p in sandbox_prompts if p["review_status"] == "rejected")
    pending = sum(1 for p in sandbox_prompts if p["review_status"] == "pending")

    # Quality distribution
    quality_buckets = {"5.0": 0, "4.5-4.9": 0, "4.0-4.4": 0, "3.5-3.9": 0, "<3.5": 0}
    high_quality = sum(1 for p in sandbox_prompts if p["quality"] == "high")

    # Per-project stats
    projects = {}
    for p in sandbox_prompts:
        proj = p["project"]
        if proj not in projects:
            projects[proj] = {"total": 0, "accepted": 0, "rejected": 0, "pending": 0}
        projects[proj]["total"] += 1
        projects[proj][p["review_status"]] = projects[proj].get(p["review_status"], 0) + 1

    # Income tracking — placeholder for user to fill in
    income_data = {
        "total_earnings": 0,
        "monthly_recurring": 0,
        "projects": [],
    }

    data = {
        "generated_at": datetime.now().isoformat(),
        "token_usage": token_data,
        "sandbox": {
            "total": total,
            "accepted": accepted,
            "rejected": rejected,
            "pending": pending,
            "high_quality": high_quality,
            "prompts": sandbox_prompts[-50:],  # Last 50 for performance
        },
        "projects": projects,
        "promotions": promotions[-20:],
        "run_log": run_log[-30:],
        "income": income_data,
    }

    return data

if __name__ == "__main__":
    data = generate_dashboard_data()
    output = json.dumps(data, indent=2, default=str)
    print(output)

    # Also write to file for HTML to read
    output_path = Path("/home/spatula/Projects/ZenNew/agent-os-data.json")
    output_path.write_text(output)

    # Update embedded JSON in agent-os.html
    html_path = Path("/home/spatula/Projects/ZenNew/agent-os.html")
    html = html_path.read_text()
    marker_start = '<script type="application/json" id="embedded-data">'
    marker_end = '</script>'
    idx_start = html.find(marker_start)
    idx_end = html.find(marker_end, idx_start + len(marker_start))
    if idx_start > 0 and idx_end > 0:
        new_block = marker_start + '\n' + json.dumps(data, default=str) + '\n' + marker_end
        html = html[:idx_start] + new_block + html[idx_end + len(marker_end):]
        html_path.write_text(html)
        print("✅ Embedded data updated in agent-os.html")
    else:
        print("⚠️ Could not find embedded data markers in HTML")
    print(f"\n✅ Data written to {output_path}")
    print(f"📊 Token usage today: {data['token_usage']['today']['in']:,} in / {data['token_usage']['today']['out']:,} out")
    print(f"📊 Sandbox: {data['sandbox']['total']} total, {data['sandbox']['accepted']} accepted, {data['sandbox']['rejected']} rejected")
