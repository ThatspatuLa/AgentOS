#!/usr/bin/env python3
"""
Zen Graph Exporter — Stage 0
Reads Zen system data and produces memory-world.json for the Memory World visual graph.
Pure Python 3.11, stdlib only. No dependencies.
Read-only: does not modify any source files.
"""
import json
import os
import re
import sqlite3
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

HOME = Path.home()
OUTPUT = Path(__file__).parent.parent / "data" / "memory-world.json"

# ── Paths ──────────────────────────────────────────────────────────────────
HERMES_CONFIG = HOME / ".hermes/config.yaml"
STATE_DB = HOME / ".hermes/state.db"
AGENT_LOG = HOME / ".hermes/logs/agent.log"
LOCAL_WORKER_USAGE = HOME / ".hermes/sandbox/local-worker-usage.json"
TOKEN_USAGE = HOME / ".hermes/sandbox/token-usage.json"
VAULT = HOME / "Obsidian/ZenVault"
SOUL_FILE = VAULT / "00_System/SOUL.md"
ZEN_MEMORY = VAULT / "00_System/Project Memories/Zen Memory.md"
SANDBOX_RUNS = VAULT / "Sandbox/Runs"
AGENT_OS_DATA = Path(__file__).parent.parent / "agent-os-data.json"
ZEN_WORKER = HOME / ".local/bin/zen-worker"

# ── Helpers ────────────────────────────────────────────────────────────────

def now_iso():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def read_file_safe(path, default=""):
    try:
        return Path(path).read_text(errors="replace")
    except Exception:
        return default


def grep_yaml_value(text, key):
    """Extract value from a simple YAML key: value line."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(f"{key}:"):
            val = stripped.split(":", 1)[1].strip().strip('"').strip("'")
            return val
    return None


def redact_secrets(text):
    """Redact API keys and secrets from text before including in graph."""
    # Redact anything that looks like an API key
    text = re.sub(r'(sk-[a-zA-Z0-9]{20,})', '[REDACTED]', text)
    text = re.sub(r'(Bearer\s+[a-zA-Z0-9\-._~+/]+)', 'Bearer [REDACTED]', text)
    return text


# ── Node ID helpers ────────────────────────────────────────────────────────

_node_counter = 0

def nid(prefix=None):
    global _node_counter
    _node_counter += 1
    if prefix:
        return f"{prefix}-{_node_counter}"
    return f"node-{_node_counter}"


# ── Source: Hermes Config ─────────────────────────────────────────────────

def extract_models_from_config():
    """Extract model/provider info from Hermes config."""
    config_text = read_file_safe(HERMES_CONFIG)
    config_text = redact_secrets(config_text)

    nodes = []
    edges = []

    default_model = grep_yaml_value(config_text, "default") or "unknown"
    provider = grep_yaml_value(config_text, "provider") or "unknown"

    # Default cloud model node
    cloud_model_id = "model-default-cloud"
    nodes.append({
        "id": cloud_model_id,
        "type": "model",
        "label": default_model.split("/")[-1] if "/" in default_model else default_model,
        "full_name": default_model,
        "provider": provider,
        "role": "cloud",
        "active": True,
        "data": {
            "usage": "planning, research, review",
            "is_default": True
        }
    })

    # Extract all referenced models
    model_names = set()
    for line in config_text.splitlines():
        line = line.strip()
        if "model" in line.lower() and ":" in line and not line.startswith("#"):
            val = line.split(":", 1)[1].strip().strip('"').strip("'")
            if val and "/" in val and not val.startswith("http"):
                model_names.add(val)

    for mn in sorted(model_names):
        mn_id = f"model-{mn.replace('/', '-').replace(':', '-').replace('.', '-')}"
        role = "local" if mn.startswith("ollama/") or "gpt-oss" in mn else "cloud"
        nodes.append({
            "id": mn_id,
            "type": "model",
            "label": mn.split("/")[-1] if "/" in mn else mn,
            "full_name": mn,
            "provider": mn.split("/")[0] if "/" in mn else "unknown",
            "role": role,
            "active": role == "local",  # local model is actively running
            "data": {"usage": "local execution" if role == "local" else "cloud inference"}
        })
        edges.append({
            "source": mn_id,
            "target": "zen-agent",
            "type": "available_to",
            "label": "configured"
        })

    # Local worker model node
    worker_model_id = "model-worker-local"
    nodes.append({
        "id": worker_model_id,
        "type": "model",
        "label": "gpt-oss:20b",
        "full_name": "ollama/gpt-oss:20b",
        "provider": "ollama",
        "role": "local",
        "active": True,
        "data": {
            "endpoint": "localhost:11434",
            "usage": "file edits, artifact creation",
            "via": "zen-worker → Aider"
        }
    })

    return nodes, edges


# ── Source: SOUL.md (Agent Roster) ─────────────────────────────────────────

def extract_agent_roster():
    """Extract agent roster from SOUL.md."""
    nodes = []
    edges = []

    soul_text = read_file_safe(SOUL_FILE)

    # Zen agent (central)
    zen_id = "zen-agent"
    nodes.append({
        "id": zen_id,
        "type": "agent",
        "label": "Zen",
        "domain": "Governance, safety, system law",
        "authority": "root",
        "risk": "high",
        "active": True,
        "data": {
            "channel": "zen-chat",
            "role": "COO — second in command",
            "instructions_path": "00_System/Project Instructions/Zen Instructions.md",
            "memory_path": "00_System/Project Memories/Zen Memory.md"
        }
    })

    # Parse agent table from SOUL.md
    agent_section = False
    for line in soul_text.splitlines():
        if "## Project Zen — Agent Roster" in line:
            agent_section = True
            continue
        if agent_section and line.strip().startswith("|"):
            parts = [p.strip() for p in line.split("|")]
            # Skip header separator rows (e.g. |-------|---------|)
            if all(re.match(r'^-+$', p.strip()) for p in parts[1:-1] if p.strip()):
                continue
            if len(parts) >= 4 and parts[1] and parts[1] != "Agent":
                agent_name = parts[1]
                # Skip Zen — already added as zen-agent above
                if agent_name == "Zen":
                    continue
                channel = parts[2] if len(parts) > 2 else ""
                domain = parts[3] if len(parts) > 3 else ""
                agent_id = f"agent-{agent_name.lower()}"

                authority = "managed"
                risk = "high" if any(k in domain.lower() for k in ["trading", "automation"]) else "medium"

                nodes.append({
                    "id": agent_id,
                    "type": "agent",
                    "label": agent_name,
                    "domain": domain,
                    "authority": authority,
                    "risk": risk,
                    "active": True,
                    "data": {"channel": f"{agent_name.lower()}-chat" if not channel else channel}
                })

                # Edge: Zen governs all agents
                edges.append({
                    "source": zen_id,
                    "target": agent_id,
                    "type": "governs",
                    "label": "authority"
                })

                # Edge: Agent routes to cloud model for planning
                edges.append({
                    "source": agent_id,
                    "target": "model-default-cloud",
                    "type": "routes_to",
                    "label": "planning & review"
                })

                # Edge: Agent delegates to local worker for edits
                edges.append({
                    "source": agent_id,
                    "target": "model-worker-local",
                    "type": "delegates_to",
                    "label": "file edits"
                })

        if agent_section and line.strip().startswith("## ") and "Agent Roster" not in line:
            break

    return nodes, edges


# ── Source: Zen Memory ─────────────────────────────────────────────────────

def extract_system_facts():
    """Extract system facts from Zen Memory for resource/rule nodes."""
    nodes = []
    edges = []

    memory_text = read_file_safe(ZEN_MEMORY)
    memory_text = redact_secrets(memory_text)

    # Multi-LLM routing rule node
    rule_id = "rule-local-vs-cloud"
    nodes.append({
        "id": rule_id,
        "type": "rule",
        "label": "Local vs Cloud Routing",
        "authority": "system",
        "data": {
            "cloud": "Owl Alpha + Codex — planning, research, review",
            "local": "Aider + Ollama gpt-oss:20b — file edits, artifacts",
            "boundary": "Local never touches cloud keys. Cloud never processes raw file generation."
        }
    })
    edges.append({"source": "zen-agent", "target": rule_id, "type": "enforces", "label": "policy"})

    # Token budget resource node
    resource_id = "resource-token-budget"
    nodes.append({
        "id": resource_id,
        "type": "resource",
        "label": "Cloud Token Budget",
        "risk": "medium",
        "data": {
            "description": "Daily cloud token spend tracking",
            "warning": "Cloud tokens are finite — local worker should handle execution"
        }
    })
    edges.append({"source": "model-default-cloud", "target": resource_id, "type": "consumes", "label": "spend"})

    # Worker config resource
    worker_res_id = "resource-worker-config"
    nodes.append({
        "id": worker_res_id,
        "type": "resource",
        "label": "zen-worker",
        "risk": "low",
        "data": {
            "path": "~/.local/bin/zen-worker",
            "description": "Guarded local-only worker launcher",
            "safety": "Refuses cloud model args. Ollama only."
        }
    })
    edges.append({"source": "zen-agent", "target": worker_res_id, "type": "configures", "label": "owns"})
    edges.append({"source": worker_res_id, "target": "model-worker-local", "type": "launches", "label": "executes via Aider"})

    return nodes, edges


# ── Source: State DB (Sessions, Messages) ─────────────────────────────────

def extract_session_graph():
    """Extract session activity summary from Hermes state.db."""
    nodes = []
    edges = []

    if not STATE_DB.exists():
        return nodes, edges

    try:
        conn = sqlite3.connect(f"file:{STATE_DB}?mode=ro", uri=True)
        cur = conn.cursor()

        # Count sessions
        cur.execute("SELECT COUNT(*) FROM sessions")
        session_count = cur.fetchone()[0]

        # Count total messages
        cur.execute("SELECT COUNT(*) FROM messages")
        msg_count = cur.fetchone()[0]

        # Recent sessions (last 7 days)
        cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        cur.execute("SELECT title, created_at FROM sessions WHERE created_at >= ? ORDER BY created_at DESC LIMIT 10", (cutoff,))
        recent = cur.fetchall()

        # Session activity node
        session_node_id = "resource-sessions"
        nodes.append({
            "id": session_node_id,
            "type": "resource",
            "label": "Conversation Sessions",
            "risk": "low",
            "data": {
                "total_sessions": session_count,
                "total_messages": msg_count,
                "recent_7d": len(recent),
                "recent_titles": [r[0][:40] if r[0] else "(untitled)" for r in recent[:5]]
            }
        })
        edges.append({"source": "zen-agent", "target": session_node_id, "type": "participates_in", "label": "conversations"})

        conn.close()
    except Exception as e:
        pass  # If DB is locked or corrupt, skip

    return nodes, edges


# ── Source: Hermes Agent Log (Token Usage) ────────────────────────────────

def extract_token_activity():
    """Extract recent token activity from agent log."""
    nodes = []
    edges = []

    if not AGENT_LOG.exists():
        return nodes, edges

    try:
        # Read last 500 lines for recent activity
        result = subprocess.run(
            ["tail", "-500", str(AGENT_LOG)],
            capture_output=True, text=True, errors="replace", timeout=5
        )
        lines = result.stdout.splitlines()

        model_usage = {}
        recent_topics = []

        for line in lines:
            # Extract model usage
            if "agent.conversation_loop" in line:
                for model_name in ["owl-alpha", "codex", "gpt-oss", "minimax", "nemotron"]:
                    if model_name in line.lower():
                        model_usage[model_name] = model_usage.get(model_name, 0) + 1

        # Token activity node
        if model_usage:
            activity_node_id = "resource-token-activity"
            nodes.append({
                "id": activity_node_id,
                "type": "resource",
                "label": "Inference Activity",
                "risk": "low",
                "data": {
                    "recent_calls_by_model": model_usage,
                    "source": "last 500 log lines"
                }
            })
            for model_name in model_usage:
                model_id = f"model-{model_name}"
                edges.append({
                    "source": model_id,
                    "target": activity_node_id,
                    "type": "contributes_to",
                    "label": "activity"
                })

    except Exception:
        pass

    return nodes, edges


# ── Source: Obsidian Vault (Projects) ──────────────────────────────────────

def extract_projects():
    """Extract project nodes from Obsidian vault structure."""
    nodes = []
    edges = []

    if not VAULT.exists():
        return nodes, edges

    # Scan 01_Projects directory
    projects_dir = VAULT / "01_Projects"
    if projects_dir.exists():
        for item in sorted(projects_dir.iterdir()):
            if item.is_dir():
                project_id = f"project-{item.name.lower().replace(' ', '-')}"
                # Count markdown files
                md_files = list(item.rglob("*.md"))
                nodes.append({
                    "id": project_id,
                    "type": "project",
                    "label": item.name,
                    "active": len(md_files) > 0,
                    "data": {
                        "file_count": len(md_files),
                        "path": str(item.relative_to(VAULT))
                    }
                })
                edges.append({"source": "zen-agent", "target": project_id, "type": "oversees", "label": "governance"})

                # Connect project to its agent if exists
                agent_name = item.name.split()[0]  # e.g., "Swarm" from "Swarm Project"
                agent_id = f"agent-{agent_name.lower()}"
                edges.append({"source": agent_id, "target": project_id, "type": "owns", "label": "domain"})

    return nodes, edges


# ── Source: Sandbox Runs ───────────────────────────────────────────────────

def extract_sandbox_summary():
    """Extract sandbox activity summary — aggregated, not individual runs."""
    nodes = []
    edges = []

    if not SANDBOX_RUNS.exists():
        return nodes, edges

    total_runs = 0
    status_counts = {}
    review_counts = {"pending": 0, "accepted": 0, "rejected": 0}

    for run_file in SANDBOX_RUNS.rglob("SB-*.md"):
        total_runs += 1
        content = read_file_safe(run_file, "")
        status_match = re.search(r'^status:\s*(\w+)', content, re.M)
        review_match = re.search(r'^review_status:\s*(\w+)', content, re.M)
        if status_match:
            s = status_match.group(1)
            status_counts[s] = status_counts.get(s, 0) + 1
        if review_match:
            r = review_match.group(1)
            if r in review_counts:
                review_counts[r] += 1

    # Aggregated sandbox node
    sb_node_id = "resource-sandbox"
    nodes.append({
        "id": sb_node_id,
        "type": "resource",
        "label": "Sandbox",
        "risk": "low",
        "data": {
            "total_runs": total_runs,
            "status_breakdown": status_counts,
            "review_breakdown": review_counts
        }
    })
    edges.append({"source": "zen-agent", "target": sb_node_id, "type": "manages", "label": "orchestrator"})

    return nodes, edges


# ── Source: Local Worker Usage ─────────────────────────────────────────────

def extract_worker_activity():
    """Extract local worker activity."""
    nodes = []
    edges = []

    if LOCAL_WORKER_USAGE.exists():
        try:
            wu = json.loads(LOCAL_WORKER_USAGE.read_text())
            runs = wu.get("runs", [])
            worker_node_id = "resource-local-worker"
            nodes.append({
                "id": worker_node_id,
                "type": "resource",
                "label": "Local Worker Activity",
                "risk": "low",
                "data": {
                    "total_runs": len(runs),
                    "model": "gpt-oss:20b",
                    "tool": "Aider"
                }
            })
            edges.append({"source": "model-worker-local", "target": worker_node_id, "type": "executes", "label": "work"})
            edges.append({"source": worker_node_id, "target": "zens-agent", "type": "reports_to", "label": "results"})
        except Exception:
            pass

    return nodes, edges


# ── Source: Agent OS Data (reuse existing pipeline) ───────────────────────

def extract_agent_os_data():
    """Extract from the existing agent-os-data.json if available."""
    nodes = []
    edges = []

    if not AGENT_OS_DATA.exists():
        return nodes, edges

    try:
        aos = json.loads(AGENT_OS_DATA.read_text())
        token_usage = aos.get("token_usage", {})
        sandbox = aos.get("sandbox", {})

        # Token usage resource
        today = token_usage.get("today", {})
        if today:
            token_node_id = "resource-tokens-today"
            nodes.append({
                "id": token_node_id,
                "type": "resource",
                "label": "Today's Token Usage",
                "risk": "medium",
                "data": {
                    "calls": today.get("calls", 0),
                    "cost": today.get("cost", 0),
                    "in_tokens": today.get("in", 0),
                    "out_tokens": today.get("out", 0)
                }
            })
            edges.append({"source": "model-default-cloud", "target": token_node_id, "type": "generates", "label": "usage"})

        # Sandbox summary
        total_sb = sandbox.get("total", 0)
        if total_sb:
            node_id = "resource-sandbox-summary"
            # Only add if not already added from direct scan
            nodes.append({
                "id": node_id,
                "type": "resource",
                "label": "Sandbox Summary",
                "risk": "low",
                "data": {
                    "total": total_sb,
                    "accepted": sandbox.get("accepted", 0),
                    "rejected": sandbox.get("rejected", 0),
                    "pending": sandbox.get("needs_review", 0)
                }
            })
    except Exception:
        pass

    return nodes, edges


# ── Source: zen-worker config ──────────────────────────────────────────────

def extract_worker_config():
    """Extract zen-worker configuration."""
    nodes = []
    edges = []

    if ZEN_WORKER.exists():
        content = read_file_safe(ZEN_WORKER)
        content = redact_secrets(content)

        worker_node_id = "resource-zen-worker"
        nodes.append({
            "id": worker_node_id,
            "type": "tool",
            "label": "zen-worker",
            "risk": "low",
            "data": {
                "path": str(ZEN_WORKER),
                "description": "Guarded local-only worker: Aider + Ollama gpt-oss:20b",
                "safety": "Refuses non-Ollama models and cloud-looking args"
            }
        })
        edges.append({"source": "zen-agent", "target": worker_node_id, "type": "configures", "label": "owns"})
        edges.append({"source": worker_node_id, "target": "model-worker-local", "type": "invokes", "label": "Aider → Ollama"})

    return nodes, edges


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    all_nodes = []
    all_edges = []
    seen_ids = set()

    def add_nodes_edges(new_nodes, new_edges):
        for n in new_nodes:
            if n["id"] not in seen_ids:
                all_nodes.append(n)
                seen_ids.add(n["id"])
        all_edges.extend(new_edges)

    # Run all extractors
    add_nodes_edges(*extract_models_from_config())
    add_nodes_edges(*extract_agent_roster())
    add_nodes_edges(*extract_system_facts())
    add_nodes_edges(*extract_session_graph())
    add_nodes_edges(*extract_token_activity())
    add_nodes_edges(*extract_projects())
    add_nodes_edges(*extract_sandbox_summary())
    add_nodes_edges(*extract_worker_activity())
    add_nodes_edges(*extract_agent_os_data())
    add_nodes_edges(*extract_worker_config())

    # Build output
    output = {
        "generated_at": now_iso(),
        "exporter_version": "0.1.0",
        "stats": {
            "total_nodes": len(all_nodes),
            "total_edges": len(all_edges),
            "node_types": {},
            "edge_types": {}
        },
        "nodes": all_nodes,
        "edges": all_edges
    }

    # Compute type stats
    for n in all_nodes:
        t = n.get("type", "unknown")
        output["stats"]["node_types"][t] = output["stats"]["node_types"].get(t, 0) + 1
    for e in all_edges:
        t = e.get("type", "unknown")
        output["stats"]["edge_types"][t] = output["stats"]["edge_types"].get(t, 0) + 1

    # Ensure output directory exists
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    # Write
    OUTPUT.write_text(json.dumps(output, indent=2, ensure_ascii=False))

    print(f"✅ Memory World exported: {OUTPUT}")
    print(f"   Nodes: {len(all_nodes)} | Edges: {len(all_edges)}")
    print(f"   Node types: {output['stats']['node_types']}")
    print(f"   Edge types: {output['stats']['edge_types']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
