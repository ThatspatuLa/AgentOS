#!/usr/bin/env python3
"""Agent OS local dev server with session + Kanban persistence.

Reads message history from Hermes state.db so chat history is the
same across Discord, Hermes, and the Agent OS dashboard.

Session <-> Hermes mapping is stored in data/sessions/index.json
via the hermesSessionId field on each session.

Serves static files and adds:
  GET/PUT  /api/kanban-tasks              -> data/kanban-tasks.json
  GET      /api/sessions                  -> session index (with hermes links)
  GET/PUT  /api/sessions/:id              -> session summary + metadata
  GET      /api/sessions/:id/messages     -> reads from Hermes state.db
  POST     /api/sessions/:id/messages     -> Agent OS chat (stores locally)
  GET      /api/sessions/:id/rollup       -> child task summaries (filtered by parentSessionId)
  GET      /api/events/:sessionId         -> session event log (events.jsonl)
  GET      /api/memory                    -> Hermes MEMORY.md + USER.md
  GET      /api/memory-world              -> data/memory-world.json
  GET      /api/health                    -> live Hermes + git status

Data layout:
  data/sessions/index.json                   -> session registry + hermesSessionId links
  data/sessions/<sessionId>/summary.json     -> decisions, blockers, files, validation, recentActivity
  data/sessions/<sessionId>/messages.jsonl   -> Agent OS chat messages (supplemental)
  data/sessions/<sessionId>/events.jsonl     -> execution event log (hermes_turn, etc.)
  data/memory-world.json                     -> force-directed graph data
  ~/.hermes/memories/MEMORY.md               -> project rules, conventions, roadmap
  ~/.hermes/memories/USER.md                 -> user profile and preferences
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sqlite3
import shutil
import subprocess
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
TASKS_FILE = ROOT / "data" / "kanban-tasks.json"
SESSIONS_DIR = ROOT / "data" / "sessions"
SESSIONS_INDEX = SESSIONS_DIR / "index.json"
HERMES_SESSION_MAP_FILE = SESSIONS_DIR / "hermes-session-map.json"

# Hermes session DB — the source of truth for Discord message history
HERMES_DB = Path.home() / ".hermes" / "state.db"
HERMES_AGENT_DIR = Path.home() / ".hermes" / "hermes-agent"
HERMES_PYTHON = HERMES_AGENT_DIR / "venv" / "bin" / "python"
ZEN_HERMES_PROGRESS_PREFIX = "__ZEN_HERMES_PROGRESS__"

# ---------------------------------------------------------------------------
# Defaults — sessions are created on first server start if missing
# ---------------------------------------------------------------------------

DEFAULT_SESSIONS = [
    {
        "id": "zen-os",
        "type": "global",
        "label": "Zen OS",
        "icon": "🖥️",
        "projectId": None,
        "parentSessionId": None,
        "hermesSessionId": None,  # will be set to the active zen-os Discord channel Hermes session
        "channel": "1516644810834841672",
        "color": "#7c3aed",
        "summary": "Agent OS · UI & Frontend · Unified dashboard view",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M4 — Hermes webhook integration",
        "recentActivity": [],
        "taskId": None,
        "messageCount": 0,
        "repo": str(ROOT),
        "branchPrefix": "main",
    },
    {
        "id": "zen",
        "type": "main",
        "label": "Zen",
        "icon": "🦉",
        "projectId": "zen",
        "parentSessionId": None,
        "hermesSessionId": "20260607_193056_683caea5",
        "channel": "zen-chat",
        "color": "#6366f1",
        "summary": "Project Zen — intelligence engine, monitoring, monetization",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M4 — Zen intelligence engine scaffold",
        "recentActivity": [],
        "taskId": None,
        "messageCount": 110,
        "repo": str(ROOT),
        "branchPrefix": "session/zen/",
    },
    {
        "id": "kiyosaki",
        "type": "main",
        "label": "Kiyosaki",
        "icon": "📊",
        "projectId": "kiyosaki",
        "parentSessionId": None,
        "hermesSessionId": "20260607_193646_a685023b",
        "channel": "kiyosaki-chat",
        "color": "#059669",
        "summary": "Kiyosaki — quantitative trading system",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M3 — ETHUSDT strategy backtest complete",
        "recentActivity": [],
        "taskId": None,
        "messageCount": 356,
        "repo": str(Path.home() / "Projects" / "Kiyosaki"),
        "branchPrefix": "session/kiyosaki/",
    },
    {
        "id": "minato",
        "type": "main",
        "label": "Minato",
        "icon": "🔧",
        "projectId": "minato",
        "parentSessionId": None,
        "hermesSessionId": None,
        "channel": "minato-chat",
        "color": "#d97706",
        "summary": "Minato — devops & automation",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M1 — project scoping",
        "recentActivity": [],
        "taskId": None,
        "messageCount": 0,
        "repo": str(ROOT),
        "branchPrefix": "session/minato/",
    },
    {
        "id": "rin",
        "type": "main",
        "label": "Rin",
        "icon": "🎯",
        "projectId": "rin",
        "parentSessionId": None,
        "hermesSessionId": "20260608_042151_b4f5cf15",
        "channel": "rin-chat",
        "color": "#dc2626",
        "summary": "Rin — targeting & execution systems",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M1 — project scoping",
        "recentActivity": [],
        "taskId": None,
        "messageCount": 3,
        "repo": str(ROOT),
        "branchPrefix": "session/rin/",
    },
    {
        "id": "toji",
        "type": "main",
        "label": "Toji",
        "icon": "⚡",
        "projectId": "toji",
        "parentSessionId": None,
        "hermesSessionId": "20260608_042419_3a98b4c7",
        "channel": "toji-chat",
        "color": "#2563eb",
        "summary": "Toji — rapid execution & infrastructure",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M1 — project scoping",
        "recentActivity": [],
        "taskId": None,
        "messageCount": 17,
        "repo": str(ROOT),
        "branchPrefix": "session/toji/",
    },
    {
        "id": "kazuki",
        "type": "main",
        "label": "Kazuki",
        "icon": "🛡️",
        "projectId": "kazuki",
        "parentSessionId": None,
        "hermesSessionId": None,
        "channel": "kazuki-chat",
        "color": "#f59e0b",
        "summary": "Kazuki — creative & music",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M1 — project scoping",
        "recentActivity": [],
        "taskId": None,
        "messageCount": 0,
        "repo": str(ROOT),
        "branchPrefix": "session/kazuki/",
    },
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _run_status_command(args: list[str], timeout: int = 10) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            args,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        )
        return proc.returncode, proc.stdout.strip()
    except Exception as exc:
        return 1, str(exc)


def _parse_hermes_status(output: str) -> dict:
    status = {
        "running": False,
        "model": "—",
        "provider": "—",
        "checkedAt": _now_iso(),
        "source": "hermes status",
    }
    for line in output.splitlines():
        clean = line.strip()
        if clean.startswith("Model:"):
            status["model"] = clean.split("Model:", 1)[1].strip() or "—"
        elif clean.startswith("Provider:"):
            status["provider"] = clean.split("Provider:", 1)[1].strip() or "—"
    status["running"] = bool(status["model"] != "—" or status["provider"] != "—")
    return status


def _git_health() -> dict:
    code, out = _run_status_command(["git", "status", "--short", "--branch"], timeout=5)
    if code != 0:
        return {"status": "Unknown", "ok": False, "details": out}
    lines = [line for line in out.splitlines() if line.strip()]
    dirty = [line for line in lines if not line.startswith("##")]
    branch = lines[0].replace("##", "").strip() if lines and lines[0].startswith("##") else ""
    return {
        "status": "Clean" if not dirty else f"Dirty · {len(dirty)}",
        "ok": not dirty,
        "branch": branch,
        "details": out,
    }


def _live_health() -> dict:
    code, out = _run_status_command(["hermes", "status"], timeout=15)
    hermes = _parse_hermes_status(out) if code == 0 else {
        "running": False,
        "model": "—",
        "provider": "—",
        "checkedAt": _now_iso(),
        "source": "hermes status",
        "error": out,
    }
    return {
        "updatedAt": _now_iso(),
        "hermes": hermes,
        "git": _git_health(),
    }


def _read_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return default
    return default


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=2)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)


# ═══════════════════════════════════════════════════════════════════════════
# M5 — Live Data Bridge: helpers for hot-reloading data/memory-world.json
# These pure functions are also mirrored in agent-os.html (the JS code uses
# the same logic). Tests live in tests/test_memory_world_refresh.py.
# ═══════════════════════════════════════════════════════════════════════════

# Sub-second jitter tolerance for mtime comparison. Filesystem mtimes can
# differ by microseconds between reads; treating that as "changed" would
# cause spurious reloads every poll.
_MTIME_TOLERANCE = 0.001


def _memory_world_mtime_payload(path: Path) -> dict:
    """Return the {mtime, size} payload served by /api/memory-world/mtime.

    mtime is normalised to integer seconds so the value is stable across
    pollers that read the same file within the same second.
    """
    st = path.stat()
    return {
        "mtime": int(st.st_mtime),
        "size": st.st_size,
    }


def mtime_changed(last_known: float | None, current: float) -> bool:
    """Polling comparator: did the file change since the last poll?

    Returns True on the first poll (last_known is None) or whenever the
    current mtime is strictly greater than the last known mtime (with a
    small tolerance to absorb filesystem jitter).
    """
    if last_known is None:
        return True
    return current > last_known + _MTIME_TOLERANCE


def diff_node_ids(prev_ids: list[str], new_ids: list[str]) -> tuple[list[str], list[str]]:
    """Return (added, removed) node IDs between two snapshots.

    Order-independent: the caller passes lists, but comparison is set-based.
    Returned lists are sorted for deterministic output (and to match the
    JS implementation, which uses Set operations and Array.from().sort()).
    """
    prev = set(prev_ids)
    new = set(new_ids)
    added = sorted(new - prev)
    removed = sorted(prev - new)
    return added, removed


def format_relative_time(ts: float, now: float | None = None) -> str:
    """Format a unix timestamp as a Discord-style '5s ago' / '2m ago' string.

    Mirrors the JS helper used in the Memory World controls bar.
    """
    if now is None:
        now = time.time()
    delta = now - ts
    if delta < 5:
        return "just now"
    if delta < 60:
        return f"{int(delta)}s ago"
    if delta < 3600:
        return f"{int(delta // 60)}m ago"
    if delta < 86400:
        return f"{int(delta // 3600)}h ago"
    return f"{int(delta // 86400)}d ago"


def _append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _append_session_event(session_id: str, event: dict) -> None:
    """Append an event to the session's events.jsonl file."""
    event_path = SESSIONS_DIR / session_id / "events.jsonl"
    _append_jsonl(event_path, event)


def _read_session_events(session_id: str) -> list[dict]:
    """Read all events from a session's events.jsonl file."""
    event_path = SESSIONS_DIR / session_id / "events.jsonl"
    return _read_jsonl(event_path)


def _session_summary_payload(session_id: str, index_session: dict | None = None) -> dict:
    """Read session summary fields from index plus optional summary.json."""
    summary_path = SESSIONS_DIR / session_id / "summary.json"
    payload = _read_json(summary_path, {}) if summary_path.exists() else {}
    if index_session:
        for key in (
            "id", "label", "summary", "filesTouched", "validationProof",
            "recentActivity", "nextSafeGate", "taskId", "projectId",
        ):
            if key in index_session and key not in payload:
                payload[key] = index_session.get(key)
    payload.setdefault("filesTouched", [])
    payload.setdefault("validationProof", [])
    payload.setdefault("recentActivity", [])
    payload.setdefault("nextSafeGate", "")
    return payload


def _run_git_readonly(args: list[str], timeout: int = 8) -> str:
    """Run a read-only git command against this repo and return stdout."""
    allowed = {
        ("status", "--short"),
        ("diff", "--stat"),
        ("diff", "--name-status"),
        ("diff", "--"),
    }
    key = tuple(args[:2]) if len(args) >= 2 else tuple(args)
    if key not in allowed:
        raise ValueError("unsupported read-only git command")
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        return (result.stderr or result.stdout or "").strip()
    return (result.stdout or "").strip()


def _review_risk_notes(changed_files: list[dict]) -> list[str]:
    notes: list[str] = []
    paths = [str(f.get("path") or "") for f in changed_files]
    if any(p.startswith("agent_os_server.py") or p.endswith(".py") for p in paths):
        notes.append("Backend or Python files changed: confirm API behavior and syntax before accepting.")
    if any(p.endswith((".html", ".css", ".js")) for p in paths):
        notes.append("Frontend files changed: inspect layout, drawers, chat widget, spacing, typography, buttons, cards, animation, and visual consistency.")
    if any(p.startswith("data/") for p in paths):
        notes.append("Data files changed: confirm this is intended state, not temporary runtime noise.")
    if any(p.startswith("tmp/") or "__pycache__" in p for p in paths):
        notes.append("Temporary/generated files detected: avoid including them in the final GitHub update.")
    if not notes:
        notes.append("No elevated risk detected from the current file list; still review behavior before accepting.")
    return notes


def _git_review_payload(session_id: str = "", include_raw: bool = False) -> dict:
    """Build read-only backend review evidence for the current worktree."""
    status_text = _run_git_readonly(["status", "--short"])
    stat_text = _run_git_readonly(["diff", "--stat"])
    name_status = _run_git_readonly(["diff", "--name-status"])
    changed_files: list[dict] = []
    for line in name_status.splitlines():
        parts = line.split("\t")
        if not parts:
            continue
        status = parts[0].strip()
        path = parts[-1].strip() if len(parts) > 1 else ""
        if path:
            changed_files.append({"status": status, "path": path})
    if not changed_files:
        for line in status_text.splitlines():
            status = line[:2].strip() or "?"
            path = line[3:].strip()
            if path:
                changed_files.append({"status": status, "path": path})

    index = _load_index()
    session = _get_session(index, session_id) if session_id else None
    summary = _session_summary_payload(session_id, session) if session_id else {}
    events = _read_session_events(session_id)[-12:] if session_id else []
    validation = list(summary.get("validationProof") or [])
    event_validation = [
        item
        for event in events
        for item in (event.get("validation") or [])
    ]
    if event_validation:
        validation.extend(event_validation[-8:])

    payload = {
        "ok": True,
        "repo": str(ROOT),
        "sessionId": session_id,
        "changedFiles": changed_files,
        "diffStat": stat_text,
        "status": status_text,
        "rawDiff": _run_git_readonly(["diff", "--"], timeout=12)[:60000] if include_raw else "",
        "validationProof": validation,
        "riskNotes": _review_risk_notes(changed_files),
        "recentActivity": list(summary.get("recentActivity") or [])[-10:],
        "nextSafeGate": summary.get("nextSafeGate", ""),
        "events": events,
        "frontendChecklist": [
            "Layout and responsive spacing",
            "Drawer behavior and overlays",
            "Zen Chat widget and completion gates",
            "Typography hierarchy and line lengths",
            "Buttons, pills, cards, and disabled states",
            "Motion/animation continuity",
            "Visual consistency across full, drawer, and small chat views",
        ],
    }
    return payload


def _update_session_recent_activity(session_id: str, milestone: str, cap: int = 10) -> None:
    """Append a curated milestone to the session's recentActivity list."""
    index = _load_index()
    s = _get_session(index, session_id)
    if not s:
        return
    recent = list(s.get("recentActivity") or [])
    recent.append(milestone)
    if len(recent) > cap:
        recent = recent[-cap:]
    s["recentActivity"] = recent
    s["lastActive"] = _now_iso()
    _save_index(index)


def _build_context_prefix(s: dict) -> str:
    """Build a compact context block (<500 chars) from session state for Hermes injection.

    Includes: task title/status if linked, nextSafeGate, blockers, last 1-2 decisions.
    Returns empty string if no useful context available.
    """
    parts = []
    task_id = s.get("taskId")
    if task_id:
        parts.append(f"[Task: {task_id}]")
    next_gate = s.get("nextSafeGate", "")
    if next_gate:
        parts.append(f"[Next gate: {next_gate}]")
    blockers = s.get("blockers", [])
    if blockers:
        blocker_text = "; ".join(str(b) for b in blockers[:2])
        parts.append(f"[Blockers: {blocker_text}]")
    decisions = s.get("decisions", [])
    if decisions:
        dec_text = "; ".join(str(d) for d in decisions[-2:])
        parts.append(f"[Decisions: {dec_text}]")
    if not parts:
        return ""
    prefix = "\n".join(parts)
    if len(prefix) > 500:
        prefix = prefix[:497] + "..."
    return prefix


def _extract_files_touched(assistant_record: dict | None, progress: list[str], stdout: str) -> list[str]:
    """Extract file paths touched by Hermes tool calls (conservative).

    Parses progress lines and stdout for explicit file references:
    - write_file / patch / edit tool calls with file_path arguments
    - terminal commands that touch files (git add, cp, mv, touch)
    - file paths mentioned in tool previews

    Returns deduplicated list of file paths, max 10.
    """
    if not assistant_record:
        return []

    files: list[str] = []
    seen: set[str] = set()

    def _add(path: str) -> None:
        path = str(path).strip()
        if path and path not in seen and len(files) < 10:
            # Only include paths that look like real files (have extension or are in known dirs)
            if "/" in path or "." in path or path.endswith((".py", ".js", ".html", ".css", ".json", ".md", ".sh", ".txt", ".yaml", ".yml")):
                seen.add(path)
                files.append(path)

    # Parse progress lines for tool calls
    for line in progress:
        line_str = str(line)
        # Match patterns like: 📝 write_file: "agent-os.html" or ✏️ patch: "agent-os.html"
        m = re.search(r'(?:write_file|patch|edit|create_file|update_file)[\s:]+["\x27]?([^"\x27\s,]+)["\x27]?', line_str, re.IGNORECASE)
        if m:
            _add(m.group(1))
        # Match file paths in previews like: "agent-os.html" (120 chars)
        m2 = re.search(r'["\x27]([a-zA-Z0-9_\-./]+\.(?:py|js|html|css|json|md|sh|txt|yaml|yml|toml|cfg|ini))["\x27]', line_str)
        if m2:
            _add(m2.group(1))

    # Parse stdout for file references
    for raw_line in (stdout or "").splitlines():
        line = _strip_ansi(raw_line).strip()
        # Match "File: agent-os.html" or "Writing agent-os.html" or "Patched agent-os.html"
        m = re.search(r'(?:File|Writing|Patched|Created|Updated|Editing|Touched)[\s:]+([a-zA-Z0-9_\-./]+\.(?:py|js|html|css|json|md|sh|txt|yaml|yml|toml|cfg|ini))', line, re.IGNORECASE)
        if m:
            _add(m.group(1))
        # Match git add / git commit patterns
        m2 = re.search(r'git\s+(?:add|commit|diff)\s+.*?([a-zA-Z0-9_\-./]+\.(?:py|js|html|css|json|md|sh|txt|yaml|yml))', line)
        if m2:
            _add(m2.group(1))

    return files


def _extract_validation(assistant_record: dict | None, progress: list[str], stdout: str) -> list[dict]:
    """Extract validation results from Hermes output.

    Detects patterns:
    - py_compile / python3 -m py_compile
    - node --check
    - git diff --check
    - pytest / npm test / test commands
    - API smoke checks (curl to localhost)
    - "PASS" / "OK" / "success" after a command

    Returns list of {command, status, summary} dicts, max 5.
    """
    if not assistant_record:
        return []

    validations: list[dict] = []
    seen: set[str] = set()

    def _add(command: str, status: str, summary: str) -> None:
        key = f"{command}:{status}"
        if key not in seen and len(validations) < 5:
            seen.add(key)
            validations.append({"command": command, "status": status, "summary": summary[:120]})

    # Known validation command patterns
    validation_patterns = [
        (r'(?:python3?\s+-m\s+py_compile|py_compile)\s+([^\s]+)', 'py_compile'),
        (r'(?:node\s+--check)\s+([^\s]+)', 'node-check'),
        (r'git\s+diff\s+--check', 'git-diff-check'),
        (r'(?:pytest|npm\s+test|python3?\s+-m\s+pytest)(?:\s+([^\s]+))?', 'test'),
        (r'curl\s+(?:-[sS]+\s+)?https?://localhost[^\s]+', 'api-check'),
        (r'(?:npm\s+run\s+build|python3?\s+.*\.py)', 'build'),
    ]

    # Scan stdout for validation commands
    lines = (stdout or "").splitlines()
    for i, raw_line in enumerate(lines):
        line = _strip_ansi(raw_line).strip()
        for pattern, vtype in validation_patterns:
            m = re.search(pattern, line, re.IGNORECASE)
            if m:
                cmd = m.group(0).strip()[:80]
                # Look ahead for pass/fail indicators
                status = "pass"
                summary = ""
                for j in range(i + 1, min(i + 4, len(lines))):
                    next_line = _strip_ansi(lines[j]).strip().lower()
                    if any(fail in next_line for fail in ["error", "fail", "traceback", "syntaxerror", "❌", "✗"]):
                        status = "fail"
                        summary = _strip_ansi(lines[j]).strip()[:120]
                        break
                    if any(pass_ in next_line for pass_ in ["pass", "ok", "success", "✅", "✓", "0 errors", "clean"]):
                        summary = _strip_ansi(lines[j]).strip()[:120]
                        break
                _add(cmd, status, summary)

    # Also check progress lines for validation results
    for line in (assistant_record.get("progress", []) or []):
        line_str = str(line)
        if any(v in line_str.lower() for v in ["py_compile", "node --check", "git diff --check", "pytest", "test pass", "test fail"]):
            status = "fail" if any(f in line_str.lower() for f in ["fail", "error", "❌"]) else "pass"
            _add(line_str[:80], status, "")

    return validations


def _strip_context_and_echo(summary_text: str, context_prefix: str, user_message: str) -> str:
    """Strip injected context block and Hermes query echo from a summary string.

    Hermes echoes the full query (including context prefix) in its output.
    This function removes that echo so stored summaries describe the result,
    not the hidden context.
    """
    text = summary_text
    # Strip context prefix if present (exact match)
    if context_prefix and context_prefix in text:
        text = text.replace(context_prefix, "").strip()
    # Also try with collapsed whitespace (Hermes may reformat newlines)
    if context_prefix:
        collapsed_prefix = " ".join(context_prefix.split())
        if collapsed_prefix in text and collapsed_prefix != context_prefix:
            text = text.replace(collapsed_prefix, "").strip()
    # Strip individual context block lines that may appear scattered
    if context_prefix:
        for line in context_prefix.splitlines():
            line = line.strip()
            if line and line in text:
                text = text.replace(line, "").strip()
    # Strip query echo: the user message may appear at the start of the response
    if user_message and text.startswith(user_message):
        text = text[len(user_message):].strip()
    # Strip common Hermes boilerplate prefixes
    boilerplate = [
        "Initializing agent...",
        "Resume this session with:",
        "Session:",
        "Title:",
        "Duration:",
        "Messages:",
    ]
    lines = text.splitlines()
    cleaned: list[str] = []
    skip = True
    for line in lines:
        stripped = line.strip()
        if skip and any(stripped.startswith(bp) for bp in boilerplate):
            continue
        if stripped:
            skip = False
        cleaned.append(line)
    # Remove empty lines from start/end and collapse multiple empty lines
    result = "\n".join(cleaned).strip()
    while "\n\n\n" in result:
        result = result.replace("\n\n\n", "\n\n")
    return result


def _update_kanban_activity(session_id: str, task_id: str, milestone: str) -> None:
    """Append a curated milestone to the linked Kanban task's activity[].

    Caps activity at 5 entries. Persists to data/kanban-tasks.json.
    Server-authoritative: this is the single source of truth for Kanban activity.
    """
    if not task_id or not milestone:
        return
    tasks_data = _read_json(TASKS_FILE, {"tasks": []})
    tasks = tasks_data.get("tasks", [])
    for i, t in enumerate(tasks):
        if t.get("id") == task_id:
            activity = list(t.get("activity") or [])
            activity.append(milestone)
            if len(activity) > 5:
                activity = activity[-5:]
            t["activity"] = activity
            t["lastActive"] = "Now"
            tasks[i] = t
            tasks_data["tasks"] = tasks
            _write_json(TASKS_FILE, tasks_data)
            return


def _write_hermes_turn_event(session_id: str, s: dict, user_record: dict, assistant_record: dict | None, context_prefix: str = "") -> None:
    """Write a hermes_turn event to the session's events.jsonl and update summary.

    Also updates the linked Kanban task's activity[] (server-authoritative).
    """
    now = _now_iso()
    # Determine status from assistant record
    status = "complete"
    summary_text = ""
    files_touched: list[str] = []
    evidence: list[str] = []
    validation: list[dict] = []
    progress: list[str] = []
    stdout_text = ""
    user_message = user_record.get("content", "") if user_record else ""

    if assistant_record:
        source = assistant_record.get("source", "")
        progress = assistant_record.get("progress", []) or []
        stdout_text = assistant_record.get("content", "") or ""
        if source == "hermes-error":
            status = "failed"
            summary_text = assistant_record.get("content", "")[:200]
        else:
            # PATCH 5: Strip context block and query echo BEFORE truncating
            raw_content = stdout_text
            cleaned = _strip_context_and_echo(raw_content, context_prefix, user_message)
            summary_text = _truncate_middle(cleaned, 200)
            # Extract progress lines as evidence
            if progress:
                evidence = [str(p) for p in progress[:5]]
            # PATCH 3: Extract files touched from tool output
            files_touched = _extract_files_touched(assistant_record, progress, stdout_text)
            # PATCH 4: Extract validation results from output
            validation = _extract_validation(assistant_record, progress, stdout_text)
    else:
        status = "failed"
        summary_text = "No response from Hermes"

    # Final clean summary (in case truncation re-introduced partial context)
    clean_summary = _strip_context_and_echo(summary_text, context_prefix, user_message)

    event = {
        "ts": now,
        "type": "hermes_turn",
        "sessionId": session_id,
        "taskId": s.get("taskId"),
        "status": status,
        "summary": clean_summary,
        "evidence": evidence,
        "validation": validation,
        "filesTouched": files_touched,
        "source": "hermes",
    }
    _append_session_event(session_id, event)

    # PATCH 1: Server-side Kanban activity curation
    task_id = s.get("taskId")
    if task_id:
        kanban_milestone = _curate_milestone(status, clean_summary, evidence, files_touched, validation)
        _update_kanban_activity(session_id, task_id, kanban_milestone)
    else:
        # Still update session recentActivity even without linked task
        milestone = _curate_milestone(status, clean_summary, evidence, files_touched, validation)
        if milestone:
            _update_session_recent_activity(session_id, milestone)

    # Update Obsidian project memory with facts from this turn
    _update_obsidian_memory(session_id, s, assistant_record, clean_summary, files_touched)


# Obsidian vault path — single source of truth for project knowledge
_OBSIDIAN_VAULT = Path.home() / "Obsidian" / "ZenVault"
_OBSIDIAN_MEMORIES_DIR = _OBSIDIAN_VAULT / "00_System" / "Project Memories"

# Map Agent OS session IDs → Obsidian memory filenames
_SESSION_MEMORY_FILE: dict[str, str] = {
    "zen": "Zen Memory.md",
    "zen-os": "Zen Memory.md",
    "rin": "Rin Memory.md",
    "kiyosaki": "Kiyosaki Memory.md",
    "toji": "Toji Memory.md",
    "minato": "Minato Memory.md",
    "kazuki": "Kazuki Memory.md",
}


def _update_obsidian_memory(
    session_id: str,
    s: dict,
    assistant_record: dict | None,
    summary_text: str,
    files_touched: list[str],
) -> None:
    """Append durable facts from a Hermes turn to the project's Obsidian memory file.

    This is the Obsidian → Hermes bridge (Gap C/E fix). After each Hermes turn,
    we extract key facts (files touched, decisions, summary) and append them to
    the relevant Obsidian memory file. A separate sync step compacts Obsidian
    memories into MEMORY.md for Hermes injection.
    """
    memory_filename = _SESSION_MEMORY_FILE.get(session_id)
    if not memory_filename:
        return  # session has no Obsidian memory file

    mem_path = _OBSIDIAN_MEMORIES_DIR / memory_filename
    if not mem_path.exists():
        return

    now = _now_iso()
    entry_lines = [f"## Turn — {now}", ""]

    # Add summary of what was done
    if summary_text:
        entry_lines.append(f"**Summary:** {summary_text}")
        entry_lines.append("")

    # Add files touched
    if files_touched:
        entry_lines.append(f"**Files touched:** {', '.join(files_touched[:5])}")
        entry_lines.append("")

    # Extract decisions from assistant output (commitment language)
    if assistant_record:
        content = assistant_record.get("content", "") or ""
        decisions = _extract_decisions(content)
        if decisions:
            entry_lines.append("**Decisions:**")
            for d in decisions[:3]:
                entry_lines.append(f"- {d}")
            entry_lines.append("")

    # Only write if there's meaningful content beyond the header
    if len(entry_lines) <= 2:
        return

    entry_text = "\n".join(entry_lines) + "\n"

    try:
        existing = mem_path.read_text(encoding="utf-8")
        # Insert new entry right after the frontmatter/header (after first --- block)
        # Find the end of the first --- ... --- block
        lines = existing.split("\n")
        insert_idx = 0
        in_header = False
        for i, line in enumerate(lines):
            if line.strip() == "---":
                if not in_header:
                    in_header = True
                else:
                    insert_idx = i + 1
                    break

        # Insert the new entry after the header
        lines.insert(insert_idx, "")
        lines.insert(insert_idx + 1, entry_text.rstrip())
        lines.insert(insert_idx + 2, "")
        mem_path.write_text("\n".join(lines), encoding="utf-8")
    except Exception:
        pass  # Obsidian update is best-effort; don't break the turn


def _extract_decisions(content: str) -> list[str]:
    """Extract decision/commitment statements from Hermes output.

    Looks for lines with commitment language that indicate a decision was made
    or an action was taken.
    """
    decisions = []
    commitment_patterns = [
        r"(?:I will|I'll|Let me|I'm going to|I've|I have)\s+(.+)",
        r"(?:Fixed|Patched|Updated|Created|Added|Removed|Refactored)\s+(.+)",
        r"(?:Decision|Decided|Chose|Selected):\s*(.+)",
    ]
    for line in content.split("\n"):
        line = line.strip()
        if not line or len(line) > 200:
            continue
        for pattern in commitment_patterns:
            m = re.search(pattern, line, re.IGNORECASE)
            if m:
                decision = m.group(0).strip()
                if decision and decision not in decisions:
                    decisions.append(decision)
                break
    return decisions[:5]


def _sync_obsidian_to_memory_md() -> None:
    """Compact Obsidian project memories into MEMORY.md for Hermes injection.

    This is the sync bridge: Obsidian (rich, structured) → MEMORY.md (compact,
    Hermes-consumable). Called on server start and can be triggered periodically.

    Respects Hermes's memory_char_limit (2200 chars default).
    """
    mem_dir = HERMES_DB.parent / "memories"
    mem_dir.mkdir(parents=True, exist_ok=True)
    memory_path = mem_dir / "MEMORY.md"

    # Read existing MEMORY.md to preserve non-Obsidian sections
    existing_sections: dict[str, str] = {}
    if memory_path.exists():
        current = memory_path.read_text(encoding="utf-8")
        # Split into sections by § delimiter
        parts = current.split("\n§\n")
        for part in parts:
            lines = part.strip().split("\n")
            if lines:
                # Use first line as section key
                key = lines[0].strip()
                existing_sections[key] = part.strip()

    # Build Obsidian-derived content
    obsidian_content: list[str] = []
    obsidian_content.append("## Obsidian Project Memories (auto-synced)")
    obsidian_content.append("")

    # Read SOUL.md for cross-project context
    soul_path = _OBSIDIAN_VAULT / "00_System" / "SOUL.md"
    if soul_path.exists():
        try:
            soul_text = soul_path.read_text(encoding="utf-8")
            # Extract just the key sections (user, active projects, cross-project laws)
            soul_lines = soul_text.split("\n")
            in_section = False
            section_lines: list[str] = []
            for line in soul_lines:
                if line.startswith("## User"):
                    in_section = "user"
                    section_lines.append("### User")
                    continue
                if line.startswith("## Active Projects"):
                    in_section = "projects"
                    section_lines.append("### Active Projects")
                    continue
                if line.startswith("## Cross-Project Laws"):
                    in_section = "laws"
                    section_lines.append("### Cross-Project Laws")
                    continue
                if line.startswith("## ") and in_section:
                    in_section = False
                    continue
                if in_section and line.strip():
                    # Skip table rows, keep bullet points
                    if not line.startswith("|") and not line.startswith("|-"):
                        section_lines.append(line)
            if section_lines:
                obsidian_content.extend(section_lines[:30])  # cap SOUL contribution
                obsidian_content.append("")
        except Exception:
            pass

    # Read each project memory file
    for session_id, mem_filename in _SESSION_MEMORY_FILE.items():
        mem_path = _OBSIDIAN_MEMORIES_DIR / mem_filename
        if not mem_path.exists():
            continue
        try:
            mem_text = mem_path.read_text(encoding="utf-8")
            # Extract the last 3 turn entries (most recent activity)
            turn_sections = mem_text.split("## Turn — ")
            if len(turn_sections) > 1:
                obsidian_content.append(f"### {mem_filename.replace('.md', '').replace(' Memory', '')}")
                # Take last 3 turns
                for turn in turn_sections[-3:]:
                    turn_text = turn.strip()
                    if turn_text:
                        # Truncate each turn to 200 chars
                        if len(turn_text) > 200:
                            turn_text = turn_text[:197] + "..."
                        obsidian_content.append(turn_text)
                        obsidian_content.append("")
        except Exception:
            pass

    # Assemble final MEMORY.md
    # Preserve existing non-Obsidian sections, replace Obsidian section
    obsidian_key = "## Obsidian Project Memories (auto-synced)"
    existing_sections[obsidian_key] = "\n".join(obsidian_content)

    # Build output: non-Obsidian sections first, then Obsidian
    output_parts: list[str] = []
    for key, content in existing_sections.items():
        if key != obsidian_key:
            output_parts.append(content)
    output_parts.append(existing_sections[obsidian_key])

    final_text = "\n§\n".join(output_parts)

    # Enforce char limit (2200 chars for MEMORY.md)
    if len(final_text) > 2200:
        # Truncate Obsidian section first
        excess = len(final_text) - 2200
        obsidian_section = existing_sections[obsidian_key]
        if len(obsidian_section) > excess + 50:
            obsidian_section = obsidian_section[:-(excess + 50)] + "\n... (truncated)"
            existing_sections[obsidian_key] = obsidian_section
            output_parts = []
            for key, content in existing_sections.items():
                if key != obsidian_key:
                    output_parts.append(content)
            output_parts.append(existing_sections[obsidian_key])
            final_text = "\n§\n".join(output_parts)
        else:
            final_text = final_text[:2197] + "..."

    try:
        memory_path.write_text(final_text, encoding="utf-8")
    except Exception:
        pass  # Best-effort sync


def _sync_kanban_to_obsidian() -> None:
    """Write current Kanban state to Obsidian project dashboard.

    This is the Kanban → Obsidian bridge. Called on server startup and
    after each Hermes turn that updates a task. Keeps the Obsidian
    Project Kanban.md in sync with data/kanban-tasks.json.
    """
    tasks_data = _read_json(TASKS_FILE, {"tasks": []})
    tasks = tasks_data.get("tasks", [])

    lines = []
    lines.append("---")
    lines.append("title: Project Kanban")
    lines.append("project: Zen / Agent OS")
    lines.append(f"last_updated: {_now_iso()}")
    lines.append("---")
    lines.append("")
    lines.append("# Project Kanban")
    lines.append("")
    lines.append("Auto-synced from Agent OS Kanban. Updated on server startup and after each Hermes turn.")
    lines.append("")

    # Group by status
    status_order = ["wip", "backlog", "done", "archived"]
    status_labels = {
        "wip": "🔨 In Progress",
        "backlog": "📋 Backlog",
        "done": "✅ Done",
        "archived": "📦 Archived",
    }

    for status in status_order:
        if status == "archived":
            group = [t for t in tasks if t.get("archived")]
        else:
            group = [t for t in tasks if t["status"] == status and not t.get("archived")]
        if not group:
            continue

        lines.append(f"## {status_labels.get(status, status)}")
        lines.append("")

        for t in group:
            priority = t.get("priority", "")
            risk = t.get("risk", "")
            lines.append(f"### {t['title']}")
            lines.append(f"- **ID:** `{t['id']}`")
            lines.append(f"- **Project:** {t.get('project', '—')} | **Priority:** {priority} | **Risk:** {risk}")
            if t.get("nextAction"):
                na = t["nextAction"].replace("\n", " ")[:120]
                lines.append(f"- **Next:** {na}")
            if t.get("subtasks"):
                done = sum(1 for st in t["subtasks"] if st["status"] == "done")
                total = len(t["subtasks"])
                lines.append(f"- **Progress:** {done}/{total} subtasks")
            if t.get("activity"):
                lines.append(f"- **Recent:** {t['activity'][-1]}")
            lines.append("")

    # Summary table
    lines.append("---")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("| Task | Status | Project | Priority | Progress |")
    lines.append("|:-----|:-------|:--------|:---------|:---------|")

    for t in tasks:
        if t.get("archived"):
            continue
        st_done = sum(1 for st in t.get("subtasks", []) if st["status"] == "done")
        st_total = len(t.get("subtasks", []))
        progress = f"{st_done}/{st_total}" if st_total > 0 else "—"
        title = t["title"][:40]
        lines.append(f"| {title} | {t['status']} | {t.get('project','—')} | {t.get('priority','—')} | {progress} |")

    content = "\n".join(lines) + "\n"

    try:
        dash_path = _OBSIDIAN_VAULT / "01_Projects" / "Project Kanban.md"
        dash_path.parent.mkdir(parents=True, exist_ok=True)
        dash_path.write_text(content, encoding="utf-8")
    except Exception:
        pass  # Best-effort sync


def _curate_milestone(status: str, summary: str, evidence: list[str], files_touched: list[str] | None = None, validation: list[dict] | None = None) -> str:
    """Create a curated milestone string for Kanban activity[] from a hermes_turn event."""
    if status == "failed":
        cause = summary[:80] if summary else "Unknown failure"
        return f"Failed: {cause}"
    # Prefer filesTouched over evidence for the milestone
    ft = files_touched or []
    if ft:
        files_summary = ", ".join(ft[:2])
        return f"Files touched: {files_summary}"
    if evidence:
        ev_summary = ", ".join(str(e)[:40] for e in evidence[:2])
        return f"Completed: {ev_summary}"
    # Include validation results if available
    val = validation or []
    if val:
        passed = sum(1 for v in val if v.get("status") == "pass")
        total = len(val)
        return f"Validation: {passed}/{total} passed"
    if summary:
        return _truncate_middle(summary, 60)
    return "Completed"


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


# ---------------------------------------------------------------------------
# Session index
# ---------------------------------------------------------------------------

def _load_index() -> dict:
    data = _read_json(SESSIONS_INDEX, None)
    if data is None:
        return _rebuild_index()
    return data


def _save_index(index: dict) -> None:
    index["updatedAt"] = _now_iso()
    _write_json(SESSIONS_INDEX, index)


def _rebuild_index() -> dict:
    """Build the index from DEFAULT_SESSIONS, preserving any existing summaries."""
    existing = _read_json(SESSIONS_INDEX, {})
    existing_by_id = {s["id"]: s for s in existing.get("sessions", [])}

    sessions = []
    for defn in DEFAULT_SESSIONS:
        sid = defn["id"]
        if sid in existing_by_id:
            # Preserve existing data, override with defaults for new fields
            cur = existing_by_id[sid]
            for k, v in defn.items():
                if k not in cur:
                    cur[k] = v
            sessions.append(cur)
        else:
            now = _now_iso()
            s = dict(defn)
            s["createdAt"] = now
            s["lastActive"] = now
            sessions.append(s)

    index = {"version": 1, "sessions": sessions, "updatedAt": _now_iso()}
    _save_index(index)
    return index


def _get_session(index: dict, session_id: str) -> dict | None:
    for s in index["sessions"]:
        if s["id"] == session_id:
            return s
    return None


# ---------------------------------------------------------------------------
# Hermes DB — read-only access to Discord conversation history
# ---------------------------------------------------------------------------

# Channel title patterns — maps channel key to session title prefixes.
# Hermes resets sessions frequently (context compaction), so a single Discord
# channel's conversation history spans many Hermes sessions.  We aggregate
# all sessions whose title matches one of the channel's patterns so the
# Agent OS view shows the same full history that Discord users see.
_CHANNEL_SESSION_PATTERNS: dict[str, list[str]] = {
    "zen-chat": [
        "Zen",
        "Discord Bridge Status Confirmation",
        "Requesting Concise Alerts",
    ],
    "zen-os-chat": [
        "Zen OS",
    ],
    "1516644810834841672": [
        "Zen OS",
    ],
    "kiyosaki-chat": [
        "Kiyosaki",
        "ETHUSDT 5m Strategy Backtesting Dashboard",
    ],
    "minato-chat": [
        "Introducing OWL on Project Zen",
    ],
    "rin-chat": ["Rin"],
    "toji-chat": ["Toji"],
    "kazuki-chat": ["Kazuki"],
}


def _load_hermes_session_map() -> dict:
    """Load the Hermes session → Agent OS session mapping.

    Returns a dict of hermes_session_id → {
        agentOsSession, channel, projectId, firstSeen, lastSeen, messageCount
    }
    """
    data = _read_json(HERMES_SESSION_MAP_FILE, None)
    if data is None:
        return {}
    return data.get("sessions", {})


def _save_hermes_session_map(mapping: dict) -> None:
    """Persist the Hermes session mapping."""
    _write_json(HERMES_SESSION_MAP_FILE, {
        "version": 1,
        "updatedAt": _now_iso(),
        "sessions": mapping,
    })


def _record_hermes_session(agent_os_session: dict, hermes_session_id: str) -> None:
    """Record that a Hermes session belongs to an Agent OS session.

    Called every time we send a message to Hermes, so we always know
    which project/channel a Hermes session belongs to — no title guessing.
    """
    if not hermes_session_id:
        return
    mapping = _load_hermes_session_map()
    entry = mapping.get(hermes_session_id, {})
    now = _now_iso()
    entry.update({
        "agentOsSession": agent_os_session.get("id"),
        "channel": agent_os_session.get("channel"),
        "projectId": agent_os_session.get("projectId"),
        "lastSeen": now,
        "messageCount": (entry.get("messageCount") or 0) + 1,
    })
    if "firstSeen" not in entry:
        entry["firstSeen"] = now
    mapping[hermes_session_id] = entry
    _save_hermes_session_map(mapping)


def _rebuild_hermes_session_map() -> dict:
    """Rebuild the mapping from existing Hermes DB + Agent OS session index.

    Used on first startup to create the map from historical data.
    Uses title pattern matching as a fallback for sessions we haven't
    explicitly recorded yet.
    """
    mapping: dict = {}
    index = _load_index()

    # For each Agent OS session with a channel, find matching Hermes sessions
    for s in index.get("sessions", []):
        channel = s.get("channel")
        if not channel:
            continue
        patterns = _CHANNEL_SESSION_PATTERNS.get(channel, [])
        if not patterns or not HERMES_DB.exists():
            continue
        try:
            conn = sqlite3.connect(f"file:{HERMES_DB}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            conditions = " OR ".join(["s.title LIKE ?" for _ in patterns])
            params = [f"{p}%" for p in patterns]
            rows = conn.execute(
                f"""SELECT DISTINCT s.id, s.started_at
                    FROM sessions s
                    JOIN messages m ON m.session_id = s.id
                    WHERE s.source LIKE '%%discord%%'
                    AND ({conditions})
                    AND m.role IN ('user','assistant')
                    AND s.title IS NOT NULL
                    GROUP BY s.id
                    HAVING COUNT(m.id) > 0""",
                params,
            ).fetchall()
            conn.close()
            for r in rows:
                hsid = r["id"]
                if hsid not in mapping:
                    mapping[hsid] = {
                        "agentOsSession": s["id"],
                        "channel": channel,
                        "projectId": s.get("projectId"),
                        "firstSeen": datetime.datetime.fromtimestamp(
                            r["started_at"] or 0
                        ).isoformat() if r["started_at"] else _now_iso(),
                        "lastSeen": _now_iso(),
                        "messageCount": 0,
                        "inferredBy": "title_pattern",
                    }
        except Exception:
            pass

    _save_hermes_session_map(mapping)
    return mapping


def _channel_session_ids(channel: str) -> list[str]:
    """Return all Hermes session IDs that belong to a given Discord channel.

    Uses the explicit session mapping table as the primary source.
    Falls back to title pattern matching for any sessions not yet in the map.
    Results are merged and deduplicated, sorted by firstSeen (oldest first).
    """
    # Primary: use the explicit session mapping
    mapping = _load_hermes_session_map()
    mapped_ids = [
        hsid for hsid, entry in mapping.items()
        if entry.get("channel") == channel
    ]

    # Fallback: title pattern matching for unmapped sessions
    patterns = _CHANNEL_SESSION_PATTERNS.get(channel, [])
    pattern_ids: list[str] = []
    if patterns and HERMES_DB.exists():
        try:
            conn = sqlite3.connect(f"file:{HERMES_DB}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            conditions = " OR ".join(["s.title LIKE ?" for _ in patterns])
            params = [f"{p}%" for p in patterns]
            rows = conn.execute(
                f"""SELECT DISTINCT s.id
                    FROM sessions s
                    JOIN messages m ON m.session_id = s.id
                    WHERE s.source LIKE '%%discord%%'
                    AND ({conditions})
                    AND m.role IN ('user','assistant')
                    AND s.title IS NOT NULL
                    GROUP BY s.id
                    HAVING COUNT(m.id) > 0""",
                params,
            ).fetchall()
            conn.close()
            pattern_ids = [r["id"] for r in rows]
        except Exception:
            pass

    # Merge: mapped first (they're confirmed), then pattern-matched
    seen = set(mapped_ids)
    result = list(mapped_ids)
    for hsid in pattern_ids:
        if hsid not in seen:
            result.append(hsid)

    return result


def _hermes_messages(hermes_session_id: str, limit: int = 500, channel: str | None = None, since_ts: float | None = None) -> list[dict]:
    """Read messages from Hermes state.db.

    If a channel is provided (or can be derived from the session index),
    aggregates messages from ALL Hermes sessions belonging to that channel
    so the full Discord conversation history is visible.
    Otherwise falls back to a single session ID.

    Filters out intermediate assistant messages that are followed by tool calls,
    keeping only final assistant responses so Zen Chat doesn't show internal
    reasoning steps as separate chat messages."""
    if not HERMES_DB.exists() or not hermes_session_id:
        return []

    # Determine which sessions to aggregate
    session_ids = [hermes_session_id]
    if channel:
        channel_ids = _channel_session_ids(channel)
        if channel_ids:
            session_ids = channel_ids

    try:
        conn = sqlite3.connect(f"file:{HERMES_DB}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        # Fetch user + assistant + tool roles so we can identify which
        # assistant messages are intermediate (followed by tool calls).
        if len(session_ids) == 1:
            query = """SELECT role, content, token_count, timestamp, session_id
               FROM messages
               WHERE session_id = ? AND role IN ('user','assistant','tool')"""
            params: list = [session_ids[0]]
            if since_ts is not None:
                query += " AND timestamp >= ?"
                params.append(since_ts)
            query += " ORDER BY timestamp ASC LIMIT ?"
            params.append(limit)
            rows = conn.execute(query, params).fetchall()
        else:
            placeholders = ",".join(["?" for _ in session_ids])
            query = f"""SELECT role, content, token_count, timestamp, session_id
                FROM messages
                WHERE session_id IN ({placeholders})
                AND role IN ('user','assistant','tool')"""
            params = list(session_ids)
            if since_ts is not None:
                query += " AND timestamp >= ?"
                params.append(since_ts)
            query += " ORDER BY timestamp ASC LIMIT ?"
            params.append(limit)
            rows = conn.execute(query, params).fetchall()
        conn.close()
        # reasoning step, not a final response.
        all_msgs = [
            {
                "role": r["role"],
                "content": r["content"],
                "ts": r["timestamp"],
                "tokens": r["token_count"],
                "source": "hermes",
                "hermesSessionId": r["session_id"],
            }
            for r in rows
        ]

        # Clean message content: strip Hermes CLI boilerplate,
        # background process notifications, and other noise.
        _BOILERPLINE_PREFIXES = tuple(
            s.strip() for s in [
                "Initializing agent...",
                "Resume this session with:",
                "Session:",
                "Title:",
                "Duration:",
                "Messages:",
                "Working directory:",
            ]
        )
        _BOILERPLINE_LINES_RE = re.compile(
            r"^(?:"
            r"↻ Resumed session[^\n]*"
            r"|Working directory:[^\n]*"
            r"|Resume with:[^\n]*"
            r"|─+"
            r"|[─╭╮╰╯│].*"
            r"|\s*Sessions:\s*\d+[^\n]*"
            r"|\s+\S+\s+\|.*"
            r"|Agent OS server starting on[^\n]*"
            r"|Sessions:\s*\d+[^\n]*"
            r")\n?",
            re.MULTILINE,
        )
        for msg in all_msgs:
            content = msg.get("content", "") or ""

            # Strip [System note:...] blocks (multi-line, no closing bracket)
            content = re.sub(
                r"\[System note:[^\]]*\]\n.*?(?=\n\[IMPORTANT:|\n\[System note:|\Z)",
                "", content, flags=re.DOTALL,
            )

            # Strip [IMPORTANT: Background process...] blocks (no closing bracket)
            # These blocks have: [IMPORTANT:...], Command:, [Matched output:], Output:,
            # then output lines. The actual user message starts after the block.
            if "[IMPORTANT: Background process" in content:
                lines = content.splitlines(True)
                cleaned: list[str] = []
                in_bg = False
                bg_depth = 0
                for line in lines:
                    stripped = line.strip()
                    if line.startswith("[IMPORTANT: Background process "):
                        in_bg = True
                        bg_depth = 0
                        continue
                    if in_bg:
                        bg_depth += 1
                        if stripped.startswith(("Command:", "Matched output:", "Output:")):
                            continue
                        if stripped and (stripped[0] in '│─╭╮╰╯"]' or
                                         stripped.startswith("Sessions:") or
                                         stripped.startswith("Agent OS")):
                            continue
                        if line.startswith(("  ", "\t")):
                            continue
                        # A standalone ] or [ bracket on its own line is part
                        # of the background process output block — skip it.
                        if stripped in ("]", "[", "]["):
                            continue
                        if bg_depth > 2 and stripped and stripped[0].isalpha():
                            in_bg = False
                            cleaned.append(line)
                            continue
                        if bg_depth > 99:
                            # Safety valve: exit bg mode after many lines.
                            # Don't append — the line is likely still noise.
                            in_bg = False
                            continue
                        continue
                    cleaned.append(line)
                content = "".join(cleaned).strip()

            # Strip out-of-band user message blocks
            content = re.sub(
                r"\[OUT-OF-BAND USER MESSAGE[^\]]*\][^\[]*\[/OUT-OF-BAND USER MESSAGE\]",
                "", content, flags=re.DOTALL,
            )

            if msg["role"] == "assistant":
                # Strip boilerplate lines
                content = _BOILERPLINE_LINES_RE.sub("", content)
                # Strip lines starting with known boilerplate prefixes
                lines = content.splitlines()
                cleaned_lines: list[str] = []
                skip = True
                for line in lines:
                    stripped = line.strip()
                    if skip and any(stripped.startswith(p) for p in _BOILERPLINE_PREFIXES):
                        continue
                    if stripped:
                        skip = False
                    cleaned_lines.append(line)
                content = "\n".join(cleaned_lines).strip()

            # Collapse multiple blank lines
            while "\n\n\n" in content:
                content = content.replace("\n\n\n", "\n\n")
            msg["content"] = content.strip()

        # After cleaning, filter out messages that are now empty or meaningless.
        # These are messages that contained only noise blocks (System notes,
        # background process output) with no actual user text.
        filtered_msgs = [
            msg for msg in all_msgs
            if msg.get("content", "").strip()
        ]

        # Identify which assistant messages are final responses.
        # A "final" assistant message is one that is followed by a user message
        # (or is the very last message in the session). All other assistant
        # messages are intermediate reasoning steps between tool calls.
        # Also filter out context compaction summaries injected by Hermes.
        COMPACTION_RE = re.compile(r'^\[CONTEXT COMPACTION', re.IGNORECASE)
        skip_indices = set()
        for i, msg in enumerate(filtered_msgs):
            if msg["role"] == "tool":
                skip_indices.add(i)
                continue
            if msg["role"] == "assistant":
                # Skip context compaction summaries
                if COMPACTION_RE.search(msg.get("content", "")):
                    skip_indices.add(i)
                    continue
                # Check if this assistant message is followed by a user message
                # or is the last message — those are final responses.
                is_final = False
                if i + 1 >= len(filtered_msgs):
                    is_final = True  # last message in session
                elif filtered_msgs[i + 1]["role"] == "user":
                    is_final = True  # followed by user = final response
                if not is_final:
                    skip_indices.add(i)

        # Return only user messages and final assistant responses.
        return [msg for i, msg in enumerate(filtered_msgs) if i not in skip_indices]
    except Exception:
        return []


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
HERMES_PROGRESS_RE = re.compile(
    r"""^(
        [\U0001F300-\U0001FAFF]\s+[A-Za-z_][\w-]*(?::|\.\.\.)|
        ┊\s*\S+\s+\S+|
        (?:\S+\s+)?Compacting\ context|
        (?:\S+\s+)?Working\s*[—-]
    )""",
    re.VERBOSE,
)

HERMES_PROGRESS_WRAPPER = r"""
import json
import os
import sys

agent_dir = os.environ.get("ZEN_HERMES_AGENT_DIR")
if agent_dir:
    sys.path.insert(0, agent_dir)

import cli as hermes_chat_cli

PREFIX = os.environ.get("ZEN_HERMES_PROGRESS_PREFIX", "__ZEN_HERMES_PROGRESS__")
_orig_progress = hermes_chat_cli.HermesCLI._on_tool_progress

def _json_safe(value):
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)

def _emit(payload):
    print(PREFIX + json.dumps(payload, ensure_ascii=False, default=str), flush=True)

def _emit_line(line):
    line = str(line or "").strip()
    if line:
        _emit({"line": line})

def _agent_iteration_label(agent):
    if agent is None:
        return ""
    current = getattr(agent, "api_call_count", None)
    total = getattr(agent, "max_iterations", None)
    if current is None or total is None:
        return ""
    try:
        return f"iteration {int(current)}/{int(total)}"
    except Exception:
        return ""

def _zen_progress(self, event_type, function_name=None, preview=None, function_args=None, **kwargs):
    if event_type == "tool.started" and function_name and not str(function_name).startswith("_"):
        args = function_args if isinstance(function_args, dict) else {}
        try:
            from agent.display import build_tool_preview, get_tool_emoji
            tool_preview = preview or build_tool_preview(function_name, args, max_len=0) or ""
            emoji = get_tool_emoji(function_name, default="⚙️")
        except Exception:
            tool_preview = preview or ""
            emoji = "⚙️"
        payload = {
            "event_type": event_type,
            "tool_name": function_name,
            "preview": tool_preview,
            "emoji": emoji,
            "args": _json_safe(args),
        }
        _emit(payload)
    elif event_type == "tool.completed" and function_name and not str(function_name).startswith("_"):
        duration = kwargs.get("duration", 0)
        try:
            duration_text = f"{float(duration):.1f}s"
        except Exception:
            duration_text = str(duration or "0s")
        iteration = _agent_iteration_label(getattr(self, "agent", None))
        detail = f"{iteration}, " if iteration else ""
        _emit_line(f"⏳ Working — {detail}tool completed: {function_name} ({duration_text})")
    elif event_type == "reasoning.available" and preview:
        _emit_line(str(preview))
    return _orig_progress(self, event_type, function_name, preview, function_args, **kwargs)

hermes_chat_cli.HermesCLI._on_tool_progress = _zen_progress
_orig_init_agent = hermes_chat_cli.HermesCLI._init_agent

def _zen_init_agent(self, *args, **kwargs):
    ok = _orig_init_agent(self, *args, **kwargs)
    agent = getattr(self, "agent", None)
    if ok and agent is not None:
        previous_status_callback = getattr(agent, "status_callback", None)

        def _zen_status(event_type, message):
            if message:
                _emit_line(message)
            if previous_status_callback:
                try:
                    previous_status_callback(event_type, message)
                except Exception:
                    pass

        agent.status_callback = _zen_status
    return ok

hermes_chat_cli.HermesCLI._init_agent = _zen_init_agent

from hermes_cli import main as hermes_entrypoint

sys.argv = [
    "hermes",
    "chat",
    "-q",
    os.environ.get("ZEN_HERMES_QUERY", ""),
    "--resume",
    os.environ.get("ZEN_HERMES_SESSION", ""),
    "--source",
    "tool",
    "--cli",
]
raise SystemExit(hermes_entrypoint.main())
"""


def _strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text or "")


def _truncate_middle(text: str, limit: int = 120) -> str:
    text = str(text or "").replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) <= limit:
        return text
    head = max(1, int((limit - 3) * 0.58))
    tail = max(1, (limit - 3) - head)
    return text[:head] + "..." + text[-tail:]


def _format_hermes_progress_payload(payload: dict) -> str | None:
    direct_line = str(payload.get("line") or "").strip()
    if direct_line:
        return _truncate_middle(direct_line, 160)
    tool_name = str(payload.get("tool_name") or "").strip()
    if not tool_name:
        return None
    emoji = str(payload.get("emoji") or "⚙️").strip() or "⚙️"
    preview = _truncate_middle(str(payload.get("preview") or ""), 120)
    if preview:
        return f'{emoji} {tool_name}: "{preview}"'
    return f"{emoji} {tool_name}..."


def _decode_zen_progress_line(line: str) -> str | None:
    stripped = _strip_ansi(line).strip()
    if not stripped.startswith(ZEN_HERMES_PROGRESS_PREFIX):
        return None
    raw = stripped[len(ZEN_HERMES_PROGRESS_PREFIX):]
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return _format_hermes_progress_payload(payload if isinstance(payload, dict) else {})


def _hermes_chat_command(hermes_session_id: str, user_message: str) -> tuple[list[str], dict]:
    python_bin = str(HERMES_PYTHON if HERMES_PYTHON.exists() else Path(sys.executable))
    env = {
        **os.environ,
        "NO_COLOR": "1",
        "TERM": "dumb",
        "PYTHONUNBUFFERED": "1",
        "ZEN_HERMES_AGENT_DIR": str(HERMES_AGENT_DIR),
        "ZEN_HERMES_PROGRESS_PREFIX": ZEN_HERMES_PROGRESS_PREFIX,
        "ZEN_HERMES_SESSION": hermes_session_id,
        "ZEN_HERMES_QUERY": user_message,
    }
    return [python_bin, "-u", "-c", HERMES_PROGRESS_WRAPPER], env


def _extract_hermes_progress(text: str) -> list[str]:
    progress: list[str] = []
    for raw in (text or "").splitlines():
        decoded = _decode_zen_progress_line(raw)
        if decoded:
            progress.append(decoded)
            continue
        line = _strip_ansi(raw).strip()
        if not line:
            continue
        if line.startswith("Query:"):
            continue
        if HERMES_PROGRESS_RE.search(line):
            progress.append(line)
    return progress[-80:]


def _clean_hermes_stdout(stdout: str, progress: list[str]) -> str:
    progress_set = set(progress)
    kept: list[str] = []
    skip_prefixes = (
        "Query:",
        "session_id:",
        "Resume with:",
        "Use /resume",
    )
    for raw in (stdout or "").splitlines():
        line = _strip_ansi(raw).rstrip()
        stripped = line.strip()
        if not stripped:
            kept.append("")
            continue
        if stripped.startswith(ZEN_HERMES_PROGRESS_PREFIX):
            continue
        if stripped in progress_set:
            continue
        if any(stripped.startswith(prefix) for prefix in skip_prefixes):
            continue
        if HERMES_PROGRESS_RE.search(stripped):
            continue
        kept.append(line)
    return "\n".join(kept).strip()


def _hermes_session_count(hermes_session_id: str, channel: str | None = None) -> int:
    if not HERMES_DB.exists() or not hermes_session_id:
        return 0
    try:
        conn = sqlite3.connect(f"file:{HERMES_DB}?mode=ro", uri=True)
        if channel:
            session_ids = _channel_session_ids(channel)
            if session_ids:
                placeholders = ",".join(["?" for _ in session_ids])
                n = conn.execute(
                    f"SELECT COUNT(*) FROM messages WHERE session_id IN ({placeholders}) AND role IN ('user','assistant')",
                    session_ids,
                ).fetchone()[0]
            else:
                n = conn.execute(
                    "SELECT COUNT(*) FROM messages WHERE session_id = ? AND role IN ('user','assistant')",
                    (hermes_session_id,),
                ).fetchone()[0]
        else:
            n = conn.execute(
                "SELECT COUNT(*) FROM messages WHERE session_id = ? AND role IN ('user','assistant')",
                (hermes_session_id,),
            ).fetchone()[0]
        conn.close()
        return n
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

class Handler(SimpleHTTPRequestHandler):
    """Serve static files + Agent OS API."""

    def log_message(self, format, *args):
        pass  # silence

    # -- routing ---------------------------------------------------------

    def _match(self, pattern: str):
        path = urlparse(self.path).path
        parts = [p for p in path.split("/") if p]
        pat = [p for p in pattern.split("/") if p]
        if len(parts) != len(pat):
            return None
        params: dict[str, str] = {}
        for a, b in zip(parts, pat):
            if b.startswith(":"):
                params[b[1:]] = a
            elif a != b:
                return None
        return params  # empty dict = match with no params

    def _json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _ndjson_start(self, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

    def _ndjson_event(self, event: dict):
        self.wfile.write((json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8"))
        self.wfile.flush()

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length:
            return json.loads(self.rfile.read(length))
        return {}

    # -- GET -------------------------------------------------------------

    def do_GET(self):
        # Static files
        if not self.path.startswith("/api/"):
            super().do_GET()
            return

        index = _load_index()

        # GET /api/health
        if self._match("api/health") is not None:
            return self._json(_live_health())

        # GET /api/kanban-tasks
        if self._match("api/kanban-tasks") is not None:
            data = _read_json(TASKS_FILE, {"tasks": []})
            data["writable"] = True
            return self._json(data)

        # GET /api/sessions
        if self._match("api/sessions") is not None:
            # Enrich with live Hermes message counts
            out = []
            for s in index["sessions"]:
                sm = dict(s)
                hsid = sm.get("hermesSessionId")
                if hsid:
                    sm["hermesMessageCount"] = _hermes_session_count(
                        hsid, channel=sm.get("channel")
                    )
                out.append(sm)
            return self._json({"sessions": out, "updatedAt": index.get("updatedAt")})

        # GET /api/sessions/:id
        m = self._match("api/sessions/:id")
        if m and "messages" not in self.path and "rollup" not in self.path:
            s = _get_session(index, m["id"])
            if not s:
                return self._json({"error": "not found"}, 404)
            sm = dict(s)
            hsid = sm.get("hermesSessionId")
            if hsid:
                sm["hermesMessageCount"] = _hermes_session_count(
                    hsid, channel=sm.get("channel")
                )
            return self._json(sm)

        # GET /api/sessions/:id/messages
        m = self._match("api/sessions/:id/messages")
        if m:
            s = _get_session(index, m["id"])
            if not s:
                return self._json({"error": "not found"}, 404)
            hsid = s.get("hermesSessionId")

            # Read from Hermes DB (Discord history)
            channel = s.get("channel")
            # Use a higher limit when aggregating across channel sessions
            # since a single Discord channel's history spans many Hermes sessions
            msg_limit = 5000 if channel else 500

            # Intraday filter: only return messages from today (start of day).
            # Full history is always available via ?since=0 or ?all=1.
            import datetime as _dt
            since_ts = None
            qs = urlparse(self.path).query
            if qs:
                params = dict(p.split("=", 1) for p in qs.split("&") if "=" in p)
                if params.get("all") == "1":
                    since_ts = None  # no filter, return everything
                elif "since" in params:
                    try:
                        since_ts = float(params["since"])
                    except ValueError:
                        since_ts = None
            if since_ts is None and "all" not in (qs or ""):
                # Default: start of today (midnight local time)
                _today = _dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                since_ts = _today.timestamp()

            # Determine effective session IDs for Hermes lookup.
            # When a channel is set, always aggregate from all sessions on that
            # channel (title-pattern matching) so the full Discord history is
            # visible. The direct hermesSessionId is only used as a fallback
            # when no channel is configured.
            _hsid = hsid
            _use_channel = False
            if channel:
                _channel_ids = _channel_session_ids(channel)
                if _channel_ids:
                    _hsid = _channel_ids[0]  # primary for single-session path
                    _use_channel = True
            elif not _hsid:
                # No channel and no direct link — nothing to read
                pass
            hermes_msgs = _hermes_messages(
                _hsid or "",
                limit=msg_limit,
                channel=(channel if _use_channel else None),
                since_ts=since_ts
            ) if (_hsid or channel) else []

            # Read Agent OS local messages only for sessions that have neither
            # a Hermes link nor a channel aggregation path.
            local_path = SESSIONS_DIR / m["id"] / "messages.jsonl"
            _has_hermes_data = bool(hsid) or bool(channel)
            local_msgs = [] if _has_hermes_data else [
                {**rec, "source": rec.get("source") or "agent-os"}
                for rec in _read_jsonl(local_path)
            ]

            # Merge chronologically. For Hermes-linked sessions this is only
            # Hermes history; for unlinked sessions this is local Agent OS history.
            all_msgs = sorted(hermes_msgs + local_msgs, key=lambda rec: rec.get("ts") or "")

            return self._json({
                "sessionId": m["id"],
                "hermesSessionId": hsid,
                "messages": all_msgs,
                "hermesCount": len(hermes_msgs),
                "localCount": len(local_msgs),
                "intraday": since_ts is not None,
                "sinceTs": since_ts,
            })

        # GET /api/sessions/:id/rollup
        m = self._match("api/sessions/:id/rollup")
        if m:
            s = _get_session(index, m["id"])
            if not s:
                return self._json({"error": "not found"}, 404)
            children = [
                ch
                for ch in index["sessions"]
                if ch.get("parentSessionId") == m["id"]
            ]
            rollup = []
            for ch in children:
                hsid = ch.get("hermesSessionId")
                rollup.append({
                    "sessionId": ch["id"],
                    "label": ch["label"],
                    "summary": ch.get("summary", ""),
                    "nextSafeGate": ch.get("nextSafeGate", ""),
                    "messageCount": ch.get("messageCount", 0),
                    "hermesMessageCount": _hermes_session_count(hsid, channel=ch.get("channel")) if hsid else 0,
                    "blockers": ch.get("blockers", []),
                    "taskId": ch.get("taskId"),
                    "recentActivity": ch.get("recentActivity", []),
                    "color": ch.get("color", "#666"),
                })
            return self._json({"sessionId": m["id"], "children": rollup})

        # GET /api/events/:sessionId
        m = self._match("api/events/:sessionId")
        if m:
            events = _read_session_events(m["sessionId"])
            return self._json({"sessionId": m["sessionId"], "events": events})

        # GET /api/review/diff?sessionId=zen-os&raw=1 — read-only worktree review
        if self._match("api/review/diff") is not None:
            params = parse_qs(urlparse(self.path).query)
            session_id = (params.get("sessionId") or [""])[0]
            include_raw = (params.get("raw") or [""])[0] == "1"
            try:
                return self._json(_git_review_payload(session_id, include_raw=include_raw))
            except Exception as exc:
                return self._json({"ok": False, "error": str(exc)}, 500)

        # GET /api/memory — Hermes MEMORY.md + USER.md
        if self._match("api/memory") is not None:
            memory_md = ""
            user_md = ""
            memory_path = HERMES_DB.parent / "memories" / "MEMORY.md"
            user_path = HERMES_DB.parent / "memories" / "USER.md"
            if memory_path.exists():
                memory_md = memory_path.read_text(encoding="utf-8")
            if user_path.exists():
                user_md = user_path.read_text(encoding="utf-8")
            return self._json({"memory": memory_md, "user": user_md})

        # GET /api/memory-world — force-directed graph data
        if self._match("api/memory-world") is not None:
            mw_path = ROOT / "data" / "memory-world.json"
            mw = _read_json(mw_path, {"nodes": [], "edges": []})
            return self._json(mw)

        self._json({"error": "not found"}, 404)

    # -- PUT -------------------------------------------------------------

    def do_PUT(self):
        if not self.path.startswith("/api/"):
            return self._json({"error": "method not allowed"}, 405)

        index = _load_index()

        # PUT /api/kanban-tasks
        if self._match("api/kanban-tasks") is not None:
            body = self._read_body()
            _write_json(TASKS_FILE, body)
            return self._json({"ok": True})

        # PUT /api/sessions/:id
        m = self._match("api/sessions/:id")
        if m:
            s = _get_session(index, m["id"])
            if not s:
                return self._json({"error": "not found"}, 404)
            body = self._read_body()
            for k in ("summary", "decisions", "blockers", "filesTouched",
                      "validationProof", "nextSafeGate", "label", "taskId",
                      "parentSessionId"):
                if k in body:
                    s[k] = body[k]
            s["lastActive"] = _now_iso()
            _save_index(index)
            return self._json({"ok": True, "session": s})

        self._json({"error": "not found"}, 404)

    # -- POST ------------------------------------------------------------

    def do_POST(self):
        if not self.path.startswith("/api/"):
            return self._json({"error": "method not allowed"}, 405)

        index = _load_index()

        # POST /api/sessions/:id/review-decision — record gate decisions only
        m = self._match("api/sessions/:id/review-decision")
        if m:
            s = _get_session(index, m["id"])
            if not s:
                return self._json({"error": "not found"}, 404)
            body = self._read_body()
            decision = str(body.get("decision") or "").strip().lower()
            if decision not in {"accept", "alter", "review", "cancel-github", "confirm-github"}:
                return self._json({"error": "invalid decision"}, 400)
            now = _now_iso()
            summary = str(body.get("summary") or "").strip()
            event = {
                "ts": now,
                "type": "review_decision",
                "sessionId": m["id"],
                "taskId": s.get("taskId"),
                "status": "complete" if decision == "accept" else "review",
                "decision": decision,
                "summary": summary or f"Review gate decision: {decision}",
                "evidence": body.get("evidence") or [],
                "validation": body.get("validation") or [],
                "filesTouched": body.get("filesTouched") or [],
                "source": "agent-os",
            }
            _append_session_event(m["id"], event)
            if decision == "alter":
                _update_session_recent_activity(m["id"], "Alter requested at review gate; returned to chat with no GitHub action.")
            elif decision == "accept":
                _update_session_recent_activity(m["id"], "Completion accepted; GitHub update plan shown as disabled review step.")
            elif decision == "review":
                _update_session_recent_activity(m["id"], "Full backend/frontend review opened from completion gate.")
            return self._json({"ok": True, "event": event})

        # POST /api/sessions/:id/messages/stream
        m = self._match("api/sessions/:id/messages/stream")
        if m:
            s = _get_session(index, m["id"])
            if not s:
                return self._json({"error": "not found"}, 404)
            body = self._read_body()
            role = body.get("role", "user")
            content = body.get("content", "")
            if not content:
                return self._json({"error": "empty content"}, 400)

            now = _now_iso()
            record = {"role": role, "content": content, "ts": now, "source": "agent-os"}
            msg_path = SESSIONS_DIR / m["id"] / "messages.jsonl"
            if not s.get("hermesSessionId"):
                _append_jsonl(msg_path, record)

            s["lastActive"] = now
            s["messageCount"] = (s.get("messageCount") or 0) + 1
            _save_index(index)

            self._ndjson_start()
            self._ndjson_event({"event": "accepted", "message": record})
            assistant_record = None
            try:
                # Determine which Hermes session to use for the chat stream.
                # Prefer the most recent session on the channel (so new messages
                # go to the active session, not a stale one). Fall back to the
                # direct hermesSessionId only when no channel aggregation exists.
                _chat_hsid = None
                if s.get("channel"):
                    _channel_ids = _channel_session_ids(s["channel"])
                    if _channel_ids:
                        _chat_hsid = _channel_ids[-1]  # most recent session on channel
                if not _chat_hsid:
                    _chat_hsid = s.get("hermesSessionId")
                if role == "user" and _chat_hsid:
                    # Record which Agent OS session this Hermes session belongs to
                    _record_hermes_session(s, _chat_hsid)
                    assistant_record = self._hermes_chat_stream(
                        _chat_hsid,
                        content,
                        lambda payload: self._ndjson_event(payload),
                    )
                self._ndjson_event({
                    "event": "complete",
                    "ok": True,
                    "message": record,
                    "assistant": assistant_record,
                    "progress": (assistant_record or {}).get("progress", []),
                })
            except Exception as e:
                self._ndjson_event({
                    "event": "failed",
                    "ok": False,
                    "error": str(e),
                })
            return

        # POST /api/sessions/:id/messages
        m = self._match("api/sessions/:id/messages")
        if m:
            s = _get_session(index, m["id"])
            if not s:
                return self._json({"error": "not found"}, 404)
            body = self._read_body()
            role = body.get("role", "user")
            content = body.get("content", "")
            if not content:
                return self._json({"error": "empty content"}, 400)

            now = _now_iso()
            record = {"role": role, "content": content, "ts": now, "source": "agent-os"}

            msg_path = SESSIONS_DIR / m["id"] / "messages.jsonl"

            # Hermes-linked sessions write through Hermes only. Unlinked sessions
            # remain local so future Minato/Kazuki setup can still chat before a
            # Discord/Hermes session exists.
            if not s.get("hermesSessionId"):
                _append_jsonl(msg_path, record)

            # Update index
            s["lastActive"] = now
            s["messageCount"] = (s.get("messageCount") or 0) + 1
            _save_index(index)

            # If this is a user message, forward to Hermes via `hermes chat`
            # so the agent processes it with full context (memory, tools, skills)
            # and the response goes to Discord automatically via the gateway.
            # Prefer the most recent session on the channel; fall back to the
            # direct hermesSessionId only when no channel aggregation exists.
            assistant_record = None
            _chat_hsid = None
            if s.get("channel"):
                _channel_ids = _channel_session_ids(s["channel"])
                if _channel_ids:
                    _chat_hsid = _channel_ids[-1]
            if not _chat_hsid:
                _chat_hsid = s.get("hermesSessionId")
            if role == "user" and _chat_hsid:
                # Record which Agent OS session this Hermes session belongs to
                _record_hermes_session(s, _chat_hsid)
                # Build compact context block from linked task + session state
                context_prefix = _build_context_prefix(s)
                hermes_content = content
                if context_prefix:
                    hermes_content = context_prefix + "\n\n" + content
                assistant_record = self._hermes_chat(_chat_hsid, hermes_content)
                if assistant_record and not s.get("hermesSessionId"):
                    _append_jsonl(msg_path, assistant_record)
                    s["messageCount"] = (s.get("messageCount") or 0) + 1
                    _save_index(index)

                # PATCH 2: Write lifecycle events + hermes_turn + Kanban update
                sid = m["id"]
                # thinking_started event (written when user sends, before Hermes responds)
                _append_session_event(sid, {
                    "ts": now,
                    "type": "thinking_started",
                    "sessionId": sid,
                    "taskId": s.get("taskId"),
                    "status": "active",
                    "summary": f"Received: {content[:80]}",
                    "evidence": [],
                    "validation": [],
                    "filesTouched": [],
                    "source": "agent-os",
                })
                # running_started event (written when Hermes begins processing)
                _append_session_event(sid, {
                    "ts": _now_iso(),
                    "type": "running_started",
                    "sessionId": sid,
                    "taskId": s.get("taskId"),
                    "status": "active",
                    "summary": "Hermes processing request.",
                    "evidence": [],
                    "validation": [],
                    "filesTouched": [],
                    "source": "agent-os",
                })
                # hermes_turn event (written when Hermes responds)
                _write_hermes_turn_event(sid, s, record, assistant_record, context_prefix=context_prefix)

            return self._json({
                "ok": True,
                "message": record,
                "assistant": assistant_record,
                "progress": (assistant_record or {}).get("progress", []),
            })

        self._json({"error": "not found"}, 404)

    def _hermes_chat(self, hermes_session_id: str, user_message: str) -> dict | None:
        """Send a message to Hermes via `hermes chat` CLI.

        Resumes the existing Hermes session so the agent has full context
        (memory, tools, skills, conversation history). The response is
        automatically delivered to Discord by the Hermes gateway.

        Returns the assistant response record, or None on failure.
        """
        import subprocess

        cmd, env = _hermes_chat_command(hermes_session_id, user_message)
        before_messages = _hermes_messages(hermes_session_id, limit=2000)
        before_len = len(before_messages)

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 min max — same as gateway timeout
                env=env,
            )
            progress = _extract_hermes_progress((result.stdout or "") + "\n" + (result.stderr or ""))
            if result.returncode == 0:
                after_messages = _hermes_messages(hermes_session_id, limit=2000)
                new_messages = after_messages[before_len:] if len(after_messages) >= before_len else []
                new_assistant = [m for m in new_messages if m.get("role") == "assistant"]
                # Extract the response text from stdout as a fallback. The
                # Hermes DB is preferred because non-quiet single-query mode
                # can include progress and exit-summary lines in stdout.
                stdout = _clean_hermes_stdout(result.stdout.strip(), progress)
                response_text = stdout

                # Try to strip the session_id prefix line from quiet-style output
                lines = stdout.split("\n")
                # Skip lines like "session_id: ..." and blank lines
                content_lines = []
                past_header = False
                for line in lines:
                    if past_header:
                        content_lines.append(line)
                    elif line.strip() == "":
                        past_header = True
                    elif line.startswith("session_id:"):
                        continue
                    else:
                        content_lines.append(line)
                        past_header = True

                response_text = "\n".join(content_lines).strip() if content_lines else stdout
                if new_assistant:
                    response_text = str(new_assistant[-1].get("content") or "").strip() or response_text

                if response_text:
                    return {
                        "role": "assistant",
                        "content": response_text,
                        "ts": _now_iso(),
                        "source": "hermes",
                        "progress": progress,
                    }
                return {
                    "role": "assistant",
                    "content": "",
                    "ts": _now_iso(),
                    "source": "hermes",
                    "progress": progress,
                }
            else:
                err = result.stderr.strip() or "hermes chat returned non-zero"
                return {
                    "role": "assistant",
                    "content": f"[Hermes error: {err}]",
                    "ts": _now_iso(),
                    "source": "hermes-error",
                    "progress": progress,
                }
        except subprocess.TimeoutExpired:
            return {
                "role": "assistant",
                "content": "[Hermes timeout — no response in 5 minutes]",
                "ts": _now_iso(),
                "source": "hermes-error",
            }
        except FileNotFoundError:
            return {
                "role": "assistant",
                "content": "[Hermes CLI not found — install with: pip install hermes-agent]",
                "ts": _now_iso(),
                "source": "hermes-error",
            }
        except Exception as e:
            return {
                "role": "assistant",
                "content": f"[Hermes error: {e}]",
                "ts": _now_iso(),
                "source": "hermes-error",
            }

    def _hermes_chat_stream(self, hermes_session_id: str, user_message: str, emit) -> dict | None:
        """Run Hermes and emit progress events as CLI progress lines arrive."""
        import subprocess

        cmd, env = _hermes_chat_command(hermes_session_id, user_message)
        before_messages = _hermes_messages(hermes_session_id, limit=2000)
        before_len = len(before_messages)
        progress: list[str] = []
        output_lines: list[str] = []

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            )
            assert proc.stdout is not None
            for raw in proc.stdout:
                output_lines.append(raw)
                lines = _extract_hermes_progress(raw)
                for line in lines:
                    progress.append(line)
                    emit({"event": "progress", "line": line})
            return_code = proc.wait(timeout=5)
            stdout = "".join(output_lines)
            if return_code != 0:
                cleaned = _clean_hermes_stdout(stdout, progress)
                return {
                    "role": "assistant",
                    "content": f"[Hermes error: {cleaned or 'hermes chat returned non-zero'}]",
                    "ts": _now_iso(),
                    "source": "hermes-error",
                    "progress": progress[-80:],
                }

            after_messages = _hermes_messages(hermes_session_id, limit=2000)
            new_messages = after_messages[before_len:] if len(after_messages) >= before_len else []
            new_assistant = [m for m in new_messages if m.get("role") == "assistant"]
            response_text = _clean_hermes_stdout(stdout, progress)
            if new_assistant:
                db_content = str(new_assistant[-1].get("content") or "").strip()
                if db_content:
                    response_text = _clean_hermes_stdout(db_content, progress) or response_text
            return {
                "role": "assistant",
                "content": response_text,
                "ts": _now_iso(),
                "source": "hermes",
                "progress": progress[-80:],
            }
        except subprocess.TimeoutExpired:
            try:
                proc.kill()  # type: ignore[name-defined]
            except Exception:
                pass
            return {
                "role": "assistant",
                "content": "[Hermes timeout — no response in 5 minutes]",
                "ts": _now_iso(),
                "source": "hermes-error",
                "progress": progress[-80:],
            }
        except FileNotFoundError:
            return {
                "role": "assistant",
                "content": "[Hermes CLI not found — install with: pip install hermes-agent]",
                "ts": _now_iso(),
                "source": "hermes-error",
                "progress": progress[-80:],
            }
        except Exception as e:
            return {
                "role": "assistant",
                "content": f"[Hermes error: {e}]",
                "ts": _now_iso(),
                "source": "hermes-error",
                "progress": progress[-80:],
            }

    # -- OPTIONS (CORS) --------------------------------------------------

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Agent OS dev server")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    # Seed sessions on first run
    idx = _load_index()
    print(f"Agent OS server starting on {args.host}:{args.port}")
    print(f"Sessions: {len(idx['sessions'])}")
    for s in idx["sessions"]:
        hsid = s.get("hermesSessionId") or "—"
        print(f"  {s['id']:12s} | hermes: {hsid:30s} | channel: {s.get('channel','—')}")

    # Rebuild Hermes session → Agent OS session mapping
    # This ensures every Hermes session is linked to its project/channel
    # without relying solely on fragile title pattern matching
    hsm = _rebuild_hermes_session_map()
    mapped = sum(1 for e in hsm.values() if e.get("inferredBy") != "title_pattern")
    inferred = sum(1 for e in hsm.values() if e.get("inferredBy") == "title_pattern")
    print(f"Hermes session map: {len(hsm)} sessions ({mapped} explicit, {inferred} inferred from titles)")

    # Sync Obsidian project memories → MEMORY.md (Gap C/E fix)
    # Compacts rich Obsidian knowledge into Hermes-consumable format
    _sync_obsidian_to_memory_md()
    print("Obsidian → MEMORY.md sync complete")

    # Sync Kanban → Obsidian dashboard
    _sync_kanban_to_obsidian()
    print("Kanban → Obsidian sync complete")

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.server_close()


if __name__ == "__main__":
    main()
