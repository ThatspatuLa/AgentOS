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
  GET      /api/sessions/:id/rollup       -> child task summaries
  GET      /api/memory                    -> Hermes MEMORY.md + USER.md
  GET      /api/memory-world              -> data/memory-world.json

Data layout:
  data/sessions/index.json                   -> session registry + hermesSessionId links
  data/sessions/<sessionId>/summary.json     -> decisions, blockers, files, validation
  data/sessions/<sessionId>/messages.jsonl   -> Agent OS chat messages (supplemental)
  data/sessions/<sessionId>/events.jsonl     -> reserved
  data/memory-world.json                     -> force-directed graph data
  ~/.hermes/memories/MEMORY.md               -> project rules, conventions, roadmap
  ~/.hermes/memories/USER.md                 -> user profile and preferences
  data/sessions/<sessionId>/messages.jsonl   -> Agent OS chat messages (supplemental)
  data/sessions/<sessionId>/events.jsonl     -> reserved
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
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
TASKS_FILE = ROOT / "data" / "kanban-tasks.json"
SESSIONS_DIR = ROOT / "data" / "sessions"
SESSIONS_INDEX = SESSIONS_DIR / "index.json"

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
        "hermesSessionId": None,  # will be set to the active zen-chat Hermes session
        "channel": "zen-chat",
        "color": "#7c3aed",
        "summary": "Agent OS · UI & Frontend · Unified dashboard view",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M4 — Hermes webhook integration",
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
        "hermesSessionId": "20260607_193056_683caea5",
        "channel": "zen-chat",
        "color": "#6366f1",
        "summary": "Project Zen — intelligence engine, monitoring, monetization",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M4 — Zen intelligence engine scaffold",
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
        "hermesSessionId": "20260607_193646_a685023b",
        "channel": "kiyosaki-chat",
        "color": "#059669",
        "summary": "Kiyosaki — quantitative trading system",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M3 — ETHUSDT strategy backtest complete",
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
        "hermesSessionId": None,
        "channel": "minato-chat",
        "color": "#d97706",
        "summary": "Minato — devops & automation",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M1 — project scoping",
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
        "hermesSessionId": "20260608_042151_b4f5cf15",
        "channel": "rin-chat",
        "color": "#dc2626",
        "summary": "Rin — targeting & execution systems",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M1 — project scoping",
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
        "hermesSessionId": "20260608_042419_3a98b4c7",
        "channel": "toji-chat",
        "color": "#2563eb",
        "summary": "Toji — rapid execution & infrastructure",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M1 — project scoping",
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
        "hermesSessionId": None,
        "channel": "kazuki-chat",
        "color": "#7c2d12",
        "summary": "Kazuki — security & governance",
        "decisions": [],
        "blockers": [],
        "filesTouched": [],
        "validationProof": [],
        "nextSafeGate": "M1 — project scoping",
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


def _append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


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


def _channel_session_ids(channel: str) -> list[str]:
    """Return all Hermes session IDs that belong to a given Discord channel.

    Uses title matching against known patterns because Hermes creates a new
    session on each compaction/reset rather than reusing one session per
    channel."""
    patterns = _CHANNEL_SESSION_PATTERNS.get(channel, [])
    if not patterns or not HERMES_DB.exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{HERMES_DB}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        # Build WHERE clause for title patterns
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
        return [r["id"] for r in rows]
    except Exception:
        return []


def _hermes_messages(hermes_session_id: str, limit: int = 500, channel: str | None = None) -> list[dict]:
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
            rows = conn.execute(
                """SELECT role, content, token_count, timestamp, session_id
                   FROM messages
                   WHERE session_id = ? AND role IN ('user','assistant','tool')
                   ORDER BY timestamp ASC
                   LIMIT ?""",
                (session_ids[0], limit),
            ).fetchall()
        else:
            placeholders = ",".join(["?" for _ in session_ids])
            rows = conn.execute(
                f"""SELECT role, content, token_count, timestamp, session_id
                    FROM messages
                    WHERE session_id IN ({placeholders})
                    AND role IN ('user','assistant','tool')
                    ORDER BY timestamp ASC
                    LIMIT ?""",
                (*session_ids, limit),
            ).fetchall()
        conn.close()

        # Build list and identify which assistant messages are intermediate.
        # An assistant message followed by a tool message is an internal
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

        # Identify which assistant messages are final responses.
        # A "final" assistant message is one that is followed by a user message
        # (or is the very last message in the session). All other assistant
        # messages are intermediate reasoning steps between tool calls.
        # Also filter out context compaction summaries injected by Hermes.
        COMPACTION_RE = re.compile(r'^\[CONTEXT COMPACTION', re.IGNORECASE)
        skip_indices = set()
        for i, msg in enumerate(all_msgs):
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
                if i + 1 >= len(all_msgs):
                    is_final = True  # last message in session
                elif all_msgs[i + 1]["role"] == "user":
                    is_final = True  # followed by user = final response
                if not is_final:
                    skip_indices.add(i)

        # Return only user messages and final assistant responses.
        return [msg for i, msg in enumerate(all_msgs) if i not in skip_indices]
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
            hermes_msgs = _hermes_messages(hsid, limit=msg_limit, channel=channel) if hsid else []

            # Read Agent OS local messages only for sessions that do not have
            # a Hermes link. Linked sessions use Hermes state.db as the single
            # source of truth so Discord, Hermes Chat, and Agent OS stay aligned
            # without local duplicate artifacts.
            local_path = SESSIONS_DIR / m["id"] / "messages.jsonl"
            local_msgs = [] if hsid else [
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
                if ch.get("projectId") and ch["id"] != "zen-os"
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
                    "color": ch.get("color", "#666"),
                })
            return self._json({"sessionId": m["id"], "children": rollup})

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
                      "validationProof", "nextSafeGate", "label"):
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
                if role == "user" and s.get("hermesSessionId"):
                    assistant_record = self._hermes_chat_stream(
                        s["hermesSessionId"],
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

            # If this is a user message and the session has a Hermes link,
            # forward to Hermes via `hermes chat` so the agent processes it
            # with full context (memory, tools, skills) and the response
            # goes to Discord automatically via the gateway.
            assistant_record = None
            if role == "user" and s.get("hermesSessionId"):
                assistant_record = self._hermes_chat(s["hermesSessionId"], content)
                if assistant_record and not s.get("hermesSessionId"):
                    _append_jsonl(msg_path, assistant_record)
                    s["messageCount"] = (s.get("messageCount") or 0) + 1
                    _save_index(index)

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
                response_text = str(new_assistant[-1].get("content") or "").strip() or response_text
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

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.server_close()


if __name__ == "__main__":
    main()
