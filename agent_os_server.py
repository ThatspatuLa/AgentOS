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
  GET      /api/operator/inspect          -> local/Tailscale read-only project inspection
  GET      /api/operator/screenshot       -> local/Tailscale authenticated screenshot capture
  GET      /api/operator/observe/status   -> local/Tailscale authenticated noVNC readiness
  GET      /api/operator/status           -> local/Tailscale authenticated operator status
  GET/POST  /api/operator/browser/*       -> local/Tailscale browser observation/control
  GET/POST  /api/operator/brain/*         -> local/Tailscale visible-browser brain loop
  POST     /api/operator/desktop-intent    -> local/Tailscale natural desktop/browser action bridge
  GET      /api/operator/approvals        -> local/Tailscale pending approval queue
  GET      /api/operator/audit            -> local/Tailscale operator audit/proof rows
  POST     /api/operator/pair             -> local/Tailscale one-time phone pairing
  POST     /api/operator/run              -> local/Tailscale classified operator command runner
  POST     /api/operator/approval         -> local/Tailscale approval state update
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
import base64
import datetime
import hmac
import hashlib
import ipaddress
import json
import os
import re
import secrets
import shlex
import socket
import sqlite3
import shutil
import subprocess
import sys
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import parse_qs, quote, urlparse

from intelligence_engine.operator.browser import BrowserOperator

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
OPERATOR_PROJECT_REGISTRY = {
    "zen-new": ROOT,
    "zen": Path("/home/spatula/Projects/Zen"),
    "kiyosaki": Path("/home/spatula/Projects/Kiyosaki"),
}
OPERATOR_DATA_DIR = ROOT / "data" / "operator"
OPERATOR_TOKEN_FILE = OPERATOR_DATA_DIR / ".token"
OPERATOR_PAIR_FILE = OPERATOR_DATA_DIR / ".pairing-code"
OPERATOR_TOTP_FILE = OPERATOR_DATA_DIR / ".totp-secret"
OPERATOR_AUDIT_FILE = OPERATOR_DATA_DIR / "audit.jsonl"
OPERATOR_PENDING_FILE = OPERATOR_DATA_DIR / "pending-approvals.json"
OPERATOR_BROWSER_TASKS_FILE = OPERATOR_DATA_DIR / "browser-tasks.json"
OPERATOR_ARTIFACT_DIR = OPERATOR_DATA_DIR / "artifacts"
OPERATOR_SCREENSHOT_DIR = OPERATOR_ARTIFACT_DIR / "screenshots"
OPERATOR_BROWSER_DIR = OPERATOR_ARTIFACT_DIR / "browser"
OPERATOR_CAPTURE_HELPER = ROOT / "scripts" / "operator_capture_helper.py"
OPERATOR_CAPTURE_SERVICE_URL = os.environ.get("OPERATOR_CAPTURE_SERVICE_URL", "http://127.0.0.1:8771")
OPERATOR_VISION_MODEL = os.environ.get("OPERATOR_VISION_MODEL", "").strip()
OPERATOR_BRAIN_USE_HERMES = os.environ.get("OPERATOR_BRAIN_USE_HERMES", "").strip() == "1"
OPERATOR_OLLAMA_URL = os.environ.get("OPERATOR_OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
OPERATOR_VNC_DISPLAY = os.environ.get("OPERATOR_VNC_DISPLAY", ":2")
OPERATOR_VNC_CHROME_PROFILE = OPERATOR_ARTIFACT_DIR / "vnc-chrome-profile"
OPERATOR_VNC_PORT = int(os.environ.get("OPERATOR_VNC_PORT", "5902"))
OPERATOR_NOVNC_PORT = int(os.environ.get("OPERATOR_NOVNC_PORT", "6080"))
OPERATOR_RDP_PORT = int(os.environ.get("OPERATOR_RDP_PORT", "3389"))
OPERATOR_TAILSCALE_IP = os.environ.get("OPERATOR_TAILSCALE_IP", "").strip()
if not OPERATOR_TAILSCALE_IP and shutil.which("tailscale"):
    try:
        OPERATOR_TAILSCALE_IP = subprocess.run(
            ["tailscale", "ip", "-4"],
            capture_output=True,
            text=True,
            timeout=2,
        ).stdout.strip()
    except Exception:
        OPERATOR_TAILSCALE_IP = ""
OPERATOR_TOKEN_TTL_SECONDS = 24 * 60 * 60
OPERATOR_TAILSCALE_NET = ipaddress.ip_network("100.64.0.0/10")
OPERATOR_SAFE_COMMANDS = {"ls", "cat", "grep", "rg", "ps", "git", "find", "wc", "head", "tail", "pwd"}
OPERATOR_REVIEW_COMMANDS = {"python", "python3", "pip", "npm", "systemctl", "service", "xdg-open", "google-chrome", "playwright", "touch", "mkdir", "cp", "mv", "tee"}
OPERATOR_BLOCKED_COMMANDS = {"sudo", "su", "rm", "shred", "chmod", "chown", "ssh", "scp", "curl", "wget", "nc", "ncat", "env", "printenv", "export"}
OPERATOR_SECRET_MARKERS = (".env", ".token", "secret", "secrets", "credential", "credentials", "private_key", "id_rsa", "github_token", "api_key")
OPERATOR_BROWSER_TASK_THREADS: dict[str, threading.Thread] = {}
OPERATOR_TREE_SKIP = {
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "node_modules",
    ".venv",
    "venv",
}
OPERATOR_BROWSER = BrowserOperator(OPERATOR_BROWSER_DIR)

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
        "channel": "1519228976344727674",
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


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_json(payload: dict) -> str:
    return _b64url(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))


def _operator_token_record() -> dict:
    OPERATOR_DATA_DIR.mkdir(parents=True, exist_ok=True)
    now = int(time.time())
    record = _read_json(OPERATOR_TOKEN_FILE, {}) if OPERATOR_TOKEN_FILE.exists() else {}
    if not record.get("secret") or int(record.get("expiresAt") or 0) <= now:
        secret = secrets.token_urlsafe(32)
        payload = {"iat": now, "exp": now + OPERATOR_TOKEN_TTL_SECONDS, "scope": "operator"}
        payload_part = _b64url_json(payload)
        sig = _b64url(hmac.new(secret.encode("utf-8"), payload_part.encode("ascii"), hashlib.sha256).digest())
        record = {
            "issuedAt": payload["iat"],
            "expiresAt": payload["exp"],
            "secret": secret,
            "token": f"op.{payload_part}.{sig}",
        }
        with NamedTemporaryFile("w", encoding="utf-8", dir=str(OPERATOR_DATA_DIR), delete=False) as tmp:
            json.dump(record, tmp, ensure_ascii=False, indent=2)
            tmp.write("\n")
            tmp_path = Path(tmp.name)
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, OPERATOR_TOKEN_FILE)
        os.chmod(OPERATOR_TOKEN_FILE, 0o600)
    return record


def _operator_pair_record() -> dict:
    OPERATOR_DATA_DIR.mkdir(parents=True, exist_ok=True)
    now = int(time.time())
    record = _read_json(OPERATOR_PAIR_FILE, {}) if OPERATOR_PAIR_FILE.exists() else {}
    if not record.get("code") or int(record.get("expiresAt") or 0) <= now:
        record = {
            "code": f"{secrets.randbelow(1000000):06d}",
            "issuedAt": now,
            "expiresAt": now + 10 * 60,
        }
        with NamedTemporaryFile("w", encoding="utf-8", dir=str(OPERATOR_DATA_DIR), delete=False) as tmp:
            json.dump(record, tmp, ensure_ascii=False, indent=2)
            tmp.write("\n")
            tmp_path = Path(tmp.name)
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, OPERATOR_PAIR_FILE)
        os.chmod(OPERATOR_PAIR_FILE, 0o600)
    return record


def _operator_totp_record() -> dict:
    OPERATOR_DATA_DIR.mkdir(parents=True, exist_ok=True)
    record = _read_json(OPERATOR_TOTP_FILE, {}) if OPERATOR_TOTP_FILE.exists() else {}
    if not record.get("secret"):
        secret = base64.b32encode(secrets.token_bytes(20)).decode("ascii").rstrip("=")
        record = {
            "secret": secret,
            "issuer": "Agent OS",
            "account": f"operator@{os.uname().nodename if hasattr(os, 'uname') else 'local'}",
            "createdAt": _now_iso(),
            "digits": 6,
            "period": 30,
            "algorithm": "SHA1",
        }
        with NamedTemporaryFile("w", encoding="utf-8", dir=str(OPERATOR_DATA_DIR), delete=False) as tmp:
            json.dump(record, tmp, ensure_ascii=False, indent=2)
            tmp.write("\n")
            tmp_path = Path(tmp.name)
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, OPERATOR_TOTP_FILE)
        os.chmod(OPERATOR_TOTP_FILE, 0o600)
    return record


def _operator_totp_uri(record: dict | None = None) -> str:
    record = record or _operator_totp_record()
    issuer = str(record.get("issuer") or "Agent OS")
    account = str(record.get("account") or "operator")
    label = quote(f"{issuer}:{account}", safe="")
    params = (
        f"secret={quote(str(record.get('secret') or ''), safe='')}"
        f"&issuer={quote(issuer, safe='')}"
        "&algorithm=SHA1&digits=6&period=30"
    )
    return f"otpauth://totp/{label}?{params}"


def _operator_totp_code(secret: str, timestep: int | None = None) -> str:
    clean = str(secret or "").replace(" ", "").upper()
    padded = clean + ("=" * ((8 - len(clean) % 8) % 8))
    key = base64.b32decode(padded, casefold=True)
    counter = int(time.time() // 30) if timestep is None else int(timestep)
    digest = hmac.new(key, counter.to_bytes(8, "big"), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    value = int.from_bytes(digest[offset:offset + 4], "big") & 0x7FFFFFFF
    return f"{value % 1000000:06d}"


def _operator_totp_valid(code: str, record: dict | None = None) -> bool:
    value = str(code or "").strip().replace(" ", "")
    if not re.fullmatch(r"\d{6}", value):
        return False
    record = record or _operator_totp_record()
    secret = str(record.get("secret") or "")
    if not secret:
        return False
    current_step = int(time.time() // 30)
    for drift in (-1, 0, 1):
        if hmac.compare_digest(value, _operator_totp_code(secret, current_step + drift)):
            return True
    return False


def _operator_pair_payload(code: str) -> tuple[dict, int]:
    if _operator_totp_valid(code):
        token = _operator_token_record()
        return {
            "ok": True,
            "method": "totp",
            "token": token.get("token"),
            "tokenExpiresAt": datetime.datetime.fromtimestamp(int(token.get("expiresAt") or 0), datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        }, 200
    record = _operator_pair_record()
    now = int(time.time())
    if int(record.get("expiresAt") or 0) <= now:
        return {"ok": False, "error": "pairing code expired"}, 410
    if not hmac.compare_digest(str(code or "").strip(), str(record.get("code") or "")):
        return {"ok": False, "error": "invalid authenticator or backup pairing code"}, 401
    token = _operator_token_record()
    # Rotate code immediately after successful pairing.
    try:
        OPERATOR_PAIR_FILE.unlink(missing_ok=True)
    except OSError:
        pass
    return {
        "ok": True,
        "method": "backup-code",
        "token": token.get("token"),
        "tokenExpiresAt": datetime.datetime.fromtimestamp(int(token.get("expiresAt") or 0), datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    }, 200


def _operator_token_valid(token: str) -> bool:
    record = _operator_token_record()
    expected = str(record.get("token") or "")
    if not token or not expected or not hmac.compare_digest(token, expected):
        return False
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != "op":
        return False
    secret = str(record.get("secret") or "")
    sig = _b64url(hmac.new(secret.encode("utf-8"), parts[1].encode("ascii"), hashlib.sha256).digest())
    if not hmac.compare_digest(sig, parts[2]):
        return False
    return int(record.get("expiresAt") or 0) > int(time.time())


def _operator_remote_allowed(remote: str) -> bool:
    if remote in {"127.0.0.1", "::1"}:
        return True
    try:
        return ipaddress.ip_address(remote) in OPERATOR_TAILSCALE_NET
    except ValueError:
        return False


def _operator_audit(action_type: str, target: str, result: str, **extra) -> None:
    record = {
        "ts": _now_iso(),
        "action_type": action_type,
        "target": target,
        "result": result,
        "approver": extra.get("approver"),
        "session_id": extra.get("session_id"),
    }
    for key in ("project", "remote", "status", "reason", "action_class", "backend", "path", "bytes", "error", "helperMode", "source", "url", "title", "mime", "command", "exitCode", "presentation", "taskId", "kind"):
        if key in extra and extra[key] is not None:
            record[key] = extra[key]
    _append_jsonl(OPERATOR_AUDIT_FILE, record)


def _operator_logs(session_id: str = "", limit: int = 60) -> list[dict]:
    rows = _read_jsonl(OPERATOR_AUDIT_FILE)
    if session_id:
        rows = [row for row in rows if str(row.get("session_id") or "") == session_id]
    return rows[-limit:]


def _operator_proof_payload(session_id: str = "") -> dict:
    logs = _operator_logs(session_id)
    return {
        "operatorLogs": logs,
        "commandTranscript": [
            row for row in logs
            if row.get("action_type") in {"run", "inspect", "desktop"}
        ],
        "fileAccess": [
            row for row in logs
            if row.get("action_type") == "inspect" or row.get("target") in {"operator/inspect"}
        ],
        "networkCalls": [
            {
                "ts": row.get("ts"),
                "remote": row.get("remote"),
                "target": row.get("target"),
                "result": row.get("result"),
                "status": row.get("status"),
            }
            for row in logs
            if row.get("remote")
        ],
        "screenshots": [
            row for row in logs
            if row.get("action_type") == "screenshot" or row.get("target") == "operator/browser/screenshot"
        ],
    }


def _operator_capture_service_payload() -> tuple[dict | None, int | None]:
    url = OPERATOR_CAPTURE_SERVICE_URL.rstrip("/") + "/capture"
    req = urllib_request.Request(url, data=b"{}", method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib_request.urlopen(req, timeout=8) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            payload = json.loads(raw or "{}")
            payload.setdefault("source", "capture-service")
            return payload, resp.status
    except urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw or "{}")
        except json.JSONDecodeError:
            payload = {"ok": False, "error": raw.strip()[:500] or str(exc)}
        payload.setdefault("source", "capture-service")
        return payload, exc.code
    except (urllib_error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        return None, None


def _operator_screenshot_payload() -> tuple[dict, int]:
    if not OPERATOR_CAPTURE_HELPER.exists():
        return {
            "ok": False,
            "backend": "",
            "path": "",
            "error": "desktop capture helper is missing",
            "reason": str(OPERATOR_CAPTURE_HELPER),
            "attempts": [],
        }, 501
    day_dir = OPERATOR_SCREENSHOT_DIR / datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
    service_payload, service_status = _operator_capture_service_payload()
    if service_payload is not None:
        service_payload.setdefault("helperMode", "service")
        if not service_payload.get("ok"):
            service_payload.setdefault("path", "")
            service_payload.setdefault("backend", "")
            service_payload.setdefault("error", "screenshot unavailable")
            service_payload.setdefault("reason", "Capture service did not produce a PNG artifact")
            return service_payload, service_status or 503
        payload = service_payload
    else:
        payload = None
    if payload is None:
        helper_mode = "one-shot"
        try:
            proc = subprocess.run(
                [sys.executable, str(OPERATOR_CAPTURE_HELPER), "--out-dir", str(day_dir)],
                capture_output=True,
                text=True,
                timeout=25,
            )
        except subprocess.TimeoutExpired:
            return {
                "ok": False,
                "backend": "desktop-capture-helper",
                "path": "",
                "error": "desktop capture helper timed out",
                "reason": "Helper exceeded outer Agent OS timeout",
                "helperMode": helper_mode,
                "attempts": [],
            }, 504

        try:
            payload = json.loads(proc.stdout or "{}")
            payload.setdefault("helperMode", helper_mode)
        except json.JSONDecodeError:
            return {
                "ok": False,
                "backend": "desktop-capture-helper",
                "path": "",
                "error": "desktop capture helper returned invalid JSON",
                "reason": (proc.stderr or proc.stdout or "").strip()[:500],
                "helperMode": helper_mode,
                "attempts": [],
            }, 500

        if not payload.get("ok"):
            payload.setdefault("path", "")
            payload.setdefault("backend", "")
            payload.setdefault("error", (proc.stderr or "screenshot unavailable").strip()[:500])
            payload.setdefault("reason", "No screenshot artifact produced")
            return payload, 503

    payload.setdefault("helperMode", "service")
    path = Path(str(payload.get("path") or ""))
    try:
        resolved = path.resolve(strict=True)
        screenshot_root = OPERATOR_SCREENSHOT_DIR.resolve(strict=False)
        if not str(resolved).startswith(str(screenshot_root) + os.sep):
            return {
                "ok": False,
                "backend": payload.get("backend") or "desktop-capture-helper",
                "path": str(path),
                "error": "desktop capture helper returned an unsafe artifact path",
                "reason": "Artifact path escaped operator screenshot directory",
                "attempts": payload.get("attempts") or [],
            }, 500
        raw = resolved.read_bytes()
    except OSError as exc:
        return {
            "ok": False,
            "backend": payload.get("backend") or "desktop-capture-helper",
            "path": str(path),
            "error": str(exc),
            "reason": "Screenshot artifact could not be read",
            "attempts": payload.get("attempts") or [],
        }, 500

    payload["mime"] = "image/png"
    payload["bytes"] = len(raw)
    payload["dataUrl"] = "data:image/png;base64," + base64.b64encode(raw).decode("ascii")
    return payload, 200


def _operator_browser_payload(action: str, body: dict | None = None) -> tuple[dict, int]:
    body = body or {}
    try:
        if action == "status":
            return OPERATOR_BROWSER.status(), 200
        if action == "open":
            return OPERATOR_BROWSER.open(str(body.get("url") or "")), 200
        if action == "screenshot":
            return OPERATOR_BROWSER.screenshot(), 200
        if action == "close":
            return OPERATOR_BROWSER.close(), 200
        if action in {"click", "type"}:
            detail = body.get("selector") if action == "click" else body.get("text")
            return {
                "ok": False,
                "action_class": "review",
                "reason": f"browser {action} requires review before live control is enabled",
                "action": action,
                "detail": str(detail or "")[:300],
            }, 202
        return {"ok": False, "error": "unknown browser action"}, 404
    except ValueError as exc:
        return {"ok": False, "error": str(exc), "action_class": "blocked"}, 400
    except Exception as exc:
        return {"ok": False, "error": str(exc), "action_class": "failed"}, 500


OPERATOR_SITE_ALIASES = {
    "woolworths": "https://www.woolworths.com.au",
    "woolies": "https://www.woolworths.com.au",
    "coles": "https://www.coles.com.au",
    "google": "https://www.google.com",
}


def _operator_url_from_text(text: str) -> str:
    raw = str(text or "").strip()
    lower = raw.lower()
    direct = re.search(r"https?://[^\s\"'<>]+", raw, flags=re.I)
    if direct:
        return direct.group(0).rstrip(".,)")
    domain = re.search(r"\b(?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)+(?:/[^\s\"'<>]*)?", lower, flags=re.I)
    if domain:
        value = domain.group(0).rstrip(".,)")
        return value if value.startswith(("http://", "https://")) else f"https://{value}"
    for name, url in OPERATOR_SITE_ALIASES.items():
        if name in lower:
            return url
    if "firefox" in lower and any(word in lower for word in ("open", "launch", "start")):
        return "about:blank"
    return ""


def _operator_desktop_url_safe(url: str) -> tuple[bool, str]:
    if not url or any(ch.isspace() for ch in url):
        return False, "empty or invalid URL"
    parsed = urlparse(url)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return True, ""
    if url == "about:blank":
        return True, ""
    return False, "only http(s) URLs and about:blank are allowed"


def _operator_desktop_env() -> dict:
    env = {
        "HOME": str(Path.home()),
        "LANG": os.environ.get("LANG", "en_US.UTF-8"),
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin:/snap/bin"),
        "XDG_RUNTIME_DIR": f"/run/user/{os.getuid()}",
        "DBUS_SESSION_BUS_ADDRESS": f"unix:path=/run/user/{os.getuid()}/bus",
        "DISPLAY": ":0",
        "WAYLAND_DISPLAY": "wayland-0",
        "XAUTHORITY": f"/run/user/{os.getuid()}/.mutter-Xwaylandauth.LWY4Q3",
    }
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "show-environment"],
            capture_output=True,
            text=True,
            timeout=2,
        )
        for line in (proc.stdout or "").splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key in {"HOME", "LANG", "PATH", "XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS", "DISPLAY", "WAYLAND_DISPLAY", "XAUTHORITY", "XDG_CURRENT_DESKTOP", "XDG_SESSION_TYPE"}:
                env[key] = value
    except Exception:
        pass
    return env


def _operator_desktop_presentation(text: str) -> str:
    lower = str(text or "").lower()
    if any(term in lower for term in ("full screen", "fullscreen", "full-screen", "kiosk")):
        return "fullscreen"
    if any(term in lower for term in ("split screen", "split-screen", "side by side", "left side", "right side", "left and right")):
        return "split-requested"
    return "window"


def _operator_complex_browser_task(text: str) -> bool:
    lower = str(text or "").lower()
    task_markers = (
        "add ", "cart", "basket", "checkout", "buy ", "order ",
        "search", "find", "ingredients", "recipe", "lasagna", "meal",
        "choose", "select", "quantity", "cheap", "cheapest",
    )
    return any(marker in lower for marker in task_markers)


def _operator_browser_task_plan(text: str, url: str) -> list[dict]:
    lower = str(text or "").lower()
    if any(term in lower for term in ("lasagna", "lasagne")):
        ingredients = [
            "budget lasagne sheets",
            "budget beef mince or lentils",
            "pasta sauce or passata",
            "ricotta or cottage cheese",
            "shredded mozzarella or tasty cheese",
            "onion",
            "garlic",
        ]
        return [
            {"id": "open-store", "title": "Open Woolworths in a visible task window", "status": "done", "risk": "safe"},
            {"id": "choose-list", "title": "Use a cheap lasagna ingredient list", "status": "planned", "risk": "safe", "items": ingredients},
            {"id": "search-items", "title": "Search Woolworths for each ingredient and prefer budget/private-label options", "status": "planned", "risk": "review"},
            {"id": "add-cart", "title": "Add selected ingredients to cart", "status": "needs_approval", "risk": "cart_change"},
            {"id": "review-cart", "title": "Show cart total and substitutions before checkout", "status": "blocked_until_cart_done", "risk": "review"},
            {"id": "stop-payment", "title": "Stop before checkout/payment", "status": "blocked", "risk": "blocked"},
        ]
    return [
        {"id": "open-site", "title": f"Open {url} in a visible task window", "status": "done", "risk": "safe"},
        {"id": "inspect-page", "title": "Inspect page and identify required browser actions", "status": "planned", "risk": "safe"},
        {"id": "review-actions", "title": "Request approval before clicks, typing, cart changes, or purchases", "status": "needs_approval", "risk": "review"},
        {"id": "execute-reviewed", "title": "Execute approved browser actions with proof screenshots", "status": "blocked_until_approval", "risk": "review"},
    ]


def _operator_browser_task_summary(plan: list[dict]) -> str:
    lines = []
    for index, step in enumerate(plan, start=1):
        title = str(step.get("title") or "").strip()
        status = str(step.get("status") or "planned").replace("_", " ")
        lines.append(f"{index}. {title} [{status}]")
        items = step.get("items")
        if isinstance(items, list) and items:
            lines.append("   Items: " + ", ".join(str(item) for item in items[:10]))
    return "\n".join(lines)


def _operator_create_browser_task(goal: str, url: str, presentation: str, open_payload: dict) -> tuple[dict, dict]:
    now = int(time.time())
    task_id = "bt-" + secrets.token_hex(5)
    plan = _operator_browser_task_plan(goal, url)
    task = {
        "id": task_id,
        "ts": _now_iso(),
        "updatedAt": _now_iso(),
        "expiresAt": now + 60 * 60,
        "status": "needs_approval",
        "goal": goal,
        "url": url,
        "presentation": presentation,
        "plan": plan,
        "riskNotes": [
            "Cart-changing browser actions require approval.",
            "Checkout/payment remains blocked.",
            "Each approved click/type action should produce proof before/after.",
        ],
        "evidence": [
            {
                "type": "desktop-open",
                "url": url,
                "command": open_payload.get("command"),
                "exitCode": open_payload.get("exitCode"),
                "ts": _now_iso(),
            }
        ],
    }
    store = _read_json(OPERATOR_BROWSER_TASKS_FILE, {"items": []})
    items = []
    for item in store.get("items", []):
        if int(item.get("expiresAt") or 0) <= now:
            continue
        if item.get("status") in {"complete", "cancelled"}:
            continue
        if item.get("url") == url and str(item.get("goal") or "").strip().lower() == goal.strip().lower():
            item["status"] = "superseded"
            item["updatedAt"] = _now_iso()
            item["supersededBy"] = task_id
        items.append(item)
    items.append(task)
    _write_json(OPERATOR_BROWSER_TASKS_FILE, {"items": items})
    pending = _read_json(OPERATOR_PENDING_FILE, {"items": []})
    pending_items = []
    for item in pending.get("items", []):
        if int(item.get("expiresAt") or 0) <= now or item.get("status") != "pending":
            pending_items.append(item)
            continue
        if item.get("kind") == "browser-task" and item.get("url") == url:
            item["status"] = "superseded"
            item["resolvedAt"] = _now_iso()
            item["reason"] = f"{item.get('reason') or 'Browser task approval'} Superseded by {task_id}."
        pending_items.append(item)
    _write_json(OPERATOR_PENDING_FILE, {"items": pending_items})
    approval = _operator_pending_approval(
        "zen-new",
        f"browser-task:{task_id}:approve-cart-changing-steps",
        "Approve supervised browser/cart actions for this task. Checkout/payment stays blocked.",
        session_id="mobile-operator",
        extra={"kind": "browser-task", "taskId": task_id, "url": url},
        ttl_seconds=60 * 60,
    )
    return task, approval


def _operator_browser_tasks_payload(include_all: bool = False) -> dict:
    now = int(time.time())
    store = _read_json(OPERATOR_BROWSER_TASKS_FILE, {"items": []})
    items = list(store.get("items") or [])
    if not include_all:
        items = [
            item for item in items
            if int(item.get("expiresAt") or 0) > now and item.get("status") not in {"complete", "cancelled", "superseded"}
        ]
    return {"ok": True, "items": items[-20:]}


def _operator_update_browser_task(task_id: str, status: str, note: str = "") -> dict | None:
    if not task_id:
        return None
    store = _read_json(OPERATOR_BROWSER_TASKS_FILE, {"items": []})
    items = list(store.get("items") or [])
    found = None
    for item in items:
        if item.get("id") != task_id:
            continue
        item["status"] = status
        item["updatedAt"] = _now_iso()
        if note:
            item.setdefault("activity", []).append({
                "ts": _now_iso(),
                "type": "operator",
                "summary": note,
            })
        found = item
        break
    if found:
        _write_json(OPERATOR_BROWSER_TASKS_FILE, {"items": items})
    return found


def _operator_browser_task_item(task_id: str) -> dict | None:
    if not task_id:
        return None
    store = _read_json(OPERATOR_BROWSER_TASKS_FILE, {"items": []})
    for item in store.get("items") or []:
        if item.get("id") == task_id:
            return item
    return None


def _operator_browser_task_mutate(task_id: str, mutator) -> dict | None:
    store = _read_json(OPERATOR_BROWSER_TASKS_FILE, {"items": []})
    items = list(store.get("items") or [])
    found = None
    for item in items:
        if item.get("id") != task_id:
            continue
        mutator(item)
        item["updatedAt"] = _now_iso()
        found = item
        break
    if found:
        _write_json(OPERATOR_BROWSER_TASKS_FILE, {"items": items})
    return found


def _operator_task_activity(task_id: str, summary: str, kind: str = "brain", **extra) -> dict | None:
    def mutate(item: dict) -> None:
        row = {"ts": _now_iso(), "type": kind, "summary": summary}
        row.update({k: v for k, v in extra.items() if v not in (None, "")})
        activity = list(item.get("activity") or [])
        activity.append(row)
        item["activity"] = activity[-80:]

    return _operator_browser_task_mutate(task_id, mutate)


def _operator_task_evidence(task_id: str, evidence: dict) -> dict | None:
    def mutate(item: dict) -> None:
        row = {"ts": _now_iso(), **evidence}
        rows = list(item.get("evidence") or [])
        rows.append(row)
        item["evidence"] = rows[-80:]

    return _operator_browser_task_mutate(task_id, mutate)


def _operator_task_plan_status(task_id: str, step_id: str, status: str) -> dict | None:
    def mutate(item: dict) -> None:
        for step in item.get("plan") or []:
            if step.get("id") == step_id:
                step["status"] = status

    return _operator_browser_task_mutate(task_id, mutate)


def _operator_brain_memory(task: dict | None) -> dict:
    return dict((task or {}).get("brain") or {})


def _operator_brain_memory_update(task_id: str, **updates) -> dict | None:
    def mutate(item: dict) -> None:
        brain = dict(item.get("brain") or {})
        for key, value in updates.items():
            if value is not None:
                brain[key] = value
        item["brain"] = brain

    return _operator_browser_task_mutate(task_id, mutate)


def _operator_brain_memory_append(task_id: str, key: str, value: dict, cap: int = 40) -> dict | None:
    def mutate(item: dict) -> None:
        brain = dict(item.get("brain") or {})
        rows = list(brain.get(key) or [])
        rows.append(value)
        brain[key] = rows[-cap:]
        item["brain"] = brain

    return _operator_browser_task_mutate(task_id, mutate)


def _operator_brain_progress(task_id: str, stage: str, summary: str, state: str = "running", **extra) -> dict | None:
    now = _now_iso()

    def mutate(item: dict) -> None:
        brain = dict(item.get("brain") or {})
        current = {"stage": stage, "summary": summary, "state": state, "ts": now}
        current.update({k: v for k, v in extra.items() if v not in (None, "")})
        brain["currentAction"] = current
        progress = list(brain.get("progress") or [])
        progress.append(current)
        brain["progress"] = progress[-80:]
        item["brain"] = brain

    _operator_task_activity(task_id, summary, stage, state=state, **extra)
    return _operator_browser_task_mutate(task_id, mutate)


def _operator_vnc_xauthority() -> str:
    configured = os.environ.get("XAUTHORITY") or ""
    if configured and Path(configured).exists():
        return configured
    runtime = Path(f"/run/user/{os.getuid()}")
    for candidate in sorted(runtime.glob(".mutter-Xwaylandauth.*")):
        if candidate.exists():
            return str(candidate)
    home_auth = Path.home() / ".Xauthority"
    return str(home_auth) if home_auth.exists() else ""


def _operator_vnc_env() -> dict:
    env = {
        **os.environ,
        "DISPLAY": OPERATOR_VNC_DISPLAY,
        "XDG_SESSION_TYPE": "x11",
        "NO_AT_BRIDGE": "1",
    }
    env.pop("WAYLAND_DISPLAY", None)
    xauthority = _operator_vnc_xauthority()
    if xauthority:
        env["XAUTHORITY"] = xauthority
    return env


def _operator_brain_cdp_json(path: str, timeout: float = 0.5) -> dict | None:
    try:
        with urllib_request.urlopen(f"http://127.0.0.1:9222{path}", timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        return None


def _operator_visible_browser_ready() -> bool:
    return bool(_operator_brain_cdp_json("/json/version"))


def _operator_chrome_binary() -> str:
    for name in ("google-chrome", "google-chrome-stable", "chromium", "chromium-browser"):
        path = shutil.which(name)
        if path:
            return path
    return ""


def _operator_visible_browser_open_url(url: str, presentation: str = "window") -> tuple[dict, int]:
    safe, reason = _operator_desktop_url_safe(url)
    if not safe:
        return {"ok": False, "handled": True, "action_class": "blocked", "reason": reason, "url": url}, 400
    chrome = _operator_chrome_binary()
    if not chrome:
        return {"ok": False, "handled": True, "action_class": "failed", "reason": "google-chrome/chromium not found", "url": url}, 501

    OPERATOR_VNC_CHROME_PROFILE.mkdir(parents=True, exist_ok=True)
    cmd = [
        chrome,
        f"--user-data-dir={OPERATOR_VNC_CHROME_PROFILE}",
        "--remote-debugging-address=127.0.0.1",
        "--remote-debugging-port=9222",
        "--no-first-run",
        "--disable-extensions",
        "--disable-gpu",
        "--no-sandbox",
        "--ozone-platform=x11",
        "--window-position=10,10",
        "--window-size=1220,980",
    ]
    if presentation == "fullscreen":
        cmd.append("--start-fullscreen")
    cmd.extend(["--new-window", url])
    if _operator_visible_browser_ready():
        action = _operator_brain_apply_action({"type": "open_url", "url": url}, approved=True)
        return {
            "ok": bool(action.get("ok")),
            "handled": True,
            "action_class": "safe" if action.get("ok") else "failed",
            "action": "open-url",
            "url": url,
            "presentation": presentation,
            "command": "cdp: Page.goto",
            "exitCode": 0 if action.get("ok") else 1,
            "message": f"Opened {url} in the visible Operator browser.",
            "brain": action,
        }, 200 if action.get("ok") else 500
    try:
        subprocess.Popen(
            cmd,
            env=_operator_vnc_env(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception as exc:
        return {"ok": False, "handled": True, "action_class": "failed", "reason": str(exc), "url": url, "command": " ".join(cmd)}, 500
    deadline = time.time() + 8
    while time.time() < deadline:
        if _operator_visible_browser_ready():
            return {
                "ok": True,
                "handled": True,
                "action_class": "safe",
                "action": "open-url",
                "url": url,
                "presentation": presentation,
                "command": " ".join(shlex.quote(part) for part in cmd),
                "exitCode": 0,
                "message": f"Opened {url} in the visible Operator browser.",
            }, 200
        time.sleep(0.25)
    return {
        "ok": False,
        "handled": True,
        "action_class": "failed",
        "reason": "visible Operator browser did not expose remote debugging in time",
        "url": url,
        "command": " ".join(shlex.quote(part) for part in cmd),
        "exitCode": 1,
    }, 500


def _operator_brain_with_page(callback) -> dict:
    if not _operator_visible_browser_ready():
        return {"ok": False, "error": "visible Operator browser is not running", "reason": "open a browser task first"}
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return {"ok": False, "error": "playwright is not installed", "reason": str(exc)}
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=5000)
            try:
                context = browser.contexts[0] if browser.contexts else browser.new_context()
                page = None
                for candidate in context.pages:
                    if not candidate.url.startswith("devtools://"):
                        page = candidate
                        break
                page = page or context.new_page()
                return callback(page)
            finally:
                browser.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc), "reason": "visible browser control failed"}


def _operator_control_label(item: dict) -> str:
    return str(item.get("text") or item.get("placeholder") or item.get("ariaLabel") or item.get("href") or item.get("tag") or "").strip()


def _operator_brain_visual_heuristics(dom: dict) -> dict:
    controls = list(dom.get("controls") or [])
    viewport = dom.get("viewport") or {}
    width = int(viewport.get("width") or 1)
    height = int(viewport.get("height") or 1)

    def region(item: dict) -> str:
        x = int(item.get("x") or 0)
        y = int(item.get("y") or 0)
        vertical = "top" if y < height * 0.28 else "bottom" if y > height * 0.72 else "middle"
        horizontal = "left" if x < width * 0.33 else "right" if x > width * 0.66 else "center"
        return f"{vertical}-{horizontal}"

    scroll_y = int(dom.get("scrollY") or 0)
    scroll_height = int(dom.get("scrollHeight") or height)
    search_controls = []
    add_controls = []
    modal_controls = []
    for item in controls:
        label = _operator_control_label(item).lower()
        tag = str(item.get("tag") or "")
        kind = " ".join(str(item.get(key) or "") for key in ("type", "role", "placeholder", "ariaLabel")).lower()
        if tag in {"input", "textarea"} and ("search" in label or "search" in kind or not search_controls):
            search_controls.append({**item, "region": region(item), "label": _operator_control_label(item)})
        is_header = int(item.get("y") or 0) < 90
        is_cart = re.search(r"\b(view cart|your cart|\$\d|cart has|checkout)\b", label)
        if re.search(r"\b(add|add to cart|add to trolley|buy)\b", label) and not is_cart and not is_header:
            add_controls.append({**item, "region": region(item), "label": _operator_control_label(item)})
        if re.search(r"\b(accept|allow|close|not now|skip|later|postcode|suburb|sign in)\b", label):
            modal_controls.append({**item, "region": region(item), "label": _operator_control_label(item)})

    hints = []
    if search_controls:
        first = search_controls[0]
        hints.append(f"Likely search/input control near {first.get('region')}: {first.get('label') or first.get('placeholder') or 'input'}")
    if add_controls:
        hints.append(f"{len(add_controls)} visible Add/cart control(s), first near {add_controls[0].get('region')}.")
    if modal_controls:
        labels = ", ".join((_operator_control_label(item) or item.get("tag") or "control")[:40] for item in modal_controls[:4])
        hints.append(f"Possible modal/banner controls visible: {labels}.")
    if not hints:
        hints.append("No obvious search/add/modal controls detected from geometry.")

    return {
        "source": "geometry-dom",
        "summary": " ".join(hints),
        "scroll": {"y": scroll_y, "height": scroll_height, "viewport": height},
        "searchControls": search_controls[:8],
        "addControls": add_controls[:12],
        "modalControls": modal_controls[:8],
    }


def _operator_brain_vision_summary(image_path: str, dom: dict) -> dict:
    heuristic = _operator_brain_visual_heuristics(dom)
    if not OPERATOR_VISION_MODEL or not image_path:
        return heuristic
    path = Path(image_path)
    try:
        raw = path.read_bytes()
    except OSError:
        return heuristic
    prompt = (
        "Describe the visible browser screen for a computer-control agent. "
        "Focus on search bars, buttons, modals, product cards, cart indicators, and where the next safe click likely is. "
        "Do not invent hidden information. Keep it under 120 words."
    )
    body = json.dumps({
        "model": OPERATOR_VISION_MODEL,
        "prompt": prompt,
        "images": [base64.b64encode(raw).decode("ascii")],
        "stream": False,
    }).encode("utf-8")
    req = urllib_request.Request(
        OPERATOR_OLLAMA_URL + "/api/generate",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="replace"))
        text = str(payload.get("response") or "").strip()
        if text:
            heuristic["source"] = f"ollama:{OPERATOR_VISION_MODEL}"
            heuristic["modelSummary"] = text[:1200]
            heuristic["summary"] = text[:1200]
    except Exception as exc:
        heuristic["visionError"] = str(exc)[:300]
    return heuristic


def _operator_brain_observe(task_id: str = "") -> dict:
    def capture(page) -> dict:
        OPERATOR_BROWSER_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = OPERATOR_BROWSER_DIR / f"brain-{task_id or 'snapshot'}-{stamp}.png"
        try:
            page.wait_for_load_state("domcontentloaded", timeout=2500)
        except Exception:
            pass
        screenshot_error = ""
        try:
            raw = page.screenshot(path=str(path), full_page=False)
        except Exception as exc:
            raw = b""
            screenshot_error = str(exc)
        dom = page.evaluate(
            """() => {
                const visible = (el) => {
                  const rect = el.getBoundingClientRect();
                  const style = window.getComputedStyle(el);
                  return rect.width > 4 && rect.height > 4 && style.visibility !== 'hidden' && style.display !== 'none';
                };
                const compact = (value) => String(value || '').replace(/\\s+/g, ' ').trim().slice(0, 140);
                const collectControls = () => {
                  const roots = [document];
                  const controls = [];
                  for (let i = 0; i < roots.length; i += 1) {
                    const root = roots[i];
                    for (const el of Array.from(root.querySelectorAll('*'))) {
                      if (el.shadowRoot) roots.push(el.shadowRoot);
                      if (el.matches('input, textarea, button, a, [role="button"], [aria-label], select')) controls.push(el);
                    }
                  }
                  return controls;
                };
                const controls = collectControls()
                  .filter(visible).map((el, index) => {
                    const rect = el.getBoundingClientRect();
                    return {
                      index,
                      tag: el.tagName.toLowerCase(),
                      role: el.getAttribute('role') || '',
                      type: el.getAttribute('type') || '',
                      text: compact(el.innerText || el.textContent || el.value || el.getAttribute('aria-label') || el.getAttribute('placeholder') || el.getAttribute('title')),
                      placeholder: compact(el.getAttribute('placeholder')),
                      ariaLabel: compact(el.getAttribute('aria-label')),
                      href: compact(el.getAttribute('href')),
                      x: Math.round(rect.x),
                      y: Math.round(rect.y),
                      w: Math.round(rect.width),
                      h: Math.round(rect.height),
                      shadow: el.getRootNode && el.getRootNode() instanceof ShadowRoot
                    };
                  })
                  .sort((a, b) => (a.y - b.y) || (a.x - b.x))
                  .slice(0, 220);
                const bodyText = document.body ? document.body.innerText : '';
                const text = compact(bodyText);
                const cartText = (bodyText.match(/Your Cart has[\\s\\S]{0,140}/i) || [''])[0].replace(/\\s+/g, ' ').trim();
                const cartCountMatch = cartText.match(/Your Cart has\\s+(\\d+)/i);
                const cartValueMatch = cartText.match(/\\$\\s*([0-9]+(?:\\.[0-9]{2})?)/);
                return {
                  controls,
                  text,
                  cart: {
                    text: cartText,
                    count: cartCountMatch ? Number(cartCountMatch[1]) : null,
                    value: cartValueMatch ? cartValueMatch[1] : ''
                  },
                  viewport: {width: window.innerWidth, height: window.innerHeight},
                  scrollY: Math.round(window.scrollY || document.documentElement.scrollTop || 0),
                  scrollHeight: Math.round(document.documentElement.scrollHeight || document.body.scrollHeight || 0)
                };
            }"""
        )
        observation = {
            "ok": True,
            "url": page.url,
            "title": page.title(),
            "capturedAt": _now_iso(),
            "screenshot": {
                "ok": bool(raw),
                "path": str(path) if raw else "",
                "bytes": len(raw),
                "error": screenshot_error,
            },
            "dom": dom,
        }
        observation["visual"] = _operator_brain_vision_summary(str(path) if raw else "", dom)
        if raw and task_id:
            _operator_task_evidence(task_id, {
                "type": "brain-screenshot",
                "path": str(path),
                "bytes": len(raw),
                "url": page.url,
                "title": page.title(),
                "visualSummary": (observation.get("visual") or {}).get("summary"),
            })
        return observation

    payload = _operator_brain_with_page(capture)
    if task_id and payload.get("ok"):
        visual = payload.get("visual") or {}
        _operator_brain_progress(
            task_id,
            "observe",
            f"Observed {payload.get('title') or payload.get('url')}. {visual.get('summary') or ''}".strip(),
            "done",
            path=(payload.get("screenshot") or {}).get("path"),
        )
        _operator_brain_memory_update(
            task_id,
            lastObservation={
                "ts": _now_iso(),
                "url": payload.get("url"),
                "title": payload.get("title"),
                "visual": visual.get("summary"),
            },
        )
    return payload


def _operator_brain_observation_text(observation: dict) -> str:
    dom = observation.get("dom") or {}
    controls = dom.get("controls") or []
    control_lines = []
    for item in controls[:30]:
        label = item.get("text") or item.get("placeholder") or item.get("ariaLabel") or item.get("href") or item.get("tag")
        control_lines.append(
            f"- #{item.get('index')} {item.get('tag')} {item.get('type') or item.get('role')}: {label} "
            f"at ({item.get('x')},{item.get('y')}) {item.get('w')}x{item.get('h')}"
        )
    return "\n".join([
        f"URL: {observation.get('url')}",
        f"Title: {observation.get('title')}",
        f"Visual summary: {((observation.get('visual') or {}).get('summary') or '')[:900]}",
        f"Visible text: {(dom.get('text') or '')[:600]}",
        "Visible controls:",
        *control_lines,
    ])[:5000]


def _operator_json_from_text(text: str) -> dict | None:
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", text or ""):
        try:
            value, _ = decoder.raw_decode(text[match.start():])
        except Exception:
            continue
        if isinstance(value, dict):
            return value
    return None


def _operator_brain_goal_terms(goal: str) -> list[str]:
    lower = goal.lower()
    if any(term in lower for term in ("lasagna", "lasagne")):
        return ["lasagne sheets", "pasta sauce", "canned lentils", "cottage cheese", "shredded mozzarella", "brown onion", "garlic"]
    quoted = re.findall(r"['\"]([^'\"]{2,80})['\"]", goal)
    if quoted:
        return quoted[:12]
    return []


def _operator_brain_fallback_plan(goal: str, observation: dict, task: dict | None = None) -> dict:
    url = str(observation.get("url") or "")
    lower_goal = goal.lower()
    if "woolworths" in lower_goal or "woolies" in lower_goal:
        terms = _operator_brain_goal_terms(goal)
        done_terms = set((task or {}).get("brain", {}).get("completedTerms") or [])
        current = next((term for term in terms if term not in done_terms), "")
        if terms and not current:
            return {
                "thought": "All planned ingredient searches have been handled; the cart needs user review before checkout.",
                "action": {"type": "done", "reason": "ready_for_cart_review"},
                "safety": "review",
            }
        query_text = " ".join(parse_qs(urlparse(url).query).get("searchTerm") or [""]).lower()
        if current and current.lower() not in query_text:
            return {
                "thought": f"I need to search Woolworths for {current}.",
                "action": {"type": "open_url", "url": f"https://www.woolworths.com.au/shop/search/products?searchTerm={quote(current)}"},
                "safety": "safe",
            }
        if current:
            controls = ((observation.get("dom") or {}).get("controls") or [])
            dom = observation.get("dom") or {}
            scroll_y = int(dom.get("scrollY") or 0)
            label_for = lambda c: str(c.get("text") or c.get("ariaLabel") or c.get("placeholder") or "")
            add = next((
                c for c in controls
                if re.search(r"\badd\b", label_for(c), re.I)
                and not re.search(r"\b(view cart|your cart|\$\d|checkout|cart has)\b", label_for(c), re.I)
                and int(c.get("y") or 0) >= 90
            ), None)
            if add:
                return {
                    "thought": f"I found an Add control for {current}.",
                    "action": {"type": "click", "target": add.get("text") or add.get("ariaLabel") or "Add", "markTermComplete": current},
                    "safety": "cart_change",
                }
            if scroll_y > 1400:
                return {
                    "thought": f"I overscrolled past the {current} product results; I need to return to the top of the result list.",
                    "action": {"type": "press", "key": "Home"},
                    "safety": "safe",
                }
            return {
                "thought": f"I am on the search page for {current}; I need a visible Add button.",
                "action": {"type": "scroll", "direction": "down"},
                "safety": "safe",
            }
    if not url or url == "about:blank":
        target = _operator_url_from_text(goal) or "https://www.google.com"
        return {"thought": f"I need to open {target}.", "action": {"type": "open_url", "url": target}, "safety": "safe"}
    return {"thought": "I need user review before choosing the next general action.", "action": {"type": "ask_user", "question": "I can see the page, but need a clearer next instruction before acting."}, "safety": "review"}


def _operator_zen_hermes_session_id() -> str:
    try:
        index = _load_index()
        session = _get_session(index, "zen")
        if not session:
            return ""
        if session.get("channel"):
            ids = _channel_session_ids(str(session.get("channel")))
            if ids:
                return ids[-1]
        return str(session.get("hermesSessionId") or "")
    except Exception:
        return ""


def _operator_brain_plan_with_hermes(goal: str, observation: dict, task: dict | None = None) -> dict:
    if not OPERATOR_BRAIN_USE_HERMES:
        plan = _operator_brain_fallback_plan(goal, observation, task)
        plan["source"] = "local-planner"
        return plan
    hsid = _operator_zen_hermes_session_id()
    if not hsid:
        return _operator_brain_fallback_plan(goal, observation, task)
    prompt = (
        "You are the Agent OS Local Operator Brain. Choose the next browser action from the current visible browser state.\n"
        "Return JSON only, no markdown. Schema:\n"
        '{"thought":"short reason","safety":"safe|cart_change|review|blocked","action":{"type":"open_url|fill|click|press|scroll|wait|ask_user|done","target":"optional visible label","text":"optional text","url":"optional url","direction":"down|up","question":"optional"}}\n'
        "Allowed actions: open_url, fill, click, press, scroll, wait, ask_user, done.\n"
        "Never checkout, pay, place an order, enter payment details, or handle passwords. Use ask_user for login/payment/checkout.\n"
        "Prefer DOM labels and visible controls over coordinates. If a cart-changing click is needed, set safety to cart_change.\n\n"
        f"User goal:\n{goal}\n\n"
        f"Task memory:\n{json.dumps((task or {}).get('brain') or {}, ensure_ascii=False)[:1200]}\n\n"
        f"Current observation:\n{_operator_brain_observation_text(observation)}"
    )
    try:
        cmd, env = _hermes_chat_command(hsid, prompt)
        proc = subprocess.run(cmd, env=env, cwd=ROOT, capture_output=True, text=True, timeout=12)
        output = _clean_hermes_stdout(proc.stdout or "", _extract_hermes_progress(proc.stdout or ""))
        parsed = _operator_json_from_text(output) or _operator_json_from_text(proc.stdout or "")
        if isinstance(parsed, dict) and isinstance(parsed.get("action"), dict):
            parsed.setdefault("thought", "Hermes chose the next visible browser action.")
            parsed.setdefault("safety", "review")
            parsed.setdefault("source", "hermes")
            return parsed
    except Exception:
        pass
    return _operator_brain_fallback_plan(goal, observation, task)


def _operator_brain_action_needs_approval(action: dict, safety: str, approved: bool) -> tuple[bool, str]:
    text = " ".join(str(action.get(k) or "") for k in ("type", "target", "text", "url")).lower()
    blocked_terms = ("checkout", "place order", "payment", "pay now", "card number", "password", "sign in", "log in", "login")
    if any(term in text for term in blocked_terms):
        return True, "blocked high-risk browser action"
    if safety in {"cart_change", "review"} and not approved:
        return True, "approval required before this browser action"
    return False, ""


def _operator_brain_dismiss_overlays(page) -> list[str]:
    try:
        return page.evaluate(
            """() => {
                const visible = (el) => {
                  const rect = el.getBoundingClientRect();
                  const style = window.getComputedStyle(el);
                  return rect.width > 4 && rect.height > 4 && style.visibility !== 'hidden' && style.display !== 'none';
                };
                const label = (el) => [el.innerText, el.textContent, el.value, el.getAttribute('aria-label'), el.getAttribute('title')].join(' ').replace(/\\s+/g, ' ').trim();
                const safePatterns = [/^accept( all)?$/i, /^allow$/i, /^ok$/i, /^got it$/i, /^not now$/i, /^maybe later$/i, /^close$/i, /^dismiss$/i, /^skip$/i];
                    const collectControls = () => {
                      const roots = [document];
                      const controls = [];
                      for (let i = 0; i < roots.length; i += 1) {
                        const root = roots[i];
                        for (const el of Array.from(root.querySelectorAll('*'))) {
                          if (el.shadowRoot) roots.push(el.shadowRoot);
                          if (el.matches('button, [role="button"], a, input[type="button"], input[type="submit"], [aria-label]')) controls.push(el);
                        }
                      }
                      return controls;
                    };
                    const controls = collectControls().filter(visible);
                const clicked = [];
                for (const el of controls) {
                  const text = label(el);
                  if (!text || !safePatterns.some(re => re.test(text))) continue;
                  el.click();
                  clicked.push(text.slice(0, 80));
                  if (clicked.length >= 2) break;
                }
                return clicked;
            }"""
        ) or []
    except Exception:
        return []


def _operator_brain_apply_action_once(page, action: dict) -> dict:
    action_type = str(action.get("type") or "").strip().lower()
    if action_type in {"done", "ask_user"}:
        return {"ok": True, "action": action, "stopped": True}
    try:
        page.wait_for_load_state("domcontentloaded", timeout=2500)
    except Exception:
        pass
    if action_type == "open_url":
        url = str(action.get("url") or "").strip()
        safe, reason = _operator_desktop_url_safe(url)
        if not safe:
            return {"ok": False, "action": action, "error": reason}
        page.goto(url, wait_until="domcontentloaded", timeout=25000)
        return {"ok": True, "action": action, "summary": f"Opened {url}"}
    if action_type == "fill":
        target = str(action.get("target") or "search").lower()
        text = str(action.get("text") or "")
        if not text:
            return {"ok": False, "action": action, "error": "fill action requires text"}
        result = page.evaluate(
            """({target, text}) => {
                    const visible = (el) => {
                      const rect = el.getBoundingClientRect();
                      const style = window.getComputedStyle(el);
                      return rect.width > 4 && rect.height > 4 && style.visibility !== 'hidden' && style.display !== 'none';
                    };
                    const fields = Array.from(document.querySelectorAll('input, textarea')).filter(visible);
                    const score = (el) => {
                      const hay = [el.placeholder, el.name, el.id, el.getAttribute('aria-label'), el.type].join(' ').toLowerCase();
                      if (hay.includes(target)) return 4;
                      if (hay.includes('search')) return 3;
                      if ((el.type || '').toLowerCase() === 'search') return 2;
                      return 1;
                    };
                    const el = fields.sort((a,b) => score(b) - score(a))[0];
                    if (!el) return {ok:false, error:'no visible input field'};
                    el.focus();
                    el.value = text;
                    el.dispatchEvent(new Event('input', {bubbles:true}));
                    el.dispatchEvent(new Event('change', {bubbles:true}));
                    return {ok:true, label: el.placeholder || el.name || el.id || el.type || 'input'};
                }""",
            {"target": target, "text": text},
        )
        if not result.get("ok"):
            return {"ok": False, "action": action, "error": result.get("error") or "fill failed"}
        if action.get("submit", True):
            page.keyboard.press("Enter")
            page.wait_for_timeout(1200)
        return {"ok": True, "action": action, "summary": f"Typed {text} into {result.get('label')}"}
    if action_type == "click":
        target = str(action.get("target") or action.get("text") or "").strip()
        if not target:
            return {"ok": False, "action": action, "error": "click action requires target"}
        result = page.evaluate(
            """(target) => {
                    const needle = String(target || '').toLowerCase();
                    const visible = (el) => {
                      const rect = el.getBoundingClientRect();
                      const style = window.getComputedStyle(el);
                      return rect.width > 4 && rect.height > 4 && style.visibility !== 'hidden' && style.display !== 'none';
                    };
                    const label = (el) => [el.innerText, el.textContent, el.value, el.getAttribute('aria-label'), el.getAttribute('title'), el.getAttribute('placeholder')].join(' ').replace(/\\s+/g, ' ').trim();
                    const collectControls = () => {
                      const roots = [document];
                      const controls = [];
                      for (let i = 0; i < roots.length; i += 1) {
                        const root = roots[i];
                        for (const el of Array.from(root.querySelectorAll('*'))) {
                          if (el.shadowRoot) roots.push(el.shadowRoot);
                          if (el.matches('button, a, [role="button"], input[type="button"], input[type="submit"], [aria-label]')) controls.push(el);
                        }
                      }
                      return controls;
                    };
                    const controls = collectControls().filter(visible);
                    const blocked = /(checkout|place order|payment|pay now|card number|password|sign in|log in|login)/i;
                    const scored = controls.map((el) => {
                      const text = label(el);
                      const lower = text.toLowerCase();
                      let score = 0;
                      if (lower === needle) score += 8;
                      if (lower.includes(needle)) score += 5;
                      if (needle === 'add' && /\\badd\\b/i.test(lower)) score += 6;
                      if (/\\badd to cart\\b/i.test(lower) && /\\badd\\b/i.test(needle)) score += 10;
                      if (/\\badd .* to cart\\b/i.test(lower) && /\\badd\\b/i.test(needle)) score += 8;
                      if (blocked.test(lower)) score -= 99;
                      const rect = el.getBoundingClientRect();
                      if (rect.y > 0 && rect.y < window.innerHeight) score += 1;
                      if (String(el.className || '').includes('add-to-cart')) score += 5;
                      return {el, text, score, rect};
                    }).filter(row => row.score > 0).sort((a,b) => b.score - a.score);
                    const row = scored[0];
                    const el = row && row.el;
                    if (!el) return {ok:false, error:'no visible matching control'};
                    const rect = el.getBoundingClientRect();
                    el.scrollIntoView({block:'center', inline:'center'});
                    el.click();
                    return {ok:true, label: row.text.slice(0,120), x: Math.round(rect.x), y: Math.round(rect.y), score: row.score};
                }""",
            target,
        )
        if not result.get("ok"):
            return {"ok": False, "action": action, "error": result.get("error") or "click failed"}
        page.wait_for_timeout(1400)
        return {"ok": True, "action": action, "summary": f"Clicked {result.get('label') or target}", "target": result}
    if action_type == "press":
        key = str(action.get("key") or action.get("text") or "Enter")
        page.keyboard.press(key)
        page.wait_for_timeout(500)
        return {"ok": True, "action": action, "summary": f"Pressed {key}"}
    if action_type == "scroll":
        direction = str(action.get("direction") or "down").lower()
        delta = -650 if direction == "up" else 650
        page.mouse.wheel(0, delta)
        page.wait_for_timeout(500)
        return {"ok": True, "action": action, "summary": f"Scrolled {direction}"}
    if action_type == "wait":
        page.wait_for_timeout(int(action.get("ms") or 1200))
        return {"ok": True, "action": action, "summary": "Waited for page state"}
    return {"ok": False, "action": action, "error": f"unsupported action type: {action_type}"}


def _operator_brain_apply_action(action: dict, approved: bool = False) -> dict:
    def apply(page) -> dict:
        attempts = []
        for attempt in range(1, 4):
            dismissed = _operator_brain_dismiss_overlays(page)
            result = _operator_brain_apply_action_once(page, action)
            result["attempt"] = attempt
            if dismissed:
                result["dismissedOverlays"] = dismissed
            attempts.append(result)
            if result.get("ok"):
                result["attempts"] = [{k: v for k, v in item.items() if k != "attempts"} for item in attempts]
                return result
            if str(action.get("type") or "").lower() not in {"click", "fill"}:
                break
            page.wait_for_timeout(700)
        last = attempts[-1] if attempts else {"ok": False, "error": "action was not attempted"}
        last["attempts"] = [{k: v for k, v in item.items() if k != "attempts"} for item in attempts]
        return last

    return _operator_brain_with_page(apply)


def _operator_brain_verify_action(goal: str, action: dict, before: dict, after: dict, result: dict) -> dict:
    action_type = str(action.get("type") or "").lower()
    before_text = str(((before.get("dom") or {}).get("text")) or "")
    after_text = str(((after.get("dom") or {}).get("text")) or "")
    before_url = str(before.get("url") or "")
    after_url = str(after.get("url") or "")
    before_cart = ((before.get("dom") or {}).get("cart") or {})
    after_cart = ((after.get("dom") or {}).get("cart") or {})
    if not result.get("ok"):
        return {"ok": False, "confidence": "high", "reason": result.get("error") or "action failed"}
    if re.search(r"access denied|captcha|unusual traffic", after_text, re.I):
        return {"ok": False, "confidence": "high", "reason": "page is blocked by access/captcha protection"}
    if action_type == "open_url":
        expected = str(action.get("url") or "")
        ok = bool(expected and (after_url == expected or urlparse(expected).netloc in after_url or urlparse(expected).query in after_url))
        return {"ok": ok, "confidence": "medium" if ok else "high", "reason": "target URL loaded" if ok else "target URL did not load"}
    if action_type in {"click", "fill", "press", "scroll", "wait"}:
        changed = before_url != after_url or before_text[:500] != after_text[:500]
        if action.get("markTermComplete"):
            if "securelogin" in after_url or re.search(r"\b(log in|sign up|sign in|required to continue)\b", after_text, re.I):
                return {
                    "ok": False,
                    "confidence": "high",
                    "reason": "Woolworths requires login before more cart changes can continue",
                }
            before_count = before_cart.get("count")
            after_count = after_cart.get("count")
            if isinstance(before_count, int) and isinstance(after_count, int) and after_count <= before_count:
                return {
                    "ok": False,
                    "confidence": "high",
                    "reason": f"cart count did not increase after click ({before_count} -> {after_count})",
                }
            return {
                "ok": True,
                "confidence": "high" if isinstance(after_count, int) and isinstance(before_count, int) else ("medium" if changed else "low"),
                "reason": "cart count increased after click" if isinstance(after_count, int) and isinstance(before_count, int) else ("cart/action click completed; visual review still recommended" if changed else "click returned success but page text did not visibly change"),
            }
        return {"ok": True, "confidence": "medium" if changed else "low", "reason": "action returned success"}
    if action_type in {"done", "ask_user"}:
        return {"ok": True, "confidence": "high", "reason": "brain stopped intentionally"}
    return {"ok": True, "confidence": "low", "reason": "unsupported action was not independently verified"}


def _operator_brain_step(task_id: str, approved: bool = False) -> tuple[dict, int]:
    task = _operator_browser_task_item(task_id)
    if not task:
        return {"ok": False, "error": "browser task not found"}, 404
    goal = str(task.get("goal") or "")
    _operator_update_browser_task(task_id, "running")
    if not _operator_visible_browser_ready():
        _operator_brain_progress(task_id, "recover", "Visible browser was not running; reopening the task URL.", "active")
        open_payload, open_status = _operator_visible_browser_open_url(str(task.get("url") or "about:blank"), presentation=str(task.get("presentation") or "window"))
        if open_status >= 400 or not open_payload.get("ok"):
            _operator_brain_progress(task_id, "recover", open_payload.get("reason") or open_payload.get("error") or "Visible browser recovery failed.", "failed")
            task = _operator_update_browser_task(task_id, "blocked_browser_recovery_failed", open_payload.get("reason") or "Visible browser recovery failed.")
            return {"ok": False, "taskId": task_id, "stage": "recover", "result": open_payload, "task": task}, open_status if open_status >= 400 else 500
        _operator_brain_progress(task_id, "recover", "Visible browser recovered and task URL reopened.", "done")
    _operator_brain_progress(task_id, "observe", "Observing the visible browser.", "active")
    observation = _operator_brain_observe(task_id)
    if not observation.get("ok"):
        _operator_brain_progress(task_id, "observe", observation.get("error") or "Observation failed.", "failed")
        _operator_update_browser_task(task_id, "blocked_observe_failed", observation.get("error") or "Observation failed.")
        return {"ok": False, "taskId": task_id, "stage": "observe", "observation": observation}, 500
    task = _operator_browser_task_item(task_id) or task
    _operator_brain_progress(task_id, "plan", "Asking Hermes for the next browser action.", "active")
    plan = _operator_brain_plan_with_hermes(goal, observation, task)
    action = plan.get("action") if isinstance(plan.get("action"), dict) else {}
    safety = str(plan.get("safety") or "review")
    thought = plan.get("thought") or f"Planned {action.get('type') or 'next action'}."
    _operator_brain_progress(task_id, "thought", thought, "done", safety=safety)
    _operator_brain_memory_append(task_id, "decisions", {
        "ts": _now_iso(),
        "thought": thought,
        "safety": safety,
        "action": action,
        "source": str(plan.get("source") or "brain"),
    })
    needs_approval, reason = _operator_brain_action_needs_approval(action, safety, approved)
    if needs_approval:
        status = "blocked_high_risk" if reason.startswith("blocked") else "needs_approval"
        _operator_brain_progress(task_id, "gate", reason, "blocked", safety=safety)
        task = _operator_update_browser_task(task_id, status, reason)
        return {"ok": False, "taskId": task_id, "stage": "approval", "reason": reason, "plan": plan, "task": task}, 202
    _operator_brain_progress(task_id, "act", f"Running action: {action.get('type') or 'unknown'}.", "active", safety=safety)
    result = _operator_brain_apply_action(action, approved=approved)
    if not result.get("ok"):
        _operator_brain_progress(task_id, "act", result.get("error") or "Action failed.", "failed")
        _operator_brain_memory_append(task_id, "failures", {
            "ts": _now_iso(),
            "action": action,
            "error": result.get("error") or "action failed",
            "attempts": result.get("attempts") or [],
        })
        task = _operator_update_browser_task(task_id, "blocked_action_failed", result.get("error") or "Action failed.")
        return {"ok": False, "taskId": task_id, "stage": "act", "plan": plan, "result": result, "task": task}, 422
    _operator_brain_progress(task_id, "verify", "Verifying the result of the browser action.", "active")
    after = _operator_brain_observe(task_id)
    verification = _operator_brain_verify_action(goal, action, observation, after if after.get("ok") else {}, result)
    if not verification.get("ok"):
        _operator_brain_progress(task_id, "verify", verification.get("reason") or "Verification failed.", "failed")
        _operator_brain_memory_append(task_id, "failures", {
            "ts": _now_iso(),
            "action": action,
            "error": verification.get("reason") or "verification failed",
            "result": result,
        })
        task = _operator_update_browser_task(task_id, "blocked_verify_failed", verification.get("reason") or "Verification failed.")
        return {"ok": False, "taskId": task_id, "stage": "verify", "plan": plan, "result": result, "verification": verification, "observation": after, "task": task}, 422
    if action.get("markTermComplete") and verification.get("ok"):
        def mutate(item: dict) -> None:
            brain = dict(item.get("brain") or {})
            terms = list(brain.get("completedTerms") or [])
            if action["markTermComplete"] not in terms:
                terms.append(action["markTermComplete"])
            selected = list(brain.get("selectedItems") or [])
            selected.append({
                "ts": _now_iso(),
                "term": action["markTermComplete"],
                "result": result.get("summary") or "cart action completed",
                "verification": verification,
            })
            brain["completedTerms"] = terms
            brain["selectedItems"] = selected[-40:]
            item["brain"] = brain

        _operator_browser_task_mutate(task_id, mutate)
    _operator_brain_memory_append(task_id, "actions", {
        "ts": _now_iso(),
        "action": action,
        "result": result.get("summary") or result,
        "verification": verification,
    })
    _operator_brain_progress(task_id, "verify", verification.get("reason") or "Action verified.", "done", confidence=verification.get("confidence"))
    task = _operator_update_browser_task(
        task_id,
        "needs_review" if action.get("type") in {"done", "ask_user"} else "running",
        "Captured proof after the browser action.",
    )
    return {"ok": True, "taskId": task_id, "stage": "complete" if result.get("stopped") else "acted", "plan": plan, "result": result, "verification": verification, "observation": after, "task": task}, 200


def _operator_brain_run_task(task_id: str, approved: bool = False, max_steps: int = 8) -> None:
    for _ in range(max(1, min(max_steps, 40))):
        payload, status = _operator_brain_step(task_id, approved=approved)
        if status != 200:
            return
        stage = payload.get("stage")
        task = payload.get("task") or {}
        if stage == "complete" or task.get("status") in {"needs_review", "complete", "cancelled", "needs_approval", "blocked_high_risk", "blocked_action_failed", "blocked_observe_failed"}:
            return
        time.sleep(0.75)
    _operator_update_browser_task(task_id, "needs_review", "Operator Brain paused after its step budget; review progress before continuing.")


def _operator_start_browser_task_executor(task_id: str, approved: bool = False) -> bool:
    existing = OPERATOR_BROWSER_TASK_THREADS.get(task_id)
    if existing and existing.is_alive():
        return False
    thread = threading.Thread(target=_operator_brain_run_task, args=(task_id, approved), kwargs={"max_steps": 30}, daemon=True)
    OPERATOR_BROWSER_TASK_THREADS[task_id] = thread
    thread.start()
    return True


def _operator_desktop_open_url(url: str, presentation: str = "window") -> tuple[dict, int]:
    safe, reason = _operator_desktop_url_safe(url)
    if not safe:
        return {"ok": False, "handled": True, "action_class": "blocked", "reason": reason, "url": url}, 400
    visible_payload, visible_status = _operator_visible_browser_open_url(url, presentation=presentation)
    if visible_status < 500:
        return visible_payload, visible_status
    firefox = shutil.which("firefox")
    opener = firefox or shutil.which("xdg-open") or shutil.which("gio")
    if not opener:
        return {"ok": False, "handled": True, "action_class": "failed", "reason": "no desktop browser opener found", "url": url}, 501
    if Path(opener).name == "firefox":
        if presentation == "fullscreen":
            cmd = [opener, "--kiosk", url]
        else:
            cmd = [opener, "--new-window", url]
    elif Path(opener).name == "gio":
        cmd = [opener, "open", url]
    else:
        cmd = [opener, url]
    try:
        proc = subprocess.run(
            cmd,
            env=_operator_desktop_env(),
            capture_output=True,
            text=True,
            timeout=8,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "handled": True, "action_class": "failed", "reason": "desktop browser launch timed out", "url": url, "command": " ".join(cmd)}, 408
    stderr = (proc.stderr or "").strip()
    stdout = (proc.stdout or "").strip()
    ok = proc.returncode == 0
    return {
        "ok": ok,
        "handled": True,
        "action_class": "safe" if ok else "failed",
        "action": "open-url",
        "url": url,
        "presentation": presentation,
        "command": " ".join(shlex.quote(part) for part in cmd),
        "exitCode": proc.returncode,
        "stdout": stdout[:1000],
        "stderr": stderr[:1000],
        "message": (
            f"Opened {url} in a visible Firefox task window."
            if ok and presentation == "window"
            else f"Opened {url} in Firefox full-screen mode."
            if ok and presentation == "fullscreen"
            else f"Opened {url} in a visible Firefox task window. Split-screen arrangement is queued for the next layout layer."
            if ok and presentation == "split-requested"
            else (stderr or stdout or "Desktop browser launch failed")
        ),
    }, 200 if ok else 500


def _operator_desktop_intent_payload(content: str) -> tuple[dict, int]:
    text = str(content or "").strip()
    lower = text.lower()
    looks_like_open = any(word in lower for word in ("open", "launch", "start", "go to", "navigate"))
    looks_like_browser = any(word in lower for word in ("firefox", "browser", "web", "website", "site", "url", "http", "www.", "woolworths", "woolies", "coles", "google"))
    looks_like_operator_task = looks_like_browser and (looks_like_open or _operator_complex_browser_task(text))
    if not looks_like_operator_task:
        return {"ok": True, "handled": False, "reason": "not a desktop browser intent"}, 200
    url = _operator_url_from_text(text)
    if not url:
        return {"ok": False, "handled": True, "action_class": "blocked", "reason": "could not identify a site or URL to open"}, 400
    if _operator_complex_browser_task(text):
        presentation = _operator_desktop_presentation(text)
        payload, status = _operator_desktop_open_url(url, presentation=presentation)
        task, approval = _operator_create_browser_task(text, url, presentation, payload)
        plan_text = _operator_browser_task_summary(task.get("plan") or [])
        hermes_prompt = (
            "Agent OS Operator created a supervised browser task. "
            "Plan the next browser steps, but do not claim cart changes are complete. "
            "Cart-changing click/type actions require explicit approval and checkout/payment is blocked.\n\n"
            f"User goal: {text}\n"
            f"URL opened: {url}\n"
            f"Task id: {task['id']}\n"
            f"Current plan:\n{plan_text}"
        )
        payload.update({
            "ok": bool(payload.get("ok")),
            "handled": True,
            "action_class": "review",
            "action": "complex-browser-task",
            "reviewRequired": True,
            "reason": "Created a supervised browser task and approval gate for cart-changing actions.",
            "message": (
                f"Opened {url} as the task starting point and created supervised browser task {task['id']}.\n\n"
                f"Plan:\n{plan_text}\n\n"
                "Waiting for approval before cart-changing browser actions. Checkout/payment remains blocked."
            ),
            "task": task,
            "approval": approval,
            "continueHermes": True,
            "hermesPrompt": hermes_prompt,
            "nextLayer": "Hermes browser-control planner with reviewed click/type/cart actions",
        })
        return payload, 202 if status < 500 else status
    return _operator_desktop_open_url(url, presentation=_operator_desktop_presentation(text))


def _operator_cmd_status(cmd: list[str], timeout: int = 3) -> dict:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {
            "ok": proc.returncode == 0,
            "exitCode": proc.returncode,
            "stdout": (proc.stdout or "").strip()[:1200],
            "stderr": (proc.stderr or "").strip()[:1200],
        }
    except Exception as exc:
        return {"ok": False, "exitCode": None, "stdout": "", "stderr": str(exc)[:1200]}


def _operator_port_open(host: str, port: int, timeout: float = 0.35) -> bool:
    if not host:
        return False
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def _operator_vnc_password_param() -> str:
    password_file = OPERATOR_DATA_DIR / ".vnc-password"
    try:
        password = password_file.read_text(encoding="utf-8").strip()
    except OSError:
        password = ""
    return f"&password={quote(password, safe='')}" if password else ""


def _operator_real_desktop_bridge_pid() -> int:
    bridge_pid_file = OPERATOR_ARTIFACT_DIR / "real-desktop" / "freerdp.pid"
    try:
        bridge_pid = int(bridge_pid_file.read_text(encoding="utf-8").strip())
        os.kill(bridge_pid, 0)
        return bridge_pid
    except (OSError, ValueError):
        pass
    try:
        proc = subprocess.run(
            ["pgrep", "-f", rf"xfreerdp.*127[.]0[.]0[.]1:{OPERATOR_RDP_PORT}"],
            capture_output=True,
            text=True,
            timeout=1,
        )
    except Exception:
        return 0
    for line in (proc.stdout or "").splitlines():
        try:
            bridge_pid = int(line.strip())
            os.kill(bridge_pid, 0)
        except (OSError, ValueError):
            continue
        try:
            bridge_pid_file.parent.mkdir(parents=True, exist_ok=True)
            bridge_pid_file.write_text(f"{bridge_pid}\n", encoding="utf-8")
        except OSError:
            pass
        return bridge_pid
    return 0


def _operator_observe_status_payload() -> dict:
    tailscale_ip = OPERATOR_TAILSCALE_IP
    tools = {
        "novnc_proxy": shutil.which("novnc_proxy") or ("/usr/share/novnc/utils/novnc_proxy" if Path("/usr/share/novnc/utils/novnc_proxy").exists() else ""),
        "websockify": shutil.which("websockify") or "",
        "x11vnc": shutil.which("x11vnc") or "",
        "wayvnc": shutil.which("wayvnc") or "",
        "tigervncserver": shutil.which("tigervncserver") or "",
        "grdctl": shutil.which("grdctl") or "",
        "xfreerdp": shutil.which("xfreerdp3") or shutil.which("xfreerdp") or "",
    }
    session = {
        "display": os.environ.get("DISPLAY") or "",
        "waylandDisplay": os.environ.get("WAYLAND_DISPLAY") or "",
        "sessionType": os.environ.get("XDG_SESSION_TYPE") or "",
        "desktop": os.environ.get("XDG_CURRENT_DESKTOP") or "",
    }
    services = {}
    vnc_display_active = _operator_port_open("127.0.0.1", OPERATOR_VNC_PORT)
    websockify_active = _operator_port_open(tailscale_ip, OPERATOR_NOVNC_PORT)
    bridge_pid = _operator_real_desktop_bridge_pid()
    real_desktop_active = bool(bridge_pid)
    blockers = []
    if not tools["novnc_proxy"] and not tools["websockify"]:
        blockers.append("noVNC/websockify bridge is not installed")
    if not vnc_display_active and not tools["x11vnc"] and not tools["wayvnc"]:
        blockers.append("no VNC server backend is installed")
    if not vnc_display_active and session["sessionType"].lower() == "wayland" and tools["x11vnc"] and not tools["wayvnc"]:
        blockers.append("current desktop is Wayland; x11vnc cannot observe it directly")
    if not vnc_display_active:
        blockers.append(f"TigerVNC virtual desktop {OPERATOR_VNC_DISPLAY} is not active")
    if not websockify_active:
        blockers.append(f"noVNC websockify bridge is not active on port {OPERATOR_NOVNC_PORT}")
    url_base = (
        f"http://{tailscale_ip}:{OPERATOR_NOVNC_PORT}/vnc.html"
        f"?host={tailscale_ip}&port={OPERATOR_NOVNC_PORT}&path=websockify&autoconnect=true&resize=scale"
    ) if tailscale_ip and not blockers else ""
    password_param = _operator_vnc_password_param()
    observe_url = f"{url_base}&view_only=true{password_param}" if url_base else ""
    control_url = f"{url_base}&view_only=false{password_param}" if url_base else ""
    next_actions = [
        "Run scripts/operator_m7_start.sh if the virtual desktop or noVNC bridge is not active.",
        "Open the observeUrl over Tailscale and enter the generated VNC password.",
        "Keep real desktop control disabled until M8 review.",
    ]
    return {
        "ok": True,
        "stage": "M7",
        "state": "ready" if not blockers else "blocked",
        "live": not blockers,
        "observeUrl": observe_url,
        "viewUrl": observe_url,
        "controlUrl": control_url,
        "realDesktop": {
            "active": real_desktop_active,
            "bridgePid": bridge_pid if real_desktop_active else None,
            "backend": "gnome-rdp-freerdp" if real_desktop_active else "virtual-vnc-fallback",
            "dependencyReady": bool(tools["xfreerdp"]),
        },
        "tailscaleIp": tailscale_ip,
        "tools": tools,
        "services": services,
        "vnc": {
            "display": OPERATOR_VNC_DISPLAY,
            "port": OPERATOR_VNC_PORT,
            "active": vnc_display_active,
        },
        "websockify": {
            "port": OPERATOR_NOVNC_PORT,
            "active": websockify_active,
        },
        "session": session,
        "blockers": blockers,
        "nextActions": next_actions,
        "notes": "Operator is view-only; Manual uses the control-enabled URL. Real desktop requires the local GNOME RDP/FreeRDP bridge.",
    }


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


def _operator_project_root(project: str) -> Path | None:
    root = OPERATOR_PROJECT_REGISTRY.get(str(project or "").strip())
    if not root:
        return None
    try:
        return root.resolve(strict=True)
    except FileNotFoundError:
        return None


def _operator_safe_child(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root)
        return True
    except ValueError:
        return False


def _operator_tree(root: Path, max_depth: int = 3, max_entries: int = 220) -> tuple[list[dict], list[str]]:
    entries: list[dict] = []
    blocked: list[str] = []

    def walk(path: Path, depth: int) -> None:
        if len(entries) >= max_entries or depth > max_depth:
            return
        try:
            children = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
        except OSError:
            return
        for child in children:
            if len(entries) >= max_entries:
                return
            rel = child.relative_to(root).as_posix()
            if child.name in OPERATOR_TREE_SKIP:
                continue
            if not _operator_safe_child(child, root):
                blocked.append(rel)
                continue
            is_dir = child.is_dir()
            entries.append({
                "path": rel,
                "type": "dir" if is_dir else "file",
                "depth": depth,
            })
            if is_dir and not child.is_symlink():
                walk(child, depth + 1)

    walk(root, 1)
    return entries, blocked


def _operator_extension_counts(root: Path, max_files: int = 8000) -> dict[str, int]:
    counts: dict[str, int] = {}
    seen = 0
    for path in root.rglob("*"):
        if seen >= max_files:
            break
        if any(part in OPERATOR_TREE_SKIP for part in path.relative_to(root).parts):
            continue
        if not _operator_safe_child(path, root) or not path.is_file():
            continue
        seen += 1
        suffix = path.suffix.lower() or "[no extension]"
        counts[suffix] = counts.get(suffix, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:40])


def _run_operator_git(root: Path, args: list[str], timeout: int = 5) -> str:
    allowed = {
        ("status", "--short"),
        ("log", "--oneline", "-10"),
    }
    if tuple(args) not in allowed:
        raise ValueError("unsupported operator git command")
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return (result.stdout if result.returncode == 0 else (result.stderr or result.stdout)).strip()


def _operator_inspect_payload(project: str) -> tuple[dict, int]:
    root = _operator_project_root(project)
    if not root:
        return {
            "ok": False,
            "error": "unknown project",
            "project": project,
            "availableProjects": sorted(OPERATOR_PROJECT_REGISTRY.keys()),
        }, 404
    tree, blocked = _operator_tree(root)
    payload = {
        "ok": True,
        "project": project,
        "root": str(root),
        "generatedAt": _now_iso(),
        "safety": {
            "scope": "localhost-only",
            "mode": "read-only",
            "registryLocked": True,
            "arbitraryCommands": False,
            "blockedSymlinkEscapes": blocked,
        },
        "tree": {
            "maxDepth": 3,
            "entries": tree,
        },
        "git": {
            "statusShort": _run_operator_git(root, ["status", "--short"]),
            "logOneline": _run_operator_git(root, ["log", "--oneline", "-10"]),
        },
        "fileCountsByExtension": _operator_extension_counts(root),
    }
    return payload, 200


def _operator_secret_path(value: str) -> bool:
    lower = str(value or "").lower()
    return any(marker in lower for marker in OPERATOR_SECRET_MARKERS)


def _operator_command_paths_ok(tokens: list[str], root: Path) -> tuple[bool, str]:
    for token in tokens[1:]:
        if not token or token.startswith("-"):
            continue
        if any(ch in token for ch in (";", "|", "&", "`", "$(", ">", "<")):
            return False, "shell control syntax is blocked"
        if _operator_secret_path(token):
            return False, "secret-like paths are blocked"
        looks_like_path = "/" in token or token.startswith(".") or (root / token).exists()
        if not looks_like_path:
            continue
        try:
            target = (Path(token) if Path(token).is_absolute() else root / token).resolve(strict=False)
            target.relative_to(root)
        except ValueError:
            return False, "path escapes registered project root"
    return True, ""


def _operator_classify_command(command: str, project: str) -> dict:
    root = _operator_project_root(project)
    if not root:
        return {"action_class": "blocked", "reason": "unknown project", "tokens": [], "root": None}
    try:
        tokens = shlex.split(command or "")
    except ValueError as exc:
        return {"action_class": "blocked", "reason": f"invalid command syntax: {exc}", "tokens": [], "root": root}
    if not tokens:
        return {"action_class": "blocked", "reason": "empty command", "tokens": [], "root": root}
    base = Path(tokens[0]).name
    joined = " ".join(tokens).lower()
    if any(marker in joined for marker in ("github push", "git push", "production trading", "exchange api", "private key")):
        return {"action_class": "blocked", "reason": "blocked high-risk operation", "tokens": tokens, "root": root}
    if base in OPERATOR_BLOCKED_COMMANDS:
        return {"action_class": "blocked", "reason": f"{base} is blocked", "tokens": tokens, "root": root}
    if base == "git":
        if tokens in (["git", "status"], ["git", "status", "--short"], ["git", "log", "--oneline", "-10"]):
            return {"action_class": "safe", "reason": "fixed read-only git command", "tokens": tokens, "root": root}
        if len(tokens) > 1 and tokens[1] == "push":
            return {"action_class": "blocked", "reason": "git push is blocked", "tokens": tokens, "root": root}
        return {"action_class": "review", "reason": "project-local git command requires approval", "tokens": tokens, "root": root}
    if base in OPERATOR_REVIEW_COMMANDS:
        return {"action_class": "review", "reason": f"{base} requires approval", "tokens": tokens, "root": root}
    if base not in OPERATOR_SAFE_COMMANDS:
        return {"action_class": "review", "reason": "unknown command requires approval", "tokens": tokens, "root": root}
    paths_ok, reason = _operator_command_paths_ok(tokens, root)
    if not paths_ok:
        return {"action_class": "blocked", "reason": reason, "tokens": tokens, "root": root}
    return {"action_class": "safe", "reason": "read-only allowlist command", "tokens": tokens, "root": root}


def _operator_safe_env() -> dict[str, str]:
    safe = {}
    for key in ("PATH", "LANG", "LC_ALL", "HOME"):
        if key in os.environ:
            safe[key] = os.environ[key]
    for key in list(safe.keys()):
        upper = key.upper()
        if upper.endswith(("_KEY", "_TOKEN", "_SECRET")) or "PASSWORD" in upper:
            safe.pop(key, None)
    return safe


def _operator_command_hash(command: str, root: Path) -> str:
    return hashlib.sha256(f"{root}\n{command}".encode("utf-8")).hexdigest()


def _operator_pending_approval(
    project: str,
    command: str,
    reason: str,
    session_id: str | None = None,
    extra: dict | None = None,
    ttl_seconds: int = 5 * 60,
) -> dict:
    root = _operator_project_root(project)
    now = int(time.time())
    pending = _read_json(OPERATOR_PENDING_FILE, {"items": []})
    item = {
        "id": secrets.token_hex(8),
        "ts": _now_iso(),
        "expiresAt": now + ttl_seconds,
        "project": project,
        "working_dir": str(root) if root else "",
        "command": command,
        "sha256": _operator_command_hash(command, root or ROOT),
        "reason": reason,
        "status": "pending",
        "session_id": session_id,
    }
    if extra:
        item.update(extra)
    pending["items"] = [i for i in pending.get("items", []) if int(i.get("expiresAt") or 0) > now and i.get("status") == "pending"]
    pending["items"].append(item)
    _write_json(OPERATOR_PENDING_FILE, pending)
    return item


def _operator_pending_items(include_all: bool = False) -> list[dict]:
    now = int(time.time())
    pending = _read_json(OPERATOR_PENDING_FILE, {"items": []})
    items = list(pending.get("items") or [])
    if include_all:
        return items
    return [
        item for item in items
        if item.get("status") == "pending" and int(item.get("expiresAt") or 0) > now
    ]


def _operator_update_approval(approval_id: str, action: str, approver: str = "") -> tuple[dict, int]:
    action = str(action or "").strip().lower()
    if action not in {"approve", "deny", "abort"}:
        return {"ok": False, "error": "invalid approval action"}, 400
    pending = _read_json(OPERATOR_PENDING_FILE, {"items": []})
    items = list(pending.get("items") or [])
    now = int(time.time())
    for item in items:
        if item.get("id") != approval_id:
            continue
        if int(item.get("expiresAt") or 0) <= now:
            item["status"] = "expired"
            _write_json(OPERATOR_PENDING_FILE, {"items": items})
            return {"ok": False, "error": "approval expired", "approval": item}, 410
        if item.get("status") != "pending":
            return {"ok": False, "error": "approval already resolved", "approval": item}, 409
        item["status"] = {"approve": "approved", "deny": "denied", "abort": "aborted"}[action]
        item["resolvedAt"] = _now_iso()
        item["approver"] = approver or "operator"
        _write_json(OPERATOR_PENDING_FILE, {"items": items})
        execution = "not_auto_executed"
        task = None
        if item.get("kind") == "browser-task":
            task_id = str(item.get("taskId") or "")
            if action == "approve":
                if _operator_chrome_binary():
                    task = _operator_update_browser_task(
                        task_id,
                        "approved_running",
                        "Browser task approved. Starting Operator Brain observe-plan-act loop.",
                    )
                    started = _operator_start_browser_task_executor(task_id, approved=True)
                    execution = "operator_brain_started" if started else "operator_brain_already_running"
                else:
                    task = _operator_update_browser_task(
                        task_id,
                        "blocked_missing_visible_browser",
                        "Approval recorded, but the visible-browser executor is unavailable because Chrome/Chromium is not installed.",
                    )
                    execution = "blocked_missing_visible_browser"
            elif action == "deny":
                task = _operator_update_browser_task(task_id, "denied", "Browser task was denied by the operator.")
                execution = "denied_not_executed"
            elif action == "abort":
                task = _operator_update_browser_task(task_id, "cancelled", "Browser task was aborted by the operator.")
                execution = "aborted_not_executed"
        return {"ok": True, "approval": item, "execution": execution, "task": task}, 200
    return {"ok": False, "error": "approval not found"}, 404


def _operator_status_payload() -> dict:
    token = _operator_token_record()
    pair = _operator_pair_record()
    observe = _operator_observe_status_payload()
    return {
        "ok": True,
        "generatedAt": _now_iso(),
        "host": {
            "name": os.uname().nodename if hasattr(os, "uname") else "unknown",
            "tailscaleIp": OPERATOR_TAILSCALE_IP,
        },
        "auth": {
            "tokenExpiresAt": datetime.datetime.fromtimestamp(int(token.get("expiresAt") or 0), datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
            "pairingCode": pair.get("code"),
            "pairingExpiresAt": datetime.datetime.fromtimestamp(int(pair.get("expiresAt") or 0), datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "network": {
            "allowed": "localhost + 100.64.0.0/10",
            "tailnet": [],
        },
        "approvals": {
            "pending": len(_operator_pending_items()),
        },
        "audit": {
            "rows": len(_read_jsonl(OPERATOR_AUDIT_FILE)),
        },
        "observe": {
            "state": observe.get("state"),
            "live": observe.get("live"),
            "observeUrl": observe.get("observeUrl"),
            "viewUrl": observe.get("viewUrl"),
            "controlUrl": observe.get("controlUrl"),
            "realDesktop": observe.get("realDesktop"),
            "blockers": observe.get("blockers", [])[:3],
        },
    }


def _operator_mobile_status_payload() -> dict:
    observe = _operator_observe_status_payload()
    return {
        "ok": True,
        "generatedAt": _now_iso(),
        "host": {
            "name": os.uname().nodename if hasattr(os, "uname") else "unknown",
            "tailscaleIp": OPERATOR_TAILSCALE_IP,
        },
        "approvals": {"pending": len(_operator_pending_items())},
        "audit": {"rows": len(_read_jsonl(OPERATOR_AUDIT_FILE))},
        "observe": {
            "state": observe.get("state") or "blocked",
            "live": bool(observe.get("live")),
            "observeUrl": observe.get("observeUrl") or "",
            "viewUrl": observe.get("viewUrl") or observe.get("observeUrl") or "",
            "controlUrl": observe.get("controlUrl") or "",
            "realDesktop": observe.get("realDesktop") or {},
            "blockers": observe.get("blockers") or [],
        },
    }


def _operator_run_payload(project: str, command: str, session_id: str | None = None) -> tuple[dict, int]:
    decision = _operator_classify_command(command, project)
    root = decision.get("root")
    action_class = decision["action_class"]
    reason = decision["reason"]
    tokens = decision["tokens"]
    if action_class == "blocked":
        return {
            "ok": False,
            "action_class": "blocked",
            "reason": reason,
            "project": project,
            "command": command,
        }, 403
    if action_class == "review":
        pending = _operator_pending_approval(project, command, reason, session_id=session_id)
        return {
            "ok": False,
            "action_class": "review",
            "reason": reason,
            "approval": pending,
        }, 202
    assert isinstance(root, Path)
    try:
        proc = subprocess.run(
            tokens,
            cwd=root,
            env=_operator_safe_env(),
            capture_output=True,
            text=True,
            timeout=8,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "action_class": "safe", "reason": "command timed out", "project": project, "command": command}, 408
    stdout = (proc.stdout or "")[:12000]
    stderr = (proc.stderr or "")[:4000]
    return {
        "ok": proc.returncode == 0,
        "action_class": "safe",
        "project": project,
        "working_dir": str(root),
        "command": command,
        "exitCode": proc.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "stdoutSha256": hashlib.sha256((proc.stdout or "").encode("utf-8")).hexdigest(),
    }, 200 if proc.returncode == 0 else 422


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
        "operatorProof": _operator_proof_payload(session_id),
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
    "1519228976344727674": [
    ],
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
    entry.pop("inferredBy", None)
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
        if re.fullmatch(r"\d{10,}", str(channel)):
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
    numeric_channel = bool(re.fullmatch(r"\d{10,}", str(channel or "")))
    mapped_ids = [
        hsid for hsid, entry in mapping.items()
        if entry.get("channel") == channel and not (numeric_channel and entry.get("inferredBy"))
    ]
    if numeric_channel:
        return mapped_ids

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

    def _operator_bearer(self) -> str:
        auth = str(self.headers.get("Authorization") or "")
        if not auth.startswith("Bearer "):
            return ""
        return auth.split(" ", 1)[1].strip()

    def _operator_auth_error(self, action: str, target: str, project: str = ""):
        remote = self.client_address[0] if self.client_address else ""
        if not _operator_remote_allowed(remote):
            _operator_audit(action, target, "denied", project=project or None, remote=remote, status=403, reason="non-local non-tailnet client")
            return {"ok": False, "error": f"{target} is local-or-tailscale only"}, 403, remote
        if not _operator_token_valid(self._operator_bearer()):
            _operator_audit(action, target, "denied", project=project or None, remote=remote, status=401, reason="missing or invalid bearer token")
            return {"ok": False, "error": "operator auth required"}, 401, remote
        return None, 200, remote

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length:
            return json.loads(self.rfile.read(length))
        return {}

    # -- GET -------------------------------------------------------------

    def do_GET(self):
        # Static files
        if not self.path.startswith("/api/"):
            if urlparse(self.path).path.rstrip("/") == "/m":
                mobile = ROOT / "agent-os-mobile.html"
                if not mobile.exists():
                    return self._json({"error": "mobile shell not found"}, 404)
                body = mobile.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
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
                elif re.fullmatch(r"\d{10,}", str(channel)):
                    _hsid = ""
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

        # GET /api/operator/inspect?project=zen-new — M1A localhost-only read-only inspection
        if self._match("api/operator/inspect") is not None:
            params = parse_qs(urlparse(self.path).query)
            project = (params.get("project") or [""])[0]
            auth_error, auth_status, remote = self._operator_auth_error("inspect", "operator/inspect", project)
            if auth_error:
                return self._json(auth_error, auth_status)
            payload, status = _operator_inspect_payload(project)
            _operator_audit(
                "inspect",
                "operator/inspect",
                "ok" if status < 400 else "denied",
                project=project,
                remote=remote,
                status=status,
                reason=payload.get("error"),
                action_class="safe",
            )
            return self._json(payload, status)

        # GET /api/operator/screenshot — authenticated read-only screen capture
        if self._match("api/operator/screenshot") is not None:
            auth_error, auth_status, remote = self._operator_auth_error("screenshot", "operator/screenshot")
            if auth_error:
                return self._json(auth_error, auth_status)
            payload, status = _operator_screenshot_payload()
            _operator_audit(
                "screenshot",
                "operator/screenshot",
                "ok" if status < 400 else "failed",
                remote=remote,
                status=status,
                reason=payload.get("error"),
                backend=payload.get("backend"),
                path=payload.get("path"),
                bytes=payload.get("bytes"),
                error=payload.get("error"),
                helperMode=payload.get("helperMode"),
                source=payload.get("source"),
                action_class="safe",
            )
            return self._json(payload, status)

        # GET /api/operator/observe/status — authenticated M7 noVNC readiness
        if self._match("api/operator/observe/status") is not None:
            auth_error, auth_status, remote = self._operator_auth_error("observe", "operator/observe/status")
            if auth_error:
                return self._json(auth_error, auth_status)
            payload = _operator_observe_status_payload()
            _operator_audit(
                "observe",
                "operator/observe/status",
                payload.get("state") or "unknown",
                remote=remote,
                status=200,
                reason="; ".join(payload.get("blockers") or []) or "ready",
                action_class="safe",
            )
            return self._json(payload)

        # GET /api/operator/status — authenticated mobile/operator status
        if self._match("api/operator/mobile-status") is not None:
            auth_error, auth_status, remote = self._operator_auth_error("status", "operator/mobile-status")
            if auth_error:
                return self._json(auth_error, auth_status)
            payload = _operator_mobile_status_payload()
            _operator_audit("status", "operator/mobile-status", "ok", remote=remote, status=200, action_class="safe")
            return self._json(payload)

        # GET /api/operator/status — authenticated diagnostics status
        if self._match("api/operator/status") is not None:
            auth_error, auth_status, remote = self._operator_auth_error("status", "operator/status")
            if auth_error:
                return self._json(auth_error, auth_status)
            payload = _operator_status_payload()
            _operator_audit("status", "operator/status", "ok", remote=remote, status=200, action_class="safe")
            return self._json(payload)

        # GET /api/operator/browser/status — authenticated browser session state
        if self._match("api/operator/browser/status") is not None:
            auth_error, auth_status, remote = self._operator_auth_error("browser", "operator/browser/status")
            if auth_error:
                return self._json(auth_error, auth_status)
            payload, status = _operator_browser_payload("status")
            _operator_audit(
                "browser",
                "operator/browser/status",
                "ok" if status < 400 else "failed",
                remote=remote,
                status=status,
                reason=payload.get("error"),
                action_class="safe",
                url=payload.get("url"),
            )
            return self._json(payload, status)

        # GET /api/operator/browser-tasks — authenticated supervised browser task list
        if self._match("api/operator/browser-tasks") is not None:
            auth_error, auth_status, remote = self._operator_auth_error("browser", "operator/browser-tasks")
            if auth_error:
                return self._json(auth_error, auth_status)
            params = parse_qs(urlparse(self.path).query)
            payload = _operator_browser_tasks_payload(include_all=(params.get("all") or [""])[0] == "1")
            _operator_audit("browser", "operator/browser-tasks", "ok", remote=remote, status=200, action_class="safe")
            return self._json(payload, 200)

        # GET /api/operator/brain/status — authenticated visible-browser brain state
        if self._match("api/operator/brain/status") is not None:
            auth_error, auth_status, remote = self._operator_auth_error("brain", "operator/brain/status")
            if auth_error:
                return self._json(auth_error, auth_status)
            params = parse_qs(urlparse(self.path).query)
            task_id = (params.get("taskId") or [""])[0]
            payload = {
                "ok": True,
                "visibleBrowser": {
                    "ready": _operator_visible_browser_ready(),
                    "debugPort": 9222,
                    "version": _operator_brain_cdp_json("/json/version") or {},
                    "chrome": _operator_chrome_binary(),
                    "display": OPERATOR_VNC_DISPLAY,
                    "visionModel": OPERATOR_VISION_MODEL,
                },
                "activeTasks": [
                    tid for tid, thread in OPERATOR_BROWSER_TASK_THREADS.items()
                    if thread.is_alive()
                ],
                "task": _operator_browser_task_item(task_id) if task_id else None,
            }
            _operator_audit("brain", "operator/brain/status", "ok", remote=remote, status=200, action_class="safe", taskId=task_id or None)
            return self._json(payload, 200)

        # GET /api/operator/approvals — authenticated pending approval queue
        if self._match("api/operator/approvals") is not None:
            auth_error, auth_status, remote = self._operator_auth_error("approvals", "operator/approvals")
            if auth_error:
                return self._json(auth_error, auth_status)
            payload = {"ok": True, "items": _operator_pending_items(include_all=True)}
            _operator_audit("approvals", "operator/approvals", "ok", remote=remote, status=200, action_class="safe")
            return self._json(payload)

        # GET /api/operator/audit — authenticated proof/audit rows
        if self._match("api/operator/audit") is not None:
            auth_error, auth_status, remote = self._operator_auth_error("audit", "operator/audit")
            if auth_error:
                return self._json(auth_error, auth_status)
            params = parse_qs(urlparse(self.path).query)
            session_id = (params.get("sessionId") or [""])[0]
            payload = {"ok": True, **_operator_proof_payload(session_id)}
            _operator_audit("audit", "operator/audit", "ok", remote=remote, status=200, action_class="safe", session_id=session_id or None)
            return self._json(payload)

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

        # POST /api/operator/pair — one-time local/Tailscale phone pairing
        if self._match("api/operator/pair") is not None:
            body = self._read_body()
            code = str(body.get("code") or "").strip()
            remote = self.client_address[0] if self.client_address else ""
            if not _operator_remote_allowed(remote):
                payload, status = {"ok": False, "error": "operator pairing is local/Tailscale only"}, 403
            else:
                payload, status = _operator_pair_payload(code)
            _operator_audit(
                "pair",
                "operator/pair",
                "ok" if status < 300 else "denied",
                remote=remote,
                status=status,
                reason=payload.get("error"),
                action_class="safe",
            )
            return self._json(payload, status)

        # POST /api/operator/approval — resolve pending approval state only
        if self._match("api/operator/approval") is not None:
            body = self._read_body()
            approval_id = str(body.get("id") or "").strip()
            action = str(body.get("action") or "").strip().lower()
            approver = str(body.get("approver") or "").strip() or "operator"
            auth_error, auth_status, remote = self._operator_auth_error("approval", "operator/approval")
            if auth_error:
                return self._json(auth_error, auth_status)
            payload, status = _operator_update_approval(approval_id, action, approver)
            _operator_audit(
                "approval",
                "operator/approval",
                "ok" if status < 300 else "denied",
                remote=remote,
                status=status,
                reason=payload.get("error") or action,
                action_class="review",
                approver=approver,
            )
            return self._json(payload, status)

        # POST /api/operator/run — M2 classified command runner
        if self._match("api/operator/run") is not None:
            body = self._read_body()
            project = str(body.get("project") or "zen-new").strip()
            command = str(body.get("command") or "").strip()
            session_id = body.get("session_id") or body.get("sessionId")
            auth_error, auth_status, remote = self._operator_auth_error("run", "operator/run", project)
            if auth_error:
                return self._json(auth_error, auth_status)
            payload, status = _operator_run_payload(project, command, session_id=session_id)
            _operator_audit(
                "run",
                "operator/run",
                "ok" if status < 300 else ("pending" if status == 202 else "denied"),
                project=project,
                remote=remote,
                status=status,
                reason=payload.get("reason") or payload.get("error"),
                action_class=payload.get("action_class"),
                session_id=session_id,
            )
            return self._json(payload, status)

        # POST /api/operator/desktop-intent — natural desktop/browser action bridge
        if self._match("api/operator/desktop-intent") is not None:
            body = self._read_body()
            content = str(body.get("content") or body.get("command") or "").strip()
            session_id = body.get("session_id") or body.get("sessionId")
            auth_error, auth_status, remote = self._operator_auth_error("desktop", "operator/desktop-intent")
            if auth_error:
                return self._json(auth_error, auth_status)
            payload, status = _operator_desktop_intent_payload(content)
            if payload.get("handled"):
                audit_result = "ok" if status < 300 and payload.get("ok") else "failed"
                if payload.get("action_class") == "review" and status == 202:
                    audit_result = "pending"
                _operator_audit(
                    "desktop",
                    "operator/desktop/open-url" if payload.get("action") == "open-url" else "operator/desktop-intent",
                    audit_result,
                    remote=remote,
                    status=status,
                    reason=payload.get("reason") or payload.get("message"),
                    action_class=payload.get("action_class") or "safe",
                    url=payload.get("url"),
                    command=payload.get("command"),
                    exitCode=payload.get("exitCode"),
                    presentation=payload.get("presentation"),
                    taskId=(payload.get("task") or {}).get("id"),
                    kind="browser-task" if payload.get("task") else None,
                    session_id=session_id,
                )
            return self._json(payload, status)

        # POST /api/operator/browser/:action — M6 browser observation/review layer
        m = self._match("api/operator/browser/:action")
        if m:
            action = str(m["action"] or "").strip().lower()
            body = self._read_body()
            auth_error, auth_status, remote = self._operator_auth_error("browser", f"operator/browser/{action}")
            if auth_error:
                return self._json(auth_error, auth_status)
            payload, status = _operator_browser_payload(action, body)
            target = f"operator/browser/{action}"
            _operator_audit(
                "screenshot" if action == "screenshot" else "browser",
                target,
                "ok" if status < 300 else ("review" if status == 202 else "failed"),
                remote=remote,
                status=status,
                reason=payload.get("reason") or payload.get("error"),
                action_class=payload.get("action_class") or ("safe" if action in {"open", "screenshot", "close", "status"} else "review"),
                path=payload.get("path"),
                bytes=payload.get("bytes"),
                error=payload.get("error"),
                url=payload.get("url"),
                title=payload.get("title"),
                mime=payload.get("mime"),
            )
            return self._json(payload, status)

        # POST /api/operator/brain/observe — capture visible-browser state for a task
        if self._match("api/operator/brain/observe") is not None:
            body = self._read_body()
            task_id = str(body.get("taskId") or body.get("task_id") or "").strip()
            auth_error, auth_status, remote = self._operator_auth_error("brain", "operator/brain/observe")
            if auth_error:
                return self._json(auth_error, auth_status)
            payload = _operator_brain_observe(task_id)
            status = 200 if payload.get("ok") else 500
            _operator_audit(
                "brain",
                "operator/brain/observe",
                "ok" if payload.get("ok") else "failed",
                remote=remote,
                status=status,
                action_class="safe",
                taskId=task_id or None,
                path=(payload.get("screenshot") or {}).get("path"),
                url=payload.get("url"),
                title=payload.get("title"),
                reason=payload.get("error"),
            )
            return self._json(payload, status)

        # POST /api/operator/brain/step — one observe-plan-act cycle
        if self._match("api/operator/brain/step") is not None:
            body = self._read_body()
            task_id = str(body.get("taskId") or body.get("task_id") or "").strip()
            approved = bool(body.get("approved"))
            auth_error, auth_status, remote = self._operator_auth_error("brain", "operator/brain/step")
            if auth_error:
                return self._json(auth_error, auth_status)
            payload, status = _operator_brain_step(task_id, approved=approved)
            _operator_audit(
                "brain",
                "operator/brain/step",
                "ok" if status < 300 else ("review" if status == 202 else "failed"),
                remote=remote,
                status=status,
                action_class="review" if approved else "safe",
                taskId=task_id,
                reason=payload.get("reason") or payload.get("error"),
            )
            return self._json(payload, status)

        # POST /api/operator/brain/start — background brain loop for an approved task
        if self._match("api/operator/brain/start") is not None:
            body = self._read_body()
            task_id = str(body.get("taskId") or body.get("task_id") or "").strip()
            approved = bool(body.get("approved"))
            auth_error, auth_status, remote = self._operator_auth_error("brain", "operator/brain/start")
            if auth_error:
                return self._json(auth_error, auth_status)
            task = _operator_browser_task_item(task_id)
            if not task:
                return self._json({"ok": False, "error": "browser task not found"}, 404)
            started = _operator_start_browser_task_executor(task_id, approved=approved)
            task = _operator_update_browser_task(
                task_id,
                "running",
                "Operator Brain background loop started." if started else "Operator Brain is already running.",
            )
            payload = {"ok": True, "started": started, "task": task}
            _operator_audit(
                "brain",
                "operator/brain/start",
                "ok",
                remote=remote,
                status=200,
                action_class="review" if approved else "safe",
                taskId=task_id,
            )
            return self._json(payload, 200)

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
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
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
    pair = _operator_pair_record()
    pair_exp = datetime.datetime.fromtimestamp(int(pair.get("expiresAt") or 0), datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    print(f"Operator pairing code: {pair.get('code')} (expires {pair_exp})")
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
