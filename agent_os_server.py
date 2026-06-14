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

Data layout:
  data/sessions/index.json                   -> session registry + hermesSessionId links
  data/sessions/<sessionId>/summary.json     -> decisions, blockers, files, validation
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
    return datetime.datetime.utcnow().isoformat() + "Z"


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

def _hermes_messages(hermes_session_id: str, limit: int = 200) -> list[dict]:
    """Read messages from Hermes state.db for a given Hermes session ID.
    This is how Discord chat history appears in Agent OS."""
    if not HERMES_DB.exists() or not hermes_session_id:
        return []
    try:
        conn = sqlite3.connect(f"file:{HERMES_DB}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT role, content, token_count, timestamp
               FROM messages
               WHERE session_id = ? AND role IN ('user','assistant')
               ORDER BY timestamp ASC
               LIMIT ?""",
            (hermes_session_id, limit),
        ).fetchall()
        conn.close()
        return [
            {
                "role": r["role"],
                "content": r["content"],
                "ts": r["timestamp"],
                "tokens": r["token_count"],
                "source": "hermes",
            }
            for r in rows
        ]
    except Exception:
        return []


def _hermes_session_count(hermes_session_id: str) -> int:
    if not HERMES_DB.exists() or not hermes_session_id:
        return 0
    try:
        conn = sqlite3.connect(f"file:{HERMES_DB}?mode=ro", uri=True)
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
        parts = [p for p in self.path.split("/") if p]
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

        # GET /api/kanban-tasks
        if self._match("api/kanban-tasks") is not None:
            return self._json(_read_json(TASKS_FILE, {"tasks": []}))

        # GET /api/sessions
        if self._match("api/sessions") is not None:
            # Enrich with live Hermes message counts
            out = []
            for s in index["sessions"]:
                sm = dict(s)
                hsid = sm.get("hermesSessionId")
                if hsid:
                    sm["hermesMessageCount"] = _hermes_session_count(hsid)
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
                sm["hermesMessageCount"] = _hermes_session_count(hsid)
            return self._json(sm)

        # GET /api/sessions/:id/messages
        m = self._match("api/sessions/:id/messages")
        if m:
            s = _get_session(index, m["id"])
            if not s:
                return self._json({"error": "not found"}, 404)
            hsid = s.get("hermesSessionId")

            # Read from Hermes DB (Discord history)
            hermes_msgs = _hermes_messages(hsid) if hsid else []

            # Read Agent OS local messages
            local_path = SESSIONS_DIR / m["id"] / "messages.jsonl"
            local_msgs = [
                {**rec, "source": "agent-os"}
                for rec in _read_jsonl(local_path)
            ]

            # Merge: Hermes first (chronological), then local
            all_msgs = hermes_msgs + local_msgs

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
                    "hermesMessageCount": _hermes_session_count(hsid) if hsid else 0,
                    "blockers": ch.get("blockers", []),
                    "color": ch.get("color", "#666"),
                })
            return self._json({"sessionId": m["id"], "children": rollup})

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

            # Store user message locally
            msg_path = SESSIONS_DIR / m["id"] / "messages.jsonl"
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
                if assistant_record:
                    _append_jsonl(msg_path, assistant_record)
                    s["messageCount"] = (s.get("messageCount") or 0) + 1
                    _save_index(index)

            return self._json({
                "ok": True,
                "message": record,
                "assistant": assistant_record,
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

        cmd = [
            "hermes", "chat",
            "-q", user_message,
            "--resume", hermes_session_id,
            "-Q",
            "--source", "tool",
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 min max — same as gateway timeout
            )
            if result.returncode == 0:
                # Extract the response text from stdout
                # Format: session_id line, blank line, then the response
                stdout = result.stdout.strip()
                response_text = stdout

                # Try to strip the session_id prefix line
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

                if response_text:
                    return {
                        "role": "assistant",
                        "content": response_text,
                        "ts": _now_iso(),
                        "source": "hermes",
                    }
            else:
                err = result.stderr.strip() or "hermes chat returned non-zero"
                return {
                    "role": "assistant",
                    "content": f"[Hermes error: {err}]",
                    "ts": _now_iso(),
                    "source": "hermes-error",
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
