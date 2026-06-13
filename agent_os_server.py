#!/usr/bin/env python3
"""Agent OS local dev server with shared Kanban task persistence.

Serves static files like python -m http.server and adds:
  GET/PUT /api/kanban-tasks -> data/kanban-tasks.json

This lets browser changes made inside Agent OS persist to disk so Hermes/Zen can
read them immediately instead of being trapped in one browser's localStorage.
"""

from __future__ import annotations

import argparse
import json
import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
TASKS_FILE = ROOT / "data" / "kanban-tasks.json"


class AgentOSHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "no-store" if self.path.startswith("/api/") else "")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        if urlparse(self.path).path == "/api/kanban-tasks":
            return self.handle_get_kanban_tasks()
        return super().do_GET()

    def do_PUT(self):
        if urlparse(self.path).path == "/api/kanban-tasks":
            return self.handle_put_kanban_tasks()
        self.send_error(404, "Unknown API endpoint")

    def handle_get_kanban_tasks(self):
        TASKS_FILE.parent.mkdir(parents=True, exist_ok=True)
        if TASKS_FILE.exists():
            try:
                payload = json.loads(TASKS_FILE.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                payload = {"version": 0, "tasks": [], "error": "kanban-tasks.json is malformed"}
        else:
            payload = {"version": 0, "tasks": []}
        self.write_json(payload)

    def handle_put_kanban_tasks(self):
        length = int(self.headers.get("Content-Length") or "0")
        if length > 5_000_000:
            self.send_error(413, "Payload too large")
            return
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError as exc:
            self.send_error(400, f"Invalid JSON: {exc}")
            return
        tasks = payload.get("tasks")
        if not isinstance(tasks, list):
            self.send_error(400, "Expected JSON object with tasks array")
            return
        clean_payload = {
            "version": int(payload.get("version") or 0),
            "updated_at": payload.get("updated_at") or __import__("datetime").datetime.utcnow().isoformat() + "Z",
            "tasks": tasks,
        }
        TASKS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile("w", encoding="utf-8", dir=str(TASKS_FILE.parent), delete=False) as tmp:
            json.dump(clean_payload, tmp, ensure_ascii=False, indent=2)
            tmp.write("\n")
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, TASKS_FILE)
        self.write_json({"ok": True, "path": str(TASKS_FILE), "count": len(tasks), "version": clean_payload["version"]})

    def write_json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    os.chdir(ROOT)
    server = ThreadingHTTPServer((args.host, args.port), AgentOSHandler)
    print(f"Serving Agent OS on {args.host} port {args.port} (http://{args.host}:{args.port}/) ...", flush=True)
    print(f"Kanban API: /api/kanban-tasks -> {TASKS_FILE}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
