#!/usr/bin/env python3
"""Agent OS desktop capture helper.

Runs as the current user and returns structured JSON for Agent OS proof flows.
Screenshots are optional evidence: failure must be explicit and non-blocking.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import shutil
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def iso_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z")


def png_ok(path: Path) -> bool:
    try:
        return path.exists() and path.stat().st_size > 8 and path.read_bytes()[:8] == PNG_MAGIC
    except OSError:
        return False


def compact_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return str(value).strip()[:500]


def attempt(name: str, cmd: list[str], out: Path, timeout: float = 5.0) -> dict:
    start = time.monotonic()
    record = {
        "backend": name,
        "cmd": " ".join(cmd),
        "status": "started",
        "elapsedMs": 0,
    }
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        record["elapsedMs"] = int((time.monotonic() - start) * 1000)
        record["exitCode"] = proc.returncode
        record["stdout"] = compact_text(proc.stdout)
        record["stderr"] = compact_text(proc.stderr)
        if proc.returncode == 0 and png_ok(out):
            record["status"] = "ok"
            record["path"] = str(out)
            record["bytes"] = out.stat().st_size
        else:
            record["status"] = "failed"
            record["reason"] = record["stderr"] or record["stdout"] or "backend did not produce a PNG artifact"
    except subprocess.TimeoutExpired as exc:
        record["elapsedMs"] = int((time.monotonic() - start) * 1000)
        record["status"] = "timeout"
        record["reason"] = f"{name} timed out after {timeout:g}s"
        record["stdout"] = compact_text(exc.stdout)
        record["stderr"] = compact_text(exc.stderr)
    except Exception as exc:  # pragma: no cover - defensive helper boundary
        record["elapsedMs"] = int((time.monotonic() - start) * 1000)
        record["status"] = "failed"
        record["reason"] = str(exc)
    return record


def skip(name: str, reason: str) -> dict:
    return {"backend": name, "status": "skipped", "reason": reason}


def build_attempts(out: Path) -> list[tuple[str, list[str], float] | dict]:
    attempts: list[tuple[str, list[str], float] | dict] = []
    gdbus = shutil.which("gdbus")
    if gdbus:
        attempts.append((
            "gnome-shell-dbus",
            [
                gdbus,
                "call",
                "--session",
                "--dest",
                "org.gnome.Shell",
                "--object-path",
                "/org/gnome/Shell/Screenshot",
                "--method",
                "org.gnome.Shell.Screenshot.Screenshot",
                "true",
                "false",
                str(out),
            ],
            5.0,
        ))
        if os.environ.get("ZEN_CAPTURE_PORTAL") == "1":
            attempts.append((
                "xdg-desktop-portal",
                [
                    gdbus,
                    "call",
                    "--session",
                    "--dest",
                    "org.freedesktop.portal.Desktop",
                    "--object-path",
                    "/org/freedesktop/portal/desktop",
                    "--method",
                    "org.freedesktop.portal.Screenshot.Screenshot",
                    "",
                    "{'modal': <false>, 'interactive': <false>}",
                ],
                3.0,
            ))
        else:
            attempts.append(skip("xdg-desktop-portal", "disabled by default to avoid interactive portal prompts"))
    else:
        attempts.append(skip("gnome-shell-dbus", "gdbus not found"))
        attempts.append(skip("xdg-desktop-portal", "gdbus not found"))

    for backend, binary, cmd in (
        ("grim", "grim", lambda exe: [exe, str(out)]),
        ("scrot", "scrot", lambda exe: [exe, str(out)]),
        ("maim", "maim", lambda exe: [exe, str(out)]),
        ("imagemagick-import", "import", lambda exe: [exe, "-window", "root", str(out)]),
    ):
        exe = shutil.which(binary)
        attempts.append((backend, cmd(exe), 5.0) if exe else skip(backend, f"{binary} not found"))

    xwd = shutil.which("xwd")
    convert = shutil.which("convert") or shutil.which("magick")
    if xwd and convert:
        helper = f"{xwd} -root -silent | {convert} xwd:- {out}"
        attempts.append(("xwd-convert", ["sh", "-c", helper], 5.0))
    else:
        attempts.append(skip("xwd-convert", "xwd or ImageMagick convert not found"))
    return attempts


def capture(out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = out_dir / f"operator-screenshot-{stamp}.png"
    attempts: list[dict] = []
    env = {
        "display": os.environ.get("DISPLAY") or "",
        "waylandDisplay": os.environ.get("WAYLAND_DISPLAY") or "",
        "sessionType": os.environ.get("XDG_SESSION_TYPE") or "",
        "currentDesktop": os.environ.get("XDG_CURRENT_DESKTOP") or "",
        "hasSessionBus": bool(os.environ.get("DBUS_SESSION_BUS_ADDRESS")),
    }
    for item in build_attempts(out):
        result = item if isinstance(item, dict) else attempt(item[0], item[1], out, item[2])
        attempts.append(result)
        if result.get("status") == "ok":
            return {
                "ok": True,
                "backend": result.get("backend"),
                "path": str(out),
                "bytes": result.get("bytes"),
                "mime": "image/png",
                "capturedAt": iso_now(),
                "env": env,
                "attempts": attempts,
            }
        try:
            out.unlink(missing_ok=True)
        except OSError:
            pass

    useful = [a for a in attempts if a.get("status") not in {"skipped"}]
    last = useful[-1] if useful else attempts[-1] if attempts else {}
    return {
        "ok": False,
        "backend": last.get("backend") or "",
        "path": "",
        "error": last.get("reason") or last.get("stderr") or "screenshot unavailable",
        "reason": "No capture backend produced a PNG artifact",
        "capturedAt": iso_now(),
        "env": env,
        "attempts": attempts,
    }


class CaptureHandler(BaseHTTPRequestHandler):
    out_dir: Path

    def _json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        return

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/health":
            return self._json({
                "ok": True,
                "service": "operator-capture-helper",
                "generatedAt": iso_now(),
                "env": {
                    "display": os.environ.get("DISPLAY") or "",
                    "waylandDisplay": os.environ.get("WAYLAND_DISPLAY") or "",
                    "sessionType": os.environ.get("XDG_SESSION_TYPE") or "",
                    "hasSessionBus": bool(os.environ.get("DBUS_SESSION_BUS_ADDRESS")),
                },
            })
        return self._json({"ok": False, "error": "not found"}, 404)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path != "/capture":
            return self._json({"ok": False, "error": "not found"}, 404)
        result = capture(self.out_dir)
        return self._json(result, 200 if result.get("ok") else 503)


def serve(out_dir: Path, host: str, port: int) -> int:
    CaptureHandler.out_dir = out_dir.expanduser().resolve()
    CaptureHandler.out_dir.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer((host, port), CaptureHandler)
    print(json.dumps({
        "ok": True,
        "service": "operator-capture-helper",
        "url": f"http://{host}:{port}",
        "outDir": str(CaptureHandler.out_dir),
        "startedAt": iso_now(),
    }), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture desktop screenshot artifact for Agent OS")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--serve", action="store_true", help="Run a localhost user-session capture service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8771)
    args = parser.parse_args()
    if args.serve:
        return serve(Path(args.out_dir), args.host, args.port)
    result = capture(Path(args.out_dir).expanduser().resolve())
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
