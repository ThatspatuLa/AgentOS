"""Small Playwright browser operator for Agent OS M6.

This module keeps browser control scoped and observable. Navigation and
screenshots are safe/read-only; click/type are surfaced to Agent OS for review.
"""

from __future__ import annotations

import base64
import datetime as _dt
import shutil
from pathlib import Path
from urllib.parse import urlparse

try:
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover - import boundary
    sync_playwright = None


class BrowserOperator:
    def __init__(self, artifact_dir: Path):
        self.artifact_dir = artifact_dir
        self.current_url = ""
        self.current_title = ""
        self.opened_at = ""

    def _launch_kwargs(self) -> dict:
        if sync_playwright is None:
            raise RuntimeError("playwright is not installed")
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        launch_args = {
            "headless": True,
            "args": ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        }
        executable = self._system_chrome()
        if executable:
            launch_args["executable_path"] = executable
        return launch_args

    @staticmethod
    def _system_chrome() -> str:
        for name in (
            "google-chrome",
            "google-chrome-stable",
            "chromium",
            "chromium-browser",
        ):
            path = shutil.which(name)
            if path:
                return path
        return ""

    @staticmethod
    def normalize_url(url: str) -> str:
        value = (url or "").strip()
        if not value:
            raise ValueError("url is required")
        if "://" not in value and "." in value:
            value = "https://" + value
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("only http/https URLs are allowed")
        return value

    def status(self) -> dict:
        return {
            "ok": True,
            "open": bool(self.current_url),
            "url": self.current_url,
            "title": self.current_title,
            "openedAt": self.opened_at,
        }

    def open(self, url: str) -> dict:
        target = self.normalize_url(url)
        with sync_playwright() as pw:
            browser = pw.chromium.launch(**self._launch_kwargs())
            try:
                context = browser.new_context(viewport={"width": 1440, "height": 960}, locale="en-AU")
                page = context.new_page()
                page.goto(target, wait_until="domcontentloaded", timeout=20000)
                self.current_url = page.url
                self.current_title = page.title()
                self.opened_at = _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z")
            finally:
                browser.close()
        return {"ok": True, **self.status()}

    def screenshot(self) -> dict:
        if not self.current_url:
            raise ValueError("browser is not open")
        stamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.artifact_dir / f"browser-{stamp}.png"
        with sync_playwright() as pw:
            browser = pw.chromium.launch(**self._launch_kwargs())
            try:
                context = browser.new_context(viewport={"width": 1440, "height": 960}, locale="en-AU")
                page = context.new_page()
                page.goto(self.current_url, wait_until="domcontentloaded", timeout=20000)
                self.current_url = page.url
                self.current_title = page.title()
                raw = page.screenshot(path=str(path), full_page=True)
            finally:
                browser.close()
        return {
            "ok": True,
            "mime": "image/png",
            "bytes": len(raw),
            "path": str(path),
            "url": self.current_url,
            "title": self.current_title,
            "capturedAt": _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "dataUrl": "data:image/png;base64," + base64.b64encode(raw).decode("ascii"),
        }

    def close(self) -> dict:
        self.current_url = ""
        self.current_title = ""
        self.opened_at = ""
        return {"ok": True, "closed": True}
