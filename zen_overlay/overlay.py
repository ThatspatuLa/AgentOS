"""Zen Overlay v2 — floating prompt input for Project Zen (2026-08-03).

Single-file Tk overlay that submits prompts to the running agent_os_server
via HTTP POST /api/zen/chat. No foreground Discord control. No pynput.
No cua-driver Discord clicks. The bot handles all Discord I/O.

Specs (from CONTEXT_HANDOFF.md):
- 460x280 frameless Tk window, top-right of primary monitor
- Drag-by-header
- Always-on-top
- Multi-line input with @<project> autocomplete (Tab to complete)
- Submit on Enter (when @<project> is selected) or Ctrl+Enter (always)
- Esc to dismiss (hide window)
- Hotkey Ctrl+Shift+Z to summon
- Read panel below input shows agent's response
- Submit goes through agent_os_server REST endpoint
- Failure mode: "Agent offline — try Discord #<channel> directly."

Run:
    python C:/Users/Tyson/Projects/ZenNew/zen_overlay/overlay.py
"""

from __future__ import annotations

import json
import os
import queue
import sys
import threading
import time
import tkinter as tk
import tkinter.font as tkfont
from pathlib import Path
from typing import Optional
from urllib import error as urllib_error
from urllib import request as urllib_request

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Where the agent OS server lives. Defaults match agent_os_server.py.
SERVER_URL = os.environ.get("ZEN_OVERLAY_SERVER", "http://127.0.0.1:8765")
CHAT_ENDPOINT = f"{SERVER_URL}/api/zen/chat"

# History endpoint for prefilling last messages per project.
HISTORY_ENDPOINT_TEMPLATE = f"{SERVER_URL}/api/zen/history/{{project}}"

# Project registry — channel id is for the failure-mode fallback hint.
PROJECTS = [
    # (id, display label, discord channel name)
    ("zen", "Zen", "zen-chat"),
    ("kiyosaki", "Kiyosaki", "kiyosaki-chat"),
    ("minato", "Minato", "minato-chat"),
    ("nami", "Nami", "nami-chat"),
    ("rin", "Rin", "rin-chat"),
    ("toji", "Toji", "toji-chat"),
    ("kazuki", "Kazuki", "kazuki-chat"),
    ("zen-os", "Zen OS", "zen-os"),
]
PROJECT_IDS = [p[0] for p in PROJECTS]
PROJECT_BY_ID = {p[0]: p for p in PROJECTS}

# Hotkey: Ctrl+Shift+Z (already verified to work, unlike Ctrl+Shift+Space
# which Windows reserves for Sticky Keys).
HOTKEY_LABEL = "Ctrl+Shift+Z"

# Window geometry
WINDOW_W = 460
WINDOW_H = 280
MARGIN_FROM_EDGE = 24

# ---------------------------------------------------------------------------
# Network — minimal stdlib only (no `requests` dependency)
# ---------------------------------------------------------------------------


def _post_json(url: str, payload: dict, timeout: float = 30.0) -> dict:
    """POST a JSON payload, return parsed JSON dict. Raises on transport."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib_request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib_request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body.strip() else {}


def _get_json(url: str, timeout: float = 10.0) -> dict:
    """GET a URL, return parsed JSON dict. Raises on transport."""
    req = urllib_request.Request(url, method="GET")
    with urllib_request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body.strip() else {}


def _server_reachable(timeout: float = 5.0) -> bool:
    """Probe the server — return True if /api/health responds.

    Retries once on connection error before giving up. 5s default
    timeout because the server can be slow under load (Hermes calls
    hogging the SimpleHTTPServer thread).
    """
    for attempt in range(2):
        try:
            _get_json(f"{SERVER_URL}/api/health", timeout=timeout)
            return True
        except Exception:
            if attempt == 0:
                time.sleep(0.5)
    return False


# ---------------------------------------------------------------------------
# Tk overlay
# ---------------------------------------------------------------------------

# Color scheme — dark, minimal.
COLOR_BG = "#1a1a1a"
COLOR_BG_INPUT = "#222222"
COLOR_BG_RESPONSE = "#0d0d0d"
COLOR_FG = "#eeeeee"
COLOR_FG_DIM = "#888888"
COLOR_FG_RESPONSE = "#a8c5e0"
COLOR_ACCENT = "#0078d4"
COLOR_BORDER = "#333333"
COLOR_PULSE_IDLE = "#555555"
COLOR_PULSE_BUSY = "#e6b800"
COLOR_PULSE_OK = "#3fb950"
COLOR_PULSE_ERR = "#f85149"

# Status states
STATUS_IDLE = "idle"
STATUS_BUSY = "busy"
STATUS_OK = "ok"
STATUS_ERR = "err"


class ZenOverlay:
    """Floating Tk overlay — POSTs to /api/zen/chat, renders reply inline."""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Zen Overlay v2")
        self.root.overrideredirect(True)
        self.root.attributes("-topmost", True)
        self._configure_geometry()

        # Tk variables
        self._input_var = tk.StringVar()
        self._autocomplete_visible = False
        self._autocomplete_index = -1
        self._autocomplete_matches: list[str] = []
        self._autocomplete_anchor: int = 0  # index in input where @ started

        # Async plumbing
        self._result_queue: queue.Queue = queue.Queue()
        self._inflight: Optional[threading.Thread] = None
        self._status = STATUS_IDLE

        # Build UI
        self._build()

        # Bindings
        self._wire_drag()
        self._wire_keys()
        self._wire_hotkey()

        # History cache per project (last fetched text).
        self._history_cache: dict[str, list[dict]] = {}

        # First-run appearance
        self._position_top_right()
        # Auto-summon on first launch — no hidden process. After this,
        # the user toggles visibility with Ctrl+Shift+Z.
        self.root.after(120, self.summon)
        self._tick()  # poll async results

    # ------------------------------------------------------------------
    # Geometry / positioning
    # ------------------------------------------------------------------

    def _configure_geometry(self) -> None:
        """Pin window size; allow centering later."""
        self.root.minsize(WINDOW_W, WINDOW_H)
        self.root.maxsize(WINDOW_W, WINDOW_H)
        self.root.geometry(f"{WINDOW_W}x{WINDOW_H}")

    def _position_top_right(self) -> None:
        """Place window at top-right of primary monitor."""
        try:
            self.root.update_idletasks()
            sw = self.root.winfo_screenwidth()
        except Exception:
            sw = 1920
        x = max(0, sw - WINDOW_W - MARGIN_FROM_EDGE)
        y = MARGIN_FROM_EDGE
        self.root.geometry(f"{WINDOW_W}x{WINDOW_H}+{x}+{y}")

    # ------------------------------------------------------------------
    # Build UI
    # ------------------------------------------------------------------

    def _build(self) -> None:
        """Header (drag handle) + input + status strip + response panel."""
        outer = tk.Frame(
            self.root, bg=COLOR_BG, bd=0,
            highlightthickness=1, highlightbackground=COLOR_BORDER,
        )
        outer.pack(fill="both", expand=True)
        self._outer = outer

        # ---- Header (drag handle) ----
        header = tk.Frame(outer, bg=COLOR_BG, height=22)
        header.pack(fill="x")
        header.pack_propagate(False)
        self._header = header

        self._pulse = tk.Label(
            header, text="●", fg=COLOR_PULSE_IDLE, bg=COLOR_BG,
            font=("Segoe UI", 11, "bold"),
        )
        self._pulse.pack(side="left", padx=(8, 4))

        self._title = tk.Label(
            header, text="Zen", fg=COLOR_FG, bg=COLOR_BG,
            font=("Segoe UI", 9, "bold"),
        )
        self._title.pack(side="left")

        self._project_label = tk.Label(
            header, text="", fg=COLOR_FG_DIM, bg=COLOR_BG,
            font=("Segoe UI", 8),
        )
        self._project_label.pack(side="left", padx=(8, 0))

        self._hint = tk.Label(
            header, text=f"{HOTKEY_LABEL}  •  ⏎ send  •  Esc hide  •  Tab @complete",
            fg=COLOR_FG_DIM, bg=COLOR_BG, font=("Segoe UI", 8),
        )
        self._hint.pack(side="right", padx=8)

        # ---- Input area (multi-line Text) ----
        input_wrap = tk.Frame(outer, bg=COLOR_BG)
        input_wrap.pack(fill="x", padx=8, pady=(6, 4))
        self.input = tk.Text(
            input_wrap, height=3, wrap="word",
            font=("Segoe UI", 10),
            bg=COLOR_BG_INPUT, fg=COLOR_FG,
            insertbackground=COLOR_FG,
            selectbackground=COLOR_BORDER,
            relief="flat", bd=0,
            highlightthickness=1,
            highlightbackground=COLOR_BORDER,
            highlightcolor=COLOR_ACCENT,
            padx=8, pady=6,
        )
        self.input.pack(fill="x")
        self._input_wrap = input_wrap

        # ---- Autocomplete popup (hidden until invoked) ----
        self._ac_listbox: Optional[tk.Listbox] = None
        self._ac_frame: Optional[tk.Frame] = None

        # ---- Status strip ----
        self._status_var = tk.StringVar(value="")
        status = tk.Label(
            outer, textvariable=self._status_var,
            fg=COLOR_FG_DIM, bg=COLOR_BG,
            font=("Segoe UI", 8), anchor="w",
        )
        status.pack(fill="x", padx=10, pady=(0, 2))

        # ---- Response panel ----
        resp_frame = tk.Frame(
            outer, bg=COLOR_BG_RESPONSE,
            highlightthickness=1, highlightbackground="#222222",
        )
        resp_frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        self._response_frame = resp_frame

        self._response = tk.Text(
            resp_frame, wrap="word",
            font=("Segoe UI", 9),
            bg=COLOR_BG_RESPONSE, fg=COLOR_FG_RESPONSE,
            relief="flat", bd=0,
            padx=8, pady=6,
            state="disabled", cursor="arrow",
            height=8,
        )
        self._response.pack(fill="both", expand=True)

        # Track current text inside the (disabled) Text widget.
        self._response_text = ""

    # ------------------------------------------------------------------
    # Drag handling
    # ------------------------------------------------------------------

    def _wire_drag(self) -> None:
        """Make header a drag handle. Press on header, drag window."""
        self._drag_x = 0
        self._drag_y = 0

        def on_press(e):
            self._drag_x = e.x_root
            self._drag_y = e.y_root

        def on_drag(e):
            dx = e.x_root - self._drag_x
            dy = e.y_root - self._drag_y
            x = self.root.winfo_x() + dx
            y = self.root.winfo_y() + dy
            self.root.geometry(f"+{x}+{y}")
            self._drag_x = e.x_root
            self._drag_y = e.y_root

        for w in (self._header, self._title, self._pulse, self._hint, self._project_label):
            w.bind("<Button-1>", on_press)
            w.bind("<B1-Motion>", on_drag)
            w.bind("<Double-Button-1>", lambda e: self.dismiss())

    # ------------------------------------------------------------------
    # Key bindings
    # ------------------------------------------------------------------

    def _wire_keys(self) -> None:
        """Enter / Cmd+Enter / Tab / Esc."""
        self.input.bind("<Return>", self._on_enter)
        self.input.bind("<Escape>", lambda e: self.dismiss())
        self.input.bind("<Tab>", self._on_tab)
        self.input.bind("<KeyRelease>", self._on_keyrelease)
        # Ctrl+Enter always submits, even if @<project> is mid-typed.
        self.input.bind("<Control-Return>", self._on_enter)

    def _on_enter(self, event=None):
        """Enter sends when input looks ready; otherwise inserts newline."""
        text = self.input.get("1.0", "end-1c").strip()
        if not text:
            return "break"
        # If autocomplete popup is open, Enter accepts the highlighted entry.
        if self._autocomplete_visible and self._autocomplete_index >= 0:
            self._accept_autocomplete(self._autocomplete_matches[self._autocomplete_index])
            return "break"
        # Otherwise, submit.
        self._submit(text)
        return "break"

    def _on_tab(self, event=None):
        """Tab cycles / accepts autocomplete."""
        if not self._autocomplete_matches:
            return "break"
        if self._autocomplete_index < 0:
            self._autocomplete_index = 0
        else:
            self._autocomplete_index = (self._autocomplete_index + 1) % len(self._autocomplete_matches)
        self._highlight_autocomplete()
        return "break"

    def _on_keyrelease(self, event=None):
        """After any key, refresh autocomplete state."""
        self._refresh_autocomplete()

    # ------------------------------------------------------------------
    # @project autocomplete
    # ------------------------------------------------------------------

    def _refresh_autocomplete(self):
        """Detect an `@<partial>` token at the cursor; show popup."""
        text = self.input.get("1.0", "end-1c")
        cursor = self.input.index("insert")
        # Convert "line.col" to linear index.
        line, col = cursor.split(".")
        line = int(line)
        col = int(col)
        # Walk backward from cursor on the current line to find an `@`.
        line_start_idx = self.input.index(f"{line}.0")
        line_text = self.input.get(line_start_idx, f"{line}.end")
        # Compute char offset of cursor within line_text.
        caret = col
        prefix = line_text[:caret]
        at_idx = prefix.rfind("@")
        if at_idx < 0:
            self._hide_autocomplete()
            return

        # Token after `@` must not contain whitespace.
        token = prefix[at_idx + 1:]
        if any(ch.isspace() for ch in token):
            self._hide_autocomplete()
            return

        # Match projects that start with `token` (case-insensitive).
        token_lc = token.lower()
        matches = [pid for pid in PROJECT_IDS if pid.lower().startswith(token_lc)]

        # If exactly one match and the token already equals it, no popup.
        if matches == [token] and token:
            self._hide_autocomplete()
            return

        self._autocomplete_matches = matches
        self._autocomplete_anchor = at_idx  # within the current line
        self._autocomplete_index = 0 if matches else -1

        if not matches:
            self._hide_autocomplete()
            return

        self._show_autocomplete()

    def _show_autocomplete(self):
        """Render the autocomplete popup beneath the input."""
        # Remove old popup if present.
        self._hide_autocomplete()

        matches = self._autocomplete_matches
        if not matches:
            return

        frame = tk.Frame(
            self._input_wrap, bg=COLOR_BG,
            highlightthickness=1, highlightbackground=COLOR_ACCENT,
        )
        # Place it just below the input widget.
        frame.place(in_=self.input, relx=0, rely=1.0, anchor="nw",
                    x=0, y=2, relwidth=1.0)
        lb = tk.Listbox(
            frame, height=min(5, len(matches)),
            bg=COLOR_BG_INPUT, fg=COLOR_FG,
            selectbackground=COLOR_ACCENT, selectforeground="#ffffff",
            font=("Segoe UI", 9), relief="flat", bd=0,
            highlightthickness=0, activestyle="none",
            exportselection=False,
        )
        lb.pack(fill="x")
        for m in matches:
            label = PROJECT_BY_ID[m][1] if m in PROJECT_BY_ID else m
            lb.insert("end", f"@{m}    — {label}")
        lb.selection_clear(0, "end")
        lb.selection_set(0)
        lb.activate(0)
        lb.bind("<Button-1>", self._on_ac_click)
        lb.bind("<Key>", lambda e: "break")  # don't propagate
        self._ac_frame = frame
        self._ac_listbox = lb
        self._autocomplete_visible = True

    def _highlight_autocomplete(self):
        if not self._ac_listbox:
            return
        self._ac_listbox.selection_clear(0, "end")
        self._ac_listbox.selection_set(self._autocomplete_index)
        self._ac_listbox.activate(self._autocomplete_index)

    def _on_ac_click(self, event):
        if not self._ac_listbox:
            return
        idx = self._ac_listbox.nearest(event.y)
        if 0 <= idx < len(self._autocomplete_matches):
            self._accept_autocomplete(self._autocomplete_matches[idx])
        return "break"

    def _accept_autocomplete(self, project_id: str):
        """Replace the `@<partial>` token at the cursor with `@<project>`."""
        line, col = self._index_to_line_col(self.input.index("insert"))
        line_start = self.input.index(f"{line}.0")
        line_text = self.input.get(line_start, f"{line}.end")
        caret = col
        prefix = line_text[:caret]
        at_idx = prefix.rfind("@")
        if at_idx < 0:
            self._hide_autocomplete()
            return
        # Replace [at_idx:caret] with @<project> + space.
        new_prefix = prefix[:at_idx] + f"@{project_id} "
        new_line = new_prefix + line_text[caret:]
        self.input.delete(line_start, f"{line}.end")
        self.input.insert(line_start, new_line)
        # Place cursor right after the inserted token.
        new_col = at_idx + 1 + len(project_id) + 1
        self.input.mark_set("insert", f"{line}.{new_col}")
        self.input.focus_set()
        self._hide_autocomplete()
        self._update_project_label(project_id)

    def _hide_autocomplete(self):
        self._autocomplete_visible = False
        self._autocomplete_matches = []
        self._autocomplete_index = -1
        if self._ac_frame is not None:
            try:
                self._ac_frame.destroy()
            except Exception:
                pass
            self._ac_frame = None
            self._ac_listbox = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _index_to_line_col(self, idx: str) -> tuple[int, int]:
        line, col = idx.split(".")
        return int(line), int(col)

    def _set_status(self, text: str, color: str = COLOR_FG_DIM):
        self._status_var.set(text)

    def _set_pulse(self, color: str):
        try:
            self._pulse.config(fg=color)
        except Exception:
            pass

    def _set_response(self, text: str):
        self._response_text = text
        self._response.config(state="normal")
        self._response.delete("1.0", "end")
        self._response.insert("1.0", text)
        self._response.config(state="disabled")

    def _update_project_label(self, project_id: Optional[str]):
        if project_id and project_id in PROJECT_BY_ID:
            label = PROJECT_BY_ID[project_id][1]
            self._project_label.config(text=f"→ {label}")
        else:
            self._project_label.config(text="")

    # ------------------------------------------------------------------
    # Submit / network
    # ------------------------------------------------------------------

    def _extract_project(self, text: str) -> Optional[str]:
        """Return the project id if text starts with `@<project>`, else None."""
        text = text.lstrip()
        if not text.startswith("@"):
            return None
        # Take the first whitespace-delimited token after @.
        first = text[1:].split(None, 1)[0] if len(text) > 1 else ""
        first = first.rstrip(",;:").lower()
        if first in PROJECT_BY_ID:
            return first
        return None

    def _submit(self, text: str):
        """Send the prompt to the agent OS server."""
        if self._inflight is not None and self._inflight.is_alive():
            self._set_status("busy — wait for current reply", COLOR_PULSE_BUSY)
            return

        project = self._extract_project(text)
        if not project:
            self._set_status(
                "start with @<project> (Tab to complete): zen, kiyosaki, minato, nami, rin, toji, kazuki, zen-os",
                COLOR_PULSE_ERR,
            )
            return

        # Strip the @<project> prefix for the actual prompt body.
        body = text.lstrip()
        body = body[1 + len(project):].lstrip()

        self._set_status(f"sending to @{project}…", COLOR_PULSE_BUSY)
        self._set_pulse(COLOR_PULSE_BUSY)
        self._status = STATUS_BUSY
        self._set_response("(waiting for response…)")

        self._inflight = threading.Thread(
            target=self._post_thread, args=(project, body), daemon=True,
        )
        self._inflight.start()

    def _post_thread(self, project: str, body: str):
        """Worker thread — POST to /api/zen/chat (Discord + Hermes dispatch).

        Per the user's architecture spec, Discord is the LLM frontend.
        The server posts the user's prompt to Discord, calls Hermes
        directly for a reply, and posts the reply to Discord too. The
        overlay shows the summary inline.
        """
        try:
            if not _server_reachable():
                raise ConnectionError("server not reachable")
            result = _post_json(CHAT_ENDPOINT, {
                "target": project,
                "prompt": body,
            }, timeout=300.0)  # allow time for two hermes calls (reply + summary)
            if not result.get("ok"):
                raise RuntimeError(result.get("error", "post failed"))

            summary = result.get("summary") or ""
            server_reply = result.get("server_reply") or ""
            # Prefer the LLM summary; fall back to the full reply.
            display = summary or server_reply
            if not display:
                display = (
                    f"(no reply — discord={'ok' if result.get('discord_delivered') else 'fail'})"
                )

            self._result_queue.put({
                "ok": True,
                "project": project,
                "response": display,
                "session": result.get("session", project),
                "queued": True,
                "discord_delivered": result.get("discord_delivered", False),
                "reply_posted": result.get("server_reply_delivered", False),
            })
        except Exception as e:
            self._result_queue.put({
                "ok": False,
                "project": project,
                "error": str(e),
            })

    def _tick(self):
        """Poll the result queue every 100ms; route to UI."""
        try:
            while True:
                msg = self._result_queue.get_nowait()
                self._handle_result(msg)
        except queue.Empty:
            pass
        self.root.after(100, self._tick)

    def _handle_result(self, msg: dict):
        """Update UI based on async result message."""
        project = msg.get("project", "")
        if msg.get("ok"):
            self._status = STATUS_OK
            self._set_pulse(COLOR_PULSE_OK)
            response_text = msg.get("response", "").strip() or "(no response)"
            self._set_response(response_text)
            session = msg.get("session", project)
            channel = PROJECT_BY_ID.get(project, (None, None, project))[2]
            flags = []
            if msg.get("discord_delivered"):
                flags.append("discord✓")
            if msg.get("reply_posted"):
                flags.append("reply✓")
            flag_str = ("•" + " ".join(flags)) if flags else ""
            self._set_status(
                f"@{session} #{channel}  {flag_str}".strip(),
                COLOR_PULSE_OK,
            )
        else:
            self._status = STATUS_ERR
            self._set_pulse(COLOR_PULSE_ERR)
            err = msg.get("error", "unknown error")
            fallback_channel = PROJECT_BY_ID.get(project, (None, None, project))[2]
            self._set_response(
                f"Agent offline — try Discord #{fallback_channel} directly.\n\n"
                f"({err})"
            )
            self._set_status(f"offline — #{fallback_channel}", COLOR_PULSE_ERR)

    # ------------------------------------------------------------------
    # History (auto-fetch on @<project>)
    # ------------------------------------------------------------------

    def fetch_history(self, project: str, limit: int = 5):
        """Best-effort fetch of recent history for a project. Non-blocking."""
        def _worker():
            try:
                data = _get_json(
                    HISTORY_ENDPOINT_TEMPLATE.format(project=project),
                    timeout=5.0,
                )
                msgs = data.get("messages", [])
                self._history_cache[project] = msgs[-limit:]
                self._result_queue.put({
                    "_history": True, "project": project,
                    "messages": msgs[-limit:],
                })
            except Exception as e:
                self._result_queue.put({
                    "_history": True, "project": project,
                    "error": str(e),
                })
        threading.Thread(target=_worker, daemon=True).start()

    def _handle_history(self, project: str, msgs: list[dict]):
        """Render cached history into response panel (when input is empty)."""
        if not msgs:
            self._set_response(f"(no prior history for @{project})")
            return
        lines = [f"— recent @{project} —"]
        for m in msgs:
            role = m.get("role", "?")
            content = (m.get("content") or "").strip().replace("\n", " ")
            if len(content) > 120:
                content = content[:117] + "…"
            lines.append(f"{role}: {content}")
        self._set_response("\n".join(lines))

    # ------------------------------------------------------------------
    # Visibility / hotkey
    # ------------------------------------------------------------------

    def summon(self):
        """Show window, focus input, position top-right."""
        self._position_top_right()
        self.root.deiconify()
        self.root.attributes("-topmost", True)
        self.root.lift()
        self.input.focus_set()
        # If empty and we have a project label cached, fetch history.
        if not self.input.get("1.0", "end-1c").strip():
            pass  # no-op: project is detected on submit, not on summon

    def dismiss(self):
        """Hide window, return focus to whatever was foregrounded."""
        self._hide_autocomplete()
        self.root.withdraw()

    def _wire_hotkey(self):
        """Register Ctrl+Shift+Z via the `keyboard` library.

        Tk's bind('<Control-Shift-z>') only fires when the Tk window has
        focus. The `keyboard` library hooks a low-level Windows hook so
        the hotkey works system-wide. The callback marshals back to the
        Tk thread via `root.after`.
        """
        try:
            import keyboard  # provided by hermes venv
        except Exception as e:
            print(f"[zen_overlay] keyboard import failed: {e}", file=sys.stderr)
            return  # hotkey disabled — user can still summon via bat

        def _hotkey_cb():
            try:
                self.root.after(0, self._on_hotkey)
            except Exception:
                pass

        try:
            keyboard.add_hotkey(
                "ctrl+shift+z",
                _hotkey_cb,
                suppress=False,
                trigger_on_release=True,
            )
        except Exception as e:
            print(f"[zen_overlay] hotkey registration failed: {e}",
                  file=sys.stderr)

    def _on_hotkey(self):
        """Toggle visibility on hotkey."""
        if self.root.state() == "withdrawn":
            self.summon()
        else:
            self.dismiss()

    # ------------------------------------------------------------------
    # Mainloop
    # ------------------------------------------------------------------

    def run(self):
        self.root.mainloop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    overlay = ZenOverlay()
    overlay.run()


if __name__ == "__main__":
    main()