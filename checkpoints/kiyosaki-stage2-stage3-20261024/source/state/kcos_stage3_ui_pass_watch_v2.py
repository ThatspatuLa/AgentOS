#!/usr/bin/env python3
"""Zero-token MT5 Tester UI pass counter watcher.

Reads the visible bottom-right Tester counter (e.g. ``12 262 of 12 544``)
from a screenshot, and optionally invokes the existing controlled Stop UI
helper at the configured ceiling. Use --dry-run for non-invasive testing.
"""
from __future__ import annotations

import argparse
import ctypes
import re
import time
from pathlib import Path
from ctypes import wintypes

from PIL import Image, ImageGrab, ImageOps, ImageFilter
from kcos_stage3_duration_policy_v2 import parse_mt5_elapsed_estimate
import pytesseract

from kcos_stage3_stop_v2 import click_stop

_TESSERACT = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
if _TESSERACT.is_file():
    pytesseract.pytesseract.tesseract_cmd = str(_TESSERACT)

COUNTER_RE = re.compile(r"(\d[\d ,.]*\d)\s*(?:of|/|out of)\s*(\d[\d ,.]*\d)", re.I)
DEFAULT_LIMIT = 11000


class _RECT(ctypes.Structure):
    _fields_ = [("left", wintypes.LONG), ("top", wintypes.LONG),
                ("right", wintypes.LONG), ("bottom", wintypes.LONG)]


_user32 = ctypes.WinDLL("user32", use_last_error=True) if __import__("os").name == "nt" else None
if _user32 is not None:
    _user32.IsWindowVisible.argtypes = [wintypes.HWND]
    _user32.IsWindowVisible.restype = wintypes.BOOL
    _user32.GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
    _user32.GetWindowThreadProcessId.restype = wintypes.DWORD
    _user32.GetWindowRect.argtypes = [wintypes.HWND, ctypes.POINTER(_RECT)]
    _user32.GetWindowRect.restype = wintypes.BOOL
    _user32.EnumChildWindows.argtypes = [wintypes.HWND, ctypes.WINFUNCTYPE(
        wintypes.BOOL, wintypes.HWND, wintypes.LPARAM), wintypes.LPARAM]
    _user32.EnumChildWindows.restype = wintypes.BOOL
    _user32.GetClassNameW.argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]
    _user32.GetClassNameW.restype = ctypes.c_int
    _user32.GetWindowTextW.argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]
    _user32.GetWindowTextW.restype = ctypes.c_int


def _visible_windows_for_pid(pid: int) -> list[tuple[int, tuple[int, int, int, int]]]:
    if _user32 is None:
        return []
    found: list[tuple[int, tuple[int, int, int, int]]] = []
    callback_type = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

    def visit(hwnd: int, _extra: int) -> bool:
        if not _user32.IsWindowVisible(hwnd):
            return True
        owner = wintypes.DWORD()
        _user32.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
        if owner.value != int(pid):
            return True
        rect = _RECT()
        if not _user32.GetWindowRect(hwnd, ctypes.byref(rect)):
            return True
        bounds = (rect.left, rect.top, rect.right, rect.bottom)
        if bounds[2] > bounds[0] and bounds[3] > bounds[1]:
            found.append((int(hwnd), bounds))
        return True

    callback = callback_type(visit)
    _user32.EnumWindows(callback, 0)
    return found


def _tester_button_state(hwnd: int) -> str | None:
    """Read the exact bound window's Tester button without OCR or pywin32."""
    if _user32 is None:
        return None
    buttons: list[str] = []
    callback_type = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

    def visit(child: int, _extra: int) -> bool:
        class_name = ctypes.create_unicode_buffer(128)
        title = ctypes.create_unicode_buffer(128)
        _user32.GetClassNameW(child, class_name, len(class_name))
        _user32.GetWindowTextW(child, title, len(title))
        if class_name.value.casefold() == "button" and title.value.strip().casefold() in {"start", "stop"}:
            buttons.append(title.value.strip().upper())
        return True

    callback = callback_type(visit)
    _user32.EnumChildWindows(hwnd, callback, 0)
    if buttons.count("STOP") == 1 and not buttons.count("START"):
        return "STOP"
    if buttons.count("START") == 1 and not buttons.count("STOP"):
        return "START"
    return None


def window_for_pid(pid: int) -> tuple[int, tuple[int, int, int, int]]:
    found = _visible_windows_for_pid(pid)
    if not found:
        raise RuntimeError(f"no visible window found for terminal PID {pid}")
    return max(found, key=lambda x: (x[1][2] - x[1][0]) * (x[1][3] - x[1][1]))


def read_tester_timing(image: Image.Image) -> tuple[dict | None, str]:
    """Read the clock from a tight bottom-right crop, away from the results pane."""
    width, height = image.size
    # On MT5 the elapsed/estimated clock sits immediately left of the Tester
    # Stop button. Keep the crop narrow so large numbers elsewhere in the
    # optimization table cannot dominate OCR's line segmentation.
    crop = image.crop((int(width * .70), int(height * .93),
                       int(width * .95), int(height * .99)))
    gray = ImageOps.grayscale(crop).resize((crop.width * 8, crop.height * 8))
    gray = gray.filter(ImageFilter.SHARPEN)
    observations = []
    for config in ("--psm 6", "--psm 11"):
        text = pytesseract.image_to_string(gray, config=config).strip()
        observations.append(text)
        timing = parse_mt5_elapsed_estimate(text)
        if timing is not None:
            return timing, text
    return None, "\n".join(observations)


def read_counter(pid: int) -> dict:
    hwnd, rect = window_for_pid(pid)
    # Pillow's Windows window capture uses PrintWindow without requiring
    # pywin32's optional win32ui extension (which is absent in the Hermes venv).
    image = ImageGrab.grab(window=hwnd)
    width, height = image.size
    # MT5 places ``current of total`` in the bottom-right status bar, left of Stop.
    crop = image.crop((int(width * .35), int(height * .90), int(width * .98), int(height * .998)))
    gray = ImageOps.grayscale(crop).resize((crop.width * 5, crop.height * 5))
    gray = gray.filter(ImageFilter.SHARPEN)
    text = pytesseract.image_to_string(gray, config="--psm 6")
    timing, timing_text = read_tester_timing(image)
    if timing is None:
        timing = parse_mt5_elapsed_estimate(text)
    # The Tester button is immediately to the right of the counter.  Reading
    # it distinguishes a user-stopped run (Start visible) from an active run
    # (Stop visible) while keeping the existing counter OCR unchanged.
    button_crop = image.crop((int(width * .94), int(height * .925), int(width * .995), int(height * .995)))
    button_gray = ImageOps.grayscale(button_crop).resize((button_crop.width * 5, button_crop.height * 5))
    button_text = pytesseract.image_to_string(button_gray, config="--psm 7").strip()
    button_state = _tester_button_state(hwnd)
    if button_state is None:
        button_state = "START" if re.search(r"\bstart\b", button_text, re.I) else ("STOP" if re.search(r"\bstop\b", button_text, re.I) else None)
    if button_state is None:
        button_state = "START" if re.search(r"\bstart\b", text, re.I) else ("STOP" if re.search(r"\bstop\b", text, re.I) else None)
    # The wide status-bar crop also contains optimization-table values. Read
    # the pass pair only from the tight Tester timing strip, whose line is
    # formatted as "current of total - elapsed / estimate".
    timing_pairs = set()
    for line in timing_text.splitlines():
        if not re.search(r"\d{1,2}:\d{2}:\d{2}", line):
            continue
        candidate = COUNTER_RE.search(line)
        if candidate:
            timing_pairs.add((
                int(re.sub(r"[^0-9]", "", candidate.group(1))),
                int(re.sub(r"[^0-9]", "", candidate.group(2))),
            ))
    if len(timing_pairs) != 1:
        return {"ok": False, "pid": pid, "hwnd": hwnd, "text": text.strip(), "button_text": button_text,
                "button_state": button_state, "rect": rect, "tester_timing": timing,
                "tester_timing_text": timing_text,
                "detail": "trusted Tester timing strip did not yield one unambiguous pass counter"}
    current, total = next(iter(timing_pairs))
    if total <= 0 or current <= 0 or current > total:
        return {"ok": False, "pid": pid, "hwnd": hwnd, "current": current,
                "total": total, "counter_source": "tester_timing_crop",
                "detail": "OCR counter is impossible; current must be within total",
                "text": text.strip(), "button_text": button_text, "button_state": button_state,
                "rect": rect, "tester_timing": timing, "tester_timing_text": timing_text}
    return {"ok": True, "pid": pid, "hwnd": hwnd, "current": current,
            "total": total, "counter_source": "tester_timing_crop",
            "text": text.strip(), "button_text": button_text,
            "button_state": button_state, "rect": rect, "tester_timing": timing,
            "tester_timing_text": timing_text}


def watch(pid: int, limit: int, interval: float, dry_run: bool, once: bool) -> int:
    stop_sent = False
    over_limit_reads = 0
    while True:
        result = read_counter(pid)
        result["limit"] = limit
        result["dry_run"] = dry_run
        print(result, flush=True)
        if (result.get("ok") and result["current"] >= limit
                and result["current"] == result["total"]):
            over_limit_reads += 1
            confirmed = dry_run or once or over_limit_reads >= 2
            if confirmed:
                if dry_run:
                    print({"event": "PASS_LIMIT_REACHED", "action": "DRY_RUN_NO_STOP", **result}, flush=True)
                    return 0
                if not stop_sent:
                    print({"event": "PASS_LIMIT_REACHED", "action": "CONTROLLED_STOP_REQUESTED", **result}, flush=True)
                    print(click_stop(pid), flush=True)
                    stop_sent = True
                return 0
        else:
            over_limit_reads = 0
        if once:
            return 0 if result.get("ok") else 2
        time.sleep(interval)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pid", type=int, required=True)
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()
    return watch(args.pid, args.limit, args.interval, args.dry_run, args.once)


if __name__ == "__main__":
    raise SystemExit(main())
