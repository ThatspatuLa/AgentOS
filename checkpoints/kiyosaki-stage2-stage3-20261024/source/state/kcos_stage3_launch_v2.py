#!/usr/bin/env python3
"""Build, launch and register a deterministic Stage 3 MT5 optimization."""

from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil
try:
    from PIL import ImageGrab
    import pytesseract
    _TESSERACT = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    if _TESSERACT.is_file():
        pytesseract.pytesseract.tesseract_cmd = str(_TESSERACT)
except ImportError:  # UI journal OCR is optional; file evidence remains mandatory.
    ImageGrab = None
    pytesseract = None

import kcos_stage3_prepare_v2 as preparer
import kcos_input_contract_v2 as input_contract
from kcos_stage3_common_v2 import atomic_json, load_object, require, sha256, utc_now


INSTALLED_EIGHTCAP_TERMINAL = Path(r"C:\Program Files\EightCap MetaTrader 5\terminal64.exe")
EIGHTCAP_TERMINAL = Path(__file__).resolve().parent.parent / "state" / "mt5_stage45_portable" / "terminal64.exe"
EIGHTCAP_DATA_DIR = Path(r"C:\Users\Tyson\AppData\Roaming\MetaQuotes\Terminal\608AB61EFF9C7B3585EC08B8CF6800E3")
# Stage 3 must never attach to the live singleton. Reuse the authenticated
# isolated portable root already provisioned for KCOS tester work.
STAGE3_PORTABLE_ROOT = Path(__file__).resolve().parent.parent / "state" / "mt5_stage45_portable"
STAGE3_PORTABLE_TERMINAL = STAGE3_PORTABLE_ROOT / "terminal64.exe"
STAGE3_PORTABLE_DATA_DIR = STAGE3_PORTABLE_ROOT
EIGHTCAP_AGENTS_REGISTRY = STAGE3_PORTABLE_DATA_DIR / "config" / "agents.dat"
DEFAULT_TERMINAL = STAGE3_PORTABLE_TERMINAL
DEFAULT_DATA_DIR = STAGE3_PORTABLE_DATA_DIR
MINATO_V2_TARGET_EA = "B1_1_ConditionalTailExpectation_ES_Crypto_RegimeGate_v2_CSV"
MINATO_V2_VERSION = "MINATO_BASE_SCORE_V2.0"
MINATO_V2_MANIFEST = Path(__file__).with_name("minato_v2_fleet_manifest.json")
STAGE3_AUTOSTART = True  # Prior proven Tester Start handoff; authenticate with workers/journal/OPT evidence.
WORKER_ADDRESS_RE = re.compile(r"/address:([^\s\"]+)", re.I)
JOURNAL_ERROR_RE = re.compile(r"^\s*\S+\s+2\s+\d{2}:\d{2}:\d{2}\.\d+\s+[^\t ]+\s+(.+?)\s*$", re.I)
NO_OPTIMIZED_PARAMETERS_RE = re.compile(r"no optimized parameter selected", re.I)
INVALID_STOPS_RE = re.compile(r"\bInvalid stops\b", re.I)
NO_EXPERT_IN_START_CONFIG_RE = re.compile(r"no expert specified in the start config", re.I)
GENETIC_INVALID_PARAMETER_PASS_RE = re.compile(
    r"\bgenetic pass\s+\([^)]*\)\s+tested with error\s+['\"]incorrect input parameters['\"]",
    re.I,
)
INPUT_GRID_ERROR_RE = re.compile(
    # The numeric boundary must include the whole decimal token. A plain ``\b``
    # after 0 also matches the start of a valid decimal such as 0.251; OCR of
    # the optimization grid produced exactly that false rejection in the live run.
    r"(?:step\s*[:=]?\s*[+-]?0+(?:\.0+)?(?=$|[\s,;])|optimization\s+cannot\s+be\s+started|"
    r"invalid\s+(?:tester\s+)?input|start\s*=.*step\s*=.*stop\s*=)", re.I)


class TesterJournalError(RuntimeError):
    def __init__(self, diagnostic: dict):
        self.diagnostic = diagnostic
        code = diagnostic.get("code") or "TESTER_JOURNAL_ERROR"
        lines = diagnostic.get("error_lines") or []
        detail = " | ".join(lines[-5:]) or "Tester journal reported an error"
        super().__init__(f"MT5 Tester journal rejected Stage 3 startup [{code}]: {detail}")




def move_terminal_to_secondary_monitor(terminal_pid: int) -> bool:
    """Move the MT5 top-level window to monitor 2 without activating it."""
    if os.name != "nt":
        return False
    u = ctypes.windll.user32
    windows = []
    @ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
    def cb(hwnd, _):
        owner = wintypes.DWORD()
        u.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
        if owner.value == int(terminal_pid) and u.IsWindowVisible(hwnd):
            windows.append(hwnd)
        return True
    u.EnumWindows(cb, 0)
    if not windows:
        return False
    # Current two-monitor layout: primary 0..2560, secondary 2560..3928.
    # SWP_NOACTIVATE prevents this placement from stealing the user's focus.
    return bool(u.SetWindowPos(windows[0], 0, 2600, 100, 1200, 800,
                               0x0010 | 0x0004))


class _NativeGui:
    """Small user32 adapter available in Hermes' standard Python runtime."""

    def __init__(self, user):
        self.user = user
        for name in ('IsWindowVisible', 'IsWindowEnabled', 'GetDlgCtrlID'):
            getattr(user, name).argtypes = [wintypes.HWND]
        user.IsWindowEnabled.restype = wintypes.BOOL
        user.GetParent.argtypes = [wintypes.HWND]
        user.GetParent.restype = wintypes.HWND
        for name in ('GetWindowTextW', 'GetClassNameW'):
            getattr(user, name).argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]

    def _enumerate(self, callback, value, parent=None):
        @ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
        def native(hwnd, _):
            callback(hwnd, value)
            return True
        if parent is None:
            self.user.EnumWindows(native, 0)
        else:
            self.user.EnumChildWindows(parent, native, 0)

    def EnumWindows(self, callback, value):
        self._enumerate(callback, value)

    def EnumChildWindows(self, parent, callback, value):
        self._enumerate(callback, value, parent)

    def IsWindowVisible(self, hwnd):
        return bool(self.user.IsWindowVisible(hwnd))

    def IsWindowEnabled(self, hwnd):
        return bool(self.user.IsWindowEnabled(hwnd))

    def _text(self, name, hwnd):
        buffer = ctypes.create_unicode_buffer(2048)
        getattr(self.user, name)(hwnd, buffer, len(buffer))
        return buffer.value

    def GetClassName(self, hwnd):
        return self._text('GetClassNameW', hwnd)

    def GetWindowText(self, hwnd):
        return self._text('GetWindowTextW', hwnd)

    def GetDlgCtrlID(self, hwnd):
        return self.user.GetDlgCtrlID(hwnd)

    def GetParent(self, hwnd):
        return self.user.GetParent(hwnd)


class _TesterControls:
    """Native controls scoped to one authenticated terminal; no screen OCR."""

    def __init__(self, terminal_pid: int, ini_path: Path,
                 expected_terminal_path: Path | None = None):
        self.terminal_pid = int(terminal_pid)
        self.user = ctypes.windll.user32
        self.gui = _NativeGui(self.user)
        self.user.SendMessageW.argtypes = [wintypes.HWND, wintypes.UINT,
                                          wintypes.WPARAM, wintypes.LPARAM]
        self.user.SendMessageW.restype = ctypes.c_ssize_t
        process = psutil.Process(int(terminal_pid))
        expected_terminal = Path(expected_terminal_path or STAGE3_PORTABLE_TERMINAL).resolve()
        require(Path(process.exe()).resolve() == expected_terminal,
                "schema binding requires the exact portable terminal")
        require(any(str(arg).casefold() == f"/config:{ini_path.resolve()}".casefold()
                    for arg in process.cmdline()), "schema binding INI identity mismatch")
        self.tops = []
        def top(hwnd, _):
            owner = wintypes.DWORD()
            self.user.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
            if owner.value == int(terminal_pid) and self.gui.IsWindowVisible(hwnd):
                self.tops.append(hwnd)
        self.gui.EnumWindows(top, None)
        self.children = []
        for hwnd in self.tops:
            self.gui.EnumChildWindows(hwnd, lambda child, _: self.children.append(child), None)

    def stopped(self) -> bool:
        # MT5 can leave a disabled Stop button visible when Tester is idle.
        # Only an enabled Stop button proves an active optimization.
        return not any(self.gui.IsWindowVisible(hwnd) and
                       self.gui.IsWindowEnabled(hwnd) and
                       self.gui.GetClassName(hwnd) == "Button" and
                       self.gui.GetWindowText(hwnd).strip().casefold() == "stop"
                       for hwnd in self.children)

    def expert_choices(self) -> list[tuple[int, int, str]]:
        choices = []
        for hwnd in self.children:
            # Hidden terminal windows can retain a stale Tester expert list.
            # Only inspect controls currently visible in the authenticated UI.
            if (self.gui.GetClassName(hwnd) != "ComboBox"
                    or not self.gui.IsWindowVisible(hwnd)):
                continue
            count = self.user.SendMessageW(hwnd, 0x0146, 0, 0)  # CB_GETCOUNT
            if not 0 < count <= 1024:
                continue
            for index in range(count):
                buf = ctypes.create_unicode_buffer(2048)
                self.user.SendMessageW(hwnd, 0x0148, index, ctypes.addressof(buf))
                choices.append((hwnd, index, buf.value))
        return choices

    def current_expert_index(self, hwnd: int) -> int:
        """Return the selected expert without triggering an input-grid refresh."""
        return int(self.user.SendMessageW(hwnd, 0x0147, 0, 0))  # CB_GETCURSEL

    def select_tester_timeframe(self, expected: str) -> bool:
        """Set and verify the Strategy Tester Period combo from the admitted INI."""
        expected = str(expected).strip().upper()
        period_values = {"M1", "M2", "M3", "M4", "M5", "M6", "M10", "M12",
                         "M15", "M20", "M30", "H1", "H2", "H3", "H4", "H6",
                         "H8", "H12", "DAILY", "WEEKLY", "MONTHLY"}
        matches = []
        for hwnd in self.children:
            if self.gui.GetClassName(hwnd) != "ComboBox":
                continue
            count = int(self.user.SendMessageW(hwnd, 0x0146, 0, 0))  # CB_GETCOUNT
            if not 0 < count <= 64:
                continue
            values = []
            for index in range(count):
                buf = ctypes.create_unicode_buffer(128)
                self.user.SendMessageW(hwnd, 0x0148, index, ctypes.addressof(buf))
                values.append(buf.value.strip().upper())
            if period_values.issubset(set(values)) and expected in values:
                matches.append((hwnd, values.index(expected)))
        if len(matches) != 1:
            self.timeframe_binding_detail = f"expected one Tester Period combo, found {len(matches)}"
            return False
        hwnd, index = matches[0]
        self.user.SendMessageW(hwnd, 0x014E, index, 0)  # CB_SETCURSEL
        control_id = self.gui.GetDlgCtrlID(hwnd)
        parent = self.gui.GetParent(hwnd)
        for notification in (9, 1, 8):
            self.user.SendMessageW(parent, 0x0111,
                                   (notification << 16) | control_id, hwnd)
        selected = int(self.user.SendMessageW(hwnd, 0x0147, 0, 0))  # CB_GETCURSEL
        verify = ctypes.create_unicode_buffer(128)
        if selected >= 0:
            self.user.SendMessageW(hwnd, 0x0148, selected, ctypes.addressof(verify))
        actual = verify.value.strip().upper()
        self.timeframe_binding_detail = f"expected {expected}; selected {actual}"
        return actual == expected

    def refresh_navigator(self) -> bool:
        """Ask the authenticated terminal to rescan its Navigator before binding.

        EX5 files are copied into the isolated portable data folder after Stage 2.
        MT5 can keep the tester's prior expert list until Navigator is refreshed.
        Use the documented F5 action with the Navigator tree focused, then restore
        the user's prior foreground window.
        """
        trees = [hwnd for hwnd in self.children
                 if self.gui.GetClassName(hwnd) == "SysTreeView32"
                 and self.gui.IsWindowVisible(hwnd)]
        if len(trees) != 1 or not self.tops:
            return False
        user = self.user
        previous = user.GetForegroundWindow()
        top = self.tops[0]
        owner = wintypes.DWORD()
        user.GetWindowThreadProcessId(top, ctypes.byref(owner))
        if owner.value != int(self.terminal_pid):
            # The containing top-level window was already authenticated against
            # terminal_pid in __init__; this guard also rejects stale HWND reuse.
            return False
        current_thread = ctypes.windll.kernel32.GetCurrentThreadId()
        attached = bool(user.AttachThreadInput(
            current_thread, int(user.GetWindowThreadProcessId(top, None)), True))
        try:
            user.SetForegroundWindow(top)
            user.BringWindowToTop(top)
            user.SetFocus(trees[0])
            if user.GetForegroundWindow() != top:
                return False
            user.keybd_event(0x74, 0, 0, 0)  # VK_F5: Navigator refresh
            user.keybd_event(0x74, 0, 0x0002, 0)
            time.sleep(0.25)
        finally:
            if attached:
                user.AttachThreadInput(current_thread,
                                       int(user.GetWindowThreadProcessId(top, None)), False)
            if previous and user.IsWindow(previous):
                user.SetForegroundWindow(previous)
        return True

    @staticmethod
    def _navigator_prefix_is_unique(label: str, expected_stem: str,
                                    sibling_stems: list[str]) -> bool:
        visible = str(label).strip().rstrip("….").casefold()
        expected = str(expected_stem).casefold()
        unique_sibling = sum(str(name).casefold().startswith(visible)
                             for name in sibling_stems) == 1
        return (expected.startswith(visible) and unique_sibling
                and (visible == expected or len(visible) >= 32))

    def _refresh_child_windows(self) -> None:
        # Opening the exact EA in Strategy Tester can create the Tester pane
        # after this controller was initialized. Refresh top-level windows as
        # well as children so subsequent combo reads do not use a stale handle set.
        tops = []
        def top(hwnd, _):
            owner = wintypes.DWORD()
            self.user.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
            if owner.value == self.terminal_pid and self.gui.IsWindowVisible(hwnd):
                tops.append(hwnd)
        self.gui.EnumWindows(top, None)
        self.tops = tops
        self.children = []
        for hwnd in self.tops:
            self.gui.EnumChildWindows(
                hwnd, lambda child, _: self.children.append(child), None)

    def show_navigator(self) -> bool:
        """Open the Navigator pane through the authenticated terminal menu."""
        if any(self.gui.GetClassName(hwnd) == "SysTreeView32"
               and self.gui.IsWindowVisible(hwnd) for hwnd in self.children):
            return True
        user = self.user
        user.GetMenuStringW.argtypes = [wintypes.HMENU, wintypes.UINT,
                                        wintypes.LPWSTR, ctypes.c_int, wintypes.UINT]
        user.GetMenuStringW.restype = ctypes.c_int
        user.GetMenu.argtypes = [wintypes.HWND]
        user.GetMenu.restype = wintypes.HMENU
        user.GetMenuItemCount.argtypes = [wintypes.HMENU]
        user.GetMenuItemCount.restype = ctypes.c_int
        user.GetMenuState.argtypes = [wintypes.HMENU, wintypes.UINT, wintypes.UINT]
        user.GetMenuState.restype = wintypes.UINT
        user.GetSubMenu.argtypes = [wintypes.HMENU, ctypes.c_int]
        user.GetSubMenu.restype = wintypes.HMENU
        user.GetMenuItemID.argtypes = [wintypes.HMENU, ctypes.c_int]
        user.GetMenuItemID.restype = wintypes.UINT
        top = self.tops[0] if self.tops else 0
        menu = user.GetMenu(top) if top else 0
        if not menu:
            # Some portable Strategy Tester windows expose no Win32 menu even
            # though MT5 still handles its Navigator shortcut. Only send the
            # shortcut after focusing the exact terminal PID, then restore the
            # user's previous foreground window.
            if not top:
                self.navigator_selection_detail = "Navigator hidden and terminal menu unavailable"
                return False
            previous = user.GetForegroundWindow()
            owner = wintypes.DWORD()
            thread_id = user.GetWindowThreadProcessId(top, ctypes.byref(owner))
            if owner.value != int(self.terminal_pid):
                self.navigator_selection_detail = "Navigator shortcut rejected for stale terminal window"
                return False
            current_thread = ctypes.windll.kernel32.GetCurrentThreadId()
            attached = bool(user.AttachThreadInput(current_thread, int(thread_id), True))
            try:
                user.SetForegroundWindow(top)
                user.BringWindowToTop(top)
                if user.GetForegroundWindow() != top:
                    self.navigator_selection_detail = "Navigator shortcut could not focus authenticated terminal"
                    return False
                user.keybd_event(0x11, 0, 0, 0)  # Ctrl+N: MT5 Navigator
                user.keybd_event(0x4E, 0, 0, 0)
                user.keybd_event(0x4E, 0, 0x0002, 0)
                user.keybd_event(0x11, 0, 0x0002, 0)
                time.sleep(0.35)
            finally:
                if attached:
                    user.AttachThreadInput(current_thread, int(thread_id), False)
                if previous and user.IsWindow(previous):
                    user.SetForegroundWindow(previous)
            self._refresh_child_windows()
            visible_trees = [hwnd for hwnd in self.children
                             if self.gui.GetClassName(hwnd) == "SysTreeView32"
                             and self.gui.IsWindowVisible(hwnd)]
            if len(visible_trees) != 1:
                self.navigator_selection_detail = (
                    f"Ctrl+N Navigator fallback found {len(visible_trees)} visible tree controls")
                return False
            self.navigator_selection_detail = "Navigator opened with authenticated Ctrl+N fallback"
            return True

        def menu_text(handle: int, index: int) -> str:
            buffer = ctypes.create_unicode_buffer(256)
            user.GetMenuStringW(handle, index, buffer, len(buffer), 0x0400)
            return buffer.value.replace("&", "").split("\t", 1)[0].strip()

        view = next((i for i in range(user.GetMenuItemCount(menu))
                     if menu_text(menu, i).casefold() == "view"), None)
        submenu = user.GetSubMenu(menu, view) if view is not None else 0
        if not submenu:
            self.navigator_selection_detail = "Navigator hidden and View menu unavailable"
            return False
        nav_index = next((i for i in range(user.GetMenuItemCount(submenu))
                          if menu_text(submenu, i).casefold().startswith("navigator")), None)
        if nav_index is None:
            self.navigator_selection_detail = "Navigator command not found in terminal View menu"
            return False
        state = int(user.GetMenuState(submenu, nav_index, 0x0400))
        if state != 0xFFFFFFFF and state & 0x0008:  # MF_CHECKED
            self.navigator_selection_detail = "Navigator menu says visible but its tree is unavailable"
            return False
        command_id = int(user.GetMenuItemID(submenu, nav_index))
        if command_id == 0xFFFFFFFF:
            self.navigator_selection_detail = "Navigator menu item has no command ID"
            return False
        user.PostMessageW(top, 0x0111, command_id, 0)  # WM_COMMAND
        time.sleep(0.35)
        self._refresh_child_windows()
        visible_trees = [hwnd for hwnd in self.children
                         if self.gui.GetClassName(hwnd) == "SysTreeView32"
                         and self.gui.IsWindowVisible(hwnd)]
        if len(visible_trees) != 1:
            self.navigator_selection_detail = (
                f"View/Navigator toggled but found {len(visible_trees)} visible tree controls")
            return False
        return True

    def test_exact_navigator_expert(self, expected: str,
                                    expected_ex5_sha256: str) -> bool:
        """Open the exact hashed EA in Strategy Tester from Navigator.

        Strategy Tester keeps a history dropdown and can omit a newly created
        expert. Navigator's tree label is truncated, so selection is allowed
        only when the full portable EX5 hash matches and the visible prefix is
        unique among files in the exact Experts subfolder.
        """
        self.navigator_selection_detail = "starting exact Navigator resolution"
        def fail(reason: str) -> bool:
            self.navigator_selection_detail = reason
            return False
        relative = Path(str(expected).replace("/", "\\"))
        expert_root = STAGE3_PORTABLE_DATA_DIR / "MQL5" / "Experts"
        expert_path = (expert_root / relative).resolve()
        try:
            expert_path.relative_to(expert_root.resolve())
        except ValueError:
            return fail("expected expert path escaped portable Experts")
        if (not expert_path.is_file() or len(expected_ex5_sha256) != 64
                or sha256(expert_path).casefold() != expected_ex5_sha256.casefold()):
            return fail("portable EX5 missing or hash differs from authenticated Stage 2 artifact")
        trees = [hwnd for hwnd in self.children
                 if self.gui.GetClassName(hwnd) == "SysTreeView32"
                 and self.gui.IsWindowVisible(hwnd)]
        if not trees and self.show_navigator():
            trees = [hwnd for hwnd in self.children
                     if self.gui.GetClassName(hwnd) == "SysTreeView32"
                     and self.gui.IsWindowVisible(hwnd)]
        if len(trees) != 1 or not self.tops:
            return fail(getattr(self, "navigator_selection_detail", None)
                        or f"expected one visible Navigator tree, found {len(trees)}")
        tree = trees[0]
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.VirtualAllocEx.restype = ctypes.c_void_p
        kernel.VirtualAllocEx.argtypes = [wintypes.HANDLE, ctypes.c_void_p,
                                          ctypes.c_size_t, wintypes.DWORD, wintypes.DWORD]
        kernel.WriteProcessMemory.argtypes = [wintypes.HANDLE, ctypes.c_void_p,
                                              ctypes.c_void_p, ctypes.c_size_t,
                                              ctypes.POINTER(ctypes.c_size_t)]
        kernel.ReadProcessMemory.argtypes = kernel.WriteProcessMemory.argtypes
        kernel.VirtualFreeEx.argtypes = [wintypes.HANDLE, ctypes.c_void_p,
                                         ctypes.c_size_t, wintypes.DWORD]
        process = kernel.OpenProcess(0x0038, False, self.terminal_pid)
        if not process:
            return fail("cannot open authenticated terminal for read-only Navigator inspection")
        remote = kernel.VirtualAllocEx(process, None, 4096, 0x3000, 0x04)
        if not remote:
            kernel.CloseHandle(process)
            return fail("cannot allocate bounded remote buffer for Navigator text query")

        class _TreeItem(ctypes.Structure):
            _fields_ = [("mask", wintypes.UINT), ("hItem", wintypes.HWND),
                        ("state", wintypes.UINT), ("stateMask", wintypes.UINT),
                        ("pszText", wintypes.LPWSTR), ("cchTextMax", ctypes.c_int),
                        ("iImage", ctypes.c_int), ("iSelectedImage", ctypes.c_int),
                        ("cChildren", ctypes.c_int), ("lParam", ctypes.c_ssize_t)]

        text_address = remote + 512
        byte_count = ctypes.c_size_t()

        def text_for(item: int) -> str:
            value = _TreeItem()
            value.mask = 1  # TVIF_TEXT
            value.hItem = item
            value.pszText = ctypes.cast(text_address, wintypes.LPWSTR)
            value.cchTextMax = 256
            if not kernel.WriteProcessMemory(process, remote, ctypes.byref(value),
                                             ctypes.sizeof(value), ctypes.byref(byte_count)):
                return ""
            self.user.SendMessageW(tree, 0x113E, 0, remote)  # TVM_GETITEMW
            buffer = ctypes.create_string_buffer(512)
            if not kernel.ReadProcessMemory(process, text_address, buffer, 512,
                                            ctypes.byref(byte_count)):
                return ""
            return buffer.raw.decode("utf-16le", "ignore").split("\0", 1)[0]

        def next_item(item: int, relation: int) -> int:
            return int(self.user.SendMessageW(tree, 0x110A, relation, item) or 0)

        nodes: list[tuple[int, tuple[str, ...]]] = []
        visited = 0
        def walk(item: int, path: tuple[str, ...] = ()) -> None:
            nonlocal visited
            sibling = item
            while sibling and visited < 1600:
                visited += 1
                label = text_for(sibling)
                current_path = path + (label,)
                if label:
                    nodes.append((sibling, current_path))
                child = next_item(sibling, 4)  # TVGN_CHILD
                if child:
                    walk(child, current_path)
                sibling = next_item(sibling, 1)  # TVGN_NEXT

        try:
            root_item = next_item(0, 0)  # TVGN_ROOT
            walk(root_item)
        finally:
            kernel.VirtualFreeEx(process, remote, 0, 0x8000)
            kernel.CloseHandle(process)

        expected_stem = expert_path.stem
        folder_parts = ["Expert Advisors", *relative.parts[:-1]]
        parents = [path for _, path in nodes
                   if len(path) >= len(folder_parts)
                   and tuple(part.casefold() for part in path[-len(folder_parts):])
                       == tuple(part.casefold() for part in folder_parts)]
        if len(parents) != 1:
            return fail(f"expected one Expert Advisors subfolder, found {len(parents)}")
        parent = parents[0]
        sibling_stems = [path.stem for path in expert_path.parent.glob("*.ex5")]
        matches = [(item, path[-1]) for item, path in nodes
                   if len(path) == len(parent) + 1 and path[:-1] == parent
                   and self._navigator_prefix_is_unique(path[-1], expected_stem,
                                                        sibling_stems)]
        if len(matches) != 1:
            return fail("no unique long Navigator label maps to the exact authenticated EX5")

        # Select the exact tree item and ask its context menu to open it in
        # Strategy Tester. The context menu is a modal Win32 menu; posted key
        # messages can be accepted without activating its Test command. Send
        # real keystrokes only while the authenticated terminal owns foreground,
        # then restore the operator's prior foreground window.
        if not self.user.SendMessageW(tree, 0x110B, 9, matches[0][0]):
            return fail("Navigator refused selection of the exact EA node")
        top = self.tops[0]
        previous_foreground = self.user.GetForegroundWindow()
        terminal_thread = wintypes.DWORD()
        self.user.GetWindowThreadProcessId(top, ctypes.byref(terminal_thread))
        current_thread = ctypes.windll.kernel32.GetCurrentThreadId()
        attached = bool(self.user.AttachThreadInput(
            current_thread, int(terminal_thread.value), True))
        try:
            # Hermes launches this worker in the interactive session, but the
            # foreground-lock policy can still reject SetForegroundWindow for
            # a terminal started by an earlier cron tick. A brief Alt key tap
            # grants the caller the normal foreground-switch opportunity; the
            # exact terminal PID and window are still verified immediately
            # below before any Navigator command is sent.
            if previous_foreground != top:
                self.user.keybd_event(0x12, 0, 0, 0)       # VK_MENU down
                self.user.keybd_event(0x12, 0, 0x0002, 0)  # VK_MENU up
            self.user.SetForegroundWindow(top)
            self.user.BringWindowToTop(top)
            if self.user.GetForegroundWindow() != top:
                return fail("could not focus authenticated terminal for exact Navigator Test")
            self.user.SetFocus(tree)
            self.user.PostMessageW(tree, 0x007B, tree, 0xFFFFFFFF)  # WM_CONTEXTMENU
            menu = 0
            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline and not menu:
                time.sleep(0.05)
                menu = self.user.FindWindowW("#32768", None)
            if not menu:
                return fail("exact EA Navigator context menu did not open")
            menu_owner = wintypes.DWORD()
            self.user.GetWindowThreadProcessId(menu, ctypes.byref(menu_owner))
            if menu_owner.value != self.terminal_pid:
                return fail("exact EA context menu is not owned by the authenticated terminal")
            for key in (ord("T"), 0x0D):
                self.user.keybd_event(key, 0, 0, 0)
                self.user.keybd_event(key, 0, 0x0002, 0)
            deadline = time.monotonic() + 2.0
            expected_labels = {expected.casefold(), expected_stem.casefold(),
                               expert_path.name.casefold()}
            while time.monotonic() < deadline:
                time.sleep(0.1)
                self._refresh_child_windows()
                if any(value.strip().replace("/", "\\").casefold() in expected_labels
                       for _hwnd, _index, value in self.expert_choices()):
                    self.navigator_selection_detail = (
                        "exact hashed EA Test command delivered; Tester selector confirmed the EA")
                    return True
            self.user.keybd_event(0x1B, 0, 0, 0)  # dismiss unconfirmed menu
            self.user.keybd_event(0x1B, 0, 0x0002, 0)
            return fail("exact Navigator Test did not add the authenticated EA to the Tester selector")
        finally:
            if attached:
                self.user.AttachThreadInput(current_thread, int(terminal_thread.value), False)
            if previous_foreground and self.user.IsWindow(previous_foreground):
                self.user.SetForegroundWindow(previous_foreground)

    def select_expert(self, hwnd: int, index: int) -> bool:
        self.user.SendMessageW(hwnd, 0x014E, index, 0)  # CB_SETCURSEL
        control_id = self.gui.GetDlgCtrlID(hwnd)
        parent = self.gui.GetParent(hwnd)
        # MT5 refreshes the expert's input metadata on the completed-selection
        # notification. CBN_SELCHANGE alone changes the combo text but leaves
        # the Inputs grid empty, so a later Start is rejected as having no
        # optimized parameters.
        for notification in (9, 1, 8):  # CBN_SELENDOK, CBN_SELCHANGE, CBN_CLOSEUP
            self.user.SendMessageW(parent, 0x0111,
                                   (notification << 16) | control_id, hwnd)
        time.sleep(0.1)
        selected = int(self.user.SendMessageW(hwnd, 0x0147, 0, 0))  # CB_GETCURSEL
        value = ctypes.create_unicode_buffer(2048)
        if selected >= 0:
            self.user.SendMessageW(hwnd, 0x0148, selected,
                                   ctypes.addressof(value))  # CB_GETLBTEXT
        return selected == index and bool(value.value.strip())

    def select_inputs_tab(self) -> bool:
        """Select the Strategy Tester Inputs tab before examining its grid.

        The Journal list also has six columns. Counting every six-column
        ListView can therefore report a false input schema when Journal is
        active. The Tester tab order is Overview, Settings, Inputs, Agents,
        Journal; require the expected five-tab control and verify selection.
        """
        tab_controls = []
        for hwnd in self.children:
            if self.gui.GetClassName(hwnd) != "SysTabControl32":
                continue
            count = int(self.user.SendMessageW(hwnd, 0x1304, 0, 0))  # TCM_GETITEMCOUNT
            if count in {5, 6} and self.gui.IsWindowVisible(hwnd):
                tab_controls.append(hwnd)
        if len(tab_controls) != 1:
            self.input_tab_detail = f"expected one visible 5/6-tab Tester control; found {len(tab_controls)}"
            return False
        hwnd = tab_controls[0]
        selected = int(self.user.SendMessageW(hwnd, 0x130B, 0, 0))  # TCM_GETCURSEL
        if selected != 2:
            rect = self._remote_item_rect(hwnd, 0x130A, 2)
            if rect is None:
                self.input_tab_detail = "could not read Inputs tab rectangle from authenticated terminal"
                return False
            x = (rect.left + rect.right) // 2
            y = (rect.top + rect.bottom) // 2
            point = (y << 16) | (x & 0xFFFF)
            self.user.SendMessageW(hwnd, 0x0201, 1, point)  # WM_LBUTTONDOWN
            self.user.SendMessageW(hwnd, 0x0202, 0, point)  # WM_LBUTTONUP
            selected = int(self.user.SendMessageW(hwnd, 0x130B, 0, 0))
        self.input_tab_detail = f"selected tab index {selected}"
        return selected == 2

    def _remote_item_rect(self, control: int, message: int, index: int):
        """Read a common-control RECT using memory in the authenticated MT5 process."""
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.VirtualAllocEx.restype = ctypes.c_void_p
        kernel.VirtualAllocEx.argtypes = [wintypes.HANDLE, ctypes.c_void_p,
                                          ctypes.c_size_t, wintypes.DWORD, wintypes.DWORD]
        kernel.ReadProcessMemory.argtypes = [wintypes.HANDLE, ctypes.c_void_p,
                                             ctypes.c_void_p, ctypes.c_size_t,
                                             ctypes.POINTER(ctypes.c_size_t)]
        kernel.VirtualFreeEx.argtypes = [wintypes.HANDLE, ctypes.c_void_p,
                                         ctypes.c_size_t, wintypes.DWORD]
        process = kernel.OpenProcess(0x0038, False, self.terminal_pid)
        if not process:
            return None
        remote = kernel.VirtualAllocEx(process, None, ctypes.sizeof(wintypes.RECT),
                                       0x3000, 0x04)
        if not remote:
            kernel.CloseHandle(process)
            return None
        try:
            if not self.user.SendMessageW(control, message, index, remote):
                return None
            rect = wintypes.RECT()
            transferred = ctypes.c_size_t()
            if not kernel.ReadProcessMemory(process, remote, ctypes.byref(rect),
                                            ctypes.sizeof(rect), ctypes.byref(transferred)):
                return None
            if transferred.value != ctypes.sizeof(rect):
                return None
            return rect
        finally:
            kernel.VirtualFreeEx(process, remote, 0, 0x8000)
            kernel.CloseHandle(process)

    def input_grid_stats(self) -> dict:
        rows = []
        for hwnd in self.children:
            if (self.gui.GetClassName(hwnd) != "SysListView32" or
                    not self.gui.IsWindowVisible(hwnd)):
                continue
            header = self.user.SendMessageW(hwnd, 0x101F, 0, 0)  # LVM_GETHEADER
            if header and self.user.SendMessageW(header, 0x1200, 0, 0) == 6:
                rows.append(int(self.user.SendMessageW(hwnd, 0x1004, 0, 0)))
        return {"rows": max(rows, default=0)}


def authenticated_input_surface_reconciliation(handoff: dict, ini_text: str) -> dict:
    """Reconcile a prepared Tester profile against the hash-bound EA/include surface.

    A source-declared input omitted from the profile is accepted only when the
    approved config does not optimize it and its effective fixed value equals
    the authenticated source default. Unknown or duplicate profile rows fail closed.
    """
    source_path = Path(str(handoff.get("source") or ""))
    include_key = "canonical_minato" if handoff.get("canonical_minato") else "score_include"
    include_hash_key = ("canonical_minato_sha256" if handoff.get("canonical_minato_sha256")
                        else "score_include_sha256")
    include_path = Path(str(handoff.get(include_key) or ""))
    require(source_path.is_file() and sha256(source_path) == handoff.get("source_sha256"),
            "input schema source is missing or differs from the authenticated handoff")
    require(include_path.is_file() and sha256(include_path) == handoff.get(include_hash_key),
            "input schema shared include is missing or differs from the authenticated handoff")
    stage1_path = Path(str(handoff.get("stage1_spec") or ""))
    require(stage1_path.is_file() and sha256(stage1_path) == handoff.get("stage1_spec_sha256"),
            "input schema Stage 1 authority is missing or changed")
    source_text = source_path.read_text(encoding="utf-8-sig", errors="replace")
    approved = preparer.stage2_config_from_spec(load_object(stage1_path), source_text=source_text)
    sources = {
        str(source_path.resolve()): source_text,
        str(include_path.resolve()): include_path.read_text(encoding="utf-8-sig", errors="replace"),
    }
    declared_group_rows = sum(
        len(re.findall(r"(?im)^\s*input\s+group\b", text))
        for text in sources.values())
    contract = input_contract.discover_inputs(sources)
    declarations = contract.get("inputs") or {}
    require(bool(declarations), "authenticated source surface contains no supported inputs")
    unsupported = sorted(name for name, declaration in declarations.items()
                         if declaration.get("supported") is not True)
    require(not unsupported,
            f"authenticated source has input types the runtime contract cannot bind: {unsupported}")
    section = re.split(r"(?im)^\[TesterInputs\]\s*$", ini_text, maxsplit=1)
    require(len(section) == 2, "schema binding INI lacks TesterInputs")
    input_text = re.split(r"(?m)^\[", section[1], maxsplit=1)[0]
    profile_rows = {}
    for line in input_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith((";", "#")) or "=" not in stripped:
            continue
        name, raw = stripped.split("=", 1)
        name = name.strip()
        if name in profile_rows:
            require(False, f"duplicate Tester input row: {name}")
        profile_rows[name] = raw.split("||", 1)[0].strip()
    extra = sorted(set(profile_rows) - set(declarations))
    require(not extra, f"Tester profile has inputs absent from authenticated source: {extra}")
    missing = sorted(set(declarations) - set(profile_rows))
    config_inputs = approved.get("inputs", {}) if isinstance(approved, dict) else {}
    reconciled_missing = []
    for name in missing:
        declaration = declarations[name]
        cfg = config_inputs.get(name, {}) if isinstance(config_inputs, dict) else {}
        require(not (isinstance(cfg, dict) and str(cfg.get("optimize", "N")).upper() == "Y"),
                f"optimized authenticated input is absent from Tester profile: {name}")
        source_value = declaration.get("source_default")
        expected_value = source_value
        if isinstance(cfg, dict) and "default" in cfg:
            expected_value = input_contract.coerce_value(cfg["default"], declaration["mql_type"])
        actual_value = input_contract.coerce_value(source_value, declaration["mql_type"])
        require(actual_value == expected_value,
                f"omitted fixed input does not resolve to its authenticated source default: {name}")
        reconciled_missing.append({"name": name, "mql_type": declaration["mql_type"],
                                   "effective_default": actual_value})
    return {
        "source_sha256": handoff["source_sha256"],
        "shared_include_sha256": handoff[include_hash_key],
        "declared_input_rows": len(declarations),
        "declared_group_rows": declared_group_rows,
        "profile_input_rows": len(profile_rows),
        "missing_fixed_source_defaults": reconciled_missing,
        "profile_input_names": sorted(profile_rows),
    }


def bind_tester_input_schema(terminal_pid: int, ini_path: Path, controls=None,
                             startup_ini_path: Path | None = None,
                             allow_navigator_refresh: bool = True,
                             expected_ex5_sha256: str | None = None,
                             expected_terminal_path: Path | None = None,
                             handoff: dict | None = None) -> dict:
    """Replay the proven exact-EA reselect only while Tester is stopped."""
    text = _read_profile_text(ini_path)
    expected = _profile_value(text, "Expert").replace("/", "\\").casefold()
    period_match = re.search(r"(?im)^Period\s*=\s*([^\r\n]+)", text)
    expected_timeframe = (period_match.group(1).split("||", 1)[0].strip().strip('"').upper()
                          if period_match else "")
    input_section = re.split(r"(?im)^\[TesterInputs\]\s*$", text, maxsplit=1)
    require(len(input_section) == 2, "schema binding INI lacks TesterInputs")
    input_text = re.split(r"(?m)^\[", input_section[1], maxsplit=1)[0]
    profile_rows = sum("=" in line and not line.lstrip().startswith((";", "#"))
                       for line in input_text.splitlines())
    expected_optimized_rows = sum("=" in line and line.rstrip().endswith("||Y")
                                  and not line.lstrip().startswith((";", "#"))
                                  for line in input_text.splitlines())
    reconciliation = None
    expected_rows = profile_rows
    if handoff is not None:
        try:
            reconciliation = authenticated_input_surface_reconciliation(handoff, text)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            return {"status": "INPUT_SCHEMA_INVALID", "terminal_pid": terminal_pid,
                    "detail": str(exc)}
        expected_rows = (int(reconciliation["declared_input_rows"])
                         + int(reconciliation.get("declared_group_rows", 0)))
    launch_profile = Path(startup_ini_path or ini_path)
    launch_text = _read_profile_text(launch_profile)
    optimization_mode = _profile_value(launch_text, "Optimization")
    if optimization_mode != "2":
        return {"status": "OPTIMIZATION_NOT_ENABLED", "terminal_pid": terminal_pid,
                "optimization_mode": optimization_mode}
    tester_section = re.search(r"(?ims)^\[Tester\]\s*$([\s\S]*?)(?=^\[|\Z)", launch_text)
    deferred_expert = bool(
        startup_ini_path is not None
        and tester_section is not None
        and re.search(r"(?im)^Expert\s*=", tester_section.group(1)) is None
    )
    try:
        controls = controls or _TesterControls(
            terminal_pid, startup_ini_path or ini_path,
            expected_terminal_path=expected_terminal_path)
        if not controls.stopped():
            return {"status": "ALREADY_RUNNING", "terminal_pid": terminal_pid}
        all_choices = controls.expert_choices()
        choices = [(hwnd, index) for hwnd, index, value in all_choices
                   if value.replace("/", "\\").casefold() == expected]
        navigator_tested = False
        if (not choices and expected_ex5_sha256
                and hasattr(controls, "test_exact_navigator_expert")):
            navigator_tested = controls.test_exact_navigator_expert(
                expected, expected_ex5_sha256)
            if navigator_tested:
                time.sleep(0.25)
                choices = [(hwnd, index) for hwnd, index, value in controls.expert_choices()
                           if value.replace("/", "\\").casefold() == expected]
        navigator_refresh_attempted = False
        if (not choices and allow_navigator_refresh
                and hasattr(controls, "refresh_navigator")):
            navigator_refresh_attempted = True
            if controls.refresh_navigator():
                time.sleep(0.25)
                choices = [(hwnd, index) for hwnd, index, value in controls.expert_choices()
                           if value.replace("/", "\\").casefold() == expected]
        # Some MT5 builds expose a Navigator-tested expert in the Tester
        # dropdown by basename only. Accept that form only when the exact
        # EX5 remains hash-bound and its basename is unique in portable Experts.
        if not choices and navigator_tested and expected_ex5_sha256:
            expected_path = Path(expected.replace("\\", "/"))
            expert_root = STAGE3_PORTABLE_DATA_DIR / "MQL5" / "Experts"
            expected_file = (expert_root / expected_path).resolve()
            try:
                expected_file.relative_to(expert_root.resolve())
                matching_files = [path for path in expert_root.rglob(expected_file.name)
                                  if path.is_file() and path.name.casefold() == expected_file.name.casefold()]
                hash_bound = (expected_file.is_file() and len(matching_files) == 1
                              and matching_files[0].resolve() == expected_file
                              and sha256(expected_file).casefold() == expected_ex5_sha256.casefold())
            except (OSError, ValueError):
                hash_bound = False
            if hash_bound:
                bare_names = {expected_file.name.casefold(), expected_file.stem.casefold()}
                bare_choices = [(hwnd, index) for hwnd, index, value in controls.expert_choices()
                                if value.strip().casefold() in bare_names]
                if len(bare_choices) == 1:
                    choices = bare_choices
        if len(choices) != 1:
            return {"status": "EXPERT_NOT_READY", "exact_matches": len(choices),
                    "navigator_refresh_attempted": navigator_refresh_attempted,
                    "navigator_tested": navigator_tested,
                    "navigator_test_detail": getattr(
                        controls, "navigator_selection_detail", None)}
        current_expert_index = getattr(controls, "current_expert_index", None)
        already_selected = (callable(current_expert_index)
                            and current_expert_index(choices[0][0]) == choices[0][1])
        # The no-autostart profile intentionally has no Tester.Expert entry.
        # Re-select the hash-authenticated expert in the Tester UI even when
        # the dropdown appears to retain it, so MT5 commits that selection to
        # this run's start configuration before Start is pressed.
        if (not already_selected or deferred_expert) and controls.select_expert(*choices[0]) is False:
            return {"status": "EXPERT_SELECTION_NOT_CONFIRMED",
                    "terminal_pid": terminal_pid,
                    "expected_expert": expected}
        if deferred_expert:
            if not callable(current_expert_index) or current_expert_index(choices[0][0]) != choices[0][1]:
                return {"status": "EXPERT_SELECTION_NOT_CONFIRMED",
                        "terminal_pid": terminal_pid,
                        "expected_expert": expected,
                        "detail": "deferred Tester.Expert reselection was not confirmed"}
        if expected_timeframe:
            if not hasattr(controls, "select_tester_timeframe") or not controls.select_tester_timeframe(expected_timeframe):
                return {"status": "TIMEFRAME_NOT_READY", "terminal_pid": terminal_pid,
                        "expected_timeframe": expected_timeframe,
                        "detail": getattr(controls, "timeframe_binding_detail", None)}
        if not controls.select_inputs_tab():
            return {"status": "INPUTS_TAB_NOT_READY", "terminal_pid": terminal_pid,
                    "detail": getattr(controls, "input_tab_detail", None)}
        grid = controls.input_grid_stats()
        count = int(grid.get("rows", 0))
        if already_selected and count == 0:
            # A /config launch can show the exact EA in the combo before MT5
            # has materialized its Inputs grid. Re-select only in this proven
            # empty-grid case so metadata loads; never reset a populated grid.
            if controls.select_expert(*choices[0]) is False:
                return {"status": "EXPERT_SELECTION_NOT_CONFIRMED",
                        "terminal_pid": terminal_pid,
                        "expected_expert": expected,
                        "detail": "reselection required because the selected EA had no input rows"}
            if not controls.select_inputs_tab():
                return {"status": "INPUTS_TAB_NOT_READY", "terminal_pid": terminal_pid,
                        "detail": getattr(controls, "input_tab_detail", None)}
            grid = controls.input_grid_stats()
            count = int(grid.get("rows", 0))
            already_selected = False
        # The MT5 input ListView exposes one trailing placeholder item in
        # builds where the tester includes the Optimization Results tab.
        grid_matches = (expected_rows <= count <= expected_rows + 1
                        and expected_optimized_rows > 0)
        return {"status": "INPUTS_BOUND" if grid_matches else "INPUTS_NOT_READY",
                "terminal_pid": terminal_pid, "expert": expected,
                "visible_input_rows": count, "expected_input_rows": expected_rows,
                "profile_input_rows": profile_rows,
                "expected_optimized_rows": expected_optimized_rows,
                "input_surface_reconciliation": reconciliation,
                "timeframe": expected_timeframe,
                "optimization_mode": optimization_mode,
                "expert_selection_action": ("reselected_exact_expert_for_deferred_profile"
                                             if deferred_expert else
                                             "preserved_current_selection" if already_selected else
                                             "selected_exact_expert_or_refreshed_empty_grid"),
                "navigator_tested": navigator_tested,
                "ini_sha256": sha256(ini_path), "bound_utc": utc_now()}
    except (OSError, psutil.Error, ImportError, ctypes.ArgumentError,
            ValueError, OverflowError) as exc:
        return {"status": "UI_UNAVAILABLE", "detail": str(exc)}


def _navigator_refresh_recovery_is_bound(runtime: dict) -> bool:
    recovery = runtime.get("navigator_refresh_recovery") or {}
    return bool(
        recovery.get("status") == "authorized_once"
        and recovery.get("cycle_id") == runtime.get("cycle_id")
        and recovery.get("terminal_pid") == runtime.get("terminal_pid")
        and recovery.get("ini_sha256") == runtime.get("ini_sha256")
        and recovery.get("startup_ini_sha256") == runtime.get("startup_ini_sha256")
        and recovery.get("ex5_sha256") == (runtime.get("deployment_reconciliation") or {}).get("authenticated_sha256")
    )


def prepare_tester_schema_start(runtime: dict, ini_path: Path,
                                capture_start_journal: bool = False,
                                handoff: dict | None = None) -> bool:
    """Bind before Start, preserving the automatic unbound-schema rejection."""
    prior_binding = runtime.get("input_schema_binding") or {}
    if prior_binding.get("status") != "INPUTS_BOUND":
        attempts = int(runtime.get("input_schema_binding_attempts", 0))
        attempt_limit = 16 if _navigator_refresh_recovery_is_bound(runtime) else 8
        if attempts >= attempt_limit:
            runtime["start_required_reason"] = "exact Tester input schema not ready after 8 bounded binding attempts"
            return False
        runtime["input_schema_binding_attempts"] = attempts + 1
        startup_ini = Path(runtime.get("startup_ini", ini_path))
        binding = bind_tester_input_schema(int(runtime["terminal_pid"]), ini_path,
                                           startup_ini_path=startup_ini,
                                           allow_navigator_refresh=True,
                                           expected_ex5_sha256=str(
                                               (runtime.get("deployment_reconciliation") or {})
                                               .get("authenticated_sha256") or ""),
                                           expected_terminal_path=Path(
                                               runtime.get("terminal_path") or
                                               STAGE3_PORTABLE_TERMINAL),
                                           handoff=handoff)
        runtime["input_schema_binding"] = binding
        if binding.get("navigator_tested"):
            runtime["navigator_expert_tested"] = True
        if binding.get("navigator_test_detail"):
            runtime["navigator_expert_detail"] = binding["navigator_test_detail"]
        if binding.get("navigator_refresh_attempted"):
            runtime["navigator_refresh_attempted"] = True
        if binding.get("detail"):
            runtime["input_schema_binding_detail"] = binding["detail"]
        if binding.get("status") != "INPUTS_BOUND":
            if (binding.get("status") == "ALREADY_RUNNING" and capture_start_journal
                    and not runtime.get("journal_ui_selected")
                    and not isinstance(runtime.get("journal_ui_start_baseline"), dict)):
                try:
                    baseline = capture_tester_journal_ui(
                        int(runtime["terminal_pid"]), select_journal=False)
                except Exception as exc:
                    baseline = {"available": False, "lines": [], "error_lines": [],
                                "error": str(exc), "captured_utc": utc_now()}
                runtime["journal_ui_start_baseline"] = baseline
                runtime["journal_ui_selected"] = False
                runtime["journal_ui_selection_disabled"] = True
                runtime["journal_ui_observation_started_epoch"] = time.time()
            runtime["start_required_reason"] = "waiting for exact Tester input schema: " + binding["status"]
            return False
        log = Path(runtime["tester_log"])
        diagnostic = read_fresh_tester_journal(log, int(runtime["tester_log_start_offset"]))
        startup_ini = Path(runtime.get("startup_ini", ""))
        startup_text = _read_profile_text(startup_ini) if startup_ini.is_file() else ""
        tester_section = re.search(
            r"(?ims)^\[Tester\]\s*$([\s\S]*?)(?=^\[|\Z)", startup_text)
        startup_non_autostart_authenticated = (
            startup_ini.is_file()
            and runtime.get("startup_ini_sha256") == sha256(startup_ini)
            and tester_section is not None
            and re.search(r"(?im)^Expert=", tester_section.group(1)) is None
        )
        if startup_non_autostart_authenticated:
            benign = [line for line in diagnostic.get("error_lines", [])
                      if NO_EXPERT_IN_START_CONFIG_RE.search(line)]
            if benign:
                diagnostic["benign_startup_lines"] = benign
                diagnostic["error_lines"] = [line for line in diagnostic.get("error_lines", [])
                                             if not NO_EXPERT_IN_START_CONFIG_RE.search(line)]
                if not diagnostic["error_lines"]:
                    diagnostic["original_code"] = diagnostic.get("code")
                    diagnostic["code"] = None
        if diagnostic.get("benign_startup_lines"):
            runtime["journal_non_autostart_notice"] = diagnostic
            runtime["tester_log_start_offset"] = log.stat().st_size
        if diagnostic.get("error_lines"):
            if (diagnostic.get("code") != "NO_OPTIMIZED_PARAMETERS" or
                    not all(NO_OPTIMIZED_PARAMETERS_RE.search(line)
                            for line in diagnostic["error_lines"])):
                raise TesterJournalError(diagnostic)
            runtime["journal_before_schema_binding"] = diagnostic
            runtime["tester_log_start_offset"] = log.stat().st_size
    if (capture_start_journal
            and not runtime.get("journal_ui_selected")
            and not isinstance(runtime.get("journal_ui_start_baseline"), dict)):
        # Observe the current UI without changing tabs. File-journal offsets
        # remain authoritative; never select the Journal tab from the cron.
        try:
            baseline = capture_tester_journal_ui(
                int(runtime["terminal_pid"]), select_journal=False)
        except Exception as exc:
            baseline = {"available": False, "lines": [], "error_lines": [],
                        "error": str(exc), "captured_utc": utc_now()}
        runtime["journal_ui_start_baseline"] = baseline
        runtime["journal_ui_selected"] = False
        runtime["journal_ui_selection_disabled"] = True
        runtime["journal_ui_observation_started_epoch"] = time.time()
    return auto_click_strategy_tester_start(int(runtime["terminal_pid"]))


def auto_click_strategy_tester_start(terminal_pid: int) -> bool:
    """Invoke a unique visible Start control owned by the authenticated MT5 terminal.

    This is only an input attempt. The caller authenticates success with fresh
    tester-worker and Journal evidence; a True return is not proof of a run.
    """
    if os.name != "nt":
        return False
    u = ctypes.windll.user32
    previous_foreground = u.GetForegroundWindow()
    tops = []

    @ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
    def top_cb(hwnd, _):
        owner = wintypes.DWORD()
        u.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
        if owner.value == int(terminal_pid) and u.IsWindowVisible(hwnd):
            tops.append(hwnd)
        return True

    def scan_tester_controls(parents):
        starts, stops = [], []
        for parent in parents:
            @ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
            def child_cb(hwnd, _):
                if not u.IsWindowVisible(hwnd):
                    return True
                text = ctypes.create_unicode_buffer(64)
                cls = ctypes.create_unicode_buffer(64)
                u.GetWindowTextW(hwnd, text, 64)
                u.GetClassNameW(hwnd, cls, 64)
                label = text.value.strip().casefold()
                control_class = cls.value.casefold()
                if (control_class in {"button", ""} and label in {"start", "stop"}
                        and u.IsWindowEnabled(hwnd)):
                    (starts if label == "start" else stops).append((parent, hwnd))
                return True
            u.EnumChildWindows(parent, child_cb, 0)
        return starts, stops

    u.EnumWindows(top_cb, 0)
    starts, stops = scan_tester_controls(tops)
    # Only open the Tester pane if neither its Start nor Stop control is already
    # present. Ctrl+R toggles that pane, so sending it when Start is visible
    # would close the pane and make the authorized launch impossible.
    if not starts and not stops and tops:
        u.SetForegroundWindow(tops[0])
        u.PostMessageW(tops[0], 0x0100, 0x11, 0)  # Ctrl down
        u.PostMessageW(tops[0], 0x0100, 0x52, 0)  # R down
        u.PostMessageW(tops[0], 0x0101, 0x52, 0xC0000001)  # R up
        u.PostMessageW(tops[0], 0x0101, 0x11, 0xC0000001)  # Ctrl up
        time.sleep(1.5)
        tops = []
        u.EnumWindows(top_cb, 0)
        starts, stops = scan_tester_controls(tops)
    if len(starts) != 1 or stops:
        if previous_foreground and u.IsWindow(previous_foreground):
            u.SetForegroundWindow(previous_foreground)
        return False
    _parent, button = starts[0]
    # Dispatch BM_CLICK synchronously with a timeout. PostMessage only proves
    # that Windows queued the message; it does not prove the Tester button's
    # thread processed it, which produced false "delivered" telemetry on cron.
    # Caller still requires independent worker/Journal/.opt proof.
    result = wintypes.DWORD()
    dispatched = u.SendMessageTimeoutW(
        button, 0x00F5, 0, 0, 0x0002, 2000, ctypes.byref(result))
    return bool(dispatched)


def capture_tester_journal_ui(terminal_pid: int, select_journal: bool = False) -> dict:
    """Capture the visible MT5 Tester Journal when MT5 has not flushed a log.

    MT5 can keep Tester Journal rows in the live UI while the corresponding
    file remains unchanged.  The cron therefore captures the lower Tester pane
    after Start and OCRs only diagnostic lines.  This is evidence collection,
    not a substitute for worker/journal authentication.
    """
    if os.name != "nt":
        return {"available": False, "lines": [], "error_lines": []}
    u = ctypes.windll.user32
    previous_foreground = u.GetForegroundWindow()

    def restore_previous_foreground() -> None:
        if previous_foreground and u.IsWindow(previous_foreground):
            u.SetForegroundWindow(previous_foreground)

    # The isolated probe owns its dependencies and does not require PIL or
    # Tesseract in the cron interpreter.
    probe_python = Path(__file__).resolve().parent.parent / ".venv_mt5_journal_probe" / "Scripts" / "python.exe"
    probe_script = Path(__file__).resolve().parent / "tools" / "mt5_journal_probe_ocr.py"
    if probe_python.exists() and probe_script.exists():
        try:
            probe_args = [str(probe_python), str(probe_script)]
            if not select_journal:
                probe_args.append("--no-select")
            proc = subprocess.run(probe_args,
                                  cwd=str(Path(__file__).resolve().parent.parent),
                                  capture_output=True, text=True, timeout=45)
            payload = json.loads(proc.stdout)
            matches = payload.get("matches", [])
            if matches:
                result = {"available": True, "method": "native_windows_ocr_probe",
                        "lines": payload.get("lines", [])[-120:],
                        "error_lines": matches[-20:],
                        "screenshot": payload.get("screenshot"),
                        "captured_utc": utc_now()}
                restore_previous_foreground()
                return result
        except Exception:
            pass
    tops = []
    @ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
    def top_cb(hwnd, _):
        owner = wintypes.DWORD()
        u.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
        if owner.value == int(terminal_pid) and u.IsWindowVisible(hwnd):
            tops.append(hwnd)
        return True
    u.EnumWindows(top_cb, 0)
    if not tops:
        restore_previous_foreground()
        return {"available": False, "lines": [], "error_lines": []}
    # Keep every UI observation on monitor 2.  This is non-activating; it does
    # not move the user's mouse or change the active application.
    u.SetWindowPos(tops[0], 0, 2600, -300, 1200, 800, 0x0010 | 0x0004)
    # Use the isolated, tested Windows-native OCR probe when available. It
    # focuses MT5, selects Journal, captures the live grid, and returns JSON.
    probe_python = Path(__file__).resolve().parent.parent / ".venv_mt5_journal_probe" / "Scripts" / "python.exe"
    probe_script = Path(__file__).resolve().parent / "tools" / "mt5_journal_probe_ocr.py"
    if probe_python.exists() and probe_script.exists():
        try:
            probe_args = [str(probe_python), str(probe_script)]
            if not select_journal:
                probe_args.append("--no-select")
            proc = subprocess.run(probe_args,
                                  cwd=str(Path(__file__).resolve().parent.parent),
                                  capture_output=True, text=True, timeout=45)
            payload = json.loads(proc.stdout)
            matches = payload.get("matches", [])
            if matches:
                result = {"available": True, "method": "native_windows_ocr_probe",
                        "lines": payload.get("lines", [])[-120:],
                        "error_lines": matches[-20:],
                        "screenshot": payload.get("screenshot"),
                        "captured_utc": utc_now()}
                restore_previous_foreground()
                return result
        except Exception:
            pass
    # Prefer native clipboard extraction: MT5 may keep Tester Journal rows in
    # the live control without flushing them to Tester/logs yet.
    journal_tabs = []
    @ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
    def child_cb(hwnd, _):
        if not u.IsWindowVisible(hwnd):
            return True
        text = ctypes.create_unicode_buffer(128)
        u.GetWindowTextW(hwnd, text, 128)
        if text.value.strip().casefold() == "journal":
            journal_tabs.append(hwnd)
        return True
    u.EnumChildWindows(tops[0], child_cb, 0)
    if not select_journal:
        if ImageGrab is None or pytesseract is None:
            restore_previous_foreground()
            return {"available": False, "method": "ui_observer_unavailable", "lines": [], "error_lines": []}
        rect = wintypes.RECT()
        if not u.GetWindowRect(tops[0], ctypes.byref(rect)):
            restore_previous_foreground()
            return {"available": False, "method": "ui_observer_no_window", "lines": [], "error_lines": []}
        try:
            image = ImageGrab.grab(bbox=(rect.left, rect.top, rect.right, rect.bottom))
            width, height = image.size
            text = pytesseract.image_to_string(image.crop((0, int(height * 0.45), width, height)))
        except Exception as exc:
            restore_previous_foreground()
            return {"available": False, "method": "ui_observer_ocr_failed", "lines": [], "error_lines": [], "error": str(exc)}
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        errors = [line for line in lines if INPUT_GRID_ERROR_RE.search(line)
                  or NO_EXPERT_IN_START_CONFIG_RE.search(line)
                  or "incorrect input parameters" in line.lower()]
        restore_previous_foreground()
        return {"available": bool(lines), "method": "ui_observer_ocr", "lines": lines[-80:], "error_lines": errors[-20:], "captured_utc": utc_now()}
    u.SetForegroundWindow(tops[0])
    for tab in journal_tabs:
        u.SendMessageW(tab, 0x00F5, 0, 0)
    time.sleep(0.25)
    # Ctrl+A/C to the focused Tester Journal control.
    u.PostMessageW(tops[0], 0x0100, 0x11, 0)
    u.PostMessageW(tops[0], 0x0100, 0x41, 0)
    u.PostMessageW(tops[0], 0x0101, 0x41, 0xC0000001)
    u.PostMessageW(tops[0], 0x0101, 0x11, 0xC0000001)
    time.sleep(0.05)
    u.PostMessageW(tops[0], 0x0100, 0x11, 0)
    u.PostMessageW(tops[0], 0x0100, 0x43, 0)
    u.PostMessageW(tops[0], 0x0101, 0x43, 0xC0000001)
    u.PostMessageW(tops[0], 0x0101, 0x11, 0xC0000001)
    time.sleep(0.25)
    clipboard_text = ""
    if u.OpenClipboard(None):
        try:
            handle = u.GetClipboardData(13)  # CF_UNICODETEXT
            if handle:
                ptr = ctypes.windll.kernel32.GlobalLock(handle)
                if ptr:
                    try:
                        clipboard_text = ctypes.wstring_at(ptr)
                    except (OSError, ValueError):
                        # Clipboard handles can be invalidated by MT5 while the
                        # Tester pane refreshes. Fall through to local OCR.
                        clipboard_text = ""
                    finally:
                        ctypes.windll.kernel32.GlobalUnlock(handle)
        finally:
            u.CloseClipboard()
    if clipboard_text.strip():
        lines = [line.strip() for line in clipboard_text.splitlines() if line.strip()]
        error_lines = [line for line in lines if INPUT_GRID_ERROR_RE.search(line)
                       or NO_EXPERT_IN_START_CONFIG_RE.search(line)
                       or "incorrect input parameters" in line.lower()]
        restore_previous_foreground()
        return {"available": True, "method": "native_clipboard", "lines": lines[-120:],
                "error_lines": error_lines[-20:], "captured_utc": utc_now()}
    if ImageGrab is None or pytesseract is None:
        restore_previous_foreground()
        return {"available": False, "method": "native_clipboard_empty", "lines": [], "error_lines": []}
    rect = wintypes.RECT()
    if not u.GetWindowRect(tops[0], ctypes.byref(rect)):
        restore_previous_foreground()
        return {"available": False, "lines": [], "error_lines": []}
    try:
        image = ImageGrab.grab(bbox=(rect.left, rect.top, rect.right, rect.bottom))
        width, height = image.size
        text = pytesseract.image_to_string(image.crop((0, int(height * 0.45), width, height)))
    except Exception as exc:
        restore_previous_foreground()
        return {"available": False, "method": "ocr_unavailable", "lines": [], "error_lines": [],
                "error": str(exc)}
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    error_lines = [line for line in lines if INPUT_GRID_ERROR_RE.search(line)
                   or NO_EXPERT_IN_START_CONFIG_RE.search(line)
                   or "incorrect input parameters" in line.lower()]
    result = {"available": True, "method": "ocr", "lines": lines[-80:], "error_lines": error_lines[-20:],
            "captured_utc": utc_now()}
    restore_previous_foreground()
    return result


def resolve_tester_journal_path(data_dir: Path, preferred: Path | None = None) -> Path:
    """Use the Tester journal; terminal activity must never change log identity."""
    today = datetime.now().strftime('%Y%m%d')
    tester = data_dir / 'Tester' / 'logs' / f'{today}.log'
    if preferred is not None and preferred.parent.resolve() == tester.parent.resolve():
        return preferred
    # A missing/unflushed Tester journal is an observation, not permission to
    # substitute terminal-network logs with an unrelated byte offset.
    return tester


def journal_snapshot(log_path: Path, start_offset: int = 0, phase: str = '') -> dict:
    """Capture bounded raw journal evidence for lifecycle auditing."""
    raw = b''
    if log_path.is_file():
        raw = log_path.read_bytes()[max(0, int(start_offset)):]
    text = max((raw.decode('utf-16le', errors='ignore'),
                raw.decode('utf-8-sig', errors='ignore')),
               key=lambda value: (value.count('Tester'), len(value)))
    return {
        'phase': phase, 'path': str(log_path.resolve()),
        'start_offset': int(start_offset), 'bytes': len(raw),
        'sha256': hashlib.sha256(raw).hexdigest().upper(),
        'lines': [line.strip() for line in text.splitlines() if line.strip()][-120:],
        'captured_utc': utc_now(),
    }


def read_fresh_tester_journal(log_path: Path, start_offset: int) -> dict:
    """Read only post-launch Tester Journal bytes and classify error-level lines."""
    if not log_path.is_file() or log_path.stat().st_size <= start_offset:
        return {"path": str(log_path), "start_offset": start_offset,
                "error_lines": [], "invalid_parameter_passes": [], "code": None}
    with log_path.open("rb") as handle:
        handle.seek(start_offset)
        raw = handle.read()
    candidates = (raw.decode("utf-16le", errors="ignore"), raw.decode("utf-8-sig", errors="ignore"))
    text = max(candidates, key=lambda value: (len(JOURNAL_ERROR_RE.findall(value)), value.count("Tester")))
    error_lines = []
    invalid_parameter_passes = []
    for line in text.splitlines():
        if GENETIC_INVALID_PARAMETER_PASS_RE.search(line):
            # INIT_PARAMETERS_INCORRECT is a candidate-level rejection during
            # genetic optimization. Other candidates can still run and produce
            # valid results, so retain it as diagnostic evidence without
            # treating the entire Tester execution as a startup failure.
            invalid_parameter_passes.append(line.strip())
            continue
        match = JOURNAL_ERROR_RE.match(line)
        if match or INPUT_GRID_ERROR_RE.search(line):
            error_lines.append(line.strip())
        elif NO_EXPERT_IN_START_CONFIG_RE.search(line):
            error_lines.append(line.strip())
        elif NO_OPTIMIZED_PARAMETERS_RE.search(line):
            # Keep the exact MT5 diagnostic even if a future log format changes
            # the severity-column layout.
            error_lines.append(line.strip())
    code = None
    if any(NO_EXPERT_IN_START_CONFIG_RE.search(line) for line in error_lines):
        code = "NO_EXPERT_IN_START_CONFIG"
    elif any(INPUT_GRID_ERROR_RE.search(line) for line in error_lines):
        code = "TESTER_INPUT_GRID_REJECTED"
    elif any(NO_OPTIMIZED_PARAMETERS_RE.search(line) for line in error_lines):
        code = "NO_OPTIMIZED_PARAMETERS"
    elif any(INVALID_STOPS_RE.search(line) for line in error_lines):
        code = "TESTER_EA_ORDER_REJECTED"
    elif error_lines:
        code = "TESTER_JOURNAL_ERROR"
    fresh_lines = [line.strip() for line in text.splitlines() if line.strip()]
    run_identity_lines = [line for line in fresh_lines
                          if re.search(r"\bon\s+[^,\s]+\s*,\s*[A-Z]+\d+\s+from\s+", line, re.I)]
    return {"path": str(log_path.resolve()), "start_offset": start_offset,
            "fresh_lines": fresh_lines[-80:], "run_identity_lines": run_identity_lines[-20:],
            "error_lines": error_lines[-10:],
            "invalid_parameter_passes": invalid_parameter_passes[-80:], "code": code}


def read_authenticated_terminal_progress(runtime: dict, handoff: dict) -> dict:
    """Read launch-bound MT5 root-log progress without treating it as Tester evidence."""
    def decode_log(raw_bytes: bytes) -> str:
        sample = raw_bytes[:512]
        if raw_bytes.startswith((b"\xff\xfe", b"\xfe\xff")):
            return raw_bytes.decode("utf-16", errors="ignore")
        if sample and sample.count(b"\x00") > len(sample) // 8:
            return raw_bytes.decode("utf-16le", errors="ignore")
        return raw_bytes.decode("utf-8-sig", errors="ignore")

    launched = float(runtime.get("launched_epoch", 0) or 0)
    startup_ini_path = Path(str(runtime.get("startup_ini", "")))
    if not launched or not startup_ini_path.is_file():
        return {"authenticated": False, "reason": "launch identity unavailable"}
    path_value = runtime.get("terminal_log")
    path = (Path(str(path_value)) if path_value else
            DEFAULT_DATA_DIR / "logs" / f"{datetime.fromtimestamp(launched).strftime('%Y%m%d')}.log")
    offset = int(runtime.get("terminal_log_start_offset", -1))
    if not path.is_file():
        return {"authenticated": False, "reason": "terminal log unavailable", "path": str(path)}
    try:
        all_raw = path.read_bytes()
    except OSError as exc:
        return {"authenticated": False, "reason": str(exc)}
    base_offset = offset
    if 0 <= offset <= len(all_raw):
        raw = all_raw[offset:]
        text = decode_log(raw)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
    else:
        # Legacy manifests have no root-log offset. Bind the exact start-config
        # record to the recorded launch time, then inspect only following lines.
        text = decode_log(all_raw)
        all_lines = [line.strip() for line in text.splitlines() if line.strip()]
        startup_ini = str(startup_ini_path.resolve())
        expected = datetime.fromtimestamp(launched)
        anchor = None
        for index, line in enumerate(all_lines):
            if ("successfully initialized from start config" not in line.casefold()
                    or startup_ini.casefold() not in line.casefold()):
                continue
            match = re.search(r"\b(\d{2}):(\d{2}):(\d{2})\.\d+\b", line)
            if not match:
                continue
            actual = int(match.group(1)) * 3600 + int(match.group(2)) * 60 + int(match.group(3))
            expected_seconds = expected.hour * 3600 + expected.minute * 60 + expected.second
            delta = abs(actual - expected_seconds)
            if min(delta, 86400 - delta) <= 120:
                anchor = index
                break
        if anchor is None:
            return {"authenticated": False, "reason": "no launch-time config record in legacy terminal log",
                    "path": str(path.resolve())}
        lines = all_lines[anchor:]
        raw = "\n".join(lines).encode("utf-8")
        base_offset = -1
    startup_ini = str(startup_ini_path.resolve())
    launch_bound = any("successfully initialized from start config" in line.casefold()
                       and startup_ini.casefold() in line.casefold() for line in lines)
    started = any("tester\tautomatic testing started" in line.casefold() for line in lines)
    progress = []
    for line in lines:
        match = re.search(r"autotesting\s+processing\s+(\d+)\s*%", line, re.IGNORECASE)
        if match:
            item = {"percent": int(match.group(1)), "line": line}
            clock = re.search(r"\b(\d{2}:\d{2}:\d{2}\.\d+)\b", line)
            try:
                log_date = datetime.strptime(path.stem, "%Y%m%d").date()
                log_time = datetime.strptime(clock.group(1), "%H:%M:%S.%f").time()
                item["epoch"] = datetime.combine(log_date, log_time).timestamp()
            except (AttributeError, ValueError, OSError):
                pass
            progress.append(item)
    finished = any("optimization finished" in line.casefold() for line in lines)
    authenticated = bool(
        launch_bound and started and progress
        and runtime.get("cycle_id") == handoff.get("cycle_id")
        and runtime.get("ea") == handoff.get("ea")
        and runtime.get("symbol") == handoff.get("symbol")
        and runtime.get("timeframe") in handoff.get("timeframe_order", [])
        and runtime.get("startup_ini_sha256") == sha256(startup_ini_path)
    )
    return {"authenticated": authenticated, "path": str(path.resolve()),
            "start_offset": base_offset, "bytes": len(raw), "launch_config_seen": launch_bound,
            "automatic_testing_started": started, "progress": progress[-20:],
            "latest_percent": progress[-1]["percent"] if progress else None,
            "progress_age_seconds": (max(0.0, time.time() - progress[-1]["epoch"])
                                     if progress and isinstance(progress[-1].get("epoch"), (int, float))
                                     else None),
            "optimization_finished": finished, "captured_utc": utc_now()}


def tester_timeframe_mismatch(journal: dict, handoff: dict, expected_timeframe: str) -> str | None:
    """Return parent-journal run identity evidence that contradicts the INI."""
    symbol = re.escape(str(handoff.get("symbol", "")))
    ea = str(handoff.get("ea", "")).casefold()
    for line in journal.get("run_identity_lines", journal.get("fresh_lines", [])):
        if ea not in str(line).casefold():
            continue
        match = re.search(rf"\bon\s+{symbol}\s*,\s*([A-Z]+\d+)\s+from\s+",
                          str(line), re.I)
        if match and match.group(1).upper() != str(expected_timeframe).upper():
            return str(line)
    return None


def authenticated_tester_run_started(runtime: dict, handoff: dict, journal: dict) -> bool:
    """Prove the exact Tester run already started before considering another Start click."""
    startup_path = Path(str(runtime.get("startup_ini", "")))
    if (not startup_path.is_file()
            or runtime.get("startup_ini_sha256") != sha256(startup_path)
            or not runtime.get("start_button_invoked")):
        return False
    section = re.search(r"(?ims)^\[Tester\]\s*$([\s\S]*?)(?=^\[|\Z)",
                        _read_profile_text(startup_path))
    if section is None or re.search(r"(?im)^Expert=", section.group(1)):
        return False
    identity = (
        f"{handoff.get('ea')}.ex5 on {handoff.get('symbol')},{runtime.get('timeframe')} "
        f"from {handoff.get('from_date')} 00:00 to {handoff.get('to_date')} 00:00"
    ).casefold()
    exact_identity = any(identity in str(line).casefold()
                         for line in journal.get("run_identity_lines", []))
    if not exact_identity:
        return False
    try:
        log_path = Path(str(runtime["tester_log"]))
        start_offset = int(runtime["tester_log_start_offset"])
        raw = b""
        if log_path.is_file() and log_path.stat().st_size > start_offset:
            with log_path.open("rb") as handle:
                handle.seek(start_offset)
                raw = handle.read()
        text = max((raw.decode("utf-16le", errors="ignore"),
                    raw.decode("utf-8-sig", errors="ignore")),
                   key=lambda value: (value.count("Tester"), len(value)))
    except (OSError, KeyError, TypeError, ValueError):
        return False
    # Exact run identity plus a Tester pass/result proves that MT5 loaded the
    # configured EA after the deliberately expert-free startup profile.
    return bool(re.search(r"(?:Best result|genetic calculation started|optimization started)",
                          text, re.I))


def filter_authenticated_no_expert_notice(journal: dict, runtime: dict,
                                          handoff: dict | None = None) -> dict:
    """Ignore the expert-free startup notice only after the exact Tester run starts."""
    startup_path = Path(str(runtime.get("startup_ini", "")))
    if (not startup_path.is_file()
            or runtime.get("startup_ini_sha256") != sha256(startup_path)):
        return journal
    text = _read_profile_text(startup_path)
    section = re.search(r"(?ims)^\[Tester\]\s*$([\s\S]*?)(?=^\[|\Z)", text)
    if section is None or re.search(r"(?im)^Expert=", section.group(1)):
        return journal
    errors = list(journal.get("error_lines", []))
    benign = [line for line in errors if NO_EXPERT_IN_START_CONFIG_RE.search(line)]
    # A persisted diagnostic may already have removed this startup notice from
    # error_lines while retaining it in benign_startup_lines. Keep reprocessing
    # idempotent so a held runtime can be corrected by the scheduled driver.
    benign.extend(line for line in journal.get("benign_startup_lines", [])
                  if NO_EXPERT_IN_START_CONFIG_RE.search(str(line))
                  and line not in benign)
    if not benign:
        return journal
    if handoff is None or not authenticated_tester_run_started(runtime, handoff, journal):
        return journal
    result = dict(journal)
    result["benign_startup_lines"] = benign
    result["error_lines"] = [line for line in errors
                             if not NO_EXPERT_IN_START_CONFIG_RE.search(line)]
    result["original_code"] = result.get("code")
    # MT5 can emit its expected expert-free startup notice before the UI
    # binds the exact EA. Once that exact run is authenticated, classify the
    # actual terminal result: a complete genetic grid rejected as invalid
    # inputs belongs to Stage 2 repair, not the startup-infrastructure path.
    all_passes_failed = any(
        re.search(r"\b(\d+)\s+passes\s+of\s+\1\s+failed\b", str(line), re.I)
        for line in result["error_lines"]
    )
    if all_passes_failed and journal.get("invalid_parameter_passes"):
        result["code"] = "TESTER_INPUT_GRID_REJECTED"
    elif any(INPUT_GRID_ERROR_RE.search(str(line)) for line in result["error_lines"]):
        result["code"] = "TESTER_INPUT_GRID_REJECTED"
    elif any(INVALID_STOPS_RE.search(str(line)) for line in result["error_lines"]):
        result["code"] = "TESTER_EA_ORDER_REJECTED"
    elif result["error_lines"]:
        result["code"] = "TESTER_JOURNAL_ERROR"
    else:
        result["code"] = None
    return result


def worker_address(pid: int) -> str | None:
    try:
        command = " ".join(psutil.Process(int(pid)).cmdline())
    except (psutil.AccessDenied, psutil.NoSuchProcess):
        return None
    match = WORKER_ADDRESS_RE.search(command)
    return match.group(1) if match else None


def agent_log_offsets(data_dir: Path, launched_epoch: float) -> dict[str, int]:
    date_name = datetime.fromtimestamp(launched_epoch).strftime("%Y%m%d.log")
    tester_root = _tester_agent_root(data_dir)
    offsets = {}
    for agent in tester_root.glob("Agent-*"):
        match = re.fullmatch(r"Agent-(.+)-(\d+)", agent.name, re.I)
        if not match:
            continue
        path = agent / "logs" / date_name
        offsets[f"{match.group(1)}:{match.group(2)}"] = path.stat().st_size if path.is_file() else 0
    return offsets


def minato_diagnostic_file_offsets(data_dir: Path) -> dict[str, dict]:
    """Snapshot Minato per-pass output files before this Stage 3 run starts."""
    tester_root = _tester_agent_root(data_dir)
    snapshots = {}
    for agent in tester_root.glob("Agent-*"):
        match = re.fullmatch(r"Agent-(.+)-(\d+)", agent.name, re.I)
        if not match:
            continue
        path = (agent / "MQL5" / "Files" / "minato_v2_opt_results.csv").resolve()
        raw = path.read_bytes() if path.is_file() else b""
        snapshots[str(path)] = {
            "address": f"{match.group(1)}:{match.group(2)}",
            "size": len(raw),
            "prefix_sha256": hashlib.sha256(raw).hexdigest().upper(),
        }
    return snapshots


def _tester_agent_root(data_dir: Path) -> Path:
    """Resolve worker folders for either portable or roaming MT5 data roots."""
    portable_tester = data_dir / "Tester"
    if portable_tester.is_dir():
        return portable_tester
    return data_dir.parent.parent / "Tester" / data_dir.name


def agent_log_registrations(worker_pids: list[int], data_dir: Path,
                            launched_epoch: float | None = None,
                            baseline_offsets: dict[str, int] | None = None,
                            worker_evidence: list[dict] | None = None) -> list[dict]:
    """Resolve each local worker to its append-only agent log and byte offset."""
    launched_epoch = time.time() if launched_epoch is None else launched_epoch
    date_name = datetime.fromtimestamp(launched_epoch).strftime("%Y%m%d.log")
    tester_root = _tester_agent_root(data_dir)
    registrations = []
    for pid in sorted(set(int(value) for value in worker_pids)):
        address = worker_address(pid)
        if not address:
            address = next((str(item.get("address")) for item in (worker_evidence or [])
                            if int(item.get("pid", -1)) == pid and item.get("address")), None)
        if not address:
            continue
        agent_dir = f"Agent-{address.replace(':', '-')}"
        path = tester_root / agent_dir / "logs" / date_name
        registrations.append({
            "worker_pid": pid,
            "address": address,
            "path": str(path.resolve()),
            "start_offset": (baseline_offsets or {}).get(
                address, path.stat().st_size if path.is_file() else 0
            ),
        })
    return registrations


def fresh_agent_optimization_progress(handoff: dict, runtime: dict,
                                      workers: list[dict], data_dir: Path,
                                      *, allow_exited: bool = False) -> dict | None:
    """Prove the exact launched optimization from authenticated local-agent logs.

    MT5 may auto-start from its config before GUI input binding can complete.
    A matching ALREADY_RUNNING terminal plus worker-side expert/input/symbol
    records is sufficient evidence of start. Historical logs are admitted only
    when the launch manifest already bound their worker PID, address, path, and
    pre-run byte offset; this preserves interruption evidence after shutdown.
    """
    binding = runtime.get("input_schema_binding", {})
    timeframe = str(runtime.get("timeframe", ""))
    terminal_pid = int(runtime.get("terminal_pid") or 0)
    bound_start = (binding.get("status") == "INPUTS_BOUND"
                   and runtime.get("start_button_invoked") is True)
    config_autostart = (binding.get("status") == "ALREADY_RUNNING"
                        and int(binding.get("terminal_pid") or 0) == terminal_pid
                        and terminal_pid > 0)
    if (not (bound_start or config_autostart)
            or binding.get("timeframe") not in (None, timeframe)
            or binding.get("ini_sha256") not in (None, handoff.get("ini_sha256", {}).get(timeframe))
            or not workers):
        return None
    try:
        launched = float(runtime["launched_epoch"])
    except (KeyError, TypeError, ValueError):
        return None
    worker_evidence = {int(item.get("pid", -1)): item for item in
                       runtime.get("worker_process_evidence", []) if isinstance(item, dict)}
    supplied = {int(item.get("pid", -1)): item for item in workers if isinstance(item, dict)}
    supplied.update({pid: item for pid, item in worker_evidence.items() if pid not in supplied})
    live_pids = {int(item["pid"]) for item in fresh_local_workers(
        runtime.get("worker_baseline_pids", []), launched,
        expected_addresses=runtime.get("agent_offsets_before", {}).keys())}
    if allow_exited:
        registrations = [item for item in runtime.get("agent_logs", [])
                         if isinstance(item, dict)
                         and int(item.get("worker_pid", -1)) in supplied]
        if not registrations:
            registrations = agent_log_registrations(
                list(supplied), data_dir, launched,
                runtime.get("agent_offsets_before", {}), list(supplied.values()))
    else:
        live_workers = [item for pid, item in supplied.items() if pid in live_pids]
        if not live_workers:
            return None
        registrations = agent_log_registrations(
            [int(item["pid"]) for item in live_workers], data_dir, launched,
            runtime.get("agent_offsets_before", {}), live_workers)
    if not registrations:
        return None
    baseline_offsets = runtime.get("agent_offsets_before", {})
    expected_root = _tester_agent_root(data_dir).resolve()
    date_name = datetime.fromtimestamp(launched).strftime("%Y%m%d.log")
    expected_expert = (
        "experts\\kiyosaki\\" + str(handoff.get("ea", "")).casefold() + ".ex5"
    )
    validated = []
    for registration in registrations:
        try:
            pid = int(registration.get("worker_pid", -1))
            address = str(registration.get("address", ""))
            evidence = supplied.get(pid)
            expected_path = (expected_root / f"Agent-{address.replace(':', '-')}"
                             / "logs" / date_name).resolve()
            path = Path(registration["path"]).resolve()
            if (evidence is None or not address or path != expected_path
                    or evidence.get("address") != address
                    or int(registration.get("start_offset", -1))
                        != int(baseline_offsets.get(address, registration.get("start_offset", -2)))):
                continue
            if psutil.pid_exists(pid):
                try:
                    process = psutil.Process(pid)
                    if (abs(float(process.create_time()) - float(evidence.get("create_time", -2))) > 1.0
                            or Path(process.exe()).resolve() != Path(evidence.get("exe", "")).resolve()):
                        continue
                except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                    continue
            if not path.is_file():
                continue
            contents = path.read_bytes()
            utf16 = contents.startswith(b"\xff\xfe")
            encoding = "utf-16-le" if utf16 else "utf-8"
            if address in baseline_offsets:
                offset = int(baseline_offsets[address])
                raw = contents[offset:]
            else:
                # Older runtime manifests may lack offsets because the
                # portable agent root was resolved incorrectly. Recover only
                # from a new agent-process startup within 90 seconds of this
                # launch, then inspect bytes after that startup marker.
                offset = None
                cursor = 2 if utf16 else 0
                launch_local = datetime.fromtimestamp(launched)
                startup_re = re.compile(
                    r"^\S+\s+0\s+(\d{2}:\d{2}:\d{2}\.\d+)\s+Startup\s+MetaTester 5 build",
                    re.I)
                decoded = contents[2:].decode(encoding, errors="replace") if utf16 else contents.decode(encoding, errors="replace")
                for raw_line in decoded.splitlines(keepends=True):
                    match = startup_re.search(raw_line.rstrip("\r\n"))
                    if match:
                        try:
                            timestamp = datetime.strptime(
                                launch_local.strftime("%Y-%m-%d") + " " +
                                match.group(1), "%Y-%m-%d %H:%M:%S.%f"
                            ).timestamp()
                        except (UnicodeDecodeError, ValueError, OverflowError):
                            timestamp = 0
                        if abs(timestamp - launched) <= 90:
                            offset = cursor + len(raw_line)
                    cursor += len(raw_line.encode(encoding))
                if offset is None:
                    continue
                raw = contents[offset:]
            lines = raw.decode(encoding, errors="replace").lstrip("\ufeff").splitlines()
        except (OSError, ValueError):
            continue
        folded = [line.casefold() for line in lines]
        exact_expert_loaded = any(
            "expert file added:" in line and expected_expert in line
            for line in folded)
        optimized_inputs_loaded = any("optimized inputs info loaded" in line for line in folded)
        symbol_synced = any(f"{str(handoff.get('symbol', '')).casefold()}: symbol synchronized" in line
                            for line in folded)
        pass_started = any(re.search(r"optimization pass .* started", line) for line in folded)
        pass_succeeded = any(re.search(r"ontester result .*: passed in ", line) for line in folded)
        optimization_finished = any("optimization finished" in line for line in folded)
        if all((exact_expert_loaded, optimized_inputs_loaded, symbol_synced,
                pass_started, pass_succeeded)):
            validated.append({
                "worker_pid": int(registration["worker_pid"]),
                "address": registration["address"],
                "path": str(path.resolve()),
                "start_offset": offset,
                "bytes": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest().upper(),
                "last_write_epoch": path.stat().st_mtime,
                "exact_expert_loaded": True,
                "optimized_inputs_loaded": True,
                "symbol_synchronized": True,
                "optimization_pass_started": True,
                "optimization_pass_succeeded": True,
                "optimization_finished": optimization_finished,
            })
    if not validated:
        return None
    return {
        "source": "authenticated_fresh_local_agent_logs",
        "cycle_id": handoff.get("cycle_id"),
        "ea": handoff.get("ea"),
        "symbol": handoff.get("symbol"),
        "timeframe": timeframe,
        "ini_sha256": handoff.get("ini_sha256", {}).get(timeframe),
        "evidence": validated,
        "workers_live": any(int(item["worker_pid"]) in live_pids for item in validated),
        "optimization_finished": any(item.get("optimization_finished") is True for item in validated),
        "observed_utc": datetime.now(timezone.utc).isoformat(),
    }


def authenticated_live_gui_progress(runtime: dict, handoff: dict,
                                    counter: dict, workers: list[dict]) -> bool:
    """Accept a live Tester counter only for the exact bound run and workers.

    The UI is used here only to attach the observer to an already-running
    optimization. Final completion still requires the fresh Tester journal
    and structurally complete OPT artifact in the watcher.
    """
    timeframe = str(runtime.get("timeframe", ""))
    binding = runtime.get("input_schema_binding")
    if not isinstance(binding, dict):
        return False
    try:
        current = int(counter.get("current", 0))
        total = int(counter.get("total", 0))
        limit = int(runtime.get("max_passes", 0))
    except (TypeError, ValueError):
        return False
    return bool(
        counter.get("ok") is True
        and counter.get("observer_route") == "bounded_gui_helper_subprocess"
        and counter.get("button_state") == "STOP"
        and 0 < current < total <= limit
        and runtime.get("start_button_invoked") is True
        and runtime.get("cycle_id") == handoff.get("cycle_id")
        and runtime.get("ea") == handoff.get("ea")
        and runtime.get("symbol") == handoff.get("symbol")
        and timeframe in handoff.get("timeframe_order", [])
        and binding.get("status") == "INPUTS_BOUND"
        and binding.get("timeframe") == timeframe
        and binding.get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
        and runtime.get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
        and bool(workers)
        and all(int(item.get("pid", 0)) > 0 for item in workers)
    )


def worker_cpu_registrations(worker_pids: list[int]) -> dict[str, float]:
    samples = {}
    for pid in worker_pids:
        try:
            cpu = psutil.Process(int(pid)).cpu_times()
            samples[str(int(pid))] = float(cpu.user + cpu.system)
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    return samples


def build_command(handoff: dict, timeframe: str, terminal: Path,
                  ini_override: Path | None = None,
                  startup_ini_override: Path | None = None) -> list[str]:
    require(timeframe in handoff.get("timeframe_order", []), "timeframe is outside Stage 3 contract")
    ini = ini_override or Path(handoff["ini_files"][timeframe])
    require(ini.is_file(), f"prepared INI is missing: {ini}")
    require(terminal.resolve() == STAGE3_PORTABLE_TERMINAL.resolve(),
            "Stage 3 is bound to the isolated portable EightCap tester")
    require(terminal.is_file(), f"isolated Stage 3 terminal is missing: {terminal}")
    require(ini.resolve() == Path(handoff["ini_files"][timeframe]).resolve(),
            "Stage 3 launch must use the prepared INI for this timeframe")
    date_start = str(handoff["from_date"]).replace(".", "")
    date_end = str(handoff["to_date"]).replace(".", "")
    expected_prefix = f"{handoff['cycle_id']}.{handoff['ea']}.{handoff['symbol']}.{timeframe}.{date_start}_{date_end}."
    require(ini.name.startswith(expected_prefix),
            "prepared INI path does not encode the authenticated cycle and experiment identity")
    expected_hash = str(handoff.get("ini_sha256", {}).get(timeframe, "")).upper()
    require(len(expected_hash) == 64 and sha256(ini) == expected_hash,
            "Stage 3 prepared INI hash is missing or changed")
    profile = _read_profile_text(ini)
    require(_profile_value(profile, "Optimization") == "2",
            "Stage 3 prepared INI is not in optimization mode")
    optimized_rows = [line.strip() for line in profile.splitlines()
                      if line.strip().endswith("||Y")]
    require(optimized_rows, "Stage 3 prepared INI contains no optimized parameters")
    for key, expected in (("Expert", handoff["ea"]), ("Symbol", handoff["symbol"]),
                          ("Period", timeframe), ("FromDate", handoff["from_date"]),
                          ("ToDate", handoff["to_date"])):
        value = _profile_value(profile, key)
        if key == "Expert":
            value = Path(value.replace("/", "\\")).stem
        require(value.casefold() == str(expected).casefold(),
                f"Stage 3 INI {key} identity mismatch: {value} != {expected}")
    lineage = validate_stage3_launch_lineage(handoff)
    # A valid INI hash proves which bytes are launching, not that those bytes
    # still follow the current Stage 3 wide-grid and fixed-input policy.
    stage1_path = Path(handoff.get("stage1_spec", ""))
    require(stage1_path.is_file() and sha256(stage1_path) == handoff.get("stage1_spec_sha256"),
            "Stage 3 launch cannot authenticate the Stage 1 input contract")
    stage1_spec = load_object(stage1_path)
    runtime_spec = dict(stage1_spec)
    # Stage 2 owns the finalized optimizer grid. Re-deriving it from Stage 1
    # here can turn a valid Stage 2 grid into a different domain and reject the
    # very INI that Stage 3 is required to consume unchanged.
    runtime_spec["tester_config"] = lineage["approved_tester_config"]
    preparer.validate_tester_inputs(profile, runtime_spec, str(handoff.get("grid_mode", "default")))
    startup_ini = startup_ini_override or ini
    require(startup_ini.is_file(), f"Stage 3 startup INI is missing: {startup_ini}")
    if startup_ini_override is not None:
        canonical_startup = ini.with_name(ini.stem + ".no_autostart.ini").resolve()
        if startup_ini.resolve() != canonical_startup:
            recovered = authoritative_recovery_ini(handoff, timeframe)
            if startup_ini.resolve() == recovered.resolve():
                require(sha256(startup_ini) == expected_hash
                        and startup_ini.read_bytes() == ini.read_bytes(),
                        "Stage 3 startup retry profile is not the hash-bound authenticated wide INI")
            else:
                parameter_file_recovery = authoritative_parameter_file_recovery_ini(handoff, timeframe)
                require(startup_ini.resolve() == parameter_file_recovery.resolve(),
                        "Stage 3 startup retry profile is not an authenticated recovery profile")
    return [str(terminal.resolve()), "/portable", f"/config:{startup_ini.resolve()}"]


def authoritative_recovery_ini(handoff: dict, timeframe: str) -> Path:
    """Create one fresh-path copy of the authenticated wide INI for a no-opt retry."""
    require(timeframe in handoff.get("timeframe_order", []),
            "recovery INI timeframe is outside the Stage 3 contract")
    canonical = Path(handoff["ini_files"][timeframe]).resolve()
    expected_hash = str(handoff.get("ini_sha256", {}).get(timeframe, "")).upper()
    require(len(expected_hash) == 64, "recovery INI lacks an authenticated canonical hash")
    if canonical.is_file() and sha256(canonical) == expected_hash:
        payload = canonical.read_bytes()
    else:
        template = Path(handoff.get("template_ini") or "")
        spec_path = Path(handoff.get("stage1_spec") or "")
        require(template.is_file()
                and sha256(template) == str(handoff.get("template_ini_sha256") or "").upper(),
                "recovery INI cannot be regenerated from the authenticated template")
        require(spec_path.is_file()
                and sha256(spec_path) == str(handoff.get("stage1_spec_sha256") or "").upper(),
                "recovery INI cannot be regenerated without authenticated Stage 1 input authority")
        spec = load_object(spec_path)
        rendered = preparer.render_ini(
            template.read_text(encoding="utf-8-sig"), handoff["ea"], timeframe, spec,
            handoff.get("from_date", FROM_DATE), handoff.get("to_date", TO_DATE))
        payload = rendered.encode("utf-8")
        require(hashlib.sha256(payload).hexdigest().upper() == expected_hash,
                "regenerated recovery INI does not match the authenticated wide-profile hash")
    recovery = canonical.with_name(f"{canonical.stem}.journal-retry1.wide.ini")
    if recovery.exists():
        require(sha256(recovery) == expected_hash,
                "existing Stage 3 recovery INI is changed or mismatched")
    else:
        recovery.write_bytes(payload)
    require(sha256(recovery) == expected_hash and recovery.read_bytes() == payload,
            "Stage 3 recovery INI failed exact-byte verification")
    return recovery.resolve()


def authoritative_parameter_file_recovery_ini(handoff: dict, timeframe: str) -> Path:
    """Build a one-shot startup profile that explicitly loads the authenticated .set grid."""
    base_ini = authoritative_recovery_ini(handoff, timeframe)
    profile = _read_profile_text(base_ini)
    rows = []
    in_inputs = False
    for line in profile.splitlines():
        stripped = line.strip()
        if stripped == "[TesterInputs]":
            in_inputs = True
            continue
        if in_inputs and stripped.startswith("["):
            break
        if in_inputs and "=" in stripped:
            rows.append(stripped)
    optimized = [row for row in rows if row.endswith("||Y")]
    require(optimized, "authenticated recovery profile contains no selected optimization inputs")

    data_dir = STAGE3_PORTABLE_DATA_DIR
    profile_dir = data_dir / "MQL5" / "Profiles" / "Tester"
    profile_dir.mkdir(parents=True, exist_ok=True)
    cycle_token = re.sub(r"[^A-Za-z0-9_-]", "_", str(handoff["cycle_id"]))
    tf_token = re.sub(r"[^A-Za-z0-9_-]", "_", str(timeframe))
    set_path = profile_dir / f"{cycle_token}.{tf_token}.authenticated-retry.set"
    set_payload = ("\r\n".join(rows) + "\r\n").encode("utf-16")
    if set_path.exists():
        require(set_path.read_bytes() == set_payload,
                "existing explicit Stage 3 recovery .set differs from authenticated inputs")
    else:
        set_path.write_bytes(set_payload)
    require(sum(line.strip().endswith("||Y") for line in
                set_path.read_bytes().decode("utf-16").splitlines()) == len(optimized),
            "explicit Stage 3 recovery .set failed selected-input verification")

    tester_match = re.search(r"(?ims)^\[Tester\]\s*(.*?)(?=^\[|\Z)", profile)
    require(tester_match is not None, "authenticated recovery INI lacks [Tester]")
    tester_block = tester_match.group(0)
    tester_block = re.sub(r"(?im)^ExpertParameters\s*=.*(?:\r?\n|$)", "", tester_block)
    tester_block = tester_block.rstrip("\r\n") + f"\r\nExpertParameters={set_path.name}\r\n"
    rendered = profile[:tester_match.start()] + tester_block + profile[tester_match.end():]
    recovery = base_ini.with_name(base_ini.stem + ".journal-retry2.expert-parameters.ini")
    payload = rendered.encode("utf-8")
    if recovery.exists():
        require(recovery.read_bytes() == payload,
                "existing explicit Stage 3 recovery INI differs from authenticated profile")
    else:
        recovery.write_bytes(payload)
    check = _read_profile_text(recovery)
    require(_profile_value(check, "ExpertParameters") == set_path.name
            and sum(line.strip().endswith("||Y") for line in
                    re.split(r"(?im)^\[TesterInputs\]\s*$", check, maxsplit=1)[-1].splitlines()) == len(optimized)
            and sha256(set_path) == hashlib.sha256(set_payload).hexdigest().upper(),
            "explicit Stage 3 recovery INI/.set binding failed verification")
    return recovery.resolve()


def create_non_autostart_ini(handoff: dict, timeframe: str, canonical_ini: Path,
                             startup_path: Path | None = None) -> Path:
    """Create the exact authenticated Tester startup profile for this run.

    Defer the run until the exact authenticated EA and input grid have been
    rebound in the Tester UI. MT5 otherwise autostarts a configured Expert
    before schema binding and can use stale or default inputs. Keep the
    historical filename for runtime identity compatibility.
    """
    expected_bytes = non_autostart_ini_bytes(canonical_ini)
    startup = (Path(startup_path) if startup_path is not None else
               canonical_ini.with_name(canonical_ini.stem + ".no_autostart.ini"))
    startup.parent.mkdir(parents=True, exist_ok=True)
    temporary = startup.with_name(startup.name + ".tmp")
    temporary.write_bytes(expected_bytes)
    os.replace(temporary, startup)
    rendered = _read_profile_text(startup)
    tester_section = re.search(r"(?ims)^\[Tester\]\s*$([\s\S]*?)(?=^\[|\Z)", rendered)
    require(tester_section is not None
            and re.search(r"(?im)^Expert\s*=", tester_section.group(1)) is None,
            "Stage 3 deferred startup profile must omit Tester.Expert")
    require(_profile_value(rendered, "Optimization") == "2",
            "Stage 3 startup profile must keep genetic optimization enabled")
    require(_profile_value(rendered, "ExecutionMode") == "100",
            "Stage 3 startup profile must keep the 100 ms execution delay")
    require("[TesterInputs]" in rendered and "||Y" in rendered,
            "Stage 3 startup profile lost its optimization grid")
    return startup


def non_autostart_ini_bytes(canonical_ini: Path) -> bytes:
    """Remove only Tester.Expert so MT5 cannot autostart before UI binding."""
    raw = Path(canonical_ini).read_bytes()
    if raw.startswith(b"\xff\xfe"):
        encoding, prefix, payload = "utf-16-le", b"\xff\xfe", raw[2:]
    elif raw.startswith(b"\xfe\xff"):
        encoding, prefix, payload = "utf-16-be", b"\xfe\xff", raw[2:]
    elif raw.startswith(b"\xef\xbb\xbf"):
        encoding, prefix, payload = "utf-8", b"\xef\xbb\xbf", raw[3:]
    else:
        encoding, prefix, payload = "utf-8", b"", raw
    rendered = payload.decode(encoding, errors="strict")
    expert = _profile_value(rendered, "Expert").casefold()
    require(expert.startswith("kiyosaki") and expert.endswith(".ex5"),
            "Stage 3 canonical profile is missing its authenticated Kiyosaki Expert")
    require(_profile_value(rendered, "Optimization") == "2",
            "Stage 3 startup profile must enable genetic optimization")
    require(_profile_value(rendered, "ExecutionMode") == "100",
            "Stage 3 startup profile must set a 100 ms execution delay")
    section = re.search(r"(?ims)^\[Tester\]\s*$([\s\S]*?)(?=^\[|\Z)", rendered)
    require(section is not None, "Stage 3 canonical profile lacks a Tester section")
    tester_text = section.group(1)
    matches = list(re.finditer(r"(?im)^Expert\s*=[^\r\n]*(?:\r?\n|$)", tester_text))
    require(len(matches) == 1, "Stage 3 canonical Tester section must contain exactly one Expert")
    deferred_tester = tester_text[:matches[0].start()] + tester_text[matches[0].end():]
    deferred = rendered[:section.start(1)] + deferred_tester + rendered[section.end(1):]
    verified = re.search(r"(?ims)^\[Tester\]\s*$([\s\S]*?)(?=^\[|\Z)", deferred)
    require(verified is not None
            and re.search(r"(?im)^Expert\s*=", verified.group(1)) is None,
            "Stage 3 deferred profile still contains Tester.Expert")
    return prefix + deferred.encode(encoding)


def authenticated_startup_ini(handoff: dict, timeframe: str, canonical_ini: Path,
                              runtime: dict) -> Path:
    """Verify the exact Expert-bound /config INI before terminal identity checks."""
    recovery = runtime.get("journal_recovery_profile")
    if isinstance(recovery, dict):
        expected = Path(str(recovery.get("path") or "")).resolve()
        recovery_attempt = int(recovery.get("attempt") or 1)
        authenticated_recovery = (authoritative_parameter_file_recovery_ini(handoff, timeframe)
                                  if recovery_attempt == 2 else
                                  authoritative_recovery_ini(handoff, timeframe))
        require(expected == authenticated_recovery
                and recovery.get("canonical_ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
                and recovery.get("sha256") == sha256(expected),
                "Stage 3 journal-retry profile receipt is not authenticated")
        if recovery_attempt == 2:
            set_path = Path(str(recovery.get("expert_parameters_file") or "")).resolve()
            expected_set = STAGE3_PORTABLE_DATA_DIR / "MQL5" / "Profiles" / "Tester" / (
                re.sub(r"[^A-Za-z0-9_-]", "_", str(handoff["cycle_id"])) + "." +
                re.sub(r"[^A-Za-z0-9_-]", "_", str(timeframe)) + ".authenticated-retry.set")
            require(set_path == expected_set.resolve()
                    and set_path.is_file()
                    and recovery.get("expert_parameters_sha256") == sha256(set_path),
                    "Stage 3 explicit ExpertParameters file receipt is not authenticated")
    else:
        expected = canonical_ini.with_name(canonical_ini.stem + ".no_autostart.ini").resolve()
    startup = Path(str(runtime.get("startup_ini") or "")).resolve()
    require(startup == expected, "Stage 3 runtime startup INI path is not the authenticated derived profile")
    require(startup.is_file(), "Stage 3 runtime startup INI is missing")
    if isinstance(recovery, dict) and int(recovery.get("attempt") or 1) == 2:
        expected_retry = authoritative_parameter_file_recovery_ini(handoff, timeframe)
        require(startup == expected_retry and recovery.get("sha256") == sha256(startup),
                "Stage 3 runtime startup INI differs from the explicit authenticated retry profile")
    else:
        payload = non_autostart_ini_bytes(canonical_ini)
        require(startup.read_bytes() == payload,
                "Stage 3 runtime startup INI differs from the authenticated deferred profile")
    require(runtime.get("startup_ini_sha256") == sha256(startup),
            "Stage 3 runtime startup INI hash mismatch")
    return startup

def validate_stage3_launch_lineage(handoff: dict) -> dict:
    """Authenticate Stage 1/2 lineage and the exact source artifacts before MT5."""
    stage1 = Path(handoff.get("stage1_spec", ""))
    stage2 = Path(handoff.get("stage2_event", ""))
    require(stage1.is_file() and sha256(stage1) == handoff.get("stage1_spec_sha256"),
            "Stage 3 source Stage 1 specification is missing or changed")
    require(stage2.is_file() and preparer.consumption_insensitive_event_sha256(stage2) ==
            handoff.get("stage2_event_sha256"),
            "Stage 3 source Stage 2 event is missing or changed")
    event = load_object(stage2)
    require(event.get("schema_version") == 2 and event.get("stage") == 2 and
            event.get("status") == "complete" and event.get("cycle_id") == handoff.get("cycle_id") and
            event.get("ea") == handoff.get("ea") and
            event.get("stage1_spec_sha256") == handoff.get("stage1_spec_sha256"),
            "Stage 3 Stage 2 event identity/lineage mismatch")
    verification_path = Path(event.get("verification", ""))
    require(verification_path.is_file() and sha256(verification_path) == event.get("verification_sha256"),
            "Stage 3 Stage 2 verification is missing or changed")
    verification = load_object(verification_path)
    require(verification.get("passed") is True and verification.get("stage") == 2 and
            verification.get("cycle_id") == handoff.get("cycle_id") and
            verification.get("ea") == handoff.get("ea"),
            "Stage 3 Stage 2 verification identity mismatch")
    minato_key = "canonical_minato" if handoff.get("canonical_minato") else "score_include"
    minato_hash_key = "canonical_minato_sha256" if handoff.get("canonical_minato_sha256") else "score_include_sha256"
    for path_key, handoff_hash, event_hash in (
        ("source", "source_sha256", "mq5_sha256"),
        ("ex5", "ex5_sha256", "ex5_sha256"),
        (minato_key, minato_hash_key, "canonical_minato_sha256"),
    ):
        artifact = Path(handoff.get(path_key, ""))
        digest = handoff.get(handoff_hash)
        require(artifact.is_file() and digest and sha256(artifact) == digest == event.get(event_hash),
                f"Stage 3 {path_key} does not match authenticated Stage 2 artifact")
        verified_path = Path(verification.get(path_key, ""))
        require(verified_path.resolve() == artifact.resolve(),
                f"Stage 3 {path_key} path differs from Stage 2 verification")
    spec = load_object(stage1)
    source_text = Path(handoff.get("source", "")).read_text(encoding="utf-8-sig", errors="replace")
    approved = authenticated_stage2_config_for_launch(handoff, event, verification)
    require(preparer.config_sha256(approved) == handoff.get("approved_tester_config_sha256") ==
            event.get("approved_tester_config_sha256") == verification.get("approved_tester_config_sha256"),
            "Stage 3 approved tester configuration lineage mismatch")
    return {"stage1_spec_sha256": sha256(stage1),
            "stage2_event_sha256": preparer.consumption_insensitive_event_sha256(stage2),
            "stage2_verification_sha256": sha256(verification_path),
            "source_sha256": handoff["source_sha256"], "ex5_sha256": handoff["ex5_sha256"],
            "score_include_sha256": handoff["score_include_sha256"],
            "approved_tester_config_sha256": handoff["approved_tester_config_sha256"],
            "approved_tester_config": approved}


def authenticated_stage2_config_for_launch(handoff: dict, event: dict,
                                           verification: dict) -> dict:
    """Return the exact Stage 2-approved config used to materialize Stage 3 INIs."""
    stage2 = Path(handoff.get("stage2_event", "")).resolve()
    stage2_handoff_path = preparer.STAGE2_HANDOFF
    require(stage2_handoff_path.is_file(),
            "Stage 3 cannot authenticate the Stage 2 approved tester configuration")
    stage2_handoff = load_object(stage2_handoff_path)
    require(stage2_handoff.get("schema_version") == 2 and stage2_handoff.get("stage") == 2
            and stage2_handoff.get("cycle_id") == handoff.get("cycle_id")
            and stage2_handoff.get("ea") == handoff.get("ea")
            and Path(stage2_handoff.get("expected_event", "")).resolve() == stage2,
            "Stage 3 Stage 2 handoff identity mismatch")
    approved = preparer.authenticated_stage2_tester_config(stage2_handoff)
    require(preparer.config_sha256(approved) == handoff.get("approved_tester_config_sha256") ==
            stage2_handoff.get("approved_tester_config_sha256") ==
            event.get("approved_tester_config_sha256") == verification.get("approved_tester_config_sha256"),
            "Stage 3 approved tester configuration lineage mismatch")
    return approved


def _read_profile_text(path: Path) -> str:
    raw = Path(path).read_bytes()
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16").replace("\r\r\n", "\r\n")
    return raw.decode("utf-8-sig", errors="replace").replace("\r\r\n", "\r\n")


def _profile_value(profile: str, key: str) -> str:
    match = re.search(rf"(?im)^{re.escape(key)}\s*=\s*([^\r\n]+)\r?$", profile)
    require(match is not None, f"required tester profile field is missing: {key}")
    return match.group(1).split("||", 1)[0].strip().strip('"')


def _mt5_datetime_value(date_text: str) -> str:
    return str(int(datetime.strptime(date_text, "%Y.%m.%d").replace(tzinfo=timezone.utc).timestamp()))


def verify_minato_v2_prelaunch(handoff: dict, ini_path: Path,
                               manifest_path: Path = MINATO_V2_MANIFEST) -> dict | None:
    """Fail closed before launch when target score source, EX5, dates, or version differ."""
    profile_text = _read_profile_text(ini_path)
    handoff_ea = str(handoff.get("ea") or "")
    expert_match = re.search(r"(?im)^Expert\s*=\s*([^\r\n]+)\r?$", profile_text)
    profile_expert = expert_match.group(1).split("||", 1)[0].strip().strip('"') if expert_match else ""
    profile_ea = profile_expert.replace("/", "\\").rsplit("\\", 1)[-1]
    if profile_ea.casefold().endswith(".ex5"):
        profile_ea = profile_ea[:-4]
    require(bool(profile_ea), "required tester profile field is missing: Expert")
    require(bool(handoff_ea), "MINATO V2 handoff is missing EA identity")
    require(handoff_ea == profile_ea,
            f"MINATO V2 handoff/profile EA mismatch: {handoff_ea} != {profile_ea}")

    manifest = None
    manifest_entry = None
    if manifest_path.is_file():
        manifest = load_object(manifest_path)
        require(manifest.get("score_version") == MINATO_V2_VERSION,
                "MINATO V2 build manifest score version mismatch")
        if isinstance(manifest.get("eas"), dict):
            manifest_entry = manifest["eas"].get(handoff_ea)
        elif Path(str(manifest.get("source", ""))).stem == handoff_ea:
            manifest_entry = manifest

    artifact_specs = None
    if all(handoff.get(key) for key in ("source", "source_sha256", "score_include",
                                        "score_include_sha256", "ex5", "ex5_sha256")):
        require(handoff.get("score_version") == MINATO_V2_VERSION,
                "Stage 3 handoff score version is not MINATO_BASE_SCORE_V2.0")
        artifact_specs = handoff
    elif manifest_entry:
        artifact_specs = manifest_entry
    require(artifact_specs is not None,
            f"no V2 source/include/EX5 attestation exists for {handoff_ea}")

    checked = {}
    for path_key, hash_key in (("source", "source_sha256"),
                               ("score_include", "score_include_sha256"),
                               ("ex5", "ex5_sha256")):
        path = Path(artifact_specs.get(path_key, ""))
        expected = str(artifact_specs.get(hash_key, "")).upper()
        require(path.is_file() and bool(expected), f"MINATO V2 attestation lacks {path_key} evidence")
        actual = sha256(path)
        require(actual == expected, f"MINATO V2 {path_key} hash mismatch; refusing stale executable")
        checked[path_key] = {"path": str(path.resolve()), "sha256": actual}

    source_text = Path(checked["source"]["path"]).read_text(encoding="utf-8-sig", errors="replace")
    require("#include <minato_base_score_v2.mqh>" in source_text and
            "MinatoV2ScoreFromTester()" in source_text and
            "MinatoV2ObserveTick();" in source_text and
            "MinatoV2ConfigurationValid()" in source_text,
            "EA source does not implement the complete MINATO V2 hook contract")

    profile_text = _read_profile_text(ini_path)
    # A Stage 3 launch must use the exact authenticated prepared INI. Validate
    # the tester surface without rewriting the signed artifact in place.
    import normalize_opt_ini as _norm
    require(_norm.is_normalized(ini_path),
            "tester profile is not normalized to the agreed Stage-2/3 config "
            f"({sorted(_norm.REQUIRED)})")
    # Re-read after normalize.
    profile_text = _read_profile_text(ini_path)
    from_date = _profile_value(profile_text, "FromDate")
    to_date = _profile_value(profile_text, "ToDate")
    require(datetime.strptime(to_date, "%Y.%m.%d") > datetime.strptime(from_date, "%Y.%m.%d"),
            "MINATO V2 tester dates are invalid")
    require(_profile_value(profile_text, "InpMinatoTestStart") == _mt5_datetime_value(from_date),
            "InpMinatoTestStart does not match launcher FromDate")
    require(_profile_value(profile_text, "InpMinatoTestEnd") == _mt5_datetime_value(to_date),
            "InpMinatoTestEnd does not match launcher ToDate")
    require(_profile_value(profile_text, "InpMinatoExpectedScoreVersion") == MINATO_V2_VERSION,
            "tester profile score version is not MINATO_BASE_SCORE_V2.0")
    return {
        "score_version": MINATO_V2_VERSION,
        "source_identifier": (manifest_entry or artifact_specs).get("source_identifier"),
        "compiled_utc": (manifest_entry or artifact_specs).get("compiled_utc"),
        "from_date": from_date,
        "to_date": to_date,
        "starting_deposit": _profile_value(profile_text, "Deposit"),
        "symbol": _profile_value(profile_text, "Symbol"),
        "timeframe": _profile_value(profile_text, "Period"),
        "artifacts": checked,
        "manifest": str(manifest_path.resolve()) if manifest_path.is_file() else None,
        "manifest_sha256": sha256(manifest_path) if manifest_path.is_file() else None,
    }


def register(handoff_path: Path, timeframe: str, terminal: Path, opt_path: Path,
             tester_log: Path, worker_pids: list[int], runtime_path: Path | None = None,
             log_start_offset: int | None = None, launched_utc: str | None = None,
             opt_size_before: int | None = None, opt_mtime_before: float | None = None,
             data_dir: Path = DEFAULT_DATA_DIR, launched_epoch: float | None = None,
             agent_offsets_before: dict[str, int] | None = None,
             terminal_pid: int | None = None, replace_starting: bool = False,
             ini_override: Path | None = None) -> dict:
    handoff = load_object(handoff_path)
    command = build_command(handoff, timeframe, terminal, ini_override)
    require(bool(worker_pids) and all(pid > 0 for pid in worker_pids), "positive run-owned worker PIDs are required")
    require(terminal_pid is not None and int(terminal_pid) > 0, "positive run-owned terminal PID is required")
    runtime_path = runtime_path or Path(str(handoff["runtime_manifest"]).replace("\\\\", "\\"))
    runtime = {
        "schema_version": 2, "cycle_id": handoff["cycle_id"], "stage": 3, "ea": handoff["ea"],
        "timeframe": timeframe, "status": "STARTING", "command": command,
        "grid_mode": handoff.get("grid_mode", "default"),
        "ini": str((ini_override or Path(handoff["ini_files"][timeframe])).resolve()),
        "ini_sha256": sha256(ini_override or Path(handoff["ini_files"][timeframe])),
        "opt_path": str(opt_path.resolve()), "tester_log": str(tester_log.resolve()),
        "tester_log_start_offset": (tester_log.stat().st_size if tester_log.is_file() else 0) if log_start_offset is None else log_start_offset,
        "worker_pids": worker_pids, "launched_utc": launched_utc or utc_now(), "last_opt_size": -1,
        "agent_logs": agent_log_registrations(worker_pids, data_dir, launched_epoch, agent_offsets_before),
        "worker_cpu_seconds": worker_cpu_registrations(worker_pids),
        "first_pass_budget_seconds": int(handoff.get("first_pass_budget_seconds", 600)),
        "first_pass_hard_limit_seconds": int(handoff.get("first_pass_hard_limit_seconds", 900)),
        "progress_stall_budget_seconds": int(handoff.get("progress_stall_budget_seconds", 180)),
        "timeframe_budget_seconds": int(handoff.get("timeframe_budget_seconds", 900)),
        "opt_size_before": opt_size_before,
        "opt_mtime_before": opt_mtime_before,
        "poll_seconds": handoff["poll_seconds"], "normal_settle_seconds": handoff["normal_settle_seconds"],
        "fallback_settle_seconds": int(handoff.get("fallback_settle_seconds", 60)), "timeout_seconds": 7200,
        "max_passes": int(handoff.get("max_passes", 11000)), "pass_limit_settle_seconds": int(handoff.get("pass_limit_settle_seconds", 60)),
        "timeframe_event": str((runtime_path.parent / f"{handoff['cycle_id']}.stage3.{timeframe}.complete.json").resolve()),
        "zero_token": True,
        "terminal_pid": terminal_pid,
    }
    existing = load_object(runtime_path) if runtime_path.exists() else {}
    may_replace = (replace_starting and existing.get("status") == "STARTING"
                   and existing.get("cycle_id") == handoff.get("cycle_id")
                   and existing.get("terminal_pid") == terminal_pid)
    require(not existing or existing.get("status") not in {"STARTING", "OPTIMIZING"} or may_replace,
            "another Stage 3 runtime is already starting or optimizing")
    atomic_json(runtime_path, runtime)
    return runtime


def process_ids(name: str) -> list[int]:
    wanted = name.casefold()
    found = []
    for process in psutil.process_iter(("pid", "name")):
        try:
            if str(process.info.get("name") or "").casefold() == wanted:
                found.append(int(process.info["pid"]))
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    return found


def configured_terminal_ids() -> list[int]:
    """Return only standalone MT5 terminals launched with a tester /config."""
    found = []
    for process in psutil.process_iter(("pid", "name", "cmdline")):
        try:
            if str(process.info.get("name") or "").casefold() != "terminal64.exe":
                continue
            cmdline = [str(item) for item in (process.info.get("cmdline") or [])]
            if any(item.casefold().startswith("/config:") for item in cmdline):
                found.append(int(process.info["pid"]))
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    return found


def resolve_persistent_terminal(config_path: Path, launched_epoch: float, timeout: float = 120.0) -> int:
    """Resolve the persistent terminal PID after MT5 replaces its bootstrap PID."""
    expected = f"/config:{config_path.resolve()}".casefold()
    deadline = time.time() + timeout
    while time.time() < deadline:
        for pid in configured_terminal_ids():
            try:
                proc = psutil.Process(pid)
                command = [str(value) for value in proc.cmdline()]
                if (Path(proc.exe()).resolve() == EIGHTCAP_TERMINAL.resolve()
                        and proc.create_time() >= launched_epoch - 1.0
                        and any(value.casefold() == expected for value in command)):
                    time.sleep(2.0)
                    stable = psutil.Process(pid)
                    stable_cmd = [str(value) for value in stable.cmdline()]
                    if stable.is_running() and any(value.casefold() == expected for value in stable_cmd):
                        return int(pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                continue
        time.sleep(0.5)
    raise RuntimeError("MT5 did not expose a persistent terminal for the exact authenticated /config path")


def reconcile_existing_mt5_profile(terminal: Path, timeout: float = 15.0) -> list[int]:
    """Stop only the installed EightCap terminal; never stop unowned workers."""
    terminal = terminal.resolve()
    targets = []
    for process in psutil.process_iter(("pid", "name", "exe", "create_time")):
        try:
            name = str(process.info.get("name") or "").casefold()
            exe = Path(str(process.info.get("exe") or "")).resolve()
            exact_terminal = name == "terminal64.exe" and exe == terminal
            if exact_terminal:
                targets.append((process, float(process.info["create_time"])))
        except (OSError, psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    if not targets:
        return []
    pids = [int(process.pid) for process, _created in targets]
    for process, created in targets:
        try:
            current = psutil.Process(int(process.pid))
            require(Path(current.exe()).resolve() == terminal and
                    abs(float(current.create_time()) - created) < 0.01,
                    f"EightCap terminal PID identity changed before stop: {process.pid}")
            process.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs([process for process, _created in targets], timeout=timeout)
    for process in alive:
        try:
            prior = next(created for candidate, created in targets if candidate.pid == process.pid)
            current = psutil.Process(int(process.pid))
            require(Path(current.exe()).resolve() == terminal and
                    abs(float(current.create_time()) - prior) < 0.01,
                    f"EightCap terminal PID identity changed before force stop: {process.pid}")
            process.kill()
        except psutil.NoSuchProcess:
            pass
    _, still_alive = psutil.wait_procs(alive, timeout=10.0)
    require(not still_alive, f"authorized MT5 profile cleanup did not settle: {[p.pid for p in still_alive]}")
    return pids


def ensure_portable_agent_registry(registry_path: Path = EIGHTCAP_AGENTS_REGISTRY) -> dict:
    """Provision the isolated tester registry from a preserved same-root backup."""
    if registry_path.exists():
        return {"path": str(registry_path), "restored": False, "present": True}
    backups = sorted(registry_path.parent.glob("agents.dat.backup.*"),
                     key=lambda item: item.stat().st_mtime, reverse=True)
    require(backups, "isolated tester agents.dat is missing and no preserved backup exists")
    backup = backups[0]
    shutil.copy2(backup, registry_path)
    require(sha256(registry_path) == sha256(backup), "isolated tester agents.dat restore hash mismatch")
    return {"path": str(registry_path), "restored": True, "backup": str(backup),
            "sha256": sha256(registry_path)}


def reset_eightcap_agent_registry(registry_path: Path = EIGHTCAP_AGENTS_REGISTRY) -> dict:
    """Preserve then remove the stale EightCap local-agent registry."""
    if not registry_path.exists():
        return {"path": str(registry_path), "present_before": False, "removed": False}
    original_hash = sha256(registry_path)
    stamp = utc_now().replace(":", "").replace("-", "")
    backup = registry_path.with_name(f"{registry_path.name}.backup.{stamp}")
    require(not backup.exists(), f"EightCap agents.dat backup already exists: {backup}")
    shutil.copy2(registry_path, backup)
    require(sha256(backup) == original_hash, "EightCap agents.dat backup hash mismatch")
    registry_path.unlink()
    require(not registry_path.exists(), "stale EightCap agents.dat could not be removed")
    return {"path": str(registry_path), "present_before": True, "removed": True,
            "backup": str(backup), "sha256": original_hash, "backup_sha256": sha256(backup)}


def verify_eightcap_terminal(pid: int, ini_path: Path, launched_epoch: float) -> dict:
    process = psutil.Process(int(pid))
    executable = Path(process.exe()).resolve()
    command = [str(value) for value in process.cmdline()]
    require(executable == EIGHTCAP_TERMINAL.resolve(), "launched process is not the EightCap terminal")
    require(float(process.create_time()) >= launched_epoch - 1.0,
            "EightCap terminal process predates this Stage 3 launch")
    expected_config = f"/config:{ini_path.resolve()}".casefold()
    require(any(value.casefold() == expected_config for value in command),
            "EightCap terminal is not bound to the authenticated Stage 3 INI")
    return {"pid": int(pid), "exe": str(executable), "create_time": float(process.create_time()),
            "command": redact_process_command(command)}


def redact_process_command(arguments: list[str]) -> list[str]:
    """Preserve process identity while removing password values from recorded argv."""
    redacted = []
    redact_next = False
    password_option = re.compile(r"(?i)^((?:--?|/)(?:password|pass)(?::|=))")
    for raw in arguments:
        argument = str(raw)
        if redact_next:
            redacted.append("[REDACTED]")
            redact_next = False
            continue
        match = password_option.match(argument)
        if match:
            redacted.append(match.group(1) + "[REDACTED]")
        elif argument.casefold() in {"-password", "--password", "/password", "-pass", "--pass", "/pass"}:
            redacted.append(argument)
            redact_next = True
        else:
            redacted.append(argument)
    if redact_next:
        redacted.append("[REDACTED]")
    return redacted


def fresh_local_workers(baseline_pids: list[int], launched_epoch: float,
                        expected_addresses=None) -> list[dict]:
    """Return fresh local agents, constrained by launch-time registry evidence.

    When the runtime captured agent log offsets, their addresses are the
    authoritative set. Older manifests without that evidence retain the
    post-launch, localhost-only compatibility check without a port guess.
    """
    evidence = []
    baseline = {int(pid) for pid in baseline_pids}
    expected = {str(address).strip().casefold() for address in (expected_addresses or [])
                if str(address).strip()}
    for pid in process_ids("metatester64.exe"):
        if pid in baseline:
            continue
        try:
            process = psutil.Process(pid)
            created = float(process.create_time())
            command = [str(value) for value in process.cmdline()]
            exe = Path(process.exe()).resolve()
        except (psutil.AccessDenied, psutil.NoSuchProcess, OSError):
            continue
        address_match = WORKER_ADDRESS_RE.search(" ".join(command))
        address = address_match.group(1) if address_match else ""
        host, separator, raw_port = address.rpartition(":")
        try:
            port = int(raw_port)
        except ValueError:
            port = -1
        if (created < launched_epoch - 1.0 or not any(item.casefold() == "/local" for item in command)
                or host != "127.0.0.1" or not separator or not 1 <= port <= 65535
                or (expected and address.casefold() not in expected)):
            continue
        evidence.append({"pid": pid, "exe": str(exe), "create_time": created,
                         "address": address, "port": port,
                         "command": redact_process_command(command)})
    ports = [item["port"] for item in evidence]
    require(len(ports) == len(set(ports)), "fresh EightCap local workers have duplicate /address ports")
    return sorted(evidence, key=lambda item: item["port"])


def reconcile_known_stage5_terminals(root: Path, terminal: Path) -> list[dict]:
    """Stop only prior KCOS Stage 5 terminals with published completion evidence."""
    reconciled = []
    stage5_dir = root / "state" / "stage5_v2"
    for runtime_path in stage5_dir.glob("*.stage5.runtime.json"):
        try:
            runtime = load_object(runtime_path)
            pid = int(runtime.get("terminal_pid") or 0)
            completion = Path(str(runtime_path).replace(".stage5.runtime.json", ".stage5.complete.json"))
            event = root / "state" / "events" / f"{runtime.get('cycle_id')}.stage5.complete.json"
            if not pid or not completion.is_file() or load_object(completion).get("status") != "COMPLETE":
                continue
            if not event.is_file():
                continue
            event_data = load_object(event)
            if not event_data.get("verification_passed") or not event_data.get("consumed"):
                continue
            proc = psutil.Process(pid)
            if proc.name().casefold() != "terminal64.exe" or Path(proc.exe()).resolve() != terminal.resolve():
                continue
            command = " ".join(proc.cmdline())
            expected_ini = str(Path(runtime.get("forward_ini", "")).resolve())
            if not expected_ini or f"/config:{expected_ini}".casefold() not in command.casefold():
                continue
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except psutil.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
            reconciled.append({"pid": pid, "cycle_id": runtime.get("cycle_id"),
                               "runtime": str(runtime_path.resolve()), "completion": str(completion.resolve()),
                               "reason": "published_prior_stage5"})
        except (OSError, ValueError, psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
            continue
    return reconciled


def capture_owned_process(pid: int, role: str, terminal: Path, runtime_path: Path,
                          launched_epoch: float, expected_address: str | None = None,
                          expected_executable: Path | None = None, require_portable: bool = False) -> dict | None:
    """Capture a live process object only after proving its runtime identity."""
    pid = int(pid)
    if not psutil.pid_exists(pid):
        return None
    try:
        process = psutil.Process(pid)
        name = process.name().casefold()
        executable = (process.exe() or "").casefold()
        command = " ".join(process.cmdline()).casefold()
        created_epoch = process.create_time()
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        return None
    except psutil.AccessDenied as exc:
        # Windows service processes can deny psutil inspection.  Use the
        # native tasklist image name only to prove that a recorded worker PID
        # is no longer an MT5 worker; otherwise remain fail-closed.
        if role == "worker":
            try:
                result = subprocess.run(
                    ["tasklist.exe", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                    capture_output=True, text=True, timeout=5, check=False,
                )
                image = result.stdout.split(",", 1)[0].strip().strip('"').casefold()
                if image and image != "metatester64.exe":
                    return None
            except (OSError, subprocess.SubprocessError):
                pass
        raise ValueError(f"cannot verify recorded {role} PID {pid}: {exc}") from exc
    if role == "terminal":
        expected = (name == "terminal64.exe"
                    and Path(executable).resolve() == terminal.resolve()
                    and launched_epoch - 5 <= created_epoch <= launched_epoch + 300)
    elif role == "worker":
        expected_exe_path = Path(expected_executable).resolve() if expected_executable else None
        expected = (name == "metatester64.exe" and executable.endswith("metatester64.exe")
                    and (expected_exe_path is None or Path(executable).resolve() == expected_exe_path)
                    and (not require_portable or "/portable" in command)
                    and bool(expected_address)
                    and f"/address:{expected_address.casefold()}" in command
                    and launched_epoch - 5 <= created_epoch <= launched_epoch + 300)
    elif role == "watcher":
        expected = (name in {"python.exe", "pythonw.exe"}
                    and "kcos_stage3_watch_v2.py" in command
                    and str(runtime_path).casefold() in command
                    and launched_epoch - 5 <= created_epoch <= launched_epoch + 300)
    else:
        raise ValueError(f"unknown Stage 3 process role: {role}")
    if not expected:
        raise ValueError(f"recorded {role} PID {pid} has ambiguous or reused process identity")
    return {
        "process": process, "pid": pid, "role": role, "create_time": created_epoch,
        "terminal": terminal, "runtime_path": runtime_path,
        "launched_epoch": launched_epoch, "expected_address": expected_address,
        "expected_executable": expected_executable, "require_portable": require_portable,
    }


def _revalidate_owned_process(ownership: dict) -> bool:
    process = ownership["process"]
    role = ownership["role"]
    try:
        if process.pid != ownership["pid"] or process.create_time() != ownership["create_time"]:
            raise ValueError(f"recorded {role} PID {ownership['pid']} changed identity")
        # Re-open nothing: validate the same psutil.Process handle immediately
        # before a destructive operation.
        name = process.name().casefold()
        executable = (process.exe() or "").casefold()
        command = " ".join(process.cmdline()).casefold()
    except psutil.NoSuchProcess:
        return False
    except psutil.AccessDenied as exc:
        raise ValueError(f"cannot reverify recorded {role} PID {ownership['pid']}: {exc}") from exc
    if role == "terminal":
        valid = name == "terminal64.exe" and Path(executable).resolve() == ownership["terminal"].resolve()
    elif role == "worker":
        address = ownership.get("expected_address")
        expected_executable = ownership.get("expected_executable")
        executable_matches = (not expected_executable or
                              Path(executable).resolve() == Path(expected_executable).resolve())
        portable_matches = (not ownership.get("require_portable") or "/portable" in command)
        valid = (name == "metatester64.exe" and executable.endswith("metatester64.exe")
                 and executable_matches and portable_matches and bool(address)
                 and f"/address:{address.casefold()}" in command)
    else:
        valid = (name in {"python.exe", "pythonw.exe"} and "kcos_stage3_watch_v2.py" in command
                 and str(ownership["runtime_path"]).casefold() in command)
    if not valid:
        raise ValueError(f"recorded {role} PID {ownership['pid']} changed identity")
    return True


def terminate_verified_processes(ownership_records: list[dict], timeout: float = 10.0) -> None:
    terminated = []
    by_pid = {record["pid"]: record for record in ownership_records}
    for record in ownership_records:
        if not _revalidate_owned_process(record):
            continue
        try:
            record["process"].terminate()
            terminated.append(record["process"])
        except psutil.NoSuchProcess:
            continue
    _, alive = psutil.wait_procs(terminated, timeout=timeout)
    for process in alive:
        record = by_pid[process.pid]
        if not _revalidate_owned_process(record):
            continue
        try:
            process.kill()
        except psutil.NoSuchProcess:
            pass


def cleanup_previous_runtime(previous: dict, terminal: Path, runtime_path: Path,
                             include_worker_process_evidence: bool = False) -> None:
    """Stop only live PIDs proven to belong to the prior runtime manifest."""
    worker_evidence = previous.get("worker_process_evidence", []) if include_worker_process_evidence else []
    if not any((previous.get("terminal_pid"), previous.get("watcher_pid"), previous.get("worker_pids"), worker_evidence)):
        return
    status = str(previous.get("status", ""))
    terminal_statuses = {"TIMEFRAME_COMPLETE", "PARTIAL_TIMEFRAME_COMPLETE", "COMPUTE_BUDGET_EXCEEDED", "ROTATED", "CLEANED", "LAUNCH_FAILED", "START_REQUIRED", "INFRASTRUCTURE_FAILURE", "WATCHER_REGISTRATION_FAILED"}
    try:
        launched_epoch = datetime.strptime(
            previous["launched_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    except (KeyError, TypeError, ValueError):
        if any((previous.get("terminal_pid"), previous.get("watcher_pid"), previous.get("worker_pids"), worker_evidence)):
            raise ValueError("prior runtime has PIDs but no valid launch identity")
        return
    worker_addresses = {
        int(item["worker_pid"]): str(item["address"])
        for item in previous.get("agent_logs", [])
        if isinstance(item, dict) and item.get("worker_pid") and item.get("address")
    }
    worker_executables = {}
    worker_evidence_pids = []
    for item in worker_evidence:
        if not isinstance(item, dict):
            raise ValueError("prior runtime has malformed worker process evidence")
        raw_pid = item.get("pid")
        if not raw_pid or not item.get("address") or not item.get("exe"):
            raise ValueError("prior runtime worker process evidence is incomplete")
        pid = int(raw_pid)
        worker_evidence_pids.append(pid)
        worker_addresses[pid] = str(item["address"])
        worker_executables[pid] = Path(item["exe"])
    owned = [
        (previous.get("terminal_pid"), "terminal"),
        (previous.get("watcher_pid"), "watcher"),
        *[(pid, "worker") for pid in previous.get("worker_pids", [])],
        *[(pid, "worker") for pid in worker_evidence_pids if pid not in previous.get("worker_pids", [])],
    ]
    live_owned = []
    for raw_pid, role in owned:
        if not raw_pid:
            continue
        try:
            pid = int(raw_pid)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid recorded {role} PID: {raw_pid!r}") from exc
        try:
            expected_executable = (worker_executables.get(pid) or terminal.with_name("metatester64.exe")) if include_worker_process_evidence and role == "worker" else None
            ownership = capture_owned_process(pid, role, terminal, runtime_path, launched_epoch,
                                              worker_addresses.get(pid), expected_executable,
                                              include_worker_process_evidence and role == "worker")
        except ValueError as exc:
            if status in terminal_statuses and "ambiguous or reused process identity" in str(exc):
                continue
            raise
        if ownership is not None:
            live_owned.append(ownership)
    if live_owned and status not in terminal_statuses:
        raise ValueError(f"refusing automatic cleanup for live non-terminal prior runtime status: {status or 'unknown'}")
    # Preserve the final MT5 Journal evidence before any owned process is closed.
    if previous.get("tester_log"):
        previous["journal_pre_close"] = journal_snapshot(
            resolve_tester_journal_path(STAGE3_PORTABLE_DATA_DIR, Path(previous["tester_log"])),
            int(previous.get("tester_log_start_offset", 0)), "pre_close")
        atomic_json(runtime_path, previous)
    if live_owned:
        terminate_verified_processes(live_owned)


def cleanup_runtime_manifest(runtime: dict, runtime_path: Path) -> None:
    command = runtime.get("command") or []
    require(bool(command), "runtime has no terminal executable ownership identity")
    cleanup_previous_runtime(runtime, Path(command[0]), runtime_path, include_worker_process_evidence=True)


def wait_for_watcher_ready(ready_path: Path, watcher_pid: int, runtime_path: Path,
                           timeout_seconds: float = 60.0) -> dict:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if ready_path.is_file():
            ready = load_object(ready_path)
            require(ready.get("status") == "WATCHER_READY", "invalid watcher handshake status")
            ready_pid = int(ready.get("watcher_pid", 0))
            require(ready_pid > 0 and psutil.pid_exists(ready_pid),
                    "watcher handshake PID is not alive")
            require(Path(ready.get("runtime", "")).resolve() == runtime_path.resolve(),
                    "watcher handshake runtime mismatch")
            return ready
        time.sleep(0.05)
    raise ValueError("watcher registration handshake timed out")


def opt_candidates(handoff: dict, timeframe: str, data_dir: Path) -> list[Path]:
    start = handoff["from_date"].replace(".", "")
    end = handoff["to_date"].replace(".", "")
    # MT5's program identity can truncate the basename to 59 characters
    # (the observed 63-character filename including .ex5). Keep experiment
    # identity exact; freshness and header/INI validation remain mandatory.
    names = {handoff['ea'], handoff['ea'][:59]}
    matches = set()
    for name in names:
        pattern = f"{name}.{handoff['symbol']}.{timeframe}.{start}.{end}.*.opt"
        matches.update((data_dir / "Tester" / "cache").glob(pattern))
    return sorted(matches)


def _start_registered_watcher(runtime: dict, runtime_path: Path, handoff: dict,
                              watcher_path: Path | None = None) -> dict:
    runtime_path = runtime_path.resolve()
    runtime["status"] = "OPTIMIZING"
    runtime["optimizing_verified_utc"] = utc_now()
    atomic_json(runtime_path, runtime)
    watcher_path = (watcher_path or Path(__file__).with_name("kcos_stage3_watch_v2.py")).resolve()
    ready_path = runtime_path.with_name(runtime_path.name + ".watcher.ready.json")
    error_path = runtime_path.with_name(runtime_path.name + ".watcher.stderr.log")
    stamp = utc_now().replace(":", "").replace("-", "")
    for evidence_path in (ready_path, error_path):
        if evidence_path.exists():
            archived = evidence_path.with_name(f"{evidence_path.name}.previous.{stamp}")
            require(not archived.exists(), f"watcher evidence archive already exists: {archived}")
            evidence_path.replace(archived)
    flags = 0
    if os.name == "nt":
        flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW
    watcher_python = Path(sys.executable)
    hermes_python = Path.home() / "AppData" / "Local" / "hermes" / "hermes-agent" / "venv" / "Scripts" / "python.exe"
    if hermes_python.is_file():
        watcher_python = hermes_python
    with error_path.open("ab") as error_handle:
        watcher = subprocess.Popen(
            [str(watcher_python), str(watcher_path), "--runtime", str(runtime_path),
             "--heartbeat", str(Path(handoff["heartbeat"]).resolve()), "--ready", str(ready_path)],
            cwd=str(Path(__file__).parent), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=error_handle, creationflags=flags)
    try:
        wait_for_watcher_ready(ready_path, watcher.pid, runtime_path)
    except Exception as exc:
        runtime.update({"status": "WATCHER_REGISTRATION_FAILED", "watcher_failure": str(exc),
                        "watcher_pid": watcher.pid, "watcher_recovery_allowed": True})
        atomic_json(runtime_path, runtime)
        return runtime
    return load_object(runtime_path)


def archive_prior_timeframe_evidence(handoff: dict, timeframe: str) -> list[dict]:
    """Retain same-cycle artifacts while clearing canonical names for an explicit retry."""
    stamp = utc_now().replace(":", "").replace("-", "")
    output_dir = Path(handoff["runtime_manifest"]).parent
    decision_path = Path(handoff["decision"])
    paths = [
        output_dir / f"{handoff['cycle_id']}.stage3.{timeframe}.complete.json",
        output_dir / f"{handoff['cycle_id']}.stage3.{timeframe}.compute.json",
        output_dir / f"{handoff['cycle_id']}.stage3.{timeframe}.parse.json",
        output_dir / f"{handoff['cycle_id']}.stage3.{timeframe}.csv",
        decision_path,
        decision_path.parent.parent / "verification" /
        f"{handoff['cycle_id']}.stage3.verification.json",
        *opt_candidates(handoff, timeframe, DEFAULT_DATA_DIR),
    ]
    archived = []
    for path in paths:
        if not path.is_file():
            continue
        destination = path.with_name(f"{path.name}.archive.{stamp}")
        require(not destination.exists(), f"Stage 3 evidence archive already exists: {destination}")
        path.replace(destination)
        archived.append({"path": str(path.resolve()), "archive": str(destination.resolve()),
                         "sha256": sha256(destination)})
    return archived


def sync_cached_tester_profiles(data_dir: Path, ea: str, ini_path: Path) -> list[dict]:
    """Mirror the authenticated INI into matching MT5 Tester profiles.

    MT5 can reuse a truncated EA/profile filename and retain a profile that has
    no ``[TesterInputs]`` rows.  Quarantining that file alone is insufficient:
    MT5 can recreate the same blank profile on the next launch.  Preserve the
    old bytes as evidence, then write the authenticated INI to every matching
    profile and verify the resulting optimization surface before launch.
    """
    profile_dir = Path(data_dir) / "MQL5" / "Profiles" / "Tester"
    if not profile_dir.is_dir():
        return []
    canonical = _read_profile_text(ini_path)
    y_rows = [line.strip() for line in canonical.splitlines()
              if line.strip().endswith("||Y")]
    require(y_rows, "authenticated Stage 3 INI has no optimized [TesterInputs] rows")
    identity = {}
    for line in canonical.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() in {"Symbol", "Period", "FromDate", "ToDate"}:
            identity[key.strip()] = value.strip()
    symbol = identity.get("Symbol", "ETHUSD")
    timeframe = identity.get("Period", "")
    start = identity.get("FromDate", "").replace(".", "")
    end = identity.get("ToDate", "").replace(".", "")
    profile_marker = f".{symbol}.{timeframe}.{start}_{end}."
    ea_stem = str(ea).split("_202", 1)[0]
    # MT5 truncates long EA names in saved Tester profile filenames. Matching
    # only ea_stem misses profiles such as v5_closed_ba.ETHUSD.M5...; those
    # stale profiles can keep every optimization flag set to N.
    profile_prefixes = {ea_stem, str(ea)[:59]}
    stamp = utc_now().replace(":", "").replace("-", "")
    records = []
    for cached in sorted(profile_dir.glob("*.ini")):
        if cached.resolve() == ini_path.resolve() or cached.name.endswith(".ini.bak"):
            continue
        # Match both the complete EA name and MT5's truncated profile prefix.
        if not any(cached.name.startswith(prefix) for prefix in profile_prefixes) or profile_marker not in cached.name:
            continue
        backup = cached.with_name(f"{cached.name}.presync.{stamp}.evidence")
        require(not backup.exists(), f"cached Tester backup already exists: {backup}")
        before_sha = sha256(cached)
        shutil.copy2(cached, backup)
        cached.write_text(canonical, encoding="utf-8-sig", errors="strict")
        verified = _read_profile_text(cached)
        verified_y = [line.strip() for line in verified.splitlines()
                      if line.strip().endswith("||Y")]
        require(len(verified_y) == len(y_rows),
                f"cached Tester profile verification failed: {cached}")
        records.append({"path": str(cached.resolve()), "backup": str(backup.resolve()),
                        "before_sha256": before_sha, "after_sha256": sha256(cached),
                        "optimized_rows": len(verified_y), "profile_marker": profile_marker})
    if not records:
        # MT5 may have removed the active file while leaving the prior profile
        # as .bak/.stale evidence. Recreate that exact truncated identity so
        # the next portable launch cannot fall back to a blank profile.
        evidence = sorted(profile_dir.glob("*.ini.*"))
        for old in evidence:
            if not any(old.name.startswith(prefix) for prefix in profile_prefixes) or profile_marker not in old.name:
                continue
            base_name = old.name.split(".ini", 1)[0] + ".ini"
            seeded = profile_dir / base_name
            if seeded.exists():
                continue
            shutil.copy2(old, seeded)
            before_sha = sha256(seeded)
            seeded.write_text(canonical, encoding="utf-8-sig", errors="strict")
            verified = _read_profile_text(seeded)
            verified_y = [line.strip() for line in verified.splitlines()
                          if line.strip().endswith("||Y")]
            require(len(verified_y) == len(y_rows),
                    f"seeded Tester profile verification failed: {seeded}")
            records.append({"path": str(seeded.resolve()), "backup": str(old.resolve()),
                            "before_sha256": before_sha, "after_sha256": sha256(seeded),
                            "optimized_rows": len(verified_y), "profile_marker": profile_marker,
                            "action": "seeded_from_evidence"})
            break
    if not records:
        # First launch of a newly compiled EA has no saved Tester INI profile
        # to refresh or recover. Seed MT5's normal symbol/timeframe/date
        # profile identity from the authenticated INI. A .set alone did not
        # make MT5 honor the optimization grid in the clean-profile canary.
        seeded = profile_dir / f"{str(ea)[:59]}{profile_marker}120.ini"
        require(not seeded.exists(), f"unexpected Stage 3 Tester profile collision: {seeded}")
        seeded.write_text(canonical, encoding="utf-8-sig", errors="strict")
        verified = _read_profile_text(seeded)
        verified_y = [line.strip() for line in verified.splitlines()
                      if line.strip().endswith("||Y")]
        require(len(verified_y) == len(y_rows),
                f"seeded Tester INI verification failed: {seeded}")
        records.append({"path": str(seeded.resolve()), "before_sha256": None,
                        "after_sha256": sha256(seeded), "optimized_rows": len(verified_y),
                        "profile_marker": profile_marker, "profile_type": "ini",
                        "action": "seeded_from_authenticated_ini"})
    # MT5 also persists the active expert inputs in a truncated .set file.
    # A stale .set can override the correct .ini and cause "no optimized
    # parameter selected" even when the .ini has valid [TesterInputs].
    tester_rows = []
    in_inputs = False
    for line in canonical.splitlines():
        stripped = line.strip()
        if stripped == "[TesterInputs]":
            in_inputs = True
            continue
        if in_inputs and stripped.startswith("["):
            break
        if in_inputs and "=" in stripped:
            tester_rows.append(stripped)
    set_payload = ("; Stage 3 authenticated TesterInputs\r\n" +
                   "\r\n".join(tester_rows) + "\r\n")
    matching_sets = []
    for cached in sorted(profile_dir.glob("*.set")):
        if not any(cached.name.startswith(prefix) for prefix in profile_prefixes):
            continue
        matching_sets.append(cached)
        backup = cached.with_name(f"{cached.name}.presync.{stamp}.evidence")
        require(not backup.exists(), f"cached Tester .set backup already exists: {backup}")
        before_sha = sha256(cached)
        shutil.copy2(cached, backup)
        cached.write_bytes(set_payload.encode("utf-16"))
        verified = cached.read_bytes().decode("utf-16")
        verified_y = [line.strip() for line in verified.splitlines()
                      if line.strip().endswith("||Y")]
        require(len(verified_y) == len(y_rows),
                f"cached Tester .set verification failed: {cached}")
        records.append({"path": str(cached.resolve()), "backup": str(backup.resolve()),
                        "before_sha256": before_sha, "after_sha256": sha256(cached),
                        "optimized_rows": len(verified_y), "profile_marker": profile_marker,
                        "profile_type": "set"})
    if not matching_sets:
        # MT5 stores the active EA inputs under its truncated expert basename.
        # Without this exact-name profile, it can reopen the Tester with the
        # EX5 defaults and every optimization flag disabled even though the
        # authenticated launch INI contains valid Y rows.
        seeded = profile_dir / f"{str(ea)[:59]}.set"
        require(not seeded.exists(), f"unexpected Stage 3 .set profile collision: {seeded}")
        seeded.write_bytes(set_payload.encode("utf-16"))
        verified = seeded.read_bytes().decode("utf-16")
        verified_y = [line.strip() for line in verified.splitlines()
                      if line.strip().endswith("||Y")]
        require(len(verified_y) == len(y_rows),
                f"seeded Tester .set verification failed: {seeded}")
        records.append({"path": str(seeded.resolve()), "before_sha256": None,
                        "after_sha256": sha256(seeded), "optimized_rows": len(verified_y),
                        "profile_marker": profile_marker, "profile_type": "set",
                        "action": "seeded_from_authenticated_ini"})
    return records


def launch_and_register(handoff_path: Path, timeframe: str, terminal: Path = DEFAULT_TERMINAL,
                        data_dir: Path = DEFAULT_DATA_DIR, runtime_path: Path | None = None,
                        watcher_path: Path | None = None,
                        _ini_override: Path | None = None,
                        _recovery_reason: str | None = None) -> dict:
    """Launch the authenticated profile in installed EightCap and request GUI Start."""
    handoff = load_object(handoff_path)
    require(data_dir.resolve() == STAGE3_PORTABLE_DATA_DIR.resolve(),
            "Stage 3 requires the isolated portable tester data directory")
    ini_path = Path(handoff["ini_files"][timeframe]).resolve()
    recovery_profile = None
    if _ini_override is None:
        startup_ini_path = create_non_autostart_ini(handoff, timeframe, ini_path)
    else:
        authenticated_recovery = authoritative_recovery_ini(handoff, timeframe)
        override_path = Path(_ini_override).resolve()
        if override_path == authenticated_recovery:
            startup_ini_path = authenticated_recovery
            recovery_attempt = 1
            expert_parameters_file = None
        else:
            explicit_recovery = authoritative_parameter_file_recovery_ini(handoff, timeframe)
            require(override_path == explicit_recovery,
                    "Stage 3 retry may only use an authenticated recovery INI")
            startup_ini_path = explicit_recovery
            recovery_attempt = 2
            cycle_token = re.sub(r"[^A-Za-z0-9_-]", "_", str(handoff["cycle_id"]))
            tf_token = re.sub(r"[^A-Za-z0-9_-]", "_", str(timeframe))
            expert_parameters_file = STAGE3_PORTABLE_DATA_DIR / "MQL5" / "Profiles" / "Tester" / (
                f"{cycle_token}.{tf_token}.authenticated-retry.set")
        recovery_profile = {
            "path": str(startup_ini_path), "sha256": sha256(startup_ini_path),
            "canonical_ini": str(ini_path),
            "canonical_ini_sha256": handoff["ini_sha256"][timeframe],
            "reason": _recovery_reason or "NO_OPTIMIZED_PARAMETERS",
            "attempt": recovery_attempt,
        }
        if expert_parameters_file is not None:
            recovery_profile["expert_parameters_file"] = str(expert_parameters_file.resolve())
            recovery_profile["expert_parameters_sha256"] = sha256(expert_parameters_file)
    command = build_command(handoff, timeframe, terminal,
                            startup_ini_override=startup_ini_path)
    require(handoff.get("cycle_id") and handoff.get("ea") and handoff.get("symbol") and
            handoff.get("from_date") and handoff.get("to_date"), "Stage 3 handoff identity is incomplete")
    runtime_path = runtime_path or Path(str(handoff["runtime_manifest"]).replace("\\\\", "\\"))
    if runtime_path.is_file():
        prior = load_object(runtime_path)
        if prior.get("status") in {"START_REQUIRED", "STARTING", "OPTIMIZING"}:
            # A prior launch handoff is resumable only while its recorded
            # terminal/workers are alive.  A dead START_REQUIRED manifest is a
            # stale handoff, not an active run; preserve it below and allow a
            # fresh authenticated launch.
            live_terminal = bool(prior.get("terminal_pid") and
                                 psutil.pid_exists(int(prior["terminal_pid"])))
            live_workers = any(psutil.pid_exists(int(pid)) for pid in prior.get("worker_pids", []))
            if live_terminal or live_workers:
                raise ValueError("an uncompleted Stage 3 launch handoff already exists; resume it")
        cleanup_previous_runtime(prior, terminal, runtime_path)
        stamp = utc_now().replace(":", "").replace("-", "")
        archive_path = runtime_path.with_name(f"{runtime_path.name}.archive.{stamp}")
        require(not archive_path.exists(), f"Stage 3 runtime archive already exists: {archive_path}")
        runtime_path.replace(archive_path)

    # The portable tester must execute the same authenticated EX5 that Stage 2
    # compiled.  The live EightCap data directory and the isolated portable
    # tester have separate MQL5 trees; never assume they are synchronized.
    score_attestation = verify_minato_v2_prelaunch(handoff, ini_path)
    artifacts = score_attestation.get("artifacts", {}) if isinstance(score_attestation, dict) else {}
    authenticated_ex5 = Path(artifacts.get("ex5", {}).get("path", ""))
    require(authenticated_ex5.is_file(), "authenticated Stage 2 EX5 is missing")
    expert_rel = None
    for raw in _read_profile_text(ini_path).splitlines():
        if raw.startswith("Expert="):
            expert_rel = raw.split("=", 1)[1].strip().replace("\\", "/")
            break
    require(expert_rel, "Stage 3 INI is missing Expert path")
    portable_ex5 = STAGE3_PORTABLE_ROOT / "MQL5" / "Experts" / Path(expert_rel)
    portable_ex5.parent.mkdir(parents=True, exist_ok=True)
    stamp = utc_now().replace(":", "").replace("-", "")
    authenticated_ex5_sha256 = sha256(authenticated_ex5)
    portable_ex5_before_sha256 = sha256(portable_ex5) if portable_ex5.exists() else None
    portable_ex5_evidence = None
    if portable_ex5_before_sha256 != authenticated_ex5_sha256:
        if portable_ex5.exists():
            portable_ex5_evidence = portable_ex5.with_name(
                f"{portable_ex5.name}.stale-portable.{stamp}.evidence")
            shutil.copy2(portable_ex5, portable_ex5_evidence)
        shutil.copy2(authenticated_ex5, portable_ex5)
    portable_ex5_after_sha256 = sha256(portable_ex5)
    require(portable_ex5_after_sha256 == authenticated_ex5_sha256,
            "portable EX5 does not match authenticated Stage 2 EX5")
    deployment_reconciliation = {
        "status": "MATCHED" if portable_ex5_before_sha256 == authenticated_ex5_sha256 else "REPAIRED",
        "authenticated_ex5": str(authenticated_ex5.resolve()),
        "portable_ex5": str(portable_ex5.resolve()),
        "authenticated_sha256": authenticated_ex5_sha256,
        "portable_before_sha256": portable_ex5_before_sha256,
        "portable_after_sha256": portable_ex5_after_sha256,
        "stale_evidence": str(portable_ex5_evidence.resolve()) if portable_ex5_evidence else None,
        "verified": True,
    }
    authenticated_source = authenticated_ex5.with_suffix(".mq5")
    portable_source_reconciliation = None
    if authenticated_source.is_file():
        portable_source = portable_ex5.with_suffix(".mq5")
        authenticated_source_sha256 = sha256(authenticated_source)
        portable_source_before_sha256 = sha256(portable_source) if portable_source.exists() else None
        portable_source_evidence = None
        if portable_source_before_sha256 != authenticated_source_sha256:
            if portable_source.exists():
                portable_source_evidence = portable_source.with_name(
                    f"{portable_source.name}.stale-portable.{stamp}.evidence")
                shutil.copy2(portable_source, portable_source_evidence)
            shutil.copy2(authenticated_source, portable_source)
        portable_source_after_sha256 = sha256(portable_source)
        require(portable_source_after_sha256 == authenticated_source_sha256,
                "portable MQ5 does not match authenticated Stage 2 MQ5")
        portable_source_reconciliation = {
            "status": "MATCHED" if portable_source_before_sha256 == authenticated_source_sha256 else "REPAIRED",
            "authenticated_mq5": str(authenticated_source.resolve()),
            "portable_mq5": str(portable_source.resolve()),
            "authenticated_sha256": authenticated_source_sha256,
            "portable_before_sha256": portable_source_before_sha256,
            "portable_after_sha256": portable_source_after_sha256,
            "stale_evidence": str(portable_source_evidence.resolve()) if portable_source_evidence else None,
            "verified": True,
        }
    deployment_reconciliation["source"] = portable_source_reconciliation

    # MT5 may reuse a cached Tester profile instead of the [TesterInputs]
    # section supplied through /config:. Keep matching cached profiles in lockstep
    # with the authenticated cycle INI, preserving the old bytes as evidence.
    cached_profile_sync = sync_cached_tester_profiles(data_dir, handoff["ea"], ini_path)

    # Existing EightCap terminals are identified by the exact installed image.
    # Metatester PIDs without this run's ownership record are left untouched.
    try:
        require(sha256(ini_path) == handoff["ini_sha256"][timeframe],
                "prelaunch validation changed the authenticated Stage 3 INI")
    except Exception as exc:
        raise RuntimeError(f"Stage 3 prelaunch attestation failed: {exc}") from exc
    archived_evidence = archive_prior_timeframe_evidence(handoff, timeframe)
    stopped_terminals = reconcile_existing_mt5_profile(terminal)
    registry = {"path": str(EIGHTCAP_AGENTS_REGISTRY),
                "present": EIGHTCAP_AGENTS_REGISTRY.exists(),
                "sha256": sha256(EIGHTCAP_AGENTS_REGISTRY) if EIGHTCAP_AGENTS_REGISTRY.exists() else None}
    log_path = resolve_tester_journal_path(data_dir)
    log_offset = log_path.stat().st_size if log_path.is_file() else 0
    terminal_log_path = data_dir / "logs" / f"{datetime.now().strftime('%Y%m%d')}.log"
    terminal_log_offset = terminal_log_path.stat().st_size if terminal_log_path.is_file() else 0
    pre_run_journal = journal_snapshot(log_path, log_offset, "pre_run")
    workers_before = process_ids("metatester64.exe")
    candidates_before = {str(path.resolve()): {"size": path.stat().st_size,
                                               "mtime": path.stat().st_mtime}
                         for path in opt_candidates(handoff, timeframe, data_dir)}
    launched_epoch = time.time()
    launched_utc = datetime.fromtimestamp(launched_epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    agent_offsets = agent_log_offsets(data_dir, launched_epoch)
    minato_offsets = minato_diagnostic_file_offsets(data_dir)

    starting = {
        "schema_version": 2, "cycle_id": handoff["cycle_id"], "stage": 3,
        "ea": handoff["ea"], "symbol": handoff["symbol"], "timeframe": timeframe,
        "from_date": handoff["from_date"], "to_date": handoff["to_date"],
        "status": "STARTING", "command": command, "terminal_pid": None,
        "grid_mode": handoff.get("grid_mode", "default"), "worker_pids": [], "worker_baseline_pids": workers_before,
        "worker_process_evidence": [], "agent_logs": [], "launched_epoch": launched_epoch,
        "launched_utc": launched_utc, "ini": str(ini_path),
        "startup_ini": str(startup_ini_path.resolve()),
        "startup_ini_sha256": sha256(startup_ini_path),
        "ini_sha256": sha256(ini_path), "tester_log": str(log_path.resolve()),
        "tester_log_start_offset": log_offset, "journal_pre_run": pre_run_journal,
        "terminal_log": str(terminal_log_path.resolve()),
        "terminal_log_start_offset": terminal_log_offset,
        "opt_candidates_before": candidates_before,
        "agent_offsets_before": agent_offsets,
        "minato_diagnostic_offsets_before": minato_offsets,
        "agents_registry": registry,
        "archived_prior_timeframe_evidence": archived_evidence,
        "cached_profile_sync": cached_profile_sync,
        "deployment_reconciliation": deployment_reconciliation,
        "stopped_eightcap_terminal_pids": stopped_terminals,
        "minato_score_attestation": score_attestation,
        "first_pass_budget_seconds": int(handoff.get("first_pass_budget_seconds", 120)),
        "first_pass_hard_limit_seconds": int(handoff.get("first_pass_hard_limit_seconds", 200)),
        "progress_stall_budget_seconds": int(handoff.get("progress_stall_budget_seconds", 180)),
        "timeframe_budget_seconds": int(handoff.get("timeframe_budget_seconds", 7200)),
        "poll_seconds": int(handoff.get("poll_seconds", 10)),
        "normal_settle_seconds": int(handoff.get("normal_settle_seconds", 20)),
        "fallback_settle_seconds": int(handoff.get("fallback_settle_seconds", 60)),
        "timeout_seconds": int(handoff.get("timeframe_budget_seconds", 7200)),
        "max_passes": int(handoff.get("max_passes", 11000)),
        "pass_limit_settle_seconds": int(handoff.get("pass_limit_settle_seconds", 60)),
        "timeframe_event": str((runtime_path.parent /
            f"{handoff['cycle_id']}.stage3.{timeframe}.complete.json").resolve()),
        "zero_token": True,
    }
    if recovery_profile is not None:
        starting["journal_recovery_profile"] = recovery_profile
    atomic_json(runtime_path, starting)
    flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS if os.name == "nt" else 0
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               creationflags=flags)
    # The process is launched from the derived profile with [Tester] Expert=
    # removed so GUI schema binding can happen before Start. Authenticate the
    # persistent process against that exact command-line path.
    persistent_pid = resolve_persistent_terminal(startup_ini_path, launched_epoch)
    starting["terminal_pid"] = persistent_pid
    starting["terminal_window_moved_to_secondary"] = move_terminal_to_secondary_monitor(persistent_pid)
    starting["terminal_popen_pid"] = int(process.pid)
    starting["status"] = "START_REQUIRED"
    starting["start_required_reason"] = "Strategy Tester GUI Start must be handed off to desktop control"
    starting["desktop_handoff"] = {
        "action": "open_strategy_tester_and_click_start_if_not_already_running",
        "terminal_pid": persistent_pid, "ini": str(ini_path), "ini_sha256": sha256(ini_path),
        "startup_ini": str(startup_ini_path.resolve()),
        "startup_ini_sha256": sha256(startup_ini_path),
        "cycle_id": handoff["cycle_id"], "ea": handoff["ea"], "symbol": handoff["symbol"],
        "timeframe": timeframe, "from_date": handoff["from_date"], "to_date": handoff["to_date"],
    }
    atomic_json(runtime_path, starting)
    # Automatically invoke the authenticated Strategy Tester Start control for
    # this exact terminal.  If MT5 is still constructing the tester pane, the
    # bounded poll below retries through resume_start_required without claiming
    # a start until fresh workers and journal evidence exist.
    starting["start_button_invoked"] = False if not STAGE3_AUTOSTART else prepare_tester_schema_start(
        starting, ini_path, capture_start_journal=True, handoff=handoff)
    if starting["start_button_invoked"]:
        starting["start_button_last_click_epoch"] = time.time()
    # Count input attempts separately from delivered clicks. A retry that ran
    # before the terminal finished creating its menus must not consume the
    # bounded Start-click allowance.
    starting["start_button_attempted"] = True
    starting["start_button_attempt_count"] = 1
    starting["start_button_delivery_count"] = int(bool(starting["start_button_invoked"]))
    starting["start_button_next_attempt_epoch"] = time.time() + 10.0
    starting["start_retry_deadline_epoch"] = launched_epoch + max(
        30.0, float(starting.get("startup_grace_seconds", 300)))
    starting["start_button_attempt_history"] = [{
        "attempt": 1, "attempted_utc": utc_now(),
        "binding_status": (starting.get("input_schema_binding") or {}).get("status"),
        "delivered": bool(starting["start_button_invoked"]),
    }]
    atomic_json(runtime_path, starting)

    # Unattended EightCap launches can register tester workers shortly after
    # /config: starts the terminal.  Give that proven path a bounded window
    # to complete the same handshake that the old GUI workflow provided.
    # Only fall back to START_REQUIRED when the authenticated runtime cannot
    # prove fresh workers/journal/OPT evidence during this window.
    # Tester workers can take substantially longer than the terminal GUI to
    # spawn, especially when MT5 is starting a full local agent pool. Keep this
    # bounded but long enough to observe the authenticated worker/journal/OPT
    # handshake in the same launch attempt.
    handshake_seconds = float(handoff.get("post_launch_handshake_seconds", 120))
    deadline = time.time() + min(180.0, max(30.0, handshake_seconds))
    first_handshake = True
    while time.time() < deadline:
        time.sleep(0.1 if first_handshake else 2.0)
        first_handshake = False
        try:
            resumed = resume_start_required(handoff_path, runtime_path)
        except Exception:
            resumed = None
        if resumed and resumed.get("status") in {"OPTIMIZING", "TIMEFRAME_COMPLETE", "COMPUTE_BUDGET_EXCEEDED"}:
            return resumed
        if resumed and resumed.get("status") in {"INFRASTRUCTURE_FAILURE", "WATCHER_REGISTRATION_FAILED"}:
            return resumed
    final = load_object(runtime_path)
    if final.get("status") not in {"OPTIMIZING", "TIMEFRAME_COMPLETE", "COMPUTE_BUDGET_EXCEEDED",
                                    "INFRASTRUCTURE_FAILURE", "WATCHER_REGISTRATION_FAILED"}:
        final.update({"status": "START_REQUIRED",
                      "start_required_reason": "post-click handshake incomplete; Journal-aware retry required"})
        atomic_json(runtime_path, final)
    return final


def resume_start_required(handoff_path: Path, runtime_path: Path,
                          watcher_path: Path | None = None) -> dict:
    """Advance only after post-click workers, journal, growing OPT and identity verify."""
    handoff, runtime = load_object(handoff_path), load_object(runtime_path)
    command_text = " ".join(str(x) for x in runtime.get("command", []))
    if "/portable" not in command_text.casefold() or str(STAGE3_PORTABLE_TERMINAL).casefold() not in command_text.casefold():
        runtime.update({"status": "INFRASTRUCTURE_FAILURE", "failure_kind": "NON_ISOLATED_TERMINAL",
                        "failure_detail": "existing Stage 3 runtime is bound to the live MT5 singleton; preserved and rejected",
                        "infrastructure_only": True})
        atomic_json(runtime_path, runtime)
        return runtime
    require(runtime.get("status") == "START_REQUIRED", "runtime is not awaiting Strategy Tester Start")
    for key in ("cycle_id", "ea", "symbol", "from_date", "to_date"):
        require(runtime.get(key) == handoff.get(key),
                f"START_REQUIRED runtime {key} identity mismatch")
    timeframe = runtime["timeframe"]
    require(timeframe in handoff.get("timeframe_order", []), "START_REQUIRED timeframe is outside handoff")
    ini_path = Path(handoff["ini_files"][timeframe]).resolve()
    require(runtime.get("ini") == str(ini_path) and
            runtime.get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe) == sha256(ini_path),
            "START_REQUIRED prepared INI identity/hash mismatch")
    try:
        startup_ini_path = authenticated_startup_ini(handoff, timeframe, ini_path, runtime)
        terminal = verify_eightcap_terminal(int(runtime["terminal_pid"]), startup_ini_path,
                                            float(runtime["launched_epoch"]))
    except (psutil.NoSuchProcess, psutil.AccessDenied, OSError, ValueError) as exc:
        runtime.update({"status": "INFRASTRUCTURE_FAILURE", "failure_kind": "EIGHTCAP_TERMINAL_NOT_FRESH",
                        "failure_detail": f"EightCap terminal identity is not verifiable: {exc}",
                        "infrastructure_only": True})
        atomic_json(runtime_path, runtime)
        return runtime
    terminals = []
    for pid in process_ids("terminal64.exe"):
        try:
            if Path(psutil.Process(pid).exe()).resolve() == EIGHTCAP_TERMINAL.resolve():
                terminals.append(pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            continue
    if terminals != [int(runtime["terminal_pid"])]:
        runtime.update({"status": "INFRASTRUCTURE_FAILURE", "failure_kind": "TERMINAL_SINGLETON_COLLISION",
                        "failure_detail": f"EightCap terminal singleton collision: {terminals}",
                        "infrastructure_only": True})
        atomic_json(runtime_path, runtime)
        return runtime
    terminal_log_progress = read_authenticated_terminal_progress(runtime, handoff)
    runtime["terminal_log_progress"] = terminal_log_progress
    if terminal_log_progress.get("authenticated"):
        atomic_json(runtime_path, runtime)
    workers = fresh_local_workers(runtime.get("worker_baseline_pids", []),
                                  float(runtime["launched_epoch"]),
                                  expected_addresses=runtime.get("agent_offsets_before", {}).keys())
    if not workers:
        workers = [item for item in runtime.get("worker_process_evidence", [])
                   if int(item.get("create_time", 0)) >= float(runtime["launched_epoch"])]
    # Do not return before Journal inspection. MT5 can reject the tester in
    # the live Journal without creating workers; that diagnostic must be
    # captured and routed before declaring a GUI handoff timeout.
    # Resume the proven GUI handoff on every bounded supervisor tick while the
    # authenticated terminal is still START_REQUIRED. This closes the old
    # cron/UI gap where the launch-time click happened before MT5 finished
    # constructing the tester pane and no later tick retried it.
    start_attempt_count = int(runtime.get("start_button_attempt_count",
                                          1 if runtime.get("start_button_attempted") else 0))
    start_delivery_count = int(runtime.get("start_button_delivery_count",
                                          1 if runtime.get("start_button_invoked") else 0))
    prior_binding = runtime.get("input_schema_binding") or {}
    focus_blocked_exact_test = bool(
        prior_binding.get("navigator_tested") is False
        and prior_binding.get("navigator_test_detail")
            == "could not focus authenticated terminal for exact Navigator Test")
    exact_short_label_blocked = (
        prior_binding.get("navigator_tested") is False
        and prior_binding.get("navigator_test_detail")
            == "no unique long Navigator label maps to the exact authenticated EX5")
    if ("navigator_refresh_recovery" not in runtime
            and start_attempt_count >= 8 and start_delivery_count == 0
            and int(runtime.get("input_schema_binding_attempts", 0)) >= 8
            and runtime.get("status") == "START_REQUIRED"
            and prior_binding.get("status") == "EXPERT_NOT_READY"
            and prior_binding.get("exact_matches") == 0
            and (prior_binding.get("navigator_tested") is True
                 or focus_blocked_exact_test or exact_short_label_blocked)
            and not runtime.get("worker_pids")
            and not runtime.get("worker_process_evidence")
            and not runtime.get("watcher_pid")
            and not (runtime.get("terminal_log_progress") or {}).get("authenticated")
            and not runtime.get("last_opt_growth_epoch")):
        runtime["navigator_refresh_recovery"] = {
            "status": "authorized_once", "cycle_id": runtime.get("cycle_id"),
            "terminal_pid": runtime.get("terminal_pid"),
            "ini_sha256": runtime.get("ini_sha256"),
            "startup_ini_sha256": runtime.get("startup_ini_sha256"),
            "ex5_sha256": (runtime.get("deployment_reconciliation") or {}).get("authenticated_sha256"),
            "prior_binding_attempts": int(runtime.get("input_schema_binding_attempts", 0)),
            "prior_start_attempts": start_attempt_count,
            "reason": ("exact Navigator Test could not focus the authenticated terminal"
                       if focus_blocked_exact_test else
                       "full exact Navigator label was rejected by the prior truncation-only matcher"
                       if exact_short_label_blocked else
                       "exact Navigator Test handoff remained unbound after its initial Navigator refresh"),
            "authorized_utc": utc_now(),
        }
        # This exact runtime already exhausted its original launch grace
        # without delivering a Start click. Give the one-time Navigator
        # refresh repair a fresh, bounded window; never extend it on later
        # cron ticks.
        recovery_now = time.time()
        runtime["start_retry_deadline_epoch"] = recovery_now + 600.0
        runtime["start_button_next_attempt_epoch"] = recovery_now
        runtime["navigator_refresh_recovery"]["retry_window_seconds"] = 600
        atomic_json(runtime_path, runtime)
    start_attempt_limit = 16 if _navigator_refresh_recovery_is_bound(runtime) else 8
    retry_now = time.time()
    start_retry_deadline_raw = runtime.get("start_retry_deadline_epoch")
    if start_retry_deadline_raw is None:
        # Upgrade a START_REQUIRED manifest created before retry deadlines
        # were persisted. Its launch-time grace may have elapsed while the
        # cron was paused; allow one bounded recovery window on this exact,
        # identity-verified terminal, then persist the deadline so later
        # ticks cannot keep extending it.
        startup_grace = float(runtime.get("startup_grace_seconds", 300))
        start_retry_deadline = retry_now + max(600.0, min(900.0, startup_grace))
        runtime["start_retry_deadline_epoch"] = start_retry_deadline
        runtime["start_button_next_attempt_epoch"] = retry_now
        runtime["start_retry_migration"] = "legacy_start_required_bounded_recovery"
        atomic_json(runtime_path, runtime)
    else:
        start_retry_deadline = float(start_retry_deadline_raw)
    next_start_attempt = float(runtime.get("start_button_next_attempt_epoch", 0) or 0)
    # MT5 can spawn an idle local-agent pool after a Start click that arrived
    # before the Tester pane was ready. Worker presence and successful click
    # delivery are not proof that optimization began. Permit one additional
    # bounded click; the native binding refuses it when the Tester exposes Stop.
    journal_before_start = read_fresh_tester_journal(
        Path(runtime.get("tester_log", "")), int(runtime.get("tester_log_start_offset", 0)))
    bound_tester_run_started = authenticated_tester_run_started(
        runtime, handoff, journal_before_start)
    if (STAGE3_AUTOSTART and not terminal_log_progress.get("authenticated")
            and not bound_tester_run_started
            and start_delivery_count < 2 and start_attempt_count < start_attempt_limit
            and retry_now >= next_start_attempt and retry_now <= start_retry_deadline):
        runtime["start_button_last_attempt_utc"] = datetime.now(timezone.utc).isoformat()
        runtime["agent_registry_provision"] = ensure_portable_agent_registry()
        invoked = prepare_tester_schema_start(runtime, ini_path,
                                              capture_start_journal=True,
                                              handoff=handoff)
        if invoked:
            runtime["start_button_last_click_epoch"] = time.time()
        runtime["start_button_attempt_count"] = start_attempt_count + 1
        runtime["start_button_delivery_count"] = start_delivery_count + int(bool(invoked))
        runtime["start_button_next_attempt_epoch"] = time.time() + 10.0
        runtime["start_button_attempted"] = True
        runtime["start_button_last_result"] = "delivered" if invoked else "not_delivered"
        runtime["start_button_invoked"] = bool(runtime.get("start_button_invoked", False) or invoked)
        history = list(runtime.get("start_button_attempt_history", []))
        history.append({
            "attempt": start_attempt_count + 1,
            "attempted_utc": runtime["start_button_last_attempt_utc"],
            "binding_status": (runtime.get("input_schema_binding") or {}).get("status"),
            "delivered": bool(invoked),
        })
        runtime["start_button_attempt_history"] = history[-16:]
        runtime["start_required_reason"] = (
            "authenticated auto-click sent; waiting for fresh tester workers"
            if invoked else "visible Tester Start control not found")
        atomic_json(runtime_path, runtime)
    # Preserve a separate byte offset whenever upgrading a legacy terminal-log
    # manifest to the actual Tester journal. Offsets are never interchangeable.
    resolved_log = resolve_tester_journal_path(DEFAULT_DATA_DIR, Path(runtime.get("tester_log", "")) if runtime.get("tester_log") else None)
    if str(resolved_log.resolve()) != str(Path(runtime.get("tester_log", "")).resolve()):
        runtime["tester_log_legacy"] = runtime.get("tester_log")
        runtime["tester_log_legacy_start_offset"] = runtime.get("tester_log_start_offset")
        runtime["tester_log"] = str(resolved_log.resolve())
        runtime["tester_log_start_offset"] = int(runtime.get("tester_log_offsets_before", {}).get(str(resolved_log.resolve()), resolved_log.stat().st_size if resolved_log.is_file() else 0))
        atomic_json(runtime_path, runtime)
    journal = read_fresh_tester_journal(Path(runtime["tester_log"]),
                                        int(runtime["tester_log_start_offset"]))
    now = time.time()
    ui_journal = runtime.get("journal_ui_last", {"available": False, "lines": [], "error_lines": []})
    # Observe the Tester UI without selecting or clicking the Journal tab.
    # Fresh file-journal evidence is authoritative for startup diagnostics.
    ui_started = float(runtime.get("journal_ui_observation_started_epoch", 0) or 0)
    ui_last = float(runtime.get("journal_ui_last_sample_epoch", 0) or 0)
    binding_status = (runtime.get("input_schema_binding") or {}).get("status")
    observe_unstarted_busy_tester = binding_status == "ALREADY_RUNNING"
    if ((runtime.get("start_button_invoked") is True or observe_unstarted_busy_tester)
            and ui_started and now - ui_started <= 300.0
            and (not ui_last or now - ui_last >= 10.0)):
        if not ui_last:
            click_epoch = float(runtime.get("start_button_last_click_epoch", 0) or 0)
            remaining_delay = 0.1 - (time.time() - click_epoch) if click_epoch else 0.1
            if remaining_delay > 0:
                time.sleep(remaining_delay)
        try:
            ui_journal = capture_tester_journal_ui(
                int(runtime["terminal_pid"]),
                select_journal=False)
        except Exception as exc:
            ui_journal = {"available": False, "lines": [], "error_lines": [],
                          "error": str(exc), "captured_utc": utc_now()}
        runtime["journal_ui_post_start_selection_attempted"] = True
        runtime["journal_ui_post_start_selection_available"] = bool(
            ui_journal.get("available"))
        runtime["journal_ui_post_start_selection_method"] = ui_journal.get("method")
        runtime["journal_ui_selected"] = False
        runtime["journal_ui_selection_disabled"] = True
        runtime["journal_ui_last_sample_epoch"] = time.time()
        runtime["journal_ui_last"] = ui_journal
        baseline_lines = [str(line).strip().casefold() for line in
                          (runtime.get("journal_ui_start_baseline") or {}).get("lines", [])]
        remaining_baseline = list(baseline_lines)
        fresh_no_expert = []
        for line in (ui_journal.get("lines") or []):
            line = str(line).strip()
            if not NO_EXPERT_IN_START_CONFIG_RE.search(line):
                continue
            normalized = line.casefold()
            if normalized in remaining_baseline:
                remaining_baseline.remove(normalized)
            else:
                fresh_no_expert.append(line)
        if fresh_no_expert:
            journal["fresh_lines"] = [*journal.get("fresh_lines", []), *fresh_no_expert][-120:]
            journal["error_lines"] = [*journal.get("error_lines", []), *fresh_no_expert][-20:]
            journal["code"] = "NO_EXPERT_IN_START_CONFIG"
            journal["ui_journal"] = ui_journal
        else:
            fresh_ui_errors = []
            baseline_errors = {
                str(line).strip().casefold() for line in
                (runtime.get("journal_ui_start_baseline") or {}).get("error_lines", [])
            }
            for line in (ui_journal.get("error_lines") or []):
                line = str(line).strip()
                if line and line.casefold() not in baseline_errors:
                    fresh_ui_errors.append(line)
            if fresh_ui_errors:
                journal["fresh_lines"] = [*journal.get("fresh_lines", []), *fresh_ui_errors][-120:]
                journal["error_lines"] = [*journal.get("error_lines", []), *fresh_ui_errors][-20:]
                journal["code"] = "TESTER_JOURNAL_UI_REJECTED"
                journal["ui_journal"] = ui_journal
    # The post-Start segment must not suppress the no-expert error merely
    # because the deliberate pre-Start profile omits Expert.
    runtime["journal_after_run"] = journal_snapshot(
        Path(runtime["tester_log"]), int(runtime["tester_log_start_offset"]), "after_run")
    runtime["journal_after_run"]["ui_journal"] = ui_journal
    journal = filter_authenticated_no_expert_notice(journal, runtime, handoff)
    if journal.get("error_lines"):
        runtime.update({"status": "INFRASTRUCTURE_FAILURE", "failure_kind": "TESTER_JOURNAL_REJECTED",
                        "journal_diagnostic": journal, "infrastructure_only": True})
        atomic_json(runtime_path, runtime)
        return runtime
    timeframe_mismatch = tester_timeframe_mismatch(journal, handoff, timeframe)
    if timeframe_mismatch:
        runtime.update({
            "status": "INFRASTRUCTURE_FAILURE",
            "failure_kind": "TESTER_TIMEFRAME_MISMATCH",
            "failure_detail": (
                f"Tester executed {timeframe_mismatch}; authenticated INI requires "
                f"{handoff.get('symbol')},{timeframe}. Artifact preserved and not parseable for this timeframe."
            ),
            "journal_diagnostic": journal,
            "infrastructure_only": True,
        })
        atomic_json(runtime_path, runtime)
        return runtime
    # A genetic optimization can be live in MT5 before its parent Tester log
    # or cache file has produced a second size sample. If the exact terminal,
    # INI binding, fresh local workers, running Stop control, and positive live
    # pass counter all agree, register the existing cache stream with the
    # normal watcher. The watcher still requires a fresh journal and complete
    # OPT before publishing timeframe completion.
    try:
        import kcos_stage3_watch_v2 as stage3_watcher
        gui_now = time.time()
        last_ui_sample = float(runtime.get("tester_ui_sampled_epoch") or 0)
        cached_ui = runtime.get("tester_ui_counter")
        if (isinstance(cached_ui, dict) and last_ui_sample
                and gui_now - last_ui_sample < stage3_watcher.UI_OBSERVATION_INTERVAL_SECONDS):
            gui_counter = cached_ui
        else:
            gui_counter = stage3_watcher.read_gui_counter(int(runtime["terminal_pid"]))
            runtime["tester_ui_counter"] = gui_counter
            runtime["tester_ui_sampled_epoch"] = gui_now
            runtime["tester_ui_sampled_utc"] = utc_now()
            runtime["tester_ui_sampled_this_tick"] = True
            atomic_json(runtime_path, runtime)
    except Exception as exc:
        gui_counter = {"ok": False, "detail": str(exc)}
    if authenticated_live_gui_progress(runtime, handoff, gui_counter, workers):
        launched_epoch = float(runtime["launched_epoch"])
        opt_before = runtime.get("opt_candidates_before", {})
        live_candidates = []
        for candidate in opt_candidates(handoff, timeframe, DEFAULT_DATA_DIR):
            try:
                stat = candidate.stat()
            except OSError:
                continue
            before = opt_before.get(str(candidate.resolve()), {})
            if stat.st_mtime >= launched_epoch - 1.0 and (
                    candidate.resolve().as_posix() not in opt_before
                    and str(candidate.resolve()) not in opt_before
                    or stat.st_size != before.get("size")
                    or stat.st_mtime != before.get("mtime")):
                live_candidates.append((candidate, stat, before))
        if live_candidates:
            selected_opt, opt_stat, before = max(live_candidates, key=lambda item: item[1].st_mtime)
            worker_pids = [int(item["pid"]) for item in workers]
            registrations = agent_log_registrations(
                worker_pids, DEFAULT_DATA_DIR, launched_epoch,
                runtime.get("agent_offsets_before", {}), workers)
            now_epoch = time.time()
            live_progress = {
                "source": "authenticated_bound_tester_gui",
                "cycle_id": handoff["cycle_id"], "ea": handoff["ea"],
                "symbol": handoff["symbol"], "timeframe": timeframe,
                "ini_sha256": handoff["ini_sha256"][timeframe],
                "terminal_pid": int(runtime["terminal_pid"]),
                "worker_pids": worker_pids,
                "completed_passes": int(gui_counter["current"]),
                "total_passes": int(gui_counter["total"]),
                "button_state": gui_counter["button_state"],
                "observed_utc": utc_now(),
            }
            runtime.update({
                "status": "STARTING", "worker_pids": worker_pids,
                "worker_process_evidence": workers, "agent_logs": registrations,
                "expected_local_worker_addresses": sorted(
                    str(item["address"]) for item in workers if item.get("address")),
                "opt_path": str(selected_opt.resolve()),
                "opt_size_before": before.get("size"),
                "opt_mtime_before": before.get("mtime"),
                "last_opt_size": opt_stat.st_size,
                "last_opt_growth_epoch": now_epoch,
                "worker_cpu_seconds": worker_cpu_registrations(worker_pids),
                "tester_ui_live_progress": live_progress,
                "start_required_reason": "authenticated live Tester progress registered with watcher",
            })
            atomic_json(runtime_path, runtime)
            registered = register(
                handoff_path, timeframe, EIGHTCAP_TERMINAL, selected_opt,
                Path(runtime["tester_log"]), worker_pids, runtime_path,
                int(runtime["tester_log_start_offset"]), runtime["launched_utc"],
                before.get("size"), before.get("mtime"), DEFAULT_DATA_DIR,
                launched_epoch, runtime.get("agent_offsets_before", {}),
                int(runtime["terminal_pid"]), True)
            registered.update(runtime)
            registered["status"] = "STARTING"
            atomic_json(runtime_path, registered)
            return _start_registered_watcher(registered, runtime_path, handoff, watcher_path)
    opt_fresh = []
    prior_opt = runtime.get("opt_candidates_before", {})
    for path in opt_candidates(handoff, timeframe, DEFAULT_DATA_DIR):
        stat = path.stat()
        before = prior_opt.get(str(path.resolve()), {})
        if stat.st_mtime >= float(runtime["launched_epoch"]) - 1.0 and (
                stat.st_size != before.get("size") or stat.st_mtime != before.get("mtime")):
            opt_fresh.append(path)
    observed = runtime.get("opt_growth_observation", {})
    growth = []
    for path in opt_fresh:
        current_size = path.stat().st_size
        prior_size = observed.get(str(path.resolve()))
        if prior_size is not None and current_size > int(prior_size):
            growth.append(path)
        observed[str(path.resolve())] = current_size
    if (not growth and opt_fresh and
            any("optimization finished" in line.lower() for line in journal.get("fresh_lines", [])) and
            any("new records saved" in line.lower() for line in journal.get("fresh_lines", []))):
        growth = opt_fresh
    missing = []
    if not workers:
        missing.append("fresh_local_workers")
    if not journal.get("fresh_lines") and not growth:
        missing.append("fresh_tester_journal_lines")
    if not growth:
        missing.append("growing_fresh_opt")
    runtime["opt_growth_observation"] = observed
    runtime["terminal_process_evidence"] = terminal
    runtime["worker_process_evidence"] = workers
    runtime["journal_diagnostic"] = journal
    runtime["terminal_log_progress"] = terminal_log_progress
    agent_progress = fresh_agent_optimization_progress(handoff, runtime, workers,
                                                       DEFAULT_DATA_DIR, allow_exited=True)
    if agent_progress:
        runtime["tester_agent_progress"] = agent_progress
    if missing:
        if agent_progress:
            runtime.update({
                "status": "START_REQUIRED",
                "worker_pids": [item["pid"] for item in workers],
                "start_required_reason": (
                    "authenticated fresh agent logs confirm optimization passes; "
                    "waiting for parent Tester journal or timeframe OPT"
                ),
            })
            atomic_json(runtime_path, runtime)
            return runtime
        # Do not call an idle worker pool OPTIMIZING. Retain process and log
        # registrations for diagnosis, but stay START_REQUIRED until fresh
        # tester journal, growing OPT, or authenticated pass logs prove work.
        registrations = agent_log_registrations(
            [item["pid"] for item in workers], DEFAULT_DATA_DIR,
            float(runtime["launched_epoch"]),
            runtime.get("agent_offsets_before", {}), workers)
        runtime.update({
            "status": "START_REQUIRED",
            "worker_pids": [item["pid"] for item in workers],
            "worker_process_evidence": workers,
            "agent_logs": registrations,
            "start_required_reason": "waiting for authenticated optimization progress: " + ", ".join(missing),
        })
        atomic_json(runtime_path, runtime)
        return runtime
    addresses = {item["address"] for item in workers}
    registrations = agent_log_registrations([item["pid"] for item in workers], DEFAULT_DATA_DIR,
                                              float(runtime["launched_epoch"]),
                                              runtime.get("agent_offsets_before", {}), workers)
    registered_addresses = {item.get("address") for item in registrations}
    require(addresses == registered_addresses, "fresh /local worker addresses do not match fresh agent logs")
    selected_opt = max(growth, key=lambda path: path.stat().st_mtime)
    before = prior_opt.get(str(selected_opt.resolve()), {})
    runtime["status"] = "STARTING"
    runtime["worker_pids"] = [item["pid"] for item in workers]
    runtime["agent_logs"] = registrations
    runtime["expected_local_worker_addresses"] = sorted(addresses)
    runtime["fresh_journal_sha256"] = hashlib.sha256(
        _fresh_journal_bytes(Path(runtime["tester_log"]), int(runtime["tester_log_start_offset"]))).hexdigest().upper()
    runtime["opt_path"] = str(selected_opt.resolve())
    runtime["opt_size_before"] = before.get("size")
    runtime["opt_mtime_before"] = before.get("mtime")
    runtime["last_opt_size"] = selected_opt.stat().st_size
    runtime["last_opt_growth_epoch"] = time.time()
    runtime["worker_cpu_seconds"] = worker_cpu_registrations(runtime["worker_pids"])
    runtime["terminal_pid"] = int(runtime["terminal_pid"])
    runtime["launched_utc"] = runtime["launched_utc"]
    atomic_json(runtime_path, runtime)
    registered = register(handoff_path, timeframe, EIGHTCAP_TERMINAL, selected_opt,
                          Path(runtime["tester_log"]), runtime["worker_pids"], runtime_path,
                          int(runtime["tester_log_start_offset"]), runtime["launched_utc"],
                          before.get("size"), before.get("mtime"), DEFAULT_DATA_DIR,
                          float(runtime["launched_epoch"]), runtime.get("agent_offsets_before", {}),
                          int(runtime["terminal_pid"]), True)
    registered.update(runtime)
    registered["status"] = "OPTIMIZING"
    return _start_registered_watcher(registered, runtime_path, handoff, watcher_path)


def _fresh_journal_bytes(path: Path, start_offset: int) -> bytes:
    if not path.is_file() or path.stat().st_size <= start_offset:
        return b""
    with path.open("rb") as handle:
        handle.seek(start_offset)
        return handle.read()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handoff", type=Path, required=True)
    parser.add_argument("--timeframe", choices=("M5", "M15", "H1"), required=True)
    parser.add_argument("--terminal", type=Path, default=DEFAULT_TERMINAL)
    parser.add_argument("--launch", action="store_true")
    parser.add_argument("--resume-start", action="store_true")
    parser.add_argument("--runtime", type=Path)
    args = parser.parse_args()
    handoff = load_object(args.handoff)
    if args.resume_start:
        require(args.runtime is not None, "--runtime is required with --resume-start")
        result = resume_start_required(args.handoff, args.runtime)
    elif args.launch:
        result = launch_and_register(args.handoff, args.timeframe, args.terminal,
                                     runtime_path=args.runtime)
    else:
        result = {"launch": False, "command": build_command(handoff, args.timeframe, args.terminal),
                  "instruction": "Use the Stage 3 driver to launch and complete the desktop Start handoff."}
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
