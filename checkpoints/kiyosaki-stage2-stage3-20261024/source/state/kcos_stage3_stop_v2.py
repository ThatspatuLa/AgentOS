#!/usr/bin/env python3
"""Deterministic MT5 tester Stop-button control for Stage 3."""
from __future__ import annotations

import json
import subprocess
import ctypes
import ctypes.wintypes
from PIL import Image
from typing import Callable

try:
    import win32gui
    import win32ui
    import win32process
except ModuleNotFoundError:
    # Hermes' bundled Python may lack optional pywin32 extensions. The normal
    # Stop path uses Windows UI Automation; pywin32 is only needed by the
    # named-button and pixel fallbacks.
    win32gui = None
    win32ui = None
    win32process = None


_PS_STOP = r'''
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
$terminalPid = [int]$env:KCOS_TERMINAL_PID
$p = Get-Process -Id $terminalPid -ErrorAction Stop
if ($p.MainWindowHandle -eq 0) { throw "terminal has no main window" }
$root = [System.Windows.Automation.AutomationElement]::FromHandle($p.MainWindowHandle)
$buttons = $root.FindAll(
  [System.Windows.Automation.TreeScope]::Descendants,
  (New-Object System.Windows.Automation.PropertyCondition(
    [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
    [System.Windows.Automation.ControlType]::Button))
)
$found = $false
for ($i=0; $i -lt $buttons.Count; $i++) {
  $b = $buttons.Item($i)
  if ($b.Current.Name -match '^(Stop|停止)$') {
    if (-not $b.Current.IsEnabled) { throw "Stop button is disabled" }
    $invoke = $b.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern)
    $invoke.Invoke()
    $found = $true
    break
  }
}
if (-not $found) { throw "enabled MT5 Stop button not found" }
[pscustomobject]@{ stopped = $true; pid = $terminalPid } | ConvertTo-Json -Compress
'''


def _pixel_stop_fallback(terminal_pid: int) -> dict:
    """Click the red Stop control when MT5 omits it from UI Automation."""
    if win32gui is None or win32ui is None or win32process is None:
        raise RuntimeError("pixel Stop fallback requires pywin32, unavailable in this Python environment")
    hwnd = 0
    def visit(candidate, _):
        nonlocal hwnd
        if win32gui.IsWindowVisible(candidate):
            try:
                _, actual = win32process.GetWindowThreadProcessId(candidate)
                if actual == int(terminal_pid): hwnd = candidate
            except Exception: pass
    win32gui.EnumWindows(visit, None)
    if not hwnd: raise RuntimeError("no visible terminal window for Stop fallback")
    left, top, right, bottom = win32gui.GetWindowRect(hwnd)
    width, height = right-left, bottom-top
    wdc=win32gui.GetWindowDC(hwnd); dc=win32ui.CreateDCFromHandle(wdc)
    mem=dc.CreateCompatibleDC(); bmp=win32ui.CreateBitmap(); bmp.CreateCompatibleBitmap(dc,width,height); mem.SelectObject(bmp)
    if not ctypes.windll.user32.PrintWindow(hwnd,mem.GetSafeHdc(),2): raise RuntimeError("Stop fallback PrintWindow failed")
    info,bits=bmp.GetInfo(),bmp.GetBitmapBits(True)
    image=Image.frombuffer("RGB",(info["bmWidth"],info["bmHeight"]),bits,"raw","BGRX",0,1)
    mem.DeleteDC(); dc.DeleteDC(); win32gui.ReleaseDC(hwnd,wdc); win32gui.DeleteObject(bmp.GetHandle())
    points=[]
    for y in range(int(height*.88),height):
        for x in range(int(width*.88),width):
            r,g,b=image.getpixel((x,y))
            if r>150 and r>g*1.45 and r>b*1.35: points.append((x,y))
    if len(points)<30: raise RuntimeError("red MT5 Stop control not detected")
    x=sum(p[0] for p in points)//len(points); y=sum(p[1] for p in points)//len(points)
    # PrintWindow returns window-relative pixels. Convert that point to screen
    # coordinates for real mouse input; treating it as a screen point and then
    # calling ScreenToClient offsets it twice (the old fallback could report a
    # click while leaving the optimization running).
    win_left, win_top, _, _ = win32gui.GetWindowRect(hwnd)
    screen_point = (win_left + x, win_top + y)
    try:
        win32gui.SetForegroundWindow(hwnd)
    except Exception:
        # Foreground activation can be denied when Hermes runs in the background.
        # Send the click to the verified MT5 window instead of crashing the watcher.
        target = win32gui.WindowFromPoint(screen_point)
        if not target:
            target = hwnd
        import win32process
        _, target_pid = win32process.GetWindowThreadProcessId(target)
        if target_pid != int(terminal_pid):
            target = hwnd
        client_x, client_y = win32gui.ScreenToClient(target, screen_point)
        lparam = ((client_y & 0xffff) << 16) | (client_x & 0xffff)
        win32gui.SendMessage(target, 0x0201, 0x0001, lparam)
        win32gui.SendMessage(target, 0x0202, 0, lparam)
        return {"stopped":True,"pid":int(terminal_pid),"method":"pixel_window_message_click","point":[x,y]}
    ctypes.windll.user32.SetCursorPos(*screen_point)
    ctypes.windll.user32.mouse_event(0x0002, 0, 0, 0, 0)
    ctypes.windll.user32.mouse_event(0x0004, 0, 0, 0, 0)
    return {"stopped":True,"pid":int(terminal_pid),"method":"pixel_mouse_click","point":[x,y]}


def _click_named_stop_button(terminal_pid: int) -> dict | None:
    """Invoke the actual MT5 tester Stop button by its child-window handle."""
    if win32gui is None or win32process is None:
        return None

    matches: list[int] = []
    def visit_window(hwnd: int, _extra: object) -> None:
        try:
            _, pid = win32process.GetWindowThreadProcessId(hwnd)
            if pid == int(terminal_pid) and win32gui.IsWindowVisible(hwnd):
                def visit_child(child: int, _child_extra: object) -> None:
                    if (win32gui.GetClassName(child) == "Button"
                            and win32gui.GetWindowText(child).strip() in {"Stop", "停止"}
                            and win32gui.IsWindowEnabled(child)):
                        matches.append(child)
                win32gui.EnumChildWindows(hwnd, visit_child, None)
        except Exception:
            return
    win32gui.EnumWindows(visit_window, None)
    if len(matches) != 1:
        return None
    win32gui.SendMessage(matches[0], 0x00F5, 0, 0)  # BM_CLICK
    return {"stopped": True, "pid": int(terminal_pid), "method": "tester_stop_button"}


def click_stop(terminal_pid: int, runner: Callable = subprocess.run) -> dict:
    if int(terminal_pid) <= 0:
        raise ValueError("terminal_pid is required")
    direct = _click_named_stop_button(int(terminal_pid))
    if direct is not None:
        return direct
    env = dict(__import__('os').environ)
    env["KCOS_TERMINAL_PID"] = str(int(terminal_pid))
    completed = runner(
        ["powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", _PS_STOP],
        capture_output=True, text=True, timeout=30, check=False, env=env,
    )
    output = (completed.stdout or completed.stderr or "").strip()
    if completed.returncode != 0:
        return _pixel_stop_fallback(int(terminal_pid))
    try:
        result = json.loads(output)
    except json.JSONDecodeError as exc:
        return _pixel_stop_fallback(int(terminal_pid))
    if result.get("stopped") is not True:
        return _pixel_stop_fallback(int(terminal_pid))
    return result
