@echo off
REM Zen Overlay v2 launcher — runs the overlay attached to the user's
REM active desktop session. Running it from Explorer or via shortcut
REM binds the Tk window to the same Session/desktop the keyboard is in,
REM which is required for the global Ctrl+Shift+Z hotkey to dispatch.
REM
REM v2 calls POST /api/zen/chat on agent_os_server (port 8765).
REM No foreground Discord control — bot handles all Discord I/O.

start "" "C:\Users\Tyson\AppData\Local\hermes\hermes-agent\venv\Scripts\python.exe" "C:\Users\Tyson\Projects\ZenNew\zen_overlay\overlay.py"