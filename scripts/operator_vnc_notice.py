#!/usr/bin/env python3
"""Tiny X11 notice window for the Agent OS M7 virtual desktop.

The TigerVNC desktop is intentionally minimal and may not have a window manager
or usable terminal. This creates one visible X11 window using ctypes only, so
no extra Python or X development packages are required.
"""

from __future__ import annotations

import ctypes
import os
import signal
import time


X = ctypes.cdll.LoadLibrary("libX11.so.6")

X.XOpenDisplay.argtypes = [ctypes.c_char_p]
X.XOpenDisplay.restype = ctypes.c_void_p
X.XDefaultScreen.argtypes = [ctypes.c_void_p]
X.XDefaultScreen.restype = ctypes.c_int
X.XRootWindow.argtypes = [ctypes.c_void_p, ctypes.c_int]
X.XRootWindow.restype = ctypes.c_ulong
X.XWhitePixel.argtypes = [ctypes.c_void_p, ctypes.c_int]
X.XWhitePixel.restype = ctypes.c_ulong
X.XBlackPixel.argtypes = [ctypes.c_void_p, ctypes.c_int]
X.XBlackPixel.restype = ctypes.c_ulong
X.XCreateSimpleWindow.argtypes = [
    ctypes.c_void_p,
    ctypes.c_ulong,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_uint,
    ctypes.c_uint,
    ctypes.c_uint,
    ctypes.c_ulong,
    ctypes.c_ulong,
]
X.XCreateSimpleWindow.restype = ctypes.c_ulong
X.XStoreName.argtypes = [ctypes.c_void_p, ctypes.c_ulong, ctypes.c_char_p]
X.XMapRaised.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
X.XCreateGC.argtypes = [ctypes.c_void_p, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_void_p]
X.XCreateGC.restype = ctypes.c_void_p
X.XSetForeground.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]
X.XFillRectangle.argtypes = [
    ctypes.c_void_p,
    ctypes.c_ulong,
    ctypes.c_void_p,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_uint,
    ctypes.c_uint,
]
X.XDrawString.argtypes = [
    ctypes.c_void_p,
    ctypes.c_ulong,
    ctypes.c_void_p,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_int,
]
X.XFlush.argtypes = [ctypes.c_void_p]


RUNNING = True


def stop(_signum, _frame):
    global RUNNING
    RUNNING = False


def draw(display, window, gc, width: int, height: int):
    blue = 0x102033
    cyan = 0x7DD3FC
    white = 0xF8FAFC
    green = 0x86EFAC
    muted = 0xA7B4C8

    X.XSetForeground(display, gc, blue)
    X.XFillRectangle(display, window, gc, 0, 0, width, height)

    lines = [
        (cyan, 70, 90, "Agent OS M7 Observation"),
        (white, 70, 135, "noVNC is connected to the virtual desktop."),
        (green, 70, 175, "If you can read this, the screen path is working."),
        (muted, 70, 225, "This is observation-only. Desktop control remains disabled."),
        (muted, 70, 265, "Use the Agent OS mobile page for Operator / Manual / Proof review."),
    ]
    for color, x, y, text in lines:
        encoded = text.encode("utf-8")
        X.XSetForeground(display, gc, color)
        X.XDrawString(display, window, gc, x, y, encoded, len(encoded))
    X.XFlush(display)


def main() -> int:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    display_name = os.environ.get("DISPLAY", ":2").encode("utf-8")
    display = X.XOpenDisplay(display_name)
    if not display:
        print(f"Could not open X display {display_name.decode()}", flush=True)
        return 1

    screen = X.XDefaultScreen(display)
    root = X.XRootWindow(display, screen)
    black = X.XBlackPixel(display, screen)
    white = X.XWhitePixel(display, screen)
    width, height = 980, 360
    window = X.XCreateSimpleWindow(display, root, 80, 80, width, height, 2, white, black)
    X.XStoreName(display, window, b"Agent OS M7 Observation")
    X.XMapRaised(display, window)
    gc = X.XCreateGC(display, window, 0, None)

    while RUNNING:
        draw(display, window, gc, width, height)
        time.sleep(2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
