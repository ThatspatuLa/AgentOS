"""Regression tests for the Stage 3 GUI pass observer runtime dependencies."""
import sys
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(r"C:\Users\Tyson\Projects\kiyosaki_stage2b")
sys.path.insert(0, str(ROOT / "state"))

import kcos_stage3_ui_pass_watch_v2 as ui_watch


class Stage3UiPassWatchTests(unittest.TestCase):
    def test_captures_bound_window_without_win32ui(self):
        image = ui_watch.Image.new("RGB", (800, 600))
        with (
            mock.patch.object(ui_watch, "window_for_pid", return_value=(123, (0, 0, 800, 600))),
            mock.patch.object(ui_watch.ImageGrab, "grab", return_value=image) as capture,
            mock.patch.object(ui_watch.pytesseract, "image_to_string", return_value=""),
            mock.patch.object(ui_watch, "read_tester_timing", return_value=(None, "")),
        ):
            result = ui_watch.read_counter(42)

        capture.assert_called_once_with(window=123)
        self.assertFalse(result["ok"])
        self.assertNotIn("win32ui", result.get("detail", ""))


if __name__ == "__main__":
    unittest.main()
