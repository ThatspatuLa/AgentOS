"""
Tests for Memory World M5 — Live Data Bridge (refresh behavior).

The Python implementations here are pure functions that mirror the JS code
in agent-os.html so the same logic can be tested without a browser. The
server endpoint (GET /api/memory-world/mtime) is also tested here.

Run with:
    .venv/bin/python -m unittest tests.test_memory_world_refresh -v
"""

import json
import os
import sys
import time
import unittest
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Import the pure functions under test. Importing the full agent_os_server
# module also starts its HTTP handler class but does not start a server, so
# this is safe in unit tests.
from agent_os_server import (
    _memory_world_mtime_payload,  # type: ignore  # added by M5
    mtime_changed,  # type: ignore  # added by M5
    diff_node_ids,  # type: ignore  # added by M5
    format_relative_time,  # type: ignore  # added by M5
)


MW_PATH = REPO_ROOT / "data" / "memory-world.json"


class MtimeChangedTests(unittest.TestCase):
    """Polling comparator: did the file change since the last poll?"""

    def test_first_poll_returns_changed(self):
        # First poll (no last-known mtime) must always report a change so the
        # client fetches the body.
        self.assertTrue(mtime_changed(None, 1000.0))

    def test_same_mtime_returns_unchanged(self):
        self.assertFalse(mtime_changed(1000.0, 1000.0))

    def test_newer_mtime_returns_changed(self):
        self.assertTrue(mtime_changed(1000.0, 1001.5))

    def test_older_mtime_returns_unchanged(self):
        # Defensive: a stale-but-equal value is still "not changed since last poll".
        # An older mtime should NOT trigger a reload (data is at least as new as
        # the cache).
        self.assertFalse(mtime_changed(1000.0, 999.0))

    def test_float_tolerance(self):
        # mtime is a float; sub-second jitter should be treated as equal.
        self.assertFalse(mtime_changed(1000.0001, 1000.0004))


class DiffNodeIdsTests(unittest.TestCase):
    """State diff: which node IDs were added/removed between two snapshots?"""

    def test_no_change_returns_empty_lists(self):
        prev = ["a", "b", "c"]
        new = ["a", "b", "c"]
        added, removed = diff_node_ids(prev, new)
        self.assertEqual(added, [])
        self.assertEqual(removed, [])

    def test_added_node_detected(self):
        added, removed = diff_node_ids(["a", "b"], ["a", "b", "c"])
        self.assertEqual(added, ["c"])
        self.assertEqual(removed, [])

    def test_removed_node_detected(self):
        added, removed = diff_node_ids(["a", "b", "c"], ["a", "b"])
        self.assertEqual(added, [])
        self.assertEqual(removed, ["c"])

    def test_mixed_changes(self):
        added, removed = diff_node_ids(["a", "b", "c"], ["a", "c", "d"])
        self.assertEqual(added, ["d"])
        self.assertEqual(removed, ["b"])

    def test_empty_inputs(self):
        added, removed = diff_node_ids([], [])
        self.assertEqual(added, [])
        self.assertEqual(removed, [])

    def test_order_independent(self):
        # Sets, not lists — order should not matter.
        added, removed = diff_node_ids(["c", "a", "b"], ["b", "a", "c"])
        self.assertEqual(added, [])
        self.assertEqual(removed, [])


class FormatRelativeTimeTests(unittest.TestCase):
    """Last-updated timestamp: human-readable '5s ago' / '2m ago' / '1h ago'."""

    def test_seconds(self):
        now = 1000.0
        self.assertEqual(format_relative_time(995.0, now=now), "5s ago")
        self.assertEqual(format_relative_time(999.0, now=now), "1s ago")

    def test_minutes(self):
        now = 1000.0
        self.assertEqual(format_relative_time(940.0, now=now), "1m ago")
        self.assertEqual(format_relative_time(895.0, now=now), "1m ago")
        self.assertEqual(format_relative_time(880.0, now=now), "2m ago")

    def test_hours(self):
        now = 1000.0
        self.assertEqual(format_relative_time(100.0, now=now), "15m ago")  # not an hour
        self.assertEqual(format_relative_time(-2600.0, now=now), "1h ago")

    def test_days(self):
        now = 1000.0
        # 25 hours ago
        self.assertEqual(format_relative_time(1000.0 - 25 * 3600, now=now), "1d ago")

    def test_just_now(self):
        now = 1000.0
        self.assertEqual(format_relative_time(1000.0, now=now), "just now")
        self.assertEqual(format_relative_time(999.5, now=now), "just now")


class MtimePayloadTests(unittest.TestCase):
    """Server-side mtime payload shape."""

    def test_payload_shape(self):
        payload = _memory_world_mtime_payload(MW_PATH)
        self.assertIn("mtime", payload)
        self.assertIn("size", payload)
        self.assertIsInstance(payload["mtime"], (int, float))
        self.assertIsInstance(payload["size"], int)
        self.assertGreater(payload["mtime"], 0)
        self.assertGreater(payload["size"], 0)

    def test_payload_reflects_file_mtime(self):
        st = MW_PATH.stat()
        payload = _memory_world_mtime_payload(MW_PATH)
        # Floor mtime to seconds (stat returns sub-second on some FS, the
        # payload normalises to int seconds for stable polling).
        self.assertEqual(int(payload["mtime"]), int(st.st_mtime))
        self.assertEqual(payload["size"], st.st_size)


class ServerEndpointTests(unittest.TestCase):
    """
    Hit the running agent_os_server to confirm /api/memory-world/mtime
    returns the expected JSON shape. The server must be running on
    http://localhost:8765 for these tests to pass.
    """

    SERVER = "http://localhost:8765"

    def _server_up(self) -> bool:
        try:
            urlopen(self.SERVER + "/", timeout=1.0)
            return True
        except (URLError, HTTPError, OSError):
            return False

    def test_endpoint_returns_json_with_mtime(self):
        if not self._server_up():
            self.skipTest("agent_os_server not running on " + self.SERVER)
        req = Request(self.SERVER + "/api/memory-world/mtime")
        with urlopen(req, timeout=2.0) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
        self.assertIn("mtime", data)
        self.assertIn("size", data)
        self.assertGreater(data["mtime"], 0)
        self.assertGreater(data["size"], 0)

    def test_endpoint_cors_header(self):
        if not self._server_up():
            self.skipTest("agent_os_server not running on " + self.SERVER)
        req = Request(self.SERVER + "/api/memory-world/mtime")
        with urlopen(req, timeout=2.0) as resp:
            self.assertEqual(resp.headers.get("Access-Control-Allow-Origin"), "*")

    def test_memory_world_endpoint_still_works(self):
        # Sanity check that M4 isn't broken: existing /api/memory-world still returns data.
        if not self._server_up():
            self.skipTest("agent_os_server not running on " + self.SERVER)
        req = Request(self.SERVER + "/api/memory-world")
        with urlopen(req, timeout=2.0) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
        self.assertIn("nodes", data)
        self.assertIn("edges", data)
        self.assertIsInstance(data["nodes"], list)


if __name__ == "__main__":
    unittest.main(verbosity=2)
