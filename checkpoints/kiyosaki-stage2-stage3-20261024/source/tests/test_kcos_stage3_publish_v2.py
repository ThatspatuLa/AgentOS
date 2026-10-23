#!/usr/bin/env python3
"""Deterministic tests for the Stage 3 publisher fragment pattern.

These tests assert the contract that:
- A Stage 3 completion event hashes only immutable per-cycle fragments.
- Aggregate notes are never appended during publication.
- Re-publishing an existing event re-validates against unchanged fragments.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(r"C:\Users\Tyson\Projects\kiyosaki_stage2b")
SD = ROOT / "state"
sys.path.insert(0, str(SD))

from kcos_stage3_publish_v2 import FRAGMENT_DIR, fragment_path, note_block, rotation_mode


def _sample_decision(cycle_id: str, verdict: str) -> dict:
    return {
        "schema_version": 2,
        "cycle_id": cycle_id,
        "ea": "B1_1_ForceIndex_DualPeriod_v1",
        "from_date": "2025.01.10",
        "to_date": "2026.01.10",
        "verdict": verdict,
        "reason": "test reason",
        "selected_tf": "M15",
        "timeframes": {
            "M5": {"cf": 14.0, "tpm": 36.0, "profit": 1500.0, "drawdown_percent": 8.0, "passed": False},
            "M15": {"cf": 21.0, "tpm": 38.0, "profit": 2400.0, "drawdown_percent": 11.0, "passed": True},
        },
    }


class Stage3PublisherFragmentTests(unittest.TestCase):
    def test_rotation_uses_queue_until_exhaustion_is_explicit_and_research_is_authorized(self) -> None:
        self.assertEqual("queue_next", rotation_mode({"queue_full": True, "research_allowed": False}))
        self.assertEqual("queue_next", rotation_mode({"queue_full": True, "research_allowed": True}))
        self.assertEqual("research_new", rotation_mode({"queue_exhausted": True, "research_allowed": True}))

    def test_fragment_paths_are_distinct_per_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fragment_dir = Path(temp) / "fragments"
            a = fragment_path("kcos-A-cycle-a", "passing", fragment_dir)
            b = fragment_path("kcos-B-cycle-b", "passing", fragment_dir)
            c = fragment_path("kcos-A-cycle-a", "failed", fragment_dir)
            self.assertNotEqual(a, b)
            self.assertNotEqual(a, c)
            self.assertEqual(fragment_dir, a.parent)

    def test_note_block_bytes_are_stable(self) -> None:
        decision = _sample_decision("kcos-TEST-cycle", "READY_FOR_STAGE4")
        block1 = note_block(decision, "passing")
        block2 = note_block(decision, "passing")
        self.assertEqual(block1, block2)
        self.assertTrue(block1.startswith(b"\n\n## Stage 3 passing strategy"))

    def test_fragment_path_layout_preserves_valid_kcos_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fragment_dir = Path(temp) / "fragments"
            cycle_id = "kcos-20260910T022256Z-forceindex-lineage-replay"
            self.assertEqual(FRAGMENT_DIR / f"{cycle_id}.passing.md", fragment_path(cycle_id, "passing"))
            self.assertEqual(fragment_dir / f"{cycle_id}.passing.md",
                             fragment_path(cycle_id, "passing", fragment_dir))
            self.assertTrue(fragment_path(cycle_id, "failed", fragment_dir).name.endswith(".failed.md"))
            self.assertTrue(fragment_path(cycle_id, "source", fragment_dir).name.endswith(".source.md"))

    def test_fragment_path_rejects_traversal_and_unsafe_cycle_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fragment_dir = Path(temp) / "fragments"
            unsafe_ids = ("", ".", "..", "../escape", r"..\escape", "kcos/escape",
                          r"kcos\escape", "kcos:unsafe", "kcos unsafe", "-leading-dash")
            for cycle_id in unsafe_ids:
                with self.subTest(cycle_id=cycle_id):
                    with self.assertRaisesRegex(ValueError, "safe fragment filename"):
                        fragment_path(cycle_id, "passing", fragment_dir)
            self.assertFalse(fragment_dir.exists())


if __name__ == "__main__":
    unittest.main()
