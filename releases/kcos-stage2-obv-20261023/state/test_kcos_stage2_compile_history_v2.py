import unittest

from pathlib import Path

from kcos_stage2_compile_v2 import attempt_log_path, next_source_attempt, reusable_compile_result


class CompileHistoryTests(unittest.TestCase):
    def test_retry_budget_is_per_source_without_dropping_other_revision_history(self):
        history = [
            {"mq5_sha256": "OLD", "attempt": 1},
            {"mq5_sha256": "OLD", "attempt": 2},
            {"mq5_sha256": "NEW", "attempt": 1},
        ]
        self.assertEqual(next_source_attempt(history, "new", 5), 2)
        self.assertEqual(len(history), 3)
        self.assertEqual(next_source_attempt(history, "fresh", 5), 1)

    def test_attempt_ceiling_applies_only_to_matching_source_hash(self):
        history = [{"mq5_sha256": "SAME", "attempt": n} for n in range(1, 6)]
        with self.assertRaisesRegex(ValueError, "attempt limit exhausted"):
            next_source_attempt(history, "same", 5)
        self.assertEqual(next_source_attempt(history, "different", 5), 1)

    def test_attempt_log_names_are_scoped_to_source_revision(self):
        output = Path("compile.latest.json")
        first = attempt_log_path(output, 4, "A" * 64)
        second = attempt_log_path(output, 4, "B" * 64)
        self.assertNotEqual(first, second)
        self.assertEqual("compile.latest.attempt4." + "A" * 16 + ".log", first.name)

    def test_reuses_only_a_still_fresh_clean_compile_for_same_source_and_ex5(self):
        import hashlib
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "candidate.mq5"
            ex5 = root / "candidate.ex5"
            source.write_text("source", encoding="utf-8")
            ex5.write_bytes(b"compiled")
            os.utime(ex5, (source.stat().st_mtime + 1, source.stat().st_mtime + 1))
            source_hash = hashlib.sha256(source.read_bytes()).hexdigest().upper()
            ex5_hash = hashlib.sha256(ex5.read_bytes()).hexdigest().upper()
            prior = [{"mq5_sha256": source_hash, "passed": True, "timed_out": False,
                      "errors": 0, "warnings": 0, "fresh_ex5": True, "ex5_sha256": ex5_hash,
                      "attempt": 5, "cycle_id": "cycle-1", "ea": "EA"}]
            reusable = {"cycle_id": "cycle-1", "ea": "EA", "artifact_root": root / "archives"}
            reused = reusable_compile_result(prior, source_hash, source, ex5, **reusable)
            self.assertTrue(reused["reused"])
            self.assertEqual(5, reused["attempt"])
            self.assertIsNone(reusable_compile_result(prior, "different-source", source, ex5, **reusable))
            ex5.write_bytes(b"changed")
            self.assertIsNone(reusable_compile_result(prior, source_hash, source, ex5, **reusable))


if __name__ == "__main__":
    unittest.main()
