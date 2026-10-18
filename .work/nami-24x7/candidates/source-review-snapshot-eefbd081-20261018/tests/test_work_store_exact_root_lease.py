import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from engine.lib.exact_root_lease import ExactRootQuiescenceLease, ExactRootLeaseError
from engine.lib.work_store import WorkStore


class WorkStoreExactRootLeaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = WorkStore(self.root / "workflow.sqlite3")

    def test_workflow_mutation_waits_for_exact_root_writer(self):
        started = threading.Event()
        finished = threading.Event()
        outcome = []

        def writer():
            started.set()
            outcome.append(self.store.enqueue_hypotheses([{"id": "blocked-until-release"}]))
            finished.set()

        with ExactRootQuiescenceLease(self.root):
            thread = threading.Thread(target=writer)
            thread.start()
            self.assertTrue(started.wait(1))
            self.assertFalse(finished.wait(0.1))
        thread.join(timeout=2)
        self.assertTrue(finished.is_set())
        self.assertEqual(outcome, [1])

    def test_missing_or_invalid_lease_fails_workflow_write_closed(self):
        with patch.object(ExactRootQuiescenceLease, "__init__",
                          side_effect=ExactRootLeaseError("synthetic_missing_lease")):
            with self.assertRaisesRegex(ExactRootLeaseError, "synthetic_missing_lease"):
                self.store.enqueue_hypotheses([{"id": "must-not-write"}])
        self.assertEqual(self.store.pending_count(), 0)

    def test_wrong_root_lease_cannot_authorize_workflow_root(self):
        other = self.root / "other"
        other.mkdir()
        with ExactRootQuiescenceLease(other) as lease:
            with self.assertRaisesRegex(ExactRootLeaseError, "not_active_for_root"):
                lease.assert_bound_root(self.root)


if __name__ == "__main__":
    unittest.main()
