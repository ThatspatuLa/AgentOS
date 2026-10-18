import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from engine.lib.exact_root_lease import ExactRootQuiescenceLease, ExactRootLeaseError
from engine.lib.mention_store import MentionStore


class MentionStoreExactRootLeaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = MentionStore(self.root / "workflow.sqlite3")

    def test_mention_connection_waits_for_exact_root_writer(self):
        started = threading.Event()
        finished = threading.Event()
        errors = []

        def writer():
            try:
                started.set()
                with self.store.connect() as db:
                    db.execute("INSERT INTO mentions(id,payload) VALUES (?,?)",
                               ("synthetic-mention", "{}"))
                finished.set()
            except BaseException as exc:
                errors.append(exc)

        with ExactRootQuiescenceLease(self.root):
            thread = threading.Thread(target=writer)
            thread.start()
            self.assertTrue(started.wait(1))
            self.assertFalse(finished.wait(0.1))
        thread.join(timeout=2)
        self.assertTrue(finished.is_set())
        self.assertEqual(errors, [])

    def test_missing_or_invalid_lease_fails_mention_write_closed(self):
        with patch.object(ExactRootQuiescenceLease, "__init__",
                          side_effect=ExactRootLeaseError("synthetic_missing_lease")):
            with self.assertRaisesRegex(ExactRootLeaseError, "synthetic_missing_lease"):
                with self.store.connect() as db:
                    db.execute("INSERT INTO mentions(id,payload) VALUES (?,?)",
                               ("must-not-write", "{}"))
        with self.store.connect() as db:
            self.assertEqual(db.execute(
                "SELECT count(*) FROM mentions WHERE id='must-not-write'").fetchone()[0], 0)

    def test_wrong_root_lease_cannot_authorize_workflow_database(self):
        other = self.root / "other"
        other.mkdir()
        with ExactRootQuiescenceLease(other) as lease:
            with self.assertRaisesRegex(ExactRootLeaseError, "not_active_for_root"):
                lease.assert_bound_root(self.store.path.parent)


if __name__ == "__main__":
    unittest.main()
