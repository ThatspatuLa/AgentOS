import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from engine.lib.exact_root_lease import ExactRootLeaseError, ExactRootQuiescenceLease
from engine.lib.raw_response_retention import RawResponseRetentionHold, assess_raw_responses


class ExactRootQuiescenceLeaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "state"
        self.root.mkdir()

    def tearDown(self):
        self.temp.cleanup()

    def test_wrong_root_and_inactive_or_invalid_attestation_hold(self):
        lease = ExactRootQuiescenceLease(self.root, "a" * 32)
        with self.assertRaisesRegex(ExactRootLeaseError, "not_active"):
            lease.assert_bound_root(self.root)
        with lease:
            with self.assertRaisesRegex(ExactRootLeaseError, "not_active_for_root"):
                lease.assert_bound_root(self.root.parent)
            self.assertFalse(lease.verify_quiescence_attestation(
                "fabricated", self.root, "a" * 32))
            self.assertTrue(lease.verify_quiescence_attestation(
                lease.attestation, self.root, "a" * 32))
        with self.assertRaisesRegex(ExactRootLeaseError, "transaction_id_invalid"):
            ExactRootQuiescenceLease(self.root, "bad-id")

    def test_missing_lease_holds_raw_response_assessment(self):
        with self.assertRaisesRegex(RawResponseRetentionHold, "exact_root_lease_required"):
            assess_raw_responses(
                self.root, expected_root=self.root, release_sha256="0" * 64,
                source_identity_to_connector={}, isolation_guard=None,
                transaction_id="b" * 32, quiescence_attestation=None,
                now=datetime.now(timezone.utc))

    def test_symlink_root_and_symlink_gate_cannot_escape_root(self):
        outside = Path(self.temp.name) / "outside"
        outside.mkdir()
        sentinel = outside / "sentinel"
        sentinel.write_bytes(b"preserve")
        linked_root = Path(self.temp.name) / "linked-state"
        try:
            linked_root.symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError) as exc:
            self.skipTest(f"directory symlink unavailable: {exc}")
        with self.assertRaisesRegex(ExactRootLeaseError, "reparse_point"):
            ExactRootQuiescenceLease(linked_root)

        gate = self.root / ".nami-raw-dataset-write-gate.lock"
        gate.symlink_to(sentinel)
        lease = ExactRootQuiescenceLease(self.root)
        with self.assertRaises(OSError):
            lease.__enter__()
        self.assertEqual(sentinel.read_bytes(), b"preserve")

    def test_concurrent_leases_serialize_raw_dataset_writers(self):
        first_entered = threading.Event()
        release_first = threading.Event()
        second_entered = threading.Event()
        errors = []

        def first_writer():
            try:
                with ExactRootQuiescenceLease(self.root):
                    first_entered.set()
                    if not release_first.wait(5):
                        errors.append("first writer release timeout")
            except Exception as exc:  # surfaced in the main test thread
                errors.append(type(exc).__name__)

        def second_writer():
            try:
                if not first_entered.wait(5):
                    errors.append("first writer did not enter")
                    return
                with ExactRootQuiescenceLease(self.root):
                    second_entered.set()
            except Exception as exc:
                errors.append(type(exc).__name__)

        first = threading.Thread(target=first_writer)
        second = threading.Thread(target=second_writer)
        first.start()
        self.assertTrue(first_entered.wait(5))
        second.start()
        self.assertFalse(second_entered.wait(0.15))
        release_first.set()
        first.join(5)
        second.join(5)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertTrue(second_entered.is_set())
        self.assertEqual(errors, [])

    def test_directory_creation_rejects_symlink_escape(self):
        outside = Path(self.temp.name) / "outside-directory"
        outside.mkdir()
        try:
            (self.root / "raw").symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError) as exc:
            self.skipTest(f"directory symlink unavailable: {exc}")
        with ExactRootQuiescenceLease(self.root) as lease:
            with self.assertRaises(OSError):
                lease.ensure_directory("raw/source")
        self.assertEqual(list(outside.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
