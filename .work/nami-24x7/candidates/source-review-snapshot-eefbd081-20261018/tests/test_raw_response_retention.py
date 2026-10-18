import hashlib
import json
import tempfile
import threading
import time
import unittest
from unittest import mock
from datetime import datetime, timezone
from pathlib import Path

from engine.lib import nami_io
from engine.lib.contracts import canonical_dataset_bytes
from engine.lib.dataset_store import persist_frozen_events, persist_raw_response
from engine.lib.raw_response_retention import (
    RawDatasetStoreLease, RawResponseRetentionHold, _candidate_release_sha256, _safe_source,
    assess_raw_responses, stage_raw_response_assessment,
    verify_raw_response_assessment,
)
from clearance_fixtures import write_clearance_fixtures, write_source_selection_fixture


def sha(data):
    return hashlib.sha256(data).hexdigest()


def tree_hashes(root):
    return {p.relative_to(root).as_posix(): sha(p.read_bytes())
            for p in root.rglob("*") if p.is_file()}


class Lease(RawDatasetStoreLease):
    """Use the same cross-process gate as production dataset writers."""
    def __init__(self, root, transaction_id="d" * 32):
        super().__init__(root, transaction_id)


class RawResponseRetentionTests(unittest.TestCase):
    def test_assessment_fails_closed_without_handle_bound_io(self):
        with self.assertRaisesRegex(RawResponseRetentionHold, "root_component_unavailable"):
            assess_raw_responses(Path("untrusted"), expected_root=Path("untrusted"),
                release_sha256="0" * 64, source_identity_to_connector={},
                isolation_guard=None, transaction_id="d" * 32,
                quiescence_attestation=None, now=datetime.now(timezone.utc))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name) / "state"
        self.state.mkdir()
        self.raw_root = self.state / "raw"
        self.raw_root.mkdir()
        self.datasets = self.state / "datasets"
        self.datasets.mkdir()
        self.now = datetime.now(timezone.utc)
        self.release = _candidate_release_sha256()
        self.txid = "d" * 32
        self.identity = "synthetic-adapter"
        self.source_map = {self.identity: "news"}
        write_source_selection_fixture(self.state, ["news"])
        write_clearance_fixtures(self.state, ["news"])
        self.raw = self.add_response("market-fixture")

    def tearDown(self):
        self.temp.cleanup()

    def token(self, lease=None, txid=None):
        return (lease or Lease(self.state, txid or self.txid)).attestation

    def add_response(self, market, *, captured_at=None):
        captured = captured_at or self.now.isoformat()
        raw = persist_raw_response({"market": market, "data_source": self.identity,
            "events": [{"id": "fixture-event", "value": market}]}, state_dir=self.state,
            captured_at=captured)
        persist_frozen_events(hypothesis_id=f"hyp-{market}", market=market,
            events=[{"id": f"event-{market}", "value": 1}], source_identity=self.identity,
            raw_response_id=raw["response_id"], captured_at=captured, state_dir=self.state)
        return raw

    def assess(self, **kwargs):
        lease = kwargs.pop("lease", Lease(self.state))
        return assess_raw_responses(self.state, expected_root=self.state,
            release_sha256=self.release, source_identity_to_connector=self.source_map,
            isolation_guard=lease, transaction_id=self.txid,
            quiescence_attestation=self.token(lease), now=self.now, **kwargs)

    def test_explicit_p3m_and_deterministic_complete_catalog(self):
        first = self.assess()
        second = self.assess()
        self.assertTrue(first["complete"])
        self.assertFalse(first["retention_ready"])
        self.assertEqual(first["descendant_record_count"], 1)
        self.assertEqual(first["inventory_sha256"], second["inventory_sha256"])

    def test_writer_id_canonicalization_matches_non_ascii_payloads(self):
        self.add_response("marché")
        result = self.assess()
        self.assertEqual(result["raw_response_count"], 2)

    def test_malformed_record_holds(self):
        path = next(self.raw_root.rglob("*.jsonl"))
        path.write_bytes(b"{not-json}\n")
        with self.assertRaisesRegex(RawResponseRetentionHold, "record_malformed"):
            self.assess()

    def test_duplicate_ids_hold(self):
        path = next(self.raw_root.rglob("*.jsonl"))
        content = path.read_bytes()
        path.write_bytes(content + content)
        with self.assertRaisesRegex(RawResponseRetentionHold, "duplicate_id"):
            self.assess()

    def test_bad_response_id_digest_holds(self):
        path = next(self.raw_root.rglob("*.jsonl"))
        row = json.loads(path.read_text().splitlines()[0])
        row["response_id"] = "0" * 64
        path.write_text(json.dumps(row) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(RawResponseRetentionHold, "id_digest_mismatch"):
            self.assess()

    def test_invalid_p3m_metadata_holds_without_mutation(self):
        path = next(self.raw_root.rglob("*.jsonl"))
        row = json.loads(path.read_text().splitlines()[0])
        row["retention_policy"] = "unknown"
        path.write_text(json.dumps(row) + "\n", encoding="utf-8")
        before = tree_hashes(self.state)
        with self.assertRaisesRegex(RawResponseRetentionHold, "p3m_metadata_invalid"):
            self.assess()
        self.assertEqual(before, tree_hashes(self.state))

    def test_missing_authorization_mapping_and_tampered_evidence_hold(self):
        self.source_map = {}
        with self.assertRaisesRegex(RawResponseRetentionHold, "authorization_missing"):
            self.assess()
        self.source_map = {self.identity: "news"}
        evidence = next((self.state / "source_clearance_evidence").glob("*"))
        evidence.write_bytes(b"tampered reviewed evidence")
        with self.assertRaisesRegex(RawResponseRetentionHold, "authorization_evidence_invalid"):
            self.assess()

    def test_active_run_clearance_start_pin_is_enforced(self):
        from engine.lib.source_clearance import source_clearance_snapshot, source_selection_snapshot
        selection = source_selection_snapshot(self.state)
        clearance = source_clearance_snapshot(self.state, ["news"], now=self.now)
        control = {"canary_started_at": self.now.isoformat(),
            "source_selection_fingerprint": selection["source_selection_fingerprint"],
            "source_selection_enabled": selection["source_selection_enabled"],
            "source_processing_profile": selection["source_processing_profile"],
            "source_clearance_fingerprint": clearance["source_clearance_fingerprint"],
            "source_clearance_sources": clearance["source_clearance_sources"]}
        (self.state / "service_control.json").write_text(json.dumps(control), encoding="utf-8")
        registry_path = self.state / "source_clearance.json"
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
        registry["records"]["news"]["retention_rule"] = "reviewed P3M retention updated"
        registry_path.write_text(json.dumps(registry), encoding="utf-8")
        with self.assertRaisesRegex(RawResponseRetentionHold, "authorization_evidence_invalid"):
            self.assess()

    def _rewrite_dataset(self, mutate_capture, mutate_metadata):
        data_path = next(self.datasets.glob("*.jsonl"))
        manifest_path = next(self.datasets.glob("*.manifest.json"))
        rows = [json.loads(line) for line in data_path.read_text(encoding="utf-8").splitlines()]
        metadata = json.loads(manifest_path.read_text(encoding="utf-8"))
        mutate_capture(rows[0]["_capture"])
        mutate_metadata(metadata)
        new_bytes = canonical_dataset_bytes(rows)
        new_hash = sha(new_bytes)
        new_id = f"dataset-{new_hash[:16]}"
        metadata["dataset_manifest"]["dataset_id"] = new_id
        metadata["dataset_manifest"]["dataset_hash"] = new_hash
        new_data_path = self.datasets / f"{new_id}.jsonl"
        lock_path = Path(str(data_path) + ".lock")
        new_lock_path = Path(str(new_data_path) + ".lock")
        data_path.rename(new_data_path)
        lock_path.rename(new_lock_path)
        new_manifest_path = self.datasets / f"{new_id}-{_safe_source(metadata['hypothesis_id'])}.manifest.json"
        manifest_path.rename(new_manifest_path)
        Path(str(manifest_path) + ".lock").rename(Path(str(new_manifest_path) + ".lock"))
        new_data_path.write_bytes(new_bytes)
        new_manifest_path.write_text(json.dumps(metadata), encoding="utf-8")

    def test_manifest_filename_and_retention_fields_are_bound(self):
        manifest_path = next(self.datasets.glob("*.manifest.json"))
        wrong_manifest = manifest_path.with_name("wrong-dataset-hyp.manifest.json")
        Path(str(manifest_path) + ".lock").rename(Path(str(wrong_manifest) + ".lock"))
        manifest_path.rename(wrong_manifest)
        with self.assertRaisesRegex(RawResponseRetentionHold, "manifest_path_binding_invalid"):
            self.assess()

    def test_manifest_metadata_cannot_diverge_from_dataset_capture(self):
        manifest_path = next(self.datasets.glob("*.manifest.json"))
        metadata = json.loads(manifest_path.read_text(encoding="utf-8"))
        metadata["retention_policy"] = "other-policy"
        manifest_path.write_text(json.dumps(metadata), encoding="utf-8")
        with self.assertRaisesRegex(RawResponseRetentionHold, "manifest_metadata_mismatch"):
            self.assess()

    def test_dataset_capture_expiry_and_parent_lineage_are_revalidated(self):
        def change_expiry(capture):
            capture["expires_at"] = "2099-01-01T00:00:00+00:00"
        def change_expiry_meta(metadata):
            metadata["expires_at"] = "2099-01-01T00:00:00+00:00"
        self._rewrite_dataset(change_expiry, change_expiry_meta)
        with self.assertRaisesRegex(RawResponseRetentionHold, "descendant_retention_or_lineage_mismatch"):
            self.assess()

    def test_dataset_parent_lineage_cannot_be_rebound(self):
        def change_parent(capture):
            capture["raw_response_id"] = "f" * 64
            capture["retention_lineage"][0]["response_id"] = "f" * 64
        def change_parent_meta(metadata):
            metadata["raw_response_id"] = "f" * 64
            metadata["retention_lineage"][0]["response_id"] = "f" * 64
        self._rewrite_dataset(change_parent, change_parent_meta)
        with self.assertRaisesRegex(RawResponseRetentionHold, "descendant_parent_identity_mismatch"):
            self.assess()

    def test_raw_dataset_and_gate_reparse_paths_hold_before_following(self):
        def link_and_check(link, target, *, directory, expected):
            backup = link.with_name(link.name + ".review-backup")
            link.rename(backup)
            try:
                link.symlink_to(target, target_is_directory=directory)
            except OSError as exc:
                backup.rename(link)
                self.skipTest(f"symlink creation unavailable: {exc}")
            try:
                with self.assertRaisesRegex(RawResponseRetentionHold, expected):
                    self.assess()
            finally:
                link.unlink()
                backup.rename(link)

        outside_raw = self.state.parent / "outside-raw"
        outside_raw.mkdir()
        link_and_check(self.raw_root, outside_raw, directory=True,
                       expected="bound_path_reparse_point")
        outside_datasets = self.state.parent / "outside-datasets"
        outside_datasets.mkdir()
        link_and_check(self.datasets, outside_datasets, directory=True,
                       expected="bound_path_reparse_point")
        gate = self.state / ".nami-raw-dataset-write-gate.lock"
        outside_gate = self.state.parent / "outside-gate.lock"
        outside_gate.write_bytes(b"" if nami_io._HAS_FCNTL else b"0")
        link_and_check(gate, outside_gate, directory=False,
                       expected="bound_path_reparse_point")

    def test_missing_descendant_and_extra_unrecognized_dataset_hold(self):
        (self.state / "datasets").rename(self.state / "datasets.saved")
        with self.assertRaisesRegex(RawResponseRetentionHold, "inventory_held"):
            self.assess()
        (self.state / "datasets.saved").rename(self.state / "datasets")
        extra = self.datasets / "unrecognized.jsonl"
        extra.write_text(json.dumps({"unlinked": True}) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(RawResponseRetentionHold, "lineage_missing"):
            self.assess()

    def test_root_and_lease_mismatch_hold(self):
        root_mismatch_lease = Lease(self.state)
        with self.assertRaisesRegex(RawResponseRetentionHold, "root_mismatch"):
            assess_raw_responses(self.state, expected_root=self.raw_root,
                release_sha256=self.release, source_identity_to_connector=self.source_map,
                isolation_guard=root_mismatch_lease, transaction_id=self.txid,
                quiescence_attestation=self.token(root_mismatch_lease), now=self.now)
        root_mismatch_lease.close()
        bad = Lease(self.raw_root)
        with self.assertRaisesRegex(RawResponseRetentionHold, "lease_root_mismatch"):
            assess_raw_responses(self.state, expected_root=self.state,
                release_sha256=self.release, source_identity_to_connector=self.source_map,
                isolation_guard=bad, transaction_id=self.txid,
                quiescence_attestation=self.token(bad), now=self.now)
        bad.close()

    def test_absent_verifier_fabricated_or_wrong_transaction_attestation_hold(self):
        class FabricatedLease:
            root = self.state
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def assert_bound_root(self, root):
                if Path(root).resolve() != Path(self.root).resolve(): raise RuntimeError()
        forged = FabricatedLease()
        with self.assertRaisesRegex(RawResponseRetentionHold, "quiescence_verifier_missing"):
            assess_raw_responses(self.state, expected_root=self.state,
                release_sha256=self.release, source_identity_to_connector=self.source_map,
                isolation_guard=forged, transaction_id=self.txid,
                quiescence_attestation="fabricated", now=self.now)
        lease = Lease(self.state)
        with self.assertRaisesRegex(RawResponseRetentionHold, "quiescence_attestation_invalid"):
            assess_raw_responses(self.state, expected_root=self.state,
                release_sha256=self.release, source_identity_to_connector=self.source_map,
                isolation_guard=lease, transaction_id=self.txid,
                quiescence_attestation="fabricated", now=self.now)
        lease.close()
        wrong_tx = "e" * 32
        wrong_lease = Lease(self.state, wrong_tx)
        other_lease = Lease(self.state)
        with self.assertRaisesRegex(RawResponseRetentionHold, "quiescence_attestation_invalid"):
            assess_raw_responses(self.state, expected_root=self.state,
                release_sha256=self.release, source_identity_to_connector=self.source_map,
                isolation_guard=wrong_lease, transaction_id=wrong_tx,
                quiescence_attestation=self.token(other_lease), now=self.now)
        wrong_lease.close()
        other_lease.close()

    def test_persist_frozen_events_creates_and_catalogs_manifest_lock(self):
        # setUp used the real writer; both data and manifest lock artifacts are
        # persistent outputs of its file_lock calls.
        data_lock = next(self.datasets.glob("*.jsonl.lock"))
        manifest_lock = next(self.datasets.glob("*.manifest.json.lock"))
        manifest = Path(str(manifest_lock)[:-5])
        expected = b"" if nami_io._HAS_FCNTL else b"0"
        self.assertTrue(data_lock.is_file())
        self.assertTrue(manifest.is_file())
        self.assertEqual(data_lock.read_bytes(), expected)
        self.assertEqual(manifest_lock.read_bytes(), expected)
        result = self.assess()
        catalog_paths = {row["path"] for row in result["descendant_catalog"]["artifacts"]}
        self.assertIn(manifest_lock.relative_to(self.datasets).as_posix(), catalog_paths)

    def test_lock_sidecar_accepts_only_platform_nami_io_shape(self):
        lock = next(self.raw_root.rglob("*.jsonl.lock"))
        expected = b"" if nami_io._HAS_FCNTL else b"0"
        self.assertEqual(lock.read_bytes(), expected)
        lock.write_bytes(b"fabricated")
        with self.assertRaisesRegex(RawResponseRetentionHold, "lock_sidecar_invalid"):
            self.assess()

    def test_manifest_lock_sidecar_rejects_invalid_platform_bytes(self):
        lock = next(self.datasets.glob("*.manifest.json.lock"))
        lock.write_bytes(b"fabricated")
        with self.assertRaisesRegex(RawResponseRetentionHold, "lock_sidecar_invalid"):
            self.assess()

    def test_raw_tree_rejects_manifest_and_manifest_lock_junk(self):
        manifest = self.raw_root / "untrusted.manifest.json"
        manifest.write_text("{}", encoding="utf-8")
        lock = Path(str(manifest) + ".lock")
        lock.write_bytes(b"" if nami_io._HAS_FCNTL else b"0")
        with self.assertRaisesRegex(RawResponseRetentionHold, "unknown_artifact"):
            self.assess()

    def test_retained_directory_handle_pins_namespace_and_rejects_ads(self):
        from engine.lib.secure_state_io import SecureStateRoot, _CloseHandle, SecureStateIOError
        secure = SecureStateRoot(self.state)
        handle, chain = secure.open_handle("raw", directory=True)
        renamed = self.state / "raw-renamed"
        try:
            with self.assertRaises(PermissionError):
                self.raw_root.rename(renamed)
            self.assertEqual(secure.list_names("raw"), ["market-fixture"])
            for invalid in ("raw/file.jsonl:stream", "C:/outside.jsonl", "raw/../outside.jsonl"):
                with self.assertRaises(SecureStateIOError):
                    secure.open_handle(invalid)
        finally:
            for opened in reversed(chain):
                _CloseHandle(opened)
            secure.close()
        self.raw_root.rename(renamed)
        renamed.rename(self.raw_root)

    def _inject_entry_swap(self, replacement):
        from engine.lib import secure_state_io
        source = self.raw_root / "market-fixture"
        outside = self.state.parent / "injected-outside"
        outside.mkdir(exist_ok=True)
        saved = self.raw_root / "market-fixture.saved-race"
        original_list = secure_state_io.SecureStateRoot.list_entries
        state = {"done": False}

        def injected(secure, relative="", *, expected_identity=None):
            entries = original_list(secure, relative, expected_identity=expected_identity)
            if not state["done"] and relative == "raw":
                state["done"] = True
                source.rename(saved)
                if replacement == "reparse":
                    source.symlink_to(outside, target_is_directory=True)
                else:
                    source.mkdir()
            return entries

        return mock.patch.object(secure_state_io.SecureStateRoot, "list_entries", injected), source, saved

    def test_injected_reparse_insertion_after_enumeration_holds(self):
        patcher, source, saved = self._inject_entry_swap("reparse")
        try:
            with patcher:
                with self.assertRaisesRegex(RawResponseRetentionHold, "bound_path_reparse_point"):
                    self.assess()
        finally:
            if source.is_symlink():
                source.unlink()
            if saved.exists():
                saved.rename(source)

    def test_injected_name_swap_after_enumeration_checks_file_id(self):
        patcher, source, saved = self._inject_entry_swap("ordinary")
        try:
            with patcher:
                with self.assertRaisesRegex(RawResponseRetentionHold, "enumerated_entry_identity_mismatch"):
                    self.assess()
        finally:
            if source.exists() and source.is_dir():
                source.rmdir()
            if saved.exists():
                saved.rename(source)

    def test_gate_and_root_file_handles_pin_exact_objects(self):
        lease = Lease(self.state)
        gate = self.state / ".nami-raw-dataset-write-gate.lock"
        gate_backup = gate.with_name(gate.name + ".race-backup")
        root_renamed = self.state.with_name(self.state.name + "-race-backup")
        with lease:
            secure = lease._secure_root
            gate_handle, gate_chain = secure.open_handle(
                ".nami-raw-dataset-write-gate.lock", writable=True,
                expected_identity=lease._gate_identity)
            try:
                self.assertEqual(secure.handle_identity(gate_handle)["file_id"],
                                 lease._gate_identity["file_id"])
                with self.assertRaises(PermissionError):
                    gate.rename(gate_backup)
                with self.assertRaises(PermissionError):
                    self.state.rename(root_renamed)
            finally:
                from engine.lib.secure_state_io import _CloseHandle
                for handle in reversed(gate_chain):
                    _CloseHandle(handle)
        self.state.rename(root_renamed)
        root_renamed.rename(self.state)

    def test_retained_file_handle_matches_enumerated_identity_and_pins_name(self):
        from engine.lib.secure_state_io import SecureStateRoot, _CloseHandle
        secure = SecureStateRoot(self.state)
        raw_entry = next(row for row in secure.list_entries("raw")
                         if row[0] == "market-fixture")
        dataset_entry = next(row for row in secure.list_entries(
            "raw/market-fixture", expected_identity=raw_entry[1])
            if row[0].endswith(".jsonl"))
        relative = "raw/market-fixture/" + dataset_entry[0]
        handle, chain = secure.open_handle(relative, expected_identity=dataset_entry[1])
        actual = secure.handle_identity(handle)
        self.assertEqual(actual["device"], dataset_entry[1]["device"])
        self.assertEqual(actual["file_id"], dataset_entry[1]["file_id"])
        original = self.state / Path(*relative.split("/"))
        backup = original.with_suffix(original.suffix + ".race-backup")
        try:
            with self.assertRaises(PermissionError):
                original.rename(backup)
        finally:
            for opened in reversed(chain):
                _CloseHandle(opened)
            secure.close()
        original.rename(backup)
        backup.rename(original)

    def test_outside_hardlink_alias_holds_file_containment(self):
        from engine.lib.secure_state_io import SecureStateRoot, SecureStateIOError
        source = next(self.raw_root.rglob("*.jsonl"))
        alias = self.state.parent / "outside-hardlink.jsonl"
        try:
            alias.hardlink_to(source)
        except OSError as exc:
            self.skipTest(f"hardlink creation unavailable: {exc}")
        try:
            relative = source.relative_to(self.state).as_posix()
            with SecureStateRoot(self.state) as secure:
                with self.assertRaisesRegex(SecureStateIOError, "link_count_invalid"):
                    secure.read_bytes(relative)
        finally:
            alias.unlink(missing_ok=True)

    def test_post_read_dataset_mutation_holds_before_receipt(self):
        from engine.lib import raw_response_retention as retention
        path = next(self.datasets.glob("*.jsonl"))
        original_bytes = path.read_bytes()
        original_catalog = retention._inventory_descendants

        def mutate_after_read(*args, **kwargs):
            catalog = original_catalog(*args, **kwargs)
            path.write_bytes(original_bytes + b" ")
            return catalog

        try:
            with mock.patch.object(retention, "_inventory_descendants", mutate_after_read):
                with self.assertRaisesRegex(RawResponseRetentionHold, "final_inventory_changed"):
                    self.assess()
        finally:
            path.write_bytes(original_bytes)

    def test_exact_root_lease_blocks_the_actual_raw_writer_path(self):
        lease = Lease(self.state)
        completed = threading.Event()
        errors = []

        def writer():
            try:
                persist_raw_response({"market": "writer-block-test", "data_source": self.identity,
                                      "events": []}, state_dir=self.state,
                                      captured_at=self.now.replace(microsecond=0).isoformat())
            except Exception as exc:  # surfaced on the test thread below
                errors.append(exc)
            finally:
                completed.set()

        with lease:
            lease.assert_bound_root(self.state)
            self.assertTrue(lease.verify_quiescence_attestation(
                lease.attestation, self.state, self.txid))
            worker = threading.Thread(target=writer)
            worker.start()
            time.sleep(0.1)
            self.assertFalse(completed.is_set(), "writer must wait behind the exact-root lease")
        worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        self.assertTrue(completed.is_set())
        self.assertFalse(errors, errors)

    def test_stage_is_read_only_and_verification_detects_stale_catalog(self):
        before = tree_hashes(self.state)
        lease = Lease(self.state)
        assessment = stage_raw_response_assessment(self.state,
            staging_dir=self.state / "stage-absent", expected_root=self.state,
            release_sha256=self.release, source_identity_to_connector=self.source_map,
            isolation_guard=lease, transaction_id=self.txid,
            quiescence_attestation=self.token(lease), now=self.now)
        self.assertFalse((self.state / "stage-absent").exists())
        self.assertEqual(before, tree_hashes(self.state))
        self.assertFalse(assessment["retention_ready"])
        from engine.lib.source_clearance import source_selection_snapshot
        self.assertEqual(assessment["source_selection_fingerprint"],
                         source_selection_snapshot(self.state)["source_selection_fingerprint"])
        self.assertEqual(assessment["source_clearance_fingerprint"], assessment["active_run_clearance_pin"])
        self.add_response("second-market-fixture", captured_at=self.now.replace(microsecond=0).isoformat())
        fresh_lease = Lease(self.state)
        with self.assertRaisesRegex(RawResponseRetentionHold, "inventory_stale"):
            verify_raw_response_assessment(self.state, assessment=assessment,
                expected_root=self.state, release_sha256=self.release,
                source_identity_to_connector=self.source_map,
                isolation_guard=fresh_lease, transaction_id=self.txid,
                quiescence_attestation=self.token(fresh_lease), now=self.now)

    def test_no_release_or_global_readiness_claim(self):
        result = self.assess()
        self.assertIs(result["retention_ready"], False)
        self.assertNotIn("purge_complete", result)


if __name__ == "__main__":
    unittest.main()
