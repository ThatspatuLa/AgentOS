import json
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
import kcos_stage3_driver_v2 as driver


class DeadRuntimeRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state_path = self.root / "kcos_cycle_state_v2.json"
        self.ledger_path = self.root / "ea_ledger.json"
        self.handoff_path = self.root / "handoff.json"
        self.runtime_path = self.root / "runtime.json"
        self.event_path = self.root / "events" / "cycle.stage3.M15.complete.json"
        self.ini = self.root / "M15.ini"
        self.ini.write_text("authenticated ini", encoding="utf-8")
        opt = bytearray(436 + 232)
        opt[428:432] = (436).to_bytes(4, "little")
        opt[432:436] = (232).to_bytes(4, "little")
        self.opt = self.root / "M15.opt"
        self.opt.write_bytes(opt)
        old = time.time() - 1000
        import os
        os.utime(self.opt, (old, old))
        ini_hash = driver.sha256(self.ini)
        self.handoff = {"cycle_id": "cycle", "ea": "ETHUSD-EA",
                        "timeframe_order": ["M5", "M15", "H1"],
                        "ini_files": {"M15": str(self.ini)},
                        "ini_sha256": {"M15": ini_hash},
                        "runtime_manifest": str(self.runtime_path)}
        self.runtime = {"schema_version": 2, "cycle_id": "cycle", "stage": 3,
                        "ea": "ETHUSD-EA", "timeframe": "M15", "status": "OPTIMIZING",
                        "launched_epoch": old, "startup_grace_seconds": 300,
                        "last_opt_growth_epoch": old + 10,
                        "normal_settle_seconds": 20, "terminal_pid": 100,
                        "watcher_pid": 101, "worker_pids": list(range(200, 224)),
                        "worker_process_evidence": [{"pid": p} for p in range(200, 224)],
                        "worker_baseline_pids": [], "agent_offsets_before": {},
                        "opt_candidates_before": {str(self.opt.resolve()): {"size": 0, "mtime": old - 10}},
                        "ini": str(self.ini), "ini_sha256": ini_hash,
                        "opt_path": str(self.opt), "opt_structurally_complete": True,
                        "command": [str(driver.launcher.STAGE3_PORTABLE_TERMINAL), "/portable"],
                        "timeframe_event": str(self.event_path), "fresh_log_completion": False,
                        "completion_mode": None, "stable_seconds": 600,
                        "partial_completion_ratio": .9}
        self.state = {"schema_version": 2, "cycle_id": "cycle", "current_stage": "stage3_running",
                      "stage_status": "stage3_m15_running", "current_ea": "ETHUSD-EA",
                      "hold_after_stage": 5}
        self._write(self.state_path, self.state)
        self._write(self.handoff_path, self.handoff)
        self._write(self.runtime_path, self.runtime)
        self._write(self.ledger_path, {"records": []})
        driver.EA_LEDGER = self.ledger_path

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def _write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def _classify(self):
        with mock.patch.object(driver.psutil, "pid_exists", return_value=False), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[]), \
             mock.patch.object(driver.launcher, "opt_candidates", return_value=[self.opt]):
            return driver.classify_dead_optimizing_runtime(
                self.state_path, self.handoff_path, self.runtime, [])

    def test_dead_runtime_is_infrastructure_hold_with_hash_bound_partial_evidence(self):
        self.assertTrue(self._classify())
        event = json.loads((self.root / "events" / "cycle.stage3.failed.json").read_text())
        state = json.loads(self.state_path.read_text())
        self.assertEqual("STAGE3_DEAD_RUNTIME_NO_COMPLETION", event["failure_kind"])
        self.assertFalse(event["rotation_eligible"])
        self.assertEqual(driver.sha256(self.opt), event["opt_sha256"])
        self.assertEqual(driver.sha256(self.runtime_path), event["runtime_manifest_sha256"])
        self.assertEqual("stage3_infrastructure_failure", state["stage_status"])
        self.assertFalse(self._classify())

    def test_live_registered_owner_prevents_classification(self):
        with mock.patch.object(driver.psutil, "pid_exists", side_effect=lambda pid: pid == 222), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[]), \
             mock.patch.object(driver.launcher, "opt_candidates", return_value=[self.opt]):
            self.assertFalse(driver.classify_dead_optimizing_runtime(
                self.state_path, self.handoff_path, self.runtime, []))

    def test_completion_event_or_sufficient_ui_partial_evidence_prevents_classification(self):
        self.event_path.parent.mkdir(parents=True, exist_ok=True)
        self.event_path.write_text("{}", encoding="utf-8")
        self.assertFalse(self._classify())
        self.event_path.unlink()
        runtime = dict(self.runtime)
        runtime["ui_pass_counter"] = {"ok": True, "current": 9000, "total": 10000,
                                      "button_state": "START"}
        runtime["stable_seconds"] = 60
        runtime["fresh_log_evidence"] = {"byte_count": 64, "clean": True}
        self._write(self.runtime_path, runtime)
        with mock.patch.object(driver.psutil, "pid_exists", return_value=False), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[]), \
             mock.patch.object(driver.launcher, "opt_candidates", return_value=[self.opt]):
            self.assertFalse(driver.classify_dead_optimizing_runtime(
                self.state_path, self.handoff_path, runtime, []))

    def test_unregistered_fresh_worker_prevents_classification(self):
        with mock.patch.object(driver.psutil, "pid_exists", return_value=False), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[999]):
            self.assertFalse(driver.classify_dead_optimizing_runtime(
                self.state_path, self.handoff_path, self.runtime, []))

    def test_legacy_empty_opt_snapshot_accepts_exact_in_run_growth_observation(self):
        self.runtime["opt_candidates_before"] = {}
        self.runtime["opt_growth_observation"] = {
            str(self.opt.resolve()): self.opt.stat().st_size - 16}
        self._write(self.runtime_path, self.runtime)
        self.assertTrue(self._classify())

    def test_legacy_empty_opt_snapshot_rejects_missing_or_wrong_path_observation(self):
        self.runtime["opt_candidates_before"] = {}
        self.runtime["opt_growth_observation"] = {}
        self._write(self.runtime_path, self.runtime)
        self.assertFalse(self._classify())
        self.runtime["opt_growth_observation"] = {
            str(self.root / "other.opt"): self.opt.stat().st_size - 16}
        self._write(self.runtime_path, self.runtime)
        self.assertFalse(self._classify())

    def test_legacy_empty_opt_snapshot_rejects_non_growth_observation(self):
        self.runtime["opt_candidates_before"] = {}
        self.runtime["opt_growth_observation"] = {
            str(self.opt.resolve()): self.opt.stat().st_size}
        self._write(self.runtime_path, self.runtime)
        self.assertFalse(self._classify())

    def test_legacy_growth_with_post_launch_file_touch_but_prelaunch_growth_epoch_is_rejected(self):
        import os
        self.runtime["opt_candidates_before"] = {}
        self.runtime["opt_growth_observation"] = {
            str(self.opt.resolve()): self.opt.stat().st_size - 16}
        self.runtime["last_opt_growth_epoch"] = self.runtime["launched_epoch"] - 1
        post_launch_touch = time.time() - 350
        os.utime(self.opt, (post_launch_touch, post_launch_touch))
        self._write(self.runtime_path, self.runtime)
        self.assertFalse(self._classify())

    def test_exact_prior_infrastructure_receipt_is_preserved_before_replacement(self):
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        prior = {"schema_version": 2, "event_id": "cycle:stage3:failed",
                 "cycle_id": "cycle", "stage": 3, "status": "failed",
                 "failure_kind": "STAGE3_EXECUTION_FAILURE", "ea": "ETHUSD-EA",
                 "error": "Stage 3 parser mismatch", "rotation_eligible": False,
                 "consumed": False}
        failure_path.parent.mkdir(parents=True, exist_ok=True)
        failure_path.write_text(json.dumps(prior), encoding="utf-8")
        exact_bytes = failure_path.read_bytes()
        exact_hash = driver.sha256(failure_path)
        self.assertTrue(self._classify())
        new_event = json.loads(failure_path.read_text(encoding="utf-8"))
        archived = Path(new_event["prior_failure_receipt"]["path"])
        self.assertEqual(exact_hash, new_event["prior_failure_receipt"]["sha256"])
        self.assertEqual(exact_hash, driver.sha256(archived))
        self.assertEqual(exact_bytes, archived.read_bytes())

    def test_custom_event_rejects_transaction_link_to_external_parent(self):
        self.assertTrue(self._classify())
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        event = json.loads(failure_path.read_text(encoding="utf-8"))
        source = Path(event["classification_transaction"]["path"])
        external = self.root / "external" / source.name
        external.parent.mkdir(parents=True)
        external.write_bytes(source.read_bytes())
        event["classification_transaction"]["path"] = str(external)
        self._write(failure_path, event)
        self.assertFalse(self._classify())

    def test_custom_event_rejects_prepared_transaction_symlink(self):
        self.assertTrue(self._classify())
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        event = json.loads(failure_path.read_text(encoding="utf-8"))
        archive = Path(event["classification_transaction"]["path"])
        target = archive.with_name(archive.name + ".real")
        archive.rename(target)
        try:
            archive.symlink_to(target)
        except OSError as exc:
            target.rename(archive)
            self.skipTest(f"symlink creation unavailable: {exc}")
        self.assertFalse(self._classify())

    def test_archive_install_rejects_existing_symlink_without_hashing_target(self):
        import hashlib
        directory = self.root / "events"
        directory.mkdir()
        payload = b"immutable archive bytes"
        digest = hashlib.sha256(payload).hexdigest().upper()
        target = self.root / "outside.bin"
        target.write_bytes(payload)
        destination = directory / f"receipt.prior.{digest}.json"
        try:
            destination.symlink_to(target)
        except OSError as exc:
            self.skipTest(f"symlink creation unavailable: {exc}")
        with mock.patch.object(driver, "sha256", side_effect=AssertionError("must reject before hashing")):
            self.assertFalse(driver._atomic_preserve_receipt(destination, payload, digest))

    def _set_generic_failure_receipt(self):
        driver.record_failure(
            self.state_path, "dead_optimizing_runtime_without_completion_evidence")

    def _symlink_archive_for_scan(self, archive):
        target = archive.with_name(archive.name + ".real")
        archive.rename(target)
        try:
            archive.symlink_to(target)
        except OSError as exc:
            target.rename(archive)
            self.skipTest(f"symlink creation unavailable: {exc}")
        return target

    def _assert_symlink_scan_rejected_without_open(self, link, target):
        original_open = Path.open

        def reject_link_open(path, *args, **kwargs):
            if path in {link, target}:
                raise AssertionError("archive scan opened a symlink or its target")
            return original_open(path, *args, **kwargs)

        with mock.patch.object(Path, "open", reject_link_open):
            self.assertFalse(self._classify())

    def test_generic_receipt_prior_archive_scan_skips_symlink(self):
        self._write_prior_receipt()
        self.assertTrue(self._classify())
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        event = json.loads(failure_path.read_text(encoding="utf-8"))
        prepared = Path(event["classification_transaction"]["path"])
        prepared.unlink()
        prior = Path(event["prior_failure_receipt"]["path"])
        target = self._symlink_archive_for_scan(prior)
        self._set_generic_failure_receipt()
        self._assert_symlink_scan_rejected_without_open(prior, target)

    def test_generic_receipt_prepared_archive_scan_skips_symlink(self):
        self.assertTrue(self._classify())
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        event = json.loads(failure_path.read_text(encoding="utf-8"))
        prepared = Path(event["classification_transaction"]["path"])
        target = self._symlink_archive_for_scan(prepared)
        self._set_generic_failure_receipt()
        self._assert_symlink_scan_rejected_without_open(prepared, target)

    def test_custom_event_rejects_hash_valid_but_tampered_transaction_payload(self):
        import hashlib
        self.assertTrue(self._classify())
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        event = json.loads(failure_path.read_text(encoding="utf-8"))
        old_path = Path(event["classification_transaction"]["path"])
        payload = json.loads(old_path.read_text(encoding="utf-8"))
        payload["ea"] = "TAMPERED-EA"
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        digest = hashlib.sha256(body).hexdigest().upper()
        new_path = old_path.with_name(f"{failure_path.name}.prepared.{digest}.json")
        old_path.unlink()
        new_path.write_bytes(body)
        event["classification_transaction"] = {"path": str(new_path), "sha256": digest}
        self._write(failure_path, event)
        self.assertFalse(self._classify())

    def test_prior_receipt_link_rejects_external_parent(self):
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        self._write_prior_receipt()
        self.assertTrue(self._classify())
        event = json.loads(failure_path.read_text(encoding="utf-8"))
        source = Path(event["prior_failure_receipt"]["path"])
        external = self.root / "external" / source.name
        external.parent.mkdir(parents=True)
        external.write_bytes(source.read_bytes())
        event["prior_failure_receipt"]["path"] = str(external)
        self._write(failure_path, event)
        read_bytes = Path.read_bytes

        def reject_external_read(path):
            if path == external:
                raise AssertionError("external prior receipt was opened")
            return read_bytes(path)

        with mock.patch.object(Path, "read_bytes", reject_external_read):
            self.assertFalse(self._classify())

    def test_prior_receipt_link_rejects_same_directory_symlink(self):
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        self._write_prior_receipt()
        self.assertTrue(self._classify())
        event = json.loads(failure_path.read_text(encoding="utf-8"))
        archive = Path(event["prior_failure_receipt"]["path"])
        target = archive.with_name(archive.name + ".real")
        archive.rename(target)
        try:
            archive.symlink_to(target)
        except OSError as exc:
            target.rename(archive)
            self.skipTest(f"symlink creation unavailable: {exc}")
        read_bytes = Path.read_bytes

        def reject_symlink_read(path):
            if path == archive:
                raise AssertionError("symlinked prior receipt was opened")
            return read_bytes(path)

        with mock.patch.object(Path, "read_bytes", reject_symlink_read):
            self.assertFalse(self._classify())

    def test_unrelated_preexisting_failure_receipt_is_rejected_unchanged(self):
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        prior = {"schema_version": 2, "event_id": "other:stage3:failed",
                 "cycle_id": "other", "stage": 3, "status": "failed",
                 "failure_kind": "STAGE3_EXECUTION_FAILURE", "ea": "ETHUSD-EA",
                 "rotation_eligible": False, "consumed": False}
        failure_path.parent.mkdir(parents=True, exist_ok=True)
        failure_path.write_text(json.dumps(prior), encoding="utf-8")
        exact_bytes = failure_path.read_bytes()
        self.assertFalse(self._classify())
        self.assertEqual(exact_bytes, failure_path.read_bytes())
        self.assertEqual([], list(failure_path.parent.glob("*.prior.*.json")))

    def test_prior_receipt_requires_schema_and_canonical_event_id(self):
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        base = {"schema_version": 2, "event_id": "cycle:stage3:failed",
                "cycle_id": "cycle", "stage": 3, "status": "failed",
                "failure_kind": "STAGE3_EXECUTION_FAILURE", "ea": "ETHUSD-EA",
                "rotation_eligible": False, "consumed": False}
        failure_path.parent.mkdir(parents=True, exist_ok=True)
        for key, value in (("schema_version", 1), ("event_id", "cycle:stage3:other")):
            invalid = dict(base, **{key: value})
            self._write(failure_path, invalid)
            exact_bytes = failure_path.read_bytes()
            self.assertFalse(self._classify())
            self.assertEqual(exact_bytes, failure_path.read_bytes())
        self.assertEqual([], list(failure_path.parent.glob("*.prior.*.json")))

    def _write_prior_receipt(self):
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        prior = {"schema_version": 2, "event_id": "cycle:stage3:failed",
                 "cycle_id": "cycle", "stage": 3, "status": "failed",
                 "failure_kind": "STAGE3_EXECUTION_FAILURE", "ea": "ETHUSD-EA",
                 "error": "Stage 3 parser mismatch", "rotation_eligible": False,
                 "consumed": False}
        failure_path.parent.mkdir(parents=True, exist_ok=True)
        failure_path.write_text(json.dumps(prior), encoding="utf-8")
        return failure_path

    def test_interrupted_archive_install_leaves_original_and_can_resume(self):
        failure_path = self._write_prior_receipt()
        original = failure_path.read_bytes()
        with mock.patch.object(driver.os, "link", side_effect=OSError("injected archive interruption")):
            self.assertFalse(self._classify())
        self.assertEqual(original, failure_path.read_bytes())
        self.assertEqual([], list(failure_path.parent.glob("*.prior.*.json")))
        self.assertTrue(self._classify())
        event = json.loads(failure_path.read_text(encoding="utf-8"))
        self.assertEqual(driver.sha256(Path(event["prior_failure_receipt"]["path"])),
                         event["prior_failure_receipt"]["sha256"])

    def test_interrupted_event_replacement_resumes_from_original_plus_archive(self):
        failure_path = self._write_prior_receipt()
        original = failure_path.read_bytes()
        atomic = driver.atomic_json

        def fail_custom_event(path, value):
            if Path(path) == failure_path and value.get("failure_kind") == "STAGE3_DEAD_RUNTIME_NO_COMPLETION":
                raise OSError("injected event replacement interruption")
            return atomic(path, value)

        with mock.patch.object(driver, "atomic_json", side_effect=fail_custom_event):
            with self.assertRaises(OSError):
                self._classify()
        self.assertEqual(original, failure_path.read_bytes())
        self.assertEqual(1, len(list(failure_path.parent.glob("*.prior.*.json"))))
        self.assertTrue(self._classify())

    def test_interrupted_record_failure_resumes_from_replacement_event(self):
        failure_path = self._write_prior_receipt()
        with mock.patch.object(driver, "record_failure", side_effect=RuntimeError("injected hold interruption")):
            with self.assertRaises(RuntimeError):
                self._classify()
        current = json.loads(failure_path.read_text(encoding="utf-8"))
        self.assertIn("prior_failure_receipt", current)
        self.assertTrue(self._classify())
        state = json.loads(self.state_path.read_text())
        self.assertEqual("stage3_infrastructure_failure", state["stage_status"])

    def test_absent_receipt_record_failure_interruption_resumes_from_hash_bound_event(self):
        failure_path = self.root / "events" / "cycle.stage3.failed.json"
        self.assertFalse(failure_path.exists())
        record_failure = driver.record_failure

        def write_then_interrupt(state_path, detail):
            record_failure(state_path, detail)
            raise RuntimeError("injected interruption after generic event and hold write")

        with mock.patch.object(driver, "record_failure", side_effect=write_then_interrupt):
            with self.assertRaises(RuntimeError):
                self._classify()
        event = json.loads(failure_path.read_text(encoding="utf-8"))
        self.assertEqual("STAGE3_EXECUTION_FAILURE", event["failure_kind"])
        self.assertEqual("stage3_infrastructure_failure",
                         json.loads(self.state_path.read_text())["stage_status"])
        transactions = list(failure_path.parent.glob(f"{failure_path.name}.prepared.*.json"))
        self.assertEqual(1, len(transactions))
        self.assertTrue(self._classify())
        state = json.loads(self.state_path.read_text(encoding="utf-8"))
        restored = json.loads(failure_path.read_text(encoding="utf-8"))
        self.assertEqual("stage3_infrastructure_failure", state["stage_status"])
        self.assertEqual("STAGE3_DEAD_RUNTIME_NO_COMPLETION", restored["failure_kind"])
        self.assertEqual(driver.sha256(self.runtime_path), restored["runtime_manifest_sha256"])
        self.assertEqual(driver.sha256(self.opt), restored["opt_sha256"])
        self.assertEqual(driver.sha256(transactions[0]),
                         restored["classification_transaction"]["sha256"])

    def test_interrupted_final_restore_resumes_from_generic_hold_event(self):
        failure_path = self._write_prior_receipt()
        atomic = driver.atomic_json
        custom_writes = 0

        def fail_second_custom_write(path, value):
            nonlocal custom_writes
            if Path(path) == failure_path and value.get("failure_kind") == "STAGE3_DEAD_RUNTIME_NO_COMPLETION":
                custom_writes += 1
                if custom_writes == 2:
                    raise OSError("injected final restore interruption")
            return atomic(path, value)

        with mock.patch.object(driver, "atomic_json", side_effect=fail_second_custom_write):
            with self.assertRaises(OSError):
                self._classify()
        current = json.loads(failure_path.read_text(encoding="utf-8"))
        self.assertEqual("STAGE3_EXECUTION_FAILURE", current["failure_kind"])
        self.assertEqual("dead_optimizing_runtime_without_completion_evidence", current["error"])
        state = json.loads(self.state_path.read_text())
        self.assertEqual("stage3_infrastructure_failure", state["stage_status"])
        self.assertTrue(self._classify())
        restored = json.loads(failure_path.read_text(encoding="utf-8"))
        self.assertIn("prior_failure_receipt", restored)
        state = json.loads(self.state_path.read_text())
        self.assertEqual("ready", state["stage3_auto_timeframe_retry_history"][-1]["status"])

    def test_retry_gate_requires_bound_failure_and_allows_only_first_attempt(self):
        self.assertTrue(self._classify())
        state = json.loads(self.state_path.read_text())
        with mock.patch.object(driver.psutil, "pid_exists", return_value=False), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[]), \
             mock.patch.object(driver.launcher, "opt_candidates", return_value=[self.opt]):
            self.assertTrue(driver.stale_runtime_restart_is_ready(
                state, self.handoff, self.runtime, []))
            state["stage3_auto_timeframe_retry_history"] = [{
                "cycle_id": "cycle", "timeframe": "M15", "attempt": 1, "status": "started"}]
            self.assertFalse(driver.stale_runtime_restart_is_ready(
                state, self.handoff, self.runtime, []))

    def test_retry_gate_rechecks_completion_event_and_runtime_evidence(self):
        self.assertTrue(self._classify())
        state = json.loads(self.state_path.read_text())
        with mock.patch.object(driver.psutil, "pid_exists", return_value=False), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[]), \
             mock.patch.object(driver.launcher, "opt_candidates", return_value=[self.opt]):
            self.event_path.parent.mkdir(parents=True, exist_ok=True)
            self.event_path.write_text("{}", encoding="utf-8")
            self.assertFalse(driver.stale_runtime_restart_is_ready(
                state, self.handoff, self.runtime, []))
            self.event_path.unlink()
            runtime = dict(self.runtime, fresh_log_completion=True)
            self.assertFalse(driver.stale_runtime_restart_is_ready(
                state, self.handoff, runtime, []))

    def test_retry_gate_requires_exact_failure_event_digest(self):
        self.assertTrue(self._classify())
        state = json.loads(self.state_path.read_text())
        failure_path = Path(state["stage3_failure_event"])
        original = failure_path.read_text(encoding="utf-8")
        failure = json.loads(original)
        failure["error"] += " changed"
        failure_path.write_text(json.dumps(failure), encoding="utf-8")
        with mock.patch.object(driver.psutil, "pid_exists", return_value=False), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[]), \
             mock.patch.object(driver.launcher, "opt_candidates", return_value=[self.opt]):
            self.assertFalse(driver.stale_runtime_restart_is_ready(
                state, self.handoff, self.runtime, []))

    def test_retry_gate_rejects_matching_live_watcher_even_with_dead_recorded_pid(self):
        self.assertTrue(self._classify())
        state = json.loads(self.state_path.read_text())
        watcher = SimpleNamespace(info={
            "pid": 8123, "name": "python.exe",
            "cmdline": ["python.exe", "kcos_stage3_watch_v2.py", "--runtime",
                        str(self.runtime_path.resolve())]})
        with mock.patch.object(driver.psutil, "pid_exists", return_value=False), \
             mock.patch.object(driver.psutil, "process_iter", return_value=[watcher]), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[]), \
             mock.patch.object(driver.launcher, "opt_candidates", return_value=[self.opt]):
            self.assertFalse(driver.stale_runtime_restart_is_ready(
                state, self.handoff, self.runtime, []))

    def test_retry_manifest_must_be_fresh_and_identity_bound(self):
        self.assertTrue(self._classify())
        with mock.patch.object(driver.psutil, "pid_exists", return_value=False), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[]), \
             mock.patch.object(driver.launcher, "opt_candidates", return_value=[self.opt]), \
             mock.patch.object(driver.launcher, "process_ids", return_value=[]):
            result = driver.restart_stale_timeframe(
                self.state_path, self.handoff_path,
                launch_runner=lambda *_args, **_kwargs: {"status": "OPTIMIZING"})
        self.assertEqual("STAGE3_AUTORETRY_HELD", result["status"])
        state = json.loads(self.state_path.read_text())
        self.assertEqual("failed", state["stage3_auto_timeframe_retry_history"][-1]["status"])
        self.assertEqual("stage3_infrastructure_failure", state["stage_status"])

    def test_valid_retry_is_bound_and_second_dead_attempt_only_holds(self):
        self.assertTrue(self._classify())

        def write_fresh_retry(_handoff_path, timeframe):
            runtime = json.loads(self.runtime_path.read_text())
            runtime.update({"status": "OPTIMIZING", "timeframe": timeframe,
                            "launched_epoch": time.time() - 600})
            import os
            os.utime(self.opt, (time.time() - 350, time.time() - 350))
            self._write(self.runtime_path, runtime)
            return {"status": "OPTIMIZING"}

        with mock.patch.object(driver.psutil, "pid_exists", return_value=False), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[]), \
             mock.patch.object(driver.launcher, "opt_candidates", return_value=[self.opt]), \
             mock.patch.object(driver.launcher, "process_ids", return_value=[]):
            result = driver.restart_stale_timeframe(
                self.state_path, self.handoff_path, launch_runner=write_fresh_retry)
        self.assertEqual("STAGE3_TIMEFRAME_RETRY_STARTED", result["status"])
        retried = json.loads(self.runtime_path.read_text())
        self.assertEqual(1, retried["stale_retry_binding"]["attempt"])
        state = json.loads(self.state_path.read_text())
        state["stage_status"] = "stage3_m15_running"
        self._write(self.state_path, state)
        with mock.patch.object(driver.psutil, "pid_exists", return_value=False), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[]), \
             mock.patch.object(driver.launcher, "opt_candidates", return_value=[self.opt]):
            failure_path = Path(state["stage3_failure_event"])
            failure_original = failure_path.read_bytes()
            tampered_failure = json.loads(failure_original)
            tampered_failure["error"] = "changed after retry launch"
            failure_path.write_text(json.dumps(tampered_failure), encoding="utf-8")
            self.assertFalse(driver.classify_dead_optimizing_runtime(
                self.state_path, self.handoff_path, retried, []))
            failure_path.write_bytes(failure_original)
            runtime_original = self.runtime_path.read_bytes()
            current_state = json.loads(self.state_path.read_text())
            attempt = current_state["stage3_auto_timeframe_retry_history"][-1]
            archived_runtime = json.loads(
                (Path(attempt["incomplete_evidence_archive"]) / self.runtime_path.name).read_text())
            archived_runtime["stale_retry_binding"] = retried["stale_retry_binding"]
            self._write(self.runtime_path, archived_runtime)
            self.assertFalse(driver.classify_dead_optimizing_runtime(
                self.state_path, self.handoff_path, archived_runtime, []))
            self.runtime_path.write_bytes(runtime_original)
            tampered_runtime = json.loads(runtime_original)
            tampered_runtime["stale_retry_binding"]["failure_event_sha256"] = "0" * 64
            self._write(self.runtime_path, tampered_runtime)
            current_state = json.loads(self.state_path.read_text())
            attempt = current_state["stage3_auto_timeframe_retry_history"][-1]
            attempt["new_runtime_sha256"] = driver.sha256(self.runtime_path)
            self._write(self.state_path, current_state)
            self.assertFalse(driver.classify_dead_optimizing_runtime(
                self.state_path, self.handoff_path, tampered_runtime, []))
            self.runtime_path.write_bytes(runtime_original)
            current_state = json.loads(self.state_path.read_text())
            current_state["stage3_auto_timeframe_retry_history"][-1]["new_runtime_sha256"] = driver.sha256(self.runtime_path)
            self._write(self.state_path, current_state)
            retried = json.loads(runtime_original)
            retried.update({"completed_passes": 8088, "last_opt_size": self.opt.stat().st_size,
                            "stable_seconds": 900, "observed_utc": "2026-10-12T00:00:00Z"})
            self._write(self.runtime_path, retried)
            self.assertEqual(driver.sha256(failure_path), current_state["stage3_auto_timeframe_retry_history"][-1]["failure_event_sha256"])
            self.assertEqual(driver.sha256(failure_path), retried["stale_retry_binding"]["failure_event_sha256"])
            self.assertTrue(driver.classify_dead_optimizing_runtime(
                self.state_path, self.handoff_path, retried, []))
        held = json.loads(self.state_path.read_text())
        failure = json.loads((self.root / "events" / "cycle.stage3.failed.json").read_text())
        self.assertEqual("stage3_infrastructure_failure", held["stage_status"])
        self.assertEqual("dead_optimizing_runtime_after_one_retry", held["stage3_failure_detail"])
        self.assertEqual(driver.sha256(self.opt), failure["retry_exhausted"]["opt_sha256"])
        with mock.patch.object(driver.psutil, "pid_exists", return_value=False), \
             mock.patch.object(driver.launcher, "fresh_local_workers", return_value=[]), \
             mock.patch.object(driver.launcher, "opt_candidates", return_value=[self.opt]):
            self.assertFalse(driver.stale_runtime_restart_is_ready(
                held, self.handoff, retried, []))


if __name__ == "__main__":
    unittest.main()
