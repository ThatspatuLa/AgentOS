"""Stage 3 cron progression and stale-cycle ownership tests."""
import json
import sys
from pathlib import Path
import tempfile
import unittest
from unittest import mock

ROOT = Path(r"C:\Users\Tyson\Projects\kiyosaki_stage2b")
sys.path.append(str(ROOT / "state"))

import kcos_stage3_cron_v2 as cron
import kcos_stage3_driver_v2 as driver


class Stage3CronTests(unittest.TestCase):
    def _race_repair_fixture(self, root: Path):
        cycle = "cycle-race"
        failure_path = root / "failure.json"
        runtime_path = root / "runtime.json"
        receipt_path = root / "receipt.json"
        driver_path = Path(driver.__file__).resolve()
        ids = ["stage3-execution", "supervisor-execution"]
        failure = {"cycle_id": cycle, "event_id": f"{cycle}:stage3:failed", "stage": 3,
                   "status": "failed", "failure_kind": "STAGE3_DEAD_RUNTIME_NO_COMPLETION",
                   "rotation_eligible": False}
        runtime = {"cycle_id": cycle, "status": "INFRASTRUCTURE_FAILURE",
                   "failure_kind": "EIGHTCAP_TERMINAL_NOT_FRESH", "infrastructure_only": True,
                   "timeframe": "M15", "terminal_pid": 0, "worker_pids": [],
                   "timeframe_event": str(root / "missing-completion.json")}
        failure_path.write_text(json.dumps(failure), encoding="utf-8")
        runtime_path.write_text(json.dumps(runtime), encoding="utf-8")
        receipt = {"cycle_id": cycle, "event_id": failure["event_id"], "stage": 3,
                   "repair_complete": True, "repair_kind": "supervisor_retry_race",
                   "failure_kind": failure["failure_kind"],
                   "runtime_failure_kind": runtime["failure_kind"],
                   "failure_event_sha256": driver.sha256(failure_path),
                   "runtime_manifest_sha256": driver.sha256(runtime_path),
                   "runtime_manifest": str(runtime_path.resolve()),
                   "repaired_driver_sha256": driver.sha256(driver_path),
                   "hermes_execution_ids": ids}
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        state = {"cycle_id": cycle, "current_stage": "stage3_running",
                 "stage_status": "stage3_infrastructure_failure",
                 "next_action": "inspect_stage3_infrastructure_failure",
                 "stage3_failure_event": str(failure_path.resolve()),
                 "run_authorization": {"cycle_id": cycle, "authorized_by": "user_confirmed",
                     "scope": "current_cycle_stages_1_to_5", "allowed_stages": [1,2,3,4,5],
                     "hold_after_stage": 5},
                 "stage3_repair_retry": {"status": "ready", "cycle_id": cycle,
                     "repair_kind": "supervisor_retry_race",
                     "failure_event": str(failure_path.resolve()),
                     "failure_event_sha256": driver.sha256(failure_path),
                     "runtime_manifest": str(runtime_path.resolve()),
                     "runtime_manifest_sha256": driver.sha256(runtime_path),
                     "repair_receipt": str(receipt_path.resolve()),
                     "repair_receipt_sha256": driver.sha256(receipt_path),
                     "repaired_driver_path": str(driver_path),
                     "repaired_driver_sha256": driver.sha256(driver_path),
                     "hermes_execution_ids": ids}}
        return state, receipt_path

    def test_consumed_prior_cycle_is_cleaned_before_new_pending_cycle_starts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime_path = root / "old.runtime.json"
            runtime_path.write_text(json.dumps({
                "cycle_id": "old", "ea": "EA-old", "status": "OPTIMIZING",
                "terminal_pid": 123, "worker_pids": [456],
            }), encoding="utf-8")
            events = root / "events"
            events.mkdir()
            (events / "old.stage3.failed.json").write_text(json.dumps({
                "cycle_id": "old", "status": "failed", "consumed": True,
                "consumed_action": "rotate_stage1",
            }), encoding="utf-8")
            state = {"cycle_id": "new", "current_stage": "stage3_pending"}
            handoff = {"cycle_id": "old", "ea": "EA-old", "runtime_manifest": str(runtime_path)}

            with mock.patch.object(cron, "STATE", root), \
                 mock.patch.object(driver.launcher, "cleanup_previous_runtime") as cleanup:
                result = cron.cleanup_consumed_stale_runtime(state, handoff, [123], driver)

            self.assertTrue(result)
            cleanup.assert_called_once()
            self.assertEqual("INFRASTRUCTURE_FAILURE", cleanup.call_args.args[0]["status"])
            updated = json.loads(runtime_path.read_text(encoding="utf-8"))
            self.assertEqual("STALE_CYCLE_CLEANED", updated["status"])

    def test_unconsumed_or_same_cycle_runtime_is_not_cleaned(self):
        self.assertFalse(cron.cleanup_consumed_stale_runtime(
            {"cycle_id": "old", "current_stage": "stage3_pending"},
            {"cycle_id": "old"}, [123], driver))
        self.assertEqual("ALREADY_RUNNING_EXTERNAL", cron.choose_action(
            {"current_stage": "stage3_pending"}, None, [123]))

    def test_infrastructure_failure_waits_for_supervisor_before_stage3_retries(self):
        state = {"current_stage": "stage3_running",
                 "stage_status": "stage3_infrastructure_failure",
                 "next_action": "inspect_stage3_infrastructure_failure"}
        runtime = {"status": "TIMEFRAME_COMPLETE", "timeframe": "M15"}
        gate = mock.Mock()
        gate.stage3_repair_retry_is_ready.return_value = False
        self.assertEqual("HOLD_STAGE3_INFRASTRUCTURE_FAILURE",
                         cron.choose_action(state, runtime, [], gate))
        gate.stage3_repair_retry_is_ready.assert_called_once_with(state)
        gate.stage3_repair_retry_is_ready.return_value = True
        self.assertEqual("START", cron.choose_action(state, runtime, [], gate))

    def test_repaired_infrastructure_retry_is_single_shot_and_hash_authenticated(self):
        state = {"current_stage": "stage3_running"}
        runtime = {"status": "INFRASTRUCTURE_FAILURE"}
        retry_gate = mock.Mock(return_value=True)
        self.assertEqual("START", cron.choose_action(state, runtime, [], retry_gate))
        retry_gate.stage3_repair_retry_is_ready.assert_called_once_with(state)
        retry_gate.stage3_repair_retry_is_ready.return_value = False
        self.assertEqual("RESUME", cron.choose_action(state, runtime, [], retry_gate))
        retry_gate.stage3_repair_retry_is_ready.return_value = True
        self.assertEqual("ALREADY_RUNNING_EXTERNAL",
                         cron.choose_action(state, runtime, [59376], retry_gate))

    def test_supervisor_race_retry_requires_hash_bound_repair_and_full_cycle_authorization(self):
        with tempfile.TemporaryDirectory() as directory:
            state, receipt_path = self._race_repair_fixture(Path(directory))
            with mock.patch.object(driver, "_matching_stage3_watcher", return_value=False), \
                 mock.patch.object(driver.launcher, "process_ids", return_value=[]), \
                 mock.patch.object(driver.psutil, "pid_exists", return_value=False):
                self.assertTrue(driver.stage3_repair_retry_is_ready(state))
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                receipt["repaired_driver_sha256"] = "0" * 64
                receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
                state["stage3_repair_retry"]["repair_receipt_sha256"] = driver.sha256(receipt_path)
                self.assertFalse(driver.stage3_repair_retry_is_ready(state))
                state["stage3_repair_retry"]["repair_receipt_sha256"] = "1" * 64
                self.assertFalse(driver.stage3_repair_retry_is_ready(state))

    def test_supervisor_race_retry_routes_only_its_infrastructure_runtime(self):
        state = {"cycle_id": "cycle-race", "stage3_repair_retry": {
            "repair_kind": "supervisor_retry_race"}}
        runtime = {"cycle_id": "cycle-race", "status": "INFRASTRUCTURE_FAILURE",
                   "failure_kind": "EIGHTCAP_TERMINAL_NOT_FRESH",
                   "infrastructure_only": True}
        self.assertTrue(driver.stage3_repair_retry_matches_runtime(state, runtime))
        self.assertFalse(driver.stage3_repair_retry_matches_runtime(
            state, dict(runtime, cycle_id="other")))
        self.assertFalse(driver.stage3_repair_retry_matches_runtime(
            state, dict(runtime, infrastructure_only=False)))
        self.assertFalse(driver.stage3_repair_retry_matches_runtime(
            state, dict(runtime, failure_kind="TESTER_JOURNAL_REJECTED")))

    def test_wide_duration_retry_uses_same_profile_when_new_budget_covers_projection(self):
        state = {"current_stage": "stage3_running"}
        runtime = {"status": "DURATION_REDUCTION_REQUIRED"}
        retry_gate = mock.Mock()
        retry_gate.stage3_duration_retry_is_ready.return_value = True
        self.assertEqual("START", cron.choose_action(state, runtime, [], retry_gate))
        retry_gate.stage3_duration_retry_is_ready.assert_called_once_with(state, runtime)
        self.assertEqual("ALREADY_RUNNING_EXTERNAL", cron.choose_action(
            state, runtime, [59376], retry_gate))

    def test_dead_start_required_terminal_is_never_silently_restarted(self):
        gate = mock.Mock()
        gate.stage3_repair_retry_is_ready.return_value = False
        state = {"current_stage": "stage3_running"}
        runtime = {"status": "START_REQUIRED", "terminal_pid": 59376}
        self.assertEqual("RESUME", cron.choose_action(state, runtime, [], gate))
        self.assertEqual("RESUME", cron.choose_action(state, runtime, [59376], gate))

    def test_prework_terminal_loss_uses_one_authenticated_same_timeframe_retry(self):
        gate = mock.Mock()
        gate.stage3_repair_retry_is_ready.return_value = False
        gate.stale_runtime_restart_is_ready.return_value = False
        gate.unstarted_terminal_retry_is_ready.return_value = True
        state = {"current_stage": "stage3_running"}
        runtime = {"status": "INFRASTRUCTURE_FAILURE", "failure_kind": "EIGHTCAP_TERMINAL_NOT_FRESH"}
        handoff = {"cycle_id": "cycle"}
        self.assertEqual("RETRY_UNSTARTED_TERMINAL",
                         cron.choose_action(state, runtime, [], gate, handoff))
        gate.unstarted_terminal_retry_is_ready.assert_called_once_with(state, handoff, runtime, [])


if __name__ == "__main__":
    unittest.main()
