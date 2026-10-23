import contextlib
import hashlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import kcos_stage3_driver_v2 as driver


class Stage3HoldGateTests(unittest.TestCase):
    def test_consumed_release_remains_valid_for_same_cycle_stage3_recovery(self):
        state = {"cycle_id": "cycle", "current_stage": "stage3_running",
                 "stage_status": "stage3_infrastructure_failure", "hold_after_stage": 2,
                 "next_action": "inspect_stage3_infrastructure_failure"}
        with mock.patch.object(driver, "authenticated_consumed_stage2_release", return_value={"authorization_id": "cycle:release"}):
            self.assertIsNone(driver.stage3_admission_hold(state))
        self.assertIsNotNone(driver.stage3_admission_hold(state))

    def test_stage3_retry_marker_binds_failure_repair_and_runtime_hashes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            failure = root / "failure.json"
            receipt = root / "repair.json"
            runtime = root / "runtime.json"
            failure.write_text(json.dumps({"cycle_id": "cycle", "stage": 3,
                                           "status": "failed", "rotation_eligible": False}), encoding="utf-8")
            receipt.write_text(json.dumps({"cycle_id": "cycle", "stage": 3,
                                           "repair_complete": True}), encoding="utf-8")
            runtime.write_text(json.dumps({"cycle_id": "cycle", "status": "INFRASTRUCTURE_FAILURE",
                                           "infrastructure_only": True}), encoding="utf-8")
            state = {"cycle_id": "cycle", "run_authorization": {"cycle_id": "cycle"},
                     "stage2_release_attempt": {"cycle_id": "cycle", "status": "released"},
                     "stage3_repair_retry": {
                         "cycle_id": "cycle", "status": "ready",
                         "failure_event": str(failure), "failure_event_sha256": driver.sha256(failure),
                         "repair_receipt": str(receipt), "repair_receipt_sha256": driver.sha256(receipt),
                         "runtime_manifest": str(runtime), "runtime_manifest_sha256": driver.sha256(runtime),
                     }}
            with mock.patch.object(driver, "authenticated_consumed_stage2_release", return_value={"ok": True}):
                self.assertTrue(driver.stage3_repair_retry_is_ready(state))
                runtime.write_text(json.dumps({"cycle_id": "cycle", "status": "START_REQUIRED"}), encoding="utf-8")
                self.assertFalse(driver.stage3_repair_retry_is_ready(state))

    def test_stage3_cron_is_noop_while_stage2_hold_is_active(self):
        state = {
            "schema_version": 2,
            "cycle_id": "held-cycle",
            "current_ea": "held-ea",
            "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held",
            "hold_after_stage": 2,
            "next_action": "audit_stage2_before_stage3",
        }
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "kcos_cycle_state_v2.json"
            original = json.dumps(state, indent=2) + "\n"
            path.write_text(original, encoding="utf-8")
            output = io.StringIO()
            with mock.patch.object(sys, "argv", ["kcos_stage3_driver_v2.py", "--state", str(path)]):
                with contextlib.redirect_stdout(output):
                    result = driver.main()
            report = json.loads(output.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(report.get("status"), "HELD_AFTER_STAGE2")
            self.assertEqual(report.get("cycle_id"), "held-cycle")
            self.assertEqual(path.read_text(encoding="utf-8"), original)

    def test_direct_start_stage3_also_refuses_a_hold(self):
        state = {
            "schema_version": 2,
            "cycle_id": "held-cycle",
            "current_ea": "held-ea",
            "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held",
            "hold_after_stage": 2,
            "next_action": "audit_stage2_before_stage3",
        }
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "kcos_cycle_state_v2.json"
            path.write_text(json.dumps(state), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "hold_after_stage=2"):
                driver.start_stage3(path, Path(temp) / "stage2.json", Path(temp) / "stage3.json",
                                    Path(temp) / "lease.json", Path(temp) / "out")

    def test_held_state_admission_requires_cycle_bound_one_run_release_attempt(self):
        cycle = driver.STAGE2_RELEASE_CYCLE_ID
        state = {
            "schema_version": 2,
            "cycle_id": cycle,
            "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held",
            "hold_after_stage": 2,
            "next_action": "audit_stage2_before_stage3",
        }
        auth = {
            "schema_version": 1,
            "authorization_id": f"{cycle}:stage3:one-run:testhash",
            "cycle_id": cycle,
            "target_stage": 3,
            "scope": "current_cycle_stage3_once",
            "authorized_by": "user_confirmed",
            "status": "attempt_in_progress",
        }
        state["stage2_release_attempt"] = auth
        self.assertIsNone(driver.stage3_admission_hold(state, auth))
        wrong_cycle = dict(auth, cycle_id="other-cycle")
        self.assertIsNotNone(driver.stage3_admission_hold(state, wrong_cycle))
        self.assertIsNotNone(driver.stage3_admission_hold(state))

    def test_release_dispatch_is_bound_to_the_authorized_held_cycle(self):
        state = {
            "cycle_id": driver.STAGE2_RELEASE_CYCLE_ID,
            "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held",
            "hold_after_stage": 2,
            "next_action": "audit_stage2_before_stage3",
        }
        self.assertTrue(driver.should_attempt_stage2_release(state))
        self.assertFalse(driver.should_attempt_stage2_release(dict(state, cycle_id="other")))
        self.assertFalse(driver.should_attempt_stage2_release(dict(state, hold_after_stage=3)))
        self.assertFalse(driver.should_attempt_stage2_release(dict(state, stage2_release_attempt={})))
        self.assertFalse(driver.should_attempt_stage2_release(dict(state, stage2_release_attempt={"status": "failed"})))

    def test_current_full_cycle_authorization_releases_exact_stage2_hold_once(self):
        cycle = "kcos-current-user-authorized-cycle"
        state = {
            "cycle_id": cycle,
            "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held",
            "hold_after_stage": 2,
            "next_action": "audit_stage2_before_stage3",
            "run_authorization": {
                "schema_version": 1,
                "scope": "current_cycle_stages_1_to_5",
                "cycle_id": cycle,
                "authorized_by": "user_confirmed",
                "allowed_stages": [1, 2, 3, 4, 5],
                "hold_after_stage": 5,
            },
        }
        self.assertTrue(driver.authorizes_full_cycle_stage3(state))
        self.assertTrue(driver.should_attempt_stage2_release(state))
        auth = {"cycle_id": cycle, "target_stage": 3, "scope": "current_cycle_stage3_once",
                "authorized_by": "user_confirmed", "authorization_id": f"{cycle}:stage3:one-run:test"}
        state["stage2_release_attempt"] = dict(auth, status="attempt_in_progress")
        self.assertIsNone(driver.stage3_admission_hold(state, auth))
        malformed = dict(state, run_authorization=dict(state["run_authorization"], cycle_id="other"))
        self.assertFalse(driver.authorizes_full_cycle_stage3(malformed))
        self.assertIsNotNone(driver.stage3_admission_hold(malformed, auth))

    def test_full_cycle_cron_audits_stage2_hold_before_admitting_stage3(self):
        cycle = "kcos-current-user-authorized-cycle"
        state = {
            "cycle_id": cycle,
            "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held",
            "hold_after_stage": 2,
            "next_action": "audit_stage2_before_stage3",
            "run_authorization": {
                "schema_version": 1,
                "scope": "current_cycle_stages_1_to_5",
                "cycle_id": cycle,
                "authorized_by": "user_confirmed",
                "allowed_stages": [1, 2, 3, 4, 5],
                "hold_after_stage": 5,
            },
        }
        with mock.patch.object(driver, "authenticate_stage2_execution",
                               return_value="DPAPI-authenticated execution attestation verified") as audit:
            self.assertIsNone(driver.stage3_admission_hold(state))
            audit.assert_called_once_with(state)
        with mock.patch.object(driver, "authenticate_stage2_execution",
                               side_effect=ValueError("bad Stage 2 provenance")):
            self.assertIn("Stage 3 held pending explicit Stage 2 audit/release",
                          driver.stage3_admission_hold(state))

    def test_failed_stage3_preflight_can_retry_boundedly_before_any_handoff(self):
        cycle = "kcos-preflight-retry-cycle"
        state = {
            "cycle_id": cycle, "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held", "hold_after_stage": 2,
            "next_action": "audit_stage2_before_stage3",
            "run_authorization": {
                "schema_version": 1, "scope": "current_cycle_stages_1_to_5",
                "cycle_id": cycle, "authorized_by": "user_confirmed",
                "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
            },
            "stage2_release_attempt": {"cycle_id": cycle, "status": "failed", "failure_phase": "preflight",
                                        "preflight_utc": "2026-10-09T08:00:00Z"},
        }
        self.assertTrue(driver.should_attempt_stage2_release(state))
        state["stage2_release_attempt_history"] = [{"cycle_id": cycle}]
        self.assertTrue(driver.should_attempt_stage2_release(state))
        state["stage2_release_attempt_history"].append({"cycle_id": cycle})
        self.assertTrue(driver.should_attempt_stage2_release(state))
        state["stage2_release_attempt_history"].append({"cycle_id": cycle})
        self.assertFalse(driver.should_attempt_stage2_release(state))

    def test_new_authenticated_stage2_publication_gets_one_fresh_bounded_release(self):
        cycle = "kcos-current-user-authorized-cycle"
        with tempfile.TemporaryDirectory() as temp:
            event = Path(temp) / "stage2.complete.json"
            event.write_text(json.dumps({"cycle_id": cycle, "stage": 2, "status": "complete",
                                         "mq5_sha256": "NEW-SOURCE"}), encoding="utf-8")
            state = {
                "cycle_id": cycle,
                "current_stage": "stage3_pending",
                "stage_status": "stage2_complete_held",
                "hold_after_stage": 2,
                "next_action": "audit_stage2_before_stage3",
                "last_stage2_event": str(event),
                "run_authorization": {
                    "schema_version": 1, "scope": "current_cycle_stages_1_to_5",
                    "cycle_id": cycle, "authorized_by": "user_confirmed",
                    "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
                },
                "stage2_release_attempt": {
                    "cycle_id": cycle, "status": "released",
                    "evidence": {"stage2_event_sha256": "OLD-STAGE2-PUBLICATION"},
                },
                "stage2_release_attempt_history": [
                    {"cycle_id": cycle, "status": "released",
                     "evidence": {"stage2_event_sha256": "OLDER-PUBLICATION"}},
                ],
            }
            self.assertEqual(0, driver.stage2_release_attempts_for_current_publication(state))
            self.assertTrue(driver.should_attempt_stage2_release(state))
            current_event = json.loads(event.read_text(encoding="utf-8"))
            current_hash = hashlib.sha256(json.dumps(
                current_event, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")).hexdigest().upper()
            state["stage2_release_attempt"] = {
                "cycle_id": cycle, "status": "released",
                "evidence": {"stage2_event_sha256": current_hash},
            }
            self.assertEqual(1, driver.stage2_release_attempts_for_current_publication(state))
            self.assertFalse(driver.should_attempt_stage2_release(state))

    def test_exact_unstarted_m5_runtime_requires_portable_exe_config_and_worker_identity(self):
        cycle = driver.STAGE2_RELEASE_CYCLE_ID
        ini = r"C:\\isolated\\cycle.M5.ini"
        terminal = Path(driver.launcher.DEFAULT_TERMINAL)
        worker = terminal.with_name("metatester64.exe")
        address = "127.0.0.1:3001"
        state = {"cycle_id": cycle, "current_ea": "DCC"}
        prior = {"ini_files": {"M5": ini}}
        runtime = {
            "status": "START_REQUIRED", "cycle_id": cycle, "ea": "DCC", "timeframe": "M5",
            "start_button_attempted": True, "start_button_invoked": False, "watcher_pid": None,
            "command": [str(terminal), "/portable", f"/config:{ini}"], "worker_pids": [21],
            "worker_process_evidence": [{"pid": 21, "address": address, "exe": str(worker),
                                          "command": [str(worker), "/portable", f"/address:{address}"]}],
        }
        self.assertTrue(driver.exact_unstarted_m5_runtime(runtime, state, prior))
        bad = dict(runtime, start_button_invoked=True)
        self.assertFalse(driver.exact_unstarted_m5_runtime(bad, state, prior))
        bad_worker = json.loads(json.dumps(runtime))
        bad_worker["worker_process_evidence"][0]["exe"] = r"C:\\Program Files\\EightCap\\metatester64.exe"
        self.assertFalse(driver.exact_unstarted_m5_runtime(bad_worker, state, prior))

    def test_release_attempt_prevents_an_automatic_second_attempt(self):
        state = {
            "schema_version": 2,
            "cycle_id": "held-cycle",
            "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held",
            "hold_after_stage": 2,
            "next_action": "audit_stage2_before_stage3",
            "stage2_release_attempt": {
                "authorization_id": "held-cycle:stage3:one-run",
                "cycle_id": "held-cycle",
                "scope": "current_cycle_stage3_once",
                "authorized_by": "user_confirmed",
                "status": "failed",
            },
        }
        self.assertIsNotNone(driver.stage3_admission_hold(state))

    def test_explicit_hold_after_stage3_allows_stage3_admission(self):
        state = {
            "schema_version": 2,
            "cycle_id": "released-cycle",
            "current_stage": "stage3_pending",
            "stage_status": "stage3_pending",
            "hold_after_stage": 3,
            "next_action": "start_stage3",
        }
        self.assertIsNone(driver.stage3_admission_hold(state))

    def test_malformed_hold_fails_closed(self):
        state = {
            "schema_version": 2,
            "cycle_id": "malformed-hold-cycle",
            "current_ea": "held-ea",
            "current_stage": "stage3_pending",
            "stage_status": "stage3_pending",
            "hold_after_stage": "not-an-integer",
        }
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "kcos_cycle_state_v2.json"
            path.write_text(json.dumps(state), encoding="utf-8")
            output = io.StringIO()
            with mock.patch.object(sys, "argv", ["kcos_stage3_driver_v2.py", "--state", str(path)]):
                with contextlib.redirect_stdout(output):
                    result = driver.main()
            report = json.loads(output.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(report.get("status"), "HELD_AFTER_STAGE2")
            self.assertIn("invalid hold_after_stage", report.get("detail", ""))
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), state)


if __name__ == "__main__":
    unittest.main()
