#!/usr/bin/env python3
"""Focused contract tests for deterministic Stage 3 compute-limit rotation."""

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SD = Path(__file__).resolve().parent
sys.path.insert(0, str(SD))

import kcos_stage3_driver_v2 as driver
import kcos_stage3_evaluate_v2 as evaluator
import kcos_stage3_prepare_v2 as preparer
import kcos_stage3_publish_v2 as publisher
import kcos_stage3_verify_v2 as verifier
import kcos_stage3_watch_v2 as watcher
from kcos_stage3_common_v2 import sha256


class Stage3ComputeBudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.cycle = "cycle-compute-budget"
        self.spec = self.root / "spec.json"
        self.spec.write_text("{}", encoding="utf-8")
        self.stage2_event = self.root / "stage2.json"
        self.stage2_event.write_text(json.dumps({
            "cycle_id": self.cycle,
            "stage": 2,
            "stage1_spec_sha256": sha256(self.spec),
        }), encoding="utf-8")
        self.runtime = self.root / "runtime.json"
        self.tf_event = self.root / "timeframe.compute.json"
        self.decision = self.root / "decision.json"
        self.verification = self.root / "verification.json"
        self.stage3_event = self.root / "stage3.complete.json"
        self.lease = self.root / "stage3.lock.json"
        self.lease.write_text("{}", encoding="utf-8")
        self.queue = self.root / "stage4_queue.json"
        self.handoff = self.root / "handoff.json"
        self.handoff.write_text(json.dumps({
            "schema_version": 2,
            "cycle_id": self.cycle,
            "stage": 3,
            "ea": "ExpensiveEA",
            "from_date": "2025.01.10",
            "to_date": "2026.01.10",
            "timeframe_order": ["M5", "M15", "H1"],
            "stage1_spec": str(self.spec),
            "stage1_spec_sha256": sha256(self.spec),
            "stage2_event": str(self.stage2_event),
            "runtime_manifest": str(self.runtime),
            "decision": str(self.decision),
            "event": str(self.stage3_event),
            "failed_notes": str(self.root / "failed.md"),
            "source_notes": str(self.root / "source.md"),
            "passing_notes": str(self.root / "passing.md"),
            "lease": str(self.lease),
        }), encoding="utf-8")
        self.state = self.root / "state.json"
        self.state.write_text(json.dumps({
            "schema_version": 2,
            "cycle_id": self.cycle,
            "current_stage": "stage3_running",
            "stage_status": "stage3_m15_running",
            "current_ea": "ExpensiveEA",
            "queue_full": False,
        }), encoding="utf-8")

        # Prior M5 evidence must survive a compute stop during M15.
        csv_path = self.root / f"{self.cycle}.stage3.M5.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=[
                "Pass", "Profit", "CustomFitness", "Trades", "DrawdownPercent",
                "SharpeRatio", "ProfitFactor", "MarginLevel",
            ])
            writer.writeheader()
            writer.writerow({"Pass": 1, "Profit": 500, "CustomFitness": 20,
                             "Trades": 500, "DrawdownPercent": 10,
                             "SharpeRatio": 1.2, "ProfitFactor": 1.4,
                             "MarginLevel": 120.0})
        opt = self.root / "M5.opt"
        opt.write_bytes(b"prior-opt")
        self.m5_evidence = self.root / f"{self.cycle}.stage3.M5.parse.json"
        self.m5_evidence.write_text(json.dumps({
            "cycle_id": self.cycle,
            "timeframe": "M5",
            "passed": True,
            "from_date": "2025.01.10",
            "to_date": "2026.01.10",
            "csv": str(csv_path),
            "csv_sha256": sha256(csv_path),
            "opt": str(opt),
            "opt_sha256": sha256(opt),
        }), encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def write_compute_stop(self, timeframe="M15", failure_mode="compute_budget_exceeded"):
        event = {
            "schema_version": 2,
            "event_id": f"{self.cycle}:stage3:{timeframe}:compute_budget_exceeded",
            "cycle_id": self.cycle,
            "stage": 3,
            "status": "COMPUTE_BUDGET_EXCEEDED",
            "timeframe": timeframe,
            "failure_mode": failure_mode,
            "completion_reason": failure_mode,
            "metrics": {
                "elapsed_seconds": 901,
                "completed_passes": 12,
                "seconds_since_last_pass": 181,
                "compute_qualification": "slow_first_pass",
                "first_pass_seconds": 150.25,
                "first_pass_censored_seconds": None,
            },
            "evidence": {
                "active_worker_pids": [101, 102],
                "agent_logs": ["agent-3001.log"],
                "budgets": {"first_pass_seconds": 120, "first_pass_hard_limit_seconds": 200,
                            "timeframe_budget_seconds": 900},
            },
            "completed_utc": "2026-09-10T01:00:00Z",
            "consumed": False,
        }
        self.tf_event.write_text(json.dumps(event), encoding="utf-8")
        self.runtime.write_text(json.dumps({
            **event,
            "timeframe_event": str(self.tf_event),
            "terminal_pid": 100,
            "worker_pids": [101, 102],
            "watcher_pid": 103,
        }), encoding="utf-8")

    def qualification_runtime(self):
        launch = 1_700_000_000.0
        return launch, {
            "schema_version": 2, "cycle_id": self.cycle, "stage": 3, "ea": "ExpensiveEA",
            "timeframe": "M5", "status": "OPTIMIZING", "opt_path": str(self.root / "none.opt"),
            "tester_log": str(self.root / "none.log"), "worker_pids": [101],
            "agent_logs": [{"worker_pid": 101, "path": str(self.root / "agent.log"), "start_offset": 0}],
            "launched_utc": "2023-11-14T22:13:20Z", "first_pass_budget_seconds": 120,
            "first_pass_hard_limit_seconds": 200, "progress_stall_budget_seconds": 180,
            "timeframe_budget_seconds": 900,
        }

    def test_no_pass_after_120_keeps_observing_until_200_censor(self):
        launch, runtime = self.qualification_runtime()
        with mock.patch.object(watcher, "completed_agent_passes", return_value=(0, [])):
            waiting = watcher.evaluate_snapshot(runtime, launch + 121, [101], [101])
            censored = watcher.evaluate_snapshot(runtime, launch + 200, [101], [101])
        self.assertEqual("OPTIMIZING", waiting["status"])
        self.assertEqual("COMPUTE_BUDGET_EXCEEDED", censored["status"])
        self.assertEqual("censored_no_first_pass", censored["compute_qualification"])
        self.assertEqual(200, censored["first_pass_censored_seconds"])
        self.assertIsNone(censored["first_pass_seconds"])

    def test_first_pass_between_thresholds_records_exact_slow_time(self):
        launch, runtime = self.qualification_runtime()
        with mock.patch.object(watcher, "completed_agent_passes", return_value=(1, [])):
            slow = watcher.evaluate_snapshot(runtime, launch + 150.25, [101], [101])
        self.assertEqual("COMPUTE_BUDGET_EXCEEDED", slow["status"])
        self.assertEqual("slow_first_pass", slow["compute_qualification"])
        self.assertEqual(150.25, slow["first_pass_seconds"])
        self.assertIsNone(slow["first_pass_censored_seconds"])

    def test_first_pass_at_qualification_cutoff_remains_eligible(self):
        launch, runtime = self.qualification_runtime()
        with mock.patch.object(watcher, "completed_agent_passes", return_value=(1, [])):
            qualified = watcher.evaluate_snapshot(runtime, launch + 120, [101], [101])
        self.assertEqual("OPTIMIZING", qualified["status"])
        self.assertEqual("qualified", qualified["compute_qualification"])
        self.assertEqual(120, qualified["first_pass_seconds"])

    def test_prepare_exposes_approved_compute_budgets(self):
        state = self.root / "prepare-state.json"
        state.write_text(json.dumps({"cycle_id": self.cycle, "current_ea": "ExpensiveEA"}), encoding="utf-8")
        source, ex5 = self.root / "EA.mq5", self.root / "EA.ex5"
        source.write_text("source", encoding="utf-8")
        ex5.write_bytes(b"ex5")
        stage2 = self.root / "prepare-stage2.json"
        stage2.write_text(json.dumps({
            "stage1_spec": str(self.spec), "expected_event": str(self.stage2_event),
        }), encoding="utf-8")
        template = self.root / "template.ini"
        template.write_text(
            "[Common]\nEA_NAME=Old\n[Tester]\nExpert=Old.ex5\nSymbol=ETHUSD\nPeriod=M5\n"
            "Optimization=2\nFromDate=2025.01.05\nToDate=2026.01.05\nOptimizationCriterion=6\nVisual=0\n"
            "[TesterInputs]\nInpPeriod=20||10||1||30||Y\n",
            encoding="utf-8",
        )
        event = {"mq5_sha256": sha256(source), "ex5_sha256": sha256(ex5)}
        verification = {"source": str(source), "ex5": str(ex5)}
        with mock.patch.object(preparer, "validate_stage2", return_value=(event, verification)):
            handoff = preparer.prepare(
                state, stage2, self.root / "prepared.json", self.root / "prepared.lock",
                template, self.root / "inis", dry_run=True)
        self.assertEqual(600, handoff["first_pass_budget_seconds"])
        self.assertEqual(900, handoff["first_pass_hard_limit_seconds"])
        self.assertEqual(180, handoff["progress_stall_budget_seconds"])
        self.assertEqual(900, handoff["timeframe_budget_seconds"])

    def test_m5_compute_limit_is_immutable_and_launches_m15(self):
        self.write_compute_stop("M5")
        launched, stopped = [], []
        def fake_launch(_handoff, timeframe):
            launched.append(timeframe)
            return {"watcher_pid": 999}
        with mock.patch.object(driver.launcher, "cleanup_runtime_manifest",
                               side_effect=lambda runtime, _path: stopped.extend(
                                   [runtime.get("terminal_pid"), *runtime.get("worker_pids", []), runtime.get("watcher_pid")])):
            result = driver.process_completed_timeframe(self.state, self.handoff, fake_launch)
        self.assertEqual("NEXT_TIMEFRAME_STARTED", result["status"])
        self.assertEqual(["M15"], launched)
        self.assertEqual([100, 101, 102, 103], stopped)
        compute_path = driver.timeframe_compute_path(json.loads(self.handoff.read_text()), "M5")
        self.assertTrue(compute_path.is_file())
        immutable = json.loads(compute_path.read_text())
        self.assertEqual("COMPUTE_SKIPPED", immutable["outcome"])
        self.assertNotIn("consumed", immutable)
        self.assertEqual("stage3_m15_running", json.loads(self.state.read_text())["stage_status"])

        # The mutable supervisor event can change after dispatch without
        # invalidating the copied evidence.
        source = json.loads(self.tf_event.read_text())
        source["consumed"] = True
        self.tf_event.write_text(json.dumps(source), encoding="utf-8")
        self.assertEqual("COMPUTE_SKIPPED", json.loads(compute_path.read_text())["outcome"])

    def test_authenticated_eta_skip_is_generic_and_launches_next_timeframe(self):
        event = {
            "schema_version": 2, "event_id": f"{self.cycle}:stage3:M5:compute_budget_exceeded",
            "cycle_id": self.cycle, "ea": "ExpensiveEA", "stage": 3, "status": "COMPUTE_BUDGET_EXCEEDED",
            "timeframe": "M5", "failure_mode": "compute_budget_exceeded",
            "completion_reason": "eta_over_4h_after_4m", "duration_policy_action": "skip_eta_over_4h",
            "duration_projection": {"elapsed_seconds": 240, "estimated_total_seconds": 14401,
                "projected_seconds": 14401, "source": "authenticated_bound_tester_gui_bottom_right"},
            "duration_stop": {"stopped": True, "method": "tester_stop_button"},
            "metrics": {"elapsed_seconds": 240, "completed_passes": 1,
                "compute_qualification": "qualified", "first_pass_seconds": 20,
                "first_pass_censored_seconds": None},
            "evidence": {"active_worker_pids": [], "tester_ui_timing": {
                "source": "authenticated_bound_tester_gui_bottom_right", "elapsed_seconds": 240,
                "estimated_total_seconds": 14401}, "duration_stop": {"stopped": True}},
            "completed_utc": "2026-09-10T01:00:00Z", "consumed": False,
        }
        self.tf_event.write_text(json.dumps(event), encoding="utf-8")
        self.runtime.write_text(json.dumps({**event, "timeframe_event": str(self.tf_event),
            "terminal_pid": 100, "worker_pids": [101], "watcher_pid": 103}), encoding="utf-8")
        launched = []
        with mock.patch.object(driver.launcher, "cleanup_runtime_manifest"), \
             mock.patch.object(driver, "prior_outcome_evidence", return_value=[]):
            result = driver.process_completed_timeframe(
                self.state, self.handoff,
                lambda _handoff, tf: launched.append(tf) or {"watcher_pid": 999})
        self.assertEqual("NEXT_TIMEFRAME_STARTED", result["status"])
        self.assertEqual(["M15"], launched)
        compute = json.loads(driver.timeframe_compute_path(
            json.loads(self.handoff.read_text()), "M5").read_text())
        self.assertEqual("skip_eta_over_4h", compute["duration_policy_action"])
        self.assertEqual(14401, compute["duration_projection"]["estimated_total_seconds"])
        self.assertTrue(evaluator.validate_compute_metrics("compute_stalled", {}, {} ) is False)

    def test_m15_compute_limit_forces_h1_when_m5_is_normal(self):
        self.write_compute_stop("M15")
        launched = []
        with mock.patch.object(driver.launcher, "cleanup_runtime_manifest"), \
             mock.patch.object(driver, "prior_outcome_evidence", return_value=[self.m5_evidence]):
            result = driver.process_completed_timeframe(
                self.state, self.handoff,
                lambda _handoff, tf: launched.append(tf) or {"watcher_pid": 999})
        self.assertEqual("NEXT_TIMEFRAME_STARTED", result["status"])
        self.assertEqual(["H1"], launched)

    def test_h1_compute_limit_selects_prior_passing_tf_and_can_enqueue_stage4(self):
        self.write_compute_stop("H1")
        stopped = []
        fragment_dir = self.root / "fragments"

        real_publish = publisher.publish
        def isolated_publish(*args, **kwargs):
            kwargs.update({"stage4_queue": self.queue, "fragment_dir": fragment_dir})
            return real_publish(*args, **kwargs)

        m15 = self.root / f"{self.cycle}.stage3.M15.compute.json"
        m15.write_text(json.dumps({
            "schema_version": 2, "cycle_id": self.cycle, "stage": 3, "timeframe": "M15",
            "outcome": "COMPUTE_SKIPPED", "failure_mode": "compute_budget_exceeded",
            "metrics": {"elapsed_seconds": 200, "completed_passes": 0,
                        "compute_qualification": "censored_no_first_pass",
                        "first_pass_seconds": None, "first_pass_censored_seconds": 200}, "evidence": {},
        }), encoding="utf-8")
        with mock.patch.object(driver.launcher, "cleanup_runtime_manifest",
                               side_effect=lambda runtime, _path: stopped.extend(
                                   [runtime.get("terminal_pid"), *runtime.get("worker_pids", []), runtime.get("watcher_pid")])), \
             mock.patch.object(driver.publisher, "publish", side_effect=isolated_publish), \
             mock.patch.object(driver, "prior_outcome_evidence", return_value=[self.m5_evidence, m15]):
            result = driver.process_completed_timeframe(self.state, self.handoff)

        self.assertEqual("STAGE3_COMPLETE_HELD", result["status"])
        self.assertEqual([100, 101, 102, 103], stopped)
        self.assertFalse(self.lease.exists())
        updated = json.loads(self.state.read_text())
        self.assertEqual("stage4_pending", updated["current_stage"])
        decision = json.loads(self.decision.read_text())
        self.assertEqual("READY_FOR_STAGE4", decision["verdict"])
        self.assertEqual("M5", decision["selected_tf"])
        self.assertIn("M15", decision["compute_skips"])
        self.assertIn("H1", decision["compute_skips"])
        serialized = json.dumps(decision)
        self.assertNotIn(str(self.tf_event), serialized)
        self.assertNotIn(sha256(self.tf_event), serialized)

    def test_all_three_compute_limits_fail_rotate_and_notes_label_compute_skips(self):
        compute_paths = []
        for tf in ("M5", "M15", "H1"):
            path = self.root / f"{self.cycle}.stage3.{tf}.compute.json"
            path.write_text(json.dumps({
                "schema_version": 2, "cycle_id": self.cycle, "stage": 3, "timeframe": tf,
                "outcome": "COMPUTE_SKIPPED", "failure_mode": "compute_budget_exceeded",
                "metrics": {"elapsed_seconds": 200, "completed_passes": 0,
                            "compute_qualification": "censored_no_first_pass",
                            "first_pass_seconds": None, "first_pass_censored_seconds": 200}, "evidence": {},
            }), encoding="utf-8")
            compute_paths.append(path)
        decision = evaluator.evaluate(self.handoff, compute_paths, self.decision)
        self.assertEqual("FAIL_ROTATE", decision["verdict"])
        self.assertEqual(["M5", "M15", "H1"], list(decision["compute_skips"]))
        verification = verifier.verify(self.handoff, self.decision, self.verification)
        self.assertTrue(verification["passed"], verification["checks"])
        note = publisher.note_block(decision, "failed").decode()
        self.assertIn("M5: skipped for compute", note)
        self.assertNotIn("all tested timeframes failed", note.lower())

    def test_mismatched_compute_event_fails_closed_before_process_stop(self):
        self.write_compute_stop()
        event = json.loads(self.tf_event.read_text())
        event["cycle_id"] = "wrong-cycle"
        self.tf_event.write_text(json.dumps(event), encoding="utf-8")
        with mock.patch.object(driver.launcher, "cleanup_runtime_manifest") as stop:
            with self.assertRaisesRegex(ValueError, "compute-limit event identity mismatch"):
                driver.process_completed_timeframe(self.state, self.handoff)
        stop.assert_not_called()

    def test_duplicate_launch_is_already_running_and_does_not_record_failure(self):
        argv = ["driver", "--state", str(self.state), "--handoff", str(self.handoff), "--start"]
        with mock.patch.object(sys, "argv", argv), \
             mock.patch.object(driver, "start_stage3", side_effect=ValueError("another Stage 3 runtime is already optimizing")), \
             mock.patch.object(driver, "record_failure") as record:
            self.assertEqual(0, driver.main())
        record.assert_not_called()
        self.assertEqual("stage3_m15_running", json.loads(self.state.read_text())["stage_status"])


if __name__ == "__main__":
    unittest.main()
