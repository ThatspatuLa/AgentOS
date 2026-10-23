import base64
import hashlib
import io
import json
import sqlite3
import subprocess
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import kcos_supervisor_v2 as supervisor


def accepted_dispatch(command, **_kwargs):
    return subprocess.CompletedProcess(command, 0, stdout="trigger accepted", stderr="")


def accepted_driver(command, **_kwargs):
    return subprocess.CompletedProcess(command, 0, stdout='{"status":"NEXT_TIMEFRAME_STARTED","zero_token":true}', stderr="")


class SupervisorTests(unittest.TestCase):
    def _stage5_evidence(self, *, event_verdict="STAGE5_FORWARD_FAIL", decision_verdict="STAGE5_FORWARD_FAIL",
                         result_verdict=None, include_hashes=True):
        cycle, ea = "cycle-stage5-auth", "EA_Stage5"
        result = self.root / "stage5.result.json"
        result_verdict = result_verdict or {
            "STAGE5_FORWARD_FAIL": "HOLD_FOR_REVIEW", "STAGE5_FORWARD_PASS": "record_pass",
            "HOLD_FOR_REVIEW": "HOLD_FOR_REVIEW",
        }.get(event_verdict)
        result.write_text(json.dumps({"schema_version": 3, "cycle_id": cycle, "ea": ea,
                                     "stage": "5", "verdict": result_verdict}), encoding="utf-8")
        decision = self.root / "stage5.decision.json"
        decision_payload = {"schema_version": 1, "cycle_id": cycle, "stage": "5", "ea": ea,
                            "verdict": decision_verdict, "result": str(result),
                            "result_sha256": supervisor.sha256(result)}
        decision.write_text(json.dumps(decision_payload), encoding="utf-8")
        verification = self.root / "stage5.verification.json"
        verification.write_text(json.dumps({"schema_version": 1, "cycle_id": cycle, "stage": "5",
                                            "passed": True, "result_sha256": supervisor.sha256(result)}),
                                encoding="utf-8")
        event_payload = {"schema_version": 1, "cycle_id": cycle, "stage": "5", "ea": ea,
                         "verdict": event_verdict, "verification_passed": True}
        if include_hashes:
            event_payload.update({"decision_sha256": supervisor.sha256(decision),
                                  "verification_sha256": supervisor.sha256(verification)})
        event = self.root / "stage5.complete.json"
        event.write_text(json.dumps(event_payload), encoding="utf-8")
        state = {"cycle_id": cycle, "current_ea": ea, "last_stage5_decision": str(decision),
                 "last_stage5_verification": str(verification), "last_stage5_event": str(event)}
        return state

    def test_stage5_transition_requires_both_event_evidence_hashes(self):
        state = self._stage5_evidence(include_hashes=False)
        valid, detail, _ = supervisor._validate_verified_transition(
            state, 5, "last_stage5_decision", "last_stage5_verification", "last_stage5_event",
            {"HOLD_FOR_REVIEW", "STAGE5_FORWARD_FAIL", "STAGE5_FORWARD_PASS"})
        self.assertFalse(valid)
        self.assertIn("binding is invalid", detail)

    def test_stage5_transition_rejects_contradictory_event_and_decision_verdicts(self):
        state = self._stage5_evidence(event_verdict="STAGE5_FORWARD_PASS",
                                      decision_verdict="STAGE5_FORWARD_FAIL")
        valid, detail, _ = supervisor._validate_verified_transition(
            state, 5, "last_stage5_decision", "last_stage5_verification", "last_stage5_event",
            {"HOLD_FOR_REVIEW", "STAGE5_FORWARD_FAIL", "STAGE5_FORWARD_PASS"})
        self.assertFalse(valid)
        self.assertIn("binding is invalid", detail)

    def test_stage5_transition_rejects_result_verdict_conflict(self):
        state = self._stage5_evidence(event_verdict="STAGE5_FORWARD_FAIL",
                                      decision_verdict="STAGE5_FORWARD_FAIL",
                                      result_verdict="record_pass")
        valid, detail, _ = supervisor._validate_verified_transition(
            state, 5, "last_stage5_decision", "last_stage5_verification", "last_stage5_event",
            {"HOLD_FOR_REVIEW", "STAGE5_FORWARD_FAIL", "STAGE5_FORWARD_PASS"})
        self.assertFalse(valid)
        self.assertIn("result", detail)

    def test_stage5_transition_accepts_hash_bound_matching_failure(self):
        state = self._stage5_evidence()
        valid, detail, event = supervisor._validate_verified_transition(
            state, 5, "last_stage5_decision", "last_stage5_verification", "last_stage5_event",
            {"HOLD_FOR_REVIEW", "STAGE5_FORWARD_FAIL", "STAGE5_FORWARD_PASS"})
        self.assertTrue(valid, detail)
        self.assertEqual("STAGE5_FORWARD_FAIL", event["verdict"])

    def test_authenticated_compile_failure_authorizes_one_same_candidate_repair(self):
        state_dir = self.root / "state"
        for folder in ("events", "candidates", "specs", "attempts", "leases"):
            (state_dir / folder).mkdir(parents=True, exist_ok=True)
        cycle = "cycle-compile"
        source = state_dir / "candidates" / "EA.mq5"
        source.write_text("g_trade.SetDeviationsInPoints(10);\n"
                          "g_trade.SetTypeFillingByVolume(_Symbol, SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN));\n",
                          encoding="utf-8")
        spec = state_dir / "specs" / "stage1.json"
        spec.write_text('{"tester_config":{}}\n', encoding="utf-8")
        log = state_dir / "attempts" / "compile.log"
        log.write_text("undeclared identifier 'SetDeviationsInPoints'\n"
                       "undeclared identifier 'SetTypeFillingByVolume'\n", encoding="utf-8")
        compile_path = state_dir / "attempts" / f"{cycle}.stage2.compile.latest.json"
        compile_path.write_text(json.dumps({"cycle_id": cycle, "stage": 2, "ea": "EA",
                                            "passed": False, "fresh_ex5": False, "errors": 8,
                                            "mq5_sha256": supervisor.sha256(source), "log": str(log)}),
                                encoding="utf-8")
        failure_path = state_dir / "events" / f"{cycle}.stage2.failed.json"
        failure = {"schema_version": 2, "cycle_id": cycle, "event_id": f"{cycle}:stage2:failed",
                   "stage": 2, "status": "failed", "ea": "EA", "candidate_ea": "EA",
                   "source": str(source), "source_sha256": supervisor.sha256(source),
                   "stage1_spec": str(spec), "stage1_spec_sha256": supervisor.sha256(spec),
                   "source_identity_available": True, "failure_kind": "COMPILE_INTEGRITY_FAILURE",
                   "failed_checks": ["compile_clean"], "verification": None,
                   "compile_attempt": str(compile_path), "rotation_eligible": False}
        failure_path.write_text(json.dumps(failure), encoding="utf-8")
        state = {"cycle_id": cycle, "candidate_source": str(source), "candidate_ea": "EA",
                 "current_ea": "EA", "last_stage1_artifact": str(spec)}
        control = {"dispatch_enabled": True, "allowed_stages": [1, 2],
                   "stage_jobs": {"2": "job-2"}}
        connection = sqlite3.connect(self.db)
        connection.execute("INSERT INTO executions VALUES (?, ?, ?, ?, ?)",
                           ("execution-compile", "job-2", "failed", "", ""))
        connection.commit()
        connection.close()
        identity = supervisor.authenticated_stage2_compile_repair_retry(
            state, control, failure_path, failure, self.db)
        self.assertEqual("one_same_candidate_compile_repair_episode", identity["repair_scope"])
        self.assertEqual(supervisor.sha256(source), identity["candidate_source_sha256"])
        connection = sqlite3.connect(self.db)
        connection.execute("UPDATE executions SET status='running' WHERE id='execution-compile'")
        connection.commit()
        connection.close()
        source.write_text("g_trade.SetDeviationInPoints(10);\n", encoding="utf-8")
        retry_state = {**state, "stage2_compile_repair_cycle_id": cycle,
                       "stage2_compile_repair_attempt_count": 1,
                       "stage2_compile_repair_identity": identity,
                       "stage2_compile_repair_execution_id": "execution-compile"}
        identity = supervisor.authenticated_stage2_compile_repair_retry(
            retry_state, control, failure_path, failure, self.db)
        self.assertEqual("active", identity["retry_state"])
        connection = sqlite3.connect(self.db)
        connection.execute("UPDATE executions SET status='failed' WHERE id='execution-compile'")
        connection.commit()
        connection.close()
        identity = supervisor.authenticated_stage2_compile_repair_retry(
            retry_state, control, failure_path, failure, self.db)
        self.assertEqual("exhausted", identity["retry_state"])

    def test_compile_source_repair_counter_is_scoped_to_the_active_cycle(self):
        state = {"cycle_id": "cycle-new", "stage2_compile_source_repair_count": 1,
                 "stage2_compile_source_repair_identity": {"cycle_id": "cycle-old"}}
        self.assertFalse(supervisor.stage2_compile_source_repair_used(state, "cycle-new"))
        state["stage2_compile_source_repair_cycle_id"] = "cycle-new"
        self.assertTrue(supervisor.stage2_compile_source_repair_used(state, "cycle-new"))
        state["stage2_compile_source_repair_count"] = 0
        self.assertFalse(supervisor.stage2_compile_source_repair_used(state, "cycle-new"))

    def test_source_repair_followup_requires_exact_rolled_back_lock_false_positive(self):
        root = self.root
        state_dir = root / "state"
        tx_root = state_dir / "diagnostics" / "stage2_repair_transactions"
        episode_id = "episode-1"
        attempt_dir = tx_root / episode_id / "attempt-1"
        candidate_output = attempt_dir / "candidate-output"
        candidate_output.mkdir(parents=True)
        (state_dir / "diagnostics").mkdir(parents=True, exist_ok=True)
        cycle = "cycle-stage2-followup"
        source = state_dir / "candidate.mq5"
        source.write_text("input int InpX=1;\n", encoding="utf-8")
        baseline_hash = supervisor.sha256(source)
        intermediate = "void CloseOpposite(){\n" \
            "   trade.PositionClose(PositionGetTicket(PositionGetInteger(POSITION_TICKET)));\n" \
            "   trade.PositionClose(PositionGetTicket(PositionGetInteger(POSITION_TICKET)));\n}\n"
        intermediate_hash = hashlib.sha256(intermediate.encode()).hexdigest().upper()
        (candidate_output / "intermediate.mq5").write_bytes(intermediate.encode("utf-8"))
        spec = state_dir / "stage1.json"
        spec.write_text("{}\n", encoding="utf-8")
        compile_path = state_dir / "compile.json"
        compile_path.write_text(json.dumps({"cycle_id": cycle, "stage": 2, "ea": "EA",
                                            "passed": False, "fresh_ex5": False, "errors": 4,
                                            "mq5_sha256": baseline_hash}), encoding="utf-8")
        failure_path = state_dir / "failure.json"
        failure = {"schema_version": 2, "cycle_id": cycle, "stage": 2, "status": "failed",
                   "failure_kind": "COMPILE_INTEGRITY_FAILURE", "compile_attempt": str(compile_path),
                   "source": str(source), "source_sha256": baseline_hash,
                   "stage1_spec": str(spec), "stage1_spec_sha256": supervisor.sha256(spec)}
        failure_path.write_text(json.dumps(failure), encoding="utf-8")
        report_path = state_dir / "repair-report.json"
        compile_log = candidate_output / "warnings.log"
        compile_log.write_text(
            f"{source}(10,1) : warning 43: possible loss of data due to type conversion from 'long' to 'uint'\n"
            f"{source}(11,1) : warning 43: possible loss of data due to type conversion from 'long' to 'uint'\n",
            encoding="utf-8")
        intermediate_compile = candidate_output / "repair-stage2.compile.latest.json"
        intermediate_compile.write_text(json.dumps({"cycle_id": cycle, "stage": 2, "ea": "EA",
                                                    "passed": False, "errors": 0, "warnings": 2,
                                                    "fresh_ex5": True, "mq5_sha256": intermediate_hash,
                                                    "log": str(compile_log)}), encoding="utf-8")
        report = {"cycle_id": cycle, "ea": "EA", "attempts": [
            {"attempt": 0, "before_sha256": baseline_hash},
            {"attempt": 1, "after_sha256": intermediate_hash,
             "source_admission_refresh": {"source_sha256": intermediate_hash},
             "compile_returncode": 0},
            {"attempt": 2, "before_sha256": intermediate_hash, "after_sha256": intermediate_hash,
             "ai": {"changes": [
                 "Replaced PositionGetTicket(PositionGetInteger(POSITION_TICKET)) with an explicit ulong cast in the long-signal close path.",
                 "Applied the same type-safe correction in the short-signal close path."]},
             "ai_application": {"status": "PATCH_REJECTED",
                                "reason": "locked strategy inputs, regimes, or score callback changed"}}
        ]}
        report_path.write_text(json.dumps(report), encoding="utf-8")
        (attempt_dir / "repair-loop-report.json").write_text(json.dumps(report), encoding="utf-8")
        (attempt_dir / "canonical-stage2-failure-event.json").write_text(json.dumps(failure), encoding="utf-8")
        (attempt_dir / "outcome.json").write_text(json.dumps({"outcome": "REJECTED",
                                                                  "episode_id": episode_id}), encoding="utf-8")
        (attempt_dir / "transaction-journal.json").write_text(json.dumps({"status": "ROLLED_BACK"}), encoding="utf-8")
        control_path = state_dir / "kcos_control_v2.json"
        control_path.write_text(json.dumps({"stage_jobs": {"2": "job-2"}}), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.execute("INSERT INTO executions VALUES (?, ?, ?, ?, ?)",
                           ("stage2-repair", "job-2", "failed", "", ""))
        connection.commit()
        connection.close()
        reason = json.dumps({"attempts": 2, "report": str(report_path), "passed": False}) + "\n" + json.dumps({
            "outcome": "REJECTED", "episode_id": episode_id, "attempt_dir": str(attempt_dir)})
        state = {"cycle_id": cycle, "current_stage": "stage2_pending", "stage_status": "stage2_repair_required",
                 "next_action": "repair_stage2_failure", "stage2_failure_event": str(failure_path),
                 "stage2_repair_reason": reason, "candidate_ea": "EA", "candidate_source": str(source),
                 "last_stage1_artifact": str(spec), "stage_jobs": {"2": "job-2"},
                 "stage2_compile_source_repair_cycle_id": cycle, "stage2_compile_source_repair_count": 1,
                 "stage2_compile_source_repair_execution_id": "stage2-repair",
                 "stage2_compile_source_repair_identity": {
                     "cycle_id": cycle, "failure_event_sha256": supervisor.sha256(failure_path),
                     "compile_attempt_sha256": supervisor.sha256(compile_path),
                     "source_sha256": baseline_hash, "stage1_spec_sha256": supervisor.sha256(spec)}}
        with mock.patch.object(supervisor, "ROOT", root):
            identity = supervisor.authenticated_stage2_source_repair_policy_followup(
                state, failure, failure_path, self.db)
            self.assertEqual("one_same_candidate_stage2_repair_after_compiler_proven_ticket_fix_lock_false_positive",
                             identity["retry_scope"])
            report["attempts"][2]["ai_application"]["reason"] = "some other source rejection"
            report_path.write_text(json.dumps(report), encoding="utf-8")
            (attempt_dir / "repair-loop-report.json").write_text(json.dumps(report), encoding="utf-8")
            self.assertIsNone(supervisor.authenticated_stage2_source_repair_policy_followup(
                state, failure, failure_path, self.db))

    def test_authenticated_generated_candidate_path_preflight_gets_one_retry(self):
        state_dir = self.root / "state"
        for folder in ("events", "candidates", "specs", "leases"):
            (state_dir / folder).mkdir(parents=True, exist_ok=True)
        cycle = "cycle-generated-path"
        source = state_dir / "candidates" / "EA.mq5"
        source.write_text("void OnTick() {}\n", encoding="utf-8")
        spec = state_dir / "specs" / "stage1.json"
        spec.write_text("{}\n", encoding="utf-8")
        failure_path = state_dir / "events" / f"{cycle}.stage2.failed.json"
        error_text = "expected str, bytes or os.PathLike object, not NoneType"
        traceback = ('File "kcos_stage2_autonomous_executor.py", line 128, in stage2_repair_loop_required\n'
                     '    candidate_source = Path(candidate_source)\n'
                     'File "kcos_stage2_autonomous_executor.py", line 1104, in main\n'
                     '    repair_evidence_present = stage2_repair_loop_required(\n')
        failure = {"schema_version": 2, "cycle_id": cycle,
                   "event_id": f"{cycle}:stage2:failed", "stage": 2,
                   "status": "failed", "failure_kind": "STAGE2_EXECUTOR_FAILURE",
                   "rotation_eligible": False, "failed_checks": [], "verification": None,
                   "compile_attempt": None, "source_identity_available": True,
                   "ea": "EA", "candidate_ea": "EA", "source": str(source),
                   "source_sha256": supervisor.sha256(source),
                   "stage1_spec": str(spec), "stage1_spec_sha256": supervisor.sha256(spec),
                   "error": error_text, "error_sha256": hashlib.sha256(error_text.encode()).hexdigest().upper(),
                   "exception_type": "TypeError", "traceback": traceback}
        failure_path.write_text(json.dumps(failure), encoding="utf-8")
        state = {"cycle_id": cycle, "candidate_source": str(source), "candidate_ea": "EA",
                 "current_ea": "EA", "last_stage1_artifact": str(spec)}
        control = {"dispatch_enabled": True, "allowed_stages": [1, 2],
                   "stage_jobs": {"2": "job-2"}}
        identity = supervisor.authenticated_stage2_initial_preflight_retry(
            state, control, failure_path, failure, self.db)
        self.assertEqual("generated_candidate_path_not_refreshed", identity["preflight_failure_class"])

    def test_authenticated_stage2_tester_diagnostic_removal_retry_is_single_and_evidence_bound(self):
        state_dir = self.root / "state"
        for folder in ("events", "candidates", "specs", "verification", "attempts", "leases"):
            (state_dir / folder).mkdir(parents=True, exist_ok=True)
        cycle = "cycle-tester-gate"
        source = state_dir / "candidates" / "EA.mq5"
        source.write_text("void OnTick() {}\n", encoding="utf-8")
        spec = state_dir / "specs" / f"{cycle}.stage1.spec.json"
        spec.write_text(json.dumps({"tester_config": {"symbol": "ETHUSD"}}), encoding="utf-8")
        stage1_event_path = state_dir / "events" / f"{cycle}.stage1.complete.json"
        stage1_event_path.write_text(json.dumps({
            "cycle_id": cycle, "stage": 1, "status": "complete",
            "artifact_sha256": supervisor.sha256(spec), "consumed": True,
            "consumed_action": "dispatch_stage2"}), encoding="utf-8")
        compile_path = state_dir / "attempts" / f"{cycle}.stage2.compile.latest.json"
        compile_path.write_text(json.dumps({
            "cycle_id": cycle, "stage": 2, "ea": "EA", "passed": True,
            "errors": 0, "warnings": 0, "fresh_ex5": True,
            "mq5_sha256": supervisor.sha256(source)}), encoding="utf-8")
        verification_path = state_dir / "verification" / f"{cycle}.stage2.verification.json"
        verification_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": cycle, "stage": 2, "ea": "EA", "passed": True,
            "checks": {"compile_clean": True},
            "source_sha256": supervisor.sha256(source),
            "stage1_spec_sha256": supervisor.sha256(spec)}), encoding="utf-8")
        (state_dir / "kcos_stage2_autonomous_executor.py").write_text(
            "run([PUBLISH, '--state', STATE, '--verification', verification, '--event', event])\n",
            encoding="utf-8")
        (state_dir / "kcos_stage2_publish_v2.py").write_text(
            "def publish(state_path, verification_path, event_path): pass\n", encoding="utf-8")
        error = "unowned terminal/tester process appeared during Stage 2 run: 25288"
        failure_path = state_dir / "events" / f"{cycle}.stage2.failed.json"
        failure = {
            "schema_version": 2, "cycle_id": cycle, "event_id": f"{cycle}:stage2:failed",
            "stage": 2, "status": "failed", "failure_kind": "STAGE2_EXECUTOR_FAILURE",
            "rotation_eligible": False, "exception_type": "RuntimeError", "failed_checks": [],
            "source_identity_available": True, "source": str(source),
            "source_sha256": supervisor.sha256(source), "stage1_spec": str(spec),
            "stage1_spec_sha256": supervisor.sha256(spec),
            "verification": str(verification_path), "compile_attempt": str(compile_path),
            "error": error,
            "error_sha256": hashlib.sha256(error.encode("utf-8")).hexdigest().upper(),
            "traceback": "run_diagnostic -> verify_live_tester_process_census"}
        failure_path.write_text(json.dumps(failure), encoding="utf-8")
        state = {"cycle_id": cycle, "current_stage": "stage2_pending",
                 "stage_status": "stage2_repair_required", "candidate_source": str(source),
                 "candidate_ea": "EA", "current_ea": "EA",
                 "last_stage1_artifact": str(spec), "last_stage1_event": str(stage1_event_path)}
        control = {"last_stage2_execution_id": "execution-stage2-failed",
                   "stage_jobs": {"2": "job-2"}}
        connection = sqlite3.connect(self.db)
        connection.execute("ALTER TABLE executions ADD COLUMN output TEXT")
        connection.execute(
            "INSERT INTO executions (id, job_id, status, started_at, finished_at, output) VALUES (?, ?, ?, ?, ?, ?)",
            ("execution-stage2-failed", "job-2", "failed", "", "",
             json.dumps({"stage": 2, "published": False, "error": error,
                         "failure_event": str(failure_path)})))
        connection.commit()
        connection.close()
        identity = supervisor.authenticated_stage2_tester_diagnostic_removal_retry(
            state, control, failure_path, failure, self.db)
        self.assertEqual("one_same_candidate_retry_after_stage2_tester_diagnostic_removal",
                         identity["retry_scope"])
        self.assertTrue(Path(identity["archived_failure_path"]).is_file())
        self.assertIsNone(supervisor.authenticated_stage2_tester_diagnostic_removal_retry(
            {**state, "stage2_tester_diagnostic_removal_retry_count": 1},
            control, failure_path, failure, self.db))
        failure["error"] = "some other Stage 2 executor failure"
        failure["error_sha256"] = hashlib.sha256(failure["error"].encode()).hexdigest().upper()
        self.assertIsNone(supervisor.authenticated_stage2_tester_diagnostic_removal_retry(
            state, control, failure_path, failure, self.db))

    def test_authenticated_stage3_zero_trade_repair_dispatches_stage2(self):
        cycle = self.state["cycle_id"]
        decision_path = self.root / f"{cycle}.stage3.decision.json"
        verification_path = self.root / f"{cycle}.stage3.verification.json"
        decision = {"cycle_id": cycle, "stage": 3, "ea": "EA",
                    "verdict": "REPAIR_REQUIRED",
                    "repair_trigger": {"kind": "completed_timeframe_zero_trades", "timeframe": "M5"},
                    "timeframes": {"M5": {"zero_trades": True, "trades": 0,
                                          "max_trades": 0, "row_count": 12}}}
        decision_path.write_text(json.dumps(decision), encoding="utf-8")
        verification = {"cycle_id": cycle, "stage": 3, "decision": str(decision_path),
                        "decision_sha256": supervisor.sha256(decision_path),
                        "passed": True, "checks": {"metrics_recomputed": True,
                                                   "stage2_lineage": True}}
        verification_path.write_text(json.dumps(verification), encoding="utf-8")
        state = {**self.state, "current_stage": "stage2_pending",
                 "current_ea": "EA", "candidate_ea": "EA",
                 "stage_status": "stage2_repair_required",
                 "next_action": "dispatch_stage2",
                 "stage3_zero_trade_decision": str(decision_path),
                 "stage3_zero_trade_verification": str(verification_path)}
        control = {**self.control, "allowed_stages": [1, 2, 3, 4, 5],
                   "hold_after_stage": 5, "dispatch_enabled": True,
                   "stage_jobs": {"1": "job-1", "2": "job-2", "3": "job-3",
                                  "4": "job-4", "4.5": "job-45", "5": "job-5"}}
        with mock.patch.object(supervisor, "STATE", self.root / "kcos_cycle_state_v2.json"):
            result = supervisor.evaluate(state, control, self.db)
        self.assertEqual("DISPATCH_READY_STAGE2", result["status"])
        self.assertEqual("dispatch_stage2", result["action"])
        self.assertTrue(result["event_id"].startswith(f"{cycle}:stage3:zero_trade_repair:"))

    def test_stage3_zero_trade_repair_requires_exact_all_zero_metrics_and_verifier_binding(self):
        cycle = self.state["cycle_id"]
        decision_path = self.root / "stage3-decision.json"
        verification_path = self.root / "stage3-verification.json"
        decision = {"cycle_id": cycle, "stage": 3, "ea": "EA",
                    "verdict": "REPAIR_REQUIRED",
                    "timeframes": {tf: {"zero_trades": True, "trades": 0,
                                        "max_trades": 0, "row_count": 12}
                                   for tf in ("M5", "M15", "H1")}}
        decision_path.write_text(json.dumps(decision), encoding="utf-8")
        verification_path.write_text(json.dumps({
            "cycle_id": cycle, "stage": 3, "decision": str(decision_path),
            "decision_sha256": supervisor.sha256(decision_path), "passed": True,
            "checks": {"metrics_recomputed": True}}), encoding="utf-8")
        self.assertIsNotNone(supervisor.authenticated_stage3_zero_trade_repair({
            "cycle_id": cycle, "current_ea": "EA",
            "stage3_zero_trade_decision": str(decision_path),
            "stage3_zero_trade_verification": str(verification_path)}))
        decision["timeframes"]["M15"]["trades"] = 1
        decision_path.write_text(json.dumps(decision), encoding="utf-8")
        with self.assertRaises(ValueError):
            supervisor.authenticated_stage3_zero_trade_repair({
                "cycle_id": cycle, "current_ea": "EA",
                "stage3_zero_trade_decision": str(decision_path),
                "stage3_zero_trade_verification": str(verification_path)})

    def test_zero_trade_repair_ignores_same_named_receipts_from_an_older_cycle(self):
        stale_cycle = "cycle-old"
        current_cycle = "cycle-current"
        decision_path = self.root / f"{stale_cycle}.stage3.decision.json"
        verification_path = self.root / f"{stale_cycle}.stage3.verification.json"
        result = supervisor.authenticated_stage3_zero_trade_repair({
            "cycle_id": current_cycle,
            "stage3_zero_trade_decision": str(decision_path),
            "stage3_zero_trade_verification": str(verification_path),
        })
        self.assertIsNone(result)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.artifact = self.root / "spec.json"
        self.event = self.root / "event.json"
        self.db = self.root / "executions.db"
        self.artifact.write_text('{"valid":true}\n', encoding="utf-8")
        self.state = {
            "schema_version": 2,
            "cycle_id": "cycle-1",
            "current_stage": "stage2_pending",
            "last_stage1_artifact": str(self.artifact),
            "last_stage1_event": str(self.event),
        }
        self.control = {
            "schema_version": 2,
            "paused": False,
            "allowed_stages": [1],
            "hold_after_stage": 1,
            "stage_jobs": {"1": "job-1"},
            "last_stage1_execution_id": "execution-1",
        }
        self.event.write_text(json.dumps({
            "schema_version": 2,
            "cycle_id": "cycle-1",
            "stage": 1,
            "artifact": str(self.artifact.resolve()),
            "artifact_sha256": supervisor.sha256(self.artifact),
            "completed_utc": "2026-01-01T00:01:00Z",
        }), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.execute("CREATE TABLE executions (id TEXT, job_id TEXT, status TEXT, started_at TEXT, finished_at TEXT)")
        connection.execute("INSERT INTO executions VALUES (?, ?, ?, ?, ?)", ("execution-1", "job-1", "completed", "2026-01-01T00:00:00Z", "2026-01-01T00:02:00Z"))
        connection.commit()
        connection.close()

    def tearDown(self):
        self.temp.cleanup()

    def test_stage3_start_claim_is_bound_to_current_stage2_publication(self):
        event = self.root / "stage2-complete.json"
        verification = self.root / "stage2-verification.json"
        source = self.root / "candidate.mq5"
        source.write_text("// candidate v1\n", encoding="utf-8")
        verification.write_text(json.dumps({"cycle_id": "cycle-1", "source": str(source)}), encoding="utf-8")
        event.write_text(json.dumps({"cycle_id": "cycle-1", "stage": 2, "status": "complete",
                                     "verification_sha256": supervisor.sha256(verification),
                                     "mq5_sha256": supervisor.sha256(source), "ex5_sha256": "EX5-A"}),
                         encoding="utf-8")
        state = {"cycle_id": "cycle-1", "last_stage2_event": str(event),
                 "last_stage2_verification": str(verification)}
        first = supervisor.stage3_start_content_identity(state)
        claimed, first_path = supervisor.claim_action(self.root / "claims", "cycle-1:stage2:complete",
                                                       "start_stage3", first)
        self.assertTrue(claimed)
        same, same_path = supervisor.claim_action(self.root / "claims", "cycle-1:stage2:complete",
                                                   "start_stage3", first)
        self.assertFalse(same)
        self.assertEqual(first_path, same_path)

        source.write_text("// candidate v2\n", encoding="utf-8")
        verification.write_text(json.dumps({"cycle_id": "cycle-1", "source": str(source)}), encoding="utf-8")
        event.write_text(json.dumps({"cycle_id": "cycle-1", "stage": 2, "status": "complete",
                                     "verification_sha256": supervisor.sha256(verification),
                                     "mq5_sha256": supervisor.sha256(source), "ex5_sha256": "EX5-B"}),
                         encoding="utf-8")
        second = supervisor.stage3_start_content_identity(state)
        self.assertNotEqual(first, second)
        second_claimed, second_path = supervisor.claim_action(self.root / "claims", "cycle-1:stage2:complete",
                                                              "start_stage3", second)
        self.assertTrue(second_claimed)
        self.assertNotEqual(first_path, second_path)

    def test_stage2_dispatch_claim_is_persisted_before_execution_and_is_replay_stable(self):
        state_path = self.root / "cycle-state.json"
        state_path.write_text(json.dumps({"cycle_id": "cycle-stage2", "current_ea": "EA"}),
                              encoding="utf-8")
        identity = {"cycle_id": "cycle-stage2", "candidate_ea": "EA",
                    "candidate_source_sha256": "A" * 64, "event_id": "stage1-event"}
        claimed, claim_path = supervisor.claim_action(
            self.root / "claims", "stage1-event", "dispatch_stage2", identity)
        self.assertTrue(claimed)
        first = supervisor.persist_stage2_dispatch_claim(state_path, claim_path, "stage1-event")
        replay = supervisor.persist_stage2_dispatch_claim(state_path, claim_path, "stage1-event")
        self.assertEqual(first, replay)
        persisted = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(first, persisted["stage2_dispatch_claim"])

        next_identity = {**identity, "event_id": "stage1-event-retry"}
        next_claimed, next_path = supervisor.claim_action(
            self.root / "claims", "stage1-event-retry", "dispatch_stage2", next_identity)
        self.assertTrue(next_claimed)
        second = supervisor.persist_stage2_dispatch_claim(
            state_path, next_path, "stage1-event-retry")
        self.assertNotEqual(first["event_id"], second["event_id"])
        self.assertNotEqual(first["claim_path"], second["claim_path"])
        self.assertNotEqual(first["content_identity_sha256"], second["content_identity_sha256"])

    def test_stage3_continuation_claim_identity_tracks_completion_receipt_content(self):
        cycle = "cycle-1"
        event_path = self.root / "stage3-m5-complete.json"
        event = {"cycle_id": cycle, "event_id": f"{cycle}:stage3:M5:complete",
                 "stage": 3, "status": "TIMEFRAME_COMPLETE", "timeframe": "M5",
                 "opt_sha256": "OPT-A", "consumed": False}
        event_path.write_text(json.dumps(event), encoding="utf-8")
        state = {"cycle_id": cycle}
        result = {"event_id": event["event_id"], "event_path": str(event_path)}
        first = supervisor.stage3_continuation_content_identity(result, state)
        first_claimed, first_path = supervisor.claim_action(
            self.root / "claims", result["event_id"], "continue_stage3", first)
        self.assertTrue(first_claimed)

        event["opt_sha256"] = "OPT-B"
        event_path.write_text(json.dumps(event), encoding="utf-8")
        second = supervisor.stage3_continuation_content_identity(result, state)
        second_claimed, second_path = supervisor.claim_action(
            self.root / "claims", result["event_id"], "continue_stage3", second)
        self.assertTrue(second_claimed)
        self.assertNotEqual(first, second)
        self.assertNotEqual(first_path, second_path)

        event["consumed"] = True
        event_path.write_text(json.dumps(event), encoding="utf-8")
        consumed_identity = supervisor.stage3_continuation_content_identity(result, state)
        self.assertEqual(second, consumed_identity)

    def test_stage3_infrastructure_rotation_requires_explicit_eligibility(self):
        self.assertFalse(supervisor.stage3_infrastructure_failure_allows_rotation({
            "status": "failed", "failure_kind": "STAGE3_EXECUTION_FAILURE",
            "rotation_eligible": False,
        }))
        self.assertTrue(supervisor.stage3_infrastructure_failure_allows_rotation({
            "status": "failed", "failure_kind": "STAGE3_EXECUTION_FAILURE",
            "rotation_eligible": True,
        }))
        self.assertTrue(supervisor.stage3_infrastructure_failure_allows_rotation({
            "status": "COMPUTE_BUDGET_EXCEEDED",
        }))

    def test_execution_event_must_not_match_an_unrelated_prior_run(self):
        matched = supervisor.completed_execution_for_event(
            self.db, "job-1", "2026-01-01T00:02:01Z"
        )
        self.assertIsNotNone(matched)
        unrelated = supervisor.completed_execution_for_event(
            self.db, "job-1", "2026-01-01T00:07:00Z"
        )
        self.assertIsNone(unrelated)

    def test_dispatch_reuses_a_cron_execution_claimed_during_resume(self):
        success = subprocess.CompletedProcess([], 0, stdout="ok", stderr="")
        with mock.patch.object(supervisor, "validate_dispatch_entrypoint", return_value=(True, "script")), \
             mock.patch.object(supervisor.subprocess, "run", side_effect=[success, success]) as run, \
             mock.patch.object(supervisor, "newest_unseen_execution", return_value="raced-execution"), \
             mock.patch.object(supervisor, "execution_status", return_value="running"), \
             mock.patch.object(supervisor.subprocess, "Popen") as popen:
            accepted, detail, execution_id = supervisor.dispatch_job(
                {"stage_jobs": {"1": "job-1"}}, "dispatch_stage1", db_path=self.db
            )
        self.assertTrue(accepted)
        self.assertEqual("raced-execution", execution_id)
        self.assertIn("reused already-claimed", detail)
        self.assertEqual(2, run.call_count)  # resume, then pause the recurring schedule
        popen.assert_not_called()

    def test_stage3_dispatch_keeps_recurring_schedule_active_after_raced_claim(self):
        success = subprocess.CompletedProcess([], 0, stdout="ok", stderr="")
        with mock.patch.object(supervisor, "validate_dispatch_entrypoint", return_value=(True, "script")), \
             mock.patch.object(supervisor.subprocess, "run", return_value=success) as run, \
             mock.patch.object(supervisor, "newest_unseen_execution", return_value="raced-execution"), \
             mock.patch.object(supervisor, "execution_status", return_value="running"), \
             mock.patch.object(supervisor.subprocess, "Popen") as popen:
            accepted, detail, execution_id = supervisor.dispatch_job(
                {"stage_jobs": {"3": "job-3"}}, "dispatch_stage3", db_path=self.db
            )
        self.assertTrue(accepted)
        self.assertEqual("raced-execution", execution_id)
        self.assertIn("recurring schedule kept active", detail)
        self.assertEqual(1, run.call_count)  # resume only; do not pause Stage 3
        popen.assert_not_called()

    def _stage3_input_contract_failure_fixture(self):
        cycle, ea = "input-contract-cycle", "EA-CONTRACT"
        spec = self.root / "stage1.spec.json"
        stage2 = self.root / "stage2.complete.json"
        stage2_handoff = self.root / "Stage2_v2.json"
        handoff = self.root / "stage3.handoff.json"
        for path, payload in ((spec, {"cycle_id": cycle}),
                              (stage2, {"cycle_id": cycle, "stage": 2}),
                              (stage2_handoff, {"cycle_id": cycle,
                                                "expected_event": str(stage2.resolve())}),
                              (handoff, {"cycle_id": cycle, "stage": 3})):
            path.write_text(json.dumps(payload), encoding="utf-8")
        event_path = self.root / "stage3.failed.json"
        evidence = {}
        for name, path in (("stage1_spec", spec), ("stage2_event", stage2),
                           ("stage2_handoff", stage2_handoff)):
            evidence[name] = {"path": str(path.resolve()), "sha256": supervisor.sha256(path)}
        event = {"schema_version": 2, "event_id": f"{cycle}:stage3:input_contract_rejected",
                 "cycle_id": cycle, "stage": 3, "status": "failed",
                 "failure_kind": "STAGE3_INPUT_CONTRACT_FAILURE", "rotation_eligible": True,
                 "ea": ea, "evidence": evidence,
                 "findings": [{"code": "INPUT_CONSTRAINTS_UNDECLARED"}]}
        event_path.write_text(json.dumps(event), encoding="utf-8")
        state = {"schema_version": 2, "cycle_id": cycle, "current_ea": ea,
                 "current_stage": "stage3_failed_held", "stage_status": "stage3_input_contract_rejected",
                 "stage3_failure_event": str(event_path.resolve()),
                 "last_stage1_artifact": str(spec.resolve()),
                 "last_stage2_event": str(stage2.resolve()),
                 "last_stage2_handoff": str(stage2_handoff.resolve()),
                 "last_stage3_handoff": str(handoff.resolve()),
                 "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
                 "run_authorization": {"schema_version": 1, "cycle_id": cycle,
                     "scope": "current_cycle_stages_1_to_5", "allowed_stages": [1, 2, 3, 4, 5],
                     "hold_after_stage": 5, "authorized_by": "user_confirmed",
                     "reason": "Continue the authorized full Kiyosaki run",
                     "authorized_utc": "2026-10-10T00:00:00Z"}}
        return state, event_path, stage2

    def test_authenticated_stage3_input_contract_failure_rotates_candidate(self):
        state, event_path, _stage2_path = self._stage3_input_contract_failure_fixture()
        control = {"schema_version": 2, "paused": False, "dispatch_enabled": True,
                   "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5}
        with mock.patch.object(supervisor, "STATE", self.root / "kcos_cycle_state_v2.json"):
            result = supervisor.evaluate(state, control, self.db)
        self.assertEqual("rotate_stage1", result["action"])
        self.assertEqual(str(event_path.resolve()), result["event_path"])

    def test_stage3_input_contract_accepts_only_consumption_mutation_of_stage2_event(self):
        state, failure_path, stage2_path = self._stage3_input_contract_failure_fixture()
        stage2_event = json.loads(stage2_path.read_text(encoding="utf-8"))
        stage2_event.update({"schema_version": 2,
                             "event_id": f"{state['cycle_id']}:stage2:complete",
                             "status": "complete"})
        stage2_path.write_text(json.dumps(stage2_event, indent=2) + "\n", encoding="utf-8")
        failure = json.loads(failure_path.read_text(encoding="utf-8"))
        failure["evidence"]["stage2_event"]["sha256"] = supervisor.sha256(stage2_path)
        failure_path.write_text(json.dumps(failure, indent=2) + "\n", encoding="utf-8")

        stage2_event.update({"consumed": True, "consumed_utc": "2026-10-12T07:33:23Z",
                             "consumed_action": "start_stage3"})
        stage2_path.write_text(json.dumps(stage2_event, indent=2) + "\n", encoding="utf-8")
        valid, detail, _ = supervisor.validate_stage3_input_contract_failure(state)
        self.assertTrue(valid, detail)

        stage2_event["mq5_sha256"] = "tampered"
        stage2_path.write_text(json.dumps(stage2_event, indent=2) + "\n", encoding="utf-8")
        valid, detail, _ = supervisor.validate_stage3_input_contract_failure(state)
        self.assertFalse(valid)
        self.assertIn("hash/path mismatch", detail)

    def test_stage3_input_contract_rotation_rejects_tampered_upstream_evidence(self):
        state, _event_path, stage2_path = self._stage3_input_contract_failure_fixture()
        stage2_path.write_text('{"cycle_id":"different"}', encoding="utf-8")
        control = {"schema_version": 2, "paused": False, "dispatch_enabled": True,
                   "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5}
        with mock.patch.object(supervisor, "STATE", self.root / "kcos_cycle_state_v2.json"):
            result = supervisor.evaluate(state, control, self.db)
        self.assertEqual("none", result["action"])
        self.assertEqual("UNKNOWN_STALLED", result["status"])

    def test_stage1_research_retry_is_same_cycle_and_bounded(self):
        cycle = "research-retry-cycle"
        state = {"schema_version": 2, "cycle_id": cycle, "current_stage": "stage1_running",
                 "stage_status": "stage1_research_required", "next_action": "research_new",
                 "research_mode": "research_new", "failure_count": 9,
                 "stage1_research_retry_count": 0}
        handoff = {"cycle_id": cycle, "research_mode": "research_new",
                   "research_allowed": True, "queue_exhausted": True}
        failure = {"cycle_id": cycle, "stage": 1, "status": "failed", "rotation_eligible": True}
        self.assertTrue(supervisor.stage1_research_retry_eligible(state, handoff, failure, source_available=True))
        self.assertFalse(supervisor.stage1_research_retry_eligible(
            dict(state, stage1_research_retry_count=2), handoff, failure, source_available=True))
        self.assertFalse(supervisor.stage1_research_retry_eligible(
            state, handoff, dict(failure, cycle_id="another-cycle"), source_available=True))
        self.assertFalse(supervisor.stage1_research_retry_eligible(
            state, handoff, failure, source_available=False))

    def test_stage1_sear_readiness_requires_a_usable_url_result(self):
        class Response:
            status = 200

            def __init__(self, payload):
                self.payload = io.BytesIO(json.dumps(payload).encode("utf-8"))

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit):
                return self.payload.read()

        with mock.patch.object(supervisor, "stage1_research_hermes_web_available", return_value=False), \
                mock.patch.object(supervisor.urllib.request, "urlopen", return_value=Response({"results": []})):
            self.assertFalse(supervisor.stage1_research_source_endpoint_ready())
        with mock.patch.object(supervisor, "stage1_research_hermes_web_available", return_value=False), \
                mock.patch.object(supervisor.urllib.request, "urlopen", return_value=Response(
                {"results": [{"url": "https://example.org/research"}]})):
            self.assertTrue(supervisor.stage1_research_source_endpoint_ready())

    def test_stage1_retry_uses_configured_hermes_web_when_searxng_is_unavailable(self):
        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit):
                return b'{"results": []}'

        with mock.patch.object(supervisor, "stage1_research_hermes_web_available", return_value=True), \
                mock.patch.object(supervisor.urllib.request, "urlopen", return_value=Response()):
            self.assertTrue(supervisor.stage1_research_source_endpoint_ready())

    def test_stage1_repair_receipt_authorizes_one_retry_without_resetting_count(self):
        cycle = "research-repair-retry-cycle"
        receipt_path = self.root / "stage1-auto-evolve-repair.json"
        receipt = {
            "schema_version": 1,
            "record_kind": "kcos_stage1_auto_evolve_repair",
            "cycle_id": cycle,
            "repair_id": "auto-evolve-fix-1234",
            "retry_count_at_authorization": 3,
            "authorized_action": "one_same_cycle_stage1_retry",
        }
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        state = {"schema_version": 2, "cycle_id": cycle, "current_stage": "stage1_running",
                 "stage_status": "stage1_research_required", "next_action": "research_new",
                 "research_mode": "research_new", "stage1_research_retry_count": 3,
                 "stage1_research_retry_repair": {
                     "status": "authorized", "cycle_id": cycle,
                     "repair_id": receipt["repair_id"],
                     "retry_count_at_authorization": 3,
                     "receipt_path": str(receipt_path),
                     "receipt_sha256": supervisor.sha256(receipt_path),
                 }}
        handoff = {"cycle_id": cycle, "research_mode": "research_new",
                   "research_allowed": True, "queue_exhausted": True}
        failure = {"cycle_id": cycle, "stage": 1, "status": "failed", "rotation_eligible": True}

        self.assertTrue(supervisor.stage1_research_retry_eligible(state, handoff, failure, source_available=True))
        control = {"schema_version": 2, "paused": False, "dispatch_enabled": True,
                   "allowed_stages": [1]}
        with mock.patch.object(supervisor, "STATE", self.root / "kcos_cycle_state_v2.json"):
            state_path = self.root / "stage1-research-repair-state.json"
            handoff_path = self.root / "stage1-research-repair-handoff.json"
            failure_path = self.root / "stage1-research-repair-failure.json"
            state["last_stage1_handoff"] = str(handoff_path)
            state["stage1_failure_event"] = str(failure_path)
            handoff_path.write_text(json.dumps(handoff), encoding="utf-8")
            failure_path.write_text(json.dumps(failure), encoding="utf-8")
            with mock.patch.object(supervisor, "stage1_research_source_endpoint_ready", return_value=True):
                decision = supervisor.evaluate(state, control, self.db)
        self.assertEqual("STAGE1_RESEARCH_RETRY_READY", decision["status"])
        self.assertEqual(f"{cycle}:stage1:research-retry:repair:{receipt['repair_id']}",
                         decision["event_id"])
        receipt_path.write_text(json.dumps({**receipt, "tampered": True}), encoding="utf-8")
        self.assertFalse(supervisor.stage1_research_retry_eligible(state, handoff, failure, source_available=True))

    def test_stage1_research_retry_claim_identity_advances_per_allowed_attempt(self):
        cycle = "research-retry-claim-cycle"
        handoff_path = self.root / "stage1-handoff.json"
        failure_path = self.root / "stage1-failure.json"
        handoff_path.write_text(json.dumps({
            "cycle_id": cycle, "research_mode": "research_new",
            "research_allowed": True, "queue_exhausted": True,
        }), encoding="utf-8")
        failure_path.write_text(json.dumps({
            "cycle_id": cycle, "stage": 1, "status": "failed",
            "rotation_eligible": True,
        }), encoding="utf-8")
        control = {"schema_version": 2, "paused": False,
                   "dispatch_enabled": True, "allowed_stages": [1]}
        event_ids = []
        with mock.patch.object(supervisor, "stage1_research_source_endpoint_ready", return_value=True):
            for retry_count in (0, 1):
                state = {"schema_version": 2, "cycle_id": cycle,
                         "current_stage": "stage1_running",
                         "stage_status": "stage1_research_required",
                         "next_action": "research_new", "research_mode": "research_new",
                         "stage1_research_retry_count": retry_count,
                         "last_stage1_handoff": str(handoff_path),
                         "stage1_failure_event": str(failure_path)}
                decision = supervisor.evaluate(state, control, self.db)
                self.assertEqual("dispatch_stage1", decision["action"])
                self.assertEqual("STAGE1_RESEARCH_RETRY_READY", decision["status"])
                event_ids.append(decision["event_id"])
        self.assertEqual([
            f"{cycle}:stage1:research-retry:1",
            f"{cycle}:stage1:research-retry:2",
        ], event_ids)
        self.assertNotEqual(f"{cycle}:stage1:pending", event_ids[0])
        self.assertNotEqual(event_ids[0], event_ids[1])
        self.assertTrue(supervisor.stage1_dispatch_uses_claim_only({
            "action": "dispatch_stage1", "status": "STAGE1_RESEARCH_RETRY_READY",
            "event_id": event_ids[1],
        }))
        self.assertTrue(supervisor.stage1_dispatch_uses_claim_only({
            "action": "dispatch_stage1", "status": "DISPATCH_READY_STAGE1",
            "event_id": f"{cycle}:stage1:pending",
        }))
        self.assertFalse(supervisor.stage1_dispatch_uses_claim_only({
            "action": "dispatch_stage1", "status": "ROTATION_READY_STAGE1",
            "event_id": f"{cycle}:stage3:input_contract_rejected",
        }))
        with mock.patch.object(supervisor, "stage1_research_source_endpoint_ready", return_value=True):
            exhausted = supervisor.evaluate({
                "schema_version": 2, "cycle_id": cycle, "current_stage": "stage1_running",
                "stage_status": "stage1_research_required", "next_action": "research_new",
                "research_mode": "research_new", "stage1_research_retry_count": 2,
                "last_stage1_handoff": str(handoff_path),
                "stage1_failure_event": str(failure_path),
            }, control, self.db)
        self.assertEqual("none", exhausted["action"])

    def test_stage1_research_retry_waits_without_consuming_attempt_when_sear_is_unavailable(self):
        cycle = "research-retry-provider-hold-cycle"
        handoff_path = self.root / "stage1-provider-handoff.json"
        failure_path = self.root / "stage1-provider-failure.json"
        handoff_path.write_text(json.dumps({
            "cycle_id": cycle, "research_mode": "research_new",
            "research_allowed": True, "queue_exhausted": True,
        }), encoding="utf-8")
        failure_path.write_text(json.dumps({
            "cycle_id": cycle, "stage": 1, "status": "failed",
            "rotation_eligible": True,
        }), encoding="utf-8")
        state = {"schema_version": 2, "cycle_id": cycle,
                 "current_stage": "stage1_running",
                 "stage_status": "stage1_research_required",
                 "next_action": "research_new", "research_mode": "research_new",
                 "stage1_research_retry_count": 1,
                 "last_stage1_handoff": str(handoff_path),
                 "stage1_failure_event": str(failure_path)}
        control = {"schema_version": 2, "paused": False,
                   "dispatch_enabled": True, "allowed_stages": [1]}
        with mock.patch.object(supervisor, "stage1_research_source_endpoint_ready", return_value=False):
            decision = supervisor.evaluate(state, control, self.db)
        self.assertEqual("WAITING_STAGE1_RESEARCH_SOURCE_PROVIDER", decision["status"])
        self.assertEqual("none", decision["action"])

    def test_stage1_timeout_does_not_rotate_while_latest_execution_is_live(self):
        state_path = self.root / "stage1-state.json"
        state_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": "stage1-live-cycle",
            "current_stage": "stage1_running", "stage_status": "stage1_rotation_prepared",
            "updated_utc": "2026-01-01T00:00:00Z",
        }), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        try:
            connection.execute(
                "INSERT INTO executions VALUES (?, ?, ?, ?, ?)",
                ("stage1-live-execution", "job-1", "running", "2026-10-09T00:00:00Z", None),
            )
            connection.commit()
        finally:
            connection.close()

        changed = supervisor.expire_stale_stage1(
            state_path, {"last_stage1_execution_id": "stage1-live-execution"}, self.db
        )

        self.assertFalse(changed)
        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("stage1_rotation_prepared", current["stage_status"])
        self.assertFalse((self.root / "events" / "stage1-live-cycle.stage1.failed.json").exists())

    def test_completed_stage1_without_artifact_is_reconciled_to_bounded_research_retry(self):
        cycle = "stage1-no-artifact-cycle"
        handoff_path = self.root / "handoffs" / "Stage1_v2.json"
        handoff_path.parent.mkdir(parents=True)
        spec_path = self.root / "specs" / f"{cycle}.stage1.spec.json"
        event_path = self.root / "events" / f"{cycle}.stage1.complete.json"
        handoff_path.write_text(json.dumps({
            "cycle_id": cycle, "research_mode": "research_new",
            "expected_spec_path": str(spec_path), "expected_event_path": str(event_path),
        }), encoding="utf-8")
        state_path = self.root / "stage1-no-artifact-state.json"
        state_path.write_text(json.dumps({
            "cycle_id": cycle, "current_stage": "stage1_running",
            "stage_status": "stage1_rotation_prepared", "research_mode": "research_new",
            "last_stage1_handoff": str(handoff_path),
            "stage1_rotation_prepared_utc": "2026-10-12T00:00:00Z",
            "updated_utc": "2026-01-01T00:00:00Z",
        }), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.execute(
            "INSERT INTO executions (id, job_id, status, started_at, finished_at) VALUES (?, ?, ?, ?, ?)",
            ("stage1-no-artifact-execution", "job-1", "completed",
             "2026-10-13T00:00:00Z", "2026-10-13T00:04:00Z"),
        )
        connection.commit()
        connection.close()

        changed = supervisor.reconcile_completed_stage1_without_artifact(
            state_path, {"last_stage1_execution_id": "stage1-no-artifact-execution",
                         "stage_jobs": {"1": "job-1"}}, self.db
        )

        self.assertTrue(changed)
        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("stage1_research_required", current["stage_status"])
        self.assertEqual("research_new", current["next_action"])
        failure = json.loads(Path(current["stage1_failure_event"]).read_text(encoding="utf-8"))
        self.assertEqual("STAGE1_EXECUTION_NO_ARTIFACT", failure["failure_kind"])
        self.assertEqual("stage1-no-artifact-execution", failure["hermes_execution_id"])

        first_event = Path(current["stage1_failure_event"])
        connection = sqlite3.connect(self.db)
        connection.execute(
            "INSERT INTO executions (id, job_id, status, started_at, finished_at) VALUES (?, ?, ?, ?, ?)",
            ("stage1-no-artifact-retry", "job-1", "completed",
             "2026-10-13T00:05:00Z", "2026-10-13T00:09:00Z"),
        )
        connection.commit()
        connection.close()
        changed_retry = supervisor.reconcile_completed_stage1_without_artifact(
            state_path, {"last_stage1_execution_id": "stage1-no-artifact-retry",
                         "stage_jobs": {"1": "job-1"}}, self.db
        )
        retried = json.loads(state_path.read_text(encoding="utf-8"))
        retry_event = Path(retried["stage1_failure_event"])
        self.assertTrue(changed_retry)
        self.assertNotEqual(first_event, retry_event)
        self.assertEqual("stage1-no-artifact-retry",
                         json.loads(retry_event.read_text(encoding="utf-8"))["hermes_execution_id"])

    def test_completed_stage1_reconciles_hash_bound_research_hold_without_retry(self):
        cycle = "stage1-research-hold-cycle"
        handoff_path = self.root / "handoffs" / "Stage1_v2.json"
        handoff_path.parent.mkdir(parents=True)
        spec_path = self.root / "specs" / f"{cycle}.stage1.spec.json"
        event_path = self.root / "events" / f"{cycle}.stage1.complete.json"
        handoff_path.write_text(json.dumps({
            "cycle_id": cycle, "research_mode": "research_new",
            "expected_spec_path": str(spec_path), "expected_event_path": str(event_path),
        }), encoding="utf-8")
        events_dir = self.root / "events"
        events_dir.mkdir(parents=True)
        source_paths = [self.root / "source-a.pdf", self.root / "source-b.pdf"]
        source_records = []
        for source_path in source_paths:
            source_path.write_bytes(source_path.name.encode("utf-8"))
            source_records.append({
                "content_path": str(source_path),
                "content_sha256": supervisor.sha256(source_path),
            })
        hold_path = events_dir / f"{cycle}.stage1.hold.no-eligible-candidate.json"
        hold_path.write_text(json.dumps({
            "schema_version": 1, "kind": "stage1_research_new_hold",
            "event_type": "stage1.hold.no_eligible_candidate", "cycle_id": cycle,
            "tick_utc": "2026-10-12T23:58:00+11:00",
            "edge": "Vol Regime", "research_mode": "research_new",
            "eligible_decision": "PERSIST_HOLD", "publisher_invoked": False,
            "spec_written": False, "pool_mutated": False, "queue_mutated": False,
            "selection_not_committed": True, "source_verification": source_records,
            "candidate_review": [{"decision": "rejected_not_novel"}],
            "no_state_mutation_to": [
                "state/kcos_cycle_state_v2.json", "state/handoffs/Stage1_v2.json",
                "state/primitive_pool.json", "state/edge_queue.json",
                "state/attempt_history.json", "expected_spec_path", "expected_event_path",
                "published_spec", "published_event",
            ],
            "reason": "No eligible source-backed candidate was found.",
        }), encoding="utf-8")
        prior_failure_path = events_dir / f"{cycle}.stage1.failed.retry-prior.json"
        prior_failure_path.write_text(json.dumps({
            "cycle_id": cycle, "status": "failed",
            "failure_kind": "STAGE1_EXECUTION_NO_ARTIFACT",
            "hermes_execution_id": "stage1-hold-execution",
        }), encoding="utf-8")
        state_path = self.root / "stage1-research-hold-state.json"
        state_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": cycle, "current_edge": "Vol Regime",
            "current_stage": "stage1_running", "stage_status": "stage1_research_required",
            "next_action": "research_new", "research_mode": "research_new",
            "stage1_research_retry_count": 2, "failure_count": 4,
            "stage1_research_retry_repair": {
                "status": "consumed", "cycle_id": cycle,
                "repair_id": "repair-research-boundary", "consumed_utc": "2026-10-13T00:01:00+11:00",
            },
            "last_stage1_handoff": str(handoff_path),
            "stage1_failure_event": str(prior_failure_path),
            "stage1_rotation_prepared_utc": "2026-10-12T00:00:00Z",
        }), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.execute(
            "INSERT INTO executions (id, job_id, status, started_at, finished_at) VALUES (?, ?, ?, ?, ?)",
            ("stage1-hold-execution", "job-1", "completed",
             "2026-10-13T00:00:00+11:00", "2026-10-13T00:04:00+11:00"),
        )
        connection.commit()
        connection.close()
        output_path = self.root / "hermes" / "cron" / "output" / "job-1" / "2026-10-13_00-04-00.md"
        output_path.parent.mkdir(parents=True)
        output_path.write_text(
            "# Cron Job: kcos_stage1_research_new\n"
            "**Job ID:** job-1\n**Run Time:** 2026-10-13 00:04:00\n"
            "## Response\n"
            f"Stage 1 held for {cycle}. Existing same-cycle hold remains the valid outcome: "
            f"{hold_path.name}. No spec was written, no publisher was invoked, "
            "and no queue/pool mutation was made.\n",
            encoding="utf-8",
        )

        with mock.patch.dict("os.environ", {"LOCALAPPDATA": str(self.root)}):
            changed = supervisor.reconcile_completed_stage1_without_artifact(
                state_path, {"last_stage1_execution_id": "stage1-hold-execution",
                             "stage_jobs": {"1": "job-1"}}, self.db
            )

        self.assertTrue(changed)
        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("stage1_failed_held", current["stage_status"])
        self.assertEqual("hold_no_eligible_stage1_candidate", current["next_action"])
        self.assertEqual(4, current["failure_count"])
        self.assertEqual(str(hold_path.resolve()), current["stage1_research_event"])
        held = json.loads(Path(current["stage1_failure_event"]).read_text(encoding="utf-8"))
        self.assertEqual("STAGE1_RESEARCH_HOLD", held["failure_kind"])
        self.assertEqual(supervisor.sha256(hold_path), held["stage1_hold_event_sha256"])
        self.assertEqual(str(output_path.resolve()), held["stage1_reconfirmation_output"])
        self.assertEqual(supervisor.sha256(output_path), held["stage1_reconfirmation_output_sha256"])
        decision = supervisor.evaluate(current, {"schema_version": 2, "paused": False,
                                  "dispatch_enabled": True, "allowed_stages": [1]}, self.db)
        self.assertEqual("COMPLETE_HELD_STAGE1_RESEARCH", decision["status"])
        self.assertEqual("none", decision["action"])

    def test_tampered_stage1_hold_remains_a_missing_artifact_failure(self):
        cycle = "stage1-tampered-hold-cycle"
        handoff_path = self.root / "Stage1_v2.json"
        spec_path = self.root / "specs" / f"{cycle}.stage1.spec.json"
        event_path = self.root / "events" / f"{cycle}.stage1.complete.json"
        handoff_path.write_text(json.dumps({
            "cycle_id": cycle, "research_mode": "research_new",
            "expected_spec_path": str(spec_path), "expected_event_path": str(event_path),
        }), encoding="utf-8")
        source_path = self.root / "source.pdf"
        source_path.write_bytes(b"captured source")
        events_dir = self.root / "events"
        events_dir.mkdir(parents=True)
        hold_path = events_dir / f"{cycle}.stage1.hold.no-eligible-candidate.json"
        hold_path.write_text(json.dumps({
            "schema_version": 1, "kind": "stage1_research_new_hold",
            "event_type": "stage1.hold.no_eligible_candidate", "cycle_id": cycle,
            "edge": "Vol Regime", "research_mode": "research_new",
            "eligible_decision": "PERSIST_HOLD", "publisher_invoked": False,
            "spec_written": False, "pool_mutated": False, "queue_mutated": False,
            "selection_not_committed": True,
            "source_verification": [{"content_path": str(source_path),
                                     "content_sha256": "0" * 64}],
            "candidate_review": [{"decision": "rejected_not_novel"}],
            "no_state_mutation_to": [
                "state/kcos_cycle_state_v2.json", "state/handoffs/Stage1_v2.json",
                "state/primitive_pool.json", "state/edge_queue.json",
                "state/attempt_history.json", "expected_spec_path", "expected_event_path",
                "published_spec", "published_event",
            ],
        }), encoding="utf-8")
        state_path = self.root / "stage1-tampered-hold-state.json"
        state_path.write_text(json.dumps({
            "cycle_id": cycle, "current_edge": "Vol Regime",
            "current_stage": "stage1_running", "stage_status": "stage1_rotation_prepared",
            "research_mode": "research_new", "last_stage1_handoff": str(handoff_path),
            "stage1_rotation_prepared_utc": "2026-10-12T00:00:00Z",
        }), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.execute(
            "INSERT INTO executions (id, job_id, status, started_at, finished_at) VALUES (?, ?, ?, ?, ?)",
            ("stage1-tampered-hold-execution", "job-1", "completed",
             "2026-10-13T00:00:00Z", "2026-10-13T00:04:00Z"),
        )
        connection.commit()
        connection.close()

        changed = supervisor.reconcile_completed_stage1_without_artifact(
            state_path, {"last_stage1_execution_id": "stage1-tampered-hold-execution",
                         "stage_jobs": {"1": "job-1"}}, self.db
        )

        self.assertTrue(changed)
        current = json.loads(state_path.read_text(encoding="utf-8"))
        failure = json.loads(Path(current["stage1_failure_event"]).read_text(encoding="utf-8"))
        self.assertEqual("STAGE1_EXECUTION_NO_ARTIFACT", failure["failure_kind"])

    def test_completed_stage1_reconciles_current_zero_search_schema_as_hold(self):
        cycle = "stage1-current-search-hold-cycle"
        state_path = self.root / "state" / "kcos_cycle_state_v2.json"
        events_dir = state_path.parent / "events"
        research_dir = state_path.parent / "research" / cycle
        events_dir.mkdir(parents=True)
        research_dir.mkdir(parents=True)
        handoff_path = state_path.parent / "handoffs" / "Stage1_v2.json"
        handoff_path.parent.mkdir(parents=True)
        spec_path = state_path.parent / "specs" / f"{cycle}.stage1.spec.json"
        event_path = state_path.parent / "events" / f"{cycle}.stage1.complete.json"
        handoff_path.write_text(json.dumps({
            "cycle_id": cycle, "research_mode": "research_new",
            "expected_spec_path": str(spec_path), "expected_event_path": str(event_path),
        }), encoding="utf-8")
        hold_path = events_dir / f"{cycle}.stage1.hold.research_evidence_required.retry.json"
        created = "2026-10-13T00:02:00Z"
        attempts = [{"query": f"query-{index}", "result_count": 0,
                     "retrieved_utc": "2026-10-13T00:01:00Z"}
                    for index in range(1, 4)]
        hold_path.write_text(json.dumps({
            "schema_version": 2, "event_id": f"{cycle}:stage1:hold:RESEARCH_EVIDENCE_REQUIRED",
            "cycle_id": cycle, "stage": 1, "status": "held",
            "failure_kind": "RESEARCH_EVIDENCE_REQUIRED",
            "reason": "Fresh bounded search returned no eligible evidence.",
            "research_mode": "research_new", "edge": "Vol Regime",
            "current_stage": "stage1_running", "next_action": "research_new",
            "candidate_primitive": None, "candidate_ea": None,
            "selection_not_committed": True, "rotation_eligible": False,
            "consumed": False, "publisher_invoked": False, "spec_written": False,
            "pool_mutated": False, "queue_mutated": False,
            "search_attempts": attempts, "created_utc": created,
        }), encoding="utf-8")
        wrapper_path = research_dir / "stage1_hold_retry_20261013T000200Z.json"
        wrapper_path.write_text(json.dumps({
            "cycle_id": cycle, "hold_event": str(hold_path),
            "hold_reason": "Fresh bounded search returned no eligible evidence.",
            "created_utc": created, "selection_not_committed": True,
            "publisher_invoked": False, "spec_written": False,
            "pool_mutated": False, "queue_mutated": False,
        }), encoding="utf-8")
        state_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": cycle, "current_edge": "Vol Regime",
            "current_stage": "stage1_running", "stage_status": "stage1_research_required",
            "next_action": "research_new", "research_mode": "research_new",
            "stage1_research_retry_count": 0, "failure_count": 4,
            "last_stage1_handoff": str(handoff_path),
            "stage1_rotation_prepared_utc": "2026-10-12T23:00:00Z",
        }), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.execute(
            "INSERT INTO executions (id, job_id, status, started_at, finished_at) VALUES (?, ?, ?, ?, ?)",
            ("stage1-current-search-hold-execution", "job-1", "completed",
             "2026-10-13T00:00:00Z", "2026-10-13T00:04:00Z"),
        )
        connection.commit()
        connection.close()

        changed = supervisor.reconcile_completed_stage1_without_artifact(
            state_path, {"last_stage1_execution_id": "stage1-current-search-hold-execution",
                         "stage_jobs": {"1": "job-1"}}, self.db
        )

        self.assertTrue(changed)
        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("stage1_failed_held", current["stage_status"])
        self.assertEqual("hold_no_eligible_stage1_candidate", current["next_action"])
        self.assertEqual(4, current["failure_count"])
        held = json.loads(Path(current["stage1_failure_event"]).read_text(encoding="utf-8"))
        self.assertEqual("STAGE1_RESEARCH_HOLD", held["failure_kind"])
        self.assertEqual(supervisor.sha256(hold_path), held["stage1_hold_event_sha256"])

    def test_recently_completed_stage1_waits_for_artifact_settlement_grace(self):
        cycle = "stage1-artifact-settlement-cycle"
        handoff_path = self.root / "handoffs" / "Stage1_v2.json"
        handoff_path.parent.mkdir(parents=True)
        spec_path = self.root / "specs" / f"{cycle}.stage1.spec.json"
        event_path = self.root / "events" / f"{cycle}.stage1.complete.json"
        handoff_path.write_text(json.dumps({
            "cycle_id": cycle, "research_mode": "research_new",
            "expected_spec_path": str(spec_path), "expected_event_path": str(event_path),
        }), encoding="utf-8")
        now = supervisor.utc_now()
        state_path = self.root / "stage1-artifact-settlement-state.json"
        state_path.write_text(json.dumps({
            "cycle_id": cycle, "current_stage": "stage1_running",
            "stage_status": "stage1_rotation_prepared", "research_mode": "research_new",
            "last_stage1_handoff": str(handoff_path),
            "stage1_rotation_prepared_utc": "2026-10-12T00:00:00Z",
        }), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.execute(
            "INSERT INTO executions (id, job_id, status, started_at, finished_at) VALUES (?, ?, ?, ?, ?)",
            ("stage1-recent-completion", "job-1", "completed",
             "2026-10-18T00:00:00Z", now),
        )
        connection.commit()
        connection.close()

        changed = supervisor.reconcile_completed_stage1_without_artifact(
            state_path, {"last_stage1_execution_id": "stage1-recent-completion",
                         "stage_jobs": {"1": "job-1"}}, self.db
        )

        self.assertFalse(changed)
        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("stage1_rotation_prepared", current["stage_status"])
        self.assertFalse(event_path.with_name(event_path.name.replace(".complete.json", ".failed.json")).exists())

    def test_stage1_no_artifact_ignores_execution_started_before_cycle(self):
        cycle = "kcos-20261016T130652Z-research-new"
        handoff_path = self.root / "Stage1_v2.json"
        spec_path = self.root / "specs" / f"{cycle}.stage1.spec.json"
        event_path = self.root / "events" / f"{cycle}.stage1.complete.json"
        handoff_path.parent.mkdir(parents=True, exist_ok=True)
        handoff_path.write_text(json.dumps({
            "cycle_id": cycle, "research_mode": "research_new",
            "expected_spec_path": str(spec_path), "expected_event_path": str(event_path),
        }), encoding="utf-8")
        state_path = self.root / "stale-stage1-state.json"
        state_path.write_text(json.dumps({
            "cycle_id": cycle, "current_stage": "stage1_running",
            "stage_status": "stage1_rotation_prepared",
            "last_stage1_handoff": str(handoff_path),
        }), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.execute(
            "INSERT INTO executions (id, job_id, status, started_at, finished_at) VALUES (?, ?, ?, ?, ?)",
            ("stage1-stale-previous-cycle", "job-1", "completed",
             "2026-10-16T21:04:25+11:00", "2026-10-16T21:09:32+11:00"),
        )
        connection.commit()
        connection.close()

        changed = supervisor.reconcile_completed_stage1_without_artifact(
            state_path, {"last_stage1_execution_id": "stage1-stale-previous-cycle",
                         "stage_jobs": {"1": "job-1"}}, self.db
        )

        self.assertFalse(changed)
        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("stage1_rotation_prepared", current["stage_status"])
        self.assertFalse(event_path.with_name(event_path.name.replace(".complete.json", ".failed.json")).exists())

    def test_stage1_timeout_rotates_when_no_live_execution_remains(self):
        state_path = self.root / "stage1-state.json"
        state_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": "stage1-expired-cycle",
            "current_stage": "stage1_running", "stage_status": "stage1_rotation_prepared",
            "updated_utc": "2026-01-01T00:00:00Z",
        }), encoding="utf-8")

        changed = supervisor.expire_stale_stage1(state_path, {}, self.db)

        self.assertTrue(changed)
        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("stage1_failed_held", current["stage_status"])
        self.assertEqual("rotate_stage1", current["next_action"])

    def test_stage1_execution_contract_failure_is_rotation_eligible(self):
        cycle = "stage1-contract-cycle"
        failure_path = self.root / "stage1-contract-failed.json"
        failure = {
            "schema_version": 2,
            "event_id": f"{cycle}:stage1:failed:STAGE1_EXECUTION_CONTRACT_FAILURE",
            "cycle_id": cycle, "stage": 1, "status": "failed",
            "failure_kind": "STAGE1_EXECUTION_CONTRACT_FAILURE",
            "rotation_eligible": True, "consumed": False,
        }
        failure_path.write_text(json.dumps(failure), encoding="utf-8")
        state = {
            "schema_version": 2, "cycle_id": cycle, "current_ea": "EA-contract-failed",
            "current_stage": "stage1_running", "stage_status": "stage1_failed_held",
            "next_action": "queue_next", "stage1_failure_event": str(failure_path),
            "last_stage1_event": str(failure_path),
        }
        control = {
            "schema_version": 2, "paused": False, "allowed_stages": [1, 2, 3, 4, 5],
            "dispatch_enabled": True, "hold_after_stage": 5,
        }

        result = supervisor.evaluate(state, control, self.db)

        self.assertEqual("ROTATION_READY_STAGE1", result["status"])
        self.assertEqual("rotate_stage1", result["action"])
        self.assertEqual(failure["event_id"], result["event_id"])
        self.assertEqual(str(failure_path.resolve()), result["event_path"])
        self.assertIn("eligible for candidate rotation", result["detail"])

    def test_stage1_provider_quota_failure_holds_without_candidate_rotation(self):
        state_path = self.root / "stage1-quota-state.json"
        state_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": "stage1-quota-cycle",
            "current_stage": "stage1_running", "stage_status": "stage1_rotation_prepared",
            "current_ea": "candidate-a", "updated_utc": "2026-01-01T00:00:00Z",
        }), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        try:
            connection.execute("ALTER TABLE executions ADD COLUMN error TEXT")
            connection.execute(
                "INSERT INTO executions (id, job_id, status, started_at, finished_at, error) VALUES (?, ?, ?, ?, ?, ?)",
                ("stage1-quota-execution", "job-1", "failed", "2026-10-13T00:29:53Z",
                 "2026-10-13T00:29:54Z",
                 "RuntimeError: HTTP 429: Token Plan usage limit reached. (2056)"),
            )
            connection.commit()
        finally:
            connection.close()

        changed = supervisor.expire_stale_stage1(
            state_path, {"last_stage1_execution_id": "stage1-quota-execution"}, self.db
        )

        self.assertTrue(changed)
        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("stage1_failed_held", current["stage_status"])
        self.assertEqual("hold_provider_quota", current["next_action"])
        failure = json.loads(Path(current["stage1_failure_event"]).read_text(encoding="utf-8"))
        self.assertEqual("STAGE1_PROVIDER_QUOTA_FAILURE", failure["failure_kind"])
        self.assertFalse(failure["rotation_eligible"])

        current["stage_status"] = "stage1_provider_quota_recovery_ready"
        current["next_action"] = "dispatch_stage1"
        state_path.write_text(json.dumps(current), encoding="utf-8")
        changed_again = supervisor.reconcile_stage1_provider_quota_failure(
            state_path, {"last_stage1_execution_id": "stage1-retry-failed-execution"}, self.db
        )
        self.assertFalse(changed_again)
        self.assertEqual("stage1_provider_quota_recovery_ready", json.loads(state_path.read_text(encoding="utf-8"))["stage_status"])
        result = supervisor.evaluate(current, {
            "schema_version": 2, "paused": False, "allowed_stages": [1], "dispatch_enabled": True,
        }, self.db)
        self.assertEqual("COMPLETE_HELD_STAGE1_PROVIDER_QUOTA", result["status"])
        self.assertEqual("none", result["action"])

    def test_stage1_provider_quota_recovery_requires_hash_bound_probe_ticket(self):
        failure_path = self.root / "stage1-quota-failure.json"
        failure_path.write_text(json.dumps({
            "schema_version": 2, "event_id": "quota-cycle:stage1:provider_quota_failure",
            "cycle_id": "quota-cycle", "stage": 1, "status": "failed",
            "failure_kind": "STAGE1_PROVIDER_QUOTA_FAILURE", "rotation_eligible": False,
            "hermes_execution_id": "failed-execution",
        }), encoding="utf-8")
        recovery_path = self.root / "stage1-quota-recovery.json"
        recovery_path.write_text(json.dumps({
            "schema_version": 1, "record_kind": "kcos_stage1_provider_quota_recovery",
            "event_id": "quota-cycle:stage1:provider-quota-retry:1", "cycle_id": "quota-cycle",
            "status": "provider_recovered_single_retry", "probe_result": "OK",
            "probe_utc": "2026-10-13T00:47:23Z", "retry_ordinal": 1,
            "failed_execution_id": "failed-execution",
            "failure_event_sha256": supervisor.sha256(failure_path),
        }), encoding="utf-8")
        state = {
            "schema_version": 2, "cycle_id": "quota-cycle", "current_stage": "stage1_running",
            "stage_status": "stage1_provider_quota_recovery_ready",
            "stage1_failure_event": str(failure_path), "stage1_provider_quota_retry_count": 1,
            "stage1_provider_quota_recovery": {
                "path": str(recovery_path), "sha256": supervisor.sha256(recovery_path),
            },
        }
        control = {"schema_version": 2, "paused": False, "allowed_stages": [1],
                   "dispatch_enabled": True}

        result = supervisor.evaluate(state, control, self.db)
        self.assertEqual("DISPATCH_READY_STAGE1", result["status"])
        self.assertEqual("dispatch_stage1", result["action"])
        second_ticket_path = self.root / "stage1-quota-recovery-2.json"
        second_ticket = {
            "schema_version": 1, "record_kind": "kcos_stage1_provider_quota_recovery",
            "event_id": "quota-cycle:stage1:provider-quota-retry:2", "cycle_id": "quota-cycle",
            "status": "provider_recovered_single_retry", "probe_result": "OK",
            "probe_utc": "2026-10-13T01:00:00Z", "retry_ordinal": 2,
            "failed_execution_id": "failed-execution",
            "failure_event_sha256": supervisor.sha256(failure_path),
        }
        second_ticket_path.write_text(json.dumps(second_ticket), encoding="utf-8")
        second_state = dict(state, stage1_provider_quota_retry_count=2, stage1_provider_quota_recovery={
            "path": str(second_ticket_path), "sha256": supervisor.sha256(second_ticket_path),
        })
        second = supervisor.evaluate(second_state, control, self.db)
        self.assertEqual("DISPATCH_READY_STAGE1", second["status"])
        self.assertEqual("dispatch_stage1", second["action"])
        self.assertEqual("quota-cycle:stage1:provider-quota-retry:2", second["event_id"])
        invalid = supervisor.evaluate(dict(second_state, stage1_provider_quota_retry_count=3), control, self.db)
        self.assertEqual("COMPLETE_HELD_STAGE1_PROVIDER_QUOTA", invalid["status"])
        self.assertEqual("none", invalid["action"])

    def test_stage1_terminal_quota_failure_reconciles_from_recovery_ready_state(self):
        state_path = self.root / "stage1-retry-failed-state.json"
        state_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": "stage1-retry-failed-cycle",
            "current_stage": "stage1_running", "stage_status": "stage1_provider_quota_recovery_ready",
            "current_ea": "candidate-a", "next_action": "dispatch_stage1",
            "stage1_provider_quota_retry_count": 1,
        }), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        try:
            connection.execute("ALTER TABLE executions ADD COLUMN error TEXT")
            connection.execute(
                "INSERT INTO executions (id, job_id, status, started_at, finished_at, error) VALUES (?, ?, ?, ?, ?, ?)",
                ("stage1-retry-failed-execution", "job-1", "failed", "2026-10-13T00:53:55Z",
                 "2026-10-13T00:55:41Z",
                 "RuntimeError: HTTP 429: Token Plan usage limit reached. (2056)"),
            )
            connection.commit()
        finally:
            connection.close()

        changed = supervisor.reconcile_stage1_provider_quota_failure(
            state_path, {"last_stage1_execution_id": "stage1-retry-failed-execution"}, self.db
        )

        self.assertTrue(changed)
        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("stage1_failed_held", current["stage_status"])
        self.assertEqual("hold_provider_quota", current["next_action"])
        failure = json.loads(Path(current["stage1_failure_event"]).read_text(encoding="utf-8"))
        self.assertEqual("STAGE1_PROVIDER_QUOTA_FAILURE", failure["failure_kind"])
        self.assertEqual("stage1-retry-failed-execution", failure["hermes_execution_id"])
        self.assertFalse(failure["rotation_eligible"])

        current["stage_status"] = "stage1_provider_quota_recovery_ready"
        current["next_action"] = "dispatch_stage1"
        state_path.write_text(json.dumps(current), encoding="utf-8")
        changed_again = supervisor.reconcile_stage1_provider_quota_failure(
            state_path, {"last_stage1_execution_id": "stage1-retry-failed-execution"}, self.db
        )
        self.assertFalse(changed_again)
        self.assertEqual("stage1_provider_quota_recovery_ready", json.loads(state_path.read_text(encoding="utf-8"))["stage_status"])

    def test_cooldown_verifier_false_negative_gets_one_authenticated_retry(self):
        cycle = "cooldown-retry-cycle"
        source = self.root / "candidate.mq5"
        source.write_text(
            "input int InpMinBarsBetweenTrades = 0; void OnTick(){ if(InpMinBarsBetweenTrades > 0) { if(bars < InpMinBarsBetweenTrades) return; } }\n",
            encoding="utf-8")
        ex5 = self.root / "candidate.ex5"; ex5.write_bytes(b"fresh-ex5")
        artifact = self.root / "stage1.spec.json"; artifact.write_text('{"cycle":"ok"}\n', encoding="utf-8")
        stage1_event = self.root / "stage1.complete.json"
        stage1_event.write_text(json.dumps({
            "cycle_id": cycle, "status": "complete", "artifact_sha256": supervisor.sha256(artifact),
        }), encoding="utf-8")
        compile_path = self.root / "compile.json"
        compile_path.write_text(json.dumps({
            "passed": True, "fresh_ex5": True, "ex5_sha256": supervisor.sha256(ex5),
        }), encoding="utf-8")
        verification_path = self.root / "verification.json"
        verification_path.write_text(json.dumps({
            "cycle_id": cycle, "stage": 2, "passed": False,
            "checks": {"cooldown_zero_disables_entry": False, "compile_clean": True},
            "source": str(source.resolve()), "source_sha256": supervisor.sha256(source),
            "ex5": str(ex5.resolve()), "ex5_sha256": supervisor.sha256(ex5),
            "stage1_spec_sha256": supervisor.sha256(artifact),
        }), encoding="utf-8")
        failure_path = self.root / "stage2.failed.json"
        failure_path.write_text(json.dumps({
            "cycle_id": cycle, "stage": 2, "status": "failed",
            "failure_kind": "STAGE2_VERIFICATION_FAILURE", "rotation_eligible": False,
            "failed_checks": ["cooldown_zero_disables_entry"],
            "verification": str(verification_path.resolve()), "compile_attempt": str(compile_path.resolve()),
        }), encoding="utf-8")
        state_path = self.root / "cycle-state.json"
        state = {
            "schema_version": 2, "cycle_id": cycle, "current_stage": "stage2_pending",
            "stage_status": "stage2_repair_required", "stage2_failure_event": str(failure_path.resolve()),
            "candidate_source": str(source.resolve()), "last_stage1_artifact": str(artifact.resolve()),
            "last_stage1_event": str(stage1_event.resolve()),
            "run_authorization": {"schema_version": 1, "cycle_id": cycle,
                "scope": "current_cycle_stages_1_to_5", "allowed_stages": [1, 2, 3, 4, 5],
                "hold_after_stage": 5, "authorized_by": "user_confirmed",
                "reason": "Continue this complete five stage research cycle.",
                "authorized_utc": "2026-10-01T00:00:00Z"},
        }
        state_path.write_text(json.dumps(state), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.execute("INSERT INTO executions VALUES (?, ?, ?, ?, ?)",
                           ("stage2-failed", "job-2", "failed", "2026-10-01T00:00:00Z", "2026-10-01T00:01:00Z"))
        connection.commit(); connection.close()
        control = {"schema_version": 2, "paused": False, "hold_after_stage": 5,
                   "dispatch_enabled": True, "allowed_stages": [1, 2, 3, 4, 5],
                   "stage_jobs": {"2": "job-2"}, "last_stage2_execution_id": "stage2-failed"}

        outcome = supervisor.prepare_stage2_verifier_retry(state_path, control, self.db)
        self.assertEqual("retry_prepared", outcome)
        current = json.loads(state_path.read_text(encoding="utf-8"))
        valid, _, retry_event, retry_path = supervisor.validate_stage2_verifier_retry(current)
        self.assertTrue(valid)
        self.assertTrue(retry_path.is_file())
        result = supervisor.evaluate(current, control, self.db)
        self.assertEqual("STAGE2_VERIFIER_RETRY_READY", result["status"])
        self.assertEqual("dispatch_stage2", result["action"])
        self.assertEqual(retry_event["event_id"], result["event_id"])
        self.assertEqual("not_eligible", supervisor.prepare_stage2_verifier_retry(state_path, control, self.db))

    def test_stale_retry_metadata_is_retired_at_cycle_boundary_without_deleting_evidence(self):
        old_ticket = self.root / "old-ticket.json"
        old_ticket.write_text(json.dumps({"cycle_id": "cycle-old"}), encoding="utf-8")
        state_path = self.root / "cycle-state.json"
        state_path.write_text(json.dumps({
            "cycle_id": "cycle-current", "stage2_verifier_retry_count": 1,
            "stage2_verifier_retry_ticket": str(old_ticket),
            "stage2_verifier_retry_event": "old-event.json",
            "stage2_verifier_retry_dispatch_claim_sha256": "old-claim",
        }), encoding="utf-8")

        self.assertTrue(supervisor.clear_stale_stage2_retry_metadata(state_path))
        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(0, current["stage2_verifier_retry_count"])
        self.assertNotIn("stage2_verifier_retry_ticket", current)
        self.assertNotIn("stage2_verifier_retry_event", current)
        self.assertEqual("cycle-old", current["stale_stage2_retry_metadata_audit"][0]["stale_cycle_id"])
        self.assertTrue(old_ticket.is_file())

    def test_nonterminal_stage2_executor_failure_stays_on_candidate_for_repair(self):
        failure_path = self.root / "stage2.failed.json"
        failure_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": "cycle-1", "stage": 2,
            "status": "failed", "failure_kind": "STAGE2_EXECUTOR_FAILURE",
            "rotation_eligible": False,
        }), encoding="utf-8")
        state = {
            "schema_version": 2, "cycle_id": "cycle-1", "current_stage": "stage2_pending",
            "stage_status": "stage2_repair_required", "stage2_failure_event": str(failure_path),
        }
        self.control.update({"allowed_stages": [1, 2, 3], "dispatch_enabled": True,
                             "hold_after_stage": 3})

        result = supervisor.evaluate(state, self.control, self.db)

        self.assertEqual("STAGE2_REPAIR_REQUIRED_HELD", result["status"])
        self.assertEqual("none", result["action"])
        self.assertEqual("STAGE2_EXECUTOR_FAILURE", result["failure_kind"])

    def test_authenticated_zero_trade_stage2_failure_retries_until_four_proposals(self):
        cycle = "cycle-zero-trade-budget"
        failure_path = self.root / "stage2.failed.json"
        failure_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": cycle, "stage": 2,
            "status": "failed", "failure_kind": "STAGE2_ZERO_TRADE_REPAIR_FAILURE",
            "rotation_eligible": False,
            "stage2_zero_trade_repair": {"episode_id": "episode-1", "proposal_count": 2},
        }), encoding="utf-8")
        state = {"schema_version": 2, "cycle_id": cycle, "current_ea": "EA",
                 "candidate_ea": "EA", "current_stage": "stage2_pending",
                 "stage_status": "stage2_repair_required",
                 "stage2_failure_event": str(failure_path),
                 "next_action": "repair_stage2_failure"}
        control = {"schema_version": 2, "paused": False, "dispatch_enabled": True,
                   "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
                   "stage_jobs": {"2": "job-2"}}
        budget = {"status": "verified", "episode_id": "episode-1", "used": 2,
                  "limit": 4, "remaining": 2}
        with mock.patch.object(supervisor, "authenticated_stage3_zero_trade_repair",
                               return_value={"decision_sha256": "A" * 64}), \
                mock.patch.object(supervisor, "stage2_proposal_budget_status", return_value=budget):
            result = supervisor.evaluate(state, control, self.db)
        self.assertEqual("STAGE2_ZERO_TRADE_RETRY_READY", result["status"])
        self.assertEqual("dispatch_stage2", result["action"])
        self.assertIn(":2:", result["event_id"])

    def test_authenticated_zero_trade_budget_exhaustion_without_four_bundles_holds_no_rotation(self):
        cycle = "cycle-zero-trade-diagnostic-budget-exhausted"
        failure_path = self.root / "stage2.failed.json"
        failure_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": cycle, "stage": 2,
            "status": "failed", "failure_kind": "STAGE2_ZERO_TRADE_REPAIR_FAILURE",
            "rotation_eligible": False,
            "stage2_zero_trade_repair": {"episode_id": "episode-1", "proposal_count": 4},
        }), encoding="utf-8")
        state = {"schema_version": 2, "cycle_id": cycle, "current_ea": "EA",
                 "candidate_ea": "EA", "current_stage": "stage2_pending",
                 "stage_status": "stage2_repair_required",
                 "stage2_failure_event": str(failure_path),
                 "next_action": "repair_stage2_failure"}
        control = {"schema_version": 2, "paused": False, "dispatch_enabled": True,
                   "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
                   "stage_jobs": {"2": "job-2"}}
        budget = {"status": "verified", "episode_id": "episode-1", "used": 4,
                  "limit": 4, "remaining": 0}
        with mock.patch.object(supervisor, "authenticated_stage3_zero_trade_repair",
                               return_value={"decision_sha256": "A" * 64}), \
                mock.patch.object(supervisor, "stage2_proposal_budget_status", return_value=budget):
            result = supervisor.evaluate(state, control, self.db)
        self.assertEqual("STAGE2_ZERO_TRADE_BUDGET_EXHAUSTED_HELD", result["status"])
        self.assertEqual("none", result["action"])
        self.assertFalse(result.get("rotation_eligible", False))
        self.assertIn("four distinct no-change transaction bundles are not present", result["detail"])

    def test_legacy_zero_trade_no_change_failure_authenticates_from_transaction_bundle(self):
        cycle, episode, ea = "cycle-zero-trade-legacy", "episode-legacy", "EA"
        root = self.root / "runtime"
        attempt = (root / "state" / "diagnostics" / "stage2_repair_transactions" /
                   episode / "attempt-1")
        attempt.mkdir(parents=True)
        source = root / "state" / "candidates" / "EA.mq5"
        source.parent.mkdir(parents=True)
        source.write_text("input int InpPeriod=10;\n", encoding="utf-8")
        source_hash = supervisor.sha256(source)
        report = {"cycle_id": cycle, "ea": ea, "baseline_source_sha256": source_hash,
                  "attempts": [{"attempt": 0, "baseline": True,
                                "before_sha256": source_hash, "verification_passed": True},
                               {"attempt": 1, "ai_application": {"status": "PATCH_REJECTED"}}]}
        report_path = attempt / "repair-loop-report.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        outcome_path = attempt / "outcome.json"
        outcome_path.write_text(json.dumps({"episode_id": episode,
                                            "outcome": "REPAIR_REQUIRED_NO_CHANGE"}), encoding="utf-8")
        (attempt / "transaction-journal.json").write_text(json.dumps({"status": "ROLLED_BACK"}), encoding="utf-8")
        canonical_report = root / "state" / "reports" / f"{cycle}.stage2.repair-loop.json"
        canonical_report.parent.mkdir(parents=True)
        canonical_report.write_text(json.dumps(report), encoding="utf-8")
        report_line = json.dumps({"attempts": 2, "report": str(canonical_report), "passed": True})
        outcome_line = json.dumps({"outcome": "REPAIR_REQUIRED_NO_CHANGE", "episode_id": episode,
                                   "attempt_dir": str(attempt), "returncode": 0})
        error = report_line + "\n" + outcome_line
        failure_path = root / "state" / "events" / f"{cycle}.stage2.failed.json"
        failure_path.parent.mkdir(parents=True)
        failure = {"cycle_id": cycle, "stage": 2, "status": "failed",
                   "failure_kind": "STAGE2_EXECUTOR_FAILURE", "rotation_eligible": False,
                   "error": error, "error_sha256": hashlib.sha256(error.encode()).hexdigest().upper()}
        failure_path.write_text(json.dumps(failure), encoding="utf-8")
        state = {"cycle_id": cycle, "candidate_ea": ea, "current_ea": ea,
                 "candidate_source": str(source), "stage2_failure_event": str(failure_path),
                 "stage2_repair_reason": report_line + "\n" + outcome_line}
        budget = {"status": "verified", "episode_id": episode, "used": 1,
                  "limit": 4, "remaining": 3}
        zero_trade = {"decision_sha256": "A" * 64, "verification_sha256": "B" * 64}
        with mock.patch.object(supervisor, "ROOT", root), \
                mock.patch.object(supervisor, "authenticated_stage3_zero_trade_repair",
                                  return_value=zero_trade), \
                mock.patch.object(supervisor, "stage2_proposal_budget_status", return_value=budget):
            identity = supervisor.authenticated_stage2_zero_trade_retry_identity(
                state, failure_path, failure)
        self.assertIsNotNone(identity)
        self.assertEqual(episode, identity["episode_id"])
        self.assertEqual(1, identity["budget"]["used"])

    def test_authenticated_duplicate_unsupported_candidate_rotates_through_supervisor(self):
        cycle = "cycle-duplicate-unsupported"
        failure_path = self.root / "stage2.failed.json"
        failure_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": cycle, "event_id": f"{cycle}:stage2:failed",
            "stage": 2, "status": "failed", "failure_kind": "STAGE2_EXECUTOR_FAILURE",
            "rotation_eligible": False, "error": "unsupported implementation; refusing to overwrite",
        }), encoding="utf-8")
        state = {
            "schema_version": 2, "cycle_id": cycle, "current_stage": "stage2_pending",
            "stage_status": "stage2_repair_required", "next_action": "repair_stage2_failure",
            "stage2_failure_event": str(failure_path),
            "run_authorization": {"schema_version": 1, "scope": "current_cycle_stages_1_to_5",
                                  "cycle_id": cycle, "allowed_stages": [1, 2, 3, 4, 5],
                                  "hold_after_stage": 5, "authorized_by": "user_confirmed"},
        }
        control = {"schema_version": 2, "paused": False, "dispatch_enabled": True,
                   "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5}
        identity = {"identity_sha256": "A" * 64, "candidate_ea": "EA"}
        with mock.patch.object(supervisor.rotation, "active_stage3_runtime", return_value=None), \
                mock.patch.object(supervisor.rotation, "authenticated_duplicate_unsupported_candidate",
                                  return_value=identity), \
                mock.patch.object(supervisor, "authorizes_stages_1_to_5", return_value=True):
            result = supervisor.evaluate(state, control, self.db)

        self.assertEqual("ROTATION_READY_STAGE1", result["status"])
        self.assertEqual("rotate_stage1", result["action"])
        self.assertEqual(identity, result["dispatch_identity"])
        self.assertEqual(str(failure_path.resolve()), result["event_path"])

    def test_stale_consumed_stage2_recovery_pointer_does_not_mask_current_rotation(self):
        old_cycle, current_cycle = "cycle-old", "cycle-current"
        stale_event = self.root / "old.stage2.recovery.json"
        stale_ticket = self.root / "old.stage2.ticket.json"
        stale_event.write_text(json.dumps({
            "schema_version": 1, "cycle_id": old_cycle, "stage": 2,
            "event_id": "cycle-old:stage2:source-defect:dispatch:2",
            "status": "repair_requested", "consumed": True,
            "consumed_action": "dispatch_stage2", "rotation_eligible": False,
        }), encoding="utf-8")
        stale_ticket.write_text(json.dumps({"cycle_id": old_cycle}), encoding="utf-8")
        failure_path = self.root / "current.stage2.failed.json"
        failure_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": current_cycle, "stage": 2,
            "status": "failed", "failure_kind": "MODEL_SOURCE_PROPOSAL_BUDGET_EXHAUSTED",
            "rotation_eligible": True,
        }), encoding="utf-8")
        state = {
            "schema_version": 2, "cycle_id": current_cycle, "current_stage": "stage2_pending",
            "stage_status": "stage2_repair_required", "stage2_failure_event": str(failure_path),
            "stage2_source_defect_event": str(stale_event),
            "stage2_source_defect_event_id": "cycle-old:stage2:source-defect:dispatch:2",
            "stage2_source_defect_ticket": str(stale_ticket),
        }
        self.control.update({"allowed_stages": [1, 2, 3], "dispatch_enabled": True,
                             "hold_after_stage": 3})

        with mock.patch.object(supervisor, "stage2_recovery_execution_rows") as recovery_rows:
            result = supervisor.evaluate(state, self.control, self.db)

        self.assertEqual("ROTATION_READY_STAGE1", result["status"])
        self.assertEqual("rotate_stage1", result["action"])
        self.assertEqual(str(failure_path.resolve()), result["event_path"])
        recovery_rows.assert_not_called()

    def test_source_generation_exhaustion_authenticates_all_executor_source_failure_kinds(self):
        cycle = "cycle-source-generation"
        ea = "InformationDrivenTripleBarrier_VolRegimeGate_v1"
        state_dir = self.root / "source-generation-state"
        events = state_dir / "events"
        specs = state_dir / "specs"
        generation = state_dir / "candidates" / ".generation" / cycle / "attempt"
        for folder in (events, specs, generation):
            folder.mkdir(parents=True, exist_ok=True)

        spec_path = specs / f"{cycle}.stage1.spec.json"
        spec_path.write_text(json.dumps({"cycle_id": cycle, "stage": 1,
                                         "ea": ea, "candidate_ea": ea}), encoding="utf-8")
        stage1_event_path = events / f"{cycle}.stage1.complete.json"
        stage1_event = {
            "cycle_id": cycle, "stage": 1, "status": "complete", "ea": ea,
            "artifact": str(spec_path.resolve()),
            "artifact_sha256": supervisor.sha256(spec_path),
            "consumed": True, "consumed_action": "dispatch_stage2",
        }
        stage1_event_path.write_text(json.dumps(stage1_event), encoding="utf-8")
        normalized_stage1_event_hash = hashlib.sha256(json.dumps(
            {key: value for key, value in stage1_event.items()
             if key not in {"consumed", "consumed_utc", "consumed_action"}},
            sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")).hexdigest().upper()

        proposal_kinds = ["NORMALIZATION_NOT_RECOGNIZED", "RESPONSE_SCHEMA",
                          "CANONICAL_HOOK", "IMPLEMENTATION_REFUSAL"]
        proposal_sources = [f"int AttemptSource{i}() {{ return {i}; }}" for i in range(1, 5)]
        proposals = [{
            "failure_kind": kind,
            "failed_check": f"source rejection {i}",
            "repair_scope": "source_proposal_only",
            "failure_fingerprint": hashlib.sha256(f"fingerprint-{i}".encode()).hexdigest().upper(),
            "same_failure_count": 1,
            "proposed_source_sha256": hashlib.sha256(proposal_sources[i - 1].encode()).hexdigest().upper(),
        } for i, kind in enumerate(proposal_kinds, 1)]
        normalization_diagnosis = {
            "code": "NORMALIZATION_NOT_RECOGNIZED", "side": "BUY",
            "volume_variable": "volume", "source_line_start": 10, "source_line_end": 20,
            "quantity_to_margin_sha256": hashlib.sha256(b"quantity-to-margin").hexdigest().upper(),
            "helper_authenticated": False, "normalizer_call_count": 0,
            "matching_margin_guard_found": False, "span_kind": "quantity_to_order_fallback",
        }
        proposals[0]["normalization_diagnosis"] = normalization_diagnosis
        proposals[0]["failed_check"] = "NORMALIZATION_NOT_RECOGNIZED " + json.dumps(
            normalization_diagnosis, sort_keys=True, separators=(",", ":"))
        proposals[0]["failure_fingerprint"] = hashlib.sha256(
            ("NORMALIZATION_NOT_RECOGNIZED\n" + normalization_diagnosis["quantity_to_margin_sha256"])
            .encode("utf-8")).hexdigest().upper()
        hold_path = generation / "hold.json"
        for attempt_number, proposal in enumerate(proposals, 1):
            proposal_label = "proposal" if attempt_number == 1 else f"proposal-retry-{attempt_number}"
            raw_path = generation / f"{proposal_label}.stdout.txt"
            (generation / f"{proposal_label}.prompt.txt").write_text(
                f"candidate-local prompt for attempt {attempt_number}", encoding="utf-8")
            raw_path.write_text(f"```mql5\n{proposal_sources[attempt_number - 1]}\n```", encoding="utf-8")
            (generation / f"proposal-rejection-{attempt_number}.json").write_text(json.dumps({
                "attempt": attempt_number, "limit": 4, "reason": proposal["failed_check"],
                "diagnosis": proposal, "raw_proposal": str(raw_path.resolve()),
            }), encoding="utf-8")
        hold_path.write_text(json.dumps({
            "cycle_id": cycle, "ea": ea, "status": "MODEL_SOURCE_HOLD", "phase": "proposal",
            "proposal_limit": 4, "proposal_attempts": 4, "stop_reason": "PROPOSAL_BUDGET_EXHAUSTED",
            "source_spec_sha256": supervisor.sha256(spec_path),
            "stage1_event_sha256": normalized_stage1_event_hash,
            "proposal_diagnoses": proposals,
        }), encoding="utf-8")
        error = "authenticated exhausted source proposal episode"
        failure_path = events / f"{cycle}.stage2.failed.json"
        failure_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": cycle, "event_id": f"{cycle}:stage2:failed",
            "stage": 2, "status": "failed", "failure_kind": "MODEL_SOURCE_PROPOSAL_BUDGET_EXHAUSTED",
            "rotation_eligible": True, "recovery_action": "rotate_candidate",
            "source": None, "source_sha256": None, "source_identity_available": False,
            "ea": ea, "candidate_ea": ea,
            "stage1_spec": str(spec_path.resolve()), "stage1_spec_sha256": supervisor.sha256(spec_path),
            "source_diagnosis": {"artifact": str(hold_path.resolve()),
                                 "sha256": supervisor.sha256(hold_path), "diagnoses": proposals},
            "error": error,
            "error_sha256": hashlib.sha256(error.encode("utf-8")).hexdigest().upper(),
        }), encoding="utf-8")
        state = {"cycle_id": cycle, "current_ea": ea,
                 "last_stage1_event": str(stage1_event_path.resolve())}

        identity = supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)
        self.assertEqual([item["proposed_source_sha256"] for item in proposals],
                         identity["proposal_source_sha256"])

        def rewrite_episode(updated_proposals):
            hold = json.loads(hold_path.read_text(encoding="utf-8"))
            hold["proposal_diagnoses"] = updated_proposals
            hold_path.write_text(json.dumps(hold), encoding="utf-8")
            failure = json.loads(failure_path.read_text(encoding="utf-8"))
            failure["source_diagnosis"]["sha256"] = supervisor.sha256(hold_path)
            failure["source_diagnosis"]["diagnoses"] = updated_proposals
            failure_path.write_text(json.dumps(failure), encoding="utf-8")
            for attempt_number, proposal in enumerate(updated_proposals, 1):
                rejection_path = generation / f"proposal-rejection-{attempt_number}.json"
                rejection = json.loads(rejection_path.read_text(encoding="utf-8"))
                rejection["reason"] = proposal["failed_check"]
                rejection["diagnosis"] = proposal
                rejection_path.write_text(json.dumps(rejection), encoding="utf-8")

        # Missing and malformed normalization evidence must not authorize rotation.
        missing_payload = json.loads(json.dumps(proposals))
        missing_payload[0].pop("normalization_diagnosis")
        rewrite_episode(missing_payload)
        with self.assertRaises(ValueError):
            supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)

        invalid_payload = json.loads(json.dumps(proposals))
        invalid_payload[0]["normalization_diagnosis"]["source_line_start"] = True
        rewrite_episode(invalid_payload)
        with self.assertRaises(ValueError):
            supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)

        repeated_normalization = json.loads(json.dumps(proposals))
        repeated_normalization[1]["failure_kind"] = "NORMALIZATION_NOT_RECOGNIZED"
        repeated_normalization[1]["normalization_diagnosis"] = json.loads(
            json.dumps(normalization_diagnosis))
        repeated_normalization[1]["failed_check"] = proposals[0]["failed_check"]
        repeated_normalization[1]["failure_fingerprint"] = proposals[0]["failure_fingerprint"]
        repeated_normalization[1]["same_failure_count"] = 2
        rewrite_episode(repeated_normalization)
        with self.assertRaises(ValueError):
            supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)

        # The normalization failed_check is independently bound to its parsed
        # diagnosis, even when the hold and failure event hashes are refreshed.
        changed_failed_check = json.loads(json.dumps(proposals))
        failed_check_prefix, failed_check_json = changed_failed_check[0]["failed_check"].split(" ", 1)
        changed_failed_check[0]["failed_check"] = failed_check_prefix + " " + failed_check_json.replace(
            '"side":"BUY"', '"side":"SELL"')
        rewrite_episode(changed_failed_check)
        with self.assertRaisesRegex(ValueError, "failed_check differs from its diagnosis"):
            supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)

        # The normalization fingerprint is also independently recomputed from
        # the authenticated diagnosis; refreshed artifact hashes cannot bless
        # a changed fingerprint.
        changed_fingerprint = json.loads(json.dumps(proposals))
        changed_fingerprint[0]["failure_fingerprint"] = hashlib.sha256(
            b"tampered-normalization-fingerprint").hexdigest().upper()
        rewrite_episode(changed_fingerprint)
        with self.assertRaisesRegex(ValueError, "fingerprint does not match its diagnosis"):
            supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)

        rewrite_episode(proposals)

        # A source-bearing response with a trailing unsupported explanation is
        # rejected by the generator, but its source body still proves a distinct
        # candidate-local attempt for the exhausted four-call budget.
        malformed_response = json.loads(json.dumps(proposals))
        malformed_response[2].update({
            "failure_kind": "RESPONSE_FORMAT",
            "failed_check": "STAGE2_MODEL_SOURCE_HOLD: model response must contain one unambiguous JSON object",
            "repair_action": "Return exactly one fenced mql5 code block with the complete source and no metadata.",
            "failure_fingerprint": hashlib.sha256(b"response-format\nmalformed-source-envelope").hexdigest().upper(),
            "same_failure_count": 1,
            "proposed_source_sha256": None,
        })
        proposal3_path = generation / "proposal-retry-3.stdout.txt"
        proposal3_path.write_text(
            f"```mql5\n{proposal_sources[2]}\n```\n\n**Unsupported:** cannot satisfy the unchanged contract.",
            encoding="utf-8")
        rewrite_episode(malformed_response)
        identity = supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)
        self.assertEqual(hashlib.sha256(proposal_sources[2].encode()).hexdigest().upper(),
                         identity["proposal_source_sha256"][2])

        # The special response-format case cannot authenticate an ambiguous
        # response containing multiple candidate source bodies.
        proposal3_path.write_text(
            f"```mql5\n{proposal_sources[2]}\n```\n\n```mql5\n{proposal_sources[1]}\n```", encoding="utf-8")
        with self.assertRaises(ValueError):
            supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)
        proposal3_path.write_text(
            f"```mql5\n{proposal_sources[2]}\n```\n\n**Unsupported:** cannot satisfy the unchanged contract.",
            encoding="utf-8")

        # A valid JSON refusal is also one consumed generator attempt. It has
        # no source to publish, but its exact response and rejection artifacts
        # still prove the call consumed a slot from the fixed four-attempt cap.
        refusal_response = json.loads(json.dumps(proposals))
        refusal_response[1].update({
            "failure_kind": "IMPLEMENTATION_REFUSAL",
            "failed_check": "STAGE2_MODEL_SOURCE_HOLD: generator refused: contract is unsupported",
            "repair_action": "Implement the unchanged contract or identify the precise unsupported requirement.",
            "failure_fingerprint": hashlib.sha256(b"implementation-refusal").hexdigest().upper(),
            "same_failure_count": 1,
            "proposed_source_sha256": None,
        })
        proposal2_path = generation / "proposal-retry-2.stdout.txt"
        proposal2_path.write_text(json.dumps({"unsupported": "contract is unsupported"}), encoding="utf-8")
        rewrite_episode(refusal_response)
        identity = supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)
        self.assertIsNone(identity["proposal_source_sha256"][1])
        self.assertEqual(hashlib.sha256(proposal2_path.read_bytes()).hexdigest().upper(),
                         identity["proposal_response_sha256"][1])
        proposal2_path.write_text(json.dumps({"unsupported": "contract is unsupported", "source": "x"}),
                                  encoding="utf-8")
        with self.assertRaises(ValueError):
            supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)

        # Restore the ordinary four-source fixture for the remaining tamper tests.
        rewrite_episode(proposals)
        proposal2_path.write_text(f"```mql5\n{proposal_sources[1]}\n```", encoding="utf-8")
        proposal3_path.write_text(f"```mql5\n{proposal_sources[2]}\n```", encoding="utf-8")

        mutated_event = json.loads(stage1_event_path.read_text(encoding="utf-8"))
        mutated_event["unbound_metadata"] = "tampered"
        stage1_event_path.write_text(json.dumps(mutated_event), encoding="utf-8")
        with self.assertRaises(ValueError):
            supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)

        # Restore valid lineage before checking that unknown failure kinds fail closed.
        stage1_event_path.write_text(json.dumps(stage1_event), encoding="utf-8")

        proposals[1]["failure_kind"] = "UNRECOGNIZED_FAILURE"
        hold_path.write_text(json.dumps({
            "cycle_id": cycle, "ea": ea, "status": "MODEL_SOURCE_HOLD", "phase": "proposal",
            "proposal_limit": 4, "proposal_attempts": 4, "stop_reason": "PROPOSAL_BUDGET_EXHAUSTED",
            "source_spec_sha256": supervisor.sha256(spec_path),
            "stage1_event_sha256": normalized_stage1_event_hash,
            "proposal_diagnoses": proposals,
        }), encoding="utf-8")
        failure = json.loads(failure_path.read_text(encoding="utf-8"))
        failure["source_diagnosis"]["sha256"] = supervisor.sha256(hold_path)
        failure["source_diagnosis"]["diagnoses"] = proposals
        failure_path.write_text(json.dumps(failure), encoding="utf-8")
        with self.assertRaises(ValueError):
            supervisor.stage2_source_generation_exhaustion_identity(state, failure_path)

    def _stage45_exhaustion_fixture(self):
        cycle, ea = "stage45-cycle", "EA-RANGE"
        state_dir = self.root / "stage45-state"
        state = {
            "schema_version": 2, "cycle_id": cycle, "current_ea": ea,
            "current_stage": "stage4_complete_held", "stage_status": "narrow_opt_failed",
            "stage45_run_number": 3, "narrow_run_number": 3,
            "last_stage45_event": str((state_dir / "events" / f"{cycle}.stage45.run3.complete.json").resolve()),
        }
        state_dir.mkdir()
        for run in (1, 2, 3):
            decision_path = state_dir / "decisions" / f"{cycle}.stage45.run{run}.decision.json"
            verification_path = state_dir / "verification" / f"{cycle}.stage45.run{run}.verification.json"
            event_path = state_dir / "events" / f"{cycle}.stage45.run{run}.complete.json"
            runtime_path = state_dir / "stage45_v2" / f"{cycle}.stage45.run{run}.runtime.json"
            complete_path = state_dir / "stage45_v2" / f"{cycle}.stage45.H1.run{run}.complete.json"
            csv_path = state_dir / "stage45_v2" / f"{cycle}.stage45.H1.run{run}.csv"
            for path in (decision_path, verification_path, event_path, runtime_path, complete_path, csv_path):
                path.parent.mkdir(parents=True, exist_ok=True)
            csv_path.write_text(f"csv-{run}", encoding="utf-8")
            passed = run == 1
            decision = {
                "schema_version": 2, "cycle_id": cycle, "stage": "4.5", "ea": ea,
                "selected_tf": "H1", "run_number": run, "run_count_required": 3,
                "verdict": "NARROW_OPT_COMPLETE" if passed else "NARROW_OPT_FAILED",
                "qualifying_row_count": 2 if passed else 0,
                "csv": str(csv_path.resolve()), "csv_sha256": supervisor.sha256(csv_path),
            }
            checks = [
                {"name": "decision exists", "passed": True},
                {"name": "csv exists", "passed": True},
                {"name": "csv sha256 matches decision", "passed": True},
                {"name": "verdict", "passed": passed},
                {"name": "all Stage 3 gates applied", "passed": passed},
                {"name": "run number in three-run sequence", "passed": True},
                {"name": "launched INI hash matches runtime", "passed": True},
                {"name": "all optimized ranges match plan and include clusters", "passed": True},
            ]
            verification = {"schema_version": 2, "cycle_id": cycle, "stage": "4.5",
                            "passed": passed, "checks": checks}
            decision_path.write_text(json.dumps(decision), encoding="utf-8")
            verification_path.write_text(json.dumps(verification), encoding="utf-8")
            event = {
                "schema_version": 2, "cycle_id": cycle, "stage": "4.5", "ea": ea,
                "timeframe": "H1", "run_number": run, "verdict": decision["verdict"],
                "verification_passed": passed,
                "decision": str(decision_path.resolve()), "decision_sha256": supervisor.sha256(decision_path),
                "verification": str(verification_path.resolve()), "verification_sha256": supervisor.sha256(verification_path),
                "csv": str(csv_path.resolve()), "csv_sha256": supervisor.sha256(csv_path), "consumed": False,
            }
            event_path.write_text(json.dumps(event), encoding="utf-8")
            runtime_path.write_text(json.dumps({
                "cycle_id": cycle, "run_number": run, "timeframe": "H1",
                "status": "TIMEFRAME_COMPLETE", "timed_out": False,
                "terminal_closed_after_run": True, "narrow_ini_sha256": f"ini-{run}",
            }), encoding="utf-8")
            complete_path.write_text(json.dumps({
                "cycle_id": cycle, "timeframe": "H1", "status": "TIMEFRAME_COMPLETE",
                "timed_out": False, "narrow_ini_sha256": f"ini-{run}",
            }), encoding="utf-8")
        return state, state_dir

    def test_stage45_final_gate_failure_is_rotation_ready_after_three_verified_runs(self):
        state, state_dir = self._stage45_exhaustion_fixture()
        valid, detail, event = supervisor.validate_stage45_terminal_failure(state, state_dir)
        self.assertTrue(valid, detail)
        self.assertEqual("stage45-cycle:terminal", event["event_id"])
        self.control.update({"allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
                             "dispatch_enabled": True})
        with mock.patch.object(supervisor, "STATE", state_dir / "kcos_cycle_state_v2.json"):
            result = supervisor.evaluate(state, self.control, self.db)
        self.assertEqual("rotate_stage1", result["action"])
        self.assertEqual(str(Path(state["last_stage45_event"]).resolve()), result["event_path"])

    def test_stage45_rotation_fails_closed_on_structural_check_failure(self):
        state, state_dir = self._stage45_exhaustion_fixture()
        verification_path = state_dir / "verification" / "stage45-cycle.stage45.run3.verification.json"
        verification = json.loads(verification_path.read_text())
        verification["checks"][-1]["passed"] = False
        verification_path.write_text(json.dumps(verification), encoding="utf-8")
        event_path = state_dir / "events" / "stage45-cycle.stage45.run3.complete.json"
        event = json.loads(event_path.read_text())
        event["verification_sha256"] = supervisor.sha256(verification_path)
        event_path.write_text(json.dumps(event), encoding="utf-8")
        valid, detail, _event = supervisor.validate_stage45_terminal_failure(state, state_dir)
        self.assertFalse(valid)
        self.assertIn("structural or range-integrity", detail)

    def test_verified_completion_holds_stage2(self):
        result = supervisor.evaluate(self.state, self.control, self.db)
        self.assertEqual("COMPLETE_HELD_STAGE2", result["status"])
        self.assertEqual("none", result["action"])

    def test_verified_completion_dispatches_stage2_when_authorized(self):
        self.control.update({
            "allowed_stages": [1, 2, 3],
            "hold_after_stage": 3,
            "dispatch_enabled": True,
            "stage_jobs": {"1": "job-1", "2": "job-2", "3": "job-3"},
            "last_stage1_execution_id": "stale-execution-id",
        })
        result = supervisor.evaluate(self.state, self.control, self.db)
        self.assertEqual("DISPATCH_READY_STAGE2", result["status"])
        self.assertEqual("dispatch_stage2", result["action"])
        self.assertIn("execution_id=execution-1", result["detail"])

    def test_stage1_event_dispatches_stage2_once_and_consumes_correct_event(self):
        self.control.update({
            "allowed_stages": [1, 2, 3],
            "hold_after_stage": 3,
            "dispatch_enabled": True,
            "stage_jobs": {"1": "job-1", "2": "job-2", "3": "job-3"},
        })
        state_path = self.root / "stage1-state.json"
        control_path = self.root / "stage1-control.json"
        heartbeat = self.root / "stage1-heartbeat.json"
        # A real Stage 1 handoff identifies the candidate before Stage 2 is
        # dispatched, even though Stage 2 has not fetched its source yet.
        dispatch_state = {**self.state, "candidate_ea": "EA candidate"}
        state_path.write_text(json.dumps(dispatch_state), encoding="utf-8")
        control_path.write_text(json.dumps(self.control), encoding="utf-8")
        first = supervisor.run(
            state_path, control_path, heartbeat, self.db, self.root / "stage1-claims", accepted_dispatch
        )
        second = supervisor.run(
            state_path, control_path, heartbeat, self.db, self.root / "stage1-claims", accepted_dispatch
        )
        self.assertEqual("dispatch_stage2", first["action"])
        self.assertTrue(first["dispatch_accepted"])
        self.assertTrue(json.loads(self.event.read_text())["consumed"])
        self.assertEqual("none", second["action"])
        self.assertEqual("already_claimed", second["claim"])

    def test_stage2_dispatch_pauses_stage1_before_launch(self):
        commands = []

        def recording_runner(command, **_kwargs):
            commands.append(command)
            return subprocess.CompletedProcess(command, 0, stdout="trigger accepted", stderr="")

        self.control.update({"stage_jobs": {"1": "job-1", "2": "job-2"}})
        accepted, _detail, _execution_id = supervisor.dispatch_job(
            self.control, "dispatch_stage2", recording_runner, self.db
        )
        self.assertTrue(accepted)
        self.assertEqual(["hermes", "cron", "pause", "job-1"], commands[0])
        self.assertEqual(["hermes", "cron", "run", "job-2"], commands[1])

    def test_hash_mismatch_fails_closed(self):
        self.artifact.write_text("changed", encoding="utf-8")
        result = supervisor.evaluate(self.state, self.control, self.db)
        self.assertEqual("UNKNOWN_STALLED", result["status"])

    def test_nonterminal_execution_fails_closed(self):
        connection = sqlite3.connect(self.db)
        connection.execute("UPDATE executions SET status='running'")
        connection.commit()
        connection.close()
        result = supervisor.evaluate(self.state, self.control, self.db)
        self.assertEqual("UNKNOWN_STALLED", result["status"])

    def test_new_execution_binding_ignores_all_preexisting_ids(self):
        before = {"execution-1"}
        connection = sqlite3.connect(self.db)
        connection.execute("INSERT INTO executions VALUES (?, ?, ?, ?, ?)",
                           ("execution-2", "job-1", "running", "2026-01-01T00:03:00Z", None))
        connection.commit()
        connection.close()
        self.assertEqual("execution-2", supervisor.newest_unseen_execution(self.db, "job-1", before))

    def test_paused_is_silent_action(self):
        self.control["paused"] = True
        result = supervisor.evaluate(self.state, self.control, self.db)
        self.assertEqual("PAUSED", result["status"])
        self.assertEqual("none", result["action"])

    def test_missing_control_writes_unknown_stalled(self):
        state_path = self.root / "state.json"
        heartbeat_path = self.root / "heartbeat.json"
        state_path.write_text(json.dumps(self.state), encoding="utf-8")
        result = supervisor.run(
            state_path=state_path,
            control_path=self.root / "missing-control.json",
            heartbeat_path=heartbeat_path,
            db_path=self.db,
        )
        self.assertEqual("UNKNOWN_STALLED", result["status"])
        self.assertEqual("none", result["action"])
        self.assertEqual("UNKNOWN_STALLED", json.loads(heartbeat_path.read_text())["status"])

    def test_stage3_cycle_hold_gate_honors_canonical_hold_and_fails_closed(self):
        self.assertIn("hold_after_stage=2", supervisor.stage3_cycle_hold({"hold_after_stage": 2}))
        self.assertIn("Stage 2 hold/audit", supervisor.stage3_cycle_hold({
            "hold_after_stage": 5,
            "stage_status": "stage2_complete_held",
            "next_action": "audit_stage2_before_stage3",
        }))
        self.assertIn("invalid cycle hold_after_stage", supervisor.stage3_cycle_hold({"hold_after_stage": "2"}))
        for release_hold in (4.5, 5):
            with self.subTest(release_hold=release_hold):
                self.assertIsNone(supervisor.stage3_cycle_hold({
                    "hold_after_stage": release_hold,
                    "stage_status": "stage3_pending",
                    "next_action": "start_stage3",
                }))

    def test_stage3_cycle_hold_keeps_stage2_audit_stop_despite_full_cycle_authorization(self):
        state = {
            "schema_version": 2,
            "cycle_id": "cycle-1",
            "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held",
            "hold_after_stage": 2,
            "next_action": "audit_stage2_before_stage3",
            "run_authorization": {
                "schema_version": 1,
                "scope": "current_cycle_stages_1_to_5",
                "cycle_id": "cycle-1",
                "authorized_by": "user_confirmed",
                "allowed_stages": [1, 2, 3, 4, 5],
                "hold_after_stage": 5,
                "reason": "User authorized this complete Kiyosaki cycle.",
                "authorized_utc": "2026-10-09T11:28:51Z",
            },
        }
        self.assertIn("hold_after_stage=2", supervisor.stage3_cycle_hold(state))
        state["hold_after_stage"] = 5
        self.assertIn("Stage 2 hold/audit", supervisor.stage3_cycle_hold(state))
        state["run_authorization"]["cycle_id"] = "other-cycle"
        self.assertIn("Stage 2 hold/audit", supervisor.stage3_cycle_hold(state))

    def test_stale_stage2_requalification_failure_is_bounded_to_one_retry(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            spec = root / "stage1.json"
            failure_path = root / "stage2.failed.json"
            spec.write_text("stage1", encoding="utf-8")
            failure = {
                "schema_version": 2, "cycle_id": "cycle-new", "stage": 2,
                "status": "failed", "failure_kind": "STAGE2_EXECUTOR_FAILURE",
                "exception_type": "RuntimeError",
                "error": "Stage 2 requalification claim no longer authenticates its same-cycle source and zero-trade evidence",
                "source": None, "source_sha256": None, "source_identity_available": False,
                "verification": None, "compile_attempt": None, "failed_checks": [],
                "stage1_spec": str(spec), "stage1_spec_sha256": supervisor.sha256(spec),
            }
            failure_path.write_text(json.dumps(failure), encoding="utf-8")
            state = {
                "cycle_id": "cycle-new", "current_stage": "stage2_pending",
                "stage_status": "stage2_repair_required", "next_action": "repair_stage2_failure",
                "stage2_failure_event": str(failure_path), "stage2_requalification_count": 1,
                "stage2_requalification": {"cycle_id": "cycle-old"},
                "last_stage1_artifact": str(spec), "candidate_ea": "EA",
            }
            identity = supervisor.stale_stage2_requalification_retry_identity(state, failure, failure_path)
            self.assertIsNotNone(identity)
            state["stage2_stale_requalification_retry_cycle_id"] = "cycle-new"
            state["stage2_stale_requalification_retry_count"] = 1
            self.assertEqual(1, state["stage2_stale_requalification_retry_count"])
    def test_stage2_requalification_guard_is_cycle_scoped(self):
        state = {
            "cycle_id": "cycle-new",
            "stage2_requalification_count": 1,
            "stage2_requalification": {"cycle_id": "cycle-old", "status": "authorized"},
        }
        self.assertFalse(supervisor.stage2_requalification_is_current(state))
        state["stage2_requalification"]["cycle_id"] = "cycle-new"
        self.assertTrue(supervisor.stage2_requalification_is_current(state))
        state["stage2_requalification_count"] = 0
        self.assertFalse(supervisor.stage2_requalification_is_current(state))
    def test_stage3_pending_stale_stage2_receipt_dispatches_one_requalification(self):
        cycle = "cycle-requal"
        source = self.root / "candidate.mq5"
        ex5 = self.root / "candidate.ex5"
        minato = self.root / "MinatoV2.mqh"
        spec = self.root / "stage1-spec.json"
        stage2_event = self.root / "stage2-event.json"
        stage2_verification = self.root / "stage2-verification.json"
        decision = self.root / f"{cycle}.stage3.decision.json"
        stage3_verification = self.root / f"{cycle}.stage3.verification.json"
        for path, content in ((source, "source-new"), (ex5, "fresh-ex5"),
                              (minato, "fresh-minato"), (spec, '{"tester_config":{}}')):
            path.write_text(content, encoding="utf-8")
        source_hash, ex5_hash = supervisor.sha256(source), supervisor.sha256(ex5)
        minato_hash, spec_hash = supervisor.sha256(minato), supervisor.sha256(spec)
        checks = {"lineage": True, "compile_clean": True, "safety": True}
        verification = {
            "schema_version": 2, "cycle_id": cycle, "stage": 2, "ea": "EA",
            "passed": True, "checks": checks, "source": str(source),
            "source_sha256": source_hash, "ex5": str(ex5),
            "canonical_minato": str(minato), "canonical_minato_sha256": minato_hash,
            "stage1_spec": str(spec), "stage1_spec_sha256": spec_hash,
            "compile": {"passed": True, "fresh_ex5": True, "errors": 0, "warnings": 0,
                        "mq5_sha256": source_hash, "ex5_sha256": ex5_hash},
        }
        stage2_verification.write_text(json.dumps(verification), encoding="utf-8")
        stage2_event.write_text(json.dumps({
            "schema_version": 2, "cycle_id": cycle, "stage": 2, "status": "complete",
            "ea": "EA", "verification": str(stage2_verification),
            "verification_sha256": "STALE", "mq5_sha256": "OLD-SOURCE",
            "ex5_sha256": "OLD-EX5", "canonical_minato_sha256": "OLD-MINATO",
            "stage1_spec": str(spec), "stage1_spec_sha256": spec_hash,
            "completed_utc": "2026-01-01T00:01:00Z",
        }), encoding="utf-8")
        decision.write_text(json.dumps({
            "cycle_id": cycle, "stage": 3, "ea": "EA", "verdict": "REPAIR_REQUIRED",
            "repair_trigger": {"kind": "completed_timeframe_zero_trades", "timeframe": "M5"},
            "timeframes": {"M5": {"zero_trades": True, "trades": 0,
                                   "max_trades": 0, "row_count": 12}},
        }), encoding="utf-8")
        stage3_verification.write_text(json.dumps({
            "cycle_id": cycle, "stage": 3, "decision": str(decision),
            "decision_sha256": supervisor.sha256(decision), "passed": True,
            "checks": {"metrics_recomputed": True},
        }), encoding="utf-8")
        state = {
            "schema_version": 2, "cycle_id": cycle, "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held", "next_action": "audit_stage2_before_stage3",
            "hold_after_stage": 2, "current_ea": "EA", "candidate_ea": "EA",
            "candidate_source": str(source), "last_stage1_artifact": str(spec),
            "last_stage2_event": str(stage2_event),
            "last_stage2_verification": str(stage2_verification),
            "stage3_zero_trade_decision": str(decision),
            "stage3_zero_trade_verification": str(stage3_verification),
            "run_authorization": {"schema_version": 1, "scope": "current_cycle_stages_1_to_5",
                "cycle_id": cycle, "authorized_by": "user_confirmed", "allowed_stages": [1, 2, 3, 4, 5],
                "hold_after_stage": 5, "authorized_utc": "2026-01-01T00:00:00Z",
                "reason": "authorized test cycle"},
        }
        control = {"schema_version": 2, "paused": False, "dispatch_enabled": True,
                   "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
                   "stage_jobs": {"1": "job-1", "2": "job-2", "3": "job-3"}}
        result = supervisor.evaluate(state, control, self.db)
        self.assertEqual("STAGE2_REQUALIFICATION_READY", result["status"])
        self.assertEqual("dispatch_stage2", result["action"])
        state["stage2_requalification_count"] = 1
        self.assertIsNone(supervisor.authenticated_stage2_requalification(state, control))

    def _policy_requalification_fixture(self, root: Path):
        state_dir = root / "state"
        (state_dir / "requests").mkdir(parents=True)
        (state_dir / "events").mkdir()
        (state_dir / "verification").mkdir()
        (state_dir / "stage3_v2").mkdir()
        cycle = "cycle-policy-requal"
        source = root / "EA.mq5"
        ex5 = root / "EA.ex5"
        minato = root / "minato.mqh"
        spec = state_dir / "spec.json"
        for path, value in ((source, "source"), (ex5, "binary"),
                            (minato, "score"), (spec, '{"spec":true}')):
            path.write_text(value, encoding="utf-8")
        verification_path = state_dir / "verification" / f"{cycle}.stage2.verification.json"
        verification = {
            "schema_version": 2, "cycle_id": cycle, "stage": 2, "ea": "EA",
            "passed": True, "checks": {"compile_clean": True, "protected_contract": True},
            "source": str(source), "source_sha256": supervisor.sha256(source),
            "ex5": str(ex5), "canonical_minato": str(minato),
            "canonical_minato_sha256": supervisor.sha256(minato),
            "stage1_spec": str(spec), "stage1_spec_sha256": supervisor.sha256(spec),
            "compile": {"passed": True, "fresh_ex5": True, "errors": 0, "warnings": 0,
                        "ex5_sha256": supervisor.sha256(ex5)},
        }
        verification_path.write_text(json.dumps(verification), encoding="utf-8")
        event_path = state_dir / "events" / f"{cycle}.stage2.complete.json"
        event_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": cycle, "stage": 2, "status": "complete",
            "ea": "EA", "verification": str(verification_path),
            "verification_sha256": supervisor.sha256(verification_path),
            "mq5_sha256": supervisor.sha256(source), "ex5_sha256": supervisor.sha256(ex5),
            "canonical_minato_sha256": supervisor.sha256(minato),
            "completed_utc": "2026-10-23T14:21:25Z",
        }), encoding="utf-8")
        runtime_path = state_dir / "stage3_v2" / f"{cycle}.stage3.runtime.json"
        runtime_path.write_text(json.dumps({
            "cycle_id": cycle, "stage": 3, "timeframe": "M15",
            "status": "TIMEFRAME_COMPLETE", "terminal_pid": 123,
        }), encoding="utf-8")
        pipeline_hashes = {}
        for name in supervisor.STAGE2_PIPELINE_CODE_FILES:
            path = state_dir / name
            path.write_text(f"{name}\n", encoding="utf-8")
            pipeline_hashes[name] = supervisor.sha256(path)
        basis = {
            "stage2_event_sha256": supervisor.sha256(event_path),
            "stage2_verification_sha256": supervisor.sha256(verification_path),
            "source_sha256": supervisor.sha256(source),
            "stage1_spec_sha256": supervisor.sha256(spec),
            "stage3_runtime_sha256": supervisor.sha256(runtime_path),
            "pipeline_code_hashes": pipeline_hashes,
        }
        request = {
            "schema_version": 1, "request_id": "operator-grid-update-1",
            "status": "requested", "cycle_id": cycle,
            "scope": "current_cycle_stage2_once", "grid_policy_id": "mt5_15_20_intervals_v1",
            "authorization": {"authorized_by": "user_confirmed", "cycle_id": cycle,
                              "scope": "current_cycle_stage2_once"},
            "basis": basis,
        }
        request_path = state_dir / "requests" / "stage2_policy_requalification.json"
        request_path.write_text(json.dumps(request), encoding="utf-8")
        state = {
            "schema_version": 2, "cycle_id": cycle, "candidate_ea": "EA", "current_ea": "EA",
            "candidate_source": str(source), "last_stage1_artifact": str(spec),
            "current_stage": "stage3_running", "stage_status": "stage3_m15_running",
            "next_action": "watch_stage3_m15", "last_stage2_event": str(event_path),
            "last_stage2_verification": str(verification_path),
            "run_authorization": {"schema_version": 1, "cycle_id": cycle,
                "scope": "current_cycle_stages_1_to_5", "authorized_by": "user_confirmed",
                "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
                "reason": "operator requested policy requalification for this cycle",
                "authorized_utc": "2026-10-24T00:00:00Z"},
        }
        control = {"schema_version": 2, "paused": False, "dispatch_enabled": True,
                   "allowed_stages": [1, 2, 3, 4, 5], "stage_jobs": {"3": "job-3"}}
        return state_dir, state, control

    def test_policy_requalification_is_exact_cycle_and_runtime_bound(self):
        state_dir, state, control = self._policy_requalification_fixture(self.root)
        with mock.patch.object(supervisor.rotation, "active_stage3_runtime",
                               return_value={"status": "TIMEFRAME_COMPLETE", "timeframe": "M15",
                                             "pids": [123]}):
            result = supervisor.authenticated_stage2_policy_requalification(state, control, state_dir)
            with mock.patch.object(supervisor, "STATE", state_dir / "kcos_cycle_state_v2.json"):
                decision = supervisor.evaluate(state, control, self.db)
        self.assertIsNotNone(result)
        self.assertEqual("operator-grid-update-1", result["request_id"])
        self.assertEqual("STAGE2_POLICY_REQUALIFICATION_READY", decision["status"])
        self.assertEqual("dispatch_stage2", decision["action"])

    def test_policy_requalification_rejects_worker_or_changed_policy_identity(self):
        state_dir, state, control = self._policy_requalification_fixture(self.root)
        with mock.patch.object(supervisor.rotation, "active_stage3_runtime",
                               return_value={"status": "TIMEFRAME_COMPLETE", "timeframe": "M15",
                                             "pids": [123, 456]}):
            self.assertIsNone(supervisor.authenticated_stage2_policy_requalification(
                state, control, state_dir))
        with mock.patch.object(supervisor.rotation, "active_stage3_runtime",
                               return_value={"status": "TIMEFRAME_COMPLETE", "timeframe": "M15",
                                             "pids": [123]}):
            request_path = state_dir / "requests" / "stage2_policy_requalification.json"
            request = json.loads(request_path.read_text(encoding="utf-8"))
            request["basis"]["pipeline_code_hashes"]["kcos_tester_config_v2.py"] = "0" * 64
            request_path.write_text(json.dumps(request), encoding="utf-8")
            self.assertIsNone(supervisor.authenticated_stage2_policy_requalification(
                state, control, state_dir))

    def test_policy_requalification_followup_requires_exact_stage3_grid_rejection(self):
        state_dir, state, control = self._policy_requalification_fixture(self.root)
        state["stage2_policy_requalification_count"] = 1
        state["stage_status"] = "stage3_infrastructure_failure"
        state["stage3_failure_class"] = "infrastructure"
        state["next_action"] = "inspect_stage3_infrastructure_failure"
        runtime_original = state_dir / "stage3_v2" / f"{state['cycle_id']}.stage3.runtime.json"
        archive_dir = state_dir / "diagnostics" / "stage2_policy_requalification" / state["cycle_id"] / "first-request"
        archive_dir.mkdir(parents=True)
        archived_runtime = archive_dir / runtime_original.name
        archived_runtime.write_bytes(runtime_original.read_bytes())
        manifest_path = archive_dir / "manifest.json"
        manifest_path.write_text(json.dumps({"cycle_id": state["cycle_id"], "files": [{
            "original_path": str(runtime_original.resolve()),
            "archive_path": str(archived_runtime.resolve()),
            "sha256": supervisor.sha256(archived_runtime),
            "size_bytes": archived_runtime.stat().st_size,
        }]}), encoding="utf-8")
        state["stage2_policy_requalification"] = {"archive": {
            "path": str(archive_dir.resolve()), "manifest_sha256": supervisor.sha256(manifest_path)}}
        failure_path = state_dir / "events" / f"{state['cycle_id']}.stage3.failed.json"
        failure_path.write_text(json.dumps({
            "cycle_id": state["cycle_id"], "stage": 3, "status": "failed",
            "failure_kind": "STAGE3_EXECUTION_FAILURE", "rotation_eligible": False,
            "error": "[TesterInputs] optimized parameter InpObvDivLookback has 20 MT5 steps (21 values); invalid for default profile",
            "failed_utc": "2026-10-23T14:26:41Z",
        }), encoding="utf-8")
        state["stage3_failure_event"] = str(failure_path)
        exact_error = ("Script exited with code 2\nstdout:\n{\"status\":\"INFRASTRUCTURE_FAILURE\","
                       "\"detail\":\"[TesterInputs] optimized parameter InpObvDivLookback has 20 MT5 steps "
                       "(21 values); invalid for default profile\",\"zero_token\":true}")
        connection = sqlite3.connect(self.db)
        connection.execute("DROP TABLE executions")
        connection.execute("CREATE TABLE executions (id TEXT, job_id TEXT, status TEXT, claimed_at TEXT, finished_at TEXT, error TEXT)")
        connection.execute("INSERT INTO executions VALUES (?, ?, ?, ?, ?, ?)", (
            "stage3-grid-error", "job-3", "failed", "2026-10-24T01:22:39+11:00",
            "2026-10-24T01:22:41+11:00", exact_error))
        connection.commit(); connection.close()
        first_request_path = state_dir / "requests" / "stage2_policy_requalification.json"
        first_request_path.unlink()
        request_path = state_dir / "requests" / "stage2_policy_requalification_after_stage3_grid_rejection.json"
        prior_request = {
            "schema_version": 1, "request_id": "operator-grid-followup-1", "status": "requested",
            "cycle_id": state["cycle_id"],
            "scope": "current_cycle_stage2_once_after_authenticated_stage3_grid_rejection",
            "grid_policy_id": "mt5_15_20_intervals_v1",
            "authorization": {"authorized_by": "user_confirmed", "cycle_id": state["cycle_id"],
                              "scope": "current_cycle_stage2_once_after_authenticated_stage3_grid_rejection"},
            "basis": {
                "stage2_event_sha256": supervisor.sha256(Path(state["last_stage2_event"])),
                "stage2_verification_sha256": supervisor.sha256(Path(state["last_stage2_verification"])),
                "source_sha256": supervisor.sha256(Path(state["candidate_source"])),
                "stage1_spec_sha256": supervisor.sha256(Path(state["last_stage1_artifact"])),
                "stage3_runtime_sha256": supervisor.sha256(state_dir / "stage3_v2" / f"{state['cycle_id']}.stage3.runtime.json"),
                "pipeline_code_hashes": {
                    name: supervisor.sha256(state_dir / name)
                    for name in supervisor.STAGE2_PIPELINE_CODE_FILES
                },
                "stage3_failure_event_sha256": supervisor.sha256(failure_path),
                "stage3_grid_rejection_execution": supervisor.authenticated_stage3_grid_rejection_execution(
                    state, control, self.db),
            },
        }
        request_path.write_text(json.dumps(prior_request), encoding="utf-8")
        with mock.patch.object(supervisor.rotation, "active_stage3_runtime", return_value=None):
            result = supervisor.authenticated_stage2_policy_requalification(state, control, state_dir, self.db)
            with mock.patch.object(supervisor, "STATE", state_dir / "kcos_cycle_state_v2.json"):
                decision = supervisor.evaluate(state, control, self.db)
            self.assertIsNotNone(result)
            self.assertEqual("operator-grid-followup-1", result["request_id"])
            self.assertEqual("STAGE2_POLICY_REQUALIFICATION_READY", decision["status"])
            self.assertEqual("dispatch_stage2", decision["action"])
            failure_path.write_text(json.dumps({**json.loads(failure_path.read_text()),
                                                "error": "unrelated failure"}), encoding="utf-8")
            self.assertIsNone(supervisor.authenticated_stage2_policy_requalification(
                state, control, state_dir, self.db))

    def test_verified_stage2_completion_holds_stage3(self):
        source = self.root / "ea.mq5"
        ex5 = self.root / "ea.ex5"
        minato = self.root / "minato.mqh"
        verification = self.root / "stage2-verification.json"
        event = self.root / "stage2-event.json"
        for path, value in ((source, "source"), (ex5, "binary"), (minato, "score")):
            path.write_text(value, encoding="utf-8")
        verification.write_text(json.dumps({
            "schema_version": 2, "cycle_id": "cycle-1", "stage": 2, "passed": True,
            "source": str(source), "ex5": str(ex5), "canonical_minato": str(minato),
            "stage1_spec_sha256": supervisor.sha256(self.artifact),
            "checks": {"lineage": True, "compile": True, "safety": True},
        }), encoding="utf-8")
        event.write_text(json.dumps({
            "schema_version": 2, "cycle_id": "cycle-1", "stage": 2, "status": "complete",
            "verification": str(verification.resolve()),
            "verification_sha256": supervisor.sha256(verification),
            "mq5_sha256": supervisor.sha256(source), "ex5_sha256": supervisor.sha256(ex5),
            "canonical_minato_sha256": supervisor.sha256(minato),
            "stage1_spec": str(self.artifact), "stage1_spec_sha256": supervisor.sha256(self.artifact),
            "completed_utc": "2026-01-01T00:01:00Z",
        }), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.execute("INSERT INTO executions VALUES (?, ?, ?, ?, ?)", ("execution-2", "job-2", "completed", "2026-01-01T00:00:00Z", "2026-01-01T00:02:00Z"))
        connection.commit()
        connection.close()
        self.state.update({
            "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held",
            "hold_after_stage": 2,
            "next_action": "audit_stage2_before_stage3",
            "last_stage2_verification": str(verification),
            "last_stage2_event": str(event),
        })
        self.control.update({
            "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
            "dispatch_enabled": True,
            "stage_jobs": {"1": "job-1", "2": "job-2", "3": "job-3", "4": "job-4", "4.5": "job-45", "5": "job-5"},
            "last_stage2_execution_id": "execution-2",
        })
        result = supervisor.evaluate(self.state, self.control, self.db)
        self.assertEqual("COMPLETE_HELD_STAGE3", result["status"])
        self.assertEqual("none", result["action"])
        attestations_dir = self.root / "attestations"
        archived = list(attestations_dir.glob("cycle-1.stage2.execution.*.json")) if attestations_dir.is_dir() else []
        self.assertEqual(1, len(archived), "held Stage 2 evidence should be durably attested while its DB row exists")
        saved = json.loads(archived[0].read_text(encoding="utf-8"))
        self.assertEqual("windows_dpapi_user", saved.get("protection"))
        self.assertNotIn("attestation_sha256", saved)

        # A DPAPI-valid record still must match its live Hermes DB row.
        forged = json.loads(supervisor.unprotect_attestation_payload(saved["protected_payload_b64"]))
        forged["execution_id"] = "forged-but-DPAPI-protected"
        forged_envelope = dict(saved)
        forged_envelope["protected_payload_b64"] = supervisor.protect_attestation_payload(
            json.dumps(forged, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )
        archived[0].write_text(json.dumps(forged_envelope), encoding="utf-8")
        valid, detail = supervisor.validate_stage2_execution_attestation(
            archived[0], self.state, "job-2", event, verification,
            json.loads(event.read_text(encoding="utf-8")), self.db
        )
        self.assertFalse(valid)
        self.assertIn("disagrees with the current matching Hermes event row", detail)
        valid, detail = supervisor.validate_stage2_completion(self.state, self.control, self.db)
        self.assertTrue(valid, detail)
        self.assertIn("DPAPI-authenticated execution attestation verified", detail)
        self.assertIn("legacy/corrupt attestation quarantined", detail)

        # Recomputing the old public checksum cannot establish provenance.
        saved = json.loads(archived[0].read_text(encoding="utf-8"))
        legacy = json.loads(supervisor.unprotect_attestation_payload(saved["protected_payload_b64"]))
        legacy["execution_id"] = "forged-execution-id"
        legacy["attestation_sha256"] = hashlib.sha256(
            json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest().upper()
        archived[0].write_text(json.dumps(legacy), encoding="utf-8")
        valid, detail = supervisor.validate_stage2_completion(self.state, self.control, self.db)
        self.assertTrue(valid, detail)  # Live matching DB row permits reauthentication.
        self.assertIn("legacy/corrupt attestation quarantined", detail)
        self.assertEqual(2, len(list(attestations_dir.glob("*.untrusted.*.json"))))
        saved = json.loads(archived[0].read_text(encoding="utf-8"))
        self.assertEqual("windows_dpapi_user", saved.get("protection"))

        connection = sqlite3.connect(self.db)
        connection.execute("DELETE FROM executions WHERE id = ?", ("execution-2",))
        connection.commit()
        connection.close()
        valid, detail = supervisor.validate_stage2_completion(self.state, self.control, self.db)
        self.assertTrue(valid, detail)
        self.assertIn("DPAPI-authenticated execution attestation", detail)

        # Altering protected bytes is detected after the DB row has expired.
        ciphertext = bytearray(base64.b64decode(saved["protected_payload_b64"]))
        ciphertext[len(ciphertext) // 2] ^= 1
        saved["protected_payload_b64"] = base64.b64encode(ciphertext).decode("ascii")
        archived[0].write_text(json.dumps(saved), encoding="utf-8")
        valid, detail = supervisor.validate_stage2_completion(self.state, self.control, self.db)
        self.assertFalse(valid)
        self.assertIn("DPAPI", detail)

        # Even a freshly recomputed legacy SHA-256 is rejected without the DB row.
        legacy["execution_id"] = "forged-after-eviction"
        legacy.pop("attestation_sha256", None)
        legacy["attestation_sha256"] = hashlib.sha256(
            json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest().upper()
        archived[0].write_text(json.dumps(legacy), encoding="utf-8")
        valid, detail = supervisor.validate_stage2_completion(self.state, self.control, self.db)
        self.assertFalse(valid)
        self.assertIn("no matching DB row", detail)

    def test_verified_stage2_completion_uses_zero_token_stage3_start(self):
        source = self.root / "start-ea.mq5"; ex5 = self.root / "start-ea.ex5"; minato = self.root / "start-minato.mqh"
        for path in (source, ex5, minato):
            path.write_text(path.name, encoding="utf-8")
        verification = self.root / "start-stage2-verification.json"
        verification.write_text(json.dumps({"cycle_id": "cycle-1", "passed": True, "source": str(source),
                                             "ex5": str(ex5), "canonical_minato": str(minato),
                                             "stage1_spec_sha256": supervisor.sha256(self.artifact),
                                             "checks": {"lineage": True, "compile": True, "safety": True}}), encoding="utf-8")
        event = self.root / "start-stage2-event.json"
        event.write_text(json.dumps({"schema_version": 2, "cycle_id": "cycle-1", "stage": 2, "status": "complete",
                                     "verification": str(verification.resolve()), "verification_sha256": supervisor.sha256(verification),
                                     "mq5_sha256": supervisor.sha256(source), "ex5_sha256": supervisor.sha256(ex5),
                                     "canonical_minato_sha256": supervisor.sha256(minato),
                                     "stage1_spec": str(self.artifact), "stage1_spec_sha256": supervisor.sha256(self.artifact),
                                     "completed_utc": "2026-01-01T00:01:00Z"}), encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.execute("INSERT INTO executions VALUES (?, ?, ?, ?, ?)",
                           ("execution-2", "job-2", "completed", "2026-01-01T00:00:00Z", "2026-01-01T00:02:00Z"))
        connection.commit(); connection.close()
        self.state.update({"current_stage": "stage3_pending", "stage_status": "stage3_pending",
                           "hold_after_stage": 3, "next_action": "start_stage3",
                           "last_stage2_verification": str(verification), "last_stage2_event": str(event)})
        self.control.update({"allowed_stages": [1, 2, 3], "hold_after_stage": 3, "dispatch_enabled": True,
                             "stage_jobs": {"1": "job-1", "2": "job-2", "3": "job-3"},
                             "last_stage2_execution_id": "execution-2"})
        result = supervisor.evaluate(self.state, self.control, self.db)
        self.assertEqual("start_stage3", result["action"])
        self.assertEqual("DISPATCH_READY_STAGE3", result["status"])

    def test_stage3_journal_reclassification_retry_gets_new_hash_bound_claim(self):
        source = self.root / "retry-ea.mq5"; source.write_text("// retry candidate\n", encoding="utf-8")
        verification = self.root / "retry-stage2-verification.json"
        verification.write_text(json.dumps({"cycle_id": "cycle-1", "source": str(source)}), encoding="utf-8")
        stage2_event = self.root / "retry-stage2-event.json"
        stage2_event.write_text(json.dumps({"cycle_id": "cycle-1", "stage": 2,
            "event_id": "cycle-1:stage2:complete", "status": "complete"}), encoding="utf-8")
        failure = self.root / "retry-stage3-failed.json"
        runtime = self.root / "retry-stage3-runtime.json"
        receipt = self.root / "retry-receipt.json"
        failure.write_text(json.dumps({"cycle_id": "cycle-1", "stage": 3, "status": "failed",
            "rotation_eligible": False}), encoding="utf-8")
        runtime.write_text(json.dumps({"cycle_id": "cycle-1", "stage": 3,
            "status": "INFRASTRUCTURE_FAILURE", "infrastructure_only": True}), encoding="utf-8")
        receipt.write_text(json.dumps({"cycle_id": "cycle-1", "stage": 3, "repair_complete": True,
            "retry_event_id": "cycle-1:stage3:journal-reclassification-retry:1",
            "failure_event_sha256": supervisor.sha256(failure),
            "runtime_manifest_sha256": supervisor.sha256(runtime)}), encoding="utf-8")
        retry = {"status": "ready", "cycle_id": "cycle-1",
            "repair_kind": "stage3_journal_candidate_error_reclassification",
            "event_id": "cycle-1:stage3:journal-reclassification-retry:1",
            "failure_event": str(failure), "failure_event_sha256": supervisor.sha256(failure),
            "runtime_manifest": str(runtime), "runtime_manifest_sha256": supervisor.sha256(runtime),
            "repair_receipt": str(receipt), "repair_receipt_sha256": supervisor.sha256(receipt)}
        state = {"schema_version": 2, "cycle_id": "cycle-1", "current_ea": "EA",
            "current_stage": "stage3_pending", "stage_status": "stage2_complete_held",
            "hold_after_stage": 5, "last_stage2_event": str(stage2_event),
            "last_stage2_verification": str(verification), "run_authorization": {
                "schema_version": 1, "scope": "current_cycle_stages_1_to_5", "cycle_id": "cycle-1",
                "authorized_by": "user_confirmed", "allowed_stages": [1, 2, 3, 4, 5],
                "hold_after_stage": 5, "reason": "Explicit authorization for this complete test cycle.",
                "authorized_utc": "2026-01-01T00:00:00Z"},
            "stage3_journal_reclassification_retry_count": 1,
            "stage3_journal_reclassification_retry_event_id": retry["event_id"],
            "stage3_repair_retry": retry}
        control = {"schema_version": 2, "paused": False, "allowed_stages": [1, 2, 3, 4, 5],
            "hold_after_stage": 5, "dispatch_enabled": True,
            "stage_jobs": {"1": "job-1", "2": "job-2", "3": "job-3", "4": "job-4", "4.5": "job-45", "5": "job-5"}}
        with mock.patch.object(supervisor, "validate_stage2_completion", return_value=(True, "authenticated")):
            result = supervisor.evaluate(state, control, self.db)
        self.assertEqual("STAGE3_JOURNAL_RECLASSIFICATION_RETRY_READY", result["status"])
        self.assertEqual(retry["event_id"], result["event_id"])
        self.assertEqual(supervisor.sha256(receipt),
                         result["dispatch_identity"]["stage3_repair_receipt_sha256"])
        original_claim = supervisor.dispatch_claim_path(self.root / "claims",
            "cycle-1:stage2:complete", "start_stage3", supervisor.stage3_start_content_identity(state))
        retry_claim = supervisor.dispatch_claim_path(self.root / "claims", result["event_id"],
            "start_stage3", result["dispatch_identity"])
        self.assertNotEqual(original_claim, retry_claim)

    def test_stage3_completion_holds_then_claims_dispatch_exactly_once(self):
        decision = self.root / "stage3-decision.json"
        verification = self.root / "stage3-verification.json"
        note = self.root / "passing.md"
        event = self.root / "stage3-event.json"
        stage2_event = self.root / "stage2-source-event.json"
        stage2_event.write_text('{"stage":2}', encoding="utf-8")
        decision.write_text(json.dumps({"cycle_id": "cycle-1", "verdict": "READY_FOR_STAGE4"}), encoding="utf-8")
        verification.write_text(json.dumps({"cycle_id": "cycle-1", "passed": True,
                                            "decision_sha256": supervisor.sha256(decision)}), encoding="utf-8")
        note.write_text("durable pass", encoding="utf-8")
        event.write_text(json.dumps({
            "schema_version": 2, "event_id": "cycle-1:stage3:complete", "cycle_id": "cycle-1", "stage": 3,
            "status": "complete", "verdict": "READY_FOR_STAGE4", "decision_sha256": supervisor.sha256(decision),
            "verification_sha256": supervisor.sha256(verification), "note_hashes": {str(note): supervisor.sha256(note)},
            "stage1_spec": str(self.artifact), "stage1_spec_sha256": supervisor.sha256(self.artifact),
            "stage2_event": str(stage2_event), "stage2_event_sha256": supervisor.sha256(stage2_event),
        }), encoding="utf-8")
        self.state.update({"current_stage": "stage4_pending", "stage_status": "stage3_complete_held",
                           "last_stage3_decision": str(decision), "last_stage3_verification": str(verification),
                           "last_stage3_event": str(event)})
        self.control.update({"allowed_stages": [1, 2, 3, 4], "hold_after_stage": 3, "dispatch_enabled": False,
                             "stage_jobs": {"1": "job-1", "4": "job-4"}})
        self.assertEqual("COMPLETE_HELD_AFTER_STAGE3", supervisor.evaluate(self.state, self.control, self.db)["status"])
        self.control.update({"hold_after_stage": None, "dispatch_enabled": True})
        self.state["run_authorization"] = {
            "schema_version": 1, "scope": "current_cycle_stages_1_to_5",
            "cycle_id": self.state["cycle_id"], "authorized_by": "user_confirmed",
            "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
            "reason": "User explicitly authorized all Kiyosaki cron stages for this cycle.",
            "authorized_utc": "2026-01-01T00:00:00Z",
        }
        self.control.update({"allowed_stages": [1, 2, 3, 4, 5],
                             "stage_jobs": {"1": "job-1", "2": "job-2", "3": "job-3",
                                            "4": "job-4", "4.5": "job-45", "5": "job-5"}})
        state_path, control_path = self.root / "state3.json", self.root / "control3.json"
        state_path.write_text(json.dumps(self.state), encoding="utf-8"); control_path.write_text(json.dumps(self.control), encoding="utf-8")
        def stage4_dispatch(command, **kwargs):
            committed = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual("stage4_pending", committed["stage_status"])
            self.assertIsNone(committed["hold_after_stage"])
            return accepted_dispatch(command, **kwargs)

        first = supervisor.run(state_path, control_path, self.root / "heartbeat3.json", self.db, self.root / "claims", stage4_dispatch)
        second = supervisor.run(state_path, control_path, self.root / "heartbeat3.json", self.db, self.root / "claims", accepted_dispatch)
        self.assertEqual("WAITING_STAGE4", first["status"]); self.assertEqual("acquired", first["claim"])
        self.assertTrue(first["dispatch_accepted"])
        self.assertTrue(json.loads(event.read_text())["consumed"])
        self.assertEqual("none", second["action"]); self.assertEqual("WAITING_STAGE4", second["status"])

    def test_completed_timeframe_triggers_one_stage3_continuation(self):
        opt = self.root / "m5.opt"
        opt.write_bytes(b"result")
        event = self.root / "m5.complete.json"
        runtime = self.root / "runtime.json"
        handoff = self.root / "stage3-handoff.json"
        event.write_text(json.dumps({
            "schema_version": 2, "event_id": "cycle-1:stage3:M5:complete",
            "cycle_id": "cycle-1", "stage": 3, "status": "TIMEFRAME_COMPLETE",
            "timeframe": "M5", "opt_sha256": supervisor.sha256(opt), "consumed": False,
        }), encoding="utf-8")
        runtime.write_text(json.dumps({
            "schema_version": 2, "cycle_id": "cycle-1", "stage": 3, "ea": "EA",
            "timeframe": "M5", "status": "TIMEFRAME_COMPLETE", "opt_path": str(opt),
            "timeframe_event": str(event),
        }), encoding="utf-8")
        handoff.write_text(json.dumps({
            "schema_version": 2, "cycle_id": "cycle-1", "stage": 3, "ea": "EA",
            "runtime_manifest": str(runtime),
        }), encoding="utf-8")
        self.state.update({"current_stage": "stage3_running", "current_ea": "EA", "last_stage3_handoff": str(handoff)})
        self.control.update({
            "allowed_stages": [1, 2, 3], "hold_after_stage": 3,
            "dispatch_enabled": False, "continuation_dispatch_enabled": True,
            "stage_jobs": {"3": "job-3"},
        })
        state_path, control_path = self.root / "state-tf.json", self.root / "control-tf.json"
        state_path.write_text(json.dumps(self.state), encoding="utf-8")
        control_path.write_text(json.dumps(self.control), encoding="utf-8")
        first = supervisor.run(state_path, control_path, self.root / "heartbeat-tf.json", self.db,
                               self.root / "claims-tf", accepted_driver)
        second = supervisor.run(state_path, control_path, self.root / "heartbeat-tf.json", self.db,
                                self.root / "claims-tf", accepted_driver)
        self.assertEqual("continue_stage3", first["action"])
        self.assertTrue(first["dispatch_accepted"])
        self.assertTrue(json.loads(event.read_text())["consumed"])
        self.assertEqual("TIMEFRAME_ALREADY_CONSUMED", second["status"])
        self.assertEqual("none", second["action"])

    def test_compute_budget_event_triggers_one_stage3_continuation(self):
        event = self.root / "m5.compute-budget.json"
        runtime = self.root / "runtime-compute.json"
        handoff = self.root / "stage3-compute-handoff.json"
        event.write_text(json.dumps({
            "schema_version": 2,
            "event_id": "cycle-1:stage3:M5:compute_budget_exceeded",
            "cycle_id": "cycle-1",
            "stage": 3,
            "status": "COMPUTE_BUDGET_EXCEEDED",
            "timeframe": "M5",
            "consumed": False,
        }), encoding="utf-8")
        runtime.write_text(json.dumps({
            "schema_version": 2,
            "cycle_id": "cycle-1",
            "stage": 3,
            "ea": "EA",
            "timeframe": "M5",
            "status": "COMPUTE_BUDGET_EXCEEDED",
            "timeframe_event": str(event),
        }), encoding="utf-8")
        handoff.write_text(json.dumps({
            "schema_version": 2,
            "cycle_id": "cycle-1",
            "stage": 3,
            "ea": "EA",
            "runtime_manifest": str(runtime),
        }), encoding="utf-8")
        self.state.update({"current_stage": "stage3_running", "current_ea": "EA",
                           "last_stage3_handoff": str(handoff)})
        self.control.update({
            "allowed_stages": [1, 2, 3], "hold_after_stage": 3,
            "dispatch_enabled": False, "continuation_dispatch_enabled": True,
            "stage_jobs": {"3": "job-3"},
        })
        state_path, control_path = self.root / "state-compute.json", self.root / "control-compute.json"
        state_path.write_text(json.dumps(self.state), encoding="utf-8")
        control_path.write_text(json.dumps(self.control), encoding="utf-8")

        def accepted_compute_driver(command, **_kwargs):
            return subprocess.CompletedProcess(
                command, 0, stdout='{"status":"COMPUTE_BUDGET_EXCEEDED","zero_token":true}', stderr=""
            )

        first = supervisor.run(state_path, control_path, self.root / "heartbeat-compute.json", self.db,
                               self.root / "claims-compute", accepted_compute_driver)
        second = supervisor.run(state_path, control_path, self.root / "heartbeat-compute.json", self.db,
                                self.root / "claims-compute", accepted_compute_driver)
        self.assertEqual("continue_stage3", first["action"])
        self.assertTrue(first["dispatch_accepted"])
        self.assertTrue(json.loads(event.read_text())["consumed"])
        self.assertEqual("TIMEFRAME_ALREADY_CONSUMED", second["status"])
        self.assertEqual("none", second["action"])

    def test_compute_budget_event_must_be_unconsumed_to_dispatch(self):
        event = self.root / "m5.consumed-compute-budget.json"
        runtime = self.root / "runtime-consumed-compute.json"
        handoff = self.root / "stage3-consumed-compute-handoff.json"
        event.write_text(json.dumps({
            "schema_version": 2, "event_id": "cycle-1:stage3:M5:compute_budget_exceeded",
            "cycle_id": "cycle-1", "stage": 3, "status": "COMPUTE_BUDGET_EXCEEDED",
            "timeframe": "M5", "consumed": True,
        }), encoding="utf-8")
        runtime.write_text(json.dumps({
            "schema_version": 2, "cycle_id": "cycle-1", "stage": 3, "ea": "EA",
            "timeframe": "M5", "status": "COMPUTE_BUDGET_EXCEEDED", "timeframe_event": str(event),
        }), encoding="utf-8")
        handoff.write_text(json.dumps({
            "schema_version": 2, "cycle_id": "cycle-1", "stage": 3, "ea": "EA",
            "runtime_manifest": str(runtime),
        }), encoding="utf-8")
        self.state.update({"current_stage": "stage3_running", "current_ea": "EA",
                           "last_stage3_handoff": str(handoff)})
        self.control.update({"allowed_stages": [1, 2, 3], "hold_after_stage": 3,
                             "continuation_dispatch_enabled": True})
        result = supervisor.evaluate(self.state, self.control, self.db)
        self.assertEqual("TIMEFRAME_ALREADY_CONSUMED", result["status"])
        self.assertEqual("none", result["action"])

    def test_stage3_driver_accepts_duplicate_launch_as_already_running(self):
        driver = self.root / "driver.py"
        driver.write_text("# test driver\n", encoding="utf-8")
        control = {"stage3_driver": str(driver), "stage3_handoff": str(self.root / "handoff.json")}

        def duplicate_runner(command, **_kwargs):
            return subprocess.CompletedProcess(
                command, 2,
                stdout='{"status":"INFRASTRUCTURE_FAILURE","detail":"another Stage 3 runtime is already optimizing","zero_token":true}',
                stderr="",
            )

        accepted, detail = supervisor.run_stage3_driver(
            self.root / "state.json", control, start=True, runner=duplicate_runner
        )
        self.assertTrue(accepted)
        self.assertEqual("ALREADY_RUNNING", json.loads(detail)["status"])

    def test_dispatch_job_reports_rejection_without_external_call(self):
        def rejected(command, **_kwargs):
            return subprocess.CompletedProcess(command, 2, stdout="", stderr="rejected")

        ok, detail, execution_id = supervisor.dispatch_job({"stage_jobs": {"3": "job-3"}}, "dispatch_stage3", rejected)
        self.assertFalse(ok)
        self.assertIn("rejected", detail)
        self.assertIsNone(execution_id)

    def test_stage4_rejection_requires_hash_authenticated_evidence_before_rotation(self):
        decision = self.root / "stage4-rejected.decision.json"
        verification = self.root / "stage4-rejected.verification.json"
        event = self.root / "stage4-rejected.event.json"
        decision.write_text(json.dumps({"cycle_id": "cycle-1", "stage": 4,
                                        "verdict": "STAGE4_REJECTED"}), encoding="utf-8")
        verification.write_text(json.dumps({"cycle_id": "cycle-1", "stage": 4,
                                            "decision_sha256": supervisor.sha256(decision),
                                            "passed": True}), encoding="utf-8")
        event.write_text(json.dumps({"cycle_id": "cycle-1", "stage": 4,
                                     "verdict": "STAGE4_REJECTED",
                                     "decision": str(decision),
                                     "decision_sha256": supervisor.sha256(decision),
                                     "verification": str(verification),
                                     "verification_sha256": supervisor.sha256(verification),
                                     "consumed": False}), encoding="utf-8")
        self.state.update({"current_stage": "stage4_rejected", "stage_status": "stage4_rejected",
                           "next_action": "queue_next", "last_stage4_event": str(event)})
        self.state["run_authorization"] = {
            "schema_version": 1, "scope": "current_cycle_stages_1_to_5",
            "cycle_id": "cycle-1", "authorized_by": "user_confirmed",
            "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
            "reason": "User authorized the complete Kiyosaki pipeline for this cycle.",
            "authorized_utc": "2026-01-01T00:00:00Z",
        }
        self.control.update({"allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": None,
                             "dispatch_enabled": True, "stage_jobs": {"1": "job-1"}})

        result = supervisor.evaluate(self.state, self.control, self.db)
        self.assertEqual("ROTATION_READY_STAGE1", result["status"])

        verification.write_text(json.dumps({"cycle_id": "cycle-1", "stage": 4,
                                            "decision_sha256": "stale", "passed": True}), encoding="utf-8")
        result = supervisor.evaluate(self.state, self.control, self.db)
        self.assertEqual("UNKNOWN_STALLED", result["status"])
        self.assertIn("hash-authenticated", result["detail"])

    def test_dispatch_job_rejects_paused_message_even_with_zero_exit(self):
        def paused(command, **_kwargs):
            return subprocess.CompletedProcess(command, 0, stdout="Job is paused/disabled; resume it before running.", stderr="")

        ok, detail, execution_id = supervisor.dispatch_job({"stage_jobs": {"3": "job-3"}}, "dispatch_stage3", paused)
        self.assertFalse(ok)
        self.assertIn("paused", detail)
        self.assertIsNone(execution_id)


if __name__ == "__main__":
    unittest.main()
