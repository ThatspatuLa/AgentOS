import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import hashlib

import kcos_stage2_autonomous_executor as executor


class Stage2ExecutorSafetyTests(unittest.TestCase):

    def test_failure_occurrence_key_is_bound_to_supervisor_dispatch_claim(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            identity = {"cycle_id": "cycle-1", "candidate_ea": "EA",
                        "candidate_source_sha256": "A" * 64}
            identity_hash = hashlib.sha256(json.dumps(
                identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode()).hexdigest().upper()
            claim_path = root / "dispatch.claim.json"

            def write_claim(claimed_utc):
                claim_path.write_text(json.dumps({
                    "schema_version": 3, "event_id": "dispatch-event-1",
                    "action": "dispatch_stage2", "claimed_utc": claimed_utc,
                    "content_identity": identity,
                    "content_identity_sha256": identity_hash,
                }), encoding="utf-8")
                return {"schema_version": 1, "event_id": "dispatch-event-1",
                        "action": "dispatch_stage2", "claim_path": str(claim_path),
                        "claimed_utc": claimed_utc,
                        "content_identity_sha256": identity_hash}

            first_state = {"stage2_dispatch_claim": write_claim("2026-10-23T00:00:00Z")}
            first = executor.stage2_dispatch_occurrence_key(
                first_state, cycle="cycle-1", ea="EA")
            replay = executor.stage2_dispatch_occurrence_key(
                first_state, cycle="cycle-1", ea="EA")
            second_state = {"stage2_dispatch_claim": write_claim("2026-10-23T00:01:00Z")}
            second = executor.stage2_dispatch_occurrence_key(
                second_state, cycle="cycle-1", ea="EA")

            self.assertEqual(first, replay)
            self.assertNotEqual(first, second)
            self.assertIsNone(executor.stage2_dispatch_occurrence_key(
                second_state, cycle="different-cycle", ea="EA"))

    def test_source_proposal_exhaustion_receipt_records_attempt_artifacts(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_dir = root / "state"
            (state_dir / "candidates" / ".generation" / "cycle" / "attempt").mkdir(parents=True)
            (state_dir / "events").mkdir(parents=True)
            attempt = state_dir / "candidates" / ".generation" / "cycle" / "attempt"
            hold_path = attempt / "hold.json"
            hold_path.write_text("{}", encoding="utf-8")
            raw_path = attempt / "proposal-1.stdout.txt"
            raw_path.write_text("rejected model output", encoding="utf-8")
            rejection_path = attempt / "proposal-rejection-1.json"
            rejection_path.write_text(json.dumps({
                "attempt": 1, "reason": "wrong source shape", "raw_proposal": str(raw_path),
            }), encoding="utf-8")
            state_path = state_dir / "kcos_cycle_state_v2.json"
            state_path.write_text(json.dumps({"cycle_id": "cycle", "current_ea": "EA"}), encoding="utf-8")
            identity = {"cycle_id": "cycle", "current_ea": "EA", "candidate_ea": "EA"}
            diagnosis = {
                "artifact": str(hold_path), "sha256": hashlib.sha256(hold_path.read_bytes()).hexdigest().upper(),
                "proposal_budget_exhausted": True, "failure_kind": "MODEL_SOURCE_PROPOSAL_BUDGET_EXHAUSTED",
                "diagnoses": [{"failure_kind": "RESPONSE_FORMAT", "failed_check": "wrong source shape",
                               "failure_fingerprint": "F" * 64, "proposed_source_sha256": "A" * 64,
                               "same_failure_count": 1}],
            }
            with mock.patch.object(executor, "ROOT", root), \
                 mock.patch.object(executor, "authenticated_source_diagnosis", return_value=diagnosis), \
                 mock.patch.object(executor, "_failed_checks", return_value=([], None)):
                failure_path = executor.record_failure(identity, RuntimeError("exhausted"))
            receipts = list((state_dir / "diagnostics" / "stage2_failures").glob("*.source-proposal-exhaustion.*.json"))
            self.assertEqual(1, len(receipts))
            receipt = json.loads(receipts[0].read_text(encoding="utf-8"))
            evidence = receipt["proposal_attempt_evidence"][0]
            self.assertEqual("wrong source shape", evidence["rejection_reason"])
            self.assertEqual(str(rejection_path.resolve()), evidence["rejection_artifact"])
            self.assertEqual(hashlib.sha256(rejection_path.read_bytes()).hexdigest().upper(),
                             evidence["rejection_artifact_sha256"])
            self.assertEqual(str(raw_path.resolve()), evidence["raw_proposal_artifact"])
            self.assertEqual(hashlib.sha256(raw_path.read_bytes()).hexdigest().upper(),
                             evidence["raw_proposal_artifact_sha256"])
            self.assertTrue(failure_path.is_file())
            persisted_state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(str(failure_path.resolve()), persisted_state["stage2_failure_event"])

    def test_budget_exhausted_hold_requalifies_only_an_exact_replayable_proposal(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_dir = root / "state"
            state_dir.mkdir()
            spec_path = state_dir / "stage1.spec.json"
            spec_path.write_text("{}", encoding="utf-8")
            minato_path = root / "minato.mqh"
            minato_path.write_text("// approved include", encoding="utf-8")
            event_path = state_dir / "stage1.complete.json"
            event_path.write_text("{}", encoding="utf-8")
            state = {"cycle_id": "cycle", "current_ea": "EA", "current_stage": "stage2_pending",
                     "last_stage1_artifact": str(spec_path)}
            spec = {"stage2_runtime_policy": {}}
            shared = {"sha256": "S"}
            replay = {"source_text": "int OnInit(){return 0;}", "raw_proposal": "proposal.stdout.txt",
                      "prior_hold_sha256": "A" * 64}
            with mock.patch.object(executor, "ROOT", root), \
                 mock.patch.object(executor, "STATE", state_dir / "canonical_state.json"), \
                 mock.patch.object(executor, "CANDIDATES_DIR", state_dir / "candidates"), \
                 mock.patch("kcos_stage2_generate_source_v2.authenticate",
                            return_value=(state, spec, {}, event_path, minato_path)), \
                 mock.patch("kcos_stage2_generate_source_v2.sha", return_value="B" * 64), \
                 mock.patch("kcos_stage2_generate_source_v2.stage1_event_lineage_sha256", return_value="C" * 64), \
                 mock.patch("kcos_stage2_generate_source_v2.strategy_input_lock_manifest", return_value={}), \
                 mock.patch("kcos_input_contract_v2.discover_inputs", return_value=shared), \
                 mock.patch("kcos_tester_config_v2.resolve_input_spec", side_effect=lambda value, **kwargs: (value, {})), \
                 mock.patch("kcos_tester_config_v2.stage2_spec_from_spec", side_effect=lambda value: value), \
                 mock.patch("kcos_stage2_generate_source_v2.replay_prior_admissible_proposal", return_value=replay) as replay_mock:
                self.assertTrue(executor.prior_stage2_source_proposal_revalidates(state))
                self.assertEqual(1, replay_mock.call_count)
                replay_mock.return_value = None
                self.assertFalse(executor.prior_stage2_source_proposal_revalidates(state))

    def test_stale_stage2_requalification_marker_does_not_block_new_cycle(self):
        state = {"cycle_id": "cycle-new", "stage2_requalification": {"cycle_id": "cycle-old"}}
        self.assertFalse(executor.stage2_requalification_is_current_cycle(state))
        state["stage2_requalification"]["cycle_id"] = "cycle-new"
        self.assertTrue(executor.stage2_requalification_is_current_cycle(state))
    def test_authenticated_stage3_all_zero_trade_requires_fresh_bound_full_timeframe_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            decision_path = root / "decision.json"
            verification_path = root / "verification.json"
            decision = {"cycle_id": "cycle", "stage": 3, "ea": "EA",
                        "verdict": "REPAIR_REQUIRED",
                        "timeframes": {tf: {"zero_trades": True, "trades": 0,
                                            "max_trades": 0, "row_count": 17}
                                       for tf in ("M5", "M15", "H1")}}
            decision_path.write_text(json.dumps(decision), encoding="utf-8")
            decision_sha = hashlib.sha256(decision_path.read_bytes()).hexdigest().upper()
            verification = {"cycle_id": "cycle", "stage": 3, "decision": str(decision_path),
                            "decision_sha256": decision_sha, "passed": True,
                            "checks": {"identity": True, "metrics_recomputed": True}}
            verification_path.write_text(json.dumps(verification), encoding="utf-8")
            state = {"stage3_zero_trade_decision": str(decision_path),
                     "stage3_zero_trade_verification": str(verification_path)}
            evidence = executor.authenticated_stage3_zero_trade_repair(state, "cycle", "EA")
            self.assertEqual(decision_sha, evidence["decision_sha256"])
            self.assertEqual(hashlib.sha256(verification_path.read_bytes()).hexdigest().upper(),
                             evidence["verification_sha256"])

            decision["timeframes"]["H1"]["trades"] = 1
            decision_path.write_text(json.dumps(decision), encoding="utf-8")
            verification["decision_sha256"] = hashlib.sha256(decision_path.read_bytes()).hexdigest().upper()
            verification_path.write_text(json.dumps(verification), encoding="utf-8")
            with self.assertRaises(ValueError):
                executor.authenticated_stage3_zero_trade_repair(state, "cycle", "EA")

    def test_authenticated_stage3_early_zero_trade_repair_accepts_completed_m5_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            decision_path = root / "decision.json"
            verification_path = root / "verification.json"
            decision = {
                "cycle_id": "cycle", "stage": 3, "ea": "EA", "verdict": "REPAIR_REQUIRED",
                "repair_trigger": {"kind": "completed_timeframe_zero_trades", "timeframe": "M5"},
                "timeframes": {"M5": {"zero_trades": True, "trades": 0, "max_trades": 0, "row_count": 17}},
            }
            decision_path.write_text(json.dumps(decision), encoding="utf-8")
            decision_sha = hashlib.sha256(decision_path.read_bytes()).hexdigest().upper()
            verification_path.write_text(json.dumps({
                "cycle_id": "cycle", "stage": 3, "decision": str(decision_path),
                "decision_sha256": decision_sha, "passed": True,
                "checks": {"identity": True, "metrics_recomputed": True, "h1_rule": True},
            }), encoding="utf-8")
            evidence = executor.authenticated_stage3_zero_trade_repair({
                "stage3_zero_trade_decision": str(decision_path),
                "stage3_zero_trade_verification": str(verification_path),
            }, "cycle", "EA")
            self.assertEqual(decision_sha, evidence["decision_sha256"])

    def test_stage3_zero_trade_repair_rejects_cycle_or_verifier_mismatch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            decision_path = root / "decision.json"
            verification_path = root / "verification.json"
            decision = {"cycle_id": "other", "stage": 3, "ea": "EA", "verdict": "REPAIR_REQUIRED",
                        "timeframes": {tf: {"zero_trades": True, "trades": 0, "max_trades": 0, "row_count": 1}
                                       for tf in ("M5", "M15", "H1")}}
            decision_path.write_text(json.dumps(decision), encoding="utf-8")
            verification_path.write_text("{}", encoding="utf-8")
            with self.assertRaises(ValueError):
                executor.authenticated_stage3_zero_trade_repair(
                    {"stage3_zero_trade_decision": str(decision_path),
                     "stage3_zero_trade_verification": str(verification_path)}, "cycle", "EA")

    def test_compile_evidence_triggers_only_authenticated_missing_trade_include_rebuild(self):
        cycle = "cycle-c-trade-repair"
        ea = "ea_candidate"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            candidates = root / "state" / "candidates"
            candidates.mkdir(parents=True)
            source = candidates / f"{ea}.mq5"
            source.write_text("#include <minato_base_score_v2.mqh>\nCTrade trade;\n", encoding="utf-8")
            receipt_path = source.with_suffix(source.suffix + ".build.json")
            receipt = {"implementation_mode": "model_generated_source_admitted", "cycle_id": cycle,
                       "candidate_ea": ea, "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            log_path = root / "compile.log"
            log_path.write_text("error 149: 'CTrade' - unexpected token", encoding="utf-16")
            history_path = root / f"{cycle}.stage2.compile.json"
            history_path.write_text(json.dumps({"cycle_id": cycle, "attempts": [{"log": str(log_path)}]}),
                                    encoding="utf-8")
            latest_path = root / f"{cycle}.stage2.compile.latest.json"
            latest_path.write_text("{}", encoding="utf-8")

            def fake_run(_args):
                repaired = "#include <minato_base_score_v2.mqh>\n#include <Trade/Trade.mqh>\nCTrade trade;\n"
                source.write_text(repaired, encoding="utf-8")
                receipt["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
                receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

            with mock.patch.object(executor, "ROOT", root), \
                    mock.patch.object(executor, "CANDIDATES_DIR", candidates), \
                    mock.patch.object(executor, "run", side_effect=fake_run):
                self.assertTrue(executor.repair_missing_trade_include_from_compile_evidence(
                    cycle, ea, str(source), latest_path, "spec.json"))
            self.assertIn("#include <Trade/Trade.mqh>", source.read_text(encoding="utf-8"))
            archived = list((root / "state" / "diagnostics" / "stage2_source_repairs" / cycle).glob("*"))
            self.assertEqual(len(archived), 1)
            self.assertTrue((archived[0] / source.name).is_file())

    def test_load_accepts_string_path_from_cycle_state(self):
        with tempfile.TemporaryDirectory() as temp:
            artifact = Path(temp) / "artifact.json"
            artifact.write_text(json.dumps({"cycle_id": "cycle-a"}), encoding="utf-8")
            self.assertEqual(executor.load(str(artifact)), {"cycle_id": "cycle-a"})

    def test_executor_failure_is_held_and_never_eligible_for_rotation(self):
        cycle = "cycle-runtime-failure"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_dir = root / "state"
            (state_dir / "events").mkdir(parents=True)
            (state_dir / "verification").mkdir()
            (state_dir / "attempts").mkdir()
            canonical = state_dir / "kcos_cycle_state_v2.json"
            canonical.write_text(json.dumps({"cycle_id": cycle, "current_stage": "stage2_pending"}), encoding="utf-8")
            with mock.patch.object(executor, "ROOT", root):
                failure_path = executor.record_failure(
                    {"cycle_id": cycle, "current_ea": "FixtureEA"},
                    RuntimeError("'str' object has no attribute 'read_text'"),
                    traceback_text="Traceback (most recent call last):\nRuntimeError: fixture\n",
                )
            report = json.loads(failure_path.read_text(encoding="utf-8"))
            state_after = json.loads(canonical.read_text(encoding="utf-8"))
            self.assertFalse(report["rotation_eligible"])
            self.assertEqual(report["recovery_action"], "repair_stage2_failure")
            self.assertIn("Traceback", report["traceback"])
            self.assertEqual(state_after["next_action"], "repair_stage2_failure")

    def test_authenticated_source_hold_rotates_and_records_four_failures(self):
        cycle = "cycle-four-proposal-budget"
        ea = "FixtureDCC"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_dir = root / "state"
            (state_dir / "events").mkdir(parents=True)
            (state_dir / "verification").mkdir()
            (state_dir / "attempts").mkdir()
            spec = state_dir / "spec.json"
            spec.write_text('{"cycle_id":"cycle-four-proposal-budget"}\n', encoding="utf-8")
            stage1_event = state_dir / "stage1.complete.json"
            immutable_event = {"cycle_id": cycle, "stage": 1, "status": "complete",
                               "artifact": str(spec.resolve()),
                               "artifact_sha256": hashlib.sha256(spec.read_bytes()).hexdigest().upper()}
            consumed_event = {**immutable_event, "consumed": True,
                              "consumed_utc": "2026-01-01T00:01:00Z",
                              "consumed_action": "dispatch_stage2"}
            stage1_event.write_text(json.dumps(consumed_event), encoding="utf-8")
            stage1_event_hash = hashlib.sha256(json.dumps(
                immutable_event, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")).hexdigest().upper()
            mt5_root = root / "mt5"
            include = mt5_root / "MQL5" / "Include" / "minato_base_score_v2.mqh"
            include.parent.mkdir(parents=True)
            include.write_text("// approved score include\n", encoding="utf-8")
            include_hash = hashlib.sha256(include.read_bytes()).hexdigest().upper()
            approval = state_dir / "contracts" / "minato_base_score_approved_v2.json"
            approval.parent.mkdir(parents=True)
            approval.write_text(json.dumps({"approved": True, "score_version": "MINATO_BASE_SCORE_V2.0",
                                            "sha256": include_hash}), encoding="utf-8")
            generation = state_dir / "candidates" / ".generation" / cycle / "attempt-04"
            generation.mkdir(parents=True)
            hold_path = generation / "hold.json"
            diagnoses = [
                {"failure_kind": kind, "repair_scope": "source_proposal_only",
                 "failure_fingerprint": f"{index:064X}", "same_failure_count": 1,
                 "proposed_source_sha256": f"{index + 10:064X}"}
                for index, kind in enumerate(("INPUT_USAGE", "STRATEGY_INPUT_LOCK", "SOURCE_ADMISSION", "INPUT_USAGE"), 1)
            ]
            hold = {
                "cycle_id": cycle, "ea": ea, "source_spec": str(spec.resolve()),
                "source_spec_sha256": hashlib.sha256(spec.read_bytes()).hexdigest().upper(),
                "stage1_event_sha256": stage1_event_hash,
                "canonical_minato_sha256": include_hash, "phase": "proposal",
                "status": "MODEL_SOURCE_HOLD", "proposal_diagnoses": diagnoses,
                "proposal_limit": 4, "proposal_attempts": 4,
                "stop_reason": "PROPOSAL_BUDGET_EXHAUSTED",
            }
            hold_path.write_text(json.dumps(hold), encoding="utf-8")
            hold_hash = hashlib.sha256(hold_path.read_bytes()).hexdigest().upper()
            detail = ("authenticated source admission hold\n"
                      f"KCOS_SOURCE_HOLD_ARTIFACT={hold_path}\n"
                      f"KCOS_SOURCE_HOLD_SHA256={hold_hash}")
            canonical = state_dir / "kcos_cycle_state_v2.json"
            canonical.write_text(json.dumps({"cycle_id": cycle, "current_stage": "stage2_pending"}), encoding="utf-8")
            state = {"cycle_id": cycle, "current_ea": ea,
                     "last_stage1_artifact": str(spec), "last_stage1_event": str(stage1_event)}
            with mock.patch.object(executor, "ROOT", root), \
                    mock.patch.object(executor, "mt5_data_root", return_value=mt5_root):
                failure_path = executor.record_failure(state, RuntimeError(detail), traceback_text="trace")
            failure = json.loads(failure_path.read_text(encoding="utf-8"))
            self.assertEqual(failure["failure_kind"], "MODEL_SOURCE_PROPOSAL_BUDGET_EXHAUSTED")
            self.assertTrue(failure["source_diagnosis"]["proposal_budget_exhausted"])
            self.assertTrue(failure["rotation_eligible"])
            self.assertEqual(failure["recovery_action"], "rotate_candidate")
            receipts = list((state_dir / "diagnostics" / "stage2_failures").glob(
                f"{cycle}.source-proposal-exhaustion.*.json"))
            self.assertEqual(len(receipts), 1)
            receipt = json.loads(receipts[0].read_text(encoding="utf-8"))
            self.assertEqual(receipt["proposal_attempts"], 4)
            self.assertEqual(len(receipt["proposal_failures"]), 4)
            self.assertEqual(receipt["recovery_action"], "rotate_candidate")

    def test_repeated_failure_preserves_previous_failure_evidence(self):
        cycle = "cycle-repeated-failure"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_dir = root / "state"
            events = state_dir / "events"
            events.mkdir(parents=True)
            (state_dir / "verification").mkdir()
            (state_dir / "attempts").mkdir()
            canonical = state_dir / "kcos_cycle_state_v2.json"
            canonical.write_text(json.dumps({"cycle_id": cycle, "current_stage": "stage2_pending"}), encoding="utf-8")
            event = events / f"{cycle}.stage2.failed.json"
            original = json.dumps({"cycle_id": cycle, "error": "previous evidence"}, indent=2)
            event.write_text(original, encoding="utf-8")
            with mock.patch.object(executor, "ROOT", root):
                executor.record_failure({"cycle_id": cycle}, RuntimeError("new failure"), traceback_text="trace")
            archived = list((state_dir / "diagnostics" / "stage2_failures").glob("*.json"))
            self.assertEqual(len(archived), 1)
            self.assertEqual(archived[0].read_text(encoding="utf-8"), original)

    def test_executor_uses_cycle_artifact_and_skips_legacy_global_reconcile(self):
        cycle = "cycle-dcc-runtime"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_dir = root / "state"
            candidates = state_dir / "candidates"
            handoffs = state_dir / "handoffs"
            candidates.mkdir(parents=True)
            handoffs.mkdir()
            source = candidates / "FixtureDCC.mq5"
            source.write_text("// DCC_GARCH_IMPLEMENTATION_V1\n", encoding="utf-8")
            spec = state_dir / "fixture-spec.json"
            spec.write_text(json.dumps({"strategy": "DCC-GARCH", "cycle_id": cycle}), encoding="utf-8")
            state_path = state_dir / "kcos_cycle_state_v2.json"
            state_path.write_text(json.dumps({
                "cycle_id": cycle,
                "current_stage": "stage2_pending",
                "stage_status": "stage2_repair_required",
                "current_ea": "FixtureDCC",
                "candidate_ea": "FixtureDCC",
                "candidate_source": str(source),
                "last_stage1_artifact": str(spec),
            }), encoding="utf-8")
            with contextlib.ExitStack() as stack:
                stack.enter_context(mock.patch.object(executor, "ROOT", root))
                stack.enter_context(mock.patch.object(executor, "STATE", state_path))
                stack.enter_context(mock.patch.object(executor, "CANDIDATES_DIR", candidates))
                stack.enter_context(mock.patch.object(executor, "LEASE", state_dir / "leases" / "stage2.lock.json"))
                for name in ("PREPARE", "REPAIR_LOOP", "COMPILE", "VERIFY", "PUBLISH"):
                    stack.enter_context(mock.patch.object(executor, name, state_dir / f"{name.lower()}.py"))
                run_mock = stack.enter_context(mock.patch.object(executor, "run"))
                # This routing fixture mocks prepare/compile/verify rather than
                # writing their artifacts. Transaction evidence is exercised by
                # test_kcos_stage2_reconcile_v1, not these synthetic paths.
                stack.enter_context(mock.patch.object(
                    executor.implementation_reconcile, "ImplementationReconciliation"))
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(executor.main(), 0)
            executed = [str(call.args[0][0]) for call in run_mock.call_args_list]
            self.assertNotIn(str(state_dir / "kcos_stage2_reconcile_v2.py"), executed)
            self.assertTrue(any(path.endswith("prepare.py") for path in executed))


if __name__ == "__main__":
    unittest.main()
