import json
import hashlib
import sys
import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import kcos_stage2_build_ea_v1 as builder
from kcos_input_contract_v2 import discover_inputs
from kcos_runtime_paths_v2 import mt5_data_root
from kcos_stage2_generate_source_v2 import (ensure_trade_class_include,
                                            remove_unused_position_info_include,
                                            repair_local_regime_macro_namespace,
                                            replay_prior_admissible_proposal,
                                            stage1_event_lineage_sha256)
from kcos_stage2_verify_v2 import atr_uses_authenticated_closed_buffer


ROOT = Path(__file__).resolve().parent.parent
SPEC = ROOT / "state" / "specs" / "kcos-20261009T065915Z-research-new.stage1.spec.json"


class DccAdmissionRouteTests(unittest.TestCase):
    def _prior_candidate_fixture(self, root: Path, *, verdict="FAIL_ROTATE"):
        state_dir = root / "state"
        candidates = state_dir / "candidates"
        events = state_dir / "events"
        candidates.mkdir(parents=True)
        events.mkdir(parents=True)
        current_spec = state_dir / "current.stage1.spec.json"
        current_spec.write_text('{"cycle_id":"current"}', encoding="utf-8")
        state_path = state_dir / "kcos_cycle_state_v2.json"
        state_path.write_text("{}", encoding="utf-8")
        ea = "EA"
        source = candidates / f"{ea}.mq5"
        source.write_text("// prior candidate\n", encoding="utf-8")
        source_hash = hashlib.sha256(source.read_bytes()).hexdigest().upper()
        receipt = source.with_suffix(source.suffix + ".build.json")
        receipt.write_text(json.dumps({
            "cycle_id": "prior", "candidate_ea": ea, "output": str(source),
            "sha256": source_hash, "source_spec_sha256": "OLD-SPEC",
            "implementation_mode": "model_generated_source_admitted",
        }), encoding="utf-8")
        stage2_verification_path = state_dir / "prior.stage2.verification.json"
        stage2_verification_path.write_text(json.dumps({
            "cycle_id": "prior", "passed": True, "source_sha256": source_hash,
        }), encoding="utf-8")
        stage2_path = events / "prior.stage2.complete.json"
        stage2_path.write_text(json.dumps({
            "cycle_id": "prior", "stage": 2, "status": "complete", "ea": ea,
            "mq5_sha256": source_hash, "verification": str(stage2_verification_path),
            "verification_sha256": hashlib.sha256(
                stage2_verification_path.read_bytes()).hexdigest().upper(),
        }), encoding="utf-8")
        decision_path = state_dir / "prior.stage3.decision.json"
        decision_path.write_text(json.dumps({
            "cycle_id": "prior", "ea": ea, "verdict": verdict,
            "stage2_event_sha256": hashlib.sha256(stage2_path.read_bytes()).hexdigest().upper(),
        }), encoding="utf-8")
        verification_path = state_dir / "prior.stage3.verification.json"
        verification_path.write_text(json.dumps({
            "cycle_id": "prior", "passed": True,
            "decision_sha256": hashlib.sha256(decision_path.read_bytes()).hexdigest().upper(),
        }), encoding="utf-8")
        stage3_path = events / "prior.stage3.complete.json"
        stage3_path.write_text(json.dumps({
            "cycle_id": "prior", "stage": 3, "status": "complete",
            "verdict": verdict, "next_stage": "stage1_pending",
            "stage2_event_sha256": hashlib.sha256(stage2_path.read_bytes()).hexdigest().upper(),
            "decision": str(decision_path),
            "decision_sha256": hashlib.sha256(decision_path.read_bytes()).hexdigest().upper(),
            "verification": str(verification_path),
            "verification_sha256": hashlib.sha256(verification_path.read_bytes()).hexdigest().upper(),
        }), encoding="utf-8")
        args = SimpleNamespace(candidates_dir=candidates, state=state_path,
                               spec=current_spec, cycle_id="current")
        return args, source, receipt

    def test_prior_candidate_archives_only_after_verified_fail_rotate(self):
        with tempfile.TemporaryDirectory() as directory:
            args, source, receipt = self._prior_candidate_fixture(Path(directory))
            self.assertIsNone(builder._validated_prior_candidate(args, "EA"))
            archive_dir = (args.candidates_dir / ".generation" / "current"
                           / "prior_candidates" / "prior")
            manifest = json.loads((archive_dir / "EA.archive.json").read_text(encoding="utf-8"))
            self.assertEqual("PRIOR_CANDIDATE_ARCHIVED_AFTER_AUTHENTICATED_ROTATION", manifest["status"])
            self.assertEqual(manifest["source_sha256"], hashlib.sha256(
                (archive_dir / "EA.mq5").read_bytes()).hexdigest().upper())
            self.assertFalse(source.exists())
            self.assertFalse(receipt.exists())

    def test_prior_candidate_remains_when_rotation_evidence_does_not_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            args, source, receipt = self._prior_candidate_fixture(Path(directory), verdict="PASS")
            with self.assertRaises(builder.Stage2BuildContractError) as raised:
                builder._validated_prior_candidate(args, "EA")
            self.assertEqual("STAGE2_CANDIDATE_COLLISION_HOLD", raised.exception.failure_kind)
            self.assertTrue(source.is_file())
            self.assertTrue(receipt.is_file())

    def test_same_cycle_source_reuses_exact_admission_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            args, source, receipt = self._prior_candidate_fixture(Path(directory))
            admission_path = (args.candidates_dir / ".generation" / "current"
                              / "attempt" / "source-admission-receipt.json")
            admission_path.parent.mkdir(parents=True)
            source.write_text("// same-cycle candidate\n", encoding="utf-8")
            source_hash = hashlib.sha256(source.read_bytes()).hexdigest().upper()
            proof = {"cycle_id": "current", "ea": "EA",
                     "source_spec_sha256": hashlib.sha256(args.spec.read_bytes()).hexdigest().upper(),
                     "source_sha256": source_hash, "passed": True,
                     "status": "DETERMINISTIC_SOURCE_ADMISSION"}
            admission_path.write_text(json.dumps(proof), encoding="utf-8")
            receipt.write_text(json.dumps({
                "cycle_id": "current", "candidate_ea": "EA", "output": str(source),
                "sha256": source_hash, "source_spec_sha256": proof["source_spec_sha256"],
                "implementation_mode": "model_generated_source_admitted",
                "implementation_admission": {
                    "path": str(admission_path),
                    "sha256": hashlib.sha256(admission_path.read_bytes()).hexdigest().upper(),
                },
            }), encoding="utf-8")
            text, admission = builder._validated_prior_candidate(args, "EA")
            self.assertEqual("// same-cycle candidate\n", text)
            self.assertEqual(str(admission_path.resolve()), admission["path"])
            self.assertTrue(source.is_file())

    def test_stage1_event_lineage_ignores_only_supervisor_consumption_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            event_path = Path(directory) / "stage1.complete.json"
            event = {"cycle_id": "cycle", "stage": 1, "status": "complete",
                     "artifact_sha256": "A" * 64}
            event_path.write_text(json.dumps(event), encoding="utf-8")
            original = stage1_event_lineage_sha256(event_path)
            consumed = {**event, "consumed": True, "consumed_utc": "2026-10-10T00:00:00Z",
                        "consumed_action": "dispatch_stage2"}
            event_path.write_text(json.dumps(consumed), encoding="utf-8")
            self.assertEqual(original, stage1_event_lineage_sha256(event_path))
            consumed["artifact_sha256"] = "B" * 64
            event_path.write_text(json.dumps(consumed), encoding="utf-8")
            self.assertNotEqual(original, stage1_event_lineage_sha256(event_path))

    def test_c_trade_use_gets_standard_include_without_strategy_changes(self):
        source = "#include <minato_base_score_v2.mqh>\nCTrade trade;\n"
        repaired, records = ensure_trade_class_include(source)
        self.assertIn("#include <Trade/Trade.mqh>", repaired)
        self.assertEqual(source.count("CTrade trade;"), repaired.count("CTrade trade;"))
        self.assertEqual(records[0]["kind"], "standard_trade_include_added")

    def test_existing_trade_include_is_left_unchanged(self):
        source = "#include <Trade/Trade.mqh>\nCTrade trade;\n"
        repaired, records = ensure_trade_class_include(source)
        self.assertEqual(source, repaired)
        self.assertEqual([], records)

    def test_only_unused_position_info_include_is_removed(self):
        source = "#include <Trade\\PositionInfo.mqh>\nvoid OnTick() {}\n"
        repaired, records = remove_unused_position_info_include(source)
        self.assertNotIn("PositionInfo.mqh", repaired)
        self.assertEqual(records[0]["kind"], "unused_position_info_include_removed")
        used = source + "CPositionInfo position;\n"
        unchanged, records = remove_unused_position_info_include(used)
        self.assertEqual(used, unchanged)
        self.assertEqual([], records)

    def test_dcc_without_exact_cycle_contract_routes_to_authenticated_model_lane(self):
        spec = json.loads(SPEC.read_text(encoding="utf-8-sig"))
        contract = ROOT / "state" / "implementation_contracts" / f"{spec['cycle_id']}.dcc-garch.v1.json"
        self.assertFalse(contract.exists())
        with self.assertRaises(builder.Stage2BuildContractError) as raised:
            builder.require_supported_research_implementation(spec)
        self.assertEqual(raised.exception.failure_kind, "STAGE2_IMPLEMENTATION_UNSUPPORTED")
        self.assertIn("authenticated model source admission", str(raised.exception))

    def test_atr_closed_buffer_gate_accepts_authenticated_stage1_input_name(self):
        source = """
        int g_atr = iATR(_Symbol, _Period, InpAtrPeriod);
        double values[];
        CopyBuffer(g_atr, 0, 1, 1, values);
        """
        self.assertTrue(atr_uses_authenticated_closed_buffer(source, "InpAtrPeriod"))
        forming_bar_source = source.replace("CopyBuffer(g_atr, 0, 1, 1, values)",
                                           "CopyBuffer(g_atr, 0, 0, 1, values)")
        self.assertFalse(atr_uses_authenticated_closed_buffer(forming_bar_source, "InpAtrPeriod"))

    def test_atr_gate_accepts_only_a_shift_one_buffer_wrapper(self):
        source = """
        int atrHandle = iATR(_Symbol, _Period, InpAtrPeriod);
        double ReadShift1(int handle, int buffer) {
            double values[1];
            if (CopyBuffer(handle, buffer, 1, 1, values) != 1) return 0.0;
            return values[0];
        }
        double atr = ReadShift1(atrHandle, 0);
        """
        self.assertTrue(atr_uses_authenticated_closed_buffer(source, "InpAtrPeriod"))
        forming = source.replace("CopyBuffer(handle, buffer, 1, 1, values)",
                                 "CopyBuffer(handle, buffer, 0, 1, values)")
        self.assertFalse(atr_uses_authenticated_closed_buffer(forming, "InpAtrPeriod"))

    def test_prior_proposal_replay_requires_exact_hash_and_same_lineage(self):
        keys = ("cycle_id", "ea", "source_spec_sha256", "stage1_event_sha256",
                "canonical_minato_sha256", "effective_spec_sha256",
                "input_resolution_sha256", "strategy_input_lock_sha256",
                "shared_input_contract_sha256")
        bound = {key: f"{key}-value" for key in keys}
        source = "int AuthenticatedProposal() { return 1; }"
        proposal_raw = f"```mql5\n{source}\n```\n"
        proposal_hash = hashlib.sha256(source.encode("utf-8")).hexdigest().upper()
        with tempfile.TemporaryDirectory() as directory:
            history = Path(directory)
            attempt = history / "same-cycle-attempt"
            attempt.mkdir()
            (attempt / "hold.json").write_text(json.dumps({
                **bound, "status": "MODEL_SOURCE_HOLD", "phase": "proposal",
            }), encoding="utf-8")
            raw_path = attempt / "proposal.stdout.txt"
            raw_path.write_text(proposal_raw, encoding="utf-8")
            (attempt / "proposal-rejection-1.json").write_text(json.dumps({
                "raw_proposal": str(raw_path),
                "diagnosis": {"proposed_source_sha256": proposal_hash},
            }), encoding="utf-8")
            with mock.patch("kcos_stage2_generate_source_v2.prepare_source_for_admission",
                            return_value=("validated source", [], {"InpA": {"owner": "ea_source"}})):
                replay = replay_prior_admissible_proposal(history, bound, "EA", {}, {})
                self.assertEqual("validated source", replay["source_text"])
                self.assertEqual("same-cycle-attempt", replay["prior_attempt"])

                changed_lineage = {**bound, "source_spec_sha256": "changed"}
                self.assertIsNone(replay_prior_admissible_proposal(
                    history, changed_lineage, "EA", {}, {}))
                (attempt / "proposal-rejection-1.json").write_text(json.dumps({
                    "raw_proposal": str(raw_path),
                    "diagnosis": {"proposed_source_sha256": "0" * 64},
                }), encoding="utf-8")
                self.assertIsNone(replay_prior_admissible_proposal(history, bound, "EA", {}, {}))

    def test_local_regime_namespace_repair_preserves_values_and_references(self):
        include = mt5_data_root() / "MQL5" / "Include" / "minato_base_score_v2.mqh"
        shared = discover_inputs({str(include): include.read_text(encoding="utf-8-sig")})
        source = "#define MINATO_REGIME_GREED 1\nint active = MINATO_REGIME_GREED;\n"
        repaired, records = repair_local_regime_macro_namespace(source, shared)
        self.assertIn("#define KCOS_REGIME_GREED 1", repaired)
        self.assertIn("active = KCOS_REGIME_GREED", repaired)
        self.assertEqual(1, len(records))
        self.assertEqual({"MINATO_REGIME_GREED": "KCOS_REGIME_GREED"},
                         records[0]["renamed_identifiers"])


if __name__ == "__main__":
    unittest.main()
