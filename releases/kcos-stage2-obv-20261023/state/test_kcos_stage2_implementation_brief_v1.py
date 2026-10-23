from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from kcos_stage2_implementation_brief_v1 import (resolve_implementation_brief,
                                                render_implementation_guidance)


def signed_volume_spec():
    return {
        "ea": "AnyCandidateName",
        "strategy": {
            "family": "PivotPrice_SignedTickVolume_Divergence",
            "execution_contract": {
                "closed_bar_policy": {"signal_shift": 1, "allow_forming_bar": False},
                "series": [
                    {"id": "custom_signed_tick_volume", "bar_shift": 1,
                     "equation": "Cumulative sum: +tick_volume[i] if close[i]>open[i], -tick_volume[i] if close[i]<open[i], unchanged if equal; source physically reverses the array and then sets series indexing."},
                    {"id": "ema_trend", "bar_shift": 1,
                     "equation": "EMA(close,InpEmaTrendPeriod) on PERIOD_CURRENT; source copies from start position 1 for three values and reads array index 1."},
                    {"id": "atr", "bar_shift": 1,
                     "equation": "ATR(InpAtrPeriod) on PERIOD_CURRENT; source copies from start position 0 for three values and reads array index 1."},
                    {"id": "h1_regime", "bar_shift": 1,
                     "equation": "CKiyosakiRegime classification from up to 60 completed H1 highs, lows, and closes; the legacy source currently requests shift 0, which Stage 2 must change to closed data."},
                ],
                "signals": {"long": "close[1]>EMA[1]", "short": "close[1]<EMA[1]",
                            "conflict_policy": "Do not enter if both directions qualify, cooldown has not elapsed, volume is invalid."},
                "execution": {"cooldown": "Require InpMinBarsBetweenTrades since last trade.",
                              "margin_check": "Require MinatoAllowsProjectedMargin."},
                "regimes": {"classification": "CKiyosakiRegime", "multipliers": [1, 2, 3]},
                "exits": {"stop_loss": "ATR[1]*InpAtrSlMult*selected regime SL multiplier."},
                "input_bindings": {"InpMinBarsBetweenTrades": ["execution"], "InpLots": ["execution"]},
            },
        },
        "stage2_requirements": {"note": "Verify EMA/ATR copy alignment and whether to substitute conventional OBV."},
        "tester_config": {"inputs": {"InpLots": {"start": 0.1, "stop": 0.5}}},
    }


class ImplementationBriefTests(unittest.TestCase):
    def test_resolves_actual_legacy_observation_conflicts_without_new_strategy(self):
        spec = signed_volume_spec()
        brief = resolve_implementation_brief(spec)
        contract = brief["execution_contract"]
        equations = {item["id"]: item["equation"] for item in contract["series"]}
        self.assertEqual("EMA(close,InpEmaTrendPeriod) on PERIOD_CURRENT.", equations["ema_trend"])
        self.assertNotIn("physically reverses", equations["custom_signed_tick_volume"])
        self.assertIn("close[i]>open[i]", equations["custom_signed_tick_volume"])
        self.assertNotIn("shift 0", equations["h1_regime"])
        self.assertTrue(all(item["bar_shift"] == 1 for item in contract["series"]))
        self.assertNotIn("cooldown has not elapsed", contract["signals"]["conflict_policy"])
        self.assertNotIn("InpMinBarsBetweenTrades", contract["input_bindings"])
        original = spec["strategy"]["execution_contract"]
        for key in ("regimes", "exits"):
            self.assertEqual(original[key], contract[key])
        self.assertEqual(original["execution"]["margin_check"], contract["execution"]["margin_check"])
        resolutions = {item["id"]: item["instruction"] for item in brief["canonical_resolutions"]}
        self.assertIn("Do not substitute conventional", resolutions["signed_volume_identity"])
        self.assertIn("incremental", resolutions["incremental_history"])
        self.assertIn("bounded rolling", resolutions["incremental_history"])
        self.assertIn("history reset", resolutions["incremental_history"])
        self.assertEqual({"volume_sign_rule": "close_vs_open", "signal_shift": 1,
                          "h1_regime_shift": 1}, brief["resolved_intent"])
        self.assertEqual(5, len(brief["legacy_observations"]))

    def test_original_evidence_and_nested_spec_are_immutable(self):
        spec = signed_volume_spec()
        original = deepcopy(spec)
        expected_hash = hashlib.sha256(json.dumps(spec, sort_keys=True, ensure_ascii=False,
                                                   separators=(",", ":")).encode()).hexdigest().upper()
        brief = resolve_implementation_brief(spec)
        self.assertEqual(expected_hash, brief["source_spec_canonical_sha256"])
        brief["execution_contract"]["regimes"]["multipliers"].append(5)
        brief["stage1_implementation_context"]["stage2_requirements"]["note"] = "changed"
        self.assertEqual(original, spec)
        self.assertIn("whether to substitute", render_implementation_guidance(spec))
        self.assertIn("not instructions to preserve defective", render_implementation_guidance(spec))

    def test_other_family_and_name_only_lookalike_retain_contract_exactly(self):
        spec = signed_volume_spec()
        spec["ea"] = "OBV_Divergence"
        spec["strategy"]["family"] = "Conventional_OBV"
        expected = deepcopy(spec["strategy"]["execution_contract"])
        brief = resolve_implementation_brief(spec)
        self.assertEqual(expected, brief["execution_contract"])
        self.assertEqual([], brief["canonical_resolutions"])
        self.assertEqual([], brief["legacy_observations"])

    def test_top_level_contract_and_missing_contract(self):
        spec = {"execution_contract": {"signals": {"long": "x>y"}}}
        self.assertEqual(spec["execution_contract"], resolve_implementation_brief(spec)["execution_contract"])
        with self.assertRaisesRegex(ValueError, "execution contract is missing"):
            resolve_implementation_brief({})

    def test_known_family_with_changed_approved_timing_is_not_reinterpreted(self):
        spec = signed_volume_spec()
        spec["strategy"]["execution_contract"]["series"][1]["bar_shift"] = 2
        contract = deepcopy(spec["strategy"]["execution_contract"])
        brief = resolve_implementation_brief(spec)
        self.assertEqual(contract, brief["execution_contract"])
        self.assertEqual({}, brief["resolved_intent"])
        self.assertEqual([], brief["canonical_resolutions"])

    def test_changed_signal_shift_keeps_original_contract(self):
        spec = signed_volume_spec()
        spec["strategy"]["execution_contract"]["closed_bar_policy"]["signal_shift"] = 2
        contract = deepcopy(spec["strategy"]["execution_contract"])
        brief = resolve_implementation_brief(spec)
        self.assertEqual(contract, brief["execution_contract"])
        self.assertEqual({}, brief["resolved_intent"])

    def test_changed_same_family_mathematics_is_never_reinterpreted(self):
        original_equation = signed_volume_spec()["strategy"]["execution_contract"]["series"][0]["equation"]
        changes = (
            original_equation.replace("+tick_volume[i]", "+2*tick_volume[i]"),
            original_equation.replace("+tick_volume[i]", "-tick_volume[i]").replace(
                "-tick_volume[i] if close[i]<open[i]", "+tick_volume[i] if close[i]<open[i]"),
            original_equation.replace("unchanged if equal", "+tick_volume[i] if equal"),
            original_equation.replace("unchanged if equal", "unchanged if equal except add 1"),
            original_equation + " Scale all values by 2.",
        )
        for equation in changes:
            with self.subTest(equation=equation):
                spec = signed_volume_spec()
                spec["strategy"]["execution_contract"]["series"][0]["equation"] = equation
                expected = deepcopy(spec["strategy"]["execution_contract"])
                brief = resolve_implementation_brief(spec)
                self.assertEqual(expected, brief["execution_contract"])
                self.assertEqual({}, brief["resolved_intent"])
                self.assertEqual([], brief["canonical_resolutions"])

    def test_audited_equation_allows_whitespace_and_optional_known_legacy_suffix(self):
        original_equation = signed_volume_spec()["strategy"]["execution_contract"]["series"][0]["equation"]
        for equation in (original_equation.replace(" ", "\t\n"),
                         original_equation.split("; source", 1)[0],
                         original_equation.split("; source", 1)[0] + "."):
            with self.subTest(equation=equation):
                spec = signed_volume_spec()
                spec["strategy"]["execution_contract"]["series"][0]["equation"] = equation
                self.assertEqual("close_vs_open", resolve_implementation_brief(spec)["resolved_intent"]["volume_sign_rule"])

    def test_creation_prompt_uses_the_exact_shared_rendered_instruction(self):
        import kcos_stage2_generate_source_v2 as generator
        spec = signed_volume_spec()
        instruction = render_implementation_guidance(spec)
        self.assertTrue(generator.authenticated_contract_guidance("prefix", spec, {}).endswith(instruction))

    def test_repair_prompt_uses_the_exact_shared_rendered_instruction(self):
        import kcos_stage2_repair_loop_v1 as repair
        spec = signed_volume_spec()
        captured = {}

        def fake_run(command, **kwargs):
            captured["prompt"] = command[-1]
            return SimpleNamespace(returncode=0, stdout='{"patch":"","diagnosis":"no defect"}', stderr="")

        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / "AnyCandidateName.mq5"
            source.write_text("void OnTick() {}", encoding="utf-8")
            spec_path = Path(folder) / "stage1.spec.json"
            spec_path.write_text(json.dumps(spec), encoding="utf-8")
            handoff = {"stage1_spec": str(spec_path), "stage1_spec_sha256": repair.sha(spec_path)}
            with patch.object(repair, "_ACTIVE_EPISODE", None), \
                 patch.object(repair.subprocess, "run", side_effect=fake_run):
                result = repair.ai_proposal(source, "isolated.mq5: error 100: sample", [], handoff=handoff)
        self.assertIn(render_implementation_guidance(spec), captured["prompt"])
        self.assertEqual("PROPOSED", result["status"])
        self.assertIn("Apply edits only to the source lines that directly establish a named failed check", captured["prompt"])


if __name__ == "__main__":
    unittest.main()
