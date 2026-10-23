import copy
import unittest

from kcos_tester_config_v2 import (apply_common_sense_domains,
    validate_executable_runtime_bounds, validate_config_against_source,
    validate_config, stage2_config_from_spec, stage2_spec_from_spec,
    stage3_config_from_spec, is_timing_control_input,
    finalize_stage2_optimization_domains)


SOURCE = """
input double InpYzHighPct = 75.0;
void OnInit() {
  if (InpYzHighPct < 0.0 || InpYzHighPct > 100.0) return;
}
"""


class ExecutableRuntimeBoundsTests(unittest.TestCase):
    def config(self, stop):
        return {"inputs": {"InpYzHighPct": {
            "default": 75.0, "start": 45.0, "step": 12.0,
            "stop": stop, "optimize": "Y"
        }}}

    def test_rejects_range_beyond_executable_domain(self):
        with self.assertRaisesRegex(ValueError, "exceeds executable runtime domain"):
            validate_executable_runtime_bounds(self.config(105.0), SOURCE)

    def test_accepts_range_inside_executable_domain(self):
        result = validate_executable_runtime_bounds(self.config(99.0), SOURCE)
        self.assertEqual(result["runtime_bounds_checked"], ["InpYzHighPct"])

    def test_common_sense_domain_fallback_is_bounded_and_provenanced(self):
        source = "input int InpTELookback = 20;\\nvoid OnTick(){ int x=InpTELookback; if(InpTELookback < 2 || InpTELookback > 200) return; }"
        config = {"schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
                  "from_date": "2020.01.01", "to_date": "2021.01.01",
                  "inputs": {"InpTELookback": {"default": 20, "start": 1, "step": 1, "stop": 2, "optimize": "Y"}}}
        self.assertEqual(apply_common_sense_domains(config, source), ["InpTELookback"])
        item = config["inputs"]["InpTELookback"]
        self.assertEqual(item["domain_provenance"]["method"], "common_sense")
        self.assertTrue(2 <= item["start"] < item["stop"] <= 200)

    def test_stage2_owns_final_step_count_from_defaults_not_stage1_search_hints(self):
        source = """input double InpRatio = 2.5; // [lo=2,hi=3,step=0.5]
input int InpLookback = 20; // [lo=18,hi=22,step=1]
void OnInit() {
  if (InpRatio < 0.0 || InpRatio > 10.0) return;
  if (InpLookback < 1 || InpLookback > 100) return;
}
void OnTick() { double x = InpRatio; int y = InpLookback; }
"""
        spec = {"ea": "B1_1_Test", "tester_config": {
            "schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
            "from_date": "2025.01.10", "to_date": "2026.01.10", "input_constraints": [],
            "inputs": {
                "InpRatio": {"type": "double", "default": 2.5, "start": 2.0,
                             "step": 0.5, "stop": 3.0, "optimize": "Y"},
                "InpLookback": {"type": "int", "default": 20, "start": 18,
                                "step": 1, "stop": 22, "optimize": "Y"},
            }}}
        original = copy.deepcopy(spec)
        config = stage2_config_from_spec(spec, source_text=source)
        self.assertEqual(spec, original)
        for name, item in config["inputs"].items():
            count = round((item["stop"] - item["start"]) / item["step"])
            self.assertGreaterEqual(count, 15, name)
            self.assertLessEqual(count, 20, name)
            self.assertEqual("stage2_owned_optimization_grid", item["domain_provenance"]["method"])
            self.assertEqual(original["tester_config"]["inputs"][name]["default"], item["default"])
            self.assertEqual("hint_only", item["domain_provenance"]["stage1_bounds_role"])
            default_index = (item["default"] - item["start"]) / item["step"]
            self.assertAlmostEqual(default_index, round(default_index))
        self.assertEqual((1.25, 3.75),
                         (config["inputs"]["InpRatio"]["start"], config["inputs"]["InpRatio"]["stop"]))
        self.assertEqual((10, 30),
                         (config["inputs"]["InpLookback"]["start"], config["inputs"]["InpLookback"]["stop"]))

    def test_stage2_fails_closed_when_integer_domain_cannot_supply_minimum_values(self):
        source = "input int InpSmall = 5;\nvoid OnInit(){ if(InpSmall < 1 || InpSmall > 10) return false; }\nvoid OnTick(){ int x=InpSmall; }"
        spec = {"tester_config": {
            "schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
            "from_date": "2025.01.10", "to_date": "2026.01.10", "input_constraints": [],
            "inputs": {"InpSmall": {"type": "int", "default": 5, "start": 1,
                                     "step": 1, "stop": 10, "optimize": "Y"}}}}
        with self.assertRaisesRegex(ValueError, "only 9 distinct values"):
            stage2_config_from_spec(spec, source_text=source)

    def test_stage1_source_comment_bounds_are_hints_and_zero_scale_fails_closed(self):
        source = "input double InpRatio = 2.0; // [lo=1.9,hi=2.1,step=0.05]\nvoid OnTick(){double x=InpRatio;}"
        spec = {"tester_config": {"schema_version": 1, "symbol": "ETHUSD",
            "timeframes": ["M5", "M15", "H1"], "from_date": "2025.01.10",
            "to_date": "2026.01.10", "input_constraints": [], "inputs": {
                "InpRatio": {"type": "double", "default": 2.0, "start": 1.9,
                             "step": 0.05, "stop": 2.1, "optimize": "Y"}}}}
        config = stage2_config_from_spec(spec, source_text=source)
        self.assertEqual(1.0, config["inputs"]["InpRatio"]["start"])
        self.assertEqual(3.0, config["inputs"]["InpRatio"]["stop"])
        self.assertEqual([1.9, 0.05, 2.1],
                         config["inputs"]["InpRatio"]["domain_provenance"]["source_comment_domain_hint"])
        zero_source = "input double InpOffset = 0.0; // [-1..1] step 0.1\nvoid OnTick(){double x=InpOffset;}"
        zero_spec = copy.deepcopy(spec)
        zero_spec["tester_config"]["inputs"] = {"InpOffset": {
            "type": "double", "default": 0.0, "start": -1.0,
            "step": 0.1, "stop": 1.0, "optimize": "Y"}}
        with self.assertRaisesRegex(ValueError, "ranges are advisory only"):
            stage2_config_from_spec(zero_spec, source_text=zero_source)

    def test_stage2_finalizes_nineteen_inputs_to_fifteen_to_twenty_mt5_steps(self):
        inputs = {}
        declarations = []
        for index, (name, kind, default) in enumerate((
                ("InpObvDivLookback", "int", 25), ("InpPivotStrength", "int", 2),
                ("InpEmaTrendPeriod", "int", 50), ("InpAtrPeriod", "int", 14),
                ("InpAtrSlMult", "double", 2.0), ("InpAtrTpMult", "double", 3.0),
                ("InpLots", "double", 0.5))):
            inputs[name] = {"type": kind, "default": default, "start": default * 0.9,
                            "step": 0.1, "stop": default * 1.1, "optimize": "Y"}
            declarations.append(f"input {kind} {name} = {default};")
        regime_names = [f"InpRegime_{label}_{field}" for label in
                        ("Greed", "Neutral", "Fear", "ExtFear") for field in
                        ("Lot", "SLMult", "TPMult")]
        for name in regime_names:
            inputs[name] = {"type": "double", "default": 1.0, "start": 0.25,
                            "step": 0.25, "stop": 1.0, "optimize": "Y"}
            declarations.append(f"input double {name} = 1.0;")
        source = "\n".join(declarations) + "\nvoid OnTick(){\n" + "\n".join(
            f"double v{index}={name};" for index, name in enumerate(inputs)) + "\n}"
        spec = {"ea": "B1_1_OBV_Divergence_v1", "tester_config": {
            "schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
            "from_date": "2025.01.10", "to_date": "2026.01.10", "input_constraints": [],
            "inputs": inputs}}
        config = stage2_config_from_spec(spec, source_text=source)
        optimized = {name: item for name, item in config["inputs"].items()
                     if item.get("optimize") == "Y"}
        self.assertEqual(19, len(optimized))
        for name, item in optimized.items():
            count = round((item["stop"] - item["start"]) / item["step"])
            self.assertTrue(15 <= count <= 20, (name, count))
            self.assertEqual(inputs[name]["default"], item["default"], name)

    def test_stage2_timing_controls_are_zero_and_never_optimized_without_mutating_stage1(self):
        spec = {"tester_config": {"schema_version": 1, "symbol": "ETHUSD",
            "timeframes": ["M5", "M15", "H1"], "from_date": "2025.01.10",
            "to_date": "2026.01.10", "input_constraints": [
                {"id": "cooldown_nonnegative", "coefficients": {"InpMinBarsBetweenTrades": 1},
                 "operator": ">=", "limit": 0},
            ], "inputs": {
            "InpMaxTradeBars": {"type": "int", "default": 48, "start": 12,
                                "step": 4, "stop": 200, "optimize": "Y",
                                "description": "maximum holding time stop"},
            "InpMinBarsBetweenTrades": {"type": "int", "default": 12, "start": 1,
                                        "step": 1, "stop": 96, "optimize": "Y"},
            "InpRegime_Greed_Lot_Mult": {"type": "double", "default": 1.0,
                                          "start": 0, "step": 0.25, "stop": 5,
                                          "optimize": "Y"},
        }}, "strategy": {"parameters": [
            {"name": "InpMaxTradeBars", "default": 48},
            {"name": "InpMinBarsBetweenTrades", "default": 12},
        ]}}
        original = copy.deepcopy(spec)
        effective = stage2_spec_from_spec(spec)
        self.assertEqual(spec, original)
        self.assertEqual(48, spec["tester_config"]["inputs"]["InpMaxTradeBars"]["default"])
        self.assertEqual(12, spec["tester_config"]["inputs"]["InpMinBarsBetweenTrades"]["default"])
        name = "InpMaxTradeBars"
        self.assertEqual({"default": 0, "start": 0, "step": 0, "stop": 0,
                          "optimize": "N"},
                         {key: effective["tester_config"]["inputs"][name][key]
                          for key in ("default", "start", "step", "stop", "optimize")})
        self.assertNotIn("InpMinBarsBetweenTrades", effective["tester_config"]["inputs"])
        self.assertEqual(0, effective["strategy"]["parameters"][0]["default"])
        self.assertEqual(["InpMaxTradeBars"], [row["name"] for row in effective["strategy"]["parameters"]])
        self.assertEqual("Y", effective["tester_config"]["inputs"]["InpRegime_Greed_Lot_Mult"]["optimize"])
        self.assertEqual([], effective["tester_config"]["input_constraints"])
        self.assertEqual("cooldown_nonnegative",
                         effective["tester_config"]["timing_control_policy"][
                             "removed_input_constraints"][0]["id"])

    def test_mixed_constraint_keeps_non_timing_rule_after_zeroing_control(self):
        spec = {"tester_config": {"schema_version": 1, "symbol": "ETHUSD",
            "timeframes": ["M5", "M15", "H1"], "from_date": "2025.01.10", "to_date": "2026.01.10",
            "inputs": {
                "InpMinBarsBetweenTrades": {"default": 12, "start": 1, "step": 1,
                    "stop": 96, "optimize": "Y"},
                "InpRegime_Greed_Lot_Mult": {"default": 1, "start": 0,
                    "step": 0.25, "stop": 5, "optimize": "Y"},
            },
            "input_constraints": [{"id": "regime_and_cooldown",
                "coefficients": {"InpMinBarsBetweenTrades": 2,
                                 "InpRegime_Greed_Lot_Mult": 1},
                "operator": ">=", "limit": 1}]}}
        config = stage2_config_from_spec(spec)
        self.assertEqual([{"id": "regime_and_cooldown",
            "coefficients": {"InpRegime_Greed_Lot_Mult": 1},
            "operator": ">=", "limit": 1}], config["input_constraints"])
        self.assertEqual([], config["timing_control_policy"]["removed_input_constraints"])

    def test_stage2_owns_regime_lot_optimizer_policy_and_stage3_is_identity(self):
        names = ["InpRegime_Greed_Lot_Mult", "InpRegime_Neutral_Lot_Mult",
                 "InpRegime_Fear_Lot_Mult", "InpRegime_ExtFear_Lot_Mult"]
        inputs = {name: {"type": "double", "default": 1.0, "start": 0.0,
                         "step": 0.25, "stop": 5.0, "optimize": "Y"}
                  for name in names}
        constraints = [{"id": f"ordered_{left}_{right}", "operator": ">=", "limit": 0,
                        "coefficients": {left: 1, right: -1}}
                       for left, right in zip(names, names[1:])]
        spec = {"tester_config": {"schema_version": 1, "symbol": "ETHUSD",
            "timeframes": ["M5", "M15", "H1"], "from_date": "2025.01.10",
            "to_date": "2026.01.10", "inputs": inputs,
            "input_constraints": constraints}}

        stage2 = stage2_config_from_spec(spec)
        finalize_stage2_optimization_domains(stage2, "", ea_name="regime-lot-test")
        stage3 = stage3_config_from_spec(spec)
        finalize_stage2_optimization_domains(stage3, "", ea_name="regime-lot-test")
        self.assertEqual([], stage2["input_constraints"])
        self.assertEqual("independent_regime_lot_multipliers",
                         stage2["optimizer_policy_overrides"][0]["policy"])
        self.assertEqual(stage2, stage3)
        for name in names:
            item = stage2["inputs"][name]
            self.assertEqual("stage2_owned_optimization_grid",
                             item["domain_provenance"]["method"])
            steps = round((item["stop"] - item["start"]) / item["step"])
            self.assertTrue(15 <= steps <= 20, (name, steps))

    def test_timing_control_policy_applies_to_stage2_and_stage3_contracts(self):
        spec = {"tester_config": {"schema_version": 1, "symbol": "ETHUSD",
            "timeframes": ["M5", "M15", "H1"], "from_date": "2025.01.10",
            "to_date": "2026.01.10", "input_constraints": [], "inputs": {
            "InpPositionDurationLimit": {"type": "int", "default": 10, "start": 1,
                                          "step": 1, "stop": 20, "optimize": "Y",
                                          "description": "maximum holding period"},
            "InpEntryCoolingDown": {"type": "int", "default": 4, "start": 1,
                                     "step": 1, "stop": 10, "optimize": "Y"},
        }}}
        for config_fn in (stage2_config_from_spec, stage3_config_from_spec):
            config = config_fn(spec)
            self.assertTrue(all(item["default"] == 0 and item["optimize"] == "N"
                                and item["start"] == item["step"] == item["stop"] == 0
                                for item in config["inputs"].values()))
        self.assertEqual("kcos_no_cooldown_fixed_time_stop",
                         stage2_config_from_spec(spec)["timing_control_policy"]["method"])
        self.assertNotIn("InpEntryCoolingDown", stage2_config_from_spec(spec)["inputs"])
        self.assertEqual(stage2_config_from_spec(spec), stage3_config_from_spec(spec))
        self.assertEqual("kcos_no_cooldown_fixed_time_stop",
                         stage3_config_from_spec(spec)["timing_control_policy"]["method"])

    def test_stage3_fixes_time_stop_and_omits_cooldown_without_mutating_stage1(self):
        spec = {"tester_config": {
            "schema_version": 1, "symbol": "ETHUSD",
            "timeframes": ["M5", "M15", "H1"],
            "from_date": "2025.01.10", "to_date": "2026.01.10",
            "inputs": {
            "InpMaxTradeBars": {"default": 48, "start": 12, "step": 2,
                                 "stop": 104, "optimize": "Y", "type": "int"},
            "InpMinBarsBetweenTrades": {"default": 12, "start": 2, "step": 2,
                                         "stop": 92, "optimize": "Y", "type": "int"},
            "InpAtrPeriod": {"default": 14, "start": 5, "step": 1,
                             "stop": 23, "optimize": "Y", "type": "int"},
        }}}
        stage2 = stage2_config_from_spec(spec)
        runtime = stage3_config_from_spec(spec)
        self.assertEqual(stage2, runtime)
        name = "InpMaxTradeBars"
        self.assertEqual({"default": 0, "start": 0, "step": 0,
                          "stop": 0, "optimize": "N"},
                         {key: runtime["inputs"][name][key] for key in
                          ("default", "start", "step", "stop", "optimize")})
        self.assertEqual("kcos_fixed_timing_control",
                         runtime["inputs"][name]["domain_provenance"]["method"])
        self.assertNotIn("InpMinBarsBetweenTrades", runtime["inputs"])
        self.assertEqual("Y", runtime["inputs"]["InpAtrPeriod"]["optimize"])
        self.assertEqual(48, spec["tester_config"]["inputs"]["InpMaxTradeBars"]["default"])

    def test_timing_control_classifier_uses_generic_names_and_labels(self):
        self.assertEqual("cooldown", is_timing_control_input("InpTradeCooldownBars"))
        self.assertEqual("cooldown", is_timing_control_input("InpBarsBetweenEntries"))
        self.assertEqual("time_stop", is_timing_control_input("InpMaxTradeBars"))
        self.assertEqual("time_stop", is_timing_control_input(
            "InpPositionLimit", {"description": "maximum holding period in bars"}))
        self.assertIsNone(is_timing_control_input("InpAtrPeriod"))

    def test_authenticated_config_domain_is_used_when_source_comment_is_missing(self):
        source = "input int InpDccLookback = 100;\\nvoid OnTick(){ int x=InpDccLookback; }"
        config = {"schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
                  "from_date": "2025.01.10", "to_date": "2026.01.10",
                  "inputs": {"InpDccLookback": {
            "default": 100, "start": 60, "step": 20, "stop": 200, "optimize": "Y"
        }}}
        result = validate_config_against_source(config, source)
        self.assertEqual(result["domain_checked_inputs"], 1)

    def test_present_source_domain_comment_must_match_config(self):
        source = "input int InpDccLookback = 100; // [lo=40,hi=180,step=20]\\nvoid OnTick(){ int x=InpDccLookback; }"
        config = {"schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
                  "from_date": "2025.01.10", "to_date": "2026.01.10",
                  "inputs": {"InpDccLookback": {
            "default": 100, "start": 60, "step": 20, "stop": 200, "optimize": "Y"
        }}}
        with self.assertRaisesRegex(ValueError, "exceeds source domain"):
            validate_config_against_source(config, source)

    def test_config_rejects_declared_constraint_violated_by_default(self):
        config = {
            "schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
            "from_date": "2025.01.10", "to_date": "2026.01.10",
            "inputs": {
                "InpAlpha": {"default": 0.06, "start": 0.01, "step": 0.01, "stop": 0.16, "optimize": "Y"},
                "InpBeta": {"default": 0.95, "start": 0.80, "step": 0.01, "stop": 0.99, "optimize": "Y"},
            },
            "input_constraints": [{"id": "persistence", "coefficients": {"InpAlpha": 1, "InpBeta": 1},
                                   "operator": "<", "limit": 1.0}],
        }
        with self.assertRaisesRegex(ValueError, "persistence rejects EA defaults"):
            validate_config(config)

    def test_config_accepts_signed_threshold_constraint_metadata(self):
        config = {
            "schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
            "from_date": "2025.01.10", "to_date": "2026.01.10",
            "inputs": {
                "InpLow": {"default": -0.5, "start": -2, "step": 0.1, "stop": -0.5, "optimize": "Y"},
                "InpHigh": {"default": 0.5, "start": 0.5, "step": 0.1, "stop": 2, "optimize": "Y"},
            },
            "input_constraints": [{"id": "ordered", "coefficients": {"InpLow": 1, "InpHigh": -1},
                                   "operator": "<", "limit": 0.0}],
        }
        self.assertIs(validate_config(config), config)


if __name__ == "__main__":
    unittest.main()
