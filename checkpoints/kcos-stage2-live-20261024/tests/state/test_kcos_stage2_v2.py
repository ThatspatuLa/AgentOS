import json
import hashlib
import re
import shutil
import tempfile
import unittest

import kcos_stage2_generate_source_v2 as source_builder
from pathlib import Path
from unittest import mock

import kcos_stage2_prepare_v2 as prepare
import kcos_stage2_verify_v2 as verify
import kcos_stage2_publish_v2 as publish
from kcos_input_contract_v2 import discover_inputs
from kcos_tester_config_v2 import stage2_spec_from_spec
from kcos_execution_contract_v2 import validate_strategy_input_bindings


def discover_empty_shared_contract():
    return discover_inputs({"minato_base_score_v2.mqh": ""})


def fixture_tester_config(cycle_id="fixture-cycle", ea="FixtureEA"):
    return {
        "schema_version": 1, "cycle_id": cycle_id, "ea": ea, "symbol": "ETHUSD",
        "timeframes": ["M5", "M15", "H1"], "from_date": "2025.01.10", "to_date": "2026.01.10",
        "inputs": {"InpFixture": {"default": "1", "start": "0", "step": "1", "stop": "20", "optimize": "Y"}},
    }


class Stage2V2Tests(unittest.TestCase):
    def test_diagnostic_followup_authorization_is_projected_only_for_stage2(self):
        pointer = {"path": "ticket.json", "sha256": "A" * 64}
        state = {"stage2_diagnostic_followup": pointer,
                 "run_authorization": {"authorized_by": "user_confirmed", "allowed_stages": [2]}}
        self.assertEqual({"pointer": pointer,
                          "authorization": {"authorized_by": "user_confirmed", "stage": 2}},
                         prepare.stage2_diagnostic_followup_context(state))
        state["run_authorization"]["allowed_stages"] = [3]
        self.assertIsNone(prepare.stage2_diagnostic_followup_context(state))
        state["run_authorization"] = {"authorized_by": "system", "allowed_stages": [2]}
        self.assertIsNone(prepare.stage2_diagnostic_followup_context(state))

    def test_stage2_grid_preflight_owns_integer_domain_from_source_default(self):
        spec = {"schema_version": 2, "stage": 1, "cycle_id": "grid-cycle", "ea": "ElderEA",
                "timeframes": ["M5", "M15", "H1"], "tester_config": {
                    "schema_version": 1, "cycle_id": "grid-cycle", "ea": "ElderEA",
                    "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
                    "from_date": "2025.01.10", "to_date": "2026.01.10",
                    "inputs": {"InpMacdFast": {"type": "int", "default": 7,
                        "start": 6, "step": 1, "stop": 8, "optimize": "Y"}},
                }}
        receipt = prepare.validate_stage3_grid_before_publish(
            spec, "input int InpMacdFast=7;", "ElderEA")
        self.assertTrue(receipt["passed"])
        self.assertEqual("stage2_owned_optimization_grid+stage3_read_only_validation",
                         receipt["policy"])
        self.assertEqual(1, receipt["optimized_input_count"])

    def test_stage2_grid_preflight_rederives_domain_when_stage1_hint_misses_default(self):
        spec = {"schema_version": 2, "stage": 1, "cycle_id": "grid-cycle", "ea": "ElderEA",
                "timeframes": ["M5", "M15", "H1"], "tester_config": {
                    "schema_version": 1, "cycle_id": "grid-cycle", "ea": "ElderEA",
                    "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
                    "from_date": "2025.01.10", "to_date": "2026.01.10",
                    "inputs": {"InpMacdFast": {"type": "int", "default": 7,
                        "start": 6.1, "step": 0.1, "stop": 6.9, "optimize": "Y"}},
                }}
        receipt = prepare.validate_stage3_grid_before_publish(
            spec, "input int InpMacdFast=7;", "ElderEA")
        self.assertTrue(receipt["passed"])

    def test_dcc_prompt_specialization_uses_effective_spec_signals(self):
        effective_spec = {
            "rationale": "Stage1 selection of DCC_GARCH_Multivariate_Correlation_Regime primitive.",
            "stage2_requirements": {"expected_inp_declarations": [
                "InpDccLookback", "InpDccEmaQ", "InpDccEmaP", "InpBTCCrossSymbol",
            ]},
        }
        self.assertTrue(source_builder.is_dcc_garch_spec(effective_spec))

    def test_dcc_prompt_specialization_rejects_unrelated_specs(self):
        unrelated_spec = {
            "rationale": "A moving average breakout strategy.",
            "stage2_requirements": {"expected_inp_declarations": ["InpAtrPeriod"]},
        }
        self.assertFalse(source_builder.is_dcc_garch_spec(unrelated_spec))

    def test_source_admission_lock_uses_zero_fixed_timing_defaults(self):
        spec = {"tester_config": {
            "schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
            "from_date": "2025.01.10", "to_date": "2026.01.10", "input_constraints": [],
            "inputs": {
                "InpMaxTradeBars": {"type": "int", "default": 48, "start": 12,
                                    "step": 4, "stop": 200, "optimize": "Y",
                                    "description": "maximum holding time stop"},
                "InpMinBarsBetweenTrades": {"type": "int", "default": 12, "start": 1,
                                            "step": 1, "stop": 96, "optimize": "Y"},
                "InpRegime_Greed_Lot_Mult": {"type": "double", "default": 1.0,
                                              "start": 0.0, "step": 0.25, "stop": 5.0,
                                              "optimize": "Y"},
            },
        }}
        effective = stage2_spec_from_spec(spec)
        lock = source_builder.strategy_input_lock_manifest(effective)
        self.assertEqual(0, lock["InpMaxTradeBars"]["default"])
        self.assertNotIn("InpMinBarsBetweenTrades", lock)
        self.assertEqual("N", lock["InpMaxTradeBars"]["optimize"])
        self.assertEqual(1.0, lock["InpRegime_Greed_Lot_Mult"]["default"])
        self.assertEqual("Y", lock["InpRegime_Greed_Lot_Mult"]["optimize"])

    def test_stage1_accepts_cooldown_metadata_for_stage2_removal(self):
        validate_strategy_input_bindings(
            {"parameters": ["InpSignal=1", "InpMinBarsBetweenTrades=12"]},
            {"InpSignal": {}, "InpMinBarsBetweenTrades": {}},
            {"input_bindings": {
                "InpSignal": ["execution"],
                "InpMinBarsBetweenTrades": ["execution"],
            }, "execution": "InpSignal InpMinBarsBetweenTrades"},
        )

    def test_stage2_spec_removes_cooldown_from_inputs_parameters_and_bindings(self):
        config = fixture_tester_config()
        config["inputs"] = {
            "InpSignal": {"type": "int", "default": 1, "start": 1,
                           "step": 1, "stop": 2, "optimize": "Y"},
            "InpMinBarsBetweenTrades": {"type": "int", "default": 12,
                                         "start": 1, "step": 1, "stop": 96,
                                         "optimize": "Y"},
        }
        spec = {
            "strategy": {
                "parameters": ["InpSignal=1", "InpMinBarsBetweenTrades=12"],
                "execution_contract": {"input_bindings": {
                    "InpSignal": ["execution"],
                    "InpMinBarsBetweenTrades": ["execution"],
                }},
            },
            "tester_config": config,
        }
        effective = stage2_spec_from_spec(spec)
        self.assertNotIn("InpMinBarsBetweenTrades", effective["tester_config"]["inputs"])
        self.assertEqual(["InpSignal=1"], effective["strategy"]["parameters"])
        self.assertNotIn("InpMinBarsBetweenTrades",
                         effective["strategy"]["execution_contract"]["input_bindings"])
        self.assertIn("InpMinBarsBetweenTrades",
                      effective["stage2_runtime_policy"]["excluded_cooldown_controls"])

    def test_generated_runtime_repairs_preserve_fixed_zero_timing_and_literalize_array_bound(self):
        source = """input int InpTimeStopBars = 0;
input int InpMinBarsBetweenTrades = 0;
const int MAX_RV_SAMPLES = 1024;
double g_rv_samples[MAX_RV_SAMPLES];
int OnInit()
{
   // Touches KCOS-fixed timing inputs to ensure declaration-only status is impossible.
   // Both are pinned to 0 and explicitly disable behavior; keep this as executable evidence.
   int    _time_stop_used = (InpTimeStopBars > 0) ? InpTimeStopBars : 0;
   int    _cooldown_used  = (InpMinBarsBetweenTrades > 0) ? InpMinBarsBetweenTrades : 0;
   if(_time_stop_used == 0 && _cooldown_used == 0)
   {
      PrintFormat("kcos_timing_controls_disabled time_stop=%d cooldown=%d", _time_stop_used, _cooldown_used);
   }
   return INIT_SUCCEEDED;
}
void ManageOpenPosition()
{
   // Zero KCOS-fixed time_stop is never used to delay or close a position.
   if(InpTimeStopBars > 0) return;
   double value = 0.0;
}
"""

        repaired, changes = source_builder.repair_generated_mql5_runtime_patterns(source)

        self.assertIn("double g_rv_samples[1024];", repaired)
        self.assertIn('PrintFormat("kcos_time_stop_fixed_input=%d", InpTimeStopBars)', repaired)
        self.assertNotIn("InpMinBarsBetweenTrades", repaired)
        self.assertNotIn("_time_stop_used", repaired)
        self.assertNotIn("InpTimeStopBars > 0) return", repaired)
        self.assertTrue(verify.zero_disables_time_stop_controls(repaired))
        self.assertTrue(verify.cooldown_controls_absent(repaired))
        self.assertEqual({
            "mql5_const_array_bound_literalized",
            "fixed_time_stop_evidence_logging",
            "remove_fixed_time_stop_management_bypass",
            "stage2_cooldown_input_declaration_removed",
        }, {item["kind"] for item in changes})

    def test_runtime_nan_sentinel_uses_mql5_invalid_result_without_constant_division(self):
        source = "double InvalidValue() { return 0.0/0.0; }\n"
        repaired, changes = source_builder.repair_generated_mql5_runtime_patterns(source)
        self.assertEqual("double InvalidValue() { return MathArcsin(2.0); }\n", repaired)
        self.assertEqual(["mql5_nan_sentinel_replaced"], [item["kind"] for item in changes])
        self.assertIn("MQL5 invalid-result sentinel", changes[0]["reason"])
        untouched = "// 0.0/0.0 remains comment text\nconst string s=\"0.0/0.0\";\n"
        self.assertEqual((untouched, []), source_builder.repair_generated_mql5_runtime_patterns(untouched))

    def test_zero_time_stop_requires_a_positive_guard(self):
        unsafe = "input int InpMaxTradeBars = 48; void Manage(){ if (bars - entry >= InpMaxTradeBars) Close(); }"
        safe = "input int InpMaxTradeBars = 48; void Manage(){ if (InpMaxTradeBars > 0 && bars - entry >= InpMaxTradeBars) Close(); }"
        self.assertFalse(verify.zero_disables_time_stop_controls(unsafe))
        self.assertTrue(verify.zero_disables_time_stop_controls(safe))
        nested_positive_guard = "input int InpMaxTradeBars = 48; void Manage(){ if (InpMaxTradeBars > 0) { if (bars - entry >= InpMaxTradeBars) Close(); } }"
        self.assertTrue(verify.zero_disables_time_stop_controls(nested_positive_guard))

    def test_zero_time_stop_accepts_only_dominating_type_correct_early_returns(self):
        void_return = ("input int InpMaxTradeBars = 0; void Manage(){ "
                       "if (InpMaxTradeBars <= 0) return; "
                       "if (bars >= InpMaxTradeBars) Close(); }")
        bool_return = ("input int InpMaxTradeBars = 0; bool Expired(){ "
                       "if (InpMaxTradeBars == 0) return false; "
                       "return bars >= InpMaxTradeBars; }")
        wrong_bool_return = ("input int InpMaxTradeBars = 0; bool Expired(){ "
                             "if (InpMaxTradeBars <= 0) return; "
                             "return bars >= InpMaxTradeBars; }")
        wrong_void_return = ("input int InpMaxTradeBars = 0; void Manage(){ "
                             "if (InpMaxTradeBars <= 0) return false; "
                             "if (bars >= InpMaxTradeBars) Close(); }")
        negative_only = ("input int InpMaxTradeBars = 0; void Manage(){ "
                         "if (InpMaxTradeBars < 0) return; "
                         "if (bars >= InpMaxTradeBars) Close(); }")
        conditional = ("input int InpMaxTradeBars = 0; void Manage(){ "
                       "if (enabled) { if (InpMaxTradeBars <= 0) return; } "
                       "if (bars >= InpMaxTradeBars) Close(); }")
        unbraced_conditional = ("input int InpMaxTradeBars = 0; void Manage(){ "
                                "if (enabled) if (InpMaxTradeBars <= 0) return; "
                                "if (bars >= InpMaxTradeBars) Close(); }")
        self.assertTrue(verify.zero_disables_time_stop_controls(void_return))
        self.assertTrue(verify.zero_disables_time_stop_controls(bool_return))
        for unsafe in (wrong_bool_return, wrong_void_return, negative_only,
                       conditional, unbraced_conditional):
            with self.subTest(source=unsafe):
                self.assertFalse(verify.zero_disables_time_stop_controls(unsafe))

    def test_zero_time_stop_rejects_or_routes_that_bypass_positive_guard(self):
        direct_or = ("input int InpMaxTradeBars = 0; void Manage(){ "
                     "if (InpMaxTradeBars > 0 || force_close) "
                     "if (bars >= InpMaxTradeBars) Close(); }")
        enclosing_or = ("input int InpMaxTradeBars = 0; void Manage(){ "
                        "if ((InpMaxTradeBars > 0 && enabled) || force_close) { "
                        "if (bars >= InpMaxTradeBars) Close(); } }")
        self.assertFalse(verify.zero_disables_time_stop_controls(direct_or))
        self.assertFalse(verify.zero_disables_time_stop_controls(enclosing_or))

    def test_zero_cooldown_accepts_explicit_zero_bypass_before_direct_return(self):
        source = """input int InpMinBarsBetweenTrades = 0;
void OnTick() {
  bool cooldown_satisfied = (InpMinBarsBetweenTrades <= 0) || (bars_since == 0) || (bars_since > InpMinBarsBetweenTrades);
  if (!cooldown_satisfied) return;
  EnterTrade();
}"""
        self.assertTrue(verify.zero_disables_cooldown_controls(source))
        self.assertFalse(verify.zero_disables_cooldown_controls(
            source.replace("InpMinBarsBetweenTrades <= 0", "InpMinBarsBetweenTrades < 0")))

    def test_generated_source_must_omit_cooldown_controls(self):
        self.assertTrue(verify.cooldown_controls_absent("void OnTick(){ PlaceOrder(); }"))
        for source in (
            "input int InpMinBarsBetweenTrades=0; void OnTick(){ PlaceOrder(); }",
            "input int InpTradeCooldownBars=0; void OnTick(){ PlaceOrder(); }",
            "void OnTick(){ if(CooldownElapsed()) return; }",
        ):
            self.assertFalse(verify.cooldown_controls_absent(source))

    def test_zero_cooldown_requires_a_positive_guard_when_used(self):
        unsafe = "input int InpMinBarsBetweenTrades = 12; void Entry(){ if (bars < InpMinBarsBetweenTrades) return; }"
        safe = "input int InpMinBarsBetweenTrades = 12; void Entry(){ if (InpMinBarsBetweenTrades > 0 && bars < InpMinBarsBetweenTrades) return; }"
        positive_guard_block = "input int InpMinBarsBetweenTrades = 0; void Entry(){ if (InpMinBarsBetweenTrades > 0) { if (bars < InpMinBarsBetweenTrades) return; } }"
        unsafe_or_guard = "input int InpMinBarsBetweenTrades = 0; void Entry(){ if (bars == 0 || InpMinBarsBetweenTrades > 0) { if (bars < InpMinBarsBetweenTrades) return; } }"
        unused = "input int InpTradeCooldownBars = 0; void Entry(){ PlaceOrder(); }"
        self.assertFalse(verify.zero_disables_cooldown_controls(unsafe))
        self.assertFalse(verify.zero_disables_cooldown_controls(unsafe_or_guard))
        self.assertTrue(verify.zero_disables_cooldown_controls(safe))
        self.assertTrue(verify.zero_disables_cooldown_controls(positive_guard_block))
        alias_guarded = ("input int InpMinBarsBetweenTrades = 0; void Entry(){ "
                         "int cooldown_disabled = InpMinBarsBetweenTrades; "
                         "if (cooldown_disabled > 0) { "
                         "if (bars < cooldown_disabled) return; } }")
        alias_unguarded = ("input int InpMinBarsBetweenTrades = 0; void Entry(){ "
                           "int cooldown_disabled = InpMinBarsBetweenTrades; "
                           "Print(cooldown_disabled); "
                           "if (cooldown_disabled > 0) { if (bars < cooldown_disabled) return; } }")
        self.assertTrue(verify.zero_disables_cooldown_controls(alias_guarded))
        self.assertFalse(verify.zero_disables_cooldown_controls(alias_unguarded))
        self.assertTrue(verify.zero_disables_cooldown_controls(unused))
        early_true_guard = ("input int InpMinBarsBetweenTrades = 0; "
                            "bool CooldownElapsed(){ "
                            "if (InpMinBarsBetweenTrades <= 0) return true; "
                            "return bars_since_entry >= InpMinBarsBetweenTrades; }")
        self.assertTrue(verify.zero_disables_cooldown_controls(early_true_guard))
        negative_only_guard = ("input int InpMinBarsBetweenTrades = 0; "
                               "bool CooldownElapsed(){ "
                               "if (InpMinBarsBetweenTrades < 0) return true; "
                               "return bars_since_entry >= InpMinBarsBetweenTrades; }")
        self.assertFalse(verify.zero_disables_cooldown_controls(negative_only_guard))
        use_before_guard = ("input int InpMinBarsBetweenTrades = 0; "
                            "bool CooldownElapsed(){ int x = InpMinBarsBetweenTrades; "
                            "if (InpMinBarsBetweenTrades <= 0) return true; "
                            "return bars_since_entry >= InpMinBarsBetweenTrades; }")
        self.assertFalse(verify.zero_disables_cooldown_controls(use_before_guard))

    def test_atr_verifier_accepts_authenticated_closed_buffer_output_helper(self):
        source = """input int InpAtrPeriod = 14;
int g_atr_handle;
bool CopyClosedBuffer(int h, double &out) {
  double buf[1];
  if(CopyBuffer(h, 0, 1, 1, buf) != 1) return false;
  out = buf[0];
  return true;
}
int OnInit(){ g_atr_handle = iATR(_Symbol, PERIOD_M5, InpAtrPeriod); return 0; }
void OnTick(){ double atr; if(!CopyClosedBuffer(g_atr_handle, atr)) return; }
"""
        self.assertTrue(verify.atr_uses_authenticated_closed_buffer(
            source, "InpAtrPeriod", "PERIOD_M5"))
        self.assertFalse(verify.atr_uses_authenticated_closed_buffer(
            source.replace("CopyBuffer(h, 0, 1, 1, buf)",
                           "CopyBuffer(h, 0, 0, 1, buf)"),
            "InpAtrPeriod", "PERIOD_M5"))

    def test_normalize_lot_authentication_accepts_one_exact_forward_prototype(self):
        helper = """double NormalizeLot(double requestedVolume) {
double minVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
double maxVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
double volumeStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
if (requestedVolume <= 0.0) return 0.0;
if (minVolume <= 0.0) return 0.0;
if (maxVolume < minVolume) return 0.0;
if (volumeStep <= 0.0) return 0.0;
double cappedVolume = MathMin(requestedVolume, maxVolume);
double flooredVolume = MathFloor(cappedVolume / volumeStep) * volumeStep;
if (flooredVolume < minVolume) return 0.0;
double normalizedVolume = NormalizeDouble(flooredVolume, 8);
if (normalizedVolume < minVolume) return 0.0;
if (normalizedVolume > maxVolume) return 0.0;
return normalizedVolume;
}"""
        self.assertTrue(verify.normalize_lot_helper_is_authenticated(helper))
        prototype = "//--- Forward declarations\ndouble NormalizeLot(double requestedVolume);\n"
        self.assertTrue(verify.normalize_lot_helper_is_authenticated(prototype + helper))
        self.assertFalse(verify.normalize_lot_helper_is_authenticated(prototype + prototype + helper))
        self.assertFalse(verify.normalize_lot_helper_is_authenticated(
            prototype + helper.replace("MathFloor(cappedVolume / volumeStep)", "MathRound(cappedVolume / volumeStep)")))

    def test_margin_close_gate_accepts_exact_owned_loop_with_magic_setup_and_bookkeeping_reset(self):
        source = """#include <minato_base_score_v2.mqh>
CTrade g_trade;
input int InpMagicNumber = 123;
bool MarginBreached() {
double used = AccountInfoDouble(ACCOUNT_MARGIN);
double level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
return (used > 0.0 && level > 0.0 && level < MINATO_MIN_MARGIN_LEVEL);
}
void CloseAllPositions() {
g_trade.SetExpertMagicNumber((ulong)InpMagicNumber);
for(int i = PositionsTotal() - 1; i >= 0; i--) {
ulong ticket = PositionGetTicket(i);
if(ticket == 0) continue;
if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
if(PositionGetInteger(POSITION_MAGIC) != (long)InpMagicNumber) continue;
g_trade.PositionClose(ticket);
}
g_last_closed_bar_active_regime = KCOS_REGIME_NEUTRAL;
g_entry_bar_index = -1;
}
void OnTick() {
MinatoV2ObserveTick();
if(MarginBreached()) { CloseAllPositions(); return; }
}
"""
        self.assertTrue(verify._canonical_margin_wrapper_is_concrete(verify.executable_source(source)))
        self.assertFalse(verify._canonical_margin_wrapper_is_concrete(
            verify.executable_source(source.replace("i >= 0; i--", "i >= 0; i++"))))
        local_trade_source = source.replace("CTrade g_trade;\n", "").replace(
            "void CloseAllPositions() {\n", "void CloseAllPositions() {\nCTrade g_trade;\n")
        self.assertTrue(verify._canonical_margin_wrapper_is_concrete(
            verify.executable_source(local_trade_source)))
        wrong_setup = local_trade_source.replace(
            "g_trade.SetExpertMagicNumber", "other.SetExpertMagicNumber")
        self.assertFalse(verify._canonical_margin_wrapper_is_concrete(
            verify.executable_source(wrong_setup)))

    def test_missing_cycle_magic_input_and_zero_identity_literals_are_repaired_only_as_metadata(self):
        source = '''input int InpATRPeriod = 14;
CTrade trade;
bool MarginBreached() {
  double used = AccountInfoDouble(ACCOUNT_MARGIN);
  double level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
  return used > 0.0 && level > 0.0 && level < MINATO_MIN_MARGIN_LEVEL;
}
void CloseAllPositions() {
  trade.SetExpertMagicNumber((ulong)0);
  for(int i = PositionsTotal() - 1; i >= 0; i--) {
    ulong ticket = PositionGetTicket(i);
    if(ticket == 0) continue;
    if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
    if(PositionGetInteger(POSITION_MAGIC) != (long)0) continue;
    trade.PositionClose(ticket);
  }
}
void OnTick() {
  MinatoV2ObserveTick();
  if(MarginBreached()) { CloseAllPositions(); return; }
  if(PositionGetInteger(POSITION_MAGIC) == (long)0) return;
  if(signal_value == 0) return; // keep strategy literals untouched
}
'''
        repaired, records = source_builder.ensure_magic_number_identity_control(
            source, {"cycle_id": "kcos-20261020T150245Z-research-new"})
        self.assertIn("input ulong InpMagicNumber = 261020001;", repaired)
        self.assertIn("SetExpertMagicNumber((ulong)InpMagicNumber)", repaired)
        self.assertEqual(2, repaired.count("PositionGetInteger(POSITION_MAGIC)"))
        self.assertEqual(2, repaired.count("(long)InpMagicNumber"))
        self.assertIn("signal_value == 0", repaired)
        self.assertIn("// keep strategy literals untouched", repaired)
        self.assertEqual(2, len(records))
        self.assertTrue(verify._canonical_margin_wrapper_is_concrete(
            verify.executable_source(repaired)))

    def test_existing_magic_default_is_preserved_and_ambiguous_declarations_fail_closed(self):
        source = ("input ulong InpMagicNumber = 90691;\n"
                  "void CloseAllPositions(){trade.SetExpertMagicNumber((ulong)0);}\n")
        repaired, records = source_builder.ensure_magic_number_identity_control(
            source, {"cycle_id": "kcos-20261020T150245Z-research-new"})
        self.assertIn("input ulong InpMagicNumber = 90691;", repaired)
        self.assertNotIn("261020001", repaired)
        self.assertIn("SetExpertMagicNumber((ulong)InpMagicNumber)", repaired)
        self.assertEqual(1, len(records))
        duplicate = source.replace("input ulong InpMagicNumber = 90691;",
                                   "input ulong InpMagicNumber = 90691;\ninput ulong InpMagicNumber2 = 7;\ninput ulong InpMagicNumber = 90692;")
        with self.assertRaises(ValueError):
            source_builder.ensure_magic_number_identity_control(
                duplicate, {"cycle_id": "kcos-20261020T150245Z-research-new"})

    def test_stage2_resolves_empty_research_source_pointer_from_build_evidence(self):
        state = {"cycle_id": "cycle", "current_ea": "FixtureEA"}
        handoff = {"cycle_id": "cycle", "candidate_ea": "FixtureEA", "candidate_source": ""}
        resolved = {**handoff, "candidate_source": "C:/FixtureEA.mq5"}
        with mock.patch.object(prepare, "load", return_value=handoff), \
             mock.patch.object(prepare, "bind_generated_source", return_value=resolved) as bind:
            self.assertEqual(resolved, prepare.resolve_stage1_handoff(state))
        bind.assert_called_once_with(state, handoff)

    def test_gk_window_changes_only_after_new_closed_bar_gate(self):
        safe = "void OnTick(){MinatoV2ObserveTick();if(!NewBarClosed())return;PushGKSample(value);}"
        unsafe = "void OnTick(){MinatoV2ObserveTick();PushGKSample(value);if(!NewBarClosed())return;}"
        self.assertTrue(verify.gk_sample_update_is_closed_bar_gated(safe))
        self.assertFalse(verify.gk_sample_update_is_closed_bar_gated(unsafe))

    def test_atr_verifier_accepts_redundant_int_cast_only_for_int_input(self):
        safe = ("input int InpATRPeriod=14; int g_atr_handle; "
                "int OnInit(){g_atr_handle=iATR(_Symbol,_Period,(int)InpATRPeriod);return 0;} "
                "void OnTick(){double x[1];CopyBuffer(g_atr_handle,0,1,1,x);}")
        self.assertTrue(verify.atr_uses_authenticated_closed_buffer(safe))
        self.assertFalse(verify.atr_uses_authenticated_closed_buffer(
            safe.replace("input int InpATRPeriod", "input double InpATRPeriod")))
        self.assertFalse(verify.atr_uses_authenticated_closed_buffer(
            safe.replace("(int)InpATRPeriod", "(int)(InpATRPeriod+1)")))

    def test_atr_period_input_is_resolved_from_authenticated_spec(self):
        spec = {"tester_config": {"inputs": {"InpAtrLength": {}, "InpChannelMultiplier": {}}}}
        self.assertEqual("InpAtrLength", verify.atr_period_input_name(spec))
        source = ("input int InpAtrLength=14; int g_atr_handle; "
                  "int OnInit(){g_atr_handle=iATR(_Symbol,_Period,InpAtrLength);return 0;} "
                  "void OnTick(){double x[1];CopyBuffer(g_atr_handle,0,1,1,x);}")
        self.assertTrue(verify.atr_uses_authenticated_closed_buffer(source, verify.atr_period_input_name(spec)))

    def test_atr_period_input_resolution_rejects_ambiguous_spec(self):
        spec = {"tester_config": {"inputs": {"InpAtrLength": {}, "InpATRPeriod": {}}}}
        with self.assertRaisesRegex(ValueError, "ambiguous authenticated ATR"):
            verify.atr_period_input_name(spec)

    def test_atr_source_uses_builtin_period_and_closed_shift(self):
        safe = "int g_atr_handle; int OnInit(){g_atr_handle=iATR(_Symbol,_Period,InpATRPeriod);return 0;} void OnTick(){double x[1];CopyBuffer(g_atr_handle,0,1,1,x);}"
        forming = safe.replace("CopyBuffer(g_atr_handle,0,1,1,x)", "CopyBuffer(g_atr_handle,0,0,1,x)")
        self.assertTrue(verify.atr_uses_authenticated_closed_buffer(safe))
        self.assertFalse(verify.atr_uses_authenticated_closed_buffer(forming))
        self.assertFalse(verify.closed_bar_indicator_reads_are_safe(forming))

    def test_closed_copyclose_shift_one_allows_series_index_zero_read(self):
        safe = ("double closebuf[2]; ArraySetAsSeries(closebuf,true); "
                "bool NewBarClosed(){return true;} "
                "void OnTick(){if(!NewBarClosed())return; "
                "if(CopyClose(_Symbol,PERIOD_CURRENT,1,2,closebuf)<=0)return; "
                "double close1=closebuf[0];}")
        forming = safe.replace("CopyClose(_Symbol,PERIOD_CURRENT,1,2,closebuf)",
                               "CopyClose(_Symbol,PERIOD_CURRENT,0,2,closebuf)")
        self.assertTrue(verify.closed_bar_indicator_reads_are_safe(safe))
        self.assertFalse(verify.closed_bar_indicator_reads_are_safe(forming))

    def test_closed_slot_read_is_not_admitted_when_same_array_has_forming_copy(self):
        mixed = ("double values[3]; ArraySetAsSeries(values,true); "
                 "bool NewBarClosed(){return true;} "
                 "void OnTick(){if(!NewBarClosed())return; "
                 "CopyBuffer(handle,0,1,2,values); CopyBuffer(handle,0,0,3,values); "
                 "double x=values[0];}")
        self.assertFalse(verify.closed_bar_indicator_reads_are_safe(mixed))

    def test_dcc_stage1_fixed_m5_atr_period_is_not_misread_as_current_tester_period(self):
        source = """
        bool ReadAtrClosed(double &out) {
          int handle = iATR(_Symbol, PERIOD_M5, InpAtrPeriod);
          double values[1];
          bool ok = CopyBuffer(handle, 0, 1, 1, values) == 1;
          IndicatorRelease(handle);
          if (!ok) return false;
          out = values[0]; return out > 0.0;
        }
        """
        self.assertTrue(verify.atr_uses_authenticated_closed_buffer(
            source, "InpAtrPeriod", "PERIOD_M5"))
        self.assertFalse(verify.atr_uses_authenticated_closed_buffer(
            source, "InpAtrPeriod", "_Period"))

    def test_free_margin_notional_guard_is_injected_idempotently(self):
        source = "#include <minato_base_score_v2.mqh>\nvoid TryOpen() {\n   if(!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, volume, ask)) return;\n   trade.Buy(volume, _Symbol, ask, sl, tp);\n}"
        repaired, events = source_builder.ensure_free_margin_notional_guards(source)
        self.assertEqual(2, len(events))
        self.assertIn("bool FreeMarginAllowsNotional(double volume, double price)", repaired)
        self.assertIn("if(!FreeMarginAllowsNotional(volume, ask)) return;", repaired)
        again, second_events = source_builder.ensure_free_margin_notional_guards(repaired)
        self.assertEqual(repaired, again)
        self.assertEqual([], second_events)

    def test_free_margin_notional_guard_repair_does_not_guess_price_or_volume(self):
        source = "#include <minato_base_score_v2.mqh>\nif(!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lots, entry)) return;"
        repaired, events = source_builder.ensure_free_margin_notional_guards(source)
        self.assertIn("if(!FreeMarginAllowsNotional(lots, entry)) return;", repaired)
        guard_event = next(event for event in events if event["kind"] == "projected_margin_followup_added")
        self.assertEqual("lots", guard_event["volume"])
        self.assertEqual("entry", guard_event["price"])

    def test_canonical_ontester_body_normalizes_to_cron_hook(self):
        source = "int x=1;\ndouble OnTester() { return MinatoV2ScoreFromTester(); }\n"
        normalized = source_builder.normalize_canonical_ontester_hook(source)
        self.assertIn(source_builder.ONTESTER_TOKEN, normalized)
        self.assertNotIn("double OnTester()", normalized)

    def test_marker_banner_plus_canonical_body_normalizes_to_one_hook(self):
        source = "double OnTester() { return MinatoV2ScoreFromTester(); }\n//@@KCOS_CANONICAL_ONTESTER@@\n"
        normalized = source_builder.normalize_canonical_ontester_hook(source)
        self.assertEqual(1, normalized.count(source_builder.ONTESTER_TOKEN))
        injected = normalized.replace(source_builder.ONTESTER_TOKEN, source_builder.ONTESTER)
        code = source_builder.code_only(injected)
        self.assertEqual(1, len(__import__("re").findall(r"\bOnTester\s*\(", code)))
        self.assertIn(source_builder.ONTESTER.strip(), code)

    def test_standalone_marker_comment_is_promoted_to_cron_hook(self):
        source = "int OnInit() { return INIT_SUCCEEDED; }\n//@@KCOS_CANONICAL_ONTESTER@@\n"
        normalized = source_builder.normalize_canonical_ontester_hook(source)
        self.assertEqual(1, normalized.count(source_builder.ONTESTER_TOKEN))
        self.assertNotIn("//@@KCOS_CANONICAL_ONTESTER@@", normalized)

    def test_dcc_prompt_reference_uses_verifier_compatible_rolling_zscore(self):
        reference = source_builder.ROLLING_ZSCORE_BOUNDED_REFERENCE
        self.assertIn(
            "double RollingZScore(const double &values[], int total, int lookback, double current)",
            reference,
        )
        self.assertNotIn("double RollingZScore(", reference.replace(
            "double RollingZScore(const double &values[], int total, int lookback, double current)", ""
        ))
        self.assertIn("for(int k = total - lookback; k < total; k++)", reference)

    def test_dcc_history_array_pair_is_split_without_changing_other_source(self):
        dcc_spec = {
            "stage2_requirements": {"expected_inp_declarations": [
                "InpDccLookback", "InpBTCCrossSymbol",
            ]},
        }
        source = "   double h_eth_hist[], h_btc_hist[];\n"
        repaired, events = source_builder.split_dcc_history_array_declaration(source, dcc_spec)
        self.assertEqual("   double h_eth_hist[];\n   double h_btc_hist[];\n", repaired)
        self.assertEqual(["dcc_history_array_declarations_split"],
                         [event["kind"] for event in events])

    def test_dcc_history_array_split_is_not_applied_to_non_dcc_specs(self):
        source = "double h_eth_hist[], h_btc_hist[];\n"
        repaired, events = source_builder.split_dcc_history_array_declaration(
            source, {"stage2_requirements": {"expected_inp_declarations": ["InpAtrPeriod"]}}
        )
        self.assertEqual(source, repaired)
        self.assertEqual([], events)

    def test_multidimensional_array_row_bounds_rejects_array_size(self):
        unsafe = "void f(double &x[][]) { int n = ArraySize(x); for(int i=0;i<n;i++) Print(x[i][0]); }"
        safe = "void f(double &x[][]) { int n = ArrayRange(x, 0); for(int i=0;i<n;i++) Print(x[i][0]); }"
        self.assertFalse(verify.multidimensional_array_row_bounds_are_safe(unsafe))
        self.assertTrue(verify.multidimensional_array_row_bounds_are_safe(safe))
        self.assertTrue(verify.multidimensional_array_row_bounds_are_safe("double a[]; int n=ArraySize(a);"))

    def test_dcc_bounds_accepts_exact_loop_proven_return_index_aliases(self):
        fixture = (Path(r"C:\Users\Tyson\Projects\kiyosaki_stage2b\state")
                   / "candidates" / ".generation" / "kcos-20261012T121045Z-research-new"
                   / "20261012T122148Z-428758690afc" / "proposal-retry-3.stdout.txt")
        proposal = fixture.read_text(encoding="utf-8-sig")
        self.assertTrue(verify.dcc_closed_return_arrays_are_bounded(
            proposal, required_dcc=True))
        shifted = proposal.replace("int idx_a = k + 1;", "int idx_a = k;", 1)
        self.assertNotEqual(proposal, shifted)
        self.assertFalse(verify.dcc_closed_return_arrays_are_bounded(
            shifted, required_dcc=True))

    def test_captured_dcc_order_repair_preserves_body_and_passes_strict_gate(self):
        fixture = (Path(r"C:\Users\Tyson\Projects\kiyosaki_stage2b\state")
                   / "candidates" / ".generation" / "kcos-20261012T050309Z-research-new"
                   / "20261012T051312Z-ebaade9237d7" / "proposal-retry-3.stdout.txt")
        source = fixture.read_text(encoding="utf-8-sig")
        dcc_spec = {"stage2_requirements": {"expected_inp_declarations": [
            "InpDccLookback", "InpBTCCrossSymbol",
        ]}}
        self.assertFalse(verify.dcc_closed_return_arrays_are_bounded(source, required_dcc=True))
        repaired, events = source_builder.reorder_exact_dcc_reference_after_oninit(source, dcc_spec)
        self.assertEqual(["exact_dcc_reference_moved_after_oninit"],
                         [event["kind"] for event in events])

        definition = re.compile(
            r"\bbool\s+ComputeDCCSignals\s*\(\s*double\s*&\s*z_corr\s*,"
            r"\s*int\s*&\s*regime_out\s*\)\s*\{", re.I)

        def function_text(text):
            masked = source_builder._mask_comments_and_strings(text)
            match = definition.search(masked)
            self.assertIsNotNone(match)
            depth = 0
            for index in range(match.end() - 1, len(masked)):
                if masked[index] == "{":
                    depth += 1
                elif masked[index] == "}":
                    depth -= 1
                    if depth == 0:
                        return text[match.start():index + 1]
            self.fail("unterminated ComputeDCCSignals")

        self.assertEqual(function_text(source), function_text(repaired))
        executable = source_builder._mask_comments_and_strings(repaired)
        oninit = re.search(r"\bint\s+OnInit\s*\(\s*\)\s*\{", executable, re.I)
        dcc_definition = definition.search(executable)
        self.assertIsNotNone(oninit)
        self.assertIsNotNone(dcc_definition)
        self.assertLess(oninit.start(), dcc_definition.start())
        self.assertIn("bool ComputeDCCSignals(double &z_corr, int &regime_out);", repaired)
        self.assertTrue(verify.dcc_closed_return_arrays_are_bounded(repaired, required_dcc=True))

    def test_exact_dcc_order_repair_is_a_noop_after_first_application(self):
        fixture = (Path(r"C:\Users\Tyson\Projects\kiyosaki_stage2b\state")
                   / "candidates" / ".generation" / "kcos-20261012T050309Z-research-new"
                   / "20261012T051312Z-ebaade9237d7" / "proposal-retry-3.stdout.txt")
        source = fixture.read_text(encoding="utf-8-sig")
        dcc_spec = {"stage2_requirements": {"expected_inp_declarations": [
            "InpDccLookback", "InpBTCCrossSymbol",
        ]}}
        repaired, _ = source_builder.reorder_exact_dcc_reference_after_oninit(source, dcc_spec)
        unchanged, events = source_builder.reorder_exact_dcc_reference_after_oninit(repaired, dcc_spec)
        self.assertEqual(repaired, unchanged)
        self.assertEqual([], events)

    def test_dcc_order_repair_fails_closed_for_altered_or_ambiguous_sources(self):
        fixture = (Path(r"C:\Users\Tyson\Projects\kiyosaki_stage2b\state")
                   / "candidates" / ".generation" / "kcos-20261012T050309Z-research-new"
                   / "20261012T051312Z-ebaade9237d7" / "proposal-retry-3.stdout.txt")
        source = fixture.read_text(encoding="utf-8-sig")
        dcc_spec = {"stage2_requirements": {"expected_inp_declarations": [
            "InpDccLookback", "InpBTCCrossSymbol",
        ]}}
        altered = source.replace("MathMin(eth_total, btc_total)",
                                 "MathMax(eth_total, btc_total)", 1)
        duplicate_function = source + "\nbool ComputeDCCSignals(double &z_corr, int &regime_out) { return false; }\n"
        duplicate_oninit = source + "\nint OnInit() { return INIT_SUCCEEDED; }\n"
        for candidate in (altered, duplicate_function, duplicate_oninit):
            repaired, events = source_builder.reorder_exact_dcc_reference_after_oninit(candidate, dcc_spec)
            self.assertEqual(candidate, repaired)
            self.assertEqual([], events)
            self.assertFalse(verify.dcc_closed_return_arrays_are_bounded(candidate, required_dcc=True))
        non_dcc, events = source_builder.reorder_exact_dcc_reference_after_oninit(
            source, {"stage2_requirements": {"expected_inp_declarations": ["InpAtrPeriod"]}}
        )
        self.assertEqual(source, non_dcc)
        self.assertEqual([], events)
        self.assertFalse(verify.dcc_closed_return_arrays_are_bounded(non_dcc, required_dcc=True))

    def test_noncanonical_ontester_body_is_not_rewritten(self):
        source = "double OnTester() { return 1.0; }"
        self.assertEqual(source, source_builder.normalize_canonical_ontester_hook(source))

    def test_source_response_needs_only_one_mql5_block(self):
        raw = "```mql5\nint OnInit() { return INIT_SUCCEEDED; }\n```"
        proposal = source_builder.decode_source_proposal(raw, "proposal")
        self.assertEqual({"source_text": "int OnInit() { return INIT_SUCCEEDED; }"}, proposal)

    def test_legacy_source_metadata_remains_accepted(self):
        raw = (
            "KCOS_SOURCE_PROPOSAL_V1\n```mql5\nint OnInit() { return INIT_SUCCEEDED; }\n```\n"
            "```json\n{\"filename\":\"FixtureEA.mq5\"}\n```"
        )
        proposal = source_builder.decode_source_proposal(raw, "proposal")
        self.assertEqual({"filename": "FixtureEA.mq5", "source_text": "int OnInit() { return INIT_SUCCEEDED; }"}, proposal)

    def test_review_adjudication_discards_only_explicit_no_issue_conclusions(self):
        self.assertTrue(source_builder.review_issue_is_explicitly_nonblocking(
            "The TP path is correct. No double-multiplier violation is present on TP or SL."))
        self.assertTrue(source_builder.review_issue_is_explicitly_nonblocking("No issue raised."))
        self.assertFalse(source_builder.review_issue_is_explicitly_nonblocking(
            "The source double-applies the regime multiplier and violates the TP equation."))
        self.assertFalse(source_builder.review_issue_is_explicitly_nonblocking(
            "The source is missing the required margin guard."))

    def test_prior_repair_feedback_requires_matching_cycle_lineage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempt = root / "attempt-1"
            attempt.mkdir()
            bound = {"cycle_id": "cycle", "ea": "EA", "source_spec_sha256": "a",
                     "stage1_event_sha256": "b", "canonical_minato_sha256": "c"}
            source_builder.write_json(attempt / "hold.json", {**bound, "status": "MODEL_SOURCE_HOLD",
                "phase": "deterministic_source_admission", "reason": "source code issue"})
            stale_review = root / "attempt-2-review"
            stale_review.mkdir()
            source_builder.write_json(stale_review / "hold.json", {**bound, "status": "MODEL_SOURCE_HOLD",
                "phase": "independent_review", "reason": "obsolete reviewer issue"})
            feedback = source_builder.prior_repair_feedback(root, bound)
            self.assertEqual("source code issue", feedback["prior_holds"][0]["reason"])
            self.assertEqual(1, len(feedback["prior_holds"]))
            self.assertIsNone(source_builder.prior_repair_feedback(root, {**bound, "stage1_event_sha256": "changed"}))

    def test_strategy_input_defaults_are_locked_to_stage1_values(self):
        source = "input double InpRegime_Greed = 1.25;\ndouble x = InpRegime_Greed;"
        spec = {"tester_config": {"inputs": {"InpRegime_Greed": {
            "type": "double", "default": 1.25, "start": 0.0, "step": 0.25, "stop": 5.0, "optimize": "Y"}}}}
        shared = discover_empty_shared_contract()
        self.assertEqual({"InpRegime_Greed": {"type": "double", "default": 1.25}},
                         source_builder.validate_strategy_input_defaults(source, spec, shared))
        changed = source.replace("1.25", "1.5", 1)
        with self.assertRaisesRegex(ValueError, "STRATEGY_INPUT_LOCK.*approved default"):
            source_builder.validate_strategy_input_defaults(changed, spec, shared)

    def test_strategy_input_string_literal_matches_stage1_serialized_default(self):
        source = 'input string InpBtcSymbol = "BTCUSD";\nstring feed = InpBtcSymbol;'
        spec = {"tester_config": {"inputs": {"InpBtcSymbol": {
            "type": "string", "default": '"BTCUSD"', "start": '"BTCUSD"',
            "step": '"BTCUSD"', "stop": '"BTCUSD"', "optimize": "N"}}}}
        shared = discover_empty_shared_contract()
        self.assertEqual({"InpBtcSymbol": {"type": "string", "default": "BTCUSD"}},
                         source_builder.validate_strategy_input_defaults(source, spec, shared))
        changed = source.replace("BTCUSD", "ETHUSD")
        with self.assertRaisesRegex(ValueError, "STRATEGY_INPUT_LOCK.*approved default"):
            source_builder.validate_strategy_input_defaults(changed, spec, shared)

    def test_approved_source_domain_comment_is_added_without_changing_input(self):
        source = "input int InpGKLookbackBars = 288;\nint n = InpGKLookbackBars;"
        spec = {"tester_config": {"inputs": {"InpGKLookbackBars": {
            "default": 288, "start": 144, "step": 12, "stop": 576, "optimize": "Y",
            "domain_provenance": {"method": "source_comment", "source_domain": [144, 576, 12]}}}}}
        repaired, events = source_builder.ensure_approved_optimization_domain_comments(source, spec, {"inputs": {}})
        self.assertIn("[lo=144,hi=576,step=12]", repaired)
        self.assertIn("InpGKLookbackBars = 288; // [lo=144,hi=576,step=12]", repaired)
        self.assertIn("InpGKLookbackBars = 288", repaired)
        self.assertEqual("approved_optimization_domain_comment_added", events[0]["kind"])
        again, second_events = source_builder.ensure_approved_optimization_domain_comments(repaired, spec, {"inputs": {}})
        self.assertEqual(repaired, again)
        self.assertEqual([], second_events)

    def test_researched_source_range_supplies_missing_domain_without_changing_range(self):
        source = "input int InpDccLookback = 250;\nint n = InpDccLookback;"
        item = {"default": 250, "start": 100, "step": 25, "stop": 500, "optimize": "Y",
                "domain_provenance": {"method": "source_comment", "basis": "Engle 2002 typical range 100-500",
                                      "research_status": "researched"}}
        spec = {"tester_config": {"inputs": {"InpDccLookback": item}}}
        repaired, events = source_builder.ensure_approved_optimization_domain_comments(source, spec, {"inputs": {}})
        self.assertIn("InpDccLookback = 250; // [lo=100,hi=500,step=25]", repaired)
        self.assertEqual((100, 25, 500), (item["start"], item["step"], item["stop"]))
        self.assertEqual("researched Stage 1 source_comment range bounds and step", events[0]["reason"])
        again, second_events = source_builder.ensure_approved_optimization_domain_comments(repaired, spec, {"inputs": {}})
        self.assertEqual(repaired, again)
        self.assertEqual([], second_events)

    def test_researched_source_range_requires_step_alignment(self):
        source = "input int InpDccLookback = 250;\nint n = InpDccLookback;"
        item = {"default": 250, "start": 100, "step": 30, "stop": 500, "optimize": "Y",
                "domain_provenance": {"method": "source_comment", "basis": "researched source",
                                      "research_status": "researched"}}
        spec = {"tester_config": {"inputs": {"InpDccLookback": item}}}
        with self.assertRaisesRegex(ValueError, "OPTIMIZATION_DOMAIN_LOCK.*not aligned"):
            source_builder.ensure_approved_optimization_domain_comments(source, spec, {"inputs": {}})

    def test_conflicting_source_domain_comment_is_not_rewritten(self):
        source = "input int InpGKLookbackBars = 288; // [lo=100,hi=600,step=12]\nint n=InpGKLookbackBars;"
        spec = {"tester_config": {"inputs": {"InpGKLookbackBars": {
            "default": 288, "start": 144, "step": 12, "stop": 576, "optimize": "Y",
            "domain_provenance": {"method": "source_comment", "source_domain": [144, 576, 12]}}}}}
        with self.assertRaisesRegex(ValueError, "OPTIMIZATION_DOMAIN_LOCK.*differs from authenticated"):
            source_builder.ensure_approved_optimization_domain_comments(source, spec, {"inputs": {}})

    def test_canonical_regime_grid_comment_is_added_and_locked(self):
        source = "input double InpRegime_Greed_Lot_Mult = 1.25; // regime weight\ndouble x = InpRegime_Greed_Lot_Mult;"
        item = {"type": "double", "default": 1.25, "start": 0.0, "step": 0.25, "stop": 5.0,
                "optimize": "Y", "domain_provenance": {"method": "stage2_regime_grid_policy"}}
        spec = {"tester_config": {"inputs": {"InpRegime_Greed_Lot_Mult": item}}}
        repaired, events = source_builder.ensure_approved_optimization_domain_comments(source, spec, {"inputs": {}})
        self.assertIn("[lo=0,hi=5,step=0.25]", repaired)
        self.assertEqual("canonical Stage 2 regime-grid policy", events[0]["reason"])
        again, second_events = source_builder.ensure_approved_optimization_domain_comments(repaired, spec, {"inputs": {}})
        self.assertEqual(repaired, again)
        self.assertEqual([], second_events)
        altered = {**item, "step": 0.5}
        bad_spec = {"tester_config": {"inputs": {"InpRegime_Greed_Lot_Mult": altered}}}
        with self.assertRaisesRegex(ValueError, "differ from canonical grid"):
            source_builder.ensure_approved_optimization_domain_comments(source, bad_spec, {"inputs": {}})

    def test_surgical_repair_feedback_preserves_constants_and_shows_rejected_source(self):
        failure = ValueError("STRATEGY_INPUT_LOCK: InpRegime_Greed default changed")
        diagnosis = source_builder.proposal_diagnosis(failure, {"source_text": "bad"}, [])
        feedback = source_builder.surgical_repair_feedback(
            failure, diagnosis, "input double InpRegime_Greed = 1.5;")
        self.assertEqual("STRATEGY_INPUT_LOCK", diagnosis["failure_kind"])
        self.assertIn("custom maximum", feedback)
        self.assertIn("input double InpRegime_Greed = 1.5;", feedback)
        self.assertIn("Repair only the failing implementation code", feedback)

    def test_source_input_usage_is_checked_without_model_quotes(self):
        spec = {"tester_config": {"inputs": {"InpATRPeriod": {}}}}
        source = """input int InpATRPeriod = 14; // comment
double atr = GetATR(InpATRPeriod);"""
        usage = source_builder.source_input_usage(source, spec, {"inputs": {}})
        self.assertEqual({"owner": "ea_source", "shared": False, "executable_occurrences": 2},
                         usage["InpATRPeriod"])

    def test_fixed_research_window_strings_are_authenticated_metadata(self):
        window = {"from_date": "2025.01.10", "to_date": "2026.01.10"}
        spec = {"test_plan": dict(window), "tester_config": {
            **window, "inputs": {
                "InpFromDate": {"default": window["from_date"], "start": window["from_date"],
                                "step": window["from_date"], "stop": window["from_date"], "optimize": "N"},
                "InpToDate": {"default": window["to_date"], "start": window["to_date"],
                              "step": window["to_date"], "stop": window["to_date"], "optimize": "N"},
            },
        }}
        source = '''input string InpFromDate = "2025.01.10"; // Informational only
input string InpToDate = "2026.01.10"; // Informational date string'''
        usage = source_builder.source_input_usage(source, spec, {"inputs": {}})
        self.assertEqual({"owner": "tester_config.test_window", "shared": False,
                          "metadata_only": True, "bound_field": "from_date", "value": "2025.01.10"},
                         usage["InpFromDate"])
        self.assertEqual({"owner": "tester_config.test_window", "shared": False,
                          "metadata_only": True, "bound_field": "to_date", "value": "2026.01.10"},
                         usage["InpToDate"])

    def test_window_metadata_exception_rejects_mismatched_dates(self):
        spec = {"test_plan": {"from_date": "2025.01.10", "to_date": "2026.01.10"},
                "tester_config": {"from_date": "2025.01.10", "to_date": "2026.01.10",
                                  "inputs": {"InpFromDate": {
                                      "default": "2024.01.10", "start": "2024.01.10",
                                      "step": "2024.01.10", "stop": "2024.01.10", "optimize": "N"}}}}
        source = 'input string InpFromDate = "2024.01.10";'
        with self.assertRaisesRegex(ValueError, "InpFromDate: declaration-only input"):
            source_builder.source_input_usage(source, spec, {"inputs": {}})
    def test_source_input_usage_ignores_comment_and_string_mentions(self):
        spec = {"tester_config": {"inputs": {"InpATRPeriod": {}}}}
        source = 'input int InpATRPeriod = 14; // InpATRPeriod\nstring note = "InpATRPeriod";'
        with self.assertRaisesRegex(ValueError, "InpATRPeriod: declaration-only input"):
            source_builder.source_input_usage(source, spec, {"inputs": {}})

    def test_source_input_usage_reports_all_missing_inputs_together(self):
        spec = {"tester_config": {"inputs": {"InpATRPeriod": {}, "InpSLMultiplier": {}}}}
        source = """input int InpATRPeriod = 14;
input double InpSLMultiplier = 2.0;
double atr = 0.0;"""
        with self.assertRaisesRegex(ValueError, "InpSLMultiplier: declaration-only input") as caught:
            source_builder.source_input_usage(source, spec, {"inputs": {}})
        self.assertIn("InpATRPeriod: declaration-only input", str(caught.exception))
    def write_json(self, path, value):
        path.write_text(json.dumps(value), encoding="utf-8")

    def isolated_verify_handoff(self, root, ea="FixtureEA"):
        cycle = "fixture-cycle"
        source = root / f"{ea}.mq5"
        ex5 = root / f"{ea}.ex5"
        minato = root / "minato_base_score_v2.mqh"
        spec = root / "stage1-spec.json"
        event = root / "stage1-event.json"
        source.write_text(
            """
            #include <minato_base_score_v2.mqh>
            double NormalizeLot(double requestedVolume)
            {
                double minVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
                double maxVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
                double volumeStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
                if (requestedVolume <= 0.0) return 0.0;
                if (minVolume <= 0.0) return 0.0;
                if (maxVolume < minVolume) return 0.0;
                if (volumeStep <= 0.0) return 0.0;
                double cappedVolume = MathMin(requestedVolume, maxVolume);
                double flooredVolume = MathFloor(cappedVolume / volumeStep) * volumeStep;
                if (flooredVolume < minVolume) return 0.0;
                double normalizedVolume = NormalizeDouble(flooredVolume, 8);
                if (normalizedVolume < minVolume) return 0.0;
                if (normalizedVolume > maxVolume) return 0.0;
                return normalizedVolume;
            }
            input int InpFixture = 1; // [lo=0,hi=20,step=1]
            input int InpRegime_A = 1;
            input int InpRegime_B = 1;
            input int InpRegime_C = 1;
            input int InpRegime_D = 1;
            input int InpRegime_E = 1;
            input int InpRegime_F = 1;
            input int InpRegime_G = 1;
            input int InpRegime_H = 1;
            input int InpRegime_I = 1;
            input int InpRegime_J = 1;
            input int InpRegime_K = 1;
            input int InpRegime_L = 1;
            input double InpMarginKillLevel = 120.0;
            int g_prevBars = 0;
            double RegimeSum() { return InpRegime_A+InpRegime_B+InpRegime_C+InpRegime_D+InpRegime_E+InpRegime_F+InpRegime_G+InpRegime_H+InpRegime_I+InpRegime_J+InpRegime_K+InpRegime_L; }
            bool NewBarClosed() { int bars=Bars(_Symbol, PERIOD_CURRENT); if(bars<=g_prevBars) return false; g_prevBars=bars; return true; }
            int OnInit() { MinatoV2ResetCapture(); if (!MinatoV2ConfigurationValid()) return INIT_PARAMETERS_INCORRECT; return INIT_SUCCEEDED; }
            void OnTick() {
                MinatoV2ObserveTick();
                double ml = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
                if (ml > 0.0 && ml < InpMarginKillLevel) { trade.PositionClose(_Symbol); return; }
                if (!NewBarClosed()) return;
                double entryPrice = 100.0;
                double lot = MinatoRegimeQuantity(1.0, 1.0);
                if (lot <= 0.0) return;
                lot = NormalizeLot(lot);
                if (lot <= 0.0) return;
                if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lot, entryPrice)) return;
                if (!FreeMarginAllowsNotional(lot, entryPrice)) return;
                double tp = MinatoTakeProfit(entryPrice, 5.0, 1.0, true);
                trade.Buy(lot, _Symbol, entryPrice, 95.0, tp);
            }
            bool FreeMarginAllowsNotional(double volume, double price) {
                if (volume <= 0.0 || price <= 0.0) return false;
                double notional = volume * price;
                double freeMargin = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
                if (freeMargin <= 0.0) return false;
                return (freeMargin >= 2.0 * notional);
            }
            double OnTester() { return MinatoV2ScoreFromTester(); }
            """,
            encoding="utf-8",
        )
        ex5.write_bytes(b"fixture-ex5")
        minato.write_text(
            "#define MINATO_BASE_SCORE_V2_VERSION \"MINATO_BASE_SCORE_V2.0\"\n"
            "#define MINATO_MIN_MARGIN_LEVEL 120.0\n"
            "double STAT_EQUITY_DDREL_PERCENT;\n"
            "double STAT_MIN_MARGINLEVEL;\n"
            "double ACCOUNT_EQUITY;\n"
            "double MinatoV2ScoreFromTester(){return 0.0;}\n"
            "void MinatoV2ObserveTick(){}\n"
            "void MinatoV2ResetCapture(){}\n"
            "bool MinatoV2ConfigurationValid(){return true;}\n"
            "bool g_minato_v2_runtime_margin_breach;\n"
            "double MinatoRegimeQuantity(double a,double b){return a;}\n"
            "double MinatoTakeProfit(double a,double b,double c,bool d){return c;}\n"
            "bool MinatoAllowsProjectedMargin(int a,double b,double c){return true;}\n"
            "bool NewBarClosed(){return true;}\n",
            encoding="utf-8",
        )
        self.write_json(spec, {"schema_version": 2, "cycle_id": cycle, "ea": ea, "tester_config": fixture_tester_config(cycle, ea)})
        self.write_json(event, {
            "schema_version": 2,
            "cycle_id": cycle,
            "artifact": str(spec.resolve()),
            "artifact_sha256": verify.sha256(spec),
        })
        return {
            "schema_version": 2,
            "cycle_id": cycle,
            "ea": ea,
            "source": str(source),
            "ex5": str(ex5),
            "canonical_minato": str(minato),
            "canonical_minato_sha256": verify.sha256(minato),
            "stage1_spec": str(spec),
            "stage1_spec_sha256": verify.sha256(spec),
            "approved_tester_config_sha256": verify.config_sha256(verify.stage2_config_from_spec(json.loads(spec.read_text(encoding="utf-8")), source_text=source.read_text(encoding="utf-8"))),
            "stage1_event": str(event),
            "score_version": "MINATO_BASE_SCORE_V2.0",
            "protected_behavior_contract": verify.verify_protected_contract(
                verify.assert_safe_root()
            ),
            "stage3_grid_preflight": prepare.validate_stage3_grid_before_publish(
                json.loads(spec.read_text(encoding="utf-8")),
                source.read_text(encoding="utf-8"), ea),
        }

    def valid_current_handoff(self, root):
        # Tests must never depend on or mutate the live Stage 2 handoff.
        return self.isolated_verify_handoff(root)

    def test_fixture_source_is_closed_bar_and_minato_structurally_valid(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.isolated_verify_handoff(Path(directory))
            result = verify.verify(handoff)
            self.assertTrue(result["passed"])
            self.assertTrue(result["checks"]["closed_bar_only"])
            self.assertTrue(result["checks"]["canonical_minato_hash"])
            self.assertEqual("FixtureEA", result["ea"])

    def test_verifier_rejects_any_second_unguarded_order_path(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.isolated_verify_handoff(Path(directory))
            source = Path(handoff["source"])
            text = source.read_text(encoding="utf-8")
            source.write_text(text.replace(
                "double OnTester() { return MinatoV2ScoreFromTester(); }",
                "trade.Sell(lot, _Symbol, 100.0, 105.0, tp);\n"
                "double OnTester() { return MinatoV2ScoreFromTester(); }"), encoding="utf-8")
            result = verify.verify(handoff)
            self.assertFalse(result["passed"])
            self.assertFalse(result["checks"]["every_order_path_margin_quantity_tp_safe"])

    def test_verifier_accepts_take_profit_assignment_inside_nested_validity_guard(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.isolated_verify_handoff(Path(directory))
            source = Path(handoff["source"])
            text = source.read_text(encoding="utf-8")
            text = text.replace(
                "double tp = MinatoTakeProfit(entryPrice, 5.0, 1.0, true);",
                "double tp = 0.0;\n                if (entryPrice > 0.0) {\n                    tp = MinatoTakeProfit(entryPrice, 5.0, 1.0, true);\n                }")
            source.write_text(text, encoding="utf-8")
            result = verify.verify(handoff)
            self.assertTrue(result["passed"], result)

    def test_free_margin_notional_helper_accepts_equivalent_bounded_comparison(self):
        source = """bool FreeMarginAllowsNotional(double volume, double price) {
if (volume <= 0.0 || price <= 0.0) return false;
double notional = volume * price;
double free_margin = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
if (free_margin <= 0.0) return false;
return (2.0 * notional) <= free_margin;
}"""
        self.assertTrue(verify.free_margin_notional_helper_is_safe(source))
        self.assertFalse(verify.free_margin_notional_helper_is_safe(
            source.replace("(2.0 * notional) <= free_margin", "free_margin <= (2.0 * notional)")))

    def test_free_margin_notional_helper_accepts_equivalent_rejecting_branch(self):
        source = """bool FreeMarginAllowsNotional(double volume, double price) {
if(volume <= 0.0 || price <= 0.0) return false;
double notional = volume * price;
double equity = AccountInfoDouble(ACCOUNT_EQUITY);
double free_m = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
if(equity <= 0.0 || free_m <= 0.0) return false;
double required = 2.0 * notional;
if(free_m < required) return false;
return true;
}"""
        self.assertTrue(verify.free_margin_notional_helper_is_safe(source))
        self.assertFalse(verify.free_margin_notional_helper_is_safe(
            source.replace("free_m < required", "free_m > required")))
        self.assertFalse(verify.free_margin_notional_helper_is_safe(
            source.replace("2.0 * notional", "1.0 * notional")))

    def test_free_margin_notional_helper_requires_read_only_2x_contract(self):
        source = """bool FreeMarginAllowsNotional(double volume, double price) {
if (volume <= 0.0 || price <= 0.0) return false;
double n = volume * price;
double fm = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
return (fm >= 2.0 * n);
}"""
        self.assertTrue(verify.free_margin_notional_helper_is_safe(source))
        self.assertFalse(verify.free_margin_notional_helper_is_safe(source.replace("double volume", "double &volume")))

    def test_verifier_rejects_order_path_with_zero_tp_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.isolated_verify_handoff(Path(directory))
            source = Path(handoff["source"])
            text = source.read_text(encoding="utf-8")
            text = text.replace("trade.Buy(lot, _Symbol, entryPrice, 95.0, tp);",
                                "trade.Buy(lot, _Symbol, entryPrice, 95.0, 0.0);")
            source.write_text(text, encoding="utf-8")
            result = verify.verify(handoff)
            self.assertFalse(result["passed"])
            self.assertFalse(result["checks"]["every_order_path_margin_quantity_tp_safe"])

    def test_verifier_accepts_guarded_order_with_direct_failure_return(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.isolated_verify_handoff(Path(directory))
            source = Path(handoff["source"])
            text = source.read_text(encoding="utf-8")
            source.write_text(text.replace(
                "trade.Buy(lot, _Symbol, entryPrice, 95.0, tp);",
                "if (!trade.Buy(lot, _Symbol, entryPrice, 95.0, tp)) return;"),
                encoding="utf-8")
            result = verify.verify(handoff)
            self.assertTrue(result["passed"], result)
            self.assertTrue(result["checks"]["every_order_path_margin_quantity_tp_safe"])

    def test_verifier_accepts_guarded_order_in_if_with_single_statement_success_body(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.isolated_verify_handoff(Path(directory))
            source = Path(handoff["source"])
            text = source.read_text(encoding="utf-8")
            text = text.replace(
                "int g_prevBars = 0;",
                "int g_prevBars = 0;\n                datetime g_lastEntryBar;\n                datetime g_closedBarTime;",
            )
            text = text.replace(
                "trade.Buy(lot, _Symbol, entryPrice, 95.0, tp);",
                "if (trade.Buy(lot, _Symbol, entryPrice, 95.0, tp)) g_lastEntryBar = g_closedBarTime;",
            )
            source.write_text(text, encoding="utf-8")
            result = verify.verify(handoff)
            self.assertTrue(result["passed"], result)
            self.assertTrue(result["checks"]["every_order_path_margin_quantity_tp_safe"])

            unsafe = text.replace(
                "if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lot, entryPrice)) return;",
                "",
            )
            source.write_text(unsafe, encoding="utf-8")
            rejected = verify.verify(handoff)
            self.assertFalse(rejected["checks"]["every_order_path_margin_quantity_tp_safe"], rejected)

            unsupported = text.replace(
                "if (trade.Buy(lot, _Symbol, entryPrice, 95.0, tp)) g_lastEntryBar = g_closedBarTime;",
                "bool placed = trade.Buy(lot, _Symbol, entryPrice, 95.0, tp);",
            )
            source.write_text(unsupported, encoding="utf-8")
            unsupported_result = verify.verify(handoff)
            self.assertFalse(unsupported_result["checks"]["every_order_path_margin_quantity_tp_safe"], unsupported_result)

    def test_order_guard_accepts_quantity_normalization_in_ancestor_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.isolated_verify_handoff(Path(directory))
            source_path = Path(handoff["source"])
            source = source_path.read_text(encoding="utf-8")
            original = (
                "                if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lot, entryPrice)) return;\n"
                "                if (!FreeMarginAllowsNotional(lot, entryPrice)) return;\n"
                "                double tp = MinatoTakeProfit(entryPrice, 5.0, 1.0, true);\n"
                "                trade.Buy(lot, _Symbol, entryPrice, 95.0, tp);"
            )
            nested = (
                "                if (entryPrice > 0.0) {\n"
                "                    if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lot, entryPrice)) return;\n"
                "                    if (!FreeMarginAllowsNotional(lot, entryPrice)) return;\n"
                "                    double tp = MinatoTakeProfit(entryPrice, 5.0, 1.0, true);\n"
                "                    trade.Buy(lot, _Symbol, entryPrice, 95.0, tp);\n"
                "                } else {\n"
                "                    if (!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, lot, entryPrice)) return;\n"
                "                    if (!FreeMarginAllowsNotional(lot, entryPrice)) return;\n"
                "                    double tp = MinatoTakeProfit(entryPrice, 5.0, 1.0, false);\n"
                "                    trade.Sell(lot, _Symbol, entryPrice, 105.0, tp);\n"
                "                }"
            )
            self.assertIn(original, source)
            source_path.write_text(source.replace(original, nested), encoding="utf-8")
            result = verify.verify(handoff)
            self.assertTrue(result["checks"]["every_order_path_margin_quantity_tp_safe"], result)

            unsafe = source.replace("lot = NormalizeLot(lot);",
                                    "lot = NormalizeLot(lot);\n                UnknownVolumePolicy(lot);")
            source_path.write_text(unsafe, encoding="utf-8")
            rejected = verify.verify(handoff)
            self.assertFalse(rejected["checks"]["every_order_path_margin_quantity_tp_safe"], rejected)

    def test_order_guard_accepts_math_is_valid_number_for_submitted_tp(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.isolated_verify_handoff(Path(directory))
            source_path = Path(handoff["source"])
            source = source_path.read_text(encoding="utf-8")
            canonical_tp = "double tp = MinatoTakeProfit(entryPrice, 5.0, 1.0, true);"
            guarded_tp = canonical_tp + "\n                if (!MathIsValidNumber(tp) || tp <= 0.0) return;"
            self.assertIn(canonical_tp, source)
            source_path.write_text(source.replace(canonical_tp, guarded_tp), encoding="utf-8")

            result = verify.verify(handoff)
            self.assertTrue(result["checks"]["every_order_path_margin_quantity_tp_safe"], result)

            mathabs_guarded = source.replace(
                canonical_tp, canonical_tp + "\n                if (MathAbs(tp - entryPrice) < 1.0) return;")
            source_path.write_text(mathabs_guarded, encoding="utf-8")
            mathabs_result = verify.verify(handoff)
            self.assertTrue(mathabs_result["checks"]["every_order_path_margin_quantity_tp_safe"], mathabs_result)

            unsafe = mathabs_guarded.replace(
                "MathAbs(tp - entryPrice)", "UnknownTpPolicy(tp)")
            source_path.write_text(unsafe, encoding="utf-8")
            rejected = verify.verify(handoff)
            self.assertFalse(rejected["checks"]["every_order_path_margin_quantity_tp_safe"], rejected)

    def test_order_guard_accepts_only_unchanged_one_hop_quote_alias(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.isolated_verify_handoff(Path(directory))
            source_path = Path(handoff["source"])
            source = source_path.read_text(encoding="utf-8")
            source = source.replace("double entryPrice = 100.0;", "double ask = 100.0;")
            source = source.replace(
                "MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lot, entryPrice)",
                "MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lot, ask)")
            source = source.replace("FreeMarginAllowsNotional(lot, entryPrice)",
                                    "FreeMarginAllowsNotional(lot, ask)")
            source = source.replace("double tp = MinatoTakeProfit(entryPrice,",
                                    "double entryPrice = ask;\n"
                                    "                double tp = MinatoTakeProfit(entryPrice,")
            source_path.write_text(source, encoding="utf-8")
            result = verify.verify(handoff)
            self.assertTrue(result["checks"]["every_order_path_margin_quantity_tp_safe"], result)

            # Reassigning the captured order price breaks the alias proof.
            source_path.write_text(source.replace("double entryPrice = ask;",
                                                  "double entryPrice = ask; entryPrice = 1.0;"),
                                   encoding="utf-8")
            unsafe = verify.verify(handoff)
            self.assertFalse(unsafe["checks"]["every_order_path_margin_quantity_tp_safe"])

    def test_verifier_accepts_order_call_in_success_block(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.isolated_verify_handoff(Path(directory))
            source = Path(handoff["source"])
            text = source.read_text(encoding="utf-8")
            text = text.replace(
                "trade.Buy(lot, _Symbol, entryPrice, 95.0, tp);",
                "if (trade.Buy(lot, _Symbol, entryPrice, 95.0, tp)) {\n"
                "                    g_prevBars = Bars(_Symbol, PERIOD_CURRENT);\n"
                "                }\n                return;",
            )
            source.write_text(text, encoding="utf-8")
            result = verify.verify(handoff)
            self.assertTrue(result["passed"], result)
            self.assertTrue(result["checks"]["every_order_path_margin_quantity_tp_safe"])

    def test_verifier_accepts_normalized_take_profit_before_order_call(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.isolated_verify_handoff(Path(directory))
            source = Path(handoff["source"])
            text = source.read_text(encoding="utf-8")
            text = text.replace(
                "double tp = MinatoTakeProfit(entryPrice, 5.0, 1.0, true);",
                "double tp = MinatoTakeProfit(entryPrice, 5.0, 1.0, true);\n"
                "                if (tp > 0.0) tp = NormalizeDouble(tp, _Digits);",
            )
            source.write_text(text, encoding="utf-8")
            result = verify.verify(handoff)
            self.assertTrue(result["passed"], result)
            self.assertTrue(result["checks"]["every_order_path_margin_quantity_tp_safe"])

    def test_authenticated_margin_gate_allows_strategy_regime_defines(self):
        import kcos_runtime_paths_v2 as runtime_paths

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            terminal = root / "terminal"
            include = terminal / "MQL5" / "Include" / "minato_base_score_v2.mqh"
            include.parent.mkdir(parents=True)
            include.write_text("#define MINATO_MIN_MARGIN_LEVEL 120.0\n", encoding="utf-8")
            approval_path = root / "state" / "contracts" / "minato_base_score_approved_v2.json"
            approval_path.parent.mkdir(parents=True)
            include_hash = hashlib.sha256(include.read_bytes()).hexdigest().upper()
            self.write_json(approval_path, {
                "approved": True, "score_version": "MINATO_BASE_SCORE_V2.0", "sha256": include_hash,
            })
            source = """
            #include <minato_base_score_v2.mqh>
            #define KCOS_REGIME_NEUTRAL 0
            CTrade trade;
            int g_pending_regime = KCOS_REGIME_NEUTRAL;
            input int InpMagicNumber = 1;
            bool MarginBreached() {
                double used = AccountInfoDouble(ACCOUNT_MARGIN);
                double level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
                return (used > 0.0 && level > 0.0 && level < MINATO_MIN_MARGIN_LEVEL);
            }
            void CloseAllPositions() {
                for(int i = PositionsTotal() - 1; i >= 0; i--) {
                    ulong ticket = PositionGetTicket(i);
                    if(ticket == 0) continue;
                    if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
                    if(PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;
                    trade.PositionClose(ticket);
                }
                g_has_position = false;
                g_pending_regime = KCOS_REGIME_NEUTRAL;
            }
            void OnTick() {
                MinatoV2ObserveTick();
                if(MarginBreached()) { CloseAllPositions(); g_entry_bar_index = -1; g_has_position = false; g_pending_buy = false; g_pending_regime = KCOS_REGIME_NEUTRAL; return; }
            }
            """
            self.assertTrue(verify.live_margin_path_is_safe(source))
            handoff = {"canonical_minato": str(include), "canonical_minato_sha256": include_hash}
            with mock.patch.object(runtime_paths, "mt5_data_root", return_value=terminal), \
                 mock.patch.object(runtime_paths, "assert_safe_root", return_value=root):
                self.assertTrue(verify.margin_kill_120(source, handoff))

    def test_authenticated_margin_wrapper_accepts_last_entry_bar_reset(self):
        source = """
        CTrade g_trade;
        input int InpMagicNumber = 7;
        bool MarginBreached() {
          double used = AccountInfoDouble(ACCOUNT_MARGIN);
          double level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
          return used > 0.0 && level > 0.0 && level < MINATO_MIN_MARGIN_LEVEL;
        }
        void CloseAllPositions() {
          CTrade closer;
          closer.SetExpertMagicNumber((ulong)InpMagicNumber);
          for(int i = PositionsTotal() - 1; i >= 0; i--) {
            ulong ticket = PositionGetTicket(i);
            if(ticket == 0) continue;
            if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
            if(PositionGetInteger(POSITION_MAGIC) != (long)InpMagicNumber) continue;
            closer.PositionClose(ticket);
          }
          g_last_entry_bar = -1;
        }
        void OnTick() {
          MinatoV2ObserveTick();
          if(MarginBreached()) { CloseAllPositions(); return; }
        }
        """
        self.assertTrue(verify._canonical_margin_wrapper_is_concrete(
            verify.executable_source(source)))
        parenthesized_decrement = source.replace("i >= 0; i--", "i >= 0; (i--)")
        self.assertIn("i >= 0; (i--)", parenthesized_decrement)
        self.assertTrue(verify._canonical_margin_wrapper_is_concrete(
            verify.executable_source(parenthesized_decrement)))

    def test_authenticated_margin_wrapper_accepts_magic_number_type_cast(self):
        source = """
        CTrade g_trade;
        input int InpMagicNumber = 7;
        bool MarginBreached() {
          double used = AccountInfoDouble(ACCOUNT_MARGIN);
          double level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
          return (used > 0.0 && level > 0.0 && level < MINATO_MIN_MARGIN_LEVEL);
        }
        void CloseAllPositions() {
          for (int i = PositionsTotal() - 1; i >= 0; i--) {
            ulong ticket = PositionGetTicket(i);
            if (ticket == 0) continue;
            if (PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
            if (PositionGetInteger(POSITION_MAGIC) != (long)InpMagicNumber) continue;
            g_trade.PositionClose(ticket);
          }
        }
        void OnTick() {
          MinatoV2ObserveTick();
          if (MarginBreached()) { CloseAllPositions(); return; }
          return;
        }
        """
        self.assertTrue(verify._canonical_margin_wrapper_is_concrete(
            verify.executable_source(source)))

    def test_prepare_fails_closed_on_current_invalidated_lineage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cycle = "fixture-invalid-lineage"
            ea = "FixtureEA"
            state = root / "state.json"
            stage1_handoff = root / "Stage1_v2.json"
            source = root / f"{ea}.mq5"
            spec = root / "stage1-spec.json"
            event = root / "stage1-event.json"
            minato = root / "minato_base_score.mqh"
            metaeditor = root / "metaeditor64.exe"
            approval = root / "minato-approval.json"
            source.write_text("// fixture source", encoding="utf-8")
            minato.write_text("// fixture Minato", encoding="utf-8")
            metaeditor.write_bytes(b"fixture-metaeditor")
            self.write_json(state, {
                "schema_version": 2,
                "cycle_id": cycle,
                "current_stage": "stage2_pending",
                "current_ea": ea,
            })
            self.write_json(spec, {
                "schema_version": 2,
                "cycle_id": cycle,
                "stage": 1,
                "ea": ea,
                "decision": "approved_for_stage2",
                "tester_config": fixture_tester_config(cycle, ea),
            })
            self.write_json(event, {
                "schema_version": 2,
                "stage": 1,
                "status": "complete",
                "cycle_id": cycle,
                "artifact": str(spec.resolve()),
                "artifact_sha256": "STALE-SPEC-HASH",
            })
            self.write_json(stage1_handoff, {
                "schema_version": 2,
                "cycle_id": cycle,
                "candidate_ea": ea,
                "candidate_source": str(source),
                "expected_spec_path": str(spec),
                "expected_event_path": str(event),
            })
            self.write_json(approval, {"approved": True, "sha256": prepare.sha256(minato)})
            with mock.patch.multiple(
                prepare,
                STATE=state,
                STAGE1_HANDOFF=stage1_handoff,
                MINATO=minato,
                METAEDITOR=metaeditor,
                MINATO_APPROVAL=approval,
            ):
                with self.assertRaisesRegex(ValueError, "completion event does not prove"):
                    prepare.prepare(dry_run=True)

    def test_publisher_advances_only_v2_and_holds_stage3(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_root = Path(__file__).resolve().parent
            state_root = root / "state"
            (state_root / "contracts").mkdir(parents=True)
            for name in verify.STAGE2_PIPELINE_CODE_FILES:
                shutil.copy2(source_root / name, state_root / name)
            shutil.copy2(source_root / "contracts" / "kcos_stage2_protected_behavior_v1.json",
                         state_root / "contracts" / "kcos_stage2_protected_behavior_v1.json")
            contract = json.loads((state_root / "contracts" / "kcos_stage2_protected_behavior_v1.json").read_text())
            for name in ("regime", "minato"):
                relative = Path(contract["protected_behavior"][name]["include"])
                destination = state_root / "mt5_stage45_portable" / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_root / "mt5_stage45_portable" / relative, destination)
            protected = publish.verify_protected_contract(root)
            artifact = root / "verify.json"
            event = root / "event.json"
            state = root / "state.json"
            stage1_spec = root / "stage1.json"; stage1_spec.write_text(json.dumps({"tester_config": fixture_tester_config("cycle-1", "EA")}), encoding="utf-8")
            stage1_event = root / "stage1-event.json"
            stage1_event.write_text(json.dumps({"artifact_sha256": publish.sha256(stage1_spec)}), encoding="utf-8")
            source_path = root / "EA.mq5"; source_path.write_text("input double InpFixture = 1.0;\n", encoding="utf-8")
            ex5_path = root / "EA.ex5"; ex5_path.write_bytes(b"verified binary")
            minato_path = state_root / "mt5_stage45_portable" / "MQL5" / "Include" / "minato_base_score_v2.mqh"
            artifact.write_text(json.dumps({
                "schema_version": 2,
                "cycle_id": "cycle-1",
                "stage": 2,
                "ea": "EA",
                "passed": True,
                "score_version": "MINATO_BASE_SCORE_V2.0",
                "compile": {"errors": 0, "warnings": 0, "attempt": 2, "ex5_sha256": publish.sha256(ex5_path)},
                "checks": {key: True for key in verify.STAGE2_RECOVERY_REQUIRED_CHECK_KEYS},
                "source": str(source_path), "source_sha256": publish.sha256(source_path),
                "ex5": str(ex5_path),
                "canonical_minato": str(minato_path),
                "canonical_minato_sha256": publish.sha256(minato_path),
                "protected_behavior_contract": protected,
                "verification_policy_sha256": publish.sha256(state_root / "contracts" / "kcos_stage2_protected_behavior_v1.json"),
                "verification_policy_id": "kcos-stage2-protected-behavior-v1+math-correction-admission-v1",
                "verifier_code_sha256": publish.sha256(state_root / "kcos_stage2_verify_v2.py"),
                "stage2_pipeline_code_hashes": {name: publish.sha256(state_root / name) for name in verify.STAGE2_PIPELINE_CODE_FILES},
                "stage1_spec": str(stage1_spec),
                "stage1_spec_sha256": publish.sha256(stage1_spec),
                "approved_tester_config_sha256": publish.config_sha256(publish.stage2_config_from_spec(
                    {"tester_config": fixture_tester_config("cycle-1", "EA")},
                    source_text=source_path.read_text(encoding="utf-8"))),
                "stage1_event": str(stage1_event),
            }), encoding="utf-8")
            failed_dir = root / "state" / "events"
            failed_dir.mkdir(parents=True)
            failed_event = failed_dir / "cycle-1.stage2.failed.json"
            failed_event.write_text(json.dumps({"cycle_id": "cycle-1", "rotation_eligible": False}), encoding="utf-8")
            state.write_text(json.dumps({
                "schema_version": 2,
                "cycle_id": "cycle-1",
                "current_stage": "stage2_running",
                "current_ea": "EA",
                "stage2_failure_event": str(failed_event),
                "stage2_repair_reason": "a prior repair reason",
                "stage2_repair_required": True,
            }), encoding="utf-8")
            stage2_handoff = root / "state" / "handoffs" / "Stage2_v2.json"
            stage2_handoff.parent.mkdir(parents=True)
            approved_hash = publish.config_sha256(publish.stage2_config_from_spec(
                {"tester_config": fixture_tester_config("cycle-1", "EA")},
                source_text=source_path.read_text(encoding="utf-8")))
            stage2_handoff.write_text(json.dumps({
                "cycle_id": "cycle-1", "source": str(source_path.resolve()),
                "approved_tester_config_sha256": approved_hash,
            }), encoding="utf-8")
            template = root / "state" / "kcos_genetic_opt_template.stage3-utf8.ini"
            shutil.copy2(source_root / "kcos_genetic_opt_template.stage3-utf8.ini", template)
            with mock.patch.multiple(publish, ROOT=root, LEASE=root / "state" / "leases" / "stage2.lock.json",
                                    STAGE2_HANDOFF=stage2_handoff, STAGE3_TEMPLATE=template):
                result = publish.publish(state, artifact, event)
            published_handoff = json.loads(stage2_handoff.read_text(encoding="utf-8"))
            ini_artifacts = published_handoff["tester_ini_artifacts"]
            self.assertEqual("published", ini_artifacts["status"])
            self.assertEqual(publish.sha256(template), ini_artifacts["template_ini_sha256"])
            self.assertEqual("published", json.loads(event.read_text(encoding="utf-8"))["tester_ini_artifacts"]["status"])
            self.assertEqual({"M5", "M15", "H1"}, set(ini_artifacts["files"]))
            for timeframe, entry in ini_artifacts["files"].items():
                ini_path = Path(entry["path"])
                self.assertEqual(entry["sha256"], publish.sha256(ini_path))
                payload = ini_path.read_text(encoding="utf-8")
                values = re.search(r"(?m)^InpFixture=([^\r\n]+)$", payload).group(1).split("||")
                step_count = round((float(values[3]) - float(values[1])) / float(values[2]))
                self.assertTrue(15 <= step_count <= 20, (timeframe, step_count))
                self.assertAlmostEqual(1.0, float(values[0]))
            # The source is unchanged, but an older verifier-policy receipt is
            # not current clearance. Publication explains the policy drift and
            # requires requalification under the active contract.
            verified_artifact_bytes = artifact.read_bytes()
            old_policy = json.loads(verified_artifact_bytes)
            old_policy["verification_policy_id"] = "kcos-stage2-protected-behavior-v0"
            artifact.write_text(json.dumps(old_policy), encoding="utf-8")
            policy_state = root / "policy-requalification-state.json"
            policy_state.write_text(json.dumps({"schema_version": 2, "cycle_id": "cycle-1",
                                                "current_stage": "stage2_running", "current_ea": "EA"}),
                                    encoding="utf-8")
            with mock.patch.multiple(publish, ROOT=root, LEASE=root / "state" / "leases" / "stage2.lock.json",
                                    STAGE2_HANDOFF=stage2_handoff, STAGE3_TEMPLATE=template):
                with self.assertRaisesRegex(ValueError, "verifier, policy, or protected integration changed"):
                    publish.publish(policy_state, artifact, root / "policy-event.json")
            artifact.write_bytes(verified_artifact_bytes)
            for label, target, replacement, message in (
                ("binary", ex5_path, b"tampered binary", "binary changed after final verification"),
                ("minato", minato_path, minato_path.read_bytes() + b"// tampered\\n",
                 "canonical Minato include changed after final verification"),
            ):
                original = target.read_bytes()
                target.write_bytes(replacement)
                blocked_state = root / f"{label}-tamper-state.json"
                blocked_state.write_text(json.dumps({"schema_version": 2, "cycle_id": "cycle-1",
                                                     "current_stage": "stage2_running", "current_ea": "EA"}),
                                         encoding="utf-8")
                with mock.patch.multiple(publish, ROOT=root, LEASE=root / "state" / "leases" / "stage2.lock.json",
                                        STAGE2_HANDOFF=stage2_handoff, STAGE3_TEMPLATE=template):
                    with self.assertRaisesRegex(ValueError, message):
                        publish.publish(blocked_state, artifact, root / f"{label}-tamper-event.json")
                target.write_bytes(original)
            regime_relative = Path(contract["protected_behavior"]["regime"]["include"])
            regime_path = state_root / "mt5_stage45_portable" / regime_relative
            original_regime = regime_path.read_bytes()
            regime_path.write_bytes(original_regime + b"// changed regime contract\\n")
            regime_state = root / "regime-tamper-state.json"
            regime_state.write_text(json.dumps({"schema_version": 2, "cycle_id": "cycle-1",
                                                "current_stage": "stage2_running", "current_ea": "EA"}),
                                    encoding="utf-8")
            with mock.patch.multiple(publish, ROOT=root, LEASE=root / "state" / "leases" / "stage2.lock.json",
                                    STAGE2_HANDOFF=stage2_handoff, STAGE3_TEMPLATE=template):
                with self.assertRaises((ValueError, AssertionError)):
                    publish.publish(regime_state, artifact, root / "regime-tamper-event.json")
            regime_path.write_bytes(original_regime)
            config_receipt = json.loads(verified_artifact_bytes)
            config_receipt["approved_tester_config_sha256"] = "0" * 64
            artifact.write_text(json.dumps(config_receipt), encoding="utf-8")
            config_state = root / "config-tamper-state.json"
            config_state.write_text(json.dumps({"schema_version": 2, "cycle_id": "cycle-1",
                                                "current_stage": "stage2_running", "current_ea": "EA"}),
                                    encoding="utf-8")
            with mock.patch.multiple(publish, ROOT=root, LEASE=root / "state" / "leases" / "stage2.lock.json",
                                    STAGE2_HANDOFF=stage2_handoff, STAGE3_TEMPLATE=template):
                with self.assertRaisesRegex(ValueError, "approved tester_config"):
                    publish.publish(config_state, artifact, root / "config-tamper-event.json")
            artifact.write_bytes(verified_artifact_bytes)
            next_state = json.loads(state.read_text())
            quarantined = root / "state" / "diagnostics" / "superseded" / failed_event.name
            self.assertTrue(quarantined.is_file())
            self.assertEqual(str(quarantined.resolve()), next_state.get("stage2_superseded_failure_event"))
            self.assertIsNone(next_state.get("stage2_failure_event"))
            self.assertIsNone(next_state.get("stage2_repair_reason"))
            self.assertFalse(next_state.get("stage2_repair_required"))
            self.assertEqual("stage3_pending", next_state["current_stage"])
            self.assertEqual("stage2_complete_held", next_state["stage_status"])
            self.assertEqual(2, result["stage"])

            tamper_state = root / "tamper-state.json"
            tamper_state.write_text(json.dumps({"schema_version": 2, "cycle_id": "cycle-1",
                                                "current_stage": "stage2_running", "current_ea": "EA"}),
                                    encoding="utf-8")
            source_path.write_text("// changed after verification\n", encoding="utf-8")
            with mock.patch.multiple(publish, ROOT=root, LEASE=root / "state" / "leases" / "stage2.lock.json",
                                    STAGE2_HANDOFF=stage2_handoff, STAGE3_TEMPLATE=template):
                with self.assertRaisesRegex(ValueError, "source changed after final verification"):
                    publish.publish(tamper_state, artifact, root / "tamper-event.json")
            source_path.write_text("input double InpFixture = 1.0;\n", encoding="utf-8")

            full_state = root / "full-state.json"
            full_event = root / "full-event.json"
            full_state.write_text(json.dumps({
                "schema_version": 2,
                "cycle_id": "cycle-1",
                "current_stage": "stage2_running",
                "current_ea": "EA",
                "run_authorization": {
                    "schema_version": 1,
                    "cycle_id": "cycle-1",
                    "scope": "current_cycle_stages_1_to_5",
                    "allowed_stages": [1, 2, 3, 4, 5],
                    "authorized_by": "user_confirmed",
                    "hold_after_stage": 5,
                    "reason": "User authorized this cycle through Stage 5.",
                    "authorized_utc": "2026-10-10T00:00:00Z",
                },
            }), encoding="utf-8")
            with mock.patch.multiple(publish, ROOT=root, LEASE=root / "state" / "leases" / "stage2.lock.json",
                                    STAGE2_HANDOFF=stage2_handoff, STAGE3_TEMPLATE=template):
                publish.publish(full_state, artifact, full_event)
            full_next_state = json.loads(full_state.read_text())
            self.assertEqual("stage3_pending", full_next_state["current_stage"])
            self.assertEqual("stage2_complete_held", full_next_state["stage_status"])
            self.assertEqual("audit_stage2_before_stage3", full_next_state["next_action"])
            self.assertEqual(2, full_next_state["hold_after_stage"])

    def test_publisher_rejects_warnings(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "verify.json"
            state = root / "state.json"
            artifact.write_text(json.dumps({
                "schema_version": 2, "cycle_id": "cycle-1", "stage": 2,
                "ea": "EA", "passed": True,
                "compile": {"errors": 0, "warnings": 1, "attempt": 1, "ex5_sha256": "ABC"},
                "checks": {"closed_bar_only": True, "canonical_minato_hash": True},
            }), encoding="utf-8")
            state.write_text(json.dumps({
                "schema_version": 2, "cycle_id": "cycle-1",
                "current_stage": "stage2_running", "current_ea": "EA",
            }), encoding="utf-8")
            with self.assertRaises(ValueError):
                publish.publish(state, artifact, root / "event.json")

    def test_compile_log_counts_are_parsed(self):
        from kcos_stage2_compile_v2 import parse_compile_log
        self.assertEqual((0, 2), parse_compile_log("Result: 0 errors, 2 warnings"))
        self.assertEqual((3, 0), parse_compile_log("Result: 3 errors, 0 warnings"))

    def test_bollinger_prior_band_rejects_bar1_stddev(self):
        source = """
        if (!NewBarClosed()) return;
        CopyClose(_Symbol, PERIOD_CURRENT, 2, 1, closebuf);
        CopyBuffer(g_bb_mid_handle, 0, 2, 1, midbuf);
        CopyBuffer(g_bb_std_handle, 0, 2, 1, stdbuf);
        double prior_upper = prior_mid + InpBbStdDev * std1;
        double prior_lower = prior_mid - InpBbStdDev * std1;
        """
        self.assertFalse(verify.closed_bar_check(source))

    def test_verifier_normalizes_stage2_tester_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = self.isolated_verify_handoff(root)
            ini = root / "stage2.ini"
            ini.write_text(
                "[Tester]\n"
                "Expert=EA\n"
                "Model=0\nOptimization=0\nProfitInPips=1\nLeverage=50\nExecutionMode=0\n",
                encoding="utf-8-sig",
            )
            handoff["ini"] = str(ini)
            result = verify.verify(handoff)
            self.assertTrue(result["checks"]["mt5_tester_config_normalized"])
            rewritten = {k for k, v in result["ini_normalize_report"].items()
                        if v["action"] == "rewritten"}
            self.assertEqual({"Model", "Optimization", "ProfitInPips",
                              "Leverage", "ExecutionMode"}, rewritten)
            text = ini.read_text(encoding="utf-8-sig")
            for key, value in (("Model", "1"), ("Optimization", "2"),
                               ("ProfitInPips", "0"), ("Leverage", "25"),
                               ("ExecutionMode", "100")):
                self.assertIn(f"{key}={value}", text)

    def test_verifier_rejects_missing_ini_when_present(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handoff = self.isolated_verify_handoff(root)
            handoff["ini"] = str(root / "does_not_exist.ini")
            result = verify.verify(handoff)
            self.assertFalse(result["checks"]["mt5_tester_config_normalized"])

    def test_verifier_rejects_compile_evidence_for_an_old_source(self):
        with tempfile.TemporaryDirectory() as directory:
            handoff = self.valid_current_handoff(Path(directory))
            stale = {
                "schema_version": 2, "cycle_id": handoff["cycle_id"], "stage": 2,
                "ea": handoff["ea"], "attempt": 4, "errors": 0, "warnings": 0,
                "fresh_ex5": True, "mq5_sha256": "OLD", "ex5_sha256": "OLD",
            }
            result = verify.verify(handoff, stale)
            self.assertFalse(result["passed"])
            self.assertFalse(result["checks"]["compile_clean"])


if __name__ == "__main__":
    unittest.main()


