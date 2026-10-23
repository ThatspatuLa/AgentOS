import importlib.util
import sys
import unittest
from pathlib import Path


sys.dont_write_bytecode = True
LIVE_STATE = Path(r"C:\Users\Tyson\Projects\kiyosaki_stage2b\state")
sys.path.insert(0, str(LIVE_STATE))
MODULE_PATH = Path(__file__).with_name("kcos_stage2_generate_source_v2.py")
spec = importlib.util.spec_from_file_location("stage2_generator_under_test", MODULE_PATH)
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


class SharedCreationPromptTests(unittest.TestCase):
    def test_generation_retrieval_rejects_unscoped_failure_identity(self):
        result = generator.reusable_lessons_for_rejected_source(
            {"failure_fingerprint": "not-a-fingerprint"}, "iOBV()", Path("OBV.mq5"))
        self.assertEqual(result, [])

    def test_non_dcc_prompt_drops_foreign_regime_and_gk_rules(self):
        prompt = """Platform requirements.
- NewBarClosed detects the first tick of a new bar. On that tick, shift 1 is the just-closed bar and a market order at the current ask/bid is the next-bar-open entry. This satisfies the closed-bar and next-open requirements.
- With no open position, entries are only the declared NEUTRAL->GREED buy and NEUTRAL->EXTREME_FEAR sell. With an open short, a transition into GREED closes it and schedules a buy for the next NewBarClosed event; with an open long, a transition into EXTREME_FEAR closes it and schedules a sell for the next NewBarClosed event. That existing-position conflict rule explicitly permits the delayed opposite entry without inventing a new regime transition. Never close and reopen on the same event. Otherwise use only the stated GREED->NEUTRAL long exit and EXTREME_FEAR->FEAR short exit; do not invent extra neutral exits.
- The exit contract names the current closed-bar regime for SL/TP multipliers. Recalculate with that current regime; retain the most recently classified closed-bar regime between NewBarClosed events for per-tick SL/TP checks; do not substitute entry-time regime. Opposite-direction conflict handling must not require prev==NEUTRAL.
OnTick ordering: Minato tick capture; live margin close/refuse; manage any open position on every tick using the latest classified closed-bar regime and ATR shift 1; then `if (!NewBarClosed()) return;`; only after that one-shot gate compute/push exactly one shift-1 GK sample and evaluate regimes/signals. Never update the GK ring before the gate or insert the same closed candle on every tick. Use built-in `iATR(_Symbol, _Period, InpATRPeriod)` and `CopyBuffer(handle, 0, 1, 1, values)`; release the handle in OnDeinit. Never read CopyBuffer shift 0 or hand-roll ATR. Maintain the last-N valid GK samples; the mean and sample stdev are order-independent, so summing all N occupied ring slots is correct regardless of physical order. Invalid OHLC bars are skipped and do not increment valid-sample count or push sentinels. Do not signal until the authenticated GK and ATR data are ready. Initialize the first eligible prior regime to NEUTRAL. Current valid closed-bar observations are part of the rolling last-N values used by the contract z-score.
- Keep an explicit InpTP_ATR_Mult >= 0.5 refusal as specified, even though the current optimization grid starts at 1.5.
"""
        result = generator.remove_foreign_strategy_guidance(prompt)
        for foreign_rule in (
            "NEUTRAL->GREED", "EXTREME_FEAR", "pending reversal", "GK sample",
            "InpTP_ATR_Mult >= 0.5", "Recalculate with that current regime",
            "market order at the current ask/bid is the next-bar-open entry",
        ):
            self.assertNotIn(foreign_rule, result)
        self.assertIn("authenticated execution_contract", result)
        self.assertIn("preserve its exact entry timing and order type", result)
        self.assertIn("Do not substitute entry-time or current regime unless that contract specifies it", result)
        self.assertIn("Minato state", result)
        self.assertIn("Implement input refusals only when", result)

    def test_prompt_shape_drift_holds_instead_of_leaking_old_rules(self):
        with self.assertRaisesRegex(ValueError, "no longer match"):
            generator.remove_foreign_strategy_guidance("incomplete prompt")

    def test_authenticated_contract_is_appended_as_strategy_authority(self):
        contract = {"signals": {"direction": "ema_cross"}, "execution": {"invalid_stop": "refuse"}}
        result = generator.authenticated_contract_guidance(
            "Platform requirements.", {"strategy": {"execution_contract": contract}}, {}
        )
        self.assertIn('"direction":"ema_cross"', result)
        self.assertIn("controlling strategy authority", result)


if __name__ == "__main__":
    unittest.main()
