import tempfile
import unittest
from pathlib import Path

import kcos_stage2_repair_v2 as repair_tool


class Stage2GenericRepairTests(unittest.TestCase):
    def test_obv_old_path_replays_only_closed_atr_and_safe_quantity(self):
        from kcos_stage2_repair_loop_v1 import locked_strategy_surface
        from kcos_stage2_verify_v2 import (
            atr_uses_authenticated_closed_buffer,
            every_order_path_is_safe,
            normalize_lot_helper_is_authenticated,
        )

        fixture = (Path(__file__).resolve().parent
                   / "tests" / "fixtures"
                   / "obv_stage2_before_safety_repair_20260918.mq5")
        original = fixture.read_text(encoding="utf-8-sig")
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "B1_1_OBV_Divergence_v1.mq5"
            source.write_text(original, encoding="utf-8", newline="")
            result = repair_tool.repair(source)
            repaired = source.read_text(encoding="utf-8-sig")

        self.assertTrue(result["changed"])
        self.assertIn("obv_closed_atr_shift_1", result["changes"])
        self.assertIn("obv_zero_preserving_lot_normalizer", result["changes"])
        self.assertIn("obv_post_normalization_zero_refusal", result["changes"])
        self.assertTrue(atr_uses_authenticated_closed_buffer(
            repaired, period_input="InpAtrPeriod"))
        self.assertTrue(normalize_lot_helper_is_authenticated(repaired))
        diagnostics = []
        self.assertTrue(every_order_path_is_safe(repaired, diagnostics), diagnostics)
        self.assertEqual(locked_strategy_surface(original), locked_strategy_surface(repaired))
        self.assertEqual(
            [line.strip() for line in original.splitlines() if line.strip().startswith("input ")],
            [line.strip() for line in repaired.splitlines() if line.strip().startswith("input ")],
        )
        self.assertEqual(original.count("MinatoV2ObserveTick();"),
                         repaired.count("MinatoV2ObserveTick();"))
        self.assertEqual(original.count("OBV_Div_L"), repaired.count("OBV_Div_L"))
        self.assertEqual(original.count("OBV_Div_S"), repaired.count("OBV_Div_S"))
        changed_signal = repaired.replace(
            "short_signal && close1 >= ema1", "short_signal && close1 > ema1", 1)
        self.assertNotEqual(locked_strategy_surface(original), locked_strategy_surface(changed_signal))

    def test_obv_old_path_repair_is_idempotent(self):
        fixture = (Path(__file__).resolve().parent
                   / "tests" / "fixtures"
                   / "obv_stage2_before_safety_repair_20260918.mq5")
        original = fixture.read_text(encoding="utf-8-sig")
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "B1_1_OBV_Divergence_v1.mq5"
            source.write_text(original, encoding="utf-8", newline="")
            first = repair_tool.repair(source)
            once = source.read_bytes()
            second = repair_tool.repair(source)
            self.assertTrue(first["changed"])
            self.assertFalse(second["changed"])
            self.assertEqual(once, source.read_bytes())

    def test_obv_policy_repairs_pass_strategy_lock_together(self):
        from kcos_stage2_repair_loop_v1 import (
            apply_stage2_cooldown_removal,
            locked_strategy_surface,
        )
        from kcos_stage2_verify_v2 import (
            atr_uses_authenticated_closed_buffer,
            cooldown_controls_absent,
            every_order_path_is_safe,
        )

        fixture = (Path(__file__).resolve().parent
                   / "tests" / "fixtures"
                   / "obv_stage2_before_safety_repair_20260918.mq5")
        original = fixture.read_text(encoding="utf-8-sig")
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "B1_1_OBV_Divergence_v1.mq5"
            source.write_text(original, encoding="utf-8", newline="")
            repair_tool.repair(source)
            cleanup = apply_stage2_cooldown_removal(source)
            repaired = source.read_text(encoding="utf-8-sig")

        self.assertEqual(cleanup["status"], "APPLIED")
        self.assertTrue(cooldown_controls_absent(repaired))
        self.assertTrue(atr_uses_authenticated_closed_buffer(
            repaired, period_input="InpAtrPeriod"))
        self.assertTrue(every_order_path_is_safe(repaired))
        self.assertEqual(locked_strategy_surface(original), locked_strategy_surface(repaired))

    def test_preserves_authenticated_entry_margin_threshold(self):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "GenericEA.mq5"
            source.write_text("input double InpMarginKillLevel = 90.0;\n", encoding="utf-8")
            result = repair_tool.repair(source)
            self.assertFalse(result["changed"])
            self.assertIn("InpMarginKillLevel = 90.0", source.read_text(encoding="utf-8"))

    def test_repair_does_not_preserve_cooldown_behavior(self):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "GenericEA.mq5"
            source.write_text(
                "input int InpMinBarsBetweenTrades = 0; // cooldown\n"
                "int g_last_entry_bar_index = -1;\n"
                "bool CooldownActive()\n"
                "{\n"
                "   if(InpMinBarsBetweenTrades <= 0) return false;\n"
                "   return g_last_entry_bar_index >= InpMinBarsBetweenTrades;\n"
                "}\n"
                "void TryEntry()\n"
                "{\n"
                "   if(CooldownActive()) return;\n"
                "}\n",
                encoding="utf-8",
            )

            result = repair_tool.repair(source)
            repaired = source.read_text(encoding="utf-8")

            self.assertFalse(result["changed"])
            self.assertEqual(source.read_text(encoding="utf-8"), repaired)
            from kcos_stage2_verify_v2 import cooldown_controls_absent
            self.assertFalse(cooldown_controls_absent(repaired))

    def test_repair_leaves_unrecognized_cooldown_for_strict_verifier_hold(self):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "GenericEA.mq5"
            original = (
                "input int InpMinBarsBetweenTrades = 0; // cooldown\n"
                "bool CooldownActive() { return g_last_entry_bar_index < InpMinBarsBetweenTrades; }\n"
                "void TryEntry() { if(CooldownActive()) return; }\n"
            )
            source.write_text(original, encoding="utf-8")
            result = repair_tool.repair(source)
            self.assertFalse(result["changed"])
            self.assertEqual(original, source.read_text(encoding="utf-8"))
            from kcos_stage2_verify_v2 import cooldown_controls_absent
            self.assertFalse(cooldown_controls_absent(original))


if __name__ == "__main__":
    unittest.main()
