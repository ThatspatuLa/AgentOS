import hashlib
import json
import unittest
from pathlib import Path

import kcos_stage2_build_ea_v1 as builder


ROOT = Path(__file__).resolve().parent.parent
CYCLE = "kcos-20260930t061535z-stage1-research-new"
SPEC_PATH = ROOT / "state" / "specs" / f"{CYCLE}.stage1.spec.json"
CONTRACT_PATH = ROOT / "state" / "implementation_contracts" / f"{CYCLE}.dcc-garch.v1.json"


class DccStage2BuilderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spec = json.loads(SPEC_PATH.read_text(encoding="utf-8-sig"))
        cls.signature = builder._extract_strategy_signature(cls.spec)
        cls.source = builder.render_mq5(
            cls.spec["ea"],
            cls.spec["primitive_selection"]["primitive"],
            cls.spec["strategy"]["edge"],
            cls.signature,
            cls.spec["cycle_id"],
            cls.spec["source_verification"]["sources"],
            cls.spec["tester_config"]["inputs"],
        )

    def test_dcc_spec_generates_real_strategy_not_generic_scaffold(self):
        self.assertIn("DCC_GARCH_IMPLEMENTATION_V1", self.source)
        self.assertNotIn("Auto-generated scaffold", self.source)
        self.assertNotIn("return REGIME_NEUTRAL", self.source)
        for required in (
            "Garch11(",
            "DccGarchCorrelation(",
            "PearsonTail(",
            "InpDccLookback",
            "InpCorrThreshold",
            "IsNewM5Bar()",
            "InpRegime_Greed_Lot_Mult",
            "InpRegime_ExtFear_TP_Mult",
            "MinatoAllowsProjectedMargin(",
            "InpMarginKillLevel",
            "240",
        ):
            with self.subTest(required=required):
                self.assertIn(required, self.source)

    def test_contract_and_spec_lineage_hashes_are_bound_into_source(self):
        contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(contract["cycle_id"], self.spec["cycle_id"])
        self.assertEqual(
            hashlib.sha256(SPEC_PATH.read_bytes()).hexdigest(),
            contract["source_spec_sha256"],
        )
        contract_hash = hashlib.sha256(CONTRACT_PATH.read_bytes()).hexdigest()
        self.assertIn(f"Implementation contract SHA256: {contract_hash}", self.source)

    def test_directional_correlation_intersection_is_encoded(self):
        self.assertIn("g_realized_corr_24>InpCorrThreshold", self.source)
        self.assertIn("g_realized_corr_24 < -InpCorrThreshold", self.source)
        self.assertIn("g_dcc_corr>=InpCorrThreshold", self.source)

    def test_each_order_side_has_explicit_projected_margin_guard(self):
        self.assertIn("if(!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY,norm_lot,ask)) return false;", self.source)
        self.assertIn("if(!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL,norm_lot,bid)) return false;", self.source)
        self.assertIn("bool MarginKillActive()", self.source)
        self.assertIn("if(!MarginKillActive()) return false;", self.source)


if __name__ == "__main__":
    unittest.main()
