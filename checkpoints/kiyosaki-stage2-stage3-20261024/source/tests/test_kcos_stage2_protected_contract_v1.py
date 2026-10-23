from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from kcos_stage2_protected_contract_v1 import verify_protected_contract


class ProtectedContractTests(unittest.TestCase):
    def fixture(self, root: Path) -> Path:
        source_root = Path(__file__).resolve().parent
        include_root = root / "state" / "mt5_stage45_portable" / "MQL5" / "Include"
        include_root.mkdir(parents=True)
        contract = json.loads((source_root / "contracts" / "kcos_stage2_protected_behavior_v1.json").read_text())
        for name in ("regime", "minato"):
            item = contract["protected_behavior"][name]
            include = include_root / Path(item["include"]).name
            shutil.copy2(source_root / "mt5_stage45_portable" / item["include"], include)
        path = root / "state" / "contracts" / "kcos_stage2_protected_behavior_v1.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(contract), encoding="utf-8")
        return path

    def test_accepts_exact_regime_and_minato_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            contract_path = self.fixture(root)
            result = verify_protected_contract(root, contract_path)
            self.assertTrue(result["passed"])
            self.assertEqual(set(result["integrations"]), {"regime", "minato"})

    def test_rejects_changed_regime_or_minato_include(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            contract_path = self.fixture(root)
            regime = root / "state" / "mt5_stage45_portable" / "MQL5" / "Include" / "kiyosaki_regime.mqh"
            regime.write_text(regime.read_text() + "\n// drift\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "regime include"):
                verify_protected_contract(root, contract_path)

    def test_rejects_policy_signature_drift_even_if_file_hash_is_rebound(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            contract_path = self.fixture(root)
            include = root / "state" / "mt5_stage45_portable" / "MQL5" / "Include" / "minato_base_score_v2.mqh"
            include.write_text(include.read_text().replace("double MinatoV2ScoreFromTester()", "double ChangedScore()"), encoding="utf-8")
            contract = json.loads(contract_path.read_text())
            contract["protected_behavior"]["minato"]["sha256"] = hashlib.sha256(include.read_bytes()).hexdigest().upper()
            contract_path.write_text(json.dumps(contract), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "API signature changed"):
                verify_protected_contract(root, contract_path)


if __name__ == "__main__":
    unittest.main()
