"""Verify the versioned Stage 2 regime and Minato behavior lock."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from kcos_runtime_paths_v2 import assert_safe_root


ROOT = assert_safe_root()
CONTRACT = ROOT / "state" / "contracts" / "kcos_stage2_protected_behavior_v1.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def verify_protected_contract(root: Path = ROOT, contract_path: Path | None = None) -> dict[str, Any]:
    """Fail closed if canonical shared includes drift from the approved version."""
    root = root.resolve()
    contract_path = (contract_path or root / "state" / "contracts" / CONTRACT.name).resolve()
    contract = json.loads(contract_path.read_text(encoding="utf-8-sig"))
    if (contract.get("schema_version") != 1
            or contract.get("contract_id") != "kcos-stage2-protected-behavior-v1"):
        raise ValueError("unsupported Stage 2 protected behavior contract")
    verified: dict[str, Any] = {"contract_id": contract["contract_id"], "integrations": {}}
    for name in ("regime", "minato"):
        entry = contract.get("protected_behavior", {}).get(name)
        if not isinstance(entry, dict):
            raise ValueError(f"protected Stage 2 {name} contract is missing")
        include = (root / "state" / "mt5_stage45_portable" / entry["include"]).resolve()
        try:
            include.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"protected Stage 2 {name} include escapes runtime root") from exc
        if not include.is_file() or sha256(include) != str(entry.get("sha256", "")).upper():
            raise ValueError(f"protected Stage 2 {name} include does not match {contract['contract_id']}")
        text = include.read_text(encoding="utf-8-sig", errors="replace")
        signatures = entry.get("api_signatures")
        if not isinstance(signatures, dict) or not signatures:
            raise ValueError(f"protected Stage 2 {name} API signature list is empty")
        missing = [signature for signature in signatures.values() if signature not in text]
        if missing:
            raise ValueError(f"protected Stage 2 {name} API signature changed: {missing[0]}")
        if name == "regime":
            for enum_name, enum_value in entry.get("enum_values", {}).items():
                if not re.search(rf"\b{re.escape(enum_name)}\s*=\s*{int(enum_value)}\b", text):
                    raise ValueError(f"protected regime enum changed: {enum_name}")
        elif entry.get("score_version") != "MINATO_BASE_SCORE_V2.0" or entry.get("score_version") not in text:
            raise ValueError("protected Minato score version changed")
        verified["integrations"][name] = {
            "path": str(include), "sha256": sha256(include),
            "api_signatures": sorted(signatures),
        }
    verified["passed"] = True
    return verified
