"""Canonical source-derived optimization policy for KCOS Stage 1/2."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

REGIME_FIELDS = tuple(
    f"InpRegime_{regime}{kind}"
    for regime in ("Greed", "Neutral", "Fear", "ExtFear")
    for kind in ("Lot", "SLMult", "TPMult")
)
POLICY_VERSION = "regime-required-v1"


def _executable_source(source_text: str) -> str:
    text = re.sub(r"/\*.*?\*/", "", source_text, flags=re.S)
    text = re.sub(r"//[^\r\n]*", "", text)
    return re.sub(r"^\s*input\b[^\r\n]*$", "", text, flags=re.I | re.M)


def derive_contract(source_text: str) -> dict:
    declared = sorted(set(re.findall(r"\bInpRegime_[A-Za-z0-9_]+", source_text)))
    executable = _executable_source(source_text)
    used = sorted(name for name in declared if re.search(rf"\b{re.escape(name)}\b", executable))
    required = sorted(set(REGIME_FIELDS) & set(declared) & set(used))
    return {
        "policy_version": POLICY_VERSION,
        "declared_regime_inputs": declared,
        "executable_regime_inputs": used,
        "required_optimized_regime_inputs": required,
        "regime_contract_active": len(required) >= 12,
    }


def enforce_config(config: dict, source_text: str) -> dict:
    contract = derive_contract(source_text)
    if not contract["regime_contract_active"]:
        return contract
    inputs = config.get("inputs", {})
    missing = sorted(name for name in contract["required_optimized_regime_inputs"]
                     if str(inputs.get(name, {}).get("optimize", "N")).upper() != "Y")
    if missing:
        raise ValueError(
            "REGIME_OPTIMIZATION_POLICY_VIOLATION: executable regime contract requires "
            f"optimize=Y for {', '.join(missing)}"
        )
    return contract


def experiment_identity(ea: str, source_sha256: str, config: dict) -> str:
    payload = {
        "policy_version": POLICY_VERSION,
        "ea": ea,
        "source_sha256": source_sha256.upper(),
        "tester_config": config,
    }
    return hashlib.sha256((json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()).hexdigest().upper()


def reject_prior_authenticated_stage3(ea: str, state_root: Path) -> None:
    """Prevent queue-only reruns after an authenticated Stage 3 verdict."""
    events = state_root / "events"
    if not events.is_dir():
        return
    for path in events.glob("*.stage3.complete.json"):
        try:
            event = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError):
            continue
        if event.get("ea") == ea and event.get("status") == "complete":
            raise ValueError(
                "DUPLICATE_AUTHENTICATED_EXPERIMENT: "
                f"{ea} already has Stage 3 evidence at {path}"
            )
