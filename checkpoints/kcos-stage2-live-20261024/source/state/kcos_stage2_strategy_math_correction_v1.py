"""Hash-bound admission for the single approved QuantileRegression math repair."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


TARGET_EA = "B1_1_QuantileRegression_VolatilityRegimeGate_v1"
TARGET_CYCLE_ID = "kcos-20261023T154721Z-b1-1-quantileregression-volatilityregimegate-v1-rotation"
CONTRACT_ID = "kcos-stage2-strategy-math-correction-v1"
APPROVED_CONTRACT_SHA256 = "E8BAB0A52C82D7EF3FB5AD9BCF4AC0E1BE77C04391DF61AD0B6D6A4E171A0FC6"
APPROVED_BEFORE_SHA256 = "6FFEE16ECA5BCFCDBEBE6F1B0E365DDD21CA6723EBB8D0F40BCB8E1D1FB652ED"
APPROVED_CANDIDATE_SHA256 = "FE180F045FDABABA782EEA97812156A4ACF0FB1A00806E989231476BED5C7EB3"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def validate_authorization(contract_path: Path, handoff: dict[str, Any],
                           source_path: Path, state_path: Path) -> dict[str, Any]:
    """Validate contract, lineage, protected includes, and active zero-trade evidence.

    The approved correction contract binds the exact before/after source hashes.
    Its original trigger hash is retained as historical evidence; the current
    Stage 3 decision is independently requalified through the active handoff,
    current state pointer, and passing Stage 3 verification receipt.
    """
    contract = _load_object(contract_path)
    contract_sha = file_sha256(contract_path)
    if contract_sha != APPROVED_CONTRACT_SHA256:
        raise ValueError("approved math-correction contract hash changed")
    source_contract = contract.get("source") or {}
    spec_contract = contract.get("parent_stage1_spec") or {}
    protected = contract.get("parent_protected_behavior_contract") or {}
    expected_cycle = str(contract.get("cycle_id") or "")
    if (contract.get("contract_id") != CONTRACT_ID
            or contract.get("status") != "authorized_candidate_verified_isolated"
            or contract.get("publication_status") != "not_published"
            or handoff.get("cycle_id") != expected_cycle
            or handoff.get("ea") != TARGET_EA
            or source_path.stem != TARGET_EA
            or handoff.get("stage3_zero_trade_repair") is not True):
        raise ValueError("math-correction contract does not apply to this Stage 2 cycle")

    before_path = Path(str(source_contract.get("before_path") or ""))
    candidate_path = Path(str(source_contract.get("candidate_path") or ""))
    if source_path.resolve() != before_path.resolve():
        raise ValueError("active source path differs from the authorized source")
    before_sha = str(source_contract.get("before_sha256") or "").upper()
    candidate_sha = str(source_contract.get("candidate_sha256") or "").upper()
    if before_sha != APPROVED_BEFORE_SHA256 or candidate_sha != APPROVED_CANDIDATE_SHA256:
        raise ValueError("contract source pair differs from the approved correction")
    current_sha = file_sha256(source_path)
    if current_sha not in {before_sha, candidate_sha}:
        raise ValueError("active source hash is outside the authorized before/after pair")
    if not candidate_path.is_file() or file_sha256(candidate_path) != candidate_sha:
        raise ValueError("hash-bound authorized candidate is missing or changed")

    spec_path = Path(str(handoff.get("stage1_spec") or ""))
    if (not spec_path.is_file()
            or spec_path.resolve() != Path(str(spec_contract.get("path") or "")).resolve()
            or file_sha256(spec_path) != str(spec_contract.get("sha256") or "").upper()
            or file_sha256(spec_path) != str(handoff.get("stage1_spec_sha256") or "").upper()):
        raise ValueError("Stage 1 specification lineage does not match the approved contract")

    protected_path = Path(str(protected.get("path") or ""))
    if (not protected_path.is_file()
            or file_sha256(protected_path) != str(protected.get("sha256") or "").upper()):
        raise ValueError("protected behavior contract changed")
    protected_contract = _load_object(protected_path)
    integrations = protected_contract.get("protected_behavior") or {}
    project_state = protected_path.parent.parent
    include_paths: dict[str, Path] = {}
    for key, contract_key in (("regime", "regime_include_sha256"),
                              ("minato", "minato_include_sha256")):
        integration = integrations.get(key) or {}
        include = project_state / "mt5_stage45_portable" / str(integration.get("include") or "")
        include_paths[key] = include
        if (not include.is_file()
                or file_sha256(include) != str(contract.get("protected_unchanged", {}).get(contract_key) or "").upper()
                or file_sha256(include) != str(integration.get("sha256") or "").upper()):
            raise ValueError(f"protected {key} include changed")

    evidence = handoff.get("stage3_zero_trade_evidence") or {}
    decision_path = Path(str(evidence.get("decision") or ""))
    verification_path = Path(str(evidence.get("verification") or ""))
    state = _load_object(state_path)
    if (state.get("cycle_id") != expected_cycle
            or Path(str(state.get("stage3_zero_trade_decision") or "")).resolve() != decision_path.resolve()
            or Path(str(state.get("stage3_zero_trade_verification") or "")).resolve() != verification_path.resolve()
            or not decision_path.is_file() or not verification_path.is_file()):
        raise ValueError("active Stage 3 zero-trade pointers do not match cycle state")
    decision_sha = file_sha256(decision_path)
    verification_sha = file_sha256(verification_path)
    if (decision_sha != str(evidence.get("decision_sha256") or "").upper()
            or verification_sha != str(evidence.get("verification_sha256") or "").upper()):
        raise ValueError("active Stage 3 decision or verifier hash is stale")
    decision = _load_object(decision_path)
    verification = _load_object(verification_path)
    metrics = (decision.get("timeframes") or {}).get("M5") or {}
    checks = verification.get("checks")
    if (decision.get("cycle_id") != expected_cycle or decision.get("stage") != 3
            or decision.get("ea") != TARGET_EA or decision.get("verdict") != "REPAIR_REQUIRED"
            or (decision.get("repair_trigger") or {}).get("kind") != "completed_timeframe_zero_trades"
            or (decision.get("repair_trigger") or {}).get("timeframe") != "M5"
            or type(metrics.get("row_count")) is not int or metrics.get("row_count", 0) <= 0
            or metrics.get("trades") != 0 or metrics.get("max_trades") != 0
            or metrics.get("zero_trades") is not True
            or verification.get("cycle_id") != expected_cycle or verification.get("stage") != 3
            or verification.get("passed") is not True or not isinstance(checks, dict)
            or not checks or any(value is not True for value in checks.values())
            or Path(str(verification.get("decision") or "")).resolve() != decision_path.resolve()
            or str(verification.get("decision_sha256") or "").upper() != decision_sha):
        raise ValueError("current Stage 3 evidence does not authenticate the M5 zero-trade defect")

    recorded_trigger = contract.get("trigger_evidence") or {}
    correction_ref = handoff.get("strategy_math_correction") or {}
    correction_receipt = None
    if current_sha == candidate_sha:
        correction_receipt_path = Path(str(correction_ref.get("receipt") or ""))
        if (not correction_receipt_path.is_file()
                or file_sha256(correction_receipt_path)
                != str(correction_ref.get("receipt_sha256") or "").upper()):
            raise ValueError("candidate source lacks its hash-bound Stage 2 correction receipt")
        correction_receipt = _load_object(correction_receipt_path)
        if (correction_receipt.get("status") != "APPLIED_PENDING_STAGE2_TRANSACTION"
                or correction_receipt.get("contract_sha256") != contract_sha
                or correction_receipt.get("source_before_sha256") != before_sha
                or correction_receipt.get("source_candidate_sha256") != candidate_sha
                or correction_receipt.get("source_after_sha256") != candidate_sha
                or correction_receipt.get("stage3_decision_sha256") != decision_sha
                or correction_receipt.get("stage3_verification_sha256") != verification_sha):
            raise ValueError("Stage 2 correction receipt does not bind current source and Stage 3 evidence")
    return {
        "schema_version": 1,
        "passed": True,
        "contract_id": CONTRACT_ID,
        "contract_sha256": contract_sha,
        "cycle_id": expected_cycle,
        "ea": TARGET_EA,
        "source_before_sha256": before_sha,
        "source_candidate_sha256": candidate_sha,
        "source_current_sha256": current_sha,
        "stage1_spec_sha256": file_sha256(spec_path),
        "protected_contract_sha256": file_sha256(protected_path),
        "regime_include_sha256": file_sha256(include_paths["regime"]),
        "minato_include_sha256": file_sha256(include_paths["minato"]),
        "stage3_decision_path": str(decision_path.resolve()),
        "stage3_decision_sha256": decision_sha,
        "stage3_verification_path": str(verification_path.resolve()),
        "stage3_verification_sha256": verification_sha,
        "stage3_trigger_requalified": (
            str(recorded_trigger.get("stage3_decision_sha256") or "").upper() != decision_sha
        ),
        "stage2_repair_receipt_sha256": file_sha256(
            Path(str(correction_ref["receipt"]))) if correction_receipt is not None else None,
        "stage2_repair_receipt_path": str(Path(str(correction_ref["receipt"])).resolve())
            if correction_receipt is not None else None,
        "stage2_repair_receipt_status": correction_receipt.get("status")
            if correction_receipt is not None else None,
        "stage2_transaction_journal_path": str(
            Path(str(correction_ref["receipt"])).resolve().parent / "transaction-journal.json"
        ) if correction_receipt is not None else None,
        "stage2_transaction_outcome_path": str(
            Path(str(correction_ref["receipt"])).resolve().parent / "outcome.json"
        ) if correction_receipt is not None else None,
        "status": "AUTHORIZED_BEFORE_SOURCE_CHANGE" if current_sha == before_sha else "AUTHORIZED_CANDIDATE",
    }


def apply_authorized_candidate(contract_path: Path, handoff: dict[str, Any],
                               source_path: Path, state_path: Path,
                               receipt_path: Path) -> dict[str, Any]:
    """Atomically apply only the exact contract candidate to its exact parent."""
    authorization = validate_authorization(contract_path, handoff, source_path, state_path)
    before_sha = authorization["source_before_sha256"]
    if authorization["source_current_sha256"] != before_sha:
        return {"status": "NOT_APPLICABLE", "reason": "source is not the exact authorized parent",
                "authorization": authorization}
    candidate_path = Path(str((_load_object(contract_path).get("source") or {}).get("candidate_path") or ""))
    candidate_bytes = candidate_path.read_bytes()
    temporary = source_path.with_name(source_path.name + ".math-correction.tmp")
    try:
        temporary.write_bytes(candidate_bytes)
        temporary.replace(source_path)
    finally:
        temporary.unlink(missing_ok=True)
    after_sha = file_sha256(source_path)
    if after_sha != authorization["source_candidate_sha256"]:
        raise RuntimeError("atomic candidate write did not preserve the authorized hash")
    receipt = {
        **authorization,
        "status": "APPLIED_PENDING_STAGE2_TRANSACTION",
        "source_after_sha256": after_sha,
        "source_before_sha256": before_sha,
    }
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return receipt


def find_committed_candidate_receipt(contract_path: Path, handoff: dict[str, Any],
                                    source_path: Path, state_path: Path,
                                    transactions_root: Path) -> Path | None:
    """Recover the receipt for an already-applied candidate from a committed transaction.

    A retry may start after the source candidate was committed but before the
    active handoff retained its receipt pointer. Reuse only a receipt whose
    sibling transaction journal is COMMITTED and whose full authorization
    still validates against the current source, cycle, protected includes, and
    Stage 3 evidence.
    """
    if not transactions_root.is_dir():
        return None
    for receipt_path in sorted(transactions_root.glob("*/*/strategy-math-correction-authorization.json"),
                                key=lambda path: path.stat().st_mtime, reverse=True):
        journal_path = receipt_path.parent / "transaction-journal.json"
        try:
            journal = _load_object(journal_path)
            if journal.get("status") != "COMMITTED":
                continue
            recovery_handoff = dict(handoff)
            recovery_handoff["strategy_math_correction"] = {
                "contract_id": CONTRACT_ID,
                "contract_sha256": file_sha256(contract_path),
                "receipt": str(receipt_path.resolve()),
                "receipt_sha256": file_sha256(receipt_path),
            }
            validate_authorization(contract_path, recovery_handoff, source_path, state_path)
            return receipt_path.resolve()
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    return None


def exact_transaction_candidate_is_authorized(authorization: dict[str, Any] | None,
                                              source_before_sha256: str,
                                              source_after_sha256: str,
                                              verification: dict[str, Any]) -> bool:
    """Allow the protected-surface delta only for the exact verified pair."""
    return bool(
        isinstance(authorization, dict)
        and authorization.get("status") == "APPLIED_PENDING_STAGE2_TRANSACTION"
        and authorization.get("source_before_sha256") == source_before_sha256.upper()
        and authorization.get("source_candidate_sha256") == source_after_sha256.upper()
        and verification.get("passed") is True
        and (verification.get("checks") or {}).get("strategy_math_correction_contract") is True
    )
