import hashlib
import json
from pathlib import Path

import pytest

import kcos_stage2_repair_loop_v1 as repair_loop
import kcos_stage2_prepare_v2 as stage2_prepare
import kcos_stage2_publish_v2 as stage2_publish
import kcos_stage2_strategy_math_correction_v1 as correction
from kcos_stage2_strategy_math_correction_v1 import (
    CONTRACT_ID,
    TARGET_CYCLE_ID,
    TARGET_EA,
    apply_authorized_candidate,
    exact_transaction_candidate_is_authorized,
    validate_authorization,
)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def dump(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


@pytest.fixture
def correction_case(tmp_path, monkeypatch):
    state_root = tmp_path / "state"
    source = tmp_path / "Experts" / f"{TARGET_EA}.mq5"
    candidate = tmp_path / "repairs" / f"{TARGET_EA}.mq5"
    source.parent.mkdir(parents=True)
    candidate.parent.mkdir(parents=True)
    source.write_bytes(b"approved parent source\n")
    candidate.write_bytes(b"exact approved corrected source\n")

    spec = state_root / "specs" / "stage1.json"
    dump(spec, {"cycle_id": TARGET_CYCLE_ID})
    regime = state_root / "mt5_stage45_portable" / "MQL5" / "Include" / "kiyosaki_regime.mqh"
    minato = state_root / "mt5_stage45_portable" / "MQL5" / "Include" / "minato_base_score_v2.mqh"
    regime.parent.mkdir(parents=True)
    regime.write_bytes(b"regime include\n")
    minato.write_bytes(b"minato include\n")
    protected = state_root / "contracts" / "protected.json"
    protected_value = {"contract_id": "kcos-stage2-protected-behavior-v1", "protected_behavior": {
        "regime": {"include": "MQL5/Include/kiyosaki_regime.mqh", "sha256": digest(regime)},
        "minato": {"include": "MQL5/Include/minato_base_score_v2.mqh", "sha256": digest(minato)},
    }}
    dump(protected, protected_value)

    decision = state_root / "decisions" / "stage3.json"
    decision_value = {
        "cycle_id": TARGET_CYCLE_ID, "stage": 3, "ea": TARGET_EA,
        "verdict": "REPAIR_REQUIRED",
        "repair_trigger": {"kind": "completed_timeframe_zero_trades", "timeframe": "M5"},
        "timeframes": {"M5": {"row_count": 100, "trades": 0, "max_trades": 0,
                               "zero_trades": True}},
    }
    dump(decision, decision_value)
    decision_sha = digest(decision)
    verification = state_root / "verification" / "stage3.json"
    dump(verification, {"cycle_id": TARGET_CYCLE_ID, "stage": 3, "passed": True,
                        "decision": str(decision), "decision_sha256": decision_sha,
                        "checks": {"authenticated_rows": True}})
    state_path = state_root / "kcos_cycle_state_v2.json"
    dump(state_path, {"cycle_id": TARGET_CYCLE_ID,
                      "stage3_zero_trade_decision": str(decision),
                      "stage3_zero_trade_verification": str(verification)})
    handoff = {
        "cycle_id": TARGET_CYCLE_ID, "ea": TARGET_EA,
        "stage1_spec": str(spec), "stage1_spec_sha256": digest(spec),
        "stage3_zero_trade_repair": True,
        "stage3_zero_trade_evidence": {
            "decision": str(decision), "decision_sha256": decision_sha,
            "verification": str(verification), "verification_sha256": digest(verification),
        },
    }
    contract_path = state_root / "contracts" / "kcos_stage2_strategy_math_correction_v1.json"
    contract = {
        "contract_id": CONTRACT_ID,
        "status": "authorized_candidate_verified_isolated",
        "publication_status": "not_published",
        "cycle_id": TARGET_CYCLE_ID,
        "ea": TARGET_EA,
        "source": {"before_path": str(source), "before_sha256": digest(source),
                   "candidate_path": str(candidate), "candidate_sha256": digest(candidate)},
        "parent_stage1_spec": {"path": str(spec), "sha256": digest(spec)},
        "parent_protected_behavior_contract": {"path": str(protected), "sha256": digest(protected)},
        "protected_unchanged": {"regime_include_sha256": digest(regime),
                                 "minato_include_sha256": digest(minato)},
        "trigger_evidence": {"stage3_decision_sha256": "0" * 64},
    }
    dump(contract_path, contract)
    monkeypatch.setattr(correction, "APPROVED_CONTRACT_SHA256", digest(contract_path))
    monkeypatch.setattr(correction, "APPROVED_BEFORE_SHA256", digest(source))
    monkeypatch.setattr(correction, "APPROVED_CANDIDATE_SHA256", digest(candidate))
    return {"root": tmp_path, "contract_path": contract_path, "handoff": handoff, "source": source,
            "candidate": candidate, "state_path": state_path, "decision": decision,
            "verification": verification, "regime": regime}


def test_authorization_requalifies_new_authenticated_trigger_hash(correction_case, tmp_path):
    case = correction_case
    authorization = validate_authorization(
        case["contract_path"], case["handoff"], case["source"], case["state_path"])
    assert authorization["passed"] is True
    assert authorization["stage3_trigger_requalified"] is True
    receipt = tmp_path / "attempt" / "authorization.json"
    result = apply_authorized_candidate(
        case["contract_path"], case["handoff"], case["source"], case["state_path"], receipt)
    assert result["source_after_sha256"] == digest(case["candidate"])
    assert case["source"].read_bytes() == case["candidate"].read_bytes()
    assert json.loads(receipt.read_text(encoding="utf-8"))["status"] == "APPLIED_PENDING_STAGE2_TRANSACTION"


@pytest.mark.parametrize("mutation", ["decision_hash", "verification_hash", "source", "protected_include"])
def test_rejects_changed_authenticated_inputs(correction_case, mutation):
    case = correction_case
    if mutation == "decision_hash":
        case["handoff"]["stage3_zero_trade_evidence"]["decision_sha256"] = "0" * 64
    elif mutation == "verification_hash":
        case["handoff"]["stage3_zero_trade_evidence"]["verification_sha256"] = "0" * 64
    elif mutation == "source":
        case["source"].write_bytes(b"different parent\n")
    else:
        case["regime"].write_bytes(b"changed include\n")
    with pytest.raises(ValueError):
        validate_authorization(case["contract_path"], case["handoff"], case["source"], case["state_path"])


def test_transaction_exception_requires_exact_hash_pair_and_verifier_pass(correction_case, tmp_path):
    case = correction_case
    receipt = tmp_path / "attempt" / "authorization.json"
    authorization = apply_authorized_candidate(
        case["contract_path"], case["handoff"], case["source"], case["state_path"], receipt)
    verifier = {"passed": True, "checks": {"strategy_math_correction_contract": True}}
    assert exact_transaction_candidate_is_authorized(
        authorization, digest(tmp_path / "Experts" / f"{TARGET_EA}.mq5"),
        authorization["source_candidate_sha256"], verifier) is False
    assert exact_transaction_candidate_is_authorized(
        authorization, authorization["source_before_sha256"],
        authorization["source_candidate_sha256"], verifier) is True
    verifier["passed"] = False
    assert exact_transaction_candidate_is_authorized(
        authorization, authorization["source_before_sha256"],
        authorization["source_candidate_sha256"], verifier) is False


def test_retry_reuses_only_receipt_from_matching_committed_transaction(correction_case, tmp_path):
    case = correction_case
    first_receipt = tmp_path / "transactions" / "episode-a" / "attempt-a" / "strategy-math-correction-authorization.json"
    apply_authorized_candidate(case["contract_path"], case["handoff"], case["source"],
                               case["state_path"], first_receipt)
    dump(first_receipt.parent / "transaction-journal.json", {"status": "COMMITTED"})

    recovered = correction.find_committed_candidate_receipt(
        case["contract_path"], case["handoff"], case["source"], case["state_path"],
        tmp_path / "transactions")

    assert recovered == first_receipt.resolve()


def test_retry_does_not_reuse_rolled_back_candidate_receipt(correction_case, tmp_path):
    case = correction_case
    first_receipt = tmp_path / "transactions" / "episode-a" / "attempt-a" / "strategy-math-correction-authorization.json"
    apply_authorized_candidate(case["contract_path"], case["handoff"], case["source"],
                               case["state_path"], first_receipt)
    dump(first_receipt.parent / "transaction-journal.json", {"status": "ROLLED_BACK"})

    recovered = correction.find_committed_candidate_receipt(
        case["contract_path"], case["handoff"], case["source"], case["state_path"],
        tmp_path / "transactions")

    assert recovered is None


def _published_correction_evidence(case, tmp_path):
    receipt_path = tmp_path / "attempt" / "authorization.json"
    applied = apply_authorized_candidate(
        case["contract_path"], case["handoff"], case["source"], case["state_path"], receipt_path)
    case["handoff"]["strategy_math_correction"] = {
        "contract_id": CONTRACT_ID, "contract_sha256": applied["contract_sha256"],
        "receipt": str(receipt_path), "receipt_sha256": digest(receipt_path),
    }
    journal_path = receipt_path.parent / "transaction-journal.json"
    outcome_path = receipt_path.parent / "outcome.json"
    dump(journal_path, {"schema_version": 1, "status": "COMMITTED"})
    dump(outcome_path, {"outcome": "REPAIRED", "passed": True,
                        "episode_id": receipt_path.parent.parent.name,
                        "attempt_dir": str(receipt_path.parent.resolve())})
    verification = {
        "ea": TARGET_EA, "cycle_id": TARGET_CYCLE_ID,
        "source_sha256": applied["source_after_sha256"],
        "strategy_math_correction_contract": {
            "applicable": True, "passed": True, "status": "AUTHORIZED_CANDIDATE",
            "contract_sha256": applied["contract_sha256"],
            "stage2_repair_receipt_path": str(receipt_path.resolve()),
            "stage2_repair_receipt_sha256": digest(receipt_path),
            "stage2_transaction_journal_path": str(journal_path.resolve()),
            "stage2_transaction_outcome_path": str(outcome_path.resolve()),
            "stage3_decision_sha256": applied["stage3_decision_sha256"],
            "stage3_verification_sha256": applied["stage3_verification_sha256"],
        },
    }
    return verification, receipt_path


def test_publication_rechecks_math_correction_evidence_hashes(correction_case, tmp_path, monkeypatch):
    case = correction_case
    monkeypatch.setattr(stage2_publish, "ROOT", case["root"])
    verification, _receipt_path = _published_correction_evidence(case, tmp_path)
    assert stage2_publish.validate_strategy_math_correction_receipt(verification, case["handoff"])


def test_publication_does_not_require_one_cycle_correction_for_later_same_ea_cycle():
    verification = {
        "ea": TARGET_EA, "cycle_id": TARGET_CYCLE_ID + "-later",
        "strategy_math_correction_contract": {"applicable": False, "passed": True},
    }
    assert stage2_publish.validate_strategy_math_correction_receipt(verification, {})


def test_target_cycle_publication_rejects_missing_correction_receipt():
    verification = {
        "ea": TARGET_EA, "cycle_id": TARGET_CYCLE_ID,
        "strategy_math_correction_contract": {"applicable": False, "passed": True},
    }
    assert not stage2_publish.validate_strategy_math_correction_receipt(verification, {})


@pytest.mark.parametrize("mutation", [
    "handoff_pointer", "handoff_contract", "receipt_bytes", "contract_bytes",
    "transaction_pending", "transaction_rolled_back",
])
def test_publication_rejects_math_correction_evidence_mutated_after_verification(
        correction_case, tmp_path, monkeypatch, mutation):
    case = correction_case
    monkeypatch.setattr(stage2_publish, "ROOT", case["root"])
    verification, receipt_path = _published_correction_evidence(case, tmp_path)
    if mutation == "handoff_pointer":
        case["handoff"]["strategy_math_correction"]["receipt_sha256"] = "0" * 64
    elif mutation == "handoff_contract":
        case["handoff"]["strategy_math_correction"]["contract_sha256"] = "0" * 64
    elif mutation == "receipt_bytes":
        receipt_path.write_text(receipt_path.read_text(encoding="utf-8") + " ", encoding="utf-8")
    elif mutation == "contract_bytes":
        case["contract_path"].write_text(
            case["contract_path"].read_text(encoding="utf-8") + " ", encoding="utf-8")
    else:
        journal_path = receipt_path.parent / "transaction-journal.json"
        dump(journal_path, {"schema_version": 1,
                            "status": "PENDING" if mutation == "transaction_pending" else "ROLLED_BACK"})
    assert not stage2_publish.validate_strategy_math_correction_receipt(
        verification, case["handoff"])


def test_stage2_requalifies_stale_decision_copy_from_current_passing_verifier(correction_case, monkeypatch):
    case = correction_case
    monkeypatch.setattr(repair_loop, "ROOT", case["root"])
    current_sha = case["handoff"]["stage3_zero_trade_evidence"]["decision_sha256"]
    case["handoff"]["stage3_zero_trade_evidence"]["decision_sha256"] = "E39C7175A7287CE11FE143EBF40F862181C142AE7EA3E131C0C0012B9F24DC94"
    receipt = repair_loop.requalify_stage3_zero_trade_decision(case["handoff"])
    assert receipt["previous_handoff_decision_sha256"] != current_sha
    assert receipt["current_verified_decision_sha256"] == current_sha
    assert case["handoff"]["stage3_zero_trade_evidence"]["decision_sha256"] == current_sha


def test_stage2_does_not_requalify_when_stage3_verifier_changed(correction_case, monkeypatch):
    case = correction_case
    monkeypatch.setattr(repair_loop, "ROOT", case["root"])
    evidence = case["handoff"]["stage3_zero_trade_evidence"]
    evidence["decision_sha256"] = "E39C7175A7287CE11FE143EBF40F862181C142AE7EA3E131C0C0012B9F24DC94"
    evidence["verification_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="Stage 3 verifier hash changed"):
        repair_loop.requalify_stage3_zero_trade_decision(case["handoff"])


def test_grid_policy_hash_can_refresh_without_changing_approved_grid(correction_case, monkeypatch):
    case = correction_case
    monkeypatch.setattr(repair_loop, "ROOT", case["root"])
    spec = case["root"] / "state" / "specs" / "stage1.json"
    old = {"policy": "same-policy", "policy_sha256": "1" * 64,
           "approved_config_sha256": "2" * 64, "normalized_grid_sha256": "3" * 64,
           "optimized_input_count": 19, "passed": True}
    current = {**old, "policy_sha256": "4" * 64}
    case["handoff"].update({"stage1_spec": str(spec), "stage1_spec_sha256": digest(spec),
                            "stage3_grid_preflight": old})
    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(stage2_prepare, "validate_stage3_grid_before_publish",
                        lambda *_args: current)
        receipt = repair_loop.requalify_stage3_grid_policy_hash(case["handoff"], case["source"])
    assert receipt["normalized_grid_sha256"] == old["normalized_grid_sha256"]
    assert receipt["previous_policy_sha256"] == old["policy_sha256"]
    assert case["handoff"]["stage3_grid_preflight"] == current


def test_grid_requalification_rejects_changed_approved_grid(correction_case, monkeypatch):
    case = correction_case
    monkeypatch.setattr(repair_loop, "ROOT", case["root"])
    spec = case["root"] / "state" / "specs" / "stage1.json"
    old = {"policy": "same-policy", "policy_sha256": "1" * 64,
           "approved_config_sha256": "2" * 64, "normalized_grid_sha256": "3" * 64,
           "optimized_input_count": 19, "passed": True}
    current = {**old, "policy_sha256": "4" * 64, "normalized_grid_sha256": "5" * 64}
    case["handoff"].update({"stage1_spec": str(spec), "stage1_spec_sha256": digest(spec),
                            "stage3_grid_preflight": old})
    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(stage2_prepare, "validate_stage3_grid_before_publish",
                        lambda *_args: current)
        with pytest.raises(ValueError, match="changed beyond its policy hash"):
            repair_loop.requalify_stage3_grid_policy_hash(case["handoff"], case["source"])
