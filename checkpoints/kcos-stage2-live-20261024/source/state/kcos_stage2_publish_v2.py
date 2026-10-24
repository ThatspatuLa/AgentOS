#!/usr/bin/env python3
"""Publish Stage 2 completion and advance only canonical v2 state."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from kcos_tester_config_v2 import stage2_config_from_spec, config_sha256
from kcos_runtime_paths_v2 import assert_safe_root
from kcos_stage2_protected_contract_v1 import verify_protected_contract
from kcos_stage2_verify_v2 import STAGE2_PIPELINE_CODE_FILES
from kcos_stage2_strategy_math_correction_v1 import (
    CONTRACT_ID as STRATEGY_MATH_CORRECTION_ID,
    TARGET_CYCLE_ID as STRATEGY_MATH_CORRECTION_CYCLE,
    TARGET_EA as STRATEGY_MATH_CORRECTION_EA,
)


ROOT = assert_safe_root()
LEASE = ROOT / "state" / "leases" / "stage2_v2.lock.json"
STAGE2_HANDOFF = ROOT / "state" / "handoffs" / "Stage2_v2.json"
STAGE3_TEMPLATE = ROOT / "state" / "kcos_genetic_opt_template.stage3-utf8.ini"


def load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain an object")
    return value


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def validate_strategy_math_correction_receipt(verification: dict, handoff: dict) -> bool:
    """Rebind the exceptional correction evidence at publication time."""
    receipt = verification.get("strategy_math_correction_contract") or {}
    if (verification.get("ea") != STRATEGY_MATH_CORRECTION_EA
            or verification.get("cycle_id") != STRATEGY_MATH_CORRECTION_CYCLE):
        return receipt.get("applicable") is not True
    if (receipt.get("applicable") is not True or receipt.get("passed") is not True
            or receipt.get("status") != "AUTHORIZED_CANDIDATE"):
        return False

    contract_path = ROOT / "state" / "contracts" / "kcos_stage2_strategy_math_correction_v1.json"
    correction_ref = handoff.get("strategy_math_correction") or {}
    receipt_path = Path(str(correction_ref.get("receipt") or ""))
    try:
        contract = load(contract_path)
        correction_receipt = load(receipt_path)
        journal_path = receipt_path.resolve().parent / "transaction-journal.json"
        outcome_path = receipt_path.resolve().parent / "outcome.json"
        transaction_journal = load(journal_path)
        transaction_outcome = load(outcome_path)
    except (OSError, ValueError, TypeError):
        return False
    contract_sha = sha256(contract_path)
    receipt_sha = sha256(receipt_path)
    source_sha = str(verification.get("source_sha256") or "").upper()
    return bool(
        contract.get("contract_id") == STRATEGY_MATH_CORRECTION_ID
        and contract_sha == str(receipt.get("contract_sha256") or "").upper()
        and correction_ref.get("contract_id") == STRATEGY_MATH_CORRECTION_ID
        and str(correction_ref.get("contract_sha256") or "").upper() == contract_sha
        and receipt_path.resolve() == Path(str(receipt.get("stage2_repair_receipt_path") or "")).resolve()
        and receipt_sha == str(correction_ref.get("receipt_sha256") or "").upper()
        and receipt_sha == str(receipt.get("stage2_repair_receipt_sha256") or "").upper()
        and journal_path.resolve() == Path(str(receipt.get("stage2_transaction_journal_path") or "")).resolve()
        and outcome_path.resolve() == Path(str(receipt.get("stage2_transaction_outcome_path") or "")).resolve()
        and transaction_journal.get("schema_version") == 1
        and transaction_journal.get("status") == "COMMITTED"
        and transaction_outcome.get("outcome") == "REPAIRED"
        and transaction_outcome.get("passed") is True
        and transaction_outcome.get("episode_id") == receipt_path.resolve().parent.parent.name
        and Path(str(transaction_outcome.get("attempt_dir") or "")).resolve() == receipt_path.resolve().parent
        and correction_receipt.get("status") == "APPLIED_PENDING_STAGE2_TRANSACTION"
        and correction_receipt.get("contract_sha256") == contract_sha
        and correction_receipt.get("contract_id") == STRATEGY_MATH_CORRECTION_ID
        and correction_receipt.get("cycle_id") == verification.get("cycle_id")
        and correction_receipt.get("ea") == verification.get("ea")
        and correction_receipt.get("source_before_sha256") == str(
            (contract.get("source") or {}).get("before_sha256") or "").upper()
        and correction_receipt.get("source_after_sha256") == source_sha
        and correction_receipt.get("source_candidate_sha256") == source_sha
        and correction_receipt.get("stage3_decision_sha256") == receipt.get("stage3_decision_sha256")
        and correction_receipt.get("stage3_verification_sha256") == receipt.get("stage3_verification_sha256")
    )


def materialize_stage2_tester_inis(stage1_spec: dict, source_path: Path,
                                   source_text: str, approved_config: dict,
                                   approved_config_sha256: str,
                                   cycle_id: str) -> dict:
    """Create the immutable, normalized M5/M15/H1 INIs owned by Stage 2."""
    import kcos_stage3_prepare_v2 as stage3

    if not STAGE3_TEMPLATE.is_file():
        raise ValueError("canonical Stage 3 INI template is missing during Stage 2 publication")
    template_hash = sha256(STAGE3_TEMPLATE)
    rederived_config = stage2_config_from_spec(stage1_spec, source_text=source_text)
    if config_sha256(rederived_config) != approved_config_sha256:
        raise ValueError("Stage 2 final tester config differs from its authenticated source/spec derivation")
    if config_sha256(approved_config) != approved_config_sha256:
        raise ValueError("Stage 2 final tester config hash does not match its publication identity")
    runtime_config = approved_config
    runtime_spec = dict(stage1_spec)
    runtime_spec["tester_config"] = runtime_config
    template_text = STAGE3_TEMPLATE.read_text(encoding="utf-8-sig")
    from_date = str(approved_config["from_date"])
    to_date = str(approved_config["to_date"])
    source_hash = sha256(source_path)
    rendered_by_tf = {}
    for timeframe in ("M5", "M15", "H1"):
        rendered = stage3.render_ini(template_text, str(approved_config.get("ea") or stage1_spec.get("ea") or "EA"),
                                     timeframe, runtime_spec, from_date, to_date,
                                     source_text=source_text)
        rendered = stage3.apply_config_surface(rendered, runtime_config)
        rendered = stage3.rebuild_smoke_inputs_from_source(
            rendered, source_path, approved_config)
        rendered = stage3.ensure_stage1_parameter_surface(rendered, runtime_spec)
        rendered = stage3.apply_source_fixed_nflag_inputs(rendered, source_text)
        rendered = stage3.normalize_optimized_ini_numbers(rendered)
        rendered = stage3.apply_shared_input_surface(rendered, runtime_spec)
        stage3.validate_source_regime_contract(rendered, source_path, "default")
        stage3.validate_tester_inputs(rendered, runtime_spec, "default")
        rendered_by_tf[timeframe] = rendered.encode("utf-8")

    destination = (ROOT / "state" / "stage2_v2" / "tester_inis" / cycle_id /
                   f"{source_hash}.{approved_config_sha256}")
    destination.mkdir(parents=True, exist_ok=True)
    files = {}
    for timeframe, payload in rendered_by_tf.items():
        path = destination / f"{timeframe}.ini"
        descriptor, temporary = tempfile.mkstemp(prefix=f".{timeframe}.", suffix=".ini", dir=destination)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            stage3.normalize_opt_ini.normalize_opt_ini(Path(temporary))
            if not stage3.normalize_opt_ini.is_normalized(Path(temporary)):
                raise ValueError(f"Stage 2 tester INI normalization failed for {timeframe}")
            normalized_payload = Path(temporary).read_bytes()
            if path.exists():
                if path.read_bytes() != normalized_payload:
                    raise ValueError(f"Stage 2 tester INI artifact collision for {timeframe}")
                Path(temporary).unlink()
            else:
                os.replace(temporary, path)
        except Exception:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise
        files[timeframe] = {"path": str(path.resolve()), "sha256": sha256(path)}
    return {
        "schema_version": 1,
        "status": "prepared_unpublished",
        "policy": "stage2_materialized_immutable_tester_inis_v1",
        "template_ini": str(STAGE3_TEMPLATE.resolve()),
        "template_ini_sha256": template_hash,
        "source_sha256": source_hash,
        "approved_tester_config_sha256": approved_config_sha256,
        "timeframe_order": ["M5", "M15", "H1"],
        "files": files,
    }
def publish(state_path: Path, verification_path: Path, event_path: Path) -> dict:
    state = load(state_path)
    verification = load(verification_path)
    if state.get("schema_version") != 2 or state.get("current_stage") != "stage2_running":
        raise ValueError("canonical v2 state is not stage2_running")
    if verification.get("schema_version") != 2 or verification.get("stage") != 2:
        raise ValueError("invalid Stage 2 verification")
    if verification.get("cycle_id") != state.get("cycle_id") or verification.get("ea") != state.get("current_ea"):
        raise ValueError("Stage 2 verification identity mismatch")
    if verification.get("passed") is not True or not all(verification.get("checks", {}).values()):
        raise ValueError("Stage 2 structural verification did not pass")
    if verification.get("score_version") != "MINATO_BASE_SCORE_V2.0":
        raise ValueError("Stage 2 did not attest MINATO_BASE_SCORE_V2.0")
    stage1_spec = Path(verification.get("stage1_spec", ""))
    stage1_event = Path(verification.get("stage1_event", ""))
    if not stage1_spec.is_file() or sha256(stage1_spec) != verification.get("stage1_spec_sha256"):
        raise ValueError("Stage 1 specification changed during Stage 2")
    if not stage1_event.is_file():
        raise ValueError("Stage 1 completion event is missing during Stage 2 publication")
    stage1_event_value = load(stage1_event)
    if stage1_event_value.get("artifact_sha256") != verification.get("stage1_spec_sha256"):
        raise ValueError("Stage 1 event/spec lineage changed during Stage 2")
    source_path = Path(verification.get("source", ""))
    if not source_path.is_file():
        raise ValueError("Stage 2 verification source is missing during publication")
    source_text = source_path.read_text(encoding="utf-8-sig", errors="replace")
    if sha256(source_path) != str(verification.get("source_sha256") or "").upper():
        raise ValueError("Stage 2 source changed after final verification")
    ex5_path = Path(verification.get("ex5") or "")
    if (not ex5_path.is_file()
            or sha256(ex5_path) != str((verification.get("compile") or {}).get("ex5_sha256") or "").upper()):
        raise ValueError("Stage 2 binary changed after final verification")
    minato_path = Path(verification.get("canonical_minato") or "")
    if (not minato_path.is_file()
            or sha256(minato_path) != str(verification.get("canonical_minato_sha256") or "").upper()):
        raise ValueError("canonical Minato include changed after final verification")
    protected = verify_protected_contract(ROOT)
    if (protected != verification.get("protected_behavior_contract")
            or sha256(ROOT / "state" / "contracts" / "kcos_stage2_protected_behavior_v1.json")
                != str(verification.get("verification_policy_sha256") or "").upper()
                or verification.get("verification_policy_id")
                    != "kcos-stage2-protected-behavior-v1+math-correction-admission-v1"
            or sha256(ROOT / "state" / "kcos_stage2_verify_v2.py")
                != str(verification.get("verifier_code_sha256") or "").upper()):
        raise ValueError("Stage 2 verifier, policy, or protected integration changed after verification")
    code_hashes = verification.get("stage2_pipeline_code_hashes") or {}
    if set(code_hashes) != set(STAGE2_PIPELINE_CODE_FILES):
        raise ValueError("Stage 2 verification has incomplete pipeline code identity")
    for name, expected_hash in code_hashes.items():
        if Path(name).name != name:
            raise ValueError("Stage 2 pipeline code identity contains a non-canonical filename")
        code_path = ROOT / "state" / name
        if not code_path.is_file() or sha256(code_path) != str(expected_hash).upper():
            raise ValueError(f"Stage 2 pipeline code changed after verification: {name}")
    tester_config = stage2_config_from_spec(load(stage1_spec), source_text=source_text)
    tester_config_hash = config_sha256(tester_config)
    if verification.get("approved_tester_config_sha256") != tester_config_hash:
        raise ValueError("Stage 2 verification did not attest the approved tester_config")
    tester_ini_artifacts = materialize_stage2_tester_inis(
        load(stage1_spec), source_path, source_text, tester_config,
        tester_config_hash, str(state["cycle_id"]))
    stage2_handoff = load(STAGE2_HANDOFF)
    if (stage2_handoff.get("cycle_id") != state.get("cycle_id")
            or stage2_handoff.get("approved_tester_config_sha256") != tester_config_hash
            or stage2_handoff.get("source") != str(source_path.resolve())):
        raise ValueError("Stage 2 handoff changed before tester INI publication")
    if not validate_strategy_math_correction_receipt(verification, stage2_handoff):
        raise ValueError("Stage 2 math-correction evidence changed after verification")
    tester_ini_artifacts["status"] = "published"
    stage2_handoff["tester_ini_artifacts"] = tester_ini_artifacts
    atomic_json(STAGE2_HANDOFF, stage2_handoff)
    compile_result = verification.get("compile") or {}
    if compile_result.get("errors") != 0 or compile_result.get("warnings") != 0:
        raise ValueError("Stage 2 compile must have zero errors and zero warnings")
    if not 1 <= int(compile_result.get("attempt", 0)) <= 5:
        raise ValueError("Stage 2 compile attempt is outside the approved limit")
    completed = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    event = {
        "schema_version": 2,
        "cycle_id": state["cycle_id"],
        "stage": 2,
        "status": "complete",
        "ea": state["current_ea"],
        "score_version": verification["score_version"],
        "next_stage": 3,
        "verification": str(verification_path.resolve()),
        "verification_sha256": sha256(verification_path),
        "mq5_sha256": verification["source_sha256"],
        "ex5_sha256": compile_result["ex5_sha256"],
        "canonical_minato_sha256": verification["canonical_minato_sha256"],
        "stage1_spec": str(stage1_spec.resolve()),
        "stage1_spec_sha256": verification["stage1_spec_sha256"],
        "approved_tester_config_sha256": tester_config_hash,
        "configuration_authority": "stage2_finalized_tester_config_and_hash_bound_tester_inis",
        "tester_ini_artifacts": tester_ini_artifacts,
        "compile_attempts": compile_result["attempt"],
        "completed_utc": completed,
    }
    next_state = dict(state)
    next_state.update({
        "current_stage": "stage3_pending",
        "stage_status": "stage2_complete_held",
        "next_action": "audit_stage2_before_stage3",
        "hold_after_stage": 2,
        "last_stage2_verification": str(verification_path.resolve()),
        "last_stage2_event": str(event_path.resolve()),
        "updated_utc": completed,
    })
    atomic_json(event_path, event)
    # A successful same-cycle retry supersedes an earlier failure artifact.
    # Quarantine it instead of leaving supervisors to read stale failure
    # evidence after the canonical completion event has been published.
    failed_path = ROOT / "state" / "events" / f"{state['cycle_id']}.stage2.failed.json"
    if failed_path.is_file():
        quarantine = ROOT / "state" / "diagnostics" / "superseded"
        quarantine.mkdir(parents=True, exist_ok=True)
        quarantined = quarantine / failed_path.name
        failed_path.replace(quarantined)
        next_state["stage2_superseded_failure_event"] = str(quarantined.resolve())
    # Success resolves any active Stage 2 repair/failure pointer. Keep the
    # archived event path separately so rotation/supervisor consumers never
    # follow a missing canonical pointer or mistake old failure for current.
    next_state["stage2_failure_event"] = None
    next_state["stage2_repair_reason"] = None
    next_state["stage2_repair_required"] = False
    next_state["stage2_repair_resolved_by"] = str(event_path.resolve())
    atomic_json(state_path, next_state)
    if LEASE.is_file():
        LEASE.unlink()
    return event


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--verification", required=True, type=Path)
    parser.add_argument("--event", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(publish(args.state, args.verification, args.event)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
