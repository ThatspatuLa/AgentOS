#!/usr/bin/env python3
"""Prepare a single-flight Stage 2 handoff from canonical v2 state."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from kcos_tester_config_v2 import config_from_spec, config_sha256, validate_config_against_source, validate_optimized_source_usage
from kcos_optimization_contract_v2 import enforce_config
from kcos_runtime_paths_v2 import assert_safe_root, mt5_data_root


ROOT = assert_safe_root()
STATE = ROOT / "state" / "kcos_cycle_state_v2.json"
LEGACY_STATE = ROOT / "state" / "kcos_cron_state.json"
STAGE1_HANDOFF = ROOT / "state" / "handoffs" / "Stage1_v2.json"
STAGE2_HANDOFF = ROOT / "state" / "handoffs" / "Stage2_v2.json"
LEASE = ROOT / "state" / "leases" / "stage2_v2.lock.json"
MINATO = mt5_data_root() / "MQL5" / "Include" / "minato_base_score_v2.mqh"
MINATO_SCORE_VERSION = "MINATO_BASE_SCORE_V2.0"
METAEDITOR = Path(r"C:\Program Files\EightCap MetaTrader 5\metaeditor64.exe")
MINATO_APPROVAL = ROOT / "state" / "contracts" / "minato_base_score_approved_v2.json"


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain an object")
    return value


def resolve_stage1_handoff(state: dict) -> dict:
    """Resolve Stage 1 from canonical cycle pointers, not a stale singleton."""
    configured = load(STAGE1_HANDOFF)
    if (configured.get("cycle_id") == state.get("cycle_id")
            and configured.get("candidate_ea") == state.get("current_ea")):
        return configured
    spec_path = Path(state.get("last_stage1_artifact", ""))
    event_path = Path(state.get("last_stage1_event", ""))
    if not spec_path.is_file() or not event_path.is_file():
        raise ValueError("canonical cycle has no usable Stage 1 artifact/event pointers")
    spec = load(spec_path)
    # Prefer the actual .mq5 candidate_source if the cycle state names one
    # (research-new flow: Stage 2 builds the .mq5 from the spec, then writes
    # it back to state.candidate_source). Fall back to spec.source_paths[0]
    # only when the cycle state has no usable .mq5 pointer.
    candidate_source = None
    state_source = state.get("candidate_source") or ""
    if state_source and Path(state_source).is_file() and Path(state_source).suffix.lower() == ".mq5":
        candidate_source = state_source
    if candidate_source is None:
        sources = spec.get("source_paths") or []
        if not sources:
            raise ValueError("canonical Stage 1 specification has no source path")
        candidate_source = sources[0]
    return {
        "schema_version": 2,
        "cycle_id": state["cycle_id"],
        "stage": 1,
        "candidate_ea": state["current_ea"],
        "candidate_source": candidate_source,
        "expected_spec_path": str(spec_path.resolve()),
        "expected_event_path": str(event_path.resolve()),
        "resolved_from_canonical_state": True,
    }


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


def reconcile_lease(state: dict) -> None:
    """Quarantine only a lease proven stale by canonical state."""
    if not LEASE.exists():
        return
    lease = load(LEASE)
    owner_cycle = lease.get("cycle_id")
    current_cycle = state.get("cycle_id")
    active = state.get("current_stage") == "stage2_running" and state.get("stage_status") == "stage2_preflight"
    if active and owner_cycle == current_cycle:
        return
    if active and owner_cycle != current_cycle:
        raise ValueError("Stage 2 lease belongs to another active cycle")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    quarantine = LEASE.with_name(f"{LEASE.name}.stale-{stamp}")
    LEASE.replace(quarantine)


def prepare(dry_run: bool = False) -> dict:
    state = load(STATE)
    stage1 = resolve_stage1_handoff(state)
    if state.get("schema_version") != 2 or stage1.get("schema_version") != 2:
        raise ValueError("v2 state or Stage 1 handoff schema mismatch")
    resumable = (
        state.get("current_stage") == "stage2_running"
        and state.get("stage_status") == "stage2_preflight"
    )
    if not dry_run and state.get("current_stage") != "stage2_pending" and not resumable:
        raise ValueError(
            "Stage 2 requires stage2_pending, or resumable "
            f"stage2_running+stage2_preflight; got {state.get('current_stage')}+{state.get('stage_status')}"
        )
    if state.get("cycle_id") != stage1.get("cycle_id"):
        raise ValueError("Stage 1 handoff belongs to a different cycle")
    if state.get("current_ea") != stage1.get("candidate_ea"):
        raise ValueError("canonical EA does not match Stage 1")

    spec_path = Path(stage1["expected_spec_path"])
    event_path = Path(stage1["expected_event_path"])
    source = Path(stage1["candidate_source"])
    for required in (spec_path, event_path, source, MINATO, METAEDITOR, MINATO_APPROVAL):
        if not required.is_file():
            raise ValueError(f"required Stage 2 input missing: {required}")
    spec = load(spec_path)
    valid_spec = (
        spec.get("schema_version") == 2
        and spec.get("stage") == 1
        and spec.get("cycle_id") == state["cycle_id"]
        and spec.get("ea") == state["current_ea"]
        and spec.get("decision") == "approved_for_stage2"
    )
    if not valid_spec:
        raise ValueError("Stage 1 specification is not valid for this candidate")
    tester_config = config_from_spec(spec)
    if tester_config.get("cycle_id", state["cycle_id"]) != state["cycle_id"]:
        raise ValueError("Stage 1 tester_config cycle mismatch")
    if tester_config.get("ea", state["current_ea"]) != state["current_ea"]:
        raise ValueError("Stage 1 tester_config EA mismatch")
    event = load(event_path)
    if (
        event.get("schema_version") != 2
        or event.get("stage") != 1
        or event.get("status") != "complete"
        or event.get("cycle_id") != state["cycle_id"]
        or Path(event.get("artifact", "")).resolve() != spec_path.resolve()
        or event.get("artifact_sha256") != sha256(spec_path)
    ):
        raise ValueError("Stage 1 completion event does not prove this specification")
    source_text = source.read_text(encoding="utf-8-sig", errors="replace")
    enforce_config(tester_config, source_text)
    source_contract = validate_config_against_source(
        tester_config,
        source_text,
    )
    validate_optimized_source_usage(tester_config, source_text)
    # Hash only after the complete Stage 2 policy/source reconciliation.  The
    # previous ordering attested the pre-repair surface and allowed stale
    # ranges to survive into Stage 3.
    approved_config_sha = config_sha256(tester_config)
    minato_approval = load(MINATO_APPROVAL)
    approved_sha256 = minato_approval.get("sha256", minato_approval.get("score_include_sha256"))
    approved_flag = minato_approval.get("approved", True if minato_approval.get("score_include_sha256") else False)
    if approved_sha256 != sha256(MINATO) or approved_flag is not True:
        raise ValueError("canonical Minato include changed or is not operator-approved")

    cycle = state["cycle_id"]
    verification = ROOT / "state" / "verification" / f"{cycle}.stage2.verification.json"
    completion = ROOT / "state" / "events" / f"{cycle}.stage2.complete.json"
    attempts = ROOT / "state" / "attempts" / f"{cycle}.stage2.compile.json"
    latest_compile = ROOT / "state" / "attempts" / f"{cycle}.stage2.compile.latest.json"
    handoff = {
        "schema_version": 2,
        "cycle_id": cycle,
        "stage": 2,
        "ea": state["current_ea"],
        "stage1_spec": str(spec_path.resolve()),
        "stage1_spec_sha256": sha256(spec_path),
        "stage1_event": str(event_path.resolve()),
        "source": str(source.resolve()),
        "approved_tester_config": tester_config,
        "approved_tester_config_sha256": approved_config_sha,
        "configuration_authority": "stage1_spec.tester_config",
        "source_configuration_contract": source_contract,
        "source_sha256_before": sha256(source),
        "ex5": str(source.with_suffix(".ex5").resolve()),
        "canonical_minato": str(MINATO.resolve()),
        "canonical_minato_sha256": sha256(MINATO),
        "canonical_minato_approval": str(MINATO_APPROVAL.resolve()),
        "score_version": MINATO_SCORE_VERSION,
        "metaeditor": str(METAEDITOR.resolve()),
        "rebuild_from_stage1": True,
        "legacy_without_stage1_policy": "verify_and_repair_without_forced_rebuild",
        "closed_bar_required": True,
        "minimum_margin_level": 120.0,
        "exact_margin_floor_allowed": True,
        "zero_regime_quantity_semantics": "skip_trade_before_lot_normalization",
        "zero_regime_tp_semantics": "no_fixed_take_profit",
        "projected_margin_check_required": True,
        "optimization_surface_policy": {
            "regime_inputs": "EA-specific; GARCH/MIDAS requires all 12 optimized, Donchian compatibility fields remain fixed",
            "score_version": MINATO_SCORE_VERSION,
            "metadata_inputs_not_optimized": ["InpMagicNumber", "InpSlippagePts", "InpTradeComment"]
        },
        "max_compile_attempts": 5,
        "compile_attempts_file": str(attempts.resolve()),
        "latest_compile_result": str(latest_compile.resolve()),
        "expected_verification": str(verification.resolve()),
        "expected_event": str(completion.resolve()),
        "legacy_state": str(LEGACY_STATE.resolve()),
        "legacy_state_sha256_before": sha256(LEGACY_STATE),
        "legacy_state_write_allowed": False,
        "mt5_launch_allowed": False,
        "web_allowed": False,
        "hold_after_stage": 2,
        "prepared_utc": now(),
        "dry_run": dry_run,
    }
    if dry_run:
        return handoff
    reconcile_lease(state)
    if LEASE.exists():
        raise ValueError(f"Stage 2 single-flight lease already exists: {LEASE}")
    LEASE.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(str(LEASE), os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump({"schema_version": 2, "cycle_id": cycle, "created_utc": now()}, handle, indent=2)
        handle.write("\n")
    next_state = dict(state)
    next_state.update({"current_stage": "stage2_running", "stage_status": "stage2_preflight", "updated_utc": now()})
    atomic_json(STAGE2_HANDOFF, handoff)
    atomic_json(STATE, next_state)
    return handoff


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(json.dumps(prepare(dry_run=args.dry_run), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
