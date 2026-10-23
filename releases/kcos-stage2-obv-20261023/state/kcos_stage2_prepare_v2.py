#!/usr/bin/env python3
"""Prepare a single-flight Stage 2 handoff from canonical v2 state."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from kcos_tester_config_v2 import (stage2_config_from_spec, stage3_config_from_spec,
                                   config_sha256, validate_config_against_source,
                                   validate_optimized_source_usage)
from kcos_optimization_contract_v2 import enforce_config
from kcos_runtime_paths_v2 import assert_safe_root, mt5_data_root
from kcos_stage2_protected_contract_v1 import verify_protected_contract


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
        # Queue candidates retain their explicit immutable source pointer.
        # Research publishes a null source because Stage 2 owns its creation;
        # bind that source through authenticated build evidence instead.
        if configured.get("candidate_source"):
            return configured
        return bind_generated_source(state, configured)
    spec_path = Path(state.get("last_stage1_artifact", ""))
    event_path = Path(state.get("last_stage1_event", ""))
    if not spec_path.is_file() or not event_path.is_file():
        raise ValueError("canonical cycle has no usable Stage 1 artifact/event pointers")
    spec = load(spec_path)
    if spec.get("research_mode") == "research_new":
        return bind_generated_source(state, {
            "schema_version": 2,
            "cycle_id": state["cycle_id"],
            "stage": 1,
            "candidate_ea": state["current_ea"],
            "candidate_source": None,
            "research_mode": "research_new",
            "expected_spec_path": str(spec_path.resolve()),
            "expected_event_path": str(event_path.resolve()),
            "resolved_from_canonical_state": True,
        })
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


def bind_generated_source(state: dict, stage1: dict) -> dict:
    """Bind a research build without rewriting the published Stage 1 handoff."""
    cycle, ea = state.get("cycle_id"), state.get("current_ea")
    if any(not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value) for value in (cycle, ea)):
        raise ValueError("generated source cycle and EA require safe canonical filenames")
    spec_path = Path(stage1.get("expected_spec_path") or "")
    event_path = Path(stage1.get("expected_event_path") or "")
    if (
        not spec_path.is_file() or not event_path.is_file()
        or Path(state.get("last_stage1_artifact") or "").resolve() != spec_path.resolve()
        or Path(state.get("last_stage1_event") or "").resolve() != event_path.resolve()
        or spec_path.resolve() != (ROOT / "state" / "specs" / f"{cycle}.stage1.spec.json").resolve()
        or event_path.resolve() != (ROOT / "state" / "events" / f"{cycle}.stage1.complete.json").resolve()
    ):
        raise ValueError("canonical Stage 1 pointers do not identify this research handoff")
    spec, event = load(spec_path), load(event_path)
    if (
        stage1.get("cycle_id") != cycle or stage1.get("candidate_ea") != ea
        or spec.get("schema_version") != 2 or spec.get("stage") != 1
        or spec.get("cycle_id") != cycle or spec.get("ea") != ea
        or spec.get("decision") != "approved_for_stage2"
        or spec.get("research_mode") != "research_new"
    ):
        raise ValueError("Stage 1 specification is not approved research for this cycle and EA")
    if (
        event.get("schema_version") != 2 or event.get("stage") != 1
        or event.get("status") != "complete" or event.get("cycle_id") != cycle
        or event.get("ea") != ea
        or Path(event.get("artifact") or "").resolve() != spec_path.resolve()
        or str(event.get("artifact_sha256") or "").upper() != sha256(spec_path)
    ):
        raise ValueError("Stage 1 completion event does not prove this research specification")
    source = Path(state.get("candidate_source") or "")
    expected = ROOT / "state" / "candidates" / f"{ea}.mq5"
    try:
        expected.parent.resolve().relative_to(ROOT.resolve())
        source.resolve().relative_to(expected.parent.resolve())
    except ValueError as exc:
        raise ValueError("generated source escapes the canonical candidate directory") from exc
    if not source.is_file() or source.resolve() != expected.resolve():
        raise ValueError("generated source requires the exact candidate path and a build receipt")
    receipt_path = source.with_suffix(source.suffix + ".build.json")
    if not receipt_path.is_file():
        raise ValueError("generated source requires the exact candidate path and a build receipt")
    receipt = load(receipt_path)
    if (
        receipt.get("schema_version") != 1 or receipt.get("stage") != 2
        or receipt.get("cycle_id") != cycle or receipt.get("candidate_ea") != ea
        or receipt.get("status") != "BUILT_FROM_SPEC"
        or Path(receipt.get("output") or "").resolve() != source.resolve()
        or Path(receipt.get("source_spec") or "").resolve() != spec_path.resolve()
        or str(receipt.get("source_spec_sha256") or "").upper() != sha256(spec_path)
        or str(receipt.get("sha256") or "").upper() != sha256(source)
        or receipt.get("implementation_mode") not in {"deterministic_template", "model_reviewed", "model_generated_source_admitted"}
    ):
        raise ValueError("generated source build receipt does not authenticate this cycle/spec/source")
    implementation_review = (receipt.get("implementation_admission")
                             if receipt.get("implementation_mode") == "model_generated_source_admitted"
                             else receipt.get("implementation_review"))
    if receipt.get("implementation_mode") == "model_generated_source_admitted":
        if not isinstance(implementation_review, dict):
            raise ValueError("generated source requires a deterministic source-admission receipt")
        admission_path = Path(implementation_review.get("path") or "")
        try:
            admission_path.resolve().relative_to(expected.parent.resolve())
        except ValueError as exc:
            raise ValueError("generated source admission escapes the candidate directory") from exc
        if (not admission_path.is_file()
                or str(implementation_review.get("sha256") or "").upper() != sha256(admission_path)):
            raise ValueError("generated source admission file or hash mismatch")
        admission = load(admission_path)
        if (admission.get("schema_version") != 1 or admission.get("passed") is not True
                or admission.get("status") != "DETERMINISTIC_SOURCE_ADMISSION"
                or admission.get("cycle_id") != cycle or admission.get("ea") != ea
                or str(admission.get("source_spec_sha256") or "").upper() != sha256(spec_path)
                or str(admission.get("source_sha256") or "").upper() != sha256(source)):
            raise ValueError("generated source admission does not prove this cycle/spec/source")
    elif receipt.get("implementation_mode") == "model_reviewed":
        if not isinstance(implementation_review, dict):
            raise ValueError("generated source requires an independent implementation review")
        review_path = Path(implementation_review.get("path") or "")
        try:
            review_path.resolve().relative_to(expected.parent.resolve())
        except ValueError as exc:
            raise ValueError("generated source review escapes the candidate directory") from exc
        if (not review_path.is_file()
                or str(implementation_review.get("sha256") or "").upper() != sha256(review_path)):
            raise ValueError("generated source implementation review file or hash mismatch")
        review = load(review_path)
        if (
            review.get("schema_version") != 1 or review.get("passed") is not True
            or review.get("cycle_id") != cycle or review.get("ea") != ea
            or str(review.get("source_spec_sha256") or "").upper() != sha256(spec_path)
            or str(review.get("source_sha256") or "").upper() != sha256(source)
            or not review.get("reviewer_provider") or not review.get("reviewer_model")
        ):
            raise ValueError("generated source implementation review does not prove this spec/source")
    return {
        **stage1,
        "candidate_source": str(source.resolve()),
        "resolved_generated_source": True,
        "generated_source_build_receipt": str(receipt_path.resolve()),
        "generated_source_build_receipt_sha256": sha256(receipt_path),
        "generated_source_implementation_review": implementation_review,
        "generated_source_implementation_mode": receipt.get("implementation_mode"),
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


def prepare_gjr_base_lot_source(stage1: dict, state: dict, spec_path: Path,
                                *, dry_run: bool) -> tuple[str, dict | None]:
    """Apply the Stage 2 GJR sizing repair before binding source/config hashes."""
    ea = str(state.get("current_ea") or "")
    compact_ea = re.sub(r"[^a-z0-9]", "", ea.casefold())
    source = Path(str(stage1.get("candidate_source") or ""))
    if "gjr" not in compact_ea or "garch" not in compact_ea:
        return source.read_text(encoding="utf-8-sig"), None

    from kcos_stage2_repair_v2 import repair_gjr_base_lot

    raw = source.read_bytes()
    has_bom = raw.startswith(b"\xef\xbb\xbf")
    original_text = raw.decode("utf-8-sig")
    repaired_text, repairs = repair_gjr_base_lot(original_text)
    if not repairs:
        return original_text, None
    if stage1.get("resolved_generated_source"):
        raise ValueError(
            "GJR Stage 2 sizing repair cannot rewrite a generated source while its build/review receipt is bound; "
            "regenerate it through the updated Stage 2 admission path first"
        )

    repaired_bytes = (b"\xef\xbb\xbf" if has_bom else b"") + repaired_text.encode("utf-8")
    original_sha = hashlib.sha256(raw).hexdigest().upper()
    repaired_sha = hashlib.sha256(repaired_bytes).hexdigest().upper()
    receipt = {
        "schema_version": 1,
        "stage": 2,
        "status": "DETERMINISTIC_SOURCE_REPAIR",
        "cycle_id": state["cycle_id"],
        "ea": ea,
        "source": str(source.resolve()),
        "stage1_spec": str(spec_path.resolve()),
        "stage1_spec_sha256": sha256(spec_path),
        "source_sha256_before": original_sha,
        "source_sha256_after": repaired_sha,
        "repair": repairs[0],
        "policy": {
            "input": "InpBaseLot",
            "default": 0.01,
            "optimization": "stage3_grid",
            "grid": [0.01, 0.01, 0.20],
            "minimum_volume_clamp": "MathMax(InpBaseLot, SYMBOL_VOLUME_MIN)",
            "regime_multiplier": "preserved and applied after base clamp",
            "stage1_artifact_mutated": False,
        },
        "created_utc": now(),
        "dry_run": bool(dry_run),
    }
    if dry_run:
        receipt["written"] = False
        return repaired_text, receipt

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = source.with_name(source.name + ".bak.stage2-gjr-base-lot-" + timestamp)
    if backup.exists():
        raise FileExistsError(f"GJR Stage 2 source backup already exists: {backup}")
    import shutil
    shutil.copy2(source, backup)

    descriptor, temporary = tempfile.mkstemp(prefix=f".{source.name}.", suffix=".tmp", dir=str(source.parent))
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(repaired_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, source)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise

    receipt_path = ROOT / "state" / "receipts" / f"{state['cycle_id']}.stage2.gjr-base-lot-repair.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt.update({"written": True, "backup": str(backup.resolve())})
    atomic_json(receipt_path, receipt)
    receipt["path"] = str(receipt_path.resolve())
    receipt["sha256"] = sha256(receipt_path)
    return repaired_text, receipt


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


def validate_stage3_grid_before_publish(spec: dict, source_text: str, ea_name: str) -> dict:
    """Run Stage 3's existing grid writer and validator as a pure preflight.

    This shares the approved grid policy without writing an INI, starting MT5,
    or changing the Stage 2 configuration that is published downstream.
    """
    import kcos_stage3_prepare_v2 as stage3

    grid_spec = dict(spec)
    grid_spec["tester_config"] = stage3_config_from_spec(spec, source_text=source_text)
    if "B1_1_MTFTrend_v4_Elder" in ea_name:
        for name, item in grid_spec["tester_config"].get("inputs", {}).items():
            if name.startswith("InpRegime_"):
                item["optimize"] = "N"
    rows = []
    for name, item in grid_spec["tester_config"].get("inputs", {}).items():
        if str(item.get("optimize", "N")).upper() == "Y":
            try:
                values = [float(item[key]) for key in ("default", "start", "step", "stop")]
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"optimized Stage 3 input {name} has a malformed authenticated domain") from exc
            rows.append(name + "=" + "||".join(stage3._clean_ini_number(value) for value in values) + "||Y")
        else:
            if "default" not in item:
                raise ValueError(f"fixed Stage 3 input {name} has no authenticated default")
            rows.append(name + "=" + str(item["default"]))
    rendered = "[TesterInputs]\n" + "\n".join(rows) + "\n"
    normalized = stage3.apply_wide_grid_policy(rendered, grid_spec)
    normalized = stage3.apply_coupled_wide_constraints(normalized)
    # Run Stage 3's exact grid validator; omit only the template-specific
    # shared-row check because no INI template is rendered at Stage 2.
    stage3.validate_tester_inputs(normalized, grid_spec, validate_shared_surface=False)
    normalized_rows = [line.strip() for line in normalized.splitlines()
                       if "=" in line and not line.lstrip().startswith(";")]
    return {
        "policy": "stage3_prepare_v2.apply_wide_grid_policy+validate_tester_inputs(shared_surface=false)",
        "policy_sha256": hashlib.sha256(Path(stage3.__file__).read_bytes()).hexdigest().upper(),
        "approved_config_sha256": config_sha256(grid_spec["tester_config"]),
        "normalized_grid_sha256": hashlib.sha256("\n".join(normalized_rows).encode("utf-8")).hexdigest().upper(),
        "optimized_input_count": len(normalized_rows),
        "passed": True,
    }


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
    source_text, source_repair_receipt = prepare_gjr_base_lot_source(
        stage1, state, spec_path, dry_run=dry_run)
    tester_config = stage2_config_from_spec(spec, source_text=source_text)
    if tester_config.get("cycle_id", state["cycle_id"]) != state["cycle_id"]:
        raise ValueError("Stage 1 tester_config cycle mismatch")
    if tester_config.get("ea", state["current_ea"]) != state["current_ea"]:
        raise ValueError("Stage 1 tester_config EA mismatch")
    implementation_contract_path = None
    implementation_contract_sha256 = None
    if ("dcc-garch" in json.dumps(spec, ensure_ascii=False).lower()
            and stage1.get("generated_source_implementation_mode") != "model_generated_source_admitted"):
        implementation_contract_path = ROOT / "state" / "implementation_contracts" / f"{state['cycle_id']}.dcc-garch.v1.json"
        if not implementation_contract_path.is_file():
            raise ValueError("DCC-GARCH implementation contract is missing")
        implementation_contract = load(implementation_contract_path)
        implementation_contract_sha256 = sha256(implementation_contract_path)
        if (
            implementation_contract.get("cycle_id") != state["cycle_id"]
            or implementation_contract.get("ea") != state["current_ea"]
            or implementation_contract.get("source_spec_sha256", "").upper() != sha256(spec_path)
            or f"Implementation contract SHA256: {implementation_contract_sha256.lower()}".lower() not in source_text.lower()
        ):
            raise ValueError("DCC-GARCH implementation contract does not authenticate this cycle/spec/source")
    enforce_config(tester_config, source_text)
    source_contract = validate_config_against_source(
        tester_config,
        source_text,
    )
    validate_optimized_source_usage(tester_config, source_text)
    stage3_grid_preflight = validate_stage3_grid_before_publish(spec, source_text, state["current_ea"])
    # Hash only after the complete Stage 2 policy/source reconciliation.  The
    # previous ordering attested the pre-repair surface and allowed stale
    # ranges to survive into Stage 3.
    approved_config_sha = config_sha256(tester_config)
    minato_approval = load(MINATO_APPROVAL)
    approved_sha256 = minato_approval.get("sha256", minato_approval.get("score_include_sha256"))
    approved_flag = minato_approval.get("approved", True if minato_approval.get("score_include_sha256") else False)
    if approved_sha256 != sha256(MINATO) or approved_flag is not True:
        raise ValueError("canonical Minato include changed or is not operator-approved")
    protected_contract = verify_protected_contract(ROOT)

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
        "source_build_receipt": stage1.get("generated_source_build_receipt"),
        "source_build_receipt_sha256": stage1.get("generated_source_build_receipt_sha256"),
        "source_implementation_review": stage1.get("generated_source_implementation_review"),
        "source_implementation_mode": stage1.get("generated_source_implementation_mode"),
        "approved_tester_config": tester_config,
        "approved_tester_config_sha256": approved_config_sha,
        "configuration_authority": "stage1_spec.tester_config_plus_stage2_derived_policy",
        "implementation_contract": str(implementation_contract_path.resolve()) if implementation_contract_path else None,
        "implementation_contract_sha256": implementation_contract_sha256,
        "source_configuration_contract": source_contract,
        "stage3_grid_preflight": stage3_grid_preflight,
        "source_sha256_before": ((source_repair_receipt or {}).get("source_sha256_after") or sha256(source)),
        "source_sha256_before_stage2_repair": (source_repair_receipt or {}).get("source_sha256_before"),
        "stage2_source_repair_receipt": (source_repair_receipt or {}).get("path"),
        "stage2_source_repair_receipt_sha256": (source_repair_receipt or {}).get("sha256"),
        "ex5": str(source.with_suffix(".ex5").resolve()),
        "canonical_minato": str(MINATO.resolve()),
        "canonical_minato_sha256": sha256(MINATO),
        "canonical_minato_approval": str(MINATO_APPROVAL.resolve()),
        "protected_behavior_contract": protected_contract,
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
            "base_position_size": "GJR InpBaseLot is Stage 2 derived and Stage 3 optimized on [0.01,0.20] step 0.01 before regime scaling",
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
