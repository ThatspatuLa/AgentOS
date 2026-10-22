#!/usr/bin/env python3
"""Deterministic Stage 2 executor: prepare -> compile -> verify -> publish."""
from __future__ import annotations
import json
import hashlib
import os
import subprocess
import sys
import re
import shutil
import traceback
import errno
import msvcrt
from datetime import datetime, timezone
from pathlib import Path
from kcos_runtime_paths_v2 import assert_safe_root, mt5_data_root
import kcos_stage2_source_defect_recovery_v1 as stage2_source_recovery
import kcos_stage2_tester_diagnostic_v1 as stage2_tester
import kcos_stage2_repair_loop_v1 as stage2_repair_loop
import kcos_rotation_v2 as rotation
from kcos_stage2_verify_v2 import (dcc_closed_return_arrays_are_bounded,
                                   zero_disables_cooldown_controls,
                                   zero_disables_time_stop_controls)

ROOT = assert_safe_root()
STATE = ROOT / "state" / "kcos_cycle_state_v2.json"
PREPARE = ROOT / "state" / "kcos_stage2_prepare_v2.py"
COMPILE = ROOT / "state" / "kcos_stage2_compile_v2.py"
VERIFY = ROOT / "state" / "kcos_stage2_verify_v2.py"
REPAIR = ROOT / "state" / "kcos_stage2_repair_v2.py"

REPAIR_LOOP = ROOT / "state" / "kcos_stage2_repair_loop_v1.py"
PUBLISH = ROOT / "state" / "kcos_stage2_publish_v2.py"
BUILD = ROOT / "state" / "kcos_stage2_build_ea_v1.py"
CANDIDATES_DIR = ROOT / "state" / "candidates"
LEASE = ROOT / "state" / "leases" / "stage2_v2.lock.json"
EXECUTOR_LOCK = ROOT / "state" / "leases" / "stage2_executor.lock"




def authenticated_stage3_zero_trade_repair(state: dict, cycle: str, ea: str) -> dict | None:
    """Return zero-trade provenance only when the same-cycle Stage 3 verifier binds it."""
    decision_value = state.get("stage3_zero_trade_decision")
    verification_value = state.get("stage3_zero_trade_verification")
    if not decision_value and not verification_value:
        return None
    if not decision_value or not verification_value:
        raise ValueError("Stage 3 zero-trade repair has incomplete decision/verifier pointers")
    decision_path, verification_path = Path(decision_value), Path(verification_value)
    decision_suffix = ".stage3.decision.json"
    verification_suffix = ".stage3.verification.json"
    decision_cycle = decision_path.name[:-len(decision_suffix)] if decision_path.name.endswith(decision_suffix) else None
    verification_cycle = verification_path.name[:-len(verification_suffix)] if verification_path.name.endswith(verification_suffix) else None
    if (decision_cycle and verification_cycle and decision_cycle == verification_cycle
            and decision_cycle != cycle):
        return None
    if not decision_path.is_file() or not verification_path.is_file():
        raise ValueError("Stage 3 zero-trade repair evidence is missing")
    decision = load(decision_path)
    verification = load(verification_path)
    decision_sha = hashlib.sha256(decision_path.read_bytes()).hexdigest().upper()
    checks = verification.get("checks")
    timeframes = decision.get("timeframes")
    if (decision.get("cycle_id") != cycle or decision.get("stage") != 3
            or decision.get("ea") != ea or decision.get("verdict") != "REPAIR_REQUIRED"
            or verification.get("cycle_id") != cycle or verification.get("stage") != 3
            or Path(verification.get("decision") or "").resolve() != decision_path.resolve()
            or str(verification.get("decision_sha256") or "").upper() != decision_sha
            or verification.get("passed") is not True or not isinstance(checks, dict)
            or not checks or any(value is not True for value in checks.values())
            or not isinstance(timeframes, dict) or not timeframes):
        raise ValueError("Stage 3 zero-trade decision is not bound by a passing same-cycle verifier")
    order = ("M5", "M15", "H1")
    names = list(timeframes)
    if names != list(order[:len(names)]):
        raise ValueError("Stage 3 repair decision lacks an ordered completed timeframe prefix")
    zero_trade_timeframes = []
    for timeframe, metrics in timeframes.items():
        if not isinstance(metrics, dict) or type(metrics.get("row_count")) is not int or metrics["row_count"] <= 0:
            raise ValueError(f"Stage 3 {timeframe} lacks authenticated completed result rows")
        if (metrics.get("zero_trades") is True and metrics.get("trades") == 0
                and metrics.get("max_trades") == 0):
            zero_trade_timeframes.append(timeframe)
    trigger = decision.get("repair_trigger") or {}
    if trigger.get("kind") == "completed_timeframe_zero_trades":
        if trigger.get("timeframe") not in names or trigger.get("timeframe") not in zero_trade_timeframes:
            raise ValueError("Stage 3 repair trigger does not identify an authenticated zero-trade timeframe")
    elif trigger.get("kind") == "all_timeframes_below_minimum_trade_count":
        if (set(names) != set(order)
                or any(type(metrics.get("max_trades")) is not int or metrics["max_trades"] >= 10
                       for metrics in timeframes.values())):
            raise ValueError("Stage 3 all-timeframe repair lacks completed low-trade evidence")
    elif trigger:
        raise ValueError("Stage 3 repair decision has an unknown repair trigger")
    elif set(names) != set(order) or set(zero_trade_timeframes) != set(order):
        raise ValueError("legacy Stage 3 repair decision lacks full all-zero-trade evidence")
    return {"decision": str(decision_path.resolve()), "decision_sha256": decision_sha,
            "verification": str(verification_path.resolve()),
            "verification_sha256": hashlib.sha256(verification_path.read_bytes()).hexdigest().upper()}


def authenticated_stage3_input_grid_repair(state: dict, cycle: str, ea: str) -> dict | None:
    """Bind a complete Stage 3 invalid-input grid rejection to its exact run."""
    journal_state = state.get("stage2_repair_journal") or {}
    if (state.get("stage2_repair_source") != "stage3_tester_journal"
            or journal_state.get("code") != "TESTER_INPUT_GRID_REJECTED"):
        return None
    stage3_handoff_path = ROOT / "state" / "handoffs" / "Stage3_v2.json"
    if not stage3_handoff_path.is_file():
        raise ValueError("Stage 3 input-grid repair handoff is missing")
    stage3_handoff = load(stage3_handoff_path)
    runtime_path = Path(str(stage3_handoff.get("runtime_manifest") or ""))
    expected_runtime = ROOT / "state" / "stage3_v2" / f"{cycle}.stage3.runtime.json"
    if runtime_path.resolve() != expected_runtime.resolve() or not runtime_path.is_file():
        raise ValueError("Stage 3 input-grid runtime is not the exact-cycle manifest")
    runtime = load(runtime_path)
    diagnostic = runtime.get("journal_diagnostic") or {}
    source = Path(str(state.get("candidate_source") or state.get("stage2_candidate_source") or ""))
    stage2_event_path = Path(str(stage3_handoff.get("stage2_event") or ""))
    stage3_ini = Path(str((stage3_handoff.get("ini_files") or {}).get("M15") or ""))
    log_path = Path(str(runtime.get("tester_log") or ""))
    journal_after = runtime.get("journal_after_run") or {}
    try:
        source_hash = stage2_source_recovery.sha256(source)
        from kcos_stage3_prepare_v2 import consumption_insensitive_event_sha256
        stage2_event_hash = consumption_insensitive_event_sha256(stage2_event_path)
        stage3_ini_hash = stage2_source_recovery.sha256(stage3_ini)
        stage3_handoff_hash = stage2_source_recovery.sha256(stage3_handoff_path)
        runtime_hash = stage2_source_recovery.sha256(runtime_path)
        with log_path.open("rb") as stream:
            stream.seek(int(runtime["tester_log_start_offset"]))
            log_segment_hash = hashlib.sha256(stream.read()).hexdigest().upper()
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise ValueError("Stage 3 input-grid evidence is incomplete or unreadable") from exc
    stage2_event = load(stage2_event_path)
    binding = runtime.get("input_schema_binding") or {}
    identity = (
        f"{ea}.ex5 on {stage3_handoff.get('symbol')},M15 from "
        f"{stage3_handoff.get('from_date')} 00:00 to {stage3_handoff.get('to_date')} 00:00"
    ).casefold()
    exact_identity = any(identity in str(line).casefold()
                         for line in diagnostic.get("run_identity_lines", []))
    all_passes_failed = any(
        re.search(r"\b(\d+)\s+passes\s+of\s+\1\s+failed\b", str(line), re.I)
        for line in diagnostic.get("error_lines", [])
    )
    if not (
        state.get("cycle_id") == cycle and state.get("current_ea") == ea
        and stage3_handoff.get("cycle_id") == cycle and stage3_handoff.get("ea") == ea
        and stage3_handoff.get("source")
        and Path(str(stage3_handoff.get("source"))).resolve() == source.resolve()
        and str(stage3_handoff.get("source_sha256") or "").upper() == source_hash
        and stage3_handoff.get("stage2_event_sha256") == stage2_event_hash
        and stage2_event.get("cycle_id") == cycle and stage2_event.get("stage") == 2
        and stage2_event.get("status") == "complete" and stage2_event.get("ea") == ea
        and str(stage2_event.get("mq5_sha256") or "").upper() == source_hash
        and stage3_handoff.get("ini_sha256", {}).get("M15") == stage3_ini_hash
        and runtime.get("cycle_id") == cycle and runtime.get("ea") == ea
        and runtime.get("timeframe") == "M15"
        and runtime.get("status") == "INFRASTRUCTURE_FAILURE"
        and runtime.get("failure_kind") == "TESTER_JOURNAL_REJECTED"
        and runtime.get("start_button_invoked") is True
        and binding.get("status") == "INPUTS_BOUND"
        and diagnostic.get("code") == "TESTER_INPUT_GRID_REJECTED"
        and diagnostic == journal_state and exact_identity and all_passes_failed
        and diagnostic.get("invalid_parameter_passes")
        and journal_after.get("sha256") == log_segment_hash
        and runtime.get("journal_reclassification", {}).get("handoff_sha256") == stage3_handoff_hash
    ):
        raise ValueError("Stage 3 input-grid evidence failed exact cycle, source, INI, run, or Journal authentication")
    return {
        "kind": "authenticated_stage3_input_grid_rejection",
        "cycle_id": cycle, "ea": ea, "timeframe": "M15",
        "runtime_manifest": str(runtime_path.resolve()), "runtime_manifest_sha256": runtime_hash,
        "stage3_handoff": str(stage3_handoff_path.resolve()), "stage3_handoff_sha256": stage3_handoff_hash,
        "stage2_event": str(stage2_event_path.resolve()), "stage2_event_sha256": stage2_event_hash,
        "source": str(source.resolve()), "source_sha256": source_hash,
        "tester_ini": str(stage3_ini.resolve()), "tester_ini_sha256": stage3_ini_hash,
        "tester_log": str(log_path.resolve()), "tester_log_start_offset": int(runtime["tester_log_start_offset"]),
        "tester_log_segment_sha256": log_segment_hash,
        "journal_diagnostic": diagnostic,
    }


def acquire_executor_lock(path: Path):
    """OS ownership covers generation as well as compile/verify/publish.

    Keep the byte-lock file: unlinking it would let peers lock different files.
    No clock-based expiry can evict a healthy long-running owner. Windows
    releases the byte lock when the handle/process closes, including a crash.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open('a+b')
    try:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b'0')
            handle.flush()
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return handle
    except OSError as exc:
        handle.close()
        if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
            return None
        raise


def release_executor_lock(handle):
    try:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    finally:
        handle.close()


def load(path: Path | str) -> dict:
    path = Path(path)
    return json.loads(path.read_text(encoding="utf-8-sig"))


def stage2_repair_loop_required(state: dict, cycle: str, ea_name: str,
                               candidate_source: Path, stage1_spec: Path,
                               zero_trade_evidence: dict | None = None,
                               recovery_archive: dict | None = None,
                               input_grid_evidence: dict | None = None) -> bool:
    """Enter source repair only for authenticated failures or Stage 3 repair evidence."""
    # Canonical cycle state stores paths as strings. Normalize at this boundary
    # because callers may pass those values through without wrapping them.
    # A candidate may have been generated moments earlier in this same
    # executor run. Prefer the refreshed canonical state value when the local
    # pre-build snapshot is empty, and fail with a useful message if neither
    # boundary supplied a path.
    candidate_source = candidate_source or state.get("candidate_source") or state.get("stage2_candidate_source")
    stage1_spec = stage1_spec or state.get("last_stage1_artifact")
    if not candidate_source or not stage1_spec:
        raise ValueError("Stage 2 repair gate requires the generated candidate source and authenticated Stage 1 spec")
    candidate_source = Path(candidate_source)
    stage1_spec = Path(stage1_spec)
    failure_value = state.get("stage2_failure_event") or state.get("stage2_invalid_event")
    # A pointer alone is not repair evidence: it may refer to another cycle,
    # or to an executor/infrastructure failure created by a failed retry. Only
    # enter the source-repair loop when a same-cycle event binds this exact
    # candidate/spec and records an actual compile/verification diagnosis.
    authenticated_failure = False
    if failure_value:
        failure_path = Path(failure_value)
        failure_event = load(failure_path) if failure_path.is_file() else {}
        source_binding = str(failure_event.get("source_sha256") or "").upper()
        spec_binding = str(failure_event.get("stage1_spec_sha256") or "").upper()
        try:
            source_matches = (Path(str(failure_event.get("source") or "")).resolve()
                              == candidate_source.resolve()
                              and source_binding == stage2_source_recovery.sha256(candidate_source))
            spec_matches = (Path(str(failure_event.get("stage1_spec") or "")).resolve()
                            == stage1_spec.resolve()
                            and spec_binding == stage2_source_recovery.sha256(stage1_spec))
        except (OSError, ValueError):
            source_matches = spec_matches = False
        failure_has_diagnosis = bool(
            failure_event.get("failed_checks")
            or failure_event.get("verification")
            or failure_event.get("compile_attempt")
            or failure_event.get("source_diagnosis")
        )
        authenticated_failure = (
            failure_event.get("schema_version") == 2
            and failure_event.get("cycle_id") == cycle
            and failure_event.get("stage") == 2
            and failure_event.get("status") == "failed"
            and failure_event.get("ea") == ea_name
            and source_matches and spec_matches and failure_has_diagnosis
        )
    return bool(authenticated_failure or zero_trade_evidence is not None
                or recovery_archive is not None or input_grid_evidence is not None)


def archive_stage3_dcc_source_repair(state: dict) -> dict | None:
    """Authenticate and archive the exact prior Stage 2 bundle before overwrite."""
    ticket_value = state.get("stage2_source_defect_ticket")
    event_value = state.get("stage2_source_defect_event")
    if not ticket_value and not event_value:
        return None
    if not ticket_value or not event_value:
        raise ValueError("incomplete Stage 3 DCC recovery ticket/event state")
    ticket_path, event_path = Path(ticket_value), Path(event_value)
    ticket = stage2_source_recovery.load(ticket_path)
    if ticket.get("cycle_id") != state.get("cycle_id"):
        # Canonical state can retain the prior cycle's consumed recovery
        # pointers after a terminal Stage 5 rotation. Ignore only a stale
        # pointer whose event is consumed and whose immutable archive still
        # validates; same-cycle tickets continue through full validation below.
        event = stage2_source_recovery.load(event_path)
        archive = state.get("stage2_source_defect_archive")
        if (event.get("cycle_id") != ticket.get("cycle_id")
                or event.get("ticket") != str(ticket_path.resolve())
                or event.get("ticket_sha256") != stage2_source_recovery.sha256(ticket_path)
                or event.get("consumed") is not True
                or event.get("consumed_action") != "dispatch_stage2"
                or not isinstance(archive, dict)):
            raise ValueError("stale Stage 2 recovery ticket lacks consumed archived provenance")
        stage2_source_recovery.validate_archive_receipt(ticket, ticket_path, archive)
        return None
    # A legacy immutable archive may lack ticket evidence added to the newer
    # ticket schema. Reconcile only from still-matching original bytes, by
    # writing a separate supplement; never edit its sealed base manifest.
    nonce = str(ticket["ticket_id"]).rsplit(":", 1)[-1]
    archive_dir = ROOT / "state" / "diagnostics" / "stage2_source_defect_rebuild" / str(ticket["cycle_id"]) / nonce
    if state.get("stage2_source_defect_archive"):
        state["stage2_source_defect_archive"] = stage2_source_recovery.extend_archive_for_missing_ticket_artifacts(
            ticket, ticket_path, state["stage2_source_defect_archive"])
    elif archive_dir.exists():
        receipt = stage2_source_recovery.recover_archive_receipt(
            ticket, ticket_path, archive_dir, allow_missing_ticket_artifacts=True)
        state["stage2_source_defect_archive"] = stage2_source_recovery.extend_archive_for_missing_ticket_artifacts(
            ticket, ticket_path, receipt)
    ticket, event = stage2_source_recovery.validate_ticket(ticket_path, event_path, state)
    claim_path = Path(state.get("stage2_source_defect_dispatch_claim") or "")
    if not claim_path.is_file() or stage2_source_recovery.sha256(claim_path) != state.get(
            "stage2_source_defect_dispatch_claim_sha256"):
        raise ValueError("Stage 2 recovery has no hash-bound supervisor dispatch claim")
    claim = load(claim_path)
    if claim.get("event_id") != event.get("event_id") or claim.get("action") != "dispatch_stage2":
        raise ValueError("Stage 2 recovery claim does not bind its one-shot event")
    if (state.get("current_stage") != "stage2_pending"
            or state.get("stage_status") != "stage2_repair_required"
            or state.get("next_action") != "dispatch_stage2"):
        raise ValueError("canonical state is outside the same-cycle Stage 2 repair hold")
    attempt = event.get("dispatch_attempt", 1)
    if type(attempt) is not int or not 1 <= attempt <= stage2_source_recovery.MAX_DISPATCH_ATTEMPTS:
        raise ValueError("Stage 2 recovery dispatch attempt is outside the bounded ticket chain")
    if attempt > 1 and event.get("parent_attempt_failed") is True:
        prior_attempt = state.get("stage2_source_defect_prior_attempt_archive") or {}
        prior_path = Path(prior_attempt.get("path") or "")
        if (prior_attempt.get("event_id") != event.get("parent_event_id")
                or not prior_path.is_file()
                or stage2_source_recovery.sha256(prior_path) != prior_attempt.get("sha256")):
            raise ValueError("retry dispatch lacks its immutable parent-attempt evidence bundle")
        stage2_source_recovery.validate_attempt_archive(
            prior_attempt, ticket_id=ticket["ticket_id"], cycle_id=ticket["cycle_id"],
            event_id=str(event.get("parent_event_id")), execution_id=prior_attempt.get("execution_id"))
    nonce = str(ticket["ticket_id"]).rsplit(":", 1)[-1]
    archive_dir = ROOT / "state" / "diagnostics" / "stage2_source_defect_rebuild" / str(ticket["cycle_id"]) / nonce
    if state.get("stage2_source_defect_archive"):
        receipt = state["stage2_source_defect_archive"]
        stage2_source_recovery.validate_archive_receipt(ticket, ticket_path, receipt)
    elif archive_dir.exists():
        # Reconcile a crash after exclusive archive creation but before the
        # receipt was committed. Partial or mismatched bundles stay held.
        receipt = stage2_source_recovery.recover_archive_receipt(ticket, ticket_path, archive_dir)
    else:
        archive = stage2_source_recovery.archive_stage2_bundle(
            ticket, ROOT / "state" / "diagnostics")
        receipt = {**archive, "ticket_id": ticket["ticket_id"],
                   "ticket_sha256": stage2_source_recovery.sha256(ticket_path)}
    state["stage2_source_defect_archive"] = receipt
    temp = STATE.with_suffix(STATE.suffix + ".tmp")
    temp.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    temp.replace(STATE)
    return receipt


def run(args: list[str]) -> None:
    result = subprocess.run([sys.executable, *args], cwd=ROOT, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError((result.stdout + "\n" + result.stderr).strip()[-4000:])


def repair_stale_generated_admission(handoff: Path, compile_path: Path,
                                     verification_path: Path, cycle: str) -> dict:
    """Use the transactional repair ledger before retrying a stale-admission prepare."""
    command = [str(REPAIR_LOOP), "--handoff", str(handoff), "--compile", str(compile_path),
               "--verification", str(verification_path)]
    result = subprocess.run([sys.executable, *command], cwd=ROOT, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError((result.stdout + "\n" + result.stderr).strip()[-4000:])
    try:
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        outcome_path = Path(payload["attempt_dir"]) / "outcome.json"
        outcome = load(outcome_path)
    except (IndexError, KeyError, ValueError, OSError, TypeError) as exc:
        raise RuntimeError("transactional stale-admission repair did not publish an authenticated outcome") from exc
    if (payload.get("outcome") != "REPAIRED" or outcome.get("outcome") != "REPAIRED"
            or outcome.get("passed") is not True or payload.get("episode_id") != outcome.get("episode_id")):
        raise RuntimeError("transactional stale-admission repair did not pass its verification gate")
    report = load(verification_path)
    if report.get("cycle_id") != cycle or report.get("stage") != 2 or report.get("passed") is not True:
        raise RuntimeError("transactional stale-admission repair lacks a passing current-cycle Stage 2 verifier")
    return {"episode_id": payload["episode_id"], "attempt_dir": str(outcome_path.parent),
            "verification": str(verification_path), "status": "REPAIRED"}


def repair_missing_trade_include_from_compile_evidence(cycle: str, ea_name: str,
                                                        candidate_source: str | None,
                                                        compile_path: Path,
                                                        spec_path: str | None) -> bool:
    """Rebuild only an authenticated model source with the diagnosed CTrade include defect."""
    if not candidate_source or not spec_path or not compile_path.is_file():
        return False
    source_path = Path(candidate_source)
    receipt_path = source_path.with_suffix(source_path.suffix + ".build.json")
    if not source_path.is_file() or not receipt_path.is_file():
        return False
    receipt = load(receipt_path)
    source_sha = hashlib.sha256(source_path.read_bytes()).hexdigest().upper()
    if (receipt.get("implementation_mode") != "model_generated_source_admitted"
            or receipt.get("cycle_id") != cycle or receipt.get("candidate_ea") != ea_name
            or str(receipt.get("sha256") or "").upper() != source_sha):
        return False
    source_text = source_path.read_text(encoding="utf-8-sig", errors="replace")
    if re.search(r"(?im)^\s*#\s*include\s*[<\"]Trade[/\\]Trade\.mqh[>\"]", source_text):
        return False
    history_path = compile_path.with_name(compile_path.name.replace(
        ".compile.latest.json", ".compile.json"))
    try:
        history = load(history_path if history_path.is_file() else compile_path)
    except Exception:
        return False
    attempts = history.get("attempts", []) if history.get("cycle_id") == cycle else []
    if not attempts:
        return False
    latest = attempts[-1]
    log_path = Path(latest.get("log", ""))
    if not log_path.is_file():
        return False
    log_bytes = log_path.read_bytes()
    log_encoding = "utf-16" if log_bytes.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    compile_log = log_bytes.decode(log_encoding, errors="replace")
    if not re.search(r"'CTrade'\s*-\s*unexpected token", compile_log, re.I):
        return False
    archive = ROOT / "state" / "diagnostics" / "stage2_source_repairs" / cycle / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + source_sha[:12])
    archive.mkdir(parents=True, exist_ok=False)
    source_path.replace(archive / source_path.name)
    receipt_path.replace(archive / receipt_path.name)
    run([str(BUILD), "--cycle-id", cycle, "--spec", spec_path, "--state", str(STATE),
         "--candidates-dir", str(CANDIDATES_DIR), "--output", str(source_path)])
    repaired_receipt = load(receipt_path)
    repaired_sha = hashlib.sha256(source_path.read_bytes()).hexdigest().upper()
    if (repaired_receipt.get("implementation_mode") != "model_generated_source_admitted"
            or repaired_receipt.get("cycle_id") != cycle
            or str(repaired_receipt.get("sha256") or "").upper() != repaired_sha
            or not re.search(r"(?im)^\s*#\s*include\s*[<\"]Trade[/\\]Trade\.mqh[>\"]",
                             source_path.read_text(encoding="utf-8-sig", errors="replace"))):
        raise RuntimeError("cron source rebuild did not produce an authenticated CTrade include")
    (archive / "repair.json").write_text(json.dumps({
        "cycle_id": cycle, "ea": ea_name, "kind": "model_source_rebuild_missing_trade_include",
        "before_sha256": source_sha, "after_sha256": repaired_sha,
        "compile_log": str(log_path), "archive": str(archive),
    }, indent=2) + "\n", encoding="utf-8")
    return True


def _failed_checks(cycle: str, verification_path: Path, compile_path: Path) -> tuple[list[str], str]:
    """Collect durable evidence even when an upstream subprocess exits early."""
    failed: list[str] = []
    kind = "STAGE2_EXECUTOR_FAILURE"
    if verification_path.is_file():
        try:
            verification = load(verification_path)
            failed = sorted(k for k, value in (verification.get("checks") or {}).items()
                            if value is False)
        except Exception:
            pass
    if not failed and compile_path.is_file():
        try:
            compile_result = load(compile_path)
            if compile_result.get("cycle_id") == cycle and compile_result.get("passed") is not True:
                failed = ["compile_clean"]
        except Exception:
            pass
    if "compile_clean" in failed:
        kind = "COMPILE_INTEGRITY_FAILURE"
    elif "every_order_path_margin_quantity_tp_safe" in failed:
        kind = "ORDER_PATH_SAFETY_FAILURE"
    elif "regime_parameters" in failed:
        kind = "REGIME_OPTIMIZATION_POLICY_VIOLATION"
    elif any(item in failed for item in ("approved_tester_config", "stage1_lineage", "canonical_name")):
        kind = "STAGE2_LINEAGE_OR_CONFIGURATION_FAILURE"
    elif failed:
        kind = "STAGE2_VERIFICATION_FAILURE"
    return failed, kind


def authenticated_source_diagnosis(state: dict, detail: str) -> dict | None:
    """Use a child's exact held artifact, never a guessed latest directory."""
    paths = re.findall(r"^KCOS_SOURCE_HOLD_ARTIFACT=(.+)$", detail, re.M)
    hashes = re.findall(r"^KCOS_SOURCE_HOLD_SHA256=([A-F0-9]{64})$", detail, re.M)
    if len(paths) != 1 or len(hashes) != 1:
        return None
    try:
        path = Path(paths[0].strip()).resolve()
        expected = (ROOT / 'state/candidates/.generation' / re.sub(r'[^A-Za-z0-9_.-]', '_', state['cycle_id'])).resolve()
        path.relative_to(expected)
        if path.name != 'hold.json' or len(path.relative_to(expected).parts) != 2:
            return None
        artifact_hash = hashlib.sha256(path.read_bytes()).hexdigest().upper()
        if artifact_hash != hashes[0]:
            return None
        hold = load(path)
        if not isinstance(hold, dict):
            return None
        spec = Path(state['last_stage1_artifact']).resolve()
        event = Path(state['last_stage1_event']).resolve()
        include = mt5_data_root() / 'MQL5/Include/minato_base_score_v2.mqh'
        approval = load(ROOT / 'state/contracts/minato_base_score_approved_v2.json')
        if not isinstance(approval, dict):
            return None
        include_hash = hashlib.sha256(include.read_bytes()).hexdigest().upper()
        stage1_event = load(event)
        for key in ('consumed', 'consumed_utc', 'consumed_action'):
            stage1_event.pop(key, None)
        stage1_event_hash = hashlib.sha256(json.dumps(
            stage1_event, sort_keys=True, separators=(',', ':'), ensure_ascii=False
        ).encode('utf-8')).hexdigest().upper()
        if (hold.get('cycle_id') != state['cycle_id'] or hold.get('ea') != state.get('current_ea')
                or Path(hold.get('source_spec', '')).resolve() != spec
                or hold.get('source_spec_sha256') != hashlib.sha256(spec.read_bytes()).hexdigest().upper()
                or hold.get('stage1_event_sha256') != stage1_event_hash
                or hold.get('canonical_minato_sha256') != include_hash
                or approval.get('approved') is not True or approval.get('score_version') != 'MINATO_BASE_SCORE_V2.0'
                or approval.get('sha256', approval.get('score_include_sha256')) != include_hash
                or hold.get('phase') != 'proposal' or hold.get('status') != 'MODEL_SOURCE_HOLD'):
            return None
        diagnoses = hold.get('proposal_diagnoses')
        common_kinds = {'RESPONSE_FORMAT', 'ORDER_REFUSAL_FLOW', 'CLOSED_BAR_FLOW',
                        'LIVE_MARGIN_FLOW', 'EVIDENCE_BINDING', 'RESPONSE_SCHEMA', 'CANONICAL_HOOK',
                        'IMPLEMENTATION_REFUSAL', 'SOURCE_ADMISSION', 'INPUT_USAGE', 'STRATEGY_INPUT_LOCK',
                        'ORDER_MARGIN_NOTIONAL_HELPER'}
        def valid_normalization_diagnosis(item: dict) -> bool:
            diagnostic = item.get('normalization_diagnosis')
            if not isinstance(diagnostic, dict):
                return False
            if (diagnostic.get('code') != 'NORMALIZATION_NOT_RECOGNIZED'
                    or diagnostic.get('side') not in {'BUY', 'SELL'}
                    or not isinstance(diagnostic.get('volume_variable'), str)
                    or re.fullmatch(r'[A-Za-z_]\w*', diagnostic['volume_variable']) is None
                    or type(diagnostic.get('source_line_start')) is not int
                    or type(diagnostic.get('source_line_end')) is not int
                    or diagnostic['source_line_start'] < 1
                    or diagnostic['source_line_end'] < diagnostic['source_line_start']
                    or diagnostic['source_line_end'] - diagnostic['source_line_start'] > 1600
                    or not isinstance(diagnostic.get('quantity_to_margin_sha256'), str)
                    or re.fullmatch(r'[A-F0-9]{64}', diagnostic['quantity_to_margin_sha256']) is None
                    or type(diagnostic.get('helper_authenticated')) is not bool
                    or type(diagnostic.get('normalizer_call_count')) is not int
                    or diagnostic['normalizer_call_count'] < 0
                    or type(diagnostic.get('matching_margin_guard_found')) is not bool
                    or diagnostic.get('span_kind') not in {
                        'quantity_to_margin_refusal', 'quantity_to_order_fallback'}):
                return False
            call_start, call_end = diagnostic.get('normalizer_line_start'), diagnostic.get('normalizer_line_end')
            if (call_start is None) != (call_end is None):
                return False
            if call_start is not None and (
                    type(call_start) is not int or type(call_end) is not int
                    or call_start < 1 or call_end < call_start
                    or diagnostic.get('matching_margin_guard_found') is not True
                    or call_start < diagnostic['source_line_start']):
                return False
            if diagnostic.get('matching_margin_guard_found') is True:
                if diagnostic.get('span_kind') != 'quantity_to_margin_refusal':
                    return False
            elif (diagnostic.get('span_kind') != 'quantity_to_order_fallback'
                  or call_start is not None
                  or (diagnostic.get('helper_authenticated') is True
                      and diagnostic.get('normalizer_call_count') == 1)):
                return False
            if (diagnostic.get('matching_margin_guard_found') is True
                    and diagnostic.get('helper_authenticated') is True
                    and diagnostic.get('normalizer_call_count') == 1
                    and call_start is None):
                return False
            if call_start is not None and (
                    diagnostic.get('helper_authenticated') is not True
                    or diagnostic.get('normalizer_call_count') != 1):
                return False
            detail = item.get('failed_check')
            prefix = 'NORMALIZATION_NOT_RECOGNIZED '
            if not isinstance(detail, str) or not detail.startswith(prefix):
                return False
            try:
                if json.loads(detail[len(prefix):]) != diagnostic:
                    return False
            except (json.JSONDecodeError, TypeError):
                return False
            expected_fingerprint = hashlib.sha256(
                ('NORMALIZATION_NOT_RECOGNIZED\n' + diagnostic['quantity_to_margin_sha256']).encode('utf-8')
            ).hexdigest().upper()
            return item.get('failure_fingerprint') == expected_fingerprint

        normalization_attempts_valid = False
        if isinstance(diagnoses, list):
            normalization_attempts_valid = True
            previous_fingerprint = None
            previous_count = 0
            for item in diagnoses:
                if not isinstance(item, dict) or item.get('failure_kind') != 'NORMALIZATION_NOT_RECOGNIZED':
                    previous_fingerprint, previous_count = None, 0
                    continue
                if not valid_normalization_diagnosis(item):
                    normalization_attempts_valid = False
                    break
                fingerprint = item.get('failure_fingerprint')
                expected_count = previous_count + 1 if fingerprint == previous_fingerprint else 1
                if item.get('same_failure_count') != expected_count or expected_count >= 2:
                    normalization_attempts_valid = False
                    break
                previous_fingerprint, previous_count = fingerprint, expected_count
        ordinary_budget_hold = (
            hold.get('proposal_limit') == 4 and len(diagnoses) == 4
            and hold.get('proposal_attempts') == 4
            and hold.get('stop_reason') == 'PROPOSAL_BUDGET_EXHAUSTED'
            and normalization_attempts_valid
            and all(isinstance(item, dict) and item.get('failure_kind') in common_kinds
                    | {'NORMALIZATION_NOT_RECOGNIZED'} for item in diagnoses)) if isinstance(diagnoses, list) else False
        normalization_no_progress_hold = (
            hold.get('proposal_limit') == 2 and len(diagnoses) == 2
            and hold.get('proposal_attempts') == 2
            and hold.get('stop_reason') == 'REPEATED_NORMALIZATION_NO_PROGRESS'
            and all(isinstance(item, dict)
                    and item.get('failure_kind') == 'NORMALIZATION_NOT_RECOGNIZED'
                    and item.get('repair_scope') == 'source_proposal_only'
                    and isinstance(item.get('normalization_diagnosis'), dict)
                    and item['normalization_diagnosis'].get('code') == 'NORMALIZATION_NOT_RECOGNIZED'
                    for item in diagnoses)
            and diagnoses[0].get('failure_fingerprint') == diagnoses[1].get('failure_fingerprint')
            and diagnoses[0]['normalization_diagnosis'].get('quantity_to_margin_sha256')
                == diagnoses[1]['normalization_diagnosis'].get('quantity_to_margin_sha256')) if isinstance(diagnoses, list) and len(diagnoses) == 2 else False
        if (not isinstance(diagnoses, list) or not diagnoses
                or not all(isinstance(item, dict) and item.get('repair_scope') == 'source_proposal_only'
                           for item in diagnoses)
                or not (ordinary_budget_hold or normalization_no_progress_hold)):
            return None
        return {'artifact':str(path), 'sha256':artifact_hash, 'diagnoses':diagnoses,
                'failure_kind':'MODEL_SOURCE_' + diagnoses[-1]['failure_kind'],
                'proposal_budget_exhausted': ordinary_budget_hold,
                'proposal_stop_reason': hold.get('stop_reason')}
    except (OSError, ValueError, TypeError, KeyError):
        return None


def _stage2_zero_trade_repair_context(identity_state: dict, detail: str) -> dict | None:
    """Authenticate one no-change repair result and its persistent proposal budget."""
    cycle = str(identity_state.get("cycle_id") or "")
    candidate = str(identity_state.get("candidate_ea") or identity_state.get("current_ea") or "")
    if not cycle or not candidate:
        return None
    try:
        zero_trade = authenticated_stage3_zero_trade_repair(identity_state, cycle, candidate)
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None
    if zero_trade is None:
        return None
    records = []
    for line in detail.splitlines():
        try:
            value = json.loads(line)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            records.append(value)
    outcome = next((item for item in records if item.get("outcome") == "REPAIR_REQUIRED_NO_CHANGE"), None)
    attempt_dir_value = (outcome or {}).get("attempt_dir")
    episode_id = str((outcome or {}).get("episode_id") or "")
    if not attempt_dir_value or not episode_id:
        return None
    try:
        tx_root = (ROOT / "state" / "diagnostics" / "stage2_repair_transactions").resolve()
        attempt_dir = Path(attempt_dir_value).resolve()
        attempt_dir.relative_to(tx_root)
        if attempt_dir.parent.name != episode_id or not (attempt_dir / "outcome.json").is_file():
            return None
        attempt_outcome = load(attempt_dir / "outcome.json")
        report_path = attempt_dir / "repair-loop-report.json"
        report = load(report_path)
        source_value = identity_state.get("candidate_source") or identity_state.get("source")
        source_path = Path(source_value) if source_value else None
        spec_value = identity_state.get("last_stage1_artifact") or identity_state.get("stage1_spec")
        spec_path = Path(spec_value) if spec_value else None
        if (attempt_outcome.get("outcome") != "REPAIR_REQUIRED_NO_CHANGE"
                or attempt_outcome.get("episode_id") != episode_id
                or report.get("cycle_id") != cycle or report.get("ea") != candidate
                or source_path is None or not source_path.is_file()
                or spec_path is None or not spec_path.is_file()
                or str(report.get("baseline_source_sha256") or "").upper() not in {
                    "", hashlib.sha256(source_path.read_bytes()).hexdigest().upper()}):
            return None
        ledger_path = ROOT / "state" / "ledgers" / "stage2_repair_episodes.sqlite3"
        count = stage2_repair_loop.episode_proposal_count(ledger_path, episode_id)
        identity = stage2_repair_loop.episode_identity(
            load(ROOT / "state" / "handoffs" / "Stage2_v2.json"), source_path,
            Path(identity_state.get("last_stage2_verification") or ""), state_path=STATE)
        if identity.get("episode_id") != episode_id:
            return None
        return {"cycle_id": cycle, "candidate_ea": candidate,
                "episode_id": episode_id, "proposal_count": count,
                "attempt_dir": str(attempt_dir), "report": str(report_path),
                "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest().upper(),
                "outcome_sha256": hashlib.sha256((attempt_dir / "outcome.json").read_bytes()).hexdigest().upper(),
                "source": str(source_path.resolve()),
                "source_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest().upper(),
                "stage1_spec": str(spec_path.resolve()),
                "stage1_spec_sha256": hashlib.sha256(spec_path.read_bytes()).hexdigest().upper(),
                "stage3_zero_trade_evidence": zero_trade}
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None


def _record_stage2_zero_trade_exhaustion(context: dict) -> dict | None:
    """Write immutable evidence before authorizing candidate-local rotation."""
    if context.get("proposal_count") != stage2_repair_loop.MAX_PROPOSALS:
        return None
    episode_id = context["episode_id"]
    episode_root = ROOT / "state" / "diagnostics" / "stage2_repair_transactions" / episode_id
    attempts = []
    for path in sorted(episode_root.iterdir()):
        if not path.is_dir():
            continue
        outcome_path, report_path = path / "outcome.json", path / "repair-loop-report.json"
        if not outcome_path.is_file() or not report_path.is_file():
            continue
        outcome = load(outcome_path)
        report = load(report_path)
        if (outcome.get("episode_id") != episode_id
                or outcome.get("outcome") != "REPAIR_REQUIRED_NO_CHANGE"
                or report.get("cycle_id") != context["cycle_id"]
                or report.get("ea") != context["candidate_ea"]):
            return None
        attempts.append({"attempt_dir": str(path.resolve()),
                         "outcome": str(outcome_path.resolve()),
                         "outcome_sha256": hashlib.sha256(outcome_path.read_bytes()).hexdigest().upper(),
                         "report": str(report_path.resolve()),
                         "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest().upper()})
    if len(attempts) != stage2_repair_loop.MAX_PROPOSALS:
        return None
    receipt = {"schema_version": 1, "record_type": "stage2_zero_trade_repair_budget_exhausted",
               "cycle_id": context["cycle_id"], "candidate_ea": context["candidate_ea"],
               "episode_id": episode_id, "proposal_limit": stage2_repair_loop.MAX_PROPOSALS,
               "proposal_attempts": context["proposal_count"],
               "source": context["source"], "source_sha256": context["source_sha256"],
               "stage1_spec": context["stage1_spec"],
               "stage1_spec_sha256": context["stage1_spec_sha256"],
               "stage3_zero_trade_evidence": context["stage3_zero_trade_evidence"],
               "attempts": attempts, "recorded_utc": datetime.now(timezone.utc).isoformat()}
    payload = (json.dumps(receipt, indent=2) + "\n").encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest().upper()
    receipt_path = ROOT / "state" / "diagnostics" / "stage2_failures" / (
        f"{receipt['cycle_id']}.zero-trade-repair-exhaustion.{digest[:16]}.json")
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    if not receipt_path.exists():
        receipt_path.write_bytes(payload)
    return {"path": str(receipt_path.resolve()),
            "sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest().upper(),
            "episode_id": episode_id, "proposal_attempts": context["proposal_count"],
            "proposal_limit": stage2_repair_loop.MAX_PROPOSALS}


def _record_stage2_compile_repair_exhaustion(state: dict, source_path: Path | None,
                                             spec_path: Path | None) -> dict | None:
    """Record four same-cycle model repair failures before candidate rotation."""
    cycle = str(state.get("cycle_id") or "")
    candidate = str(state.get("candidate_ea") or state.get("current_ea") or "")
    if (not cycle or not candidate or source_path is None or spec_path is None
            or not source_path.is_file() or not spec_path.is_file()):
        return None
    tx_root = ROOT / "state" / "diagnostics" / "stage2_repair_transactions"
    matches = []
    try:
        for report_path in tx_root.glob("*/*/repair-loop-report.json"):
            report = load(report_path)
            report_attempts = report.get("attempts") if isinstance(report.get("attempts"), list) else []
            baseline_record = next((item for item in report_attempts
                                    if isinstance(item, dict) and item.get("baseline") is True), {})
            baseline_sha = str(report.get("baseline_source_sha256")
                               or baseline_record.get("before_sha256") or "").upper()
            if (report.get("cycle_id") != cycle or report.get("ea") != candidate
                    or report.get("max_attempts") != stage2_repair_loop.MAX_PROPOSALS
                    or baseline_sha != hashlib.sha256(source_path.read_bytes()).hexdigest().upper()):
                continue
            attempts = report_attempts
            proposals = [item for item in attempts if isinstance(item, dict)
                         and type(item.get("attempt")) is int and item["attempt"] > 0
                         and (item.get("ai") or {}).get("status") in {"PROPOSED", "INVALID_RESPONSE"}]
            if (not proposals or any(not item.get("adapter") or "deterministic" not in item
                                     for item in proposals)):
                continue
            attempt_dir = report_path.parent
            outcome_path, journal_path = attempt_dir / "outcome.json", attempt_dir / "transaction-journal.json"
            if not outcome_path.is_file() or not journal_path.is_file():
                continue
            outcome, journal = load(outcome_path), load(journal_path)
            if (outcome.get("outcome") not in {"REJECTED", "EXHAUSTED"}
                    or journal.get("status") != "ROLLED_BACK"
                    or outcome.get("episode_id") != attempt_dir.parent.name):
                continue
            proposals_dir = attempt_dir / "proposals"
            proposal_files = sorted(path for path in proposals_dir.glob("proposal-*.json")
                                    if re.fullmatch(r"proposal-\d+\.json", path.name))
            if len(proposal_files) != sum((item.get("ai") or {}).get("status") == "PROPOSED"
                                          for item in proposals):
                continue
            for proposal_path in proposal_files:
                proposal_doc = load(proposal_path)
                if not isinstance(proposal_doc.get("response"), dict):
                    break
            else:
                matches.append((report_path, outcome_path, attempt_dir, outcome["episode_id"], len(proposals)))
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None
    if not matches:
        return None
    # Aggregate every same-episode transaction: a bounded continuation may
    # consume the remaining slots after an earlier one-proposal run.
    episode_ids = sorted({item[3] for item in matches})
    ledger_path = ROOT / "state" / "ledgers" / "stage2_repair_episodes.sqlite3"
    if (sum(item[4] for item in matches) != stage2_repair_loop.MAX_PROPOSALS
            or any(stage2_repair_loop.episode_proposal_count(ledger_path, episode_id)
                   < sum(item[4] for item in matches if item[3] == episode_id)
                   or stage2_repair_loop.episode_proposal_count(ledger_path, episode_id)
                   > stage2_repair_loop.MAX_PROPOSALS for episode_id in episode_ids)):
        return None
    transactions = [{"episode_id": item[3], "attempt_dir": str(item[2].resolve()),
                     "report": str(item[0].resolve()),
                     "report_sha256": hashlib.sha256(item[0].read_bytes()).hexdigest().upper(),
                     "outcome": str(item[1].resolve()),
                     "outcome_sha256": hashlib.sha256(item[1].read_bytes()).hexdigest().upper(),
                     "proposal_attempts": item[4]} for item in matches]
    receipt = {"schema_version": 1, "record_type": "stage2_compile_repair_budget_exhausted",
               "cycle_id": cycle, "candidate_ea": candidate, "episode_ids": episode_ids,
               "proposal_limit": stage2_repair_loop.MAX_PROPOSALS,
               "proposal_attempts": stage2_repair_loop.MAX_PROPOSALS,
               "source": str(source_path.resolve()),
               "source_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest().upper(),
               "stage1_spec": str(spec_path.resolve()),
               "stage1_spec_sha256": hashlib.sha256(spec_path.read_bytes()).hexdigest().upper(),
               "transactions": transactions,
               "recorded_utc": datetime.now(timezone.utc).isoformat()}
    payload = (json.dumps(receipt, indent=2) + "\n").encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest().upper()
    receipt_path = ROOT / "state" / "diagnostics" / "stage2_failures" / (
        f"{cycle}.compile-repair-exhaustion.{digest[:16]}.json")
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    if not receipt_path.exists():
        receipt_path.write_bytes(payload)
    return {"path": str(receipt_path.resolve()),
            "sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest().upper(),
            "episode_ids": episode_ids, "proposal_attempts": stage2_repair_loop.MAX_PROPOSALS,
            "proposal_limit": stage2_repair_loop.MAX_PROPOSALS}


def record_failure(state: dict, exc: Exception, traceback_text: str | None = None) -> Path:
    cycle = state["cycle_id"]
    state_path = ROOT / "state" / "kcos_cycle_state_v2.json"
    identity_state = dict(state)
    try:
        latest_state = load(state_path)
        if latest_state.get("cycle_id") == cycle:
            identity_state = {**identity_state, **latest_state}
    except (OSError, ValueError, TypeError):
        pass
    source_value = identity_state.get("candidate_source") or identity_state.get("source")
    source_path = Path(source_value) if source_value else None
    spec_value = identity_state.get("last_stage1_artifact") or identity_state.get("stage1_spec")
    spec_path = Path(spec_value) if spec_value else None
    def artifact_sha(path: Path | None) -> str | None:
        try:
            return hashlib.sha256(path.read_bytes()).hexdigest().upper() if path is not None and path.is_file() else None
        except OSError:
            return None
    source_sha = artifact_sha(source_path)
    spec_sha = artifact_sha(spec_path)
    candidate_ea = identity_state.get("candidate_ea") or identity_state.get("current_ea")
    events = ROOT / "state" / "events"
    events.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    detail = str(exc)[-4000:]
    verification_path = ROOT / "state" / "verification" / f"{cycle}.stage2.verification.json"
    compile_path = ROOT / "state" / "attempts" / f"{cycle}.stage2.compile.latest.json"
    failed_checks, failure_kind = _failed_checks(cycle, verification_path, compile_path)
    missing_domains = sorted(set(re.findall(
        r"source input ([A-Za-z_][A-Za-z0-9_]*) has no declared optimization domain",
        detail,
    )))
    research_path = None
    if missing_domains:
        failure_kind = "INPUT_DOMAIN_RESEARCH_REQUIRED"
        research_dir = ROOT / "state" / "research" / "input_domains"
        research_dir.mkdir(parents=True, exist_ok=True)
        research_path = research_dir / f"{cycle}.json"
        research_path.write_text(json.dumps({
            "schema_version": 1,
            "cycle_id": cycle,
            "ea": candidate_ea,
            "source": str(source_path.resolve()) if source_path and source_path.is_file() else source_value,
            "source_sha256": source_sha,
            "inputs": missing_domains,
            "status": "pending_web_research",
            "required_evidence": ["primary_or_authoritative_sources", "bounded_range", "step", "rationale"],
            "next_action": "research_input_domains_before_resume",
            "created_utc": now,
        }, indent=2), encoding="utf-8")
    if not missing_domains and not failed_checks and any(token in detail.casefold() for token in (
        "tester_config", "approved range", "source domain", "declaration-only",
        "cycle mismatch", "identity mismatch", "completion event", "build receipt",
        "source admission", "source-admission receipt",
    )):
        failure_kind = "STAGE2_LINEAGE_OR_CONFIGURATION_FAILURE"
    source_diagnosis = authenticated_source_diagnosis(identity_state, detail)
    source_budget_exhausted = bool(
        source_diagnosis and source_diagnosis.get('proposal_budget_exhausted') is True
    )
    source_hold_terminal = source_budget_exhausted and not missing_domains and not failed_checks
    if source_diagnosis is not None and not missing_domains and not failed_checks:
        failure_kind = ('MODEL_SOURCE_PROPOSAL_BUDGET_EXHAUSTED' if source_hold_terminal
                        else source_diagnosis['failure_kind'])
        failed_checks = [source_diagnosis['diagnoses'][-1]['failure_kind']]
    zero_trade_repair = _stage2_zero_trade_repair_context(identity_state, detail)
    zero_trade_exhaustion = None
    if (zero_trade_repair is not None
            and 0 < zero_trade_repair["proposal_count"] <= stage2_repair_loop.MAX_PROPOSALS):
        if zero_trade_repair["proposal_count"] == stage2_repair_loop.MAX_PROPOSALS:
            zero_trade_exhaustion = _record_stage2_zero_trade_exhaustion(zero_trade_repair)
        if zero_trade_exhaustion is not None:
            failure_kind = "STAGE2_ZERO_TRADE_REPAIR_BUDGET_EXHAUSTED"
            failed_checks = ["stage3_zero_trade_repair_exhausted"]
        elif zero_trade_repair["proposal_count"] < stage2_repair_loop.MAX_PROPOSALS:
            failure_kind = "STAGE2_ZERO_TRADE_REPAIR_FAILURE"
            failed_checks = ["stage3_zero_trade_repair_no_change"]
    compile_repair_exhaustion = None
    if failure_kind == "COMPILE_INTEGRITY_FAILURE" and failed_checks == ["compile_clean"]:
        compile_repair_exhaustion = _record_stage2_compile_repair_exhaustion(
            identity_state, source_path, spec_path)
        if compile_repair_exhaustion is not None:
            failure_kind = "STAGE2_COMPILE_REPAIR_BUDGET_EXHAUSTED"
            failed_checks = ["compile_repair_proposals_exhausted"]
    # Only an authenticated, exact-cycle four-proposal hold makes this
    # candidate terminal. Other executor, compile, lineage, and verifier
    # failures remain repair holds and are never rotation eligible.
    rotation_eligible = (source_hold_terminal or zero_trade_exhaustion is not None
                         or compile_repair_exhaustion is not None)
    failure = {
        "schema_version": 2,
        "cycle_id": cycle,
        "event_id": f"{cycle}:stage2:failed",
        "stage": 2,
        "status": "failed",
        "ea": candidate_ea,
        "candidate_ea": candidate_ea,
        "source": str(source_path.resolve()) if source_path and source_path.is_file() else source_value,
        "source_sha256": source_sha,
        "stage1_spec": str(spec_path.resolve()) if spec_path and spec_path.is_file() else spec_value,
        "stage1_spec_sha256": spec_sha,
        "source_identity_available": bool(candidate_ea and source_sha and spec_sha),
        "failure_kind": failure_kind,
        "error": detail,
        "verification": str(verification_path.resolve()) if verification_path.is_file() else None,
        "compile_attempt": str(compile_path.resolve()) if compile_path.is_file() else None,
        "failed_checks": failed_checks,
        "source_diagnosis": source_diagnosis,
        "stage2_zero_trade_repair": zero_trade_repair,
        "stage2_zero_trade_repair_exhaustion": zero_trade_exhaustion,
        "stage2_compile_repair_exhaustion": compile_repair_exhaustion,
        "recovery_action": ("research_input_domain" if missing_domains else
                            "rotate_candidate" if rotation_eligible else "repair_stage2_failure"),
        "recovery_policy": ("rotate only after four authenticated compile-repair model failures in the same Stage 2 episode; preserve all four proposal records, rollback transaction, and receipt"
                            if compile_repair_exhaustion is not None else
                            "rotate only after four authenticated no-change proposals in the same Stage 2 zero-trade repair episode; preserve all four transaction bundles and receipt"
                            if zero_trade_exhaustion is not None else
                            "retry the authenticated same-cycle zero-trade repair while its four-proposal Stage 2 budget remains; record each failure"
                            if zero_trade_repair is not None else
                            "rotate only after the authenticated four-proposal source budget is exhausted; record each rejected proposal and the rotation decision; do not admit the held source"
                            if rotation_eligible else
                            "research missing domains; otherwise preserve evidence and repair/hold Stage 2. Generic executor failures do not authorize rotation."),
        "research_request": str(research_path.resolve()) if research_path else None,
        "error_sha256": hashlib.sha256(detail.encode()).hexdigest().upper(),
        "exception_type": type(exc).__name__,
        "traceback": traceback_text[-12000:] if traceback_text else None,
        "failed_utc": now,
        "rotation_eligible": rotation_eligible,
    }
    path = events / f"{cycle}.stage2.failed.json"
    if path.is_file():
        history_dir = ROOT / "state" / "diagnostics" / "stage2_failures"
        history_dir.mkdir(parents=True, exist_ok=True)
        previous_sha = hashlib.sha256(path.read_bytes()).hexdigest().upper()[:12]
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        archive = history_dir / f"{cycle}.stage2.failed.{stamp}.{previous_sha}.json"
        shutil.copy2(path, archive)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(failure, indent=2), encoding="utf-8")
    tmp.replace(path)
    if source_budget_exhausted and source_diagnosis is not None:
        try:
            hold_path = Path(str(source_diagnosis.get("artifact") or ""))
            hold = load(hold_path) if hold_path.is_file() else {}
            proposal_rows = list(source_diagnosis.get("diagnoses") or [])
            attempt_evidence = []
            attempt_root = hold_path.parent
            for i, row in enumerate(proposal_rows, 1):
                rejection_path = attempt_root / f"proposal-rejection-{i}.json"
                rejection = {}
                if rejection_path.is_file():
                    try:
                        rejection = load(rejection_path)
                    except (OSError, ValueError, TypeError):
                        rejection = {}
                if not isinstance(rejection, dict):
                    rejection = {}
                raw_path = None
                raw_hash = None
                raw_value = rejection.get("raw_proposal")
                if isinstance(raw_value, str) and raw_value:
                    candidate_raw = Path(raw_value)
                    if not candidate_raw.is_absolute():
                        candidate_raw = attempt_root / candidate_raw
                    try:
                        candidate_raw = candidate_raw.resolve()
                        candidate_raw.relative_to(attempt_root.resolve())
                        if candidate_raw.is_file():
                            raw_path = str(candidate_raw)
                            raw_hash = hashlib.sha256(candidate_raw.read_bytes()).hexdigest().upper()
                    except (OSError, ValueError):
                        pass
                attempt_evidence.append({
                    "attempt": i,
                    "rejection_reason": rejection.get("reason") or row.get("failed_check"),
                    "rejection_artifact": str(rejection_path.resolve()) if rejection_path.is_file() else None,
                    "rejection_artifact_sha256": (hashlib.sha256(rejection_path.read_bytes()).hexdigest().upper()
                                                   if rejection_path.is_file() else None),
                    "raw_proposal_artifact": raw_path,
                    "raw_proposal_artifact_sha256": raw_hash,
                })
            audit_record = {
                "schema_version": 1,
                "record_type": "stage2_source_proposal_exhaustion",
                "cycle_id": cycle,
                "candidate_ea": candidate_ea,
                "stage1_spec_sha256": spec_sha,
                "failure_event": str(path.resolve()),
                "failure_event_sha256_at_recording": hashlib.sha256(path.read_bytes()).hexdigest().upper(),
                "hold_artifact": str(hold_path.resolve()) if hold_path.is_file() else None,
                "hold_artifact_sha256": source_diagnosis.get("sha256"),
                "proposal_limit": hold.get("proposal_limit"),
                "proposal_attempts": len(proposal_rows),
                "proposal_failures": [
                    {"attempt": i, "failure_kind": row.get("failure_kind"),
                     "failure_fingerprint": row.get("failure_fingerprint"),
                     "proposed_source_sha256": row.get("proposed_source_sha256"),
                     "same_failure_count": row.get("same_failure_count")}
                    for i, row in enumerate(proposal_rows, 1)
                ],
                "proposal_attempt_evidence": attempt_evidence,
                "rotation_eligible": rotation_eligible,
                "recovery_action": failure["recovery_action"],
                "recorded_utc": now,
            }
            audit_dir = ROOT / "state" / "diagnostics" / "stage2_failures"
            audit_dir.mkdir(parents=True, exist_ok=True)
            audit_hash = hashlib.sha256(json.dumps(audit_record, sort_keys=True).encode("utf-8")).hexdigest().upper()
            audit_path = audit_dir / f"{cycle}.source-proposal-exhaustion.{audit_hash[:16]}.json"
            if not audit_path.exists():
                audit_tmp = audit_path.with_suffix(audit_path.suffix + ".tmp")
                audit_tmp.write_text(json.dumps(audit_record, indent=2), encoding="utf-8")
                audit_tmp.replace(audit_path)
        except Exception:
            # The canonical Stage 2 failure event is already durable; an
            # auxiliary audit receipt must not suppress supervisor state updates.
            pass
    live = load(state_path)
    if live.get("cycle_id") != cycle:
        return path
    live.update({
        "current_stage": "stage2_pending",
        "stage_status": "stage2_research_required" if missing_domains else "stage2_repair_required",
        "next_action": "research_input_domains" if missing_domains else "repair_stage2_failure",
        "hold_after_stage": 2,
        "stage2_failure_event": str(path.resolve()),
        "stage2_repair_reason": detail,
        "stage2_research_request": str(research_path.resolve()) if research_path else None,
        "updated_utc": now,
    })
    tmp_state = state_path.with_suffix(state_path.suffix + ".tmp")
    tmp_state.write_text(json.dumps(live, indent=2), encoding="utf-8")
    tmp_state.replace(state_path)
    return path


def prior_stage2_source_proposal_revalidates(state: dict) -> bool:
    """Reopen a budget-exhausted source hold only if an exact prior proposal now passes every gate."""
    cycle = state.get("cycle_id")
    ea = state.get("current_ea") or state.get("candidate_ea")
    spec_path = Path(state.get("last_stage1_artifact") or "")
    if (state.get("current_stage") not in {"stage2_pending", "stage2_running"}
            or not cycle or not ea or not spec_path.is_file()):
        return False
    try:
        import kcos_stage2_generate_source_v2 as source_builder
        from kcos_input_contract_v2 import discover_inputs
        from kcos_tester_config_v2 import resolve_input_spec, stage2_spec_from_spec

        authenticated_state, spec, _contract, event_path, minato = source_builder.authenticate(
            spec_path, STATE, cycle, ea, CANDIDATES_DIR)
        minato_text = minato.read_bytes().decode("utf-8-sig")
        shared = discover_inputs({str(minato.resolve()): minato_text})
        spec, input_resolution = resolve_input_spec(
            spec, shared_sources={str(minato.resolve()): minato_text})
        spec = stage2_spec_from_spec(spec)
        input_resolution["kcos_runtime_policy"] = {
            "timing_controls": spec.get("stage2_runtime_policy", {}).get("timing_controls", {})
        }
        strategy_lock = source_builder.strategy_input_lock_manifest(spec)

        def json_sha(value: dict) -> str:
            payload = (json.dumps(value, indent=2) + "\n").encode("utf-8")
            return hashlib.sha256(payload).hexdigest().upper()

        bound = {
            "cycle_id": cycle,
            "ea": ea,
            "source_spec": str(spec_path.resolve()),
            "source_spec_sha256": source_builder.sha(spec_path),
            "stage1_event_sha256": source_builder.stage1_event_lineage_sha256(event_path),
            "canonical_minato_sha256": source_builder.sha(minato),
            "effective_spec_sha256": json_sha(spec),
            "input_resolution_sha256": json_sha(input_resolution),
            "strategy_input_lock_sha256": json_sha(strategy_lock),
            "shared_input_contract_sha256": shared["sha256"],
        }
        history_root = STATE.parent / "candidates" / ".generation" / re.sub(
            r"[^A-Za-z0-9_.-]", "_", cycle)
        replay = source_builder.replay_prior_admissible_proposal(
            history_root, bound, ea, spec, shared)
        return bool(isinstance(replay, dict) and isinstance(replay.get("source_text"), str)
                    and replay.get("source_text") and replay.get("raw_proposal")
                    and replay.get("prior_hold_sha256"))
    except (OSError, ValueError, TypeError, KeyError, ImportError):
        return False


def refresh_authenticated_terminal_source_hold(state: dict) -> Path | None:
    cycle = state.get("cycle_id")
    failure_path = ROOT / "state" / "events" / f"{cycle}.stage2.failed.json"
    try:
        failure = load(failure_path)
    except (OSError, ValueError):
        return None
    if failure.get("cycle_id") != cycle or failure.get("status") != "failed":
        return None
    diagnosis = authenticated_source_diagnosis(state, failure.get("error", ""))
    if not diagnosis or diagnosis.get("proposal_budget_exhausted") is not True:
        return None
    # A deterministic admission repair may make an exact, lineage-bound source
    # proposal from this cycle pass again. In that case the executor may replay
    # that proposal once through the unchanged guards instead of recording the
    # stale validator rejection as a candidate failure or spending model retries.
    if prior_stage2_source_proposal_revalidates(state):
        return None
    return record_failure(state, RuntimeError(failure["error"]), traceback_text=failure.get("traceback"))
def validate_verifier_retry_admission(state: dict) -> None:
    """Fail closed unless the supervisor consumed the one-time verifier retry event."""
    ticket_path = Path(state.get("stage2_verifier_retry_ticket") or "")
    event_path = Path(state.get("stage2_verifier_retry_event") or "")
    if not ticket_path.is_file() or not event_path.is_file():
        raise RuntimeError("Stage 2 verifier retry ticket/event is missing")
    ticket, event = load(ticket_path), load(event_path)
    expected_ticket_hash = hashlib.sha256(json.dumps(
        {k: v for k, v in ticket.items() if k not in {"ticket_id", "ticket_sha256"}},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
    claim_path = Path(state.get("stage2_verifier_retry_dispatch_claim") or "")
    retry_mode = ticket.get("retry_mode")
    expected_failed_check = ("compile_clean" if retry_mode == "authenticated_mql5_nan_sentinel_repair"
                             else "cooldown_zero_disables_entry")
    if (ticket.get("schema_version") != 1 or ticket.get("cycle_id") != state.get("cycle_id")
            or ticket.get("stage") != 2 or ticket.get("failed_check") != expected_failed_check
            or ticket.get("retry_limit") != 1 or ticket.get("ticket_sha256") != expected_ticket_hash
            or ticket.get("ticket_sha256") != state.get("stage2_verifier_retry_ticket_sha256")
            or ticket.get("ticket_id") != state.get("stage2_verifier_retry_event_id")
            or event.get("cycle_id") != state.get("cycle_id") or event.get("stage") != 2
            or event.get("event_id") != ticket.get("ticket_id")
            or event.get("ticket_sha256") != expected_ticket_hash
            or event.get("consumed") is not True or event.get("consumed_action") != "dispatch_stage2"
            or not claim_path.is_file()
            or hashlib.sha256(claim_path.read_bytes()).hexdigest().upper()
                != str(state.get("stage2_verifier_retry_dispatch_claim_sha256") or "").upper()):
        raise RuntimeError("Stage 2 verifier retry is not bound to a consumed supervisor dispatch claim")
    def matches_file(path_value: str, hash_value: str) -> Path:
        path = Path(path_value)
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest().upper() != str(hash_value).upper():
            raise RuntimeError(f"Stage 2 verifier retry evidence changed: {path}")
        return path
    failure_path = matches_file(ticket.get("failure_event", ""), ticket.get("failure_sha256", ""))
    verification_path = matches_file(ticket.get("verification", ""), ticket.get("verification_sha256", ""))
    source_path = matches_file(ticket.get("source", ""), ticket.get("source_sha256", ""))
    compile_path = matches_file(ticket.get("compile_attempt", ""), ticket.get("compile_attempt_sha256", ""))
    retry_mode = ticket.get("retry_mode")
    if retry_mode == "authenticated_deterministic_source_repair":
        ex5_path = None
        if ticket.get("ex5") or ticket.get("ex5_sha256"):
            raise RuntimeError("Deterministic source repair retry must not reuse an EX5 from the failed compile")
    else:
        ex5_path = matches_file(ticket.get("ex5", ""), ticket.get("ex5_sha256", ""))
    stage1_event = matches_file(ticket.get("stage1_event", ""), ticket.get("stage1_event_sha256", ""))
    stage1_spec = matches_file(ticket.get("stage1_spec", ""), ticket.get("stage1_spec_sha256", ""))
    failure, verification = load(failure_path), load(verification_path)
    if retry_mode in {"restored_authenticated_source", "authenticated_deterministic_source_repair",
                      "authenticated_cooldown_helper_repair", "authenticated_mql5_nan_sentinel_repair"}:
        rejected = matches_file(ticket.get("rejected_source_archive", ""),
                                ticket.get("rejected_source_archive_sha256", ""))
        receipt_path = matches_file(ticket.get("restored_source_build_receipt", ""),
                                    ticket.get("restored_source_build_receipt_sha256", ""))
        receipt = load(receipt_path)
        deterministic_repair = retry_mode == "authenticated_deterministic_source_repair"
        cooldown_repair = retry_mode == "authenticated_cooldown_helper_repair"
        nan_repair = retry_mode == "authenticated_mql5_nan_sentinel_repair"
        expected_failed = ({"compile_clean", "cooldown_zero_disables_entry", "time_stop_zero_disables_close"}
                           if deterministic_repair else
                           {"compile_clean"} if nan_repair else
                           {"cooldown_zero_disables_entry"} if cooldown_repair else
                           {"cooldown_zero_disables_entry", "generated_source_review_current"})
        proof_path = None
        if deterministic_repair:
            proof_path = matches_file(ticket.get("source_admission_proof", ""),
                                      ticket.get("source_admission_proof_sha256", ""))
            proof = load(proof_path)
            failed_compile = load(compile_path)
            compile_log = Path(failed_compile.get("log") or "")
            if (failed_compile.get("cycle_id") != state.get("cycle_id")
                    or failed_compile.get("passed") is not False
                    or str(failed_compile.get("mq5_sha256") or "").upper()
                        != stage2_source_recovery.sha256(rejected)
                    or not compile_log.is_file()
                    or "error 203: invalid index value" not in (
                        compile_log.read_bytes().decode("utf-16", errors="replace")
                        if compile_log.read_bytes().startswith((b"\xff\xfe", b"\xfe\xff"))
                        else compile_log.read_text(encoding="utf-8-sig", errors="replace")
                    ).casefold()):
                raise RuntimeError("Stage 2 deterministic retry compile evidence no longer binds the preserved failing source")
            required_repairs = set()
            repair_kinds = set()
        elif cooldown_repair:
            report_path = matches_file(ticket.get("repair_report", ""), ticket.get("repair_report_sha256", ""))
            report = load(report_path)
            attempts = report.get("attempts") or []
            baseline = next((a for a in attempts if a.get("attempt") == 0), {})
            repaired = next((a for a in attempts if a.get("deterministic", {}).get("changed") is True), {})
            proof_path = matches_file(ticket.get("source_admission_proof", ""),
                                      ticket.get("source_admission_proof_sha256", ""))
            proof = load(proof_path)
            current_compile = load(compile_path)
            checks = verification.get("checks") or {}
            if (report.get("cycle_id") != state.get("cycle_id")
                    or report.get("ea") != state.get("candidate_ea")
                    or baseline.get("checks", {}).get("cooldown_zero_disables_entry") is not False
                    or {k for k, v in (baseline.get("checks") or {}).items() if v is False}
                       != {"cooldown_zero_disables_entry"}
                    or stage2_source_recovery.sha256(rejected) != str(repaired.get("before_sha256") or "").upper()
                    or stage2_source_recovery.sha256(source_path) != str(repaired.get("after_sha256") or "").upper()
                    or repaired.get("verification_passed") is not True
                    or failure.get("failure_kind") != "STAGE2_VERIFICATION_FAILURE"
                    or failure.get("failed_checks") != ["cooldown_zero_disables_entry"]
                    or verification.get("passed") is not True
                    or any(type(v) is not bool or v is not True for v in checks.values())
                    or current_compile.get("cycle_id") != state.get("cycle_id")
                    or current_compile.get("passed") is not True
                    or current_compile.get("fresh_ex5") is not True
                    or str(current_compile.get("mq5_sha256") or "").upper() != stage2_source_recovery.sha256(source_path)
                    or not ex5_path.is_file()
                    or str(current_compile.get("ex5_sha256") or "").upper() != stage2_source_recovery.sha256(ex5_path)
                    or proof.get("passed") is not True
                    or proof.get("cycle_id") != state.get("cycle_id")
                    or proof.get("ea") != state.get("candidate_ea")
                    or str(proof.get("source_sha256") or "").upper() != stage2_source_recovery.sha256(source_path)
                    or str(proof.get("source_spec_sha256") or "").upper() != stage2_source_recovery.sha256(stage1_spec)):
                raise RuntimeError("Stage 2 cooldown repair retry no longer binds its original failure, repair report, passing verifier, fresh compile, and admission proof")
        elif nan_repair:
            report_path = matches_file(ticket.get("repair_report", ""), ticket.get("repair_report_sha256", ""))
            report = load(report_path)
            attempts = report.get("attempts") or []
            baseline = next((a for a in attempts if a.get("attempt") == 0), {})
            repaired = next((a for a in attempts if a.get("attempt") == 1), {})
            deterministic = repaired.get("deterministic") or {}
            proof_path = matches_file(ticket.get("source_admission_proof", ""),
                                      ticket.get("source_admission_proof_sha256", ""))
            proof = load(proof_path)
            failed_verification_path = matches_file(ticket.get("failure_verification", ""),
                                                    ticket.get("failure_verification_sha256", ""))
            failed_verification = load(failed_verification_path)
            failed_compile_path = matches_file(ticket.get("failed_compile_attempt", ""),
                                               ticket.get("failed_compile_attempt_sha256", ""))
            failed_compile = load(failed_compile_path)
            failed_attempts = failed_compile.get("attempts") or []
            failed_last = failed_attempts[-1] if failed_attempts else failed_compile
            failed_log = Path(failed_last.get("log") or "")
            if not failed_log.is_file():
                raise RuntimeError("Stage 2 NaN repair retry lost its compiler log")
            raw = failed_log.read_bytes()
            log_text = (raw.decode("utf-16", errors="replace") if raw.startswith((b"\xff\xfe", b"\xfe\xff"))
                        else raw.decode("utf-8-sig", errors="replace"))
            error_count = len(re.findall(r"(?im):\s*error\s+198\s*:.*division by zero in the constant expression", log_text))
            changes = [c for c in deterministic.get("changes", []) if c.get("kind") == "mql5_nan_sentinel_replaced"]
            checks = verification.get("checks") or {}
            current_compile = load(compile_path)
            if (report.get("cycle_id") != state.get("cycle_id") or report.get("ea") != state.get("candidate_ea")
                    or baseline.get("checks", {}).get("compile_clean") is not False
                    or {k for k, v in (baseline.get("checks") or {}).items() if v is False} != {"compile_clean"}
                    or stage2_source_recovery.sha256(rejected) != str(baseline.get("before_sha256") or "").upper()
                    or deterministic.get("kind") != "mql5_nan_sentinel_replaced"
                    or deterministic.get("status") != "APPLIED" or deterministic.get("changed") is not True
                    or sum(int(c.get("count", 0)) for c in changes) != error_count or error_count < 1
                    or stage2_source_recovery.sha256(source_path) != str(repaired.get("after_sha256") or "").upper()
                    or stage2_source_recovery.sha256(source_path) == stage2_source_recovery.sha256(rejected)
                    or repaired.get("verification_passed") is not True
                    or failed_verification.get("cycle_id") != state.get("cycle_id")
                    or failed_verification.get("passed") is not False
                    or Path(failure.get("verification") or "").resolve() != failed_verification_path.resolve()
                    or str(failed_verification.get("source_sha256") or "").upper() != stage2_source_recovery.sha256(rejected)
                    or set(failure.get("failed_checks") or []) != {"compile_clean"}
                    or failed_compile.get("cycle_id") != state.get("cycle_id")
                    or failed_compile.get("passed") is not False
                    or str(failed_last.get("mq5_sha256") or "").upper() != stage2_source_recovery.sha256(rejected)
                    or "error 198" not in log_text.casefold()
                    or Path(verification.get("source") or "").resolve() != source_path.resolve()
                    or str(verification.get("source_sha256") or "").upper() != stage2_source_recovery.sha256(source_path)
                    or verification.get("stage1_spec_sha256") != ticket.get("stage1_spec_sha256")
                    or verification.get("passed") is not True
                    or any(type(v) is not bool or v is not True for v in checks.values())
                    or current_compile.get("cycle_id") != state.get("cycle_id")
                    or current_compile.get("passed") is not True or current_compile.get("fresh_ex5") is not True
                    or str(current_compile.get("mq5_sha256") or "").upper() != stage2_source_recovery.sha256(source_path)
                    or not ex5_path.is_file()
                    or str(current_compile.get("ex5_sha256") or "").upper() != stage2_source_recovery.sha256(ex5_path)
                    or proof.get("passed") is not True or proof.get("cycle_id") != state.get("cycle_id")
                    or proof.get("ea") != state.get("candidate_ea")
                    or str(proof.get("source_sha256") or "").upper() != stage2_source_recovery.sha256(source_path)
                    or str(proof.get("source_spec_sha256") or "").upper() != stage2_source_recovery.sha256(stage1_spec)):
                raise RuntimeError("Stage 2 NaN sentinel repair retry no longer binds the failed source, exact compiler error, one deterministic replacement, admission proof, passing verifier, and fresh EX5")
        else:
            proof = {}
            required_repairs = set()
            repair_kinds = set()
        deterministic_proof_invalid = deterministic_repair and (
            proof.get("passed") is not True
            or proof.get("cycle_id") != state.get("cycle_id")
            or proof.get("ea") != state.get("candidate_ea")
            or str(proof.get("source_sha256") or "").upper() != stage2_source_recovery.sha256(source_path)
            or str(proof.get("source_spec_sha256") or "").upper() != stage2_source_recovery.sha256(stage1_spec)
            or not required_repairs.issubset(repair_kinds)
            or not zero_disables_time_stop_controls(source_path.read_text(encoding="utf-8-sig"))
        )
        if (failure.get("cycle_id") != state.get("cycle_id")
                or set(failure.get("failed_checks") or []) != expected_failed
                or ((deterministic_repair or nan_repair) and failure.get("failure_kind") != "COMPILE_INTEGRITY_FAILURE")
                or Path(verification.get("source") or "").resolve() != source_path.resolve()
                or (not cooldown_repair and str(verification.get("source_sha256") or "").upper() != stage2_source_recovery.sha256(rejected)
                    and not nan_repair)
                or (not cooldown_repair and stage2_source_recovery.sha256(source_path) == stage2_source_recovery.sha256(rejected))
                or (cooldown_repair and (str(verification.get("source_sha256") or "").upper() != stage2_source_recovery.sha256(source_path)
                    or stage2_source_recovery.sha256(source_path) == stage2_source_recovery.sha256(rejected)))
                or verification.get("stage1_spec_sha256") != ticket.get("stage1_spec_sha256")
                or (not deterministic_repair and not cooldown_repair and
                    (verification.get("ex5_sha256") or (verification.get("compile") or {}).get("ex5_sha256")) != ticket.get("ex5_sha256"))
                or receipt.get("cycle_id") != state.get("cycle_id")
                or receipt.get("candidate_ea") != state.get("candidate_ea")
                or Path(receipt.get("output") or "").resolve() != source_path.resolve()
                or str(receipt.get("sha256") or "").upper() != stage2_source_recovery.sha256(source_path)
                or Path(receipt.get("source_spec") or "").resolve() != stage1_spec.resolve()
                or str(receipt.get("source_spec_sha256") or "").upper() != stage2_source_recovery.sha256(stage1_spec)
                or receipt.get("implementation_mode") != "model_generated_source_admitted"
                or not isinstance(receipt.get("implementation_admission"), dict)
                or not zero_disables_cooldown_controls(source_path.read_text(encoding="utf-8-sig"))
                or deterministic_proof_invalid):
            raise RuntimeError("Stage 2 restored-source retry no longer matches its preserved failure and authenticated build receipt")
    elif (failure.get("cycle_id") != state.get("cycle_id")
            or failure.get("failed_checks") != ["cooldown_zero_disables_entry"]
            or Path(verification.get("source") or "").resolve() != source_path.resolve()
            or verification.get("source_sha256") != ticket.get("source_sha256")
            or verification.get("stage1_spec_sha256") != ticket.get("stage1_spec_sha256")
            or (verification.get("ex5_sha256") or (verification.get("compile") or {}).get("ex5_sha256")) != ticket.get("ex5_sha256")
            or not zero_disables_cooldown_controls(source_path.read_text(encoding="utf-8-sig"))):
        raise RuntimeError("Stage 2 verifier retry no longer matches the authenticated corrected cooldown case")
    event1 = load(stage1_event)
    if (event1.get("cycle_id") != state.get("cycle_id")
            or event1.get("artifact_sha256") != ticket.get("stage1_spec_sha256")
            or stage1_spec.resolve() != Path(state.get("last_stage1_artifact") or "").resolve()
            or source_path.resolve() != Path(state.get("candidate_source") or "").resolve()
            or (retry_mode not in {"authenticated_deterministic_source_repair", "authenticated_cooldown_helper_repair", "authenticated_mql5_nan_sentinel_repair"}
                and ex5_path.resolve() != Path(verification.get("ex5") or "").resolve())):
        raise RuntimeError("Stage 2 verifier retry lineage or artifact identity changed")


def stage2_requalification_is_current_cycle(state: dict) -> bool:
    marker = state.get("stage2_requalification")
    return isinstance(marker, dict) and marker.get("cycle_id") == state.get("cycle_id")


def validate_stage2_requalification(state: dict) -> bool:
    """Validate the supervisor's one-shot requalification claim before skipping source repair."""
    marker = state.get("stage2_requalification")
    if (not isinstance(marker, dict) or marker.get("schema_version") != 1
            or marker.get("status") != "authorized"
            or marker.get("cycle_id") != state.get("cycle_id")
            or state.get("stage2_requalification_count") != 1
            or state.get("current_stage") != "stage2_pending"
            or state.get("stage_status") != "stage2_repair_required"
            or state.get("next_action") != "dispatch_stage2"):
        return False
    claim_path = Path(marker.get("dispatch_claim") or "")
    if not claim_path.is_file():
        return False
    claim = load(claim_path)
    claim_identity = claim.get("content_identity") or {}
    source_path = Path(state.get("candidate_source") or state.get("stage2_candidate_source") or "")
    prior_event_path = Path(state.get("last_stage2_event") or "")
    verification_path = Path(state.get("last_stage2_verification") or "")
    if not all(path.is_file() for path in (source_path, prior_event_path, verification_path)):
        return False
    identity = marker.get("identity") or {}
    source_hash = hashlib.sha256(source_path.read_bytes()).hexdigest().upper()
    prior_event = load(prior_event_path)
    for consumed_key in ("consumed", "consumed_utc", "consumed_action"):
        prior_event.pop(consumed_key, None)
    event_identity_hash = hashlib.sha256(json.dumps(
        prior_event, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest().upper()
    if (claim.get("schema_version") != 3 or claim.get("event_id") != marker.get("event_id")
            or claim.get("action") != "dispatch_stage2"
            or claim.get("content_identity_sha256") != marker.get("dispatch_claim_content_identity_sha256")
            or claim_identity.get("cycle_id") != state.get("cycle_id")
            or claim_identity.get("candidate_ea") != (state.get("candidate_ea") or state.get("current_ea"))
            or claim_identity.get("candidate_source_sha256") != source_hash
            or claim_identity.get("event_sha256") != event_identity_hash
            or identity.get("stale_event_sha256") != claim_identity.get("event_sha256")
            or identity.get("verification_sha256") != hashlib.sha256(verification_path.read_bytes()).hexdigest().upper()
            or identity.get("source_sha256") != source_hash):
        return False
    ea_name = state.get("candidate_ea") or state.get("current_ea")
    evidence = authenticated_stage3_zero_trade_repair(state, state.get("cycle_id"), ea_name)
    if evidence is None:
        return False
    identity = marker.get("identity") or {}
    return bool(
        marker.get("event_id") == claim.get("event_id")
        and identity.get("cycle_id") == state.get("cycle_id")
        and identity.get("source_sha256") == claim_identity.get("candidate_source_sha256")
        and identity.get("zero_trade_decision_sha256") == evidence.get("decision_sha256")
        and identity.get("zero_trade_verification_sha256") == evidence.get("verification_sha256")
        and claim_identity.get("zero_trade_decision_sha256") == evidence.get("decision_sha256")
        and claim_identity.get("zero_trade_verification_sha256") == evidence.get("verification_sha256")
    )


def main() -> int:
    state = load(STATE)
    # A failed prepare/repair can leave the canonical pointer at
    # stage2_running + stage2_preflight before the executor exits. Allow a
    # same-cycle retry for that exact resumable state; reject all other
    # running/finished combinations to preserve fail-closed behavior.
    current_stage = state.get("current_stage")
    stage_status = state.get("stage_status")
    if current_stage != "stage2_pending" and not (
        current_stage == "stage2_running" and stage_status == "stage2_preflight"
    ):
        print(json.dumps({"stage": 2, "cycle_id": state.get("cycle_id"), "status": "NOOP_STAGE_NOT_OWNED", "current_stage": current_stage, "stage_status": stage_status}))
        return 0
    cycle = state["cycle_id"]
    requalification_valid = validate_stage2_requalification(state)
    if stage2_requalification_is_current_cycle(state) and not requalification_valid:
        raise RuntimeError("Stage 2 requalification claim no longer authenticates its same-cycle source and zero-trade evidence")
    terminal_source_ticket = Path(state.get("stage2_source_defect_ticket") or "")
    if (state.get("stage_status") == "stage2_repair_required"
            and state.get("stage2_source_defect_retry_terminal_hold")
            and terminal_source_ticket.is_file()):
        try:
            terminal_ticket = stage2_source_recovery.load(terminal_source_ticket)
        except (OSError, ValueError, KeyError, TypeError):
            terminal_ticket = {}
        if terminal_ticket.get("cycle_id") == cycle:
            print(json.dumps({"stage": 2, "cycle_id": cycle,
                              "status": "WAITING_SUPERVISOR_RECOVERY_HOLD",
                              "detail": "same-cycle source-defect retry budget is held by the supervisor"},
                             separators=(",", ":")))
            return 0
    live_stage3 = rotation.active_stage3_runtime(state, STATE.parent)
    if live_stage3:
        print(json.dumps({"stage": 2, "cycle_id": cycle,
                          "status": "WAITING_STAGE3_RUNTIME",
                          "detail": "Stage 2 is waiting for the authenticated Stage 3 owner to release the shared MT5 profile",
                          "runtime": live_stage3}, separators=(",", ":")))
        return 0
    try:
        stage2_tester.assert_history_readable(stage2_tester.DATA_ROOT, "ETHUSD")
    except PermissionError as exc:
        blocked_path = Path(exc.filename) if exc.filename else None
        if blocked_path and blocked_path.suffix.casefold() == ".hcc":
            print(json.dumps({"stage": 2, "cycle_id": cycle,
                              "status": "WAITING_MT5_PROFILE_RELEASE",
                              "detail": "Stage 2 is waiting for the canonical MT5 profile history file to become readable",
                              "blocked_path": str(blocked_path)}, separators=(",", ":")))
            return 0
        raise
    if state.get("stage_status") == "stage2_repair_required" and state.get("stage2_verifier_retry_event"):
        validate_verifier_retry_admission(state)
    terminal_hold_event = refresh_authenticated_terminal_source_hold(state)
    if terminal_hold_event is not None:
        print(json.dumps({"stage": 2, "cycle_id": cycle, "published": False, "status": "AUTHENTICATED_PROPOSAL_BUDGET_EXHAUSTED", "failure_event": str(terminal_hold_event)}))
        return 2
    recovery_archive = archive_stage3_dcc_source_repair(state)
    if recovery_archive is not None:
        state = load(STATE)
    attempts = ROOT / "state" / "attempts" / f"{cycle}.stage2.compile.latest.json"
    verification = ROOT / "state" / "verification" / f"{cycle}.stage2.verification.json"
    event = ROOT / "state" / "events" / f"{cycle}.stage2.complete.json"

    # If Stage 1 published without a candidate .mq5 (research-new path), build
    # one from the spec before prepare/reconcile so the rest of the pipeline
    # has a real source file to operate on.
    ea_name = state.get("candidate_ea") or state.get("current_ea")
    candidate_source = state.get("candidate_source")
    last_artifact = state.get("last_stage1_artifact")
    if ea_name and last_artifact and (
        not candidate_source
        or not Path(candidate_source).is_file()
        or Path(candidate_source).suffix.lower() != ".mq5"
    ):
        built = CANDIDATES_DIR / f"{ea_name}.mq5"
        run([
            str(BUILD),
            "--cycle-id", cycle,
            "--spec", last_artifact,
            "--state", str(STATE),
            "--candidates-dir", str(CANDIDATES_DIR),
            "--output", str(built),
        ])
        # Reflect the new source into state so downstream steps see it.
        state = load(STATE)
        if state.get("candidate_source") != str(built):
            state["candidate_source"] = str(built)
            tmp = STATE.with_suffix(STATE.suffix + ".tmp")
            tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
            tmp.replace(STATE)
            state = load(STATE)
        candidate_source = state.get("candidate_source")
        if (not candidate_source or Path(candidate_source).resolve() != built.resolve()
                or not built.is_file()):
            raise RuntimeError("Stage 2 candidate generation did not publish the expected source path")

    # Repair only the known generated DCC scaffold. Never overwrite a
    # non-generated/manual source when its implementation contract is absent.
    if ea_name and last_artifact and candidate_source and Path(candidate_source).is_file():
        spec_doc = load(last_artifact)
        if "dcc-garch" in json.dumps(spec_doc, ensure_ascii=False).lower():
            existing_source = Path(candidate_source).read_text(encoding="utf-8-sig", errors="replace")
            if "DCC_GARCH_IMPLEMENTATION_V1" not in existing_source:
                build_receipt = Path(candidate_source).with_suffix(Path(candidate_source).suffix + ".build.json")
                receipt = load(build_receipt) if build_receipt.is_file() else {}
                model_admitted = (
                    receipt.get("implementation_mode") == "model_generated_source_admitted"
                    and receipt.get("cycle_id") == cycle
                    and receipt.get("candidate_ea") == ea_name
                    and Path(receipt.get("output") or "").resolve() == Path(candidate_source).resolve()
                    and str(receipt.get("sha256") or "").upper() == hashlib.sha256(Path(candidate_source).read_bytes()).hexdigest().upper()
                    and Path(receipt.get("source_spec") or "").resolve() == Path(last_artifact).resolve()
                    and str(receipt.get("source_spec_sha256") or "").upper() == hashlib.sha256(Path(last_artifact).read_bytes()).hexdigest().upper()
                    and isinstance(receipt.get("implementation_admission"), dict)
                )
                if model_admitted:
                    # The builder's admission receipt authenticates this exact
                    # model-generated source/spec pair; it is not the old DCC
                    # template and must not be overwritten by that renderer.
                    pass
                else:
                    if "Auto-generated scaffold by state/kcos_stage2_build_ea_v1.py" not in existing_source:
                        raise RuntimeError("DCC-GARCH source has neither authenticated model admission nor the known generated scaffold; refusing overwrite")
                    run([
                        str(BUILD),
                        "--cycle-id", cycle,
                        "--spec", last_artifact,
                        "--state", str(STATE),
                        "--candidates-dir", str(CANDIDATES_DIR),
                        "--output", str(candidate_source),
                    ])
                    state = load(STATE)
                    rebuilt = Path(candidate_source).read_text(encoding="utf-8-sig", errors="replace")
                    rebuilt_receipt = load(build_receipt) if build_receipt.is_file() else {}
                    if ("DCC_GARCH_IMPLEMENTATION_V1" not in rebuilt
                            and rebuilt_receipt.get("implementation_mode") != "model_generated_source_admitted"):
                        raise RuntimeError("DCC-GARCH builder output has no authenticated implementation mode")

    # Do not run the legacy singleton reconciler: it reads Stage1_v2.json
    # (which can belong to another cycle) and edits the published Stage 1
    # spec/event in place. prepare() resolves the active cycle from canonical
    # state and holds on out-of-domain inputs without changing Stage 1.
    repair_missing_trade_include_from_compile_evidence(
        cycle, ea_name, candidate_source, attempts, last_artifact)
    admission_repair = None
    try:
        run([str(PREPARE)])
    except RuntimeError as exc:
        if "generated source build receipt does not authenticate this cycle/spec/source" not in str(exc):
            raise
        existing_handoff = ROOT / "state" / "handoffs" / "Stage2_v2.json"
        if not existing_handoff.is_file():
            raise RuntimeError("stale generated-source admission failure has no same-cycle handoff for transactional recovery") from exc
        admission_repair = repair_stale_generated_admission(existing_handoff, attempts, verification, cycle)
        run([str(PREPARE)])
    handoff = ROOT / "state" / "handoffs" / "Stage2_v2.json"
    recovery_ticket = (stage2_source_recovery.load(Path(state.get("stage2_source_defect_ticket") or ""))
                       if recovery_archive is not None else None)
    input_grid_evidence = authenticated_stage3_input_grid_repair(state, cycle, ea_name)
    zero_trade_evidence = (
        None if input_grid_evidence is not None
        or recovery_ticket and recovery_ticket.get("repair_kind") == "stage3_tester_journal_invalid_stops"
        else authenticated_stage3_zero_trade_repair(state, cycle, ea_name)
    )
    if zero_trade_evidence is not None:
        prepared = load(handoff)
        attempts_file = Path(prepared["compile_attempts_file"])
        if attempts_file.is_file():
            archive = attempts_file.with_name(
                attempts_file.name + ".bak.pre-stage3-zero-trade-repair."
                + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
            attempts_file.replace(archive)
        prepared["stage3_zero_trade_repair"] = True
        prepared["stage3_zero_trade_evidence"] = zero_trade_evidence
        handoff.write_text(json.dumps(prepared, indent=2) + "\n", encoding="utf-8")
    if input_grid_evidence is not None:
        prepared = load(handoff)
        prepared.pop("stage3_zero_trade_repair", None)
        prepared.pop("stage3_zero_trade_evidence", None)
        prepared["stage3_input_grid_repair"] = input_grid_evidence
        handoff.write_text(json.dumps(prepared, indent=2) + "\n", encoding="utf-8")
    if recovery_archive is not None:
        ticket_path = Path(state.get("stage2_source_defect_ticket") or "")
        ticket = recovery_ticket or stage2_source_recovery.load(ticket_path)
        if ticket.get("repair_kind") in {
                "stage3_tester_journal_invalid_stops",
                "stage3_tester_journal_multidimensional_array_row_bounds",
                "stage3_tester_journal_quantile_interpolation_bounds",
                "stage3_tester_journal_ea_runtime_array_bounds"}:
            prepared = load(handoff)
            runtime_record = ticket.get("artifacts", {}).get("stage3_runtime") or {}
            diagnostic = ticket.get("stage3_journal_diagnostic") or {}
            matching = diagnostic.get("matching_error_lines") or []
            kind = ticket.get("repair_kind")
            valid_diagnostic = (
                diagnostic.get("code") == "TESTER_EA_ORDER_REJECTED" and len(matching) >= 2
                if kind == "stage3_tester_journal_invalid_stops"
                else diagnostic.get("code") in {"TESTER_JOURNAL_ERROR", "NO_EXPERT_IN_START_CONFIG"}
                and len(matching) >= 2
                and all("runtime error 502" in str(line).casefold()
                        and "array out of range" in str(line).casefold() for line in matching)
                and str(ticket.get("parent_source_sha256") or "").upper()
                == str((ticket.get("artifacts", {}).get("stage2_original_source") or {}).get("sha256") or "").upper()
            )
            if not stage2_source_recovery._check_bound_file(runtime_record) or not valid_diagnostic:
                raise RuntimeError("Stage 3 order-rejection evidence no longer matches its immutable recovery ticket")
            prepared["stage3_runtime_repair"] = {
                "kind": ticket["repair_kind"], "ticket": str(ticket_path.resolve()),
                "parent_source_sha256": ticket.get("parent_source_sha256"),
                "ticket_sha256": stage2_source_recovery.sha256(ticket_path),
                "runtime": runtime_record, "diagnostic": diagnostic}
            handoff.write_text(json.dumps(prepared, indent=2) + "\n", encoding="utf-8")
    # A fresh Stage 2 candidate has no failure episode to repair yet. Run its
    # baseline compile and verification first; invoke the bounded repair loop
    # only when canonical Stage 2 failure or authenticated Stage 3 recovery
    # evidence exists. This avoids manufacturing a failure just to enter repair.
    repair_evidence_present = stage2_repair_loop_required(
        state, cycle, ea_name, candidate_source, Path(last_artifact),
        zero_trade_evidence, recovery_archive, input_grid_evidence)
    if admission_repair is None and repair_evidence_present and not requalification_valid:
        run([str(REPAIR_LOOP), "--handoff", str(handoff), "--compile", str(attempts), "--verification", str(verification)])
    if recovery_archive is not None:
        handoff_after_repair = load(handoff)
        source_after_repair = Path(handoff_after_repair.get("source") or "")
        original_source_hash = next(
            item["sha256"] for item in load(Path(recovery_archive["path"]))["files"].values()
            if Path(item["original_path"]).resolve() == source_after_repair.resolve())
        repair_kind = (load(Path(state.get("stage2_source_defect_ticket") or "")).get("repair_kind")
                       if state.get("stage2_source_defect_ticket") else None)
        if not source_after_repair.is_file() or stage2_source_recovery.sha256(source_after_repair) == original_source_hash:
            raise RuntimeError("Stage 2 source-defect repair did not produce a changed candidate source")
        if repair_kind == "stage3_tester_journal_dcc_closed_return_bounds":
            if not dcc_closed_return_arrays_are_bounded(
                    source_after_repair.read_text(encoding="utf-8-sig", errors="replace"), required_dcc=True):
                raise RuntimeError("Stage 2 repair did not produce a fresh DCC source passing the closed-return bounds gate")
        elif repair_kind == "stage3_tester_journal_multidimensional_array_row_bounds":
            from kcos_stage2_verify_v2 import multidimensional_array_row_bounds_are_safe
            if (stage2_source_recovery.sha256(source_after_repair) == original_source_hash
                    or not multidimensional_array_row_bounds_are_safe(
                        source_after_repair.read_text(encoding="utf-8-sig", errors="replace"))):
                raise RuntimeError("Stage 2 repair did not produce a changed source passing multidimensional row-bounds verification")
        elif repair_kind == "stage3_tester_journal_quantile_interpolation_bounds":
            from kcos_stage2_repair_loop_v1 import quantile_interpolation_bounds_are_safe
            if (stage2_source_recovery.sha256(source_after_repair) == original_source_hash
                    or not quantile_interpolation_bounds_are_safe(
                        source_after_repair.read_text(encoding="utf-8-sig", errors="replace"))):
                raise RuntimeError("Stage 2 repair did not produce a changed source passing quantile interpolation bounds verification")
    run([str(COMPILE), "--handoff", str(handoff), "--output", str(attempts)])
    run([str(VERIFY), "--handoff", str(handoff), "--compile", str(attempts), "--output", str(verification)])
    run([str(PUBLISH), "--state", str(STATE), "--verification", str(verification),
         "--event", str(event)])
    print(json.dumps({"stage": 2, "cycle_id": cycle, "published": True}))
    return 0


def pause_cycle_after_proposal_exhaustion(state: dict, failure_path: Path) -> dict | None:
    """Pause on an authenticated same-cycle repair cap or a terminal no-op."""
    cycle = str(state.get("cycle_id") or "")
    failure = load(failure_path)
    ea = str(state.get("candidate_ea") or state.get("current_ea") or "")
    report_path = ROOT / "state" / "reports" / f"{cycle}.stage2.repair-loop.json"
    if not report_path.is_file():
        return None
    report = load(report_path)
    attempts = report.get("attempts") if isinstance(report.get("attempts"), list) else []
    proposals = [item for item in attempts if isinstance(item, dict)
                 and type(item.get("attempt")) is int and item["attempt"] > 0]
    baseline = attempts[0] if attempts and isinstance(attempts[0], dict) else {}
    terminal_no_progress = (
        report.get("outcome") == "NO_PROGRESS"
        and bool(proposals)
        and (proposals[-1].get("ai_application") or {}).get("status") == "NO_PROGRESS"
    )
    # The repair loop can exhaust its same-source budget across multiple
    # scheduled invocations. In that case the latest report may contain only
    # a deterministic preflight entry, so count only authenticated, rolled
    # back transaction reports bound to this cycle, EA, source, and repair
    # contract.
    cumulative_proposal_count = 0
    try:
        contract_digest = hashlib.sha256()
        for contract_file in (Path(stage2_repair_loop.__file__).resolve(),
                              Path(stage2_repair_loop.REPAIR).resolve(),
                              Path(stage2_repair_loop.VERIFY).resolve()):
            contract_digest.update(contract_file.name.encode("utf-8"))
            contract_digest.update(b"\\0")
            contract_digest.update(contract_file.read_bytes())
        current_contract_sha256 = contract_digest.hexdigest().upper()
        baseline_sha256 = str(failure.get("source_sha256") or "").upper()
        tx_root = ROOT / "state" / "diagnostics" / "stage2_repair_transactions"
        for prior_report_path in tx_root.glob("*/*/repair-loop-report.json"):
            prior_report = load(prior_report_path)
            prior_attempts = (prior_report.get("attempts")
                              if isinstance(prior_report.get("attempts"), list) else [])
            prior_baseline = next((item for item in prior_attempts
                                   if isinstance(item, dict) and item.get("baseline") is True), {})
            prior_source_sha256 = str(prior_report.get("baseline_source_sha256")
                                      or prior_baseline.get("before_sha256") or "").upper()
            if (prior_report.get("cycle_id") != cycle
                    or prior_report.get("ea") != ea
                    or prior_report.get("max_attempts") != stage2_repair_loop.MAX_PROPOSALS
                    or prior_report.get("repair_contract_sha256") != current_contract_sha256
                    or prior_source_sha256 != baseline_sha256):
                continue
            prior_proposals = [item for item in prior_attempts
                               if isinstance(item, dict)
                               and type(item.get("attempt")) is int and item["attempt"] > 0
                               and (item.get("ai") or {}).get("status") in {"PROPOSED", "INVALID_RESPONSE"}
                               and item.get("adapter") and "deterministic" in item]
            if not prior_proposals:
                continue
            attempt_dir = prior_report_path.parent
            outcome_path, journal_path = attempt_dir / "outcome.json", attempt_dir / "transaction-journal.json"
            if not outcome_path.is_file() or not journal_path.is_file():
                continue
            outcome, journal = load(outcome_path), load(journal_path)
            if (outcome.get("outcome") not in {"REJECTED", "EXHAUSTED"}
                    or outcome.get("episode_id") != attempt_dir.parent.name
                    or journal.get("status") != "ROLLED_BACK"):
                continue
            proposal_files = sorted(path for path in (attempt_dir / "proposals").glob("proposal-*.json")
                                    if re.fullmatch(r"proposal-\d+\.json", path.name))
            proposed_count = sum((item.get("ai") or {}).get("status") == "PROPOSED"
                                 for item in prior_proposals)
            if len(proposal_files) != proposed_count:
                continue
            if any(not isinstance(load(path).get("response"), dict) for path in proposal_files):
                continue
            cumulative_proposal_count += len(prior_proposals)
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        cumulative_proposal_count = 0
    cumulative_budget_exhausted = cumulative_proposal_count >= stage2_repair_loop.MAX_PROPOSALS
    proposal_budget_exhausted = (
        len(proposals) == stage2_repair_loop.MAX_PROPOSALS
        and [item.get("attempt") for item in proposals]
            == list(range(1, stage2_repair_loop.MAX_PROPOSALS + 1))
        and '"outcome": "EXHAUSTED"' in str(failure.get("error") or "")
    )
    if (failure.get("cycle_id") != cycle or failure.get("stage") != 2
            or failure.get("status") != "failed" or failure.get("ea") != ea
            or report.get("cycle_id") != cycle or report.get("ea") != ea
            or report.get("max_attempts") != stage2_repair_loop.MAX_PROPOSALS
            or not (terminal_no_progress or proposal_budget_exhausted or cumulative_budget_exhausted)
            or baseline.get("baseline") is not True or baseline.get("attempt") != 0
            or str(report.get("baseline_source_sha256") or baseline.get("before_sha256") or "").upper()
                != str(failure.get("source_sha256") or "").upper()):
        return None

    control_path = ROOT / "state" / "kcos_control_v2.json"
    control = load(control_path)
    if control.get("run_cycle_id") != cycle:
        # run_cycle_id can remain bound to the first candidate in an
        # operator-authorized multi-cycle run while the supervisor rotates
        # canonical state to a later candidate. Accept that stale control
        # pointer only when the current cycle itself carries matching,
        # explicit Stage 1-5 authorization and a matching run boundary.
        authorization = state.get("run_authorization") or {}
        boundary = state.get("effective_run_boundary") or {}
        authorized_stages = authorization.get("allowed_stages")
        boundary_stages = boundary.get("allowed_stages")
        if not (
            authorization.get("cycle_id") == cycle
            and boundary.get("cycle_id") == cycle
            and isinstance(authorized_stages, list)
            and 2 in authorized_stages
            and isinstance(boundary_stages, list)
            and 2 in boundary_stages
            and authorization.get("hold_after_stage", 0) >= 2
            and boundary.get("hold_after_stage", 0) >= 2
        ):
            return None
    control.update({
        "paused": True,
        "halted": True,
        "dispatch_enabled": False,
        "continuation_dispatch_enabled": False,
        "halt_reason": (f"Stage 2 repair made no progress after {len(proposals)} proposal attempt(s); paused for operator review."
                        if terminal_no_progress else
                        f"Stage 2 exhausted {max(len(proposals), cumulative_proposal_count)} authenticated same-source repair proposals; paused for operator review."),
        "pause_reason": ("Stage 2 repair proposal made no progress."
                         if terminal_no_progress else
                         f"Stage 2 exhausted {max(len(proposals), cumulative_proposal_count)} authenticated same-source repair proposals."),
        "updated_utc": datetime.now(timezone.utc).isoformat(),
    })
    temp = control_path.with_suffix(control_path.suffix + ".tmp")
    temp.write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")
    temp.replace(control_path)

    job_ids = {str(value) for value in (control.get("stage_jobs") or {}).values() if value}
    try:
        jobs_path = Path(os.environ.get("LOCALAPPDATA", "")) / "hermes" / "cron" / "jobs.json"
        registry = load(jobs_path)
        records = registry.get("jobs", registry if isinstance(registry, list) else [])
        supervisors = [str(item.get("id") or item.get("job_id") or "") for item in records
                       if ("kcos_supervisor_v2.py" in str(item.get("script") or "").casefold()
                           or str(item.get("name") or "").casefold() == "kcos_supervisor_v2_zero_token")]
        if len(supervisors) == 1 and supervisors[0]:
            job_ids.add(supervisors[0])
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    pause_results = []
    for job_id in sorted(job_ids):
        try:
            result = subprocess.run(["hermes", "cron", "pause", job_id],
                                    capture_output=True, text=True, timeout=15, check=False)
            pause_results.append({"job_id": job_id, "returncode": result.returncode,
                                  "detail": (result.stderr or result.stdout or "").strip()[-500:]})
        except Exception as exc:
            pause_results.append({"job_id": job_id, "error": str(exc)[:500]})
    return {"cycle_id": cycle, "proposal_limit": stage2_repair_loop.MAX_PROPOSALS,
            "cumulative_proposals": cumulative_proposal_count,
            "report": str(report_path.resolve()), "schedules_paused": pause_results}


def executor_entry() -> int:
    handle = acquire_executor_lock(EXECUTOR_LOCK)
    if handle is None:
        print(json.dumps({"stage": 2, "status": "NOOP_EXECUTOR_ACTIVE"}))
        return 0
    initial_state = None
    active_cycle = None
    try:
        initial_state = load(STATE)
        active_cycle = initial_state.get("cycle_id")
        return main()
    except Exception as exc:
        if isinstance(exc, PermissionError):
            blocked_path = Path(exc.filename) if getattr(exc, "filename", None) else None
            data_root = stage2_tester.DATA_ROOT.resolve()
            blocked_norm = os.path.normcase(os.path.abspath(str(blocked_path))) if blocked_path else ""
            root_norm = os.path.normcase(os.path.abspath(str(data_root)))
            if (blocked_path and blocked_path.suffix.casefold() == ".hcc"
                    and os.path.commonpath((blocked_norm, root_norm)) == root_norm):
                print(json.dumps({"stage": 2, "cycle_id": active_cycle,
                                  "status": "WAITING_MT5_PROFILE_RELEASE",
                                  "detail": "Stage 2 history identity is waiting for the canonical MT5 profile lock to clear",
                                  "blocked_path": str(blocked_path)}, separators=(",", ":")))
                return 0
        error_traceback = traceback.format_exc()
        exhaustion_pause = None
        try:
            if initial_state is None:
                raise RuntimeError('canonical state unavailable; cannot authenticate failure')
            failure_path = record_failure(initial_state, exc, traceback_text=error_traceback)
        except Exception as record_exc:
            failure_path = None
            record_error = str(record_exc)
        else:
            record_error = None
            try:
                exhaustion_pause = pause_cycle_after_proposal_exhaustion(initial_state, failure_path)
            except Exception as pause_exc:
                exhaustion_pause = {"error": str(pause_exc)[:1000]}
        payload = {"stage": 2, "published": False, "error": str(exc), "failure_event": str(failure_path) if failure_path else None, "record_error": record_error, "exhaustion_pause": exhaustion_pause}
        print(json.dumps(payload))
        return 2
    finally:
        if active_cycle and LEASE.exists():
            try:
                lease = load(LEASE)
                if lease.get("cycle_id") == active_cycle:
                    LEASE.unlink()
            except Exception:
                pass
        release_executor_lock(handle)


if __name__ == "__main__":
    raise SystemExit(executor_entry())
