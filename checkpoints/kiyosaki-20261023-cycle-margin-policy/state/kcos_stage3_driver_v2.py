#!/usr/bin/env python3
"""Deterministic, zero-token Stage 3 timeframe continuation driver."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import stat
import sys
import tempfile
import time
import psutil
from datetime import datetime, timezone
from pathlib import Path

# The Hermes cron runner executes scripts from ~/.hermes/scripts rather than
# from the project state directory.  Resolve local state imports explicitly.
_LOCAL_STATE = Path(__file__).resolve().parent
_IMPORT_DIRS = (_LOCAL_STATE, Path.cwd() / "state")
for _import_dir in _IMPORT_DIRS:
    if str(_import_dir) not in sys.path:
        sys.path.insert(0, str(_import_dir))
from kcos_runtime_paths_v2 import assert_safe_root

ROOT = assert_safe_root()
if str(ROOT / "state") not in sys.path:
    sys.path.insert(0, str(ROOT / "state"))

import kcos_stage3_evaluate_v2 as evaluator
import kcos_stage3_launch_v2 as launcher
import kcos_stage3_parse_v2 as parser
import kcos_stage3_publish_v2 as publisher
import kcos_stage3_verify_v2 as verifier
import kcos_stage3_prepare_v2 as preparer
import kcos_stage3_watch_v2 as watcher
import kcos_stage3_duration_policy_v2 as duration_policy
from kcos_stage3_common_v2 import TIMEFRAMES, atomic_json, load_object, require, sha256, utc_now


ROOT = assert_safe_root()
STATE = ROOT / "state" / "kcos_cycle_state_v2.json"
HANDOFF = ROOT / "state" / "handoffs" / "Stage3_v2.json"
STAGE2_HANDOFF = ROOT / "state" / "handoffs" / "Stage2_v2.json"
EA_LEDGER = ROOT / "state" / "kcos_ea_ledger_v2.json"
LEASE = ROOT / "state" / "leases" / "stage3_v2.lock.json"
OUTPUT_DIR = ROOT / "state" / "stage3_v2"
COMPUTE_LIMIT_STATUS = "COMPUTE_BUDGET_EXCEEDED"
ALREADY_RUNNING_DETAIL = "another Stage 3 runtime is already optimizing"
STAGE2_RELEASE_CYCLE_ID = "kcos-20260930t061535z-stage1-research-new"
# Preserve bounded retries while leaving one final attempt for a repaired
# deterministic preflight failure; preflight errors do not launch MT5 or
# consume the one-run Stage 3 handoff.
MAX_STAGE2_RELEASE_ATTEMPTS = 4
STAGE2_RELEASE_REASON = (
    "User-confirmed one-run release of this cycle's authenticated Stage 2 handoff for Stage 3 only; "
    "later stages remain governed by the current cycle authorization."
)


def authorizes_full_cycle_stage3(state: dict) -> bool:
    authorization = state.get("run_authorization")
    hold = authorization.get("hold_after_stage") if isinstance(authorization, dict) else None
    return (
        isinstance(authorization, dict)
        and authorization.get("schema_version") == 1
        and authorization.get("scope") == "current_cycle_stages_1_to_5"
        and authorization.get("cycle_id") == state.get("cycle_id")
        and authorization.get("authorized_by") == "user_confirmed"
        and authorization.get("allowed_stages") == [1, 2, 3, 4, 5]
        and isinstance(hold, (int, float)) and not isinstance(hold, bool) and hold >= 3
    )


def authenticated_consumed_stage2_release(state: dict) -> dict | None:
    """Validate the consumed Stage 2 release for same-cycle Stage 3 recovery."""
    attempt = state.get("stage2_release_attempt")
    if (not authorizes_full_cycle_stage3(state) or not isinstance(attempt, dict)
            or attempt.get("cycle_id") != state.get("cycle_id")
            or attempt.get("status") != "released"):
        return None
    try:
        handoff = load_object(HANDOFF)
        authorization = handoff.get("stage2_release")
        if (handoff.get("cycle_id") != state.get("cycle_id")
                or not isinstance(authorization, dict)
                or authorization.get("authorization_id") != attempt.get("authorization_id")):
            return None
        probe = dict(state)
        probe.update({"current_stage": "stage3_pending", "stage_status": "stage2_complete_held",
                      "hold_after_stage": 2, "next_action": "audit_stage2_before_stage3",
                      "stage2_release_attempt": dict(attempt, status="attempt_in_progress")})
        return preparer.validate_stage2_release_authorization(probe, STAGE2_HANDOFF, authorization)
    except (OSError, ValueError, TypeError, KeyError):
        return None


def authenticated_journal_retry_stage2_release(state: dict) -> dict | None:
    """Reuse the same-cycle release once for the exact journal-reclassification retry.

    The supervisor's one-shot retry may be consumed before Stage 3 reaches its
    preflight. If that preflight was called without the persisted release object,
    the state remains held and the original release remains ``released``. Reuse
    is limited to that hash-bound retry receipt and only while the same full-cycle
    authorization and consumed Stage 2 release still authenticate.
    """
    retry = state.get("stage3_repair_retry")
    if not (
        isinstance(retry, dict)
        and retry.get("status") == "consumed"
        and retry.get("repair_kind") == "stage3_journal_candidate_error_reclassification"
        and retry.get("cycle_id") == state.get("cycle_id")
        and retry.get("event_id") == state.get("stage3_journal_reclassification_retry_event_id")
        and state.get("stage3_journal_reclassification_retry_count") == 1
        and state.get("current_stage") == "stage3_pending"
        and state.get("stage_status") == "stage2_complete_held"
        and state.get("next_action") == "run_stage3"
        and isinstance(state.get("stage2_release_attempt"), dict)
        and (
            state["stage2_release_attempt"].get("status") == "released"
            or (
                state["stage2_release_attempt"].get("status") == "failed"
                and state["stage2_release_attempt"].get("failure_detail")
                == "Stage 2 release authorization requires the exact held Stage 3 admission state"
            )
        )
    ):
        return None
    try:
        failure_path = Path(retry["failure_event"])
        runtime_path = Path(retry["runtime_manifest"])
        receipt_path = Path(retry["repair_receipt"])
        if any(not path.is_file() for path in (failure_path, runtime_path, receipt_path)):
            return None
        if (sha256(failure_path) != retry.get("failure_event_sha256")
                or sha256(runtime_path) != retry.get("runtime_manifest_sha256")
                or sha256(receipt_path) != retry.get("repair_receipt_sha256")
                or str(receipt_path.resolve()) != str(state.get("stage3_journal_reclassification_recovery_receipt", ""))):
            return None
        failure, runtime, receipt = (load_object(path) for path in
                                     (failure_path, runtime_path, receipt_path))
        if not (
            failure.get("cycle_id") == state.get("cycle_id")
            and failure.get("stage") == 3
            and failure.get("status") == "failed"
            and failure.get("rotation_eligible") is False
            and runtime.get("cycle_id") == state.get("cycle_id")
            and runtime.get("stage") == 3
            and runtime.get("status") == "INFRASTRUCTURE_FAILURE"
            and runtime.get("infrastructure_only") is True
            and receipt.get("cycle_id") == state.get("cycle_id")
            and receipt.get("repair_complete") is True
            and receipt.get("retry_event_id") == retry.get("event_id")
            and receipt.get("failure_event_sha256") == retry.get("failure_event_sha256")
            and receipt.get("runtime_manifest_sha256") == retry.get("runtime_manifest_sha256")
        ):
            return None
        probe = dict(state)
        probe["stage2_release_attempt"] = dict(
            state["stage2_release_attempt"], status="released")
        return authenticated_consumed_stage2_release(probe)
    except (OSError, ValueError, TypeError, KeyError):
        return None


def duration_retry_fits_wide_budget(runtime: dict) -> bool:
    projection = runtime.get("duration_projection")
    try:
        projected = float(projection["projected_seconds"])
        elapsed = float(projection["elapsed_seconds"])
        passes = int(projection["completed_passes"])
        prior_budget = int(runtime.get("timeframe_budget_seconds", 0))
    except (TypeError, ValueError, KeyError):
        return False
    sample_qualified = projection.get("sample_qualified")
    if sample_qualified is None:
        sample_qualified = (elapsed >= duration_policy.MIN_PROJECTION_ELAPSED_SECONDS
                            and passes >= duration_policy.MIN_PROJECTION_PASSES)
    estimate_fits = projected <= duration_policy.BUDGET_SECONDS
    early_false_stop = not sample_qualified and projected > duration_policy.BUDGET_SECONDS
    return (
        runtime.get("status") == "DURATION_REDUCTION_REQUIRED"
        and runtime.get("duration_policy_action") in {"prepare_reduced_grid", "resume_wide_or_review"}
        and runtime.get("grid_mode", "default") == "default"
        and (estimate_fits or early_false_stop)
        and (prior_budget < duration_policy.BUDGET_SECONDS or early_false_stop)
    )


def stage3_duration_retry_is_ready(state: dict, runtime: dict | None = None) -> bool:
    """Retry the same authenticated wide profile when only its old time cap stopped it."""
    try:
        retry_count = int(state.get("stage3_duration_retry_count", 0))
    except (TypeError, ValueError):
        return False
    if not (
        state.get("current_stage") == "stage3_running"
        and state.get("stage_status") == "stage3_duration_reduction_pending"
        and state.get("next_action") == "resume_stage3_wide_after_budget_update"
        and state.get("stage3_grid_mode", "default") == "default"
        and retry_count < 2
        and authorizes_full_cycle_stage3(state)
    ):
        return False
    try:
        handoff = load_object(HANDOFF)
        runtime_path = Path(handoff["runtime_manifest"])
        current = load_object(runtime_path)
        if runtime is not None and runtime != current:
            return False
        if not duration_retry_fits_wide_budget(current):
            return False
        timeframe = str(current.get("timeframe", ""))
        if timeframe not in handoff.get("timeframe_order", []):
            return False
        if not (handoff.get("cycle_id") == state.get("cycle_id")
                and handoff.get("ea") == state.get("current_ea")
                and handoff.get("grid_mode", "default") == "default"
                and current.get("cycle_id") == state.get("cycle_id")
                and current.get("ea") == state.get("current_ea")):
            return False
        if (current.get("terminal_pid") and psutil.pid_exists(int(current["terminal_pid"]))
                or any(psutil.pid_exists(int(pid)) for pid in current.get("worker_pids", []))):
            return False
        event_path = Path(current.get("timeframe_event", ""))
        event = load_object(event_path) if event_path.is_file() else {}
        if not (event.get("cycle_id") == state.get("cycle_id")
                and event.get("timeframe") == timeframe
                and event.get("status") == "DURATION_REDUCTION_REQUIRED"):
            return False
        spec_path = Path(handoff.get("stage1_spec", ""))
        ini_path = Path(handoff.get("ini_files", {}).get(timeframe, ""))
        ini_hash = handoff.get("ini_sha256", {}).get(timeframe)
        if not (spec_path.is_file() and ini_path.is_file()
                and sha256(spec_path) == handoff.get("stage1_spec_sha256")
                and sha256(ini_path) == ini_hash
                and current.get("ini_sha256") == ini_hash
                and current.get("grid_mode", "default") == "default"):
            return False
        spec = load_object(spec_path)
        source_path = Path(handoff.get("source", ""))
        if not source_path.is_file() or sha256(source_path) != handoff.get("source_sha256"):
            return False
        source_text = source_path.read_text(encoding="utf-8-sig", errors="replace")
        stage2_config = preparer.stage2_config_from_spec(spec)
        if preparer.config_sha256(stage2_config) != handoff.get("approved_tester_config_sha256"):
            return False
        runtime_spec = dict(spec)
        runtime_spec["tester_config"] = preparer.stage3_config_from_spec(spec)
        preparer.validate_tester_inputs(ini_path.read_text(encoding="utf-8-sig"), runtime_spec)
        preparer.validate_optimized_input_source_contract(
            runtime_spec["tester_config"], source_text)
        preparer.validate_stage2(state, load_object(STAGE2_HANDOFF))
        return True
    except (OSError, ValueError, TypeError, KeyError):
        return False


def resume_stage3_wide_after_budget_update(state_path: Path, handoff_path: Path,
                                           launch_runner=launcher.launch_and_register) -> dict:
    """Preserve the stopped attempt and retry its same INI under the widened time cap."""
    state = load_object(state_path)
    handoff = load_object(handoff_path)
    runtime_path = Path(handoff["runtime_manifest"])
    runtime = load_object(runtime_path)
    require(stage3_duration_retry_is_ready(state, runtime),
            "wide Stage 3 duration retry is not authenticated or still has live MT5 owners")
    timeframe = str(runtime["timeframe"])
    retry_number = int(state.get("stage3_duration_retry_count", 0)) + 1
    archive_dir = runtime_path.parent / f"{state['cycle_id']}.stage3.{timeframe}.duration_attempt{retry_number}.previous"
    require(not archive_dir.exists(), f"duration retry evidence archive already exists: {archive_dir}")
    archive_dir.mkdir(parents=True)
    preserved = {"runtime": runtime_path, "handoff": handoff_path,
                 "ini": Path(handoff["ini_files"][timeframe])}
    if runtime.get("opt_path"):
        preserved["opt_cache"] = Path(runtime["opt_path"])
    if runtime.get("startup_ini"):
        preserved["startup_ini"] = Path(runtime["startup_ini"])
    hashes = {}
    for label, source in preserved.items():
        if source.is_file():
            target = archive_dir / f"{label}{source.suffix}"
            shutil.copy2(source, target)
            hashes[label] = {"path": str(target.resolve()), "sha256": sha256(target)}
    prior_budget = int(runtime.get("timeframe_budget_seconds", 0))
    handoff["timeframe_budget_seconds"] = duration_policy.BUDGET_SECONDS
    handoff["duration_retry"] = {
        "attempt": retry_number,
        "profile": "same_authenticated_wide_ini",
        "reason": ("recover_underqualified_early_projection"
                   if not runtime.get("duration_projection", {}).get("sample_qualified", False)
                   else "retry_under_updated_timeframe_budget"),
        "previous_timeframe_budget_seconds": prior_budget,
        "timeframe_budget_seconds": duration_policy.BUDGET_SECONDS,
        "prior_projection": runtime.get("duration_projection"),
        "preserved_attempt": str(archive_dir.resolve()),
        "preserved_artifacts": hashes,
        "prepared_utc": utc_now(),
    }
    atomic_json(handoff_path, handoff)
    state["stage3_duration_retry_count"] = retry_number
    state["stage3_duration_retry"] = handoff["duration_retry"]
    state["updated_utc"] = utc_now()
    atomic_json(state_path, state)
    result = launch_runner(handoff_path, timeframe)
    if result.get("status") == "START_REQUIRED":
        update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                        "stage3_start_required", f"desktop_start_stage3_{timeframe.lower()}")
        return {"status": "START_REQUIRED", "timeframe": timeframe,
                "runtime": str(runtime_path.resolve()), "reason": result.get("start_required_reason"),
                "zero_token": True}
    update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                    f"stage3_{timeframe.lower()}_running", f"watch_stage3_{timeframe.lower()}")
    return {"status": "STAGE3_WIDE_DURATION_RETRY_STARTED", "timeframe": timeframe,
            "runtime": str(runtime_path.resolve()), "zero_token": True}


def stage3_repair_retry_is_ready(state: dict) -> bool:
    """Accept one operator-recorded, hash-bound retry after a Stage 3 repair."""
    retry = state.get("stage3_repair_retry")
    if (not isinstance(retry, dict) or retry.get("status") != "ready"
            or retry.get("cycle_id") != state.get("cycle_id")):
        return False
    race_repair = retry.get("repair_kind") == "supervisor_retry_race"
    if not race_repair and not authenticated_consumed_stage2_release(state):
        return False
    try:
        failure_path = Path(retry["failure_event"])
        receipt_path = Path(retry["repair_receipt"])
        runtime_path = Path(retry["runtime_manifest"])
        if any(not path.is_file() for path in (failure_path, receipt_path, runtime_path)):
            return False
        if (sha256(failure_path) != retry.get("failure_event_sha256")
                or sha256(receipt_path) != retry.get("repair_receipt_sha256")
                or sha256(runtime_path) != retry.get("runtime_manifest_sha256")):
            return False
        failure, receipt, runtime = (load_object(path) for path in
                                     (failure_path, receipt_path, runtime_path))
        common = (failure.get("cycle_id") == state.get("cycle_id")
                and failure.get("stage") == 3 and failure.get("status") == "failed"
                and failure.get("rotation_eligible") is False
                and receipt.get("cycle_id") == state.get("cycle_id")
                and receipt.get("stage") == 3 and receipt.get("repair_complete") is True
                and runtime.get("cycle_id") == state.get("cycle_id")
                and runtime.get("status") == "INFRASTRUCTURE_FAILURE"
                and runtime.get("infrastructure_only") is True)
        if not race_repair:
            return common
        authorization = state.get("run_authorization")
        repaired_driver = Path(retry.get("repaired_driver_path", ""))
        execution_ids = retry.get("hermes_execution_ids")
        if (not common
                or state.get("current_stage") != "stage3_running"
                or state.get("stage_status") != "stage3_infrastructure_failure"
                or state.get("next_action") != "inspect_stage3_infrastructure_failure"
                or state.get("stage3_failure_event") != str(failure_path.resolve())
                or not isinstance(authorization, dict)
                or authorization.get("cycle_id") != state.get("cycle_id")
                or authorization.get("authorized_by") != "user_confirmed"
                or authorization.get("scope") != "current_cycle_stages_1_to_5"
                or 3 not in authorization.get("allowed_stages", [])
                or authorization.get("hold_after_stage") != 5
                or receipt.get("repair_kind") != "supervisor_retry_race"
                or receipt.get("failure_event_sha256") != retry.get("failure_event_sha256")
                or receipt.get("runtime_manifest_sha256") != retry.get("runtime_manifest_sha256")
                or receipt.get("repaired_driver_sha256") != retry.get("repaired_driver_sha256")
                or not isinstance(execution_ids, list) or len(execution_ids) != 2
                or len(set(execution_ids)) != 2
                or receipt.get("hermes_execution_ids") != execution_ids
                or receipt.get("cycle_id") != state.get("cycle_id")
                or receipt.get("stage") != 3
                or receipt.get("event_id") != failure.get("event_id")
                or receipt.get("runtime_manifest") != str(runtime_path.resolve())
                or retry.get("runtime_manifest") != str(runtime_path.resolve())
                or receipt.get("failure_kind") != "STAGE3_DEAD_RUNTIME_NO_COMPLETION"
                or receipt.get("runtime_failure_kind") != "EIGHTCAP_TERMINAL_NOT_FRESH"
                or not repaired_driver.is_file()
                or sha256(repaired_driver) != retry.get("repaired_driver_sha256")
                or failure.get("failure_kind") != "STAGE3_DEAD_RUNTIME_NO_COMPLETION"
                or runtime.get("failure_kind") != "EIGHTCAP_TERMINAL_NOT_FRESH"
                or runtime.get("infrastructure_only") is not True
                or _has_timeframe_completion_evidence(runtime)):
            return False
        if (int(runtime.get("terminal_pid") or 0)
                and psutil.pid_exists(int(runtime.get("terminal_pid") or 0))):
            return False
        if any(psutil.pid_exists(int(pid)) for pid in runtime.get("worker_pids", [])):
            return False
        if _matching_stage3_watcher(runtime_path) is not False:
            return False
        for pid in launcher.process_ids("terminal64.exe"):
            try:
                if Path(psutil.Process(pid).exe()).resolve() == launcher.STAGE3_PORTABLE_TERMINAL.resolve():
                    return False
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                return False
        return common
    except (OSError, ValueError, TypeError, KeyError):
        return False


def stage3_repair_retry_matches_runtime(state: dict, runtime: dict) -> bool:
    """Route an admitted repair retry only to the exact failed-runtime class."""
    retry = state.get("stage3_repair_retry")
    if runtime.get("failure_kind") == "TESTER_TIMEFRAME_MISMATCH":
        return True
    try:
        zero_deliveries = int(runtime.get("start_button_delivery_count", -1)) == 0
    except (TypeError, ValueError, OverflowError):
        zero_deliveries = False
    if (isinstance(retry, dict)
            and retry.get("repair_kind") == "stage3_exact_tester_binding"
            and runtime.get("status") == "INFRASTRUCTURE_FAILURE"
            and runtime.get("failure_kind") == "TESTER_JOURNAL_REJECTED"
            and runtime.get("infrastructure_only") is True
            and runtime.get("cycle_id") == state.get("cycle_id")
            and runtime.get("start_button_invoked") is False
            and zero_deliveries):
        journal = runtime.get("journal_diagnostic") or {}
        identities = journal.get("run_identity_lines") or []
        expected_identity = (
            f"{runtime.get('ea', '')}.ex5 on "
            f"{runtime.get('symbol', '')},{runtime.get('timeframe', '')}"
        ).casefold()
        runtime_path = Path(str(retry.get("runtime_manifest") or ""))
        try:
            terminal_pid = int(runtime.get("terminal_pid") or 0)
            worker_pids = {int(pid) for pid in runtime.get("worker_pids", []) if int(pid) > 0}
            worker_pids.update(int(item.get("pid") or 0)
                               for item in runtime.get("worker_process_evidence", [])
                               if isinstance(item, dict))
            launched = float(runtime.get("launched_epoch") or 0)
            no_live_processes = (terminal_pid > 0 and not psutil.pid_exists(terminal_pid)
                                 and not any(pid > 0 and psutil.pid_exists(pid)
                                             for pid in worker_pids)
                                 and (not runtime.get("watcher_pid")
                                      or not psutil.pid_exists(int(runtime["watcher_pid"])))
                                 and not launcher.fresh_local_workers(
                                     runtime.get("worker_baseline_pids", []), launched,
                                     expected_addresses=(runtime.get("agent_offsets_before") or {}).keys())
                                 and _matching_stage3_watcher(runtime_path) is False)
        except (OSError, TypeError, ValueError, psutil.Error, OverflowError):
            no_live_processes = False
        return bool(
            journal.get("code") == "NO_EXPERT_IN_START_CONFIG"
            and identities
            and all(expected_identity not in str(line).replace("/", "\\").casefold()
                    for line in identities)
            and not _has_timeframe_completion_evidence(runtime)
            and no_live_processes
        )
    return bool(isinstance(retry, dict)
                and retry.get("repair_kind") == "supervisor_retry_race"
                and runtime.get("status") == "INFRASTRUCTURE_FAILURE"
                and runtime.get("failure_kind") == "EIGHTCAP_TERMINAL_NOT_FRESH"
                and runtime.get("infrastructure_only") is True
                and runtime.get("cycle_id") == state.get("cycle_id"))


def stage2_parser_repair_requalified(state: dict) -> bool:
    """Allow a new Stage 3 one-run release only after this routed Stage 2 repair passed."""
    if (state.get("stage2_repair_source") != "stage3_opt_parse"
            or not authorizes_full_cycle_stage3(state)
            or state.get("current_stage") != "stage3_pending"
            or state.get("stage_status") != "stage2_complete_held"
            or state.get("hold_after_stage") != 2
            or state.get("next_action") != "audit_stage2_before_stage3"):
        return False
    try:
        failure_path = Path(state.get("stage3_failure_event") or "")
        stage2_event_path = Path(state.get("last_stage2_event") or "")
        verification_path = Path(state.get("last_stage2_verification") or "")
        if not all(path.is_file() for path in (failure_path, stage2_event_path, verification_path)):
            return False
        failure, event, verification = (load_object(failure_path), load_object(stage2_event_path),
                                        load_object(verification_path))
        return bool(
            failure.get("cycle_id") == state.get("cycle_id")
            and failure.get("stage") == 3 and failure.get("status") == "failed"
            and failure.get("rotation_eligible") is False
            and event.get("cycle_id") == state.get("cycle_id")
            and event.get("stage") == 2 and event.get("status") == "complete"
            and event.get("ea") == state.get("current_ea")
            and event.get("verification_sha256") == sha256(verification_path)
            and verification.get("cycle_id") == state.get("cycle_id")
            and verification.get("stage") == 2 and verification.get("passed") is True
            and verification.get("source_sha256") == event.get("mq5_sha256")
            and verification.get("stage1_spec_sha256") == event.get("stage1_spec_sha256")
        )
    except (OSError, ValueError, TypeError, KeyError):
        return False


def stage2_tester_grid_repair_requalified(state: dict) -> bool:
    """Authorize a fresh one-run Stage 3 release after the approved grid repair passes Stage 2."""
    prior_restart = state.get("stage3_restart_from_m5_authorization")
    prior_release = state.get("stage3_one_run_release_authorization")
    if not (
        state.get("current_stage") == "stage3_pending"
        and state.get("stage_status") == "stage2_complete_held"
        and state.get("hold_after_stage") == 2
        and state.get("next_action") == "audit_stage2_before_stage3"
        and state.get("stage2_repair_source") == "stage3_tester_journal"
        and (state.get("stage2_repair_journal") or {}).get("code") == "TESTER_INPUT_GRID_REJECTED"
        and isinstance(prior_restart, dict)
        and prior_restart.get("status") == "consumed"
        and prior_restart.get("authorized_by") == "user_confirmed"
        and prior_restart.get("cycle_id") == state.get("cycle_id")
        and isinstance(prior_release, dict)
        and prior_release.get("status") == "consumed"
        and prior_release.get("authorized_by") == "user_confirmed"
        and prior_release.get("scope") == "current_cycle_stage3_once"
        and prior_release.get("target_stage") == 3
        and prior_release.get("cycle_id") == state.get("cycle_id")
    ):
        return False
    try:
        runtime_path = Path(prior_restart["runtime_manifest"])
        runtime = load_object(runtime_path)
        stage3_handoff = load_object(HANDOFF)
        event_path = Path(state["last_stage2_event"])
        verification_path = Path(state["last_stage2_verification"])
        event, verification = load_object(event_path), load_object(verification_path)
        source = Path(str(state.get("candidate_source") or ""))
        diagnostic = runtime.get("journal_diagnostic") or {}
        return bool(
            runtime_path.resolve() == (STATE.parent / "stage3_v2" / f"{state['cycle_id']}.stage3.runtime.json").resolve()
            and runtime.get("cycle_id") == state.get("cycle_id")
            and runtime.get("ea") == state.get("current_ea")
            and runtime.get("timeframe") == "M15"
            and runtime.get("status") == "INFRASTRUCTURE_FAILURE"
            and runtime.get("failure_kind") == "TESTER_JOURNAL_REJECTED"
            and runtime.get("input_schema_binding", {}).get("status") == "INPUTS_BOUND"
            and diagnostic.get("code") == "TESTER_INPUT_GRID_REJECTED"
            and diagnostic == state.get("stage2_repair_journal")
            and stage3_handoff.get("cycle_id") == state.get("cycle_id")
            and stage3_handoff.get("ea") == state.get("current_ea")
            and Path(str(stage3_handoff.get("runtime_manifest") or "")).resolve() == runtime_path.resolve()
            and event.get("cycle_id") == state.get("cycle_id")
            and event.get("stage") == 2 and event.get("status") == "complete"
            and event.get("ea") == state.get("current_ea")
            and verification.get("cycle_id") == state.get("cycle_id")
            and verification.get("stage") == 2 and verification.get("passed") is True
            and verification.get("source_sha256") == event.get("mq5_sha256")
            and event.get("verification_sha256") == sha256(verification_path)
            and source.is_file() and sha256(source) == event.get("mq5_sha256")
            and sha256(source) != str(stage3_handoff.get("source_sha256") or "").upper()
            and state.get("stage2_repair_resolved_by") == str(event_path.resolve())
            and prior_restart.get("stage2_event_sha256")
                == (prior_release.get("evidence") or {}).get("stage2_event_sha256")
        )
    except (OSError, ValueError, TypeError, KeyError):
        return False


def stage2_release_attempts_for_current_publication(state: dict) -> int | None:
    """Count bounded Stage 3 release attempts against the current Stage 2 event."""
    try:
        event_path = Path(state.get("last_stage2_event") or "")
        if not event_path.is_file():
            return None
        event = load_object(event_path)
        for key in ("consumed", "consumed_utc", "consumed_action"):
            event.pop(key, None)
        event_hash = hashlib.sha256(json.dumps(
            event, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")).hexdigest().upper()
        attempts = list(state.get("stage2_release_attempt_history", []))
        current = state.get("stage2_release_attempt")
        if isinstance(current, dict):
            attempts.append(current)
        return sum(
            1 for attempt in attempts
            if isinstance(attempt, dict)
            and isinstance(attempt.get("evidence"), dict)
            and attempt["evidence"].get("stage2_event_sha256") == event_hash
        )
    except (OSError, ValueError, TypeError, KeyError):
        return None


def operator_one_run_stage3_release_is_current(state: dict) -> bool:
    """Authenticate the user's exact-cycle, Stage-3-only release approval."""
    authorization = state.get("stage3_one_run_release_authorization")
    if (not isinstance(authorization, dict)
            or authorization.get("status") != "authorized"
            or authorization.get("authorized_by") != "user_confirmed"
            or authorization.get("scope") != "current_cycle_stage3_once"
            or authorization.get("target_stage") != 3
            or authorization.get("cycle_id") != state.get("cycle_id")):
        return False
    try:
        probe = dict(state)
        probe["stage2_release_attempt"] = dict(authorization, status="attempt_in_progress")
        preparer.validate_stage2_release_authorization(
            probe, STAGE2_HANDOFF, authorization)
        return True
    except (OSError, ValueError, TypeError, KeyError):
        return False


def stage3_m5_restart_authorization_is_current(state: dict) -> bool:
    """Validate the user's one-run M5 restart against the exact failed Stage 3 attempt."""
    authorization = state.get("stage3_restart_from_m5_authorization")
    release = state.get("stage3_one_run_release_authorization")
    if (not isinstance(authorization, dict)
            or authorization.get("status") != "authorized"
            or authorization.get("authorized_by") != "user_confirmed"
            or authorization.get("cycle_id") != state.get("cycle_id")
            or not isinstance(release, dict)
            or release.get("status") != "authorized"
            or release.get("authorization_nonce") != authorization.get("authorization_nonce")):
        return False
    try:
        failure_path = Path(str(authorization.get("failure_event") or ""))
        runtime_path = Path(str(authorization.get("runtime_manifest") or ""))
        event_path = Path(str(state.get("last_stage2_event") or ""))
        backup_path = Path(str(authorization.get("state_backup") or ""))
        if not all(path.is_file() for path in (failure_path, runtime_path, event_path, backup_path)):
            return False
        failure, runtime, event = (load_object(path) for path in
                                   (failure_path, runtime_path, event_path))
        for key in ("consumed", "consumed_utc", "consumed_action"):
            event.pop(key, None)
        stable_event_hash = hashlib.sha256(json.dumps(
            event, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")).hexdigest().upper()
        return bool(
            state.get("current_stage") == "stage3_pending"
            and state.get("stage_status") == "stage2_complete_held"
            and state.get("hold_after_stage") == 2
            and state.get("next_action") == "audit_stage2_before_stage3"
            and failure.get("cycle_id") == state.get("cycle_id")
            and failure.get("stage") == 3 and failure.get("status") == "failed"
            and failure.get("rotation_eligible") is False
            and runtime.get("cycle_id") == state.get("cycle_id")
            and runtime.get("status") == "INFRASTRUCTURE_FAILURE"
            and runtime.get("infrastructure_only") is True
            and (runtime.get("journal_diagnostic") or {}).get("code") == "NO_EXPERT_IN_START_CONFIG"
            and authorization.get("failure_event_sha256") == sha256(failure_path)
            and authorization.get("runtime_manifest_sha256") == sha256(runtime_path)
            and authorization.get("state_backup_sha256") == sha256(backup_path)
            and authorization.get("stage2_event_sha256") == stable_event_hash
            and release.get("authorization_id", "").endswith(
                ":" + str(authorization.get("authorization_nonce")))
        )
    except (OSError, ValueError, TypeError, KeyError):
        return False


def should_attempt_stage2_release(state: dict) -> bool:
    authorized_full_run = authorizes_full_cycle_stage3(state)
    authorized_one_run = operator_one_run_stage3_release_is_current(state)
    authorized_requalified_grid_repair = stage2_tester_grid_repair_requalified(state)
    prior_attempt = state.get("stage2_release_attempt")
    prior_attempt_is_other_cycle = (
        prior_attempt is None
        or (isinstance(prior_attempt, dict)
            and bool(prior_attempt.get("cycle_id"))
            and prior_attempt.get("cycle_id") != state.get("cycle_id"))
    )
    current_publication_attempts = stage2_release_attempts_for_current_publication(state)
    retry_attempt_count = current_publication_attempts
    authorized_m5_restart = stage3_m5_restart_authorization_is_current(state)
    retryable_preflight = False
    if isinstance(prior_attempt, dict) and prior_attempt.get("status") == "failed":
        stage3_handoff = load_object(HANDOFF) if HANDOFF.is_file() else {}
        no_current_cycle_handoff = stage3_handoff.get("cycle_id") != state.get("cycle_id")
        if retry_attempt_count is None:
            history = state.get("stage2_release_attempt_history", [])
            retry_attempt_count = sum(
                1 for item in history
                if isinstance(item, dict) and item.get("cycle_id") == state.get("cycle_id")
            ) + int(prior_attempt.get("cycle_id") == state.get("cycle_id"))
        retryable_preflight = (
            (prior_attempt.get("failure_phase", "preflight" if prior_attempt.get("preflight_utc") else None)
             in {"preflight", "admission"}
             or prior_attempt.get("failure_detail")
             in {
                 "one-run Stage 2 release authorization authorization_id mismatch",
                 "one-run Stage 2 release authorization is required",
                 "one-run Stage 2 release authorization was already consumed or failed",
             })
            and retry_attempt_count < MAX_STAGE2_RELEASE_ATTEMPTS
            and no_current_cycle_handoff
        )
    grid_repair_attempts_available = (
        authorized_requalified_grid_repair
        and current_publication_attempts is not None
        and current_publication_attempts < MAX_STAGE2_RELEASE_ATTEMPTS
    )
    return (
        (state.get("cycle_id") == STAGE2_RELEASE_CYCLE_ID
         or authorized_full_run or authorized_one_run or grid_repair_attempts_available)
        and state.get("current_stage") == "stage3_pending"
        and state.get("stage_status") == "stage2_complete_held"
        and state.get("hold_after_stage") == 2
        and state.get("next_action") == "audit_stage2_before_stage3"
        and (prior_attempt_is_other_cycle
             or current_publication_attempts == 0
             or retryable_preflight
             or grid_repair_attempts_available
             or (authorized_m5_restart
                 and current_publication_attempts is not None
                 and current_publication_attempts < MAX_STAGE2_RELEASE_ATTEMPTS))
    )


def exact_unstarted_m5_runtime(runtime: dict, state: dict, prior_handoff: dict) -> bool:
    """Prove an idle Stage 3 M5 launch belongs to this cycle before recycling it."""
    try:
        command = [str(value) for value in runtime.get("command", [])]
        config_args = [value.split(":", 1)[1] for value in command
                       if value.casefold().startswith("/config:")]
        expected_terminal = Path(launcher.DEFAULT_TERMINAL).resolve()
        expected_worker = expected_terminal.with_name("metatester64.exe")
        old_m5_ini = prior_handoff.get("ini_files", {}).get("M5")
        if not (
            runtime.get("status") == "START_REQUIRED"
            and runtime.get("cycle_id") == state.get("cycle_id")
            and runtime.get("ea") == state.get("current_ea")
            and runtime.get("timeframe") == "M5"
            and runtime.get("start_button_attempted") is True
            and runtime.get("start_button_invoked") is False
            and not runtime.get("watcher_pid")
            and command and Path(command[0]).resolve() == expected_terminal
            and "/portable" in [value.casefold() for value in command]
            and old_m5_ini and len(config_args) == 1
            and Path(config_args[0]).resolve() == Path(old_m5_ini).resolve()
        ):
            return False
        known_workers = set()
        for item in runtime.get("worker_process_evidence", []):
            if not isinstance(item, dict) or not item.get("pid") or not item.get("address"):
                return False
            raw_command = item.get("command", [])
            raw_command = " ".join(str(value) for value in raw_command).casefold()
            if (Path(item.get("exe", "")).resolve() != expected_worker
                    or "/portable" not in raw_command
                    or f"/address:{str(item['address']).casefold()}" not in raw_command):
                return False
            known_workers.add(int(item["pid"]))
        for item in runtime.get("agent_logs", []):
            if isinstance(item, dict) and item.get("worker_pid"):
                known_workers.add(int(item["worker_pid"]))
        return all(int(pid) in known_workers for pid in runtime.get("worker_pids", []))
    except (OSError, TypeError, ValueError, KeyError):
        return False


def stage3_admission_hold(state: dict, release_authorization: dict | None = None) -> str | None:
    """Return a fail-closed hold reason when the canonical cycle bars Stage 3."""
    if (state.get("stage_status") == "stage2_complete_held"
            or state.get("next_action") == "audit_stage2_before_stage3"):
        attempt = state.get("stage2_release_attempt")
        authorized_full_run = authorizes_full_cycle_stage3(state)
        authorized_one_run = operator_one_run_stage3_release_is_current(state)
        valid_release = (
            isinstance(release_authorization, dict)
            and (state.get("cycle_id") == STAGE2_RELEASE_CYCLE_ID
                 or authorized_full_run or authorized_one_run
                 or stage2_tester_grid_repair_requalified(state))
            and state.get("current_stage") == "stage3_pending"
            and state.get("stage_status") == "stage2_complete_held"
            and state.get("hold_after_stage") == 2
            and state.get("next_action") == "audit_stage2_before_stage3"
            and release_authorization.get("cycle_id") == state.get("cycle_id")
            and release_authorization.get("target_stage") == 3
            and release_authorization.get("scope") == "current_cycle_stage3_once"
            and release_authorization.get("authorized_by") == "user_confirmed"
            and str(release_authorization.get("authorization_id", "")).startswith(
                f"{state.get('cycle_id')}:stage3:one-run:")
            and isinstance(attempt, dict)
            and attempt.get("authorization_id") == release_authorization.get("authorization_id")
            and attempt.get("status") == "attempt_in_progress"
        )
        if valid_release:
            return None
        # The supervisor may release this exact hold automatically after it
        # authenticates the current Stage 2 event, verification, source, EX5,
        # Hermes execution row, and cycle authorization. Recheck that same
        # provenance at the Stage 3 boundary so a cron tick can own the audit
        # without weakening the hold for unauthenticated or capped cycles.
        if authorizes_full_cycle_stage3(state):
            try:
                authenticate_stage2_execution(state)
                return None
            except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                pass
        return f"Stage 3 held pending explicit Stage 2 audit/release (hold_after_stage={state.get('hold_after_stage')})"
    if authenticated_consumed_stage2_release(state):
        return None
    hold_after = state.get("hold_after_stage")
    if hold_after is None:
        return None
    if isinstance(hold_after, bool) or not isinstance(hold_after, int):
        return f"invalid hold_after_stage={hold_after!r}; Stage 3 fails closed"
    if hold_after < 3:
        return f"hold_after_stage={hold_after} blocks Stage 3"
    return None


def _handoff_path(value: str) -> Path:
    # Copied Windows handoffs may contain JSON-escaped doubled separators.
    return Path(str(value).replace("\\\\", "\\"))


def timeframe_paths(handoff: dict, timeframe: str) -> tuple[Path, Path]:
    output_dir = _handoff_path(handoff["runtime_manifest"]).parent
    stem = f"{handoff['cycle_id']}.stage3.{timeframe}"
    return output_dir / f"{stem}.csv", output_dir / f"{stem}.parse.json"


def timeframe_compute_path(handoff: dict, timeframe: str) -> Path:
    output_dir = _handoff_path(handoff["runtime_manifest"]).parent
    return output_dir / f"{handoff['cycle_id']}.stage3.{timeframe}.compute.json"


def verification_path(handoff: dict) -> Path:
    decision = Path(handoff["decision"])
    return decision.parent.parent / "verification" / f"{handoff['cycle_id']}.stage3.verification.json"


def find_current_template(root: Path, ea: str) -> Path:
    candidates = sorted((root / "state" / "inis").glob(f"{ea}.ETHUSD.M5.*.wide*.ini"),
                        key=lambda path: path.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]
    generic = root / "state" / "kcos_genetic_opt_template.stage3-utf8.ini"
    require(generic.is_file(), f"no current-EA or canonical generic Stage 3 template exists for {ea}")
    return generic


def update_progress(state_path: Path, cycle_id: str, ea: str, stage_status: str, next_action: str) -> dict:
    state = load_object(state_path)
    require(state.get("schema_version") == 2 and state.get("cycle_id") == cycle_id, "canonical Stage 3 state identity mismatch")
    require(state.get("current_stage") == "stage3_running" and state.get("current_ea") == ea,
            "canonical state is not the active Stage 3 candidate")
    updated = dict(state)
    failure_detail = str(updated.get("stage3_failure_detail") or "")
    partial_match = re.match(r"Stage 3 (M5|M15|H1) produced a partial result;", failure_detail)
    if (updated.get("stage_status") == "stage3_infrastructure_failure" and partial_match
            and updated.get("stage3_failure_class") == "infrastructure"):
        timeframe = partial_match.group(1)
        receipt_path = (state_path.parent / "diagnostics" /
                        f"{cycle_id}.stage3.{timeframe}.full_ui_counter_reconciliation.json")
        failure_path = Path(str(updated.get("stage3_failure_event") or ""))
        if receipt_path.is_file() and failure_path.is_file():
            receipt = load_object(receipt_path)
            failure = load_object(failure_path)
            receipt_event_path = Path(str(receipt.get("event") or ""))
            receipt_opt_path = Path(str(receipt.get("opt_path") or ""))
            receipt_event = (load_object(receipt_event_path)
                             if receipt_event_path.is_file() else {})
            reconciled = bool(
                receipt.get("status") == "FULL_UI_COUNTER_RECONCILED"
                and receipt.get("cycle_id") == cycle_id and receipt.get("ea") == ea
                and receipt.get("timeframe") == timeframe
                and receipt_event_path.is_file()
                and sha256(receipt_event_path) == receipt.get("event_sha256")
                and receipt_event.get("cycle_id") == cycle_id
                and receipt_event.get("timeframe") == timeframe
                and stage3_ui_counter_proves_stopped_full(receipt_event)
                and stage3_completion_proves_full(receipt_event)
                and receipt_opt_path.is_file()
                and sha256(receipt_opt_path) == receipt.get("opt_sha256")
                and receipt_event.get("opt_sha256") == receipt.get("opt_sha256")
                and failure.get("cycle_id") == cycle_id and failure.get("stage") == 3
                and failure.get("status") == "failed"
                and failure.get("rotation_eligible") is False
                and failure.get("error") == failure_detail
                and failure.get("event_id") == f"{cycle_id}:stage3:failed"
            )
            if reconciled:
                history = list(updated.get("stage3_superseded_failures") or [])
                history.append({
                    "failure_event": str(failure_path.resolve()),
                    "failure_event_sha256": sha256(failure_path),
                    "failure_detail": failure_detail,
                    "superseded_by": str(receipt_path.resolve()),
                    "superseding_receipt_sha256": sha256(receipt_path),
                    "superseded_utc": utc_now(),
                })
                updated["stage3_superseded_failures"] = history[-5:]
                for key in ("stage3_failure_event", "stage3_failure_detail", "stage3_failure_class"):
                    updated.pop(key, None)
    updated.update({"stage_status": stage_status, "next_action": next_action, "updated_utc": utc_now()})
    atomic_json(state_path, updated)
    return updated


def prior_outcome_evidence(handoff: dict, current_timeframe: str) -> list[Path]:
    evidence = []
    for timeframe in TIMEFRAMES[:TIMEFRAMES.index(current_timeframe)]:
        _, parse_path = timeframe_paths(handoff, timeframe)
        compute_path = timeframe_compute_path(handoff, timeframe)
        available = [path for path in (parse_path, compute_path) if path.is_file()]
        require(len(available) == 1, "prior Stage 3 timeframe outcome evidence is missing or ambiguous")
        evidence.append(available[0])
    return evidence


def completed_evidence(handoff: dict, through_timeframe: str) -> list[Path]:
    _, current_parse = timeframe_paths(handoff, through_timeframe)
    require(current_parse.is_file(), "current Stage 3 parse evidence is missing")
    return [*prior_outcome_evidence(handoff, through_timeframe), current_parse]


def validate_existing_parse_evidence(evidence: dict, event_path: Path, runtime: dict) -> None:
    require(Path(evidence.get("timeframe_event", "")).resolve() == event_path.resolve(),
            "existing Stage 3 parser evidence points to a different completion event")
    bound_event_hash = evidence.get("timeframe_event_sha256")
    current_event_hash = sha256(event_path)
    if bound_event_hash != current_event_hash:
        current_event = load_object(event_path)
        # The supervisor records its continuation claim by changing only the
        # three scheduler-owned consumption fields. Validate the parse against
        # the exact pre-consumption event bytes, preserving all other fields.
        consumed_event_hash = None
        if (current_event.get("consumed") is True
                and current_event.get("consumed_action") == "continue_stage3"
                and current_event.get("consumed_utc")):
            unconsumed = dict(current_event)
            unconsumed["consumed"] = False
            unconsumed.pop("consumed_utc", None)
            unconsumed.pop("consumed_action", None)
            normalized_bytes = (json.dumps(unconsumed, indent=2) + "\n").encode("utf-8")
            consumed_event_hash = hashlib.sha256(normalized_bytes).hexdigest().upper()
        require(bound_event_hash == consumed_event_hash,
                "existing Stage 3 parser evidence does not bind the current completion event")
    ui = evidence.get("ui_completion_evidence", {})
    if ui.get("final") is True:
        screenshot = Path(ui.get("screenshot", ""))
        require(screenshot.is_file() and sha256(screenshot) == ui.get("screenshot_sha256"),
                "Stage 3 UI completion screenshot is missing or changed")
        require(int(ui.get("completed_passes", -1)) == int(ui.get("total_passes", -2)) > 0,
                "Stage 3 UI completion pass counts are not final")
        return
    log_path = Path(runtime.get("tester_log", ""))
    start = int(evidence.get("tester_log_start_offset", -1))
    end = int(evidence.get("tester_log_end_offset", -1))
    require(log_path.is_file() and start >= 0 and end > start and
            Path(evidence.get("tester_log", "")).resolve() == log_path.resolve(),
            "existing Stage 3 parser evidence lacks its fresh Tester Journal range")
    with log_path.open("rb") as handle:
        handle.seek(start)
        journal = handle.read(end - start)
    require(len(journal) == end - start and
            __import__("hashlib").sha256(journal).hexdigest().upper() == evidence.get("tester_log_sha256")
            == runtime.get("fresh_log_sha256"),
            "existing Stage 3 parser evidence has stale Tester Journal bytes")


def persist_compute_evidence(handoff: dict, event: dict, output_path: Path,
                             event_path: Path) -> dict:
    failure_mode = event.get("failure_mode") or event.get("completion_reason")
    require(failure_mode in {"compute_budget_exceeded", "compute_stalled"},
            "invalid Stage 3 compute failure mode")
    metrics = event.get("metrics") if isinstance(event.get("metrics"), dict) else {
        key: event.get(key) for key in ("completed_passes", "elapsed_seconds", "seconds_since_last_pass")
        if event.get(key) is not None
    }
    evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {
        "active_worker_pids": event.get("alive_worker_pids", []),
    }
    require(metrics.get("elapsed_seconds") is not None and metrics.get("completed_passes") is not None,
            "compute-limit event is missing required metrics")
    if failure_mode == "compute_stalled":
        budgets = evidence.get("budgets", {})
        stall_budget = float(budgets.get("progress_stall_seconds", 0))
        active_workers = evidence.get("active_worker_pids")
        stall_reason = event.get("completion_reason")
        workers_match_reason = (
            (stall_reason == "progress_stall_budget_exceeded_without_active_workers"
             and isinstance(active_workers, list) and not active_workers)
            or (stall_reason == "progress_stall_budget_exceeded_with_active_worker"
                and isinstance(active_workers, list) and bool(active_workers))
        )
        require(workers_match_reason and int(metrics.get("completed_passes", 0)) > 0
                and stall_budget > 0
                and float(metrics.get("seconds_since_last_pass", 0)) >= stall_budget,
                "compute-stall evidence lacks authenticated pass progress, worker state, or stall duration")
    elif failure_mode == "compute_budget_exceeded":
        projection = event.get("duration_projection", {})
        if event.get("duration_policy_action") == "skip_eta_over_4h":
            require(duration_policy.validate_eta_skip(
                        projection, evidence.get("tester_ui_timing", {}),
                        event.get("duration_stop") or evidence.get("duration_stop", {})),
                    "ETA skip lacks authenticated timer and confirmed-stop evidence")
        # Reduced-grid rotation is independently authenticated by the
        # persisted duration projection; it is not a normal compute-quality
        # admission.
        elif projection:
            require(projection.get("over_budget") is True and
                    float(projection.get("projected_seconds", 0)) > 14400,
                    "reduced-grid rotation lacks an over-budget projection")
    else:
        require(evaluator.validate_compute_metrics(failure_mode, metrics, evidence),
                "compute-limit event violates the two-threshold qualification policy")
    immutable = {
        "schema_version": 2, "cycle_id": handoff["cycle_id"], "stage": 3,
        "ea": handoff["ea"], "timeframe": event["timeframe"], "outcome": "COMPUTE_SKIPPED",
        "failure_mode": failure_mode, "metrics": metrics, "evidence": evidence,
        "source_event_id": event.get("event_id"), "completed_utc": event.get("completed_utc"),
        "source_event_sha256": sha256(event_path),
    }
    if event.get("duration_policy_action") == "skip_eta_over_4h":
        projection = event.get("duration_projection", {})
        timing = evidence.get("tester_ui_timing", {})
        stop = event.get("duration_stop") or evidence.get("duration_stop", {})
        require(duration_policy.validate_eta_skip(projection, timing, stop),
                "ETA skip lacks authenticated timer and confirmed-stop evidence")
        immutable.update({"duration_policy_action": "skip_eta_over_4h",
                          "duration_projection": projection, "duration_stop": stop})
    if output_path.is_file():
        existing = load_object(output_path)
        if existing != immutable:
            # Upgrade only this same event's earlier evidence record, retaining
            # the original bytes for audit. Never replace another cycle/result.
            require(existing.get("source_event_id") == immutable.get("source_event_id")
                    and all(existing.get(key) == immutable.get(key) for key in
                            ("schema_version", "cycle_id", "stage", "ea", "timeframe",
                             "outcome", "failure_mode", "metrics", "evidence", "completed_utc")),
                    "conflicting immutable Stage 3 compute evidence exists")
            archive = output_path.parent / "repair_archive"
            archive.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            backup = archive / f"{output_path.name}.pre_eta_provenance_{stamp}.bak"
            if not backup.exists():
                shutil.copy2(output_path, backup)
            atomic_json(output_path, immutable)
    else:
        atomic_json(output_path, immutable)
    require(load_object(output_path) == immutable, "immutable Stage 3 compute evidence write failed")
    return immutable


def stage3_completion_proves_full(event: dict) -> bool:
    """Accept either a complete Tester counter or a fresh, clean finish journal."""
    counter = event.get("ui_pass_counter")
    ui_proves_full = bool(
        event.get("ui_passes_complete") is True
        and event.get("completion_ratio") == 1.0
        and isinstance(counter, dict) and counter.get("ok") is True
        and counter.get("current") == counter.get("total")
    )
    completion = event.get("tester_log_completion_evidence") or {}
    start_offset = event.get("tester_log_start_offset")
    end_offset = event.get("tester_log_end_offset")
    byte_count = completion.get("byte_count")
    journal_bounds_valid = (
        isinstance(start_offset, int) and not isinstance(start_offset, bool) and start_offset >= 0
        and isinstance(end_offset, int) and not isinstance(end_offset, bool) and end_offset > start_offset
        and isinstance(byte_count, int) and not isinstance(byte_count, bool)
        and byte_count == end_offset - start_offset
    )
    genetic_journal_proves_full = bool(
        event.get("completion_basis") == "fresh_genetic_journal"
        and completion.get("early") is True
        and completion.get("final") is True
        and completion.get("cache_saved") is True
        and completion.get("clean") is True
        and not completion.get("error_lines")
        and journal_bounds_valid
        and isinstance(completion.get("sha256"), str)
        and re.fullmatch(r"[0-9A-Fa-f]{64}", completion["sha256"])
        and completion.get("sha256") == event.get("tester_log_sha256")
    )
    return ui_proves_full or genetic_journal_proves_full


def stage3_ui_counter_proves_stopped_full(event: dict) -> bool:
    """Require an exact full Tester counter and the bound UI's stopped state."""
    counter = event.get("ui_pass_counter")
    return bool(
        event.get("status") == "TIMEFRAME_COMPLETE"
        and event.get("ui_passes_complete") is True
        and event.get("completion_ratio") == 1.0
        and isinstance(counter, dict) and counter.get("ok") is True
        and isinstance(counter.get("current"), int) and not isinstance(counter.get("current"), bool)
        and counter.get("current", 0) > 0
        and counter.get("current") == counter.get("total")
        and counter.get("button_state") == "START"
    )


def reconcile_full_ui_counter_partial(runtime: dict, runtime_path: Path,
                                      handoff: dict) -> dict | None:
    """Reconcile a mislabeled 90% event only when current evidence proves 100%."""
    if runtime.get("status") != "PARTIAL_TIMEFRAME_COMPLETE":
        return None
    event_path = Path(str(runtime.get("timeframe_event") or ""))
    if not event_path.is_file():
        return None
    event = load_object(event_path)
    cycle, timeframe = handoff.get("cycle_id"), runtime.get("timeframe")
    if not (event.get("schema_version") == 2 and event.get("stage") == 3
            and event.get("cycle_id") == cycle and event.get("timeframe") == timeframe
            and event.get("completion_mode") == "partial_90_percent"
            and stage3_ui_counter_proves_stopped_full(event)
            and stage3_completion_proves_full(event)):
        return None

    opt_path = Path(str(runtime.get("opt_path") or "")).resolve()
    event_opt_path = Path(str(event.get("opt_path") or "")).resolve()
    opt_hash = str(event.get("opt_sha256") or "")
    require(opt_path.is_file() and event_opt_path == opt_path
            and opt_hash and opt_hash == runtime.get("opt_sha256") == sha256(opt_path),
            "full Tester counter event is not bound to the current Stage 3 OPT")
    opt_complete, _ = watcher.structurally_complete_opt(opt_path)
    try:
        launched_epoch = float(runtime.get("launched_epoch"))
    except (TypeError, ValueError):
        launched_epoch = 0.0
    require(opt_complete and launched_epoch > 0 and opt_path.stat().st_mtime >= launched_epoch
            and float(runtime.get("stable_seconds") or 0)
                >= int(runtime.get("normal_settle_seconds", 20)),
            "full Tester counter OPT is not fresh, structurally complete, and settled")

    completion = event.get("tester_log_completion_evidence") or {}
    start_offset, end_offset = event.get("tester_log_start_offset"), event.get("tester_log_end_offset")
    byte_count, log_hash = completion.get("byte_count"), event.get("tester_log_sha256")
    require(completion.get("clean") is True and not completion.get("error_lines")
            and isinstance(start_offset, int) and not isinstance(start_offset, bool)
            and isinstance(end_offset, int) and not isinstance(end_offset, bool)
            and isinstance(byte_count, int) and not isinstance(byte_count, bool)
            and byte_count > 0 and end_offset - start_offset == byte_count
            and isinstance(log_hash, str) and re.fullmatch(r"[0-9A-Fa-f]{64}", log_hash)
            and completion.get("sha256") == log_hash,
            "full Tester counter event lacks a clean, bounded Stage 3 log segment")
    log_path = Path(str(event.get("tester_log") or ""))
    require(log_path.is_file() and log_path.stat().st_size >= end_offset,
            "full Tester counter event log segment is no longer available")
    with log_path.open("rb") as handle:
        handle.seek(start_offset)
        log_segment = handle.read(byte_count)
    require(len(log_segment) == byte_count
            and hashlib.sha256(log_segment).hexdigest().upper() == log_hash.upper(),
            "full Tester counter event log segment hash does not match")

    receipt_path = (STATE.parent / "diagnostics" /
                    f"{cycle}.stage3.{timeframe}.full_ui_counter_reconciliation.json")
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt = {
        "schema_version": 1,
        "cycle_id": cycle,
        "ea": handoff.get("ea"),
        "timeframe": timeframe,
        "status": "FULL_UI_COUNTER_RECONCILED",
        "original_runtime_status": "PARTIAL_TIMEFRAME_COMPLETE",
        "event": str(event_path.resolve()),
        "event_sha256": sha256(event_path),
        "opt_path": str(opt_path),
        "opt_sha256": opt_hash,
        "tester_log_sha256": log_hash,
        "ui_pass_counter": event.get("ui_pass_counter"),
        "completion_ratio": event.get("completion_ratio"),
        "reconciled_utc": utc_now(),
    }
    if receipt_path.exists():
        existing = load_object(receipt_path)
        for key in ("schema_version", "cycle_id", "ea", "timeframe", "status", "event_sha256",
                    "opt_sha256", "tester_log_sha256", "ui_pass_counter", "completion_ratio"):
            require(existing.get(key) == receipt.get(key),
                    "existing full-counter reconciliation receipt conflicts with current evidence")
        receipt = existing
    else:
        atomic_json(receipt_path, receipt)

    runtime["status"] = "TIMEFRAME_COMPLETE"
    runtime["completion_mode"] = "full"
    runtime["completion_basis"] = "tester_pass_counter"
    runtime["completion_reason"] = "full_tester_pass_counter_reconciled_from_partial_label"
    runtime["full_ui_counter_reconciliation"] = {
        "receipt": str(receipt_path.resolve()),
        "receipt_sha256": sha256(receipt_path),
        "source_event_sha256": receipt["event_sha256"],
    }
    atomic_json(runtime_path, runtime)
    return receipt


def process_completed_timeframe(state_path: Path, handoff_path: Path,
                                launch_runner=launcher.launch_and_register) -> dict:
    handoff = load_object(handoff_path)
    require(handoff.get("schema_version") == 2 and handoff.get("stage") == 3, "Stage 3 handoff schema mismatch")
    state = load_object(state_path)
    require(state.get("cycle_id") == handoff.get("cycle_id") and state.get("current_ea") == handoff.get("ea"),
            "Stage 3 state/handoff identity mismatch")
    require(state.get("current_stage") == "stage3_running", "Stage 3 is not running")
    runtime_path = _handoff_path(handoff["runtime_manifest"])
    runtime = load_object(runtime_path)
    timeframe = runtime.get("timeframe")
    runtime_status = runtime.get("status")
    require(timeframe in TIMEFRAMES, "Stage 3 runtime timeframe is outside the authenticated contract")
    require(runtime.get("cycle_id") == handoff.get("cycle_id")
            and runtime.get("ea") == handoff.get("ea"),
            "Stage 3 runtime/handoff identity mismatch")
    startup_pending = (
        runtime_status == "OPTIMIZING"
        and str(runtime.get("start_required_reason", "")).startswith(
            "monitoring authenticated startup before workers/opt growth appear")
        and not runtime.get("tester_agent_progress")
        and not runtime.get("agent_logs")
        and not (runtime.get("journal_diagnostic") or {}).get("fresh_lines")
        and not (runtime.get("terminal_log_progress") or {}).get("authenticated")
        and not runtime.get("last_opt_growth_epoch")
    )
    if startup_pending:
        # Older launch code promoted a fresh but header-only OPT plus idle
        # workers to OPTIMIZING. Reclassify that exact unauthenticated startup
        # state so the normal bounded Start handshake can finish.
        runtime["status"] = "START_REQUIRED"
        atomic_json(runtime_path, runtime)
        runtime_status = "START_REQUIRED"
    if runtime_status in {"INFRASTRUCTURE_FAILURE", "WATCHER_REGISTRATION_FAILED"}:
        detail = (runtime.get("failure_detail") or runtime.get("watcher_failure")
                  or runtime.get("start_required_reason") or "Stage 3 infrastructure failure held")
        journal = runtime.get("journal_diagnostic") or {}
        if (runtime.get("failure_kind") == "TESTER_JOURNAL_REJECTED"
                and journal.get("code") == "TESTER_INPUT_GRID_REJECTED"):
            # A rejected Tester grid is terminal evidence. Release only the
            # exact terminal owned by this failed runtime before Stage 2 takes
            # the shared MT5 profile; cleanup preserves the final Journal.
            launcher.cleanup_runtime_manifest(runtime, runtime_path)
        current = load_object(state_path)
        if current.get("stage_status") != "stage3_infrastructure_failure":
            record_failure(state_path, str(detail))
        return {"status": "STAGE3_INFRASTRUCTURE_FAILURE", "timeframe": timeframe,
                "runtime": str(runtime_path.resolve()), "reason": detail,
                "zero_token": True}
    # OPTIMIZING polls use authenticated file journals. START_REQUIRED passes
    # enter the bounded launcher handshake, which selects/captures the Tester
    # Journal after Start and restores the user's prior foreground window.
    if runtime_status == "START_REQUIRED":
        resumed = launcher.resume_start_required(handoff_path, runtime_path)
        if resumed.get("status") != "OPTIMIZING":
            terminal_progress = resumed.get("terminal_log_progress") or {}
            progress_age = terminal_progress.get("progress_age_seconds")
            progress_stall_budget = max(900, int(resumed.get("progress_stall_budget_seconds", 900)))
            terminal_progress_active = bool(
                terminal_progress.get("authenticated")
                and isinstance(progress_age, (int, float))
                and progress_age <= progress_stall_budget
            )
            if terminal_progress_active:
                eta_observation = watcher.observe_start_required_eta(resumed, runtime_path)
                if eta_observation.get("status") == COMPUTE_LIMIT_STATUS:
                    runtime = eta_observation
                    runtime_status = COMPUTE_LIMIT_STATUS
                    resumed = eta_observation
                else:
                    update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                                    f"stage3_{timeframe.lower()}_running",
                                    f"watch_stage3_{timeframe.lower()}")
                    return {"status": "STAGE3_WAITING", "timeframe": timeframe,
                            "runtime": str(runtime_path.resolve()),
                            "reason": ("launch-bound MT5 log confirms automatic testing is running; "
                                       "Tester UI sampled at most every five minutes; "
                                       f"latest reported progress {terminal_progress.get('latest_percent')}%; "
                                       "waiting for Tester journal/OPT evidence"),
                            "terminal_log_progress": terminal_progress, "zero_token": True}
            agent_progress = resumed.get("tester_agent_progress", {})
            if (runtime_status != COMPUTE_LIMIT_STATUS
                    and agent_progress.get("source") == "authenticated_fresh_local_agent_logs"
                    and agent_progress.get("cycle_id") == handoff.get("cycle_id")
                    and agent_progress.get("ea") == handoff.get("ea")
                    and agent_progress.get("symbol") == handoff.get("symbol")
                    and agent_progress.get("timeframe") == timeframe
                    and agent_progress.get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
                    and agent_progress.get("evidence")):
                update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                                f"stage3_{timeframe.lower()}_running",
                                f"watch_stage3_{timeframe.lower()}")
                return {"status": "STAGE3_WAITING", "timeframe": timeframe,
                        "runtime": str(runtime_path.resolve()),
                        "reason": "authenticated worker logs show optimization passes; waiting for parent journal/OPT",
                        "zero_token": True}
            if resumed.get("status") == "START_REQUIRED":
                try:
                    launched = float(resumed.get("launched_epoch"))
                except (TypeError, ValueError):
                    launched = 0.0
                grace = max(300, int(resumed.get("startup_grace_seconds", 300)))
                # Terminal initialization and exact Tester binding can consume
                # most of the launch window.  Once this runtime has an
                # authenticated input schema and a delivered Start click, the
                # startup grace belongs to that click, not to process creation.
                # Otherwise a late but valid Start is declared failed before
                # MT5 has had a meaningful chance to emit Tester evidence.
                start_epoch = launched
                binding = resumed.get("input_schema_binding") or {}
                try:
                    click_epoch = float(resumed.get("start_button_last_click_epoch") or 0)
                except (TypeError, ValueError, OverflowError):
                    click_epoch = 0.0
                if (resumed.get("start_button_invoked") is True
                        and binding.get("status") == "INPUTS_BOUND"
                        and click_epoch >= launched > 0):
                    start_epoch = click_epoch
                journal_progress = bool((resumed.get("journal_diagnostic") or {}).get("fresh_lines"))
                terminal_log_progress = bool(
                    (resumed.get("terminal_log_progress") or {}).get("authenticated")
                    and isinstance((resumed.get("terminal_log_progress") or {}).get("progress_age_seconds"), (int, float))
                    and (resumed.get("terminal_log_progress") or {}).get("progress_age_seconds")
                        <= max(900, int(resumed.get("progress_stall_budget_seconds", 900)))
                )
                opt_progress = float(resumed.get("last_opt_growth_epoch") or 0) >= launched > 0
                if (start_epoch and time.time() - start_epoch >= grace and not journal_progress
                        and not terminal_log_progress and not opt_progress):
                    if (resumed.get("terminal_log_progress") or {}).get("authenticated"):
                        latest = (resumed.get("terminal_log_progress") or {}).get("latest_percent")
                        detail = (f"Stage 3 {timeframe} terminal log showed optimization at {latest}%, "
                                  "but no fresh terminal progress, agent-pass, Tester Journal, or growing OPT "
                                  "arrived within the progress stall window")
                    else:
                        detail = (f"Stage 3 {timeframe} Start handoff produced no authenticated tester, "
                                  "agent-pass, or growing-OPT progress within its startup grace")
                    binding = resumed.get("input_schema_binding") or {}
                    input_binding_exhausted = bool(
                        binding.get("status") == "EXPERT_NOT_READY"
                        and int(resumed.get("start_button_attempt_count", 0)) >= 8
                        and int(resumed.get("start_button_delivery_count", -1)) == 0
                        and resumed.get("start_button_invoked") is False
                        and not resumed.get("worker_pids")
                        and not resumed.get("worker_process_evidence")
                        and not resumed.get("watcher_pid"))
                    if input_binding_exhausted:
                        # After the bounded exact-EA/Navigator attempts, this
                        # terminal has no tester run to preserve. Mark the
                        # authenticated runtime as infrastructure-held and
                        # close only its exact owned PID before recording the
                        # failure receipt. A later one-shot retry can then use
                        # the authenticated ExpertParameters profile.
                        resumed.update({
                            "status": "INFRASTRUCTURE_FAILURE",
                            "failure_kind": "EXPERT_INPUT_BINDING_UNAVAILABLE",
                            "infrastructure_only": True,
                            "failure_detail": detail,
                            "failed_utc": utc_now(),
                        })
                        atomic_json(runtime_path, resumed)
                        command = resumed.get("command") or []
                        require(bool(command), "unbound Stage 3 runtime lacks terminal command identity")
                        launcher.cleanup_previous_runtime(
                            resumed, Path(command[0]), runtime_path)
                    record_failure(state_path, detail)
                    return {"status": "STAGE3_INFRASTRUCTURE_FAILURE", "timeframe": timeframe,
                            "runtime": str(runtime_path.resolve()), "failure_class": "infrastructure",
                            "reason": detail, "zero_token": True}
            if resumed.get("status") in {"INFRASTRUCTURE_FAILURE", "WATCHER_REGISTRATION_FAILED"}:
                detail = resumed.get("failure_detail") or resumed.get("watcher_failure") or resumed.get("start_required_reason")
                record_failure(state_path, str(detail or "Stage 3 start handoff infrastructure failure"))
                if (resumed.get("failure_kind") == "TESTER_JOURNAL_REJECTED"
                        and (resumed.get("journal_diagnostic") or {}).get("code") == "TESTER_INPUT_GRID_REJECTED"):
                    repaired = load_object(state_path)
                    repaired.update({
                        "current_stage": "stage2_pending",
                        "stage_status": "stage2_repair_required",
                        "hold_after_stage": 2,
                        "next_action": "dispatch_stage2",
                        "stage2_repair_reason": str(detail),
                        "stage2_repair_source": "stage3_tester_journal",
                        "stage2_repair_journal": resumed.get("journal_diagnostic"),
                        "updated_utc": utc_now(),
                    })
                    atomic_json(state_path, repaired)
                return {"status": "STAGE3_INFRASTRUCTURE_FAILURE", "timeframe": timeframe,
                        "runtime": str(runtime_path.resolve()), "reason": detail, "zero_token": True}
            if runtime_status != COMPUTE_LIMIT_STATUS:
                return {"status": "START_REQUIRED", "timeframe": timeframe,
                        "runtime": str(runtime_path.resolve()),
                        "desktop_handoff": resumed.get("desktop_handoff"),
                        "reason": resumed.get("start_required_reason"), "zero_token": True}
        runtime = resumed
        runtime_status = runtime.get("status")
        update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                        f"stage3_{timeframe.lower()}_running", f"watch_stage3_{timeframe.lower()}")
    if runtime_status == "STARTING" and not runtime.get("terminal_pid"):
        detail = "stale STARTING manifest has no authenticated EightCap terminal PID; archive and review before retry"
        record_failure(state_path, detail)
        return {"status": "STAGE3_INFRASTRUCTURE_FAILURE", "timeframe": timeframe,
                "runtime": str(runtime_path.resolve()), "failure_class": "infrastructure",
                "reason": detail, "zero_token": True}
    if runtime_status == "OPTIMIZING":
        # Preserve a live launch while its manifest is being reconciled. A
        # polling tick must never turn missing observation fields into cleanup.
        monitor_only_recovery = watcher.validate_monitor_only_recovery(runtime)
        if not runtime.get("opt_path") or (not runtime.get("worker_pids") and not monitor_only_recovery):
            try:
                launched = float(runtime.get("launched_epoch"))
            except (TypeError, ValueError):
                try:
                    launched = datetime.fromisoformat(
                        str(runtime["launched_utc"]).replace("Z", "+00:00")
                    ).timestamp()
                except (KeyError, TypeError, ValueError):
                    launched = None
            grace = max(0, int(runtime.get("startup_grace_seconds", 300)))
            if launched is not None and time.time() - launched > grace:
                detail = (f"Stage 3 {timeframe} runtime registration stayed incomplete beyond "
                          f"its {grace}-second startup grace; runtime preserved for review")
                record_failure(state_path, detail)
                return {"status": "STAGE3_INFRASTRUCTURE_FAILURE", "timeframe": timeframe,
                        "runtime": str(runtime_path.resolve()), "failure_class": "infrastructure",
                        "reason": detail, "zero_token": True}
            return {"status": "STAGE3_WAITING", "timeframe": timeframe,
                    "runtime": str(runtime_path.resolve()),
                    "reason": "active runtime registration is incomplete; preserve and reconcile next tick",
                    "zero_token": True}
        heartbeat_path = _handoff_path(handoff.get("heartbeat", str(ROOT / "state" / "kcos_stage3_heartbeat_v2.json")))
        # A registered background watcher owns UI observations and writes to
        # this runtime. Do not run a second observer from the periodic cron
        # tick: concurrent atomic replacements can fail on Windows and lose
        # the terminal event. Recover inline only when no exact watcher exists.
        if runtime.get("watcher_pid"):
            watcher_match = _matching_stage3_watcher(runtime_path)
            if watcher_match is True:
                return {"status": "STAGE3_WAITING", "timeframe": timeframe,
                        "runtime": str(runtime_path.resolve()),
                        "reason": "registered Stage 3 watcher owns UI sampling and runtime writes",
                        "zero_token": True}
            if watcher_match is None:
                return {"status": "STAGE3_WAITING", "timeframe": timeframe,
                        "runtime": str(runtime_path.resolve()),
                        "reason": "watcher ownership is ambiguous; preserving runtime without concurrent UI sampling",
                        "zero_token": True}
        try:
            observed = watcher.run_once(runtime_path, heartbeat_path, max_passes=int(runtime.get("max_passes", 100000000)))
        except (KeyError, FileNotFoundError, ValueError) as exc:
            return {"status": "STAGE3_WAITING", "timeframe": timeframe,
                    "runtime": str(runtime_path.resolve()),
                    "reason": f"watcher registration incomplete: {exc}",
                    "zero_token": True}
        if observed.get("status") == "DURATION_REDUCTION_REQUIRED":
            return {"status": "STAGE3_DURATION_REDUCTION_REQUIRED", "timeframe": timeframe,
                    "runtime": str(runtime_path.resolve()),
                    "projection": observed.get("duration_projection"),
                    "reason": "default wide grid exceeded its current timeframe cap; preserving evidence for retry/hold decision",
                    "zero_token": True}
        return {"status": "STAGE3_WAITING", "timeframe": timeframe,
                "runtime": str(runtime_path.resolve()),
                "reason": "optimization is still in progress", "zero_token": True}
    if runtime_status == "DURATION_REDUCTION_REQUIRED":
        fits_wide_budget = duration_retry_fits_wide_budget(runtime)
        next_action = ("resume_stage3_wide_after_budget_update" if fits_wide_budget
                       else "review_stage3_wide_duration_limit")
        update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                        "stage3_duration_reduction_pending", next_action)
        return {"status": ("STAGE3_WIDE_DURATION_RETRY_READY" if fits_wide_budget
                            else "STAGE3_WIDE_DURATION_HOLD"), "timeframe": timeframe,
                "runtime": str(runtime_path.resolve()),
                "projection": runtime.get("duration_projection"),
                "reason": ("same authenticated wide INI fits the updated per-timeframe budget"
                           if fits_wide_budget else
                           "wide grid exceeds the bounded per-timeframe budget; preserve and review"),
                "zero_token": True}
    if runtime_status == "OPTIMIZATION_STOPPED":
        # Reconcile a previously classified stop once against the fresh
        # launch-bound journal and finalized OPT. This recovers clean genetic
        # finishes whose GUI estimate did not reach its displayed total while
        # preserving the existing hold for genuine user stops or failures.
        heartbeat_path = _handoff_path(
            handoff.get("heartbeat", str(ROOT / "state" / "kcos_stage3_heartbeat_v2.json")))
        watcher.run_once(runtime_path, heartbeat_path,
                         max_passes=int(runtime.get("max_passes", 100000000)))
        runtime = load_object(runtime_path)
        runtime_status = runtime.get("status")
        if runtime_status == "OPTIMIZATION_STOPPED":
            return {"status": "STAGE3_STOPPED", "timeframe": timeframe,
                    "runtime": str(runtime_path.resolve()),
                    "reason": runtime.get("completion_reason") or
                              "tester GUI shows Start without an authenticated full completion receipt",
                    "zero_token": True}
    if (runtime_status == "COMPUTE_BUDGET_EXCEEDED"
            and runtime.get("completion_reason") == "timeframe_budget_exceeded"
            and runtime.get("duration_policy_action") != "rotate_reduced_over_budget"):
        return {"status": "STAGE3_STOPPED", "timeframe": timeframe,
                "runtime": str(runtime_path.resolve()),
                "reason": "legacy watcher timeout classification held pending fresh GUI evidence",
                "zero_token": True}
    if runtime_status == "PARTIAL_TIMEFRAME_COMPLETE":
        try:
            reconciled = reconcile_full_ui_counter_partial(runtime, runtime_path, handoff)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            reconciled = None
            reconciliation_error = str(exc)
        else:
            reconciliation_error = None
        if reconciled is not None:
            runtime_status = "TIMEFRAME_COMPLETE"
        else:
            detail = (f"Stage 3 {timeframe} produced a partial result; the timeframe must be fully "
                      "completed and parsed before advancing")
            if reconciliation_error:
                detail += f"; full-counter reconciliation was rejected: {reconciliation_error}"
            record_failure(state_path, detail)
            return {"status": "STAGE3_FAILED_HELD", "timeframe": timeframe,
                    "failure_class": "infrastructure", "runtime": str(runtime_path.resolve()),
                    "reason": detail, "zero_token": True}
    require(timeframe in TIMEFRAMES and runtime_status in {"TIMEFRAME_COMPLETE", "PARTIAL_TIMEFRAME_COMPLETE", COMPUTE_LIMIT_STATUS},
            "no completed Stage 3 timeframe or compute-limit event is ready")
    # UI is the authority for a duration-budget decision. Never consume a
    # compute event as a rotation when the Tester observation is unavailable.
    if (runtime_status == COMPUTE_LIMIT_STATUS
            and runtime.get("duration_policy_action") == "rotate_reduced_over_budget"):
        ui = runtime.get("tester_ui_observation") or runtime.get("journal_ui_last") or {}
        terminal_pid = runtime.get("terminal_pid")
        terminal_alive = isinstance(terminal_pid, int) and terminal_pid > 0 and psutil.pid_exists(terminal_pid)
        if ui.get("available") is not True and terminal_alive:
            return {"status": "STAGE3_UI_EVIDENCE_REQUIRED", "timeframe": timeframe,
                    "runtime": str(runtime_path.resolve()),
                    "reason": "reduced-grid budget event held: Tester UI evidence unavailable",
                    "ui_observation": ui, "zero_token": True}
    event_path = Path(runtime["timeframe_event"])
    if runtime_status in {"TIMEFRAME_COMPLETE", "PARTIAL_TIMEFRAME_COMPLETE"} and not event_path.is_file():
        # A prior watcher may have settled the timeframe but failed while
        # snapshotting an OPT after a same-cycle retry. Re-run the zero-token
        # observer once to publish the event from the preserved runtime/log;
        # it will use a content-bound snapshot name and retain older attempts.
        heartbeat_path = _handoff_path(
            handoff.get("heartbeat", str(ROOT / "state" / "kcos_stage3_heartbeat_v2.json")))
        watcher.run_once(runtime_path, heartbeat_path,
                         max_passes=int(runtime.get("max_passes", 100000000)))
        runtime = load_object(runtime_path)
        require(event_path.is_file(),
                "Stage 3 completion observer did not reconstruct its missing timeframe event")
    event = load_object(event_path)
    if runtime_status == COMPUTE_LIMIT_STATUS:
        require(event.get("schema_version") == 2 and event.get("stage") == 3 and
                event.get("cycle_id") == handoff["cycle_id"] and event.get("timeframe") == timeframe,
                "compute-limit event identity mismatch")
        if (event.get("failure_mode") == "compute_stalled"
                or event.get("completion_reason") == "progress_stall_budget_exceeded_with_active_worker"):
            active_workers = (event.get("evidence") or {}).get("active_worker_pids", [])
            worker_state = ("active tester workers remain" if active_workers
                            else "no active tester workers remain")
            detail = (f"Stage 3 {timeframe} made no pass progress for its configured stall budget; "
                      f"{worker_state}. The timeframe is incomplete and must not be skipped")
            compute_path = timeframe_compute_path(handoff, timeframe)
            persist_compute_evidence(handoff, event, compute_path, event_path)
            launcher.cleanup_runtime_manifest(runtime, runtime_path)
            held_state = load_object(state_path)
            failure_path = Path(str(held_state.get("stage3_failure_event") or ""))
            already_recorded = bool(
                held_state.get("stage_status") == "stage3_infrastructure_failure"
                and held_state.get("stage3_failure_detail") == detail
                and failure_path.is_file()
            )
            if not already_recorded:
                record_failure(state_path, detail)
            return {"status": "STAGE3_FAILED_HELD", "timeframe": timeframe,
                    "failure_class": "infrastructure", "event": str(event_path.resolve()),
                    "compute_evidence": str(compute_path.resolve()),
                    "reason": detail, "zero_token": True}
        if runtime.get("duration_policy_action") == "skip_eta_over_4h":
            projection = event.get("duration_projection") or runtime.get("duration_projection") or {}
            stop = event.get("duration_stop") or runtime.get("duration_stop") or {}
            timing = (event.get("evidence") or {}).get("tester_ui_timing") or {}
            checkpoint = projection.get("checkpoint_seconds")
            expected_reason = (
                "eta_over_4h_after_4m" if checkpoint == 240
                else "eta_over_4h_after_10m" if checkpoint == duration_policy.UI_ETA_CHECKPOINT_SECONDS
                else None
            )
            # Preserve this cycle's already-written event: an earlier watcher
            # emitted the 4-minute label with a 600-second checkpoint. The
            # authenticated timer and persisted checkpoint remain authoritative.
            legacy_mislabeled_checkpoint = (
                checkpoint == duration_policy.UI_ETA_CHECKPOINT_SECONDS
                and event.get("completion_reason") == "eta_over_4h_after_4m"
            )
            require(
                expected_reason is not None
                and (event.get("completion_reason") == expected_reason
                     or legacy_mislabeled_checkpoint)
                and duration_policy.validate_eta_skip(projection, timing, stop),
                "over-four-hour ETA skip lacks authenticated timer/stop evidence",
            )
        if runtime.get("duration_policy_action") == "rotate_reduced_over_budget":
            event["duration_policy_action"] = "rotate_reduced_over_budget"
            event["duration_projection"] = runtime.get("duration_projection") or event.get("duration_projection")
        require(event.get("status") == COMPUTE_LIMIT_STATUS and event.get("consumed") is not True,
                "compute-limit event is absent or already consumed")
        compute_path = timeframe_compute_path(handoff, timeframe)
        persist_compute_evidence(handoff, event, compute_path, event_path)
        launcher.cleanup_runtime_manifest(runtime, runtime_path)
        if runtime.get("duration_policy_action") == "rotate_reduced_over_budget":
            rotated = load_object(state_path)
            rotated.update({
                "stage_status": "stage3_infrastructure_failure",
                "next_action": "rotate_stage3_budget_rejection",
                "stage3_failure_event": str(compute_path.resolve()),
                "last_stage3_event": str(compute_path.resolve()),
                "stage3_failure_detail": "Reduced grid projected beyond timeframe budget",
                "updated_utc": utc_now(),
            })
            atomic_json(state_path, rotated)
            import kcos_rotation_v2 as rotation
            result = rotation.prepare_next_cycle(state_path, HANDOFF)
            return {"status": "ROTATED_TO_STAGE1", "timeframe": timeframe,
                    "event": str(compute_path.resolve()), "rotation": result,
                    "zero_token": True}
        update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                        f"stage3_{timeframe.lower()}_compute_skipped", f"evaluate_stage3_{timeframe.lower()}")
        evidence = [*prior_outcome_evidence(handoff, timeframe), compute_path]
        metrics = event.get("metrics", {})
        # A censored zero-pass run is an infrastructure failure, not a
        # skippable timeframe.  Preserve the compute evidence and rotate the
        # candidate; never advance Stage 3 or Stage 4.5 from no-pass telemetry.
        if (int(metrics.get("completed_passes", 0)) == 0
                and metrics.get("compute_qualification") == "censored_no_first_pass"):
            detail = "Stage 3 compute budget exceeded before first authenticated pass"
            record_failure(state_path, detail)
            failed_state = load_object(state_path)
            failed_state.update({
                "current_stage": "stage3_running",
                "stage_status": "stage3_infrastructure_failure",
                "next_action": "inspect_stage3_infrastructure_failure",
                "hold_after_stage": 5 if authorizes_full_cycle_stage3(failed_state) else 3,
                "stage3_failure_event": str(compute_path.resolve()),
                "stage3_failure_detail": detail,
                "updated_utc": utc_now(),
            })
            atomic_json(state_path, failed_state)
            return {"status": "STAGE3_FAILED_HELD", "timeframe": timeframe,
                    "failure_class": "infrastructure", "event": str(compute_path.resolve()),
                    "zero_token": True}
        decision_path = Path(handoff["decision"])
        decision = evaluator.evaluate(handoff_path, evidence, decision_path)
        if decision["verdict"] == "NEXT_TIMEFRAME":
            next_timeframe = decision["next_timeframe"]
            next_runtime = launch_runner(handoff_path, next_timeframe)
            if next_runtime.get("status") == "START_REQUIRED":
                update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                                "stage3_start_required", f"desktop_start_stage3_{next_timeframe.lower()}")
                return {"status": "START_REQUIRED", "timeframe": next_timeframe,
                        "runtime": str(handoff.get("runtime_manifest") or runtime_path),
                        "desktop_handoff": next_runtime.get("desktop_handoff"),
                        "reason": next_runtime.get("start_required_reason"), "zero_token": True}
            update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                            f"stage3_{next_timeframe.lower()}_running", f"watch_stage3_{next_timeframe.lower()}")
            return {"status": "NEXT_TIMEFRAME_STARTED", "timeframe": timeframe,
                    "outcome": "COMPUTE_SKIPPED", "next_timeframe": next_timeframe,
                    "runtime": str(runtime_path.resolve()), "watcher_pid": next_runtime.get("watcher_pid"),
                    "zero_token": True}
        verify_path = verification_path(handoff)
        verification = verifier.verify(handoff_path, decision_path, verify_path)
        require(verification.get("passed") is True, "deterministic Stage 3 compute decision verification failed")
        if decision["verdict"] == "REPAIR_REQUIRED":
            launcher.cleanup_runtime_manifest(runtime, runtime_path)
            if LEASE.is_file() and load_object(LEASE).get("cycle_id") == handoff["cycle_id"]:
                LEASE.unlink()
            if evaluator.requires_stage2_repair(decision.get("timeframes", {})):
                repaired_state = load_object(state_path)
                repaired_state.update({
                    "current_stage": "stage2_pending",
                    "stage_status": "stage2_repair_required",
                    "hold_after_stage": 2,
                    "next_action": "dispatch_stage2",
                    "stage2_repair_reason": decision.get("reason"),
                    "stage3_zero_trade_decision": str(decision_path.resolve()),
                    "stage3_zero_trade_verification": str(verify_path.resolve()),
                    "updated_utc": utc_now(),
                })
                atomic_json(state_path, repaired_state)
            else:
                update_progress(state_path, handoff["cycle_id"], handoff["ea"], "stage3_repair_required",
                                "diagnose_all_zero_trade_root_causes_then_repair")
            return {"status": "REPAIR_REQUIRED", "timeframe": timeframe,
                    "decision": str(decision_path.resolve()), "verification": str(verify_path.resolve()),
                    "zero_token": True}
        published = publisher.publish(
            state_path, decision_path, verify_path, Path(handoff["event"]), Path(handoff["failed_notes"]),
            Path(handoff["source_notes"]), Path(handoff["passing_notes"]),
            Path(handoff.get("lease", publisher.LEASE)),
        )
        # Results and publication are authoritative before cleanup.  Terminate
        # only the terminal/tester PIDs recorded in this runtime manifest.
        launcher.cleanup_runtime_manifest(runtime, runtime_path)
        return {"status": "STAGE3_COMPLETE_HELD", "timeframe": timeframe,
                "verdict": decision["verdict"],
                "event": published["event_id"], "zero_token": True}
    require(event.get("status") == "TIMEFRAME_COMPLETE",
            "timeframe completion event is absent or has an invalid status")
    require(event.get("cycle_id") == handoff["cycle_id"] and event.get("timeframe") == timeframe,
            "timeframe completion event identity mismatch")
    full_ui_counter = stage3_ui_counter_proves_stopped_full(event)
    require(event.get("completion_mode") == "full"
            or (event.get("completion_mode") == "partial_90_percent" and full_ui_counter),
            "partial Stage 3 completion cannot advance without an exact full Tester counter")
    if not stage3_completion_proves_full(event):
        # The watcher can publish the authenticated journal receipt after its
        # initial completion event. Refresh once before holding a clean finish.
        heartbeat_path = _handoff_path(
            handoff.get("heartbeat", str(ROOT / "state" / "kcos_stage3_heartbeat_v2.json")))
        watcher.run_once(runtime_path, heartbeat_path,
                         max_passes=int(runtime.get("max_passes", 100000000)))
        runtime = load_object(runtime_path)
        runtime_status = runtime.get("status")
        event_path = Path(runtime.get("timeframe_event") or event_path)
        require(runtime_status == "TIMEFRAME_COMPLETE" and event_path.is_file(),
                "completion observer did not refresh the full timeframe receipt")
        event = load_object(event_path)
        refreshed_full_ui_counter = stage3_ui_counter_proves_stopped_full(event)
        full_ui_counter = refreshed_full_ui_counter
        require(event.get("status") == "TIMEFRAME_COMPLETE"
                and event.get("cycle_id") == handoff["cycle_id"]
                and event.get("timeframe") == timeframe
                and (event.get("completion_mode") == "full"
                     or (event.get("completion_mode") == "partial_90_percent"
                         and refreshed_full_ui_counter)),
                "refreshed timeframe completion event identity or mode is invalid")
        require(stage3_completion_proves_full(event),
                "Stage 3 cannot advance without a full Tester counter or a fresh clean genetic-finish receipt")
    completion = event.get("tester_log_completion_evidence") or {}
    clean_journal_finish = bool(
        completion.get("final") is True and completion.get("cache_saved") is True
        and completion.get("clean") is True and not completion.get("error_lines")
    )
    require(clean_journal_finish or full_ui_counter,
            "Stage 3 completion lacks either a clean final Journal/cache receipt or a full stopped Tester counter")
    require(completion.get("clean") is True and not completion.get("error_lines"),
            "Stage 3 completion log segment is not clean")
    require(event.get("tester_log_sha256")
            and completion.get("sha256") == event.get("tester_log_sha256"),
            "Stage 3 completion Journal receipt hash is inconsistent")
    if event.get("completion_reason") == "max_pass_limit_reached":
        ratio = event.get("completion_ratio")
        require(isinstance(ratio, (int, float)) and 0.0 < float(ratio) <= 1.0
                and float(ratio) == 1.0,
                "pass-limit completion did not prove the Tester reached its full pass total")
    opt_path = Path(runtime["opt_path"])
    require(event.get("opt_sha256") == sha256(opt_path), "timeframe OPT hash changed before deterministic parsing")

    csv_path, evidence_path = timeframe_paths(handoff, timeframe)
    if evidence_path.is_file():
        existing = load_object(evidence_path)
        require(existing.get("cycle_id") == handoff["cycle_id"] and existing.get("timeframe") == timeframe,
                "conflicting Stage 3 parse evidence exists")
        require(existing.get("opt_sha256") == event["opt_sha256"] and Path(existing["csv"]).is_file()
                and existing.get("csv_sha256") == sha256(Path(existing["csv"])),
                "existing Stage 3 parse evidence is stale")
        validate_existing_parse_evidence(existing, event_path, runtime)
    else:
        try:
            parser.parse_opt(handoff_path, event_path, opt_path, csv_path, evidence_path, timeframe, runtime_path)
        except parser.OptimizationSurfaceMismatch as exc:
            return route_opt_surface_failure_to_stage2(
                state_path, handoff, runtime, timeframe, str(exc))
    update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                    f"stage3_{timeframe.lower()}_complete", f"evaluate_stage3_{timeframe.lower()}")

    evidence = completed_evidence(handoff, timeframe)
    decision_path = Path(handoff["decision"])
    decision = evaluator.evaluate(handoff_path, evidence, decision_path)
    if decision["verdict"] == "NEXT_TIMEFRAME":
        next_timeframe = decision["next_timeframe"]
        next_runtime = launch_runner(handoff_path, next_timeframe)
        if next_runtime.get("status") == "START_REQUIRED":
            update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                            "stage3_start_required", f"desktop_start_stage3_{next_timeframe.lower()}")
            return {"status": "START_REQUIRED", "timeframe": next_timeframe,
                    "runtime": str(handoff.get("runtime_manifest") or runtime_path),
                    "desktop_handoff": next_runtime.get("desktop_handoff"),
                    "reason": next_runtime.get("start_required_reason"), "zero_token": True}
        update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                        f"stage3_{next_timeframe.lower()}_running", f"watch_stage3_{next_timeframe.lower()}")
        return {"status": "NEXT_TIMEFRAME_STARTED", "timeframe": timeframe,
                "next_timeframe": next_timeframe, "runtime": str(runtime_path.resolve()),
                "watcher_pid": next_runtime.get("watcher_pid"), "zero_token": True}

    verify_path = verification_path(handoff)
    verification = verifier.verify(handoff_path, decision_path, verify_path)
    require(verification.get("passed") is True, "deterministic Stage 3 decision verification failed")
    if decision["verdict"] == "REPAIR_REQUIRED":
        launcher.cleanup_runtime_manifest(runtime, runtime_path)
        if LEASE.is_file() and load_object(LEASE).get("cycle_id") == handoff["cycle_id"]:
            LEASE.unlink()
        if evaluator.requires_stage2_repair(decision.get("timeframes", {})):
            repaired_state = load_object(state_path)
            repaired_state.update({
                "current_stage": "stage2_pending",
                "stage_status": "stage2_repair_required",
                "hold_after_stage": 2,
                "next_action": "dispatch_stage2",
                "stage2_repair_reason": decision.get("reason"),
                "stage3_zero_trade_decision": str(decision_path.resolve()),
                "stage3_zero_trade_verification": str(verify_path.resolve()),
                "updated_utc": utc_now(),
            })
            atomic_json(state_path, repaired_state)
        else:
            update_progress(state_path, handoff["cycle_id"], handoff["ea"], "stage3_repair_required",
                            "diagnose_all_zero_trade_root_causes_then_repair")
        return {"status": "REPAIR_REQUIRED", "timeframe": timeframe,
                "decision": str(decision_path.resolve()), "verification": str(verify_path.resolve()),
                "zero_token": True}

    published = publisher.publish(
        state_path, decision_path, verify_path, Path(handoff["event"]), Path(handoff["failed_notes"]),
        Path(handoff["source_notes"]), Path(handoff["passing_notes"]),
        Path(handoff.get("lease", publisher.LEASE)),
    )
    # Results and publication are authoritative before cleanup.  Terminate
    # only the terminal/tester PIDs recorded in this runtime manifest.
    launcher.cleanup_runtime_manifest(runtime, runtime_path)
    return {"status": "STAGE3_COMPLETE_HELD", "timeframe": timeframe,
            "verdict": decision["verdict"], "event": published["event_id"], "zero_token": True}


def start_stage3(state_path: Path, stage2_handoff_path: Path, handoff_path: Path,
                 lease_path: Path, output_dir: Path, launch_runner=launcher.launch_and_register,
                 release_authorization: dict | None = None) -> dict:
    state = load_object(state_path)
    admission_hold = stage3_admission_hold(state, release_authorization)
    require(admission_hold is None, admission_hold or "Stage 3 admission is held")
    retry_ready = stage3_repair_retry_is_ready(state)
    require(
        state.get("schema_version") == 2
        and state.get("current_stage") == "stage3_pending"
        or (
            state.get("schema_version") == 2
            and state.get("current_stage") == "stage3_running"
            and state.get("stage_status") == "stage3_infrastructure_failure"
            and state.get("next_action") == "inspect_stage3_infrastructure_failure"
            and retry_ready
        ),
        "deterministic Stage 3 start requires stage3_pending or an authenticated cycle-owned infrastructure hold",
    )
    if retry_ready:
        retry_record = dict(state["stage3_repair_retry"])
        retry_record.update({"status": "consumed", "consumed_utc": utc_now()})
        state["stage3_repair_retry"] = retry_record
        state["updated_utc"] = utc_now()
        atomic_json(state_path, state)
    project_root = state_path.parent.parent
    template = find_current_template(project_root, state["current_ea"])
    # A prior GUI handoff can leave a cycle-owned terminal alive in
    # START_REQUIRED without any tester workers. Reconcile that bounded stale
    # launch before prepare() acquires the lease; never touch an optimizing
    # runtime or a process lacking authenticated ownership evidence.
    prior_handoff = load_object(handoff_path) if handoff_path.is_file() else {}
    prior_stage2_release = (prior_handoff.get("stage2_release")
                            if authenticated_consumed_stage2_release(state) else None)
    existing_runtime_path = (_handoff_path(prior_handoff.get("runtime_manifest", ""))
                             if prior_handoff.get("runtime_manifest") else None)
    if existing_runtime_path and existing_runtime_path.is_file():
        existing_runtime = load_object(existing_runtime_path)
        if existing_runtime.get("status") in {"STARTING", "OPTIMIZING"}:
            terminal_pid = int(existing_runtime.get("terminal_pid") or 0)
            worker_alive = any(psutil.pid_exists(int(pid)) for pid in existing_runtime.get("worker_pids", []))
            if (terminal_pid and psutil.pid_exists(terminal_pid)) or worker_alive:
                return existing_runtime
        if (release_authorization is not None
                and existing_runtime.get("status") == "START_REQUIRED"):
            require(exact_unstarted_m5_runtime(existing_runtime, state, prior_handoff),
                    "refusing to recycle a Stage 3 runtime that is not the exact unstarted portable M5 handoff")
            launcher.cleanup_runtime_manifest(existing_runtime, existing_runtime_path)
        elif (existing_runtime.get("status") in {"START_REQUIRED", "INFRASTRUCTURE_FAILURE"}
              and not existing_runtime.get("worker_pids")
              and existing_runtime.get("infrastructure_only") is True
              and existing_runtime.get("cycle_id") == state.get("cycle_id")):
            launcher.cleanup_runtime_manifest(existing_runtime, existing_runtime_path)
    try:
        handoff = preparer.prepare(state_path, stage2_handoff_path, handoff_path, lease_path,
                                   template, output_dir, dry_run=False,
                                   grid_mode=state.get("stage3_grid_mode", "default"),
                                   release_authorization=release_authorization)
    except preparer.Stage3InputContractError as exc:
        failure = preparer.record_input_contract_failure(
            state_path, stage2_handoff_path, exc.code, str(exc))
        return {
            "status": "STAGE3_INPUT_CONTRACT_REJECTED",
            "event_id": failure.get("event_id"),
            "failure_event": load_object(state_path).get("stage3_failure_event"),
            "detail": str(exc),
            "zero_token": True,
        }
    if isinstance(prior_stage2_release, dict):
        handoff["stage2_release"] = prior_stage2_release
    # Store the actual lease in the handoff so isolated tests and alternate roots
    # never fall back to the production lease.
    handoff["lease"] = str(lease_path.resolve())
    atomic_json(handoff_path, handoff)
    # A Stage 2 repair reruns the same EA/timeframe. Archive prior immutable
    # completion/parse artifacts so the fresh .opt gets a fresh evidence chain.
    stamp = utc_now().replace(":", "").replace("-", "").replace("+", "")
    stale_paths = [
        output_dir / f"{handoff['cycle_id']}.stage3.M5.complete.json",
        output_dir / f"{handoff['cycle_id']}.stage3.M5.parse.json",
        output_dir / f"{handoff['cycle_id']}.stage3.M5.csv",
        _handoff_path(handoff["runtime_manifest"]),
        _handoff_path(handoff["runtime_manifest"] + ".watcher.ready.json"),
        _handoff_path(handoff["runtime_manifest"] + ".watcher.stderr.log"),
    ]
    for stale in stale_paths:
        if stale.is_file():
            archived = stale.with_name(stale.name + f".previous.{stamp}")
            require(not archived.exists(), f"Stage 3 stale artifact archive already exists: {archived}")
            stale.replace(archived)
    runtime = launch_runner(handoff_path, "M5")
    # The runtime manifest is authoritative. A launcher return can be stale
    # after a bounded retry; never admit Stage 3 from that return alone.
    manifest_path = Path(handoff["runtime_manifest"]).resolve()
    if manifest_path.is_file():
        manifest_runtime = load_object(manifest_path)
        if (manifest_runtime.get("status") in {"INFRASTRUCTURE_FAILURE", "WATCHER_REGISTRATION_FAILED"}
                or manifest_runtime.get("failure_kind") == "TESTER_JOURNAL_REJECTED"):
            runtime = manifest_runtime
    if runtime.get("status") in {"INFRASTRUCTURE_FAILURE", "WATCHER_REGISTRATION_FAILED"}:
        detail = runtime.get("failure_detail") or runtime.get("watcher_failure") or runtime.get("start_required_reason")
        record_failure(state_path, str(detail or "Stage 3 start handoff infrastructure failure"))
        if (runtime.get("failure_kind") == "TESTER_JOURNAL_REJECTED"
                and (runtime.get("journal_diagnostic") or {}).get("code") == "TESTER_INPUT_GRID_REJECTED"):
            repaired = load_object(state_path)
            repaired.update({
                "current_stage": "stage2_pending",
                "stage_status": "stage2_repair_required",
                "hold_after_stage": 2,
                "next_action": "dispatch_stage2",
                "stage2_repair_reason": str(detail),
                "stage2_repair_source": "stage3_tester_journal",
                "stage2_repair_journal": runtime.get("journal_diagnostic"),
                "updated_utc": utc_now(),
            })
            atomic_json(state_path, repaired)
        return {"status": "STAGE3_INFRASTRUCTURE_FAILURE", "timeframe": "M5",
                "runtime": handoff["runtime_manifest"], "reason": detail, "zero_token": True}
    if runtime.get("status") == "START_REQUIRED":
        update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                        "stage3_start_required", "desktop_start_stage3_m5")
        return {"status": "START_REQUIRED", "timeframe": "M5",
                "runtime": handoff["runtime_manifest"],
                "desktop_handoff": runtime.get("desktop_handoff"),
                "reason": runtime.get("start_required_reason"), "zero_token": True}
    update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                    "stage3_m5_running", "watch_stage3_m5")
    return {"status": "STAGE3_STARTED", "timeframe": "M5", "runtime": handoff["runtime_manifest"],
            "watcher_pid": runtime.get("watcher_pid"), "zero_token": True}


def retry_failed_timeframe(state_path: Path, handoff_path: Path,
                           launch_runner=launcher.launch_and_register) -> dict:
    """Retry only a failed timeframe after its hash-bound repair receipt is ready."""
    state = load_object(state_path)
    handoff = load_object(handoff_path)
    require(state.get("current_stage") == "stage3_running"
            and state.get("stage_status") == "stage3_infrastructure_failure"
            and state.get("cycle_id") == handoff.get("cycle_id"),
            "timeframe retry requires the held current Stage 3 cycle")
    require(stage3_repair_retry_is_ready(state),
            "timeframe retry requires an authenticated one-run repair receipt")
    runtime_path = _handoff_path(handoff.get("runtime_manifest", ""))
    runtime = load_object(runtime_path)
    retry = dict(state["stage3_repair_retry"])
    require(runtime.get("status") == "INFRASTRUCTURE_FAILURE"
            and stage3_repair_retry_matches_runtime(state, runtime)
            and runtime.get("infrastructure_only") is True
            and runtime.get("cycle_id") == handoff.get("cycle_id")
            and runtime.get("ea") == handoff.get("ea"),
            "timeframe retry is not bound to a failed current-cycle MT5 timeframe")
    timeframe = str(runtime.get("timeframe", ""))
    require(timeframe in handoff.get("timeframe_order", []),
            "failed timeframe is outside the authenticated Stage 3 order")
    retry_record = dict(retry)
    retry_record.update({"status": "consumed", "consumed_utc": utc_now()})
    state["stage3_repair_retry"] = retry_record
    state.update({"current_stage": "stage3_running",
                  "stage_status": f"stage3_{timeframe.lower()}_running",
                  "next_action": f"watch_stage3_{timeframe.lower()}",
                  "stage3_retry_launch_in_progress": {
                      "cycle_id": state.get("cycle_id"), "timeframe": timeframe,
                      "repair_kind": retry.get("repair_kind"), "status": "launching",
                      "prior_runtime_sha256": retry.get("runtime_manifest_sha256"),
                      "failure_event_sha256": retry.get("failure_event_sha256"),
                      "started_utc": utc_now()}})
    state["updated_utc"] = utc_now()
    atomic_json(state_path, state)
    try:
        result = launch_runner(handoff_path, timeframe)
        require(isinstance(result, dict) and runtime_path.is_file(),
                "repair retry launcher returned no fresh runtime")
        refreshed = load_object(runtime_path)
        require(refreshed.get("cycle_id") == state.get("cycle_id")
                and refreshed.get("ea") == handoff.get("ea")
                and refreshed.get("timeframe") == timeframe
                and refreshed.get("status") == result.get("status")
                and refreshed.get("status") in {"START_REQUIRED", "STARTING", "OPTIMIZING"}
                and float(refreshed.get("launched_epoch", 0)) > float(runtime.get("launched_epoch", 0)),
                "repair retry runtime is stale or outside the authenticated timeframe")
        retry_record.update({"status": "started", "new_runtime_sha256": sha256(runtime_path),
                             "completed_utc": utc_now(), "result_status": result.get("status")})
        state = load_object(state_path)
        state["stage3_repair_retry"] = retry_record
        state["stage3_retry_launch_in_progress"].update(
            {"status": "started", "new_runtime_sha256": sha256(runtime_path),
             "updated_utc": utc_now()})
        atomic_json(state_path, state)
    except Exception as exc:
        state = load_object(state_path)
        state.update({"stage_status": "stage3_infrastructure_failure",
                      "next_action": "inspect_stage3_infrastructure_failure",
                      "stage3_failure_class": "infrastructure",
                      "stage3_failure_detail": "repair_retry_launch_failed",
                      "stage3_retry_launch_in_progress": {
                          **state.get("stage3_retry_launch_in_progress", {}),
                          "status": "failed", "failure_detail": str(exc)[:1000],
                          "updated_utc": utc_now()}})
        atomic_json(state_path, state)
        return {"status": "STAGE3_AUTORETRY_HELD", "timeframe": timeframe,
                "runtime": str(runtime_path.resolve()), "reason": str(exc)[:1000],
                "zero_token": True}
    if result.get("status") == "START_REQUIRED":
        update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                        "stage3_start_required", f"desktop_start_stage3_{timeframe.lower()}")
        return {"status": "START_REQUIRED", "timeframe": timeframe,
                "runtime": str(handoff.get("runtime_manifest") or runtime_path),
                "reason": result.get("start_required_reason"), "zero_token": True}
    update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                    f"stage3_{timeframe.lower()}_running", f"watch_stage3_{timeframe.lower()}")
    return {"status": "STAGE3_TIMEFRAME_RETRY_STARTED", "timeframe": timeframe,
            "runtime": str(handoff.get("runtime_manifest") or runtime_path),
            "zero_token": True}


def _has_timeframe_completion_evidence(runtime: dict) -> bool:
    event_path = Path(runtime.get("timeframe_event", ""))
    if event_path.is_file():
        try:
            event_status = load_object(event_path).get("status")
        except (OSError, ValueError, TypeError):
            return True
        if event_status in {"TIMEFRAME_COMPLETE", "PARTIAL_TIMEFRAME_COMPLETE"}:
            return True
    # A finalized Tester Journal can also result from a user stopping an
    # optimization. Treat it as completion only when the authenticated Tester
    # pass counter did not explicitly say the run was incomplete; otherwise a
    # clean final journal incorrectly blocks the bounded stalled-run retry.
    if (runtime.get("fresh_log_completion") is True
            and runtime.get("ui_passes_complete") is not False):
        return True
    if runtime.get("completion_mode") in {"full", "partial_90_percent"}:
        return True
    ui = runtime.get("ui_pass_counter") or {}
    try:
        ratio = int(ui.get("current", 0)) / int(ui.get("total", 0))
    except (TypeError, ValueError, ZeroDivisionError):
        ratio = 0.0
    threshold = float(runtime.get("partial_completion_ratio", 0.90))
    log = runtime.get("fresh_log_evidence") or {}
    journal_ok = bool(
        (log.get("byte_count", 0) and log.get("clean") is True)
        or (runtime.get("journal_override") is True
            and runtime.get("journal_override_scope") == "this_run"))
    return bool(
        runtime.get("allow_partial_completion", True) is True
        and 0.0 < threshold <= 1.0 and ratio >= threshold
        and ui.get("ok") is True and ui.get("button_state") == "START"
        and runtime.get("opt_structurally_complete") is True
        and runtime.get("stable_seconds", 0) >= runtime.get("normal_settle_seconds", 20)
        and journal_ok)


def _matching_stage3_watcher(runtime_path: Path) -> bool | None:
    """Return True for an exact runtime watcher, False if none, None if ownership is ambiguous."""
    try:
        target = str(runtime_path.resolve()).casefold()
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            name = str(proc.info.get("name") or "").casefold()
            if name not in {"python.exe", "pythonw.exe"}:
                continue
            command = proc.info.get("cmdline")
            if not isinstance(command, list):
                return None
            command_text = " ".join(str(part) for part in command).casefold()
            if "kcos_stage3_watch_v2.py" in command_text and target in command_text:
                return True
        return False
    except (psutil.AccessDenied, psutil.NoSuchProcess, OSError):
        return None


def _valid_prior_stage3_receipt(receipt: dict, cycle_id: str, ea: str) -> bool:
    return bool(
        receipt.get("schema_version") == 2
        and receipt.get("event_id") == f"{cycle_id}:stage3:failed"
        and receipt.get("cycle_id") == cycle_id and receipt.get("ea") == ea
        and receipt.get("stage") == 3 and receipt.get("status") == "failed"
        and receipt.get("failure_kind") == "STAGE3_EXECUTION_FAILURE"
        and receipt.get("rotation_eligible") is False
        and receipt.get("consumed") is False)


def _atomic_preserve_receipt(path: Path, payload: bytes, expected_sha256: str) -> bool:
    """Install an immutable sibling without exposing a partial archive file."""
    if path.is_file():
        return not _is_reparse_point(path) and sha256(path) == expected_sha256
    temporary = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        temporary = Path(temporary_name)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if sha256(temporary) != expected_sha256:
            return False
        try:
            # Hard-link installation is atomic and fails rather than replacing
            # an archive another cron invocation may already have installed.
            os.link(temporary, path)
        except FileExistsError:
            return (path.is_file() and not _is_reparse_point(path)
                    and sha256(path) == expected_sha256)
        return not _is_reparse_point(path) and sha256(path) == expected_sha256
    except OSError:
        return False
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _is_reparse_point(path: Path) -> bool:
    try:
        if path.is_symlink():
            return True
        isjunction = getattr(path, "is_junction", None)
        if callable(isjunction) and isjunction():
            return True
        attributes = getattr(path.stat(follow_symlinks=False), "st_file_attributes", 0)
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        return bool(attributes & reparse_flag)
    except OSError:
        return True


def _valid_prepared_dead_event(event: dict, cycle_id: str, ea: str, timeframe: str,
                               runtime_path: Path, runtime_hash: str, opt_path: Path,
                               opt_hash: str, ini_path: Path, ini_hash: str) -> bool:
    try:
        return bool(
            event.get("schema_version") == 2
            and event.get("event_id") == f"{cycle_id}:stage3:failed"
            and event.get("cycle_id") == cycle_id and event.get("stage") == 3
            and event.get("status") == "failed" and event.get("ea") == ea
            and event.get("timeframe") == timeframe
            and event.get("failure_kind") == "STAGE3_DEAD_RUNTIME_NO_COMPLETION"
            and event.get("error") == "dead_optimizing_runtime_without_completion_evidence"
            and event.get("rotation_eligible") is False and event.get("consumed") is False
            and Path(event.get("runtime_manifest", "")).resolve() == runtime_path.resolve()
            and event.get("runtime_manifest_sha256") == runtime_hash
            and Path(event.get("opt_path", "")).resolve() == opt_path.resolve()
            and event.get("opt_sha256") == opt_hash
            and Path(event.get("ini_path", "")).resolve() == ini_path.resolve()
            and event.get("ini_sha256") == ini_hash)
    except (OSError, TypeError, ValueError):
        return False


def _resolve_prepared_transaction(link: dict, failure_path: Path, cycle_id: str, ea: str,
                                  timeframe: str, runtime_path: Path, runtime_hash: str,
                                  opt_path: Path, opt_hash: str, ini_path: Path,
                                  ini_hash: str) -> tuple[Path, str, dict] | None:
    try:
        path = Path(link.get("path", "")).resolve()
        expected_hash = link.get("sha256")
        linked_path = Path(link.get("path", ""))
        if (_is_reparse_point(linked_path) or path.parent != failure_path.parent.resolve()
                or not path.is_file() or _is_reparse_point(path)
                or path.name != f"{failure_path.name}.prepared.{expected_hash}.json"
                or sha256(path) != expected_hash):
            return None
        payload = load_object(path)
        if not _valid_prepared_dead_event(payload, cycle_id, ea, timeframe,
                                          runtime_path, runtime_hash, opt_path,
                                          opt_hash, ini_path, ini_hash):
            return None
        return path, expected_hash, payload
    except (OSError, TypeError, ValueError):
        return None


def _exact_runtime_opt_path(handoff: dict, runtime: dict, timeframe: str) -> Path | None:
    """Resolve only the runtime OPT or its single launch-observed candidate."""
    try:
        candidates = {Path(value).resolve() for value in
                      launcher.opt_candidates(handoff, timeframe, launcher.DEFAULT_DATA_DIR)}
        recorded = runtime.get("opt_path")
        if recorded:
            candidate = Path(recorded).resolve()
            return candidate if candidate in candidates else None
        observations = runtime.get("opt_growth_observation") or {}
        paths = [Path(value).resolve() for value, size in observations.items()
                 if isinstance(size, int) and not isinstance(size, bool) and size > 0]
        paths = [path for path in paths if path in candidates and path.is_file()]
        return paths[0] if len(set(paths)) == 1 else None
    except (OSError, TypeError, ValueError):
        return None


def stage3_operator_retry_is_ready(state: dict, handoff: dict, runtime: dict,
                                   terminal_pids: list[int] | None = None) -> bool:
    """Validate one explicit, hash-bound retry after a dead partial M5 run."""
    authorization = state.get("stage3_operator_retry_authorization")
    if (not isinstance(authorization, dict)
            or authorization.get("status") != "authorized"
            or authorization.get("scope") != "one_same_timeframe_retry"
            or authorization.get("authorized_by") != "user_request"
            or authorization.get("cycle_id") != state.get("cycle_id")
            or authorization.get("timeframe") != runtime.get("timeframe")
            or state.get("current_stage") != "stage3_running"
            or state.get("stage_status") != "stage3_infrastructure_failure"
            or state.get("next_action") != "inspect_stage3_infrastructure_failure"
            or runtime.get("status") != "INFRASTRUCTURE_FAILURE"
            or runtime.get("failure_kind") != "EIGHTCAP_TERMINAL_NOT_FRESH"
            or runtime.get("infrastructure_only") is not True
            or runtime.get("cycle_id") != handoff.get("cycle_id")
            or runtime.get("ea") != handoff.get("ea")
            or terminal_pids):
        return False
    try:
        runtime_path = Path(handoff.get("runtime_manifest", ""))
        failure_path = Path(state.get("stage3_failure_event", ""))
        timeframe = str(runtime.get("timeframe", ""))
        ini_path = Path(handoff.get("ini_files", {}).get(timeframe, "")).resolve()
        if (timeframe not in handoff.get("timeframe_order", [])
                or not runtime_path.is_file() or not failure_path.is_file()
                or not ini_path.is_file()
                or authorization.get("runtime_manifest") != str(runtime_path.resolve())
                or authorization.get("failure_event") != str(failure_path.resolve())
                or sha256(runtime_path) != authorization.get("runtime_manifest_sha256")
                or sha256(failure_path) != authorization.get("failure_event_sha256")
                or sha256(ini_path) != authorization.get("ini_sha256")
                or runtime.get("ini_sha256") != authorization.get("ini_sha256")
                or Path(runtime.get("ini", "")).resolve() != ini_path
                or _has_timeframe_completion_evidence(runtime)):
            return False
        failure = load_object(failure_path)
        if (failure.get("cycle_id") != state.get("cycle_id")
                or failure.get("stage") != 3
                or failure.get("status") != "failed"
                or failure.get("rotation_eligible") is not False
                or failure.get("error") != runtime.get("failure_detail")):
            return False
        progress = launcher.fresh_agent_optimization_progress(
            handoff, runtime, runtime.get("worker_process_evidence", []),
            launcher.DEFAULT_DATA_DIR, allow_exited=True)
        agent_progress_valid = (isinstance(progress, dict)
                and progress.get("source") == "authenticated_fresh_local_agent_logs"
                and progress.get("cycle_id") == state.get("cycle_id")
                and progress.get("ea") == handoff.get("ea")
                and progress.get("symbol") == handoff.get("symbol")
                and progress.get("timeframe") == timeframe
                and progress.get("ini_sha256") == authorization.get("ini_sha256")
                and progress.get("workers_live") is False
                and progress.get("optimization_finished") is False
                and bool(progress.get("evidence"))
                and all(isinstance(item, dict)
                        and item.get("exact_expert_loaded") is True
                        and item.get("optimized_inputs_loaded") is True
                        and item.get("symbol_synchronized") is True
                        and item.get("optimization_pass_started") is True
                        and item.get("optimization_pass_succeeded") is True
                        and item.get("optimization_finished") is False
                        for item in progress.get("evidence", [])))
        command = [str(item) for item in runtime.get("command", [])]
        expected_startup = str(Path(runtime.get("startup_ini", "")).resolve())
        exact_command = (len(command) >= 3
                         and Path(command[0]).resolve() == launcher.STAGE3_PORTABLE_TERMINAL.resolve()
                         and any(item.casefold() == f"/config:{expected_startup}".casefold()
                                 for item in command[1:]))
        terminal_progress = launcher.read_authenticated_terminal_progress(runtime, handoff)
        root_progress_valid = bool(
            exact_command and terminal_progress.get("authenticated")
            and terminal_progress.get("optimization_finished") is False
            and isinstance(terminal_progress.get("latest_percent"), int)
            and 0 <= terminal_progress["latest_percent"] < 100
        )
        if not (agent_progress_valid or root_progress_valid):
            return False
        history = state.get("stage3_auto_timeframe_retry_history", [])
        matching_attempts = [item for item in history if isinstance(item, dict)
                             and item.get("cycle_id") == state.get("cycle_id")
                             and item.get("timeframe") == timeframe]
        if (authorization.get("prior_attempt_count") != len(matching_attempts)
                or any(item.get("operator_authorization_id") == authorization.get("authorization_id")
                       for item in matching_attempts)
                or time.time() - float(runtime.get("launched_epoch", 0))
                   < max(300, int(runtime.get("startup_grace_seconds", 300)))):
            return False
        for pid in [int(runtime.get("terminal_pid") or 0),
                    int(runtime.get("watcher_pid") or 0),
                    *(int(pid) for pid in runtime.get("worker_pids", []))]:
            if pid > 0 and psutil.pid_exists(pid):
                return False
        if _matching_stage3_watcher(runtime_path) is not False:
            return False
        opt_path = _exact_runtime_opt_path(handoff, runtime, timeframe)
        if opt_path is None or not opt_path.is_file():
            return False
        is_complete, shape = watcher.structurally_complete_opt(opt_path)
        if (is_complete or int(shape.get("records", -1)) != 0
                or time.time() - opt_path.stat().st_mtime
                   < max(300, int(runtime.get("startup_grace_seconds", 300)))):
            return False
        journal = runtime.get("journal_diagnostic") or {}
        return not journal.get("fresh_lines") and not journal.get("error_lines")
    except (OSError, ValueError, TypeError, KeyError, psutil.Error):
        return False


def interrupted_start_retry_is_ready(state: dict, handoff: dict, runtime: dict,
                                     terminal_pids: list[int] | None = None) -> bool:
    """Permit one same-cycle retry after an unverified post-click startup died.

    This is deliberately narrower than a generic dead-runtime retry: the exact
    authorized cycle, EA, INI, failure receipt, and runtime must match; no MT5
    owner or fresh worker may remain; no Journal, authenticated pass, or
    completion evidence may exist; and any changed OPT must be partial and
    attributable to this exact launch. The launcher archives the old runtime
    and OPT before creating a new attempt.
    """
    try:
        timeframe = str(runtime.get("timeframe", ""))
        cycle_id = state.get("cycle_id")
        authorization = state.get("run_authorization") or {}
        if (state.get("current_stage") != "stage3_running"
                or state.get("stage_status") != "stage3_infrastructure_failure"
                or state.get("next_action") != "inspect_stage3_infrastructure_failure"
                or runtime.get("status") != "INFRASTRUCTURE_FAILURE"
                or runtime.get("failure_kind") != "EIGHTCAP_TERMINAL_NOT_FRESH"
                or runtime.get("infrastructure_only") is not True
                or runtime.get("start_required_reason") != "post-click handshake incomplete; Journal-aware retry required"
                or int(runtime.get("start_button_attempt_count", 0)) < 8
                or int(runtime.get("start_button_delivery_count", 0)) < 1
                or runtime.get("start_button_last_result") != "not_delivered"
                or runtime.get("cycle_id") != cycle_id
                or runtime.get("ea") != state.get("current_ea")
                or runtime.get("ea") != handoff.get("ea")
                or handoff.get("cycle_id") != cycle_id
                or timeframe not in handoff.get("timeframe_order", [])
                or terminal_pids
                or authorization.get("cycle_id") != cycle_id
                or authorization.get("authorized_by") != "user_confirmed"
                or authorization.get("scope") != "current_cycle_stages_1_to_5"
                or 3 not in authorization.get("allowed_stages", [])
                or authorization.get("hold_after_stage") != 5):
            return False

        runtime_path = _handoff_path(handoff.get("runtime_manifest", ""))
        failure_path = Path(state.get("stage3_failure_event", ""))
        expected_failure = (failure_path.parent / f"{cycle_id}.stage3.failed.json").resolve()
        ini_path = Path(handoff.get("ini_files", {}).get(timeframe, "")).resolve()
        if (not runtime_path.is_file() or load_object(runtime_path) != runtime
                or not failure_path.is_file() or failure_path.resolve() != expected_failure
                or not ini_path.is_file()
                or timeframe not in handoff.get("ini_sha256", {})
                or sha256(ini_path) != handoff["ini_sha256"][timeframe]
                or runtime.get("ini_sha256") != handoff["ini_sha256"][timeframe]
                or Path(runtime.get("ini", "")).resolve() != ini_path):
            return False
        failure = load_object(failure_path)
        if (failure.get("schema_version") != 2
                or failure.get("event_id") != f"{cycle_id}:stage3:failed"
                or failure.get("cycle_id") != cycle_id
                or failure.get("ea") != handoff.get("ea")
                or failure.get("stage") != 3
                or failure.get("status") != "failed"
                or failure.get("failure_kind") != "STAGE3_EXECUTION_FAILURE"
                or failure.get("error") != runtime.get("failure_detail")
                or failure.get("rotation_eligible") is not False
                or failure.get("consumed") is not False
                or _has_timeframe_completion_evidence(runtime)):
            return False

        binding = runtime.get("input_schema_binding") or {}
        if (binding.get("status") != "INPUTS_BOUND"
                or binding.get("ini_sha256") != handoff["ini_sha256"][timeframe]
                or (runtime.get("journal_after_run") or {}).get("bytes", 0) != 0):
            return False
        journal = runtime.get("journal_diagnostic") or {}
        if journal.get("fresh_lines") or journal.get("error_lines"):
            return False

        history = state.get("stage3_auto_timeframe_retry_history", [])
        if not isinstance(history, list) or any(
                isinstance(item, dict) and item.get("cycle_id") == cycle_id
                and item.get("timeframe") == timeframe for item in history):
            return False

        pids = [int(runtime.get("terminal_pid") or 0), int(runtime.get("watcher_pid") or 0)]
        pids.extend(int(pid) for pid in runtime.get("worker_pids", []))
        pids.extend(int(item["pid"]) for item in runtime.get("worker_process_evidence", [])
                    if isinstance(item, dict) and item.get("pid"))
        if any(pid and psutil.pid_exists(pid) for pid in pids):
            return False
        if _matching_stage3_watcher(runtime_path) is not False:
            return False
        launched = float(runtime.get("launched_epoch", 0))
        if launched <= 0 or time.time() - launched < max(300, int(runtime.get("startup_grace_seconds", 300))):
            return False
        if launcher.fresh_local_workers(
                runtime.get("worker_baseline_pids", []), launched,
                expected_addresses=(runtime.get("agent_offsets_before") or {}).keys()):
            return False
        progress = launcher.fresh_agent_optimization_progress(
            handoff, runtime, runtime.get("worker_process_evidence", []),
            launcher.DEFAULT_DATA_DIR, allow_exited=True)
        if progress is not None:
            return False
        terminal_progress = launcher.read_authenticated_terminal_progress(runtime, handoff)
        if terminal_progress.get("authenticated"):
            return False

        opt_path = _exact_runtime_opt_path(handoff, runtime, timeframe)
        if opt_path is None or not opt_path.is_file():
            return False
        opt_path = opt_path.resolve()
        candidates = {Path(item).resolve() for item in launcher.opt_candidates(
            handoff, timeframe, launcher.DEFAULT_DATA_DIR)}
        before = opt_path.stat()
        baseline = (runtime.get("opt_growth_observation") or {}).get(str(opt_path))
        if (opt_path not in candidates or not isinstance(baseline, int) or baseline <= 0
                or before.st_size <= baseline or before.st_mtime < launched
                or time.time() - before.st_mtime < max(300, int(runtime.get("startup_grace_seconds", 300)))):
            return False
        _structurally_well_formed, shape = watcher.structurally_complete_opt(opt_path)
        records = int(shape.get("records", -1))
        maximum = int(runtime.get("max_passes", 0))
        # A parseable record boundary says nothing about Tester completion;
        # require a nonempty partial result set and preserve it before retry.
        if (maximum <= 0 or records <= 0 or records >= maximum
                or int(shape.get("record_size", 0)) <= 0
                or int(shape.get("trailing_bytes", -1)) != 0):
            return False
        digest = sha256(opt_path)
        after = opt_path.stat()
        return bool(before.st_size == after.st_size
                    and before.st_mtime_ns == after.st_mtime_ns
                    and digest == sha256(opt_path))
    except (OSError, ValueError, TypeError, KeyError, psutil.Error):
        return False


def navigator_binding_terminal_loss_retry_is_ready(
        state: dict, handoff: dict, runtime: dict,
        terminal_pids: list[int] | None = None) -> bool:
    """Allow one extra same-timeframe retry when exact Navigator binding never reached Start.

    This is limited to the already-authorized Navigator recovery, after its first
    terminal has exited. It requires zero Start delivery and zero optimization
    evidence, exact candidate hashes, and exactly one prior attempt for the cycle.
    """
    try:
        cycle, timeframe = state.get("cycle_id"), str(runtime.get("timeframe", ""))
        recovery = runtime.get("navigator_refresh_recovery") or {}
        authorization = state.get("run_authorization") or {}
        history = [item for item in state.get("stage3_auto_timeframe_retry_history", [])
                   if isinstance(item, dict) and item.get("cycle_id") == cycle
                   and item.get("timeframe") == timeframe]
        if (state.get("current_stage") != "stage3_running"
                or state.get("stage_status") != "stage3_infrastructure_failure"
                or state.get("next_action") != "inspect_stage3_infrastructure_failure"
                or state.get("stage3_failure_class") != "infrastructure"
                or runtime.get("status") != "INFRASTRUCTURE_FAILURE"
                or runtime.get("failure_kind") != "EIGHTCAP_TERMINAL_NOT_FRESH"
                or runtime.get("infrastructure_only") is not True
                or runtime.get("cycle_id") != cycle or handoff.get("cycle_id") != cycle
                or runtime.get("ea") != state.get("current_ea")
                or runtime.get("ea") != handoff.get("ea")
                or timeframe not in handoff.get("timeframe_order", [])
                or runtime.get("start_button_invoked") is not False
                or int(runtime.get("start_button_delivery_count", -1)) != 0
                or int(runtime.get("start_button_attempt_count", 0)) < 8
                or (runtime.get("input_schema_binding") or {}).get("status") != "EXPERT_NOT_READY"
                or (runtime.get("input_schema_binding") or {}).get("navigator_tested") is not True
                or recovery.get("status") != "authorized_once"
                or recovery.get("cycle_id") != cycle
                or recovery.get("terminal_pid") != runtime.get("terminal_pid")
                or recovery.get("ini_sha256") != runtime.get("ini_sha256")
                or recovery.get("startup_ini_sha256") != runtime.get("startup_ini_sha256")
                or recovery.get("ex5_sha256") != (runtime.get("deployment_reconciliation") or {}).get("authenticated_sha256")
                or authorization.get("cycle_id") != cycle
                or authorization.get("authorized_by") != "user_confirmed"
                or authorization.get("scope") != "current_cycle_stages_1_to_5"
                or 3 not in authorization.get("allowed_stages", [])
                or authorization.get("hold_after_stage") != 5
                or terminal_pids or len(history) != 1
                or history[0].get("attempt") != 1
                or history[0].get("reason") != "terminal_lost_before_authenticated_run_evidence"
                or history[0].get("result_status") != "START_REQUIRED"):
            return False

        runtime_path = _handoff_path(handoff.get("runtime_manifest", ""))
        failure_path = Path(str(state.get("stage3_failure_event") or ""))
        ini_path = Path(str(handoff.get("ini_files", {}).get(timeframe, ""))).resolve()
        if (not runtime_path.is_file() or load_object(runtime_path) != runtime
                or not failure_path.is_file() or not ini_path.is_file()
                or Path(str(runtime.get("ini", ""))).resolve() != ini_path
                or runtime.get("ini_sha256") != handoff.get("ini_sha256", {}).get(timeframe)
                or sha256(ini_path) != handoff.get("ini_sha256", {}).get(timeframe)
                or int((runtime.get("journal_after_run") or {}).get("bytes", -1)) != 0
                or (runtime.get("journal_diagnostic") or {}).get("fresh_lines")
                or (runtime.get("journal_diagnostic") or {}).get("error_lines")
                or (runtime.get("terminal_log_progress") or {}).get("authenticated")
                or (runtime.get("terminal_log_progress") or {}).get("automatic_testing_started")
                or runtime.get("worker_pids") or runtime.get("worker_process_evidence")
                or runtime.get("watcher_pid") or runtime.get("fresh_log_completion") is True
                or runtime.get("completion_mode") in {"full", "partial_90_percent"}
                or _has_timeframe_completion_evidence(runtime)):
            return False
        failure = load_object(failure_path)
        if (failure.get("cycle_id") != cycle or failure.get("stage") != 3
                or failure.get("status") != "failed"
                or failure.get("rotation_eligible") is not False
                or failure.get("runtime_manifest") != str(runtime_path.resolve())
                or failure.get("runtime_manifest_sha256") != sha256(runtime_path)
                or failure.get("error") != runtime.get("failure_detail")):
            return False
        if Path(str(runtime.get("timeframe_event") or "")).exists():
            return False
        deployment = runtime.get("deployment_reconciliation") or {}
        authenticated = Path(str(deployment.get("authenticated_ex5", "")))
        portable = Path(str(deployment.get("portable_ex5", "")))
        digest = recovery.get("ex5_sha256")
        if (deployment.get("verified") is not True or not authenticated.is_file()
                or not portable.is_file() or sha256(authenticated) != digest
                or sha256(portable) != digest):
            return False
        pid = int(runtime.get("terminal_pid") or 0)
        watcher_pid = int(runtime.get("watcher_pid") or 0)
        if (not pid or psutil.pid_exists(pid) or (watcher_pid and psutil.pid_exists(watcher_pid))
                or _matching_stage3_watcher(runtime_path) is not False):
            return False
        if time.time() - float(runtime.get("launched_epoch") or 0) < max(
                300, int(runtime.get("startup_grace_seconds", 300))):
            return False
        if launcher.fresh_local_workers(runtime.get("worker_baseline_pids", []),
                                        float(runtime["launched_epoch"]),
                                        expected_addresses=(runtime.get("agent_offsets_before") or {}).keys()):
            return False
        if launcher.fresh_agent_optimization_progress(
                handoff, runtime, [], launcher.DEFAULT_DATA_DIR, allow_exited=True) is not None:
            return False
        return not launcher.read_authenticated_terminal_progress(runtime, handoff).get("authenticated")
    except (OSError, ValueError, TypeError, KeyError, psutil.Error, OverflowError):
        return False


def fixed_input_surface_terminal_loss_retry_is_ready(
        state: dict, handoff: dict, runtime: dict,
        terminal_pids: list[int] | None = None) -> bool:
    """Permit one schema-correction retry after two zero-Start terminal losses.

    This exceptional third attempt is bound to the exact current cycle, two
    already-recorded attempts, the accepted second launch, and a source-derived
    reconciliation proving that only fixed source-default inputs were omitted.
    """
    try:
        cycle, timeframe = state.get("cycle_id"), str(runtime.get("timeframe", ""))
        history = [item for item in state.get("stage3_auto_timeframe_retry_history", [])
                   if isinstance(item, dict) and item.get("cycle_id") == cycle
                   and item.get("timeframe") == timeframe]
        authorization = state.get("run_authorization") or {}
        marker = state.get("stage3_retry_launch_in_progress") or {}
        binding = runtime.get("stale_retry_binding") or {}
        attempt_log = runtime.get("start_button_attempt_history") or []
        failed_bindings = [item.get("binding_status") for item in attempt_log
                           if isinstance(item, dict)]
        if (state.get("current_stage") != "stage3_running"
                or state.get("stage_status") != "stage3_infrastructure_failure"
                or state.get("next_action") != "inspect_stage3_infrastructure_failure"
                or state.get("stage3_failure_class") != "infrastructure"
                or runtime.get("status") != "INFRASTRUCTURE_FAILURE"
                or runtime.get("failure_kind") != "EIGHTCAP_TERMINAL_NOT_FRESH"
                or runtime.get("infrastructure_only") is not True
                or runtime.get("cycle_id") != cycle or handoff.get("cycle_id") != cycle
                or runtime.get("ea") != state.get("current_ea")
                or runtime.get("ea") != handoff.get("ea")
                or timeframe not in handoff.get("timeframe_order", [])
                or runtime.get("start_button_invoked") is not False
                or int(runtime.get("start_button_delivery_count", -1)) != 0
                or int(runtime.get("start_button_attempt_count", 0)) < 8
                or "INPUTS_NOT_READY" not in failed_bindings
                or authorization.get("cycle_id") != cycle
                or authorization.get("authorized_by") != "user_confirmed"
                or authorization.get("scope") != "current_cycle_stages_1_to_5"
                or 3 not in authorization.get("allowed_stages", [])
                or authorization.get("hold_after_stage") != 5
                or terminal_pids or len(history) != 2
                or history[0].get("attempt") != 1
                or history[1].get("attempt") != 2
                or history[1].get("status") != "started"
                or history[1].get("reason") != "one_bounded_retry_after_exact_navigator_binding_terminal_loss"
                or history[1].get("result_status") != "START_REQUIRED"
                or marker.get("cycle_id") != cycle or marker.get("timeframe") != timeframe
                or marker.get("attempt") != 2 or marker.get("status") != "started"
                or marker.get("retry_launch_id") != binding.get("launch_id")
                or binding != (history[1].get("stale_retry_binding") or {})
                or binding.get("attempt") not in {1, 2}
                or binding.get("ini_sha256") != handoff.get("ini_sha256", {}).get(timeframe)):
            return False
        runtime_path = _handoff_path(handoff.get("runtime_manifest", ""))
        failure_path = Path(str(state.get("stage3_failure_event") or ""))
        ini_path = Path(str(handoff.get("ini_files", {}).get(timeframe, ""))).resolve()
        if (not runtime_path.is_file() or load_object(runtime_path) != runtime
                or not failure_path.is_file() or not ini_path.is_file()
                or runtime.get("ini_sha256") != handoff.get("ini_sha256", {}).get(timeframe)
                or sha256(ini_path) != handoff.get("ini_sha256", {}).get(timeframe)
                or Path(str(runtime.get("ini", ""))).resolve() != ini_path
                or int((runtime.get("journal_after_run") or {}).get("bytes", -1)) != 0
                or (runtime.get("journal_diagnostic") or {}).get("fresh_lines")
                or (runtime.get("journal_diagnostic") or {}).get("error_lines")
                or (runtime.get("terminal_log_progress") or {}).get("authenticated")
                or (runtime.get("terminal_log_progress") or {}).get("automatic_testing_started")
                or runtime.get("worker_pids") or runtime.get("worker_process_evidence")
                or runtime.get("watcher_pid") or runtime.get("fresh_log_completion") is True
                or runtime.get("completion_mode") in {"full", "partial_90_percent"}
                or _has_timeframe_completion_evidence(runtime)
                or Path(str(runtime.get("timeframe_event") or "")).exists()):
            return False
        failure = load_object(failure_path)
        if (failure.get("cycle_id") != cycle or failure.get("stage") != 3
                or failure.get("status") != "failed"
                or failure.get("rotation_eligible") is not False
                or failure.get("runtime_manifest") != str(runtime_path.resolve())
                or failure.get("runtime_manifest_sha256") != sha256(runtime_path)
                or failure.get("error") != runtime.get("failure_detail")):
            return False
        if time.time() - float(runtime.get("launched_epoch") or 0) < max(
                300, int(runtime.get("startup_grace_seconds", 300))):
            return False
        pid = int(runtime.get("terminal_pid") or 0)
        watcher_pid = int(runtime.get("watcher_pid") or 0)
        if (not pid or psutil.pid_exists(pid)
                or (watcher_pid and psutil.pid_exists(watcher_pid))
                or _matching_stage3_watcher(runtime_path) is not False):
            return False
        for other_pid in launcher.process_ids("terminal64.exe"):
            try:
                if (Path(psutil.Process(other_pid).exe()).resolve()
                        == launcher.STAGE3_PORTABLE_TERMINAL.resolve()):
                    return False
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                return False
        if launcher.fresh_local_workers(runtime.get("worker_baseline_pids", []),
                                        float(runtime["launched_epoch"]),
                                        expected_addresses=(runtime.get("agent_offsets_before") or {}).keys()):
            return False
        deployment = runtime.get("deployment_reconciliation") or {}
        digest = str(deployment.get("authenticated_sha256") or "").upper()
        authenticated = Path(str(deployment.get("authenticated_ex5") or ""))
        portable = Path(str(deployment.get("portable_ex5") or ""))
        if (deployment.get("verified") is not True or not digest
                or not authenticated.is_file() or not portable.is_file()
                or sha256(authenticated) != digest or sha256(portable) != digest
                or digest != str(handoff.get("ex5_sha256") or "").upper()):
            return False
        reconciliation = launcher.authenticated_input_surface_reconciliation(
            handoff, launcher._read_profile_text(ini_path))
        if not reconciliation.get("missing_fixed_source_defaults"):
            return False
        if launcher.fresh_agent_optimization_progress(
                handoff, runtime, [], launcher.DEFAULT_DATA_DIR, allow_exited=True) is not None:
            return False
        return not launcher.read_authenticated_terminal_progress(runtime, handoff).get("authenticated")
    except (OSError, ValueError, TypeError, KeyError, psutil.Error, OverflowError):
        return False


def stale_runtime_restart_is_ready(state: dict, handoff: dict, runtime: dict,
                                   terminal_pids: list[int] | None = None) -> bool:
    """Allow one same-timeframe restart only after an authenticated run is dead and stale."""
    retry_marker = state.get("stage3_retry_launch_in_progress")
    retry_launch_recovery = bool(
        isinstance(retry_marker, dict)
        and retry_marker.get("cycle_id") == state.get("cycle_id")
        and retry_marker.get("timeframe") == runtime.get("timeframe")
        and retry_marker.get("attempt") in {1, 2}
        and retry_marker.get("status") == "failed"
        and retry_marker.get("failure_detail") == "retry launch result is stale or does not match the authenticated timeframe")
    operator_retry_recovery = stage3_operator_retry_is_ready(state, handoff, runtime, terminal_pids)
    input_surface_terminal_loss_recovery = fixed_input_surface_terminal_loss_retry_is_ready(
        state, handoff, runtime, terminal_pids)
    navigator_binding_terminal_loss_recovery = (
        navigator_binding_terminal_loss_retry_is_ready(state, handoff, runtime, terminal_pids)
        or input_surface_terminal_loss_recovery)
    post_click_interrupted_start_recovery = (
        interrupted_start_retry_is_ready(state, handoff, runtime, terminal_pids))
    detail = str(state.get("stage3_failure_detail", ""))
    timeframe = str(runtime.get("timeframe", ""))
    stalled_compute_recovery = False
    if (runtime.get("status") == COMPUTE_LIMIT_STATUS
            and runtime.get("failure_mode") == "compute_stalled"
            and runtime.get("completion_reason") == "progress_stall_budget_exceeded_without_active_workers"
            and detail == (f"Stage 3 {timeframe} made no pass progress for its configured stall budget; "
                           "no active tester workers remain. The timeframe is incomplete and must not be skipped")):
        try:
            compute_event_path = Path(str(runtime.get("timeframe_event") or ""))
            compute_event = load_object(compute_event_path)
            metrics = compute_event.get("metrics") or {}
            active_workers = (compute_event.get("evidence") or {}).get("active_worker_pids")
            expected_metrics = (compute_event.get("metrics")
                                if isinstance(compute_event.get("metrics"), dict) else {
                                    key: compute_event.get(key)
                                    for key in ("completed_passes", "elapsed_seconds", "seconds_since_last_pass")
                                    if compute_event.get(key) is not None
                                })
            expected_evidence = (compute_event.get("evidence")
                                 if isinstance(compute_event.get("evidence"), dict) else {
                                     "active_worker_pids": compute_event.get("alive_worker_pids", [])
                                 })
            failure_path = Path(str(state.get("stage3_failure_event") or ""))
            compute_path = timeframe_compute_path(handoff, timeframe)
            failed_event = load_object(failure_path)
            compute_record = load_object(compute_path)
            stalled_compute_recovery = bool(
                compute_event.get("schema_version") == 2
                and compute_event.get("cycle_id") == state.get("cycle_id")
                and compute_event.get("stage") == 3
                and compute_event.get("timeframe") == timeframe
                and compute_event.get("event_id") ==
                    f"{state.get('cycle_id')}:stage3:{timeframe}:compute_budget_exceeded"
                and compute_event.get("status") == COMPUTE_LIMIT_STATUS
                and compute_event.get("failure_mode") == "compute_stalled"
                and compute_event.get("completion_reason") == "progress_stall_budget_exceeded_without_active_workers"
                and compute_event.get("consumed") is False
                and isinstance(active_workers, list) and not active_workers
                and int(metrics.get("completed_passes", 0)) > 0
                and float(metrics.get("seconds_since_last_pass", 0))
                    >= int(runtime.get("progress_stall_budget_seconds", 900))
                and failed_event.get("cycle_id") == state.get("cycle_id")
                and failed_event.get("status") == "failed"
                and failed_event.get("rotation_eligible") is False
                and failed_event.get("error") == detail
                and compute_record.get("schema_version") == 2
                and compute_record.get("cycle_id") == state.get("cycle_id")
                and compute_record.get("stage") == 3
                and compute_record.get("ea") == state.get("current_ea")
                and compute_record.get("timeframe") == timeframe
                and compute_record.get("outcome") == "COMPUTE_SKIPPED"
                and compute_record.get("failure_mode") == "compute_stalled"
                and compute_record.get("source_event_id") == compute_event.get("event_id")
                and compute_record.get("source_event_sha256") == sha256(compute_event_path)
                and compute_record.get("metrics") == expected_metrics
                and compute_record.get("evidence") == expected_evidence
            )
        except (OSError, ValueError, TypeError, KeyError):
            stalled_compute_recovery = False
    if (state.get("current_stage") != "stage3_running"
            or state.get("stage_status") != "stage3_infrastructure_failure"
            or state.get("next_action") != "inspect_stage3_infrastructure_failure"
            or runtime.get("status") not in {"START_REQUIRED", "OPTIMIZING", "INFRASTRUCTURE_FAILURE", COMPUTE_LIMIT_STATUS}
            or runtime.get("cycle_id") != state.get("cycle_id")
            or runtime.get("cycle_id") != handoff.get("cycle_id")
            or runtime.get("ea") != state.get("current_ea")
            or runtime.get("ea") != handoff.get("ea")
            or runtime.get("timeframe") not in handoff.get("timeframe_order", [])
            or (runtime.get("worker_pids") and
                detail != "dead_optimizing_runtime_without_completion_evidence"
                and not stalled_compute_recovery
                and not retry_launch_recovery and not operator_retry_recovery
                and not post_click_interrupted_start_recovery
                and not navigator_binding_terminal_loss_recovery)
            or terminal_pids):
        return False
    dead_opt_recovery = detail == "dead_optimizing_runtime_without_completion_evidence"
    if runtime.get("status") == COMPUTE_LIMIT_STATUS and not stalled_compute_recovery:
        return False
    interrupted_start_recovery = bool(
        dead_opt_recovery and runtime.get("status") == "INFRASTRUCTURE_FAILURE"
        and runtime.get("failure_kind") == "EIGHTCAP_TERMINAL_NOT_FRESH"
        and runtime.get("infrastructure_only") is True)
    if (runtime.get("status") == "INFRASTRUCTURE_FAILURE"
            and not (interrupted_start_recovery or retry_launch_recovery
                     or operator_retry_recovery or post_click_interrupted_start_recovery
                     or navigator_binding_terminal_loss_recovery)):
        return False
    if (not dead_opt_recovery and not stalled_compute_recovery
            and not retry_launch_recovery and not operator_retry_recovery
            and not post_click_interrupted_start_recovery
            and not navigator_binding_terminal_loss_recovery
            and "runtime registration stayed incomplete" not in detail):
        return False
    if _has_timeframe_completion_evidence(runtime):
        return False
    timeframe = str(runtime["timeframe"])
    opt_path = (_exact_runtime_opt_path(handoff, runtime, timeframe)
                if (dead_opt_recovery or stalled_compute_recovery or post_click_interrupted_start_recovery)
                else None)
    if ((dead_opt_recovery or stalled_compute_recovery or post_click_interrupted_start_recovery)
            and (opt_path is None or not opt_path.is_file())):
        return False
    retry_history = state.get("stage3_auto_timeframe_retry_history", [])
    failure_path = Path(state.get("stage3_failure_event", ""))
    runtime_path = Path(handoff.get("runtime_manifest", ""))
    try:
        if not failure_path.is_file() or not runtime_path.is_file():
            return False
        prior_attempt = next((item for item in reversed(retry_history)
                              if isinstance(item, dict)
                              and item.get("cycle_id") == state.get("cycle_id")
                              and item.get("timeframe") == timeframe), None)
        if stalled_compute_recovery and prior_attempt is not None:
            return False
        if retry_launch_recovery and (
                not isinstance(prior_attempt, dict)
                or prior_attempt.get("attempt") != retry_marker.get("attempt")
                or prior_attempt.get("status") != "failed"
                or prior_attempt.get("failure_detail") != "retry launch result is stale or does not match the authenticated timeframe"
                or not isinstance(state.get("stage3_retry_launch_in_progress"), dict)
                or state["stage3_retry_launch_in_progress"].get("status") != "failed"
                or state["stage3_retry_launch_in_progress"].get("failure_detail") != prior_attempt.get("failure_detail")
                or runtime.get("status") != "INFRASTRUCTURE_FAILURE"
                or runtime.get("failure_kind") != "EIGHTCAP_TERMINAL_NOT_FRESH"
                or runtime.get("infrastructure_only") is not True):
            return False
        if dead_opt_recovery and (
                not isinstance(prior_attempt, dict)
                or prior_attempt.get("attempt") != 1
                or prior_attempt.get("status") != "ready"
                or prior_attempt.get("previous_runtime_sha256") != sha256(runtime_path)
                or prior_attempt.get("failure_event_sha256") != sha256(failure_path)):
            return False
        if (not dead_opt_recovery and not retry_launch_recovery and not operator_retry_recovery
                and not navigator_binding_terminal_loss_recovery
                and prior_attempt is not None and not (
                prior_attempt.get("status") == "launching"
                and prior_attempt.get("previous_runtime_sha256") == sha256(runtime_path))):
            return False
        failure = load_object(failure_path)
        if (failure.get("cycle_id") != state.get("cycle_id")
                or failure.get("status") != "failed"
                or failure.get("rotation_eligible") is not False):
            return False
        if (isinstance(prior_attempt, dict)
                and prior_attempt.get("failure_event_sha256")
                and prior_attempt.get("failure_event_sha256") != sha256(failure_path)
                and not retry_launch_recovery and not operator_retry_recovery
                and not navigator_binding_terminal_loss_recovery):
            return False
        if dead_opt_recovery and (
                failure.get("failure_kind") != "STAGE3_DEAD_RUNTIME_NO_COMPLETION"
                or failure.get("runtime_manifest_sha256") != sha256(runtime_path)
                or failure.get("opt_sha256") != sha256(opt_path)):
            return False
        launched = float(runtime["launched_epoch"])
        startup_grace = max(300, int(runtime.get("startup_grace_seconds", 300)))
        if time.time() - launched < startup_grace:
            return False
        command = runtime.get("command", [])
        if (not command or Path(str(command[0])).resolve() != launcher.STAGE3_PORTABLE_TERMINAL.resolve()
                or "/portable" not in " ".join(str(item) for item in command).casefold()):
            return False
        terminal_pid = int(runtime.get("terminal_pid") or 0)
        watcher_pid = int(runtime.get("watcher_pid") or 0)
        recorded_pids = [int(pid) for pid in runtime.get("worker_pids", [])]
        recorded_pids.extend(int(item["pid"]) for item in runtime.get("worker_process_evidence", [])
                              if isinstance(item, dict) and item.get("pid"))
        if any(pid and psutil.pid_exists(pid) for pid in [terminal_pid, watcher_pid, *recorded_pids]):
            return False
        if _matching_stage3_watcher(runtime_path) is not False:
            return False
        live_unregistered = launcher.fresh_local_workers(
            runtime.get("worker_baseline_pids", []), launched,
            expected_addresses=runtime.get("agent_offsets_before", {}).keys())
        if live_unregistered:
            return False
        if retry_launch_recovery:
            progress = launcher.fresh_agent_optimization_progress(
                handoff, runtime, runtime.get("worker_process_evidence", []),
                launcher.DEFAULT_DATA_DIR, allow_exited=True)
            if retry_marker.get("attempt") == 1:
                if (not isinstance(progress, dict)
                        or progress.get("source") != "authenticated_fresh_local_agent_logs"
                        or progress.get("cycle_id") != state.get("cycle_id")
                        or progress.get("ea") != state.get("current_ea")
                        or progress.get("symbol") != handoff.get("symbol")
                        or progress.get("timeframe") != runtime.get("timeframe")
                        or progress.get("ini_sha256") != handoff.get("ini_sha256", {}).get(runtime.get("timeframe"))
                        or not progress.get("evidence")
                        or progress.get("optimization_finished") is True):
                    return False
            else:
                authenticated_partial_progress = bool(
                    isinstance(progress, dict)
                    and progress.get("source") == "authenticated_fresh_local_agent_logs"
                    and progress.get("cycle_id") == state.get("cycle_id")
                    and progress.get("ea") == state.get("current_ea")
                    and progress.get("symbol") == handoff.get("symbol")
                    and progress.get("timeframe") == runtime.get("timeframe")
                    and progress.get("ini_sha256") == handoff.get("ini_sha256", {}).get(runtime.get("timeframe"))
                    and bool(progress.get("evidence"))
                    and progress.get("workers_live") is False
                    and progress.get("optimization_finished") is not True
                    and all(isinstance(item, dict)
                            and item.get("exact_expert_loaded") is True
                            and item.get("optimized_inputs_loaded") is True
                            and item.get("symbol_synchronized") is True
                            and item.get("optimization_pass_started") is True
                            and item.get("optimization_pass_succeeded") is True
                            for item in progress.get("evidence", [])))
                if ((progress is not None and not authenticated_partial_progress)
                        or int(runtime.get("start_button_delivery_count", 0)) != 0
                        or int(runtime.get("start_button_attempt_count", 0)) < 8
                        or (runtime.get("input_schema_binding") or {}).get("status") != "ALREADY_RUNNING"
                        or (runtime.get("journal_diagnostic") or {}).get("fresh_lines")
                        or (runtime.get("journal_diagnostic") or {}).get("error_lines")
                        or any(
                            not Path(path).is_file()
                            or Path(path).stat().st_size != int(size)
                            or watcher.structurally_complete_opt(Path(path))[1].get("records") != 0
                            or watcher.structurally_complete_opt(Path(path))[1].get("header_size") != int(size)
                            for path, size in (runtime.get("opt_growth_observation") or {}).items())):
                    return False
        timeframe = str(runtime["timeframe"])
        expected_ini = Path(handoff["ini_files"][timeframe]).resolve()
        if (Path(runtime.get("ini", "")).resolve() != expected_ini
                or runtime.get("ini_sha256") != handoff.get("ini_sha256", {}).get(timeframe)
                or sha256(expected_ini) != handoff.get("ini_sha256", {}).get(timeframe)):
            return False
        if navigator_binding_terminal_loss_recovery:
            return True
        opt_path = _exact_runtime_opt_path(handoff, runtime, timeframe)
        if opt_path is None or not opt_path.is_file():
            return False
        stale_seconds = max(300, int(runtime.get("startup_grace_seconds", 300)))
        if time.time() - opt_path.stat().st_mtime < stale_seconds:
            return False
        return True
    except (OSError, ValueError, TypeError, KeyError, psutil.Error):
        return False


def classify_dead_optimizing_runtime(state_path: Path, handoff_path: Path,
                                     runtime: dict, terminal_pids: list[int]) -> bool:
    """Record a non-rotatable failure only when every Stage 3 owner is gone.

    This does not launch a retry. The cron entry may separately choose the
    existing one-shot RESTART_STALE path on its next tick.
    """
    state, handoff = load_object(state_path), load_object(handoff_path)
    interrupted_start = bool(
        runtime.get("status") == "INFRASTRUCTURE_FAILURE"
        and runtime.get("failure_kind") == "EIGHTCAP_TERMINAL_NOT_FRESH"
        and runtime.get("infrastructure_only") is True)
    if (state.get("current_stage") != "stage3_running"
            or state.get("cycle_id") != runtime.get("cycle_id")
            or handoff.get("cycle_id") != runtime.get("cycle_id")
            or state.get("current_ea") != runtime.get("ea")
            or handoff.get("ea") != runtime.get("ea")
            or runtime.get("stage") != 3
            or (runtime.get("status") != "OPTIMIZING" and not interrupted_start)
            or not runtime.get("timeframe")):
        return False
    resumable_hold = (state.get("stage_status") == "stage3_infrastructure_failure"
                      and state.get("stage3_failure_detail")
                          == "dead_optimizing_runtime_without_completion_evidence")
    resumable_interrupted_start = bool(
        interrupted_start and state.get("stage_status") == "stage3_infrastructure_failure"
        and state.get("stage3_failure_class") == "infrastructure"
        and state.get("stage3_failure_detail") == runtime.get("failure_detail"))
    if (state.get("stage_status") not in {"stage3_running", "stage3_m15_running", "stage3_m5_running", "stage3_h1_running"}
            and not resumable_hold and not resumable_interrupted_start):
        return False
    runtime_path = _handoff_path(handoff.get("runtime_manifest", ""))
    if not runtime_path.is_file():
        return False
    try:
        if load_object(runtime_path) != runtime:
            return False
    except (OSError, ValueError, TypeError):
        return False
    timeframe = str(runtime["timeframe"])
    if timeframe not in handoff.get("timeframe_order", []):
        return False
    command = runtime.get("command", [])
    if (not command
            or Path(str(command[0])).resolve() != launcher.STAGE3_PORTABLE_TERMINAL.resolve()
            or "/portable" not in " ".join(str(item) for item in command).casefold()):
        return False
    event_path = Path(runtime.get("timeframe_event", ""))
    if not event_path or event_path.exists() or terminal_pids:
        return False
    if (runtime.get("fresh_log_completion") is True
            or runtime.get("completion_mode") in {"full", "partial_90_percent"}):
        return False
    agent_progress = launcher.fresh_agent_optimization_progress(
        handoff, runtime, runtime.get("worker_process_evidence", []),
        launcher.DEFAULT_DATA_DIR, allow_exited=True)
    if interrupted_start and (not agent_progress
                              or agent_progress.get("workers_live") is True
                              or agent_progress.get("optimization_finished") is True):
        return False
    ui = runtime.get("ui_pass_counter") or {}
    try:
        ui_ratio = int(ui.get("current", 0)) / int(ui.get("total", 0))
    except (TypeError, ValueError, ZeroDivisionError):
        ui_ratio = 0.0
    partial_threshold = float(runtime.get("partial_completion_ratio", 0.90))
    journal_evidence = runtime.get("fresh_log_evidence") or {}
    journal_allows_partial = bool(
        (journal_evidence.get("byte_count", 0) and journal_evidence.get("clean") is True)
        or (runtime.get("journal_override") is True
            and runtime.get("journal_override_scope") == "this_run"))
    if (ui.get("ok") is True and ui.get("button_state") == "START"
            and ui_ratio >= partial_threshold and runtime.get("opt_structurally_complete") is True
            and runtime.get("allow_partial_completion", True) is True
            and 0.0 < partial_threshold <= 1.0
            and runtime.get("stable_seconds", 0) >= runtime.get("normal_settle_seconds", 20)
            and journal_allows_partial):
        return False
    launched = float(runtime.get("launched_epoch", 0))
    stale_seconds = max(300, int(runtime.get("startup_grace_seconds", 300)))
    if not launched or time.time() - launched < stale_seconds:
        return False
    owned = [int(runtime.get("terminal_pid") or 0), int(runtime.get("watcher_pid") or 0)]
    owned.extend(int(pid) for pid in runtime.get("worker_pids", []))
    owned.extend(int(item.get("pid") or 0) for item in runtime.get("worker_process_evidence", [])
                 if isinstance(item, dict))
    if any(pid > 0 and psutil.pid_exists(pid) for pid in owned):
        return False
    if _matching_stage3_watcher(runtime_path) is not False:
        return False
    try:
        fresh_workers = launcher.fresh_local_workers(
            runtime.get("worker_baseline_pids", []), launched,
            expected_addresses=(runtime.get("agent_offsets_before") or {}).keys())
    except Exception:
        return False
    if fresh_workers:
        return False
    ini = Path(handoff.get("ini_files", {}).get(timeframe, "")).resolve()
    if (not ini.is_file() or Path(runtime.get("ini", "")).resolve() != ini
            or runtime.get("ini_sha256") != handoff.get("ini_sha256", {}).get(timeframe)
            or sha256(ini) != handoff.get("ini_sha256", {}).get(timeframe)):
        return False
    opt = _exact_runtime_opt_path(handoff, runtime, timeframe)
    if opt is None:
        return False
    try:
        if not opt.is_file():
            return False
        structural, shape = watcher.structurally_complete_opt(opt)
        stale_opt_seconds = max(300, int(runtime.get("normal_settle_seconds", 20)))
        before = opt.stat()
        partial_authenticated_run = bool(
            interrupted_start and agent_progress
            and before.st_mtime >= launched
            and agent_progress.get("workers_live") is False
            and agent_progress.get("optimization_finished") is False)
        if ((not structural and not partial_authenticated_run) or before.st_size <= 0
                or before.st_mtime < launched
                or time.time() - before.st_mtime < stale_opt_seconds):
            return False
        candidates = [Path(item).resolve() for item in
                      launcher.opt_candidates(handoff, timeframe, launcher.DEFAULT_DATA_DIR)]
        prior_map = runtime.get("opt_candidates_before") or {}
        prior = prior_map.get(str(opt.resolve())) if isinstance(prior_map, dict) else None
        baseline_ok = bool(
            isinstance(prior, dict)
            and (int(prior.get("size", -1)) != before.st_size
                 or float(prior.get("mtime", -1)) != before.st_mtime))
        if partial_authenticated_run and opt.resolve() in candidates:
            observed = runtime.get("opt_growth_observation") or {}
            baseline_ok = (str(opt.resolve()) in observed
                           and isinstance(observed[str(opt.resolve())], int)
                           and before.st_mtime >= launched)
        if not baseline_ok and prior_map == {}:
            # Older launch manifests sometimes lack the candidate snapshot.
            # Use only the launcher's exact-path in-run growth observation as
            # a replacement baseline; never infer freshness from another OPT.
            growth = runtime.get("opt_growth_observation") or {}
            observed_size = growth.get(str(opt.resolve())) if isinstance(growth, dict) else None
            try:
                last_growth_epoch = float(runtime.get("last_opt_growth_epoch", 0))
                growth_epoch_valid = (math.isfinite(last_growth_epoch)
                                      and last_growth_epoch > 0
                                      and last_growth_epoch >= launched)
            except (TypeError, ValueError):
                growth_epoch_valid = False
            baseline_ok = bool(
                isinstance(observed_size, int) and not isinstance(observed_size, bool)
                and observed_size > 0 and before.st_size > observed_size
                and growth_epoch_valid)
        if opt.resolve() not in candidates or not baseline_ok:
            return False
        opt_hash = sha256(opt)
        after = opt.stat()
        if (before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns
                or sha256(opt) != opt_hash):
            return False
    except (OSError, ValueError, TypeError):
        return False
    if not runtime.get("timeframe_event") or event_path.exists():
        return False
    failure_path = Path(state_path).parent / "events" / f"{state['cycle_id']}.stage3.failed.json"
    retry_history = state.get("stage3_auto_timeframe_retry_history", [])
    attempts = [item for item in retry_history if isinstance(item, dict)
                and item.get("cycle_id") == state.get("cycle_id")
                and item.get("timeframe") == timeframe]
    retry_exhausted = False
    original_failure = None
    prior_receipt_bytes = None
    prior_receipt_hash = None
    prior_receipt_path = None
    resumable_interruption = False
    prepared_transaction = None
    prepared_event = None
    if failure_path.is_file():
        try:
            if not attempts:
                current_bytes = failure_path.read_bytes()
                current = json.loads(current_bytes.decode("utf-8-sig"))
                linked = current.get("prior_failure_receipt")
                if (current.get("failure_kind") == "STAGE3_DEAD_RUNTIME_NO_COMPLETION"
                        and current.get("schema_version") == 2
                        and current.get("event_id") == f"{state['cycle_id']}:stage3:failed"
                        and current.get("cycle_id") == state.get("cycle_id")
                        and current.get("ea") == state.get("current_ea")
                        and current.get("stage") == 3 and current.get("status") == "failed"
                        and current.get("rotation_eligible") is False
                        and current.get("consumed") is False
                        and isinstance(linked, dict)):
                    prior_receipt_hash = linked.get("sha256")
                    prior_receipt_link = Path(linked.get("path", ""))
                    if (_is_reparse_point(prior_receipt_link)
                            or prior_receipt_link.parent.resolve() != failure_path.parent.resolve()
                            or prior_receipt_link.name
                                != f"{failure_path.name}.prior.{prior_receipt_hash}.json"
                            or not prior_receipt_link.is_file()):
                        return False
                    prior_receipt_path = prior_receipt_link.resolve(strict=True)
                    if (prior_receipt_path.parent != failure_path.parent.resolve()
                            or _is_reparse_point(prior_receipt_path)
                            or not prior_receipt_path.is_file()):
                        return False
                    prior_receipt_bytes = prior_receipt_path.read_bytes()
                    original_failure = json.loads(prior_receipt_bytes.decode("utf-8-sig"))
                    if (not _valid_prior_stage3_receipt(original_failure, state["cycle_id"], state["current_ea"])
                            or sha256(prior_receipt_path) != prior_receipt_hash
                            or prior_receipt_path.name != f"{failure_path.name}.prior.{prior_receipt_hash}.json"):
                        return False
                    resumable_interruption = True
                    transaction = current.get("classification_transaction")
                    resolved_transaction = _resolve_prepared_transaction(
                        transaction if isinstance(transaction, dict) else {}, failure_path,
                        state["cycle_id"], state["current_ea"], timeframe, runtime_path,
                        sha256(runtime_path), opt, opt_hash, ini,
                        handoff["ini_sha256"][timeframe])
                    if resolved_transaction is None:
                        return False
                    prepared_transaction = {"path": str(resolved_transaction[0]),
                                            "sha256": resolved_transaction[1]}
                elif (current.get("failure_kind") == "STAGE3_DEAD_RUNTIME_NO_COMPLETION"
                      and current.get("schema_version") == 2
                      and current.get("event_id") == f"{state['cycle_id']}:stage3:failed"
                      and current.get("cycle_id") == state.get("cycle_id")
                      and current.get("stage") == 3 and current.get("status") == "failed"
                      and current.get("ea") == state.get("current_ea")
                      and current.get("timeframe") == timeframe
                      and current.get("error") == "dead_optimizing_runtime_without_completion_evidence"
                      and current.get("rotation_eligible") is False
                      and current.get("consumed") is False
                      and "prior_failure_receipt" not in current
                      and _valid_prepared_dead_event(
                          current, state["cycle_id"], state["current_ea"], timeframe,
                          runtime_path, sha256(runtime_path), opt, opt_hash, ini,
                          handoff["ini_sha256"][timeframe])):
                    # A first write without a prior receipt has no archive link.
                    # Resume only when the event itself binds the exact live
                    # runtime, OPT, and INI evidence used by this classification.
                    resumable_interruption = True
                    transaction = current.get("classification_transaction")
                    resolved_transaction = _resolve_prepared_transaction(
                        transaction if isinstance(transaction, dict) else {}, failure_path,
                        state["cycle_id"], state["current_ea"], timeframe, runtime_path,
                        sha256(runtime_path), opt, opt_hash, ini,
                        handoff["ini_sha256"][timeframe])
                    if resolved_transaction is None:
                        return False
                    prepared_transaction = {"path": str(resolved_transaction[0]),
                                            "sha256": resolved_transaction[1]}
                elif (current.get("failure_kind") == "STAGE3_EXECUTION_FAILURE"
                      and current.get("schema_version") == 2
                      and current.get("event_id") == f"{state['cycle_id']}:stage3:failed"
                      and current.get("error") == "dead_optimizing_runtime_without_completion_evidence"
                      and current.get("cycle_id") == state.get("cycle_id")
                      and current.get("ea") == state.get("current_ea")
                      and current.get("stage") == 3 and current.get("status") == "failed"
                      and current.get("rotation_eligible") is False
                      and current.get("consumed") is False):
                    # record_failure wrote its generic event before the custom
                    # restore; recover the already archived original receipt.
                    archives = []
                    for candidate in failure_path.parent.glob(f"{failure_path.name}.prior.*.json"):
                        try:
                            if (_is_reparse_point(candidate)
                                    or candidate.resolve().parent != failure_path.parent.resolve()):
                                continue
                            archived_obj = load_object(candidate)
                            if (_valid_prior_stage3_receipt(archived_obj, state["cycle_id"], state["current_ea"])
                                    and sha256(candidate) == candidate.name.removeprefix(
                                        f"{failure_path.name}.prior.").removesuffix(".json")):
                                archives.append(candidate)
                        except (OSError, ValueError, TypeError):
                            continue
                    if len(archives) == 1:
                        prior_receipt_path = archives[0]
                        prior_receipt_hash = sha256(prior_receipt_path)
                        prior_receipt_bytes = prior_receipt_path.read_bytes()
                        original_failure = load_object(prior_receipt_path)
                    else:
                        transactions = []
                        for candidate in failure_path.parent.glob(f"{failure_path.name}.prepared.*.json"):
                            try:
                                if (_is_reparse_point(candidate)
                                        or candidate.resolve().parent != failure_path.parent.resolve()):
                                    continue
                                candidate_hash = sha256(candidate)
                                candidate_event = load_object(candidate)
                                if (candidate.name == f"{failure_path.name}.prepared.{candidate_hash}.json"
                                        and _valid_prepared_dead_event(
                                            candidate_event, state["cycle_id"], state["current_ea"],
                                            timeframe, runtime_path, sha256(runtime_path), opt, opt_hash,
                                            ini, handoff["ini_sha256"][timeframe])):
                                    transactions.append((candidate, candidate_hash, candidate_event))
                            except (OSError, ValueError, TypeError):
                                continue
                        if len(transactions) != 1:
                            return False
                        prepared_transaction = {"path": str(transactions[0][0].resolve()),
                                                "sha256": transactions[0][1]}
                        prepared_event = transactions[0][2]
                    resumable_interruption = True
                else:
                    prior_receipt_bytes = current_bytes
                    original_failure = current
                    if not _valid_prior_stage3_receipt(original_failure, state["cycle_id"], state["current_ea"]):
                        return False
                    prior_receipt_hash = sha256(failure_path)
                    prior_receipt_path = failure_path.with_name(
                        f"{failure_path.name}.prior.{prior_receipt_hash}.json")
            else:
                original_failure = load_object(failure_path)
                attempt = attempts[-1]
                binding = runtime.get("stale_retry_binding") or {}
                retry_exhausted = bool(
                    len(attempts) == 1 and attempt.get("attempt") == 1
                    and attempt.get("status") in {"started", "launching"}
                    and binding.get("attempt") == 1
                    and binding.get("previous_runtime_sha256") == attempt.get("previous_runtime_sha256")
                    and binding.get("failure_event_sha256") == attempt.get("failure_event_sha256")
                    and attempt.get("failure_event_sha256") == sha256(failure_path)
                    and binding == attempt.get("stale_retry_binding")
                    and binding.get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
                    and binding.get("launch_id") == attempt.get("retry_launch_id")
                    and binding.get("launch_epoch") == attempt.get("retry_launch_epoch")
                    and float(binding.get("launch_epoch", -1)) == float(runtime.get("launched_epoch", 0))
                    and Path(runtime.get("ini", "")).resolve()
                        == Path(handoff.get("ini_files", {}).get(timeframe, "")).resolve()
                    and runtime.get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
                    and original_failure.get("failure_kind") == "STAGE3_DEAD_RUNTIME_NO_COMPLETION"
                    and original_failure.get("cycle_id") == state.get("cycle_id")
                    and original_failure.get("rotation_eligible") is False)
        except (OSError, ValueError, TypeError):
            return False
        if attempts and not retry_exhausted:
            return False
    elif attempts:
        return False
    runtime_hash = sha256(runtime_path)
    event = {
        "schema_version": 2, "event_id": f"{state['cycle_id']}:stage3:failed",
        "cycle_id": state["cycle_id"], "stage": 3, "status": "failed",
        "failure_kind": "STAGE3_DEAD_RUNTIME_NO_COMPLETION",
        "ea": state.get("current_ea"), "timeframe": timeframe,
        "error": "dead_optimizing_runtime_without_completion_evidence",
        "runtime_manifest": str(runtime_path.resolve()), "runtime_manifest_sha256": runtime_hash,
        "opt_path": str(opt.resolve()), "opt_sha256": opt_hash,
        "opt_size": after.st_size, "opt_structure": shape,
        "authenticated_worker_progress": agent_progress if interrupted_start else None,
        "incomplete_opt_retry": bool(interrupted_start and not structural),
        "ini_path": str(ini), "ini_sha256": handoff["ini_sha256"][timeframe],
        "failed_utc": utc_now(), "rotation_eligible": False, "consumed": False,
    }
    detail = ("dead_optimizing_runtime_after_one_retry" if retry_exhausted
              else "dead_optimizing_runtime_without_completion_evidence")
    if retry_exhausted:
        event = dict(original_failure)
        event["retry_exhausted"] = {
            "runtime_manifest": str(runtime_path.resolve()),
            "runtime_manifest_sha256": runtime_hash,
            "opt_path": str(opt.resolve()), "opt_sha256": opt_hash,
            "failed_utc": utc_now(),
        }
    elif prior_receipt_bytes is not None:
        if not _atomic_preserve_receipt(prior_receipt_path, prior_receipt_bytes, prior_receipt_hash):
            return False
        if not resumable_interruption and sha256(failure_path) != prior_receipt_hash:
            return False
        event["prior_failure_receipt"] = {
            "path": str(prior_receipt_path.resolve()), "sha256": prior_receipt_hash,
        }
    event_path.parent.mkdir(parents=True, exist_ok=True)
    if not retry_exhausted:
        if prepared_event is not None:
            event = prepared_event
        if prepared_transaction is None:
            transactions = []
            for candidate in failure_path.parent.glob(f"{failure_path.name}.prepared.*.json"):
                try:
                    if (_is_reparse_point(candidate)
                            or candidate.resolve().parent != failure_path.parent.resolve()):
                        continue
                    candidate_hash = sha256(candidate)
                    candidate_event = load_object(candidate)
                    if (candidate.name == f"{failure_path.name}.prepared.{candidate_hash}.json"
                            and _valid_prepared_dead_event(
                                candidate_event, state["cycle_id"], state["current_ea"], timeframe,
                                runtime_path, runtime_hash, opt, opt_hash, ini,
                                handoff["ini_sha256"][timeframe])):
                        transactions.append((candidate, candidate_hash, candidate_event))
                except (OSError, ValueError, TypeError):
                    continue
            if len(transactions) > 1:
                return False
            if transactions:
                prepared_transaction = {"path": str(transactions[0][0].resolve()),
                                        "sha256": transactions[0][1]}
                event = transactions[0][2]
        if prepared_transaction is None:
            prepared_bytes = json.dumps(event, ensure_ascii=False, sort_keys=True,
                                        indent=2).encode("utf-8")
            prepared_hash = hashlib.sha256(prepared_bytes).hexdigest().upper()
            prepared_path = failure_path.with_name(
                f"{failure_path.name}.prepared.{prepared_hash}.json")
            if not _atomic_preserve_receipt(prepared_path, prepared_bytes, prepared_hash):
                return False
            prepared_transaction = {"path": str(prepared_path.resolve()), "sha256": prepared_hash}
        event["classification_transaction"] = prepared_transaction
    atomic_json(failure_path, event)
    record_failure(state_path, detail)
    held_state = load_object(state_path)
    if (held_state.get("stage_status") != "stage3_infrastructure_failure"
            or held_state.get("stage3_failure_detail") != detail):
        # record_failure intentionally catches filesystem errors. Leave the
        # linked replacement event and receipt archive in place so the next
        # cron invocation can resume the hold transaction safely.
        return False
    # record_failure maintains the normal infrastructure hold and EA ledger;
    # restore the additional hashes that make this exact dead OPT retryable.
    failure_event = load_object(failure_path)
    failure_event.update({key: value for key, value in event.items()
                          if key not in {"failed_utc"}})
    atomic_json(failure_path, failure_event)
    if not retry_exhausted:
        held_state = load_object(state_path)
        history = list(held_state.get("stage3_auto_timeframe_retry_history", []))
        history.append({
            "cycle_id": state["cycle_id"], "timeframe": timeframe, "attempt": 1,
            "status": "ready", "reason": "dead_optimizing_runtime_one_shot_retry",
            "previous_runtime_sha256": runtime_hash,
            "failure_event_sha256": sha256(failure_path),
            "ini_sha256": handoff["ini_sha256"][timeframe], "prepared_utc": utc_now(),
        })
        held_state["stage3_auto_timeframe_retry_history"] = history[-9:]
        atomic_json(state_path, held_state)
    return True


def unstarted_terminal_retry_is_ready(state: dict, handoff: dict, runtime: dict,
                                      terminal_pids: list[int] | None = None) -> bool:
    """Allow a bounded same-timeframe retry only when startup produced no run evidence."""
    journal = runtime.get("journal_diagnostic") or {}
    ui = journal.get("ui_journal") or {}
    no_optimized_parameters = bool(
        runtime.get("failure_kind") == "TESTER_JOURNAL_REJECTED"
        and journal.get("code") == "NO_OPTIMIZED_PARAMETERS"
        and journal.get("error_lines")
        and all(launcher.NO_OPTIMIZED_PARAMETERS_RE.search(str(line))
                for line in journal.get("error_lines", []))
        and not journal.get("run_identity_lines")
        and not runtime.get("worker_pids")
        and not runtime.get("worker_process_evidence")
        and not runtime.get("watcher_pid"))
    ui_error_lines = [str(line).strip() for line in journal.get("error_lines", [])]
    ui_only_false_rejection = bool(
        runtime.get("failure_kind") == "TESTER_JOURNAL_REJECTED"
        and journal.get("code") == "TESTER_JOURNAL_UI_REJECTED"
        and ui.get("method") in {"ocr", "native_windows_ocr_probe", "ui_observer_ocr"}
        and ui_error_lines
        and all(line and line in [str(item).strip() for item in ui.get("error_lines", [])]
                and not launcher.INPUT_GRID_ERROR_RE.search(line)
                and not launcher.NO_EXPERT_IN_START_CONFIG_RE.search(line)
                and "incorrect input parameters" not in line.casefold()
                for line in ui_error_lines)
        and set(journal.get("fresh_lines") or []) == set(journal.get("error_lines") or [])
        and int((runtime.get("journal_after_run") or {}).get("bytes", -1)) == 0
        and not journal.get("run_identity_lines")
        and not runtime.get("worker_pids")
        and not runtime.get("worker_process_evidence")
        and not runtime.get("watcher_pid"))
    timeframe_hint = str(runtime.get("timeframe", "")).upper()
    failure_detail = str(state.get("stage3_failure_detail", ""))
    binding = runtime.get("input_schema_binding")
    try:
        terminal_pid = int(runtime.get("terminal_pid") or 0)
        no_progress_detail = (
            f"Stage 3 {timeframe_hint} Start handoff produced no authenticated tester, "
            "agent-pass, or growing-OPT progress within its startup grace")
        dead_terminal_detail = (
            f"EightCap terminal identity is not verifiable: process PID not found (pid={terminal_pid})")
        startup_empty_grid = bool(
            runtime.get("status") in {"START_REQUIRED", "INFRASTRUCTURE_FAILURE"}
            and isinstance(binding, dict)
            and binding.get("status") == "INPUTS_NOT_READY"
            and int(binding.get("visible_input_rows", -1)) == 0
            and int(binding.get("expected_input_rows", 0)) > 0
            and int(runtime.get("start_button_attempt_count", 0)) >= 8
            and int(runtime.get("start_button_delivery_count", -1)) == 0
            and not runtime.get("start_button_invoked")
            and not journal.get("fresh_lines")
            and not journal.get("error_lines")
            and failure_detail in {no_progress_detail, dead_terminal_detail})
    except (TypeError, ValueError, OverflowError):
        startup_empty_grid = False
    try:
        launched = float(runtime.get("launched_epoch") or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    startup_expert_unready = bool(
        runtime.get("status") in {"START_REQUIRED", "INFRASTRUCTURE_FAILURE"}
        and isinstance(binding, dict)
        and binding.get("status") == "EXPERT_NOT_READY"
        and int(runtime.get("start_button_attempt_count", 0)) >= 8
        and int(runtime.get("start_button_delivery_count", -1)) == 0
        and runtime.get("start_button_invoked") is False
        and not journal.get("fresh_lines") and not journal.get("error_lines")
        and not runtime.get("worker_pids") and not runtime.get("worker_process_evidence")
        and not runtime.get("watcher_pid")
        and not (runtime.get("terminal_log_progress") or {}).get("authenticated")
        and not runtime.get("last_opt_growth_epoch")
        and failure_detail in {no_progress_detail, dead_terminal_detail})
    if (state.get("current_stage") != "stage3_running"
            or runtime.get("status") != "INFRASTRUCTURE_FAILURE" and not startup_empty_grid and not startup_expert_unready
            or not (runtime.get("failure_kind") == "EIGHTCAP_TERMINAL_NOT_FRESH"
                    and runtime.get("infrastructure_only") is True
                    or runtime.get("failure_kind") == "EXPERT_INPUT_BINDING_UNAVAILABLE"
                    and runtime.get("infrastructure_only") is True
                    or no_optimized_parameters or startup_empty_grid or startup_expert_unready or ui_only_false_rejection)
            or runtime.get("cycle_id") != state.get("cycle_id")
            or runtime.get("cycle_id") != handoff.get("cycle_id")
            or runtime.get("ea") != state.get("current_ea")
            or runtime.get("ea") != handoff.get("ea")
            or runtime.get("timeframe") not in handoff.get("timeframe_order", [])
            or runtime.get("worker_pids") or runtime.get("worker_process_evidence")
            or runtime.get("watcher_pid")):
        return False
    timeframe = str(runtime["timeframe"])
    held_failure_state = (state.get("stage_status") == "stage3_infrastructure_failure"
                          and state.get("next_action") == "inspect_stage3_infrastructure_failure")
    stale_running_label = (no_optimized_parameters
                           and state.get("stage_status") == f"stage3_{timeframe.lower()}_running"
                           and state.get("next_action") == f"watch_stage3_{timeframe.lower()}")
    if not (held_failure_state or stale_running_label):
        return False
    history = state.get("stage3_auto_timeframe_retry_history", [])
    if not isinstance(history, list):
        return False
    prior_attempts = [item for item in history if isinstance(item, dict)
                      and item.get("cycle_id") == state.get("cycle_id")
                      and item.get("timeframe") == timeframe]
    expected_prior_reasons = {
        1: "no_optimized_parameters_retry_with_hash_bound_wide_profile",
        2: "no_optimized_parameters_retry_with_explicit_expert_parameters_file",
    }
    prior_noopt_sequence = bool(
        len(prior_attempts) in {1, 2}
        and [item.get("attempt") for item in prior_attempts] == list(range(1, len(prior_attempts) + 1))
        and all(item.get("reason") == expected_prior_reasons.get(index)
                and item.get("status") == "failed"
                and item.get("result_status") == "INFRASTRUCTURE_FAILURE"
                for index, item in enumerate(prior_attempts, 1)))
    prior_empty_grid_sequence = bool(
        startup_empty_grid
        and len(prior_attempts) == 3
        and [item.get("attempt") for item in prior_attempts] == [1, 2, 3]
        and all(item.get("reason") == expected_prior_reasons.get(index)
                and item.get("status") == "failed"
                and item.get("result_status") == "INFRASTRUCTURE_FAILURE"
                for index, item in enumerate(prior_attempts[:2], 1))
        and prior_attempts[2].get("reason") == "no_optimized_parameters_retry_preserving_loaded_expert_grid"
        and prior_attempts[2].get("status") == "started"
        and prior_attempts[2].get("result_status") == "START_REQUIRED"
        and prior_attempts[2].get("previous_runtime_sha256") == prior_attempts[1].get("new_runtime_sha256")
        and prior_attempts[2].get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
        and prior_attempts[2].get("recovery_profile_sha256") == runtime.get("startup_ini_sha256")
        and abs(float(runtime.get("launched_epoch", 0))
                - datetime.fromisoformat(str(prior_attempts[2].get("started_utc", "")).replace("Z", "+00:00")).timestamp()) < 15)
    prior_navigator_rebind_sequence = bool(
        no_optimized_parameters
        and len(prior_attempts) == 4
        and [item.get("attempt") for item in prior_attempts] == [1, 2, 3, 4]
        and all(item.get("reason") == expected_prior_reasons.get(index)
                and item.get("status") == "failed"
                and item.get("result_status") == "INFRASTRUCTURE_FAILURE"
                for index, item in enumerate(prior_attempts[:2], 1))
        and prior_attempts[2].get("reason") == "no_optimized_parameters_retry_preserving_loaded_expert_grid"
        and prior_attempts[2].get("status") == "started"
        and prior_attempts[2].get("result_status") == "START_REQUIRED"
        and prior_attempts[2].get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
        and prior_attempts[3].get("reason") == "terminal_lost_before_authenticated_run_evidence"
        and prior_attempts[3].get("status") == "failed"
        and prior_attempts[3].get("result_status") == "INFRASTRUCTURE_FAILURE"
        and (prior_attempts[3].get("runtime_input_schema_binding") or {}).get("status") == "EXPERT_NOT_READY"
        and (prior_attempts[3].get("runtime_input_schema_binding") or {}).get("navigator_tested") is False
        and "Navigator hidden" in str((prior_attempts[3].get("runtime_input_schema_binding") or {}).get("navigator_test_detail", ""))
        and prior_attempts[3].get("new_runtime_sha256") == sha256(
            Path(handoff.get("runtime_manifest", ""))))
    prior_exact_profile_rebind_sequence = bool(
        startup_expert_unready
        and len(prior_attempts) == 1
        and prior_attempts[0].get("attempt") == 1
        and prior_attempts[0].get("reason") == "terminal_lost_before_authenticated_run_evidence"
        and prior_attempts[0].get("status") == "started"
        and prior_attempts[0].get("result_status") == "START_REQUIRED"
        and prior_attempts[0].get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
        and (prior_attempts[0].get("runtime_input_schema_binding") or {}).get("status") == "EXPERT_NOT_READY"
        and (prior_attempts[0].get("runtime_input_schema_binding") or {}).get("navigator_tested") is False
        and not prior_attempts[0].get("recovery_profile"))
    if prior_attempts and not (
            no_optimized_parameters and prior_noopt_sequence
            or startup_empty_grid and prior_empty_grid_sequence
            or no_optimized_parameters and prior_navigator_rebind_sequence
            or prior_exact_profile_rebind_sequence):
        return False
    failure_path = Path(state.get("stage3_failure_event", ""))
    runtime_path = Path(handoff.get("runtime_manifest", ""))
    try:
        if not failure_path.is_file() or not runtime_path.is_file():
            return False
        failure = load_object(failure_path)
        if (failure.get("cycle_id") != state.get("cycle_id")
                or failure.get("status") != "failed"
                or failure.get("rotation_eligible") is not False):
            return False
        if startup_empty_grid and (
                failure.get("failure_kind") != "STAGE3_EXECUTION_FAILURE"
                or failure.get("ea") != handoff.get("ea")
                or failure.get("error") != failure_detail
                or datetime.fromisoformat(str(failure.get("failed_utc", "")).replace("Z", "+00:00")).timestamp()
                    < float(runtime.get("launched_epoch", 0))):
            return False
        ini_path = Path(handoff["ini_files"][timeframe]).resolve()
        if (Path(runtime.get("ini", "")).resolve() != ini_path
                or runtime.get("ini_sha256") != handoff.get("ini_sha256", {}).get(timeframe)
                or sha256(ini_path) != handoff.get("ini_sha256", {}).get(timeframe)):
            return False
        owned_terminal_pid = int(runtime.get("terminal_pid") or 0)
        active_owned_terminal = bool(owned_terminal_pid and psutil.pid_exists(owned_terminal_pid))
        if active_owned_terminal and not (no_optimized_parameters or ui_only_false_rejection):
            return False
        if terminal_pids and not (active_owned_terminal
                                  and terminal_pids == [owned_terminal_pid]
                                  and (ui_only_false_rejection or no_optimized_parameters)):
            return False
        for pid in launcher.process_ids("terminal64.exe"):
            try:
                if (Path(psutil.Process(pid).exe()).resolve() == launcher.STAGE3_PORTABLE_TERMINAL.resolve()
                        and not (active_owned_terminal and int(pid) == owned_terminal_pid)):
                    return False
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                return False
        if any(psutil.pid_exists(int(item.get("pid") or 0))
               for item in runtime.get("worker_process_evidence", []) if isinstance(item, dict)):
            return False
        fresh_workers = launcher.fresh_local_workers(
            runtime.get("worker_baseline_pids", []), launched,
            expected_addresses=(runtime.get("agent_offsets_before") or {}).keys())
        if fresh_workers:
            if not ui_only_false_rejection:
                return False
            progress = launcher.fresh_agent_optimization_progress(
                handoff, runtime, fresh_workers, launcher.DEFAULT_DATA_DIR,
                allow_exited=True)
            if not (progress and progress.get("source") == "authenticated_fresh_local_agent_logs"
                    and progress.get("cycle_id") == state.get("cycle_id")
                    and progress.get("ea") == handoff.get("ea")
                    and progress.get("symbol") == handoff.get("symbol")
                    and progress.get("timeframe") == timeframe
                    and progress.get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
                    and progress.get("evidence")):
                return False
        if ((journal.get("fresh_lines") or journal.get("error_lines"))
                and not (no_optimized_parameters or startup_empty_grid or ui_only_false_rejection)):
            return False
        if no_optimized_parameters and any(
                re.search(r"optimization pass .* started|OnTester result .*: passed in ",
                          str(line), re.I)
                for line in journal.get("fresh_lines", [])):
            return False
        before = runtime.get("opt_candidates_before", {})
        for opt_path in launcher.opt_candidates(handoff, timeframe, launcher.DEFAULT_DATA_DIR):
            snapshot = before.get(str(opt_path.resolve()))
            stat = opt_path.stat()
            if (snapshot is None or int(snapshot.get("size", -1)) != stat.st_size
                    or float(snapshot.get("mtime", -1)) != stat.st_mtime):
                if not ui_only_false_rejection:
                    return False
                structurally_complete, shape = watcher.structurally_complete_opt(opt_path)
                if (structurally_complete or int(shape.get("records", -1)) != 0
                        or int(shape.get("trailing_bytes", -1)) != 0
                        or stat.st_mtime < launched):
                    return False
        return time.time() - launched >= max(30.0, float(runtime.get("startup_grace_seconds") or 300))
    except (OSError, ValueError, TypeError, KeyError, psutil.Error):
        return False


def retry_unstarted_terminal(state_path: Path, handoff_path: Path,
                             launch_runner=launcher.launch_and_register) -> dict:
    """Retry once on the exact timeframe after authenticated pre-work terminal loss."""
    state, handoff = load_object(state_path), load_object(handoff_path)
    runtime_path = _handoff_path(handoff.get("runtime_manifest", ""))
    runtime = load_object(runtime_path)
    timeframe = str(runtime.get("timeframe", ""))
    require(unstarted_terminal_retry_is_ready(state, handoff, runtime),
            "automatic same-timeframe retry requires a dead terminal with no run evidence")
    history = list(state.get("stage3_auto_timeframe_retry_history", []))
    prior_attempts = [item for item in history if isinstance(item, dict)
                      and item.get("cycle_id") == state.get("cycle_id")
                      and item.get("timeframe") == timeframe]
    journal = runtime.get("journal_diagnostic") or {}
    no_optimized_parameters = (runtime.get("failure_kind") == "TESTER_JOURNAL_REJECTED"
                               and journal.get("code") == "NO_OPTIMIZED_PARAMETERS")
    ui_false_rejection = (runtime.get("failure_kind") == "TESTER_JOURNAL_REJECTED"
                          and journal.get("code") == "TESTER_JOURNAL_UI_REJECTED")
    navigator_rebind_retry = bool(
        no_optimized_parameters and len(prior_attempts) == 4
        and prior_attempts[3].get("reason") == "terminal_lost_before_authenticated_run_evidence"
        and (prior_attempts[3].get("runtime_input_schema_binding") or {}).get("status") == "EXPERT_NOT_READY"
        and prior_attempts[3].get("new_runtime_sha256") == sha256(runtime_path))
    binding = runtime.get("input_schema_binding")
    expert_binding_retry = bool(
        runtime.get("status") == "INFRASTRUCTURE_FAILURE"
        and runtime.get("failure_kind") == "EXPERT_INPUT_BINDING_UNAVAILABLE"
        and runtime.get("infrastructure_only") is True
        and isinstance(binding, dict)
        and binding.get("status") == "EXPERT_NOT_READY"
        and int(runtime.get("start_button_attempt_count", 0)) >= 8
        and int(runtime.get("start_button_delivery_count", -1)) == 0
        and runtime.get("start_button_invoked") is False
        and not runtime.get("worker_pids") and not runtime.get("worker_process_evidence")
        and not runtime.get("watcher_pid")
        and not (runtime.get("journal_diagnostic") or {}).get("fresh_lines")
        and not (runtime.get("terminal_log_progress") or {}).get("authenticated")
        and not runtime.get("last_opt_growth_epoch"))
    prior_exact_profile_rebind_sequence = bool(
        expert_binding_retry and len(prior_attempts) == 1
        and prior_attempts[0].get("attempt") == 1
        and prior_attempts[0].get("reason") == "terminal_lost_before_authenticated_run_evidence"
        and prior_attempts[0].get("status") == "started"
        and prior_attempts[0].get("result_status") == "START_REQUIRED"
        and (prior_attempts[0].get("runtime_input_schema_binding") or {}).get("status") == "EXPERT_NOT_READY"
        and (prior_attempts[0].get("runtime_input_schema_binding") or {}).get("navigator_tested") is False
        and not prior_attempts[0].get("recovery_profile"))
    try:
        startup_empty_grid = (runtime.get("status") == "START_REQUIRED"
                              and isinstance(binding, dict)
                              and binding.get("status") == "INPUTS_NOT_READY"
                              and int(binding.get("visible_input_rows", -1)) == 0
                              and int(runtime.get("start_button_attempt_count", 0)) >= 8
                              and int(runtime.get("start_button_delivery_count", -1)) == 0)
    except (TypeError, ValueError, OverflowError):
        startup_empty_grid = False
    if startup_empty_grid and len(prior_attempts) == 3:
        prior_attempts[-1].update({
            "status": "failed",
            "failure_detail": state.get("stage3_failure_detail"),
            "failure_event_sha256": sha256(Path(state["stage3_failure_event"])),
            "completed_utc": utc_now(),
        })
        history = [prior_attempts[-1] if item.get("cycle_id") == state.get("cycle_id")
                   and item.get("timeframe") == timeframe and item.get("attempt") == 3 else item
                   for item in history]
    original_runtime_path = runtime_path
    original_runtime_sha256 = sha256(original_runtime_path)
    original_failure_path = Path(state.get("stage3_failure_event", ""))
    require(original_failure_path.is_file(), "Stage 3 retry failure event is missing")
    original_failure_sha256 = sha256(original_failure_path)
    if ui_false_rejection:
        launched = float(runtime.get("launched_epoch") or 0)
        fresh_workers = launcher.fresh_local_workers(
            runtime.get("worker_baseline_pids", []), launched,
            expected_addresses=(runtime.get("agent_offsets_before") or {}).keys())
        progress = None
        if fresh_workers:
            progress = launcher.fresh_agent_optimization_progress(
                handoff, runtime, fresh_workers, launcher.DEFAULT_DATA_DIR, allow_exited=True)
            require(bool(progress and progress.get("source") == "authenticated_fresh_local_agent_logs"
                         and progress.get("cycle_id") == state.get("cycle_id")
                         and progress.get("ea") == handoff.get("ea")
                         and progress.get("symbol") == handoff.get("symbol")
                         and progress.get("timeframe") == timeframe
                         and progress.get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
                         and progress.get("evidence")),
                    "UI false-positive retry lacks current-cycle authenticated worker progress")
        agent_log_deltas = []
        tester_root = launcher.DEFAULT_DATA_DIR / "Tester"
        agent_log_date = datetime.fromtimestamp(launched).strftime("%Y%m%d.log")
        for address, baseline_offset in (runtime.get("agent_offsets_before") or {}).items():
            log_path = (tester_root / f"Agent-{str(address).replace(':', '-')}" / "logs" / agent_log_date)
            if not log_path.is_file():
                continue
            try:
                size = log_path.stat().st_size
                start_offset = max(0, int(baseline_offset))
                if size <= start_offset:
                    continue
                with log_path.open("rb") as handle:
                    handle.seek(start_offset)
                    delta = handle.read()
                if delta:
                    agent_log_deltas.append({
                        "address": str(address), "path": str(log_path.resolve()),
                        "start_offset": start_offset, "bytes": len(delta),
                        "sha256": hashlib.sha256(delta).hexdigest().upper(),
                        "last_write_epoch": log_path.stat().st_mtime,
                    })
            except (OSError, TypeError, ValueError):
                continue
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        recovery_runtime_path = original_runtime_path.with_name(
            f"{state['cycle_id']}.stage3.runtime.ui-false-positive-retry.{stamp}.json")
        recovery_runtime = dict(runtime)
        recovery_runtime.update({
            "worker_pids": [int(item["pid"]) for item in fresh_workers],
            "worker_process_evidence": fresh_workers,
            "agent_logs": launcher.agent_log_registrations(
                [int(item["pid"]) for item in fresh_workers], launcher.DEFAULT_DATA_DIR,
                launched, runtime.get("agent_offsets_before", {}), fresh_workers),
            "tester_agent_progress": progress,
            "superseded_ui_false_positive": {
                "failure_event": str(original_failure_path.resolve()),
                "failure_event_sha256": original_failure_sha256,
                "runtime_manifest": str(original_runtime_path.resolve()),
                "runtime_manifest_sha256": original_runtime_sha256,
                "recovered_utc": utc_now(),
            },
        })
        receipt_path = (original_runtime_path.parent.parent / "recovery" /
                        f"{state['cycle_id']}.stage3.ui-false-positive-recovery.{stamp}.json")
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt = {
            "schema_version": 1, "cycle_id": state["cycle_id"], "stage": 3,
            "timeframe": timeframe, "status": "SAME_TIMEFRAME_RETRY_AUTHORIZED",
            "reason": "exact-cycle local agents were active after an OCR-only false journal rejection; preserve their evidence and restart after terminal loss",
            "failure_event": str(original_failure_path.resolve()),
            "failure_event_sha256": original_failure_sha256,
            "runtime_manifest": str(original_runtime_path.resolve()),
            "runtime_manifest_sha256": original_runtime_sha256,
            "recovery_runtime_manifest": str(recovery_runtime_path.resolve()),
            "worker_pids": [int(item["pid"]) for item in fresh_workers],
            "worker_addresses": [str(item["address"]) for item in fresh_workers],
            "agent_log_deltas": agent_log_deltas,
            "agent_progress_sha256": hashlib.sha256(json.dumps(
                (progress or {}).get("evidence", []), sort_keys=True, separators=(",", ":")
            ).encode("utf-8")).hexdigest().upper(),
            "authorized_utc": utc_now(),
        }
        atomic_json(receipt_path, receipt)
        recovery_runtime["retry_recovery_receipt"] = str(receipt_path.resolve())
        recovery_runtime["retry_recovery_receipt_sha256"] = sha256(receipt_path)
        atomic_json(recovery_runtime_path, recovery_runtime)

        # Keep the old failure immutable and auditable; its runtime path and
        # hash still resolve to the untouched original manifest.
        archived_failure_path = original_failure_path.with_name(
            f"{original_failure_path.stem}.prior_ui_false_positive.{stamp}{original_failure_path.suffix}")
        original_failure_path.replace(archived_failure_path)
        handoff_backup = Path(handoff_path).with_name(
            f"{Path(handoff_path).name}.pre_ui_false_positive_retry.{stamp}.evidence")
        shutil.copy2(handoff_path, handoff_backup)
        handoff["runtime_manifest"] = str(recovery_runtime_path.resolve())
        launcher.atomic_json(Path(handoff_path), handoff)
        failure_history = list(state.get("stage3_superseded_failures") or [])
        failure_history.append({
            "failure_event": str(archived_failure_path.resolve()),
            "failure_event_sha256": sha256(archived_failure_path),
            "failure_detail": state.get("stage3_failure_detail"),
            "runtime_manifest": str(original_runtime_path.resolve()),
            "runtime_manifest_sha256": original_runtime_sha256,
            "superseded_by": str(receipt_path.resolve()),
            "superseding_receipt_sha256": sha256(receipt_path),
            "superseded_utc": utc_now(),
        })
        state["stage3_superseded_failures"] = failure_history[-20:]
        state["stage3_ui_false_positive_retry_receipt"] = str(receipt_path.resolve())
        state["stage3_ui_false_positive_retry_receipt_sha256"] = sha256(receipt_path)
        state.pop("stage3_failure_event", None)
        state.pop("stage3_failure_detail", None)
        state.pop("stage3_failure_class", None)
        runtime_path = recovery_runtime_path
        runtime = recovery_runtime
    attempt_number = len(prior_attempts) + 1
    attempt = {
        "cycle_id": state["cycle_id"], "timeframe": timeframe, "attempt": attempt_number,
        "status": "launching",
        "reason": ("ui_observer_false_positive_retry_after_startup_grace"
                   if ui_false_rejection
                   else "empty_input_grid_refresh_retry_after_startup_grace" if startup_empty_grid
                   else "terminal_lost_before_authenticated_run_evidence"),
        "failure_event_sha256": original_failure_sha256,
        "failure_event": str(original_failure_path.resolve()),
        "previous_runtime_sha256": original_runtime_sha256,
        "previous_runtime": str(original_runtime_path.resolve()),
        "ini_sha256": handoff["ini_sha256"][timeframe], "started_utc": utc_now(),
    }
    history.append(attempt)
    state["stage3_auto_timeframe_retry_history"] = history
    state["updated_utc"] = utc_now()
    atomic_json(state_path, state)
    if no_optimized_parameters and not navigator_rebind_retry:
        if attempt_number == 1:
            recovery_profile = launcher.authoritative_recovery_ini(handoff, timeframe)
            attempt["reason"] = "no_optimized_parameters_retry_with_hash_bound_wide_profile"
        else:
            recovery_profile = launcher.authoritative_parameter_file_recovery_ini(handoff, timeframe)
            attempt["reason"] = ("no_optimized_parameters_retry_with_explicit_expert_parameters_file"
                                  if attempt_number == 2 else
                                  "no_optimized_parameters_retry_preserving_loaded_expert_grid")
        attempt["recovery_profile"] = str(recovery_profile)
        attempt["recovery_profile_sha256"] = sha256(recovery_profile)
        if attempt_number == 2:
            cycle_token = re.sub(r"[^A-Za-z0-9_-]", "_", str(handoff["cycle_id"]))
            tf_token = re.sub(r"[^A-Za-z0-9_-]", "_", str(timeframe))
            set_path = (launcher.STAGE3_PORTABLE_DATA_DIR / "MQL5" / "Profiles" / "Tester" /
                        f"{cycle_token}.{tf_token}.authenticated-retry.set")
            attempt["expert_parameters_file"] = str(set_path.resolve())
            attempt["expert_parameters_sha256"] = sha256(set_path)
        state = load_object(state_path)
        state["stage3_auto_timeframe_retry_history"][-1] = attempt
        state["updated_utc"] = utc_now()
        atomic_json(state_path, state)
        result = launch_runner(handoff_path, timeframe, _ini_override=recovery_profile)
    elif (not ui_false_rejection and expert_binding_retry
          and (attempt_number == 1
               or (attempt_number == 2 and prior_exact_profile_rebind_sequence))):
        # A newly generated EA may not yet be present in the Tester selector.
        # The original path depends on foreground Navigator interaction to
        # register it. Use the authenticated, per-cycle .set profile instead:
        # its full EA path and explicit input grid let MT5 start the exact model
        # without a desktop focus handoff.
        recovery_profile = launcher.authoritative_parameter_file_recovery_ini(
            handoff, timeframe)
        attempt["reason"] = (
            "terminal_lost_before_authenticated_run_evidence_retry_with_exact_parameter_file"
        )
        attempt["recovery_profile"] = str(recovery_profile)
        attempt["recovery_profile_sha256"] = sha256(recovery_profile)
        cycle_token = re.sub(r"[^A-Za-z0-9_-]", "_", str(handoff["cycle_id"]))
        tf_token = re.sub(r"[^A-Za-z0-9_-]", "_", str(timeframe))
        set_path = (launcher.STAGE3_PORTABLE_DATA_DIR / "MQL5" / "Profiles" / "Tester" /
                    f"{cycle_token}.{tf_token}.authenticated-retry.set")
        attempt["expert_parameters_file"] = str(set_path.resolve())
        attempt["expert_parameters_sha256"] = sha256(set_path)
        state = load_object(state_path)
        state["stage3_auto_timeframe_retry_history"][-1] = attempt
        state["updated_utc"] = utc_now()
        atomic_json(state_path, state)
        result = launch_runner(
            handoff_path, timeframe, _ini_override=recovery_profile,
            _recovery_reason="EXPERT_INPUT_BINDING_UNAVAILABLE")
    else:
        if navigator_rebind_retry:
            attempt["reason"] = "no_optimized_parameters_retry_with_deferred_exact_navigator_rebind"
            state = load_object(state_path)
            state["stage3_auto_timeframe_retry_history"][-1] = attempt
            state["updated_utc"] = utc_now()
            atomic_json(state_path, state)
        result = launch_runner(handoff_path, timeframe)
    attempt.update({"status": "started" if result.get("status") in {
                        "START_REQUIRED", "STARTING", "OPTIMIZING"} else "failed",
                    "result_status": result.get("status"), "finished_utc": utc_now(),
                    "new_runtime_sha256": (sha256(runtime_path) if runtime_path.is_file() else None),
                    "runtime_input_schema_binding": (
                        (load_object(runtime_path).get("input_schema_binding") or {})
                        if runtime_path.is_file() else None)})
    state = load_object(state_path)
    state["stage3_auto_timeframe_retry_history"][-1] = attempt
    state["updated_utc"] = utc_now()
    atomic_json(state_path, state)
    if result.get("status") == "START_REQUIRED":
        update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                        "stage3_start_required", f"desktop_start_stage3_{timeframe.lower()}")
        return {"status": "START_REQUIRED", "timeframe": timeframe,
                "runtime": str(runtime_path.resolve()), "reason": result.get("start_required_reason"),
                "zero_token": True}
    if result.get("status") in {"INFRASTRUCTURE_FAILURE", "WATCHER_REGISTRATION_FAILED"}:
        return {"status": "STAGE3_TIMEFRAME_RETRY_FAILED", "timeframe": timeframe,
                "runtime": str(runtime_path.resolve()), "zero_token": True}
    update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                    f"stage3_{timeframe.lower()}_running", f"watch_stage3_{timeframe.lower()}")
    return {"status": "STAGE3_TIMEFRAME_RETRY_STARTED", "timeframe": timeframe,
            "runtime": str(runtime_path.resolve()), "zero_token": True}


def restart_stale_timeframe(state_path: Path, handoff_path: Path,
                            launch_runner=launcher.launch_and_register) -> dict:
    """Retry the same authenticated timeframe once after its runtime is proven dead."""
    state, handoff = load_object(state_path), load_object(handoff_path)
    runtime_path = _handoff_path(handoff.get("runtime_manifest", ""))
    runtime = load_object(runtime_path)
    require(stale_runtime_restart_is_ready(state, handoff, runtime),
            "same-timeframe restart requires a dead, stale, non-rotatable Stage 3 runtime")
    timeframe = str(runtime["timeframe"])
    operator_authorization = state.get("stage3_operator_retry_authorization")
    operator_retry_recovery = stage3_operator_retry_is_ready(state, handoff, runtime)
    input_surface_terminal_loss_recovery = fixed_input_surface_terminal_loss_retry_is_ready(
        state, handoff, runtime)
    navigator_binding_terminal_loss_recovery = navigator_binding_terminal_loss_retry_is_ready(
        state, handoff, runtime)
    post_click_interrupted_start_recovery = (
        interrupted_start_retry_is_ready(state, handoff, runtime))
    dead_opt_recovery = state.get("stage3_failure_detail") == "dead_optimizing_runtime_without_completion_evidence"
    stalled_compute_recovery = (runtime.get("status") == COMPUTE_LIMIT_STATUS
                                and runtime.get("failure_mode") == "compute_stalled")
    history = list(state.get("stage3_auto_timeframe_retry_history", []))
    prior_index = next((index for index in range(len(history) - 1, -1, -1)
                        if isinstance(history[index], dict)
                        and history[index].get("cycle_id") == state.get("cycle_id")
                        and history[index].get("timeframe") == timeframe), None)
    retry_marker = state.get("stage3_retry_launch_in_progress")
    retry_launch_recovery = bool(
        isinstance(retry_marker, dict)
        and retry_marker.get("cycle_id") == state.get("cycle_id")
        and retry_marker.get("timeframe") == timeframe
        and retry_marker.get("attempt") in {1, 2}
        and retry_marker.get("status") == "failed"
        and retry_marker.get("failure_detail") == "retry launch result is stale or does not match the authenticated timeframe")
    if operator_retry_recovery:
        attempt = {
            "cycle_id": state["cycle_id"], "timeframe": timeframe,
            "attempt": max((int(item.get("attempt", 0)) for item in history
                            if isinstance(item, dict)
                            and item.get("cycle_id") == state["cycle_id"]
                            and item.get("timeframe") == timeframe), default=0) + 1,
            "status": "launching",
            "reason": "operator_authorized_single_retry_after_authenticated_partial_optimization",
            "operator_authorization_id": operator_authorization.get("authorization_id"),
            "previous_runtime_sha256": sha256(runtime_path),
            "failure_event_sha256": sha256(Path(state["stage3_failure_event"])),
            "ini_sha256": handoff["ini_sha256"][timeframe], "started_utc": utc_now(),
        }
        history.append(attempt)
        prior_index = len(history) - 1
    elif retry_launch_recovery:
        attempt = {
            "cycle_id": state["cycle_id"], "timeframe": timeframe,
            "attempt": int(retry_marker.get("attempt", 1)) + 1, "status": "launching",
            "reason": "authenticated_partial_start_after_retry_validator_failure",
            "previous_runtime_sha256": sha256(runtime_path),
            "failure_event_sha256": sha256(Path(state["stage3_failure_event"])),
            "ini_sha256": handoff["ini_sha256"][timeframe], "started_utc": utc_now(),
        }
        history.append(attempt)
        prior_index = len(history) - 1
    elif input_surface_terminal_loss_recovery:
        attempt = {
            "cycle_id": state["cycle_id"], "timeframe": timeframe,
            "attempt": max((int(item.get("attempt", 0)) for item in history
                            if isinstance(item, dict)
                            and item.get("cycle_id") == state["cycle_id"]
                            and item.get("timeframe") == timeframe), default=0) + 1,
            "status": "launching",
            "reason": "one_bounded_retry_after_authenticated_fixed_input_surface_reconciliation",
            "previous_runtime_sha256": sha256(runtime_path),
            "failure_event_sha256": sha256(Path(state["stage3_failure_event"])),
            "ini_sha256": handoff["ini_sha256"][timeframe], "started_utc": utc_now(),
        }
        history.append(attempt)
        prior_index = len(history) - 1
    elif navigator_binding_terminal_loss_recovery:
        attempt = {
            "cycle_id": state["cycle_id"], "timeframe": timeframe,
            "attempt": 2, "status": "launching",
            "reason": "one_bounded_retry_after_exact_navigator_binding_terminal_loss",
            "previous_runtime_sha256": sha256(runtime_path),
            "failure_event_sha256": sha256(Path(state["stage3_failure_event"])),
            "ini_sha256": handoff["ini_sha256"][timeframe], "started_utc": utc_now(),
        }
        history.append(attempt)
        prior_index = len(history) - 1
    elif prior_index is not None:
        attempt = dict(history[prior_index])
    else:
        attempt = {
            "cycle_id": state["cycle_id"], "timeframe": timeframe,
            "attempt": 1, "status": "launching",
            "reason": ("one_shot_retry_after_no_worker_progress_stall"
                       if stalled_compute_recovery else
                       "one_shot_retry_after_post_click_start_handshake_failure"
                       if post_click_interrupted_start_recovery else "stale_unregistered_runtime"),
            "previous_runtime_sha256": sha256(runtime_path),
            "failure_event_sha256": sha256(Path(state["stage3_failure_event"])),
            "ini_sha256": handoff["ini_sha256"][timeframe],
            "started_utc": utc_now(),
        }
        history.append(attempt)
        prior_index = len(history) - 1
    attempt.update({"status": "launching", "last_launch_check_utc": utc_now()})
    history[prior_index] = attempt
    state["stage3_auto_timeframe_retry_history"] = history[-9:]
    state["updated_utc"] = utc_now()
    atomic_json(state_path, state)
    try:
        if input_surface_terminal_loss_recovery:
            archive_dir = runtime_path.parent / (
                f"{state['cycle_id']}.stage3.{timeframe}.fixed_input_surface_terminal_loss.archive")
            require(not archive_dir.exists(), f"fixed-input retry archive already exists: {archive_dir}")
            failure_path = Path(state["stage3_failure_event"])
            ini_path = Path(handoff["ini_files"][timeframe])
            reconciliation = launcher.authenticated_input_surface_reconciliation(
                handoff, launcher._read_profile_text(ini_path))
            require(failure_path.is_file() and ini_path.is_file()
                    and sha256(runtime_path) == attempt["previous_runtime_sha256"]
                    and sha256(failure_path) == attempt["failure_event_sha256"]
                    and sha256(ini_path) == attempt["ini_sha256"]
                    and reconciliation.get("missing_fixed_source_defaults"),
                    "fixed-input retry evidence changed before archival")
            archive_dir.mkdir(parents=True)
            archived_runtime = archive_dir / runtime_path.name
            archived_failure = archive_dir / failure_path.name
            archived_ini = archive_dir / ini_path.name
            shutil.copy2(runtime_path, archived_runtime)
            shutil.copy2(failure_path, archived_failure)
            shutil.copy2(ini_path, archived_ini)
            receipt = {
                "schema_version": 1, "cycle_id": state["cycle_id"],
                "stage": 3, "timeframe": timeframe,
                "reason": "one_bounded_retry_after_authenticated_fixed_input_surface_reconciliation",
                "runtime_manifest": str(archived_runtime.resolve()),
                "runtime_manifest_sha256": sha256(archived_runtime),
                "failure_event": str(archived_failure.resolve()),
                "failure_event_sha256": sha256(archived_failure),
                "ini_path": str(archived_ini.resolve()), "ini_sha256": sha256(archived_ini),
                "source_reconciliation": reconciliation,
                "ex5_sha256": (runtime.get("deployment_reconciliation") or {}).get("authenticated_sha256"),
                "archived_utc": utc_now(),
            }
            receipt_path = archive_dir / "retry_evidence.json"
            atomic_json(receipt_path, receipt)
            attempt.update({"incomplete_evidence_archive": str(archive_dir.resolve()),
                            "retry_evidence_receipt": str(receipt_path.resolve()),
                            "retry_evidence_receipt_sha256": sha256(receipt_path),
                            "source_reconciliation": reconciliation})
            history[prior_index] = attempt
            state = load_object(state_path)
            state["stage3_auto_timeframe_retry_history"][-1] = attempt
            atomic_json(state_path, state)
        elif navigator_binding_terminal_loss_recovery:
            archive_dir = runtime_path.parent / (
                f"{state['cycle_id']}.stage3.{timeframe}.navigator_binding_terminal_loss.archive")
            require(not archive_dir.exists(), f"binding retry archive already exists: {archive_dir}")
            failure_path = Path(state["stage3_failure_event"])
            ini_path = Path(handoff["ini_files"][timeframe])
            require(failure_path.is_file() and ini_path.is_file()
                    and sha256(runtime_path) == attempt["previous_runtime_sha256"]
                    and sha256(failure_path) == attempt["failure_event_sha256"]
                    and sha256(ini_path) == attempt["ini_sha256"],
                    "Navigator binding retry evidence changed before archival")
            archive_dir.mkdir(parents=True)
            archived_runtime = archive_dir / runtime_path.name
            archived_failure = archive_dir / failure_path.name
            archived_ini = archive_dir / ini_path.name
            shutil.copy2(runtime_path, archived_runtime)
            shutil.copy2(failure_path, archived_failure)
            shutil.copy2(ini_path, archived_ini)
            receipt = {
                "schema_version": 1, "cycle_id": state["cycle_id"],
                "stage": 3, "timeframe": timeframe,
                "reason": "one_bounded_retry_after_exact_navigator_binding_terminal_loss",
                "runtime_manifest": str(archived_runtime.resolve()),
                "runtime_manifest_sha256": sha256(archived_runtime),
                "failure_event": str(archived_failure.resolve()),
                "failure_event_sha256": sha256(archived_failure),
                "ini_path": str(archived_ini.resolve()), "ini_sha256": sha256(archived_ini),
                "ex5_sha256": (runtime.get("deployment_reconciliation") or {}).get("authenticated_sha256"),
                "archived_utc": utc_now(),
            }
            receipt_path = archive_dir / "retry_evidence.json"
            atomic_json(receipt_path, receipt)
            attempt.update({"incomplete_evidence_archive": str(archive_dir.resolve()),
                            "retry_evidence_receipt": str(receipt_path.resolve()),
                            "retry_evidence_receipt_sha256": sha256(receipt_path)})
            history[prior_index] = attempt
            state = load_object(state_path)
            state["stage3_auto_timeframe_retry_history"][-1] = attempt
            atomic_json(state_path, state)
        elif operator_retry_recovery:
            archive_dir = runtime_path.parent / (
                f"{state['cycle_id']}.stage3.{timeframe}.operator_retry_"
                f"{operator_authorization['authorization_id'][:10]}.archive")
            require(not archive_dir.exists(), f"operator retry archive already exists: {archive_dir}")
            archive_dir.mkdir(parents=True)
            preserved_opt = _exact_runtime_opt_path(handoff, runtime, timeframe)
            require(preserved_opt is not None and preserved_opt.is_file(),
                    "operator retry OPT evidence is unavailable for archival")
            shutil.copy2(runtime_path, archive_dir / runtime_path.name)
            archived_opt = archive_dir / preserved_opt.name
            shutil.copy2(preserved_opt, archived_opt)
            attempt.update({"failed_attempt_archive": str(archive_dir.resolve()),
                            "failed_attempt_runtime_sha256": sha256(archive_dir / runtime_path.name),
                            "failed_attempt_opt": str(archived_opt.resolve()),
                            "failed_attempt_opt_sha256": sha256(archived_opt)})
            history[prior_index] = attempt
            state = load_object(state_path)
            state["stage3_auto_timeframe_retry_history"][-1] = attempt
            atomic_json(state_path, state)
        elif post_click_interrupted_start_recovery:
            archive_dir = runtime_path.parent / (
                f"{state['cycle_id']}.stage3.{timeframe}.post_click_interrupted_start.archive")
            require(not archive_dir.exists(), f"interrupted-start archive already exists: {archive_dir}")
            opt_path = _exact_runtime_opt_path(handoff, runtime, timeframe)
            failure_path = Path(state["stage3_failure_event"])
            ini_path = Path(handoff["ini_files"][timeframe])
            require(opt_path is not None and opt_path.is_file()
                    and failure_path.is_file() and ini_path.is_file()
                    and sha256(runtime_path) == attempt["previous_runtime_sha256"]
                    and sha256(failure_path) == attempt["failure_event_sha256"]
                    and sha256(ini_path) == attempt["ini_sha256"],
                    "interrupted-start retry evidence changed before archival")
            archive_dir.mkdir(parents=True)
            archived_runtime = archive_dir / runtime_path.name
            archived_opt = archive_dir / opt_path.name
            archived_failure = archive_dir / failure_path.name
            archived_ini = archive_dir / ini_path.name
            shutil.copy2(runtime_path, archived_runtime)
            shutil.copy2(opt_path, archived_opt)
            shutil.copy2(failure_path, archived_failure)
            shutil.copy2(ini_path, archived_ini)
            archive_receipt = {
                "schema_version": 1,
                "cycle_id": state["cycle_id"], "stage": 3, "timeframe": timeframe,
                "reason": "one_shot_retry_after_post_click_start_handshake_failure",
                "runtime_manifest": str(archived_runtime.resolve()),
                "runtime_manifest_sha256": sha256(archived_runtime),
                "opt_path": str(archived_opt.resolve()), "opt_sha256": sha256(archived_opt),
                "failure_event": str(archived_failure.resolve()),
                "failure_event_sha256": sha256(archived_failure),
                "ini_path": str(archived_ini.resolve()), "ini_sha256": sha256(archived_ini),
                "archived_utc": utc_now(),
            }
            receipt_path = archive_dir / "retry_evidence.json"
            atomic_json(receipt_path, archive_receipt)
            attempt.update({
                "incomplete_evidence_archive": str(archive_dir.resolve()),
                "retry_evidence_receipt": str(receipt_path.resolve()),
                "retry_evidence_receipt_sha256": sha256(receipt_path),
                "archived_opt_sha256": sha256(archived_opt),
                "archived_runtime_sha256": sha256(archived_runtime),
            })
            history[prior_index] = attempt
            state = load_object(state_path)
            state["stage3_auto_timeframe_retry_history"][-1] = attempt
            atomic_json(state_path, state)
        elif retry_launch_recovery:
            archive_dir = runtime_path.parent / f"{state['cycle_id']}.stage3.{timeframe}.attempt{attempt['attempt'] - 1}.failed_retry.archive"
            require(not archive_dir.exists(), f"failed retry archive already exists: {archive_dir}")
            archive_dir.mkdir(parents=True)
            shutil.copy2(runtime_path, archive_dir / runtime_path.name)
            preserved_opt = _exact_runtime_opt_path(handoff, runtime, timeframe)
            require(preserved_opt is not None and preserved_opt.is_file(),
                    "failed retry OPT evidence is unavailable for archival")
            archived_opt = archive_dir / preserved_opt.name
            shutil.copy2(preserved_opt, archived_opt)
            attempt.update({"failed_attempt_archive": str(archive_dir.resolve()),
                            "failed_attempt_runtime_sha256": sha256(archive_dir / runtime_path.name),
                            "failed_attempt_opt": str(archived_opt.resolve()),
                            "failed_attempt_opt_sha256": sha256(archived_opt)})
        if dead_opt_recovery:
            archive_dir = runtime_path.parent / f"{state['cycle_id']}.stage3.{timeframe}.dead_attempt.archive"
            require(not archive_dir.exists(), f"incomplete attempt archive already exists: {archive_dir}")
            opt_path = _exact_runtime_opt_path(handoff, runtime, timeframe)
            require(opt_path is not None and opt_path.is_file()
                    and sha256(opt_path) == load_object(Path(state["stage3_failure_event"])).get("opt_sha256"),
                    "preserved partial OPT no longer matches the infrastructure receipt")
            archive_dir.mkdir(parents=True)
            shutil.copy2(runtime_path, archive_dir / runtime_path.name)
            shutil.copy2(opt_path, archive_dir / opt_path.name)
            attempt.update({"incomplete_evidence_archive": str(archive_dir.resolve()),
                            "archived_opt_sha256": sha256(archive_dir / opt_path.name),
                            "archived_runtime_sha256": sha256(archive_dir / runtime_path.name)})
            history[prior_index] = attempt
            state = load_object(state_path)
            state["stage3_auto_timeframe_retry_history"][-1] = attempt
            atomic_json(state_path, state)
        elif stalled_compute_recovery:
            archive_dir = runtime_path.parent / f"{state['cycle_id']}.stage3.{timeframe}.stalled_attempt.archive"
            require(not archive_dir.exists(), f"stalled attempt archive already exists: {archive_dir}")
            opt_path = _exact_runtime_opt_path(handoff, runtime, timeframe)
            compute_event_path = Path(str(runtime.get("timeframe_event") or ""))
            compute_path = timeframe_compute_path(handoff, timeframe)
            require(opt_path is not None and opt_path.is_file()
                    and compute_event_path.is_file() and compute_path.is_file(),
                    "stalled retry evidence is incomplete")
            compute_event = load_object(compute_event_path)
            compute_record = load_object(compute_path)
            event_metrics = (compute_event.get("metrics")
                             if isinstance(compute_event.get("metrics"), dict) else {
                                 key: compute_event.get(key)
                                 for key in ("completed_passes", "elapsed_seconds", "seconds_since_last_pass")
                                 if compute_event.get(key) is not None
                             })
            event_evidence = (compute_event.get("evidence")
                              if isinstance(compute_event.get("evidence"), dict) else {
                                  "active_worker_pids": compute_event.get("alive_worker_pids", [])
                              })
            require(compute_event.get("status") == COMPUTE_LIMIT_STATUS
                    and compute_event.get("schema_version") == 2
                    and compute_event.get("cycle_id") == state["cycle_id"]
                    and compute_event.get("stage") == 3
                    and compute_event.get("timeframe") == timeframe
                    and compute_event.get("event_id") ==
                        f"{state['cycle_id']}:stage3:{timeframe}:compute_budget_exceeded"
                    and compute_event.get("failure_mode") == "compute_stalled"
                    and compute_event.get("completion_reason") == "progress_stall_budget_exceeded_without_active_workers"
                    and compute_event.get("consumed") is False
                    and isinstance(event_evidence.get("active_worker_pids"), list)
                    and not event_evidence.get("active_worker_pids")
                    and int(event_metrics.get("completed_passes", 0)) > 0
                    and float(event_metrics.get("seconds_since_last_pass", 0))
                        >= int(runtime.get("progress_stall_budget_seconds", 900))
                    and compute_record.get("source_event_id") == compute_event.get("event_id")
                    and compute_record.get("source_event_sha256") == sha256(compute_event_path)
                    and compute_record.get("schema_version") == 2
                    and compute_record.get("cycle_id") == state["cycle_id"]
                    and compute_record.get("stage") == 3
                    and compute_record.get("ea") == handoff["ea"]
                    and compute_record.get("timeframe") == timeframe
                    and compute_record.get("outcome") == "COMPUTE_SKIPPED"
                    and compute_record.get("failure_mode") == "compute_stalled"
                    and compute_record.get("metrics") == event_metrics
                    and compute_record.get("evidence") == event_evidence,
                    "stalled retry evidence does not match the compute event")
            archive_dir.mkdir(parents=True)
            shutil.copy2(runtime_path, archive_dir / runtime_path.name)
            archived_opt = archive_dir / opt_path.name
            shutil.copy2(opt_path, archived_opt)
            archived_event = archive_dir / compute_event_path.name
            shutil.copy2(compute_event_path, archived_event)
            archived_compute = archive_dir / compute_path.name
            shutil.copy2(compute_path, archived_compute)
            attempt.update({
                "failed_attempt_archive": str(archive_dir.resolve()),
                "failed_attempt_runtime_sha256": sha256(archive_dir / runtime_path.name),
                "failed_attempt_opt": str(archived_opt.resolve()),
                "failed_attempt_opt_sha256": sha256(archived_opt),
                "failed_attempt_compute_event": str(archived_event.resolve()),
                "failed_attempt_compute_event_sha256": sha256(archived_event),
                "failed_attempt_compute_record": str(archived_compute.resolve()),
                "failed_attempt_compute_record_sha256": sha256(archived_compute),
            })
            history[prior_index] = attempt
            state = load_object(state_path)
            state["stage3_auto_timeframe_retry_history"][-1] = attempt
            atomic_json(state_path, state)
        # Keep the supervisor from treating the still-held failure state as a
        # terminal hold while this authenticated replacement runtime is being
        # launched. The supervisor's infrastructure path may retire owned
        # Stage 3 processes; publish the active timeframe before starting it.
        state = load_object(state_path)
        state.update({
            "current_stage": "stage3_running",
            "stage_status": f"stage3_{timeframe.lower()}_running",
            "next_action": f"watch_stage3_{timeframe.lower()}",
            "stage3_retry_launch_in_progress": {
                "cycle_id": state.get("cycle_id"),
                "timeframe": timeframe,
                "attempt": attempt.get("attempt"),
                "prior_runtime_sha256": attempt.get("previous_runtime_sha256"),
                "failure_event_sha256": attempt.get("failure_event_sha256"),
                "status": "launching",
                "operator_authorization_id": (operator_authorization.get("authorization_id")
                                              if operator_retry_recovery else None),
                "started_utc": utc_now(),
            },
            "updated_utc": utc_now(),
        })
        if operator_retry_recovery:
            state["stage3_operator_retry_authorization"] = {
                **operator_authorization, "status": "consumed", "consumed_utc": utc_now(),
            }
        atomic_json(state_path, state)
        launched = launch_runner(handoff_path, timeframe)
        require(isinstance(launched, dict), "retry launcher returned no structured result")
        require(runtime_path.is_file(), "retry launcher did not write a fresh runtime manifest")
        current_runtime = load_object(runtime_path)
        runtime_hash = sha256(runtime_path)
        valid_statuses = {"START_REQUIRED", "STARTING", "OPTIMIZING", "TIMEFRAME_COMPLETE",
                          "PARTIAL_TIMEFRAME_COMPLETE", "COMPUTE_BUDGET_EXCEEDED"}
        start_required_progress = current_runtime.get("tester_agent_progress", {})
        if current_runtime.get("status") == "START_REQUIRED":
            start_required_progress = launcher.fresh_agent_optimization_progress(
                handoff, current_runtime, current_runtime.get("worker_process_evidence", []),
                launcher.DEFAULT_DATA_DIR, allow_exited=True) or start_required_progress
        authenticated_partial_start = bool(
            current_runtime.get("status") == "START_REQUIRED"
            and isinstance(start_required_progress, dict)
            and start_required_progress.get("source") == "authenticated_fresh_local_agent_logs"
            and start_required_progress.get("cycle_id") == state.get("cycle_id")
            and start_required_progress.get("ea") == state.get("current_ea")
            and start_required_progress.get("symbol") == handoff.get("symbol")
            and start_required_progress.get("timeframe") == timeframe
            and start_required_progress.get("ini_sha256") == handoff.get("ini_sha256", {}).get(timeframe)
            and bool(start_required_progress.get("evidence"))
            and all(isinstance(item, dict)
                    and item.get("exact_expert_loaded") is True
                    and item.get("optimized_inputs_loaded") is True
                    and item.get("symbol_synchronized") is True
                    and item.get("optimization_pass_started") is True
                    and item.get("optimization_pass_succeeded") is True
                    and item.get("optimization_finished") is False
                    for item in start_required_progress.get("evidence", []))
            and start_required_progress.get("optimization_finished") is not True)
        waiting_terminal_live = False
        if current_runtime.get("status") == "START_REQUIRED":
            try:
                terminal_pid = int(current_runtime.get("terminal_pid") or 0)
                terminal_process = psutil.Process(terminal_pid)
                waiting_terminal_live = (
                    terminal_pid > 0
                    and Path(terminal_process.exe()).resolve() == launcher.STAGE3_PORTABLE_TERMINAL.resolve()
                    and float(terminal_process.create_time()) >= float(current_runtime.get("launched_epoch", 0)) - 5.0
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError, ValueError, TypeError):
                waiting_terminal_live = False
        if (current_runtime.get("status") == "START_REQUIRED"
                and not authenticated_partial_start and not waiting_terminal_live):
            valid_statuses.discard("START_REQUIRED")
        valid_manifest = bool(
            runtime_hash != attempt["previous_runtime_sha256"]
            and current_runtime.get("status") in valid_statuses
            and launched.get("status") == current_runtime.get("status")
            and current_runtime.get("cycle_id") == state.get("cycle_id")
            and current_runtime.get("ea") == state.get("current_ea")
            and current_runtime.get("timeframe") == timeframe
            and Path(current_runtime.get("ini", "")).resolve()
                == Path(handoff["ini_files"][timeframe]).resolve()
            and current_runtime.get("ini_sha256") == handoff["ini_sha256"].get(timeframe)
            and float(current_runtime.get("launched_epoch", 0)) > float(runtime.get("launched_epoch", 0)))
        require(valid_manifest, "retry launch result is stale or does not match the authenticated timeframe")
        retry_launch_epoch = float(current_runtime["launched_epoch"])
        retry_launch_id = hashlib.sha256(
            f"{state['cycle_id']}|{timeframe}|{attempt['previous_runtime_sha256']}|"
            f"{retry_launch_epoch:.6f}|{attempt['failure_event_sha256']}".encode("utf-8")
        ).hexdigest().upper()
        binding = {
            "attempt": int(attempt.get("attempt", 1)),
            "previous_runtime_sha256": attempt["previous_runtime_sha256"],
            "failure_event_sha256": attempt["failure_event_sha256"],
            "ini_sha256": handoff["ini_sha256"][timeframe],
            "archived_opt_sha256": attempt.get("archived_opt_sha256"),
            "launch_epoch": retry_launch_epoch, "launch_id": retry_launch_id,
            "bound_utc": utc_now(),
        }
        current_runtime["stale_retry_binding"] = binding
        atomic_json(runtime_path, current_runtime)
        attempt.update({"status": "started", "new_runtime_sha256": sha256(runtime_path),
                        "stale_retry_binding": binding, "retry_launch_epoch": retry_launch_epoch,
                        "retry_launch_id": retry_launch_id, "result_status": launched.get("status"),
                        "completed_utc": utc_now()})
        history[prior_index] = attempt
        state["stage3_auto_timeframe_retry_history"] = history[-9:]
        stale_retry_failure = state.pop("stage3_auto_retry_failure", None)
        if stale_retry_failure:
            attempt["cleared_stale_retry_failure"] = stale_retry_failure
            history[prior_index] = attempt
            state["stage3_auto_timeframe_retry_history"] = history[-9:]
        state["stage3_retry_launch_in_progress"] = {
            **state.get("stage3_retry_launch_in_progress", {}),
            "attempt": int(attempt.get("attempt", 1)),
            "status": "started",
            "retry_launch_id": retry_launch_id,
            "new_runtime_sha256": sha256(runtime_path),
            "updated_utc": utc_now(),
        }
        atomic_json(state_path, state)
        update_progress(state_path, handoff["cycle_id"], handoff["ea"],
                        f"stage3_{timeframe.lower()}_running", f"watch_stage3_{timeframe.lower()}")
        return {"status": "STAGE3_TIMEFRAME_RETRY_STARTED", "timeframe": timeframe,
                "runtime": str(runtime_path.resolve()), "zero_token": True}
    except Exception as exc:
        attempt.update({"status": "failed", "failure_detail": str(exc)[:1000],
                        "completed_utc": utc_now()})
        history[prior_index] = attempt
        state["stage3_auto_timeframe_retry_history"] = history[-9:]
        state["stage3_auto_retry_failure"] = str(exc)[:1000]
        state["current_stage"] = "stage3_running"
        state["stage_status"] = "stage3_infrastructure_failure"
        state["next_action"] = "inspect_stage3_infrastructure_failure"
        state["stage3_failure_class"] = "infrastructure"
        state["stage3_failure_detail"] = "dead_optimizing_runtime_retry_launch_failed"
        state["stage3_retry_launch_in_progress"] = {
            **state.get("stage3_retry_launch_in_progress", {}),
            "status": "failed",
            "failure_detail": str(exc)[:1000],
            "updated_utc": utc_now(),
        }
        state["updated_utc"] = utc_now()
        atomic_json(state_path, state)
        return {"status": "STAGE3_AUTORETRY_HELD", "timeframe": timeframe,
                "runtime": str(runtime_path.resolve()), "reason": str(exc)[:1000],
                "zero_token": True}


def route_opt_surface_failure_to_stage2(state_path: Path, handoff: dict,
                                        runtime: dict, timeframe: str, detail: str) -> dict:
    """Persist a hash-bound Stage 3 parser failure as a Stage 2 repair request."""
    source = Path(handoff.get("source") or "")
    stage1_spec = Path(handoff.get("stage1_spec") or "")
    require(source.is_file() and stage1_spec.is_file(),
            "Stage 3 optimization-surface repair lacks the bound source or Stage 1 spec")
    require(sha256(source) == handoff.get("source_sha256")
            and sha256(stage1_spec) == handoff.get("stage1_spec_sha256"),
            "Stage 3 optimization-surface repair source/spec hash changed")
    cycle, ea = handoff["cycle_id"], handoff["ea"]
    state = load_object(state_path)
    require(state.get("cycle_id") == cycle and state.get("current_ea") == ea,
            "Stage 3 optimization-surface failure is stale for the current cycle")
    failure_path = Path(state.get("stage3_failure_event") or "")
    if not failure_path.is_file():
        record_failure(state_path, detail)
        state = load_object(state_path)
        failure_path = Path(state.get("stage3_failure_event") or "")
    require(failure_path.is_file(), "Stage 3 failure evidence was not persisted")
    event_path = state_path.parent / "events" / f"{cycle}.stage2.failed.from-stage3-opt-surface.json"
    require(not event_path.exists(), "Stage 2 repair event already exists for this Stage 3 failure")
    runtime_path = Path(handoff.get("runtime_manifest") or "")
    require(runtime_path.is_file() and runtime.get("cycle_id") == cycle
            and runtime.get("ea") == ea and runtime.get("timeframe") == timeframe,
            "Stage 3 runtime evidence does not match the current optimization failure")
    stage3_handoff_path = Path(state.get("last_stage3_handoff") or HANDOFF)
    require(stage3_handoff_path.is_file(), "Stage 3 handoff evidence is missing")
    stage3_handoff = load_object(stage3_handoff_path)
    require(stage3_handoff.get("cycle_id") == cycle and stage3_handoff.get("ea") == ea,
            "Stage 3 handoff evidence is stale")
    opt_path = Path(runtime.get("opt_path") or "")
    require(opt_path.is_file(), "completed Stage 3 OPT evidence is missing")
    event = {
        "schema_version": 2,
        "event_id": f"{cycle}:stage2:failed:stage3-opt-surface",
        "cycle_id": cycle,
        "stage": 2,
        "status": "failed",
        "failure_kind": "STAGE3_OPTIMIZATION_INPUT_SURFACE_MISMATCH",
        "ea": ea,
        "candidate_ea": ea,
        "source": str(source.resolve()),
        "source_sha256": sha256(source),
        "stage1_spec": str(stage1_spec.resolve()),
        "stage1_spec_sha256": sha256(stage1_spec),
        "failed_checks": ["stage3_optimization_input_surface"],
        "error": detail,
        "error_sha256": hashlib.sha256(detail.encode("utf-8")).hexdigest().upper(),
        "stage3_failure_event": str(failure_path.resolve()),
        "stage3_runtime": str(runtime_path.resolve()),
        "stage3_runtime_sha256": sha256(runtime_path),
        "stage3_handoff": str(stage3_handoff_path.resolve()),
        "stage3_handoff_sha256": sha256(stage3_handoff_path),
        "timeframe": timeframe,
        "opt_sha256": runtime.get("opt_sha256"),
        "ini_sha256": runtime.get("ini_sha256"),
        "recovery_action": "repair_stage2_failure",
        "recovery_policy": "Audit and requalify this exact candidate against the authenticated Stage 1 contract; preserve its strategy and input declarations. Stage 3 must rebuild its wide grid from the authenticated candidate input types before retrying.",
        "failed_utc": utc_now(),
        "rotation_eligible": False,
    }
    atomic_json(event_path, event)
    state.update({
        "current_stage": "stage2_pending",
        "stage_status": "stage2_repair_required",
        "hold_after_stage": max(2, int(state.get("hold_after_stage") or 2)),
        "next_action": "dispatch_stage2",
        "stage2_failure_event": str(event_path.resolve()),
        "stage2_repair_reason": detail,
        "stage2_repair_source": "stage3_opt_parse",
        "stage2_repair_guidance": event["recovery_policy"],
        "stage3_failure_detail": detail,
        "updated_utc": utc_now(),
    })
    atomic_json(state_path, state)
    return {"status": "STAGE2_REPAIR_REQUIRED", "source": "stage3_opt_parse",
            "failure_event": str(event_path.resolve()), "timeframe": timeframe,
            "reason": detail, "zero_token": True}


def record_failure(state_path: Path, detail: str) -> None:
    try:
        state = load_object(state_path)
        if (state.get("schema_version") == 2
                and state.get("current_stage") == "stage3_running"
                and state.get("stage_status") in {"stage3_running", "stage3_infrastructure_failure", "stage3_start_required", "stage3_m5_pending", "stage3_m5_compute_skipped", "stage3_m5_running", "stage3_m15_running", "stage3_h1_running"}):
            updated = dict(state)
            # Infrastructure holds never become EA performance failures or
            # candidate rotations. Preserve the runtime and diagnostic chain.
            updated.update({
                "current_stage": "stage3_running",
                "stage_status": "stage3_infrastructure_failure",
                "next_action": "inspect_stage3_infrastructure_failure",
                "hold_after_stage": 5 if authorizes_full_cycle_stage3(state) else 3,
                "stage3_failure_class": "infrastructure",
                "stage3_failure_detail": detail,
                "updated_utc": utc_now(),
            })
            event_path = Path(state_path).parent / "events" / f"{state.get('cycle_id')}.stage3.failed.json"
            failure_event = {
                "schema_version": 2, "event_id": f"{state.get('cycle_id')}:stage3:failed",
                "cycle_id": state.get("cycle_id"), "stage": 3, "status": "failed",
                "failure_kind": "STAGE3_EXECUTION_FAILURE", "ea": state.get("current_ea"),
                "error": detail, "failed_utc": updated["updated_utc"],
                "rotation_eligible": False, "consumed": False,
            }
            # Bind the failure receipt to the precise runtime/timeframe that
            # produced it. The canonical handoff remains an independent check.
            runtime_path = (Path(state_path).parent / "stage3_v2"
                            / f"{state.get('cycle_id')}.stage3.runtime.json")
            handoff_path = Path(str(state.get("last_stage3_handoff") or ""))
            if handoff_path.is_file():
                try:
                    handoff = load_object(handoff_path)
                    bound_runtime = Path(str(handoff.get("runtime_manifest") or ""))
                    if (handoff.get("cycle_id") == state.get("cycle_id")
                            and bound_runtime.parent.resolve() == runtime_path.parent.resolve()
                            and bound_runtime.name.startswith(f"{state.get('cycle_id')}.stage3.runtime")
                            and bound_runtime.is_file()):
                        runtime_path = bound_runtime
                except (OSError, ValueError, TypeError):
                    pass
            if runtime_path.is_file():
                try:
                    runtime = load_object(runtime_path)
                    if (runtime.get("cycle_id") == state.get("cycle_id")
                            and runtime.get("stage") == 3
                            and runtime.get("ea") == state.get("current_ea")
                            and runtime.get("status") == "INFRASTRUCTURE_FAILURE"):
                        failure_event.update({
                            "runtime_manifest": str(runtime_path.resolve()),
                            "runtime_manifest_sha256": sha256(runtime_path),
                            "timeframe": runtime.get("timeframe"),
                            "runtime_failure_kind": runtime.get("failure_kind"),
                        })
                except (OSError, ValueError, TypeError):
                    # Preserve the original failure hold if optional binding
                    # evidence is unreadable; recovery will remain fail-closed.
                    pass
            event_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_json(event_path, failure_event)
            updated["stage3_failure_event"] = str(event_path.resolve())
            atomic_json(state_path, updated)
            if EA_LEDGER.is_file():
                ledger = load_object(EA_LEDGER)
                records = ledger.get("records", [])
                for record in records:
                    if record.get("ea") == state.get("current_ea"):
                        record.update({
                            "status": "STAGE3_INFRASTRUCTURE_HELD",
                            "confidence": "INFRASTRUCTURE_HELD",
                            "whole_cycle_complete": False,
                            "whole_cycle_reason": "Stage 3 infrastructure failure held for later inspection",
                            "infrastructure_hold": {
                                "cycle_id": state.get("cycle_id"),
                                "stage": 3,
                                "detail": detail,
                                "held_utc": updated["updated_utc"],
                            },
                        })
                        break
                atomic_json(EA_LEDGER, ledger)
    except Exception:
        pass


def authenticate_stage2_execution(state: dict) -> str:
    import kcos_supervisor_v2 as supervisor
    control = load_object(ROOT / "state" / "kcos_control_v2.json")
    valid, detail = supervisor.validate_stage2_completion(state, control, supervisor.DB)
    require(valid and "DPAPI-authenticated execution attestation verified" in detail,
            f"Stage 2 execution provenance is not authenticated: {detail}")
    return detail


def mark_release_attempt_failed(state_path: Path, detail: str, *, phase: str = "execution") -> None:
    try:
        state = load_object(state_path)
        attempt = state.get("stage2_release_attempt")
        if (state.get("stage_status") == "stage2_complete_held" and isinstance(attempt, dict)
                and attempt.get("status") in {"preflight_in_progress", "attempt_in_progress"}):
            attempt = dict(attempt)
            attempt.update({"status": "failed", "failure_phase": phase,
                            "failure_detail": str(detail)[:1000], "failed_utc": utc_now()})
            state["stage2_release_attempt"] = attempt
            state["updated_utc"] = utc_now()
            atomic_json(state_path, state)
    except Exception:
        pass


def authorize_stage3_restart_from_m5(state_path: Path, stage2_handoff_path: Path,
                                     handoff_path: Path) -> dict:
    """Prepare one user-authorized M5 restart after the exact no-Expert launch failure."""
    state = load_object(state_path)
    handoff = load_object(handoff_path)
    runtime_path = _handoff_path(handoff.get("runtime_manifest", ""))
    failure_path = Path(str(state.get("stage3_failure_event") or ""))
    require(state.get("current_stage") == "stage2_pending"
            and state.get("stage_status") == "stage2_repair_required"
            and state.get("stage2_repair_source") == "stage3_tester_journal"
            and (state.get("stage2_repair_journal") or {}).get("code") == "NO_EXPERT_IN_START_CONFIG",
            "M5 restart is only valid for the current no-Expert Stage 3 misroute")
    require(handoff.get("cycle_id") == state.get("cycle_id")
            and handoff.get("ea") == state.get("current_ea")
            and runtime_path.is_file() and failure_path.is_file(),
            "current Stage 3 handoff, runtime, or failure receipt is missing")
    runtime, failure = load_object(runtime_path), load_object(failure_path)
    require(runtime.get("cycle_id") == state.get("cycle_id")
            and runtime.get("status") == "INFRASTRUCTURE_FAILURE"
            and runtime.get("infrastructure_only") is True
            and (runtime.get("journal_diagnostic") or {}).get("code") == "NO_EXPERT_IN_START_CONFIG"
            and failure.get("cycle_id") == state.get("cycle_id")
            and failure.get("stage") == 3 and failure.get("status") == "failed"
            and failure.get("rotation_eligible") is False,
            "Stage 3 failure is not the authenticated no-Expert infrastructure failure")
    # The user explicitly authorized one new Stage 3 execution. Keep it bound
    # to this exact failure and Stage 2 publication; downstream stages retain
    # the existing Stage 3-only hold.
    prior_attempt = state.get("stage2_release_attempt")
    require(isinstance(prior_attempt, dict) and prior_attempt.get("status") == "released"
            and prior_attempt.get("cycle_id") == state.get("cycle_id"),
            "the prior Stage 3 release is not a consumed same-cycle release")
    state.update({"current_stage": "stage3_pending",
                  "stage_status": "stage2_complete_held",
                  "hold_after_stage": 2,
                  "next_action": "audit_stage2_before_stage3"})
    event, _verification = preparer.validate_stage2(
        state, load_object(stage2_handoff_path))
    require(event.get("cycle_id") == state.get("cycle_id")
            and event.get("ea") == state.get("current_ea"),
            "current Stage 2 publication does not match the failed Stage 3 candidate")
    nonce = "m5r-" + hashlib.sha256(
        f"{state['cycle_id']}|{sha256(failure_path)}|{utc_now()}".encode("utf-8")
    ).hexdigest()[:16]
    authorized_utc = utc_now()
    reason = ("User authorized one Stage 3 restart from M5 after the exact no-Expert Tester launch failure; "
              "Stage 4 and later remain held.")
    release = preparer.build_stage2_release_authorization(
        state, stage2_handoff_path, authorized_by="user_confirmed", reason=reason,
        authorized_utc=authorized_utc, authorization_nonce=nonce)
    release["status"] = "authorized"
    backup_dir = state_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / (
        f"{state_path.name}.pre_m5_restart_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.bak")
    require(not backup_path.exists(), "M5 restart state backup already exists")
    shutil.copy2(state_path, backup_path)
    state["stage3_one_run_release_authorization"] = release
    state["stage3_restart_from_m5_authorization"] = {
        "schema_version": 1, "status": "authorized", "cycle_id": state["cycle_id"],
        "authorized_by": "user_confirmed", "authorized_utc": authorized_utc,
        "authorization_nonce": nonce,
        "failure_event": str(failure_path.resolve()),
        "failure_event_sha256": sha256(failure_path),
        "runtime_manifest": str(runtime_path.resolve()),
        "runtime_manifest_sha256": sha256(runtime_path),
        "stage2_event_sha256": release["evidence"]["stage2_event_sha256"],
        "state_backup": str(backup_path.resolve()),
        "state_backup_sha256": sha256(backup_path),
        "reason": reason,
    }
    state["updated_utc"] = utc_now()
    atomic_json(state_path, state)
    return state


def main() -> int:
    parser_cli = argparse.ArgumentParser()
    parser_cli.add_argument("--state", type=Path, default=STATE)
    parser_cli.add_argument("--handoff", type=Path, default=HANDOFF)
    parser_cli.add_argument("--stage2-handoff", type=Path, default=STAGE2_HANDOFF)
    parser_cli.add_argument("--lease", type=Path, default=LEASE)
    parser_cli.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser_cli.add_argument("--start", action="store_true")
    parser_cli.add_argument("--restart-stale-timeframe", action="store_true",
                            help="retry one dead, stale current-cycle timeframe without rotating the EA")
    parser_cli.add_argument("--retry-unstarted-terminal", action="store_true",
                            help="retry once when the exact timeframe terminal died before run evidence")
    parser_cli.add_argument("--release-stage2-hold", action="store_true",
                            help="consume the exact-cycle, one-run Stage 2 release authorized for this cycle")
    parser_cli.add_argument("--restart-from-m5", action="store_true",
                            help="authorize one exact-cycle M5 restart after a no-Expert Stage 3 launch failure")
    args = parser_cli.parse_args()
    release_authorization = None
    try:
        state = load_object(args.state)
        if args.restart_from_m5:
            state = authorize_stage3_restart_from_m5(args.state, args.stage2_handoff, args.handoff)
            args.start = True
            args.release_stage2_hold = True
        if args.start:
            release_authorization = authenticated_journal_retry_stage2_release(state)
            if release_authorization is not None:
                attempt = dict(state["stage2_release_attempt"])
                attempt.update({
                    "status": "attempt_in_progress",
                    "retry_reuse_kind": "stage3_journal_candidate_error_reclassification",
                    "retry_reuse_event_id": state["stage3_repair_retry"]["event_id"],
                    "retry_reuse_receipt_sha256": state["stage3_repair_retry"]["repair_receipt_sha256"],
                    "retry_reuse_started_utc": utc_now(),
                })
                state["stage2_release_attempt"] = attempt
                state["updated_utc"] = utc_now()
                atomic_json(args.state, state)
        if args.restart_stale_timeframe:
            result = restart_stale_timeframe(args.state, args.handoff)
            print(json.dumps(result, separators=(",", ":")))
            return 0
        if args.retry_unstarted_terminal:
            result = retry_unstarted_terminal(args.state, args.handoff)
            print(json.dumps(result, separators=(",", ":")))
            return 0
        if args.release_stage2_hold:
            if not should_attempt_stage2_release(state):
                result = {"status": "HELD_AFTER_STAGE2", "detail": "release authorization does not match the exact held Stage 2 publication or a prior one-run attempt is recorded",
                          "cycle_id": state.get("cycle_id"), "zero_token": True}
                print(json.dumps(result, separators=(",", ":")))
                return 0
            prior_failure = {}
            prior_failure_path = Path(str(state.get("stage3_failure_event") or ""))
            if prior_failure_path.is_file():
                prior_failure = load_object(prior_failure_path)
            if (prior_failure.get("cycle_id") == state.get("cycle_id")
                    and prior_failure.get("stage") == 3
                    and prior_failure.get("error") == "Stage 3 source Stage 2 event is missing or changed"):
                release_reason = (
                    "User-confirmed one-run Stage 3 retry after repairing the current cycle's Stage 2 event identity check; subsequent stages remain governed by the existing full-cycle authorization."
                )
            elif operator_one_run_stage3_release_is_current(state):
                release_reason = state["stage3_one_run_release_authorization"]["reason"]
            else:
                release_reason = (
                    "User-confirmed one-run Stage 3 rerun after Stage 2 requalified the same EA following the diagnosed OPT input-grid mismatch; later stages remain held."
                    if state.get("stage2_repair_source") == "stage3_opt_parse"
                    else STAGE2_RELEASE_REASON
                )
            release_authorization = preparer.build_stage2_release_authorization(
                state, args.stage2_handoff, authorized_by="user_confirmed",
                reason=release_reason, authorized_utc=utc_now(),
                authorization_nonce=(
                    str(state["stage3_one_run_release_authorization"].get("authorization_nonce"))
                    if operator_one_run_stage3_release_is_current(state)
                    and state["stage3_one_run_release_authorization"].get("authorization_nonce")
                    else None))
            prior_attempt = state.get("stage2_release_attempt")
            if isinstance(prior_attempt, dict):
                if (prior_attempt.get("status") == "attempt_in_progress"
                        and stage2_tester_grid_repair_requalified(state)):
                    prior_attempt = dict(prior_attempt)
                    prior_attempt.update({
                        "status": "failed",
                        "failure_phase": "admission",
                        "failure_detail": ("Stage 3 admission did not recognize the authenticated repaired input-grid release; "
                                           "no new Stage 3 runtime was launched."),
                        "failed_utc": utc_now(),
                    })
                history = list(state.get("stage2_release_attempt_history", []))
                history.append(prior_attempt)
                state["stage2_release_attempt_history"] = history[-2:]
            state["stage2_release_attempt"] = dict(release_authorization,
                                                    status="preflight_in_progress",
                                                    attempt_started_utc=utc_now(),
                                                    authorization_source=(
                                                        "operator_one_run_release"
                                                        if operator_one_run_stage3_release_is_current(state)
                                                        else "existing_stage3_release_policy"))
            state["updated_utc"] = utc_now()
            atomic_json(args.state, state)
            try:
                provenance_detail = authenticate_stage2_execution(state)
                release_authorization["hermes_provenance"] = provenance_detail
                state = load_object(args.state)
                state["stage2_release_attempt"].update({
                    "hermes_provenance": provenance_detail,
                    "status": "attempt_in_progress",
                    "preflight_utc": utc_now(),
                })
                state["updated_utc"] = utc_now()
                atomic_json(args.state, state)
                template = find_current_template(ROOT, str(state["current_ea"]))
                dry_handoff = preparer.prepare(
                    args.state, args.stage2_handoff, args.handoff, args.lease, template, args.output_dir,
                    dry_run=True, grid_mode=state.get("stage3_grid_mode", "default"),
                    release_authorization=release_authorization)
                require(dry_handoff.get("auto_launch_allowed") is True,
                        "Stage 3 release preflight did not authorize automatic launch")
            except preparer.Stage3InputContractError as exc:
                mark_release_attempt_failed(args.state, str(exc), phase="preflight")
                failure = preparer.record_input_contract_failure(
                    args.state, args.stage2_handoff, exc.code, str(exc))
                print(json.dumps({
                    "status": "STAGE3_INPUT_CONTRACT_REJECTED",
                    "event_id": failure.get("event_id"),
                    "failure_kind": failure.get("failure_kind"),
                    "detail": str(exc),
                    "zero_token": True,
                }, separators=(",", ":")))
                return 0
            except Exception as exc:
                mark_release_attempt_failed(args.state, str(exc), phase="preflight")
                raise
        admission_hold = stage3_admission_hold(state, release_authorization)
        if admission_hold:
            result = {
                "status": "HELD_AFTER_STAGE2",
                "detail": admission_hold,
                "current_stage": state.get("current_stage"),
                "cycle_id": state.get("cycle_id"),
                "hold_after_stage": state.get("hold_after_stage"),
                "zero_token": True,
            }
            print(json.dumps(result, separators=(",", ":")))
            return 0
        # A recurring cron must not treat an inactive stage as an execution
        # failure. The supervisor owns admission; Stage 3 is a quiet no-op
        # until the canonical cycle is explicitly stage3_pending/running.
        if state.get("current_stage") not in {"stage3_pending", "stage3_running"}:
            result = {
                "status": "NOT_APPLICABLE",
                "detail": "Stage 3 is not admitted by canonical cycle state",
                "current_stage": state.get("current_stage"),
                "current_ea": state.get("current_ea"),
                "cycle_id": state.get("cycle_id"),
                "zero_token": True,
            }
            print(json.dumps(result, separators=(",", ":")))
            return 0
        restartable_hold = (
            state.get("current_stage") == "stage3_running"
            and state.get("stage_status") == "stage3_infrastructure_failure"
            and state.get("next_action") == "inspect_stage3_infrastructure_failure"
        )
        recoverable_dead_runtime = False
        if restartable_hold:
            try:
                h = load_object(args.handoff)
                mp = Path(h.get("runtime_manifest", ""))
                if mp.is_file():
                    old = load_object(mp)
                    recoverable_dead_runtime = (
                        old.get("status") == COMPUTE_LIMIT_STATUS
                        and old.get("duration_policy_action") == "rotate_reduced_over_budget"
                        and not psutil.pid_exists(int(old.get("terminal_pid") or 0))
                        and not any(psutil.pid_exists(int(pid)) for pid in old.get("worker_pids", []))
                    )
            except (OSError, ValueError, TypeError, KeyError):
                recoverable_dead_runtime = False
        retry_runtime_path = Path(load_object(args.handoff).get("runtime_manifest", ""))
        retry_runtime = load_object(retry_runtime_path) if retry_runtime_path.is_file() else {}
        retry_same_timeframe = (
            args.start
            and state.get("current_stage") == "stage3_running"
            and state.get("stage_status") == "stage3_infrastructure_failure"
            and stage3_repair_retry_is_ready(state)
            and stage3_repair_retry_matches_runtime(state, retry_runtime)
        )
        retry_wide_duration = (
            args.start and stage3_duration_retry_is_ready(state)
        )
        if retry_wide_duration:
            result = resume_stage3_wide_after_budget_update(args.state, args.handoff)
        elif retry_same_timeframe:
            result = retry_failed_timeframe(args.state, args.handoff)
        elif args.start or state.get("current_stage") == "stage3_pending" or recoverable_dead_runtime:
            result = start_stage3(args.state, args.stage2_handoff, args.handoff, args.lease,
                                  args.output_dir, release_authorization=release_authorization)
        elif restartable_hold:
            # Resume the existing authenticated START_REQUIRED runtime; do not
            # re-enter prepare or reject its live exact-cycle lease.
            result = process_completed_timeframe(args.state, args.handoff)
        else:
            result = process_completed_timeframe(args.state, args.handoff)
        # Final authoritative guard across both initial and resume paths.
        handoff_now = load_object(args.handoff)
        if "runtime_manifest" not in handoff_now:
            print(json.dumps(result, separators=(",", ":")))
            return 0
        manifest_path = Path(handoff_now["runtime_manifest"]).resolve()
        if manifest_path.is_file():
            manifest = load_object(manifest_path)
            journal = manifest.get("journal_diagnostic") or {}
            if (manifest.get("failure_kind") == "TESTER_JOURNAL_REJECTED"
                    and journal.get("code") == "TESTER_INPUT_GRID_REJECTED"):
                state_now = load_object(args.state)
                state_now.update({"current_stage": "stage2_pending",
                                  "stage_status": "stage2_repair_required",
                                  "hold_after_stage": 2,
                                  "next_action": "dispatch_stage2",
                                  "stage2_repair_reason": "MT5 Tester Journal rejected optimization input grid",
                                  "stage2_repair_source": "stage3_tester_journal",
                                  "stage2_repair_journal": journal,
                                  "updated_utc": utc_now()})
                atomic_json(args.state, state_now)
                result = {"status": "STAGE2_REPAIR_REQUIRED", "source": "stage3_tester_journal",
                          "runtime": str(manifest_path), "zero_token": True}
        print(json.dumps(result, separators=(",", ":")))
        return 0
    except Exception as exc:
        mark_release_attempt_failed(args.state, str(exc))
        if str(exc) == ALREADY_RUNNING_DETAIL:
            print(json.dumps({"status": "ALREADY_RUNNING", "detail": str(exc), "zero_token": True},
                             separators=(",", ":")))
            return 0
        record_failure(args.state, str(exc))
        print(json.dumps({"status": "INFRASTRUCTURE_FAILURE", "detail": str(exc), "zero_token": True},
                         separators=(",", ":")))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
