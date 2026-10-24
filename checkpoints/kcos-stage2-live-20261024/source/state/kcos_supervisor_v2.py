#!/usr/bin/env python3
"""Fail-closed, zero-token KCOS supervisor."""

from __future__ import annotations

import argparse
import base64
import ctypes
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Hermes executes this file from its scripts directory. Resolve project-local
# KCOS modules before importing them; keep the canonical project state as the
# only source of supervisor logic.
PROJECT_STATE = Path(r"C:\Users\Tyson\Projects\kiyosaki_stage2b\state")
LOCAL_STATE = Path(__file__).resolve().parent
for module_path in (LOCAL_STATE, PROJECT_STATE):
    try:
        sys.path.remove(str(module_path))
    except ValueError:
        pass
    sys.path.insert(0, str(module_path))

import kcos_rotation_v2 as rotation
import kcos_goal_cycle_cap_v1 as cycle_cap
import kcos_stage2_source_defect_recovery_v1 as stage2_source_recovery
import kcos_stage2_repair_loop_v1 as stage2_repair_loop
import kcos_stage1_publish_v2 as stage1_publisher
from kcos_tester_config_v2 import config_from_spec
import kcos_stage2_tester_diagnostic_v1 as stage2_tester
from kcos_stage2_verify_v2 import (dcc_closed_return_arrays_are_bounded,
                                   zero_disables_cooldown_controls,
                                   zero_disables_time_stop_controls,
                                   generated_source_review_is_current,
                                   STAGE2_RECOVERY_REQUIRED_CHECK_KEYS,
                                   STAGE2_PIPELINE_CODE_FILES)
from kcos_runtime_paths_v2 import assert_safe_root, hermes_db
from kcos_run_authorization_v2 import authorizes_stages_1_to_3, authorizes_stages_1_to_5


ROOT = assert_safe_root()
STATE = ROOT / "state" / "kcos_cycle_state_v2.json"
CONTROL = ROOT / "state" / "kcos_control_v2.json"
HEARTBEAT = ROOT / "state" / "kcos_heartbeat_v2.json"
DB = hermes_db()
STAGE3_DRIVER = ROOT / "state" / "kcos_stage3_driver_v2.py"
HERMES_JOBS = Path(r"C:\\Users\\Tyson\\AppData\\Local\\hermes\\cron\\jobs.json")
HERMES_SCRIPTS = HERMES_JOBS.parent.parent / "scripts"
GOAL_CYCLE_CAP = PROJECT_STATE / "kcos_goal_cycle_limit_v1.json"


def campaign_cycle_cap_path(control: dict, expected_path: Path = GOAL_CYCLE_CAP) -> Path | None:
    """Return the campaign cap only when canonical control explicitly enables it."""
    if control.get("campaign_cycle_cap_enabled") is not True:
        return None
    configured = control.get("campaign_cycle_cap_path")
    if not isinstance(configured, str) or not configured:
        raise ValueError("campaign cycle cap is enabled without its canonical ledger path")
    path = Path(configured)
    if path.resolve() != expected_path.resolve():
        raise ValueError("campaign cycle cap path does not match the canonical ledger")
    return path


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_object(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain an object")
    return value


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def consumption_insensitive_sha256(path: Path) -> str:
    value = load_object(path)
    for key in ("consumed", "consumed_utc", "consumed_action"):
        value.pop(key, None)
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest().upper()


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


def execution_record(db_path: Path, execution_id: str, job_id: str) -> tuple[str, str | None, str | None] | None:
    connection = sqlite3.connect(str(db_path))
    try:
        row = connection.execute(
            "SELECT status, started_at, finished_at FROM executions WHERE id = ? AND job_id = ?", (execution_id, job_id)
        ).fetchone()
    finally:
        connection.close()
    return None if row is None else (str(row[0]), row[1], row[2])


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def completed_execution_for_event(db_path: Path, job_id: str, completed_utc: str) -> tuple[str, str, str] | None:
    """Return the completed Hermes execution whose lifetime contains an event."""
    event_time = parse_utc(completed_utc)
    connection = sqlite3.connect(str(db_path))
    try:
        rows = connection.execute(
            """SELECT id, started_at, finished_at FROM executions
               WHERE job_id = ? AND status = 'completed'
                 AND started_at IS NOT NULL AND finished_at IS NOT NULL
               ORDER BY started_at DESC""",
            (job_id,),
        ).fetchall()
    finally:
        connection.close()
    for execution_id, started_at, finished_at in rows:
        started = parse_utc(started_at)
        finished = parse_utc(finished_at)
        # Hermes stores execution timestamps with sub-second precision while
        # stage events are intentionally UTC-second timestamps. A valid event
        # can therefore round down just before execution start by <1 second,
        # or land immediately after process exit during atomic readback. Keep
        # both sides within two seconds so a prior run cannot attest a later
        # event merely because it happened within a broad publication window.
        if started - timedelta(seconds=2) <= event_time <= finished + timedelta(seconds=2):
            return str(execution_id), str(started_at), str(finished_at)
    return None


def failed_execution_for_event(db_path: Path, job_id: str, failed_utc: str) -> tuple[str, str, str] | None:
    """Bind a failure event to the failed Hermes execution that enclosed it."""
    event_time = parse_utc(failed_utc)
    connection = sqlite3.connect(str(db_path))
    try:
        rows = connection.execute(
            """SELECT id, started_at, finished_at FROM executions
               WHERE job_id = ? AND status = 'failed'
                 AND started_at IS NOT NULL AND finished_at IS NOT NULL
               ORDER BY started_at DESC""", (job_id,)).fetchall()
    finally:
        connection.close()
    for execution_id, started_at, finished_at in rows:
        started, finished = parse_utc(started_at), parse_utc(finished_at)
        if started - timedelta(seconds=2) <= event_time <= finished + timedelta(seconds=2):
            return str(execution_id), str(started_at), str(finished_at)
    return None


def stage2_execution_attestation_path(event_path: Path, cycle_id: str, event_hash: str) -> Path:
    state_root = event_path.parent.parent if event_path.parent.name.casefold() == "events" else event_path.parent
    safe_cycle = "".join(char if char.isalnum() or char in "-_." else "_" for char in str(cycle_id))
    return state_root / "attestations" / f"{safe_cycle}.stage2.execution.{event_hash}.json"


_DPAPI_ENTROPY = b"KCOS.Stage2.ExecutionAttestation.v1"
_DPAPI_DESCRIPTION = "KCOS Stage 2 execution provenance"


def _dpapi_transform(payload: bytes, *, protect: bool) -> bytes:
    """Protect/unprotect bytes with Windows DPAPI for the current user profile."""
    if os.name != "nt":
        raise RuntimeError("Windows DPAPI is required for authenticated Stage 2 attestations")
    from ctypes import wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]

    def make_blob(data: bytes):
        storage = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
        blob = DATA_BLOB(len(data), ctypes.cast(storage, ctypes.POINTER(ctypes.c_byte)))
        return blob, storage

    data_blob, data_storage = make_blob(payload)
    entropy_blob, entropy_storage = make_blob(_DPAPI_ENTROPY)
    output = DATA_BLOB()
    crypt32 = ctypes.WinDLL("Crypt32.dll", use_last_error=True)
    kernel32 = ctypes.WinDLL("Kernel32.dll", use_last_error=True)
    flags = 0x1  # CRYPTPROTECT_UI_FORBIDDEN
    if protect:
        api = crypt32.CryptProtectData
        api.argtypes = [ctypes.POINTER(DATA_BLOB), wintypes.LPCWSTR,
                        ctypes.POINTER(DATA_BLOB), ctypes.c_void_p, ctypes.c_void_p,
                        wintypes.DWORD, ctypes.POINTER(DATA_BLOB)]
        api.restype = wintypes.BOOL
        ok = api(ctypes.byref(data_blob), _DPAPI_DESCRIPTION, ctypes.byref(entropy_blob),
                 None, None, flags, ctypes.byref(output))
    else:
        api = crypt32.CryptUnprotectData
        api.argtypes = [ctypes.POINTER(DATA_BLOB), ctypes.c_void_p,
                        ctypes.POINTER(DATA_BLOB), ctypes.c_void_p, ctypes.c_void_p,
                        wintypes.DWORD, ctypes.POINTER(DATA_BLOB)]
        api.restype = wintypes.BOOL
        ok = api(ctypes.byref(data_blob), None, ctypes.byref(entropy_blob),
                 None, None, flags, ctypes.byref(output))
    # Keep input and entropy storage alive through the native call.
    _ = data_storage, entropy_storage
    if not ok:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        return ctypes.string_at(output.pbData, output.cbData)
    finally:
        kernel32.LocalFree.argtypes = [ctypes.c_void_p]
        kernel32.LocalFree.restype = ctypes.c_void_p
        kernel32.LocalFree(ctypes.cast(output.pbData, ctypes.c_void_p))


def protect_attestation_payload(payload: bytes) -> str:
    protected = _dpapi_transform(payload, protect=True)
    return base64.b64encode(protected).decode("ascii")


def unprotect_attestation_payload(encoded: str) -> bytes:
    protected = base64.b64decode(encoded, validate=True)
    return _dpapi_transform(protected, protect=False)


def validate_stage2_execution_attestation(
    path: Path, state: dict, stage2_job: str, event_path: Path,
    verification_path: Path, event: dict, db_path: Path,
    expected_event_sha256: str | None = None,
) -> tuple[bool, str]:
    if not path.is_file():
        return False, "authenticated execution attestation is missing"
    try:
        envelope = load_object(path)
        if (envelope.get("schema_version") != 1 or
                envelope.get("record_kind") != "kcos_stage2_execution_attestation_envelope" or
                envelope.get("protection") != "windows_dpapi_user"):
            return False, "attestation lacks Windows DPAPI authentication; plaintext/self-checksummed records are untrusted"
        plaintext = unprotect_attestation_payload(str(envelope.get("protected_payload_b64", "")))
        attestation = json.loads(plaintext.decode("utf-8"))
        if not isinstance(attestation, dict):
            return False, "DPAPI-authenticated attestation payload is not an object"
        expected = {
            "schema_version": 1,
            "record_kind": "kcos_stage2_execution_attestation",
            "stage": 2,
            "cycle_id": state.get("cycle_id"),
            "stage2_job_id": str(stage2_job),
            "event_path": str(event_path.resolve()),
            "event_sha256": expected_event_sha256 or sha256(event_path),
            "event_completed_utc": event.get("completed_utc"),
            "verification_path": str(verification_path.resolve()),
            "verification_sha256": sha256(verification_path),
            "execution_status": "completed",
        }
        if any(attestation.get(key) != value for key, value in expected.items()):
            return False, "DPAPI-authenticated attestation identity/hash mismatch"
        if not attestation.get("execution_id") or not attestation.get("execution_started_at") or not attestation.get("execution_finished_at"):
            return False, "DPAPI-authenticated attestation lacks execution identity/timestamps"
        event_time = parse_utc(str(event["completed_utc"]))
        started = parse_utc(str(attestation["execution_started_at"]))
        finished = parse_utc(str(attestation["execution_finished_at"]))
        if not started - timedelta(seconds=2) <= event_time <= finished + timedelta(minutes=10):
            return False, "DPAPI-authenticated attestation does not enclose the Stage 2 event"
        current_record = None
        if db_path is not None:
            current_record = execution_record(db_path, str(attestation["execution_id"]), str(stage2_job))
            if current_record is None:
                return False, "DPAPI attestation execution is absent from the Hermes DB"
            if current_record != (
                "completed", attestation["execution_started_at"], attestation["execution_finished_at"]
            ):
                return False, "DPAPI attestation execution identity/timestamps disagree with the current Hermes DB row"
            if current_record is None:
                current_event_record = completed_execution_for_event(
                    db_path, str(stage2_job), str(event["completed_utc"])
                )
                if current_event_record is not None and current_event_record[0] != str(attestation["execution_id"]):
                    return False, "DPAPI attestation execution_id disagrees with the current matching Hermes event row"
    except Exception as exc:
        return False, f"DPAPI attestation authentication/decoding failed: {exc}"
    if current_record is None:
        return True, f"DPAPI-authenticated archived execution_id={attestation['execution_id']}"
    return True, f"current Hermes DB row and DPAPI envelope verified; execution_id={attestation['execution_id']}"


def quarantine_stage2_execution_attestation(path: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    archived = path.with_name(f"{path.stem}.untrusted.{stamp}{path.suffix}")
    os.replace(path, archived)
    return archived


def persist_stage2_execution_attestation(
    state: dict, stage2_job: str, event_path: Path, verification_path: Path,
    event: dict, record: tuple[str, str, str], db_path: Path,
) -> tuple[bool, str]:
    event_hash = sha256(event_path)
    path = stage2_execution_attestation_path(event_path, str(state.get("cycle_id")), event_hash)
    if path.exists():
        return validate_stage2_execution_attestation(path, state, stage2_job, event_path, verification_path, event, db_path)
    attestation = {
        "schema_version": 1,
        "record_kind": "kcos_stage2_execution_attestation",
        "stage": 2,
        "cycle_id": state.get("cycle_id"),
        "stage2_job_id": str(stage2_job),
        "event_path": str(event_path.resolve()),
        "event_sha256": event_hash,
        "event_completed_utc": event.get("completed_utc"),
        "verification_path": str(verification_path.resolve()),
        "verification_sha256": sha256(verification_path),
        "execution_id": record[0],
        "execution_status": "completed",
        "execution_started_at": record[1],
        "execution_finished_at": record[2],
        "attested_utc": utc_now(),
        "attestation_source": "completed_hermes_execution_database_record",
    }
    envelope = {
        "schema_version": 1,
        "record_kind": "kcos_stage2_execution_attestation_envelope",
        "protection": "windows_dpapi_user",
        "protected_payload_b64": protect_attestation_payload(
            json.dumps(attestation, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ),
    }
    atomic_json(path, envelope)
    return validate_stage2_execution_attestation(path, state, stage2_job, event_path, verification_path, event, db_path)


def validate_stage1_completion(state: dict, control: dict, db_path: Path) -> tuple[bool, str]:
    event_path = Path(state.get("last_stage1_event") or "")
    artifact_path = Path(state.get("last_stage1_artifact") or "")
    if not event_path.is_file() or not artifact_path.is_file():
        return False, "same-cycle Stage 1 event or artifact is missing"
    event = load_object(event_path)
    if event.get("schema_version") != 2 or event.get("stage") != 1:
        return False, "Stage 1 event schema is invalid"
    if event.get("cycle_id") != state.get("cycle_id"):
        return False, "Stage 1 event belongs to a different cycle"
    if Path(event.get("artifact", "")).resolve() != artifact_path.resolve():
        return False, "Stage 1 event points to a different artifact"
    if event.get("artifact_sha256") != sha256(artifact_path):
        return False, "Stage 1 artifact hash does not match its event"
    stage1_job = control.get("stage_jobs", {}).get("1")
    if not stage1_job or not event.get("completed_utc"):
        return False, "completion timestamps are absent"
    record = completed_execution_for_event(db_path, stage1_job, event["completed_utc"])
    if record is None:
        if state.get("stage1_manual_recovery") == "deterministic_publisher_v2" and state.get("stage1_manual_recovery_artifact_sha256") == sha256(artifact_path):
            return True, "manual deterministic Stage 1 recovery and same-cycle artifact verified"
        return False, "Stage 1 event is not enclosed by a completed Hermes execution"
    return True, f"Hermes completion and same-cycle artifact verified; execution_id={record[0]}"


def validate_stage2_completion(state: dict, control: dict, db_path: Path) -> tuple[bool, str]:
    event_path = Path(state.get("last_stage2_event", ""))
    verification_path = Path(state.get("last_stage2_verification", ""))
    if not event_path.is_file() or not verification_path.is_file():
        return False, "same-cycle Stage 2 event or verification is missing"
    event = load_object(event_path)
    verification = load_object(verification_path)
    if event.get("schema_version") != 2 or event.get("stage") != 2 or event.get("status") != "complete":
        return False, "Stage 2 event schema/status is invalid"
    if event.get("cycle_id") != state.get("cycle_id") or verification.get("cycle_id") != state.get("cycle_id"):
        return False, "Stage 2 evidence belongs to a different cycle"
    if Path(event.get("verification", "")).resolve() != verification_path.resolve():
        return False, "Stage 2 event points to a different verification"
    if event.get("verification_sha256") != sha256(verification_path) or verification.get("passed") is not True:
        return False, "Stage 2 verification hash/status is invalid"
    checks = verification.get("checks")
    if not isinstance(checks, dict) or not checks or not all(value is True for value in checks.values()):
        return False, "Stage 2 verification contains a failed or incomplete contract check"
    stage1_spec = Path(event.get("stage1_spec", ""))
    stage1_hash = event.get("stage1_spec_sha256")
    if not stage1_spec.is_file() or sha256(stage1_spec) != stage1_hash:
        return False, "Stage 1 specification changed after Stage 2 completion"
    if verification.get("stage1_spec_sha256") != stage1_hash:
        return False, "Stage 2 verification/event lineage mismatch"
    for path_key, hash_key in (
        ("source", "mq5_sha256"),
        ("ex5", "ex5_sha256"),
        ("canonical_minato", "canonical_minato_sha256"),
    ):
        artifact = Path(verification.get(path_key, ""))
        if not artifact.is_file() or sha256(artifact) != event.get(hash_key):
            return False, f"current Stage 2 {path_key} artifact does not match its event"
    stage2_job = control.get("stage_jobs", {}).get("2")
    if not stage2_job or not event.get("completed_utc"):
        return False, "Stage 2 completion timestamps are absent"
    event_hash = sha256(event_path)
    attestation_path = stage2_execution_attestation_path(event_path, str(state.get("cycle_id")), event_hash)
    existing_attestation_detail = None
    if attestation_path.is_file():
        valid, detail = validate_stage2_execution_attestation(
            attestation_path, state, str(stage2_job), event_path, verification_path, event, db_path
        )
        if valid:
            return True, f"DPAPI-authenticated execution attestation verified; {detail}"
        existing_attestation_detail = detail

    # A legacy or corrupt sidecar is never trusted by itself. Replace it only
    # when the current Hermes DB still contains the exact matching run.
    record = completed_execution_for_event(db_path, stage2_job, event["completed_utc"])
    if record is None:
        if existing_attestation_detail:
            return False, f"{existing_attestation_detail}; no matching DB row is available to reauthenticate it"
        return False, "Stage 2 event is not enclosed by a completed Hermes execution"
    if existing_attestation_detail:
        try:
            quarantine_stage2_execution_attestation(attestation_path)
        except Exception as exc:
            return False, f"untrusted Stage 2 attestation could not be preserved before reauthentication: {exc}"
    valid, detail = persist_stage2_execution_attestation(
        state, str(stage2_job), event_path, verification_path, event, record, db_path
    )
    if not valid:
        return False, f"Hermes execution matched but its DPAPI-authenticated attestation failed validation: {detail}"
    prefix = "legacy/corrupt attestation quarantined; " if existing_attestation_detail else ""
    # persist_stage2_execution_attestation validates the DPAPI envelope and
    # the exact current Hermes row before returning success. Keep the success
    # wording aligned with the already-existing-attestation path because the
    # Stage 3 release gate requires explicit verification, not merely "saved".
    return True, f"{prefix}Hermes Stage 2 completion and same-cycle artifacts verified; DPAPI-authenticated execution attestation verified and saved; {detail}"


def validate_stage3_completion(state: dict, *, allow_consumed_stage2_event: bool = False) -> tuple[bool, str, dict | None]:
    event_path = Path(state.get("last_stage3_event", ""))
    decision_path = Path(state.get("last_stage3_decision", ""))
    verification_path = Path(state.get("last_stage3_verification", ""))
    if not event_path.is_file() or not decision_path.is_file() or not verification_path.is_file():
        return False, "same-cycle Stage 3 event, decision, or verification is missing", None
    event, decision, verification = load_object(event_path), load_object(decision_path), load_object(verification_path)
    failure_hold = (state.get("stage_status") == "stage3_failed_held" and event.get("failure_kind") == "STAGE3_ZERO_TRADE_TECHNICAL_FAILURE" and event.get("rotation_eligible") is True)
    if (event.get("schema_version") != 2 or event.get("stage") != 3 or (event.get("status") != "complete" and not (event.get("status") == "failed" and failure_hold))):
        return False, "Stage 3 event schema/status is invalid", None
    if event.get("cycle_id") != state.get("cycle_id") or decision.get("cycle_id") != state.get("cycle_id"):
        return False, "Stage 3 evidence belongs to a different cycle", None
    if event.get("decision_sha256") != sha256(decision_path) or event.get("verification_sha256") != sha256(verification_path):
        return False, "Stage 3 decision/verification hash mismatch", None
    if verification.get("passed") is not True or verification.get("decision_sha256") != sha256(decision_path):
        return False, "Stage 3 deterministic verification did not pass", None
    stage1_spec = Path(event.get("stage1_spec", ""))
    if not stage1_spec.is_file() or sha256(stage1_spec) != event.get("stage1_spec_sha256"):
        return False, "Stage 1 specification changed after Stage 3 verification", None
    stage2_event = Path(event.get("stage2_event", ""))
    if not stage2_event.is_file():
        return False, "Stage 2 event is missing after Stage 3 verification", None
    recorded_stage2_sha = event.get("stage2_event_sha256")
    if (sha256(stage2_event) != recorded_stage2_sha
            and consumption_insensitive_sha256(stage2_event) != recorded_stage2_sha):
        current_stage2 = load_object(stage2_event)
        if not (allow_consumed_stage2_event and current_stage2.get("consumed") is True
                and current_stage2.get("consumed_action") == "start_stage3"):
            return False, "Stage 2 event changed after Stage 3 verification", None
    note_hashes = event.get("note_hashes", {})
    if not note_hashes:
        return False, "Stage 3 durable note evidence is absent", None
    for stored_path, expected_hash in note_hashes.items():
        path = Path(stored_path)
        if not path.is_file() or sha256(path) != expected_hash:
            return False, f"Stage 3 note changed or is missing: {path}", None
    invalidation_dir = event_path.parent.parent / "invalidations"
    for invalidation_path in sorted(invalidation_dir.glob("*.json")):
        invalidation = load_object(invalidation_path)
        target = Path(invalidation.get("target_event", ""))
        if (invalidation.get("schema_version") == 1 and
                invalidation.get("record_kind") == "kcos_event_invalidation" and
                target.resolve() == event_path.resolve() and
                invalidation.get("target_event_sha256") == sha256(event_path) and
                invalidation.get("effect") == "exclude_as_current_terminal_proof"):
            event = dict(event)
            event["_policy_invalidated"] = True
            event["_required_action"] = invalidation.get("required_action")
            return True, "Stage 3 evidence is intact but its terminal verdict is superseded; revalidation is required", event
    return True, "same-cycle Stage 3 decision, verification, and durable notes verified", event


def stage3_infrastructure_failure_allows_rotation(failure: dict) -> bool:
    """Generic Stage 3 execution errors stay held; only explicit outcomes rotate."""
    if failure.get("status") == "COMPUTE_BUDGET_EXCEEDED":
        return True
    return (failure.get("failure_kind") == "STAGE3_EXECUTION_FAILURE"
            and failure.get("rotation_eligible") is True)


def validate_stage3_input_contract_failure(state: dict) -> tuple[bool, str, dict]:
    """Authenticate a prelaunch Stage 3 input rejection before recovery."""
    failure_path = Path(state.get("stage3_failure_event") or "")
    if not failure_path.is_file():
        return False, "Stage 3 input-contract failure event is missing", {}
    failure = load_object(failure_path)
    findings = failure.get("findings") or []
    finding_code = findings[0].get("code") if findings and isinstance(findings[0], dict) else None
    grid_repair_hold = (finding_code == "INPUT_GRID_INVALID"
                        and failure.get("rotation_eligible") is False)
    if (failure.get("schema_version") != 2
            or failure.get("cycle_id") != state.get("cycle_id")
            or failure.get("stage") != 3
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != "STAGE3_INPUT_CONTRACT_FAILURE"
            or (failure.get("rotation_eligible") is not True and not grid_repair_hold)
            or failure.get("ea") != state.get("current_ea")):
        return False, "Stage 3 input-contract failure identity/status invalid", failure
    evidence = failure.get("evidence")
    if not isinstance(evidence, dict):
        return False, "Stage 3 input-contract failure evidence is missing", failure
    for key, expected_path in (("stage1_spec", state.get("last_stage1_artifact")),
                               ("stage2_event", state.get("last_stage2_event")),
                               ("stage2_handoff", state.get("last_stage2_handoff"))):
        item = evidence.get(key)
        if not isinstance(item, dict) or not expected_path:
            return False, f"Stage 3 input-contract {key} evidence is missing", failure
        path = Path(item.get("path", ""))
        if path.resolve() != Path(expected_path).resolve() or not path.is_file():
            return False, f"Stage 3 input-contract {key} evidence hash/path mismatch", failure
        evidence_sha256 = sha256(path)
        if key == "stage2_event" and item.get("sha256") != evidence_sha256:
            consumed_event = load_object(path)
            if (consumed_event.get("schema_version") == 2
                    and consumed_event.get("event_id") in
                        (None, f"{state.get('cycle_id')}:stage2:complete")
                    and consumed_event.get("cycle_id") == state.get("cycle_id")
                    and consumed_event.get("stage") == 2
                    and consumed_event.get("status") == "complete"
                    and consumed_event.get("consumed") is True
                    and consumed_event.get("consumed_action") == "start_stage3"
                    and consumed_event.get("consumed_utc")):
                unconsumed_event = dict(consumed_event)
                for field in ("consumed", "consumed_utc", "consumed_action"):
                    unconsumed_event.pop(field, None)
                serialized = json.dumps(unconsumed_event, indent=2) + "\n"
                serialized = serialized.replace("\n", os.linesep)
                reconstructed_sha256 = hashlib.sha256(serialized.encode("utf-8")).hexdigest().upper()
                if item.get("sha256") == reconstructed_sha256:
                    continue
            return False, f"Stage 3 input-contract {key} evidence hash/path mismatch", failure
        if item.get("sha256") != evidence_sha256:
            return False, f"Stage 3 input-contract {key} evidence hash/path mismatch", failure
    findings = failure.get("findings")
    if not isinstance(findings, list) or not findings or any(
            not isinstance(item, dict) or not str(item.get("code", "")).strip()
            for item in findings):
        return False, "Stage 3 input-contract failure has no typed findings", failure
    return True, "authenticated Stage 3 input-contract failure", failure


def validate_stage3_timeframe_completion(state: dict) -> tuple[bool, str, dict | None, Path | None]:
    handoff_path = Path(state.get("last_stage3_handoff", ""))
    if not handoff_path.is_file():
        return False, "Stage 3 handoff is missing", None, None
    handoff = load_object(handoff_path)
    if handoff.get("schema_version") != 2 or handoff.get("stage") != 3:
        return False, "Stage 3 handoff schema is invalid", None, None
    if handoff.get("cycle_id") != state.get("cycle_id") or handoff.get("ea") != state.get("current_ea"):
        return False, "Stage 3 handoff identity mismatch", None, None
    runtime_path = Path(str(handoff.get("runtime_manifest", "")).replace("\\\\", "\\"))
    if not runtime_path.is_file():
        return False, "Stage 3 runtime manifest is missing", None, None
    runtime = load_object(runtime_path)
    runtime_status = runtime.get("status")
    if runtime_status == "INFRASTRUCTURE_FAILURE":
        return False, "Stage 3 watcher reported infrastructure failure", None, None
    if runtime_status not in {"TIMEFRAME_COMPLETE", "COMPUTE_BUDGET_EXCEEDED"}:
        return False, "Stage 3 timeframe is still optimizing", None, None
    event_path = Path(runtime.get("timeframe_event", ""))
    if not event_path.is_file():
        return False, "Stage 3 timeframe event is missing", None, None
    event = load_object(event_path)
    suffix = "complete" if runtime_status == "TIMEFRAME_COMPLETE" else "compute_budget_exceeded"
    expected_id = f"{state.get('cycle_id')}:stage3:{runtime.get('timeframe')}:{suffix}"
    if event.get("event_id") != expected_id or event.get("cycle_id") != state.get("cycle_id"):
        return False, "Stage 3 timeframe event identity mismatch", None, None
    if event.get("status") != runtime_status or event.get("timeframe") != runtime.get("timeframe"):
        return False, "Stage 3 timeframe event status mismatch", None, None
    if not isinstance(event.get("consumed"), bool):
        return False, "Stage 3 timeframe event consumed flag is invalid", None, None
    if runtime_status == "TIMEFRAME_COMPLETE":
        opt_path = Path(runtime.get("opt_path", ""))
        if not opt_path.is_file():
            return False, "Stage 3 timeframe OPT artifact is missing", None, None
        if event.get("opt_sha256") != sha256(opt_path):
            return False, "Stage 3 timeframe OPT hash mismatch", None, None
    if event.get("consumed") is True:
        return True, "Stage 3 timeframe event is already consumed", event, event_path
    if runtime_status == "COMPUTE_BUDGET_EXCEEDED":
        return True, "Stage 3 compute-budget event verified", event, event_path
    return True, "Stage 3 timeframe completion and OPT hash verified", event, event_path


def _validate_verified_transition(state: dict, stage: int | str, decision_key: str,
                                  verification_key: str, event_key: str,
                                  accepted_verdicts: set[str] | None = None) -> tuple[bool, str, dict | None]:
    """Validate one post-Stage-3 transition without trusting status alone.

    Every downstream dispatch is bound to a same-cycle event, decision, and
    verification.  This deliberately does not consult cron history: the
    producer's event is the authority and the consumer's claim file provides
    exactly-once dispatch.
    """
    paths = {key: Path(state.get(key, "")) for key in (decision_key, verification_key, event_key)}
    if not all(path.is_file() for path in paths.values()):
        return False, f"same-cycle Stage {stage} decision, verification, or event is missing", None
    decision, verification, event = (load_object(paths[key]) for key in (decision_key, verification_key, event_key))
    if decision.get("cycle_id") != state.get("cycle_id") or verification.get("cycle_id") != state.get("cycle_id") or event.get("cycle_id") != state.get("cycle_id"):
        return False, f"Stage {stage} evidence belongs to a different cycle", None
    if str(decision.get("stage")) != str(stage) or str(verification.get("stage")) != str(stage) or str(event.get("stage")) != str(stage):
        return False, f"Stage {stage} evidence has an invalid stage identity", None
    if verification.get("passed") is not True:
        return False, f"Stage {stage} verification did not pass", None
    if str(stage) == "4.5":
        if decision.get("ea") != state.get("current_ea") or event.get("ea") != state.get("current_ea"):
            return False, "Stage 4.5 evidence has an EA identity mismatch", None
        for field in ("decision_sha256", "verification_sha256"):
            if not event.get(field):
                return False, f"Stage 4.5 event is missing mandatory {field}", None
    if str(stage) == "5":
        result_path = Path(decision.get("result") or "")
        if not result_path.is_file():
            return False, "Stage 5 result artifact is missing", None
        try:
            result = load_object(result_path)
        except (OSError, ValueError, TypeError):
            return False, "Stage 5 result artifact is invalid", None
        expected_result_verdict = {
            "STAGE5_FORWARD_FAIL": "COMPLETE",
            "STAGE5_FORWARD_PASS": "record_pass",
            "HOLD_FOR_REVIEW": "HOLD_FOR_REVIEW",  # historical Stage 5 result compatibility
            "COMPLETE": "COMPLETE",
        }.get(event.get("verdict"))
        if (decision.get("schema_version") != 1 or verification.get("schema_version") != 1
                or event.get("schema_version") != 1
                or decision.get("ea") != state.get("current_ea")
                or event.get("ea") != state.get("current_ea")
                or decision.get("verdict") != event.get("verdict")
                or (accepted_verdicts and decision.get("verdict") not in accepted_verdicts)
                or event.get("verification_passed") is not True
                or not event.get("decision_sha256") or not event.get("verification_sha256")
                or result.get("cycle_id") != state.get("cycle_id")
                or result.get("ea") != state.get("current_ea")
                or str(result.get("stage")) != "5"
                or result.get("verdict") != expected_result_verdict):
            return False, "Stage 5 event, decision, verification, result, or EA binding is invalid", None
        result_hash = sha256(result_path)
        if (decision.get("result_sha256") != result_hash
                or verification.get("result_sha256") != result_hash):
            return False, "Stage 5 result hash mismatch", None
    if event.get("verification_passed") is False:
        return False, f"Stage {stage} event reports failed verification", None
    decision_hash = sha256(paths[decision_key])
    verification_hash = sha256(paths[verification_key])
    if event.get("decision_sha256") and event.get("decision_sha256") != decision_hash:
        return False, f"Stage {stage} decision hash mismatch", None
    if event.get("verification_sha256") and event.get("verification_sha256") != verification_hash:
        return False, f"Stage {stage} verification hash mismatch", None
    if accepted_verdicts and event.get("verdict") not in accepted_verdicts and decision.get("verdict") not in accepted_verdicts:
        return False, f"Stage {stage} verdict is not dispatchable", None
    event_id = event.get("event_id") or f"{state.get('cycle_id')}:stage{stage}:complete"
    if not isinstance(event_id, str) or not event_id:
        return False, f"Stage {stage} event id is missing", None
    event = dict(event)
    event["event_id"] = event_id
    return True, f"same-cycle Stage {stage} decision, verification, and event verified", event


def reconcile_stage4_dispatch_race(state_path: Path = STATE) -> bool:
    """Restore a verified Stage 4 commit if fast cron completion was overwritten by dispatch bookkeeping."""
    state = load_object(state_path)
    if not (state.get("current_stage") == "stage4_pending"
            and state.get("stage_status") == "stage4_pending"):
        return False
    cycle = str(state.get("cycle_id") or "")
    event_path = state_path.parent / "events" / f"{cycle}.stage4.complete.json"
    if not cycle or not event_path.is_file():
        return False
    event = load_object(event_path)
    decision_path = Path(str(event.get("decision") or ""))
    verification_path = Path(str(event.get("verification") or ""))
    probe = dict(state)
    probe.update({
        "last_stage4_decision": str(decision_path),
        "last_stage4_verification": str(verification_path),
        "last_stage4_event": str(event_path),
    })
    valid, _, verified_event = _validate_verified_transition(
        probe, 4, "last_stage4_decision", "last_stage4_verification",
        "last_stage4_event", {"STAGE4_PLANNED"})
    if not (valid and verified_event and event.get("status") == "complete"
            and event.get("verdict") == "STAGE4_PLANNED"
            and event.get("ea") == state.get("current_ea")):
        return False
    stage3_valid, _, stage3_event = validate_stage3_completion(state)
    if not (stage3_valid and stage3_event
            and stage3_event.get("consumed") is True
            and stage3_event.get("consumed_action") == "dispatch_stage4"):
        return False
    decision = load_object(decision_path)
    if (decision.get("stage3_decision_sha256") != sha256(Path(state["last_stage3_decision"]))
            or decision.get("stage3_verification_sha256") != sha256(Path(state["last_stage3_verification"]))):
        return False
    queue_path = state_path.parent / "stage4_pending_queue_v2.json"
    if not queue_path.is_file():
        return False
    queue = load_object(queue_path)
    entries = [item for item in queue.get("entries", [])
               if item.get("cycle_id") == cycle and item.get("ea") == state.get("current_ea")]
    if len(entries) != 1:
        return False
    entry = entries[0]
    if not (entry.get("status") == "stage4_pending_narrow_opt"
            and entry.get("stage4_decision_sha256") == sha256(decision_path)
            and entry.get("stage4_verification_sha256") == sha256(verification_path)):
        return False
    if not authorizes_stages_1_to_5(state):
        return False
    handoff_path = state_path.parent / "handoffs" / "Stage4_v2.json"
    if not handoff_path.is_file():
        return False
    handoff = load_object(handoff_path)
    if not (handoff.get("cycle_id") == cycle
            and handoff.get("decision_sha256") == sha256(decision_path)
            and handoff.get("verification_sha256") == sha256(verification_path)
            and handoff.get("event_sha256") == sha256(event_path)):
        return False

    diagnostics = state_path.parent / "diagnostics"
    diagnostics.mkdir(parents=True, exist_ok=True)
    archive = state_path.parent / "repair_archive"
    archive.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = archive / f"{cycle}.stage4_state_before_dispatch_race_reconcile.{stamp}.json"
    receipt_path = diagnostics / f"{cycle}.stage4_dispatch_race_recovery.json"
    if backup_path.exists() or receipt_path.exists():
        return False
    source_state_hash = sha256(state_path)
    shutil.copy2(state_path, backup_path)
    receipt = {
        "schema_version": 1,
        "status": "verified_stage4_dispatch_race_reconciled",
        "cycle_id": cycle,
        "source_state_sha256": source_state_hash,
        "source_state_backup": str(backup_path.resolve()),
        "stage3_event": str(Path(state["last_stage3_event"]).resolve()),
        "stage3_event_sha256": sha256(Path(state["last_stage3_event"])),
        "stage4_event": str(event_path.resolve()), "stage4_event_sha256": sha256(event_path),
        "stage4_decision": str(decision_path.resolve()), "stage4_decision_sha256": sha256(decision_path),
        "stage4_verification": str(verification_path.resolve()),
        "stage4_verification_sha256": sha256(verification_path),
        "stage4_queue": str(queue_path.resolve()), "stage4_queue_sha256": sha256(queue_path),
        "stage4_handoff": str(handoff_path.resolve()), "stage4_handoff_sha256": sha256(handoff_path),
        "run_authorization": state.get("run_authorization"),
        "reconciled_utc": utc_now(),
    }
    atomic_json(receipt_path, receipt)
    authorization = state.get("run_authorization", {})
    state.update({
        "current_stage": "stage4_complete_held",
        "stage_status": "stage4_complete_held",
        "next_action": "inspect_stage4_before_narrow_opt",
        "allowed_stages": [1, 2, 3, 4, 4.5, 5],
        "hold_after_stage": int(authorization.get("hold_after_stage", 5)),
        "stage4_decision": str(decision_path.resolve()),
        "stage4_verification": str(verification_path.resolve()),
        "stage4_event": str(event_path.resolve()),
        "stage4_handoff": str(handoff_path.resolve()),
        "last_stage4_decision": str(decision_path.resolve()),
        "last_stage4_verification": str(verification_path.resolve()),
        "last_stage4_event": str(event_path.resolve()),
        "last_stage4_handoff": str(handoff_path.resolve()),
        "verdict": "STAGE4_PLANNED", "passed": True, "event_type": "stage4_complete",
        "narrow_run_number": 1, "stage45_run_number": 0,
        "narrow_opt_decision": None, "narrow_opt_event": None,
        "last_stage45_decision": None, "last_stage45_verification": None,
        "last_stage45_event": None, "stage45_best_score": None,
        "updated_utc": utc_now(),
    })
    atomic_json(state_path, state)
    return True


def verify_stage45_three_runs(state: dict, state_dir: Path | None = None, *,
                              successful_terminal: bool = False,
                              allow_consumed: bool = False) -> tuple[bool, str, dict | None]:
    """Verify the complete three-run refinement sequence for failure or Stage 5.

    The final refinement must fail the unchanged Stage 3 gates. Every run must
    still have complete, untimed-out MT5 evidence, matching CSV/decision/event
    hashes, and passing structural/range checks. Earlier qualifying rows are
    retained as evidence but do not override a failed final refinement.
    """
    state_dir = state_dir or STATE.parent
    cycle, ea = state.get("cycle_id"), state.get("current_ea")
    permitted_statuses = {"narrow_opt_failed"} if not successful_terminal else {
        "stage5_complete_held", "stage5_failed_held"}
    if state.get("stage_status") not in permitted_statuses:
        return False, "Stage 4.5 is not in the required terminal state", None
    if int(state.get("stage45_run_number", 0)) != 3 or int(state.get("narrow_run_number", 0)) != 3:
        return False, "Stage 4.5 has not exhausted exactly three refinement runs", None
    final_decision = final_verification = final_event = None
    timeframe = None
    previous_completed = None
    for run_number in (1, 2, 3):
        decision_path = state_dir / "decisions" / f"{cycle}.stage45.run{run_number}.decision.json"
        verification_path = state_dir / "verification" / f"{cycle}.stage45.run{run_number}.verification.json"
        event_path = state_dir / "events" / f"{cycle}.stage45.run{run_number}.complete.json"
        if not all(path.is_file() for path in (decision_path, verification_path, event_path)):
            return False, f"Stage 4.5 run {run_number} is missing its decision, verification, or event", None
        try:
            decision, verification, event = (load_object(path) for path in
                                              (decision_path, verification_path, event_path))
            runtime_path = state_dir / "stage45_v2" / f"{cycle}.stage45.run{run_number}.runtime.json"
            runtime = load_object(runtime_path)
            complete_path = state_dir / "stage45_v2" / f"{cycle}.stage45.{decision.get('selected_tf')}.run{run_number}.complete.json"
            complete = load_object(complete_path)
            csv_path = Path(decision.get("csv", ""))
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            return False, f"Stage 4.5 run {run_number} evidence is unreadable: {exc}", None
        tf = decision.get("selected_tf")
        if (decision.get("cycle_id") != cycle or decision.get("ea") != ea
                or decision.get("stage") != "4.5" or decision.get("run_number") != run_number
                or decision.get("run_count_required") != 3 or not tf
                or (timeframe is not None and tf != timeframe)):
            return False, f"Stage 4.5 run {run_number} decision identity/sequence mismatch", None
        timeframe = tf
        if (verification.get("cycle_id") != cycle or verification.get("stage") != "4.5"
                or event.get("cycle_id") != cycle or event.get("stage") != "4.5"
                or event.get("timeframe") != tf or event.get("ea") != ea
                or event.get("run_number") != run_number
                or event.get("verdict") != decision.get("verdict")):
            return False, f"Stage 4.5 run {run_number} evidence identity mismatch", None
        completed_raw = event.get("completed_utc")
        if completed_raw:
            try:
                completed_value = parse_utc(str(completed_raw))
            except Exception:
                return False, f"Stage 4.5 run {run_number} completion time is invalid", None
            if previous_completed is not None and completed_value <= previous_completed:
                return False, "Stage 4.5 run sequence is not strictly consecutive", None
            previous_completed = completed_value
        elif successful_terminal:
            return False, f"Stage 4.5 run {run_number} completion time is missing", None
        if (not csv_path.is_file() or Path(event.get("csv", "")).resolve() != csv_path.resolve()
                or sha256(csv_path) != decision.get("csv_sha256")
                or event.get("csv_sha256") != decision.get("csv_sha256")):
            return False, f"Stage 4.5 run {run_number} CSV hash/evidence mismatch", None
        if (event.get("decision") != str(decision_path.resolve())
                or event.get("verification") != str(verification_path.resolve())
                or event.get("decision_sha256") != sha256(decision_path)
                or event.get("verification_sha256") != sha256(verification_path)):
            return False, f"Stage 4.5 run {run_number} event hash/lineage mismatch", None
        opt_path = Path(runtime.get("opt_path", ""))
        narrow_ini_path = Path(runtime.get("narrow_ini", ""))
        if successful_terminal:
            runtime_invalid = (
                runtime.get("cycle_id") != cycle or runtime.get("ea") != ea
                or runtime.get("run_number") != run_number or runtime.get("run_count_required") != 3
                or runtime.get("timeframe") != tf or runtime.get("status") != "TIMEFRAME_COMPLETE"
                or runtime.get("timed_out") is True or runtime.get("terminal_closed_after_run") is not True
                or complete.get("cycle_id") != cycle or complete.get("timeframe") != tf
                or complete.get("status") != "TIMEFRAME_COMPLETE" or complete.get("timed_out") is not False
                or complete.get("narrow_ini_sha256") != runtime.get("narrow_ini_sha256")
                or not opt_path.is_file() or sha256(opt_path) != runtime.get("opt_sha256")
                or complete.get("opt_sha256") != runtime.get("opt_sha256")
                or not narrow_ini_path.is_file() or sha256(narrow_ini_path) != runtime.get("narrow_ini_sha256"))
        else:
            # Preserve the complete established rotation gate identity and artifact checks.
            runtime_invalid = (
                runtime.get("cycle_id") != cycle or runtime.get("ea") != ea
                or runtime.get("run_number") != run_number or runtime.get("run_count_required") != 3
                or runtime.get("timeframe") != tf or runtime.get("status") != "TIMEFRAME_COMPLETE"
                or runtime.get("timed_out") is True or runtime.get("terminal_closed_after_run") is not True
                or complete.get("cycle_id") != cycle or complete.get("timeframe") != tf
                or complete.get("status") != "TIMEFRAME_COMPLETE" or complete.get("timed_out") is not False
                or complete.get("narrow_ini_sha256") != runtime.get("narrow_ini_sha256")
                or not opt_path.is_file() or sha256(opt_path) != runtime.get("opt_sha256")
                or complete.get("opt_sha256") != runtime.get("opt_sha256")
                or not narrow_ini_path.is_file() or sha256(narrow_ini_path) != runtime.get("narrow_ini_sha256"))
        if runtime_invalid:
            return False, f"Stage 4.5 run {run_number} lacks complete, untimed-out, settled MT5 evidence", None
        checks = verification.get("checks")
        if not isinstance(checks, list):
            return False, f"Stage 4.5 run {run_number} verification checks are malformed", None
        structural = [check for check in checks
                      if check.get("name") not in {"verdict", "all Stage 3 gates applied"}]
        if not structural or any(check.get("passed") is not True for check in structural):
            return False, f"Stage 4.5 run {run_number} failed a structural or range-integrity check", None
        if decision.get("verdict") == "NARROW_OPT_COMPLETE":
            if (decision.get("qualifying_row_count", 0) <= 0
                    or verification.get("passed") is not True
                    or event.get("verification_passed") is not True):
                return False, f"Stage 4.5 run {run_number} claims success without passing gates", None
        elif (decision.get("verdict") != "NARROW_OPT_FAILED"
              or decision.get("qualifying_row_count") != 0
              or verification.get("passed") is not False
              or event.get("verification_passed") is not False):
            return False, f"Stage 4.5 run {run_number} has an unsupported terminal verdict", None
        if run_number == 3:
            final_decision, final_verification, final_event = decision, verification, event
    expected_final = "NARROW_OPT_COMPLETE" if successful_terminal else "NARROW_OPT_FAILED"
    if (final_decision is None or final_decision.get("verdict") != expected_final
            or (expected_final == "NARROW_OPT_FAILED" and final_decision.get("qualifying_row_count") != 0)
            or (expected_final == "NARROW_OPT_COMPLETE" and final_decision.get("qualifying_row_count", 0) <= 0)
            or (final_event.get("consumed") is True
                and not (allow_consumed and final_event.get("consumed_action") == "dispatch_stage5"))):
        return False, f"the final Stage 4.5 run did not produce an authenticated {expected_final} event", None
    event_id = final_event.get("event_id") or f"{cycle}:terminal"
    terminal_event = dict(final_event)
    terminal_event["event_id"] = event_id
    return True, f"three Stage 4.5 runs are recorded; final verdict is {expected_final}", terminal_event


def validate_stage45_terminal_failure(state: dict, state_dir: Path | None = None) -> tuple[bool, str, dict | None]:
    """Preserve rotation behavior for a verified exhausted Stage 4.5 search."""
    return verify_stage45_three_runs(state, state_dir)


def _event_consumed_as(path: Path, cycle_id: str, stage: int | str, action: str) -> tuple[bool, dict]:
    if not path.is_file():
        return False, {}
    event = load_object(path)
    return (event.get("cycle_id") == cycle_id and str(event.get("stage")) == str(stage)
            and event.get("consumed") is True and event.get("consumed_action") == action), event


def _receipt_evidence_item(evidence: dict, role: str, path: Path, *,
                          consumption_insensitive: bool | None = None) -> None:
    if not path.is_file():
        raise ValueError(f"healthy-run evidence is missing: {role}")
    if consumption_insensitive is None:
        consumption_insensitive = role.endswith("_event")
    digest = sha256(path)
    item = {"path": str(path.resolve()), "sha256": digest}
    if consumption_insensitive:
        value = load_object(path)
        for key in ("consumed", "consumed_utc", "consumed_action"):
            value.pop(key, None)
        item["sha256"] = hashlib.sha256(json.dumps(
            value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest().upper()
        item["hash_mode"] = "consumption_insensitive_json"
    evidence[role] = item


def _snapshot_mutable_receipt_evidence(evidence: dict, state_dir: Path, cycle: str) -> None:
    """Copy small shared handoffs/queue into a write-once per-cycle receipt folder."""
    snapshot_dir = state_dir / "goal_receipts" / cycle / "snapshots"
    for role in ("stage2_handoff", "stage3_handoff", "stage4_handoff", "stage4_queue"):
        item = evidence.get(role)
        if not isinstance(item, dict):
            raise ValueError(f"receipt snapshot source is missing: {role}")
        source = Path(item["path"])
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        target = snapshot_dir / f"{role}.json"
        raw_hash = sha256(source)
        if target.exists():
            if sha256(target) != raw_hash:
                raise ValueError(f"immutable receipt snapshot already exists with different bytes: {role}")
        else:
            with source.open("rb") as source_handle, target.open("xb") as handle:
                shutil.copyfileobj(source_handle, handle)
                handle.flush()
                os.fsync(handle.fileno())
        item["path"] = str(target.resolve())
        item["sha256"] = raw_hash
        item.pop("hash_mode", None)


def audit_healthy_stage5_cycle(state: dict, control: dict, db_path: Path,
                               cap_path: Path, state_dir: Path | None = None) -> tuple[bool, str, dict | None]:
    """Revalidate a complete Stage 1–5 chain before counting operational health."""
    state_dir = state_dir or STATE.parent
    cycle, ea = state.get("cycle_id"), state.get("current_ea")
    if not isinstance(cycle, str) or not cycle or not isinstance(ea, str) or not ea:
        return False, "cycle or EA identity is missing", None
    try:
        cap = cycle_cap.load_cap(cap_path)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return False, f"campaign receipt ledger is invalid: {exc}", None
    if cycle not in {entry["cycle_id"] for entry in cap["started_cycles"]}:
        return False, "Stage 5 cycle is not recorded as a campaign start", None
    evidence: dict[str, dict] = {}

    # Stage 1 publication and its consumed Stage 2 handoff.
    stage1_event_path = Path(state.get("last_stage1_event") or "")
    stage1_artifact = Path(state.get("last_stage1_artifact") or "")
    valid, detail = validate_stage1_completion(state, control, db_path)
    if not valid:
        return False, f"Stage 1: {detail}", None
    ok, stage1_event = _event_consumed_as(stage1_event_path, cycle, 1, "dispatch_stage2")
    if not ok or stage1_event.get("ea") != ea:
        return False, "Stage 1 event was not consumed into Stage 2 for this EA", None
    stage1_spec = load_object(stage1_artifact)
    if stage1_spec.get("cycle_id") != cycle or stage1_spec.get("ea") != ea:
        return False, "Stage 1 specification identity mismatch", None
    _receipt_evidence_item(evidence, "stage1_event", stage1_event_path)
    _receipt_evidence_item(evidence, "stage1_artifact", stage1_artifact)

    # Stage 2 deterministic verification, source/build/Minato hashes and handoff.
    stage2_event_path = Path(state.get("last_stage2_event") or "")
    stage2_verification_path = Path(state.get("last_stage2_verification") or "")
    stage2_event = load_object(stage2_event_path)
    stage2_verification = load_object(stage2_verification_path) if stage2_verification_path.is_file() else {}
    if (not stage2_event_path.is_file() or stage2_event.get("schema_version") != 2
            or stage2_event.get("stage") != 2 or stage2_event.get("status") != "complete"
            or stage2_event.get("cycle_id") != cycle or stage2_event.get("ea") != ea
            or stage2_verification.get("cycle_id") != cycle
            or Path(stage2_event.get("verification", "")).resolve() != stage2_verification_path.resolve()
            or stage2_event.get("verification_sha256") != sha256(stage2_verification_path)
            or stage2_verification.get("passed") is not True):
        return False, "Stage 2 event or deterministic verification identity/hash/status is invalid", None
    checks = stage2_verification.get("checks")
    if not isinstance(checks, dict) or not checks or not all(value is True for value in checks.values()):
        return False, "Stage 2 verification contains a failed or incomplete contract check", None
    spec_path = Path(stage2_event.get("stage1_spec", ""))
    if (not spec_path.is_file() or sha256(spec_path) != stage2_event.get("stage1_spec_sha256")
            or stage2_verification.get("stage1_spec_sha256") != stage2_event.get("stage1_spec_sha256")):
        return False, "Stage 2 Stage 1 specification hash link is invalid", None
    for path_key, hash_key in (("source", "mq5_sha256"), ("ex5", "ex5_sha256"),
                               ("canonical_minato", "canonical_minato_sha256")):
        artifact = Path(stage2_verification.get(path_key, ""))
        if not artifact.is_file() or sha256(artifact) != stage2_event.get(hash_key):
            return False, f"Stage 2 {path_key} artifact hash is invalid", None
    stage2_job = (control.get("stage_jobs") or {}).get("2")
    if not stage2_job or not stage2_event.get("completed_utc"):
        return False, "Stage 2 Hermes job identity or completion time is missing", None
    preconsume_stage2_hash = stage3_event_hash = None
    if stage2_event.get("ea") != ea:
        return False, "Stage 2 EA identity mismatch", None
    ok, _ = _event_consumed_as(stage2_event_path, cycle, 2, "start_stage3")
    if not ok:
        return False, "Stage 2 event was not consumed into Stage 3", None
    stage2_handoff_path = Path(state.get("last_stage2_handoff") or state_dir / "handoffs" / "Stage2_v2.json")
    if not stage2_handoff_path.is_file():
        return False, "Stage 2 handoff is missing", None
    stage2_handoff = load_object(stage2_handoff_path)
    if (stage2_handoff.get("cycle_id") != cycle or stage2_handoff.get("stage") != 2
            or stage2_handoff.get("ea") != ea
            or Path(stage2_handoff.get("expected_event", "")).resolve() != stage2_event_path.resolve()
            or Path(stage2_handoff.get("stage1_event", "")).resolve() != stage1_event_path.resolve()
            or Path(stage2_handoff.get("stage1_spec", "")).resolve() != stage1_artifact.resolve()
            or stage2_handoff.get("stage1_spec_sha256") != sha256(stage1_artifact)):
        return False, "Stage 2 handoff does not bind the current Stage 1/2 chain", None
    for source_key, hash_key in (("source", "mq5_sha256"), ("ex5", "ex5_sha256"),
                                 ("canonical_minato", "canonical_minato_sha256")):
        artifact = Path(stage2_verification.get(source_key, ""))
        if not artifact.is_file() or sha256(artifact) != stage2_event.get(hash_key):
            return False, f"Stage 2 {source_key} changed after verification", None
        if stage2_handoff.get(source_key) and Path(stage2_handoff[source_key]).resolve() != artifact.resolve():
            return False, f"Stage 2 handoff {source_key} path mismatch", None
        _receipt_evidence_item(evidence, f"stage2_{source_key}", artifact)
    for role, path in (("stage2_event", stage2_event_path),
                       ("stage2_verification", stage2_verification_path),
                       ("stage2_handoff", stage2_handoff_path)):
        _receipt_evidence_item(evidence, role, path)

    # Stage 3 full verifier and Stage 4 handoff/queue admission.
    valid, detail, stage3_event = validate_stage3_completion(state, allow_consumed_stage2_event=True)
    if not valid or stage3_event is None or stage3_event.get("_policy_invalidated"):
        return False, f"Stage 3: {detail}", None
    stage3_event_path = Path(state.get("last_stage3_event") or "")
    preconsume_stage2_hash = stage3_event.get("stage2_event_sha256")
    if (not isinstance(preconsume_stage2_hash, str) or len(preconsume_stage2_hash) != 64):
        return False, "Stage 3 event lacks the authenticated pre-consumption Stage 2 hash", None
    attestation_path = stage2_execution_attestation_path(
        stage2_event_path, cycle, preconsume_stage2_hash)
    attested, attestation_detail = validate_stage2_execution_attestation(
        attestation_path, state, str(stage2_job), stage2_event_path,
        stage2_verification_path, stage2_event, db_path,
        expected_event_sha256=preconsume_stage2_hash)
    if not attested:
        return False, f"Stage 2 DPAPI execution attestation failed: {attestation_detail}", None
    stage2_record = completed_execution_for_event(db_path, str(stage2_job), stage2_event["completed_utc"])
    if stage2_record is None:
        return False, "Stage 2 attested Hermes execution is no longer present", None
    stage3_decision_path = Path(state.get("last_stage3_decision") or "")
    stage3_verification_path = Path(state.get("last_stage3_verification") or "")
    stage3_handoff_path = Path(state.get("last_stage3_handoff") or state_dir / "handoffs" / "Stage3_v2.json")
    ok, _ = _event_consumed_as(stage3_event_path, cycle, 3, "dispatch_stage4")
    if not ok or stage3_event.get("ea") != ea:
        return False, "Stage 3 event was not consumed into Stage 4 for this EA", None
    stage3_handoff = load_object(stage3_handoff_path)
    if (stage3_handoff.get("cycle_id") != cycle or stage3_handoff.get("stage") != 3
            or stage3_handoff.get("ea") != ea
            or Path(stage3_handoff.get("stage2_event", "")).resolve() != stage2_event_path.resolve()
            or stage3_handoff.get("stage2_event_sha256") != preconsume_stage2_hash
            or Path(stage3_handoff.get("event", "")).resolve() != stage3_event_path.resolve()):
        return False, "Stage 3 handoff does not bind the verified Stage 2/3 chain", None
    stage4_event_path = Path(state.get("last_stage4_event") or "")
    stage4_decision_path = Path(state.get("last_stage4_decision") or state.get("stage4_decision") or "")
    stage4_verification_path = Path(state.get("last_stage4_verification") or state.get("stage4_verification") or "")
    stage4_probe = dict(state, last_stage4_event=str(stage4_event_path),
                        last_stage4_decision=str(stage4_decision_path),
                        last_stage4_verification=str(stage4_verification_path))
    valid, detail, stage4_event = _validate_verified_transition(
        stage4_probe, 4, "last_stage4_decision", "last_stage4_verification",
        "last_stage4_event", {"STAGE4_PLANNED"})
    if not valid or stage4_event is None or stage4_event.get("ea") != ea:
        return False, f"Stage 4: {detail}", None
    ok, _ = _event_consumed_as(stage4_event_path, cycle, 4, "dispatch_stage45")
    if not ok:
        return False, "Stage 4 event was not consumed into Stage 4.5", None
    stage4_handoff_path = Path(state.get("last_stage4_handoff") or state_dir / "handoffs" / "Stage4_v2.json")
    stage4_handoff = load_object(stage4_handoff_path)
    if (stage4_handoff.get("cycle_id") != cycle or stage4_handoff.get("stage") != 4
            or stage4_handoff.get("ea") != ea
            or Path(stage4_handoff.get("decision", "")).resolve() != stage4_decision_path.resolve()
            or stage4_handoff.get("decision_sha256") != sha256(stage4_decision_path)
            or Path(stage4_handoff.get("verification", "")).resolve() != stage4_verification_path.resolve()
            or stage4_handoff.get("verification_sha256") != sha256(stage4_verification_path)
            or Path(stage4_handoff.get("event", "")).resolve() != stage4_event_path.resolve()
            or not isinstance(stage4_handoff.get("event_sha256"), str)):
        return False, "Stage 4 handoff does not bind the Stage 4 evidence", None
    queue_path = Path(stage3_event.get("stage4_queue", ""))
    if not queue_path.is_file():
        return False, "Stage 4 pending queue referenced by Stage 3 is missing", None
    queue = load_object(queue_path)
    queue_rows = [row for row in queue.get("entries", [])
                  if row.get("cycle_id") == cycle and row.get("ea") == ea]
    if len(queue_rows) != 1 or not (
            queue_rows[0].get("stage4_decision_sha256") == sha256(stage4_decision_path)
            and queue_rows[0].get("stage4_verification_sha256") == sha256(stage4_verification_path)
            and queue_rows[0].get("stage4_event_sha256") == stage4_handoff.get("event_sha256")):
        return False, "Stage 4 queue entry does not bind the Stage 4 evidence", None
    for role, path in (("stage3_event", stage3_event_path), ("stage3_decision", stage3_decision_path),
                       ("stage3_verification", stage3_verification_path), ("stage3_handoff", stage3_handoff_path),
                       ("stage4_event", stage4_event_path), ("stage4_decision", stage4_decision_path),
                       ("stage4_verification", stage4_verification_path), ("stage4_handoff", stage4_handoff_path),
                       ("stage4_queue", queue_path)):
        _receipt_evidence_item(evidence, role, path)

    # Successful Stage 4.5 three-run sequence; every artifact is hash-bound.
    valid, detail, stage45_event = verify_stage45_three_runs(
        state, state_dir, successful_terminal=True, allow_consumed=True)
    if not valid or stage45_event is None:
        return False, f"Stage 4.5: {detail}", None
    for run_number in (1, 2, 3):
        prefix = f"{cycle}.stage45.run{run_number}"
        decision_path = state_dir / "decisions" / f"{prefix}.decision.json"
        verification_path = state_dir / "verification" / f"{prefix}.verification.json"
        event_path = state_dir / "events" / f"{prefix}.complete.json"
        decision = load_object(decision_path)
        runtime_path = state_dir / "stage45_v2" / f"{prefix}.runtime.json"
        complete_path = state_dir / "stage45_v2" / f"{cycle}.stage45.{decision['selected_tf']}.run{run_number}.complete.json"
        event_path_obj = load_object(event_path)
        if run_number == 3:
            ok, _ = _event_consumed_as(event_path, cycle, "4.5", "dispatch_stage5")
        else:
            ok = (event_path_obj.get("cycle_id") == cycle
                  and str(event_path_obj.get("stage")) == "4.5"
                  and event_path_obj.get("consumed") is False)
        if not ok or event_path_obj.get("ea") != ea:
            return False, f"Stage 4.5 run {run_number} was not consumed by the next expected stage", None
        for role, path in ((f"stage45_run{run_number}_decision", decision_path),
                           (f"stage45_run{run_number}_verification", verification_path),
                           (f"stage45_run{run_number}_event", event_path),
                           (f"stage45_run{run_number}_runtime", runtime_path),
                           (f"stage45_run{run_number}_complete", complete_path),
                           (f"stage45_run{run_number}_csv", Path(decision.get("csv", "")))):
            _receipt_evidence_item(evidence, role, path)

    # Stage 5 decision, verification and result agree; preserve market verdict separately.
    stage5_event_path = Path(state.get("last_stage5_event") or "")
    stage5_decision_path = Path(state.get("last_stage5_decision") or "")
    stage5_verification_path = Path(state.get("last_stage5_verification") or "")
    valid, detail, stage5_event = _validate_verified_transition(
        state, 5, "last_stage5_decision", "last_stage5_verification", "last_stage5_event",
        {"HOLD_FOR_REVIEW", "STAGE5_FORWARD_FAIL", "STAGE5_FORWARD_PASS"})
    if not valid or stage5_event is None or stage5_event.get("ea") != ea:
        return False, f"Stage 5: {detail}", None
    stage5_decision = load_object(stage5_decision_path)
    stage5_verification = load_object(stage5_verification_path)
    result_path = Path(stage5_decision.get("result", ""))
    if not result_path.is_file():
        return False, "Stage 5 result artifact is missing", None
    result = load_object(result_path)
    if (stage5_decision.get("cycle_id") != cycle or stage5_decision.get("ea") != ea
            or stage5_decision.get("verdict") != stage5_event.get("verdict")
            or stage5_decision.get("result_sha256") != sha256(result_path)
            or stage5_verification.get("result_sha256") != sha256(result_path)
            or result.get("cycle_id") != cycle or result.get("ea") != ea
            or str(result.get("stage")) != "5"
            or result.get("stage45_decision_sha256") != sha256(state_dir / "decisions" / f"{cycle}.stage45.run3.decision.json")
            or result.get("stage45_csv_sha256") != sha256(Path(load_object(state_dir / "decisions" / f"{cycle}.stage45.run3.decision.json")["csv"]))
            or result.get("stage45_event_sha256") != sha256(state_dir / "events" / f"{cycle}.stage45.run3.complete.json")):
        return False, "Stage 5 result does not bind the terminal Stage 4.5 evidence", None
    for role, path in (("stage5_event", stage5_event_path), ("stage5_decision", stage5_decision_path),
                       ("stage5_verification", stage5_verification_path), ("stage5_result", result_path)):
        _receipt_evidence_item(evidence, role, path)
    stage5_runtime_path = state_dir / "stage5_v2" / f"{cycle}.stage5.runtime.json"
    if not stage5_runtime_path.is_file():
        return False, "Stage 5 runtime ownership artifact is missing", None
    stage5_runtime = load_object(stage5_runtime_path)
    if (stage5_runtime.get("cycle_id") != cycle or stage5_runtime.get("ea") != ea
            or stage5_runtime.get("status") != "PUBLISHED"):
        return False, "Stage 5 runtime ownership artifact identity/status is invalid", None
    _receipt_evidence_item(evidence, "stage5_runtime", stage5_runtime_path)
    _snapshot_mutable_receipt_evidence(evidence, state_dir, cycle)
    receipt = {"cycle_id": cycle, "ea": ea,
               "stage5_event_sha256": evidence["stage5_event"]["sha256"],
               "strategy_verdict": stage5_event.get("verdict"),
               "result_verdict": result.get("verdict"),
               "evidence": evidence, "recorded_utc": utc_now()}
    return True, "Stage 1–5 evidence chain passed; Stage 5 strategy outcome recorded separately", receipt


def dispatch_claim_path(claims_dir: Path, event_id: str, action: str,
                        content_identity: dict | None = None) -> Path:
    safe_id = "".join(character if character.isalnum() or character in "-_." else "_" for character in event_id)
    if content_identity is not None:
        identity_digest = hashlib.sha256(json.dumps(
            content_identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")).hexdigest().upper()
        suffix = f".{action}.{identity_digest}.claim.json"
    else:
        suffix = f".{action}.claim.json"
    filename = f"{safe_id}{suffix}"
    # Windows limits one filename component to 255 characters. Long
    # authenticated event IDs are retained in the claim body; shorten only
    # the on-disk slug and add a digest of the complete event ID to avoid
    # collisions between IDs sharing a long prefix.
    if len(filename) > 240:
        event_digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest().upper()[:12]
        prefix_budget = max(1, 240 - len(suffix) - len(event_digest) - 1)
        filename = f"{safe_id[:prefix_budget]}~{event_digest}{suffix}"
    return claims_dir / filename


def persist_stage2_dispatch_claim(state_path: Path, claim_path: Path,
                                  expected_event_id: str) -> dict:
    """Publish the stable dispatch identity before starting the Stage 2 worker."""
    claim = load_object(claim_path)
    identity = claim.get("content_identity")
    identity_digest = str(claim.get("content_identity_sha256") or "").upper()
    if (claim.get("schema_version") != 3 or claim.get("action") != "dispatch_stage2"
            or claim.get("event_id") != expected_event_id or not isinstance(identity, dict)):
        raise ValueError("Stage 2 dispatch claim is incomplete before execution")
    computed = hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")).hexdigest().upper()
    if computed != identity_digest:
        raise ValueError("Stage 2 dispatch claim content identity hash is invalid")
    state = load_object(state_path)
    if (state.get("cycle_id") != identity.get("cycle_id")
            or (state.get("candidate_ea") or state.get("current_ea")) != identity.get("candidate_ea")):
        raise ValueError("Stage 2 dispatch claim no longer matches canonical cycle and EA")
    marker = {
        "schema_version": 1, "event_id": claim["event_id"],
        "action": claim["action"], "claim_path": str(claim_path.resolve()),
        "content_identity_sha256": identity_digest,
        "claimed_utc": claim.get("claimed_utc"), "content_identity": identity,
    }
    state["stage2_dispatch_claim"] = marker
    atomic_json(state_path, state)
    return marker


def claim_action(claims_dir: Path, event_id: str, action: str,
                 content_identity: dict | None = None) -> tuple[bool, Path]:
    claim = dispatch_claim_path(claims_dir, event_id, action, content_identity)
    identity_digest = (hashlib.sha256(json.dumps(
        content_identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")).hexdigest().upper() if content_identity is not None else None)
    claims_dir.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(str(claim), os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    except FileExistsError:
        return False, claim
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump({"schema_version": 3, "event_id": event_id, "action": action,
                   "claimed_utc": utc_now(), "dispatch_outcome": "claimed",
                   "execution_id": None, "terminal_outcome": None,
                   "content_identity": content_identity,
                   "content_identity_sha256": identity_digest}, handle, indent=2)
        handle.write("\n"); handle.flush(); os.fsync(handle.fileno())
    return True, claim


def stage2_dispatch_content_identity(result: dict, state: dict) -> dict:
    """Bind a Stage 2 dispatch claim to its current event, ticket, and candidate bytes."""
    event_value = (result.get("event_path") or state.get("last_stage1_event")
                   or state.get("stage2_failure_event") or state.get("stage2_invalid_event"))
    event_path = Path(event_value) if event_value else Path("")
    ticket_value = result.get("recovery_ticket") or state.get("stage2_source_defect_ticket")
    if result.get("status") == "STAGE2_VERIFIER_RETRY_READY":
        ticket_value = state.get("stage2_verifier_retry_ticket")
    ticket_path = Path(ticket_value) if ticket_value else None
    evidence = result.get("zero_trade_evidence") or {}
    source_value = state.get("candidate_source") or state.get("stage2_candidate_source")
    source_path = Path(source_value) if source_value else None
    identity = {
        "cycle_id": str(state.get("cycle_id") or ""),
        "candidate_ea": str(state.get("candidate_ea") or state.get("current_ea") or ""),
        "candidate_source_sha256": sha256(source_path) if source_path and source_path.is_file() else None,
        "event_id": str(result.get("event_id") or ""),
        "event_sha256": consumption_insensitive_sha256(event_path) if event_path.is_file() else None,
        "ticket_sha256": sha256(ticket_path) if ticket_path and ticket_path.is_file() else None,
        "zero_trade_decision_sha256": evidence.get("decision_sha256"),
        "zero_trade_verification_sha256": evidence.get("verification_sha256"),
    }
    repair_identity = (result.get("stage2_repair_identity") or result.get("dispatch_identity")
                       or evidence.get("dispatch_identity"))
    if isinstance(repair_identity, dict):
        identity["repair_identity"] = repair_identity
    if not identity["candidate_ea"] and not identity["candidate_source_sha256"]:
        raise ValueError("Stage 2 dispatch claim lacks candidate identity/content")
    if not any(identity[key] for key in ("event_sha256", "ticket_sha256",
                                         "zero_trade_decision_sha256", "repair_identity")):
        raise ValueError("Stage 2 dispatch claim lacks authenticated event or ticket content")
    return identity


def stage3_start_content_identity(state: dict) -> dict:
    """Bind each Stage 3 start to the exact authenticated Stage 2 publication."""
    event_path = Path(state.get("last_stage2_event") or "")
    verification_path = Path(state.get("last_stage2_verification") or "")
    if not event_path.is_file() or not verification_path.is_file():
        raise ValueError("Stage 3 dispatch identity requires the current Stage 2 event and verification")
    event = load_object(event_path)
    verification = load_object(verification_path)
    source_path = Path(verification.get("source") or "")
    if not source_path.is_file():
        raise ValueError("Stage 3 dispatch identity requires the verified Stage 2 source")
    return {
        "cycle_id": str(state.get("cycle_id") or ""),
        "stage2_event_id": str(event.get("event_id") or f"{state.get('cycle_id')}:stage2:complete"),
        "stage2_event_sha256": consumption_insensitive_sha256(event_path),
        "stage2_verification_sha256": sha256(verification_path),
        "source_sha256": sha256(source_path),
        "ex5_sha256": str(event.get("ex5_sha256") or ""),
    }


def stage3_launch_lineage_retry_identity(state: dict) -> dict | None:
    """Authorize one same-cycle Stage 3 retry after the exact repaired config-lineage preflight."""
    retry_count = state.get("stage3_launch_lineage_retry_count", 0)
    initial_failure = (
        state.get("current_stage") == "stage3_running"
        and state.get("stage_status") == "stage3_infrastructure_failure"
        and state.get("next_action") == "inspect_stage3_infrastructure_failure"
        and retry_count == 0
    )
    admitted_retry = (
        state.get("current_stage") == "stage3_pending"
        and state.get("stage_status") == "stage2_complete_held"
        and state.get("hold_after_stage") == 2
        and state.get("next_action") == "run_stage3"
        and retry_count == 1
    )
    if (not (initial_failure or admitted_retry)
            or not (authorizes_stages_1_to_3(state) or authorizes_stages_1_to_5(state))):
        return None
    cycle = str(state.get("cycle_id") or "")
    failure_path = Path(state.get("stage3_failure_event") or "")
    handoff_path = ROOT / "state" / "handoffs" / "Stage3_v2.json"
    runtime_path = ROOT / "state" / "stage3_v2" / f"{cycle}.stage3.runtime.json"
    launch_path = ROOT / "state" / "kcos_stage3_launch_v2.py"
    if (not cycle or not failure_path.is_file() or not handoff_path.is_file()
            or runtime_path.exists() or not launch_path.is_file()
            or rotation.active_stage3_runtime(state, ROOT) is not None):
        return None
    try:
        failure, handoff = load_object(failure_path), load_object(handoff_path)
        if not (failure.get("cycle_id") == cycle and failure.get("stage") == 3
                and failure.get("status") == "failed"
                and failure.get("failure_kind") == "STAGE3_EXECUTION_FAILURE"
                and failure.get("rotation_eligible") is False
                and failure.get("error") == "Stage 3 approved tester configuration lineage mismatch"
                and handoff.get("cycle_id") == cycle and handoff.get("ea") == state.get("current_ea")):
            return None
        import kcos_stage3_launch_v2 as stage3_launch
        stage3_launch.validate_stage3_launch_lineage(handoff)
        identity = {
            "cycle_id": cycle,
            "failure_event_sha256": sha256(failure_path),
            "stage3_handoff_sha256": sha256(handoff_path),
            "stage3_launch_sha256": sha256(launch_path),
            "stage2_event_sha256": str(handoff.get("stage2_event_sha256") or "").upper(),
            "source_sha256": str(handoff.get("source_sha256") or "").upper(),
            "approved_tester_config_sha256": str(handoff.get("approved_tester_config_sha256") or "").upper(),
        }
        identity["identity_sha256"] = hashlib.sha256(json.dumps(
            identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")).hexdigest().upper()
        if admitted_retry and state.get("stage3_launch_lineage_retry_identity") != identity:
            return None
        return identity
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        return None


def stage3_continuation_content_identity(result: dict, state: dict) -> dict:
    """Bind timeframe continuation to the exact authenticated completion receipt."""
    event_path = Path(result.get("event_path") or "")
    if not event_path.is_file():
        raise ValueError("Stage 3 continuation identity requires the authenticated completion event")
    event = load_object(event_path)
    if (event.get("cycle_id") != state.get("cycle_id")
            or event.get("event_id") != result.get("event_id")
            or event.get("status") != "TIMEFRAME_COMPLETE"):
        raise ValueError("Stage 3 continuation event identity does not match the current cycle")
    return {
        "cycle_id": str(state.get("cycle_id") or ""),
        "event_id": str(event.get("event_id") or ""),
        "timeframe": str(event.get("timeframe") or ""),
        "event_sha256": consumption_insensitive_sha256(event_path),
    }


def reclaim_failed_dispatch_claim(claim: Path, action: str, control: dict, db_path: Path) -> bool:
    """Reclaim only a claim with a recorded terminal dispatch failure and no active job."""
    if not claim.is_file():
        return False
    try:
        data = load_object(claim)
        # A claim without a recorded outcome may represent a process that
        # crashed between admission and Hermes registration. Age cannot prove
        # that dispatch abandoned, so leave it consumed.
        if data.get("dispatch_outcome") != "failed" or not data.get("execution_id"):
            return False
        terminal = str(data.get("terminal_outcome") or "").casefold()
        if terminal not in {"failed", "error", "cancelled", "canceled", "timed_out", "rejected"}:
            return False
        current = execution_status(db_path, str(data["execution_id"])).casefold()
        if current not in {"failed", "error", "cancelled", "canceled", "timed_out", "rejected"}:
            return False
    except Exception:
        return False
    stage_by_action = {"rotate_stage1": "1", "dispatch_stage1": "1", "dispatch_stage2": "2", "dispatch_stage3": "3",
                       "start_stage3": "3", "continue_stage3": "3", "dispatch_stage4": "4",
                       "dispatch_stage45": "4.5", "dispatch_stage5": "5"}
    job_id = (control.get("stage_jobs") or {}).get(stage_by_action.get(action, ""))
    if job_id:
        connection = sqlite3.connect(str(db_path))
        try:
            active = connection.execute(
                "SELECT 1 FROM executions WHERE job_id = ? AND status IN ('claimed','running') LIMIT 1",
                (job_id,),
            ).fetchone()
        finally:
            connection.close()
        if active:
            return False
    if action == "dispatch_stage2":
        lock_state = stage2_executor_lock_active(claim.parent.parent)
        if lock_state is not False:
            return False
    elif action not in {"dispatch_stage1", "rotate_stage1"}:
        # These stages can own terminal/tester workers. Their process evidence
        # is not available in this claim-only function, so absence cannot be
        # proven and the claim remains consumed.
        return False
    claim.unlink(missing_ok=True)
    return True


def stage2_executor_lock_active(state_dir: Path) -> bool | None:
    """Probe the Stage 2 executor byte lock; None means ownership is uncertain."""
    lock_path = state_dir / "leases" / "stage2_executor.lock"
    if not lock_path.exists():
        return False
    try:
        import msvcrt
        with lock_path.open("r+b") as handle:
            try:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                return True
            try:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                return None
        return False
    except (OSError, ImportError):
        return None


def authenticated_stage2_initial_preflight_retry(state: dict, control: dict,
                                                failure_path: Path, failure: dict,
                                                db_path: Path) -> dict | None:
    """Authorize a bounded same-candidate retry for authenticated preflight defects.

    Each defect class is retried at most once after cycle, event, source, and
    Stage 1 spec identity are revalidated. The Minato symbol mismatch case also
    requires a rolled-back transaction whose complete snapshot is unchanged and
    the corrected canonical context builder to be present. Generic executor
    failures remain held.
    """
    cycle = str(state.get("cycle_id") or "")
    if (not cycle or failure.get("schema_version") != 2
            or failure.get("cycle_id") != cycle or failure.get("stage") != 2
            or failure.get("event_id") != f"{cycle}:stage2:failed"
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != "STAGE2_EXECUTOR_FAILURE"
            or failure.get("rotation_eligible") is not False
            or failure.get("failed_checks") != []
            or failure.get("source_identity_available") is not True):
        return None
    source_value = state.get("candidate_source") or state.get("stage2_candidate_source")
    spec_value = state.get("last_stage1_artifact")
    source_path = Path(source_value) if source_value else None
    spec_path = Path(spec_value) if spec_value else None
    state_dir = failure_path.parent.parent.resolve()
    try:
        if (failure_path.resolve() != (state_dir / "events" / f"{cycle}.stage2.failed.json").resolve()
                or source_path is None or not source_path.is_file()
                or source_path.resolve() != Path(str(failure.get("source") or "")).resolve()
                or source_path.parent.resolve() != (state_dir / "candidates").resolve()
                or failure.get("ea") != (state.get("candidate_ea") or state.get("current_ea"))
                or failure.get("candidate_ea") != failure.get("ea")
                or str(failure.get("source_sha256") or "").upper() != sha256(source_path).upper()
                or spec_path is None or not spec_path.is_file()
                or spec_path.resolve() != Path(str(failure.get("stage1_spec") or "")).resolve()
                or spec_path.parent.resolve() != (state_dir / "specs").resolve()
                or str(failure.get("stage1_spec_sha256") or "").upper() != sha256(spec_path).upper()):
            return None
        error_text = str(failure.get("error") or "")
        try:
            error = json.loads(error_text)
        except (TypeError, ValueError, json.JSONDecodeError):
            error = {}
        failure_class = None
        if error == {"outcome": "INFRASTRUCTURE_BLOCKED",
                     "reason": "ValueError: cycle state has no canonical Stage 2 failure event"}:
            failure_class = "missing_canonical_failure_event"
        elif (failure.get("exception_type") == "AttributeError"
              and error_text == "'str' object has no attribute 'resolve'"
              and "in stage2_repair_loop_required" in str(failure.get("traceback") or "")
              and "candidate_source.resolve()" in str(failure.get("traceback") or "")):
            failure_class = "preflight_path_type_mismatch"
        elif (failure.get("exception_type") == "TypeError"
              and error_text == "expected str, bytes or os.PathLike object, not NoneType"
              and "in stage2_repair_loop_required" in str(failure.get("traceback") or "")
              and "Path(candidate_source)" in str(failure.get("traceback") or "")
              and "in main" in str(failure.get("traceback") or "")
              and "repair_evidence_present = stage2_repair_loop_required(" in str(failure.get("traceback") or "")):
            failure_class = "generated_candidate_path_not_refreshed"
        elif (failure.get("exception_type") == "RuntimeError"
              and error.get("outcome") == "INFRASTRUCTURE_BLOCKED"
              and error.get("returncode") == 2
              and str(error.get("episode_id") or "") in str(failure.get("traceback") or "")):
            # This failure occurred while binding hash-authenticated Stage 3
            # diagnostics. Retry only when the transaction restored every
            # snapshotted byte and the canonical repair loop contains the
            # authenticated Stage 1 symbol fallback that fixes this mismatch.
            try:
                episode_id = str(error.get("episode_id") or "")
                attempt_dir = Path(str(error.get("attempt_dir") or "")).resolve()
                transactions = (state_dir / "diagnostics" / "stage2_repair_transactions").resolve()
                expected_episode_dir = (transactions / episode_id).resolve()
                outcome_path = attempt_dir / "outcome.json"
                episode_path = attempt_dir / "episode.json"
                exception_path = attempt_dir / "exception.txt"
                journal_path = attempt_dir / "transaction-journal.json"
                outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
                episode = json.loads(episode_path.read_text(encoding="utf-8"))
                journal = json.loads(journal_path.read_text(encoding="utf-8"))
                exception_text = exception_path.read_text(encoding="utf-8").strip()
                repair_loop_path = state_dir / "kcos_stage2_repair_loop_v1.py"
                repair_loop_text = repair_loop_path.read_text(encoding="utf-8")
                snapshots = journal.get("snapshot")
                snapshot_ok = isinstance(snapshots, dict) and bool(snapshots)
                if snapshot_ok:
                    for snapshot_name, encoded in snapshots.items():
                        snap_path = Path(snapshot_name).resolve()
                        try:
                            snap_path.relative_to(state_dir.resolve())
                        except ValueError:
                            snapshot_ok = False
                            break
                        if not snap_path.is_file() or snap_path.read_bytes() != base64.b64decode(encoded, validate=True):
                            snapshot_ok = False
                            break
                required_snapshot_paths = {
                    str(source_path.resolve()).casefold(),
                    str(Path(str(failure.get("verification") or "")).resolve()).casefold(),
                    str(Path(str(failure.get("compile_attempt") or "")).resolve()).casefold(),
                }
                recorded_snapshot_paths = {str(Path(name).resolve()).casefold() for name in snapshots} if isinstance(snapshots, dict) else set()
                module_repair_present = (
                    "expected_symbol = handoff_symbol or spec_symbol" in repair_loop_text
                    and "Stage 2 handoff symbol differs from the authenticated Stage 1 spec" in repair_loop_text
                )
                if (not re.fullmatch(r"[A-F0-9]{64}", episode_id)
                        or attempt_dir.parent.resolve() != expected_episode_dir
                        or expected_episode_dir.parent.resolve() != transactions
                        or outcome != {"outcome": "INFRASTRUCTURE_BLOCKED", "passed": False,
                                       "episode_id": episode_id, "attempt_dir": str(attempt_dir),
                                       "returncode": 2, "recovered_prior_transactions": []}
                        or episode.get("cycle_id") != cycle
                        or episode.get("candidate") != failure.get("ea")
                        or str(episode.get("baseline_source_sha256") or "").upper() != sha256(source_path).upper()
                        or journal.get("schema_version") != 1
                        or journal.get("status") != "ROLLED_BACK"
                        or not snapshot_ok
                        or not required_snapshot_paths.issubset(recorded_snapshot_paths)
                        or exception_text != "ValueError: Stage 3 M5 Minato diagnostics have a mismatched identity"
                        or not module_repair_present):
                    return None
                failure_class = "stage3_minato_symbol_from_authenticated_spec"
                repair_module_sha256 = sha256(repair_loop_path).upper()
            except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, base64.binascii.Error):
                return None
        if (failure_class is None or str(failure.get("error_sha256") or "").upper()
                != hashlib.sha256(error_text.encode("utf-8")).hexdigest().upper()):
            return None
        if failure_class != "stage3_minato_symbol_from_authenticated_spec" and (
                failure.get("verification") is not None or failure.get("compile_attempt") is not None):
            return None
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None
    allowed = control.get("allowed_stages", [])
    stage2_job = str((control.get("stage_jobs") or {}).get("2") or "")
    if (control.get("dispatch_enabled") is not True or 2 not in allowed or not stage2_job):
        return None
    prior_retry_classes = state.get("stage2_preflight_retry_failure_classes")
    if (state.get("stage2_preflight_retry_cycle_id") != cycle
            or not isinstance(prior_retry_classes, list)):
        prior_retry_classes = []
    try:
        prior_retry_count = int(state.get("stage2_preflight_retry_count", 0) or 0)
    except (TypeError, ValueError):
        prior_retry_count = 1
    if (not prior_retry_classes
            and state.get("stage2_preflight_retry_cycle_id") == cycle
            and prior_retry_count >= 1
            and (state.get("stage2_preflight_retry_identity") or {}).get("repair_scope")
                == "same_candidate_initial_preflight_retry_once"):
        # R10 recorded only a count; infer the sole class it could dispatch.
        prior_retry_classes = ["missing_canonical_failure_event"]
    failure_class_consumed = failure_class in prior_retry_classes
    if not failure_class_consumed:
        try:
            connection = sqlite3.connect(str(db_path), timeout=1.0)
            try:
                active = connection.execute(
                    "SELECT 1 FROM executions WHERE job_id = ? AND status IN ('claimed','running') LIMIT 1",
                    (stage2_job,),
                ).fetchone()
            finally:
                connection.close()
        except sqlite3.Error:
            return None
        if active or stage2_executor_lock_active(state_dir) is not False:
            return None
    identity = {
        "cycle_id": cycle,
        "candidate_ea": str(failure.get("ea") or ""),
        "source_sha256": str(failure.get("source_sha256") or "").upper(),
        "stage1_spec_sha256": str(failure.get("stage1_spec_sha256") or "").upper(),
        "failure_event_sha256": sha256(failure_path).upper(),
        "failure_kind": failure.get("failure_kind"),
        "repair_scope": "same_candidate_initial_preflight_retry_once",
        "preflight_failure_class": failure_class,
        "previous_retry_classes": prior_retry_classes,
    }
    if failure_class == "stage3_minato_symbol_from_authenticated_spec":
        identity["repair_module_sha256"] = repair_module_sha256
    identity["identity_sha256"] = hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")).hexdigest().upper()
    return identity


def authenticated_stage2_compile_repair_retry(state: dict, control: dict,
                                               failure_path: Path, failure: dict,
                                               db_path: Path) -> dict | None:
    """Authorize one same-candidate Stage 2 run for a compiler-proven EA defect."""
    cycle = str(state.get("cycle_id") or "")
    if state.get("stage2_compile_repair_cycle_id") == cycle and cycle:
        recorded = state.get("stage2_compile_repair_identity")
        execution_id = str(state.get("stage2_compile_repair_execution_id") or "")
        try:
            attempt_count = int(state.get("stage2_compile_repair_attempt_count", 0))
        except (TypeError, ValueError):
            return None
        recorded_unsigned = ({key: value for key, value in recorded.items()
                              if key != "identity_sha256"} if isinstance(recorded, dict) else {})
        recorded_digest = hashlib.sha256(json.dumps(
            recorded_unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")).hexdigest().upper()
        if (attempt_count == 1 and isinstance(recorded, dict)
                and recorded.get("cycle_id") == cycle
                and recorded.get("repair_scope") == "one_same_candidate_compile_repair_episode"
                and recorded_digest == str(recorded.get("identity_sha256") or "").upper()
                and execution_id):
            execution_state = execution_status(db_path, execution_id)
            return {"cycle_id": cycle, "attempt_count": attempt_count,
                    "execution_id": execution_id, "execution_state": execution_state,
                    "retry_state": "active" if execution_state in {"claimed", "running", "started", "queued"}
                    else "exhausted"}
    source_value = state.get("candidate_source") or state.get("stage2_candidate_source")
    spec_value = state.get("last_stage1_artifact")
    source = Path(source_value) if source_value else None
    spec = Path(spec_value) if spec_value else None
    state_dir = failure_path.parent.parent.resolve()
    if (not cycle or failure.get("schema_version") != 2
            or failure.get("cycle_id") != cycle or failure.get("stage") != 2
            or failure.get("event_id") != f"{cycle}:stage2:failed"
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != "COMPILE_INTEGRITY_FAILURE"
            or failure.get("failed_checks") != ["compile_clean"]
            or failure.get("rotation_eligible") is not False
            or failure.get("verification") is not None
            or failure.get("source_identity_available") is not True
            or source is None or spec is None):
        return None
    try:
        if (failure_path.resolve() != (state_dir / "events" / f"{cycle}.stage2.failed.json").resolve()
                or source.resolve().parent != (state_dir / "candidates").resolve()
                or not source.is_file() or not spec.is_file()
                or spec.resolve().parent != (state_dir / "specs").resolve()
                or Path(str(failure.get("source") or "")).resolve() != source.resolve()
                or Path(str(failure.get("stage1_spec") or "")).resolve() != spec.resolve()
                or failure.get("ea") != (state.get("candidate_ea") or state.get("current_ea"))
                or failure.get("candidate_ea") != failure.get("ea")
                or str(failure.get("source_sha256") or "").upper() != sha256(source).upper()
                or str(failure.get("stage1_spec_sha256") or "").upper() != sha256(spec).upper()):
            return None
        compile_path = Path(str(failure.get("compile_attempt") or ""))
        if (compile_path.resolve().parent != (state_dir / "attempts").resolve()
                or compile_path.name != f"{cycle}.stage2.compile.latest.json"
                or not compile_path.is_file()):
            return None
        compile_doc = load_object(compile_path)
        if (compile_doc.get("cycle_id") != cycle or compile_doc.get("stage") != 2
                or compile_doc.get("ea") != failure.get("ea")
                or compile_doc.get("passed") is not False
                or compile_doc.get("fresh_ex5") is not False
                or int(compile_doc.get("errors", 0) or 0) <= 0
                or str(compile_doc.get("mq5_sha256") or "").upper() != sha256(source).upper()):
            return None
        log_path = Path(str(compile_doc.get("log") or ""))
        if log_path.resolve().parent != (state_dir / "attempts").resolve() or not log_path.is_file():
            return None
        raw_log = log_path.read_bytes()
        log_text = (raw_log.decode("utf-16", errors="replace")
                    if raw_log.startswith((bytes((0xFF, 0xFE)), bytes((0xFE, 0xFF))))
                    else raw_log.decode("utf-8-sig", errors="replace"))
        required = ("SetDeviationsInPoints", "SetTypeFillingByVolume")
        if not all(re.search(r"(?i)undeclared identifier\s+'" + re.escape(name) + r"'", log_text)
                   for name in required):
            return None
        source_text = source.read_text(encoding="utf-8-sig", errors="replace")
        if (len(re.findall(r"\bSetDeviationsInPoints\s*\(\s*10\s*\)", source_text)) != 1
                or len(re.findall(r"\bSetTypeFillingByVolume\s*\(\s*_Symbol\s*,\s*"
                                  r"SymbolInfoDouble\s*\(\s*_Symbol\s*,\s*SYMBOL_VOLUME_MIN\s*\)\s*\)",
                                  source_text)) != 1):
            return None
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None
    if (control.get("dispatch_enabled") is not True or 2 not in control.get("allowed_stages", [])
            or not (control.get("stage_jobs") or {}).get("2")):
        return None
    try:
        connection = sqlite3.connect(str(db_path), timeout=1.0)
        try:
            active = connection.execute(
                "SELECT 1 FROM executions WHERE job_id = ? AND status IN ('claimed','running') LIMIT 1",
                (str((control.get("stage_jobs") or {}).get("2")),),
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error:
        return None
    if active or stage2_executor_lock_active(state_dir) is not False:
        return None
    identity = {"cycle_id": cycle, "candidate_ea": str(failure.get("ea") or ""),
                "candidate_source_sha256": sha256(source).upper(),
                "stage1_spec_sha256": sha256(spec).upper(),
                "failure_event_sha256": sha256(failure_path).upper(),
                "compile_attempt_sha256": sha256(compile_path).upper(),
                "compile_log_sha256": sha256(log_path).upper(),
                "repair_scope": "one_same_candidate_compile_repair_episode"}
    identity["identity_sha256"] = hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")).hexdigest().upper()
    return identity


def record_dispatch_claim_outcome(claim: Path, *, accepted: bool, execution_id: str | None,
                                  detail: str, db_path: Path) -> None:
    """Durably attach Hermes registration and observed terminal state to a claim."""
    if not claim.is_file():
        return
    data = load_object(claim)
    terminal = execution_status(db_path, str(execution_id)).casefold() if execution_id else ""
    terminal_states = {"completed", "complete", "succeeded", "success", "failed", "error",
                       "cancelled", "canceled", "timed_out", "rejected"}
    data.update({"dispatch_outcome": "accepted" if accepted else "failed",
                 "dispatch_detail": str(detail or ""),
                 "execution_id": execution_id,
                 "terminal_outcome": terminal if terminal in terminal_states else None,
                 "outcome_recorded_utc": utc_now()})
    atomic_json(claim, data)


def compact_supervisor_status(state: dict, result: dict, control: dict,
                              cap_path: Path | None, db_path: Path,
                              state_path: Path | None = None) -> dict:
    """Expose operational status separately from the Codex goal's health ledger."""
    receipts, starts = 0, 0
    if cap_path is not None and cap_path.is_file():
        try:
            cap = cycle_cap.load_cap(cap_path)
            receipts, starts = len(cap.get("healthy_receipts", [])), len(cap.get("started_cycles", []))
        except (OSError, ValueError, TypeError):
            pass
    stage = str(state.get("current_stage") or "unknown")
    stage_key = stage.casefold().replace("_", "").replace(".", "")
    stage_number = ("4.5" if "stage45" in stage_key or "stage4.5" in stage.casefold()
                    else "4" if "stage4" in stage_key
                    else "3" if "stage3" in stage_key
                    else "2" if "stage2" in stage_key
                    else "1" if "stage1" in stage_key
                    else "5" if "stage5" in stage_key else "")
    job_id = (control.get("stage_jobs") or {}).get(stage_number)
    execution_id = result.get("execution_id") or state.get(f"last_stage{stage_number}_execution_id")
    hermes_status = execution_status(db_path, str(execution_id)) if execution_id else "none"
    active = str(execution_id) if hermes_status in {"claimed", "running", "started", "queued"} else None
    proposal_status = stage2_proposal_budget_status(state, state_path)
    blocker = result.get("detail") if ("HELD" in str(result.get("status", ""))
                                       or "FAIL" in str(result.get("status", ""))
                                       or result.get("status") == "UNKNOWN_STALLED") else None
    return {
        "healthy_receipts": receipts,
        "starts": starts,
        "last_stage": stage,
        "active_execution": active,
        "attempts_remaining": proposal_status.get("remaining"),
        "proposal_budget": proposal_status,
        "last_verified_change": state.get("last_verified_change"),
        "blocker": blocker,
        "hermes_activity": {"execution_id": execution_id, "status": hermes_status,
                            "job_id": job_id},
        "goal_state": {"healthy_receipts": receipts, "target": 5,
                       "source": "campaign_receipt_ledger" if cap_path is not None else "unconfigured"},
    }


def stage2_proposal_budget_status(state: dict, state_path: Path | None = None) -> dict:
    """Read proposal usage and limit from the repair loop's authoritative episode ledger."""
    try:
        stage2_handoff_path = Path(state.get("last_stage2_handoff") or ROOT / "state" / "handoffs" / "Stage2_v2.json")
        source_path = Path(state.get("candidate_source") or state.get("stage2_candidate_source") or "")
        failure_path = Path(state.get("stage2_failure_event") or state.get("stage2_invalid_event") or "")
        if not (stage2_handoff_path.is_file() and source_path.is_file() and failure_path.is_file()):
            return {"used": None, "limit": int(stage2_repair_loop.MAX_PROPOSALS), "remaining": None,
                    "source": "episode_ledger", "status": "unavailable"}
        handoff = load_object(stage2_handoff_path)
        handoff = bind_active_stage2_source_defect_ticket(state, handoff)
        if (handoff.get("cycle_id") != state.get("cycle_id")
                or Path(handoff.get("source") or "").resolve() != source_path.resolve()):
            return {"used": None, "limit": int(stage2_repair_loop.MAX_PROPOSALS), "remaining": None,
                    "source": "episode_ledger", "status": "handoff_mismatch"}
        verification_path = Path(state.get("last_stage2_verification") or "")
        episode = stage2_repair_loop.episode_identity(
            handoff, source_path, verification_path,
            state_path=state_path or (ROOT / "state" / "kcos_cycle_state_v2.json"))
        ledger_path = ROOT / "state" / "ledgers" / "stage2_repair_episodes.sqlite3"
        with sqlite3.connect(str(ledger_path)) as connection:
            row = connection.execute(
                "SELECT proposal_count, max_proposals, identity_json FROM episodes WHERE episode_id=?",
                (episode["episode_id"],)).fetchone()
        if row is None:
            used, configured = 0, int(episode.get(
                "proposal_limit", stage2_repair_loop.MAX_PROPOSALS))
        else:
            used, configured = int(row[0]), int(row[1])
            recorded_identity = json.loads(str(row[2]))
            if recorded_identity != episode:
                raise ValueError("episode ledger identity does not match the current Stage 2 evidence")
        limit = min(int(stage2_repair_loop.MAX_PROPOSALS), configured)
        return {"used": used, "limit": limit, "remaining": max(0, limit - used),
                "episode_id": episode["episode_id"], "source": "episode_ledger", "status": "verified"}
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
        return {"used": None, "limit": int(stage2_repair_loop.MAX_PROPOSALS), "remaining": None,
                "source": "episode_ledger", "status": "unavailable"}


def bind_active_stage2_source_defect_ticket(state: dict, handoff: dict) -> dict:
    """Bind the canonical Stage 3 invalid-stops ticket for accurate episode accounting."""
    ticket_value = state.get("stage2_source_defect_ticket")
    if not ticket_value:
        return handoff
    ticket_path = Path(ticket_value)
    ticket = load_object(ticket_path)
    ticket_hash = sha256(ticket_path)
    repair_kind = ticket.get("repair_kind")
    if (repair_kind not in {"stage3_tester_journal_invalid_stops",
                            "stage3_tester_journal_multidimensional_array_row_bounds",
                            "stage3_tester_journal_quantile_interpolation_bounds",
                            "stage3_tester_journal_ea_runtime_array_bounds"}
            or ticket.get("cycle_id") != state.get("cycle_id")
            or ticket.get("ea") != state.get("current_ea")
            or ticket_hash != str(state.get("stage2_source_defect_ticket_sha256") or "").upper()
            or ticket.get("cycle_id") != handoff.get("cycle_id")
            or ticket.get("ea") != handoff.get("ea")
            or Path(ticket.get("candidate_source") or "").resolve()
            != Path(handoff.get("source") or "").resolve()):
        raise ValueError("active Stage 2 source-defect ticket does not bind the current handoff")
    runtime = (ticket.get("artifacts", {}).get("stage3_runtime") or {})
    diagnostic = ticket.get("stage3_journal_diagnostic") or {}
    matching = diagnostic.get("matching_error_lines") or []
    valid_diagnostic = (
        diagnostic.get("code") == "TESTER_EA_ORDER_REJECTED"
        and len(matching) >= 2
        if repair_kind == "stage3_tester_journal_invalid_stops"
        else diagnostic.get("code") == "TESTER_JOURNAL_ERROR"
        and len(matching) >= 2
        and all("runtime error 502" in str(line).casefold()
                and "array out of range" in str(line).casefold() for line in matching)
        and str(ticket.get("parent_source_sha256") or "").upper()
        == str((ticket.get("artifacts", {}).get("stage2_original_source") or {}).get("sha256") or "").upper()
    )
    if not stage2_source_recovery._check_bound_file(runtime) or not valid_diagnostic:
        raise ValueError("active Stage 3 source-defect ticket lacks authenticated runtime diagnostics")
    bound = dict(handoff)
    bound["stage3_runtime_repair"] = {
        "kind": ticket["repair_kind"], "ticket": str(ticket_path.resolve()),
        "parent_source_sha256": ticket.get("parent_source_sha256"),
        "ticket_sha256": ticket_hash, "runtime": runtime, "diagnostic": diagnostic}
    return bound


def stage2_repair_identity(state: dict, failure_path: Path | None = None) -> dict:
    """Build a Stage 2 episode identity from authenticated failure and inputs."""
    failure_path = failure_path or Path(state.get("stage2_failure_event") or state.get("stage2_invalid_event") or "")
    if not failure_path.is_file():
        raise ValueError("Stage 2 failure event is missing")
    failure = load_object(failure_path)
    if failure.get("cycle_id") != state.get("cycle_id") or failure.get("stage") != 2:
        raise ValueError("Stage 2 failure event is not same-cycle Stage 2 evidence")
    candidate_value = state.get("candidate_source") or state.get("stage2_candidate_source")
    candidate_path = Path(candidate_value) if candidate_value else None
    if candidate_path is None or not candidate_path.is_file():
        raise ValueError("Stage 2 repair identity source is missing")
    candidate_hash = sha256(candidate_path)
    candidate_ea = str(state.get("candidate_ea") or state.get("current_ea") or "")
    if not candidate_ea:
        raise ValueError("Stage 2 repair identity lacks the candidate EA identity")
    spec_path = Path(state.get("last_stage1_artifact") or "")
    if not spec_path.is_file():
        raise ValueError("Stage 2 repair identity lacks the effective Stage 1 input artifact")
    spec = load_object(spec_path)
    if not isinstance(spec.get("tester_config"), dict):
        raise ValueError("Stage 2 repair identity lacks the Stage 1 tester contract")
    handoff_path = Path(state.get("last_stage2_handoff") or ROOT / "state" / "handoffs" / "Stage2_v2.json")
    handoff = load_object(handoff_path) if handoff_path.is_file() else {}
    if (handoff.get("cycle_id") == state.get("cycle_id")
            and Path(handoff.get("source") or "").resolve() == candidate_path.resolve()
            and handoff.get("approved_tester_config") is not None):
        effective_contract = {
            "approved_tester_config": handoff["approved_tester_config"],
            "approved_tester_config_sha256": handoff.get("approved_tester_config_sha256"),
            "effective_input_signature": handoff.get("effective_input_signature"),
            "effective_inputs": handoff.get("effective_inputs"),
            "experiment": handoff.get("experiment", handoff.get("experiment_contract")),
            "data_identity": handoff.get("data_identity", handoff.get("dataset_identity")),
            "canonical_minato_sha256": handoff.get("canonical_minato_sha256"),
            "include_manifest": handoff.get("include_manifest"),
            "include_hashes": handoff.get("include_hashes"),
            "configuration_authority": handoff.get("configuration_authority"),
            "source_configuration_contract": handoff.get("source_configuration_contract"),
            "optimization_surface_policy": handoff.get("optimization_surface_policy"),
            "implementation_contract_sha256": handoff.get("implementation_contract_sha256"),
        }
    else:
        # Preserve the full canonical experiment and tester contract when no
        # current Stage 2 handoff has been published yet.
        effective_contract = {
            "tester_config": spec["tester_config"],
            "experiment": spec.get("experiment", spec.get("experiment_contract")),
            "strategy": spec.get("strategy"),
            "stage1_spec_sha256": sha256(spec_path),
        }
    effective_signature = hashlib.sha256(json.dumps(
        effective_contract, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")).hexdigest().upper()
    # Reuse the Stage 2 tester's canonical identity function. When a complete
    # passing verification is available, use its validated resolved contract;
    # for pre-publication failures, bind the same contract components directly
    # from the authenticated handoff/spec and current file hashes.
    verification_path = Path(state.get("last_stage2_verification") or "")
    verification = load_object(verification_path) if verification_path.is_file() else {}
    tester_contract = None
    if handoff.get("cycle_id") == state.get("cycle_id") and verification:
        try:
            tester_contract = stage2_tester.effective_contract(handoff, verification)
        except (OSError, ValueError, RuntimeError, KeyError, TypeError):
            tester_contract = None
    if tester_contract is None:
        approved_config = handoff.get("approved_tester_config")
        tester_config = approved_config if isinstance(approved_config, dict) else spec["tester_config"]
        include_value = handoff.get("canonical_minato") or handoff.get("canonical_minato_include")
        include_path = Path(include_value) if include_value else None
        include_hash = sha256(include_path) if include_path is not None and include_path.is_file() else None
        symbol = str(tester_config.get("symbol") or "ETHUSD")
        timeframes = tester_config.get("timeframes") or ["M5"]
        tester_contract = {
            "cycle_id": str(state.get("cycle_id") or ""), "ea": candidate_ea,
            "symbol": symbol, "timeframe": "M5" if "M5" in timeframes else str(timeframes[0]),
            "from_date": tester_config.get("from_date"), "to_date": tester_config.get("to_date"),
            "effective_spec": spec, "effective_config": tester_config,
            "spec_path": str(spec_path.resolve()), "spec_sha256": sha256(spec_path),
            "source_path": str(candidate_path.resolve()), "source_sha256": candidate_hash,
            "include_path": str(include_path.resolve()) if include_path and include_path.is_file() else "",
            "include_sha256": include_hash,
            "ex5_path": str(Path(verification.get("ex5") or "").resolve()) if verification.get("ex5") else "",
            "ex5_sha256": verification.get("ex5_sha256"),
            "expert_relative": f"Kiyosaki\\Stage2\\{state.get('cycle_id')}\\{candidate_ea}",
        }
    # These are the exact semantic Strategy Tester controls used by build_ini;
    # effective_config (inside the contract) binds all fixed input values.
    tester_config = tester_contract.get("effective_config") or {}
    tester_settings = {
        "optimization": 0, "model": 0, "visual": 0, "forward_mode": 0,
        "deposit": int(tester_config.get("deposit", 10000)),
        "currency": str(tester_config.get("currency", "USD")),
        "leverage": int(tester_config.get("leverage", 100)),
        "execution_mode": 0, "shutdown_terminal": 1,
    }
    data_identity = stage2_tester.history_identity(
        Path(stage2_tester.DATA_ROOT), str(tester_contract.get("symbol") or "ETHUSD"))
    signed_experiment = stage2_tester.experiment_signature(
        tester_contract, tester_settings, data_identity)
    failure_digest = sha256(failure_path)
    identity = {"cycle_id": str(state.get("cycle_id") or ""),
                "candidate_ea": candidate_ea,
                "candidate_source_sha256": candidate_hash,
                "failure_event_sha256": failure_digest,
                "stage1_spec_sha256": sha256(spec_path),
                "effective_tester_experiment_contract_sha256": effective_signature,
                "effective_input_signature": effective_signature,
                "experiment_signature": signed_experiment["signature"],
                "experiment_identity_sha256": stage2_tester.canonical_hash(signed_experiment["identity"])}
    identity["identity_sha256"] = hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest().upper()
    return identity


def stage2_source_generation_exhaustion_identity(state: dict, failure_path: Path) -> dict:
    """Authenticate a source-less research candidate's exhausted proposal record."""
    source_proposal_failure_kinds = {
        "STRATEGY_INPUT_LOCK", "INPUT_USAGE", "ORDER_REFUSAL_FLOW",
        "CLOSED_BAR_FLOW", "LIVE_MARGIN_FLOW", "SOURCE_ADMISSION",
        "RESPONSE_FORMAT", "EVIDENCE_BINDING", "RESPONSE_SCHEMA",
        "CANONICAL_HOOK", "IMPLEMENTATION_REFUSAL", "ORDER_MARGIN_NOTIONAL_HELPER",
        "NORMALIZATION_NOT_RECOGNIZED",
    }
    failure = load_object(failure_path)
    cycle_id = str(state.get("cycle_id") or "")
    ea = str(state.get("current_ea") or state.get("candidate_ea") or "")
    if (failure.get("cycle_id") != cycle_id or failure.get("stage") != 2
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != "MODEL_SOURCE_PROPOSAL_BUDGET_EXHAUSTED"
            or failure.get("rotation_eligible") is not True
            or failure.get("recovery_action") != "rotate_candidate"
            or failure.get("source") is not None or failure.get("source_sha256") is not None
            or failure.get("source_identity_available") is not False
            or failure.get("ea") != ea or failure.get("candidate_ea") != ea):
        raise ValueError("failure is not an authenticated source-less exhausted research candidate")
    current_failure_hash = sha256(failure_path)
    recorded_hash = str(state.get("stage2_failure_event_sha256") or "").upper()
    expected_name = f"{cycle_id}.stage2.failed.json"
    # On fresh-cycle failures the legacy pointer's digest can still refer to
    # the preceding cycle. In that case bind the canonical cycle-named event
    # directly and carry its current digest through the dispatch/consumption.
    if failure_path.name != expected_name or failure.get("event_id") != f"{cycle_id}:stage2:failed":
        raise ValueError("Stage 2 failure path or id is not canonical for the active cycle")
    spec_path = Path(str(failure.get("stage1_spec") or ""))
    spec_hash = str(failure.get("stage1_spec_sha256") or "").upper()
    if (not spec_path.is_file() or not spec_hash or sha256(spec_path) != spec_hash):
        raise ValueError("Stage 1 research spec is missing or changed")
    spec = load_object(spec_path)
    if (spec.get("cycle_id") != cycle_id or spec.get("stage") != 1
            or spec.get("ea") != ea
            or (spec.get("candidate_ea") is not None and spec.get("candidate_ea") != ea)):
        raise ValueError("Stage 1 research spec does not bind this failed candidate")
    diagnosis = failure.get("source_diagnosis") or {}
    hold_path = Path(str(diagnosis.get("artifact") or ""))
    hold_hash = str(diagnosis.get("sha256") or "").upper()
    if not hold_path.is_file() or not hold_hash or sha256(hold_path) != hold_hash:
        raise ValueError("source-generation exhaustion artifact is missing or changed")
    hold = load_object(hold_path)
    stage1_event_path = Path(str(state.get("last_stage1_event") or ""))
    recorded_stage1_event_hash = str(hold.get("stage1_event_sha256") or "").upper()
    if not stage1_event_path.is_file() or not recorded_stage1_event_hash:
        raise ValueError("Stage 1 completion event is missing from source-generation lineage")
    stage1_event = load_object(stage1_event_path)
    if (stage1_event.get("cycle_id") != cycle_id or stage1_event.get("stage") != 1
            or stage1_event.get("status") != "complete" or stage1_event.get("ea") != ea
            or Path(str(stage1_event.get("artifact") or "")).resolve() != spec_path.resolve()
            or str(stage1_event.get("artifact_sha256") or "").upper() != spec_hash
            or stage1_event.get("consumed") is not True
            or stage1_event.get("consumed_action") != "dispatch_stage2"):
        raise ValueError("Stage 1 completion event does not bind the failed candidate")
    # Match the executor's normalized digest: consumption metadata may change,
    # while every Stage 1 payload field remains independently authenticated.
    for key in ("consumed", "consumed_utc", "consumed_action"):
        stage1_event.pop(key, None)
    stage1_event_hash = hashlib.sha256(json.dumps(
        stage1_event, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")).hexdigest().upper()
    if (hold.get("cycle_id") != cycle_id or hold.get("ea") != ea
            or hold.get("status") != "MODEL_SOURCE_HOLD"
            or hold.get("phase") != "proposal"
            or hold.get("proposal_limit") != 4 or hold.get("proposal_attempts") != 4
            or hold.get("stop_reason") != "PROPOSAL_BUDGET_EXHAUSTED"
            or hold.get("source_spec_sha256") != spec_hash
            or recorded_stage1_event_hash != stage1_event_hash):
        raise ValueError("source-generation artifact does not prove an exhausted four-proposal episode")
    proposals = list(hold.get("proposal_diagnoses") or [])
    event_proposals = list(diagnosis.get("diagnoses") or [])
    proposal_source_hashes = []
    proposal_response_hashes = []
    proposal_rejection_hashes = []
    proposal_prompt_hashes = []
    generation_dir = hold_path.resolve().parent
    source_block_pattern = re.compile(r"```mql5\r?\n((?:(?!```).)*?)\r?\n```", re.S)
    for attempt_number, (proposal, event_proposal) in enumerate(zip(proposals, event_proposals), start=1):
        proposal_label = "proposal" if attempt_number == 1 else f"proposal-retry-{attempt_number}"
        rejection_path = generation_dir / f"proposal-rejection-{attempt_number}.json"
        raw_path = generation_dir / f"{proposal_label}.stdout.txt"
        prompt_label = "proposal" if attempt_number == 1 else f"proposal-retry-{attempt_number}"
        prompt_path = generation_dir / f"{prompt_label}.prompt.txt"
        if not rejection_path.is_file() or not raw_path.is_file() or not prompt_path.is_file():
            raise ValueError(f"proposal attempt {attempt_number} is missing its prompt, rejection, or raw response artifact")
        rejection = load_object(rejection_path)
        recorded_raw_path = Path(str(rejection.get("raw_proposal") or ""))
        if (rejection.get("attempt") != attempt_number or rejection.get("limit") != 4
                or rejection.get("diagnosis") != proposal
                or rejection.get("reason") != event_proposal.get("failed_check")
                or recorded_raw_path.resolve() != raw_path.resolve()
                or recorded_raw_path.resolve().parent != generation_dir):
            raise ValueError(f"proposal attempt {attempt_number} artifacts do not bind its recorded diagnosis")
        raw_response = raw_path.read_text(encoding="utf-8")
        if not raw_response.strip() or not prompt_path.read_bytes().strip():
            raise ValueError(f"proposal attempt {attempt_number} has an empty prompt or raw response")
        source_blocks = list(source_block_pattern.finditer(raw_response))
        recorded_source_hash = proposal.get("proposed_source_sha256")
        extracted_source_hash = None
        if recorded_source_hash is not None:
            if (not re.fullmatch(r"[A-Fa-f0-9]{64}", str(recorded_source_hash))
                    or len(source_blocks) != 1
                    or raw_response[:source_blocks[0].start()].strip()):
                raise ValueError(f"proposal attempt {attempt_number} has no unique, well-framed source body")
            extracted_source_hash = hashlib.sha256(
                source_blocks[0].group(1).encode("utf-8")).hexdigest().upper()
            if extracted_source_hash != str(recorded_source_hash).upper():
                raise ValueError(f"proposal attempt {attempt_number} source-body hash differs from its raw response")
        elif proposal.get("failure_kind") == "IMPLEMENTATION_REFUSAL":
            # A valid explicit refusal consumes one generator proposal slot,
            # but it does not contain a source and can never be admitted.
            if source_blocks:
                raise ValueError(f"proposal attempt {attempt_number} refusal unexpectedly contains source")
            raw_value = raw_response.strip()
            try:
                refusal = json.loads(raw_value)
            except json.JSONDecodeError:
                fenced = re.fullmatch(r"```json\r?\n([\s\S]*?)\r?\n```", raw_value)
                if fenced is None:
                    raise ValueError(f"proposal attempt {attempt_number} refusal response is not one JSON object")
                try:
                    refusal = json.loads(fenced.group(1).strip())
                except json.JSONDecodeError as exc:
                    raise ValueError(f"proposal attempt {attempt_number} refusal JSON is malformed") from exc
            if (not isinstance(refusal, dict) or set(refusal) != {"unsupported"}
                    or not isinstance(refusal.get("unsupported"), str)
                    or not refusal["unsupported"].strip()):
                raise ValueError(f"proposal attempt {attempt_number} refusal does not match the supported schema")
        elif proposal.get("failure_kind") != "RESPONSE_FORMAT":
            raise ValueError(f"proposal attempt {attempt_number} is missing a source hash")
        elif source_blocks and (len(source_blocks) != 1 or raw_response[:source_blocks[0].start()].strip()):
            raise ValueError(f"proposal attempt {attempt_number} response-format failure has ambiguous source")
        elif source_blocks:
            # Keep the hash only as evidence that this rejected response
            # attempted a source body. It remains unadmitted.
            extracted_source_hash = hashlib.sha256(
                source_blocks[0].group(1).encode("utf-8")).hexdigest().upper()
        proposal_source_hashes.append(extracted_source_hash)
        proposal_response_hashes.append(hashlib.sha256(raw_path.read_bytes()).hexdigest().upper())
        proposal_rejection_hashes.append(sha256(rejection_path).upper())
        proposal_prompt_hashes.append(sha256(prompt_path).upper())
    if (len(proposals) != 4 or len(event_proposals) != 4
            or [p.get("failure_fingerprint") for p in proposals]
            != [p.get("failure_fingerprint") for p in event_proposals]
            or [p.get("proposed_source_sha256") for p in proposals]
            != [p.get("proposed_source_sha256") for p in event_proposals]
            or any(type(p.get("same_failure_count")) is not int
                   or p.get("same_failure_count") < 1 for p in proposals)
            or any(p.get("failure_kind") not in source_proposal_failure_kinds
                   or p.get("repair_scope") != "source_proposal_only"
                   or (p.get("proposed_source_sha256") is None
                       and p.get("failure_kind") not in {"RESPONSE_FORMAT", "IMPLEMENTATION_REFUSAL"})
                   or (p.get("proposed_source_sha256") is not None
                       and not re.fullmatch(r"[A-Fa-f0-9]{64}", str(p.get("proposed_source_sha256"))))
                   for p in proposals)
            ):
        raise ValueError("proposal history does not authenticate four candidate-local attempts")
    previous_fingerprint = None
    consecutive_failure_count = 0
    for proposal in proposals:
        fingerprint = proposal.get("failure_fingerprint")
        consecutive_failure_count = (
            consecutive_failure_count + 1
            if fingerprint == previous_fingerprint else 1
        )
        if proposal.get("same_failure_count") != consecutive_failure_count:
            raise ValueError("proposal history repeat counts do not match failure fingerprints")
        if proposal.get("failure_kind") == "NORMALIZATION_NOT_RECOGNIZED":
            diagnostic = proposal.get("normalization_diagnosis")
            if not isinstance(diagnostic, dict):
                raise ValueError("normalization proposal is missing its authenticated diagnosis")
            if (diagnostic.get("code") != "NORMALIZATION_NOT_RECOGNIZED"
                    or diagnostic.get("side") not in {"BUY", "SELL"}
                    or not isinstance(diagnostic.get("volume_variable"), str)
                    or re.fullmatch(r"[A-Za-z_]\w*", diagnostic["volume_variable"]) is None
                    or type(diagnostic.get("source_line_start")) is not int
                    or type(diagnostic.get("source_line_end")) is not int
                    or diagnostic["source_line_start"] < 1
                    or diagnostic["source_line_end"] < diagnostic["source_line_start"]
                    or diagnostic["source_line_end"] - diagnostic["source_line_start"] > 1600
                    or not isinstance(diagnostic.get("quantity_to_margin_sha256"), str)
                    or re.fullmatch(r"[A-F0-9]{64}", diagnostic["quantity_to_margin_sha256"]) is None
                    or type(diagnostic.get("helper_authenticated")) is not bool
                    or type(diagnostic.get("normalizer_call_count")) is not int
                    or diagnostic["normalizer_call_count"] < 0
                    or type(diagnostic.get("matching_margin_guard_found")) is not bool
                    or diagnostic.get("span_kind") not in {
                        "quantity_to_margin_refusal", "quantity_to_order_fallback"}):
                raise ValueError("normalization proposal diagnosis has invalid fields")
            call_start, call_end = diagnostic.get("normalizer_line_start"), diagnostic.get("normalizer_line_end")
            if ((call_start is None) != (call_end is None)
                    or (call_start is not None and (
                        type(call_start) is not int or type(call_end) is not int
                        or call_start < 1 or call_end < call_start
                        or diagnostic.get("matching_margin_guard_found") is not True
                        or call_start < diagnostic["source_line_start"]))):
                raise ValueError("normalization proposal diagnosis has invalid normalizer line bounds")
            if diagnostic.get("matching_margin_guard_found") is True:
                if diagnostic.get("span_kind") != "quantity_to_margin_refusal":
                    raise ValueError("normalization proposal span does not match its margin guard")
            elif (diagnostic.get("span_kind") != "quantity_to_order_fallback"
                    or call_start is not None
                    or (diagnostic.get("helper_authenticated") is True
                        and diagnostic.get("normalizer_call_count") == 1)):
                raise ValueError("normalization proposal fallback evidence is inconsistent")
            if (diagnostic.get("matching_margin_guard_found") is True
                    and diagnostic.get("helper_authenticated") is True
                    and diagnostic.get("normalizer_call_count") == 1
                    and call_start is None):
                raise ValueError("normalization proposal is missing its authenticated normalizer call")
            if call_start is not None and (
                    diagnostic.get("helper_authenticated") is not True
                    or diagnostic.get("normalizer_call_count") != 1):
                raise ValueError("normalization proposal call bounds lack authenticated helper evidence")
            failed_check = proposal.get("failed_check")
            prefix = "NORMALIZATION_NOT_RECOGNIZED "
            if not isinstance(failed_check, str) or not failed_check.startswith(prefix):
                raise ValueError("normalization proposal failed_check is missing")
            try:
                if json.loads(failed_check[len(prefix):]) != diagnostic:
                    raise ValueError("normalization proposal failed_check differs from its diagnosis")
            except (json.JSONDecodeError, TypeError) as exc:
                raise ValueError("normalization proposal failed_check is invalid JSON") from exc
            expected_fingerprint = hashlib.sha256(
                ("NORMALIZATION_NOT_RECOGNIZED\n" + diagnostic["quantity_to_margin_sha256"]).encode("utf-8")
            ).hexdigest().upper()
            if fingerprint != expected_fingerprint:
                raise ValueError("normalization proposal fingerprint does not match its diagnosis")
            if consecutive_failure_count >= 2:
                raise ValueError("ordinary four-proposal episode repeats a normalization failure")
        previous_fingerprint = fingerprint
    error_hash = hashlib.sha256(str(failure.get("error") or "").encode("utf-8")).hexdigest().upper()
    if failure.get("error_sha256") != error_hash:
        raise ValueError("Stage 2 failure diagnostic hash is invalid")
    identity = {
        "cycle_id": cycle_id,
        "candidate_ea": ea,
        "failure_event_sha256": current_failure_hash,
        "stale_state_failure_event_sha256": recorded_hash if recorded_hash != current_failure_hash else None,
        "stage1_spec_sha256": spec_hash,
        "stage1_event_sha256": stage1_event_hash,
        "source_generation_recorded_stage1_event_sha256": recorded_stage1_event_hash,
        "source_generation_hold_sha256": hold_hash,
        "proposal_source_sha256": proposal_source_hashes,
        "proposal_response_sha256": proposal_response_hashes,
        "proposal_rejection_sha256": proposal_rejection_hashes,
        "proposal_prompt_sha256": proposal_prompt_hashes,
        "failure_fingerprint": proposals[0]["failure_fingerprint"],
        "proposal_limit": 4,
        "proposal_attempts": 4,
    }
    identity["identity_sha256"] = hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest().upper()
    return identity


def consume_event(path: Path, event_id: str, action: str, expected_sha256: str | None = None) -> None:
    event = load_object(path)
    if event.get("consumed") is True:
        if event.get("consumed_action") == action:
            return
        raise ValueError(f"event is already consumed: {event_id}")
    if expected_sha256 and sha256(path) != str(expected_sha256).upper():
        raise ValueError("event digest changed before one-shot consumption")
    if event.get("event_id", event_id) != event_id:
        raise ValueError("event identity changed before consumption")
    event.update({"consumed": True, "consumed_utc": utc_now(), "consumed_action": action})
    atomic_json(path, event)


def execution_ids(db_path: Path, job_id: str) -> set[str]:
    connection = sqlite3.connect(str(db_path))
    try:
        return {str(row[0]) for row in connection.execute("SELECT id FROM executions WHERE job_id = ?", (job_id,))}
    finally:
        connection.close()


def newest_unseen_execution(db_path: Path, job_id: str, before: set[str]) -> str | None:
    connection = sqlite3.connect(str(db_path))
    try:
        row = connection.execute(
            """SELECT id FROM executions WHERE job_id = ?
               ORDER BY COALESCE(started_at, claimed_at) DESC, id DESC LIMIT 1""",
            (job_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None or str(row[0]) in before:
        return None
    return str(row[0])


def execution_status(db_path: Path, execution_id: str) -> str:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT status FROM executions WHERE id = ?", (execution_id,)).fetchone()
        return row[0] if row else "unknown"
    finally:
        conn.close()


def execution_error(db_path: Path, execution_id: str) -> str:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT error FROM executions WHERE id = ?", (execution_id,)).fetchone()
        return str(row[0] or "") if row else ""
    finally:
        conn.close()


def is_provider_quota_failure(detail: str) -> bool:
    normalized = str(detail or "").casefold()
    has_429 = "http 429" in normalized or "status code 429" in normalized
    quota_signal = any(token in normalized for token in (
        "token plan", "usage limit", "rate limit", "rate-limit", "quota exceeded",
    ))
    return has_429 and quota_signal


def validate_stage1_provider_quota_recovery(state: dict) -> tuple[bool, str]:
    recovery = state.get("stage1_provider_quota_recovery")
    if not isinstance(recovery, dict):
        return False, "provider quota recovery ticket is missing"
    try:
        recovery_path = Path(str(recovery["path"]))
        recovery_event = load_object(recovery_path)
        failure_path = Path(str(state.get("stage1_failure_event") or ""))
        failure_event = load_object(failure_path)
        if sha256(recovery_path) != recovery.get("sha256"):
            return False, "provider quota recovery ticket hash changed"
        if (recovery_event.get("record_kind") != "kcos_stage1_provider_quota_recovery"
                or recovery_event.get("schema_version") != 1):
            return False, "provider quota recovery ticket type is invalid"
        if (recovery_event.get("cycle_id") != state.get("cycle_id")
                or recovery_event.get("status") != "provider_recovered_single_retry"
                or recovery_event.get("probe_result") != "OK"):
            return False, "provider quota recovery ticket identity or probe is invalid"
        if (failure_event.get("failure_kind") != "STAGE1_PROVIDER_QUOTA_FAILURE"
                or failure_event.get("rotation_eligible") is not False
                or recovery_event.get("failure_event_sha256") != sha256(failure_path)
                or recovery_event.get("failed_execution_id") != failure_event.get("hermes_execution_id")):
            return False, "provider quota recovery ticket does not bind the held failure"
        retry_ordinal = recovery_event.get("retry_ordinal")
        retry_count = state.get("stage1_provider_quota_retry_count", 0)
        if (isinstance(retry_ordinal, bool) or not isinstance(retry_ordinal, int)
                or retry_ordinal < 1 or int(retry_count) != retry_ordinal):
            return False, "provider quota retry ordinal is absent, out of sequence, or mismatched"
        return True, str(recovery_event.get("probe_utc") or "provider recovery probe passed")
    except (OSError, ValueError, KeyError, TypeError):
        return False, "provider quota recovery evidence is incomplete or unreadable"


def _owned_pid_values(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {"terminal_pid", "watcher_pid", "tester_pid", "worker_pids", "owned_pids"}:
                for item in (child if isinstance(child, list) else [child]):
                    try:
                        pid = int(item)
                        if pid > 0:
                            yield pid
                    except (TypeError, ValueError):
                        continue
            else:
                yield from _owned_pid_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from _owned_pid_values(child)


def _runtime_process_owner(pid: int, record: dict, role: str) -> bool | None:
    """Return owned/live, definitely unrelated/dead, or uncertain for one PID."""
    try:
        import psutil
        process = psutil.Process(pid)
        created = process.create_time()
        exe = str(process.exe()).casefold()
        cmdline = [str(value).casefold() for value in process.cmdline()]
    except Exception as exc:
        try:
            import psutil
            if not psutil.pid_exists(pid):
                return False
        except Exception:
            return None
        return None
    identity_records = []
    for key in ("worker_process_evidence", "terminal_process_evidence", "process_evidence"):
        value = record.get(key)
        if isinstance(value, dict):
            identity_records.append(value)
        elif isinstance(value, list):
            identity_records.extend(item for item in value if isinstance(item, dict))
    identity = next((item for item in identity_records if int(item.get("pid", -1) or -1) == pid), None)
    if identity and identity.get("create_time") is not None:
        if abs(created - float(identity["create_time"])) > 1.0:
            return False
    else:
        start_value = record.get("launch_epoch") or record.get("started_utc")
        end_value = (record.get("terminal_closed_utc") or record.get("completed_utc")
                     or record.get("updated_utc") or record.get("finished_utc"))
        try:
            start = float(start_value) if isinstance(start_value, (float, int)) else parse_utc(str(start_value)).timestamp()
            end = float(end_value) if isinstance(end_value, (float, int)) else parse_utc(str(end_value)).timestamp()
        except Exception:
            return None
        if created < start - 3 or created > end + 3:
            return False
    expected_exe = (identity.get("exe") if identity else None)
    expected_cmd = identity.get("command") if identity else None
    if not expected_exe:
        if role == "terminal":
            expected_exe = record.get("terminal_path") or record.get("terminal_exe")
            expected_cmd = expected_cmd or record.get("command") or record.get("launch_command")
        elif role == "worker" and record.get("terminal_path"):
            expected_exe = str(Path(record["terminal_path"]).with_name("metatester64.exe"))
    if expected_exe and Path(exe).name.casefold() != Path(str(expected_exe)).name.casefold():
        return False
    if expected_exe and role == "terminal" and Path(exe).resolve() != Path(str(expected_exe)).resolve():
        return False
    if isinstance(expected_cmd, list) and expected_cmd:
        normalized = [str(value).casefold() for value in expected_cmd]
        if not cmdline or cmdline[0] != normalized[0] or any(value not in cmdline for value in normalized[1:]):
            return False
    elif not expected_exe:
        return None
    return True


def owned_stage_processes(state: dict, state_dir: Path, *,
                          started_cycles: list[str] | None = None,
                          required_cycles: set[str] | None = None) -> tuple[bool, list[dict]]:
    """Check owned process records across receipt/current cycles and any older records."""
    active = []
    try:
        import psutil
        current_cycle = str(state.get("cycle_id") or "")
        cycles = list(dict.fromkeys([*(started_cycles or []), current_cycle]))
        required = set(required_cycles or ())
        current_stage = str(state.get("current_stage") or "").casefold()
        current_status = str(state.get("stage_status") or "").casefold()
        current_required_stages = set()
        if "stage3" in current_stage or "stage3" in current_status:
            current_required_stages.add("3")
        if "stage4" in current_stage or "stage4" in current_status:
            current_required_stages.add("3")
        if any(token in current_stage or token in current_status
               for token in ("stage45", "narrow", "stage5")):
            current_required_stages.update({"3", "4.5", "5"})

        def inspect(path: Path, cycle: str, stage: str, *, mandatory: bool,
                    run_number: int | None = None) -> tuple[bool, list[dict]]:
            if not path.is_file():
                return (not mandatory, [] if not mandatory else [{"stage": stage, "cycle_id": cycle,
                    "reason": f"missing required ownership runtime {path.name}"}])
            record = load_object(path)
            if record.get("cycle_id") != cycle or (run_number is not None and record.get("run_number") != run_number):
                return False, [{"stage": stage, "cycle_id": cycle, "reason": f"runtime identity mismatch: {path.name}"}]
            if stage == "5" and mandatory and record.get("status") != "PUBLISHED":
                return False, [{"stage": stage, "cycle_id": cycle, "reason": "Stage 5 runtime is not published"}]
            if stage == "4.5" and mandatory and (record.get("status") != "TIMEFRAME_COMPLETE"
                    or record.get("terminal_closed_after_run") is not True):
                return False, [{"stage": stage, "cycle_id": cycle, "reason": f"runtime is not settled: {path.name}"}]
            owned = []
            for pid in set(_owned_pid_values(record)):
                role = ("terminal" if pid == record.get("terminal_pid") else
                        "watcher" if pid == record.get("watcher_pid") else "worker")
                owner = _runtime_process_owner(pid, record, role)
                if owner is None:
                    return False, [{"stage": stage, "cycle_id": cycle, "pid": pid,
                                    "reason": f"cannot verify live PID identity from {path.name}"}]
                if owner:
                    owned.append({"stage": stage, "cycle_id": cycle, "pid": pid, "runtime": str(path)})
            return True, owned

        for cycle in cycles:
            mandatory_stages = {"3", "4.5", "5"} if cycle in required else (
                current_required_stages if cycle == current_cycle else set())
            stage3_path = state_dir / "stage3_v2" / f"{cycle}.stage3.runtime.json"
            ok, found = inspect(stage3_path, cycle, "3", mandatory="3" in mandatory_stages)
            if not ok:
                return False, found
            active.extend(found)
            for run_number in (1, 2, 3):
                runtime_path = state_dir / "stage45_v2" / f"{cycle}.stage45.run{run_number}.runtime.json"
                ok, found = inspect(runtime_path, cycle, "4.5", mandatory="4.5" in mandatory_stages,
                                    run_number=run_number)
                if not ok:
                    return False, found
                active.extend(found)
            stage5_runtime_path = state_dir / "stage5_v2" / f"{cycle}.stage5.runtime.json"
            ok, found = inspect(stage5_runtime_path, cycle, "5", mandatory="5" in mandatory_stages)
            if not ok:
                return False, found
            active.extend(found)
    except Exception as exc:
        return False, [{"reason": f"owned-process liveness is uncertain: {exc}"}]
    return not active, active


def process_goal_shutdown(control: dict, cap_path: Path, db_path: Path,
                          hermes_runner=None, *, state: dict | None = None,
                          state_dir: Path | None = None) -> dict:
    """Pause future stage schedules, wait for their runs, then pause supervisor."""
    cap = cycle_cap.load_cap(cap_path)
    if len(cap.get("healthy_receipts", [])) < cap["target_cycles"]:
        return {"status": "not_requested", "active_executions": []}
    if cap.get("shutdown_status") == "complete":
        return {"status": "complete", "active_executions": []}
    runner = hermes_runner or subprocess.run
    jobs = control.get("stage_jobs") or {}
    stage_job_ids = [jobs.get(stage) for stage in ("1", "2", "3", "4", "4.5", "5")]
    cap_status = cap.get("shutdown_status")
    if cap_status not in {"stage_jobs_paused_waiting_for_workers", "pause_supervisor_pending"}:
        errors = []
        stage_job_ids = []
        for stage in ("1", "2", "3", "4", "4.5", "5"):
            job_id = jobs.get(stage)
            if not isinstance(job_id, str) or not job_id:
                errors.append(f"Stage {stage} Hermes job is not configured")
                continue
            stage_job_ids.append(job_id)
            try:
                result = runner(["hermes", "cron", "pause", job_id],
                                capture_output=True, text=True, timeout=15, check=False)
            except Exception as exc:
                errors.append(f"Stage {stage} pause failed: {exc}")
                continue
            if result.returncode != 0:
                errors.append(f"Stage {stage} pause failed: {(result.stderr or result.stdout or 'unknown error').strip()}")
        if errors:
            cycle_cap.set_shutdown_status(cap_path, "pause_stage_jobs_pending", "; ".join(errors))
            return {"status": "pause_stage_jobs_pending", "errors": errors, "active_executions": []}
        cycle_cap.set_shutdown_status(cap_path, "stage_jobs_paused_waiting_for_workers",
                                      "future Stage 1–5/4.5 schedules paused")
    terminal_states = {"completed", "failed", "cancelled", "canceled", "timeout",
                       "timed_out", "error", "skipped"}
    connection = sqlite3.connect(str(db_path))
    try:
        active = []
        for job_id in stage_job_ids:
            rows = connection.execute(
                "SELECT id,status FROM executions WHERE job_id = ?", (job_id,)).fetchall()
            active.extend({"job_id": job_id, "execution_id": str(row[0]), "status": str(row[1])}
                          for row in rows if str(row[1]).casefold() not in terminal_states)
    finally:
        connection.close()
    if active:
        cycle_cap.set_shutdown_status(cap_path, "stage_jobs_paused_waiting_for_workers",
                                      f"waiting for {len(active)} active Stage 1–5/4.5 Hermes execution(s)")
        return {"status": "stage_jobs_paused_waiting_for_workers", "active_executions": active}
    if state is None:
        cycle_cap.set_shutdown_status(cap_path, "stage_jobs_paused_waiting_for_workers",
                                      "state is unavailable; owned-process liveness cannot be verified")
        return {"status": "stage_jobs_paused_waiting_for_workers", "active_executions": [],
                "liveness_verified": False, "detail": "current campaign state is unavailable"}
    if state is not None:
        receipt_cycles = {str(item["cycle_id"]) for item in cap.get("healthy_receipts", [])}
        started_cycles = [str(item["cycle_id"]) for item in cap.get("started_cycles", [])]
        known, owned_active = owned_stage_processes(
            state, state_dir or STATE.parent, started_cycles=started_cycles,
            required_cycles=receipt_cycles)
        if not known or owned_active:
            cycle_cap.set_shutdown_status(cap_path, "stage_jobs_paused_waiting_for_workers",
                                          "waiting for owned MT5/tester processes to be confirmed stopped")
            return {"status": "stage_jobs_paused_waiting_for_workers",
                    "active_executions": [], "owned_processes": owned_active,
                    "liveness_verified": known}
    supervisor_job = control.get("supervisor_job_id")
    if not isinstance(supervisor_job, str) or not supervisor_job:
        try:
            payload = json.loads(HERMES_JOBS.read_text(encoding="utf-8"))
            registered = payload.get("jobs", payload if isinstance(payload, list) else [])
            matches = [item for item in registered
                       if ("kcos_supervisor_v2.py" in str(item.get("script", "")).casefold()
                           or str(item.get("name", "")).casefold() == "kcos_supervisor_v2_zero_token")]
            if len(matches) == 1:
                supervisor_job = str(matches[0].get("id") or matches[0].get("job_id") or "")
        except (OSError, ValueError, TypeError):
            supervisor_job = None
    if not isinstance(supervisor_job, str) or not supervisor_job:
        cycle_cap.set_shutdown_status(cap_path, "pause_supervisor_pending",
                                      "all stage jobs are quiet, but supervisor_job_id is not configured")
        return {"status": "pause_supervisor_pending", "active_executions": [],
                "detail": "supervisor_job_id is not configured"}
    try:
        result = runner(["hermes", "cron", "pause", supervisor_job],
                        capture_output=True, text=True, timeout=15, check=False)
    except Exception as exc:
        cycle_cap.set_shutdown_status(cap_path, "pause_supervisor_pending", str(exc))
        return {"status": "pause_supervisor_pending", "active_executions": [], "detail": str(exc)}
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "unknown error").strip()
        cycle_cap.set_shutdown_status(cap_path, "pause_supervisor_pending", detail)
        return {"status": "pause_supervisor_pending", "active_executions": [], "detail": detail}
    cycle_cap.set_shutdown_status(cap_path, "complete", "all stage executions terminal; schedules paused")
    return {"status": "complete", "active_executions": []}


def preserve_goal_health_heartbeat(committed: dict, previous: dict, cap: dict) -> dict:
    """Keep terminal campaign progress visible after final state reevaluation."""
    merged = {**committed, **{key: value for key, value in previous.items()
                              if key in {"healthy_run_count", "healthy_run_receipt", "strategy_verdict",
                                         "result_verdict", "shutdown"}}}
    count = len(cap.get("healthy_receipts", []))
    merged["healthy_run_count"] = count
    merged["healthy_target_cycles"] = cap.get("target_cycles", 5)
    if count >= cap.get("target_cycles", 5):
        merged["healthy_target_reached"] = True
        merged["status"] = "GOAL_HEALTH_TARGET_REACHED"
        merged["action"] = "none"
        merged["detail"] = "five verified healthy Stage 5 terminal receipts are recorded"
        merged.setdefault("shutdown", {"status": cap.get("shutdown_status"),
                                       "detail": cap.get("shutdown_detail")})
    return merged


def stage2_recovery_execution_rows(state: dict, control: dict, db_path: Path,
                                   event: dict) -> tuple[str, list[tuple[str, str]]]:
    """Classify the accepted recovery execution without confusing routine job runs for it."""
    job_id = (control.get("stage_jobs") or {}).get("2")
    if not job_id:
        return "uncertain", []
    baseline = state.get("stage2_source_defect_execution_baseline")
    if (not isinstance(baseline, dict) or baseline.get("event_id") != event.get("event_id")
            or baseline.get("job_id") != job_id or not isinstance(baseline.get("execution_ids"), list)):
        return "uncertain", []
    core = {key: value for key, value in baseline.items() if key != "sha256"}
    if hashlib.sha256(json.dumps(core, sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper() != baseline.get("sha256"):
        return "uncertain", []
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            accepted_event_id = state.get("stage2_source_defect_last_dispatch_event_id")
            accepted_execution_id = str(state.get("stage2_source_defect_last_dispatch_execution_id") or "")
            if (accepted_event_id == event.get("event_id")
                    and state.get("stage2_source_defect_last_dispatch_accepted") is True
                    and accepted_execution_id):
                row = conn.execute(
                    "SELECT id, status FROM executions WHERE id = ? AND job_id = ?",
                    (accepted_execution_id, job_id),
                ).fetchone()
                if row is None:
                    # An accepted dispatch with no matching ledger row is
                    # unresolved. Do not infer completion or retry from other
                    # scheduled executions that happened after the baseline.
                    return "uncertain", []
                candidates = [(str(row[0]), str(row[1]).casefold())]
                statuses = {candidates[0][1]}
                if statuses & {"claimed", "running", "started", "queued"}:
                    return "active", candidates
                if statuses <= {"failed", "error", "cancelled", "canceled", "timed_out", "rejected"}:
                    return "terminal_failure", candidates
                if statuses <= {"completed", "complete", "succeeded", "success"}:
                    return "terminal_success", candidates
                return "uncertain", candidates
            rows = conn.execute("SELECT id, status FROM executions WHERE job_id = ?", (job_id,)).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return "uncertain", []
    baseline_ids = {str(item) for item in baseline["execution_ids"]}
    candidates = [(str(row[0]), str(row[1]).casefold()) for row in rows if str(row[0]) not in baseline_ids]
    if not candidates:
        return "none", []
    statuses = {status for _, status in candidates}
    if statuses & {"claimed", "running", "started", "queued"}:
        return "active", candidates
    failures = {"failed", "error", "cancelled", "canceled", "timed_out", "rejected"}
    successes = {"completed", "complete", "succeeded", "success"}
    if len(candidates) == 1 and statuses <= failures:
        return "terminal_failure", candidates
    if len(candidates) == 1 and statuses <= successes:
        return "terminal_success", candidates
    return "uncertain", candidates


def capture_stage2_recovery_execution_baseline(state_path: Path, control: dict,
                                               db_path: Path, event_id: str) -> dict:
    """Persist the existing job executions before consuming/dispatching an event."""
    job_id = (control.get("stage_jobs") or {}).get("2")
    if not job_id:
        raise ValueError("Stage 2 Hermes job ID is unavailable; refusing one-shot event consumption")
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            ids = sorted(str(row[0]) for row in conn.execute(
                "SELECT id FROM executions WHERE job_id = ?", (job_id,)).fetchall())
        finally:
            conn.close()
    except sqlite3.Error as exc:
        raise ValueError(f"Stage 2 Hermes execution baseline unavailable: {exc}") from exc
    baseline = {"event_id": event_id, "job_id": job_id, "execution_ids": ids, "captured_utc": utc_now()}
    baseline["sha256"] = hashlib.sha256(
        json.dumps(baseline, sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
    state = load_object(state_path)
    state["stage2_source_defect_execution_baseline"] = baseline
    atomic_json(state_path, state)
    return baseline


def active_stage2_recovery_execution(state: dict, control: dict, db_path: Path,
                                     event: dict) -> tuple[bool, str | None]:
    outcome, rows = stage2_recovery_execution_rows(state, control, db_path, event)
    if outcome in {"active", "uncertain"}:
        return True, rows[0][0] if rows else f"Stage 2 recovery execution state is {outcome}; refusing retry"
    return False, None


def stage2_recovery_dispatch_outcome(state: dict, control: dict, event: dict, db_path: Path) -> str:
    """Classify an accepted dispatch without treating all terminal rows alike."""
    if (state.get("stage2_source_defect_last_dispatch_event_id") != event.get("event_id")
            or state.get("stage2_source_defect_last_dispatch_accepted") is not True):
        return "not_accepted"
    outcome, _rows = stage2_recovery_execution_rows(state, control, db_path, event)
    return {"active": "active", "terminal_failure": "terminal_failure",
            "terminal_success": "terminal_success", "none": "awaiting"}.get(outcome, "awaiting")


def stage2_failure_execution_receipt(db_path: Path, job_id: str, execution_id: str,
                                    failure_path: Path, expected_error: str | None = None) -> dict | None:
    """Read and authenticate the exact terminal Hermes row and captured failure output."""
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(executions)")}
            output_column = next((name for name in ("output", "result", "output_json", "error") if name in columns), None)
            if output_column is None:
                return None
            column_order = [str(row[1]) for row in conn.execute("PRAGMA table_info(executions)")]
            row = conn.execute("SELECT * FROM executions WHERE id=? AND job_id=?",
                               (execution_id, job_id)).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        row_values = dict(zip(column_order, row))
        status = str(row_values.get("status") or "").casefold()
        if status not in {"failed", "error", "cancelled", "canceled", "timed_out", "rejected"}:
            return None
        output_value = row_values.get(output_column)
        if isinstance(output_value, bytes):
            output_bytes = output_value
            output = output_value.decode("utf-8", errors="strict")
        elif isinstance(output_value, str):
            output = output_value
            output_bytes = output_value.encode("utf-8")
        else:
            return None
        if output_column == "error":
            if not isinstance(output, str) or not output.startswith("Script exited with code 2\nstdout:\n"):
                return None
            output = output[len("Script exited with code 2\nstdout:\n"):].strip()
        payload = json.loads(output) if isinstance(output, str) else output
        if not (isinstance(payload, dict) and payload.get("stage") == 2
                and payload.get("published") is False
                and Path(str(payload.get("failure_event") or "")).resolve() == failure_path.resolve()
                and (expected_error is None or payload.get("error") == expected_error)):
            return None
        identity = {name: (value.hex() if isinstance(value, bytes) else value)
                    for name, value in row_values.items() if name != output_column}
        identity_bytes = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return {"id": str(row_values.get("id") or ""), "job_id": str(row_values.get("job_id") or ""),
                "status": status, "output_column": output_column,
                "output_sha256": hashlib.sha256(output_bytes).hexdigest().upper(),
                "output_size": len(output_bytes),
                "row_identity_sha256": hashlib.sha256(identity_bytes).hexdigest().upper()}
    except (OSError, sqlite3.Error, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
        return None


def _failure_attestation_path(state_path: Path, ticket: dict, event: dict, execution_id: str) -> Path:
    nonce = hashlib.sha256(f"{ticket['ticket_id']}|{event['event_id']}|{execution_id}".encode()).hexdigest()[:20]
    return state_path.parent / "diagnostics" / "stage2_source_defect_failure_attestations" / nonce / "attestation.json"


def validate_stage2_recovery_failure_attestation(state: dict, state_path: Path,
                                                 ticket: dict, ticket_path: Path,
                                                 event: dict, control: dict,
                                                 db_path: Path) -> dict:
    """Validate the seal, canonical references, artifact bytes, and live Hermes row output."""
    execution_id = str(state.get("stage2_source_defect_last_dispatch_execution_id") or "")
    expected_seal_path = _failure_attestation_path(state_path, ticket, event, execution_id)
    receipt_value = state.get("stage2_source_defect_failure_attestation")
    receipt_hash = str(state.get("stage2_source_defect_failure_attestation_sha256") or "").upper()
    if receipt_value:
        receipt_path = Path(receipt_value)
        if receipt_path.resolve() != expected_seal_path.resolve():
            raise ValueError("sealed Stage 2 failure attestation path is not deterministic")
        if not receipt_path.is_file() or sha256(receipt_path) != receipt_hash:
            raise ValueError("sealed Stage 2 failure attestation is missing or changed")
    else:
        # Reconcile a crash after O_EXCL sealing but before canonical state commit.
        receipt_path = expected_seal_path
        if not receipt_path.is_file():
            raise ValueError("sealed Stage 2 failure attestation is missing")
    seal = load_object(receipt_path)
    if (seal.get("schema_version") != 1 or seal.get("cycle_id") != state.get("cycle_id")
            or seal.get("ticket_id") != ticket.get("ticket_id")
            or seal.get("event_id") != event.get("event_id")
            or seal.get("execution_id") != execution_id
            or seal.get("job_id") != (control.get("stage_jobs") or {}).get("2")
            or seal.get("ticket_sha256") != sha256(ticket_path)):
        raise ValueError("sealed Stage 2 failure attestation identity changed")
    expected_failure = Path(state.get("stage2_failure_event") or state.get("stage2_invalid_event") or "")
    expected_verification = Path(state.get("last_stage2_verification") or "")
    expected_report = state_path.parent / "reports" / f"{state.get('cycle_id')}.stage2.repair-loop.json"
    expected_source = Path(state.get("candidate_source") or state.get("stage2_candidate_source") or "")
    records = seal.get("artifacts")
    required = {"failure_event", "verification", "repair_report", "candidate_source", "ticket"}
    if not isinstance(records, dict) or not required <= set(records):
        raise ValueError("sealed Stage 2 failure attestation is incomplete")
    expected_paths = {"failure_event": expected_failure, "verification": expected_verification,
                      "repair_report": expected_report, "candidate_source": expected_source,
                      "ticket": ticket_path}
    for name, item in records.items():
        path = Path(item.get("path") or "")
        if (name not in expected_paths or path.resolve() != expected_paths[name].resolve()
                or not path.is_file() or type(item.get("size")) is not int
                or path.stat().st_size != item["size"]
                or sha256(path) != str(item.get("sha256") or "").upper()):
            raise ValueError(f"sealed Stage 2 failure artifact changed: {name}")
    execution_record = stage2_failure_execution_receipt(
        db_path, str((control.get("stage_jobs") or {}).get("2") or ""), execution_id, expected_failure)
    if execution_record != seal.get("execution_row"):
        raise ValueError("sealed Hermes failure execution row/output changed")
    if not receipt_value:
        state["stage2_source_defect_failure_attestation"] = str(receipt_path.resolve())
        state["stage2_source_defect_failure_attestation_sha256"] = sha256(receipt_path)
        atomic_json(state_path, state)
    return seal


def seal_stage2_recovery_failure(state: dict, state_path: Path, control: dict,
                                 db_path: Path, ticket: dict, ticket_path: Path,
                                 event: dict, failure_path: Path,
                                 verification_path: Path, report_path: Path,
                                 source_path: Path) -> dict:
    """Bind a terminal Hermes failure and its DCC-only artifacts before hold clearance."""
    job_id = (control.get("stage_jobs") or {}).get("2")
    execution_id = str(state.get("stage2_source_defect_last_dispatch_execution_id") or "")
    outcome, rows = stage2_recovery_execution_rows(state, control, db_path, event)
    execution_record = stage2_failure_execution_receipt(db_path, str(job_id or ""), execution_id, failure_path)
    if (not job_id or not execution_id or state.get("stage2_source_defect_last_dispatch_event_id") != event.get("event_id")
            or outcome != "terminal_failure" or len(rows) != 1 or rows[0][0] != execution_id
            or execution_record is None):
        raise ValueError("Hermes terminal Stage 2 row/output does not authenticate this failed event")
    current_paths = {"failure_event": failure_path, "verification": verification_path,
                     "repair_report": report_path, "candidate_source": source_path,
                     "ticket": ticket_path}
    existing_path = state.get("stage2_source_defect_failure_attestation")
    if existing_path:
        return validate_stage2_recovery_failure_attestation(
            state, state_path, ticket, ticket_path, event, control, db_path)
    else:
        deterministic_path = _failure_attestation_path(state_path, ticket, event, execution_id)
        if deterministic_path.is_file():
            return validate_stage2_recovery_failure_attestation(
                state, state_path, ticket, ticket_path, event, control, db_path)
    records = {name: {"path": str(path.resolve()), "sha256": sha256(path),
                      "size": path.stat().st_size} for name, path in current_paths.items()}
    seal = {"schema_version": 1, "cycle_id": state["cycle_id"], "ticket_id": ticket["ticket_id"],
            "ticket_sha256": sha256(ticket_path), "event_id": event["event_id"],
            "execution_id": execution_id, "job_id": job_id, "artifacts": records,
            "execution_row": execution_record,
            "created_utc": utc_now()}
    seal_path = _failure_attestation_path(state_path, ticket, event, execution_id)
    directory = seal_path.parent
    directory.mkdir(parents=True, exist_ok=True)
    with seal_path.open("x", encoding="utf-8") as handle:
        json.dump(seal, handle, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    seal_path.chmod(0o444)
    state["stage2_source_defect_failure_attestation"] = str(seal_path.resolve())
    state["stage2_source_defect_failure_attestation_sha256"] = sha256(seal_path)
    # Commit the seal reference before any caller may clear a durable hold.
    atomic_json(state_path, state)
    return seal


def archive_failed_stage2_recovery_attempt(state_path: Path, state: dict, event: dict,
                                           execution_id: str) -> dict:
    """Freeze the failed attempt's mutable Stage 2 outputs before a retry event exists."""
    import kcos_stage2_source_defect_recovery_v1 as recovery
    failure_path = Path(state.get("stage2_failure_event") or state.get("stage2_invalid_event") or "")
    verification_path = Path(state.get("last_stage2_verification") or "")
    compile_path = Path(state.get("stage2_compile_receipt") or "")
    if not compile_path.is_file():
        compile_path = state_path.parent / "attempts" / f"{state.get('cycle_id')}.stage2.compile.latest.json"
    ticket = recovery.load(Path(state["stage2_source_defect_ticket"]))
    source_path = Path(state.get("candidate_source") or state.get("stage2_candidate_source")
                       or ticket.get("candidate_source") or "")
    handoff_path = Path(state.get("last_stage2_handoff") or
                        (ticket.get("artifacts", {}).get("stage2_handoff") or {}).get("path") or "")
    if not source_path.is_file() or not handoff_path.is_file():
        raise ValueError("failed Stage 2 attempt is missing its current source or handoff")
    handoff = recovery.load(handoff_path)
    ex5_path = Path(handoff.get("ex5") or state.get("candidate_ex5") or
                     (ticket.get("artifacts", {}).get("stage2_ex5") or {}).get("path") or "")
    build_path = Path(handoff.get("source_build_receipt") or
                      str(source_path.with_suffix(source_path.suffix + ".build.json")))
    adjacent_build_path = source_path.with_suffix(source_path.suffix + ".build.json")
    if not build_path.is_file() and adjacent_build_path.is_file():
        build_path = adjacent_build_path
    if build_path.resolve() != adjacent_build_path.resolve():
        raise ValueError("current source build receipt is not the source-adjacent .build.json")
    if not ex5_path.is_file() or not build_path.is_file():
        raise ValueError("failed Stage 2 attempt is missing its expected EX5 or source build receipt")
    build = recovery.load(build_path)
    admission_ref = build.get("implementation_admission") or build.get("implementation_review") or {}
    admission_path = Path(admission_ref.get("path") or "")
    if not admission_path.is_file():
        raise ValueError("failed Stage 2 attempt is missing the current source-admission proof")
    required = {"failure_event": failure_path, "verification": verification_path,
                "compile": compile_path, "handoff": handoff_path, "source": source_path,
                "ex5": ex5_path, "build_receipt": build_path, "admission": admission_path}
    if any(not path.is_file() for path in required.values()):
        raise ValueError("failed Stage 2 attempt is missing its event, verification, or compile receipt")
    failure = recovery.load(failure_path)
    verification = recovery.load(verification_path)
    compile_doc = recovery.load(compile_path)
    if (failure.get("cycle_id") != state.get("cycle_id") or failure.get("stage") != 2
            or failure.get("status") not in {"failed", "invalid"}
            or failure.get("rotation_eligible") is not False
            or verification.get("cycle_id") != state.get("cycle_id")
            or verification.get("stage") != 2
            or compile_doc.get("cycle_id") != state.get("cycle_id")
            or compile_doc.get("stage") != 2):
        raise ValueError("failed Stage 2 attempt artifacts do not authenticate this cycle")
    records = {}
    for name, path in required.items():
        records[name] = {"path": str(path.resolve()), "sha256": recovery.sha256(path), "size": path.stat().st_size}
    # Bind a fresh execution attestation if this failed attempt published a new
    # completion event; the original event/attestation already lives in ticket archive.
    current_event_path = Path(state.get("last_stage2_event") or "")
    original_event_hash = (ticket.get("artifacts", {}).get("stage2_event") or {}).get("sha256")
    if current_event_path.is_file() and recovery.sha256(current_event_path) != str(original_event_hash or "").upper():
        current_event_hash = recovery.sha256(current_event_path)
        attestation_path = stage2_execution_attestation_path(
            current_event_path, str(state.get("cycle_id")), current_event_hash)
        if not attestation_path.is_file():
            raise ValueError("failed Stage 2 attempt published a new completion without its execution attestation")
        required["execution_attestation"] = attestation_path
        records["execution_attestation"] = {"path": str(attestation_path.resolve()),
                                            "sha256": recovery.sha256(attestation_path),
                                            "size": attestation_path.stat().st_size}
    attempt = int(event.get("dispatch_attempt", 1))
    ticket = recovery.load(Path(state["stage2_source_defect_ticket"]))
    nonce = str(ticket["ticket_id"]).rsplit(":", 1)[-1]
    root = state_path.parent / "diagnostics" / "stage2_source_defect_attempts" / str(state["cycle_id"]) / nonce
    folder = root / f"attempt-{attempt:02d}-{hashlib.sha256(str(event['event_id']).encode()).hexdigest()[:10]}"
    if folder.exists():
        manifest_path = folder / "manifest.json"
        if not manifest_path.is_file():
            raise ValueError("partial prior-attempt archive exists without a manifest")
        receipt = {"path": str(manifest_path.resolve()), "sha256": recovery.sha256(manifest_path),
                   "event_id": event["event_id"], "execution_id": execution_id}
        recovery.validate_attempt_archive(receipt, ticket_id=ticket["ticket_id"],
                                          cycle_id=state["cycle_id"], event_id=event["event_id"],
                                          execution_id=execution_id)
        return receipt
    folder.mkdir(parents=True, exist_ok=False)
    copied = {}
    for name, bound in records.items():
        original = Path(bound["path"])
        dest = folder / f"{name}--{original.name}"
        with original.open("rb") as src, dest.open("xb") as dst:
            shutil.copyfileobj(src, dst)
            dst.flush()
            os.fsync(dst.fileno())
        if recovery.sha256(dest) != bound["sha256"] or dest.stat().st_size != bound["size"]:
            raise ValueError(f"failed Stage 2 attempt archive mismatch: {name}")
        dest.chmod(0o444)
        copied[name] = {"path": str(dest.resolve()), "sha256": bound["sha256"],
                        "size": bound["size"], "original_path": bound["path"]}
    manifest = {"schema_version": 1, "ticket_id": ticket["ticket_id"],
                "cycle_id": state["cycle_id"], "event_id": event["event_id"],
                "execution_id": execution_id, "files": copied, "created_utc": utc_now()}
    manifest_path = folder / "manifest.json"
    with manifest_path.open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    manifest_path.chmod(0o444)
    receipt = {"path": str(manifest_path.resolve()), "sha256": recovery.sha256(manifest_path),
               "event_id": event["event_id"], "execution_id": execution_id}
    recovery.validate_attempt_archive(receipt, ticket_id=ticket["ticket_id"],
                                      cycle_id=state["cycle_id"], event_id=event["event_id"],
                                      execution_id=execution_id)
    return receipt


def stage2_repair_proposal_budget_available(ledger_path: Path, *, cycle_id: str,
                                           candidate: str, source_sha256: str,
                                           failure_digest: str) -> bool:
    """Read the exact repair episode budget without creating or changing ledger state."""
    if not ledger_path.is_file():
        return False
    try:
        uri = "file:" + ledger_path.resolve().as_posix() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=1.0)
        try:
            rows = connection.execute(
                "SELECT identity_json, proposal_count, max_proposals FROM episodes").fetchall()
        finally:
            connection.close()
        for identity_json, proposal_count, max_proposals in rows:
            identity = json.loads(identity_json)
            if (identity.get("cycle_id") == cycle_id
                    and identity.get("candidate") == candidate
                    and str(identity.get("baseline_source_sha256") or "").upper() == source_sha256.upper()
                    and str(identity.get("failure_digest") or "").upper() == failure_digest.upper()):
                return (type(proposal_count) is int and type(max_proposals) is int
                        and max_proposals == stage2_repair_loop.MAX_PROPOSALS
                        and 0 <= proposal_count < max_proposals)
    except (OSError, sqlite3.Error, TypeError, ValueError, json.JSONDecodeError):
        return False
    return False


def authenticated_invalid_stops_rejected_proposal_evidence(state: dict, ticket: dict,
                                                           failure: dict, verification: dict,
                                                           report: dict, source_path: Path) -> bool:
    """Authenticate the rejected same-cycle invalid-stops proposal independently of budget."""
    source_hash = str((ticket.get("artifacts", {}).get("stage2_source") or {}).get("sha256") or "").upper()
    if (ticket.get("repair_kind") != "stage3_tester_journal_invalid_stops"
            or failure.get("cycle_id") != state.get("cycle_id")
            or failure.get("stage") != 2 or failure.get("ea") != state.get("current_ea")
            or failure.get("failure_kind") != "STAGE2_EXECUTOR_FAILURE"
            or failure.get("error") != "Stage 2 source-defect repair did not produce a changed candidate source"
            or failure.get("rotation_eligible") is not False
            or not source_hash or not source_path.is_file()
            or Path(failure.get("source") or "").resolve() != source_path.resolve()
            or str(failure.get("source_sha256") or "").upper() != source_hash
            or verification.get("cycle_id") != state.get("cycle_id")
            or verification.get("stage") != 2 or verification.get("ea") != state.get("current_ea")
            or verification.get("passed") is not True
            or Path(verification.get("source") or "").resolve() != source_path.resolve()
            or str(verification.get("source_sha256") or "").upper() != source_hash
            or report.get("schema_version") != 2
            or report.get("cycle_id") != state.get("cycle_id")
            or report.get("ea") != state.get("current_ea")):
        return False
    attempts = report.get("attempts") if isinstance(report.get("attempts"), list) else []
    baseline = next((item for item in attempts if item.get("baseline") is True), None)
    rejected = any(item.get("kind") == "stage3_authenticated_invalid_stops"
                   and item.get("ai", {}).get("status") == "PROPOSED"
                   and item.get("ai_application", {}).get("status") == "PATCH_REJECTED"
                   for item in attempts if isinstance(item, dict))
    if (not baseline or baseline.get("verification_passed") is not True
            or str(baseline.get("before_sha256") or "").upper() != source_hash
            or not rejected):
        return False
    return True


def authenticated_invalid_stops_rejected_proposal_retry(state: dict, ticket: dict,
                                                        failure: dict, verification: dict,
                                                        report: dict, source_path: Path,
                                                        ledger_path: Path) -> bool:
    """Accept only authenticated rejected-proposal evidence with budget left."""
    if not authenticated_invalid_stops_rejected_proposal_evidence(
            state, ticket, failure, verification, report, source_path):
        return False
    source_hash = str((ticket.get("artifacts", {}).get("stage2_source") or {}).get("sha256") or "").upper()
    ticket_digest = str(state.get("stage2_source_defect_ticket_sha256") or "").upper()
    return stage2_repair_proposal_budget_available(
        ledger_path, cycle_id=str(state.get("cycle_id") or ""),
        candidate=str(state.get("current_ea") or ""), source_sha256=source_hash,
        failure_digest=ticket_digest)


def authenticated_invalid_stops_proposal_budget_exhausted(state: dict, ticket: dict,
                                                           failure: dict, verification: dict,
                                                           report: dict, source_path: Path,
                                                           budget: dict) -> bool:
    """Recognize an authenticated Invalid-stops episode that used all three proposals."""
    source_hash = str((ticket.get("artifacts", {}).get("stage2_source") or {}).get("sha256") or "").upper()
    if (ticket.get("repair_kind") != "stage3_tester_journal_invalid_stops"
            or failure.get("cycle_id") != state.get("cycle_id")
            or failure.get("stage") != 2 or failure.get("ea") != state.get("current_ea")
            or failure.get("failure_kind") != "STAGE2_EXECUTOR_FAILURE"
            or failure.get("error") not in {
                "Stage 2 source-defect repair did not produce a changed candidate source",
                "Stage 2 recovery has no hash-bound supervisor dispatch claim",
            }
            or failure.get("rotation_eligible") is not False
            or not source_hash or not source_path.is_file()
            or Path(failure.get("source") or "").resolve() != source_path.resolve()
            or str(failure.get("source_sha256") or "").upper() != source_hash
            or verification.get("cycle_id") != state.get("cycle_id")
            or verification.get("stage") != 2 or verification.get("ea") != state.get("current_ea")
            or verification.get("passed") is not True
            or Path(verification.get("source") or "").resolve() != source_path.resolve()
            or str(verification.get("source_sha256") or "").upper() != source_hash
            or report.get("schema_version") != 2
            or report.get("cycle_id") != state.get("cycle_id")
            or report.get("ea") != state.get("current_ea")
            or budget.get("status") != "verified"
            or budget.get("used") != stage2_repair_loop.MAX_PROPOSALS
            or budget.get("limit") != stage2_repair_loop.MAX_PROPOSALS
            or budget.get("remaining") != 0):
        return False
    attempts = report.get("attempts") if isinstance(report.get("attempts"), list) else []
    baseline = next((item for item in attempts if item.get("baseline") is True), None)
    exhausted = any(item.get("ai", {}).get("status") == "EXHAUSTED"
                    for item in attempts if isinstance(item, dict))
    return bool(baseline and baseline.get("verification_passed") is True
                and str(baseline.get("before_sha256") or "").upper() == source_hash
                and exhausted)


def record_authenticated_stage2_proposal_exhaustion(state: dict, state_path: Path,
                                                     ticket: dict, failure: dict,
                                                     verification: dict, report: dict,
                                                     source_path: Path, budget: dict) -> bool:
    """Route only a fully authenticated exhausted repair episode to candidate rotation."""
    if not authenticated_invalid_stops_proposal_budget_exhausted(
            state, ticket, failure, verification, report, source_path, budget):
        return False
    state["stage2_source_defect_retry_exhausted"] = True
    state["stage2_source_defect_retry_exhaustion_reason"] = "authenticated_proposal_budget_exhausted"
    state.pop("stage2_source_defect_retry_terminal_hold", None)
    atomic_json(state_path, state)
    return True


def prepare_stage2_recovery_retry(state_path: Path, control: dict, db_path: Path,
                                  max_attempts: int = stage2_source_recovery.MAX_DISPATCH_ATTEMPTS) -> str:
    """Issue a new unique event only after proving the prior Hermes dispatch is inactive."""
    state = load_object(state_path)
    if state.get("stage_status") != "stage2_repair_required":
        return "not_recovery"
    event_path = Path(state.get("stage2_source_defect_event") or "")
    ticket_path = Path(state.get("stage2_source_defect_ticket") or "")
    if not event_path.is_file() or not ticket_path.is_file():
        return "missing_ticket"
    event = load_object(event_path)
    ticket = load_object(ticket_path)
    if event.get("consumed") is not True:
        try:
            stage2_source_recovery.validate_ticket(
                ticket_path, event_path, state, allow_unconsumed_for_dispatch=True)
        except (OSError, ValueError, KeyError, TypeError) as invalid_event_error:
            # Retry files are shared by historical cycles. A stale file can
            # make the numeric attempt counter skip an attempt, producing a
            # child event whose parent is not the authenticated failed event.
            # Reconcile only from the sealed first-attempt archive and the
            # exact failed Hermes execution; preserve the malformed child.
            if (event.get("cycle_id") != state.get("cycle_id")
                    or event.get("ticket_sha256") != sha256(ticket_path)
                    or event.get("consumed") is not False
                    or type(event.get("dispatch_attempt")) is not int
                    or not 2 <= event.get("dispatch_attempt") <= max_attempts
                    or event.get("parent_attempt_failed") is not True
                    or ticket.get("repair_kind") != "stage3_tester_journal_invalid_stops"):
                return "ready"
            original_path = Path(state.get("stage2_source_defect_original_event") or "")
            prior_archive = state.get("stage2_source_defect_prior_attempt_archive") or {}
            original = load_object(original_path) if original_path.is_file() else {}
            expected_parent_archive = bool(
                original.get("event_id") == ticket.get("ticket_id")
                and original.get("cycle_id") == state.get("cycle_id")
                and original.get("consumed") is True
                and original.get("consumed_action") == "dispatch_stage2"
                and state.get("stage2_source_defect_last_dispatch_event_id") == original.get("event_id")
                and state.get("stage2_source_defect_last_dispatch_accepted") is True
                and prior_archive.get("event_id") == original.get("event_id")
            )
            if not expected_parent_archive:
                state["stage2_source_defect_retry_hold"] = (
                    "malformed child retry event has no authenticated failed parent archive")
                atomic_json(state_path, state)
                return "uncertain"
            try:
                stage2_source_recovery.validate_attempt_archive(
                    prior_archive, ticket_id=ticket["ticket_id"], cycle_id=ticket["cycle_id"],
                    event_id=str(original["event_id"]), execution_id=prior_archive.get("execution_id"))
            except (OSError, ValueError, KeyError, TypeError) as archive_error:
                state["stage2_source_defect_retry_hold"] = (
                    f"failed parent archive cannot authenticate child-event repair: {archive_error}")
                atomic_json(state_path, state)
                return "uncertain"
            original_outcome, original_rows = stage2_recovery_execution_rows(
                state, control, db_path, original)
            parent_execution_id = str(prior_archive.get("execution_id") or "")
            failure_path = Path(state.get("stage2_failure_event") or state.get("stage2_invalid_event") or "")
            job_id = str((control.get("stage_jobs") or {}).get("2") or "")
            parent_failure_receipt = (stage2_failure_execution_receipt(
                db_path, job_id, parent_execution_id, failure_path,
                expected_error="canonical state is outside the same-cycle Stage 2 repair hold")
                if job_id and parent_execution_id and failure_path.is_file() else None)
            observed_ids = {str(row[0]) for row in original_rows}
            unrelated_rows_are_admission_rejections = all(
                row[1] in {"failed", "error", "cancelled", "canceled", "timed_out", "rejected"}
                and stage2_failure_execution_receipt(
                    db_path, job_id, str(row[0]), failure_path,
                    expected_error="same-cycle Stage 2 recovery ticket/event is invalid, replayed, or unconsumed")
                is not None
                for row in original_rows if str(row[0]) != parent_execution_id
            )
            if (parent_execution_id not in observed_ids or parent_failure_receipt is None
                    or not unrelated_rows_are_admission_rejections):
                state["stage2_source_defect_retry_hold"] = (
                    f"failed parent execution is not uniquely authenticated ({original_outcome}); refusing child-event repair")
                atomic_json(state_path, state)
                return "active"
            used_attempts = {1}
            ticket_hash = sha256(ticket_path)
            attempts_dir = original_path.parent / "stage2_source_defect_dispatch"
            for candidate_path in attempts_dir.glob("*.json") if attempts_dir.exists() else []:
                candidate = load_object(candidate_path)
                attempt_value = candidate.get("dispatch_attempt")
                if (candidate.get("ticket_sha256") == ticket_hash
                        and candidate.get("cycle_id") == ticket.get("cycle_id")
                        and type(attempt_value) is int and 2 <= attempt_value <= max_attempts
                        and candidate.get("event_id") == f"{ticket['ticket_id']}:dispatch:{attempt_value}"
                        and candidate.get("parent_event_id") == (
                            ticket["ticket_id"] if attempt_value == 2
                            else f"{ticket['ticket_id']}:dispatch:{attempt_value - 1}")):
                    used_attempts.add(attempt_value)
            next_attempt = max(used_attempts) + 1
            if next_attempt > max_attempts:
                state["stage2_source_defect_retry_exhausted"] = True
                state["stage2_source_defect_retry_attempts"] = max_attempts
                atomic_json(state_path, state)
                return "exhausted"
            attempts_dir.mkdir(parents=True, exist_ok=True)
            nonce = hashlib.sha256(ticket_hash.encode("ascii")).hexdigest()[:10].lower()
            retry_path = attempts_dir / f"attempt-{next_attempt:02d}-{nonce}.json"
            retry_event = {
                "schema_version": 1,
                "event_id": f"{ticket['ticket_id']}:dispatch:{next_attempt}",
                "cycle_id": ticket["cycle_id"], "stage": 2,
                "status": "repair_requested", "consumed": False,
                "rotation_eligible": False, "ticket": str(ticket_path.resolve()),
                "ticket_sha256": ticket_hash, "dispatch_attempt": next_attempt,
                "parent_event_id": original["event_id"], "parent_attempt_failed": True,
                "created_utc": utc_now(),
            }
            try:
                with retry_path.open("x", encoding="utf-8") as handle:
                    json.dump(retry_event, handle, indent=2)
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            except FileExistsError:
                existing_retry = load_object(retry_path)
                if existing_retry != retry_event:
                    raise ValueError("reconciled retry event path contains different evidence")
            state.update({
                "stage2_source_defect_invalid_retry_event": str(event_path.resolve()),
                "stage2_source_defect_invalid_retry_event_sha256": sha256(event_path),
                "stage2_source_defect_retry_hold": None,
                "stage2_source_defect_event": str(retry_path.resolve()),
                "stage2_source_defect_event_id": retry_event["event_id"],
                "stage2_source_defect_event_sha256": sha256(retry_path),
                "stage2_source_defect_consumption_intent": None,
                "stage2_source_defect_consumed_receipt": None,
                "stage2_source_defect_retry_attempts": next_attempt,
                "next_action": "dispatch_stage2", "updated_utc": utc_now(),
            })
            state.pop("stage2_source_defect_retry_exhausted", None)
            state.pop("stage2_source_defect_retry_terminal_hold", None)
            atomic_json(state_path, state)
            return "retry_reconciled"
        return "ready"
    execution_outcome, candidate_rows = stage2_recovery_execution_rows(state, control, db_path, event)
    active = execution_outcome in {"active", "uncertain"}
    detail = candidate_rows[0][0] if candidate_rows else f"Stage 2 recovery execution state is {execution_outcome}; refusing retry"
    if active:
        state["stage2_source_defect_retry_hold"] = detail or "Stage 2 Hermes execution is active"
        atomic_json(state_path, state)
        return "active"
    retry_after_terminal_failure = False
    last_id = state.get("stage2_source_defect_last_dispatch_event_id")
    if last_id == event.get("event_id") and state.get("stage2_source_defect_last_dispatch_accepted") is True:
        outcome = stage2_recovery_dispatch_outcome(state, control, event, db_path)
        if outcome == "terminal_failure":
            retry_after_terminal_failure = True
        elif outcome == "terminal_success":
            state["stage2_source_defect_success_without_fresh_publication"] = True
            state["stage2_source_defect_retry_hold"] = "Hermes execution completed successfully without fresh Stage 2 publication; refusing duplicate dispatch"
            atomic_json(state_path, state)
            return "terminal_success"
        else:
            return "awaiting_completion"
    if last_id != event.get("event_id"):
        # The dispatch result may have been lost after Hermes registered the
        # execution. Reconcile every post-baseline row before considering a
        # child event: a completed row is never duplicated, and a failed row
        # must have its mutable attempt outputs archived first.
        if execution_outcome == "terminal_success":
            ticket = load_object(ticket_path)
            original_stage2_event = (ticket.get("artifacts", {}).get("stage2_event") or {}).get("sha256")
            published_event_path = Path(state.get("last_stage2_event") or "")
            fresh_publication = (published_event_path.is_file()
                                 and sha256(published_event_path) != str(original_stage2_event or "").upper())
            if fresh_publication:
                valid, detail = validate_stage2_completion(state, control, db_path)
                if valid:
                    state.update({"current_stage": "stage3_pending", "stage_status": "stage3_pending",
                                  "next_action": "start_stage3", "stage2_source_defect_recovered_publication_verified": True,
                                  "stage2_source_defect_recovered_publication_detail": detail,
                                  "stage2_source_defect_retry_hold": None,
                                  "updated_utc": utc_now()})
                    atomic_json(state_path, state)
                    return "fresh_completion"
            state["stage2_source_defect_success_without_fresh_publication"] = True
            state["stage2_source_defect_retry_hold"] = (
                "Hermes completed Stage 2 during supervisor crash window without verified fresh publication; refusing duplicate dispatch")
            atomic_json(state_path, state)
            return "terminal_success"
        if execution_outcome == "terminal_failure":
            retry_after_terminal_failure = True
        elif execution_outcome == "none":
            # Covers no-row process death after event consumption. The grace
            # clock comes from canonical pre-consumption intent, never from
            # the mutable consumed_utc field in the event itself.
            try:
                intent = state.get("stage2_source_defect_consumption_intent") or {}
                if intent.get("event_id") != event.get("event_id"):
                    raise ValueError("no canonical consumption intent for this event")
                consumed_at = parse_utc(str(intent.get("started_utc", "")))
            except Exception:
                state["stage2_source_defect_retry_hold"] = (
                    "consumed event has no trusted canonical dispatch-intent timestamp; refusing retry")
                atomic_json(state_path, state)
                return "uncertain"
            if datetime.now(timezone.utc) - consumed_at < timedelta(seconds=120):
                return "grace"
            retry_after_terminal_failure = False
        else:
            state["stage2_source_defect_retry_hold"] = (
                f"Stage 2 recovery execution state is {execution_outcome}; refusing retry")
            atomic_json(state_path, state)
            return "active"
    prior_terminal_hold = state.get("stage2_source_defect_retry_terminal_hold")
    nonce = str(ticket.get("ticket_id", "")).rsplit(":", 1)[-1]
    archive_dir = state_path.parent / "diagnostics" / "stage2_source_defect_rebuild" / str(ticket.get("cycle_id")) / nonce
    try:
        if state.get("stage2_source_defect_archive"):
            state["stage2_source_defect_archive"] = stage2_source_recovery.extend_archive_for_missing_ticket_artifacts(
                ticket, ticket_path, state["stage2_source_defect_archive"])
        elif archive_dir.exists():
            receipt = stage2_source_recovery.recover_archive_receipt(
                ticket, ticket_path, archive_dir, allow_missing_ticket_artifacts=True)
            state["stage2_source_defect_archive"] = stage2_source_recovery.extend_archive_for_missing_ticket_artifacts(
                ticket, ticket_path, receipt)
        stage2_source_recovery.validate_ticket(
            ticket_path, event_path, state, allow_consumed_digest_reconcile=True)
        if retry_after_terminal_failure or prior_terminal_hold:
            failure_path = Path(state.get("stage2_failure_event") or state.get("stage2_invalid_event") or "")
            failure = load_object(failure_path)
            verification_path = Path(state.get("last_stage2_verification") or "")
            verification = load_object(verification_path)
            verification_checks = verification.get("checks")
            failed_checks = {name for name, value in (verification_checks or {}).items() if value is False}
            failure_verification = Path(failure.get("verification") or "")
            failure_verification_hash = failure.get("verification_sha256")
            source_path = Path(state.get("candidate_source") or verification.get("source") or "")
            bound_source = (ticket.get("artifacts", {}).get("stage2_source") or {})
            source_hash = str(bound_source.get("sha256") or "").upper()
            source_identity_ok = (source_path.is_file()
                                  and Path(verification.get("source") or "").resolve() == source_path.resolve()
                                  and str(verification.get("source_sha256") or "").upper() == source_hash
                                  and str(bound_source.get("path") or "") == str(source_path.resolve()))
            expected_report = state_path.parent / "reports" / f"{state.get('cycle_id')}.stage2.repair-loop.json"
            report_pointer = state.get("last_stage2_repair_report")
            report = {}
            baseline_checks = None
            baseline_failed = set()
            deterministic_record = {}
            ai_application = {}
            report_ok = False
            if expected_report.is_file() and (not report_pointer or Path(report_pointer).resolve() == expected_report.resolve()):
                report = load_object(expected_report)
                report_attempts = report.get("attempts") or []
                baseline = next((item for item in report_attempts if item.get("baseline") is True), None)
                deterministic = next((item for item in report_attempts
                                      if item.get("attempt") == 1), None)
                baseline_failed = {name for name, value in (baseline or {}).get("checks", {}).items()
                                   if value is False}
                baseline_checks = (baseline or {}).get("checks")
                deterministic_record = (deterministic or {}).get("deterministic") or {}
                ai_application = (deterministic or {}).get("ai_application") or {}
            report_ok = bool(
                    report.get("schema_version") == 2
                    and report.get("cycle_id") == state.get("cycle_id")
                    and report.get("ea") == state.get("current_ea")
                    and isinstance(baseline_checks, dict)
                    and set(baseline_checks) == STAGE2_RECOVERY_REQUIRED_CHECK_KEYS
                    and all(type(value) is bool for value in baseline_checks.values())
                    and baseline_failed == {"dcc_closed_return_arrays_bounded"}
                    and deterministic_record.get("changed") is False
                    and ai_application.get("status") == "PATCH_REJECTED"
                    and str(deterministic.get("before_sha256") or "").upper() == source_hash
                    and str(deterministic.get("after_sha256") or "").upper() == source_hash)
            invalid_stops_pre_repair_error = str(failure.get("error") or "")
            invalid_stops_pre_repair_trace = str(failure.get("traceback") or "")
            invalid_stops_pre_repair_retry = bool(
                ticket.get("repair_kind") == "stage3_tester_journal_invalid_stops"
                and failure.get("cycle_id") == state.get("cycle_id")
                and failure.get("stage") == 2
                and failure.get("ea") == state.get("current_ea")
                and failure.get("failure_kind") == "STAGE2_EXECUTOR_FAILURE"
                and failure.get("rotation_eligible") is False
                and (
                    invalid_stops_pre_repair_error == "canonical state is outside the same-cycle Stage 2 repair hold"
                    and "archive_stage3_dcc_source_repair" in invalid_stops_pre_repair_trace
                    or invalid_stops_pre_repair_error == "Stage 3 zero-trade repair evidence is missing"
                    and "authenticated_stage3_zero_trade_repair" in invalid_stops_pre_repair_trace
                )
                and failure.get("source")
                and Path(failure.get("source")).resolve() == source_path.resolve()
                and str(failure.get("source_sha256") or "").upper() == source_hash
                and verification.get("cycle_id") == state.get("cycle_id")
                and verification.get("stage") == 2
                and verification.get("ea") == state.get("current_ea")
                and verification.get("passed") is True
                and Path(verification.get("source") or "").resolve() == source_path.resolve()
                and str(verification.get("source_sha256") or "").upper() == source_hash
            )
            array_bounds_pre_repair_retry = bool(
                ticket.get("repair_kind") in {
                    "stage3_tester_journal_multidimensional_array_row_bounds",
                    "stage3_tester_journal_quantile_interpolation_bounds",
                    "stage3_tester_journal_ea_runtime_array_bounds",
                }
                and failure.get("cycle_id") == state.get("cycle_id")
                and failure.get("stage") == 2
                and failure.get("ea") == state.get("current_ea")
                and failure.get("failure_kind") == "STAGE2_EXECUTOR_FAILURE"
                and failure.get("rotation_eligible") is False
                and any(marker in str(failure.get("error") or "") for marker in (
                    "cycle state has no canonical Stage 2 failure event",
                    "current Stage 3 source-defect ticket does not bind this Stage 2 handoff",
                    "Stage 2 source-defect repair did not produce a changed candidate source",
                    "Stage 3 runtime-repair evidence is incomplete, mismatched, or changed",
                    "Stage 2 recovery has no hash-bound supervisor dispatch claim",
                    "Stage 3 order-rejection evidence no longer matches its immutable recovery ticket",
                ))
                and failure.get("source")
                and Path(failure.get("source")).resolve() == source_path.resolve()
                and str(failure.get("source_sha256") or "").upper() == source_hash
                and source_hash == str(ticket.get("parent_source_sha256") or "").upper()
                and verification.get("cycle_id") == state.get("cycle_id")
                and verification.get("stage") == 2
                and verification.get("ea") == state.get("current_ea")
                and verification.get("passed") is True
                and Path(verification.get("source") or "").resolve() == source_path.resolve()
                and str(verification.get("source_sha256") or "").upper() == source_hash
            )
            invalid_stops_rejected_proposal_retry = False
            invalid_stops_rejected_proposal_evidence = False
            invalid_stops_proposal_budget_exhausted = False
            invalid_stops_proposal_budget = {}
            if (ticket.get("repair_kind") == "stage3_tester_journal_invalid_stops"
                    and expected_report.is_file()
                    and (not report_pointer or Path(report_pointer).resolve() == expected_report.resolve())
                    and failure_verification.resolve() == verification_path.resolve()
                    and (failure_verification_hash is None
                         or str(failure_verification_hash).upper() == sha256(verification_path))):
                try:
                    repair_report = load_object(expected_report)
                    invalid_stops_rejected_proposal_evidence = authenticated_invalid_stops_rejected_proposal_evidence(
                        state, ticket, failure, verification, repair_report, source_path)
                    invalid_stops_proposal_budget = stage2_proposal_budget_status(state, state_path)
                    invalid_stops_proposal_budget_exhausted = authenticated_invalid_stops_proposal_budget_exhausted(
                        state, ticket, failure, verification, repair_report, source_path,
                        invalid_stops_proposal_budget)
                    invalid_stops_rejected_proposal_retry = authenticated_invalid_stops_rejected_proposal_retry(
                        state, ticket, failure, verification, repair_report, source_path,
                        state_path.parent / "ledgers" / "stage2_repair_episodes.sqlite3")
                except (OSError, ValueError, KeyError, TypeError, sqlite3.Error):
                    invalid_stops_rejected_proposal_retry = False
                    invalid_stops_proposal_budget_exhausted = False
                    invalid_stops_proposal_budget = {}
            invalid_stops_pre_repair_retry = (invalid_stops_pre_repair_retry
                                               or invalid_stops_rejected_proposal_retry)
            if invalid_stops_proposal_budget_exhausted:
                if not record_authenticated_stage2_proposal_exhaustion(
                        state, state_path, ticket, failure, verification, repair_report,
                        source_path, invalid_stops_proposal_budget):
                    raise ValueError(
                        "authenticated Stage 3 Invalid-stops repair budget proof changed before exhaustion was recorded")
                return "exhausted"
            if (not invalid_stops_pre_repair_retry and invalid_stops_rejected_proposal_evidence):
                budget = stage2_proposal_budget_status(state, state_path)
                if budget.get("status") == "verified" and budget.get("remaining") == 0:
                    raise ValueError(
                        "authenticated Stage 3 Invalid-stops repair exhausted the persistent proposal budget "
                        f"({budget.get('used')}/{budget.get('limit')}); no further proposal is authorized")
            source_defect_pre_repair_retry = (invalid_stops_pre_repair_retry
                                              or array_bounds_pre_repair_retry)
            if (not source_defect_pre_repair_retry and (
                    failure.get("cycle_id") != state.get("cycle_id") or failure.get("stage") != 2
                    or failure.get("ea") != state.get("current_ea")
                    or failure.get("failure_kind") != "STAGE2_VERIFICATION_FAILURE"
                    or failure.get("rotation_eligible") is not False
                    or verification.get("cycle_id") != state.get("cycle_id")
                    or verification.get("stage") != 2
                    or verification.get("passed") is not False
                    or not isinstance(verification_checks, dict)
                    or set(verification_checks) != STAGE2_RECOVERY_REQUIRED_CHECK_KEYS
                    or not all(type(value) is bool for value in verification_checks.values())
                    or failure_verification.resolve() != verification_path.resolve()
                    or (failure_verification_hash is not None
                        and str(failure_verification_hash).upper() != sha256(verification_path))
                    or failure.get("failed_checks") != ["dcc_closed_return_arrays_bounded"]
                    or failed_checks != {"dcc_closed_return_arrays_bounded"})):
                raise ValueError("current Stage 2 failure is not the authenticated DCC-only source defect")
            if (not source_identity_ok or (not source_defect_pre_repair_retry and not report_ok)):
                raise ValueError("DCC-only verifier and repair-report provenance do not bind the ticket-authenticated candidate source")
            execution_id = candidate_rows[0][0] if candidate_rows else ""
            if not source_defect_pre_repair_retry:
                seal_stage2_recovery_failure(
                    state, state_path, control, db_path, ticket, ticket_path, event,
                    failure_path, verification_path, expected_report, source_path)
            attempt_archive = archive_failed_stage2_recovery_attempt(
                state_path, state, event, execution_id)
            state["stage2_source_defect_prior_attempt_archive"] = attempt_archive
        # A previous terminal hold can be cleared only after the same-cycle,
        # no-active-execution, original-archive, and authenticated source-defect failure proofs above.
        state["stage2_source_defect_event_sha256"] = sha256(event_path)
        state.pop("stage2_source_defect_retry_terminal_hold", None)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        state["stage2_source_defect_retry_terminal_hold"] = (
            f"hash-bound pre-rebuild Stage 2 evidence changed before retry: {exc}")
        atomic_json(state_path, state)
        return "terminal_hold"
    original_event = Path(state.get("stage2_source_defect_original_event") or event_path)
    attempts_dir = (event_path.parent if event_path.parent.name == "stage2_source_defect_dispatch"
                    else original_event.parent / "stage2_source_defect_dispatch")
    attempts_dir.mkdir(parents=True, exist_ok=True)
    attempt_files = list(attempts_dir.glob("*.json"))
    ticket_hash = sha256(ticket_path)
    # If the process died after creating a retry event but before publishing
    # its path in canonical state, adopt that exact unused child event.
    for candidate_path in sorted(attempt_files):
        candidate = load_object(candidate_path)
        candidate_attempt = candidate.get("dispatch_attempt")
        if (type(candidate_attempt) is int and 2 <= candidate_attempt <= max_attempts
                and candidate.get("ticket_sha256") == ticket_hash
                and candidate.get("cycle_id") == ticket.get("cycle_id")
                and candidate.get("parent_event_id") == event.get("event_id")
                and candidate.get("event_id") == f"{ticket.get('ticket_id')}:dispatch:{candidate_attempt}"
                and candidate.get("consumed") is False):
            state.update({"stage2_source_defect_original_event": state.get("stage2_source_defect_original_event") or str(event_path.resolve()),
                          "stage2_source_defect_event": str(candidate_path.resolve()),
                          "stage2_source_defect_event_id": candidate["event_id"],
                          "stage2_source_defect_event_sha256": sha256(candidate_path),
                          "stage2_source_defect_consumption_intent": None,
                          "stage2_source_defect_consumed_receipt": None,
                          "stage2_source_defect_retry_attempts": candidate_attempt,
                          "stage2_source_defect_retry_hold": None,
                          "next_action": "dispatch_stage2", "updated_utc": utc_now()})
            atomic_json(state_path, state)
            return "retry_reconciled"
    matching_attempts = [item for item in [event, *(load_object(p) for p in attempt_files)]
                         if item.get("ticket_sha256") == ticket_hash
                         and item.get("cycle_id") == ticket.get("cycle_id")
                         and type(item.get("dispatch_attempt")) is int
                         and 1 <= item.get("dispatch_attempt") <= max_attempts
                         and item.get("event_id") == (
                             ticket.get("ticket_id") if item.get("dispatch_attempt") == 1
                             else f"{ticket.get('ticket_id')}:dispatch:{item.get('dispatch_attempt')}")
                         and (item.get("dispatch_attempt") == 1
                              or item.get("parent_event_id") == (
                                  ticket.get("ticket_id") if item.get("dispatch_attempt") == 2
                                  else f"{ticket.get('ticket_id')}:dispatch:{item.get('dispatch_attempt') - 1}"))]
    attempt_number = max([int(item.get("dispatch_attempt", 1))
                          for item in matching_attempts
                          if str(item.get("dispatch_attempt", 1)).isdigit()] or [1]) + 1
    if attempt_number > max_attempts:
        state["stage2_source_defect_retry_exhausted"] = True
        state["stage2_source_defect_retry_attempts"] = max_attempts
        atomic_json(state_path, state)
        return "exhausted"
    attempt_id = f"{ticket['ticket_id']}:dispatch:{attempt_number}"
    nonce = hashlib.sha256(ticket_hash.encode("ascii")).hexdigest()[:10].lower()
    attempt_path = attempts_dir / f"attempt-{attempt_number:02d}-{nonce}.json"
    retry_event = {"schema_version": 1, "event_id": attempt_id, "cycle_id": ticket["cycle_id"],
                   "stage": 2, "status": "repair_requested", "consumed": False,
                   "rotation_eligible": False, "ticket": str(ticket_path.resolve()),
                   "ticket_sha256": ticket_hash, "dispatch_attempt": attempt_number,
                   "parent_event_id": event.get("event_id"),
                   "parent_attempt_failed": retry_after_terminal_failure,
                   "created_utc": utc_now()}
    try:
        with attempt_path.open("x", encoding="utf-8") as handle:
            json.dump(retry_event, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        retry_event = load_object(attempt_path)
        if retry_event.get("ticket_sha256") != ticket_hash or retry_event.get("event_id") != attempt_id:
            raise ValueError("recovery retry event path is occupied by mismatched evidence")
    state.update({"stage2_source_defect_original_event": state.get("stage2_source_defect_original_event") or str(event_path.resolve()),
                  "stage2_source_defect_event": str(attempt_path.resolve()),
                  "stage2_source_defect_event_id": attempt_id,
                  "stage2_source_defect_event_sha256": sha256(attempt_path),
                          "stage2_source_defect_consumption_intent": None,
                          "stage2_source_defect_consumed_receipt": None,
                  "stage2_source_defect_retry_attempts": attempt_number,
                  "stage2_source_defect_retry_hold": None,
                  "next_action": "dispatch_stage2", "updated_utc": utc_now()})
    state.pop("stage2_source_defect_retry_exhausted", None)
    state.pop("stage2_source_defect_retry_terminal_hold", None)
    atomic_json(state_path, state)
    return "retry_prepared"


def validate_dispatch_entrypoint(job_id: str) -> tuple[bool, str]:
    """Fail fast when a Hermes job points at a missing script."""
    try:
        payload = json.loads(HERMES_JOBS.read_text(encoding="utf-8"))
        jobs = payload.get("jobs", payload if isinstance(payload, list) else [])
        job = next((item for item in jobs if item.get("id") == job_id or item.get("job_id") == job_id), None)
        if not job:
            return False, f"Hermes job {job_id} is absent from jobs.json"
        script = job.get("script")
        if not script:
            return False, f"Hermes job {job_id} has no script entrypoint"
        resolved = HERMES_SCRIPTS / script
        if not resolved.is_file():
            return False, f"Hermes entrypoint missing: {resolved}"
        return True, str(resolved)
    except Exception as exc:
        return False, f"Hermes entrypoint audit failed: {exc}"


def dispatch_job(control: dict, action: str, runner=None, db_path: Path = DB) -> tuple[bool, str, str | None]:
    """Start a real Hermes cron without imposing a timeout on its owner.

    Tests may inject a synchronous runner. Production resumes the trigger-only
    job, launches ``hermes cron run`` as a detached process, and binds the new
    execution ID. Stages 3, 4.5, and 5 are recurring workers, so keep their
    schedules enabled; pause one-shot stage schedules after binding.
    """
    stage_by_action = {"dispatch_stage1": "1", "dispatch_stage2": "2", "dispatch_stage3": "3", "dispatch_stage4": "4", "dispatch_stage45": "4.5", "dispatch_stage5": "5"}
    stage = stage_by_action.get(action)
    if stage is None:
        return False, f"unsupported dispatch action: {action}", None
    job_id = control.get("stage_jobs", {}).get(stage)
    if not job_id:
        return False, f"Stage {stage} Hermes job is not configured", None
    keep_schedule_active = action in {"dispatch_stage3", "dispatch_stage45", "dispatch_stage5"}
    # An injected runner stands in for Hermes in unit tests; its fixture job
    # IDs are intentionally independent of the operator's installed jobs.json.
    if runner is None:
        entrypoint_ok, entrypoint_detail = validate_dispatch_entrypoint(str(job_id))
        if not entrypoint_ok:
            return False, f"Stage {stage} dispatch blocked before watchdog: {entrypoint_detail}", None

    # Stage 1 is operator-triggered. Close its recurring schedule before Stage 2
    # is admitted so the completed Stage 1 cannot fire again on a later tick.
    if action == "dispatch_stage2":
        stage1_job = control.get("stage_jobs", {}).get("1")
        if not stage1_job:
            return False, "Stage 1 Hermes job is not configured", None
        pause_command = ["hermes", "cron", "pause", stage1_job]
        try:
            pause_result = (runner or subprocess.run)(
                pause_command,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
        except Exception as exc:
            return False, f"Stage 1 completed, but its schedule could not be paused: {exc}", None
        if pause_result.returncode != 0:
            detail = (pause_result.stderr or pause_result.stdout or "unknown pause failure").strip()
            return False, f"Stage 1 completed, but its schedule could not be paused: {detail}", None

    if runner is not None:
        try:
            completed = runner(
                ["hermes", "cron", "run", job_id],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        except Exception as exc:
            return False, str(exc), None
        detail = (completed.stderr or completed.stdout or "Hermes cron trigger failed").strip()
        rejected = any(marker in detail.casefold() for marker in (
            "job is paused", "job is disabled", "resume it before running", "trigger rejected", "not found",
        ))
        return completed.returncode == 0 and not rejected, detail, None

    before = execution_ids(db_path, job_id)
    try:
        resumed = subprocess.run(
            ["hermes", "cron", "resume", job_id],
            capture_output=True, text=True, timeout=15, check=False,
        )
        if resumed.returncode != 0:
            return False, (resumed.stderr or resumed.stdout or "failed to resume cron").strip(), None
        # A due scheduler tick can be claimed just before the immediate
        # ``cron run`` below. Allow the scheduler's execution row to settle
        # before triggering, then reuse it and close the one-shot schedule.
        raced_execution_id = None
        race_deadline = time.monotonic() + 5.0
        while time.monotonic() < race_deadline:
            raced_execution_id = newest_unseen_execution(db_path, job_id, before)
            if raced_execution_id:
                break
            time.sleep(0.1)
        if raced_execution_id and execution_status(db_path, raced_execution_id) in {
            "claimed", "running", "started", "queued", "completed",
        }:
            if not keep_schedule_active:
                pause_result = subprocess.run(
                    ["hermes", "cron", "pause", job_id],
                    capture_output=True, text=True, timeout=15, check=False,
                )
                if pause_result.returncode != 0:
                    detail = (pause_result.stderr or pause_result.stdout or "unknown pause failure").strip()
                    return False, f"reused execution {raced_execution_id}, but schedule pause failed: {detail}", raced_execution_id
                schedule_detail = "recurring schedule paused"
            else:
                schedule_detail = "recurring schedule kept active"
            return True, f"reused already-claimed cron execution_id={raced_execution_id}; {schedule_detail}", raced_execution_id
        log_dir = ROOT / "state" / "diagnostics" / "cron_dispatch"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{utc_now().replace(':', '').replace('-', '')}.{job_id}.log"
        flags = 0
        popen_options = {}
        if os.name == "nt":
            flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
        else:
            popen_options["start_new_session"] = True
        with log_path.open("ab", buffering=0) as output:
            process = subprocess.Popen(
                ["hermes", "cron", "run", job_id],
                stdout=output,
                stderr=subprocess.STDOUT,
                cwd=str(ROOT),
                creationflags=flags,
                close_fds=True,
                **popen_options,
            )
        deadline = time.monotonic() + 45
        execution_id = None
        while time.monotonic() < deadline:
            execution_id = newest_unseen_execution(db_path, job_id, before)
            if execution_id is not None:
                break
            if process.poll() is not None:
                break
            time.sleep(0.25)
    # Stages 3, 4.5, and 5 are reconciliation loops; later ticks must remain
    # enabled to monitor and publish their completion events. Other stages are
    # one-shot trigger jobs and are paused after their execution is bound.
        paused = None
        if not keep_schedule_active:
            paused = subprocess.run(
                ["hermes", "cron", "pause", job_id],
                capture_output=True, text=True, timeout=15, check=False,
            )
        if execution_id is None:
            detail = f"no new execution appeared; launcher_pid={process.pid}; log={log_path}"
            if paused is not None and paused.returncode != 0:
                detail += f"; pause failed: {(paused.stderr or paused.stdout).strip()}"
            return False, detail, None
        return True, f"detached cron execution_id={execution_id}; launcher_pid={process.pid}; log={log_path}; reconciliation={'enabled' if action == 'dispatch_stage5' else 'disabled'}", execution_id
    except Exception as exc:
        try:
            subprocess.run(
                ["hermes", "cron", "pause", job_id],
                capture_output=True, text=True, timeout=15, check=False,
            )
        except Exception:
            pass
        return False, str(exc), None


def run_stage3_driver(state_path: Path, control: dict, start: bool = False, runner=None) -> tuple[bool, str]:
    handoff = control.get("stage3_handoff") or str(ROOT / "state" / "handoffs" / "Stage3_v2.json")
    driver = Path(control.get("stage3_driver") or STAGE3_DRIVER)
    if not driver.is_file():
        return False, f"Stage 3 deterministic driver is missing: {driver}"
    invoke = runner or subprocess.run
    try:
        command = [sys.executable, str(driver), "--state", str(state_path), "--handoff", str(handoff)]
        if start:
            command.append("--start")
        completed = invoke(
            command,
            capture_output=True, text=True, timeout=300, check=False,
        )
    except Exception as exc:
        return False, str(exc)
    detail = (completed.stdout or completed.stderr or "Stage 3 driver returned no result").strip()
    try:
        payload = json.loads(detail.splitlines()[-1])
    except (json.JSONDecodeError, IndexError):
        return False, detail
    status = payload.get("status")
    duplicate_running = (
        status == "INFRASTRUCTURE_FAILURE"
        and "another Stage 3 runtime is already optimizing" in str(payload.get("detail", ""))
    )
    if duplicate_running:
        payload = {**payload, "status": "ALREADY_RUNNING"}
        detail = json.dumps(payload, separators=(",", ":"))
        status = "ALREADY_RUNNING"
    accepted = payload.get("zero_token") is True and status in {
        "STAGE3_STARTED", "NEXT_TIMEFRAME_STARTED", "REPAIR_REQUIRED", "STAGE3_COMPLETE_HELD",
        "STAGE3_FAILED_HELD", "COMPUTE_BUDGET_EXCEEDED", "ALREADY_RUNNING",
    } and (completed.returncode == 0 or duplicate_running)
    return accepted, detail


def stage3_cycle_hold(state: dict, *, stage2_audit_passed: bool = False) -> str | None:
    """Hold Stage 3 unless an authenticated audit and cycle authorization release it."""
    audit_marker = (state.get("stage_status") == "stage2_complete_held"
                    or state.get("next_action") == "audit_stage2_before_stage3")
    authorization = state.get("run_authorization")
    authorization_hold = authorization.get("hold_after_stage") if isinstance(authorization, dict) else None
    audit_released = (
        audit_marker
        and stage2_audit_passed
        and (authorizes_stages_1_to_3(state) or authorizes_stages_1_to_5(state))
        and isinstance(authorization, dict)
        and authorization.get("cycle_id") == state.get("cycle_id")
        and isinstance(authorization_hold, (int, float))
        and not isinstance(authorization_hold, bool)
        and float(authorization_hold) in {3.0, 4.0, 4.5, 5.0}
    )
    hold_after = authorization_hold if audit_released else state.get("hold_after_stage")
    if hold_after is not None:
        if (isinstance(hold_after, bool) or not isinstance(hold_after, (int, float))
                or float(hold_after) not in {1.0, 2.0, 3.0, 4.0, 4.5, 5.0}):
            return f"invalid cycle hold_after_stage={hold_after!r}; Stage 3 remains held"
        if hold_after <= 2:
            return f"cycle-level hold_after_stage={hold_after} blocks Stage 3"
    if audit_marker and not audit_released:
        return "Stage 2 audit or matching Stage 1–5 authorization is not verified; Stage 3 remains held"
    return None


def stage1_research_retry_repair_ticket(state: dict, retry_count: int) -> dict | None:
    """Validate a one-shot same-cycle repair retry without resetting retry history."""
    ticket = state.get("stage1_research_retry_repair")
    if (not isinstance(ticket, dict) or ticket.get("status") != "authorized"
            or ticket.get("cycle_id") != state.get("cycle_id")
            or ticket.get("retry_count_at_authorization") != retry_count):
        return None
    receipt_path = Path(ticket.get("receipt_path") or "")
    if not receipt_path.is_file():
        return None
    try:
        if sha256(receipt_path) != str(ticket.get("receipt_sha256") or "").upper():
            return None
        receipt = load_object(receipt_path)
    except (OSError, ValueError, TypeError):
        return None
    if (receipt.get("schema_version") != 1
            or receipt.get("record_kind") != "kcos_stage1_auto_evolve_repair"
            or receipt.get("cycle_id") != state.get("cycle_id")
            or receipt.get("repair_id") != ticket.get("repair_id")
            or receipt.get("retry_count_at_authorization") != retry_count
            or receipt.get("authorized_action") != "one_same_cycle_stage1_retry"):
        return None
    return ticket


def stage1_research_hermes_web_available() -> bool:
    """Confirm the configured Stage 1 Hermes job has its native web toolset."""
    try:
        control = load_object(CONTROL)
        job_id = str((control.get("stage_jobs") or {}).get("1") or "")
        if not job_id or not HERMES_JOBS.is_file():
            return False
        registry = load_object(HERMES_JOBS)
        jobs = registry.get("jobs", []) if isinstance(registry, dict) else []
        job = next((item for item in jobs if isinstance(item, dict)
                    and str(item.get("id") or "") == job_id), None)
        return bool(job and job.get("no_agent") is not True
                    and "web" in (job.get("enabled_toolsets") or []))
    except (OSError, ValueError, TypeError, KeyError):
        return False


def stage1_research_source_endpoint_ready(timeout: float = 3.0) -> bool:
    """Check SearXNG, then the Stage 1 job's configured native Hermes web tool."""
    endpoint = os.environ.get("SEARXNG_URL", "http://127.0.0.1:8080").rstrip("/")
    query = urllib.parse.urlencode({"q": "kcos stage1 readiness", "format": "json", "language": "en-US"})
    request = urllib.request.Request(
        f"{endpoint}/search?{query}",
        headers={"Accept": "application/json", "User-Agent": "KCOS-Stage1-Readiness/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                return False
            payload = json.loads(response.read(512_000).decode("utf-8", errors="strict"))
        results = payload.get("results") if isinstance(payload, dict) else None
        if isinstance(results, list) and any(
            isinstance(result, dict)
            and isinstance(result.get("url"), str)
            and result["url"].startswith(("https://", "http://"))
            for result in results
        ):
            return True
    except (OSError, urllib.error.URLError, TimeoutError, ValueError, UnicodeError):
        pass
    # The injected SearXNG preflight is advisory. Stage 1 already has Hermes
    # web search; retain the one-shot retry and let its captured-source and
    # publication validators decide whether research is admissible.
    return stage1_research_hermes_web_available()


def stage1_research_retry_eligible(state: dict, handoff: dict, failure: dict,
                                   source_available: bool | None = None) -> bool:
    """Permit only bounded retries of a same-cycle failed research publication."""
    cycle_id = state.get("cycle_id")
    try:
        retry_count = int(state.get("stage1_research_retry_count", 0))
    except (TypeError, ValueError):
        return False
    if source_available is None:
        source_available = stage1_research_source_endpoint_ready()
    repair_ticket = stage1_research_retry_repair_ticket(state, retry_count)
    return (
        state.get("current_stage") == "stage1_running"
        and state.get("stage_status") == "stage1_research_required"
        and state.get("next_action") == "research_new"
        and state.get("research_mode") == "research_new"
        and source_available is True
        and (0 <= retry_count < 2 or (retry_count >= 2 and repair_ticket is not None))
        and handoff.get("cycle_id") == cycle_id
        and handoff.get("research_mode") == "research_new"
        and handoff.get("research_allowed") is True
        and handoff.get("queue_exhausted") is True
        and failure.get("cycle_id") == cycle_id
        and failure.get("stage") == 1
        and failure.get("status") == "failed"
        and failure.get("rotation_eligible") is True
    )


def stage1_dispatch_uses_claim_only(result: dict) -> bool:
    """Stage 1 retries and fresh pending cycles have no source event to consume."""
    return (
        result.get("action") == "dispatch_stage1"
        and (result.get("status") == "STAGE1_RESEARCH_RETRY_READY"
             or str(result.get("event_id") or "").endswith(":stage1:pending"))
    )


def prepare_stage2_verifier_retry(state_path: Path, control: dict, db_path: Path) -> str:
    """Authorize one same-candidate Hermes rerun for the exact cooldown-guard verifier false negative."""
    state = load_object(state_path)
    if (state.get("stage_status") != "stage2_repair_required"
            or state.get("stage2_verifier_retry_count", 0) != 0
            or not authorizes_stages_1_to_5(state)
            or control.get("dispatch_enabled") is not True
            or 2 not in control.get("allowed_stages", [])): 
        return "not_eligible"
    failure_path = Path(state.get("stage2_failure_event") or "")
    source_path = Path(state.get("candidate_source") or "")
    if not failure_path.is_file():
        return "evidence_missing"
    failure = load_object(failure_path)
    verification_path = Path(failure.get("verification") or "")
    if not all(p.is_file() for p in (verification_path, source_path)):
        return "evidence_missing"
    verification = load_object(verification_path)
    checks = verification.get("checks")
    retry_mode = "cooldown_guard_false_negative"
    allowed_failed_checks = {"cooldown_zero_disables_entry"}
    nan_repair = None
    if (failure.get("cycle_id") == state.get("cycle_id")
            and failure.get("failure_kind") == "COMPILE_INTEGRITY_FAILURE"
            and failure.get("failed_checks") == ["compile_clean"]):
        # One same-candidate retry is allowed only for the deterministic
        # compiler-proven 0/0 sentinel repair, with the original failure
        # preserved and fresh passing compile/verifier artifacts bound below.
        report_path = state_path.parent / "reports" / f"{state.get('cycle_id')}.stage2.repair-loop.json"
        archive_dir = state_path.parent / "diagnostics" / "stage2_source_repairs" / str(state.get("cycle_id"))
        receipt_path = source_path.with_suffix(source_path.suffix + ".build.json")
        stage1_spec = Path(state.get("last_stage1_artifact") or "")
        original_compile_path = Path(failure.get("compile_attempt") or "")
        if not all(p.is_file() for p in (report_path, receipt_path, stage1_spec, original_compile_path)):
            return "nan_repair_evidence_missing"
        report = load_object(report_path)
        attempts = report.get("attempts") or []
        baseline = next((a for a in attempts if a.get("attempt") == 0), {})
        repaired = next((a for a in attempts if a.get("attempt") == 1), {})
        deterministic = repaired.get("deterministic") or {}
        original_doc = load_object(original_compile_path)
        original_attempts = original_doc.get("attempts") or []
        original_last = original_attempts[-1] if original_attempts else original_doc
        original_log = Path(original_last.get("log") or "")
        if not original_log.is_file():
            return "nan_repair_compile_log_missing"
        raw_log = original_log.read_bytes()
        log_text = raw_log.decode("utf-16", errors="replace") if raw_log.startswith((b"\xff\xfe", b"\xfe\xff")) else raw_log.decode("utf-8-sig", errors="replace")
        nan_errors = len(re.findall(r"(?im):\s*error\s+198\s*:.*division by zero in the constant expression", log_text))
        archive = next((p for p in archive_dir.glob("candidate-before-*.mq5")
                        if p.is_file() and sha256(p) == str(verification.get("source_sha256") or "").upper()), None)
        repaired_compile_path = state_path.parent / "attempts" / f"{state.get('cycle_id')}.stage2.nan-repair.compile.json"
        repaired_verification_path = state_path.parent / "verification" / f"{state.get('cycle_id')}.stage2.nan-repair.verification.json"
        if not all(p.is_file() for p in (archive, repaired_compile_path, repaired_verification_path)):
            return "nan_repair_outputs_missing"
        repaired_compile = load_object(repaired_compile_path)
        repaired_verification = load_object(repaired_verification_path)
        repaired_checks = repaired_verification.get("checks")
        receipt = load_object(receipt_path)
        proof_ref = receipt.get("implementation_admission") or {}
        proof_path = Path(proof_ref.get("path") or "")
        proof = load_object(proof_path) if proof_path.is_file() else {}
        if (report.get("cycle_id") != state.get("cycle_id") or report.get("ea") != state.get("candidate_ea")
                or baseline.get("checks", {}).get("compile_clean") is not False
                or {k for k, v in (baseline.get("checks") or {}).items() if v is False} != {"compile_clean"}
                or str(baseline.get("before_sha256") or "").upper() != sha256(archive)
                or deterministic.get("kind") != "mql5_nan_sentinel_replaced"
                or deterministic.get("status") != "APPLIED" or deterministic.get("changed") is not True
                or sum(int(c.get("count", 0)) for c in deterministic.get("changes", [])
                       if c.get("kind") == "mql5_nan_sentinel_replaced") != nan_errors
                or nan_errors < 1 or sha256(source_path) != str(repaired.get("after_sha256") or "").upper()
                or sha256(source_path) == sha256(archive)
                or repaired.get("verification_passed") is not True
                or any(type(v) is not bool or v is not True for v in (repaired_checks or {}).values())
                or repaired_verification.get("cycle_id") != state.get("cycle_id")
                or repaired_verification.get("passed") is not True
                or str(repaired_verification.get("source_sha256") or "").upper() != sha256(source_path)
                or str(repaired_verification.get("stage1_spec_sha256") or "").upper() != sha256(stage1_spec)
                or repaired_compile.get("cycle_id") != state.get("cycle_id")
                or repaired_compile.get("passed") is not True or repaired_compile.get("fresh_ex5") is not True
                or str(repaired_compile.get("mq5_sha256") or "").upper() != sha256(source_path)
                or str(repaired_compile.get("ex5_sha256") or "").upper() != sha256(Path(repaired_verification.get("ex5") or ""))
                or receipt.get("cycle_id") != state.get("cycle_id") or receipt.get("candidate_ea") != state.get("candidate_ea")
                or str(receipt.get("sha256") or "").upper() != sha256(source_path)
                or str(receipt.get("source_spec_sha256") or "").upper() != sha256(stage1_spec)
                or not proof_path.is_file() or sha256(proof_path) != str(proof_ref.get("sha256") or "").upper()
                or proof.get("passed") is not True or proof.get("cycle_id") != state.get("cycle_id")
                or proof.get("ea") != state.get("candidate_ea")
                or str(proof.get("source_sha256") or "").upper() != sha256(source_path)
                or str(proof.get("source_spec_sha256") or "").upper() != sha256(stage1_spec)):
            return "nan_repair_provenance_invalid"
        nan_repair = {"archive": archive, "report_path": report_path,
                      "compile_path": repaired_compile_path, "verification_path": repaired_verification_path,
                      "proof_path": proof_path, "receipt_path": receipt_path,
                      "original_compile_path": original_compile_path}
        retry_mode = "authenticated_mql5_nan_sentinel_repair"
        allowed_failed_checks = {"compile_clean"}
    if (failure.get("cycle_id") == state.get("cycle_id")
            and failure.get("failure_kind") == "COMPILE_INTEGRITY_FAILURE"
            and set(failure.get("failed_checks") or []) == {
                "compile_clean", "cooldown_zero_disables_entry", "time_stop_zero_disables_close"}):
        archive_dir = (state_path.parent / "diagnostics" / "stage2_source_repairs"
                       / str(state.get("cycle_id")))
        archive = next((p for p in archive_dir.glob("candidate-before-*.mq5")
                        if p.is_file() and sha256(p) == str(verification.get("source_sha256") or "").upper()), None)
        receipt_path = source_path.with_suffix(source_path.suffix + ".build.json")
        compile_path = Path(failure.get("compile_attempt") or "")
        stage1_spec = Path(state.get("last_stage1_artifact") or "")
        if not archive or not receipt_path.is_file() or not compile_path.is_file() or not stage1_spec.is_file():
            return "deterministic_repair_evidence_missing"
        receipt = load_object(receipt_path)
        proof_ref = receipt.get("implementation_admission") or {}
        proof_path = Path(proof_ref.get("path") or "")
        if not proof_path.is_file() or sha256(proof_path) != str(proof_ref.get("sha256") or "").upper():
            return "deterministic_repair_proof_missing"
        proof = load_object(proof_path)
        compile_doc = load_object(compile_path)
        compile_attempts = compile_doc.get("attempts") or []
        last_compile = compile_attempts[-1] if compile_attempts else compile_doc
        compile_log = Path(last_compile.get("log") or "")
        current_source_text = source_path.read_text(encoding="utf-8-sig")
        if (sha256(source_path) == sha256(archive)
                or receipt.get("cycle_id") != state.get("cycle_id")
                or receipt.get("candidate_ea") != state.get("candidate_ea")
                or Path(receipt.get("output") or "").resolve() != source_path.resolve()
                or str(receipt.get("sha256") or "").upper() != sha256(source_path)
                or Path(receipt.get("source_spec") or "").resolve() != stage1_spec.resolve()
                or str(receipt.get("source_spec_sha256") or "").upper() != sha256(stage1_spec)
                or receipt.get("implementation_mode") != "model_generated_source_admitted"
                or proof.get("passed") is not True
                or proof.get("cycle_id") != state.get("cycle_id")
                or proof.get("ea") != state.get("candidate_ea")
                or str(proof.get("source_sha256") or "").upper() != sha256(source_path)
                or str(proof.get("source_spec_sha256") or "").upper() != sha256(stage1_spec)
                or not zero_disables_cooldown_controls(current_source_text)
                or not zero_disables_time_stop_controls(current_source_text)
                or compile_doc.get("cycle_id") != state.get("cycle_id")
                or compile_doc.get("passed") is not False
                or str(last_compile.get("mq5_sha256") or "").upper() != sha256(archive)
                or last_compile.get("attempt", 0) < 1
                or not compile_log.is_file()
                or "error 203: invalid index value" not in (
                    compile_log.read_bytes().decode("utf-16", errors="replace")
                    if compile_log.read_bytes().startswith((b"\xff\xfe", b"\xfe\xff"))
                    else compile_log.read_text(encoding="utf-8-sig", errors="replace")
                ).casefold()):
            return "deterministic_repair_provenance_invalid"
        retry_mode = "authenticated_deterministic_source_repair"
        allowed_failed_checks = {"compile_clean", "cooldown_zero_disables_entry",
                                 "time_stop_zero_disables_close"}
    elif (failure.get("cycle_id") == state.get("cycle_id")
            and failure.get("failed_checks") == ["cooldown_zero_disables_entry", "generated_source_review_current"]):
        # A previous bounded source repair can restore the authenticated
        # builder output after verification ran against a temporary edit. Admit
        # one fresh compile only when the rejected bytes are preserved and the
        # current source still matches this cycle's builder receipt.
        archive_dir = (state_path.parent / "diagnostics" / "stage2_source_repairs"
                       / str(state.get("cycle_id")))
        receipt_path = source_path.with_suffix(source_path.suffix + ".build.json")
        archive = next((p for p in archive_dir.glob("candidate-after-*.mq5")
                        if p.is_file() and sha256(p) == str(verification.get("source_sha256") or "").upper()), None)
        if archive is None or not receipt_path.is_file():
            return "restored_source_evidence_missing"
        receipt = load_object(receipt_path)
        stage1_spec = Path(state.get("last_stage1_artifact") or "")
        handoff_path = state_path.parent / "handoffs" / "Stage2_v2.json"
        handoff = load_object(handoff_path) if handoff_path.is_file() else {}
        archived_hash = sha256(archive)
        if (archived_hash != str(verification.get("source_sha256") or "").upper()
                or sha256(source_path) == archived_hash
                or receipt.get("cycle_id") != state.get("cycle_id")
                or receipt.get("candidate_ea") != state.get("candidate_ea")
                or Path(receipt.get("output") or "").resolve() != source_path.resolve()
                or str(receipt.get("sha256") or "").upper() != sha256(source_path)
                or Path(receipt.get("source_spec") or "").resolve() != Path(state.get("last_stage1_artifact") or "").resolve()
                or str(receipt.get("source_spec_sha256") or "").upper() != sha256(Path(state["last_stage1_artifact"]))
                or receipt.get("implementation_mode") != "model_generated_source_admitted"
                or not isinstance(receipt.get("implementation_admission"), dict)
                or handoff.get("cycle_id") != state.get("cycle_id")
                or handoff.get("ea") != state.get("candidate_ea")
                or Path(handoff.get("source") or "").resolve() != source_path.resolve()
                or not generated_source_review_is_current(handoff, source_path, stage1_spec)):
            return "restored_source_not_authenticated"
        retry_mode = "restored_authenticated_source"
        allowed_failed_checks = {"cooldown_zero_disables_entry", "generated_source_review_current"}
    elif (failure.get("cycle_id") == state.get("cycle_id")
            and failure.get("failure_kind") == "STAGE2_VERIFICATION_FAILURE"
            and failure.get("failed_checks") == ["cooldown_zero_disables_entry"]
            and (state_path.parent / "reports" / f"{state.get('cycle_id')}.stage2.repair-loop.json").is_file()):
        # The bounded repair loop may already have compiled and verified a
        # repaired source, replacing the mutable latest verifier/compile
        # pointers. Admit exactly one Hermes-owned rerun only when its immutable
        # repair report and rejected-source archive bind the original failure.
        report_path = state_path.parent / "reports" / f"{state.get('cycle_id')}.stage2.repair-loop.json"
        archive_dir = state_path.parent / "diagnostics" / "stage2_source_repairs" / str(state.get("cycle_id"))
        archive = next((p for p in archive_dir.glob("candidate-before-*.mq5")
                        if p.is_file()), None)
        receipt_path = source_path.with_suffix(source_path.suffix + ".build.json")
        stage1_spec = Path(state.get("last_stage1_artifact") or "")
        if not all(p.is_file() for p in (report_path, archive, receipt_path, stage1_spec)):
            return "cooldown_repair_evidence_missing"
        report = load_object(report_path)
        attempts = report.get("attempts") or []
        baseline = next((a for a in attempts if a.get("attempt") == 0), {})
        repaired = next((a for a in attempts if a.get("deterministic", {}).get("changed") is True), {})
        repaired_sha = str(repaired.get("after_sha256") or "").upper()
        receipt = load_object(receipt_path)
        proof_ref = receipt.get("implementation_admission") or {}
        proof_path = Path(proof_ref.get("path") or "")
        proof = load_object(proof_path) if proof_path.is_file() else {}
        checks = verification.get("checks")
        if (report.get("cycle_id") != state.get("cycle_id")
                or report.get("ea") != state.get("candidate_ea")
                or baseline.get("checks", {}).get("cooldown_zero_disables_entry") is not False
                or {k for k, v in (baseline.get("checks") or {}).items() if v is False}
                    != {"cooldown_zero_disables_entry"}
                or str(baseline.get("before_sha256") or sha256(archive)).upper() != sha256(archive)
                or repaired.get("before_sha256", "").upper() != sha256(archive)
                or repaired_sha != sha256(source_path)
                or repaired.get("verification_passed") is not True
                or sha256(archive) == sha256(source_path)
                or receipt.get("cycle_id") != state.get("cycle_id")
                or receipt.get("candidate_ea") != state.get("candidate_ea")
                or str(receipt.get("sha256") or "").upper() != sha256(source_path)
                or str(receipt.get("source_spec_sha256") or "").upper() != sha256(stage1_spec)
                or not proof_path.is_file()
                or sha256(proof_path) != str(proof_ref.get("sha256") or "").upper()
                or proof.get("passed") is not True
                or str(proof.get("source_sha256") or "").upper() != sha256(source_path)
                or str(proof.get("source_spec_sha256") or "").upper() != sha256(stage1_spec)
                or verification.get("cycle_id") != state.get("cycle_id")
                or verification.get("passed") is not True
                or str(verification.get("source_sha256") or "").upper() != sha256(source_path)
                or not isinstance(checks, dict)
                or checks.get("cooldown_zero_disables_entry") is not True
                or any(type(v) is not bool or v is not True for v in checks.values())
                or not zero_disables_cooldown_controls(source_path.read_text(encoding="utf-8-sig"))):
            return "cooldown_repair_provenance_invalid"
        retry_mode = "authenticated_cooldown_helper_repair"
        allowed_failed_checks = {"cooldown_zero_disables_entry"}
    expected_failure_kind = ("COMPILE_INTEGRITY_FAILURE"
                             if retry_mode in {"authenticated_deterministic_source_repair", "authenticated_mql5_nan_sentinel_repair"}
                             else "STAGE2_VERIFICATION_FAILURE")
    if (failure.get("cycle_id") != state.get("cycle_id") or failure.get("stage") != 2
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != expected_failure_kind
            or failure.get("rotation_eligible") is not False
            or set(failure.get("failed_checks") or []) != allowed_failed_checks
            or (retry_mode != "authenticated_mql5_nan_sentinel_repair"
                and failure.get("verification") != str(verification_path))
            or verification.get("cycle_id") != state.get("cycle_id")
            or verification.get("stage") != 2
            or verification.get("passed") is not (retry_mode == "authenticated_cooldown_helper_repair")
            or not isinstance(checks, dict)
            or (retry_mode not in {"authenticated_cooldown_helper_repair", "authenticated_mql5_nan_sentinel_repair"}
                and checks.get("cooldown_zero_disables_entry") is not False)
            or (retry_mode == "authenticated_cooldown_helper_repair"
                and (checks.get("cooldown_zero_disables_entry") is not True
                     or any(type(v) is not bool or v is not True for v in checks.values())))
            or (retry_mode not in {"authenticated_cooldown_helper_repair", "authenticated_mql5_nan_sentinel_repair"}
                and {k for k, v in checks.items() if v is False} != allowed_failed_checks)
            or any(type(v) is not bool for v in checks.values())
            or Path(verification.get("source") or "").resolve() != source_path.resolve()
            or (retry_mode in {"cooldown_guard_false_negative", "authenticated_cooldown_helper_repair"}
                and str(verification.get("source_sha256") or "").upper() != sha256(source_path))
            or (retry_mode == "cooldown_guard_false_negative"
                and not zero_disables_cooldown_controls(source_path.read_text(encoding="utf-8-sig")))):
        return "evidence_not_eligible"
    compile_path = Path(failure.get("compile_attempt") or "")
    if not compile_path.is_file():
        return "compile_evidence_missing"
    compile_doc = load_object(compile_path)
    if retry_mode == "authenticated_mql5_nan_sentinel_repair":
        verification_path = nan_repair["verification_path"]
        verification = load_object(verification_path)
        compile_path = nan_repair["compile_path"]
        compile_doc = load_object(compile_path)
        ex5_path = Path(verification.get("ex5") or "")
    ex5_path = Path(verification.get("ex5") or "")
    if retry_mode in {"authenticated_deterministic_source_repair", "authenticated_cooldown_helper_repair", "authenticated_mql5_nan_sentinel_repair"}:
        if retry_mode == "authenticated_cooldown_helper_repair":
            if (compile_doc.get("cycle_id") != state.get("cycle_id")
                    or compile_doc.get("passed") is not True
                    or compile_doc.get("fresh_ex5") is not True
                    or str(compile_doc.get("mq5_sha256") or "").upper() != sha256(source_path)
                    or not ex5_path.is_file()
                    or str(compile_doc.get("ex5_sha256") or "").upper() != sha256(ex5_path)):
                return "repaired_source_compile_evidence_invalid"
        elif retry_mode == "authenticated_mql5_nan_sentinel_repair":
            if (compile_doc.get("cycle_id") != state.get("cycle_id") or compile_doc.get("passed") is not True
                    or compile_doc.get("fresh_ex5") is not True
                    or str(compile_doc.get("mq5_sha256") or "").upper() != sha256(source_path)
                    or not ex5_path.is_file() or str(compile_doc.get("ex5_sha256") or "").upper() != sha256(ex5_path)):
                return "nan_repair_fresh_compile_invalid"
        else:
            if (compile_doc.get("passed") is not False
                    or str(last_compile.get("mq5_sha256") or "").upper()
                        != str(verification.get("source_sha256") or "").upper()):
                return "compile_evidence_invalid"
            if not ex5_path.is_file():
                ex5_path = None
    elif (not ex5_path.is_file() or compile_doc.get("passed") is not True
            or compile_doc.get("fresh_ex5") is not True
            or str(compile_doc.get("ex5_sha256") or "").upper() != sha256(ex5_path)
            or str(verification.get("ex5_sha256") or (verification.get("compile") or {}).get("ex5_sha256") or "").upper() != sha256(ex5_path)):
        return "compile_evidence_invalid"
    stage1_event_path = Path(state.get("last_stage1_event") or "")
    stage1_spec_path = Path(state.get("last_stage1_artifact") or "")
    if not stage1_event_path.is_file() or not stage1_spec_path.is_file():
        return "stage1_lineage_missing"
    stage1_event = load_object(stage1_event_path)
    if (stage1_event.get("cycle_id") != state.get("cycle_id")
            or stage1_event.get("status") != "complete"
            or stage1_event.get("artifact_sha256") != sha256(stage1_spec_path)
            or verification.get("stage1_spec_sha256") != sha256(stage1_spec_path)):
        return "stage1_lineage_invalid"
    stage2_job = str((control.get("stage_jobs") or {}).get("2") or "")
    execution_id = str(control.get("last_stage2_execution_id") or "")
    row = execution_record(db_path, execution_id, stage2_job) if stage2_job and execution_id else None
    if row is None or row[0] != "failed":
        matched = failed_execution_for_event(db_path, stage2_job, str(failure.get("failed_utc") or "")) if stage2_job else None
        if matched is None:
            return "hermes_execution_not_terminal_failed"
        execution_id = matched[0]

    verifier_path = Path(__import__("kcos_stage2_verify_v2").__file__).resolve()
    ticket = {
        "schema_version": 1, "cycle_id": state["cycle_id"], "stage": 2,
        "reason": ("compiler-proven MQL5 NaN sentinel repair; one Hermes retry requires fresh compile and normal verification"
                   if retry_mode == "authenticated_mql5_nan_sentinel_repair"
                   else "known cooldown/source-review false negative; retry requires fresh compile and normal verification"),
        "retry_mode": retry_mode,
        "failed_check": "cooldown_zero_disables_entry", "failure_event": str(failure_path.resolve()),
        "failure_sha256": sha256(failure_path), "verification": str(verification_path.resolve()),
        "verification_sha256": sha256(verification_path), "source": str(source_path.resolve()),
        "source_sha256": sha256(source_path), "compile_attempt": str(compile_path.resolve()),
        "compile_attempt_sha256": sha256(compile_path), "ex5": str(ex5_path.resolve()) if ex5_path else None,
        "ex5_sha256": sha256(ex5_path) if ex5_path else None, "stage1_event": str(stage1_event_path.resolve()),
        "stage1_event_sha256": sha256(stage1_event_path), "stage1_spec": str(stage1_spec_path.resolve()),
        "stage1_spec_sha256": sha256(stage1_spec_path), "prior_execution_id": execution_id,
        "verifier": str(verifier_path), "verifier_sha256": sha256(verifier_path),
        "created_utc": utc_now(), "retry_limit": 1,
    }
    ticket["repair_identity"] = stage2_repair_identity(state, failure_path)
    if retry_mode == "restored_authenticated_source":
        ticket["rejected_source_archive"] = str(archive.resolve())
        ticket["rejected_source_archive_sha256"] = sha256(archive)
        ticket["restored_source_build_receipt"] = str(receipt_path.resolve())
        ticket["restored_source_build_receipt_sha256"] = sha256(receipt_path)
    if retry_mode == "authenticated_deterministic_source_repair":
        ticket["rejected_source_archive"] = str(archive.resolve())
        ticket["rejected_source_archive_sha256"] = sha256(archive)
        ticket["restored_source_build_receipt"] = str(receipt_path.resolve())
        ticket["restored_source_build_receipt_sha256"] = sha256(receipt_path)
        ticket["source_admission_proof"] = str(proof_path.resolve())
        ticket["source_admission_proof_sha256"] = sha256(proof_path)
    if retry_mode == "authenticated_cooldown_helper_repair":
        ticket["repair_report"] = str(report_path.resolve())
        ticket["repair_report_sha256"] = sha256(report_path)
        ticket["rejected_source_archive"] = str(archive.resolve())
        ticket["rejected_source_archive_sha256"] = sha256(archive)
        ticket["restored_source_build_receipt"] = str(receipt_path.resolve())
        ticket["restored_source_build_receipt_sha256"] = sha256(receipt_path)
        ticket["source_admission_proof"] = str(proof_path.resolve())
        ticket["source_admission_proof_sha256"] = sha256(proof_path)
    if retry_mode == "authenticated_mql5_nan_sentinel_repair":
        ticket.update({"failed_check": "compile_clean",
                       "failure_verification": str(Path(failure.get("verification")).resolve()),
                       "failure_verification_sha256": sha256(Path(failure.get("verification"))),
                       "failed_compile_attempt": str(nan_repair["original_compile_path"].resolve()),
                       "failed_compile_attempt_sha256": sha256(nan_repair["original_compile_path"]),
                       "repair_report": str(nan_repair["report_path"].resolve()),
                       "repair_report_sha256": sha256(nan_repair["report_path"]),
                       "rejected_source_archive": str(nan_repair["archive"].resolve()),
                       "rejected_source_archive_sha256": sha256(nan_repair["archive"]),
                       "restored_source_build_receipt": str(nan_repair["receipt_path"].resolve()),
                       "restored_source_build_receipt_sha256": sha256(nan_repair["receipt_path"]),
                       "source_admission_proof": str(nan_repair["proof_path"].resolve()),
                       "source_admission_proof_sha256": sha256(nan_repair["proof_path"]),
                       "compile_attempt": str(nan_repair["compile_path"].resolve()),
                       "compile_attempt_sha256": sha256(nan_repair["compile_path"]),
                       "verification": str(nan_repair["verification_path"].resolve()),
                       "verification_sha256": sha256(nan_repair["verification_path"]),
                       "ex5": str(Path(verification.get("ex5")).resolve()),
                       "ex5_sha256": sha256(Path(verification.get("ex5")))})
    ticket_sha = hashlib.sha256(json.dumps(ticket, sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
    event_id = f"{state['cycle_id']}:stage2:verifier-retry:{ticket_sha}"
    recovery_dir = state_path.parent / "recovery"
    recovery_dir.mkdir(parents=True, exist_ok=True)
    ticket_path = recovery_dir / f"{state['cycle_id']}.stage2.verifier-retry.ticket.json"
    event_path = recovery_dir / f"{state['cycle_id']}.stage2.verifier-retry.event.json"
    ticket["ticket_id"] = event_id
    ticket["ticket_sha256"] = hashlib.sha256(json.dumps(
        {k: v for k, v in ticket.items() if k not in {"ticket_id", "ticket_sha256"}},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
    ticket_sha = ticket["ticket_sha256"]
    event = {"schema_version": 1, "cycle_id": state["cycle_id"], "stage": 2,
             "event_id": event_id, "status": "retry_requested", "consumed": False,
             "rotation_eligible": False, "ticket": str(ticket_path.resolve()),
             "ticket_sha256": ticket_sha, "dispatch_attempt": 1, "created_utc": utc_now()}
    for path, payload in ((ticket_path, ticket), (event_path, event)):
        if path.exists():
            return "retry_artifact_already_exists"
        with path.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2); handle.write("\n")
    state.update({"stage2_verifier_retry_count": 1,
                  "stage2_verifier_retry_ticket": str(ticket_path.resolve()),
                  "stage2_verifier_retry_ticket_sha256": ticket_sha,
                  "stage2_verifier_retry_event": str(event_path.resolve()),
                  "stage2_verifier_retry_event_id": event_id,
                  "stage2_verifier_retry_event_sha256": sha256(event_path),
                  "next_action": "dispatch_stage2", "updated_utc": utc_now()})
    atomic_json(state_path, state)
    return "retry_prepared"


def clear_stale_stage2_retry_metadata(state_path: Path) -> bool:
    """Retire prior-cycle retry pointers while preserving immutable evidence files."""
    state = load_object(state_path)
    changed = False
    audit = list(state.get("stale_stage2_retry_metadata_audit", []))
    for prefix, ticket_key in (("stage2_verifier_retry_", "stage2_verifier_retry_ticket"),
                               ("stage2_source_defect_", "stage2_source_defect_ticket")):
        ticket_path = Path(state.get(ticket_key) or "")
        if not ticket_path.is_file():
            continue
        try:
            ticket = load_object(ticket_path)
        except (OSError, ValueError):
            continue
        if ticket.get("cycle_id") == state.get("cycle_id"):
            continue
        audit.append({"field_prefix": prefix, "stale_cycle_id": ticket.get("cycle_id"),
                      "active_cycle_id": state.get("cycle_id"), "ticket": str(ticket_path.resolve()),
                      "ticket_sha256": sha256(ticket_path), "retired_utc": utc_now()})
        for key in list(state):
            if key.startswith(prefix):
                state.pop(key, None)
        if prefix == "stage2_verifier_retry_":
            state["stage2_verifier_retry_count"] = 0
        changed = True
    if changed:
        state["stale_stage2_retry_metadata_audit"] = audit
        state["updated_utc"] = utc_now()
        atomic_json(state_path, state)
    return changed


def validate_stage2_verifier_retry(state: dict) -> tuple[bool, str, dict, Path | None]:
    ticket_path = Path(state.get("stage2_verifier_retry_ticket") or "")
    event_path = Path(state.get("stage2_verifier_retry_event") or "")
    if not ticket_path.is_file() or not event_path.is_file():
        return False, "retry ticket/event missing", {}, None
    ticket, event = load_object(ticket_path), load_object(event_path)
    expected_hash = hashlib.sha256(json.dumps(
        {k: v for k, v in ticket.items() if k not in {"ticket_id", "ticket_sha256"}},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
    retry_mode = ticket.get("retry_mode")
    expected_failed_check = ("compile_clean" if retry_mode == "authenticated_mql5_nan_sentinel_repair"
                             else "cooldown_zero_disables_entry")
    valid = (ticket.get("schema_version") == 1 and ticket.get("cycle_id") == state.get("cycle_id")
             and retry_mode in {"cooldown_guard_false_negative", "authenticated_deterministic_source_repair",
                                "restored_authenticated_source", "authenticated_cooldown_helper_repair",
                                "authenticated_mql5_nan_sentinel_repair"}
             and ticket.get("failed_check") == expected_failed_check
             and ticket.get("retry_limit") == 1 and ticket.get("ticket_sha256") == expected_hash
             and ticket.get("ticket_sha256") == state.get("stage2_verifier_retry_ticket_sha256")
             and ticket.get("repair_identity") == stage2_repair_identity(
                 state, Path(state.get("stage2_failure_event") or state.get("stage2_invalid_event") or ""))
             and ticket.get("ticket_id") == state.get("stage2_verifier_retry_event_id")
             and event.get("cycle_id") == state.get("cycle_id") and event.get("stage") == 2
             and event.get("event_id") == ticket.get("ticket_id")
             and event.get("ticket_sha256") == expected_hash and event.get("ticket") == str(ticket_path.resolve())
             and event.get("rotation_eligible") is False)
    return valid, ("authenticated cooldown verifier retry" if valid else "retry ticket/event binding mismatch"), event, event_path


def validate_stage3_input_contract_repair(state: dict) -> tuple[bool, str, dict]:
    """Validate one regression-backed retry for fixed scalar constraint rows."""
    failure_path = Path(state.get("stage3_failure_event") or "")
    receipt_path = Path(state.get("stage3_input_contract_repair_receipt") or "")
    if not failure_path.is_file() or not receipt_path.is_file():
        return False, "Stage 3 input-contract repair receipt or failure event is missing", {}
    failure = load_object(failure_path)
    receipt = load_object(receipt_path)
    spec_path = Path(state.get("last_stage1_artifact") or "")
    if not spec_path.is_file():
        return False, "Stage 1 spec is missing for the Stage 3 retry", failure
    spec = load_object(spec_path)
    detail = str((failure.get("findings") or [{}])[0].get("detail") or "")
    if failure.get("recovery_class") == "stage3_grid_policy":
        source_path = Path(state.get("candidate_source") or "")
        prep_path = ROOT / "state" / "kcos_stage3_prepare_v2.py"
        report_path = Path(receipt.get("preflight_report") or "")
        try:
            report = load_object(report_path) if report_path.is_file() else {}
        except (OSError, ValueError, TypeError):
            report = {}
        failure_code = ((failure.get("findings") or [{}])[0].get("code"))
        grid_retry_consumed = (
            failure.get("consumed") is True
            and failure.get("consumed_action") == "start_stage3"
            and state.get("stage3_input_contract_retry_count") == 1
            and state.get("stage3_input_contract_retry_event_id") == failure.get("event_id")
        )
        original_failure_hash = sha256(failure_path)
        if grid_retry_consumed:
            original_event = dict(failure)
            original_event["consumed"] = False
            original_event.pop("consumed_utc", None)
            original_event.pop("consumed_action", None)
            original_failure_hash = hashlib.sha256(
                (json.dumps(original_event, indent=2) + "\n").encode("utf-8")).hexdigest().upper()
        timeframe_checks = report.get("timeframes")
        valid = (
            failure.get("cycle_id") == state.get("cycle_id")
            and failure.get("stage") == 3 and failure.get("status") == "failed"
            and failure.get("failure_kind") == "STAGE3_INPUT_CONTRACT_FAILURE"
            and failure.get("rotation_eligible") is False
            and (failure.get("consumed") is False or grid_retry_consumed)
            and failure_code == "INPUT_GRID_INVALID"
            and receipt.get("record_kind") == "kcos_stage3_grid_policy_repair"
            and receipt.get("cycle_id") == state.get("cycle_id")
            and state.get("stage3_input_contract_repair_receipt_sha256") == sha256(receipt_path)
            and receipt.get("failure_event") == str(failure_path.resolve())
            and receipt.get("failure_event_sha256") == original_failure_hash
            and source_path.is_file()
            and receipt.get("source_sha256") == sha256(source_path)
            and receipt.get("stage1_spec_sha256") == sha256(spec_path)
            and receipt.get("stage3_prepare") == str(prep_path.resolve())
            and receipt.get("stage3_prepare_sha256") == sha256(prep_path)
            and report.get("cycle_id") == state.get("cycle_id")
            and report.get("failure_event_sha256") == original_failure_hash
            and report.get("stage1_spec_sha256") == sha256(spec_path)
            and report.get("source_sha256") == sha256(source_path)
            and report.get("stage3_prepare_sha256") == sha256(prep_path)
            and report.get("passed") is True
            and isinstance(timeframe_checks, dict)
            and set(timeframe_checks) == {"M5", "M15", "H1"}
            and all(item.get("passed") is True for item in timeframe_checks.values())
            and receipt.get("preflight_report_sha256") == sha256(report_path)
            and receipt.get("passed") is True
        )
        return valid, ("grid-policy repair preflight passed for all Stage 3 timeframes" if valid
                       else "Stage 3 grid-policy repair preflight receipt does not match the authenticated cycle failure"), failure
    consumed_retry_event = (failure.get("consumed") is True
                            and failure.get("consumed_action") == "start_stage3"
                            and state.get("stage3_input_contract_retry_count") == 1
                            and state.get("stage3_input_contract_retry_event_id") == failure.get("event_id"))
    original_failure_hash = sha256(failure_path)
    if consumed_retry_event:
        # consume_event changes only these fields and uses atomic_json(indent=2).
        # Rebuild the exact pre-consumption bytes and compare them to the
        # repair receipt, rather than trusting a mutable state hash.
        original_event = dict(failure)
        original_event["consumed"] = False
        original_event.pop("consumed_utc", None)
        original_event.pop("consumed_action", None)
        original_failure_hash = hashlib.sha256(
            (json.dumps(original_event, indent=2) + "\n").encode("utf-8")).hexdigest().upper()
    row_match = re.fullmatch(
        r"input constraint ([A-Za-z0-9_.-]+) references missing TesterInputs row ([A-Za-z_][A-Za-z0-9_]*)",
        detail)
    inputs = ((spec.get("tester_config") or {}).get("inputs") or {})
    input_spec = inputs.get(row_match.group(2)) if row_match else None
    constraint_ids = {item.get("id") for item in (spec.get("tester_config") or {}).get("input_constraints", [])
                      if isinstance(item, dict)}
    prep_path = ROOT / "state" / "kcos_stage3_prepare_v2.py"
    test_path = ROOT / "state" / "test_kcos_stage3_v2.py"
    test_report_path = Path(receipt.get("test_report") or "")
    valid = (
        failure.get("cycle_id") == state.get("cycle_id")
        and failure.get("stage") == 3 and failure.get("status") == "failed"
        and failure.get("failure_kind") == "STAGE3_INPUT_CONTRACT_FAILURE"
        and failure.get("rotation_eligible") is True
        and (failure.get("consumed") is False or consumed_retry_event)
        and receipt.get("cycle_id") == state.get("cycle_id")
        and state.get("stage3_input_contract_repair_receipt_sha256") == sha256(receipt_path)
        and receipt.get("failure_event") == str(failure_path.resolve())
        and receipt.get("failure_event_sha256") == original_failure_hash
        and row_match is not None and row_match.group(1) in constraint_ids
        and isinstance(input_spec, dict) and input_spec.get("optimize") == "N"
        and receipt.get("input_name") == row_match.group(2)
        and receipt.get("passed") is True
        and receipt.get("test_name") == "test_constraints_accept_fixed_scalar_tester_inputs"
        and receipt.get("stage3_prepare") == str(prep_path.resolve())
        and receipt.get("stage3_prepare_sha256") == sha256(prep_path)
        and receipt.get("test_file") == str(test_path.resolve())
        and receipt.get("test_file_sha256") == sha256(test_path)
        and test_report_path.is_file()
        and receipt.get("test_report_sha256") == sha256(test_report_path)
        and receipt.get("test_count", 0) >= 1
        and receipt.get("stage1_spec_sha256") == sha256(spec_path)
    )
    return valid, ("one regression-backed fixed-scalar constraint retry is eligible" if valid
                   else "Stage 3 input-contract repair receipt does not match the authenticated fixed-scalar failure"), failure




def authenticated_stage3_zero_trade_repair(state: dict) -> dict | None:
    """Authenticate completed Stage 3 zero-trade evidence for Stage 2 repair."""
    cycle = str(state.get("cycle_id") or "")
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
    decision = load_object(decision_path)
    verification = load_object(verification_path)
    checks = verification.get("checks")
    timeframes = decision.get("timeframes")
    if (decision.get("cycle_id") != state.get("cycle_id") or decision.get("stage") != 3
            or decision.get("ea") != state.get("current_ea")
            or decision.get("verdict") != "REPAIR_REQUIRED"
            or verification.get("cycle_id") != state.get("cycle_id")
            or verification.get("stage") != 3
            or Path(verification.get("decision") or "").resolve() != decision_path.resolve()
            or str(verification.get("decision_sha256") or "").upper() != sha256(decision_path)
            or verification.get("passed") is not True or not isinstance(checks, dict)
            or not checks or any(value is not True for value in checks.values())
            or not isinstance(timeframes, dict) or not timeframes):
        raise ValueError("Stage 3 zero-trade decision is not bound by a passing same-cycle verifier")
    timeframe_order = ("M5", "M15", "H1")
    timeframe_names = list(timeframes)
    if timeframe_names != list(timeframe_order[:len(timeframe_names)]):
        raise ValueError("Stage 3 repair evidence does not contain an ordered completed timeframe prefix")
    zero_trade_timeframes = []
    for timeframe, metrics in timeframes.items():
        if not isinstance(metrics, dict) or type(metrics.get("row_count")) is not int or metrics["row_count"] <= 0:
            raise ValueError(f"Stage 3 {timeframe} lacks authenticated completed result rows")
        if (metrics.get("zero_trades") is True and metrics.get("trades") == 0
                and metrics.get("max_trades") == 0):
            zero_trade_timeframes.append(timeframe)
    repair_trigger = decision.get("repair_trigger")
    if isinstance(repair_trigger, dict) and repair_trigger.get("kind") == "completed_timeframe_zero_trades":
        trigger_tf = repair_trigger.get("timeframe")
        if trigger_tf not in timeframe_names or trigger_tf not in zero_trade_timeframes:
            raise ValueError("Stage 3 early repair trigger is not an authenticated zero-trade timeframe")
    elif isinstance(repair_trigger, dict) and repair_trigger.get("kind") == "all_timeframes_below_minimum_trade_count":
        if (set(timeframe_names) != set(timeframe_order)
                or any(type(metrics.get("max_trades")) is not int or metrics["max_trades"] >= 10
                       for metrics in timeframes.values())):
            raise ValueError("Stage 3 all-timeframe repair trigger lacks completed low-trade results")
    elif (set(timeframe_names) != set(timeframe_order)
          or any(type(metrics.get("max_trades")) is not int or metrics["max_trades"] >= 10
                 for metrics in timeframes.values())):
        raise ValueError("Stage 3 repair evidence lacks a completed zero-trade timeframe or all-timeframe failure")
    spec_path = Path(state.get("last_stage1_artifact") or "")
    spec = load_object(spec_path) if spec_path.is_file() else {}
    tester = spec.get("tester_config") if isinstance(spec.get("tester_config"), dict) else {}
    effective_inputs = tester.get("inputs", spec.get("effective_inputs", {}))
    repair_identity = {
        "cycle_id": str(state.get("cycle_id") or ""),
        "candidate_ea": str(state.get("candidate_ea") or state.get("current_ea") or ""),
        "decision_sha256": sha256(decision_path),
        "verification_sha256": sha256(verification_path),
        "effective_input_signature": hashlib.sha256(json.dumps(
            effective_inputs, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")).hexdigest().upper(),
    }
    repair_identity["identity_sha256"] = hashlib.sha256(json.dumps(
        repair_identity, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest().upper()
    repair_key = repair_identity["identity_sha256"]
    return {"decision": str(decision_path.resolve()),
            "decision_sha256": sha256(decision_path),
            "verification": str(verification_path.resolve()),
            "verification_sha256": sha256(verification_path),
            "zero_trade_timeframes": zero_trade_timeframes,
            "repair_trigger": repair_trigger,
            "event_id": f"{state.get('cycle_id')}:stage3:zero_trade_repair:{repair_key}",
            "dispatch_identity": repair_identity}


def authenticated_stage2_zero_trade_exhaustion_identity(state: dict, failure_path: Path) -> dict:
    """Authenticate the configured number of recorded no-change Stage 2 proposals."""
    failure = load_object(failure_path)
    cycle = str(state.get("cycle_id") or "")
    ea = str(state.get("candidate_ea") or state.get("current_ea") or "")
    if (failure.get("cycle_id") != cycle or failure.get("stage") != 2
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != "STAGE2_ZERO_TRADE_REPAIR_BUDGET_EXHAUSTED"
            or failure.get("rotation_eligible") is not True
            or failure.get("recovery_action") != "rotate_candidate"
            or failure.get("ea") != ea or failure.get("candidate_ea") != ea):
        raise ValueError("Stage 2 zero-trade failure does not authorize candidate rotation")
    exhaustion = failure.get("stage2_zero_trade_repair_exhaustion") or {}
    repair_record = failure.get("stage2_zero_trade_repair") or {}
    receipt_path = Path(str(exhaustion.get("path") or ""))
    root = (ROOT / "state" / "diagnostics" / "stage2_failures").resolve()
    receipt_path = receipt_path.resolve()
    receipt_path.relative_to(root)
    if (not receipt_path.is_file()
            or sha256(receipt_path) != str(exhaustion.get("sha256") or "").upper()):
        raise ValueError("Stage 2 zero-trade exhaustion receipt is missing or changed")
    receipt = load_object(receipt_path)
    source_path = Path(str(state.get("candidate_source") or state.get("stage2_candidate_source") or ""))
    spec_path = Path(str(state.get("last_stage1_artifact") or ""))
    zero_trade = authenticated_stage3_zero_trade_repair(state)
    budget = stage2_proposal_budget_status(state)
    if (zero_trade is None or receipt.get("record_type") != "stage2_zero_trade_repair_budget_exhausted"
            or receipt.get("cycle_id") != cycle or receipt.get("candidate_ea") != ea
            or receipt.get("episode_id") != exhaustion.get("episode_id")
            or repair_record.get("episode_id") != receipt.get("episode_id")
            or budget.get("status") != "verified"
            or repair_record.get("proposal_count") != budget.get("used")
            or repair_record.get("proposal_limit") != budget.get("limit")
            or exhaustion.get("proposal_limit") != budget.get("limit")
            or receipt.get("proposal_limit") != budget.get("limit")
            or receipt.get("proposal_attempts") != budget.get("limit")
            or receipt.get("stage3_zero_trade_evidence", {}).get("decision_sha256") != zero_trade["decision_sha256"]
            or receipt.get("stage3_zero_trade_evidence", {}).get("verification_sha256") != zero_trade["verification_sha256"]
            or not source_path.is_file() or not spec_path.is_file()
            or Path(str(receipt.get("source") or "")).resolve() != source_path.resolve()
            or str(receipt.get("source_sha256") or "").upper() != sha256(source_path).upper()
            or Path(str(receipt.get("stage1_spec") or "")).resolve() != spec_path.resolve()
            or str(receipt.get("stage1_spec_sha256") or "").upper() != sha256(spec_path).upper()):
        raise ValueError("Stage 2 zero-trade exhaustion receipt does not bind the current candidate")
    if (budget.get("status") != "verified" or budget.get("episode_id") != receipt.get("episode_id")
            or budget.get("used") != budget.get("limit")
            or budget.get("remaining") != 0):
        raise ValueError("Stage 2 zero-trade proposal ledger does not prove the configured cap is exhausted")
    attempts = receipt.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        raise ValueError("Stage 2 zero-trade exhaustion receipt has no transaction evidence")
    seen = set()
    proposal_total = 0
    episode_root = (ROOT / "state" / "diagnostics" / "stage2_repair_transactions" /
                    str(receipt.get("episode_id"))).resolve()
    for item in attempts:
        attempt_dir = Path(str(item.get("attempt_dir") or "")).resolve()
        attempt_dir.relative_to(episode_root)
        report_path = Path(str(item.get("report") or "")).resolve()
        outcome_path = Path(str(item.get("outcome") or "")).resolve()
        report_path.relative_to(attempt_dir)
        outcome_path.relative_to(attempt_dir)
        if (attempt_dir in seen or not report_path.is_file() or not outcome_path.is_file()
                or sha256(report_path) != str(item.get("report_sha256") or "").upper()
                or sha256(outcome_path) != str(item.get("outcome_sha256") or "").upper()):
            raise ValueError("Stage 2 zero-trade attempt evidence is missing, duplicated, or changed")
        seen.add(attempt_dir)
        report, outcome = load_object(report_path), load_object(outcome_path)
        report_attempts = report.get("attempts") if isinstance(report.get("attempts"), list) else []
        proposals = [record for record in report_attempts if isinstance(record, dict)
                     and type(record.get("attempt")) is int and record["attempt"] > 0
                     and (record.get("ai") or {}).get("status") in {"PROPOSED", "INVALID_RESPONSE"}]
        proposal_files = sorted(path for path in (attempt_dir / "proposals").glob("proposal-*.json")
                                if re.fullmatch(r"proposal-\d+\.json", path.name))
        proposed_count = sum((record.get("ai") or {}).get("status") == "PROPOSED"
                             for record in proposals)
        baseline = next((record for record in report_attempts
                         if isinstance(record, dict) and record.get("baseline") is True), None)
        if (report.get("cycle_id") != cycle or report.get("ea") != ea
                or outcome.get("episode_id") != receipt.get("episode_id")
                or outcome.get("outcome") != "REPAIR_REQUIRED_NO_CHANGE"
                or report.get("max_attempts") != stage2_repair_loop.MAX_PROPOSALS
                or not proposals or len(proposals) != item.get("proposal_attempts")
                or len(proposal_files) != proposed_count
                or any(not isinstance(load_object(path).get("response"), dict) for path in proposal_files)
                or not baseline
                or str(baseline.get("before_sha256") or "").upper() != sha256(source_path).upper()
                or str(report.get("baseline_source_sha256") or "").upper() not in
                    {"", sha256(source_path).upper()}):
            raise ValueError("Stage 2 zero-trade attempt was not a same-candidate no-change repair")
        journal = load_object(attempt_dir / "transaction-journal.json")
        if journal.get("status") != "ROLLED_BACK":
            raise ValueError("Stage 2 zero-trade attempt did not preserve its rollback boundary")
        proposal_total += len(proposals)
    if (proposal_total != budget.get("limit")
            or receipt.get("proposal_attempts") != proposal_total
            or failure.get("stage2_zero_trade_repair_exhaustion", {}).get("proposal_attempts") != proposal_total):
        raise ValueError("Stage 2 failure event attempt count differs from the exhaustion receipt")
    return {"cycle_id": cycle, "candidate_ea": ea,
            "episode_id": receipt["episode_id"], "proposal_attempts": proposal_total,
            "receipt": str(receipt_path), "receipt_sha256": sha256(receipt_path),
            "identity_sha256": hashlib.sha256(json.dumps(receipt, sort_keys=True,
                separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest().upper()}


def authenticated_stage2_compile_repair_exhaustion_identity(state: dict, failure_path: Path) -> dict:
    """Authenticate four rejected compile-repair proposals before candidate rotation."""
    failure = load_object(failure_path)
    cycle = str(state.get("cycle_id") or "")
    ea = str(state.get("candidate_ea") or state.get("current_ea") or "")
    exhaustion = failure.get("stage2_compile_repair_exhaustion") or {}
    receipt_path = Path(str(exhaustion.get("path") or "")).resolve()
    receipt_root = (ROOT / "state" / "diagnostics" / "stage2_failures").resolve()
    receipt_path.relative_to(receipt_root)
    if (failure.get("cycle_id") != cycle or failure.get("stage") != 2
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != "STAGE2_COMPILE_REPAIR_BUDGET_EXHAUSTED"
            or failure.get("failed_checks") != ["compile_repair_proposals_exhausted"]
            or failure.get("rotation_eligible") is not True
            or failure.get("recovery_action") != "rotate_candidate"
            or failure.get("ea") != ea or failure.get("candidate_ea") != ea
            or not receipt_path.is_file()
            or sha256(receipt_path) != str(exhaustion.get("sha256") or "").upper()):
        raise ValueError("Stage 2 compile-repair failure or receipt is not rotation eligible")
    receipt = load_object(receipt_path)
    source_path = Path(str(state.get("candidate_source") or state.get("stage2_candidate_source") or ""))
    spec_path = Path(str(state.get("last_stage1_artifact") or ""))
    tx_root = (ROOT / "state" / "diagnostics" / "stage2_repair_transactions").resolve()
    transactions = receipt.get("transactions")
    if (receipt.get("record_type") != "stage2_compile_repair_budget_exhausted"
            or receipt.get("cycle_id") != cycle or receipt.get("candidate_ea") != ea
            or receipt.get("proposal_limit") != stage2_repair_loop.MAX_PROPOSALS
            or receipt.get("proposal_attempts") != stage2_repair_loop.MAX_PROPOSALS
            or not source_path.is_file() or not spec_path.is_file()
            or Path(str(receipt.get("source") or "")).resolve() != source_path.resolve()
            or str(receipt.get("source_sha256") or "").upper() != sha256(source_path).upper()
            or Path(str(receipt.get("stage1_spec") or "")).resolve() != spec_path.resolve()
            or str(receipt.get("stage1_spec_sha256") or "").upper() != sha256(spec_path).upper()
            or not isinstance(transactions, list) or not transactions):
        raise ValueError("Stage 2 compile-repair receipt does not bind the current candidate and artifacts")
    total_proposals = 0
    seen_dirs = set()
    proposals_by_episode: dict[str, int] = {}
    for item in transactions:
        attempt_dir = Path(str(item.get("attempt_dir") or "")).resolve()
        attempt_dir.relative_to(tx_root)
        report_path = Path(str(item.get("report") or "")).resolve()
        outcome_path = Path(str(item.get("outcome") or "")).resolve()
        report_path.relative_to(attempt_dir)
        outcome_path.relative_to(attempt_dir)
        if (attempt_dir in seen_dirs or not report_path.is_file() or not outcome_path.is_file()
                or sha256(report_path) != str(item.get("report_sha256") or "").upper()
                or sha256(outcome_path) != str(item.get("outcome_sha256") or "").upper()):
            raise ValueError("Stage 2 compile-repair transaction evidence is missing, duplicated, or changed")
        seen_dirs.add(attempt_dir)
        report, outcome = load_object(report_path), load_object(outcome_path)
        attempts = report.get("attempts") if isinstance(report.get("attempts"), list) else []
        proposals = [record for record in attempts if isinstance(record, dict)
                     and type(record.get("attempt")) is int and record["attempt"] > 0
                     and (record.get("ai") or {}).get("status") in {"PROPOSED", "INVALID_RESPONSE"}]
        baseline = next((record for record in attempts
                         if isinstance(record, dict) and record.get("baseline") is True), {})
        baseline_sha = str(report.get("baseline_source_sha256") or baseline.get("before_sha256") or "").upper()
        journal = load_object(attempt_dir / "transaction-journal.json")
        proposal_files = sorted(path for path in (attempt_dir / "proposals").glob("proposal-*.json")
                                if re.fullmatch(r"proposal-\d+\.json", path.name))
        if (report.get("cycle_id") != cycle or report.get("ea") != ea
                or report.get("max_attempts") != stage2_repair_loop.MAX_PROPOSALS
                or baseline_sha != sha256(source_path).upper()
                or len(proposals) != item.get("proposal_attempts")
                or any(not record.get("adapter") or "deterministic" not in record for record in proposals)
                or len(proposal_files) != sum((record.get("ai") or {}).get("status") == "PROPOSED"
                                              for record in proposals)
                or any(not isinstance(load_object(path).get("response"), dict) for path in proposal_files)
                or outcome.get("outcome") not in {"REJECTED", "EXHAUSTED"}
                or outcome.get("episode_id") != item.get("episode_id")
                or attempt_dir.parent.name != item.get("episode_id")
                or journal.get("status") != "ROLLED_BACK"):
            raise ValueError("Stage 2 compile-repair transaction does not prove recorded candidate failures")
        total_proposals += len(proposals)
        proposals_by_episode[str(item["episode_id"])] = (
            proposals_by_episode.get(str(item["episode_id"]), 0) + len(proposals))
    if total_proposals != stage2_repair_loop.MAX_PROPOSALS:
        raise ValueError("Stage 2 compile-repair receipt does not prove four recorded model failures")
    ledger = ROOT / "state" / "ledgers" / "stage2_repair_episodes.sqlite3"
    if (sorted(proposals_by_episode) != sorted(receipt.get("episode_ids") or [])
            or any(stage2_repair_loop.episode_proposal_count(ledger, episode_id) < count
                   or stage2_repair_loop.episode_proposal_count(ledger, episode_id)
                   > stage2_repair_loop.MAX_PROPOSALS
                   for episode_id, count in proposals_by_episode.items())):
        raise ValueError("Stage 2 compile-repair ledgers do not authenticate the recorded proposal attempts")
    return {"cycle_id": cycle, "candidate_ea": ea, "episode_ids": sorted(proposals_by_episode),
            "proposal_attempts": total_proposals, "receipt": str(receipt_path),
            "receipt_sha256": sha256(receipt_path),
            "identity_sha256": hashlib.sha256(json.dumps(receipt, sort_keys=True,
                separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest().upper()}


def authenticated_stage2_zero_trade_retry_identity(state: dict, failure_path: Path,
                                                   failure: dict) -> dict | None:
    """Recognize an older generic failure only when its no-change repair bundle is exact."""
    if (failure.get("cycle_id") != state.get("cycle_id") or failure.get("stage") != 2
            or failure.get("status") != "failed" or failure.get("rotation_eligible") is not False
            or failure.get("failure_kind") != "STAGE2_EXECUTOR_FAILURE"):
        return None
    zero_trade = authenticated_stage3_zero_trade_repair(state)
    if zero_trade is None:
        return None
    try:
        rows = [json.loads(line) for line in str(failure.get("error") or "").splitlines()
                if line.strip().startswith("{")]
        outcome_record = next((row for row in rows if row.get("outcome") == "REPAIR_REQUIRED_NO_CHANGE"), None)
        report_record = next((row for row in rows if row.get("report")), None)
        state_rows = [json.loads(line) for line in str(state.get("stage2_repair_reason") or "{}").splitlines()
                      if line.strip().startswith("{")]
        state_report = next((row.get("report") for row in state_rows if row.get("report")), None)
        state_attempt = next((row.get("attempt_dir") for row in state_rows if row.get("attempt_dir")), None)
        attempt_dir = Path(str((outcome_record or {}).get("attempt_dir") or "")).resolve()
        tx_root = (ROOT / "state" / "diagnostics" / "stage2_repair_transactions").resolve()
        attempt_dir.relative_to(tx_root)
        episode_id = str((outcome_record or {}).get("episode_id") or "")
        report_path = attempt_dir / "repair-loop-report.json"
        outcome_path = attempt_dir / "outcome.json"
        canonical_report = (ROOT / "state" / "reports" /
                           f"{state.get('cycle_id')}.stage2.repair-loop.json").resolve()
        if (not episode_id or attempt_dir.parent.name != episode_id
                or not report_path.is_file() or not outcome_path.is_file()
                or Path(str((report_record or {}).get("report") or "")).resolve() != canonical_report
                or Path(str(state_report or "")).resolve() != canonical_report
                or Path(str(state_attempt or "")).resolve() != attempt_dir):
            return None
        report, outcome = load_object(report_path), load_object(outcome_path)
        source_path = Path(str(state.get("candidate_source") or state.get("stage2_candidate_source") or ""))
        if (outcome.get("outcome") != "REPAIR_REQUIRED_NO_CHANGE"
                or outcome.get("episode_id") != episode_id
                or report.get("cycle_id") != state.get("cycle_id")
                or report.get("ea") != (state.get("candidate_ea") or state.get("current_ea"))
                or not source_path.is_file()):
            return None
        baseline = next((row for row in report.get("attempts", [])
                         if isinstance(row, dict) and row.get("baseline") is True), None)
        if (not baseline or str(baseline.get("before_sha256") or "").upper() != sha256(source_path).upper()
                or str(report.get("baseline_source_sha256") or "").upper() not in
                    {"", sha256(source_path).upper()}):
            return None
        budget = stage2_proposal_budget_status(state)
        if (budget.get("status") != "verified" or budget.get("episode_id") != episode_id
                or budget.get("used", 0) < 1 or not budget.get("remaining")):
            return None
        return {"episode_id": episode_id, "budget": budget, "zero_trade_evidence": zero_trade,
                "attempt_dir": str(attempt_dir), "report": str(report_path),
                "report_sha256": sha256(report_path), "outcome_sha256": sha256(outcome_path)}
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None


def authenticated_stale_stage3_metadata_retry(state: dict, control: dict, failure_path: Path,
                                                   failure: dict, db_path: Path) -> dict | None:
    """Authorize one same-candidate retry for proven old-cycle Stage 3 pointers."""
    cycle = str(state.get("cycle_id") or "")
    if (not cycle or state.get("stage_status") != "stage2_repair_required"
            or int(state.get("stage2_stale_stage3_metadata_retry_count", 0) or 0) >= 1
            or failure.get("cycle_id") != cycle or failure.get("stage") != 2
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != "STAGE2_EXECUTOR_FAILURE"
            or failure.get("error") != "Stage 3 zero-trade repair evidence is missing"
            or failure.get("rotation_eligible") is not False):
        return None
    if hashlib.sha256(str(failure.get("error") or "").encode("utf-8")).hexdigest().upper() != str(failure.get("error_sha256") or "").upper():
        return None
    decision_value = state.get("stage3_zero_trade_decision")
    verification_value = state.get("stage3_zero_trade_verification")
    if not decision_value or not verification_value:
        return None
    decision_path, verification_path = Path(decision_value), Path(verification_value)
    decision_suffix, verification_suffix = ".stage3.decision.json", ".stage3.verification.json"
    decision_cycle = decision_path.name[:-len(decision_suffix)] if decision_path.name.endswith(decision_suffix) else None
    verification_cycle = verification_path.name[:-len(verification_suffix)] if verification_path.name.endswith(verification_suffix) else None
    if not decision_cycle or decision_cycle != verification_cycle or decision_cycle == cycle:
        return None
    source_path = Path(state.get("candidate_source") or "")
    spec_path = Path(state.get("last_stage1_artifact") or "")
    stage1_event_path = Path(state.get("last_stage1_event") or "")
    if not all(path.is_file() for path in (source_path, spec_path, stage1_event_path, failure_path)):
        return None
    if (str(source_path.resolve()) != str(Path(failure.get("source") or "").resolve())
            or sha256(source_path) != str(failure.get("source_sha256") or "").upper()
            or str(spec_path.resolve()) != str(Path(failure.get("stage1_spec") or "").resolve())
            or sha256(spec_path) != str(failure.get("stage1_spec_sha256") or "").upper()):
        return None
    stage1_event = load_object(stage1_event_path)
    if (stage1_event.get("cycle_id") != cycle or stage1_event.get("stage") != 1
            or stage1_event.get("status") != "complete"
            or stage1_event.get("artifact_sha256") != sha256(spec_path)
            or stage1_event.get("consumed") is not True
            or stage1_event.get("consumed_action") != "dispatch_stage2"):
        return None
    execution_id = str(control.get("last_stage2_execution_id") or "")
    if not execution_id or execution_status(db_path, execution_id) != "failed":
        return None
    failure_sha = sha256(failure_path)
    identity = {
        "cycle_id": cycle,
        "candidate_ea": str(state.get("candidate_ea") or state.get("current_ea") or ""),
        "failure_event_sha256": failure_sha,
        "source_sha256": sha256(source_path),
        "stage1_spec_sha256": sha256(spec_path),
        "prior_execution_id": execution_id,
        "stale_stage3_pointer_cycle": decision_cycle,
    }
    identity["identity_sha256"] = hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest().upper()
    archive_dir = failure_path.parent.parent / "diagnostics" / "stage2_failures"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / f"{cycle}.stage2.failed.pre_stale_metadata_retry.{failure_sha[:16]}.json"
    if archive_path.exists():
        if sha256(archive_path) != failure_sha:
            return None
    else:
        shutil.copy2(failure_path, archive_path)
        if sha256(archive_path) != failure_sha:
            archive_path.unlink(missing_ok=True)
            return None
    identity["archived_failure_path"] = str(archive_path.resolve())
    return identity


def authenticated_stage2_tester_diagnostic_removal_retry(state: dict, control: dict,
                                                         failure_path: Path, failure: dict,
                                                         db_path: Path) -> dict | None:
    """Authorize one retry for the removed Stage 2 tester-launch gate only."""
    cycle = str(state.get("cycle_id") or "")
    if (not cycle or state.get("stage_status") != "stage2_repair_required"
            or int(state.get("stage2_tester_diagnostic_removal_retry_count", 0) or 0) >= 1
            or failure.get("schema_version") != 2 or failure.get("cycle_id") != cycle
            or failure.get("stage") != 2 or failure.get("status") != "failed"
            or failure.get("event_id") != f"{cycle}:stage2:failed"
            or failure.get("failure_kind") != "STAGE2_EXECUTOR_FAILURE"
            or failure.get("rotation_eligible") is not False
            or failure.get("exception_type") != "RuntimeError"
            or not str(failure.get("error") or "").startswith(
                "unowned terminal/tester process appeared during Stage 2 run:")
            or "verify_live_tester_process_census" not in str(failure.get("traceback") or "")
            or "run_diagnostic" not in str(failure.get("traceback") or "")):
        return None
    if hashlib.sha256(str(failure.get("error") or "").encode("utf-8")).hexdigest().upper() != str(failure.get("error_sha256") or "").upper():
        return None

    state_dir = failure_path.parent.parent.resolve()
    expected_failure_path = state_dir / "events" / f"{cycle}.stage2.failed.json"
    source_path = Path(state.get("candidate_source") or state.get("stage2_candidate_source") or "")
    spec_path = Path(state.get("last_stage1_artifact") or "")
    stage1_event_path = Path(state.get("last_stage1_event") or "")
    verification_path = Path(failure.get("verification") or "")
    compile_path = Path(failure.get("compile_attempt") or "")
    if (failure_path.resolve() != expected_failure_path.resolve()
            or not all(path.is_file() for path in (source_path, spec_path, stage1_event_path,
                                                    verification_path, compile_path))
            or source_path.resolve() != Path(str(failure.get("source") or "")).resolve()
            or source_path.parent.resolve() != (state_dir / "candidates").resolve()
            or spec_path.resolve() != Path(str(failure.get("stage1_spec") or "")).resolve()
            or spec_path.parent.resolve() != (state_dir / "specs").resolve()
            or verification_path.resolve() != (state_dir / "verification" / f"{cycle}.stage2.verification.json").resolve()
            or compile_path.resolve() != (state_dir / "attempts" / f"{cycle}.stage2.compile.latest.json").resolve()
            or str(failure.get("source_sha256") or "").upper() != sha256(source_path).upper()
            or str(failure.get("stage1_spec_sha256") or "").upper() != sha256(spec_path).upper()):
        return None
    try:
        verification = load_object(verification_path)
        compile_attempt = load_object(compile_path)
        stage1_event = load_object(stage1_event_path)
    except (OSError, ValueError, TypeError):
        return None
    ea = str(state.get("candidate_ea") or state.get("current_ea") or "")
    if (verification.get("schema_version") != 2 or verification.get("stage") != 2
            or verification.get("cycle_id") != cycle or verification.get("ea") != ea
            or verification.get("passed") is not True
            or not all(verification.get("checks", {}).values())
            or str(verification.get("source_sha256") or "").upper() != sha256(source_path).upper()
            or str(verification.get("stage1_spec_sha256") or "").upper() != sha256(spec_path).upper()
            or compile_attempt.get("passed") is not True
            or compile_attempt.get("cycle_id") != cycle
            or compile_attempt.get("stage") != 2
            or compile_attempt.get("ea") != ea
            or compile_attempt.get("errors") != 0 or compile_attempt.get("warnings") != 0
            or compile_attempt.get("fresh_ex5") is not True
            or str(compile_attempt.get("mq5_sha256") or "").upper() != sha256(source_path).upper()
            or stage1_event.get("cycle_id") != cycle or stage1_event.get("stage") != 1
            or stage1_event.get("status") != "complete"
            or stage1_event.get("artifact_sha256") != sha256(spec_path)
            or stage1_event.get("consumed") is not True
            or stage1_event.get("consumed_action") != "dispatch_stage2"):
        return None

    executor_path = state_dir / "kcos_stage2_autonomous_executor.py"
    publisher_path = state_dir / "kcos_stage2_publish_v2.py"
    try:
        executor_source = executor_path.read_text(encoding="utf-8")
        publisher_source = publisher_path.read_text(encoding="utf-8")
    except OSError:
        return None
    if ("run_diagnostic(" in executor_source or "--tester-diagnostic" in executor_source
            or "tester diagnostic binding is required" in publisher_source
            or "--tester-diagnostic" in publisher_source):
        return None

    execution_id = str(control.get("last_stage2_execution_id") or "")
    job_id = str((control.get("stage_jobs") or {}).get("2") or "")
    try:
        connection = sqlite3.connect(str(db_path))
        try:
            active_stage2 = connection.execute(
                "SELECT 1 FROM executions WHERE job_id=? AND status IN ('claimed','running') LIMIT 1",
                (job_id,)).fetchone() is not None
        finally:
            connection.close()
    except sqlite3.Error:
        return None
    if (not execution_id or not job_id or active_stage2
            or execution_status(db_path, execution_id) != "failed"
            or stage2_executor_lock_active(state_dir) is not False
            or stage2_failure_execution_receipt(
                db_path, job_id, execution_id, failure_path,
                expected_error=str(failure.get("error") or "")) is None):
        return None

    failure_sha = sha256(failure_path)
    identity = {
        "cycle_id": cycle,
        "candidate_ea": ea,
        "failure_event_sha256": failure_sha,
        "source_sha256": sha256(source_path),
        "stage1_spec_sha256": sha256(spec_path),
        "verification_sha256": sha256(verification_path),
        "compile_attempt_sha256": sha256(compile_path),
        "executor_sha256": sha256(executor_path),
        "publisher_sha256": sha256(publisher_path),
        "prior_execution_id": execution_id,
        "retry_scope": "one_same_candidate_retry_after_stage2_tester_diagnostic_removal",
    }
    identity["identity_sha256"] = hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest().upper()
    archive_dir = state_dir / "diagnostics" / "stage2_failures"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / f"{cycle}.stage2.failed.pre_tester_diagnostic_retry.{failure_sha[:16]}.json"
    if archive_path.exists():
        if sha256(archive_path) != failure_sha:
            return None
    else:
        shutil.copy2(failure_path, archive_path)
        if sha256(archive_path) != failure_sha:
            archive_path.unlink(missing_ok=True)
            return None
    identity["archived_failure_path"] = str(archive_path.resolve())
    return identity


def validate_stage45_continuation(state: dict, completed_run: int,
                                 state_dir: Path = PROJECT_STATE) -> tuple[bool, str, dict | None]:
    """Authenticate the immediately preceding Stage 4.5 run before dispatching its successor."""
    cycle, ea = state.get("cycle_id"), state.get("current_ea")
    if not isinstance(cycle, str) or not cycle or not isinstance(ea, str) or not ea:
        return False, "Stage 4.5 continuation lacks canonical cycle/EA identity", None
    prefix = f"{cycle}.stage45.run{completed_run}"
    decision_path = state_dir / "decisions" / f"{prefix}.decision.json"
    verification_path = state_dir / "verification" / f"{prefix}.verification.json"
    event_path = state_dir / "events" / f"{prefix}.complete.json"
    runtime_path = state_dir / "stage45_v2" / f"{prefix}.runtime.json"
    try:
        decision, verification, event, runtime = (
            load_object(path) for path in (decision_path, verification_path, event_path, runtime_path))
        timeframe = decision.get("selected_tf")
        complete_path = state_dir / "stage45_v2" / f"{cycle}.stage45.{timeframe}.run{completed_run}.complete.json"
        complete = load_object(complete_path)
        csv_path = Path(str(decision.get("csv") or ""))
        checks = verification.get("checks")
        structural = ([check for check in checks
                       if check.get("name") not in {"verdict", "all Stage 3 gates applied"}]
                      if isinstance(checks, list) else [])
        opt_path = Path(str(runtime.get("opt_path") or ""))
        narrow_ini_path = Path(str(runtime.get("narrow_ini") or ""))
        valid = (
            decision.get("cycle_id") == cycle and decision.get("ea") == ea
            and decision.get("stage") == "4.5" and decision.get("run_number") == completed_run
            and decision.get("run_count_required") == 3 and bool(timeframe)
            and decision.get("verdict") == "NARROW_OPT_COMPLETE"
            and int(decision.get("qualifying_row_count") or 0) > 0
            and int(decision.get("passes_observed") or 0) > 0
            and verification.get("cycle_id") == cycle and verification.get("stage") == "4.5"
            and verification.get("passed") is True and bool(structural)
            and all(check.get("passed") is True for check in structural)
            and event.get("cycle_id") == cycle and event.get("ea") == ea
            and str(event.get("stage")) == "4.5" and event.get("run_number") == completed_run
            and event.get("timeframe") == timeframe and event.get("verdict") == decision.get("verdict")
            and event.get("verification_passed") is True and event.get("consumed") is False
            and event.get("decision") == str(decision_path.resolve())
            and event.get("verification") == str(verification_path.resolve())
            and event.get("decision_sha256") == sha256(decision_path)
            and event.get("verification_sha256") == sha256(verification_path)
            and csv_path.is_file() and Path(str(event.get("csv") or "")).resolve() == csv_path.resolve()
            and sha256(csv_path) == decision.get("csv_sha256") == event.get("csv_sha256")
            and runtime.get("cycle_id") == cycle and runtime.get("ea") == ea
            and runtime.get("run_number") == completed_run and runtime.get("run_count_required") == 3
            and runtime.get("timeframe") == timeframe and runtime.get("status") == "TIMEFRAME_COMPLETE"
            and runtime.get("terminal_closed_after_run") is True and runtime.get("timed_out") is not True
            and int(runtime.get("passes") or 0) > 0
            and complete.get("cycle_id") == cycle and complete.get("timeframe") == timeframe
            and complete.get("status") == "TIMEFRAME_COMPLETE" and complete.get("timed_out") is False
            and int(complete.get("passes") or 0) > 0
            and complete.get("narrow_ini_sha256") == runtime.get("narrow_ini_sha256")
            and complete.get("opt_sha256") == runtime.get("opt_sha256")
            and opt_path.is_file() and sha256(opt_path) == runtime.get("opt_sha256")
            and narrow_ini_path.is_file() and sha256(narrow_ini_path) == runtime.get("narrow_ini_sha256")
        )
        if not valid:
            return False, f"Stage 4.5 run {completed_run} completion evidence failed identity, hash, or structural checks", None
        identity = {
            "cycle_id": cycle, "stage": "4.5", "run_number": completed_run + 1,
            "preceding_run_number": completed_run, "preceding_run_event_sha256": sha256(event_path),
            "preceding_run_decision_sha256": sha256(decision_path),
            "preceding_run_verification_sha256": sha256(verification_path),
            "range_planner_revision": "clean-grid-decimal-start-v2",
        }
        return True, f"Stage 4.5 run {completed_run} is authenticated; run {completed_run + 1} may proceed", identity
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        return False, f"Stage 4.5 run {completed_run} continuation evidence is unavailable: {exc}", None


def stage2_compile_source_repair_used(state: dict, cycle_id: str) -> bool:
    """Count only a source-repair dispatch recorded for this exact cycle."""
    identity = state.get("stage2_compile_source_repair_identity")
    recorded_cycle = state.get("stage2_compile_source_repair_cycle_id")
    if not recorded_cycle and isinstance(identity, dict):
        recorded_cycle = identity.get("cycle_id")
    count = state.get("stage2_compile_source_repair_count", 0)
    return recorded_cycle == cycle_id and type(count) is int and count >= 1


def stage2_compile_source_repair_followup_count(state: dict, cycle_id: str) -> int:
    """Ignore mutable follow-up counters left by a previously completed cycle."""
    if state.get("stage2_compile_source_repair_followup_cycle_id") != cycle_id:
        return 0
    count = state.get("stage2_compile_source_repair_followup_count", 0)
    return count if type(count) is int and count >= 0 else 0


def stage2_compile_source_repair_second_followup_count(state: dict, cycle_id: str) -> int:
    """Read the second follow-up count only when it is bound to this cycle."""
    if state.get("stage2_compile_source_repair_second_followup_cycle_id") != cycle_id:
        return 0
    count = state.get("stage2_compile_source_repair_second_followup_count", 0)
    return count if type(count) is int and count >= 0 else 0


def authenticated_stage2_source_repair_policy_followup(state: dict, failure: dict,
                                                        failure_path: Path,
                                                        db_path: Path) -> dict | None:
    """Authorize one rerun only when a source repair was rolled back by the known lock false-positive."""
    cycle = str(state.get("cycle_id") or "")
    if (not cycle or state.get("current_stage") != "stage2_pending"
            or state.get("stage_status") != "stage2_repair_required"
            or state.get("next_action") != "repair_stage2_failure"
            or state.get("stage2_compile_source_repair_cycle_id") != cycle
            or state.get("stage2_compile_source_repair_count") != 1
            or stage2_compile_source_repair_followup_count(state, cycle) != 0
            or Path(str(state.get("stage2_failure_event") or "")).resolve() != failure_path.resolve()
            or not stage2_compile_source_repair_used(state, cycle)):
        return None
    identity = state.get("stage2_compile_source_repair_identity")
    source_path = Path(state.get("candidate_source") or state.get("stage2_candidate_source") or "")
    spec_path = Path(state.get("last_stage1_artifact") or "")
    compile_path = Path(failure.get("compile_attempt") or "")
    if not isinstance(identity, dict) or not all(p.is_file() for p in
                                                  (failure_path, source_path, spec_path, compile_path)):
        return None
    if (identity.get("cycle_id") != cycle
            or identity.get("compile_attempt_sha256") != sha256(compile_path)
            or identity.get("source_sha256") != sha256(source_path)
            or identity.get("stage1_spec_sha256") != sha256(spec_path)
            or failure.get("cycle_id") != cycle or failure.get("stage") != 2
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != "COMPILE_INTEGRITY_FAILURE"
            or Path(str(failure.get("source") or "")).resolve() != source_path.resolve()
            or str(failure.get("source_sha256") or "").upper() != sha256(source_path).upper()
            or Path(str(failure.get("stage1_spec") or "")).resolve() != spec_path.resolve()
            or str(failure.get("stage1_spec_sha256") or "").upper() != sha256(spec_path).upper()
            or Path(str(failure.get("compile_attempt") or "")).resolve() != compile_path.resolve()):
        return None
    execution_id = str(state.get("stage2_compile_source_repair_execution_id") or "")
    job_id = str((state.get("stage_jobs") or {}).get("2")
                 or (load_object(ROOT / "state" / "kcos_control_v2.json").get("stage_jobs") or {}).get("2") or "")
    if (not execution_id or not job_id or execution_status(db_path, execution_id) != "failed"):
        return None
    try:
        connection = sqlite3.connect(str(db_path))
        try:
            active = connection.execute(
                "SELECT 1 FROM executions WHERE job_id=? AND status IN ('claimed','running') LIMIT 1",
                (job_id,)).fetchone() is not None
        finally:
            connection.close()
    except sqlite3.Error:
        return None
    if active:
        return None
    try:
        reason_records = [json.loads(line) for line in str(state.get("stage2_repair_reason") or "{}").splitlines()
                          if line.strip().startswith("{")]
        repair_result = next((item for item in reason_records if item.get("report")), {})
        reason = next((item for item in reason_records if item.get("attempt_dir")), {})
        outcome_reason = next((item for item in reason_records if item.get("outcome")), {})
        attempt_dir = Path(reason["attempt_dir"]).resolve()
        tx_root = (ROOT / "state" / "diagnostics" / "stage2_repair_transactions").resolve()
        attempt_dir.relative_to(tx_root)
        report_path = Path(repair_result["report"]).resolve()
        canonical_failure_path = attempt_dir / "canonical-stage2-failure-event.json"
        outcome = load_object(attempt_dir / "outcome.json")
        journal = load_object(attempt_dir / "transaction-journal.json")
        report = load_object(attempt_dir / "repair-loop-report.json")
        attempts = report.get("attempts") or []
        first_repair = next((item for item in attempts if item.get("attempt") == 1), {})
        rejected_repair = next((item for item in attempts if item.get("attempt") == 2), {})
        candidate_compile_path = next((p for p in (attempt_dir / "candidate-output").glob("*stage2.compile.latest.json")), None)
        candidate_compile = load_object(candidate_compile_path) if candidate_compile_path else {}
        compile_log = Path(candidate_compile.get("log") or "")
        log_bytes = compile_log.read_bytes() if compile_log.is_file() else b""
        log_encoding = "utf-16" if log_bytes.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8"
        log_text = log_bytes.decode(log_encoding, errors="replace")
        baseline_hash = str((attempts[0] or {}).get("before_sha256") or "").upper()
        intermediate_hash = str((first_repair.get("source_admission_refresh") or {}).get("source_sha256") or "").upper()
        intermediate_sources = [p for p in (attempt_dir / "candidate-output").glob("*.mq5")
                                if sha256(p).upper() == intermediate_hash]
        expected_lock_rejection = "locked strategy inputs, regimes, or score callback changed"
        ai_changes = ((rejected_repair.get("ai") or {}).get("changes") or [])
        exact_ticket_changes = (
            len(ai_changes) == 2
            and any("PositionGetTicket(PositionGetInteger(POSITION_TICKET))" in str(change)
                    and "explicit ulong" in str(change) for change in ai_changes)
            and any("short-signal close path" in str(change)
                    and "type-safe correction" in str(change) for change in ai_changes)
        )
        warnings_are_exact = (len(re.findall(r"(?im)^.+\.mq5\(\d+,\d+\)\s*:\s*warning\s+43\s*:", log_text)) == 2
                              and "long' to 'uint'" in log_text)
        if (outcome_reason.get("episode_id") != outcome.get("episode_id")
                or outcome_reason.get("outcome") != "REJECTED"
                or outcome.get("outcome") != "REJECTED"
                or outcome.get("episode_id") != attempt_dir.parent.name
                or journal.get("status") != "ROLLED_BACK"
                or report.get("cycle_id") != cycle
                or report.get("ea") != state.get("candidate_ea", state.get("current_ea"))
                or not canonical_failure_path.is_file()
                or identity.get("failure_event_sha256") != sha256(canonical_failure_path)
                or not baseline_hash or baseline_hash != sha256(source_path).upper()
                or not intermediate_hash
                or (first_repair.get("source_admission_refresh") or {}).get("source_sha256", "").upper() != intermediate_hash
                or len(intermediate_sources) != 1
                or candidate_compile.get("cycle_id") != cycle or candidate_compile.get("stage") != 2
                or candidate_compile.get("passed") is not False
                or candidate_compile.get("errors") != 0 or candidate_compile.get("warnings") != 2
                or candidate_compile.get("fresh_ex5") is not True
                or str(candidate_compile.get("mq5_sha256") or "").upper() != intermediate_hash
                or not warnings_are_exact
                or rejected_repair.get("before_sha256", "").upper() != intermediate_hash
                or rejected_repair.get("after_sha256", "").upper() != intermediate_hash
                or (rejected_repair.get("ai_application") or {}).get("status") != "PATCH_REJECTED"
                or (rejected_repair.get("ai_application") or {}).get("reason") != expected_lock_rejection
                or not exact_ticket_changes):
            return None
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, sqlite3.Error):
        return None
    followup_identity = {
        "cycle_id": cycle,
        "candidate_ea": str(state.get("candidate_ea", state.get("current_ea")) or ""),
        "source_sha256": sha256(source_path),
        "failure_event_sha256": sha256(failure_path),
        "compile_attempt_sha256": sha256(compile_path),
        "stage1_spec_sha256": sha256(spec_path),
        "repair_report_sha256": sha256(report_path),
        "repair_transaction_outcome_sha256": sha256(attempt_dir / "outcome.json"),
        "retry_scope": "one_same_candidate_stage2_repair_after_compiler_proven_ticket_fix_lock_false_positive",
    }
    followup_identity["identity_sha256"] = hashlib.sha256(json.dumps(
        followup_identity, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest().upper()
    return followup_identity


def authenticated_stage2_source_repair_prompt_budget_followup(state: dict, failure: dict,
                                                               failure_path: Path,
                                                               db_path: Path) -> dict | None:
    """Retry once when the previous repair transaction never reached the model."""
    cycle = str(state.get("cycle_id") or "")
    if (not cycle or state.get("current_stage") != "stage2_pending"
            or state.get("stage_status") != "stage2_repair_required"
            or state.get("next_action") != "repair_stage2_failure"
            or state.get("stage2_compile_source_repair_cycle_id") != cycle
            or state.get("stage2_compile_source_repair_count") != 1
            or stage2_compile_source_repair_followup_count(state, cycle) != 0
            or Path(str(state.get("stage2_failure_event") or "")).resolve() != failure_path.resolve()
            or not stage2_compile_source_repair_used(state, cycle)):
        return None
    identity = state.get("stage2_compile_source_repair_identity")
    source_path = Path(state.get("candidate_source") or state.get("stage2_candidate_source") or "")
    spec_path = Path(state.get("last_stage1_artifact") or "")
    compile_path = Path(str(failure.get("compile_attempt") or ""))
    if not isinstance(identity, dict) or not all(path.is_file()
                                                  for path in (failure_path, source_path, spec_path, compile_path)):
        return None
    if (identity.get("cycle_id") != cycle
            or identity.get("source_sha256") != sha256(source_path)
            or identity.get("stage1_spec_sha256") != sha256(spec_path)
            or identity.get("compile_attempt_sha256") != sha256(compile_path)
            or not identity.get("failure_event_sha256")
            or failure.get("cycle_id") != cycle or failure.get("stage") != 2
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != "COMPILE_INTEGRITY_FAILURE"
            or failure.get("rotation_eligible") is not False
            or Path(str(failure.get("source") or "")).resolve() != source_path.resolve()
            or str(failure.get("source_sha256") or "").upper() != sha256(source_path).upper()
            or Path(str(failure.get("stage1_spec") or "")).resolve() != spec_path.resolve()
            or str(failure.get("stage1_spec_sha256") or "").upper() != sha256(spec_path).upper()
            or compile_path.resolve() != Path(str(failure.get("compile_attempt") or "")).resolve()):
        return None
    execution_id = str(state.get("stage2_compile_source_repair_execution_id") or "")
    job_id = str((state.get("stage_jobs") or {}).get("2")
                 or (load_object(ROOT / "state" / "kcos_control_v2.json").get("stage_jobs") or {}).get("2") or "")
    if not execution_id or not job_id or execution_status(db_path, execution_id) != "failed":
        return None
    try:
        connection = sqlite3.connect(str(db_path), timeout=1.0)
        try:
            active = connection.execute(
                "SELECT 1 FROM executions WHERE job_id=? AND status IN ('claimed','running') LIMIT 1",
                (job_id,)).fetchone() is not None
        finally:
            connection.close()
        if active:
            return None
        records = [json.loads(line) for line in str(state.get("stage2_repair_reason") or "{}").splitlines()
                   if line.strip().startswith("{")]
        report_record = next((item for item in records if item.get("report")), {})
        outcome_record = next((item for item in records if item.get("attempt_dir")), {})
        attempt_dir = Path(outcome_record["attempt_dir"]).resolve()
        attempt_dir.relative_to((ROOT / "state" / "diagnostics" / "stage2_repair_transactions").resolve())
        report_path = Path(report_record["report"]).resolve()
        if not report_path.is_file():
            return None
        report = load_object(attempt_dir / "repair-loop-report.json")
        outcome = load_object(attempt_dir / "outcome.json")
        journal = load_object(attempt_dir / "transaction-journal.json")
        archived_failure_path = attempt_dir / "canonical-stage2-failure-event.json"
        archived_failure = load_object(archived_failure_path)
        attempts = report.get("attempts") or []
        first_repair = next((item for item in attempts if item.get("attempt") == 1), {})
        ai = first_repair.get("ai") or {}
        baseline_sha = str((attempts[0] or {}).get("before_sha256") or "").upper()
        if (report.get("cycle_id") != cycle
                or report.get("ea") != failure.get("ea")
                or report.get("max_attempts") != 4
                or outcome.get("outcome") != "REJECTED"
                or outcome.get("episode_id") != attempt_dir.parent.name
                or journal.get("status") != "ROLLED_BACK"
                or archived_failure.get("cycle_id") != cycle
                or sha256(archived_failure_path) != identity.get("failure_event_sha256")
                or not baseline_sha or baseline_sha != sha256(source_path).upper()
                or first_repair.get("after_sha256", "").upper() != baseline_sha
                or ai.get("status") != "FAILED"
                or "repair prompt exceeds Windows-safe one-shot command budget" not in str(ai.get("error") or "")):
            return None
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, sqlite3.Error):
        return None
    retry_identity = {
        "cycle_id": cycle,
        "candidate_ea": str(failure.get("ea") or ""),
        "source_sha256": sha256(source_path),
        "stage1_spec_sha256": sha256(spec_path),
        "failure_event_sha256": sha256(failure_path),
        "prior_compile_attempt_sha256": sha256(compile_path),
        "repair_report_sha256": sha256(report_path),
        "repair_transaction_outcome_sha256": sha256(attempt_dir / "outcome.json"),
        "prior_execution_id": execution_id,
        "retry_scope": "same_candidate_repair_retry_after_prompt_budget_infrastructure_failure",
    }
    retry_identity["identity_sha256"] = hashlib.sha256(json.dumps(
        retry_identity, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest().upper()
    return retry_identity


def stage2_compile_repair_candidate_failure_count(state: dict, source_path: Path) -> int:
    """Count only same-cycle, same-source model responses in rolled-back repair transactions."""
    cycle = str(state.get("cycle_id") or "")
    ea = str(state.get("candidate_ea") or state.get("current_ea") or "")
    source_hash = sha256(source_path)
    total = 0
    tx_root = ROOT / "state" / "diagnostics" / "stage2_repair_transactions"
    for report_path in tx_root.glob("*/*/repair-loop-report.json"):
        try:
            report = load_object(report_path)
            attempts = report.get("attempts") if isinstance(report.get("attempts"), list) else []
            baseline = next((item for item in attempts
                             if isinstance(item, dict) and item.get("baseline") is True), {})
            baseline_hash = str(report.get("baseline_source_sha256") or baseline.get("before_sha256") or "").upper()
            if (report.get("cycle_id") != cycle or report.get("ea") != ea
                    or report.get("max_attempts") != stage2_repair_loop.MAX_PROPOSALS
                    or baseline_hash != source_hash):
                continue
            proposals = [item for item in attempts if isinstance(item, dict)
                         and type(item.get("attempt")) is int and item["attempt"] > 0
                         and (item.get("ai") or {}).get("status") in {"PROPOSED", "INVALID_RESPONSE"}
                         and item.get("adapter") and "deterministic" in item]
            if not proposals:
                continue
            attempt_dir = report_path.parent
            outcome_path, journal_path = attempt_dir / "outcome.json", attempt_dir / "transaction-journal.json"
            if not outcome_path.is_file() or not journal_path.is_file():
                continue
            outcome, journal = load_object(outcome_path), load_object(journal_path)
            proposal_files = sorted(path for path in (attempt_dir / "proposals").glob("proposal-*.json")
                                    if re.fullmatch(r"proposal-\d+\.json", path.name))
            if (outcome.get("outcome") not in {"REJECTED", "EXHAUSTED"}
                    or outcome.get("episode_id") != attempt_dir.parent.name
                    or journal.get("status") != "ROLLED_BACK"
                    or len(proposal_files) != len(proposals)
                    or any(not isinstance(load_object(path).get("response"), dict)
                           for path in proposal_files)):
                continue
            total += len(proposals)
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
            continue
    return total


def authenticated_stage2_source_repair_proposal_budget_followup(state: dict, failure: dict,
                                                                 failure_path: Path,
                                                                 db_path: Path) -> dict | None:
    """Continue a same-candidate repair only when a valid proposal slot remains."""
    cycle = str(state.get("cycle_id") or "")
    if (not cycle or state.get("current_stage") != "stage2_pending"
            or state.get("stage_status") != "stage2_repair_required"
            or not stage2_compile_source_repair_used(state, cycle)
            or stage2_compile_source_repair_followup_count(state, cycle) not in {0, 1}
            or stage2_compile_source_repair_second_followup_count(state, cycle) != 0
            or failure.get("cycle_id") != cycle or failure.get("stage") != 2
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != "COMPILE_INTEGRITY_FAILURE"
            or failure.get("rotation_eligible") is not False):
        return None
    source_path = Path(str(state.get("candidate_source") or state.get("stage2_candidate_source") or ""))
    spec_path = Path(str(state.get("last_stage1_artifact") or ""))
    compile_path = Path(str(failure.get("compile_attempt") or ""))
    execution_id = str(state.get("stage2_compile_source_repair_execution_id") or "")
    job_id = str((state.get("stage_jobs") or {}).get("2")
                 or (load_object(ROOT / "state" / "kcos_control_v2.json").get("stage_jobs") or {}).get("2") or "")
    if (not execution_id or not job_id or execution_status(db_path, execution_id) != "failed"
            or not source_path.is_file() or not spec_path.is_file() or not compile_path.is_file()
            or Path(str(failure.get("source") or "")).resolve() != source_path.resolve()
            or str(failure.get("source_sha256") or "").upper() != sha256(source_path).upper()
            or Path(str(failure.get("stage1_spec") or "")).resolve() != spec_path.resolve()
            or str(failure.get("stage1_spec_sha256") or "").upper() != sha256(spec_path).upper()
            or load_object(compile_path).get("passed") is not False):
        return None
    try:
        with sqlite3.connect(str(db_path), timeout=1.0) as connection:
            if connection.execute(
                    "SELECT 1 FROM executions WHERE job_id=? AND status IN ('claimed','running') LIMIT 1",
                    (job_id,)).fetchone() is not None:
                return None
        records = [json.loads(line) for line in str(state.get("stage2_repair_reason") or "").splitlines()
                   if line.strip().startswith("{")]
        report_record = next((item for item in records if item.get("report")), {})
        outcome_record = next((item for item in records if item.get("attempt_dir")), {})
        attempt_dir = Path(str(outcome_record.get("attempt_dir") or "")).resolve()
        attempt_dir.relative_to((ROOT / "state" / "diagnostics" / "stage2_repair_transactions").resolve())
        report_path = attempt_dir / "repair-loop-report.json"
        outcome_path = attempt_dir / "outcome.json"
        report, outcome = load_object(report_path), load_object(outcome_path)
        journal = load_object(attempt_dir / "transaction-journal.json")
        attempts = report.get("attempts") if isinstance(report.get("attempts"), list) else []
        candidate_proposals = [item for item in attempts if isinstance(item, dict)
                               and type(item.get("attempt")) is int and item["attempt"] > 0]
        valid_responses = [item for item in candidate_proposals
                           if (item.get("ai") or {}).get("status") in {"PROPOSED", "INVALID_RESPONSE"}]
        malformed_rows = [item for item in candidate_proposals
                          if (item.get("ai") or {}).get("status") == "FAILED"]
        baseline = next((item for item in attempts if item.get("baseline") is True), {})
        if (not Path(str(report_record.get("report") or "")).is_file()
                or sha256(Path(str(report_record["report"])).resolve()) != sha256(report_path)
                or report.get("cycle_id") != cycle or report.get("ea") != failure.get("ea")
                or report.get("max_attempts") != stage2_repair_loop.MAX_PROPOSALS
                or not valid_responses
                or any(not str((item.get("ai") or {}).get("error") or "")
                       .startswith("JSONDecodeError: Extra data:") for item in malformed_rows)
                or len(malformed_rows) > 1
                or outcome.get("outcome") != "REJECTED"
                or outcome.get("episode_id") != attempt_dir.parent.name
                or journal.get("status") != "ROLLED_BACK"
                or str(baseline.get("before_sha256") or "").upper() != sha256(source_path).upper()):
            return None
        if malformed_rows:
            failed_index = candidate_proposals.index(malformed_rows[0]) + 1
            call_path = attempt_dir / "proposals" / f"proposal-{failed_index}.call.json"
            stdout_path = attempt_dir / "proposals" / f"proposal-{failed_index}.stdout.txt"
            call_doc = load_object(call_path)
            stdout_text = stdout_path.read_text(encoding="utf-8", errors="replace")
            raw = stdout_text.strip()
            recovered_response = stage2_repair_loop.parse_ai_proposal_output(raw)
            if (call_doc.get("returncode") != 0 or not stdout_path.is_file()
                    or call_doc.get("stdout_sha256") != hashlib.sha256(stdout_text.encode("utf-8")).hexdigest().upper()
                    or not isinstance(recovered_response.get("patch"), str)
                    or not recovered_response.get("patch")):
                return None
        episode_id = str(outcome.get("episode_id") or "")
        used = stage2_compile_repair_candidate_failure_count(state, source_path)
        if not 0 < used < stage2_repair_loop.MAX_PROPOSALS:
            return None
        budget = {"status": "verified", "used": used,
                  "limit": stage2_repair_loop.MAX_PROPOSALS,
                  "remaining": stage2_repair_loop.MAX_PROPOSALS - used,
                  "episode_id": episode_id, "source": "same_cycle_candidate_repair_transactions"}
        proposal_record = next((item for item in valid_responses
                                if (item.get("ai") or {}).get("status") == "PROPOSED"), None)
        if proposal_record is None:
            return None
        proposal_index = candidate_proposals.index(proposal_record) + 1
        proposal_path = attempt_dir / "proposals" / f"proposal-{proposal_index}.json"
        proposal_doc = load_object(proposal_path)
        if not isinstance(proposal_doc.get("response"), dict):
            return None
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, sqlite3.Error):
        return None
    identity = {"cycle_id": cycle, "candidate_ea": str(failure.get("ea") or ""),
                "source_sha256": sha256(source_path), "stage1_spec_sha256": sha256(spec_path),
                "failure_event_sha256": sha256(failure_path),
                "compile_attempt_sha256": sha256(compile_path),
                "repair_report_sha256": sha256(report_path),
                "repair_transaction_outcome_sha256": sha256(outcome_path),
                "proposal_response_sha256": sha256(proposal_path),
                "prior_execution_id": execution_id, "proposal_budget": budget,
                "retry_scope": "same_candidate_repair_continuation_within_four_proposal_budget"}
    identity["identity_sha256"] = hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest().upper()
    return identity


def authenticated_stage2_source_repair_compile_fix_followup(state: dict, failure: dict,
                                                             failure_path: Path,
                                                             db_path: Path) -> dict | None:
    """Authorize one more Stage 2 run only for the compiler-proven stop-level errors left after the prior fix."""
    cycle = str(state.get("cycle_id") or "")
    if (not cycle or state.get("current_stage") != "stage2_pending"
            or state.get("stage_status") != "stage2_repair_required"
            or state.get("next_action") != "repair_stage2_failure"
            or state.get("stage2_compile_source_repair_cycle_id") != cycle
            or state.get("stage2_compile_source_repair_count") != 1
            or stage2_compile_source_repair_followup_count(state, cycle) != 1
            or stage2_compile_source_repair_second_followup_count(state, cycle) != 0
            or Path(str(state.get("stage2_failure_event") or "")).resolve() != failure_path.resolve()):
        return None
    source_path = Path(state.get("candidate_source") or state.get("stage2_candidate_source") or "")
    spec_path = Path(state.get("last_stage1_artifact") or "")
    compile_path = Path(failure.get("compile_attempt") or "")
    if not all(path.is_file() for path in (failure_path, source_path, spec_path, compile_path)):
        return None
    if (failure.get("cycle_id") != cycle or failure.get("stage") != 2
            or failure.get("status") != "failed"
            or failure.get("failure_kind") != "COMPILE_INTEGRITY_FAILURE"
            or Path(str(failure.get("source") or "")).resolve() != source_path.resolve()
            or str(failure.get("source_sha256") or "").upper() != sha256(source_path).upper()
            or Path(str(failure.get("stage1_spec") or "")).resolve() != spec_path.resolve()
            or str(failure.get("stage1_spec_sha256") or "").upper() != sha256(spec_path).upper()
            or Path(str(failure.get("compile_attempt") or "")).resolve() != compile_path.resolve()):
        return None
    execution_id = str(state.get("stage2_compile_source_repair_followup_execution_id") or "")
    control = load_object(ROOT / "state" / "kcos_control_v2.json")
    job_id = str((control.get("stage_jobs") or {}).get("2") or "")
    if not execution_id or not job_id or execution_status(db_path, execution_id) != "failed":
        return None
    try:
        connection = sqlite3.connect(str(db_path))
        try:
            active = connection.execute(
                "SELECT 1 FROM executions WHERE job_id=? AND status IN ('claimed','running') LIMIT 1",
                (job_id,)).fetchone() is not None
        finally:
            connection.close()
        if active:
            return None
        records = [json.loads(line) for line in str(state.get("stage2_repair_reason") or "{}").splitlines()
                   if line.strip().startswith("{")]
        repair_result = next(item for item in records if item.get("report"))
        tx_record = next(item for item in records if item.get("attempt_dir"))
        attempt_dir = Path(tx_record["attempt_dir"]).resolve()
        attempt_dir.relative_to((ROOT / "state" / "diagnostics" / "stage2_repair_transactions").resolve())
        outcome_record = next(item for item in records if item.get("outcome"))
        outcome = load_object(attempt_dir / "outcome.json")
        journal = load_object(attempt_dir / "transaction-journal.json")
        report_path = Path(repair_result["report"]).resolve()
        report = load_object(attempt_dir / "repair-loop-report.json")
        attempts = report.get("attempts") or []
        baseline = next((item for item in attempts if item.get("attempt") == 0), {})
        repaired = next((item for item in attempts if item.get("attempt") == 1), {})
        fix = repaired.get("deterministic_compile_fix") or {}
        compile_receipt_path = next((path for path in (attempt_dir / "candidate-output").glob("*stage2.compile.latest.json")), None)
        compile_receipt = load_object(compile_receipt_path) if compile_receipt_path else {}
        log_path = Path(compile_receipt.get("log") or "")
        raw_log = log_path.read_bytes() if log_path.is_file() else b""
        encoding = "utf-16" if raw_log.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8"
        log_text = raw_log.decode(encoding, errors="replace")
        error_rows = re.findall(r"(?im)^(.+?\.mq5)\((\d+),\d+\)\s*:\s*error\s+(199|262)\s*:", log_text)
        call_lines = {number for number, line in enumerate(source_path.read_text(encoding="utf-8-sig", errors="replace").splitlines(), 1)
                      if re.search(r"SymbolInfoDouble\s*\(\s*_Symbol\s*,\s*SYMBOL_TRADE_STOPS_LEVEL\s*\)", line, re.I)}
        errors_by_line: dict[int, set[str]] = {}
        expected_source = os.path.normcase(os.path.abspath(str(source_path)))
        for path, line, code in error_rows:
            if os.path.normcase(os.path.abspath(path)) != expected_source:
                return None
            errors_by_line.setdefault(int(line), set()).add(code)
        fixed_sources = [path for path in (attempt_dir / "candidate-output").glob("*.mq5")
                         if sha256(path).upper() == str(repaired.get("after_sha256") or "").upper()]
        if (outcome_record.get("episode_id") != outcome.get("episode_id")
                or outcome.get("outcome") != "REJECTED"
                or outcome.get("episode_id") != attempt_dir.parent.name
                or journal.get("status") != "ROLLED_BACK"
                or report.get("cycle_id") != cycle
                or report.get("ea") != state.get("candidate_ea", state.get("current_ea"))
                or not report_path.is_file()
                or str(baseline.get("before_sha256") or "").upper() != sha256(source_path).upper()
                or fix.get("status") != "APPLIED"
                or fix.get("changes") != [{"kind": "selected_position_ticket_close", "replacements": 2}]
                or str(repaired.get("before_sha256") or "").upper() != sha256(source_path).upper()
                or str(repaired.get("after_sha256") or "").upper() == sha256(source_path).upper()
                or len(fixed_sources) != 1
                or compile_receipt.get("cycle_id") != cycle or compile_receipt.get("stage") != 2
                or compile_receipt.get("passed") is not False or compile_receipt.get("errors") != 4
                or compile_receipt.get("warnings") != 0 or compile_receipt.get("fresh_ex5") is not False
                or str(compile_receipt.get("mq5_sha256") or "").upper() != str(repaired.get("after_sha256") or "").upper()
                or len(error_rows) != 4 or len(errors_by_line) != 2 or call_lines != set(errors_by_line)
                or any(codes != {"199", "262"} for codes in errors_by_line.values())):
            return None
    except (OSError, ValueError, TypeError, KeyError, StopIteration, sqlite3.Error, json.JSONDecodeError):
        return None
    identity = {
        "cycle_id": cycle,
        "candidate_ea": str(state.get("candidate_ea", state.get("current_ea")) or ""),
        "source_sha256": sha256(source_path),
        "failure_event_sha256": sha256(failure_path),
        "compile_attempt_sha256": sha256(compile_path),
        "stage1_spec_sha256": sha256(spec_path),
        "repair_report_sha256": sha256(report_path),
        "repair_transaction_outcome_sha256": sha256(attempt_dir / "outcome.json"),
        "retry_scope": "one_same_candidate_stage2_compile_fix_for_two_authenticated_stop_level_api_errors",
    }
    identity["identity_sha256"] = hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest().upper()
    return identity


def authenticated_stage2_requalification(state: dict, control: dict) -> dict | None:
    """Authorize one Stage 2 rebuild when its fresh verified artifacts outlive a stale receipt."""
    cycle = str(state.get("cycle_id") or "")
    requalification_marker = state.get("stage2_requalification")
    requalification_count = (state.get("stage2_requalification_count", 0)
                              if isinstance(requalification_marker, dict)
                              and requalification_marker.get("cycle_id") == cycle else 0)
    authorization = state.get("run_authorization")
    if (state.get("current_stage") != "stage3_pending"
            or state.get("stage_status") != "stage2_complete_held"
            or state.get("next_action") != "audit_stage2_before_stage3"
            or state.get("hold_after_stage") != 2
            or requalification_count != 0
            or not isinstance(authorization, dict)
            or authorization.get("cycle_id") != cycle
            or authorization.get("hold_after_stage") != 5
            or not authorizes_stages_1_to_5(state)
            or control.get("dispatch_enabled") is not True
            or 2 not in control.get("allowed_stages", [])):
        return None
    event_path = Path(state.get("last_stage2_event") or "")
    verification_path = Path(state.get("last_stage2_verification") or "")
    source_path = Path(state.get("candidate_source") or state.get("stage2_candidate_source") or "")
    spec_path = Path(state.get("last_stage1_artifact") or "")
    if not all(path.is_file() for path in (event_path, verification_path, source_path, spec_path)):
        return None
    try:
        event = load_object(event_path)
        verification = load_object(verification_path)
        checks = verification.get("checks")
        compile_result = verification.get("compile") or {}
        current_source_hash = sha256(source_path)
        current_spec_hash = sha256(spec_path)
        current_ex5 = Path(verification.get("ex5") or "")
        current_minato = Path(verification.get("canonical_minato") or "")
        current_artifacts_valid = (
            verification.get("schema_version") == 2
            and verification.get("cycle_id") == cycle
            and verification.get("stage") == 2
            and verification.get("ea") == (state.get("current_ea") or state.get("candidate_ea"))
            and verification.get("passed") is True
            and isinstance(checks, dict) and bool(checks) and all(value is True for value in checks.values())
            and verification.get("source") and Path(verification["source"]).resolve() == source_path.resolve()
            and str(verification.get("source_sha256") or "").upper() == current_source_hash
            and verification.get("stage1_spec") and Path(verification["stage1_spec"]).resolve() == spec_path.resolve()
            and str(verification.get("stage1_spec_sha256") or "").upper() == current_spec_hash
            and current_ex5.is_file() and current_minato.is_file()
            and str(compile_result.get("mq5_sha256") or "").upper() == current_source_hash
            and compile_result.get("passed") is True and compile_result.get("fresh_ex5") is True
            and compile_result.get("errors") == 0 and compile_result.get("warnings") == 0
            and str(compile_result.get("ex5_sha256") or "").upper() == sha256(current_ex5)
            and str(verification.get("canonical_minato_sha256") or "").upper() == sha256(current_minato)
            and event.get("schema_version") == 2 and event.get("stage") == 2
            and event.get("status") == "complete" and event.get("cycle_id") == cycle
            and event.get("ea") == verification.get("ea")
            and Path(event.get("verification") or "").resolve() == verification_path.resolve()
            and Path(event.get("stage1_spec") or "").resolve() == spec_path.resolve()
            and str(event.get("stage1_spec_sha256") or "").upper() == current_spec_hash
        )
        stale_completion = (
            event.get("verification_sha256") != sha256(verification_path)
            or str(event.get("mq5_sha256") or "").upper() != current_source_hash
            or str(event.get("ex5_sha256") or "").upper() != sha256(current_ex5)
            or str(event.get("canonical_minato_sha256") or "").upper() != sha256(current_minato)
        )
        if not current_artifacts_valid or not stale_completion:
            return None
        zero_trade = authenticated_stage3_zero_trade_repair(state)
    except (OSError, ValueError, TypeError, KeyError):
        return None
    if zero_trade is None:
        return None
    identity = {
        "cycle_id": cycle,
        "source_sha256": current_source_hash,
        "verification_sha256": sha256(verification_path),
        "stale_event_sha256": consumption_insensitive_sha256(event_path),
        "zero_trade_decision_sha256": zero_trade["decision_sha256"],
        "zero_trade_verification_sha256": zero_trade["verification_sha256"],
    }
    identity_sha = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
    return {"event_id": f"{cycle}:stage2:requalification:{identity_sha}",
            "event_path": str(event_path.resolve()), "zero_trade_evidence": zero_trade,
            "requalification_identity": identity}


def stage2_requalification_is_current(state: dict) -> bool:
    requalification = state.get("stage2_requalification")
    return (isinstance(requalification, dict)
            and state.get("stage2_requalification_count") == 1
            and requalification.get("cycle_id") == state.get("cycle_id"))


def authenticated_stage3_grid_rejection_execution(state: dict, control: dict,
                                                  db_path: Path = DB) -> dict | None:
    """Find the exact Stage 3 cron failure inside this cycle's Stage 2 window."""
    failure_path = Path(str(state.get("stage3_failure_event") or ""))
    stage2_event_path = Path(str(state.get("last_stage2_event") or ""))
    stage3_job = str((control.get("stage_jobs") or {}).get("3") or "")
    if not all(path.is_file() for path in (failure_path, stage2_event_path)) or not stage3_job:
        return None
    try:
        failure = load_object(failure_path)
        stage2_event = load_object(stage2_event_path)
        if (failure.get("cycle_id") != state.get("cycle_id") or failure.get("stage") != 3
                or failure.get("status") != "failed" or failure.get("rotation_eligible") is not False
                or stage2_event.get("cycle_id") != state.get("cycle_id")
                or stage2_event.get("stage") != 2 or stage2_event.get("status") != "complete"):
            return None
        start = datetime.fromisoformat(str(stage2_event["completed_utc"]).replace("Z", "+00:00"))
        end = datetime.fromisoformat(str(failure["failed_utc"]).replace("Z", "+00:00"))
        if start.tzinfo is None or end.tzinfo is None or end < start:
            return None
        expected_error = (
            "[TesterInputs] optimized parameter InpObvDivLookback has 20 MT5 steps "
            "(21 values); invalid for default profile")
        connection = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        try:
            rows = connection.execute(
                "SELECT id, job_id, status, claimed_at, finished_at, error FROM executions "
                "WHERE job_id=? AND status='failed' ORDER BY claimed_at ASC", (stage3_job,)
            ).fetchall()
        finally:
            connection.close()
        candidates = []
        for row in rows:
            error = str(row["error"] or "")
            if expected_error not in error:
                continue
            claimed = datetime.fromisoformat(str(row["claimed_at"]).replace("Z", "+00:00"))
            finished = datetime.fromisoformat(str(row["finished_at"]).replace("Z", "+00:00"))
            if claimed.tzinfo is None or finished.tzinfo is None or not (start <= finished <= end):
                continue
            candidates.append({
                "execution_id": str(row["id"]), "job_id": str(row["job_id"]),
                "status": str(row["status"]), "claimed_at": str(row["claimed_at"]),
                "finished_at": str(row["finished_at"]),
                "error_sha256": hashlib.sha256(error.encode("utf-8")).hexdigest().upper(),
                "error": expected_error,
            })
        return candidates[0] if candidates else None
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
        return None


def authenticated_stage2_policy_requalification(state: dict, control: dict,
                                                state_dir: Path = STATE.parent,
                                                db_path: Path = DB) -> dict | None:
    """Authorize bounded Stage 2 rebuilds after a policy-only grid change.

    This path is deliberately separate from defect repair and zero-trade
    requalification. Its first dispatch is bound to a completed M15 runtime
    with no tester workers. A single follow-up is allowed only after the exact
    Stage 3 grid-provenance rejection, to bind the repaired adapter code.
    """
    cycle = str(state.get("cycle_id") or "")
    count = state.get("stage2_policy_requalification_count", 0)
    first_request = count == 0
    followup_request = (count == 1
                        and state.get("stage_status") == "stage3_infrastructure_failure"
                        and state.get("next_action") == "inspect_stage3_infrastructure_failure")
    request_path = state_dir / "requests" / (
        "stage2_policy_requalification.json" if first_request
        else "stage2_policy_requalification_after_stage3_grid_rejection.json")
    if (not cycle or state.get("current_stage") != "stage3_running"
            or not (first_request and state.get("stage_status") == "stage3_m15_running"
                    and state.get("next_action") == "watch_stage3_m15"
                    or followup_request)
            or control.get("dispatch_enabled") is not True
            or 2 not in control.get("allowed_stages", [])
            or not authorizes_stages_1_to_5(state)
            or not request_path.is_file()):
        return None
    try:
        request = load_object(request_path)
        authorization = request.get("authorization") or {}
        request_id = str(request.get("request_id") or "")
        expected_scope = ("current_cycle_stage2_once" if first_request else
                          "current_cycle_stage2_once_after_authenticated_stage3_grid_rejection")
        if (request.get("schema_version") != 1
                or request.get("status") != "requested"
                or request.get("cycle_id") != cycle
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", request_id)
                or request.get("scope") != expected_scope
                or request.get("grid_policy_id") != "mt5_15_20_intervals_v1"
                or authorization.get("authorized_by") != "user_confirmed"
                or authorization.get("cycle_id") != cycle
                or authorization.get("scope") != expected_scope):
            return None
        event_path = Path(str(state.get("last_stage2_event") or ""))
        verification_path = Path(str(state.get("last_stage2_verification") or ""))
        if not event_path.is_file() or not verification_path.is_file():
            return None
        event, verification = load_object(event_path), load_object(verification_path)
        source_path = Path(str(verification.get("source") or ""))
        spec_path = Path(str(state.get("last_stage1_artifact") or ""))
        ex5_path = Path(str(verification.get("ex5") or ""))
        minato_path = Path(str(verification.get("canonical_minato") or ""))
        canonical_runtime_path = state_dir / "stage3_v2" / f"{cycle}.stage3.runtime.json"
        runtime_path = canonical_runtime_path
        if not first_request:
            previous = state.get("stage2_policy_requalification") or {}
            archive = previous.get("archive") or {}
            archive_dir = Path(str(archive.get("path") or ""))
            manifest_path = archive_dir / "manifest.json"
            if (not manifest_path.is_file()
                    or sha256(manifest_path) != str(archive.get("manifest_sha256") or "").upper()):
                return None
            manifest = load_object(manifest_path)
            runtime_entry = next((item for item in manifest.get("files", [])
                                  if Path(str(item.get("original_path") or "")).resolve()
                                  == canonical_runtime_path.resolve()), None)
            if runtime_entry is None:
                return None
            runtime_path = Path(str(runtime_entry.get("archive_path") or ""))
            if (not runtime_path.is_file()
                    or sha256(runtime_path) != str(runtime_entry.get("sha256") or "").upper()):
                return None
        if not all(path.is_file() for path in
                   (source_path, spec_path, ex5_path, minato_path, runtime_path)):
            return None
        runtime = load_object(runtime_path)
        active = rotation.active_stage3_runtime(state, state_dir)
        if (runtime.get("cycle_id") != cycle or runtime.get("stage") != 3
                or runtime.get("status") != "TIMEFRAME_COMPLETE"
                or runtime.get("timeframe") != "M15"):
            return None
        failure_path = Path(str(state.get("stage3_failure_event") or ""))
        if first_request:
            if (active is None or active.get("status") != "TIMEFRAME_COMPLETE"
                    or active.get("timeframe") != "M15"
                    or active.get("pids") != [runtime.get("terminal_pid")]):
                return None
        else:
            if active is not None or not failure_path.is_file():
                return None
            failure = load_object(failure_path)
            grid_rejection = authenticated_stage3_grid_rejection_execution(
                state, control, db_path)
            if (failure.get("cycle_id") != cycle or failure.get("stage") != 3
                    or failure.get("status") != "failed"
                    or failure.get("rotation_eligible") is not False
                    or grid_rejection is None):
                return None
        pipeline_hashes = {
            name: sha256(state_dir / name) for name in STAGE2_PIPELINE_CODE_FILES
            if (state_dir / name).is_file()
        }
        if set(pipeline_hashes) != set(STAGE2_PIPELINE_CODE_FILES):
            return None
        basis = {
            "stage2_event_sha256": sha256(event_path),
            "stage2_verification_sha256": sha256(verification_path),
            "source_sha256": sha256(source_path),
            "stage1_spec_sha256": sha256(spec_path),
            "stage3_runtime_sha256": sha256(runtime_path),
            "pipeline_code_hashes": pipeline_hashes,
        }
        if not first_request:
            basis["stage3_failure_event_sha256"] = sha256(failure_path)
            basis["stage3_grid_rejection_execution"] = grid_rejection
        if request.get("basis") != basis:
            return None
        if (event.get("schema_version") != 2 or event.get("cycle_id") != cycle
                or event.get("stage") != 2 or event.get("status") != "complete"
                or event.get("ea") != (state.get("current_ea") or state.get("candidate_ea"))
                or Path(str(event.get("verification") or "")).resolve() != verification_path.resolve()
                or event.get("verification_sha256") != basis["stage2_verification_sha256"]
                or verification.get("schema_version") != 2 or verification.get("cycle_id") != cycle
                or verification.get("stage") != 2 or verification.get("passed") is not True
                or not isinstance(verification.get("checks"), dict)
                or not verification["checks"] or not all(value is True for value in verification["checks"].values())
                or Path(str(state.get("candidate_source") or state.get("stage2_candidate_source") or "")).resolve()
                    != source_path.resolve()
                or Path(str(verification.get("stage1_spec") or "")).resolve() != spec_path.resolve()
                or str(verification.get("source_sha256") or "").upper() != basis["source_sha256"]
                or str(verification.get("stage1_spec_sha256") or "").upper() != basis["stage1_spec_sha256"]
                or verification.get("compile", {}).get("passed") is not True
                or verification.get("compile", {}).get("fresh_ex5") is not True
                or verification.get("compile", {}).get("errors") != 0
                or verification.get("compile", {}).get("warnings") != 0
                or str(verification.get("compile", {}).get("ex5_sha256") or "").upper() != sha256(ex5_path)
                or str(verification.get("canonical_minato_sha256") or "").upper() != sha256(minato_path)
                or str(event.get("mq5_sha256") or "").upper() != basis["source_sha256"]
                or str(event.get("ex5_sha256") or "").upper() != sha256(ex5_path)
                or str(event.get("canonical_minato_sha256") or "").upper() != sha256(minato_path)):
            return None
        identity = {"cycle_id": cycle, "request_sha256": sha256(request_path), **basis}
        identity_sha = hashlib.sha256(json.dumps(
            identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")).hexdigest().upper()
        return {"event_id": f"{cycle}:stage2:policy-requalification:{identity_sha}",
                "event_path": str(event_path.resolve()), "request_path": str(request_path.resolve()),
                "request_id": request_id, "request_sha256": sha256(request_path),
                "policy_requalification_identity": identity,
                "runtime_path": str(runtime_path.resolve())}
    except (OSError, ValueError, TypeError, KeyError):
        return None


def archive_stage2_policy_requalification(state: dict, state_dir: Path,
                                          request_id: str) -> dict:
    """Snapshot the superseded Stage 2/3 handoffs and cycle artifacts before republish."""
    cycle = str(state["cycle_id"])
    archive = state_dir / "diagnostics" / "stage2_policy_requalification" / cycle / request_id
    manifest_path = archive / "manifest.json"
    if manifest_path.is_file():
        manifest = load_object(manifest_path)
        for item in manifest.get("files", []):
            copied = Path(item["archive_path"])
            if not copied.is_file() or sha256(copied) != item["sha256"]:
                raise ValueError("Stage 2 policy requalification archive is incomplete or changed")
        return {"path": str(archive.resolve()), "manifest_sha256": sha256(manifest_path)}
    candidates = [
        Path(str(state.get("last_stage2_event") or "")),
        Path(str(state.get("last_stage2_verification") or "")),
        Path(str(state.get("last_stage1_artifact") or "")),
        Path(str(state.get("candidate_source") or state.get("stage2_candidate_source") or "")),
        state_dir / "handoffs" / "Stage2_v2.json",
        state_dir / "handoffs" / "Stage3_v2.json",
        state_dir / "stage3_v2" / f"{cycle}.stage3.runtime.json",
    ]
    if state.get("stage3_failure_event"):
        candidates.append(Path(str(state["stage3_failure_event"])))
    candidates.extend(path for path in (state_dir / "stage3_v2").glob(f"{cycle}.*")
                      if path.is_file() and path.suffix.casefold() in {".ini", ".opt", ".json"})
    unique = {str(path.resolve()): path for path in candidates if path.is_file()}
    files = []
    for index, (original, path) in enumerate(sorted(unique.items())):
        digest = sha256(path)
        archived = archive / f"{index:03d}-{path.name}"
        archived.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, archived)
        if sha256(archived) != digest:
            raise ValueError(f"Stage 2 policy requalification archive copy failed: {path}")
        files.append({"original_path": original, "archive_path": str(archived.resolve()),
                      "sha256": digest, "size_bytes": path.stat().st_size})
    manifest = {"schema_version": 1, "cycle_id": cycle, "request_id": request_id,
                "recorded_utc": utc_now(), "files": files}
    atomic_json(manifest_path, manifest)
    return {"path": str(archive.resolve()), "manifest_sha256": sha256(manifest_path)}



def stale_stage2_requalification_retry_identity(state: dict, failure: dict,
                                                  failure_path: Path) -> dict | None:
    """Bind one retry to the exact preflight failure caused by prior-cycle requalification state."""
    marker = state.get("stage2_requalification")
    cycle = str(state.get("cycle_id") or "")
    spec_path = Path(state.get("last_stage1_artifact") or "")
    expected_error = ("Stage 2 requalification claim no longer authenticates its same-cycle "
                      "source and zero-trade evidence")
    if (not isinstance(marker, dict) or not cycle or marker.get("cycle_id") == cycle
            or state.get("stage2_requalification_count") != 1
            or state.get("current_stage") != "stage2_pending"
            or state.get("stage_status") != "stage2_repair_required"
            or state.get("next_action") != "repair_stage2_failure"
            or Path(str(state.get("stage2_failure_event") or "")).resolve() != failure_path.resolve()
            or failure.get("schema_version") != 2 or failure.get("cycle_id") != cycle
            or failure.get("stage") != 2 or failure.get("status") != "failed"
            or failure.get("failure_kind") != "STAGE2_EXECUTOR_FAILURE"
            or failure.get("exception_type") != "RuntimeError"
            or failure.get("error") != expected_error
            or failure.get("source") is not None or failure.get("source_sha256") is not None
            or failure.get("source_identity_available") is not False
            or failure.get("verification") is not None or failure.get("compile_attempt") is not None
            or failure.get("failed_checks") != []
            or not spec_path.is_file()
            or Path(str(failure.get("stage1_spec") or "")).resolve() != spec_path.resolve()
            or str(failure.get("stage1_spec_sha256") or "").upper() != sha256(spec_path)):
        return None
    identity = {
        "cycle_id": cycle,
        "candidate_ea": str(state.get("candidate_ea") or state.get("current_ea") or ""),
        "failure_event_sha256": sha256(failure_path),
        "stage1_spec_sha256": sha256(spec_path),
        "stale_requalification_cycle_id": str(marker.get("cycle_id") or ""),
        "retry_scope": "one_same_candidate_retry_after_prior_cycle_requalification_guard_fix",
    }
    if not identity["candidate_ea"]:
        return None
    identity["identity_sha256"] = hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest().upper()
    return identity
def evaluate(state: dict, control: dict, db_path: Path) -> dict:
    if control.get("schema_version") != 2 or state.get("schema_version") != 2:
        raise ValueError("control/state schema mismatch")
    base = {
        "schema_version": 2,
        "cycle_id": state.get("cycle_id"),
        "observed_utc": utc_now(),
        "zero_token": True,
        "current_stage": state.get("current_stage"),
    }
    if control.get("paused", True):
        return {**base, "status": "PAUSED", "action": "none"}
    if state.get("stage2_source_defect_retry_terminal_hold"):
        return {**base, "status": "STAGE2_SOURCE_DEFECT_RETRY_TERMINAL_HOLD", "action": "none",
                "detail": state["stage2_source_defect_retry_terminal_hold"]}
    allowed = control.get("allowed_stages", [])
    hold_after = control.get("hold_after_stage")
    if allowed not in ([1], [1, 2], [1, 2, 3], [1, 2, 3, 4], [1, 2, 3, 4, 5]) or (hold_after is not None and hold_after not in allowed):
        raise ValueError("allowed stages must be a contiguous safe rollout prefix and hold must be in it")
    if state.get("stage_status") == "stage1_research_required":
        handoff_path = Path(state.get("last_stage1_handoff") or (ROOT / "state" / "handoffs" / "Stage1_v2.json"))
        failure_path = Path(state.get("stage1_failure_event") or "")
        handoff = load_object(handoff_path) if handoff_path.is_file() else {}
        failure = load_object(failure_path) if failure_path.is_file() else {}
        retry_policy_ready = stage1_research_retry_eligible(
            state, handoff, failure, source_available=True)
        if (retry_policy_ready and control.get("dispatch_enabled") is True
                and 1 in allowed):
            if not stage1_research_source_endpoint_ready():
                return {**base, "status": "WAITING_STAGE1_RESEARCH_SOURCE_PROVIDER", "action": "none",
                        "detail": "SearXNG is unavailable or returned no usable search results; preserving the same-cycle retry budget"}
            retry_ordinal = int(state.get("stage1_research_retry_count", 0)) + 1
            repair_ticket = stage1_research_retry_repair_ticket(
                state, int(state.get("stage1_research_retry_count", 0)))
            event_id = (f"{state.get('cycle_id')}:stage1:research-retry:repair:{repair_ticket['repair_id']}"
                        if repair_ticket else
                        f"{state.get('cycle_id')}:stage1:research-retry:{retry_ordinal}")
            return {**base, "status": "STAGE1_RESEARCH_RETRY_READY", "action": "dispatch_stage1",
                    "detail": ("one same-cycle Stage 1 retry is authorized by a hash-bound repair receipt"
                               if repair_ticket else
                               "same-cycle Stage 1 research publication failed; bounded one-shot cron retry is eligible"),
                    "event_id": event_id}
        return {**base, "status": "COMPLETE_HELD_STAGE1_RESEARCH", "action": "none",
                "detail": "Stage 1 research retry is absent, mismatched, disabled, or exhausted"}
    if authorizes_stages_1_to_3(state):
        # This is one observed experiment, not permission to hide a halt by
        # rotating to another candidate. Preserve its durable failure state.
        stage_status = str(state.get("stage_status", ""))
        lineage_retry = stage3_launch_lineage_retry_identity(state)
        allow_lineage_retry = (
            stage_status == "stage3_infrastructure_failure"
            and lineage_retry is not None
            and 3 in allowed
            and control.get("dispatch_enabled") is True
        )
        if (any(marker in stage_status for marker in (
                "failed", "repair_required", "research_required", "infrastructure_failure",
                "duration_reduction", "stop_required", "user_paused"))
                or state.get("promotion_blocked_reason")) and not allow_lineage_retry:
            detail = (state.get("stage3_failure_detail") or state.get("stage2_repair_reason")
                      or state.get("promotion_blocked_reason") or state.get("next_action")
                      or "operator review is required")
            failure_event = (state.get("stage3_failure_event") or state.get("stage2_failure_event")
                             or state.get("stage1_failure_event") or state.get("stage2_research_request"))
            return {**base, "status": "RUN_HALTED", "action": "none",
                    "stage_status": stage_status, "detail": f"{stage_status}: {detail}",
                    "failure_event": failure_event}
        # The cycle record can narrow controller authority, never broaden it.
        allowed = [stage for stage in allowed if stage <= 3]
        hold_after = min(hold_after, 3) if hold_after is not None else 3
    elif (isinstance(state.get("run_authorization"), dict)
          and state["run_authorization"].get("scope") == "current_cycle_stages_1_to_5"):
        if not authorizes_stages_1_to_5(state):
            return {**base, "status": "RUN_HALTED", "action": "none",
                    "detail": "full-cycle authorization is malformed or does not match this cycle"}
        authorized = state["run_authorization"]["allowed_stages"]
        if any(stage not in authorized for stage in allowed) or (hold_after is not None and hold_after not in authorized):
            return {**base, "status": "RUN_HALTED", "action": "none",
                    "detail": "control requests stages outside the full-cycle authorization"}
    if state.get("stage_status") == "stage1_provider_quota_recovery_ready":
        valid, detail = validate_stage1_provider_quota_recovery(state)
        if (not valid or state.get("current_stage") != "stage1_running"
                or control.get("dispatch_enabled") is not True or 1 not in allowed):
            return {**base, "status": "COMPLETE_HELD_STAGE1_PROVIDER_QUOTA", "action": "none",
                    "detail": detail if not valid else "provider recovery retry is not currently dispatchable"}
        return {**base, "status": "DISPATCH_READY_STAGE1", "action": "dispatch_stage1",
                "detail": "provider recovery was verified; dispatching one same-cycle Stage 1 retry through Hermes",
                "event_id": f"{state.get('cycle_id')}:stage1:provider-quota-retry:{int(state.get('stage1_provider_quota_retry_count', 0))}"}
    if state.get("promotion_blocked_reason"):
        return {**base, "status": "COMPLETE_HELD_INVALID_LINEAGE", "action": "none",
                "detail": state["promotion_blocked_reason"]}
    if state.get("stage_status") == "stage3_input_contract_rejected":
        if not (authorizes_stages_1_to_3(state) or authorizes_stages_1_to_5(state)):
            return {**base, "status": "RUN_HALTED", "action": "none",
                    "detail": "Stage 3 input-contract rejection lacks current-cycle authorization"}
        valid, detail, failure = validate_stage3_input_contract_failure(state)
        if not valid:
            return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": detail}
        if failure.get("rotation_eligible") is False:
            repair_valid, repair_detail, repair_failure = validate_stage3_input_contract_repair(state)
            if repair_valid and control.get("dispatch_enabled") is True and 3 in allowed:
                return {**base, "status": "STAGE3_INPUT_CONTRACT_RETRY_READY", "action": "start_stage3",
                        "detail": repair_detail, "event_id": repair_failure.get("event_id"),
                        "event_path": str(Path(state["stage3_failure_event"]).resolve())}
            return {**base, "status": "STAGE3_GRID_REPAIR_REQUIRED", "action": "none",
                    "detail": "Stage 3 could not build a valid optimization grid; preserve this candidate and repair Stage 3 preparation before retrying"}
        live_runtime = rotation.active_stage3_runtime(state, STATE.parent)
        if live_runtime:
            return {**base, "status": "STAGE3_RUNTIME_UNSETTLED", "action": "none",
                    "detail": "input-contract candidate cannot rotate while an owned Stage 3 runtime is live",
                    "runtime": live_runtime}
        if state.get("stage3_input_contract_retry_count", 0) == 0:
            repair_valid, repair_detail, repair_failure = validate_stage3_input_contract_repair(state)
            if repair_valid and control.get("dispatch_enabled") is True and 3 in allowed:
                return {**base, "status": "STAGE3_INPUT_CONTRACT_RETRY_READY", "action": "start_stage3",
                        "detail": repair_detail, "event_id": repair_failure.get("event_id"),
                        "event_path": str(Path(state["stage3_failure_event"]).resolve())}
        if control.get("dispatch_enabled") is not True or 1 not in allowed:
            return {**base, "status": "COMPLETE_HELD_AFTER_STAGE3", "action": "none",
                    "detail": f"{detail}; Stage 1 rotation is disabled"}
        return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1",
                "detail": detail, "event_id": failure.get("event_id"),
                "event_path": str(Path(state["stage3_failure_event"]).resolve())}
    if state.get("stage_status") == "narrow_opt_failed":
        valid, detail, event = validate_stage45_terminal_failure(state)
        if not valid or event is None:
            return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": detail}
        if control.get("dispatch_enabled") is not True or 1 not in allowed:
            return {**base, "status": "COMPLETE_HELD_AFTER_STAGE45", "action": "none",
                    "detail": f"{detail}; Stage 1 rotation is disabled"}
        return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1",
                "detail": detail, "event_id": event["event_id"],
                "event_path": state.get("last_stage45_event")}
    if (state.get("stage_status") in {"stage3_failed_held", "stage3_infrastructure_failure"}
            and state.get("stage3_failure_class") == "infrastructure"):
        # Accept the driver's typed terminal failure even when current_stage
        # remains stage3_running. Retire only identity-matched cycle workers
        # before evaluating the runtime gate; PID reuse is not ownership.
        retirement = rotation.retire_stage3_runtime(state, STATE.parent)
        live_runtime = rotation.active_stage3_runtime(state, STATE.parent)
        if live_runtime:
            return {**base, "status": "STAGE3_RUNTIME_UNSETTLED", "action": "none",
                    "detail": "Stage 3 infrastructure failure is held; owned terminal/workers still live",
                    "runtime": live_runtime}
        failure_path = Path(state.get("stage3_failure_event") or "")
        failure = load_object(failure_path) if failure_path.is_file() else {}
        if (failure.get("cycle_id") != state.get("cycle_id")
                or failure.get("stage") != 3
                or (failure.get("status") != "COMPUTE_BUDGET_EXCEEDED"
                    and failure.get("failure_kind") != "STAGE3_EXECUTION_FAILURE")):
            return {**base, "status": "UNKNOWN_STALLED", "action": "none",
                    "detail": "Stage 3 infrastructure event identity/status invalid"}
        policy_requalification = authenticated_stage2_policy_requalification(
            state, control, STATE.parent, db_path)
        if policy_requalification is not None:
            return {**base, "status": "STAGE2_POLICY_REQUALIFICATION_READY",
                    "action": "dispatch_stage2",
                    "detail": "the exact authenticated Stage 3 grid rejection authorizes one follow-up Stage 2 publication with the corrected Stage 3 consumer hash",
                    **policy_requalification}
        if not stage3_infrastructure_failure_allows_rotation(failure):
            lineage_retry = stage3_launch_lineage_retry_identity(state)
            if (lineage_retry is not None and state.get("stage3_launch_lineage_retry_count", 0) == 0
                    and 3 in allowed and control.get("dispatch_enabled") is True):
                return {**base, "status": "STAGE3_LAUNCH_LINEAGE_RETRY_READY", "action": "start_stage3",
                        "detail": "the same-cycle Stage 3 launch config lineage now verifies; retrying this candidate from M5",
                        "event_id": f"{state.get('cycle_id')}:stage3:launch-lineage-retry:1",
                        "dispatch_identity": lineage_retry}
            return {**base, "status": "COMPLETE_HELD_AFTER_STAGE3", "action": "none",
                    "detail": "Stage 3 infrastructure failure is not rotation-eligible; repair and retry"}
        if 1 in allowed and control.get("dispatch_enabled") is True:
            return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1",
                    "detail": "authenticated Stage 3 infrastructure failure is ready for queue rotation",
                    "event_id": failure.get("event_id"), "event_path": str(failure_path.resolve())}
        return {**base, "status": "COMPLETE_HELD_AFTER_STAGE3", "action": "none",
                "detail": "authenticated Stage 3 infrastructure failure recorded; rotation disabled"}
    if state.get("current_stage") == "stage1_pending" and state.get("stage_status") == "stage3_failed_held":
        if state.get("stage3_failure_class") == "infrastructure":
            live_runtime = rotation.active_stage3_runtime(state, STATE.parent)
            if live_runtime:
                return {**base, "status": "STAGE3_RUNTIME_UNSETTLED", "action": "none",
                        "detail": "Stage 3 infrastructure failure is held; owned terminal/workers still live",
                        "runtime": live_runtime}
            failure_path = Path(state.get("stage3_failure_event") or "")
            failure = load_object(failure_path) if failure_path.is_file() else {}
            if (failure.get("cycle_id") != state.get("cycle_id")
                    or failure.get("stage") != 3
                    or (failure.get("status") != "COMPUTE_BUDGET_EXCEEDED"
                        and failure.get("failure_kind") != "STAGE3_EXECUTION_FAILURE")):
                return {**base, "status": "UNKNOWN_STALLED", "action": "none",
                        "detail": "Stage 3 infrastructure event identity/status invalid"}
            if not stage3_infrastructure_failure_allows_rotation(failure):
                return {**base, "status": "COMPLETE_HELD_AFTER_STAGE3", "action": "none",
                        "detail": "Stage 3 infrastructure failure is not rotation-eligible; repair and retry"}
            if 1 in allowed and control.get("dispatch_enabled") is True:
                return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1",
                        "detail": "zero-pass Stage 3 compute failure is ready for queue rotation",
                        "event_id": failure.get("event_id"), "event_path": str(failure_path.resolve())}
            return {**base, "status": "COMPLETE_HELD_AFTER_STAGE3", "action": "none",
                    "detail": "zero-pass Stage 3 compute failure recorded; rotation disabled"}
        valid, detail, event = validate_stage3_completion(state)
        if not valid:
            return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": detail}
        if event.get("_policy_invalidated") and (hold_after == 3 or control.get("dispatch_enabled") is not True):
            return {**base, "status": "STAGE3_REVALIDATION_REQUIRED_HELD", "action": "none",
                    "detail": detail, "required_action": event.get("_required_action")}
        if hold_after == 3 or control.get("dispatch_enabled") is not True:
            return {**base, "status": "COMPLETE_HELD_AFTER_STAGE3", "action": "none", "detail": detail}
        if event.get("consumed") is True:
            return {**base, "status": "COMPLETE_ALREADY_CONSUMED", "action": "none", "detail": detail}
        return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1", "detail": detail,
                "event_id": event.get("event_id"), "event_path": state.get("last_stage3_event"),
                "policy_invalidated": event.get("_policy_invalidated") is True}
    if state.get("current_stage") == "stage1_running" and state.get("stage_status") == "stage1_rotation_prepared":
        source_value = state.get("rotation_source_event")
        if not source_value:
            return {**base, "status": "DISPATCH_READY_STAGE1", "action": "dispatch_stage1",
                    "detail": "fresh rotated Stage 1 cycle is prepared",
                    "event_id": f"{state.get('cycle_id')}:stage1:pending"}
        source = Path(source_value)
        event = load_object(source) if source.is_file() else {}
        if event.get("consumed") is True or state.get("rotation_source_consumed") is True:
            # A consumed rotation event means the new cycle was prepared, not
            # that its Stage 1 executor successfully launched. Retry dispatch
            # after a failed launch, but never duplicate a live Hermes run.
            execution_id = control.get("last_stage1_execution_id")
            execution_state = execution_status(db_path, str(execution_id)) if execution_id else "missing"
            if execution_state in {"running", "started", "queued"}:
                return {**base, "status": "WAITING_STAGE1", "action": "none", "detail": "rotated Stage 1 execution is in progress", "execution_id": execution_id}
            if execution_state == "completed" and execution_id and stage1_execution_within_artifact_settlement(
                    state, str(execution_id), control, db_path):
                return {**base, "status": "WAITING_STAGE1_ARTIFACT_SETTLEMENT", "action": "none",
                        "detail": "Stage 1 execution completed; allowing its bound artifacts to settle before retry classification",
                        "execution_id": execution_id}
            return {**base, "status": "DISPATCH_READY_STAGE1", "action": "dispatch_stage1",
                    "detail": "rotation was consumed but Stage 1 launch is not active; retry dispatch",
                    "event_id": f"{state.get('cycle_id')}:stage1:pending"}
        return {**base, "status": "ROTATION_PREPARED_STAGE1", "action": "rotate_stage1",
                "detail": "fresh Stage 1 rotation is prepared", "event_id": state.get("rotation_source_event_id"),
                "event_path": str(source.resolve())}
    if state.get("current_stage") == "stage2_pending":
        policy_requalification = state.get("stage2_policy_requalification")
        if (isinstance(policy_requalification, dict)
                and policy_requalification.get("cycle_id") == state.get("cycle_id")
                and policy_requalification.get("status") == "authorized"
                and state.get("stage2_policy_requalification_count", 0) in {1, 2}):
            claim_path = Path(str(policy_requalification.get("dispatch_claim") or ""))
            claim = load_object(claim_path) if claim_path.is_file() else {}
            execution_id = str(claim.get("execution_id") or "")
            status = execution_status(db_path, execution_id) if execution_id else None
            if status in {"claimed", "running", "started", "queued"}:
                return {**base, "status": "WAITING_STAGE2_POLICY_REQUALIFICATION", "action": "none",
                        "detail": "the authorized Stage 2 policy requalification is running under Hermes",
                        "execution_id": execution_id}
            return {**base, "status": "STAGE2_POLICY_REQUALIFICATION_HELD", "action": "none",
                    "detail": "the authorized Stage 2 policy requalification ended without publishing a fresh completion; operator review is required",
                    "execution_id": execution_id or None, "execution_status": status}
        requalification = state.get("stage2_requalification")
        if stage2_requalification_is_current(state):
            execution_id = str(control.get("last_stage2_execution_id") or "")
            status = execution_status(db_path, execution_id) if execution_id else None
            if status in {"claimed", "running", "started", "queued"}:
                return {**base, "status": "WAITING_STAGE2_REQUALIFICATION", "action": "none",
                        "detail": "the one authorized Stage 2 requalification is running under Hermes"}
            return {**base, "status": "STAGE2_REQUALIFICATION_HELD", "action": "none",
                    "detail": "the one authorized Stage 2 requalification ended without publishing a fresh completion; operator review is required"}
        if state.get("stage_status") == "stage2_research_required":
            request_path = Path(state.get("stage2_research_request") or "")
            request = load_object(request_path) if request_path.is_file() else {}
            return {**base, "status": "INPUT_DOMAIN_RESEARCH_REQUIRED", "action": "none",
                    "detail": "Stage 2 is held for documented web research of missing optimization domains",
                    "research_request": str(request_path.resolve()) if request_path.is_file() else None,
                    "inputs": request.get("inputs", [])}
        if state.get("stage_status") == "stage2_repair_required":
            live_stage3 = rotation.active_stage3_runtime(state, STATE.parent)
            if live_stage3:
                return {**base, "status": "WAITING_STAGE3_RUNTIME", "action": "none",
                        "detail": "Stage 2 repair is waiting for the authenticated Stage 3 owner to release the shared MT5 profile",
                        "runtime": live_stage3}
            retry_ok, retry_detail, retry_event, retry_event_path = validate_stage2_verifier_retry(state)
            if retry_ok and retry_event_path is not None:
                if retry_event.get("consumed") is not True:
                    if 2 in allowed and control.get("dispatch_enabled") is True:
                        return {**base, "status": "STAGE2_VERIFIER_RETRY_READY", "action": "dispatch_stage2",
                                "detail": retry_detail, "event_id": retry_event["event_id"],
                                "event_path": str(retry_event_path.resolve())}
                    return {**base, "status": "STAGE2_VERIFIER_RETRY_HELD", "action": "none",
                            "detail": "authenticated retry awaits Stage 2 dispatch authorization"}
                execution_id = str(control.get("last_stage2_execution_id") or "")
                prior_id = str(load_object(Path(state["stage2_verifier_retry_ticket"])).get("prior_execution_id") or "")
                if execution_id == prior_id:
                    return {**base, "status": "WAITING_STAGE2_VERIFIER_RETRY", "action": "none",
                            "detail": "retry event consumed; awaiting supervisor dispatch registration"}
                execution_state = execution_status(db_path, execution_id) if execution_id else "missing"
                if execution_state in {"claimed", "running", "started", "queued"}:
                    return {**base, "status": "WAITING_STAGE2_VERIFIER_RETRY", "action": "none",
                            "detail": "Hermes Stage 2 verifier retry is active", "execution_id": execution_id}
                if execution_state in {"completed", "failed"}:
                    return {**base, "status": "STAGE2_VERIFIER_RETRY_TERMINAL_HELD", "action": "none",
                            "detail": "one-time verifier retry is terminal without advancing canonical Stage 2 state",
                            "execution_id": execution_id}
            recovery_event_path = Path(state.get("stage2_source_defect_event") or "")
            recovery_ticket_path = Path(state.get("stage2_source_defect_ticket") or "")
            if recovery_event_path.is_file() and recovery_ticket_path.is_file():
                try:
                    recovery_event = load_object(recovery_event_path)
                    recovery_ticket = load_object(recovery_ticket_path)
                    ticket_hash = sha256(recovery_ticket_path)
                    if (authorizes_stages_1_to_5(state)
                            and state.get("stage2_source_defect_event_sha256") == sha256(recovery_event_path)
                            and recovery_event.get("schema_version") == 1
                            and recovery_event.get("event_id") == state.get("stage2_source_defect_event_id")
                            and (recovery_event.get("event_id") == recovery_ticket.get("ticket_id")
                                 or recovery_event.get("event_id") == state.get("stage2_source_defect_event_id"))
                            and recovery_event.get("ticket_sha256") == ticket_hash
                            and type(recovery_event.get("dispatch_attempt", 1)) is int
                            and 1 <= recovery_event.get("dispatch_attempt", 1)
                            <= stage2_source_recovery.MAX_DISPATCH_ATTEMPTS
                            and recovery_event.get("cycle_id") == state.get("cycle_id")
                            and recovery_event.get("stage") == 2
                            and recovery_event.get("status") == "repair_requested"
                            and recovery_event.get("consumed") is False
                            and recovery_event.get("rotation_eligible") is False
                            and recovery_event.get("ticket") == str(recovery_ticket_path.resolve())
                            and recovery_ticket.get("cycle_id") == state.get("cycle_id")
                            and recovery_ticket.get("ea") == state.get("current_ea")
                            and state.get("stage2_source_defect_ticket_sha256") == ticket_hash
                            and hashlib.sha256(json.dumps(
                                {key: value for key, value in recovery_ticket.items()
                                 if key not in {"ticket_id", "ticket_sha256", "created_utc"}},
                                sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
                                == recovery_ticket.get("ticket_sha256")
                            and 2 in allowed and control.get("dispatch_enabled") is True):
                        try:
                            stage2_source_recovery.validate_ticket(
                                recovery_ticket_path, recovery_event_path, state,
                                allow_consumed_digest_reconcile=False,
                                allow_unconsumed_for_dispatch=True)
                        except (OSError, ValueError, KeyError, TypeError) as exc:
                            return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                    "detail": f"Stage 2 rebuild ticket is not the current authenticated episode: {exc}"}
                        return {**base, "status": "STAGE2_SOURCE_DEFECT_REBUILD_READY",
                                "action": "dispatch_stage2", "detail": "authenticated same-cycle DCC source defect ticket is ready for Stage 2 Hermes rebuild",
                                "event_id": recovery_event["event_id"],
                                "event_path": str(recovery_event_path.resolve()),
                                "recovery_ticket": str(recovery_ticket_path.resolve())}
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                            "detail": f"Stage 2 recovery evidence is not the current authenticated episode: {exc}"}
            consumed_recovery_path = Path(state.get("stage2_source_defect_event") or "")
            if consumed_recovery_path.is_file():
                consumed_recovery = load_object(consumed_recovery_path)
                if (consumed_recovery.get("cycle_id") == state.get("cycle_id")
                        and consumed_recovery.get("event_id") == state.get("stage2_source_defect_event_id")
                        and consumed_recovery.get("consumed") is True
                        and consumed_recovery.get("consumed_action") == "dispatch_stage2"):
                    recovery_ticket_path = Path(state.get("stage2_source_defect_ticket") or "")
                    try:
                        stage2_source_recovery.validate_ticket(
                            recovery_ticket_path, consumed_recovery_path, state,
                            allow_consumed_digest_reconcile=True)
                    except (OSError, ValueError, KeyError, TypeError) as exc:
                        return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                "detail": f"consumed Stage 2 recovery event is not the current authenticated episode: {exc}"}
                    execution_outcome, candidate_rows = stage2_recovery_execution_rows(state, control, db_path, consumed_recovery)
                    active = execution_outcome in {"active", "uncertain"}
                    execution_id = candidate_rows[0][0] if candidate_rows else None
                    if active:
                        return {**base, "status": "WAITING_STAGE2_SOURCE_DEFECT_REBUILD", "action": "none",
                                "detail": "recovery event is consumed and Stage 2 Hermes execution may be active",
                                "execution_id": execution_id}
                    dispatch_outcome = stage2_recovery_dispatch_outcome(state, control, consumed_recovery, db_path)
                    if dispatch_outcome == "terminal_success":
                        return {**base, "status": "STAGE2_SOURCE_DEFECT_REBUILD_SUCCESS_UNPUBLISHED", "action": "none",
                                "detail": "Stage 2 Hermes execution completed successfully but fresh publication evidence is absent; refusing duplicate dispatch"}
                    if dispatch_outcome != "terminal_failure" and state.get("stage2_source_defect_last_dispatch_event_id") == consumed_recovery.get("event_id") and state.get("stage2_source_defect_last_dispatch_accepted") is True:
                        return {**base, "status": "WAITING_STAGE2_SOURCE_DEFECT_REBUILD", "action": "none",
                                "detail": "Stage 2 Hermes accepted the recovery dispatch; awaiting its terminal result and fresh completion evidence"}
                    if state.get("stage2_source_defect_last_dispatch_event_id") != consumed_recovery.get("event_id"):
                        try:
                            intent = state.get("stage2_source_defect_consumption_intent") or {}
                            if intent.get("event_id") != consumed_recovery.get("event_id"):
                                raise ValueError("canonical consumption intent is missing")
                            age = datetime.now(timezone.utc) - parse_utc(str(intent.get("started_utc", "")))
                        except Exception:
                            age = timedelta(0)
                        if age < timedelta(seconds=120):
                            return {**base, "status": "WAITING_STAGE2_SOURCE_DEFECT_REBUILD", "action": "none",
                                    "detail": "dispatch outcome is not committed; waiting for Hermes claim registration before retry"}
                    if state.get("stage2_source_defect_retry_exhausted"):
                        recovery_ticket = load_object(recovery_ticket_path)
                        source_path = Path(recovery_ticket.get("candidate_source") or "")
                        if not source_path.is_file():
                            return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                    "detail": "recovery attempts are exhausted but the ticket-bound candidate source is missing"}
                        exhausted_identity = {
                            "cycle_id": str(state.get("cycle_id") or ""),
                            "candidate_ea": str(recovery_ticket.get("ea") or ""),
                            "candidate_source_sha256": sha256(source_path),
                            "ticket_sha256": sha256(recovery_ticket_path),
                            "dispatch_event_sha256": sha256(consumed_recovery_path),
                            "dispatch_attempt": consumed_recovery.get("dispatch_attempt", 1),
                            "bound_stage3_failure": (recovery_ticket.get("artifacts", {}).get("stage3_failure") or {}).get("sha256"),
                        }
                        exhausted_identity["identity_sha256"] = hashlib.sha256(json.dumps(
                            exhausted_identity, sort_keys=True, separators=(",", ":")
                        ).encode("utf-8")).hexdigest().upper()
                        bound_failure = recovery_ticket.get("artifacts", {}).get("stage3_failure") or {}
                        bound_failure_path = Path(bound_failure.get("path") or "")
                        if (not bound_failure_path.is_file()
                                or sha256(bound_failure_path) != str(bound_failure.get("sha256") or "").upper()):
                            return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                    "detail": "recovery exhaustion is not bound to the current authenticated failure artifact"}
                        if 1 in allowed and control.get("dispatch_enabled") is True:
                            rotation_source_path = Path(state.get("rotation_source_event") or "")
                            rotation_source = load_object(rotation_source_path) if rotation_source_path.is_file() else {}
                            if (not rotation_source_path.is_file()
                                    or rotation_source.get("event_id") != state.get("rotation_source_event_id")
                                    or rotation_source.get("cycle_id") != state.get("cycle_id")
                                    or rotation_source.get("stage") != 2):
                                return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                        "detail": "exhausted repair cannot authenticate its same-cycle Stage 2 rotation source event"}
                            return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1",
                                    "detail": "authenticated candidate-local Stage 2 recovery attempts are exhausted",
                                    "event_id": f"{state.get('cycle_id')}:stage2:recovery-exhausted:{exhausted_identity['identity_sha256']}",
                                    "event_path": str(rotation_source_path.resolve()),
                                    "source_event_id": rotation_source["event_id"],
                                    "source_event_sha256": sha256(rotation_source_path),
                                    "dispatch_identity": exhausted_identity}
                        return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                "detail": "authenticated candidate-local Stage 2 recovery attempts are exhausted; rotation is disabled"}
                    return {**base, "status": "STAGE2_SOURCE_DEFECT_REBUILD_RETRY_READY", "action": "none",
                            "detail": "consumed dispatch has no active Hermes execution; supervisor may issue a bounded unique retry event"}
            # A current authenticated Stage 2 failure is the active repair
            # episode. Older Stage 3 zero-trade pointers cannot supersede it.
            failure_path = Path(state.get("stage2_failure_event") or state.get("stage2_invalid_event") or "")
            failure = load_object(failure_path) if failure_path.is_file() else {}
            if failure_path.is_file():
                if failure.get("cycle_id") != state.get("cycle_id") or failure.get("stage") != 2:
                    return {**base, "status": "UNKNOWN_STALLED", "action": "none",
                            "detail": "Stage 2 repair hold lacks authenticated same-cycle failure evidence"}
                failure_kind = str(failure.get("failure_kind") or "UNKNOWN_STAGE2_FAILURE")
                retry_identity = state.get("stage2_tester_diagnostic_removal_retry_identity") or {}
                retry_belongs_to_cycle = (isinstance(retry_identity, dict)
                                          and retry_identity.get("cycle_id") == state.get("cycle_id"))
                if (retry_belongs_to_cycle
                        and int(state.get("stage2_tester_diagnostic_removal_retry_count", 0) or 0) >= 1):
                    retry_execution_id = str(state.get("stage2_tester_diagnostic_removal_retry_execution_id") or "")
                    retry_execution_state = execution_status(db_path, retry_execution_id) if retry_execution_id else "missing"
                    if retry_execution_state in {"claimed", "running", "started", "queued"}:
                        return {**base, "status": "WAITING_STAGE2_TESTER_DIAGNOSTIC_REMOVAL_RETRY",
                                "action": "none", "detail": "the single Stage 2 cron retry is active",
                                "execution_id": retry_execution_id}
                    return {**base, "status": "STAGE2_TESTER_DIAGNOSTIC_REMOVAL_RETRY_TERMINAL_HOLD",
                            "action": "none", "detail": "the single Stage 2 retry is terminal or its execution cannot be authenticated",
                            "execution_id": retry_execution_id or None}
                stale_requalification_retry = stale_stage2_requalification_retry_identity(
                    state, failure, failure_path) if failure_kind == "STAGE2_EXECUTOR_FAILURE" else None
                stale_requalification_retry_cycle = state.get("stage2_stale_requalification_retry_cycle_id")
                stale_requalification_retry_count = (
                    state.get("stage2_stale_requalification_retry_count", 0)
                    if stale_requalification_retry_cycle == state.get("cycle_id") else 0)
                if stale_requalification_retry is not None:
                    if stale_requalification_retry_count >= 1:
                        return {**base, "status": "STAGE2_STALE_REQUALIFICATION_RETRY_TERMINAL_HOLD",
                                "action": "none", "detail": "the one same-candidate retry after stale requalification cleanup is terminal",
                                "failure_event": str(failure_path.resolve())}
                    if control.get("dispatch_enabled") is True and 2 in allowed:
                        return {**base, "status": "STAGE2_STALE_REQUALIFICATION_RETRY_READY",
                                "action": "dispatch_stage2",
                                "detail": "exact preflight failure from prior-cycle requalification state is eligible for one same-candidate retry",
                                "event_id": f"{state.get('cycle_id')}:stage2:stale-requalification-retry:{stale_requalification_retry['identity_sha256']}",
                                "event_path": str(failure_path.resolve()),
                                "dispatch_identity": stale_requalification_retry}
                stale_metadata_retry = None
                tester_removal_retry = None
                if failure_kind == "STAGE2_EXECUTOR_FAILURE":
                    try:
                        stale_metadata_retry = authenticated_stale_stage3_metadata_retry(
                            state, control, failure_path, failure, db_path)
                        tester_removal_retry = authenticated_stage2_tester_diagnostic_removal_retry(
                            state, control, failure_path, failure, db_path)
                    except (OSError, ValueError, TypeError, KeyError):
                        stale_metadata_retry = None
                        tester_removal_retry = None
                if (tester_removal_retry is not None and control.get("dispatch_enabled") is True
                        and 2 in allowed):
                    retry_event_id = (f"{state.get('cycle_id')}:stage2:tester-diagnostic-removal-retry:"
                                      f"{tester_removal_retry['identity_sha256']}")
                    return {**base, "status": "STAGE2_TESTER_DIAGNOSTIC_REMOVAL_RETRY_READY",
                            "action": "dispatch_stage2",
                            "detail": "the authenticated Stage 2 tester-only failure is removed; one same-candidate cron retry is authorized",
                            "event_id": retry_event_id,
                            "dispatch_identity": tester_removal_retry}
                if stale_metadata_retry is not None and control.get("dispatch_enabled") is True and 2 in allowed:
                    retry_event_id = (f"{state.get('cycle_id')}:stage2:stale-stage3-metadata-retry:"
                                      f"{stale_metadata_retry['identity_sha256']}")
                    claim_dir = failure_path.parent.parent / "claims"
                    orphan = None
                    for claim_file in claim_dir.glob("*.claim.json"):
                        try:
                            candidate_claim = load_object(claim_file)
                        except (OSError, ValueError, TypeError):
                            continue
                        if (candidate_claim.get("event_id") == retry_event_id
                                and candidate_claim.get("action") == "dispatch_stage2"
                                and candidate_claim.get("dispatch_outcome") == "claimed"
                                and candidate_claim.get("execution_id") is None):
                            orphan = (claim_file, candidate_claim)
                            break
                    stage2_job = (control.get("stage_jobs") or {}).get("2")
                    active = False
                    if stage2_job:
                        con = sqlite3.connect(str(db_path))
                        try:
                            active = con.execute(
                                "SELECT 1 FROM executions WHERE job_id = ? AND status IN ('claimed','running') LIMIT 1",
                                (stage2_job,)).fetchone() is not None
                        finally:
                            con.close()
                    if orphan:
                        orphan_path, orphan_claim = orphan
                        try:
                            claim_started = parse_utc(str(orphan_claim.get("claimed_utc") or ""))
                            claim_age = datetime.now(timezone.utc) - claim_started
                            jobs_payload = load_object(HERMES_JOBS)
                            jobs = jobs_payload.get("jobs", []) if isinstance(jobs_payload, dict) else []
                            stage2_job_state = next((item for item in jobs if item.get("id") == stage2_job), {})
                            job_last_run = parse_utc(str(stage2_job_state.get("last_run_at") or ""))
                            no_run_since_claim = job_last_run <= claim_started
                        except Exception:
                            claim_age = timedelta(0)
                            no_run_since_claim = False
                            stage2_job_state = {}
                        base_prefix = retry_event_id + ":registration-recovery:"
                        prior_recovery_claim = False
                        for claim_file in claim_dir.glob("*.claim.json"):
                            if claim_file == orphan_path:
                                continue
                            try:
                                prior = load_object(claim_file)
                            except (OSError, ValueError, TypeError):
                                continue
                            if (str(prior.get("event_id") or "").startswith(base_prefix)
                                    and prior.get("action") == "dispatch_stage2"):
                                prior_recovery_claim = True
                                break
                        if (not prior_recovery_claim and claim_age >= timedelta(seconds=120)
                                and no_run_since_claim and not active
                                and stage2_job_state.get("enabled") is False
                                and stage2_executor_lock_active(failure_path.parent.parent) is False):
                            stale_metadata_retry["orphan_claim_sha256"] = sha256(orphan_path)
                            retry_event_id = f"{state.get('cycle_id')}:s2r:{sha256(orphan_path)[:10]}"
                    return {**base, "status": "STAGE2_STALE_METADATA_RETRY_READY",
                            "action": "dispatch_stage2", "detail": "same-cycle failure is bound to stale prior-cycle Stage 3 pointers; one same-candidate retry is authorized",
                            "event_id": retry_event_id, "dispatch_identity": stale_metadata_retry}
                preflight_retry = authenticated_stage2_initial_preflight_retry(
                    state, control, failure_path, failure, db_path)
                if preflight_retry is not None:
                    failure_class = preflight_retry["preflight_failure_class"]
                    previous_classes = preflight_retry["previous_retry_classes"]
                    if failure_class in previous_classes:
                        retry_execution_id = str(state.get("stage2_preflight_retry_execution_id") or "")
                        retry_execution_state = (execution_status(db_path, retry_execution_id)
                                                 if retry_execution_id else "missing")
                        if retry_execution_state in {"claimed", "running", "started", "queued"}:
                            return {**base, "status": "WAITING_STAGE2_PREFLIGHT_RETRY",
                                    "action": "none",
                                    "detail": f"the authenticated same-candidate Stage 2 retry for {failure_class} is active",
                                    "execution_id": retry_execution_id}
                        return {**base, "status": "STAGE2_PREFLIGHT_RETRY_TERMINAL_HOLD",
                                "action": "none",
                                "detail": f"the bounded Stage 2 retry for {failure_class} is terminal; preserving the failure for diagnosis",
                                "execution_id": retry_execution_id or None}
                    return {**base, "status": "STAGE2_INITIAL_PREFLIGHT_RETRY_READY",
                            "action": "dispatch_stage2",
                            "detail": f"the exact authenticated Stage 2 preflight failure {failure_class} is eligible for one same-candidate Hermes retry",
                            "event_id": (f"{state.get('cycle_id')}:stage2:preflight-retry:"
                                         f"{preflight_retry['identity_sha256']}"),
                            "event_path": str(failure_path.resolve()),
                            "dispatch_identity": preflight_retry}
                compile_retry = authenticated_stage2_compile_repair_retry(
                    state, control, failure_path, failure, db_path)
                if compile_retry is not None:
                    if compile_retry.get("retry_state") == "active":
                        return {**base, "status": "WAITING_STAGE2_COMPILE_REPAIR",
                                "action": "none", "detail": "the one authenticated same-candidate compile-repair execution is active",
                                "execution_id": compile_retry.get("execution_id")}
                    if compile_retry.get("retry_state") == "exhausted":
                        return {**base, "status": "STAGE2_COMPILE_REPAIR_TERMINAL_HOLD",
                                "action": "none", "detail": "the bounded compile-repair episode is terminal; preserving compiler and repair evidence",
                                "execution_id": compile_retry.get("execution_id") or None}
                    return {**base, "status": "STAGE2_COMPILE_REPAIR_RETRY_READY",
                            "action": "dispatch_stage2",
                            "detail": "authenticated compiler and source evidence identify the two exact unsupported CTrade calls; one same-candidate Stage 2 repair run is authorized",
                            "event_id": (f"{state.get('cycle_id')}:stage2:compile-repair:"
                                         f"{compile_retry['identity_sha256']}"),
                            "event_path": str(failure_path.resolve()),
                            "dispatch_identity": compile_retry}
                # One bounded retry for the executor preflight bug where an
                # unauthenticated/stale failure pointer was incorrectly sent
                # to the source-repair loop. The event must bind this exact
                # cycle, candidate, source, and Stage 1 spec, and the error
                # reason must match the known false-trigger failure exactly.
                source_path = Path(state.get("candidate_source") or state.get("stage2_candidate_source") or "")
                spec_path = Path(state.get("last_stage1_artifact") or "")
                try:
                    failure_error = json.loads(str(failure.get("error") or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    failure_error = {}
                retry_source_hash = sha256(source_path) if source_path.is_file() else ""
                retry_spec_hash = sha256(spec_path) if spec_path.is_file() else ""
                exact_executor_preflight_failure = (
                    failure.get("schema_version") == 2
                    and failure.get("cycle_id") == state.get("cycle_id")
                    and failure.get("stage") == 2 and failure.get("status") == "failed"
                    and failure.get("failure_kind") == "STAGE2_EXECUTOR_FAILURE"
                    and failure.get("ea") == state.get("candidate_ea", state.get("current_ea"))
                    and failure_error.get("outcome") == "INFRASTRUCTURE_BLOCKED"
                    and failure_error.get("reason") ==
                        "ValueError: canonical failure event does not identify a Stage 2 failure"
                    and failure.get("failed_checks") == []
                    and not failure.get("verification") and not failure.get("compile_attempt")
                    and Path(str(failure.get("source") or "")).resolve() == source_path.resolve()
                    and str(failure.get("source_sha256") or "").upper() == retry_source_hash
                    and Path(str(failure.get("stage1_spec") or "")).resolve() == spec_path.resolve()
                    and str(failure.get("stage1_spec_sha256") or "").upper() == retry_spec_hash
                    and bool(retry_source_hash) and bool(retry_spec_hash)
                )
                if exact_executor_preflight_failure and 2 in allowed and control.get("dispatch_enabled") is True:
                    retry_identity = {
                        "cycle_id": str(state.get("cycle_id") or ""),
                        "candidate_ea": str(state.get("candidate_ea", state.get("current_ea")) or ""),
                        "failure_event_sha256": sha256(failure_path),
                        "source_sha256": retry_source_hash,
                        "stage1_spec_sha256": retry_spec_hash,
                        "retry_scope": "one_same_candidate_retry_after_executor_repair_gate_false_trigger",
                    }
                    identity_sha = hashlib.sha256(json.dumps(
                        retry_identity, sort_keys=True, separators=(",", ":")
                    ).encode()).hexdigest().upper()
                    used = (state.get("stage2_executor_preflight_retry_identity") == retry_identity
                            and state.get("stage2_executor_preflight_retry_count") == 1)
                    if not used:
                        return {**base, "status": "STAGE2_EXECUTOR_PREFLIGHT_RETRY_READY",
                                "action": "dispatch_stage2",
                                "detail": "exact same-cycle executor repair-gate false trigger is eligible for one rerun of the admitted candidate",
                                "event_id": f"{state.get('cycle_id')}:stage2:executor-preflight:{identity_sha}",
                                "event_path": str(failure_path.resolve()),
                                "dispatch_identity": retry_identity}
                    retry_execution_id = str(state.get("stage2_executor_preflight_retry_execution_id") or "")
                    retry_state = execution_status(db_path, retry_execution_id) if retry_execution_id else "missing"
                    if retry_state in {"claimed", "running", "started", "queued"}:
                        return {**base, "status": "WAITING_STAGE2_EXECUTOR_PREFLIGHT_RETRY",
                                "action": "none", "detail": "the one authorized same-candidate executor retry is active",
                                "execution_id": retry_execution_id}
                    return {**base, "status": "STAGE2_EXECUTOR_PREFLIGHT_RETRY_TERMINAL_HOLD",
                            "action": "none", "detail": "the bounded same-candidate executor retry is terminal; preserving its artifacts for diagnosis",
                            "execution_id": retry_execution_id or None}
                if failure_kind == "STAGE2_COMPILE_REPAIR_BUDGET_EXHAUSTED":
                    try:
                        compile_repair_identity = authenticated_stage2_compile_repair_exhaustion_identity(
                            state, failure_path)
                    except (OSError, ValueError, TypeError, KeyError) as exc:
                        return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                "detail": f"four-failure compile-repair receipt failed authentication: {exc}",
                                "failure_event": str(failure_path.resolve()), "failure_kind": failure_kind}
                    if 1 in allowed and control.get("dispatch_enabled") is True:
                        return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1",
                                "detail": "four authenticated compile-repair failures are recorded; rotate this candidate",
                                "event_id": (f"{state.get('cycle_id')}:stage2:compile-repair-exhausted:"
                                             f"{compile_repair_identity['identity_sha256']}"),
                                "event_path": str(failure_path.resolve()),
                                "source_event_id": failure.get("event_id"),
                                "stage2_compile_repair_exhaustion_identity": compile_repair_identity}
                    return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                            "detail": "four compile-repair failures are authenticated but Stage 1 dispatch is disabled",
                            "failure_event": str(failure_path.resolve()), "failure_kind": failure_kind}
                # Route an authenticated baseline compiler failure back through
                # Stage 2's bounded source-repair loop exactly once. This does
                # not rotate the candidate or alter its source in the supervisor.
                compile_path = Path(failure.get("compile_attempt") or "")
                compile_doc = load_object(compile_path) if compile_path.is_file() else {}
                exact_compile_source_failure = (
                    failure.get("schema_version") == 2
                    and failure.get("cycle_id") == state.get("cycle_id")
                    and failure.get("stage") == 2 and failure.get("status") == "failed"
                    and failure.get("failure_kind") == "COMPILE_INTEGRITY_FAILURE"
                    and failure.get("failed_checks") == ["compile_clean"]
                    and failure.get("ea") == state.get("candidate_ea", state.get("current_ea"))
                    and Path(str(failure.get("source") or "")).resolve() == source_path.resolve()
                    and str(failure.get("source_sha256") or "").upper() == retry_source_hash
                    and Path(str(failure.get("stage1_spec") or "")).resolve() == spec_path.resolve()
                    and str(failure.get("stage1_spec_sha256") or "").upper() == retry_spec_hash
                    and compile_doc.get("cycle_id") == state.get("cycle_id")
                    and compile_doc.get("stage") == 2
                    and compile_doc.get("ea") == state.get("candidate_ea", state.get("current_ea"))
                    and compile_doc.get("passed") is False
                    and compile_doc.get("errors", 0) > 0
                    and compile_doc.get("fresh_ex5") is False
                    and bool(retry_source_hash) and bool(retry_spec_hash)
                )
                if exact_compile_source_failure and 2 in allowed and control.get("dispatch_enabled") is True:
                    repair_identity = {
                        "cycle_id": str(state.get("cycle_id") or ""),
                        "candidate_ea": str(state.get("candidate_ea", state.get("current_ea")) or ""),
                        "failure_event_sha256": sha256(failure_path),
                        "compile_attempt_sha256": sha256(compile_path),
                        "source_sha256": retry_source_hash,
                        "stage1_spec_sha256": retry_spec_hash,
                        "repair_scope": "one_same_candidate_stage2_source_repair_loop",
                    }
                    repair_key = hashlib.sha256(json.dumps(
                        repair_identity, sort_keys=True, separators=(",", ":")
                    ).encode()).hexdigest().upper()
                    used = stage2_compile_source_repair_used(state, str(state.get("cycle_id") or ""))
                    if not used:
                        return {**base, "status": "STAGE2_COMPILE_SOURCE_REPAIR_READY",
                                "action": "dispatch_stage2",
                                "detail": "authenticated same-cycle baseline compile failure is eligible for one bounded Stage 2 source-repair run",
                                "event_id": f"{state.get('cycle_id')}:stage2:source-repair:{repair_key}",
                                "event_path": str(failure_path.resolve()),
                                "dispatch_identity": repair_identity}
                    repair_execution_id = str(state.get("stage2_compile_source_repair_execution_id") or "")
                    repair_state = execution_status(db_path, repair_execution_id) if repair_execution_id else "missing"
                    if repair_state in {"claimed", "running", "started", "queued"}:
                        return {**base, "status": "WAITING_STAGE2_COMPILE_SOURCE_REPAIR",
                                "action": "none", "detail": "the one bounded same-candidate source-repair run is active",
                                "execution_id": repair_execution_id}
                    followup_identity = authenticated_stage2_source_repair_policy_followup(
                        state, failure, failure_path, db_path)
                    if followup_identity is None:
                        followup_identity = authenticated_stage2_source_repair_prompt_budget_followup(
                            state, failure, failure_path, db_path)
                    if (followup_identity and 2 in allowed
                            and control.get("dispatch_enabled") is True):
                        prompt_budget_retry = (followup_identity.get("retry_scope") ==
                                               "same_candidate_repair_retry_after_prompt_budget_infrastructure_failure")
                        proposal_budget_retry = (followup_identity.get("retry_scope") ==
                                                 "same_candidate_repair_continuation_within_four_proposal_budget")
                        return {**base, "status": "STAGE2_COMPILE_SOURCE_REPAIR_FOLLOWUP_READY",
                                "action": "dispatch_stage2",
                                "detail": ("the prior repair did not reach the model because duplicated evidence exceeded the Windows command budget; the prompt is corrected and one hash-bound same-candidate cron retry is authorized"
                                           if prompt_budget_retry else
                                           "one authenticated candidate-level compile-repair refusal was recorded; the same candidate has a bounded retry within its four-proposal budget"
                                           if proposal_budget_retry else
                                           "a compiler-proven warning fix was rolled back solely by the strategy-lock false positive; one hash-bound same-candidate cron rerun is authorized"),
                                "event_id": (f"{state.get('cycle_id')}:stage2:source-repair-followup:"
                                             f"{followup_identity['identity_sha256']}"),
                                "event_path": str(failure_path.resolve()),
                                "dispatch_identity": followup_identity}
                    compile_fix_followup = authenticated_stage2_source_repair_compile_fix_followup(
                        state, failure, failure_path, db_path)
                    if (compile_fix_followup and 2 in allowed
                            and control.get("dispatch_enabled") is True):
                        return {**base, "status": "STAGE2_COMPILE_SOURCE_REPAIR_SECOND_FOLLOWUP_READY",
                                "action": "dispatch_stage2",
                                "detail": "the prior compiler-proven position-ticket fix succeeded, and its fresh compile receipt isolates two remaining stops-level API errors; one hash-bound same-candidate cron rerun is authorized",
                                "event_id": (f"{state.get('cycle_id')}:stage2:source-repair-compile-fix:"
                                             f"{compile_fix_followup['identity_sha256']}"),
                                "event_path": str(failure_path.resolve()),
                                "dispatch_identity": compile_fix_followup}
                    proposal_budget_followup = authenticated_stage2_source_repair_proposal_budget_followup(
                        state, failure, failure_path, db_path)
                    if (proposal_budget_followup and 2 in allowed
                            and control.get("dispatch_enabled") is True):
                        proposal_followup_status = (
                            "STAGE2_COMPILE_SOURCE_REPAIR_FOLLOWUP_READY"
                            if stage2_compile_source_repair_followup_count(
                                state, str(state.get("cycle_id") or "")) == 0
                            else "STAGE2_COMPILE_SOURCE_REPAIR_SECOND_FOLLOWUP_READY")
                        return {**base, "status": proposal_followup_status,
                                "action": "dispatch_stage2",
                                "detail": (f"the prior compile repair produced one authenticated candidate-level refusal; "
                                           f"continue the same candidate within the remaining "
                                           f"{proposal_budget_followup['proposal_budget']['remaining']} proposal slots"),
                                "event_id": (f"{state.get('cycle_id')}:stage2:source-repair-budget-followup:"
                                             f"{proposal_budget_followup['identity_sha256']}"),
                                "event_path": str(failure_path.resolve()),
                                "dispatch_identity": proposal_budget_followup}
                    return {**base, "status": "STAGE2_COMPILE_SOURCE_REPAIR_TERMINAL_HOLD",
                            "action": "none", "detail": "the bounded same-candidate source-repair run is terminal; preserving compiler and repair evidence",
                            "execution_id": repair_execution_id or None}
                duplicate_candidate_identity = rotation.authenticated_duplicate_unsupported_candidate(
                    state, STATE.parent)
                if duplicate_candidate_identity is not None:
                    if 1 in allowed and control.get("dispatch_enabled") is True:
                        return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1",
                                "detail": "authenticated unsupported strategy collided with a hash-bound source owned by a prior cycle; exclude this primitive and rotate",
                                "event_id": f"{state.get('cycle_id')}:stage2:duplicate-source:{duplicate_candidate_identity['identity_sha256']}",
                                "event_path": str(failure_path.resolve()),
                                "source_event_id": failure.get("event_id"),
                                "dispatch_identity": duplicate_candidate_identity}
                    return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                            "detail": "authenticated duplicate source collision found but Stage 1 rotation is not authorized",
                            "failure_event": str(failure_path.resolve()), "failure_kind": failure_kind}
                if failure_kind == "STAGE2_EXECUTOR_FAILURE":
                    try:
                        legacy_zero_trade_retry = authenticated_stage2_zero_trade_retry_identity(
                            state, failure_path, failure)
                    except (OSError, ValueError, TypeError, KeyError):
                        legacy_zero_trade_retry = None
                    if legacy_zero_trade_retry is not None:
                        budget = legacy_zero_trade_retry["budget"]
                        if 2 in allowed and control.get("dispatch_enabled") is True:
                            return {**base, "status": "STAGE2_ZERO_TRADE_RETRY_READY", "action": "dispatch_stage2",
                                    "detail": (f"authenticated zero-trade repair attempt {budget['used']} of "
                                               f"{budget['limit']} failed without changing the candidate; retry within its recorded budget"),
                                    "event_id": (f"{state.get('cycle_id')}:stage2:zero-trade-retry:"
                                                 f"{budget['episode_id']}:{budget['used']}:"
                                                 f"{sha256(failure_path)[:16]}"),
                                    "failure_event": str(failure_path.resolve()),
                                    "proposal_budget": budget,
                                    "zero_trade_evidence": legacy_zero_trade_retry["zero_trade_evidence"]}
                        return {**base, "status": "STAGE2_ZERO_TRADE_RETRY_HELD", "action": "none",
                                "detail": "authenticated Stage 2 zero-trade repair retry awaits dispatch authorization",
                                "failure_event": str(failure_path.resolve()), "proposal_budget": budget}
                if failure_kind in {"STAGE2_ZERO_TRADE_REPAIR_FAILURE",
                                    "STAGE2_ZERO_TRADE_REPAIR_BUDGET_EXHAUSTED"}:
                    try:
                        zero_trade = authenticated_stage3_zero_trade_repair(state)
                    except (OSError, ValueError, TypeError, KeyError) as exc:
                        return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                "detail": f"same-cycle zero-trade evidence failed authentication: {exc}",
                                "failure_event": str(failure_path.resolve()), "failure_kind": failure_kind}
                    if zero_trade is None:
                        return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                "detail": "Stage 2 zero-trade retry lacks authenticated same-cycle Stage 3 evidence",
                                "failure_event": str(failure_path.resolve()), "failure_kind": failure_kind}
                    if failure_kind == "STAGE2_ZERO_TRADE_REPAIR_FAILURE":
                        budget = stage2_proposal_budget_status(state)
                        attempt = (failure.get("stage2_zero_trade_repair") or {})
                        if (failure.get("rotation_eligible") is not False
                                or budget.get("status") != "verified"
                                or budget.get("episode_id") != attempt.get("episode_id")
                                or budget.get("used") != attempt.get("proposal_count")
                                or budget.get("limit") != attempt.get(
                                    "proposal_limit", stage2_repair_loop.MAX_PROPOSALS)):
                            return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                    "detail": "Stage 2 zero-trade retry budget or failure event could not be authenticated",
                                    "failure_event": str(failure_path.resolve()), "failure_kind": failure_kind,
                                    "proposal_budget": budget}
                        if not budget.get("remaining"):
                            return {**base, "status": "STAGE2_ZERO_TRADE_BUDGET_EXHAUSTED_HELD",
                                    "action": "none",
                                    "detail": ("the authenticated zero-trade proposal budget is exhausted, "
                                               f"but {budget['limit']} distinct no-change proposal transactions are not present; "
                                               "preserving the candidate without rotation"),
                                    "failure_event": str(failure_path.resolve()),
                                    "proposal_budget": budget}
                        if 2 in allowed and control.get("dispatch_enabled") is True:
                            return {**base, "status": "STAGE2_ZERO_TRADE_RETRY_READY", "action": "dispatch_stage2",
                                    "detail": (f"authenticated zero-trade repair attempt {budget['used']} of "
                                               f"{budget['limit']} failed without changing the candidate; retry within its recorded budget"),
                                    "event_id": (f"{state.get('cycle_id')}:stage2:zero-trade-retry:"
                                                 f"{budget['episode_id']}:{budget['used']}:"
                                                 f"{sha256(failure_path)[:16]}"),
                                    "failure_event": str(failure_path.resolve()), "proposal_budget": budget,
                                    "zero_trade_evidence": zero_trade}
                        return {**base, "status": "STAGE2_ZERO_TRADE_RETRY_HELD", "action": "none",
                                "detail": "authenticated Stage 2 zero-trade repair retry awaits dispatch authorization",
                                "failure_event": str(failure_path.resolve()), "proposal_budget": budget}
                    try:
                        exhaustion_identity = authenticated_stage2_zero_trade_exhaustion_identity(
                            state, failure_path)
                        failure_identity = stage2_repair_identity(state, failure_path)
                    except (OSError, ValueError, TypeError, KeyError) as exc:
                        return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                "detail": f"four-failure Stage 2 rotation receipt failed authentication: {exc}",
                                "failure_event": str(failure_path.resolve()), "failure_kind": failure_kind}
                    if failure.get("rotation_eligible") is True and 1 in allowed and control.get("dispatch_enabled") is True:
                        return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1",
                                "detail": "four authenticated no-change Stage 2 zero-trade repairs are recorded; rotate the candidate",
                                "event_id": f"{state.get('cycle_id')}:stage2:zero-trade-repair-exhausted:{exhaustion_identity['identity_sha256']}",
                                "event_path": str(failure_path.resolve()),
                                "source_event_id": failure.get("event_id"),
                                "stage2_repair_identity": failure_identity,
                                "stage2_zero_trade_exhaustion_identity": exhaustion_identity}
                    return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                            "detail": "four-failure Stage 2 rotation is authenticated but Stage 1 dispatch is disabled",
                            "failure_event": str(failure_path.resolve()), "failure_kind": failure_kind}
                infrastructure_or_lineage = (
                    any(token in failure_kind for token in ("INFRASTRUCTURE", "LINEAGE", "CONFIGURATION"))
                    or failure_kind == "STAGE2_EXECUTOR_FAILURE"
                    or str(failure.get("status") or "").upper() in {
                        "INFRASTRUCTURE_FAILURE", "LINEAGE_FAILURE", "CONFIGURATION_FAILURE"})
                if infrastructure_or_lineage:
                    return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                            "detail": f"Stage 2 {failure_kind.casefold().replace('_', ' ')} requires operator recovery; rotation is prohibited until infrastructure and lineage are revalidated",
                            "failure_event": str(failure_path.resolve()),
                            "failure_kind": failure_kind}
                if failure_kind == "MODEL_SOURCE_PROPOSAL_BUDGET_EXHAUSTED":
                    try:
                        generation_identity = stage2_source_generation_exhaustion_identity(state, failure_path)
                    except (OSError, ValueError, TypeError, KeyError) as exc:
                        return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                "detail": f"source-generation exhaustion is not fully authenticated for candidate rotation: {exc}",
                                "failure_event": str(failure_path.resolve()),
                                "failure_kind": failure_kind}
                    if failure.get("rotation_eligible") is True and 1 in allowed and control.get("dispatch_enabled") is True:
                        return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1",
                                "detail": "authenticated source-generation proposal budget is exhausted; rotate this unadmitted research candidate",
                                "event_id": f"{state.get('cycle_id')}:stage2:source-generation-exhausted:{generation_identity['identity_sha256']}",
                                "event_path": str(failure_path.resolve()),
                                "source_event_id": failure.get("event_id"),
                                "source_event_sha256": generation_identity["failure_event_sha256"],
                                "dispatch_identity": generation_identity}
                    return {**base, "status": "STAGE2_RECOVERY_HOLD", "action": "none",
                            "detail": "authenticated source-generation proposal budget is exhausted; candidate rotation is disabled",
                            "failure_event": str(failure_path.resolve()),
                            "failure_kind": failure_kind}
                try:
                    failure_identity = stage2_repair_identity(state, failure_path)
                except (OSError, ValueError, TypeError, KeyError) as exc:
                    blocked_path = Path(exc.filename) if isinstance(exc, PermissionError) and exc.filename else None
                    data_root = stage2_tester.DATA_ROOT.resolve()
                    blocked_norm = os.path.normcase(os.path.abspath(str(blocked_path))) if blocked_path else ""
                    root_norm = os.path.normcase(os.path.abspath(str(data_root)))
                    if (blocked_path and blocked_path.suffix.casefold() == ".hcc"
                            and os.path.commonpath((blocked_norm, root_norm)) == root_norm):
                        return {**base, "status": "WAITING_MT5_PROFILE_RELEASE", "action": "none",
                                "detail": "Stage 2 repair identity is waiting for the canonical MT5 profile history lock to clear",
                                "blocked_path": str(blocked_path)}
                    return {**base, "status": "UNKNOWN_STALLED", "action": "none",
                            "detail": f"Stage 2 failure identity could not be authenticated: {exc}"}
                if failure.get("rotation_eligible") is True and 1 in allowed and control.get("dispatch_enabled") is True:
                    return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1",
                            "detail": "authenticated candidate-local Stage 2 failure is ready for queue rotation",
                            "event_id": f"{state.get('cycle_id')}:stage2:failed:{failure_identity['identity_sha256']}",
                            "event_path": str(failure_path.resolve()),
                            "stage2_repair_identity": failure_identity}
                failure_kind = str(failure.get("failure_kind") or "")
                repairable_failure_kinds = {
                    "ORDER_PATH_SAFETY_FAILURE",
                    "REGIME_OPTIMIZATION_POLICY_VIOLATION",
                    "STAGE2_VERIFICATION_FAILURE",
                    "COMPILE_INTEGRITY_FAILURE",
                }
                budget = stage2_proposal_budget_status(state)
                if (failure.get("rotation_eligible") is False
                        and failure_kind in repairable_failure_kinds
                        and authorizes_stages_1_to_5(state)
                        and 2 in allowed
                        and control.get("dispatch_enabled") is True
                        and control.get("continuation_dispatch_enabled") is True
                        and budget.get("status") == "verified"
                        and type(budget.get("remaining")) is int
                        and budget["remaining"] > 0):
                    failure_hash = sha256(failure_path)
                    retry_identity = hashlib.sha256(json.dumps({
                        "cycle_id": state.get("cycle_id"),
                        "episode_id": budget["episode_id"],
                        "proposal_count": budget["used"],
                        "failure_event_sha256": failure_hash,
                    }, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:24].upper()
                    return {**base, "status": "STAGE2_REPAIR_RETRY_READY", "action": "dispatch_stage2",
                            "detail": (f"authenticated {failure_kind.casefold().replace('_', ' ')}; "
                                       f"resume the same candidate through bounded Stage 2 repair "
                                       f"({budget['used']} of {budget['limit']} proposals used)"),
                            "event_id": f"stage2-repair-retry:{retry_identity}",
                            "failure_event": str(failure_path.resolve()),
                            "failure_kind": failure_kind,
                            "proposal_budget": budget,
                            "stage2_repair_identity": failure_identity}
                return {**base, "status": "STAGE2_REPAIR_REQUIRED_HELD", "action": "none",
                        "detail": "authenticated Stage 2 failure remains on this candidate for bounded repair",
                        "failure_event": str(failure_path.resolve()),
                        "failure_kind": failure_kind,
                        "proposal_budget": budget,
                        "stage2_repair_identity": failure_identity}
            try:
                zero_trade = authenticated_stage3_zero_trade_repair(state)
            except (OSError, ValueError, TypeError, KeyError) as exc:
                return {**base, "status": "UNKNOWN_STALLED", "action": "none",
                        "detail": f"Stage 3 zero-trade repair evidence failed authentication: {exc}"}
            if zero_trade is not None:
                if 2 not in allowed or control.get("dispatch_enabled") is not True:
                    return {**base, "status": "STAGE2_ZERO_TRADE_REPAIR_HELD", "action": "none",
                            "detail": "authenticated Stage 3 zero-trade repair awaits Stage 2 dispatch authorization"}
                return {**base, "status": "DISPATCH_READY_STAGE2", "action": "dispatch_stage2",
                        "detail": ("passing same-cycle Stage 3 verifier authenticates zero trades in "
                                   + ", ".join(zero_trade["zero_trade_timeframes"])
                                   if zero_trade["zero_trade_timeframes"] else
                                   "passing same-cycle Stage 3 verifier authenticates low-trade failure on M5, M15, and H1"),
                        "event_id": zero_trade["event_id"], "zero_trade_evidence": zero_trade}
            return {**base, "status": "UNKNOWN_STALLED", "action": "none",
                    "detail": "Stage 2 repair hold lacks authenticated same-cycle failure evidence"}
        valid, detail = validate_stage1_completion(state, control, db_path)
        if valid:
            if 2 in allowed and hold_after != 1 and control.get("dispatch_enabled") is True:
                artifact_key = ""
                stage1_event_path = Path(state.get("last_stage1_event", ""))
                if stage1_event_path.is_file():
                    stage1_event = load_object(stage1_event_path)
                    artifact_key = str(stage1_event.get("artifact_sha256", ""))[:16]
                event_id = f"{state.get('cycle_id')}:stage1:complete:{artifact_key}"
                if state.get("stage_status") == "stage2_repair_required":
                    repair_key = hashlib.sha256(str(state.get("stage3_zero_trade_decision", "")).encode()).hexdigest()[:16]
                    event_id = f"{state.get('cycle_id')}:stage3:zero_trade_repair:{repair_key}"
                return {**base, "status": "DISPATCH_READY_STAGE2", "action": "dispatch_stage2", "detail": detail,
                        "event_id": event_id}
            return {**base, "status": "COMPLETE_HELD_STAGE2", "action": "none", "detail": detail}
        if detail == "Stage 1 event is not enclosed by a completed Hermes execution":
            return {**base, "status": "WAITING_STAGE1", "action": "none", "detail": detail}
        return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": detail}
    if state.get("current_stage") == "stage1_running" and state.get("stage_status") == "stage1_failed_held":
        failure_path = Path(state.get("stage1_failure_event") or state.get("last_stage1_event") or "")
        failure = load_object(failure_path) if failure_path.is_file() else {}
        if (failure.get("cycle_id") == state.get("cycle_id") and failure.get("stage") == 1
                and failure.get("status") == "held"
                and failure.get("failure_kind") == "STAGE1_RESEARCH_HOLD"):
            hold_path = Path(failure.get("stage1_hold_event") or "")
            output_path = Path(failure.get("stage1_reconfirmation_output") or "")
            hold_valid = (
                hold_path.is_file()
                and sha256(hold_path) == str(failure.get("stage1_hold_event_sha256") or "").upper()
            )
            output_valid = (
                not failure.get("stage1_reconfirmation_output")
                or (output_path.is_file()
                    and sha256(output_path)
                    == str(failure.get("stage1_reconfirmation_output_sha256") or "").upper())
            )
            if hold_valid and output_valid:
                return {**base, "status": "COMPLETE_HELD_STAGE1_RESEARCH", "action": "none",
                        "detail": failure.get("failure_detail") or "Stage 1 research hold is authenticated",
                        "failure_event": str(failure_path.resolve()),
                        "hold_event": str(hold_path.resolve())}
        allowed_failure_kinds = {"STAGE1_EXECUTION_TIMEOUT", "STAGE1_EXECUTION_CONTRACT_FAILURE", "STAGE1_PROVIDER_QUOTA_FAILURE", "STAGE1_EXECUTION_NO_ARTIFACT", "STAGE1_ARTIFACT_HASH_MISMATCH", "DUPLICATE_AUTHENTICATED_EXPERIMENT", "REGIME_OPTIMIZATION_POLICY_VIOLATION", "STAGE1_CONTRACT_FAILURE", "STAGE1_BUILD_OR_CONTRACT_FAILURE", "STAGE1_SIGNAL_REACHABILITY_FAILURE"}
        if (failure.get("cycle_id") != state.get("cycle_id") or failure.get("stage") != 1
                or failure.get("status") != "failed" or failure.get("failure_kind") not in allowed_failure_kinds
                or (failure.get("rotation_eligible") is not True
                    and failure.get("failure_kind") != "STAGE1_PROVIDER_QUOTA_FAILURE")):
            return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": "Stage 1 timeout hold lacks authenticated evidence"}
        if failure.get("failure_kind") == "STAGE1_PROVIDER_QUOTA_FAILURE":
            return {**base, "status": "COMPLETE_HELD_STAGE1_PROVIDER_QUOTA", "action": "none",
                    "detail": failure.get("failure_detail") or "Stage 1 provider quota/rate limit requires recovery before retry"}
        if 1 in allowed and control.get("dispatch_enabled") is True:
            return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1", "detail": "Authenticated Stage 1 failure is eligible for candidate rotation", "event_id": failure["event_id"], "event_path": str(failure_path.resolve())}
        return {**base, "status": "COMPLETE_HELD_STAGE1", "action": "none", "detail": "Stage 1 timed out; rotation is disabled"}
    if state.get("current_stage") in {"stage1_pending", "stage1_running", "stage1"}:
        if state.get("next_action") in {"dispatch_stage1", "run_stage1"} and control.get("dispatch_enabled") is True and 1 in allowed:
            return {**base, "status": "DISPATCH_READY_STAGE1", "action": "dispatch_stage1", "detail": "fresh Stage 1 cycle is pending" ,
                    "event_id": f"{state.get('cycle_id')}:stage1:pending"}
        return {**base, "status": "WAITING_STAGE1", "action": "none", "detail": "waiting for the single Stage 1 cron execution"}
    if state.get("current_stage") == "stage3_pending":
        valid, detail = validate_stage2_completion(state, control, db_path)
        cycle_hold = stage3_cycle_hold(state, stage2_audit_passed=valid)
        if cycle_hold is not None:
            if not valid:
                requalification = authenticated_stage2_requalification(state, control)
                if requalification is not None:
                    return {**base, "status": "STAGE2_REQUALIFICATION_READY", "action": "dispatch_stage2",
                            "detail": "same-cycle Stage 3 repair evidence and fresh passing Stage 2 artifacts authorize one Hermes Stage 2 requalification",
                            **requalification}
            return {**base, "status": "COMPLETE_HELD_STAGE3", "action": "none",
                    "detail": f"{cycle_hold}; Stage 2 provenance verified: {detail}" if valid
                    else f"{cycle_hold}; Stage 2 provenance not authenticated: {detail}"}
        if valid:
            journal_retry_identity = stage3_journal_retry_identity(state)
            if journal_retry_identity is not None and 3 in allowed and control.get("dispatch_enabled") is True:
                retry_event_id = (state.get("stage3_repair_retry") or {}).get("event_id")
                return {**base, "status": "STAGE3_JOURNAL_RECLASSIFICATION_RETRY_READY",
                        "action": "start_stage3",
                        "detail": "the exact Stage 3 journal rejection was reclassified as candidate-level; one hash-bound same-cycle retry is ready",
                        "event_id": retry_event_id, "dispatch_identity": journal_retry_identity}
            retry_ready = (
                state.get("stage3_input_contract_retry_count") == 1
                and 0 <= state.get("stage3_input_contract_retry_release_dispatch_count", 0) < 4
                and state.get("stage_status") == "stage2_complete_held"
                and state.get("hold_after_stage") == 2
                and state.get("next_action") in {"audit_stage2_before_stage3", "run_stage3"}
                and (state.get("next_action") != "run_stage3"
                     or (state.get("stage2_release_attempt") or {}).get("status") == "failed")
                and isinstance(state.get("run_authorization"), dict)
                and (authorizes_stages_1_to_3(state) or authorizes_stages_1_to_5(state))
            )
            if retry_ready:
                repair_valid, repair_detail, _ = validate_stage3_input_contract_repair(state)
                if repair_valid and 3 in allowed and control.get("dispatch_enabled") is True:
                    retry_number = int(state.get("stage3_input_contract_retry_release_dispatch_count", 0)) + 1
                    retry_event_id = f"{state.get('cycle_id')}:stage3:input-contract-retry:{retry_number}"
                    return {**base, "status": "STAGE3_INPUT_CONTRACT_RETRY_RELEASE_READY",
                            "action": "start_stage3", "detail": repair_detail,
                            "event_id": retry_event_id}
            if 3 in allowed and hold_after != 2 and control.get("dispatch_enabled") is True:
                return {**base, "status": "DISPATCH_READY_STAGE3", "action": "start_stage3", "detail": detail,
                        "event_id": f"{state.get('cycle_id')}:stage2:complete",
                        "dispatch_identity": stage3_start_content_identity(state)}
            return {**base, "status": "COMPLETE_HELD_STAGE3", "action": "none", "detail": detail}
        return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": detail}
    if state.get("current_stage") == "stage2_running":
        return {**base, "status": "WAITING_STAGE2", "action": "none", "detail": "Stage 2 execution is in progress"}
    if state.get("current_stage") == "stage3_running" and 3 in allowed:
        policy_requalification = authenticated_stage2_policy_requalification(
            state, control, STATE.parent, db_path)
        if policy_requalification is not None:
            return {**base, "status": "STAGE2_POLICY_REQUALIFICATION_READY",
                    "action": "dispatch_stage2",
                    "detail": "operator-authorized policy requalification is hash-bound to the current Stage 2 receipt and completed M15 runtime",
                    **policy_requalification}
        valid, detail, event, event_path = validate_stage3_timeframe_completion(state)
        if valid and event is not None and event.get("consumed") is True:
            return {**base, "status": "TIMEFRAME_ALREADY_CONSUMED", "action": "none", "detail": detail}
        if valid and control.get("continuation_dispatch_enabled") is True:
            return {
                **base,
                "status": "DISPATCH_READY_STAGE3_CONTINUATION",
                "action": "continue_stage3",
                "detail": detail,
                "event_id": event.get("event_id"),
                "event_path": str(event_path.resolve()),
            }
        if valid:
            return {**base, "status": "TIMEFRAME_COMPLETE_HELD", "action": "none", "detail": detail}
        if "still optimizing" not in detail:
            return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": detail}
        return {**base, "status": "WAITING_STAGE3", "action": "none", "detail": "zero-token watcher owns Stage 3 progress"}
    if state.get("stage_status") == "stage4_pending":
        if 4 not in allowed or control.get("dispatch_enabled") is not True:
            return {**base, "status": "COMPLETE_HELD_STAGE4", "action": "none", "detail": "Stage 4 is not allowed or dispatch is disabled"}
        valid, detail, event = validate_stage3_completion(state)
        if not valid:
            return {**base, "status": "UNKNOWN_STALLED", "action": "none",
                    "detail": f"Stage 4 pending without authenticated Stage 3 completion: {detail}"}
        if event.get("consumed") is True:
            return {**base, "status": "WAITING_STAGE4", "action": "none",
                    "detail": "Stage 4 dispatch was accepted; waiting for its authenticated completion event"}
        return {**base, "status": "DISPATCH_READY_STAGE4", "action": "dispatch_stage4", "detail": "verified Stage 3 completion is queued for Stage 4", "event_id": state.get("last_stage3_event") or state.get("stage3_event")}
    if state.get("stage_status") == "stage4_rejected" and state.get("next_action") == "queue_next":
        event_path = Path(state.get("last_stage4_event") or "")
        event = load_object(event_path) if event_path.is_file() else {}
        decision_path = Path(event.get("decision") or "")
        verification_path = Path(event.get("verification") or "")
        decision = load_object(decision_path) if decision_path.is_file() else {}
        verification = load_object(verification_path) if verification_path.is_file() else {}
        authenticated = (
            event.get("cycle_id") == state.get("cycle_id")
            and event.get("stage") == 4
            and event.get("verdict") == "STAGE4_REJECTED"
            and decision.get("cycle_id") == state.get("cycle_id")
            and decision.get("stage") == 4
            and decision.get("verdict") == "STAGE4_REJECTED"
            and verification.get("cycle_id") == state.get("cycle_id")
            and verification.get("stage") == 4
            and verification.get("passed") is True
            and event.get("decision_sha256") == sha256(decision_path)
            and event.get("verification_sha256") == sha256(verification_path)
            and verification.get("decision_sha256") == sha256(decision_path)
        )
        if not authenticated:
            return {**base, "status": "UNKNOWN_STALLED", "action": "none",
                    "detail": "stage4_rejected lacks hash-authenticated same-cycle decision and verification"}
        if control.get("dispatch_enabled") is not True or 1 not in allowed:
            return {**base, "status": "COMPLETE_HELD_AFTER_STAGE4", "action": "none",
                    "detail": "verified Stage 4 rejection is ready for rotation but Stage 1 dispatch is disabled"}
        if event.get("consumed") is True:
            return {**base, "status": "ROTATION_ALREADY_CONSUMED", "action": "none",
                    "detail": "Stage 4 rejection event was already consumed"}
        return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1",
                "detail": "verified Stage 4 rejection is ready for queue rotation",
                "event_id": event.get("event_id"), "event_path": str(event_path.resolve())}
    if state.get("stage_status") in {"stage3_complete_held", "stage3_failed_held"}:
        valid, detail, event = validate_stage3_completion(state)
        if not valid:
            return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": detail}
        if hold_after == 3 or control.get("dispatch_enabled") is not True:
            return {**base, "status": "COMPLETE_HELD_AFTER_STAGE3", "action": "none", "detail": detail}
        if event.get("consumed") is True:
            return {**base, "status": "COMPLETE_ALREADY_CONSUMED", "action": "none", "detail": detail}
        action = "dispatch_stage4" if event.get("verdict") == "READY_FOR_STAGE4" else "dispatch_stage1"
        target = 4 if action == "dispatch_stage4" else 1
        if target not in allowed:
            return {**base, "status": "COMPLETE_HELD_AFTER_STAGE3", "action": "none", "detail": f"{detail}; target stage is not allowed"}
        return {**base, "status": f"DISPATCH_READY_STAGE{target}", "action": action, "detail": detail,
                "event_id": event.get("event_id")}
    # Six 2026-09-15: stage4_complete_held means Stage 4 plan written,
    # ready for narrow opt (Stage 4.5) launch.
    if state.get("stage_status") == "stage4_complete_held":
        state = dict(state)
        state["last_stage4_decision"] = state.get("last_stage4_decision") or state.get("stage4_decision")
        state["last_stage4_verification"] = state.get("last_stage4_verification") or state.get("stage4_verification")
        state["last_stage4_event"] = state.get("last_stage4_event") or state.get("stage4_event")
        valid, detail, event = _validate_verified_transition(state, 4, "last_stage4_decision", "last_stage4_verification", "last_stage4_event", {"STAGE4_PLANNED"})
        if not valid:
            return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": detail}
        if control.get("dispatch_enabled") is not True:
            return {**base, "status": "DISPATCH_HELD_STAGE45", "action": "none", "detail": f"{detail}; dispatch disabled"}
        if 5 not in allowed and 4.5 not in allowed:
            return {**base, "status": "COMPLETE_HELD_STAGE45", "action": "none", "detail": f"{detail}; Stage 4.5 is not allowed"}
        try:
            next_run = int(state.get("narrow_run_number", 1))
        except (TypeError, ValueError):
            return {**base, "status": "STAGE45_CONTINUATION_HELD", "action": "none",
                    "detail": "Stage 4.5 next-run cursor is invalid"}
        if next_run not in {1, 2, 3}:
            return {**base, "status": "STAGE45_CONTINUATION_HELD", "action": "none",
                    "detail": f"Stage 4.5 next-run cursor {next_run} is outside the authorized three-run sequence"}
        dispatch_identity = None
        if next_run > 1:
            prior_valid, prior_detail, dispatch_identity = validate_stage45_continuation(
                state, next_run - 1, PROJECT_STATE)
            if not prior_valid:
                return {**base, "status": "STAGE45_CONTINUATION_HELD", "action": "none",
                        "detail": prior_detail}
        return {**base, "status": "DISPATCH_READY_STAGE45", "action": "dispatch_stage45", "detail": detail,
                "event_id": event["event_id"], "stage45_run_number": next_run,
                **({"dispatch_identity": dispatch_identity} if dispatch_identity else {})}
    if state.get("stage_status") in {"stage45_complete_held", "stage4_5_complete_held", "narrow_opt_complete"}:
        state = dict(state)
        state["last_stage45_decision"] = state.get("last_stage45_decision") or state.get("narrow_opt_decision")
        state["last_stage45_event"] = state.get("last_stage45_event") or state.get("narrow_opt_event")
        valid, detail, event = _validate_verified_transition(state, "4.5", "last_stage45_decision", "last_stage45_verification", "last_stage45_event", {"NARROW_OPT_COMPLETE"})
        if not valid:
            return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": detail}
        if control.get("dispatch_enabled") is not True or 5 not in allowed:
            return {**base, "status": "COMPLETE_HELD_STAGE5", "action": "none", "detail": detail}
        return {**base, "status": "DISPATCH_READY_STAGE5", "action": "dispatch_stage5", "detail": detail, "event_id": event["event_id"]}
    if state.get("stage_status") in {"stage5_complete_held", "stage5_failed_held"}:
        valid, detail, event = _validate_verified_transition(state, 5, "last_stage5_decision", "last_stage5_verification", "last_stage5_event", {"HOLD_FOR_REVIEW", "STAGE5_FORWARD_FAIL", "STAGE5_FORWARD_PASS"})
        if not valid:
            return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": detail}
        if event.get("consumed") is True:
            return {**base, "status": "COMPLETE_ALREADY_CONSUMED", "action": "none", "detail": detail}
        return {**base, "status": "ROTATION_READY_STAGE1", "action": "rotate_stage1", "detail": detail, "event_id": event["event_id"], "event_path": state.get("last_stage5_event")}
    return {**base, "status": "UNKNOWN_STALLED", "action": "none", "detail": "unrecognized or unauthorized stage"}



STAGE1_EXECUTION_TIMEOUT_SECONDS = 600
STAGE1_NO_ARTIFACT_SETTLE_SECONDS = 300


def stage1_execution_within_artifact_settlement(state: dict, execution_id: str,
                                                control: dict, db_path: Path = DB) -> bool:
    """Return true only for this cycle's freshly completed Stage 1 execution inside grace."""
    job_id = str((control.get("stage_jobs") or {}).get("1") or "")
    if not job_id:
        return False
    record = execution_record(db_path, execution_id, job_id)
    prepared_utc = state.get("stage1_rotation_prepared_utc") or state.get("created_utc")
    if record is None or not record[1] or not record[2] or not prepared_utc:
        return False
    try:
        if parse_utc(record[1]) < parse_utc(str(prepared_utc)):
            return False
        age = (datetime.now(timezone.utc) - parse_utc(record[2])).total_seconds()
    except (TypeError, ValueError):
        return False
    return 0 <= age < STAGE1_NO_ARTIFACT_SETTLE_SECONDS


def expire_stale_stage1(state_path: Path, control: dict | None = None, db_path: Path = DB) -> bool:
    state = load_object(state_path)
    if state.get("current_stage") != "stage1_running" or state.get("stage_status") != "stage1_rotation_prepared":
        return False
    control = control or {}
    execution_id = control.get("last_stage1_execution_id")
    job_id = str((control.get("stage_jobs") or {}).get("1") or "")
    prepared_utc = state.get("stage1_rotation_prepared_utc") or state.get("created_utc")
    # A prepared cycle can remain idle while the dispatcher or operator is
    # paused. Its update timestamp is not an execution start. Only age out a
    # Stage 1 run when Hermes has a matching execution for this job that began
    # at or after this cycle was prepared.
    if not execution_id or not job_id or not prepared_utc:
        return False
    record = execution_record(db_path, str(execution_id), job_id)
    if record is None or not record[1]:
        return False
    try:
        if parse_utc(record[1]) < parse_utc(str(prepared_utc)):
            return False
        age = (datetime.now(timezone.utc) - parse_utc(record[1])).total_seconds()
    except (TypeError, ValueError):
        return False
    if record[0] in {"claimed", "running", "started", "queued"}:
        return False
    detail = execution_error(db_path, str(execution_id)) if record[0] == "failed" else ""
    if is_provider_quota_failure(detail):
        cycle = state["cycle_id"]
        event_path = state_path.parent / "events" / f"{cycle}.stage1.provider_quota.failed.json"
        event = {
            "schema_version": 2, "event_id": f"{cycle}:stage1:provider_quota_failure",
            "cycle_id": cycle, "stage": 1, "status": "failed",
            "failure_kind": "STAGE1_PROVIDER_QUOTA_FAILURE",
            "failure_detail": "Hermes Stage 1 execution failed with HTTP 429 provider token-plan usage limit; rotation is disabled until provider capacity recovers.",
            "ea": state.get("current_ea"), "failed_utc": utc_now(),
            "hermes_execution_id": str(execution_id), "rotation_eligible": False,
        }
        atomic_json(event_path, event)
        next_state = dict(state)
        next_state.update({
            "stage_status": "stage1_failed_held", "next_action": "hold_provider_quota",
            "stage1_failure_event": str(event_path.resolve()), "updated_utc": event["failed_utc"],
        })
        atomic_json(state_path, next_state)
        return True
    if age < STAGE1_EXECUTION_TIMEOUT_SECONDS:
        return False
    cycle = state["cycle_id"]
    event_path = state_path.parent / "events" / f"{cycle}.stage1.failed.json"
    event = {
        "schema_version": 2, "event_id": f"{cycle}:stage1:timeout",
        "cycle_id": cycle, "stage": 1, "status": "failed",
        "failure_kind": "STAGE1_EXECUTION_TIMEOUT",
        "ea": state.get("current_ea"), "failed_utc": utc_now(),
        "timeout_seconds": STAGE1_EXECUTION_TIMEOUT_SECONDS,
        "rotation_eligible": True,
    }
    atomic_json(event_path, event)
    next_state = dict(state)
    next_state.update({
        "stage_status": "stage1_failed_held", "next_action": "rotate_stage1",
        "stage1_failure_event": str(event_path.resolve()), "updated_utc": event["failed_utc"],
    })
    atomic_json(state_path, next_state)
    return True


def reconcile_stage1_provider_quota_failure(state_path: Path, control: dict, db_path: Path = DB) -> bool:
    """Hold the current Stage 1 cycle immediately on a terminal provider quota error."""
    state = load_object(state_path)
    if state.get("current_stage") != "stage1_running":
        return False
    execution_id = str(control.get("last_stage1_execution_id") or "")
    if not execution_id or execution_status(db_path, execution_id) != "failed":
        return False
    existing_path = Path(str(state.get("stage1_failure_event") or ""))
    if state.get("stage_status") in {"stage1_failed_held", "stage1_provider_quota_recovery_ready"} and existing_path.is_file():
        try:
            existing = load_object(existing_path)
        except (OSError, ValueError):
            existing = {}
        if (existing.get("failure_kind") == "STAGE1_PROVIDER_QUOTA_FAILURE"
                and existing.get("hermes_execution_id") == execution_id):
            return False
    detail = execution_error(db_path, execution_id)
    if not is_provider_quota_failure(detail):
        return False
    cycle = str(state.get("cycle_id") or "")
    if not cycle:
        return False
    digest = hashlib.sha256(execution_id.encode("utf-8")).hexdigest()[:12].upper()
    event_path = state_path.parent / "events" / f"{cycle}.stage1.provider_quota.{digest}.failed.json"
    event = {
        "schema_version": 2,
        "event_id": f"{cycle}:stage1:provider_quota_failure:{digest}",
        "cycle_id": cycle,
        "stage": 1,
        "status": "failed",
        "failure_kind": "STAGE1_PROVIDER_QUOTA_FAILURE",
        "failure_detail": "Hermes Stage 1 execution failed with HTTP 429 provider token-plan usage limit; cycle held without candidate rotation.",
        "ea": state.get("current_ea"),
        "failed_utc": utc_now(),
        "hermes_execution_id": execution_id,
        "rotation_eligible": False,
    }
    if event_path.is_file():
        existing = load_object(event_path)
        if existing.get("event_id") != event["event_id"] or existing.get("hermes_execution_id") != execution_id:
            raise ValueError(f"conflicting provider quota failure event exists: {event_path}")
    else:
        atomic_json(event_path, event)
    next_state = dict(state)
    next_state.update({
        "stage_status": "stage1_failed_held",
        "next_action": "hold_provider_quota",
        "stage1_failure_event": str(event_path.resolve()),
        "stage1_provider_quota_retry_authorization": None,
        "updated_utc": event["failed_utc"],
    })
    atomic_json(state_path, next_state)
    return True


def reconcile_duplicate_stage1_candidate(state_path: Path, control: dict,
                                         db_path: Path = DB) -> bool:
    """Rotate a fresh research cycle when its immutable draft duplicates prior authenticated Stage 3 work."""
    state = load_object(state_path)
    cycle = str(state.get("cycle_id") or "")
    execution_id = str(control.get("last_stage1_execution_id") or "")
    stage1_job = str((control.get("stage_jobs") or {}).get("1") or "")
    if (not cycle or state.get("current_stage") != "stage1_running"
            or state.get("stage_status") != "stage1_rotation_prepared"
            or not execution_id or not stage1_job
            or execution_status(db_path, execution_id) != "completed"):
        return False
    record = execution_record(db_path, execution_id, stage1_job)
    prepared_utc = state.get("stage1_rotation_prepared_utc") or state.get("created_utc")
    if record is None or not record[1] or not record[2] or not prepared_utc:
        return False
    try:
        if parse_utc(record[1]) < parse_utc(str(prepared_utc)):
            return False
        if (datetime.now(timezone.utc) - parse_utc(record[2])).total_seconds() < STAGE1_NO_ARTIFACT_SETTLE_SECONDS:
            return False
    except (TypeError, ValueError):
        return False

    canonical_handoff = state_path.parent / "handoffs" / "Stage1_v2.json"
    handoff_path = Path(state.get("last_stage1_handoff") or canonical_handoff)
    if not handoff_path.is_file() or load_object(handoff_path).get("cycle_id") != cycle:
        handoff_path = canonical_handoff
    if not handoff_path.is_file():
        return False
    handoff = load_object(handoff_path)
    spec_path = Path(handoff.get("expected_spec_path") or "")
    event_path = Path(handoff.get("expected_event_path") or "")
    if (handoff.get("cycle_id") != cycle or handoff.get("research_mode") != "research_new"
            or not spec_path.is_file() or event_path.exists()):
        return False
    try:
        spec = load_object(spec_path)
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    ea = str(spec.get("ea") or "")
    selection = handoff.get("primitive_selection") or {}
    previous_artifact = Path(state.get("last_stage1_artifact") or "")
    if previous_artifact.is_file():
        try:
            prior_spec = load_object(previous_artifact)
        except (OSError, ValueError, json.JSONDecodeError):
            return False
        if (prior_spec.get("cycle_id") == cycle
                and previous_artifact.resolve() != spec_path.resolve()):
            return False
    if (not ea or spec.get("cycle_id") != cycle or spec.get("edge") != state.get("current_edge")
            or spec.get("primitive") != state.get("current_primitive")
            or handoff.get("candidate_ea") != ea
            or selection.get("primitive") != spec.get("primitive")
            or Path(handoff.get("expected_spec_path") or "").resolve() != spec_path.resolve()):
        return False

    prior = None
    for candidate in sorted((state_path.parent / "events").glob("*.stage3.complete.json"),
                            key=lambda path: path.name, reverse=True):
        prior = rotation.authenticated_prior_stage3_for_ea(ea, candidate, state_path.parent)
        if prior:
            break
    if not prior:
        return False

    failed_event = event_path.with_name(event_path.name.replace(
        ".stage1.complete.json", ".stage1.failed.json"))
    spec_hash = sha256(spec_path)
    failure = {
        "schema_version": 2,
        "event_id": f"{cycle}:stage1:failed:DUPLICATE_AUTHENTICATED_EXPERIMENT",
        "cycle_id": cycle, "stage": 1, "status": "failed",
        "failure_kind": "DUPLICATE_AUTHENTICATED_EXPERIMENT",
        "failure_detail": "The prepared candidate already has a hash-verified Stage 3 completion; rotate to a novel candidate.",
        "ea": ea, "edge": spec.get("edge"), "primitive": spec.get("primitive"),
        "stage1_spec": str(spec_path.resolve()), "stage1_spec_sha256": spec_hash,
        "duplicate_stage3_event": prior["path"],
        "duplicate_stage3_event_sha256": prior["sha256"],
        "duplicate_stage3_verdict": prior["event"].get("verdict"),
        "hermes_execution_id": execution_id, "failed_utc": utc_now(),
        "rotation_eligible": True, "consumed": False,
    }
    if failed_event.exists():
        try:
            existing = load_object(failed_event)
        except (OSError, ValueError, json.JSONDecodeError):
            return False
        if (existing.get("event_id") != failure["event_id"]
                or existing.get("stage1_spec_sha256") != spec_hash
                or existing.get("duplicate_stage3_event_sha256") != prior["sha256"]):
            return False
        failure = existing
    else:
        atomic_json(failed_event, failure)

    updated = dict(state)
    updated.update({
        "current_ea": ea, "candidate_ea": ea,
        "stage_status": "stage1_failed_held", "next_action": "rotate_stage1",
        "stage1_failure_event": str(failed_event.resolve()),
        "last_stage1_event": str(failed_event.resolve()),
        "last_stage1_artifact": str(spec_path.resolve()),
        "last_stage1_handoff": str(handoff_path.resolve()),
        "updated_utc": failure["failed_utc"],
    })
    if not rotation.authenticated_duplicate_stage1_attempt(updated, state_path.parent):
        return False
    atomic_json(state_path, updated)
    return True


def reconcile_completed_stage1_without_artifact(state_path: Path, control: dict,
                                                db_path: Path = DB) -> bool:
    """Record the same bounded research retry when a completed run publishes nothing.

    dispatch_job can observe a fast completion inline, but a longer Hermes
    execution may finish after dispatch_job returns. Reconcile that terminal
    case from the durable execution row and live Stage 1 handoff so the
    supervisor does not repeatedly redispatch an already completed run.
    """
    state = load_object(state_path)
    cycle = str(state.get("cycle_id") or "")
    execution_id = str(control.get("last_stage1_execution_id") or "")
    stage1_job = str((control.get("stage_jobs") or {}).get("1") or "")
    stage_status = state.get("stage_status")
    retrying_research = (stage_status == "stage1_research_required"
                         and state.get("next_action") == "research_new")
    if (not cycle or state.get("current_stage") != "stage1_running"
            or stage_status not in {"stage1_rotation_prepared", "stage1_research_required"}
            or not execution_id or not stage1_job
            or execution_status(db_path, execution_id) != "completed"):
        return False
    record = execution_record(db_path, execution_id, stage1_job)
    if record is None:
        return False
    # Control state can retain the previous cycle's completed execution ID
    # while a new rotation is being prepared. Only reconcile an execution
    # whose start is at or after this cycle's Stage 1 preparation boundary.
    prepared_utc = (state.get("stage1_rotation_prepared_utc")
                    or state.get("created_utc"))
    if not prepared_utc:
        match = re.match(r"^kcos-(\d{8}T\d{6}Z)(?:-|$)", cycle)
        if match:
            prepared_utc = datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc).isoformat()
    if not prepared_utc or not record[1]:
        return False
    try:
        if parse_utc(record[1]) < parse_utc(prepared_utc):
            return False
        if not record[2]:
            return False
        artifact_settlement_age = (datetime.now(timezone.utc) - parse_utc(record[2])).total_seconds()
        if artifact_settlement_age < STAGE1_NO_ARTIFACT_SETTLE_SECONDS:
            return False
    except (TypeError, ValueError):
        return False
    handoff_path = Path(state.get("last_stage1_handoff") or
                        (state_path.parent / "handoffs" / "Stage1_v2.json"))
    if not handoff_path.is_file():
        return False
    handoff = load_object(handoff_path)
    prior_failure_path = Path(state.get("stage1_failure_event") or "")
    prior_failure = load_object(prior_failure_path) if prior_failure_path.is_file() else {}
    same_execution_failure = (retrying_research
                              and prior_failure.get("hermes_execution_id") == execution_id)
    expected_spec = Path(handoff.get("expected_spec_path") or "")
    expected_event = Path(handoff.get("expected_event_path") or "")
    if (handoff.get("cycle_id") != cycle or handoff.get("research_mode") != "research_new"
            or not expected_spec or not expected_event
            or expected_spec.exists() or expected_event.exists()):
        return False
    # A completed Stage 1 research hold is a terminal outcome, not a missing
    # artifact. Accept only a hold bound to this cycle and edge that records
    # no publication or state mutation and whose captured source bodies still
    # match their recorded digests. An authorized repair retry takes precedence
    # so an older hold cannot block it.
    retry_count = int(state.get("stage1_research_retry_count", 0))
    if stage1_research_retry_repair_ticket(state, retry_count) is None:
        events_dir = state_path.parent / "events"
        required_untouched = {
            "state/kcos_cycle_state_v2.json", "state/handoffs/Stage1_v2.json",
            "state/primitive_pool.json", "state/edge_queue.json",
            "state/attempt_history.json", "expected_spec_path", "expected_event_path",
            "published_spec", "published_event",
        }
        for hold_path in sorted(events_dir.glob(f"{cycle}.stage1.hold.*.json"),
                                key=lambda item: item.stat().st_mtime, reverse=True):
            try:
                hold = load_object(hold_path)
                source_records = hold.get("source_verification") or hold.get("source_records")
                candidate_reviews = hold.get("candidate_review") or hold.get("candidate_reviews")
                untouched = set(hold.get("no_state_mutation_to") or [])
                hold_recorded_utc = (hold.get("tick_utc") or hold.get("created_utc")
                                     or hold.get("failed_utc"))
                repair_ticket = state.get("stage1_research_retry_repair")
                hold_is_fresh_after_repair = True
                reconfirmation_output_path = None
                if (isinstance(repair_ticket, dict)
                        and repair_ticket.get("cycle_id") == cycle
                        and repair_ticket.get("status") == "consumed"):
                    hold_is_fresh_after_repair = (
                        bool(hold_recorded_utc)
                        and bool(repair_ticket.get("consumed_utc"))
                        and parse_utc(str(hold_recorded_utc))
                        > parse_utc(str(repair_ticket["consumed_utc"]))
                    )
                    if not hold_is_fresh_after_repair:
                        try:
                            finished = parse_utc(str(record[2])).astimezone()
                            output_path = (Path(os.environ.get("LOCALAPPDATA", ""))
                                           / "hermes" / "cron" / "output" / stage1_job
                                           / finished.strftime("%Y-%m-%d_%H-%M-%S.md"))
                            output = output_path.read_text(encoding="utf-8")
                            response = output.rsplit("\n## Response\n", 1)[-1]
                            run_time = finished.strftime("%Y-%m-%d %H:%M:%S")
                            expected_hold_name = hold_path.name
                            report_confirms_hold = (
                                "# Cron Job: kcos_stage1_research_new" in output
                                and f"**Job ID:** {stage1_job}" in output
                                and f"**Run Time:** {run_time}" in output
                                and cycle in response
                                and "Existing same-cycle hold remains the valid outcome" in response
                                and expected_hold_name in response
                                and "No spec was written, no publisher was invoked, and no queue/pool mutation was made." in response
                            )
                            if (report_confirms_hold
                                    and parse_utc(str(record[2]))
                                    > parse_utc(str(repair_ticket["consumed_utc"]))):
                                reconfirmation_output_path = output_path
                        except (OSError, ValueError, TypeError, KeyError):
                            reconfirmation_output_path = None
                legacy_hold_valid = (
                    hold.get("schema_version") == 1
                    and hold.get("kind") == "stage1_research_new_hold"
                    and str(hold.get("event_type") or "").startswith("stage1.hold.")
                    and hold.get("cycle_id") == cycle
                    and hold.get("edge") == state.get("current_edge")
                    and hold.get("research_mode") == "research_new"
                    and hold.get("eligible_decision") == "PERSIST_HOLD"
                    and hold.get("publisher_invoked") is False
                    and hold.get("spec_written") is False
                    and hold.get("pool_mutated") is False
                    and hold.get("queue_mutated") is False
                    and hold.get("selection_not_committed") is True
                    and (hold_is_fresh_after_repair or reconfirmation_output_path is not None)
                    and required_untouched.issubset(untouched)
                    and isinstance(source_records, list) and len(source_records) >= 2
                    and isinstance(candidate_reviews, list) and len(candidate_reviews) >= 1
                    and all(isinstance(item, dict)
                            and str(item.get("decision") or "").startswith("rejected_")
                            for item in candidate_reviews)
                    and all(isinstance(item, dict)
                            and isinstance(item.get("content_path"), str)
                            and isinstance(item.get("content_sha256"), str)
                            and Path(item["content_path"]).is_file()
                            and sha256(Path(item["content_path"])) == item["content_sha256"].upper()
                            for item in source_records)
                )
                current_schema_sources = (
                    isinstance(source_records, list)
                    and len(source_records) >= 2
                    and all(isinstance(item, dict)
                            and isinstance(item.get("content_path"), str)
                            and isinstance(item.get("content_sha256"), str)
                            and Path(item["content_path"]).is_file()
                            and sha256(Path(item["content_path"]))
                            == item["content_sha256"].upper()
                            for item in source_records)
                    and isinstance(candidate_reviews, list)
                    and len(candidate_reviews) >= 1
                    and all(isinstance(item, dict)
                            and str(item.get("decision") or "").startswith("rejected_")
                            for item in candidate_reviews)
                )
                current_schema_empty_search = (
                    isinstance(hold.get("search_attempts"), list)
                    and len(hold["search_attempts"]) >= 3
                    and all(isinstance(item, dict)
                            and isinstance(item.get("query"), str) and item["query"].strip()
                            and type(item.get("result_count")) is int
                            and item["result_count"] == 0
                            and isinstance(item.get("retrieved_utc"), str)
                            and parse_utc(item["retrieved_utc"]) >= parse_utc(record[1])
                            and parse_utc(item["retrieved_utc"]) <= parse_utc(hold_recorded_utc)
                            for item in hold["search_attempts"])
                    and not source_records
                )
                stage1_hold_wrappers = [state_path.parent / "research" / cycle / "stage1_hold.json"]
                stage1_hold_wrappers.extend(
                    (state_path.parent / "research" / cycle).glob("stage1_hold_retry_*.json"))
                wrapper_bound = False
                for wrapper_path in stage1_hold_wrappers:
                    if not wrapper_path.is_file():
                        continue
                    wrapper = load_object(wrapper_path)
                    if (wrapper.get("cycle_id") == cycle
                            and Path(str(wrapper.get("hold_event") or "")).resolve()
                            == hold_path.resolve()
                            and isinstance(wrapper.get("hold_reason"), str)
                            and wrapper.get("hold_reason", "").strip()):
                        wrapper_bound = True
                        break
                current_schema_time_bound = False
                try:
                    created = parse_utc(str(hold_recorded_utc))
                    started = parse_utc(str(record[1]))
                    finished = parse_utc(str(record[2]))
                    current_schema_time_bound = started <= created <= finished
                except (TypeError, ValueError):
                    pass
                current_schema_hold_valid = (
                    hold.get("schema_version") == 2
                    and hold.get("cycle_id") == cycle
                    and hold.get("stage") == 1
                    and hold.get("status") == "held"
                    and hold.get("failure_kind") == "RESEARCH_EVIDENCE_REQUIRED"
                    and hold.get("research_mode") == "research_new"
                    and hold.get("edge") == state.get("current_edge")
                    and hold.get("current_stage") == "stage1_running"
                    and hold.get("next_action") == "research_new"
                    and hold.get("selection_not_committed") is True
                    and hold.get("candidate_primitive") is None
                    and hold.get("candidate_ea") is None
                    and hold.get("publisher_invoked") is False
                    and hold.get("spec_written") is False
                    and hold.get("pool_mutated") is False
                    and hold.get("queue_mutated") is False
                    and hold.get("rotation_eligible") is False
                    and hold.get("consumed") is False
                    and isinstance(hold.get("reason"), str) and hold["reason"].strip()
                    and (current_schema_sources or current_schema_empty_search)
                    and wrapper_bound
                    and current_schema_time_bound
                )
                valid = legacy_hold_valid or current_schema_hold_valid
            except (OSError, ValueError, TypeError, KeyError):
                continue
            if not valid:
                continue

            repair_ticket = state.get("stage1_research_retry_repair")
            repair_tag = (str(repair_ticket.get("repair_id") or "reconfirmed")
                          if isinstance(repair_ticket, dict)
                          and reconfirmation_output_path is not None else None)
            held_event_path = events_dir / (
                f"{cycle}.stage1.held.repair-{repair_tag}.json" if repair_tag
                else f"{cycle}.stage1.held.json")
            held_event = {
                "schema_version": 2,
                "event_id": f"{cycle}:stage1:held:{sha256(hold_path)}"
                            + (f":{sha256(reconfirmation_output_path)}"
                               if reconfirmation_output_path is not None else ""),
                "cycle_id": cycle, "stage": 1, "status": "held",
                "failure_kind": "STAGE1_RESEARCH_HOLD",
                "failure_detail": str(hold.get("reason") or "Stage 1 research hold"),
                "stage1_hold_event": str(hold_path.resolve()),
                "stage1_hold_event_sha256": sha256(hold_path),
                "observed_after_execution_id": execution_id,
                "reconciled_utc": utc_now(), "consumed": False,
            }
            if reconfirmation_output_path is not None:
                held_event["stage1_reconfirmation_output"] = str(reconfirmation_output_path.resolve())
                held_event["stage1_reconfirmation_output_sha256"] = sha256(reconfirmation_output_path)
            if held_event_path.exists():
                existing = load_object(held_event_path)
                if (existing.get("cycle_id") != cycle
                        or existing.get("stage1_hold_event_sha256") != held_event["stage1_hold_event_sha256"]):
                    return False
            else:
                atomic_json(held_event_path, held_event)
            updated = dict(state)
            updated.update({
                "stage_status": "stage1_failed_held",
                "next_action": "hold_no_eligible_stage1_candidate",
                "stage1_failure_event": str(held_event_path.resolve()),
                "stage1_research_event": str(hold_path.resolve()),
                "last_stage1_event": str(held_event_path.resolve()),
                "last_exit": "stage1_research_held",
                "updated_utc": held_event["reconciled_utc"],
            })
            atomic_json(state_path, updated)
            return True

    if same_execution_failure:
        return False

    failed_event = expected_event.with_name(expected_event.name.replace(".complete.json", ".failed.json"))
    if failed_event.exists():
        existing = load_object(failed_event)
        if (existing.get("cycle_id") == cycle
                and existing.get("hermes_execution_id") == execution_id
                and existing.get("failure_kind") == "STAGE1_EXECUTION_NO_ARTIFACT"):
            return False
        # A bounded retry needs its own immutable failure record. Keep the
        # canonical first-attempt event and bind this one to its Hermes run ID.
        retry_tag = hashlib.sha256(execution_id.encode("utf-8")).hexdigest()[:12]
        failed_event = failed_event.with_name(
            f"{failed_event.stem}.retry-{retry_tag}{failed_event.suffix}")
        if failed_event.exists():
            existing = load_object(failed_event)
            return (existing.get("cycle_id") == cycle
                    and existing.get("hermes_execution_id") == execution_id
                    and existing.get("failure_kind") == "STAGE1_EXECUTION_NO_ARTIFACT")
    failure = {
        "schema_version": 2,
        "event_id": f"{cycle}:stage1:failed:STAGE1_EXECUTION_NO_ARTIFACT",
        "cycle_id": cycle, "stage": 1, "status": "failed",
        "failure_kind": "STAGE1_EXECUTION_NO_ARTIFACT",
        "failure_detail": "Stage 1 Hermes execution completed without the required spec or authenticated event",
        "ea": handoff.get("candidate_ea"), "hermes_execution_id": execution_id,
        "failed_utc": utc_now(), "rotation_eligible": True, "consumed": False,
    }
    atomic_json(failed_event, failure)
    updated = dict(state)
    updated.update({
        "stage_status": "stage1_research_required", "next_action": "research_new",
        "candidate_ea": None, "candidate_source": None,
        "failure_count": int(state.get("failure_count", 0)) + 1,
        "stage1_failure_event": str(failed_event.resolve()),
        "last_stage1_event": str(failed_event.resolve()),
        "updated_utc": failure["failed_utc"],
    })
    atomic_json(state_path, updated)
    return True


def reconcile_invalid_stage1_completion(state_path: Path, control: dict, db_path: Path) -> bool:
    """Reject immutable Stage 1 completions whose published contract is unreachable."""
    state = load_object(state_path)
    if state.get("current_stage") != "stage2_pending":
        return False
    valid, detail = validate_stage1_completion(state, control, db_path)
    if not valid and detail == "Stage 1 artifact hash does not match its event":
        cycle = state["cycle_id"]
        event_path = state_path.parent / "events" / f"{cycle}.stage1.failed.json"
        failure = {
            "schema_version": 2,
            "event_id": f"{cycle}:stage1:failed:STAGE1_ARTIFACT_HASH_MISMATCH",
            "cycle_id": cycle, "stage": 1, "status": "failed",
            "failure_kind": "STAGE1_ARTIFACT_HASH_MISMATCH",
            "ea": state.get("current_ea"), "failure_detail": detail,
            "supersedes_event": state.get("last_stage1_event"),
            "failed_utc": utc_now(), "rotation_eligible": True, "consumed": False,
        }
        if event_path.is_file() and load_object(event_path) != failure:
            raise ValueError(f"conflicting Stage 1 failure event exists: {event_path}")
        atomic_json(event_path, failure)
        updated = dict(state)
        updated.update({
            "current_stage": "stage1_running", "stage_status": "stage1_failed_held",
            "next_action": "rotate_stage1", "stage1_failure_event": str(event_path.resolve()),
            "stage1_invalidated_event": state.get("last_stage1_event"),
            "updated_utc": failure["failed_utc"],
        })
        atomic_json(state_path, updated)
        return True
    if not valid:
        return False
    cycle = state["cycle_id"]
    event_path = Path(str(state.get("last_stage1_event") or ""))
    spec_path = Path(str(state.get("last_stage1_artifact") or ""))
    handoff_path = Path(str(state.get("last_stage1_handoff") or state_path.parent / "handoffs" / "Stage1_v2.json"))
    if not all(path.is_file() for path in (event_path, spec_path, handoff_path)):
        return False
    stage1_event = load_object(event_path)
    spec = load_object(spec_path)
    handoff = load_object(handoff_path)
    if (stage1_event.get("cycle_id") != cycle or stage1_event.get("stage") != 1
            or stage1_event.get("status") != "complete"
            or Path(str(stage1_event.get("artifact") or "")).resolve() != spec_path.resolve()
            or stage1_event.get("artifact_sha256") != sha256(spec_path)
            or handoff.get("cycle_id") != cycle
            or Path(str(handoff.get("expected_spec_path") or "")).resolve() != spec_path.resolve()):
        return False
    try:
        stage1_publisher.validate_regime_interval_reachability(spec, config_from_spec(spec))
    except ValueError as exc:
        detail = str(exc)
        if not detail.startswith("STAGE1_SIGNAL_REACHABILITY_FAILURE"):
            return False
    else:
        return False
    cycle_dir = state_path.parent
    receipt_path = cycle_dir / "diagnostics" / f"{cycle}.stage1.contract-review.json"
    failure_path = cycle_dir / "events" / f"{cycle}.stage1.failed.signal-reachability.json"
    receipt = {
        "schema_version": 1, "record_type": "stage1_contract_review",
        "cycle_id": cycle, "candidate_ea": state.get("current_ea"),
        "stage1_event": str(event_path.resolve()), "stage1_event_sha256": sha256(event_path),
        "stage1_spec": str(spec_path.resolve()), "stage1_spec_sha256": sha256(spec_path),
        "validator": str(Path(stage1_publisher.__file__).resolve()),
        "validator_sha256": sha256(Path(stage1_publisher.__file__)),
        "failure_kind": "STAGE1_SIGNAL_REACHABILITY_FAILURE", "failure_detail": detail,
        "reviewed_utc": utc_now(),
    }
    if receipt_path.is_file():
        existing = load_object(receipt_path)
        for key in ("cycle_id", "candidate_ea", "stage1_event_sha256", "stage1_spec_sha256",
                    "validator_sha256", "failure_kind", "failure_detail"):
            if existing.get(key) != receipt.get(key):
                raise ValueError(f"conflicting Stage 1 contract review receipt: {receipt_path}")
        receipt = existing
    else:
        atomic_json(receipt_path, receipt)
    failure = {
        "schema_version": 2,
        "event_id": f"{cycle}:stage1:failed:STAGE1_SIGNAL_REACHABILITY_FAILURE",
        "cycle_id": cycle, "stage": 1, "status": "failed",
        "failure_kind": "STAGE1_SIGNAL_REACHABILITY_FAILURE",
        "ea": state.get("current_ea"),
        "edge": spec.get("edge") or (spec.get("strategy") or {}).get("edge"),
        "primitive": spec.get("primitive"),
        "failure_detail": detail, "stage1_spec": str(spec_path.resolve()),
        "stage1_spec_sha256": sha256(spec_path), "stage1_complete_event": str(event_path.resolve()),
        "stage1_complete_event_sha256": sha256(event_path),
        "contract_review_receipt": str(receipt_path.resolve()),
        "contract_review_receipt_sha256": sha256(receipt_path),
        "supersedes_event": str(event_path.resolve()),
        "failed_utc": receipt["reviewed_utc"], "rotation_eligible": True, "consumed": False,
    }
    if failure_path.is_file() and load_object(failure_path) != failure:
        raise ValueError(f"conflicting Stage 1 failure event exists: {failure_path}")
    atomic_json(failure_path, failure)
    updated = dict(state)
    updated.update({
        "current_stage": "stage1_running", "stage_status": "stage1_failed_held",
        "next_action": "rotate_stage1", "stage1_failure_event": str(failure_path.resolve()),
        "stage1_invalidated_event": str(event_path.resolve()),
        "updated_utc": failure["failed_utc"],
    })
    atomic_json(state_path, updated)
    return True

def prepare_stage3_journal_reclassification_retry(state_path: Path, control: dict,
                                                 db_path: Path) -> dict | None:
    """Recover one exact Stage 3 journal false positive after parser reclassification.

    Only candidate-level genetic input rejects and the authenticated expert-free startup
    notice are eligible. The Stage 2 publication, prior Stage 3 event/runtime, run identity,
    fresh .opt, cycle authorization, and absence of an owned runtime are all rechecked.
    """
    state_dir = state_path.parent
    state = load_object(state_path)
    if (state.get("current_stage") != "stage2_pending"
            or state.get("stage_status") != "stage2_repair_required"
            or state.get("next_action") != "dispatch_stage2"
            or state.get("stage2_repair_source") != "stage3_tester_journal"
            or state.get("stage2_failure_event")
            or state.get("stage3_failure_class") != "infrastructure"
            or int(state.get("stage3_journal_reclassification_retry_count", 0) or 0) >= 1
            or not authorizes_stages_1_to_5(state)
            or control.get("dispatch_enabled") is not True):
        return None
    cycle, ea = state.get("cycle_id"), state.get("current_ea")
    failure_path = Path(str(state.get("stage3_failure_event") or ""))
    handoff_path = Path(str(state.get("last_stage3_handoff") or ""))
    if (not isinstance(cycle, str) or not cycle or not isinstance(ea, str) or not ea
            or not failure_path.is_file() or not handoff_path.is_file()):
        return None
    try:
        import kcos_stage3_driver_v2 as stage3_driver
        import kcos_stage3_launch_v2 as stage3_launcher
        if stage3_driver.authenticated_consumed_stage2_release(state) is None:
            return None
        valid_stage2, _ = validate_stage2_completion(state, control, db_path)
        if not valid_stage2 or rotation.active_stage3_runtime(state, state_dir):
            return None
        failure = load_object(failure_path)
        handoff = load_object(handoff_path)
        runtime_path = Path(str(failure.get("runtime_manifest") or ""))
        if not runtime_path.is_file():
            return None
        runtime = load_object(runtime_path)
        runtime_hash_matches_event = failure.get("runtime_manifest_sha256") == sha256(runtime_path)
        cleanup_binding = runtime.get("failed_terminal_owner_cleanup") or {}
        runtime_hash_matches_cleanup = (
            cleanup_binding.get("status") == "released_after_failed_stage3_with_workers_exited"
            and cleanup_binding.get("cycle_id") == cycle
            and cleanup_binding.get("ea") == ea
            and cleanup_binding.get("failure_event_sha256") == sha256(failure_path)
        )
        if (failure.get("cycle_id") != cycle or failure.get("stage") != 3
                or failure.get("status") != "failed" or failure.get("consumed") is not False
                or failure.get("rotation_eligible") is not False
                or failure.get("failure_kind") != "STAGE3_EXECUTION_FAILURE"
                or failure.get("runtime_failure_kind") != "TESTER_JOURNAL_REJECTED"
                or failure.get("ea") != ea
                or not (runtime_hash_matches_event or runtime_hash_matches_cleanup)
                or handoff.get("cycle_id") != cycle or handoff.get("ea") != ea
                or runtime.get("cycle_id") != cycle or runtime.get("stage") != 3
                or runtime.get("ea") != ea or runtime.get("status") != "INFRASTRUCTURE_FAILURE"
                or runtime.get("failure_kind") != "TESTER_JOURNAL_REJECTED"
                or runtime.get("infrastructure_only") is not True):
            return None
        old_errors = (runtime.get("journal_diagnostic") or {}).get("error_lines") or []
        invalid_pass_re = stage3_launcher.GENETIC_INVALID_PARAMETER_PASS_RE
        no_expert_re = stage3_launcher.NO_EXPERT_IN_START_CONFIG_RE
        if (not old_errors
                or not any(invalid_pass_re.search(str(line)) for line in old_errors)
                or any(not (invalid_pass_re.search(str(line)) or no_expert_re.search(str(line)))
                       for line in old_errors)):
            return None
        log_path = Path(str(runtime.get("tester_log") or ""))
        journal = stage3_launcher.read_fresh_tester_journal(
            log_path, int(runtime.get("tester_log_start_offset", 0)))
        journal = stage3_launcher.filter_authenticated_no_expert_notice(journal, runtime, handoff)
        if (journal.get("error_lines") or not journal.get("invalid_parameter_passes")
                or not stage3_launcher.authenticated_tester_run_started(runtime, handoff, journal)):
            return None
        launch_epoch = float(runtime.get("launched_epoch") or 0)
        cache_dir = log_path.parent.parent / "cache"
        from_date = str(runtime.get("from_date") or handoff.get("from_date") or "").replace(".", "")
        to_date = str(runtime.get("to_date") or handoff.get("to_date") or "").replace(".", "")
        pattern = f"{ea}.{runtime.get('symbol')}.{runtime.get('timeframe')}.{from_date}.{to_date}*.opt"
        before = runtime.get("opt_candidates_before") or {}
        fresh_opts = []
        for opt_path in cache_dir.glob(pattern):
            if not opt_path.is_file() or opt_path.stat().st_mtime < launch_epoch:
                continue
            prior = before.get(str(opt_path.resolve()))
            if prior is None or str(prior.get("sha256") or "").upper() != sha256(opt_path):
                fresh_opts.append(opt_path)
        if len(fresh_opts) != 1:
            return None
        opt_path = fresh_opts[0]
        retry_event_id = f"{cycle}:stage3:journal-reclassification-retry:1"
        archive_dir = state_dir / "diagnostics" / "stage3_journal_reclassification" / cycle
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_sources = {
            "state_before": state_path, "failure_event": failure_path,
            "runtime_manifest": runtime_path, "stage3_handoff": handoff_path, "opt": opt_path,
        }
        archived = {}
        for name, source in archive_sources.items():
            target = archive_dir / ("state.before.json" if name == "state_before" else source.name)
            if not target.exists():
                shutil.copy2(source, target)
            if sha256(source) != sha256(target):
                return None
            archived[name] = {"source": str(source.resolve()), "archive": str(target.resolve()),
                              "sha256": sha256(target)}
        receipt_path = archive_dir / "recovery_receipt.json"
        receipt = {
            "schema_version": 1, "record_kind": "stage3_journal_candidate_reclassification_retry",
            "cycle_id": cycle, "ea": ea, "stage": 3, "repair_complete": True,
            "repair_kind": "stage3_journal_candidate_error_reclassification",
            "retry_event_id": retry_event_id, "recorded_utc": utc_now(),
            "reason": "The installed parser reclassified genetic-pass incorrect-input-parameter lines as candidate diagnostics; authenticated run identity is present and no global journal errors remain.",
            "failure_event_sha256": sha256(failure_path), "runtime_manifest_sha256": sha256(runtime_path),
            "stage2_event_sha256": sha256(Path(state["last_stage2_event"])),
            "stage2_verification_sha256": sha256(Path(state["last_stage2_verification"])),
            "journal_reclassification": {"old_error_line_count": len(old_errors),
                "invalid_parameter_pass_count": len(journal.get("invalid_parameter_passes", [])),
                "global_error_lines_after_reclassification": journal.get("error_lines", []),
                "authenticated_tester_run_started": True},
            "fresh_opt": {"path": str(opt_path.resolve()), "sha256": sha256(opt_path),
                "size_bytes": opt_path.stat().st_size,
                "modified_utc": datetime.fromtimestamp(opt_path.stat().st_mtime, timezone.utc).isoformat()},
            "classifier_sha256": sha256(Path(stage3_launcher.__file__)), "archived_evidence": archived,
        }
        if receipt_path.exists():
            existing = load_object(receipt_path)
            for key in ("record_kind", "cycle_id", "retry_event_id", "failure_event_sha256",
                        "runtime_manifest_sha256", "fresh_opt", "classifier_sha256"):
                if existing.get(key) != receipt.get(key):
                    return None
            receipt = existing
        else:
            atomic_json(receipt_path, receipt)
        retry = {"status": "ready", "cycle_id": cycle, "repair_kind": receipt["repair_kind"],
            "event_id": retry_event_id, "failure_event": archived["failure_event"]["archive"],
            "failure_event_sha256": archived["failure_event"]["sha256"],
            "runtime_manifest": archived["runtime_manifest"]["archive"],
            "runtime_manifest_sha256": archived["runtime_manifest"]["sha256"],
            "repair_receipt": str(receipt_path.resolve()), "repair_receipt_sha256": sha256(receipt_path)}
        current = load_object(state_path)
        if (current.get("cycle_id") != cycle or current.get("stage_status") != "stage2_repair_required"
                or current.get("stage2_repair_source") != "stage3_tester_journal"):
            return None
        current.update({"current_stage": "stage3_pending", "stage_status": "stage2_complete_held",
            "next_action": "run_stage3", "hold_after_stage": int(state["run_authorization"]["hold_after_stage"]),
            "stage2_repair_reason": None, "stage2_repair_source": None, "stage2_repair_journal": None,
            "stage3_failure_class": None, "stage3_journal_reclassification_retry_count": 1,
            "stage3_journal_reclassification_retry_event_id": retry_event_id,
            "stage3_repair_retry": retry,
            "stage3_journal_reclassification_recovery_receipt": str(receipt_path.resolve()),
            "updated_utc": utc_now()})
        atomic_json(state_path, current)
        return {"retry_event_id": retry_event_id, "receipt": str(receipt_path.resolve()),
                "receipt_sha256": sha256(receipt_path), "opt_sha256": sha256(opt_path)}
    except (OSError, ValueError, TypeError, KeyError, RuntimeError, OverflowError):
        return None


def stage3_journal_retry_identity(state: dict) -> dict | None:
    """Validate the one hash-bound journal retry before creating a new Hermes claim."""
    retry = state.get("stage3_repair_retry")
    if (not isinstance(retry, dict) or retry.get("status") != "ready"
            or retry.get("repair_kind") != "stage3_journal_candidate_error_reclassification"
            or retry.get("cycle_id") != state.get("cycle_id")
            or state.get("stage3_journal_reclassification_retry_count") != 1
            or retry.get("event_id") != state.get("stage3_journal_reclassification_retry_event_id")):
        return None
    try:
        failure_path, runtime_path, receipt_path = (Path(retry[k]) for k in
            ("failure_event", "runtime_manifest", "repair_receipt"))
        if any(not path.is_file() for path in (failure_path, runtime_path, receipt_path)):
            return None
        if (sha256(failure_path) != retry.get("failure_event_sha256")
                or sha256(runtime_path) != retry.get("runtime_manifest_sha256")
                or sha256(receipt_path) != retry.get("repair_receipt_sha256")):
            return None
        failure, runtime, receipt = (load_object(path) for path in (failure_path, runtime_path, receipt_path))
        if (failure.get("cycle_id") != state.get("cycle_id") or failure.get("stage") != 3
                or failure.get("status") != "failed" or failure.get("rotation_eligible") is not False
                or runtime.get("cycle_id") != state.get("cycle_id") or runtime.get("stage") != 3
                or runtime.get("status") != "INFRASTRUCTURE_FAILURE" or runtime.get("infrastructure_only") is not True
                or receipt.get("cycle_id") != state.get("cycle_id") or receipt.get("stage") != 3
                or receipt.get("repair_complete") is not True or receipt.get("retry_event_id") != retry.get("event_id")
                or receipt.get("failure_event_sha256") != retry.get("failure_event_sha256")
                or receipt.get("runtime_manifest_sha256") != retry.get("runtime_manifest_sha256")):
            return None
        identity = stage3_start_content_identity(state)
        identity["stage3_retry_event_id"] = retry["event_id"]
        identity["stage3_repair_receipt_sha256"] = retry["repair_receipt_sha256"]
        return identity
    except (OSError, ValueError, TypeError, KeyError):
        return None


def cleanup_failed_stage3_runtime_owner(state: dict, state_dir: Path) -> bool:
    """Release an isolated terminal after a recorded Stage 3 failure, once all workers have exited.

    A failed Stage 3 runtime can leave its portable terminal open after the Tester
    workers have stopped. That terminal otherwise keeps same-cycle Stage 2 recovery
    waiting forever. Cleanup is limited to the exact current-cycle failure, manifest,
    launch identity, and portable process; uncertain or still-working runs stay held.
    """
    if (state.get("current_stage") != "stage2_pending"
            or state.get("stage_status") != "stage2_repair_required"
            or state.get("next_action") != "dispatch_stage2"
            or state.get("stage3_failure_class") != "infrastructure"):
        return False
    cycle, ea = state.get("cycle_id"), state.get("current_ea")
    if not isinstance(cycle, str) or not cycle or not isinstance(ea, str) or not ea:
        return False
    failure_path = Path(str(state.get("stage3_failure_event") or ""))
    handoff_path = Path(str(state.get("last_stage3_handoff") or ""))
    runtime_dir = (state_dir / "stage3_v2").resolve()
    if not (failure_path.is_file() and handoff_path.is_file()):
        return False
    try:
        handoff = load_object(handoff_path)
    except (OSError, ValueError, TypeError, KeyError):
        return False
    runtime_path = Path(str(handoff.get("runtime_manifest") or "")).resolve()
    if (runtime_path.parent != runtime_dir
            or not runtime_path.name.startswith(f"{cycle}.stage3.runtime")):
        return False
    if not runtime_path.is_file():
        return False
    failure, runtime = load_object(failure_path), load_object(runtime_path)
    if (failure.get("cycle_id") != cycle or failure.get("stage") != 3
            or failure.get("status") != "failed" or failure.get("consumed") is not False
            or failure.get("rotation_eligible") is not False or failure.get("ea") != ea
            or handoff.get("cycle_id") != cycle or handoff.get("ea") != ea
            or Path(str(handoff.get("runtime_manifest") or "")).resolve() != runtime_path
            or runtime.get("cycle_id") != cycle or runtime.get("stage") != 3
            or runtime.get("ea") != ea or runtime.get("status") != "INFRASTRUCTURE_FAILURE"
            or runtime.get("infrastructure_only") is not True
            or runtime.get("failure_kind") != "TESTER_JOURNAL_REJECTED"
            or runtime.get("timeframe") not in {"M5", "M15", "H1"}):
        return False
    # Newer failure events bind their exact runtime. Legacy events are accepted only
    # through the current-cycle handoff and canonical runtime filename above.
    event_runtime = failure.get("runtime_manifest")
    if event_runtime and Path(str(event_runtime)).resolve() != runtime_path:
        return False
    event_runtime_hash = failure.get("runtime_manifest_sha256")
    if event_runtime_hash and str(event_runtime_hash).upper() != sha256(runtime_path):
        return False
    event_timeframe = failure.get("timeframe")
    if event_timeframe and event_timeframe != runtime.get("timeframe"):
        return False

    from kcos_stage3_launch_v2 import capture_owned_process, cleanup_previous_runtime, DEFAULT_TERMINAL
    terminal = Path(DEFAULT_TERMINAL).resolve()
    launched_epoch = float(runtime.get("launched_epoch") or 0)
    if launched_epoch <= 0:
        return False
    terminal_pid = runtime.get("terminal_pid")
    terminal_evidence = runtime.get("terminal_process_evidence") or {}
    if (not isinstance(terminal_pid, int) or terminal_pid <= 0
            or terminal_evidence.get("pid") != terminal_pid
            or Path(str(terminal_evidence.get("exe") or "")).resolve() != terminal):
        return False
    try:
        terminal_owner = capture_owned_process(
            terminal_pid, "terminal", terminal, runtime_path, launched_epoch)
        if terminal_owner is None:
            return False
        expected_created = float(terminal_evidence.get("create_time"))
        if abs(float(terminal_owner["create_time"]) - expected_created) > 0.5:
            return False
        expected_command = terminal_evidence.get("command")
        if not isinstance(expected_command, list) or terminal_owner["process"].cmdline() != expected_command:
            return False

        worker_evidence = runtime.get("worker_process_evidence") or []
        if not isinstance(worker_evidence, list):
            return False
        evidence_by_pid = {int(item["pid"]): item for item in worker_evidence
                           if isinstance(item, dict) and item.get("pid") is not None}
        worker_pids = runtime.get("worker_pids") or []
        if not isinstance(worker_pids, list) or any(int(pid) not in evidence_by_pid for pid in worker_pids):
            return False
        for pid in worker_pids:
            item = evidence_by_pid[int(pid)]
            worker = capture_owned_process(
                int(pid), "worker", terminal, runtime_path, launched_epoch,
                str(item.get("address") or ""), Path(str(item.get("exe") or "")), True)
            if worker is not None:
                return False
        if runtime.get("watcher_pid"):
            watcher = capture_owned_process(
                int(runtime["watcher_pid"]), "watcher", terminal, runtime_path, launched_epoch)
            if watcher is not None:
                return False

        # The launcher revalidates PID, executable, and launch window immediately
        # before closing the terminal and snapshots the final Journal first.
        cleanup_previous_runtime(runtime, terminal, runtime_path,
                                 include_worker_process_evidence=True)
    except (OSError, TypeError, ValueError, KeyError, OverflowError):
        return False
    runtime_after = load_object(runtime_path)
    runtime_after["failed_terminal_owner_cleanup"] = {
        "status": "released_after_failed_stage3_with_workers_exited",
        "cycle_id": cycle, "ea": ea, "timeframe": runtime.get("timeframe"),
        "failure_event_sha256": sha256(failure_path),
        "released_utc": utc_now(),
    }
    atomic_json(runtime_path, runtime_after)
    return True


def prepare_stage3_launch_contract_retry(state_path: Path, control: dict,
                                        db_path: Path) -> dict | None:
    """Authorize one cron-owned retry for the exact Stage 1 range re-derivation bug.

    Stage 3 previously rejected an authenticated Stage 2 grid in its launcher
    after preparation had passed. This repair is limited to that recorded error
    and requires the current Stage 2 lineage and M5 launch profile to validate
    before the supervisor moves the cycle back to Stage 3 pending.
    """
    state = load_object(state_path)
    cycle = str(state.get("cycle_id") or "")
    exact_error = "[TesterInputs] optimized parameter InpObvDivLookback has 20 MT5 steps (21 values); invalid for default profile"
    existing_retry = state.get("stage3_launch_contract_retry")
    if (state.get("current_stage") == "stage3_pending"
            and state.get("stage_status") == "stage2_complete_ready"
            and state.get("next_action") == "run_stage3"
            and state.get("stage3_launch_contract_retry_count") == 1
            and isinstance(existing_retry, dict)
            and existing_retry.get("status") == "authorized"
            and existing_retry.get("cycle_id") == cycle):
        try:
            receipt_path = Path(str(existing_retry.get("repair_receipt") or ""))
            failure_path = Path(str(state.get("stage3_failure_event") or ""))
            if (receipt_path.is_file() and failure_path.is_file()
                    and sha256(receipt_path) == existing_retry.get("repair_receipt_sha256")
                    and sha256(failure_path) == existing_retry.get("failure_event_sha256")):
                receipt = load_object(receipt_path)
                failure = load_object(failure_path)
                if (receipt.get("record_kind") == "stage3_stage2_grid_consumer_repair"
                        and receipt.get("repair_complete") is True
                        and receipt.get("stage3_launch_code_sha256")
                            == sha256(Path(__file__).with_name("kcos_stage3_launch_v2.py"))
                        and failure.get("cycle_id") == cycle
                        and failure.get("error") == exact_error):
                    state["stage3_failure_class"] = "infrastructure"
                    state["stage3_failure_detail"] = exact_error
                    state["updated_utc"] = utc_now()
                    atomic_json(state_path, state)
        except (OSError, ValueError, TypeError, KeyError):
            return None
        return None
    if not (
        cycle
        and state.get("current_stage") == "stage3_running"
        and state.get("stage_status") == "stage3_infrastructure_failure"
        and state.get("next_action") == "inspect_stage3_infrastructure_failure"
        and state.get("stage3_failure_class") == "infrastructure"
        and state.get("stage3_failure_detail") == exact_error
        and state.get("stage3_launch_contract_retry_count", 0) == 0
        and authorizes_stages_1_to_5(state)
        and authenticated_stage2_release_for_retry(state)
    ):
        return None
    failure_path = Path(str(state.get("stage3_failure_event") or ""))
    handoff_path = Path(str(state.get("last_stage3_handoff") or ""))
    if not (failure_path.is_file() and handoff_path.is_file()):
        return None
    try:
        failure, handoff = load_object(failure_path), load_object(handoff_path)
        runtime_path = Path(str(handoff.get("runtime_manifest") or ""))
        if (failure.get("cycle_id") != cycle or failure.get("stage") != 3
                or failure.get("status") != "failed" or failure.get("consumed") is not False
                or failure.get("rotation_eligible") is not False
                or failure.get("error") != exact_error
                or failure.get("failure_kind") != "STAGE3_EXECUTION_FAILURE"
                or handoff.get("cycle_id") != cycle or handoff.get("stage") != 3
                or handoff.get("ea") != state.get("current_ea")
                or runtime_path.exists()):
            return None
        valid_stage2, _ = validate_stage2_completion(state, control, db_path)
        if not valid_stage2 or not authenticated_stage2_release_for_retry(state):
            return None
        import kcos_stage3_launch_v2 as launcher
        lineage = launcher.validate_stage3_launch_lineage(handoff)
        launcher.build_command(handoff, "M5", launcher.STAGE3_PORTABLE_TERMINAL)
        if (lineage.get("approved_tester_config_sha256")
                != handoff.get("approved_tester_config_sha256")):
            return None
        if rotation.active_stage3_runtime(state, state_path.parent):
            return None
        import psutil
        for process in psutil.process_iter(["name", "exe"]):
            if str(process.info.get("name") or "").casefold() != "terminal64.exe":
                continue
            try:
                if Path(process.exe()).resolve() == launcher.STAGE3_PORTABLE_TERMINAL.resolve():
                    return None
            except (psutil.Error, OSError):
                return None

        archive_dir = state_path.parent / "diagnostics" / "stage3_launch_contract_retry" / cycle
        archive_dir.mkdir(parents=True, exist_ok=True)
        archived = {}
        for label, source in (("failure_event", failure_path), ("stage3_handoff", handoff_path)):
            target = archive_dir / source.name
            if not target.exists():
                shutil.copy2(source, target)
            if sha256(target) != sha256(source):
                return None
            archived[label] = {"path": str(target.resolve()), "sha256": sha256(target)}
        receipt_path = archive_dir / "repair_receipt.json"
        receipt = {
            "schema_version": 1,
            "record_kind": "stage3_stage2_grid_consumer_repair",
            "cycle_id": cycle,
            "ea": state.get("current_ea"),
            "stage": 3,
            "repair_complete": True,
            "repair_kind": "launcher_uses_hash_bound_stage2_tester_config",
            "failure_event": str(failure_path.resolve()),
            "failure_event_sha256": sha256(failure_path),
            "stage2_event_sha256": sha256(Path(state["last_stage2_event"])),
            "stage2_verification_sha256": sha256(Path(state["last_stage2_verification"])),
            "stage2_handoff_sha256": sha256(launcher.preparer.STAGE2_HANDOFF),
            "stage3_handoff_sha256": sha256(handoff_path),
            "approved_tester_config_sha256": handoff["approved_tester_config_sha256"],
            "stage3_launch_code_sha256": sha256(Path(launcher.__file__)),
            "stage3_prepare_code_sha256": sha256(Path(launcher.preparer.__file__)),
            "m5_ini_sha256": handoff.get("ini_sha256", {}).get("M5"),
            "m5_launch_profile_validated": True,
            "archived_evidence": archived,
            "recorded_utc": utc_now(),
        }
        if receipt_path.exists():
            existing = load_object(receipt_path)
            if any(existing.get(key) != receipt.get(key) for key in (
                    "record_kind", "cycle_id", "failure_event_sha256",
                    "approved_tester_config_sha256", "stage3_launch_code_sha256",
                    "m5_ini_sha256")):
                return None
            receipt = existing
        else:
            atomic_json(receipt_path, receipt)
        valid, _ = validate_stage2_completion(state, control, db_path)
        if not valid:
            return None
        state.update({
            "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_ready",
            "hold_after_stage": 5,
            "next_action": "run_stage3",
            "stage3_launch_contract_retry_count": 1,
            "stage3_launch_contract_retry": {
                "status": "authorized",
                "cycle_id": cycle,
                "event_id": f"{cycle}:stage3:launcher-grid-consumer-retry:1",
                "repair_receipt": str(receipt_path.resolve()),
                "repair_receipt_sha256": sha256(receipt_path),
                "failure_event_sha256": sha256(failure_path),
                "authorized_by": "user_confirmed",
                "authorized_utc": utc_now(),
            },
            "updated_utc": utc_now(),
        })
        atomic_json(state_path, state)
        return {"event_id": f"{cycle}:stage3:launcher-grid-consumer-retry:1",
                "repair_receipt": str(receipt_path.resolve()),
                "repair_receipt_sha256": sha256(receipt_path)}
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        return None


def authenticated_stage2_release_for_retry(state: dict) -> bool:
    """Require the exact current-cycle one-run Stage 2 release to be consumed."""
    try:
        import kcos_stage3_driver_v2 as driver
        return driver.authenticated_consumed_stage2_release(state) is not None
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        return False


def run(state_path: Path = STATE, control_path: Path = CONTROL, heartbeat_path: Path = HEARTBEAT, db_path: Path = DB,
        claims_dir: Path | None = None, dispatch_runner=None, shutdown_runner=None,
        cycle_cap_path: Path = GOAL_CYCLE_CAP) -> dict:
    try:
        control = load_object(control_path)
        cap_path = campaign_cycle_cap_path(control, cycle_cap_path)
        reconcile_stage1_provider_quota_failure(state_path, control, db_path)
        reconcile_duplicate_stage1_candidate(state_path, control, db_path)
        reconcile_completed_stage1_without_artifact(state_path, control, db_path)
        expire_stale_stage1(state_path, control, db_path)
        reconcile_invalid_stage1_completion(state_path, control, db_path)
        reconcile_stage4_dispatch_race(state_path)
        clear_stale_stage2_retry_metadata(state_path)
        state = load_object(state_path)
        cleanup_failed_stage3_runtime_owner(state, state_path.parent)
        state = load_object(state_path)
        prepare_stage3_journal_reclassification_retry(state_path, control, db_path)
        state = load_object(state_path)
        prepare_stage3_launch_contract_retry(state_path, control, db_path)
        state = load_object(state_path)
        # A source repair request is only created after current-cycle failure,
        # runtime, Stage 2 lineage, and source-gate checks. A failed isolated
        # terminal may be released above only after its workers have exited.
        stage2_failure_path = Path(str(state.get("stage2_failure_event")
                                       or state.get("stage2_invalid_event") or ""))
        stage2_failure_is_current = False
        if stage2_failure_path.is_file():
            try:
                stage2_failure = load_object(stage2_failure_path)
                stage2_failure_is_current = (
                    stage2_failure.get("cycle_id") == state.get("cycle_id")
                    and stage2_failure.get("stage") == 2)
            except (OSError, ValueError, TypeError):
                stage2_failure_is_current = True
        stage2_hold_needs_stage3_recovery = bool(
            state.get("current_stage") == "stage2_pending"
            and state.get("stage_status") == "stage2_repair_required"
            and not stage2_failure_is_current
            and not state.get("stage2_source_defect_ticket"))
        if stage2_hold_needs_stage3_recovery:
            try:
                # A verified zero-trade result is a candidate-local Stage 2
                # repair trigger, even if an older duplicate-start failure left
                # an infrastructure marker in canonical state.
                stage2_hold_needs_stage3_recovery = (
                    authenticated_stage3_zero_trade_repair(state) is None)
            except (OSError, ValueError, TypeError, KeyError):
                pass
        stage3_failure_recovery_state = state
        if stage2_hold_needs_stage3_recovery:
            # Some prior supervisor transitions moved the state to the Stage 2
            # repair hold before a hash-bound Stage 3 source-defect ticket was
            # created. Re-enter the same authenticated classifier using a copy;
            # do not rewrite canonical state until it returns a complete ticket.
            stage3_failure_recovery_state = dict(state)
            stage3_failure_recovery_state["stage_status"] = "stage3_infrastructure_failure"
        if ((state.get("stage_status") in {"stage3_infrastructure_failure", "stage3_failed_held"}
             or stage2_hold_needs_stage3_recovery)
                and state.get("stage3_failure_class") == "infrastructure"
                and authorizes_stages_1_to_5(state)):
            try:
                live_runtime = rotation.active_stage3_runtime(state, state_path.parent)
                def validate_prior_stage2_attestation(current_state, event_path, verification_path,
                                                      event, attestation_path):
                    stage2_job = str((control.get("stage_jobs") or {}).get("2") or "")
                    if not stage2_job:
                        return False, "Stage 2 Hermes job identity is unavailable"
                    return validate_stage2_execution_attestation(
                        attestation_path, current_state, stage2_job, event_path,
                        verification_path, event, db_path)
                recovery = stage2_source_recovery.classify_and_prepare(
                    stage3_failure_recovery_state, state_dir=state_path.parent,
                    authorize_full_cycle=authorizes_stages_1_to_5,
                    stage3_owner_live=bool(live_runtime),
                    dcc_gate=dcc_closed_return_arrays_are_bounded,
                    stage2_attestation_validator=validate_prior_stage2_attestation,
                )
            except (OSError, ValueError, KeyError, TypeError) as exc:
                recovery = None
                admission_lock = state_path.parent / "recovery" / f"{state.get('cycle_id')}.stage2.source_defect.admission.lock"
                if stage2_hold_needs_stage3_recovery and not state.get("stage2_source_defect_ticket"):
                    state["stage2_source_defect_recovery_hold"] = str(exc)
                    atomic_json(state_path, state)
                if admission_lock.exists() and not state.get("stage2_source_defect_ticket"):
                    state["stage2_source_defect_retry_terminal_hold"] = (
                        f"recovery admission lock exists without canonical ticket/event reconciliation: {exc}")
                    atomic_json(state_path, state)
            if recovery:
                state.update({
                    "current_stage": "stage2_pending",
                    "stage_status": "stage2_repair_required",
                    "next_action": "dispatch_stage2",
                    "hold_after_stage": 2,
                    "stage2_source_defect_ticket": recovery["ticket"],
                    "stage2_source_defect_ticket_sha256": recovery["ticket_sha256"],
                    "stage2_source_defect_event": recovery["event"],
                    "stage2_source_defect_event_id": recovery["event_id"],
                    "stage2_source_defect_event_sha256": recovery["event_sha256"],
                    "stage2_repair_reason": (
                        "authenticated Stage 3 Invalid stops order rejections"
                        if recovery.get("repair_kind") == "stage3_tester_journal_invalid_stops"
                        else "authenticated Stage 3 quantile interpolation array-bounds failure"
                        if recovery.get("repair_kind") == "stage3_tester_journal_quantile_interpolation_bounds"
                        else "authenticated Stage 3 multidimensional array row-bounds failure"
                        if recovery.get("repair_kind") == "stage3_tester_journal_multidimensional_array_row_bounds"
                        else "authenticated Stage 3 EA runtime array-bounds failure"
                        if recovery.get("repair_kind") == "stage3_tester_journal_ea_runtime_array_bounds"
                        else "authenticated Stage 3 502/array-bounds failures match the current DCC source"),
                    "stage2_repair_source": recovery.get("repair_kind", "stage3_tester_journal_dcc_bounds"),
                    "updated_utc": utc_now(),
                })
                atomic_json(state_path, state)
        retry_outcome = prepare_stage2_recovery_retry(state_path, control, db_path)
        if retry_outcome in {"retry_prepared", "exhausted", "active", "terminal_hold", "fresh_completion"}:
            state = load_object(state_path)
        verifier_retry_outcome = prepare_stage2_verifier_retry(state_path, control, db_path)
        if verifier_retry_outcome == "retry_prepared":
            state = load_object(state_path)
        result = evaluate(state, control, db_path)
        if cap_path is not None:
            cap = cycle_cap.load_cap(cap_path)
            if len(cap.get("healthy_receipts", [])) >= cap["target_cycles"]:
                shutdown = process_goal_shutdown(control, cap_path, db_path, shutdown_runner,
                                                state=state, state_dir=state_path.parent)
                result = {**result, "status": "GOAL_HEALTH_TARGET_REACHED",
                          "action": "none", "detail": "five verified Stage 1–5 terminal cycles recorded",
                          "healthy_run_count": len(cap["healthy_receipts"]),
                          "shutdown": shutdown}
            elif (result.get("status") == "ROTATION_READY_STAGE1"
                  and result.get("action") == "rotate_stage1"
                  and result.get("event_path")
                  and Path(str(result["event_path"])).resolve()
                      == Path(state.get("last_stage5_event") or "").resolve()):
                stage5_decision = load_object(Path(state.get("last_stage5_decision") or ""))
                if stage5_decision.get("verdict") == "STAGE5_FORWARD_FAIL":
                    # A validated strategy failure is a rotation outcome, not a healthy receipt.
                    # Do not let an unrelated campaign-health audit veto the requested rotation.
                    result.update({"strategy_verdict": "STAGE5_FORWARD_FAIL",
                                   "healthy_run_receipt": "not_recorded_strategy_failure",
                                   "detail": f"{result.get('detail', '')}; verified forward failure is rotating without a healthy receipt"})
                else:
                    valid, audit_detail, receipt = audit_healthy_stage5_cycle(
                        state, control, db_path, cap_path, state_path.parent)
                    if not valid or receipt is None:
                        result = {**result, "status": "HEALTHY_RECEIPT_AUDIT_FAILED",
                                  "action": "none", "detail": audit_detail}
                    else:
                        created = cycle_cap.record_healthy_receipt(cap_path, receipt)
                        updated_cap = cycle_cap.load_cap(cap_path)
                        result.update({"healthy_run_receipt": "created" if created else "already_recorded",
                                       "healthy_run_count": len(updated_cap.get("healthy_receipts", [])),
                                       "strategy_verdict": receipt["strategy_verdict"],
                                       "result_verdict": receipt.get("result_verdict")})
                        if len(updated_cap.get("healthy_receipts", [])) >= updated_cap["target_cycles"]:
                            result["shutdown"] = process_goal_shutdown(control, cap_path, db_path, shutdown_runner,
                                                                      state=state, state_dir=state_path.parent)
                            result.update({"status": "GOAL_HEALTH_TARGET_REACHED", "action": "none",
                                           "detail": "fifth verified Stage 1–5 terminal recorded; shutdown is pending until stage executions end"})
        if (cap_path is not None
                and not cycle_cap.allow_action(cap_path, str(result.get("action") or "none"),
                                               str(result.get("status") or ""), str(state.get("cycle_id") or ""))):
            result = {**result, "status": "GOAL_CYCLE_CAP_REACHED", "action": "none",
                      "detail": "five healthy Stage 1–5 terminal cycles are recorded; refusing another Stage 1 cycle"}
        continuation = result.get("status") == "DISPATCH_READY_STAGE3_CONTINUATION"
        dispatch_allowed = control.get("continuation_dispatch_enabled") is True if continuation else control.get("dispatch_enabled") is True
        if (result.get("action", "none").startswith("dispatch_") or result.get("action") in {"start_stage3", "continue_stage3", "rotate_stage1"}) and dispatch_allowed:
            claim_dir = claims_dir or heartbeat_path.parent / "claims"
            content_identity = result.get("stage2_repair_identity") or result.get("dispatch_identity")
            if result.get("action") == "dispatch_stage2":
                try:
                    content_identity = stage2_dispatch_content_identity(result, state)
                except (OSError, ValueError, TypeError, KeyError) as exc:
                    result.update({"status": "STAGE2_RECOVERY_HOLD", "action": "none",
                                   "detail": f"Stage 2 dispatch identity could not be content-bound: {exc}"})
                    content_identity = None
            elif result.get("action") == "continue_stage3":
                try:
                    content_identity = stage3_continuation_content_identity(result, state)
                except (OSError, ValueError, TypeError, KeyError) as exc:
                    result.update({"status": "STAGE3_CONTINUATION_HOLD", "action": "none",
                                   "detail": f"Stage 3 continuation identity could not be content-bound: {exc}"})
                    content_identity = None
            if result.get("action") == "none":
                atomic_json(heartbeat_path, result)
                return result
            existing_claim = dispatch_claim_path(claim_dir, result["event_id"], result["action"], content_identity)
            reclaim_failed_dispatch_claim(existing_claim, result["action"], control, db_path)
            claimed, claim_path = claim_action(claim_dir, result["event_id"], result["action"], content_identity)
            result["claim"] = "acquired" if claimed else "already_claimed"
            result["claim_path"] = str(claim_path.resolve())
            if claimed:
                if result.get("action") == "dispatch_stage2":
                    # Persist the immutable claim identity before Hermes can
                    # start Stage 2. Replayed delivery sees the same claimed_utc
                    # and content identity; a later reclaimed dispatch has a
                    # newly-created claim and therefore a distinct occurrence.
                    persist_stage2_dispatch_claim(state_path, claim_path, result["event_id"])
                if result.get("status") == "STAGE2_STALE_REQUALIFICATION_RETRY_READY":
                    live_state = load_object(state_path)
                    failure_path = Path(live_state.get("stage2_failure_event") or "")
                    failure = load_object(failure_path) if failure_path.is_file() else {}
                    retry_identity = stale_stage2_requalification_retry_identity(
                        live_state, failure, failure_path) if failure_path.is_file() else None
                    retry_cycle = live_state.get("stage2_stale_requalification_retry_cycle_id")
                    retry_count = (live_state.get("stage2_stale_requalification_retry_count", 0)
                                   if retry_cycle == live_state.get("cycle_id") else 0)
                    if (retry_identity is None or retry_count != 0
                            or retry_identity.get("identity_sha256") != (result.get("dispatch_identity") or {}).get("identity_sha256")):
                        raise ValueError("stale requalification retry evidence changed before Stage 2 dispatch")
                    live_state.update({
                        "stage2_stale_requalification_retry_cycle_id": live_state.get("cycle_id"),
                        "stage2_stale_requalification_retry_count": 1,
                        "stage2_stale_requalification_retry_event_id": result["event_id"],
                        "stage2_stale_requalification_retry_failure_sha256": retry_identity["failure_event_sha256"],
                        "stage2_stale_requalification_retry_dispatch_claim": str(claim_path.resolve()),
                        "stage2_stale_requalification_retry_dispatched_utc": utc_now(),
                    })
                    atomic_json(state_path, live_state)
                if result.get("status") == "STAGE2_REQUALIFICATION_READY":
                    live_state = load_object(state_path)
                    requalification = authenticated_stage2_requalification(live_state, control)
                    if (live_state.get("current_stage") != "stage3_pending"
                            or requalification is None
                            or requalification.get("event_id") != result.get("event_id")
                            or requalification.get("requalification_identity") != result.get("requalification_identity")):
                        raise ValueError("Stage 2 requalification evidence changed before Hermes dispatch")
                    live_state.update({
                        "current_stage": "stage2_pending",
                        "stage_status": "stage2_repair_required",
                        "next_action": "dispatch_stage2",
                        "stage2_requalification_count": 1,
                        "stage2_requalification": {
                            "schema_version": 1, "cycle_id": live_state.get("cycle_id"),
                            "event_id": result["event_id"], "status": "authorized",
                            "identity": result["requalification_identity"],
                            "dispatch_claim": str(claim_path.resolve()),
                            "dispatch_claim_content_identity_sha256": json.loads(claim_path.read_text(encoding="utf-8")).get("content_identity_sha256"),
                            "authorized_utc": utc_now(),
                        },
                        "updated_utc": utc_now(),
                    })
                    atomic_json(state_path, live_state)
                if result.get("status") == "STAGE2_POLICY_REQUALIFICATION_READY":
                    live_state = load_object(state_path)
                    requalification = authenticated_stage2_policy_requalification(
                        live_state, control, state_path.parent)
                    if (requalification is None
                            or requalification.get("event_id") != result.get("event_id")
                            or requalification.get("policy_requalification_identity")
                                != result.get("policy_requalification_identity")):
                        raise ValueError("Stage 2 policy requalification evidence changed before Hermes dispatch")
                    archive = archive_stage2_policy_requalification(
                        live_state, state_path.parent, requalification["request_id"])
                    retirement = rotation.retire_stage3_runtime(live_state, state_path.parent)
                    live_runtime = rotation.active_stage3_runtime(live_state, state_path.parent)
                    if retirement.get("unverified") or live_runtime is not None:
                        raise ValueError(
                            "completed Stage 3 runtime could not be safely released for Stage 2: "
                            f"retirement={retirement}, active={live_runtime}")
                    requalification_history = list(
                        live_state.get("stage2_policy_requalification_history") or [])
                    prior_requalification = live_state.get("stage2_policy_requalification")
                    if isinstance(prior_requalification, dict):
                        requalification_history.append(prior_requalification)
                    next_requalification_count = int(
                        live_state.get("stage2_policy_requalification_count", 0)) + 1
                    live_state.update({
                        "current_stage": "stage2_pending",
                        "stage_status": "stage2_policy_requalification_pending",
                        "next_action": "dispatch_stage2",
                        "stage2_policy_requalification_count": next_requalification_count,
                        "stage2_policy_requalification_history": requalification_history,
                        "stage2_policy_requalification": {
                            "schema_version": 1, "cycle_id": live_state.get("cycle_id"),
                            "event_id": result["event_id"], "status": "authorized",
                            "request_path": requalification["request_path"],
                            "request_sha256": requalification["request_sha256"],
                            "identity": requalification["policy_requalification_identity"],
                            "archive": archive, "runtime_retirement": retirement,
                            "dispatch_claim": str(claim_path.resolve()),
                            "dispatch_claim_content_identity_sha256": json.loads(
                                claim_path.read_text(encoding="utf-8")).get("content_identity_sha256"),
                            "authorized_utc": utc_now(),
                        },
                        "updated_utc": utc_now(),
                    })
                    atomic_json(state_path, live_state)
                if result.get("status") == "STAGE3_LAUNCH_LINEAGE_RETRY_READY":
                    retry_state = load_object(state_path)
                    identity = stage3_launch_lineage_retry_identity(retry_state)
                    if (identity is None or identity != result.get("dispatch_identity")
                            or retry_state.get("stage3_launch_lineage_retry_count", 0) != 0):
                        raise ValueError("Stage 3 launch-lineage repair evidence changed before retry dispatch")
                    retry_state.update({
                        "current_stage": "stage3_pending", "stage_status": "stage2_complete_held",
                        "next_action": "run_stage3", "hold_after_stage": 2,
                        "stage3_launch_lineage_retry_count": 1,
                        "stage3_launch_lineage_retry_event_id": result["event_id"],
                        "stage3_launch_lineage_retry_identity": identity,
                        "stage3_launch_lineage_retry_started_utc": utc_now(),
                        "updated_utc": utc_now(),
                    })
                    atomic_json(state_path, retry_state)
                if result.get("status") == "STAGE3_INPUT_CONTRACT_RETRY_READY":
                    retry_state = load_object(state_path)
                    repair_valid, repair_detail, repair_failure = validate_stage3_input_contract_repair(retry_state)
                    if (not repair_valid or repair_failure.get("event_id") != result.get("event_id")
                            or repair_failure.get("consumed") is not False
                            or Path(retry_state.get("stage3_failure_event") or "").resolve()
                                != Path(result.get("event_path") or "").resolve()):
                        raise ValueError("Stage 3 input-contract repair evidence changed before Hermes dispatch")
                    retry_state.update({"current_stage": "stage3_pending",
                                        "stage_status": "stage2_complete_held",
                                        "next_action": "run_stage3",
                                        "stage3_input_contract_retry_count": 1,
                                        "stage3_input_contract_retry_event_id": result["event_id"],
                                        "stage3_input_contract_retry_started_utc": utc_now(),
                                        "updated_utc": utc_now()})
                    atomic_json(state_path, retry_state)
                if result.get("status") == "STAGE3_INPUT_CONTRACT_RETRY_RELEASE_READY":
                    retry_state = load_object(state_path)
                    repair_valid, repair_detail, _ = validate_stage3_input_contract_repair(retry_state)
                    retry_count = retry_state.get("stage3_input_contract_retry_release_dispatch_count", 0)
                    if (not repair_valid or not isinstance(retry_count, int) or retry_count < 0 or retry_count >= 4
                            or retry_state.get("next_action") not in {"audit_stage2_before_stage3", "run_stage3"}
                            or retry_state.get("hold_after_stage") != 2):
                        raise ValueError("Stage 3 retry release evidence changed before Hermes dispatch")
                    retry_state["stage3_input_contract_retry_release_dispatch_count"] = retry_count + 1
                    retry_state["stage3_input_contract_retry_release_event_id"] = result["event_id"]
                    retry_state["stage3_input_contract_retry_release_dispatched_utc"] = utc_now()
                    atomic_json(state_path, retry_state)
                if result.get("status") == "STAGE2_VERIFIER_RETRY_READY":
                    retry_event_path = Path(result["event_path"])
                    before_consume = load_object(state_path)
                    valid, detail, retry_event, _ = validate_stage2_verifier_retry(before_consume)
                    if (not valid or retry_event_path.resolve() != Path(before_consume["stage2_verifier_retry_event"]).resolve()
                            or retry_event.get("consumed") is not False
                            or retry_event.get("event_id") != result["event_id"]):
                        raise ValueError("canonical verifier-retry event changed before Hermes dispatch")
                    intent_state = load_object(state_path)
                    intent_state["stage2_verifier_retry_consumption_intent"] = {
                        "event_id": result["event_id"], "started_utc": utc_now(),
                        "unconsumed_event_sha256": sha256(retry_event_path)}
                    atomic_json(state_path, intent_state)
                    consume_event(retry_event_path, result["event_id"], result["action"], sha256(retry_event_path))
                    consumed = load_object(retry_event_path)
                    live_state = load_object(state_path)
                    live_state["stage2_verifier_retry_consumed_receipt"] = {
                        "event_id": result["event_id"], "event_sha256": sha256(retry_event_path),
                        "consumed_utc": consumed.get("consumed_utc")}
                    live_state["stage2_verifier_retry_dispatch_claim"] = str(claim_path.resolve())
                    live_state["stage2_verifier_retry_dispatch_claim_sha256"] = sha256(claim_path)
                    atomic_json(state_path, live_state)
                if result.get("status") == "STAGE2_SOURCE_DEFECT_REBUILD_READY":
                    # Commit the one-shot event before the async Hermes process
                    # can read canonical state and begin the rebuild.
                    recovery_event_path = Path(result["event_path"])
                    before_consume = load_object(state_path)
                    if (before_consume.get("stage2_source_defect_event") != str(recovery_event_path.resolve())
                            or before_consume.get("stage2_source_defect_event_sha256") != sha256(recovery_event_path)):
                        raise ValueError("canonical recovery event digest changed before Hermes dispatch")
                    capture_stage2_recovery_execution_baseline(
                        state_path, control, db_path, result["event_id"])
                    intent_started = utc_now()
                    intent_state = load_object(state_path)
                    intent_state["stage2_source_defect_consumption_intent"] = {
                        "event_id": result["event_id"], "started_utc": intent_started,
                        "unconsumed_event_sha256": before_consume["stage2_source_defect_event_sha256"]}
                    atomic_json(state_path, intent_state)
                    consume_event(recovery_event_path, result["event_id"], result["action"],
                                  before_consume["stage2_source_defect_event_sha256"])
                    live_state = load_object(state_path)
                    live_state["stage2_source_defect_event_sha256"] = sha256(recovery_event_path)
                    consumed_event = load_object(recovery_event_path)
                    live_state["stage2_source_defect_consumed_receipt"] = {
                        "event_id": result["event_id"],
                        "event_sha256": live_state["stage2_source_defect_event_sha256"],
                        "consumed_utc": consumed_event.get("consumed_utc")}
                    live_state["stage2_source_defect_dispatch_claim"] = str(claim_path.resolve())
                    live_state["stage2_source_defect_dispatch_claim_sha256"] = sha256(claim_path)
                    atomic_json(state_path, live_state)
                if result.get("action") == "dispatch_stage4":
                    # Stage 4 reads canonical state as soon as Hermes starts
                    # the cron. Commit its eligibility before triggering the
                    # process so it cannot observe the prior held Stage 3
                    # state and fail its own precondition.
                    live_state = load_object(state_path)
                    if (live_state.get("cycle_id") != result.get("cycle_id")
                            or live_state.get("stage_status") not in {"stage3_complete_held", "stage4_pending"}):
                        raise ValueError("canonical state changed before Stage 4 admission")
                    live_state.update({
                        "current_stage": "stage4_pending",
                        "stage_status": "stage4_pending",
                        "next_action": "run_stage4",
                        "hold_after_stage": None,
                        "updated_utc": utc_now(),
                    })
                    atomic_json(state_path, live_state)
                if result["action"] == "dispatch_stage1" and state.get("current_stage") == "stage1_pending":
                    # Normalize manually restarted fresh cycles before the
                    # Stage 1 publisher runs: pending -> running and refresh
                    # only the handoff's canonical-state attestation.
                    live_state = load_object(state_path)
                    live_state.update({
                        "current_stage": "stage1_running",
                        "stage_status": "stage1_research_prepared",
                        "next_action": "dispatch_stage1",
                        "hold_after_stage": None,
                        "updated_utc": utc_now(),
                        "last_stage1_artifact": None,
                        "last_stage1_event": None,
                        "last_stage2_event": None,
                        "last_stage2_verification": None,
                    })
                    atomic_json(state_path, live_state)
                    handoff_path = Path(control.get("stage1_handoff") or ROOT / "state" / "handoffs" / "Stage1_v2.json")
                    handoff = load_object(handoff_path)
                    for protected in handoff.get("protected_files", []):
                        if Path(protected.get("path", "")).resolve() == state_path.resolve():
                            protected["sha256"] = sha256(state_path)
                    atomic_json(handoff_path, handoff)
                if result["action"] == "rotate_stage1":
                    if result["status"] == "ROTATION_READY_STAGE1":
                        try:
                            prepared = rotation.prepare_next_cycle(
                                state_path,
                                Path(control.get("stage1_handoff") or ROOT / "state" / "handoffs" / "Stage1_v2.json"),
                                Path(control.get("rotation_ledger") or rotation.LEDGER),
                                Path(control.get("rotation_queue") or rotation.ROTATION_QUEUE),
                                Path(control.get("stage4_queue") or rotation.STAGE4_QUEUE),
                                Path(control.get("ea_base") or rotation.EA_BASE),
                            )
                        except Exception:
                            # Claims are transactional: a failed rotation must be retryable.
                            try:
                                claim_path.unlink(missing_ok=True)
                            except Exception:
                                pass
                            raise
                        result["rotation_cycle_id"] = prepared["cycle_id"]
                        result["rotation_ea"] = prepared["ea"]
                        if (cap_path is not None
                                and not cycle_cap.reserve_start(cap_path, str(prepared["cycle_id"]))):
                            claim_path.unlink(missing_ok=True)
                            result.update({"claim": "released_after_cycle_cap", "action": "none",
                                           "status": "GOAL_CYCLE_CAP_REACHED",
                                           "detail": "five goal cycles are already committed; refusing a sixth Stage 1 dispatch"})
                            dispatched, detail, execution_id = False, result["detail"], None
                        else:
                            dispatched, detail, execution_id = dispatch_job(control, "dispatch_stage1", dispatch_runner, db_path)
                    else:
                        prepared_cycle_id = str(state.get("cycle_id") or "")
                        if cap_path is not None and not cycle_cap.reserve_start(cap_path, prepared_cycle_id):
                            claim_path.unlink(missing_ok=True)
                            result.update({"claim": "released_after_cycle_cap", "action": "none",
                                           "status": "GOAL_CYCLE_CAP_REACHED",
                                           "detail": "five goal cycles are already committed; refusing another Stage 1 dispatch"})
                            dispatched, detail, execution_id = False, result["detail"], None
                        else:
                            dispatched, detail, execution_id = dispatch_job(control, "dispatch_stage1", dispatch_runner, db_path)
                elif result["action"] == "dispatch_stage1":
                    dispatched, detail, execution_id = dispatch_job(control, "dispatch_stage1", dispatch_runner, db_path)
                elif result["action"] in {"start_stage3", "continue_stage3"}:
                    # Stage 3 is cron-owned like every other stage. Do not
                    # invoke the driver directly: direct execution bypasses
                    # Hermes execution attestation, schedule recovery, and
                    # installed-entrypoint synchronization.
                    dispatched, detail, execution_id = dispatch_job(
                        control, "dispatch_stage3", dispatch_runner, db_path
                    )
                else:
                    dispatched, detail, execution_id = dispatch_job(
                        control, result["action"], dispatch_runner, db_path
                    )
                result["dispatch_accepted"] = dispatched
                result["dispatch_detail"] = detail
                # The source-defect Stage 2 process validates this claim as
                # soon as Hermes starts it. Keep that content-addressed claim
                # immutable; its acceptance and execution ID are recorded in
                # canonical state and the Hermes ledger below.
                if result.get("status") != "STAGE2_SOURCE_DEFECT_REBUILD_READY":
                    record_dispatch_claim_outcome(
                        claim_path, accepted=bool(dispatched), execution_id=execution_id,
                        detail=str(detail or ""), db_path=db_path)
                if result.get("status") == "STAGE2_COMPILE_SOURCE_REPAIR_READY" and dispatched:
                    repair_state = load_object(state_path)
                    repair_identity = result.get("dispatch_identity") or {}
                    failure_path = Path(result.get("event_path") or "")
                    event = load_object(failure_path) if failure_path.is_file() else {}
                    compile_path = Path(event.get("compile_attempt") or "")
                    source_path = Path(repair_state.get("candidate_source")
                                       or repair_state.get("stage2_candidate_source") or "")
                    spec_path = Path(repair_state.get("last_stage1_artifact") or "")
                    if (repair_state.get("cycle_id") == result.get("cycle_id")
                            and failure_path.is_file() and compile_path.is_file()
                            and source_path.is_file() and spec_path.is_file()
                            and sha256(failure_path) == repair_identity.get("failure_event_sha256")
                            and sha256(compile_path) == repair_identity.get("compile_attempt_sha256")
                            and sha256(source_path) == repair_identity.get("source_sha256")
                            and sha256(spec_path) == repair_identity.get("stage1_spec_sha256")
                            and not stage2_compile_source_repair_used(
                                repair_state, str(result.get("cycle_id") or ""))):
                        repair_cycle = str(result.get("cycle_id") or "")
                        if (repair_state.get("stage2_compile_source_repair_followup_cycle_id") != repair_cycle
                                or repair_state.get("stage2_compile_source_repair_second_followup_cycle_id") != repair_cycle):
                            history = repair_state.get("stage2_compile_source_repair_history")
                            if not isinstance(history, list):
                                history = []
                            prior_cycle = repair_state.get("stage2_compile_source_repair_cycle_id")
                            if prior_cycle and prior_cycle != repair_cycle:
                                history.append({
                                    "cycle_id": prior_cycle,
                                    "repair_identity": repair_state.get("stage2_compile_source_repair_identity"),
                                    "repair_execution_id": repair_state.get("stage2_compile_source_repair_execution_id"),
                                    "followup_count": repair_state.get("stage2_compile_source_repair_followup_count"),
                                    "followup_execution_id": repair_state.get("stage2_compile_source_repair_followup_execution_id"),
                                    "recorded_before_cycle_reset_utc": utc_now(),
                                })
                            repair_state["stage2_compile_source_repair_history"] = history
                            repair_state["stage2_compile_source_repair_followup_count"] = 0
                            repair_state["stage2_compile_source_repair_followup_cycle_id"] = repair_cycle
                            repair_state["stage2_compile_source_repair_second_followup_count"] = 0
                            repair_state["stage2_compile_source_repair_second_followup_cycle_id"] = repair_cycle
                        repair_state.update({
                            "stage2_compile_source_repair_cycle_id": result.get("cycle_id"),
                            "stage2_compile_source_repair_count": 1,
                            "stage2_compile_source_repair_identity": repair_identity,
                            "stage2_compile_source_repair_event_id": result.get("event_id"),
                            "stage2_compile_source_repair_execution_id": execution_id,
                            "stage2_compile_source_repair_dispatched_utc": utc_now(),
                        })
                        atomic_json(state_path, repair_state)
                if result.get("status") == "STAGE2_COMPILE_SOURCE_REPAIR_FOLLOWUP_READY" and dispatched:
                    followup_state = load_object(state_path)
                    followup_identity = result.get("dispatch_identity") or {}
                    if (followup_state.get("cycle_id") == result.get("cycle_id")
                            and stage2_compile_source_repair_followup_count(
                                followup_state, str(result.get("cycle_id") or "")) == 0
                            and followup_state.get("stage2_compile_source_repair_cycle_id") == result.get("cycle_id")
                            and followup_state.get("stage2_compile_source_repair_identity") is not None):
                        followup_state.update({
                            "stage2_compile_source_repair_followup_count": 1,
                            "stage2_compile_source_repair_followup_cycle_id": result.get("cycle_id"),
                            "stage2_compile_source_repair_followup_identity": followup_identity,
                            "stage2_compile_source_repair_followup_event_id": result.get("event_id"),
                            "stage2_compile_source_repair_followup_execution_id": execution_id,
                            "stage2_compile_source_repair_followup_dispatched_utc": utc_now(),
                        })
                        atomic_json(state_path, followup_state)
                if result.get("status") == "STAGE2_COMPILE_SOURCE_REPAIR_SECOND_FOLLOWUP_READY" and dispatched:
                    second_followup_state = load_object(state_path)
                    second_identity = result.get("dispatch_identity") or {}
                    if (second_followup_state.get("cycle_id") == result.get("cycle_id")
                            and stage2_compile_source_repair_second_followup_count(
                                second_followup_state, str(result.get("cycle_id") or "")) == 0
                            and stage2_compile_source_repair_followup_count(
                                second_followup_state, str(result.get("cycle_id") or "")) == 1
                            and second_followup_state.get("stage2_compile_source_repair_cycle_id") == result.get("cycle_id")):
                        second_followup_state.update({
                            "stage2_compile_source_repair_second_followup_count": 1,
                            "stage2_compile_source_repair_second_followup_cycle_id": result.get("cycle_id"),
                            "stage2_compile_source_repair_second_followup_identity": second_identity,
                            "stage2_compile_source_repair_second_followup_event_id": result.get("event_id"),
                            "stage2_compile_source_repair_second_followup_execution_id": execution_id,
                            "stage2_compile_source_repair_second_followup_dispatched_utc": utc_now(),
                        })
                        atomic_json(state_path, second_followup_state)
                if result.get("status") == "STAGE2_STALE_METADATA_RETRY_READY" and dispatched:
                    retry_state = load_object(state_path)
                    if (retry_state.get("cycle_id") == result.get("cycle_id")
                            and int(retry_state.get("stage2_stale_stage3_metadata_retry_count", 0) or 0) == 0):
                        retry_state.update({
                            "stage2_stale_stage3_metadata_retry_count": 1,
                            "stage2_stale_stage3_metadata_retry_event_id": result.get("event_id"),
                            "stage2_stale_stage3_metadata_retry_identity": result.get("dispatch_identity"),
                            "stage2_stale_stage3_metadata_retry_execution_id": execution_id,
                            "stage2_stale_stage3_metadata_retry_dispatched_utc": utc_now(),
                        })
                        atomic_json(state_path, retry_state)
                if result.get("status") == "STAGE2_TESTER_DIAGNOSTIC_REMOVAL_RETRY_READY" and dispatched:
                    retry_state = load_object(state_path)
                    if (retry_state.get("cycle_id") == result.get("cycle_id")
                            and int(retry_state.get("stage2_tester_diagnostic_removal_retry_count", 0) or 0) == 0):
                        retry_state.update({
                            "stage2_tester_diagnostic_removal_retry_count": 1,
                            "stage2_tester_diagnostic_removal_retry_event_id": result.get("event_id"),
                            "stage2_tester_diagnostic_removal_retry_identity": result.get("dispatch_identity"),
                            "stage2_tester_diagnostic_removal_retry_execution_id": execution_id,
                            "stage2_tester_diagnostic_removal_retry_dispatched_utc": utc_now(),
                        })
                        atomic_json(state_path, retry_state)
                if result.get("status") == "STAGE2_VERIFIER_RETRY_READY":
                    retry_state = load_object(state_path)
                    retry_state.update({
                        "stage2_verifier_retry_last_dispatch_event_id": result.get("event_id"),
                        "stage2_verifier_retry_last_dispatch_accepted": bool(dispatched),
                        "stage2_verifier_retry_last_dispatch_detail": str(detail or ""),
                        "stage2_verifier_retry_last_dispatch_execution_id": execution_id,
                        "stage2_verifier_retry_last_dispatch_utc": utc_now(),
                    })
                    atomic_json(state_path, retry_state)
                if result.get("status") == "STAGE2_SOURCE_DEFECT_REBUILD_READY":
                    recovery_state = load_object(state_path)
                    recovery_state.update({
                        "stage2_source_defect_last_dispatch_event_id": result.get("event_id"),
                        "stage2_source_defect_last_dispatch_accepted": bool(dispatched),
                        "stage2_source_defect_last_dispatch_detail": str(detail or ""),
                        "stage2_source_defect_last_dispatch_execution_id": execution_id,
                        "stage2_source_defect_last_dispatch_utc": utc_now(),
                    })
                    atomic_json(state_path, recovery_state)
                if (cap_path is not None and dispatched
                        and result.get("action") in {"rotate_stage1", "dispatch_stage1"}):
                    started_cycle_id = (result.get("rotation_cycle_id") or state.get("cycle_id"))
                    cycle_cap.commit_start(cap_path, str(started_cycle_id or ""),
                                           str(execution_id or ""), str(control.get("stage_jobs", {}).get("1") or ""))
                if execution_id is not None:
                    result["execution_id"] = execution_id
                    stage = {"rotate_stage1": "1", "dispatch_stage1": "1", "dispatch_stage2": "2", "dispatch_stage3": "3", "start_stage3": "3", "continue_stage3": "3", "dispatch_stage4": "4", "dispatch_stage45": "4.5", "dispatch_stage5": "5"}[result["action"]]
                    next_control = dict(control)
                    next_control[f"last_stage{stage}_execution_id"] = execution_id
                    atomic_json(control_path, next_control)
                    control = next_control
                if dispatched:
                    if result.get("status") == "STAGE2_EXECUTOR_PREFLIGHT_RETRY_READY":
                        retry_state = load_object(state_path)
                        retry_identity = result.get("dispatch_identity") or {}
                        retry_failure_path = Path(result.get("event_path") or "")
                        retry_source_path = Path(retry_state.get("candidate_source")
                                                 or retry_state.get("stage2_candidate_source") or "")
                        retry_spec_path = Path(retry_state.get("last_stage1_artifact") or "")
                        retry_repair_module = PROJECT_STATE / "kcos_stage2_repair_loop_v1.py"
                        repair_module_identity_ok = (
                            not retry_identity.get("repair_module_sha256")
                            or (retry_repair_module.is_file()
                                and sha256(retry_repair_module).upper()
                                    == str(retry_identity.get("repair_module_sha256") or "").upper())
                        )
                        if (retry_state.get("cycle_id") == result.get("cycle_id")
                                and retry_failure_path.is_file()
                                and retry_source_path.is_file() and retry_spec_path.is_file()
                                and sha256(retry_failure_path) == retry_identity.get("failure_event_sha256")
                                and sha256(retry_source_path) == retry_identity.get("source_sha256")
                                and sha256(retry_spec_path) == retry_identity.get("stage1_spec_sha256")
                                and repair_module_identity_ok
                                and retry_state.get("stage2_executor_preflight_retry_count", 0) == 0):
                            retry_state.update({
                                "stage2_executor_preflight_retry_count": 1,
                                "stage2_executor_preflight_retry_identity": retry_identity,
                                "stage2_executor_preflight_retry_event_id": result.get("event_id"),
                                "stage2_executor_preflight_retry_execution_id": execution_id,
                                "stage2_executor_preflight_retry_dispatched_utc": utc_now(),
                            })
                            atomic_json(state_path, retry_state)
                    if result.get("status") == "STAGE2_INITIAL_PREFLIGHT_RETRY_READY":
                        retry_state = load_object(state_path)
                        retry_identity = result.get("dispatch_identity") or {}
                        failure_class = retry_identity.get("preflight_failure_class")
                        prior_classes = retry_identity.get("previous_retry_classes") or []
                        if (retry_state.get("cycle_id") == result.get("cycle_id")
                                and failure_class and failure_class not in prior_classes):
                            retry_classes = list(prior_classes) + [failure_class]
                            retry_state.update({
                                "stage2_preflight_retry_cycle_id": result.get("cycle_id"),
                                "stage2_preflight_retry_count": len(retry_classes),
                                "stage2_preflight_retry_failure_classes": retry_classes,
                                "stage2_preflight_retry_identity": result.get("dispatch_identity"),
                                "stage2_preflight_retry_execution_id": execution_id,
                                "stage2_preflight_retry_dispatched_utc": utc_now(),
                            })
                            atomic_json(state_path, retry_state)
                    if result.get("status") == "STAGE2_COMPILE_REPAIR_RETRY_READY":
                        retry_state = load_object(state_path)
                        retry_identity = result.get("dispatch_identity") or {}
                        if (retry_state.get("cycle_id") == result.get("cycle_id")
                                and retry_state.get("stage2_compile_repair_cycle_id") != result.get("cycle_id")
                                and retry_state.get("candidate_ea", retry_state.get("current_ea"))
                                    == retry_identity.get("candidate_ea")
                                and sha256(Path(retry_state.get("candidate_source")
                                                or retry_state.get("stage2_candidate_source")))
                                    == retry_identity.get("candidate_source_sha256")):
                            retry_state.update({
                                "stage2_compile_repair_cycle_id": result.get("cycle_id"),
                                "stage2_compile_repair_attempt_count": 1,
                                "stage2_compile_repair_identity": retry_identity,
                                "stage2_compile_repair_execution_id": execution_id,
                                "stage2_compile_repair_dispatched_utc": utc_now(),
                            })
                            atomic_json(state_path, retry_state)
                    if result.get("status") == "STAGE1_RESEARCH_RETRY_READY":
                        retry_state = load_object(state_path)
                        prior_retry_count = int(retry_state.get("stage1_research_retry_count", 0))
                        retry_state["stage1_research_retry_count"] = prior_retry_count + 1
                        retry_state["stage1_research_retry_last_dispatched_utc"] = utc_now()
                        repair_ticket = stage1_research_retry_repair_ticket(retry_state, prior_retry_count)
                        if repair_ticket:
                            consumed_ticket = dict(repair_ticket)
                            consumed_ticket.update({
                                "status": "consumed",
                                "consumed_utc": retry_state["stage1_research_retry_last_dispatched_utc"],
                                "consumed_event_id": result.get("event_id"),
                                "execution_id": execution_id,
                            })
                            retry_state["stage1_research_retry_repair"] = consumed_ticket
                        atomic_json(state_path, retry_state)
                    if (result.get("action") == "dispatch_stage45"
                            and isinstance(result.get("dispatch_identity"), dict)):
                        # Continuation runs are keyed to the authenticated prior-run artifacts;
                        # the Stage 4 completion event was already consumed by run 1.
                        source_event = None
                    elif result.get("action") == "dispatch_stage4":
                        # The Stage 3 completion event has been durably consumed
                        # and Stage 4 is now admitted. Do not leave canonical
                        # state looking held after the downstream dispatch.
                        live_state = load_object(state_path)
                        # The Hermes runner may finish before dispatch_job
                        # returns. Preserve its verified terminal state and
                        # only apply pending when admission is still current.
                        if (live_state.get("cycle_id") == result.get("cycle_id")
                                and live_state.get("stage_status") == "stage4_pending"):
                            live_state.update({
                                "current_stage": "stage4_pending",
                                "stage_status": "stage4_pending",
                                "next_action": "run_stage4",
                                "hold_after_stage": None,
                                "updated_utc": utc_now(),
                            })
                            atomic_json(state_path, live_state)
                    if result.get("policy_invalidated") is True and result["action"] == "rotate_stage1":
                        # Preserve the exact bytes of the obsolete event so its
                        # hash-bound invalidation remains durable. The fresh
                        # cycle state records one-time consumption instead.
                        next_state = load_object(state_path)
                        next_state["rotation_source_consumed"] = True
                        next_state["rotation_source_consumed_utc"] = utc_now()
                        next_state["rotation_source_consumed_action"] = result["action"]
                        atomic_json(state_path, next_state)
                        source_event = None
                    elif result.get("event_path"):
                        source_event = Path(result["event_path"])
                    elif stage1_dispatch_uses_claim_only(result):
                        # A fresh cycle has no completion event to consume.  The
                        # same applies to a bounded research retry: its retry
                        # claim is the durable once-only admission. Its earlier
                        # Stage 3 rotation event was consumed when the cycle
                        # was prepared, so consuming it again would fail after
                        # the Stage 1 launch had already succeeded.
                        source_event = None
                    else:
                        event_key = {
                            "dispatch_stage2": "last_stage1_event",
                            "dispatch_stage3": "last_stage2_event",
                            "start_stage3": "last_stage2_event",
                            "dispatch_stage4": "last_stage3_event",
                            "dispatch_stage45": "last_stage4_event",
                            "dispatch_stage5": "last_stage45_event",
                            "dispatch_stage1": "last_stage3_event",
                            "rotate_stage1": "rotation_source_event",
                        }[result["action"]]
                        source_event = Path(load_object(state_path).get(event_key) or state.get(event_key, ""))
                    if (source_event is not None
                            and result.get("status") not in {"STAGE2_SOURCE_DEFECT_REBUILD_READY",
                                                              "STAGE2_VERIFIER_RETRY_READY",
                                                              "STAGE3_INPUT_CONTRACT_RETRY_RELEASE_READY",
                                                              "STAGE2_STALE_METADATA_RETRY_READY",
                                                               "STAGE2_TESTER_DIAGNOSTIC_REMOVAL_RETRY_READY",
                                                              "STAGE2_INITIAL_PREFLIGHT_RETRY_READY",
                                                              "STAGE2_COMPILE_REPAIR_RETRY_READY",
                                                              "STAGE2_EXECUTOR_PREFLIGHT_RETRY_READY",
                                                              "STAGE2_COMPILE_SOURCE_REPAIR_READY"}):
                        consume_event(source_event,
                                      result.get("source_event_id", result["event_id"]),
                                      result["action"],
                                      result.get("source_event_sha256"))
                else:
                    result["action"] = "none"
                    if execution_id is None:
                        # Preserve a known registration failure as a durable
                        # hold. Removing it would permit silent duplicate
                        # triggers with no Hermes execution provenance.
                        result.update({"status": "DISPATCH_REGISTRATION_FAILED_HELD",
                                       "claim": "failed_without_execution_id",
                                       "dispatch_claim_hold": {
                                           "status": "DISPATCH_REGISTRATION_FAILED_HELD",
                                           "detail": "Hermes dispatch was rejected before an execution ID was registered; claim retained for diagnosis"},
                                       "detail": "Hermes dispatch was rejected before an execution ID was registered; claim retained for diagnosis"})
                    else:
                        claim_path.unlink(missing_ok=True)
                        result["claim"] = "released_after_dispatch_failure"
            else:
                existing_record = load_object(claim_path)
                expected_digest = (hashlib.sha256(json.dumps(
                    content_identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
                ).encode("utf-8")).hexdigest().upper() if content_identity is not None else None)
                if (expected_digest is not None
                        and existing_record.get("content_identity_sha256") != expected_digest):
                    hold = {"status": "DISPATCH_CLAIM_IDENTITY_MISMATCH_HELD",
                            "detail": "existing claim content identity does not match current dispatch evidence"}
                elif existing_record.get("execution_id") is None:
                    hold = {"status": ("DISPATCH_REGISTRATION_FAILED_HELD"
                                       if existing_record.get("dispatch_outcome") == "failed"
                                       else "DISPATCH_REGISTRATION_UNRESOLVED_HELD"),
                            "detail": existing_record.get("dispatch_detail") or
                                     "claim has no Hermes execution ID; registration outcome is unresolved"}
                else:
                    hold = None
                if hold:
                    result.update({"status": hold["status"], "action": "none",
                                   "detail": hold["detail"], "dispatch_claim_hold": hold})
                else:
                    result["action"] = "none"
    except Exception as exc:
        result = {
            "schema_version": 2,
            "cycle_id": None,
            "observed_utc": utc_now(),
            "zero_token": True,
            "current_stage": "unknown",
            "status": "UNKNOWN_STALLED",
            "action": "none",
            "detail": str(exc),
        }
    # Canonical state may have changed during dispatch/rotation. Re-evaluate
    # the committed state so the heartbeat never advertises an already-consumed
    # action (for example rotate_stage1 after the new cycle is prepared).
    try:
        live = load_object(state_path)
        committed = evaluate(live, control, db_path)
        cap_path = campaign_cycle_cap_path(control, cycle_cap_path)
        if (cap_path is not None
                and not cycle_cap.allow_action(cap_path, str(committed.get("action") or "none"),
                                               str(committed.get("status") or ""), str(live.get("cycle_id") or ""))):
            committed = {**committed, "status": "GOAL_CYCLE_CAP_REACHED", "action": "none",
                         "detail": "five verified healthy Stage 5 terminal receipts are recorded; refusing another cycle"}
        prior_result = dict(result)
        preserved = {k: v for k, v in prior_result.items() if k in {"claim", "claim_path", "dispatch_accepted", "dispatch_detail", "execution_id", "dispatch_claim_hold"}}
        result = {**committed, **preserved}
        if prior_result.get("status") == "UNKNOWN_STALLED":
            result.update({"status": "SUPERVISOR_ERROR", "action": "none",
                           "detail": prior_result.get("detail") or "supervisor action failed before it could be committed"})
        if prior_result.get("dispatch_claim_hold"):
            result.update({"status": prior_result["dispatch_claim_hold"]["status"],
                           "action": "none", "detail": prior_result["dispatch_claim_hold"]["detail"]})
        if prior_result.get("claim") == "already_claimed":
            result["action"] = "none"
            result.setdefault("claim", "already_claimed")
        if cap_path is not None:
            result = preserve_goal_health_heartbeat(result, prior_result, cycle_cap.load_cap(cap_path))
        result.update({
            "current_ea": live.get("current_ea"),
            "stage_status": live.get("stage_status"),
            "next_action": live.get("next_action"),
        })
        result["supervisor_status"] = compact_supervisor_status(
            live, result, control, cap_path, db_path)
    except Exception:
        pass
    atomic_json(heartbeat_path, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, default=STATE)
    parser.add_argument("--control", type=Path, default=CONTROL)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT)
    parser.add_argument("--db", type=Path, default=DB)
    parser.add_argument("--show", action="store_true")
    parser.add_argument("--claims-dir", type=Path)
    args = parser.parse_args()
    result = run(args.state, args.control, args.heartbeat, args.db, args.claims_dir)
    if args.show:
        print(json.dumps(result))
    return 0 if result["status"] != "UNKNOWN_STALLED" else 2


if __name__ == "__main__":
    raise SystemExit(main())

