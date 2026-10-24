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

from kcos_tester_config_v2 import config_from_spec, config_sha256
from kcos_runtime_paths_v2 import assert_safe_root
from kcos_run_authorization_v2 import authorizes_stages_1_to_3, authorizes_stages_1_to_5


ROOT = assert_safe_root()
LEASE = ROOT / "state" / "leases" / "stage2_v2.lock.json"


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
    tester_config = config_from_spec(load(stage1_spec))
    tester_config_hash = config_sha256(tester_config)
    if verification.get("approved_tester_config_sha256") != tester_config_hash:
        raise ValueError("Stage 2 verification did not attest the approved tester_config")
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
        "configuration_authority": "stage1_spec.tester_config",
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
    # The operator can approve a fresh Stage 1-3 run up front. Admission is
    # still conditional on every verification and lineage check above.
    # Existing cycles without this exact scoped record retain their audit hold.
    if authorizes_stages_1_to_3(state):
        next_state.update({
            "stage_status": "stage2_complete_ready",
            "next_action": "run_stage3",
            "hold_after_stage": 3,
        })
    elif authorizes_stages_1_to_5(state):
        next_state.update({
            "stage_status": "stage2_complete_ready",
            "next_action": "run_stage3",
            "hold_after_stage": 5,
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
