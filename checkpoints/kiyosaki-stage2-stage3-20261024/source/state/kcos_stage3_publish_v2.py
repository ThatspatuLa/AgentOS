#!/usr/bin/env python3
"""Durably publish a verified Stage 3 decision and held transition."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from kcos_stage3_common_v2 import atomic_bytes, atomic_json, load_object, require, sha256, utc_now
from kcos_stage4_queue_v2 import QUEUE as STAGE4_QUEUE, enqueue_verified, entry_for_ea
from kcos_runtime_paths_v2 import assert_safe_root
from kcos_run_authorization_v2 import authorizes_stages_1_to_5


ROOT = assert_safe_root()
LEASE = ROOT / "state" / "leases" / "stage3_v2.lock.json"
FAILED_NOTES = ROOT / "state" / "reports" / "failed-strategies.md"
SOURCE_NOTES = ROOT / "state" / "reports" / "sources-tracker.md"
PASSING_NOTES = ROOT / "state" / "reports" / "passing-strategies.md"
FRAGMENT_DIR = ROOT / "state" / "obsidian_fragments"
SAFE_CYCLE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,199}\Z")


def rotation_mode(state: dict) -> str:
    """Choose research only from an explicit exhausted-and-authorized state."""
    if state.get("queue_exhausted") is True and state.get("research_allowed") is True:
        return "research_new"
    return "queue_next"


def rotation_action(mode: str) -> str:
    return "research_new_after_explicit_queue_exhaustion" if mode == "research_new" else "rotate_to_next_unverified_ea"


def fragment_path(cycle_id: str, kind: str, fragment_dir: Path = FRAGMENT_DIR) -> Path:
    """Immutable per-cycle Stage 3 fragment path.

    Each cycle writes exactly one fragment per (cycle_id, kind).
    The validated cycle_id is embedded so concurrent cycles cannot collide.
    """
    require(isinstance(cycle_id, str) and SAFE_CYCLE_ID.fullmatch(cycle_id) is not None,
            "cycle_id is not a safe fragment filename component")
    suffix = {"passing": "passing", "failed": "failed", "source": "source"}[kind]
    return fragment_dir / f"{cycle_id}.{suffix}.md"


def note_block(decision: dict, kind: str) -> bytes:
    metrics = []
    for tf, item in decision.get("timeframes", {}).items():
        if item.get("status") == "COMPUTE_SKIPPED":
            values = item.get("metrics", {})
            if values.get("compute_qualification") == "slow_first_pass":
                detail = f"first pass={values.get('first_pass_seconds')}s (over 120s qualification cutoff)"
            elif values.get("compute_qualification") == "censored_no_first_pass":
                detail = f"no completed pass by >={values.get('first_pass_censored_seconds')}s hard limit"
            else:
                detail = f"{item.get('failure_mode')}, elapsed={values.get('elapsed_seconds')}s"
            metrics.append(f"{tf}: skipped for compute ({detail})")
        elif item.get("zero_trades"):
            metrics.append(f"{tf}: zero trades")
        else:
            metrics.append(f"{tf}: CF={item['cf']:.4f}, TPM={item['tpm']:.4f}, profit={item['profit']:.2f}, DD={item['drawdown_percent']:.4f}%, pass={item['passed']}")
    heading = "Stage 3 failed strategy" if kind == "failed" else ("Stage 3 source lesson" if kind == "source" else "Stage 3 passing strategy")
    compute_stop = bool(decision.get("compute_skips"))
    if compute_stop:
        skipped = ", ".join(decision["compute_skips"])
        lesson = (f"Compute budget exceeded on timeframe(s) {skipped}; those timeframes were skipped for compute, "
                  "not classified as infrastructure failures. " + decision["reason"])
    else:
        lesson = "All tested timeframes failed; use the gate failures below when mining or revising the mechanism." if kind == "source" else decision["reason"]
    return (f"\n\n## {heading} - {decision['cycle_id']}\n"
            f"- EA: `{decision['ea']}`\n- Verdict: `{decision['verdict']}`\n- Window: {decision['from_date']} to {decision['to_date']}\n"
            f"- Selected TF: `{decision.get('selected_tf')}`\n- Finding: {lesson}\n- Results: {'; '.join(metrics)}\n").encode("utf-8")


def write_fragment_transaction(paths_and_blocks: list[tuple[Path, bytes]]) -> dict:
    """Write immutable fragments as one transaction, restoring all on failure."""
    existed = {path: path.exists() for path, _ in paths_and_blocks}
    originals = {path: path.read_bytes() if existed[path] else b"" for path, _ in paths_and_blocks}
    try:
        for path, data in paths_and_blocks:
            if existed[path]:
                require(originals[path] == data, f"conflicting immutable Stage 3 fragment exists: {path}")
            else:
                atomic_bytes(path, data)
        for path, data in paths_and_blocks:
            require(path.is_file() and path.read_bytes() == data, f"durable fragment verification failed: {path}")
        return {str(path.resolve()): sha256(path) for path, _ in paths_and_blocks}
    except Exception:
        for path, _ in paths_and_blocks:
            if existed[path]:
                if not path.is_file() or path.read_bytes() != originals[path]:
                    atomic_bytes(path, originals[path])
            elif path.is_file():
                path.unlink()
        raise


def clear_stale_stage3_failure_state(state: dict) -> None:
    """A verified Stage 3 completion supersedes any prior failure pointer."""
    state.pop("stage3_failure_class", None)
    state.pop("stage3_failure_event", None)


def publish(state_path: Path, decision_path: Path, verification_path: Path, event_path: Path,
            failed_notes: Path, source_notes: Path, passing_notes: Path, lease_path: Path,
            stage4_queue: Path = STAGE4_QUEUE, fragment_dir: Path = FRAGMENT_DIR) -> dict:
    state, decision, verification = load_object(state_path), load_object(decision_path), load_object(verification_path)
    require(state.get("cycle_id") == decision.get("cycle_id") == verification.get("cycle_id"), "Stage 3 cycle mismatch")
    require(state.get("current_ea") == decision.get("ea"), "Stage 3 EA mismatch")
    require(verification.get("passed") is True and verification.get("decision_sha256") == sha256(decision_path), "Stage 3 verification is invalid")
    stage1_spec = Path(verification.get("stage1_spec", ""))
    stage2_event = Path(verification.get("stage2_event", ""))
    require(stage1_spec.is_file() and sha256(stage1_spec) == verification.get("stage1_spec_sha256"),
            "Stage 1 specification changed before Stage 3 publication")
    require(stage2_event.is_file() and sha256(stage2_event) == verification.get("stage2_event_sha256"),
            "Stage 2 event changed before Stage 3 publication")
    verdict = decision.get("verdict")
    require(verdict in {"READY_FOR_STAGE4", "FAIL_ROTATE"}, "repair/infrastructure decisions cannot publish a completion event")
    if event_path.exists():
        existing = load_object(event_path)
        require(existing.get("cycle_id") == state["cycle_id"] and existing.get("decision_sha256") == sha256(decision_path), "conflicting Stage 3 event exists")
        for stored_path, expected_hash in existing.get("note_hashes", {}).items():
            note_path = Path(stored_path)
            require(note_path.is_file() and sha256(note_path) == expected_hash, f"published Stage 3 note changed: {note_path}")
        if existing.get("verdict") == "READY_FOR_STAGE4":
            queued = entry_for_ea(stage4_queue, decision["ea"], decision["cycle_id"])
            require(queued is not None and queued.get("status") == "verified_pending",
                    "published Stage 3 pass is absent from the Stage 4 queue")
            require(queued.get("decision_sha256") == sha256(decision_path),
                    "Stage 4 queue points to a different decision")
        if state.get("current_stage") == "stage3_running":
            existing_verdict = existing["verdict"]
            next_state = dict(state)
            next_state.update({
                "current_stage": existing["next_stage"],
                "stage_status": "stage3_complete_held" if existing_verdict == "READY_FOR_STAGE4" else "stage3_failed_held",
                "next_action": "inspect_stage3_before_stage4" if existing_verdict == "READY_FOR_STAGE4" else "inspect_stage3_failure_before_research",
                "hold_after_stage": 5 if authorizes_stages_1_to_5(state) else 3,
                "last_stage3_decision": str(decision_path.resolve()),
                "last_stage3_verification": str(verification_path.resolve()), "last_stage3_event": str(event_path.resolve()),
                "updated_utc": existing["completed_utc"],
            })
            if existing_verdict == "FAIL_ROTATE":
                next_state["next_mode"] = rotation_mode(state)
                next_state["next_action"] = rotation_action(next_state["next_mode"])
            next_state.pop("stage3_failure_detail", None)
            next_state.pop("promotion_blocked_reason", None)
            clear_stale_stage3_failure_state(next_state)
            atomic_json(state_path, next_state)
            if lease_path.is_file():
                lease_path.unlink()
        return existing
    require(state.get("schema_version") == 2 and state.get("current_stage") == "stage3_running", "canonical v2 state is not stage3_running")
    # Immutable per-cycle fragments. Aggregate views are regenerated by
    # kcos_reconstruct_records_v2.py --write. The completion event hashes the
    # immutable fragments only; later appends to mutable aggregate notes cannot
    # invalidate this event.
    cycle_id = state["cycle_id"]
    if verdict == "FAIL_ROTATE":
        failed_fragment = fragment_path(cycle_id, "failed", fragment_dir)
        source_fragment = fragment_path(cycle_id, "source", fragment_dir)
        note_hashes = write_fragment_transaction([
            (failed_fragment, note_block(decision, "failed")),
            (source_fragment, note_block(decision, "source")),
        ])
        next_stage, status = "stage1_pending", "stage3_failed_held"
        action = rotation_action(rotation_mode(state))
    else:
        passing_fragment = fragment_path(cycle_id, "passing", fragment_dir)
        note_hashes = write_fragment_transaction([(passing_fragment, note_block(decision, "passing"))])
        queue_entry = enqueue_verified(stage4_queue, decision, decision_path, verification_path)
        next_stage, status, action = "stage4_pending", "stage3_complete_held", "inspect_stage3_before_stage4"
    completed = utc_now()
    event = {
        "schema_version": 2, "event_id": f"{state['cycle_id']}:stage3:complete", "cycle_id": state["cycle_id"],
        "stage": 3, "status": "complete", "verdict": verdict, "ea": state["current_ea"], "next_stage": next_stage,
        "decision": str(decision_path.resolve()), "decision_sha256": sha256(decision_path),
        "verification": str(verification_path.resolve()), "verification_sha256": sha256(verification_path),
        "stage1_spec": str(stage1_spec.resolve()), "stage1_spec_sha256": verification["stage1_spec_sha256"],
        "stage2_event": str(stage2_event.resolve()), "stage2_event_sha256": verification["stage2_event_sha256"],
        "note_hashes": note_hashes, "completed_utc": completed, "consumed": False,
    }
    if verdict == "READY_FOR_STAGE4":
        event.update({"stage4_queue": str(stage4_queue.resolve()), "stage4_queue_id": queue_entry["queue_id"]})
    if decision.get("compute_skips"):
        event["compute_skips"] = decision["compute_skips"]
    next_state = dict(state)
    next_state.update({"current_stage": next_stage, "stage_status": status, "next_action": action,
                       "hold_after_stage": 5 if authorizes_stages_1_to_5(state) else 3,
                       "last_stage3_decision": str(decision_path.resolve()), "last_stage3_verification": str(verification_path.resolve()),
                       "last_stage3_event": str(event_path.resolve()), "updated_utc": completed})
    if verdict == "FAIL_ROTATE":
        next_state["next_mode"] = rotation_mode(state)
    next_state.pop("stage3_failure_detail", None)
    next_state.pop("promotion_blocked_reason", None)
    clear_stale_stage3_failure_state(next_state)
    atomic_json(event_path, event)
    atomic_json(state_path, next_state)
    if lease_path.is_file():
        lease_path.unlink()
    return event


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--decision", type=Path, required=True)
    parser.add_argument("--verification", type=Path, required=True)
    parser.add_argument("--event", type=Path, required=True)
    parser.add_argument("--failed-notes", type=Path, default=FAILED_NOTES)
    parser.add_argument("--source-notes", type=Path, default=SOURCE_NOTES)
    parser.add_argument("--passing-notes", type=Path, default=PASSING_NOTES)
    parser.add_argument("--lease", type=Path, default=LEASE)
    parser.add_argument("--stage4-queue", type=Path, default=STAGE4_QUEUE)
    parser.add_argument("--fragment-dir", type=Path, default=FRAGMENT_DIR)
    args = parser.parse_args()
    args.fragment_dir.mkdir(parents=True, exist_ok=True)
    result = publish(args.state, args.decision, args.verification, args.event, args.failed_notes, args.source_notes,
                     args.passing_notes, args.lease, args.stage4_queue, args.fragment_dir)
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
