#!/usr/bin/env python3
"""Emit compact Stage 3 context for initial and continuation Hermes runs."""

from __future__ import annotations

import json
from pathlib import Path

from kcos_stage3_common_v2 import load_object, require
from kcos_stage3_prepare_v2 import LEASE, ROOT, STAGE2_HANDOFF, STAGE3_HANDOFF, STATE, prepare


OUTPUT_DIR = ROOT / "state" / "stage3_v2"


def find_current_template(ea: str) -> Path:
    candidates = sorted(
        (ROOT / "state" / "inis").glob(f"{ea}.ETHUSD.M5.*.wide*.ini"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    require(bool(candidates), f"no same-EA M5 wide INI template exists for {ea}")
    return candidates[0]


def context() -> dict:
    state = load_object(STATE)
    current_stage = state.get("current_stage")
    if current_stage == "stage3_pending":
        template = find_current_template(state["current_ea"])
        handoff = prepare(STATE, STAGE2_HANDOFF, STAGE3_HANDOFF, LEASE, template, OUTPUT_DIR, dry_run=False)
        return {
            "mode": "INITIAL_LAUNCH",
            "contract": str((ROOT / "state" / "contracts" / "kcos_stage3_v2.md").resolve()),
            "handoff": str(STAGE3_HANDOFF.resolve()),
            "cycle_id": handoff["cycle_id"],
            "ea": handoff["ea"],
            "timeframe": "M5",
            "instruction": "Launch M5, register run-owned workers, start the zero-token watcher, then exit.",
        }
    require(current_stage == "stage3_running", f"Stage 3 context requires stage3_pending or stage3_running, got {current_stage}")
    handoff = load_object(STAGE3_HANDOFF)
    require(handoff.get("cycle_id") == state.get("cycle_id"), "Stage 3 handoff cycle mismatch")
    runtime_path = Path(handoff["runtime_manifest"])
    require(runtime_path.is_file(), "Stage 3 runtime manifest is missing")
    runtime = load_object(runtime_path)
    require(runtime.get("status") in {"TIMEFRAME_COMPLETE", "INFRASTRUCTURE_FAILURE"},
            "continuation was triggered before the watcher reached a terminal state")
    tf = runtime["timeframe"]
    event_path = Path(runtime["timeframe_event"])
    return {
        "mode": "TIMEFRAME_CONTINUATION",
        "contract": str((ROOT / "state" / "contracts" / "kcos_stage3_v2.md").resolve()),
        "handoff": str(STAGE3_HANDOFF.resolve()),
        "runtime": str(runtime_path.resolve()),
        "timeframe_event": str(event_path.resolve()),
        "cycle_id": handoff["cycle_id"],
        "ea": handoff["ea"],
        "timeframe": tf,
        "watcher_status": runtime["status"],
        "parse_csv": str((OUTPUT_DIR / f"{handoff['cycle_id']}.stage3.{tf}.csv").resolve()),
        "parse_evidence": str((OUTPUT_DIR / f"{handoff['cycle_id']}.stage3.{tf}.parse.json").resolve()),
        "decision": handoff["decision"],
        "instruction": "Parse and evaluate deterministically. Launch the requested next timeframe and watcher, or verify and publish the final held decision.",
    }


def main() -> int:
    print(json.dumps(context(), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
