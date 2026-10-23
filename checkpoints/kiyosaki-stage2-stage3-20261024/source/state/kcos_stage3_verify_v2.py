#!/usr/bin/env python3
"""Verify a final Stage 3 decision before publication."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from kcos_stage3_common_v2 import GATES, atomic_json, load_object, require, sha256, utc_now
import kcos_stage3_evaluate_v2 as evaluator
from kcos_stage3_evaluate_v2 import (
    gate_metrics, globally_inactive, should_skip_h1, validate_compute_metrics,
    zero_trade_timeframe,
)
from kcos_stage3_duration_policy_v2 import validate_eta_skip


def verify(handoff_path: Path, decision_path: Path, output_path: Path) -> dict:
    handoff, decision = load_object(handoff_path), load_object(decision_path)
    stage1_spec = Path(handoff.get("stage1_spec", ""))
    stage2_event = Path(handoff.get("stage2_event", ""))
    stage2_event_value = load_object(stage2_event) if stage2_event.is_file() else {}
    approved_from = handoff.get("from_date")
    approved_to = handoff.get("to_date")
    require(approved_from and approved_to, "handoff is missing approved date window")
    results = decision.get("timeframes", {})
    compute_skips = decision.get("compute_skips", {})
    checks = {
        "identity": decision.get("schema_version") == 2 and decision.get("stage") == 3
                    and decision.get("cycle_id") == handoff.get("cycle_id") and decision.get("ea") == handoff.get("ea"),
        "approved_dates": decision.get("from_date") == approved_from and decision.get("to_date") == approved_to
                          and all(value.get("from_date", approved_from) == approved_from and value.get("to_date", approved_to) == approved_to
                                  for value in results.values()),
        "approved_gates_only": decision.get("gates") == GATES,
        "final_verdict": decision.get("verdict") in {"READY_FOR_STAGE4", "FAIL_ROTATE", "REPAIR_REQUIRED", "INFRASTRUCTURE_FAILURE"},
        "m5_then_m15": list(results) == list(("M5", "M15", "H1")[:len(results)])
                       and "M5" in results,
        "artifacts_current": True,
        "gates_recomputed": True,
        "metrics_recomputed": True,
        "selection_valid": True,
        "h1_rule": True,
        "compute_limit_valid": True,
        "stage1_lineage": stage1_spec.is_file() and sha256(stage1_spec) == handoff.get("stage1_spec_sha256"),
        "stage2_lineage": stage2_event.is_file()
                          and stage2_event_value.get("cycle_id") == handoff.get("cycle_id")
                          and stage2_event_value.get("stage1_spec_sha256") == handoff.get("stage1_spec_sha256"),
    }
    for tf, artifact in decision.get("artifacts", {}).items():
        if results.get(tf, {}).get("status") == "COMPUTE_SKIPPED":
            evidence_path = Path(artifact.get("compute_evidence", ""))
            if (not evidence_path.is_file() or
                    sha256(evidence_path) != artifact.get("compute_evidence_sha256")):
                checks["artifacts_current"] = False
    # A terminal decision must account for every result it claims and every
    # normal result must retain its authenticated parse, CSV, and OPT chain.
    if decision.get("verdict") in {"READY_FOR_STAGE4", "FAIL_ROTATE", "REPAIR_REQUIRED"}:
        checks["artifacts_current"] = (checks["artifacts_current"]
                                        and set(decision.get("artifacts", {})) == set(results))
    if results:
        for tf, value in results.items():
            if value.get("status") == "COMPUTE_SKIPPED":
                continue
            artifact = decision.get("artifacts", {}).get(tf, {})
            for path_key, hash_key in (("parse_evidence", "parse_evidence_sha256"),
                                       ("csv", "csv_sha256"), ("opt", "opt_sha256")):
                path = Path(artifact.get(path_key, ""))
                if not path.is_file() or sha256(path) != artifact.get(hash_key):
                    checks["artifacts_current"] = False
        try:
            authenticated_evidence = []
            for tf, value in results.items():
                artifact = decision.get("artifacts", {}).get(tf, {})
                evidence_key = "compute_evidence" if value.get("status") == "COMPUTE_SKIPPED" else "parse_evidence"
                evidence_path = Path(artifact.get(evidence_key, ""))
                require(evidence_path.is_file(), f"missing authenticated Stage 3 {evidence_key}: {tf}")
                authenticated_evidence.append(evidence_path)
            recomputed, _ = evaluator.load_completed_results(handoff, authenticated_evidence)
            checks["metrics_recomputed"] = recomputed == results
        except (OSError, KeyError, TypeError, ValueError):
            checks["metrics_recomputed"] = False
    for tf, metrics in results.items():
        if metrics.get("status") != "COMPUTE_SKIPPED" and not metrics.get("zero_trades"):
            recomputed = gate_metrics(metrics)
            if metrics.get("checks") != recomputed["checks"] or metrics.get("passed") != recomputed["passed"]:
                checks["gates_recomputed"] = False
    eligible = {tf: value for tf, value in results.items() if value.get("passed") is True}
    selected = decision.get("selected_tf")
    if decision.get("verdict") == "READY_FOR_STAGE4":
        checks["selection_valid"] = selected in eligible and selected == max(eligible, key=lambda tf: eligible[tf]["cf"])
    elif decision.get("verdict") == "REPAIR_REQUIRED":
        repair_trigger = decision.get("repair_trigger") or {}
        zero_tf = zero_trade_timeframe(results)
        if repair_trigger.get("kind") == "completed_timeframe_zero_trades":
            trigger_tf = repair_trigger.get("timeframe")
            ordered_prefix = list(results) == list(evaluator.TIMEFRAMES[:len(results)])
            checks["selection_valid"] = (
                selected is None and trigger_tf == zero_tf and trigger_tf in results and ordered_prefix
            )
            checks["h1_rule"] = checks["selection_valid"]
        elif repair_trigger.get("kind") == "all_timeframes_below_minimum_trade_count":
            checks["selection_valid"] = selected is None and globally_inactive(results)
            checks["h1_rule"] = set(results) == set(evaluator.TIMEFRAMES)
        else:
            # Backward compatibility for prior cycle receipts.
            checks["selection_valid"] = selected is None and globally_inactive(results)
            checks["h1_rule"] = set(results) == set(evaluator.TIMEFRAMES)
    elif decision.get("verdict") == "FAIL_ROTATE":
        checks["selection_valid"] = selected is None and not eligible
        checks["h1_rule"] = set(results) == {"M5", "M15", "H1"}
    computed_skips = {}
    for tf, value in results.items():
        if value.get("status") != "COMPUTE_SKIPPED":
            continue
        artifact = decision.get("artifacts", {}).get(tf, {})
        evidence_path = Path(artifact.get("compute_evidence", ""))
        evidence = load_object(evidence_path) if evidence_path.is_file() else {}
        expected = {"failure_mode": value.get("failure_mode"), **value.get("metrics", {}),
                    **value.get("evidence", {})}
        computed_skips[tf] = expected
        if (evidence.get("cycle_id") != decision.get("cycle_id") or evidence.get("stage") != 3 or
                evidence.get("timeframe") != tf or evidence.get("outcome") != "COMPUTE_SKIPPED" or
                evidence.get("failure_mode") != value.get("failure_mode") or
                evidence.get("metrics") != value.get("metrics") or "consumed" in evidence):
            checks["compute_limit_valid"] = False
        else:
            eta_skip_valid = (
                evidence.get("duration_policy_action") == "skip_eta_over_4h"
                and validate_eta_skip(
                    evidence.get("duration_projection", {}),
                    evidence.get("evidence", {}).get("tester_ui_timing", {}),
                    evidence.get("duration_stop") or evidence.get("evidence", {}).get("duration_stop", {}),
                )
            )
            if not eta_skip_valid and not validate_compute_metrics(
                    value.get("failure_mode"), value.get("metrics", {}), value.get("evidence", {})):
                checks["compute_limit_valid"] = False
    if compute_skips != computed_skips:
        checks["compute_limit_valid"] = False
    comparable = ("M5" in results and "M15" in results and
                  results["M5"].get("status") != "COMPUTE_SKIPPED" and
                  results["M15"].get("status") != "COMPUTE_SKIPPED" and
                  not results["M5"].get("zero_trades") and not results["M15"].get("zero_trades"))
    if comparable:
        skip, _ = should_skip_h1(results["M5"], results["M15"])
        checks["h1_rule"] = (skip and "H1" not in results) or ((not skip) and "H1" in results)
    elif decision.get("verdict") == "FAIL_ROTATE":
        checks["h1_rule"] = set(results) == {"M5", "M15", "H1"}
    verification = {
        "schema_version": 2, "cycle_id": decision.get("cycle_id"), "stage": 3,
        "decision": str(decision_path.resolve()), "decision_sha256": sha256(decision_path),
        "checks": checks, "passed": all(checks.values()),
        "stage1_spec": str(stage1_spec.resolve()),
        "stage1_spec_sha256": sha256(stage1_spec) if stage1_spec.is_file() else None,
        "stage2_event": str(stage2_event.resolve()),
        "stage2_event_sha256": sha256(stage2_event) if stage2_event.is_file() else None,
        "verified_utc": utc_now(),
    }
    atomic_json(output_path, verification)
    return verification


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handoff", type=Path, required=True)
    parser.add_argument("--decision", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.handoff, args.decision, args.output)
    print(json.dumps(result))
    return 0 if result["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
