#!/usr/bin/env python3
"""Deterministically evaluate Stage 3 CSV artifacts and choose the cascade."""

from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import date
from pathlib import Path

from kcos_stage3_common_v2 import GATES, TIMEFRAMES, atomic_json, load_object, require, sha256, utc_now
from kcos_stage_contracts_v2 import require_equity_drawdown, select_highest_minato
from kcos_stage3_duration_policy_v2 import validate_eta_skip


def number(row: dict, *names: str) -> float:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            parsed = float(value)
            if not math.isfinite(parsed):
                raise ValueError(f"result row contains non-finite {name}: {value}")
            return parsed
    raise ValueError(f"result row is missing one of {names}")


def gate_metrics(metrics: dict) -> dict:
    checks = {
        "profit_positive": metrics["profit"] > GATES["profit_min_exclusive"],
        "cf_at_least_15": metrics["cf"] >= GATES["cf_min"],
        "tpm_at_least_35": metrics["tpm"] >= GATES["tpm_min"],
        "drawdown_at_most_35": metrics["drawdown_percent"] <= GATES["drawdown_max"],
        "margin_level_at_least_120": metrics["margin_level"] >= GATES["min_margin_level"],
    }
    return {"checks": checks, "passed": all(checks.values())}


def summarize_csv(path: Path, from_date: str, to_date: str) -> dict:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    require(bool(rows), f"no result rows in {path}")
    parsed = []
    for row in rows:
        parsed.append({
            "pass": int(number(row, "Pass")), "profit": number(row, "Profit"),
            "raw_cf": number(row, "CustomFitness", "MinatoCF"), "trades": int(number(row, "Trades")),
            # Stage 3/4 qualification is equity-based.  A balance drawdown
            # column is not an acceptable substitute.
            "drawdown_percent": require_equity_drawdown(row),
            "sharpe": number(row, "SharpeRatio"), "profit_factor": number(row, "ProfitFactor"),
            "margin_level": number(row, "MarginLevel", "MinMarginLevel"),
        })
    start = date.fromisoformat(from_date.replace(".", "-")); end = date.fromisoformat(to_date.replace(".", "-"))
    months = (end - start).days / (365.2425 / 12.0)
    require(months > 0, "test window must have positive duration")
    max_trades = max(item["trades"] for item in parsed)
    scored = []
    for item in parsed:
        margin_ok = item["trades"] == 0 or item["margin_level"] >= GATES["min_margin_level"]
        effective_cf = item["raw_cf"] if margin_ok else 0.0
        metrics = {**item, "cf": effective_cf, "margin_gate_passed": margin_ok,
                   "tpm": item["trades"] / months, "test_months": months,
                   "row_count": len(parsed), "max_trades": max_trades,
                   "zero_trades": max_trades == 0}
        scored.append({**metrics, **gate_metrics(metrics)})
    eligible = [item for item in scored if item["passed"]]
    pool = eligible or scored
    return select_highest_minato(pool)


def closest_key(item: tuple[str, dict]) -> tuple[float, float]:
    _, metrics = item
    deficits = (
        max(0.0, -metrics["profit"]) / max(1.0, abs(metrics["profit"])),
        max(0.0, GATES["cf_min"] - metrics["cf"]) / GATES["cf_min"],
        max(0.0, GATES["tpm_min"] - metrics["tpm"]) / GATES["tpm_min"],
        max(0.0, metrics["drawdown_percent"] - GATES["drawdown_max"]) / GATES["drawdown_max"],
    )
    return sum(deficits), -metrics["cf"]


def should_skip_h1(m5: dict, m15: dict) -> tuple[bool, str]:
    if m5["passed"] and not m15["passed"]:
        return True, "M5 passed all gates and M15 failed"
    return False, "H1 is required unless M5 passes and M15 fails"


def zero_trade_timeframe(results: dict) -> str | None:
    """Return the first completed timeframe with zero trades across all passes."""
    for timeframe in TIMEFRAMES:
        value = results.get(timeframe)
        if (isinstance(value, dict) and value.get("status") != "COMPUTE_SKIPPED"
                and value.get("zero_trades") is True and value.get("max_trades") == 0):
            return timeframe
    return None


def globally_inactive(results: dict) -> bool:
    """Return true only when all three authenticated timeframes are inactive."""
    return (set(results) == set(TIMEFRAMES)
            and all(value.get("status") != "COMPUTE_SKIPPED"
                    and int(value.get("max_trades", 0)) < 10
                    for value in results.values()))


def requires_stage2_repair(results: dict) -> bool:
    """Stop the timeframe cascade on authenticated zero trades or a globally inactive EA."""
    return zero_trade_timeframe(results) is not None or globally_inactive(results)


def validate_compute_metrics(failure_mode: str, metrics: dict, evidence: dict) -> bool:
    budgets = evidence.get("budgets", {}) if isinstance(evidence, dict) else {}
    qualification_cutoff = float(budgets.get("first_pass_seconds", 120))
    hard_limit = float(budgets.get("first_pass_hard_limit_seconds", 200))
    qualification = metrics.get("compute_qualification")
    first_pass = metrics.get("first_pass_seconds")
    censored = metrics.get("first_pass_censored_seconds")
    completed = metrics.get("completed_passes")
    if failure_mode == "compute_stalled":
        return (qualification == "qualified" and completed is not None and int(completed) > 0 and
                first_pass is not None and float(first_pass) <= qualification_cutoff and censored is None)
    if qualification == "slow_first_pass":
        return (completed is not None and int(completed) > 0 and first_pass is not None and
                qualification_cutoff < float(first_pass) <= hard_limit and censored is None)
    if qualification == "censored_no_first_pass":
        return (int(completed or 0) == 0 and first_pass is None and censored is not None and
                float(censored) >= hard_limit)
    return False


def load_completed_results(handoff: dict, evidence_paths: list[Path]) -> tuple[dict, dict]:
    """Load ordered normal and compute-skipped timeframe evidence."""
    results, artifacts = {}, {}
    for evidence_path in evidence_paths:
        evidence = load_object(evidence_path)
        tf = evidence.get("timeframe")
        require(tf in TIMEFRAMES and tf not in results, "duplicate or invalid timeframe evidence")
        if evidence.get("outcome") == "COMPUTE_SKIPPED":
            require(evidence.get("schema_version") == 2 and evidence.get("stage") == 3 and
                    evidence.get("cycle_id") == handoff["cycle_id"], "compute evidence mismatch")
            require(evidence.get("failure_mode") in {"compute_budget_exceeded", "compute_stalled"},
                    "invalid compute evidence failure mode")
            metrics = evidence.get("metrics")
            require(isinstance(metrics, dict) and metrics.get("elapsed_seconds") is not None and
                    metrics.get("completed_passes") is not None, "compute evidence is missing metrics")
            ui_evidence = evidence.get("evidence", {})
            eta_skip_valid = (
                evidence.get("duration_policy_action") == "skip_eta_over_4h"
                and validate_eta_skip(
                    evidence.get("duration_projection", {}),
                    ui_evidence.get("tester_ui_timing", {}),
                    ui_evidence.get("duration_stop", {}),
                )
            )
            require(eta_skip_valid or validate_compute_metrics(evidence["failure_mode"], metrics, ui_evidence),
                    "compute evidence violates the two-threshold qualification policy")
            results[tf] = {
                "status": "COMPUTE_SKIPPED", "failure_mode": evidence["failure_mode"],
                "passed": False, "zero_trades": False, "metrics": metrics,
                "evidence": evidence.get("evidence", {}),
            }
            artifacts[tf] = {
                "compute_evidence": str(evidence_path.resolve()),
                "compute_evidence_sha256": sha256(evidence_path),
            }
            continue
        require(evidence.get("cycle_id") == handoff["cycle_id"]
                and evidence.get("ea") == handoff.get("ea")
                and evidence.get("passed") is True,
                "parse evidence mismatch")
        csv_path = Path(evidence["csv"])
        require(sha256(csv_path) == evidence.get("csv_sha256"), "parsed CSV hash mismatch")
        require(evidence.get("from_date") == handoff["from_date"] and
                evidence.get("to_date") == handoff["to_date"], "result date window mismatch")
        results[tf] = summarize_csv(csv_path, handoff["from_date"], handoff["to_date"])
        artifacts[tf] = {
            "parse_evidence": str(evidence_path.resolve()),
            "parse_evidence_sha256": sha256(evidence_path),
            "csv": str(csv_path.resolve()), "csv_sha256": evidence["csv_sha256"],
            "opt": evidence["opt"], "opt_sha256": evidence["opt_sha256"],
        }
    require(list(results) == list(TIMEFRAMES[:len(results)]),
            "timeframe evidence must be M5, then M15, then optional H1")
    return results, artifacts


def evaluate(handoff_path: Path, evidence_paths: list[Path], output_path: Path) -> dict:
    handoff = load_object(handoff_path)
    require(handoff.get("schema_version") == 2 and handoff.get("stage") == 3, "Stage 3 handoff schema mismatch")
    results, artifacts = load_completed_results(handoff, evidence_paths)
    require("M5" in results, "M5 must be evaluated first")
    normal = {tf: value for tf, value in results.items() if value.get("status") != "COMPUTE_SKIPPED"}
    compute_skips = {tf: {"failure_mode": value["failure_mode"], **value["metrics"],
                          **value.get("evidence", {})}
                     for tf, value in results.items() if value.get("status") == "COMPUTE_SKIPPED"}
    zero_trade_tf = zero_trade_timeframe(results)
    repair_trigger = None
    if zero_trade_tf is not None:
        verdict, next_tf = "REPAIR_REQUIRED", None
        reason = (f"{zero_trade_tf} produced zero trades across every completed optimization pass; "
                  "return to Stage 2 for repair before launching another timeframe")
        repair_trigger = {"kind": "completed_timeframe_zero_trades", "timeframe": zero_trade_tf}
    elif globally_inactive(results):
        verdict, next_tf = "REPAIR_REQUIRED", None
        reason = ("all three authenticated timeframes produced fewer than 10 trades "
                  "across every completed optimization pass; return to Stage 2 for repair")
        repair_trigger = {"kind": "all_timeframes_below_minimum_trade_count"}
    elif "M15" not in results:
        verdict, next_tf, reason = "NEXT_TIMEFRAME", "M15", "M5 complete; M15 is mandatory"
    else:
        comparable = "M5" in normal and "M15" in normal
        skip, reason = should_skip_h1(normal["M5"], normal["M15"]) if comparable else (
            False, "H1 is mandatory because M5 and M15 lack comparable normal results")
        if not skip and "H1" not in results:
            verdict, next_tf = "NEXT_TIMEFRAME", "H1"
        else:
            eligible = {tf: metrics for tf, metrics in normal.items() if metrics["passed"]}
            if eligible:
                selected = max(eligible, key=lambda tf: eligible[tf]["cf"])
                verdict, next_tf, reason = "READY_FOR_STAGE4", None, f"{selected} has the highest qualifying Minato base score"
            else:
                selected = None
                verdict, next_tf = "FAIL_ROTATE", None
                reason = ("all attempted timeframes were compute-skipped or failed mandatory gates"
                          if compute_skips else "all tested timeframes failed one or more mandatory gates")
    diagnostic = {}
    nonzero = {tf: value for tf, value in normal.items() if not value.get("zero_trades")}
    if nonzero:
        diagnostic = {
            "highest_cf_tf": max(nonzero, key=lambda tf: nonzero[tf]["cf"]),
            "highest_tpm_tf": max(nonzero, key=lambda tf: nonzero[tf]["tpm"]),
            "closest_to_pass_tf": min(nonzero.items(), key=closest_key)[0],
        }
    selected_tf = locals().get("selected") if verdict in {"READY_FOR_STAGE4", "FAIL_ROTATE"} else None
    previous = load_object(output_path) if output_path.exists() else {}
    decision = {
        "schema_version": 2, "cycle_id": handoff["cycle_id"], "stage": 3, "ea": handoff["ea"],
        "verdict": verdict, "next_timeframe": next_tf, "reason": reason, "selected_tf": selected_tf,
        "from_date": handoff["from_date"], "to_date": handoff["to_date"], "gates": GATES,
        "timeframes": results, "artifacts": artifacts, "compute_skips": compute_skips,
        "repair_trigger": repair_trigger,
        **diagnostic, "decided_utc": previous.get("decided_utc", utc_now()),
    }
    atomic_json(output_path, decision)
    return decision


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handoff", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(evaluate(args.handoff, args.evidence, args.output)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
