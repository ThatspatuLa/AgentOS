#!/usr/bin/env python3
"""Fail-closed KCOS Stage 4 evaluator for the active v2 cycle."""
from __future__ import annotations
import csv, hashlib, json, math, re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from kcos_stage3_common_v2 import atomic_json, sha256, utc_now
from kcos_run_authorization_v2 import authorizes_stages_1_to_5
from kcos_tester_config_v2 import is_timing_control_input, stage2_config_from_spec
from kcos_stage45_workflow_v2 import (
    evaluate_refinement_range, fit_refinement_grid, input_movement_bounds, refined_step,
    select_top_records, source_init_input_bounds,
    validate_input_range_plan,
)
from kcos_runtime_paths_v2 import assert_safe_root

ROOT = assert_safe_root()
STATE = ROOT / "state"
STATE_FILE = STATE / "kcos_cycle_state_v2.json"
QUEUE_FILE = STATE / "stage4_pending_queue_v2.json"
DIAG_DIR = STATE / "diagnostics"
REQUIRED_GATES = {
    "profit_min_exclusive": 0.0,
    "cf_min": 15.0,
    "tpm_min": 35.0,
    "drawdown_max": 35.0,
    "min_margin_level": 120.0,
}


class TimingControlPolicyError(ValueError):
    """A downstream Stage 3 artifact searched a prohibited timing control."""


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def fail(message: str, cycle: str | None = None) -> int:
    DIAG_DIR.mkdir(parents=True, exist_ok=True)
    name = (cycle or "unknown") + ".stage4.diagnostic.json"
    atomic_json(DIAG_DIR / name, {"schema_version": 2, "stage": 4, "cycle_id": cycle,
                                  "passed": False, "diagnostic": message, "written_utc": utc_now()})
    print(json.dumps({"status": "precondition_failed", "diagnostic": message}))
    return 1


def finite(row, key):
    try:
        value = float(row[key])
        return value if math.isfinite(value) else None
    except (KeyError, TypeError, ValueError):
        return None


def parse_ini(path: Path, names: list[str]) -> dict[str, dict[str, float]]:
    out = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if "=" not in line:
            continue
        name, rhs = line.strip().split("=", 1)
        if name not in names:
            continue
        parts = rhs.split("||")
        if len(parts) < 5:
            continue
        try:
            out[name] = {"default": float(parts[0]), "start": float(parts[1]),
                         "step": float(parts[2]), "stop": float(parts[3]),
                         "optimize": parts[4].strip()}
        except ValueError:
            pass
    return out


def audit_input_surface(*, parse_evidence: dict, ini_path: Path,
                        param_names: list[str], cycle: str, ea: str) -> dict:
    """Bind each wide range to the EA default/type and explicit input rules.

    The first INI field is the compiled EA default. The remaining numeric
    fields describe the search grid. Keep those facts separate so scientific
    notation and signed domains are interpreted from source evidence, not
    guessed from an input name or from the range endpoints.
    """
    source_path = Path(parse_evidence.get("source", ""))
    spec_path = Path(parse_evidence.get("stage1_spec", ""))
    if not source_path.is_file() or sha256(source_path) != parse_evidence.get("source_sha256"):
        raise ValueError("EA source missing or hash mismatch in Stage 3 parse evidence")
    if not spec_path.is_file() or sha256(spec_path) != parse_evidence.get("stage1_spec_sha256"):
        raise ValueError("Stage 1 input contract missing or hash mismatch in Stage 3 parse evidence")
    spec = load(spec_path)
    if spec.get("cycle_id") != cycle or spec.get("ea") != ea:
        raise ValueError("Stage 1 input contract identity mismatch")
    raw_config = spec.get("tester_config")
    if not isinstance(raw_config, dict) or not isinstance(raw_config.get("input_constraints"), list):
        raise ValueError("Stage 1 tester_config.input_constraints must be an explicit list (use [] when independent)")
    raw_inputs = raw_config.get("inputs")
    if not isinstance(raw_inputs, dict):
        raise ValueError("Stage 1 tester_config.inputs is missing")
    source_text = source_path.read_text(encoding="utf-8-sig", errors="replace")
    config = stage2_config_from_spec(spec, source_text=source_text)
    inputs = config.get("inputs")
    ini_rows: dict[str, list[str]] = {}
    in_tester_inputs = False
    for line in ini_path.read_text(encoding="utf-8-sig").splitlines():
        row = line.strip()
        if row.startswith("["):
            in_tester_inputs = row == "[TesterInputs]"
            continue
        if in_tester_inputs and "=" in row:
            name, value = row.split("=", 1)
            ini_rows.setdefault(name.strip(), []).append(value.strip())
    excluded_cooldown_inputs = []
    for name, cfg in raw_inputs.items():
        control_kind = is_timing_control_input(name, cfg)
        if control_kind is None:
            continue
        values = ini_rows.get(name, [])
        if name in param_names or values != ["0"]:
            raise TimingControlPolicyError(
                f"timing control {name} must be a scalar zero and excluded from optimization"
            )
        if control_kind == "cooldown":
            source_match = re.search(
                rf"(?im)^\s*input\s+[A-Za-z_]\w*\s+{re.escape(name)}\s*=\s*([^;]+);",
                source_text,
            )
            literal = source_match.group(1).split("//", 1)[0].strip() if source_match else ""
            try:
                source_default = float(literal.rstrip("fF"))
            except ValueError:
                source_default = float("nan")
            if not math.isclose(source_default, 0.0, rel_tol=0.0, abs_tol=1e-15):
                raise TimingControlPolicyError(
                    f"legacy cooldown input {name} must have a verified zero source default"
                )
            excluded_cooldown_inputs.append(name)
    ini = parse_ini(ini_path, param_names)
    if set(ini) != set(param_names):
        raise ValueError("launched wide INI does not contain every optimized input")

    declaration_re = re.compile(
        r"(?m)^\s*input\s+([A-Za-z_]\w*)\s+([A-Za-z_]\w*)\s*=\s*([^;]+);"
    )
    declarations = {}
    for match in declaration_re.finditer(source_text):
        typ, name, literal = match.groups()
        literal = literal.split("//", 1)[0].strip()
        if name in declarations:
            raise ValueError(f"EA input {name} has ambiguous declarations")
        declarations[name] = (typ.lower(), literal)

    integer_types = {"int", "uint", "ulong", "long", "short", "uchar", "ushort", "integer", "byte", "sbyte"}
    real_types = {"double", "float", "real"}
    def type_family(value: str) -> str:
        value = value.lower()
        return "integer" if value in integer_types else "real" if value in real_types else value

    numeric_literal = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?[fF]?$")
    def source_numeric_default(name: str, source_type: str, literal: str) -> float:
        if source_type.lower() in {"bool", "boolean"}:
            normalized = literal.strip().lower()
            if normalized in {"true", "false"}:
                return 1.0 if normalized == "true" else 0.0
        if not numeric_literal.fullmatch(literal):
            raise ValueError(f"{name} has no verifiable numeric EA input declaration/default")
        return float(literal.rstrip("fF"))

    source_runtime_bounds = source_init_input_bounds(source_text)
    audited = {}
    for name in param_names:
        cfg = inputs.get(name)
        declaration = declarations.get(name)
        if not isinstance(cfg, dict) or str(cfg.get("optimize", "N")).upper() != "Y":
            raise ValueError(f"{name} is not an authenticated optimized Stage 1 input")
        if declaration is None:
            raise ValueError(f"{name} has no verifiable numeric EA input declaration/default")
        source_type, source_literal = declaration
        try:
            source_default = source_numeric_default(name, source_type, source_literal)
            stage1_default = float(cfg["default"])
            row = ini[name]
            default, start, step, stop = (row[k] for k in ("default", "start", "step", "stop"))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{name} has a missing or non-numeric default/grid") from exc
        if not all(math.isfinite(v) for v in (source_default, stage1_default, default, start, step, stop)):
            raise ValueError(f"{name} contains a non-finite source default or search grid")
        if type_family(str(cfg.get("type", source_type))) != type_family(source_type):
            raise ValueError(f"{name} Stage 1 type conflicts with EA source type")
        for label, value in (("Stage 1", stage1_default), ("INI", default)):
            if not math.isclose(source_default, value, rel_tol=1e-12, abs_tol=1e-15):
                raise ValueError(f"{name} {label} default {value:g} conflicts with EA source default {source_literal}")
        if row["optimize"].upper() != "Y" or step <= 0 or start >= stop or not start <= source_default <= stop:
            raise ValueError(f"{name} has an invalid optimized grid")
        grid_index = (source_default - start) / step
        if not math.isclose(grid_index, round(grid_index), rel_tol=0.0, abs_tol=1e-7):
            raise ValueError(f"{name} EA default is not an actual optimization candidate")
        if type_family(source_type) == "integer" and not all(v.is_integer() for v in (start, step, stop)):
            raise ValueError(f"{name} integer EA input has a fractional grid")
        if start < 0 < stop:
            sign = "crosses_zero"
        elif start >= 0:
            sign = "nonnegative" if start == 0 else "positive"
        elif stop <= 0:
            sign = "nonpositive" if stop == 0 else "negative"
        else:
            sign = "zero_only"
        applicable = [rule for rule in config["input_constraints"]
                      if isinstance(rule, dict) and name in (rule.get("coefficients") or {})]
        audited[name] = {
            "type": source_type, "ea_source_default_literal": source_literal,
            "ea_source_default": source_default, "stage1_default": stage1_default,
            "search_start": start, "search_step": step, "search_stop": stop,
            "interval_count": round((stop - start) / step, 9), "search_sign_domain": sign,
            "search_crosses_zero": start < 0 < stop,
            "ea_default_grid_index": int(round(grid_index)),
            "ea_default_grid_position": (source_default - start) / (stop - start),
            "default_is_optimization_candidate": True,
            "applicable_input_constraints": [str(rule.get("id", "unnamed")) for rule in applicable],
            "source_runtime_bounds": source_runtime_bounds.get(name),
        }

    # MT5 optimizes each input independently. Check each declared linear rule
    # at its worst grid corner, so invalid combinations cannot enter the run.
    fixed_constraint_values = {}
    for name, cfg in inputs.items():
        if str(cfg.get("optimize", "N")).upper() == "Y":
            continue
        declaration = declarations.get(name)
        if declaration is None:
            continue
        source_type, source_literal = declaration
        if not numeric_literal.fullmatch(source_literal) and source_type not in {"bool", "boolean"}:
            continue
        try:
            source_default = source_numeric_default(name, source_type, source_literal)
            stage1_default = float(cfg["default"])
        except (KeyError, TypeError, ValueError):
            continue
        if math.isclose(source_default, stage1_default, rel_tol=1e-12, abs_tol=1e-15):
            fixed_constraint_values[name] = source_default

    for index, rule in enumerate(config["input_constraints"]):
        if not isinstance(rule, dict):
            raise ValueError(f"input constraint #{index + 1} must be an object")
        label = str(rule.get("id", f"constraint_{index + 1}"))
        coefficients, operator = rule.get("coefficients"), rule.get("operator")
        if not isinstance(coefficients, dict) or not coefficients or operator not in {"<=", "<", ">=", ">"}:
            raise ValueError(f"input constraint {label} has an unsupported shape")
        try:
            limit = float(rule["limit"])
            low_sum = high_sum = default_sum = 0.0
            for name, raw_coefficient in coefficients.items():
                if name in audited:
                    item = audited[name]
                    lo, hi, value = item["search_start"], item["search_stop"], item["ea_source_default"]
                elif name in fixed_constraint_values:
                    lo = hi = value = fixed_constraint_values[name]
                else:
                    raise ValueError(f"input constraint {label} references missing or unverified input {name}")
                coefficient = float(raw_coefficient)
                low_sum += coefficient * (lo if coefficient >= 0 else hi)
                high_sum += coefficient * (hi if coefficient >= 0 else lo)
                default_sum += coefficient * value
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"input constraint {label} has invalid numeric metadata: {exc}") from exc
        if not math.isfinite(limit) or not all(math.isfinite(v) for v in (low_sum, high_sum, default_sum)):
            raise ValueError(f"input constraint {label} contains non-finite values")
        if operator in {"<", "<="}:
            default_ok = default_sum < limit - 1e-12 if operator == "<" else default_sum <= limit + 1e-12
            grid_ok = high_sum < limit - 1e-12 if operator == "<" else high_sum <= limit + 1e-12
            worst = high_sum
        else:
            default_ok = default_sum > limit + 1e-12 if operator == ">" else default_sum >= limit - 1e-12
            grid_ok = low_sum > limit + 1e-12 if operator == ">" else low_sum >= limit - 1e-12
            worst = low_sum
        if not default_ok or not grid_ok:
            raise ValueError(f"input constraint {label} rejects EA defaults or optimization grid (worst={worst:g} {operator} {limit:g})")

    return {"schema_version": 1, "cycle_id": cycle, "ea": ea,
            "source": {"path": str(source_path.resolve()), "sha256": sha256(source_path)},
            "stage1_spec": {"path": str(spec_path.resolve()), "sha256": sha256(spec_path)},
            "ini": {"path": str(ini_path.resolve()), "sha256": sha256(ini_path)},
            "policy": "EA declarations own numeric defaults and types; signed search domains are retained when declared constraints and defaults validate.",
            "optimized_inputs": audited,
            "constraint_ids": [str(rule.get("id", "unnamed")) for rule in config["input_constraints"]],
            "excluded_fixed_cooldown_inputs": sorted(excluded_cooldown_inputs)}


def write_rejection(*, cycle: str, ea: str, selected_tf: str, entry: dict,
                    artifacts: dict, ini_path: Path, csv_path: Path, gates: dict,
                    qualifying_count: int, invalid_count: int, total_rows: int,
                    reason: str) -> int:
    """Persist a verified fail-closed Stage 4 rejection instead of dropping the result."""
    decision_path = STATE / "decisions" / f"{cycle}.stage4.decision.json"
    verification_path = STATE / "verification" / f"{cycle}.stage4.verification.json"
    event_path = STATE / "events" / f"{cycle}.stage4.complete.json"
    handoff_path = STATE / "handoffs" / "Stage4_v2.json"
    stage3_artifacts = {}
    for tf, artifact in artifacts.items():
        item = {"status": artifact.get("status")}
        for key in ("parse_evidence", "csv", "opt"):
            if artifact.get(key):
                item[key] = artifact[key]
        for key in ("parse_evidence", "csv", "opt"):
            if artifact.get(key):
                artifact_path = Path(artifact[key])
                item[key + "_sha256"] = sha256(artifact_path) if artifact_path.is_file() else None
        if artifact.get("compute_evidence"):
            item["compute_evidence"] = artifact["compute_evidence"]
            item["compute_evidence_sha256"] = sha256(Path(artifact["compute_evidence"]))
        stage3_artifacts[tf] = item
    decision = {
        "schema_version": 2, "cycle_id": cycle, "stage": 4, "ea": ea,
        "selected_tf": selected_tf,
        "policy_source": "state/kcos_stage45_workflow_v2.py",
        "stage3_decision_sha256": sha256(Path(entry["decision"])),
        "stage3_verification_sha256": sha256(Path(entry["verification"])),
        "stage3_artifacts": stage3_artifacts,
        "wide_ini": str(ini_path), "wide_ini_sha256": sha256(ini_path),
        "csv": str(csv_path), "csv_sha256": sha256(csv_path), "gates": gates,
        "total_record_count": total_rows, "qualifying_row_count": qualifying_count,
        "invalid_or_out_of_domain_count": invalid_count, "valid_record_count": 0,
        "selected_record_count": 0, "cutoff_effective_minato_score": None,
        "input_actions": {}, "narrow_runs_by_input": {},
        "stage5_transition": "hold_reject", "verdict": "STAGE4_REJECTED",
        "reason": reason, "decided_utc": utc_now(),
    }
    atomic_json(decision_path, decision)
    checks = {"identity": decision["cycle_id"] == cycle and decision["ea"] == ea,
              "stage3_hashes": decision["stage3_decision_sha256"] == entry["decision_sha256"] and decision["stage3_verification_sha256"] == entry["verification_sha256"],
              "timeframe_evidence": all(Path(a.get("csv", "")).is_file() and
                                          a.get("csv_sha256") is not None and
                                          sha256(Path(a["csv"])) == a["csv_sha256"]
                                          for a in decision["stage3_artifacts"].values()),
              "csv_hashes": sha256(csv_path) == decision["csv_sha256"],
              "ini_hash": sha256(ini_path) == decision["wide_ini_sha256"],
              "policy": decision["policy_source"].endswith("kcos_stage45_workflow_v2.py"),
              "rejection_threshold": qualifying_count < 50}
    verification = {"schema_version": 2, "cycle_id": cycle, "stage": 4,
        "decision": str(decision_path), "decision_sha256": sha256(decision_path),
        "passed": all(checks.values()), "checks": checks, "verified_utc": utc_now()}
    atomic_json(verification_path, verification)
    event = {"schema_version": 2, "event_id": f"{cycle}:stage4:complete", "cycle_id": cycle,
        "stage": 4, "status": "complete", "verdict": decision["verdict"], "ea": ea,
        "next_stage": "queue_next", "decision": str(decision_path),
        "decision_sha256": sha256(decision_path), "verification": str(verification_path),
        "verification_sha256": sha256(verification_path), "emitted_utc": utc_now()}
    atomic_json(event_path, event)
    handoff = {"schema_version": 2, "cycle_id": cycle, "stage": 4, "ea": ea,
        "decision": str(decision_path), "decision_sha256": sha256(decision_path),
        "verification": str(verification_path), "verification_sha256": sha256(verification_path),
        "event": str(event_path), "event_sha256": sha256(event_path),
        "narrow_opt_pending": False, "current_stage": "stage4_rejected",
        "next_action": "queue_next", "hold_after_stage": 4, "prepared_utc": utc_now()}
    atomic_json(handoff_path, handoff)
    queue = load(QUEUE_FILE)
    match = next((e for e in queue["entries"] if e.get("cycle_id") == cycle and e.get("ea") == ea and e.get("status") == "verified_pending"), None)
    if match is None:
        return fail("active queue entry changed before rejection update", cycle)
    match.update({"stage4_decision": str(decision_path), "stage4_decision_sha256": sha256(decision_path),
        "stage4_verification": str(verification_path), "stage4_verification_sha256": sha256(verification_path),
        "stage4_event": str(event_path), "stage4_event_sha256": sha256(event_path),
        "status": "stage4_rejected", "updated_utc": utc_now(), "hold_reason": reason})
    queue["updated_utc"] = utc_now(); atomic_json(QUEUE_FILE, queue)
    state = load(STATE_FILE)
    if (state.get("cycle_id") != cycle or state.get("current_ea") != ea
            or state.get("stage_status") != "stage4_pending"
            or state.get("hold_after_stage") is not None
            or state.get("run_authorization") != authorization_snapshot):
        return fail("canonical state changed before rejection update", cycle)
    state.update({"current_stage": "stage4_rejected", "next_action": "queue_next",
        "stage_status": "stage4_rejected", "allowed_stages": [1,2,3,4], "hold_after_stage": 4,
        "updated_utc": utc_now(), "last_stage4_decision": str(decision_path),
        "last_stage4_verification": str(verification_path), "last_stage4_event": str(event_path),
        "last_stage4_handoff": str(handoff_path), "verdict": decision["verdict"],
        "passed": verification["passed"], "event_type": "stage4_complete"})
    atomic_json(STATE_FILE, state)
    print(json.dumps({"status":"completed", "verdict":decision["verdict"], "cycle_id":cycle,
        "ea":ea, "selected_tf":selected_tf, "qualifying":qualifying_count,
        "required":50, "queue_status":"stage4_rejected", "canonical_stage_status":state["stage_status"]}, indent=2))
    return 0


def main() -> int:
    state = load(STATE_FILE)
    queue = load(QUEUE_FILE)
    cycle, ea = state.get("cycle_id"), state.get("current_ea")
    authorization_snapshot = state.get("run_authorization")
    full_cycle_authorized = authorizes_stages_1_to_5(state)
    completion_allowed_stages = [1, 2, 3, 4, 5] if full_cycle_authorized else [1, 2, 3, 4]
    completion_hold_after_stage = 5 if full_cycle_authorized else 4
    if state.get("hold_after_stage") is not None or state.get("stage_status") != "stage4_pending":
        return fail("canonical state is not eligible: hold_after_stage must be null and stage_status must be stage4_pending", cycle)
    entry = next((e for e in queue.get("entries", []) if e.get("cycle_id") == cycle and e.get("ea") == ea), None)
    if not entry or entry.get("status") != "verified_pending":
        return fail("no matching verified_pending active-cycle/current-EA queue entry", cycle)
    if not entry.get("decision") or not entry.get("verification"):
        return fail("queue entry lacks Stage 3 decision or verification", cycle)
    if not Path(entry["decision"]).is_file() or not Path(entry["verification"]).is_file():
        return fail("referenced Stage 3 decision/verification is missing", cycle)
    if sha256(Path(entry["decision"])) != entry.get("decision_sha256") or sha256(Path(entry["verification"])) != entry.get("verification_sha256"):
        return fail("queue decision/verification hash mismatch", cycle)
    stage3, stage3v = load(Path(entry["decision"])), load(Path(entry["verification"]))
    if not (stage3.get("schema_version") == 2 and stage3.get("stage") == 3 and stage3.get("cycle_id") == cycle and stage3.get("ea") == ea and stage3.get("verdict") == "READY_FOR_STAGE4" and stage3v.get("passed") is True):
        return fail("Stage 3 identity or verification gate failed", cycle)
    if stage3.get("gates") != REQUIRED_GATES:
        return fail("Stage 3 gates do not exactly match the Stage 4 policy", cycle)
    lineage = entry.get("lineage")
    if not lineage:
        return fail("active Stage 3 queue entry lacks authenticated lineage", cycle)
    queue_tf = entry.get("selected_tf") or stage3.get("selected_tf")
    if queue_tf not in ("M5", "M15", "H1"):
        return fail("Stage 3 queue has no valid evaluated timeframe", cycle)
    eligible_tfs = [tf for tf, summary in stage3.get("timeframes", {}).items()
                    if tf in ("M5", "M15", "H1") and
                    all(summary.get("checks", {}).get(k) is True for k in (
                        "profit_positive", "cf_at_least_15", "tpm_at_least_35",
                        "drawdown_at_most_35", "margin_level_at_least_120"))]
    if not eligible_tfs:
        return fail("Stage 3 provides no qualifying timeframe for Stage 4", cycle)
    def tf_custom_score(tf):
        summary = stage3["timeframes"][tf]
        value = summary.get("custom_score", summary.get("cf"))
        return float(value)
    try:
        selected_tf = max(eligible_tfs, key=tf_custom_score)
    except (TypeError, ValueError, KeyError):
        return fail("eligible timeframe is missing a finite custom score", cycle)
    highest_tf = selected_tf

    artifacts = stage3.get("artifacts", {})
    required_timeframes = set(stage3.get("timeframes", {}))
    if set(artifacts) != required_timeframes:
        return fail("Stage 3 decision does not provide one artifact set per evaluated timeframe", cycle)
    for tf in required_timeframes:
        a = artifacts.get(tf, {})
        if stage3.get("timeframes", {}).get(tf, {}).get("status") == "COMPUTE_SKIPPED":
            continue
        for key in ("parse_evidence", "csv"):
            p = Path(a.get(key, ""))
            if not p.is_file():
                return fail(f"missing Stage 3 {tf} {key}", cycle)
            if sha256(p) != a.get(key + "_sha256"):
                return fail(f"Stage 3 {tf} {key} hash mismatch", cycle)
        pe = load(Path(a["parse_evidence"]))
        if pe.get("passed") is not True or pe.get("cycle_id") != cycle or pe.get("ea") != ea or pe.get("timeframe") != tf:
            return fail(f"Stage 3 {tf} parse evidence failed identity/passed gate", cycle)

    active = artifacts[selected_tf]
    if active.get("compute_evidence"):
        return fail("selected Stage 4 timeframe has no normal optimization result", cycle)
    csv_path, ini_path = Path(active["csv"]), Path(active["parse_evidence"][0:active["parse_evidence"].rfind(".stage3.")])
    # The parse evidence contains the authoritative wide INI path.
    parse_evidence = load(Path(active["parse_evidence"]))
    ini_path = Path(parse_evidence["ini"])
    if not ini_path.is_file() or sha256(ini_path) != parse_evidence.get("ini_sha256"):
        # A repaired Stage 3->4 handoff may carry a freshly materialized,
        # Stage-1-authorized operational INI while preserving immutable Stage 3
        # result evidence.  Accept it only when the queue binds its path/hash.
        operational_ini = Path(entry.get("operational_ini", ""))
        if not operational_ini.is_file() or sha256(operational_ini) != entry.get("operational_ini_sha256"):
            return fail("active timeframe wide INI missing or hash mismatch", cycle)
        ini_path = operational_ini

    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    param_names = list(parse_evidence.get("ini_optimized_parameters", []))
    if not param_names or any(n not in rows[0] for n in param_names):
        return fail("active CSV does not contain all parser-declared optimized inputs", cycle)
    ranges = parse_ini(ini_path, param_names)
    if set(ranges) != set(param_names):
        return fail("wide INI does not provide every optimized input range", cycle)
    try:
        input_surface_audit = audit_input_surface(
            parse_evidence=parse_evidence, ini_path=ini_path,
            param_names=param_names, cycle=cycle, ea=ea,
        )
    except TimingControlPolicyError as exc:
        return write_rejection(
            cycle=cycle, ea=ea, selected_tf=selected_tf, entry=entry,
            artifacts=artifacts, ini_path=ini_path, csv_path=csv_path,
            gates=gates, qualifying_count=0, invalid_count=0,
            total_rows=len(rows), reason=str(exc),
        )
    except (OSError, ValueError, TypeError, KeyError) as exc:
        return fail(f"Stage 3 input/default/range contract failed: {exc}", cycle)
    input_surface_audit_sha256 = hashlib.sha256(
        json.dumps(input_surface_audit, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest().upper()
    source_domains = parse_evidence.get("source_input_domains")
    attestation_path = Path(entry.get("source_metadata_attestation", ""))
    if not isinstance(source_domains, dict) or not set(param_names).issubset(source_domains):
        if not attestation_path.is_file() or sha256(attestation_path) != entry.get("source_metadata_attestation_sha256"):
            return fail("authenticated source input bounds/steps are missing for Stage 4", cycle)
        attestation = load(attestation_path)
        if (attestation.get("schema_version") != 1 or attestation.get("stage") != 3
                or attestation.get("cycle_id") != cycle or attestation.get("ea") != ea
                or attestation.get("timeframe") != selected_tf
                or attestation.get("source") != entry.get("source")
                or attestation.get("source_sha256") != entry.get("source_sha256")
                or attestation.get("ini") != str(ini_path.resolve())
                or attestation.get("ini_sha256") != parse_evidence.get("ini_sha256")):
            return fail("Stage 3 source metadata attestation identity mismatch", cycle)
        source_domains = attestation.get("source_input_domains")
    if not isinstance(source_domains, dict) or not set(param_names).issubset(source_domains):
        return fail("authenticated source input bounds/steps are missing for Stage 4", cycle)
    # Stage 1 domains and steps are range estimates. Stage 3 owns the signed
    # wide search surface, so requiring its bounds/step to equal Stage 1 source
    # comments would reject the deliberate 15-20 interval grid before Stage 4
    # can narrow it. The Stage 3 parse evidence binds the exact INI and source;
    # Stage 4 constrains every selected row to that authenticated INI below.
    for name in param_names:
        try:
            spec = ranges[name]
            values = (float(spec["start"]), float(spec["step"]), float(spec["stop"]))
            if not all(math.isfinite(value) for value in values) or values[1] <= 0 or values[0] >= values[2]:
                raise ValueError("non-finite or unusable wide range")
            metadata = source_domains[name]
            domain = (float(metadata["min"]), float(metadata["max"]), float(metadata["step"]))
            if not all(math.isfinite(value) for value in domain) or domain[0] >= domain[1] or domain[2] <= 0:
                raise ValueError("authenticated source-domain metadata is unusable")
        except (KeyError, TypeError, ValueError) as exc:
            return fail(f"authenticated Stage 3/source input metadata invalid for {name}: {exc}", cycle)

    months = float(stage3["timeframes"][selected_tf]["test_months"])
    gates = stage3["gates"]
    qualifying, invalid = [], 0
    for row in rows:
        profit, cf, trades = finite(row, "Profit"), finite(row, "CustomFitness"), finite(row, "Trades")
        eqdd, margin = finite(row, "EquityDrawdownPercent"), finite(row, "MarginLevel")
        if None in (profit, cf, trades, eqdd, margin) or profit <= gates["profit_min_exclusive"] or cf < gates["cf_min"] or trades / months < gates["tpm_min"] or eqdd > gates["drawdown_max"] or margin < gates["min_margin_level"]:
            continue
        vals = {}
        ok = True
        for name, spec in ranges.items():
            value = finite(row, name)
            if value is None or value < spec["start"] - 1e-9 or value > spec["stop"] + 1e-9:
                ok = False; break
            runtime_bounds = input_surface_audit["optimized_inputs"][name].get("source_runtime_bounds")
            if isinstance(runtime_bounds, dict):
                runtime_min, runtime_max = runtime_bounds.get("min"), runtime_bounds.get("max")
                if ((runtime_min is not None and value < float(runtime_min) - 1e-9) or
                        (runtime_max is not None and value > float(runtime_max) + 1e-9)):
                    ok = False; break
            vals[name] = value
        if not ok:
            invalid += 1; continue
        vals.update({"minato_score": cf, "min_margin_pct": margin, "valid": True})
        qualifying.append(vals)
    if not qualifying:
        return write_rejection(
            cycle=cycle, ea=ea, selected_tf=selected_tf, entry=entry,
            artifacts=artifacts, ini_path=ini_path, csv_path=csv_path,
            gates=gates, qualifying_count=len(qualifying), invalid_count=invalid,
            total_rows=len(rows),
            reason=((f"Reject Stage 4: {invalid} performance-qualified rows are outside the authenticated INI/source surface; "
                     "the optimization CSV and its bound input contract are mismatched.")
                    if invalid and not qualifying else
                    f"Reject Stage 4.5: only {len(qualifying)} qualifying {selected_tf} records. "
                    "Verified wide-opt evidence does not provide a robust candidate cluster."))
    TOP_STAGE3_PASSES = 200
    if len(qualifying) < TOP_STAGE3_PASSES:
        return write_rejection(
            cycle=cycle, ea=ea, selected_tf=selected_tf, entry=entry,
            artifacts=artifacts, ini_path=ini_path, csv_path=csv_path,
            gates=gates, qualifying_count=len(qualifying), invalid_count=invalid,
            total_rows=len(rows),
            reason=f"Reject Stage 4: selected timeframe has only {len(qualifying)} qualifying passes; {TOP_STAGE3_PASSES} are required for convergence.")
    selection = select_top_records(qualifying, param_names, limit=TOP_STAGE3_PASSES)
    best_row = max(qualifying, key=lambda row: float(row["minato_score"]))
    input_actions, per_input = {}, {}
    compact_ea = re.sub(r"[^a-z0-9]", "", ea.casefold())
    is_gjr_garch = "gjr" in compact_ea and "garch" in compact_ea
    for name in param_names:
        spec = ranges[name]
        gjr_base_lot = is_gjr_garch and name == "InpBaseLot"
        # Reuse the same semantic movement bounds when fitting the emitted grid.
        # Stage 1 search estimates are not hard domains, but safety floors such
        # as positive ATR periods still apply to the refinement interval.
        movement_bounds = input_movement_bounds(
            name, spec["start"], spec["stop"],
            domain=(float(source_domains[name]["min"]),
                    float(source_domains[name]["max"]))
            if source_domains[name].get("basis") == "source_declared_domain"
            else None,
            safety_bounds=input_surface_audit["optimized_inputs"][name].get("source_runtime_bounds"),
        )
        if gjr_base_lot:
            movement_bounds = (0.01, 0.20)
        action = evaluate_refinement_range(
            start=spec["start"], stop=spec["stop"], step=spec["step"],
            cluster=selection["input_clusters"][name], centered_streak=0,
            broad_run=True, allow_first_pass_narrow=True,
            movement_bounds=movement_bounds,
        )
        action["narrow_runs_completed"] = 0
        action["wide_range"] = {"start": spec["start"], "step": spec["step"], "stop": spec["stop"], "cluster_min": min(selection["input_clusters"][name]), "cluster_max": max(selection["input_clusters"][name]), "cluster_count": len(selection["input_clusters"][name])}
        input_type = str(input_surface_audit["optimized_inputs"][name].get("type", "double"))
        reduced_step = 0.01 if gjr_base_lot else refined_step(spec["step"], input_type=input_type)
        fit_domain_min, fit_domain_max = movement_bounds
        if name.startswith("InpRegime_") and source_domains[name].get("basis") != "source_declared_domain":
            # Keep the semantic multiplier floor while preserving the existing
            # ability to move its upper edge beyond the Stage 1 search estimate.
            fit_domain_max = None
        if gjr_base_lot:
            # Stage 4 retains the authenticated ETH grid. Higher sizes are
            # considered only by Stage 4.5 after margin-qualified results.
            action.update({"action": "keep_broad", "reason":
                           "retain the Stage 2 derived GJR ETH base-lot grid until Stage 4.5 margin evidence"})
            fitted = {"start": 0.01, "step": 0.01, "stop": 0.20}
        else:
            try:
                fitted = fit_refinement_grid(
                    # Fit from the complete authenticated current search window.
                    # evaluate_refinement_range's action bounds are a provisional
                    # cluster band and can be too narrow for the required 15-20
                    # interval MT5 grid, even though the original window can fit it.
                    start=spec["start"], stop=spec["stop"], step=reduced_step,
                    cluster=selection["input_clusters"][name], input_type=input_type,
                    action=action["action"],
                    domain_min=fit_domain_min,
                    domain_max=fit_domain_max,
                )
            except (TypeError, ValueError) as exc:
                return fail(f"cannot fit Stage 4.5 15-20 interval grid for {name}: {exc}", cycle)
        action.update(fitted)
        action.update({"next_start": fitted["start"], "next_step": fitted["step"],
                       "next_stop": fitted["stop"], "input_type": input_type,
                       "source_runtime_bounds": input_surface_audit["optimized_inputs"][name].get("source_runtime_bounds"),
                       "cluster_min": min(selection["input_clusters"][name]),
                       "cluster_max": max(selection["input_clusters"][name]),
                       "cluster_count": len(selection["input_clusters"][name]),
                       "source_metadata": source_domains[name],
                       "lineage": lineage})
        input_actions[name] = action
        per_input[name] = {"start": spec["start"], "step": spec["step"], "stop": spec["stop"], "next_start": fitted["start"], "next_stop": fitted["stop"], "next_step": fitted["step"], "cluster_count": len(selection["input_clusters"][name]), "cluster_min": min(selection["input_clusters"][name]), "cluster_max": max(selection["input_clusters"][name]), "action": action["action"], "direction": action.get("cluster_position", "unknown"), "reason": action.get("reason", ""), "source_metadata": source_domains[name], "input_type": input_type, "lineage": lineage, "centered_streak": action.get("centered_streak", 0), "narrow_runs_completed": 0}

    # The next INI must still contain the best qualifying Stage 3 result.
    for name, spec in per_input.items():
        best_value = float(best_row[name])
        if best_value < spec["next_start"] - 1e-9 or best_value > spec["next_stop"] + 1e-9:
            return fail(f"convergence plan excludes best Stage 3 {name}={best_value}", cycle)

    prefix = STATE / "decisions" / f"{cycle}.stage4"
    decision_path = prefix.with_suffix(".decision.json")
    verification_path = STATE / "verification" / f"{cycle}.stage4.verification.json"
    event_path = STATE / "events" / f"{cycle}.stage4.complete.json"
    handoff_path = STATE / "handoffs" / "Stage4_v2.json"
    stage3_artifacts = {}
    for tf, artifact in artifacts.items():
        summary = {}
        for key in ("parse_evidence", "csv", "opt", "compute_evidence"):
            value = artifact.get(key)
            if value:
                summary[key] = value
                summary[key + "_sha256"] = sha256(Path(value))
        stage3_artifacts[tf] = summary
    decision = {"schema_version": 2, "cycle_id": cycle, "lineage": lineage, "stage": 4, "ea": ea, "selected_tf": selected_tf, "timeframe_selection": {"candidates": eligible_tfs, "selected_by": "custom_score_after_guards", "scores": {tf: tf_custom_score(tf) for tf in eligible_tfs}, "queue_tf": queue_tf}, "policy_source": "state/kcos_stage45_workflow_v2.py", "stage3_decision_sha256": sha256(Path(entry["decision"])), "stage3_verification_sha256": sha256(Path(entry["verification"])), "stage3_artifacts": stage3_artifacts, "wide_ini": str(ini_path), "wide_ini_sha256": sha256(ini_path), "input_surface_audit": input_surface_audit, "input_surface_audit_sha256": input_surface_audit_sha256, "csv": str(csv_path), "csv_sha256": sha256(csv_path), "gates": REQUIRED_GATES, "qualifying_row_count": len(qualifying), "invalid_or_out_of_domain_count": invalid, "top_stage3_pass_limit": TOP_STAGE3_PASSES, "valid_record_count": selection["valid_record_count"], "selected_record_count": selection["selected_record_count"], "cutoff_effective_minato_score": selection["cutoff_effective_minato_score"], "best_stage3_minato_score": float(best_row["minato_score"]), "best_stage3_inputs": {name: float(best_row[name]) for name in param_names}, "best_stage3_in_next_range": True, "input_actions": per_input, "narrow_runs_by_input": {n: 0 for n in param_names}, "stage5_transition": "wait_for_stage4_review", "verdict": "STAGE4_PLANNED", "reason": "Selected highest custom-score timeframe that passed all guards; convergence calculated from its top 200 Stage 3 passes and verified to contain the best Stage 3 result.", "decided_utc": utc_now()}
    atomic_json(decision_path, decision)
    planned_ranges = {
        name: {"start": action["next_start"], "step": action["next_step"],
               "stop": action["next_stop"]}
        for name, action in per_input.items()
    }
    range_plan_failures = validate_input_range_plan(per_input, planned_ranges)
    checks = {"identity": decision["cycle_id"] == cycle and decision["ea"] == ea and decision["stage"] == 4, "stage3_hashes": decision["stage3_decision_sha256"] == entry["decision_sha256"] and decision["stage3_verification_sha256"] == entry["verification_sha256"], "timeframe_evidence": all(Path(a["csv"]).is_file() and sha256(Path(a["csv"])) == a["csv_sha256"] for a in decision["stage3_artifacts"].values() if a.get("csv")), "csv_hashes": sha256(csv_path) == decision["csv_sha256"], "ini_hash": sha256(ini_path) == decision["wide_ini_sha256"], "input_surface_audit": decision["input_surface_audit_sha256"] == hashlib.sha256(json.dumps(decision["input_surface_audit"], sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest().upper() and set(decision["input_surface_audit"]["optimized_inputs"]) == set(param_names), "policy": decision["policy_source"].endswith("kcos_stage45_workflow_v2.py"), "input_actions": set(decision["input_actions"]) == set(param_names), "range_plan": not range_plan_failures, "qualified_records": decision["qualifying_row_count"] > 0}
    verification = {"schema_version": 2, "cycle_id": cycle, "stage": 4, "decision": str(decision_path), "decision_sha256": sha256(decision_path), "passed": all(checks.values()), "checks": checks, "verified_utc": utc_now()}
    atomic_json(verification_path, verification)
    event = {"schema_version": 2, "event_id": f"{cycle}:stage4:complete", "cycle_id": cycle, "stage": 4, "status": "complete", "verdict": decision["verdict"], "ea": ea, "next_stage": "stage4.5_pending", "decision": str(decision_path), "decision_sha256": sha256(decision_path), "verification": str(verification_path), "verification_sha256": sha256(verification_path), "emitted_utc": utc_now()}
    atomic_json(event_path, event)
    handoff = {"schema_version": 2, "cycle_id": cycle, "stage": 4, "ea": ea, "selected_tf": selected_tf, "decision": str(decision_path), "decision_sha256": sha256(decision_path), "verification": str(verification_path), "verification_sha256": sha256(verification_path), "event": str(event_path), "event_sha256": sha256(event_path), "narrow_opt_pending": True, "current_stage": "stage4_complete_held", "next_action": "inspect_stage4_before_narrow_opt", "allowed_stages": completion_allowed_stages, "hold_after_stage": completion_hold_after_stage, "narrow_runs_by_input": {n: 0 for n in param_names}, "prepared_utc": utc_now()}
    atomic_json(handoff_path, handoff)
    queue = load(QUEUE_FILE)
    match = next((e for e in queue["entries"] if e.get("cycle_id") == cycle and e.get("ea") == ea and e.get("status") == "verified_pending"), None)
    if match is None:
        return fail("active queue entry changed before atomic update", cycle)
    match.update({"stage4_decision": str(decision_path), "stage4_decision_sha256": sha256(decision_path), "stage4_verification": str(verification_path), "stage4_verification_sha256": sha256(verification_path), "stage4_event": str(event_path), "stage4_event_sha256": sha256(event_path), "status": "stage4_pending_narrow_opt", "updated_utc": utc_now()})
    queue["updated_utc"] = utc_now(); atomic_json(QUEUE_FILE, queue)
    state = load(STATE_FILE)
    if state.get("cycle_id") != cycle or state.get("current_ea") != ea or state.get("stage_status") != "stage4_pending":
        return fail("canonical state changed before update", cycle)
    state.update({"current_stage": "stage4_complete_held", "next_action": "inspect_stage4_before_narrow_opt", "stage_status": "stage4_complete_held", "allowed_stages": completion_allowed_stages, "hold_after_stage": completion_hold_after_stage, "updated_utc": utc_now(), "last_stage4_decision": str(decision_path), "last_stage4_verification": str(verification_path), "last_stage4_event": str(event_path), "last_stage4_handoff": str(handoff_path), "verdict": decision["verdict"], "passed": verification["passed"], "event_type": "stage4_complete",
        # Stage 4 is the boundary between cycles for the Stage 4.5 counter.
        # Reset it here so stale run-3 state from the prior EA cannot make a
        # newly verified cycle skip its first and second narrow optimizations.
        "narrow_run_number": 1, "stage45_run_number": 0,
        "narrow_opt_decision": None, "narrow_opt_event": None,
        "last_stage45_decision": None, "last_stage45_verification": None,
        "last_stage45_event": None, "stage45_best_score": None})
    atomic_json(STATE_FILE, state)
    print(json.dumps({"status":"completed","cycle_id":cycle,"ea":ea,"selected_tf":selected_tf,"qualifying":len(qualifying),"selected":selection["selected_record_count"],"cutoff":selection["cutoff_effective_minato_score"],"decision":str(decision_path),"verification_passed":verification["passed"],"queue_status":"stage4_pending_narrow_opt","canonical_stage_status":state["stage_status"]}, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
