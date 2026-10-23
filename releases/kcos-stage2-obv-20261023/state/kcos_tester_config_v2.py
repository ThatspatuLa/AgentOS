"""Authoritative Stage 1/2 tester configuration contract.

Stage 3 must consume this contract; it must not derive dates or optimization
ranges from an independently supplied template.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import copy
from pathlib import Path
from datetime import datetime
from typing import Any


CONFIG_SCHEMA_VERSION = 1
REQUIRED_TIMEFRAMES = ["M5", "M15", "H1"]
REGIME_GRID_START = 0.0
REGIME_GRID_STEP = 0.25
REGIME_GRID_STOP = 5.0
GJR_BASE_LOT_GRID_START = 0.01
GJR_BASE_LOT_GRID_STEP = 0.01
GJR_BASE_LOT_GRID_STOP = 0.20
# Tester controls are not EA inputs and must never be inferred as approved
# optimization dimensions from a template.  Lots is fixed by the upstream
# Stage 2 contract; optimizing it here would bypass that contract.
_FIXED_FIELDS = {
    "InpMinatoExpectedScoreVersion", "InpMinatoTestStart", "InpMinatoTestEnd",
    "Lots", "SlippagePts", "InpSpreadPts", "InpCommissionPct",
}


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def config_sha256(config: dict) -> str:
    return hashlib.sha256(canonical_json(config)).hexdigest().upper()


def validate_optimized_source_usage(config: dict, source_text: str) -> dict:
    """Reject optimizer dimensions that are only declarations in the EA.

    A parameter declared as ``input`` but never referenced by executable
    source is metadata-only. It must not enter the Stage 2/3 optimization
    surface: MT5 will omit it from the .opt header and the run becomes a
    silently reduced search.
    """
    optimized = [name for name, spec in config.get("inputs", {}).items()
                 if str(spec.get("optimize", "N")).upper() == "Y"]
    executable = re.sub(r"/\*.*?\*/", "", source_text, flags=re.S)
    executable = re.sub(r"//[^\r\n]*", "", executable)
    metadata_only = [name for name in optimized if len(re.findall(r"\b" + re.escape(name) + r"\b", executable)) <= 1]
    # Repair the Stage 1 surface instead of rejecting the candidate. A
    # declaration-only input cannot be optimized safely because MT5 will not
    # emit it in the .opt header; keep its default fixed and preserve the
    # exact reason in provenance for later audit.
    for name in metadata_only:
        spec = config["inputs"][name]
        spec["optimize"] = "N"
        spec["domain_provenance"] = {
            "method": "stage2_metadata_only_repair",
            "basis": "input declaration has no executable references",
            "previous_optimization": {"start": spec.get("start"), "step": spec.get("step"), "stop": spec.get("stop")},
        }
    return {"optimized": [name for name in optimized if name not in metadata_only], "metadata_only": metadata_only, "repaired": metadata_only}


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def validate_config(config: dict) -> dict:
    _require(isinstance(config, dict), "tester_config must be an object")
    _require(config.get("schema_version") == CONFIG_SCHEMA_VERSION,
             "tester_config schema_version must be 1")
    _require(config.get("symbol") == "ETHUSD", "tester_config symbol must be ETHUSD")
    _require(config.get("timeframes") == REQUIRED_TIMEFRAMES,
             "tester_config timeframes must be M5, M15, H1")
    for field in ("from_date", "to_date"):
        value = config.get(field)
        _require(isinstance(value, str), f"tester_config {field} is required")
        try:
            datetime.strptime(value, "%Y.%m.%d")
        except ValueError as exc:
            raise ValueError(f"tester_config {field} must use YYYY.MM.DD") from exc
    _require(config["from_date"] < config["to_date"],
             "tester_config from_date must precede to_date")
    inputs = config.get("inputs")
    _require(isinstance(inputs, dict) and inputs, "tester_config inputs are required")
    for name, spec in inputs.items():
        _require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) is not None,
                 f"invalid tester_config input name: {name}")
        _require(isinstance(spec, dict), f"tester_config input {name} must be an object")
        for key in ("default", "start", "step", "stop", "optimize"):
            _require(key in spec, f"tester_config input {name} missing {key}")
        _require(spec["optimize"] in ("Y", "N"),
                 f"tester_config input {name} optimize must be Y or N")
        if spec["optimize"] == "Y":
            try:
                start, step, stop = float(spec["start"]), float(spec["step"]), float(spec["stop"])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"tester_config input {name} has non-numeric range") from exc
            # MT5 optimization surfaces are emitted in ascending numeric
            # order.  A descending tuple is ambiguous (and is rejected by
            # the tester even when the source comment lists a descending
            # semantic threshold, such as Williams %R).  Source-declared
            # domains are normalized separately below; the approved runtime
            # contract must still be deterministic.
            _require(step > 0 and start < stop,
                     f"tester_config input {name} has unusable optimization range")
    constraints = config.get("input_constraints", [])
    _require(isinstance(constraints, list), "tester_config input_constraints must be a list")
    for index, constraint in enumerate(constraints):
        _require(isinstance(constraint, dict), f"tester_config input constraint #{index + 1} must be an object")
        label = str(constraint.get("id", f"constraint_{index + 1}"))
        coefficients = constraint.get("coefficients")
        operator = constraint.get("operator")
        _require(isinstance(coefficients, dict) and bool(coefficients),
                 f"tester_config input constraint {label} requires coefficients")
        _require(operator in {"<=", "<", ">=", ">"},
                 f"tester_config input constraint {label} has unsupported operator {operator!r}")
        try:
            limit = float(constraint["limit"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"tester_config input constraint {label} requires a numeric limit") from exc
        _require(math.isfinite(limit), f"tester_config input constraint {label} limit must be finite")
        default_value = 0.0
        for input_name, raw_coefficient in coefficients.items():
            _require(input_name in inputs,
                     f"tester_config input constraint {label} references unknown input {input_name}")
            try:
                coefficient = float(raw_coefficient)
                default = float(inputs[input_name]["default"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"tester_config input constraint {label} requires numeric inputs and coefficients") from exc
            _require(math.isfinite(coefficient) and math.isfinite(default),
                     f"tester_config input constraint {label} contains non-finite values")
            default_value += coefficient * default
        default_ok = {
            "<=": default_value <= limit + 1e-12,
            "<": default_value < limit - 1e-12,
            ">=": default_value >= limit - 1e-12,
            ">": default_value > limit + 1e-12,
        }[operator]
        _require(default_ok,
                 f"tester_config input constraint {label} rejects EA defaults: {default_value:g} {operator} {limit:g}")
    return config


def resolve_input_spec(spec: dict, *, shared_sources: dict | None = None,
                       source_text: str | None = None) -> tuple[dict, dict]:
    """Derive a runtime view without editing authenticated Stage 1 evidence.

    MQL declarations own types. Approved experiment values own runtime
    overrides. JSON string encoding does not make an input an MQL string.
    """
    from kcos_input_contract_v2 import discover_inputs, reconcile_spec, assert_no_shared_input_shadowing
    from kcos_runtime_paths_v2 import assert_safe_root, mt5_data_root
    effective = copy.deepcopy(spec)
    config = effective.get("tester_config")
    _require(isinstance(config, dict) and isinstance(config.get("inputs"), dict),
             "tester_config.inputs must be an object")
    if shared_sources is None:
        uses_minato = (any(name.startswith("InpMinato") for name in config["inputs"])
                       or spec.get("research_mode") == "research_new"
                       or source_text and re.search(r'#\s*include\s*[<"]minato_base_score_v2\.mqh[>"]', source_text))
        if not uses_minato:
            return effective, {"schema_version": 1, "status": "NO_SHARED_INPUTS", "inputs": {}, "changes": []}
        approval_path = assert_safe_root() / "state/contracts/minato_base_score_approved_v2.json"
        include = mt5_data_root() / "MQL5/Include/minato_base_score_v2.mqh"
        try:
            approval = json.loads(approval_path.read_text(encoding="utf-8-sig"))
            raw = include.read_bytes()
        except (OSError, ValueError) as exc:
            raise ValueError(f"INPUT_CONTRACT_FAILURE: cannot load approved shared include: {exc}") from exc
        _require(isinstance(approval, dict), "INPUT_CONTRACT_FAILURE: include approval must be an object")
        expected = approval.get("sha256", approval.get("score_include_sha256"))
        approved = approval.get("approved", bool(approval.get("score_include_sha256")))
        _require(approved is True and hashlib.sha256(raw).hexdigest().upper() == str(expected).upper(),
                 "INPUT_CONTRACT_FAILURE: shared include is not authenticated")
        shared_sources = {str(include.resolve()): raw.decode("utf-8-sig")}
    shared = discover_inputs(shared_sources)
    declarations = shared["inputs"]
    # Semantic bindings are the existing KCOS scoring policy; types and
    # ownership come from the approved declarations rather than this map.
    candidates = {
        "InpMinatoTestStart": config.get("from_date"),
        "InpMinatoTestEnd": config.get("to_date"),
        "InpMinatoExpectedScoreVersion": declarations.get("InpMinatoExpectedScoreVersion", {}).get("source_default"),
    }
    bindings = {}
    added_inputs = []
    for name, value in candidates.items():
        if name not in declarations:
            continue
        _require(value is not None, f"INPUT_CONTRACT_FAILURE: runtime binding missing for {name}")
        bindings[name] = value
        if name not in config["inputs"]:
            added_inputs.append(name)
            config["inputs"][name] = {"type": declarations[name]["mql_type"], "default": value,
                                      "start": value, "step": 0, "stop": value, "optimize": "N"}
    if source_text is not None:
        assert_no_shared_input_shadowing(source_text, shared)
    effective, report = reconcile_spec(effective, shared, bindings)
    report["added_inputs"] = added_inputs
    report["changes"].extend({"input": name, "field": "tester_config.inputs", "before": None,
                              "after": effective["tester_config"]["inputs"][name], "reason": "required_runtime_binding"}
                             for name in added_inputs)
    report["shared_inputs"] = copy.deepcopy(declarations)
    # Remove contradictory descriptions only for include-owned parameters.
    strategy = effective.get("strategy")
    if isinstance(strategy, dict) and isinstance(strategy.get("parameters"), list):
        rows = []
        for row in strategy["parameters"]:
            match = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*=", row) if isinstance(row, str) else None
            if match and match[1] in declarations and match[1] in effective["tester_config"]["inputs"]:
                name = match[1]
                value = effective["tester_config"]["inputs"][name]["default"]
                replacement = f"{name} = {value} (include-owned {declarations[name]['mql_type']}; tester runtime override)"
                if replacement != row:
                    report["changes"].append({"input": name, "field": "strategy.parameters", "before": row,
                                              "after": replacement, "reason": "shared_input_ownership"})
                row = replacement
            rows.append(row)
        strategy["parameters"] = rows
    requirements = effective.get("stage2_requirements")
    if isinstance(requirements, dict) and "expected_minato_score_version" in requirements and "InpMinatoExpectedScoreVersion" in bindings:
        before = requirements["expected_minato_score_version"]
        requirements["expected_minato_score_version"] = bindings["InpMinatoExpectedScoreVersion"]
        if before != requirements["expected_minato_score_version"]:
            report["changes"].append({"input": "InpMinatoExpectedScoreVersion", "field": "stage2_requirements.expected_minato_score_version",
                                      "before": before, "after": requirements["expected_minato_score_version"], "reason": "shared_input_ownership"})
    return effective, report


def config_from_spec(spec: dict, *, source_text: str | None = None) -> dict:
    effective, _ = resolve_input_spec(spec, source_text=source_text)
    config = effective.get("tester_config")
    restore_source_reconciled_domains(config)
    apply_regime_grid_policy(config)
    validate_config(config)
    return config


def is_timing_control_input(name: str, spec: dict | None = None) -> str | None:
    """Classify user-configured time-stop and cooldown controls by name/label.

    These are operational throttles, not strategy search dimensions. Keep the
    matcher generic across EA families while avoiding unrelated period inputs.
    """
    item = spec if isinstance(spec, dict) else {}
    label = " ".join(str(item.get(key, "")) for key in
                     ("description", "label", "title", "role", "notes"))
    text = f"{name} {label}".casefold()
    compact = re.sub(r"[^a-z0-9]", "", text)
    if ("cooldown" in compact or "coolingdown" in compact or
            re.search(r"(?:min)?barsbetween(?:trades|entries|orders)", compact)):
        return "cooldown"
    if ("timestop" in compact or "maxtradebars" in compact or
            "maxholdingbars" in compact or "maxbarsintrade" in compact or
            "maxholdingperiod" in compact or "maximumholdingperiod" in compact or
            re.search(r"(?:trade|position|holding)(?:duration|timeout|stop)", compact)):
        return "time_stop"
    return None


def stage2_config_from_spec(spec: dict, *, source_text: str | None = None) -> dict:
    """Derive the tester contract with cooldown controls omitted entirely.

    Stage 1 evidence is not mutated. Time-stop controls remain fixed at zero;
    cooldown inputs and their constraint terms are excluded downstream.
    """
    config = config_from_spec(spec, source_text=source_text)
    regime_names = []
    if source_text:
        from kcos_optimization_contract_v2 import derive_contract
        declared_regime_names = derive_contract(source_text)["declared_regime_inputs"]
        regime_names = sorted(name for name in declared_regime_names if name in config["inputs"])
        if len(regime_names) >= 12:
            # Stage 2 implements the declared 12-field regime surface. Bind
            # those controls before source generation so the verifier and
            # publisher use the same optimizer contract after the code uses it.
            for name in regime_names:
                config["inputs"][name]["optimize"] = "Y"
            config = reconcile_source_comment_domains(config, source_text)["config"]
            config.setdefault("stage2_execution_policy", {})["regime_optimization"] = {
                "method": "stage2_optimizes_declared_regime_controls",
                "inputs": regime_names,
                "stage1_artifact_mutated": False,
            }
    cooldown_names = {name for name, item in config["inputs"].items()
                      if is_timing_control_input(name, item) == "cooldown"}
    time_stop_names = {name for name, item in config["inputs"].items()
                       if is_timing_control_input(name, item) == "time_stop"}
    timing_names = cooldown_names | time_stop_names
    removed_constraints = []
    constraints = config.get("input_constraints", [])
    if isinstance(constraints, list):
        kept_constraints = []
        for rule in constraints:
            coefficients = rule.get("coefficients", {}) if isinstance(rule, dict) else {}
            if isinstance(coefficients, dict) and timing_names.intersection(coefficients):
                rewritten = copy.deepcopy(rule)
                rewritten["coefficients"] = {
                    name: value for name, value in coefficients.items()
                    if name not in timing_names
                }
                if rewritten["coefficients"]:
                    kept_constraints.append(rewritten)
                else:
                    removed_constraints.append(rule)
            else:
                kept_constraints.append(rule)
        config["input_constraints"] = kept_constraints
    for name in cooldown_names:
        config["inputs"].pop(name, None)
    if timing_names:
        config["timing_control_policy"] = {
            "method": "kcos_no_cooldown_fixed_time_stop",
            "disabled_inputs": sorted(time_stop_names),
            "removed_cooldown_inputs": sorted(cooldown_names),
            "removed_input_constraints": removed_constraints,
        }
    for name in time_stop_names:
        item = config["inputs"][name]
        before = {key: item.get(key) for key in
                  ("default", "start", "step", "stop", "optimize")}
        item.update({"default": 0, "start": 0, "step": 0, "stop": 0,
                     "optimize": "N"})
        item["domain_provenance"] = {
            "method": "kcos_fixed_timing_control",
            "control_kind": "time_stop",
            "reason": "KCOS fixes time-stop controls at zero and omits cooldown controls",
            "previous_stage1_contract": before,
        }
    identity = " ".join(str(value or "") for value in (
        spec.get("ea"), spec.get("candidate_ea"),
        (spec.get("tester_config") or {}).get("ea"),
    )).casefold()
    compact_identity = re.sub(r"[^a-z0-9]", "", identity)
    if "gjr" in compact_identity and "garch" in compact_identity:
        previous_base_lot = copy.deepcopy(config["inputs"].get("InpBaseLot"))
        config["inputs"]["InpBaseLot"] = {
            "type": "double", "default": 0.01,
            "start": GJR_BASE_LOT_GRID_START,
            "step": GJR_BASE_LOT_GRID_STEP,
            "stop": GJR_BASE_LOT_GRID_STOP,
            "optimize": "Y",
            "description": "Stage 2 derived base position size optimized by Stage 3 before regime multiplier",
            "domain_provenance": {
                "method": "kcos_stage2_gjr_base_lot",
                "basis": "Stage 2 derived bounded base-size grid; Stage 1 remains unchanged",
                "grid": [GJR_BASE_LOT_GRID_START, GJR_BASE_LOT_GRID_STEP, GJR_BASE_LOT_GRID_STOP],
                "source_default": 0.01,
                "stage1_artifact_mutated": False,
                "previous_stage1_surface": previous_base_lot,
            },
        }
        config.setdefault("stage2_execution_policy", {})["gjr_base_lot"] = {
            "input": "InpBaseLot", "default": 0.01,
            "optimization": "stage3_grid",
            "grid": [GJR_BASE_LOT_GRID_START, GJR_BASE_LOT_GRID_STEP, GJR_BASE_LOT_GRID_STOP],
            "method": "optimized_base_clamped_to_symbol_volume_min_then_regime_scaled",
            "stage1_artifact_mutated": False,
        }
    validate_config(config)
    return config


def stage2_spec_from_spec(spec: dict, *, source_text: str | None = None) -> dict:
    """Return a derived Stage 2 source contract without mutating Stage 1 evidence."""
    effective = copy.deepcopy(spec)
    config = stage2_config_from_spec(effective, source_text=source_text)
    effective["tester_config"] = config
    policy = config.get("timing_control_policy", {})
    timing_names = set(policy.get("disabled_inputs", []))
    cooldown_names = set(policy.get("removed_cooldown_inputs", []))
    strategy = effective.get("strategy")
    if isinstance(strategy, dict) and isinstance(strategy.get("parameters"), list):
        def parameter_name(row):
            if isinstance(row, dict):
                return row.get("name")
            match = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*=", str(row))
            return match.group(1) if match else None
        strategy["parameters"] = [row for row in strategy["parameters"]
                                   if parameter_name(row) not in cooldown_names]
        for row in strategy["parameters"]:
            if isinstance(row, dict) and row.get("name") in timing_names:
                row["default"] = 0
    strategy_contract = strategy.get("execution_contract") if isinstance(strategy, dict) else None
    if isinstance(strategy_contract, dict):
        bindings = strategy_contract.get("input_bindings")
        if isinstance(bindings, dict):
            for name in cooldown_names:
                bindings.pop(name, None)
    effective["stage2_runtime_policy"] = {
        "timing_controls": {
            name: {"kind": is_timing_control_input(name, config["inputs"][name]),
                   "default": 0, "start": 0, "step": 0, "stop": 0,
                   "optimize": "N"}
            for name in sorted(timing_names)
        },
        "excluded_cooldown_controls": sorted(cooldown_names),
    }
    if "InpBaseLot" in config.get("inputs", {}) and config["inputs"]["InpBaseLot"].get("domain_provenance", {}).get("method") == "kcos_stage2_gjr_base_lot":
        effective["stage2_runtime_policy"]["position_sizing"] = {
            "InpBaseLot": {
                "default": 0.01,
                "start": GJR_BASE_LOT_GRID_START,
                "step": GJR_BASE_LOT_GRID_STEP,
                "stop": GJR_BASE_LOT_GRID_STOP,
                "optimization": "stage3_grid",
                "method": "optimized_base_clamped_to_symbol_volume_min_then_regime_scaled",
                "provenance": "kcos_stage2_gjr_base_lot",
            }
        }
    return effective


def stage3_config_from_spec(spec: dict, *, source_text: str | None = None) -> dict:
    """Return Stage 3 inputs with timing controls fixed and regime lots independent."""
    config = stage2_config_from_spec(spec, source_text=source_text)
    lot_order_pairs = (
        ("InpRegime_Greed_Lot_Mult", "InpRegime_Neutral_Lot_Mult"),
        ("InpRegime_Neutral_Lot_Mult", "InpRegime_Fear_Lot_Mult"),
        ("InpRegime_Fear_Lot_Mult", "InpRegime_ExtFear_Lot_Mult"),
    )
    removed_lot_order_constraints = []
    kept_constraints = []
    for rule in config.get("input_constraints", []):
        coefficients = rule.get("coefficients", {}) if isinstance(rule, dict) else {}
        try:
            normalized = {name: float(value) for name, value in coefficients.items()}
            limit = float(rule.get("limit")) if isinstance(rule, dict) else float("nan")
        except (TypeError, ValueError):
            normalized, limit = {}, float("nan")
        is_regime_lot_order = (
            isinstance(rule, dict)
            and rule.get("operator") == ">=" and math.isclose(limit, 0.0, abs_tol=1e-12)
            and any(normalized == {higher: 1.0, lower: -1.0}
                    for higher, lower in lot_order_pairs)
        )
        if is_regime_lot_order:
            removed_lot_order_constraints.append(copy.deepcopy(rule))
        else:
            kept_constraints.append(rule)
    config["input_constraints"] = kept_constraints
    if removed_lot_order_constraints:
        config["optimizer_policy_overrides"] = [{
            "policy": "independent_regime_lot_multipliers",
            "reason": "Operator direction: regime lot multipliers have no ordering constraint.",
            "removed_input_constraints": removed_lot_order_constraints,
        }]
    policy = config.get("timing_control_policy")
    if isinstance(policy, dict):
        policy["method"] = "stage3_fixed_timing_control"
    for item in config["inputs"].values():
        provenance = item.get("domain_provenance")
        if isinstance(provenance, dict) and provenance.get("method") == "kcos_fixed_timing_control":
            provenance["method"] = "stage3_fixed_timing_control"
            provenance["previous_stage2_contract"] = provenance.pop("previous_stage1_contract", {})
    return config


def validate_shared_input_surface(text: str, spec: dict) -> None:
    """Authenticate fixed shared runtime assignments at the final INI boundary."""
    from kcos_input_contract_v2 import serialize_value
    effective, report = resolve_input_spec(spec)
    _tester_inputs_section(text)
    rows = {}
    in_inputs = False
    for line in text.splitlines():
        stripped = line.strip()
        section = re.fullmatch(r"\[([^]]+)\]", stripped)
        if section:
            in_inputs = section[1].casefold() == "testerinputs"
        elif in_inputs and stripped and not stripped.startswith(";") and "=" in stripped:
            name, value = (part.strip() for part in stripped.split("=", 1))
            rows.setdefault(name, []).append(value)
    for name, declaration in report.get("inputs", {}).items():
        item = effective["tester_config"]["inputs"][name]
        if item["optimize"] != "N":
            continue
        expected = serialize_value(item["default"], declaration["mql_type"])
        _require(rows.get(name) == [expected],
                 f"INPUT_CONTRACT_FAILURE: [TesterInputs] {name} must occur once with approved fixed value {expected}")


def apply_shared_input_surface(text: str, spec: dict) -> str:
    """Restore include-owned fixed rows after source-only surface rebuilding."""
    effective, report = resolve_input_spec(spec)
    prefix, surface, suffix = _tester_inputs_section(text)
    for name in report.get("inputs", {}):
        item = effective["tester_config"]["inputs"][name]
        if item["optimize"] != "N":
            continue
        row = ini_row(name, item)
        pattern = re.compile(rf"(?im)^[ \t]*{re.escape(name)}[ \t]*=.*$")
        if pattern.search(surface):
            surface = pattern.sub(lambda _: row, surface, count=1)
        else:
            surface = "\n" + row + "\n" + surface.lstrip("\r\n")
    text = prefix + surface + suffix
    validate_shared_input_surface(text, spec)
    return text


def _tester_inputs_section(text: str) -> tuple[str, str, str]:
    sections = list(re.finditer(r"(?im)^[ \t]*\[([^]\r\n]+)\][ \t]*\r?$", text))
    matches = [(index, match) for index, match in enumerate(sections)
               if match[1].casefold() == "testerinputs"]
    _require(len(matches) == 1, "template requires exactly one [TesterInputs] section")
    index, marker = matches[0]
    end = sections[index + 1].start() if index + 1 < len(sections) else len(text)
    return text[:marker.end()], text[marker.end():end], text[end:]


def restore_source_reconciled_domains(config: dict) -> dict:
    """Restore the full authenticated source domain after stale narrowing.

    Stage 1 may record the prior range alongside the source domain.  For wide
    Stage 3, the source domain is authoritative; a previous lower/upper bound
    must never silently narrow it again.
    """
    for name, item in (config.get("inputs", {}) if isinstance(config, dict) else {}).items():
        provenance = item.get("domain_provenance", {}) if isinstance(item, dict) else {}
        domain = provenance.get("source_domain")
        if provenance.get("method") != "stage2_source_reconciliation" or not domain or len(domain) != 3:
            continue
        lo, hi, step = map(float, domain)
        if lo >= hi or step <= 0:
            raise ValueError(f"invalid authenticated source domain for {name}: {domain}")
        item.update({"start": lo, "step": step, "stop": hi})
        provenance = dict(provenance)
        provenance["method"] = "stage2_source_domain_restore"
        provenance["basis"] = "authenticated source domain; stale prior range removed"
        item["domain_provenance"] = provenance
    return config


def apply_regime_grid_policy(config: dict) -> dict:
    """Apply the canonical autonomous regime search surface.

    Regime inputs are a shared KCOS contract, not template-owned values.  The
    policy is applied at every config read so stale Stage 1/template tuples
    cannot re-enter Stage 2/3.  Metadata-only inputs are left for the source
    usage validator to repair to fixed fields.
    """
    inputs = config.get("inputs", {}) if isinstance(config, dict) else {}
    for name, item in inputs.items():
        if not name.startswith("InpRegime_") or not isinstance(item, dict):
            continue
        if item.get("domain_provenance", {}).get("method") == "stage2_metadata_only_repair":
            continue
        previous = {key: item.get(key) for key in ("default", "start", "step", "stop", "optimize")}
        default = float(item.get("default", REGIME_GRID_START))
        item["default"] = min(REGIME_GRID_STOP, max(REGIME_GRID_START, default))
        item.update({"start": REGIME_GRID_START, "step": REGIME_GRID_STEP,
                     "stop": REGIME_GRID_STOP, "optimize": "Y"})
        item["domain_provenance"] = {
            "method": "stage2_regime_grid_policy",
            "basis": "autonomous KCOS regime contract",
            "grid": [REGIME_GRID_START, REGIME_GRID_STEP, REGIME_GRID_STOP],
            "previous_surface": previous,
        }
    return config


def apply_common_sense_domains(config: dict, source_text: str) -> list[str]:
    """Fill unresolved optimized domains after insufficient research.

    The fallback is bounded, deterministic, and provenance-recorded. It uses
    source type/default/name semantics and executable bounds when available;
    explicit source-comment or web-researched domains are never overwritten.
    """
    bounds = {}
    bound_pattern = re.compile(
        r"if\s*\(\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*"
        r"(?P<lop><=|<)\s*(?P<lval>[-+0-9.eE]+)\s*\|\|\s*"
        r"(?P=name)\s*(?P<rop>>=|>)\s*(?P<rval>[-+0-9.eE]+)\s*\)", flags=re.I)
    for m in bound_pattern.finditer(source_text):
        bounds[m.group("name")] = (float(m.group("lval")), float(m.group("rval")))
    decl = re.compile(r"(?m)^\s*input\s+(?P<type>int|long|double|float|bool)\s+"
                      r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<default>[^;]+);")
    declarations = {m.group("name"): (m.group("type").lower(), m.group("default").strip())
                    for m in decl.finditer(source_text)}
    applied = []
    for name, spec in config.get("inputs", {}).items():
        if str(spec.get("optimize", "N")).upper() != "Y":
            continue
        provenance = spec.get("domain_provenance")
        if isinstance(provenance, dict) and provenance.get("method"):
            continue
        if name not in declarations:
            continue
        kind, raw_default = declarations[name]
        if kind == "bool":
            spec["optimize"] = "N"
            spec["domain_provenance"] = {"method": "common_sense", "basis": "boolean inputs are fixed"}
            applied.append(name)
            continue
        try:
            default = float(spec.get("default", raw_default))
        except (TypeError, ValueError):
            continue
        if name in bounds:
            lo, hi = sorted(bounds[name]); start = max(lo, min(hi, default))
            span = hi - lo; step = max(1.0 if kind in {"int", "long"} else span / 20.0, span / 10.0); stop = hi
        else:
            integer = kind in {"int", "long"}
            start = max(2.0, default * 0.5) if integer or "lookback" in name.casefold() or "window" in name.casefold() else default * 0.5
            stop = max(start + (10.0 if integer else 0.1), default * 1.5)
            step = max(1.0 if integer else 0.01, (stop - start) / 10.0)
        if kind in {"int", "long"}:
            start, step, stop = int(round(start)), max(1, int(round(step))), int(round(stop))
        spec.update({"start": start, "step": step, "stop": stop,
                     "domain_provenance": {"method": "common_sense",
                     "basis": "source type + default + input semantics + executable bounds",
                     "research_status": "insufficient_information"}})
        applied.append(name)
    return applied


def validate_config_against_source(config: dict, source_text: str) -> dict:
    """Validate approved optimization ranges against source and Stage 1 contracts.

    A source comment may document the range as ``[lo=...,hi=...,step=...]``.
    When it is absent, the authenticated tester_config itself is authoritative;
    the Stage 2 INI uses that exact range. A present source comment must still
    agree, and executable OnInit bounds remain an additional restriction.
    """
    validate_config(config)
    declarations = {}
    declaration_pattern = re.compile(
        r"(?m)^\s*input\s+[A-Za-z_][A-Za-z0-9_<>]*\s+([A-Za-z_][A-Za-z0-9_]*)\s*=[^;]*;(?P<comment>[^\r\n]*)"
    )
    domain_pattern = re.compile(
        r"\[\s*lo\s*=\s*([-+0-9.eE]+)\s*,\s*hi\s*=\s*([-+0-9.eE]+)\s*,\s*step\s*=\s*([-+0-9.eE]+)\s*\]"
        r"|\[\s*([-+0-9.eE]+)\s*\.\.\s*([-+0-9.eE]+)\s*\]\s*step\s*([-+0-9.eE]+)"
    )
    for match in declaration_pattern.finditer(source_text):
        domain = domain_pattern.search(match.group("comment"))
        if domain:
            values = domain.groups()
            values = values[:3] if values[0] is not None else values[3:]
            declarations[match.group(1)] = tuple(map(float, values))
        else:
            declarations[match.group(1)] = None

    # Executable OnInit bounds outrank stale or overly broad source comments.
    # Example: a comment may advertise hi=105 while the EA rejects values above
    # 100 with INIT_PARAMETERS_INCORRECT.  Such a range must fail before MT5.
    runtime_bounds = {}
    bound_pattern = re.compile(
        r"if\s*\(\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*"
        r"(?P<lop><=|<)\s*(?P<lval>[-+0-9.eE]+)\s*\|\|\s*"
        r"(?P=name)\s*(?P<rop>>=|>)\s*(?P<rval>[-+0-9.eE]+)\s*\)",
        flags=re.I,
    )
    for match in bound_pattern.finditer(source_text):
        lo = float(match.group("lval"))
        hi = float(match.group("rval"))
        if match.group("lop") == "<":
            lo = lo + 0.0
        if match.group("rop") == ">":
            hi = hi - 0.0
        runtime_bounds[match.group("name")] = (lo, hi)

    # Regime controls are executable contract inputs even when scalar/fixed.
    regime_names = sorted(set(re.findall(r"\bInpRegime_[A-Za-z0-9_]+", source_text)))
    if regime_names:
        executable = re.sub(r"/\*.*?\*/", "", source_text, flags=re.S)
        executable = re.sub(r"//[^\r\n]*", "", executable)
        executable = re.sub(r"^\s*input\b[^\r\n]*$", "", executable, flags=re.I | re.M)
        regime_source_defaults = {
            m.group("name"): m.group("default").strip()
            for m in re.finditer(
                r"(?m)^\s*input\s+[A-Za-z_][A-Za-z0-9_<>]*\s+"
                r"(?P<name>InpRegime_[A-Za-z0-9_]*)\s*=\s*(?P<default>[^;]+);",
                source_text,
            )
        }
        def same_default(left, right):
            try:
                return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=1e-12)
            except (TypeError, ValueError):
                return str(left).strip().strip('"').strip("'") == str(right).strip().strip('"').strip("'")
        inactive_are_fixed = all(
            len(re.findall(rf"\b{re.escape(name)}\b", executable)) > 0
            or (name in config.get("inputs", {})
                and config["inputs"][name].get("optimize") == "N"
                and name in regime_source_defaults
                and same_default(config["inputs"][name].get("default"), regime_source_defaults[name]))
            for name in regime_names
        )
        _require(len(regime_names) >= 12 and inactive_are_fixed,
                 "regime inputs are declaration-only in executable source and are not fixed to source defaults")

    checked = 0
    for name, spec in config["inputs"].items():
        if name in _FIXED_FIELDS or spec["optimize"] != "Y":
            continue
        _require(name in declarations, f"approved optimized input {name} is not declared by source")
        domain = declarations[name]
        if domain is None:
            domain = (float(spec["start"]), float(spec["stop"]), float(spec["step"]))
        _require(domain is not None,
                 f"source input {name} has no declared optimization domain")
        declared_lo, declared_hi, declared_step = domain
        start, step, stop = map(float, (spec["start"], spec["step"], spec["stop"]))
        regime_policy = (
            name.startswith("InpRegime_")
            and spec.get("domain_provenance", {}).get("method") == "stage2_regime_grid_policy"
        )
        if regime_policy:
            _require((start, step, stop) == (REGIME_GRID_START, REGIME_GRID_STEP, REGIME_GRID_STOP),
                     f"regime policy surface for {name} is not 0..5 step 0.25")
            checked += 1
            continue
        wide_search = (
            spec.get("domain_provenance", {}).get("method") == "wide_search"
            or config.get("domain_provenance", {}).get("method") == "wide_search"
        )
        if not wide_search and step > 0:
            wide_intervals = (stop - start) / step
            runtime = runtime_bounds.get(name)
            wide_search = (runtime is not None and 15.0 <= wide_intervals <= 20.0
                           and runtime[0] <= start <= stop <= runtime[1])
        if not wide_search:
            # Source comments occasionally express threshold domains in semantic
            # order (e.g. oversold -57..-133). Bounds are numeric here; compare
            # against their normalized envelope while preserving the approved
            # ascending MT5 surface above.
            lo, hi = sorted((declared_lo, declared_hi))
            _require(lo <= start <= stop <= hi,
                     f"approved range for {name} [{start},{stop}] exceeds source domain [{declared_lo},{declared_hi}]")
            _require(step > 0 and step <= (hi - lo),
                     f"approved step for {name} is outside source domain")
            _require(abs(step - abs(declared_step)) < max(abs(declared_step) * 1e-9, 1e-12),
                     f"approved step for {name} {step} disagrees with source step {declared_step}")
        else:
            # `wide_search` is an authenticated Stage 3 surface. The source
            # declaration is a recommendation/default hint; executable runtime
            # bounds below are the safety boundary. Stage 3 owns the wide-grid
            # step selection (normally targeting 15–20 intervals).
            _require(step > 0 and start <= stop,
                     f"wide-search range for {name} is unusable")
        if name in runtime_bounds:
            runtime_lo, runtime_hi = runtime_bounds[name]
            _require(runtime_lo <= start <= stop <= runtime_hi,
                     f"approved range for {name} [{start},{stop}] exceeds executable runtime domain [{runtime_lo},{runtime_hi}]")
        checked += 1
    return {
        "passed": True,
        "source_declared_inputs": len(declarations),
        "domain_checked_inputs": checked,
        "runtime_bounds_checked": sorted(set(runtime_bounds).intersection(config["inputs"])),
    }


def reconcile_source_comment_domains(config: dict, source_text: str) -> dict:
    """Rebind optimized domains to documented source ranges and OnInit bounds.

    This is for existing, authenticated strategies being re-entered into the
    normal queue. It changes tester search metadata only; the MQ5 is untouched.
    Inputs without an explicit source-comment domain are left as approved.
    """
    result = copy.deepcopy(config)
    declaration_pattern = re.compile(
        r"(?m)^\s*input\s+[A-Za-z_][A-Za-z0-9_<>]*\s+([A-Za-z_][A-Za-z0-9_]*)\s*=[^;]*;(?P<comment>[^\r\n]*)"
    )
    domain_pattern = re.compile(
        r"\[\s*lo\s*=\s*([-+0-9.eE]+)\s*,\s*hi\s*=\s*([-+0-9.eE]+)\s*,\s*step\s*=\s*([-+0-9.eE]+)\s*\]"
        r"|\[\s*([-+0-9.eE]+)\s*\.\.\s*([-+0-9.eE]+)\s*\]\s*step\s*([-+0-9.eE]+)"
    )
    declarations = {}
    for match in declaration_pattern.finditer(source_text):
        domain = domain_pattern.search(match.group("comment"))
        if domain:
            values = domain.groups()
            values = values[:3] if values[0] is not None else values[3:]
            declarations[match.group(1)] = tuple(map(float, values))
    bound_pattern = re.compile(
        r"if\s*\(\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?P<lop><=|<)\s*(?P<lval>[-+0-9.eE]+)\s*\|\|\s*(?P=name)\s*(?P<rop>>=|>)\s*(?P<rval>[-+0-9.eE]+)\)", flags=re.I)
    runtime = {m.group("name"): (float(m.group("lval")), float(m.group("rval")))
               for m in bound_pattern.finditer(source_text)}
    repairs = []
    for name, spec in result.get("inputs", {}).items():
        if (name not in declarations
                or str(spec.get("optimize", "N")).upper() != "Y"):
            continue
        declared_lo, declared_hi, declared_step = declarations[name]
        lo, hi = sorted((declared_lo, declared_hi))
        if name in runtime:
            lo, hi = max(lo, runtime[name][0]), min(hi, runtime[name][1])
        if lo > hi or declared_step == 0:
            continue
        start, step, stop = map(float, (spec["start"], spec["step"], spec["stop"]))
        if (lo <= start <= stop <= hi
                and abs(step - abs(declared_step)) < max(abs(declared_step) * 1e-9, 1e-12)):
            continue
        spec.update({"start": lo, "step": abs(declared_step), "stop": hi,
                     "domain_provenance": {"method": "source_comment_runtime_intersection",
                         "source_comment_domain": [declared_lo, declared_hi, declared_step],
                         "executable_runtime_bounds": list(runtime[name]) if name in runtime else None,
                         "basis": "documented source domain intersected with executable bounds"}})
        repairs.append({"input": name, "start": lo, "step": abs(declared_step), "stop": hi})
    return {"config": result, "repairs": repairs}


def activate_executable_regime_inputs(config: dict, source_text: str) -> list[str]:
    """Enable regime controls that the current executable source actually uses."""
    from kcos_optimization_contract_v2 import derive_contract
    required = derive_contract(source_text)["required_optimized_regime_inputs"]
    inputs = config.get("inputs", {})
    for name in required:
        if name not in inputs or not isinstance(inputs[name], dict):
            raise ValueError(f"executable regime input is absent from tester_config: {name}")
        inputs[name]["optimize"] = "Y"
    return required


def validate_executable_runtime_bounds(config: dict, source_text: str) -> dict:
    """Reject optimized values outside explicit executable OnInit bounds.

    This lightweight gate is safe for Stage 1 fixtures that do not carry the
    older source-comment domain metadata, while still preventing MT5 from
    receiving values that the EA itself rejects with INIT_PARAMETERS_INCORRECT.
    """
    bound_pattern = re.compile(
        r"if\s*\(\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*"
        r"(?P<lop><=|<)\s*(?P<lval>[-+0-9.eE]+)\s*\|\|\s*"
        r"(?P=name)\s*(?P<rop>>=|>)\s*(?P<rval>[-+0-9.eE]+)\s*\)",
        flags=re.I,
    )
    bounds = {
        m.group("name"): (float(m.group("lval")), float(m.group("rval")))
        for m in bound_pattern.finditer(source_text)
    }
    checked = []
    for name, spec in config.get("inputs", {}).items():
        if str(spec.get("optimize", "N")).upper() != "Y" or name not in bounds:
            continue
        lo, hi = bounds[name]
        start, stop = float(spec["start"]), float(spec["stop"])
        _require(lo <= start <= stop <= hi,
                 f"approved range for {name} [{start},{stop}] exceeds executable runtime domain [{lo},{hi}]")
        checked.append(name)
    return {"passed": True, "runtime_bounds_checked": sorted(checked)}


def _clean_number(value) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        raise ValueError(f"non-finite TesterInputs number: {value!r}")
    if number.is_integer():
        return str(int(number))
    return format(number, ".12g")


def ini_row(name: str, spec: dict) -> str:
    # MT5 accepts the five-field optimization tuple only for optimized inputs.
    # Non-optimized inputs must receive their scalar value; otherwise MT5 passes
    # the literal tuple to the EA (fatal for strings such as cross symbols).
    if str(spec.get("optimize", "N")).upper() != "Y":
        from kcos_input_contract_v2 import SUPPORTED, serialize_value
        kind = spec.get("type")
        value = serialize_value(spec["default"], kind) if kind in SUPPORTED else spec["default"]
        return f"{name}={value}"
    return f"{name}={_clean_number(spec['default'])}||{_clean_number(spec['start'])}||{_clean_number(spec['step'])}||{_clean_number(spec['stop'])}||Y"


def apply_config_surface(text: str, config: dict) -> str:
    """Rewrite every approved input row; reject template-only optimized rows."""
    validate_config(config)
    for name, spec in config["inputs"].items():
        row = ini_row(name, spec)
        pattern = re.compile(rf"(?im)^{re.escape(name)}\s*=.*$")
        if pattern.search(text):
            text = pattern.sub(row, text, count=1)
        else:
            marker = re.search(r"(?im)^\[TesterInputs\]\s*$", text)
            _require(marker is not None, "template is missing [TesterInputs]")
            text = text[:marker.end()] + "\n" + row + text[marker.end():]
    # Lots is a fixed MT5 tester control.  Normalize legacy template tuples
    # (Lots=...||...||Y) to the scalar default before the unauthorized-row
    # scan; never allow template metadata to authorize this dimension.
    text = re.sub(r"(?im)^(Lots|SlippagePts|InpSpreadPts|InpCommissionPct)\s*=\s*([^|\r\n]+)(?:\|\|[^\r\n]*)$", r"\1=\2", text)
    # Any optimized template row not approved upstream is unsafe.
    in_inputs = False
    for raw in text.splitlines():
        stripped = raw.strip()
        section = re.fullmatch(r"\[([^]]+)\]", stripped)
        if section:
            in_inputs = section.group(1).casefold() == "testerinputs"
            continue
        if not in_inputs or not stripped or stripped.startswith(";") or "=" not in stripped:
            continue
        name, raw_value = (part.strip() for part in stripped.split("=", 1))
        if name in _FIXED_FIELDS:
            continue
        fields = raw_value.split("||")
        if len(fields) == 5 and fields[-1] == "Y":
            if name not in config["inputs"]:
                # Legacy templates may carry optimized rows from another EA.
                # Remove them before rebuilding the authenticated surface.
                text = re.sub(rf"(?im)^{re.escape(name)}\s*=.*(?:\r?\n|$)", "", text, count=1)
    return text
