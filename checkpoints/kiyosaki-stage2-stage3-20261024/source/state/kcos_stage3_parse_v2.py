#!/usr/bin/env python3
"""Strict, schema-aware Stage 3 wrapper for the self-describing MT5 parser."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import re
import datetime as dt
from decimal import Decimal, InvalidOperation
from pathlib import Path

import parse_mt5_opt_v2 as mt5opt
from kcos_stage3_common_v2 import atomic_json, load_object, require, sha256, utc_now
from kcos_tester_config_v2 import (config_sha256, stage2_config_from_spec,
                                   stage3_config_from_spec, has_stage2_step_count)


PARAM_RE = re.compile(r"\b(Inp[A-Za-z0-9_]+)\s*=")
INI_PARAM_RE = re.compile(r"(?m)^\s*(Inp[A-Za-z0-9_]+)\s*=\s*([^\r\n]+?)\s*$")
INFRASTRUCTURE_PARAM_RE = re.compile(
    r"^Inp(?:Regime_|MarginKillLevel$|TestPeriodWeeks$|SpreadPoints$|CommissionPerLot$|MinatoExpectedScoreVersion$|MinatoTestStart$|MinatoTestEnd$|FromDate$|ToDate$)"
)
FIXED_TESTER_HEADER_FIELDS = {"Lots", "SlippagePts"}
MINATO_DIAGNOSTIC_FIELDS = {
    "score_version", "ea", "symbol", "trades", "min_margin",
    "tester_start", "tester_end", "first_tick", "last_tick", "endpoint_open",
}


def capture_minato_diagnostic_summary(handoff: dict, runtime: dict,
                                     runtime_path: Path | None,
                                     timeframe: str, record_count: int) -> dict:
    """Hash-bind the per-agent Minato rows appended by this Stage 3 run."""
    identity = {"cycle_id": handoff.get("cycle_id"), "stage": 3,
                "timeframe": timeframe, "ea": handoff.get("ea"),
                "symbol": handoff.get("symbol"), "from_date": handoff.get("from_date"),
                "to_date": handoff.get("to_date")}
    base = {"schema_version": 1, **identity, "status": "INCONCLUSIVE",
            "reason": "Stage 3 launch did not record Minato diagnostic file offsets"}
    offsets = runtime.get("minato_diagnostic_offsets_before")
    if not isinstance(offsets, dict) or not offsets or runtime_path is None:
        return base
    if (runtime.get("cycle_id") != identity["cycle_id"] or runtime.get("stage") != 3
            or runtime.get("ea") != identity["ea"] or runtime.get("timeframe") != timeframe
            or runtime.get("symbol") != identity["symbol"]):
        base["reason"] = "Stage 3 runtime identity does not match Minato diagnostic capture"
        return base

    tester_root = (Path(str(runtime.get("data_dir") or "")) / "Tester").resolve()
    # Older launch receipts do not persist data_dir; derive the portable root
    # from the recorded INI and agent log paths only when they prove one root.
    if not str(runtime.get("data_dir") or ""):
        registered_paths = [Path(str(item.get("path") or "")) for item in runtime.get("agent_logs", [])
                            if isinstance(item, dict) and item.get("path")]
        roots = {next((parent for parent in path.parents
                       if parent.name.casefold().startswith("agent-")), Path()).parent.resolve()
                 for path in registered_paths}
        if len(roots) != 1:
            base["reason"] = "Stage 3 agent roots are missing or ambiguous"
            return base
        tester_root = roots.pop()

    agent_logs = runtime.get("agent_logs")
    expected_addresses = [str(item.get("address")) for item in agent_logs
                          if isinstance(item, dict) and item.get("address")]
    if not expected_addresses or len(expected_addresses) != len(set(expected_addresses)):
        base["reason"] = "Stage 3 runtime worker addresses are missing or duplicated"
        return base
    expected_address_set = set(expected_addresses)
    snapshots = {}
    seen_addresses = set()
    records = []
    try:
        for raw_path, snapshot in offsets.items():
            if not isinstance(snapshot, dict):
                raise ValueError("invalid Minato diagnostic offset record")
            path = Path(str(raw_path)).resolve()
            if len(path.parents) < 4 or path.parents[3].resolve() != tester_root:
                raise ValueError("Minato diagnostic path is outside the Stage 3 Tester agents")
            agent_name = path.parents[2].name
            match = re.fullmatch(r"Agent-(.+)-(\d+)", agent_name, re.I)
            if not match or path.name.casefold() != "minato_v2_opt_results.csv":
                raise ValueError("Minato diagnostic path is not an agent output file")
            address = f"{match.group(1)}:{match.group(2)}"
            if snapshot.get("address") != address:
                raise ValueError("Minato diagnostic agent address does not match its path")
            # Offsets are snapshotted for every existing MT5 agent, but only
            # workers registered to this runtime can contribute this run's
            # records. Ignore idle/foreign agents before reading their suffix.
            if address not in expected_address_set:
                continue
            if address in seen_addresses:
                raise ValueError("Stage 3 runtime has duplicate Minato diagnostic worker offsets")
            seen_addresses.add(address)
            offset = snapshot.get("size")
            prefix_hash = str(snapshot.get("prefix_sha256") or "").upper()
            if type(offset) is not int or offset < 0 or not re.fullmatch(r"[0-9A-F]{64}", prefix_hash):
                raise ValueError("Minato diagnostic starting offset is invalid")
            if not path.is_file():
                if address in expected_addresses:
                    raise ValueError("a Stage 3 worker has no Minato diagnostic file")
                continue
            raw = path.read_bytes()
            if len(raw) < offset or hashlib.sha256(raw[:offset]).hexdigest().upper() != prefix_hash:
                raise ValueError("Minato diagnostic prefix changed after Stage 3 launch")
            appended = raw[offset:]
            if not appended:
                continue
            prefix = raw[:offset]
            if offset:
                header_line = prefix.splitlines()[0]
                text = header_line.decode("utf-8-sig") + "\n" + appended.decode("utf-8-sig")
            else:
                text = appended.decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(text, newline=""), delimiter="\t")
            headers = reader.fieldnames or []
            if not MINATO_DIAGNOSTIC_FIELDS.issubset({item.strip() for item in headers}):
                raise ValueError("Minato diagnostic header is incomplete or not tab-delimited")
            for row in reader:
                if None in row or any(value is None for value in row.values()):
                    raise ValueError("Minato diagnostic row has a malformed column count")
                normalized = {str(key).strip(): str(value).strip() for key, value in row.items()}
                if (normalized.get("score_version") != "MINATO_BASE_SCORE_V2.0"
                        or normalized.get("ea") != identity["ea"]
                        or normalized.get("symbol") != identity["symbol"]
                        or normalized.get("tester_start", "")[:10] != identity["from_date"]
                        or normalized.get("tester_end", "")[:10] != identity["to_date"]):
                    raise ValueError("Minato diagnostic row identity differs from this Stage 3 run")
                try:
                    trades = Decimal(normalized["trades"])
                    margin = float(normalized["min_margin"])
                    endpoint_open = normalized["endpoint_open"]
                    first_tick = dt.datetime.strptime(normalized["first_tick"], "%Y.%m.%d %H:%M:%S")
                    last_tick = dt.datetime.strptime(normalized["last_tick"], "%Y.%m.%d %H:%M:%S")
                except (KeyError, ValueError, InvalidOperation, OverflowError) as exc:
                    raise ValueError("Minato diagnostic row has invalid typed values") from exc
                if (not trades.is_finite() or trades < 0 or trades != trades.to_integral_value()
                        or not math.isfinite(margin) or endpoint_open not in {"0", "1"}
                        or first_tick > last_tick):
                    raise ValueError("Minato diagnostic row violates value bounds")
                records.append({"address": address, "trades": int(trades),
                                "min_margin": margin, "endpoint_open": endpoint_open == "1",
                                "first_tick": first_tick.strftime("%Y.%m.%d %H:%M:%S"),
                                "last_tick": last_tick.strftime("%Y.%m.%d %H:%M:%S")})
            snapshots[address] = {"path": str(path), "start_size": offset,
                                  "start_prefix_sha256": prefix_hash,
                                  "appended_sha256": hashlib.sha256(appended).hexdigest().upper(),
                                  "appended_bytes": len(appended)}
    except (OSError, UnicodeError, ValueError, TypeError) as exc:
        base["reason"] = f"Minato diagnostic capture rejected: {type(exc).__name__}: {exc}"
        return base

    if seen_addresses != expected_address_set:
        base["reason"] = "Minato diagnostic offsets do not cover all Stage 3 workers"
        return base
    if not records:
        base["reason"] = "No per-pass Minato diagnostics were appended during this Stage 3 run"
        return base
    if type(record_count) is not int or record_count < 0 or len(records) != record_count:
        base["reason"] = ("Minato diagnostic row count mismatch: "
                          f"{len(records)} runtime-owned rows versus {record_count} optimizer rows")
        return base
    margins = [item["min_margin"] for item in records if item["min_margin"] < 1.0e300]
    first_ticks = [item["first_tick"] for item in records if item["first_tick"] != "1970.01.01 00:00:00"]
    last_ticks = [item["last_tick"] for item in records if item["last_tick"] != "1970.01.01 00:00:00"]
    base.update({"status": "COMPLETE", "reason": None,
                 "score_version": "MINATO_BASE_SCORE_V2.0",
                 "record_count": len(records), "optimizer_row_count": record_count,
                 "optimizer_row_count_matches": True,
                 "rows_with_trades": sum(item["trades"] > 0 for item in records),
                 "sum_trades": sum(item["trades"] for item in records),
                 "rows_with_open_endpoint": sum(item["endpoint_open"] for item in records),
                 "rows_with_observed_margin": len(margins),
                 "minimum_observed_margin": min(margins) if margins else None,
                 "first_tick_earliest": min(first_ticks) if first_ticks else None,
                 "first_tick_latest": max(first_ticks) if first_ticks else None,
                 "last_tick_earliest": min(last_ticks) if last_ticks else None,
                 "last_tick_latest": max(last_ticks) if last_ticks else None,
                 "agents": snapshots})
    return base


def authenticated_source_domains(spec: dict) -> dict[str, dict]:
    """Build source-domain evidence from the authenticated Stage 3 input contract.

    Stage 1 domains remain estimates unless explicitly source-bounded. The GJR
    base-lot grid is derived by Stage 2 and becomes authoritative through the
    hash-bound Stage 2 configuration.
    """
    domains = {}
    for name, item in spec.get("tester_config", {}).get("inputs", {}).items():
        if str(item.get("optimize", "N")).upper() != "Y":
            continue
        provenance = item.get("domain_provenance") or {}
        raw = provenance.get("source_domain")
        if provenance.get("method") == "kcos_stage2_gjr_base_lot":
            lo, step, hi = map(float, (item["start"], item["step"], item["stop"]))
            basis = "stage2_derived_policy"
            hard_domain = True
        elif isinstance(raw, (list, tuple)) and len(raw) == 3:
            lo, hi, step = map(float, raw)
            basis = "source_declared_domain"
            hard_domain = True
        else:
            lo, step, hi = map(float, (item["start"], item["step"], item["stop"]))
            basis = "stage1_search_estimate"
            hard_domain = False
        domains[name] = {
            "min": min(lo, hi), "max": max(lo, hi), "step": abs(step),
            "provenance": provenance.get("method", basis),
            "basis": basis,
            "hard_domain": hard_domain,
        }
    require(domains, "Stage 1 specification has no optimized source-domain contract")
    return domains


def expected_parameters(spec: dict) -> set[str]:
    inputs = spec.get("tester_config", {}).get("inputs")
    if isinstance(inputs, dict) and inputs:
        # tester_config.inputs is the complete authenticated executable input
        # surface. strategy.parameters is explanatory metadata and can retain
        # controls that Stage 2 intentionally removed from the generated EA.
        names = {
            str(name) for name in inputs
            if re.fullmatch(r"Inp[A-Za-z0-9_]+", str(name))
        }
    else:
        values = spec.get("strategy", {}).get("parameters", [])
        names = {match.group(1) for value in values for match in [PARAM_RE.search(str(value))] if match}
    require(bool(names), "Stage 1 specification has no parseable parameter names")
    return names


def ini_parameter_surface(ini_path: Path) -> tuple[set[str], set[str], set[str]]:
    require(ini_path.is_file(), f"Stage 3 INI is missing: {ini_path}")
    all_names, optimized, fixed = set(), set(), set()
    for match in INI_PARAM_RE.finditer(ini_path.read_text(encoding="utf-8-sig", errors="replace")):
        name, fields = match.group(1), [item.strip() for item in match.group(2).split("||")]
        require(name not in all_names, f"duplicate INI parameter: {name}")
        all_names.add(name)
        if len(fields) == 1:
            fixed.add(name)
            continue
        require(len(fields) >= 5 and fields[-1].upper() in {"Y", "N"}, f"invalid optimizer tuple for {name}")
        (optimized if fields[-1].upper() == "Y" else fixed).add(name)
    require(bool(all_names), "Stage 3 INI has no parseable InpXxx parameters")
    return all_names, optimized, fixed


def authenticated_fixed_source_inputs(source_path: Path, ini_path: Path,
                                       expected: set[str], ini_fixed: set[str]) -> set[str]:
    """Admit only extra INI inputs declared by the authenticated EA source and pinned to its default.

    Stage 1 may omit fixed metadata inputs from its optimizer contract. They are
    harmless to the search surface only when the launched INI explicitly leaves
    optimization off and preserves the exact source default.
    """
    source_text = source_path.read_text(encoding="utf-8-sig", errors="replace")
    declaration_re = re.compile(
        r"(?m)^\s*input\s+(\w+)\s+([A-Za-z_]\w*)\s*=\s*([^;]+);"
    )
    declarations = {}
    for match in declaration_re.finditer(source_text):
        input_type, name, raw_default = match.groups()
        if name in declarations:
            raise ValueError(f"source input declaration is ambiguous: {name}")
        declarations[name] = (input_type.lower(), raw_default.split("//", 1)[0].strip())

    ini_rows = {match.group(1): [part.strip() for part in match.group(2).split("||")]
                for match in INI_PARAM_RE.finditer(ini_path.read_text(encoding="utf-8-sig", errors="replace"))}
    admitted = set()

    def canonical(value: str, input_type: str):
        token = value.strip()
        if input_type in {"bool", "boolean"}:
            normalized = token.lower()
            return {"true": True, "1": True, "false": False, "0": False}.get(normalized)
        if input_type in {"string"}:
            return token[1:-1] if len(token) >= 2 and token[0] == token[-1] == '"' else token
        if input_type in {"int", "uint", "ulong", "long", "short", "uchar", "ushort",
                          "byte", "sbyte", "integer", "double", "float", "real"}:
            numeric = re.sub(r"[fFuUlL]+$", "", token)
            try:
                if re.fullmatch(r"0[xX][0-9A-Fa-f]+", numeric):
                    return Decimal(int(numeric, 16))
                number = Decimal(numeric)
            except (InvalidOperation, ValueError):
                return None
            return number if number.is_finite() else None
        return None

    for name in sorted((set(ini_rows) - expected) & ini_fixed):
        declaration = declarations.get(name)
        if declaration is None:
            continue
        input_type, source_default = declaration
        source_value = canonical(source_default, input_type)
        if source_value is None:
            continue
        fields = ini_rows[name]
        if len(fields) == 1:
            retained_values = fields
        elif len(fields) == 5 and fields[-1].upper() == "N":
            retained_values = (fields[0], fields[1], fields[3])
        else:
            continue
        require(all(canonical(value, input_type) == source_value for value in retained_values),
                f"fixed source input {name} differs from its authenticated EA default")
        admitted.add(name)
    return admitted

def validate_fixed_tester_window_metadata(ini_path: Path, source_path: Path,
                                          handoff: dict) -> set[str]:
    """Admit only source-declared fixed date metadata matching the Stage 3 window."""
    source_text = (source_path.read_text(encoding="utf-8-sig", errors="replace")
                   if source_path.is_file() else "")
    source_defaults = {}
    for name in ("InpFromDate", "InpToDate"):
        matches = re.findall(
            rf"(?m)^\s*input\s+string\s+{name}\s*=\s*\"([^\"]+)\"\s*;",
            source_text,
        )
        require(len(matches) <= 1, f"source has ambiguous tester-window input {name}")
        if matches:
            source_defaults[name] = matches[0]
    ini_text = ini_path.read_text(encoding="utf-8-sig", errors="replace")
    ini_rows = {m.group(1): m.group(2).strip()
                for m in INI_PARAM_RE.finditer(ini_text)}
    declared_metadata = set(source_defaults)
    ini_metadata = {name for name in ("InpFromDate", "InpToDate") if name in ini_rows}
    require(ini_metadata == declared_metadata,
            "Stage 3 tester-window metadata does not match source declarations")
    if not declared_metadata:
        return set()
    require(declared_metadata == {"InpFromDate", "InpToDate"},
            "source must declare both tester-window metadata inputs")
    expected = {
        "InpFromDate": str(handoff.get("from_date") or ""),
        "InpToDate": str(handoff.get("to_date") or ""),
    }
    tester_values = {}
    in_tester = False
    for raw in ini_text.splitlines():
        section = re.fullmatch(r"\s*\[([^]]+)\]\s*", raw)
        if section:
            in_tester = section.group(1).casefold() == "tester"
            continue
        if in_tester and "=" in raw:
            key, value = (part.strip() for part in raw.split("=", 1))
            if key in {"FromDate", "ToDate"}:
                tester_values[key] = value
    for name, tester_key in (("InpFromDate", "FromDate"), ("InpToDate", "ToDate")):
        target = expected[name]
        require(bool(target) and source_defaults[name] == target,
                f"source {name} default differs from the authenticated Stage 3 window")
        fields = [part.strip() for part in ini_rows[name].split("||")]
        fixed_scalar = len(fields) == 1 and fields[0].strip('"') == target
        fixed_optimization_row = (
            len(fields) == 5 and fields[-1].upper() == "N"
            and fields[0].strip('"') == target
            and fields[1].strip('"') == target
            and fields[2] == "1"
            and fields[3].strip('"') == target
        )
        require(fixed_scalar or fixed_optimization_row,
                f"{name} must be a fixed value matching the authenticated Stage 3 window")
        require(tester_values.get(tester_key) == target,
                f"Tester {tester_key} differs from the authenticated Stage 3 window")
    return declared_metadata


def ini_optimization_ranges(ini_path: Path) -> dict[str, tuple[float, float, float]]:
    """Return the exact numeric optimization surface encoded by the INI."""
    ranges = {}
    for match in INI_PARAM_RE.finditer(ini_path.read_text(encoding="utf-8-sig", errors="replace")):
        name, fields = match.group(1), [item.strip() for item in match.group(2).split("||")]
        if len(fields) >= 5 and fields[-1].upper() == "Y":
            ranges[name] = (float(fields[1]), abs(float(fields[2])), float(fields[3]))
    return ranges


def validate_ini_surface_against_spec(ini_path: Path, spec: dict, optimized: set[str]) -> None:
    """Validate the launched Stage 3 surface without freezing Stage 1 estimates.

    Stage 1's ranges are estimates/source hints. Stage 3 materializes its own
    wide grid before launch and binds that exact INI by hash in the handoff and
    runtime manifest. The parser validates that launched grid and OPT rows
    against one another; numerical equality with Stage 1 would reject valid
    Stage 3 grids and prevent timeframe advance.
    """
    expected = spec.get("tester_config", {}).get("inputs", {})
    actual = ini_optimization_ranges(ini_path)

    def authenticated_regime_floor(name: str) -> float | None:
        constraints = spec.get("tester_config", {}).get("input_constraints", [])
        floors = []
        for constraint in constraints if isinstance(constraints, list) else []:
            if not isinstance(constraint, dict) or constraint.get("scope") != "single":
                continue
            coefficients = constraint.get("coefficients")
            if (constraint.get("operator") != ">=" or not isinstance(coefficients, dict)
                    or set(coefficients) != {name}):
                continue
            try:
                coefficient, limit = float(coefficients[name]), float(constraint["limit"])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(coefficient) and coefficient > 0 and math.isfinite(limit):
                floors.append(limit / coefficient)
        return max(floors) if floors else None

    for name in optimized:
        item = expected.get(name)
        require(isinstance(item, dict) and name in actual,
                f"INI optimization surface is missing Stage 1 input: {name}")
        stage2_grid = item.get("domain_provenance", {}).get("method") == "stage2_owned_optimization_grid"
        if stage2_grid:
            expected_grid = tuple(float(item[key]) for key in ("start", "step", "stop"))
            actual_grid = actual[name]
            require(all(math.isclose(a, e, rel_tol=1e-10, abs_tol=1e-12)
                        for a, e in zip(actual_grid, expected_grid)),
                    f"Stage 3 INI changed Stage 2 approved grid for {name}: actual={actual_grid}, expected={expected_grid}")
            step_count = round((actual_grid[2] - actual_grid[0]) / actual_grid[1], 9)
            require(has_stage2_step_count(*actual_grid),
                    f"Stage 2 grid for {name} has {step_count:g} MT5 steps; expected 15-20")
            continue
        if name.startswith("InpRegime_"):
            start, step, stop = actual[name]
            canonical = all(abs(a - b) <= 1e-9 for a, b in zip(actual[name], (0.0, 0.25, 5.0)))
            default_value = float(item.get("default", float("nan")))
            default_index = ((default_value - start) / step if step > 0 else float("nan"))
            default_on_grid = (math.isfinite(default_index)
                               and abs(default_index - round(default_index)) <= 1e-8
                               and start - 1e-9 <= default_value <= stop + 1e-9)
            canonical_index = default_value / 0.25
            default_misses_canonical = (not math.isfinite(canonical_index)
                                        or abs(canonical_index - round(canonical_index)) > 1e-8)
            offset_grid = (
                default_on_grid and default_misses_canonical and not canonical
                and abs(step - 0.25) <= 1e-9
                and 0.0 <= start <= default_value <= stop <= 5.0
                and has_stage2_step_count(start, step, stop)
            )
            floor = authenticated_regime_floor(name) if name.endswith("_Lot_Mult") else None
            constrained = (floor is not None and abs(step - 0.25) <= 1e-9
                           and abs(stop - 5.0) <= 1e-9 and start >= floor - 1e-9
                           and start - floor < 0.25 - 1e-9)
            require(canonical or offset_grid or constrained,
                    f"Stage 3 regime grid mismatch for {name}: actual={actual[name]}, expected=(0.0, 0.25, 5.0), a default-aligned quarter-step grid inside [0, 5], or an authenticated lower bound")
            continue
        start, step, stop = actual[name]
        require(all(map(math.isfinite, (start, step, stop))) and start < stop and step > 0,
                f"Stage 3 optimized range is invalid for {name}: actual={actual[name]}")


class OptimizationSurfaceMismatch(ValueError):
    """Completed OPT contains values outside its authenticated launch INI."""


def validate_rows_against_ini(rows: list[list], inputs: list[dict], ini_path: Path,
                             optimized: set[str]) -> None:
    ranges = ini_optimization_ranges(ini_path)
    headers = mt5opt.csv_header_for(inputs)
    positions = {name: headers.index(name) for name in optimized if name in headers}
    violations = []
    for row_number, row in enumerate(rows, 1):
        for name, index in positions.items():
            lo, step, hi = ranges[name]
            hi = mt5opt.effective_grid_stop({"start": lo, "step": step, "stop": hi})
            value = float(row[index])
            if value < min(lo, hi) - 1e-9 or value > max(lo, hi) + 1e-9:
                violations.append({"row": row_number, "parameter": name,
                                   "value": value, "range": [lo, hi], "step": step})
                break
    if violations:
        raise OptimizationSurfaceMismatch(
            f"opt rows exceed the authenticated INI surface for {len(violations)} rows: {violations[:3]}")


def parse_opt(handoff_path: Path, timeframe_event_path: Path, opt_path: Path,
              csv_path: Path, evidence_path: Path, timeframe: str,
              runtime_path: Path | None = None) -> dict:
    handoff, event = load_object(handoff_path), load_object(timeframe_event_path)
    runtime = load_object(runtime_path) if runtime_path else {}
    require(handoff.get("schema_version") == 2 and handoff.get("stage") == 3, "Stage 3 handoff schema mismatch")
    require(timeframe in handoff.get("timeframe_order", []), "timeframe is outside Stage 3 contract")
    require(event.get("cycle_id") == handoff.get("cycle_id") and event.get("timeframe") == timeframe, "timeframe event identity mismatch")
    require(event.get("status") == "TIMEFRAME_COMPLETE" and event.get("opt_sha256") == sha256(opt_path), "timeframe event does not prove current opt")
    if runtime:
        require(runtime.get("cycle_id") == handoff.get("cycle_id") and runtime.get("ea") == handoff.get("ea")
                and runtime.get("timeframe") == timeframe, "Stage 3 runtime identity mismatch")
        journal_path = Path(runtime.get("tester_log", ""))
        journal_start = int(runtime.get("tester_log_start_offset", -1))
        journal_end = int(event.get("tester_log_end_offset", -1))
        journal_hash = None
        ui = event.get("ui_completion_evidence", {})
        journal_override = bool(event.get("journal_override") is True and event.get("journal_override_reason"))
        if ui.get("final") is True:
            screenshot = Path(ui.get("screenshot", ""))
            require(screenshot.is_file() and sha256(screenshot) == ui.get("screenshot_sha256"),
                    "UI completion screenshot is missing or changed")
            require(int(ui.get("completed_passes", -1)) == int(ui.get("total_passes", -2)) > 0,
                    "UI completion pass counts are not final")
        elif not journal_override:
            require(journal_path.is_file() and journal_start >= 0 and journal_end > journal_start and
                    Path(event.get("tester_log", "")).resolve() == journal_path.resolve() and
                    int(event.get("tester_log_start_offset", -1)) == journal_start,
                    "completed timeframe lacks its fresh Tester Journal byte range")
            with journal_path.open("rb") as handle:
                handle.seek(journal_start)
                journal_bytes = handle.read(journal_end - journal_start)
            journal_hash = hashlib.sha256(journal_bytes).hexdigest().upper()
            partial_mode = event.get("completion_mode") == "partial_90_percent"
            require(len(journal_bytes) == journal_end - journal_start and
                    journal_hash == event.get("tester_log_sha256") == runtime.get("fresh_log_sha256") and
                    (partial_mode or (event.get("tester_log_completion_evidence", {}).get("final") is True and
                                      event.get("tester_log_completion_evidence", {}).get("cache_saved") is True)),
                    "fresh Tester Journal completion evidence is missing or changed")
    spec = load_object(Path(handoff["stage1_spec"]))
    require(sha256(Path(handoff["stage1_spec"])) == handoff["stage1_spec_sha256"], "Stage 1 specification changed")
    stage1_expected = expected_parameters(spec)
    source_path = Path(handoff.get("source", ""))
    source_contract = {}
    if handoff.get("source") or handoff.get("source_sha256"):
        require(source_path.is_file() and sha256(source_path) == handoff.get("source_sha256"),
                "Stage 3 source is missing or changed")
        source_text = source_path.read_text(encoding="utf-8-sig", errors="replace")
        stage2_config = stage2_config_from_spec(spec, source_text=source_text)
        require(config_sha256(stage2_config) == handoff.get("approved_tester_config_sha256"),
                "Stage 2 derived tester configuration hash differs from Stage 3 handoff")
        runtime_spec = dict(spec)
        runtime_spec["tester_config"] = stage3_config_from_spec(spec, source_text=source_text)
        source_contract = authenticated_source_domains(runtime_spec)
    else:
        runtime_spec = spec
    template_ini_path = Path(handoff.get("ini_files", {}).get(timeframe, ""))
    template_ini_hash = handoff.get("ini_sha256", {}).get(timeframe)
    require(bool(template_ini_hash) and template_ini_path.is_file(), "Stage 3 template INI is missing")
    ini_path = Path(runtime.get("ini", "")) if runtime else template_ini_path
    expected_ini_hash = runtime.get("ini_sha256") if runtime else template_ini_hash
    require(bool(expected_ini_hash) and ini_path.is_file() and sha256(ini_path) == expected_ini_hash,
            "Stage 3 launched INI is missing or changed")
    expected = expected_parameters(runtime_spec)
    ini_declared, ini_optimized, ini_fixed = ini_parameter_surface(ini_path)
    fixed_window_metadata = validate_fixed_tester_window_metadata(ini_path, source_path, handoff)
    fixed_source_metadata = (authenticated_fixed_source_inputs(source_path, ini_path, expected, ini_fixed)
                             if source_path.is_file() else set())
    missing_from_ini = sorted(expected - ini_declared)
    unexpected_in_ini = sorted(ini_declared - expected)
    unexpected_strategy = [name for name in unexpected_in_ini
                           if not INFRASTRUCTURE_PARAM_RE.match(name)
                           and name not in fixed_window_metadata
               and name not in fixed_source_metadata
                           and name not in {"Lots", "SlippagePts", "InpSpreadPts", "InpCommissionPct",
                                            "InpMinatoExpectedScoreVersion", "InpMinatoTestStart", "InpMinatoTestEnd"}]
    require(not missing_from_ini, f"Stage 3 INI is missing Stage 1 parameters: {missing_from_ini}")
    require(not unexpected_strategy,
            f"Stage 3 INI has strategy parameters outside Stage 1 contract: {unexpected_strategy}")
    validate_ini_surface_against_spec(ini_path, runtime_spec, ini_optimized)
    header_size, record_size, inputs = mt5opt.read_header_meta(str(opt_path))
    size = opt_path.stat().st_size
    require(header_size >= 436 and record_size >= 232 and size > header_size, "invalid or empty opt structure")
    require((size - header_size) % record_size == 0, "incomplete opt record tail")
    declared = {item["name"] for item in inputs}
    header_optimized = {item["name"] for item in inputs if item["optimized"]}
    fixed_header_inputs = (ini_fixed & FIXED_TESTER_HEADER_FIELDS) & header_optimized
    allowed_optimized = ini_optimized | fixed_header_inputs
    require(header_optimized == allowed_optimized,
            f"opt optimized parameter surface differs from INI: missing={sorted(ini_optimized - header_optimized)}, unexpected={sorted(header_optimized - allowed_optimized)}")
    # Fixed booleans emitted by some MT5 builds are metadata-only for parsing;
    # do not ask the record decoder to resolve an optimization slot for them.
    for i, spec in enumerate(inputs):
        if spec["name"] in fixed_header_inputs:
            spec["optimized"] = False
    # The header offset field is type-relative metadata, not a record byte
    # offset. Resolve record slots only through an exact map or joint discovery.
    offsets = {}
    map_path = Path(__file__).parent / 'opt_offset_maps.json'
    if map_path.is_file():
        try:
            maps = json.loads(map_path.read_text(encoding='utf-8-sig'))
            best_key, best_score = None, 0
            for key, m in maps.items():
                if key.startswith('_'):
                    continue
                # Offset maps are valid only for the exact cache artifact.
                # Record size alone is not an identity: different EAs can share it.
                if m.get('_record_size') != record_size:
                    continue
                if m.get('_opt_sha256') != sha256(opt_path):
                    continue
                score = sum(1 for spec in inputs
                            if spec['optimized'] and spec['name'] in m)
                if score > best_score:
                    best_key, best_score = key, score
            if best_key and best_score >= 5:
                bm = maps[best_key]
                for i, spec in enumerate(inputs):
                    if spec['optimized'] and spec['name'] in bm:
                        offsets[i] = bm[spec['name']]
                print(f'[OFFSETS] map {best_key}: {len(offsets)} inputs')
        except Exception as e:
            print(f'[OFFSETS] map lookup failed: {e}')
    # Fill in any optimized inputs the map missed with auto-discovery.
    missing = [i for i, spec in enumerate(inputs)
               if spec['optimized'] and i not in offsets]
    if missing:
        auto = mt5opt.discover_input_offsets(str(opt_path), header_size, record_size, inputs)
        filled = 0
        for i in missing:
            if i in auto:
                offsets[i] = auto[i]
                filled += 1
        if filled:
            print(f'[OFFSETS] auto-discover filled {filled} more inputs')
    if not offsets:
        # Fallback if everything above failed.
        offsets = mt5opt.discover_input_offsets(str(opt_path), header_size, record_size, inputs)
    optimized_indices = {index for index, item in enumerate(inputs) if item["optimized"]}
    unresolved = sorted(inputs[index]["name"] for index in optimized_indices - set(offsets))
    require(not unresolved, f"optimized input offsets unresolved: {unresolved}")
    count = (size - header_size) // record_size
    rows = mt5opt.parse_records(str(opt_path), header_size, record_size, inputs, 0, count, offsets)
    rows, duplicates = mt5opt.deduplicate_records(rows)
    require(bool(rows), "opt contains no complete records")
    violations = mt5opt.validate_parameter_rows(rows, inputs)
    require(not violations,
            f"Stage 3 parameter-grid validation failed for {len(violations)} cells: {violations[:3]}")
    validate_rows_against_ini(rows, inputs, ini_path, ini_optimized)
    minato_diagnostics = capture_minato_diagnostic_summary(
        handoff, runtime, runtime_path, timeframe, len(rows)) if runtime else {
            "schema_version": 1, "cycle_id": handoff["cycle_id"], "stage": 3,
            "timeframe": timeframe, "ea": handoff["ea"], "symbol": handoff.get("symbol"),
            "from_date": handoff["from_date"], "to_date": handoff["to_date"],
            "status": "INCONCLUSIVE", "reason": "Stage 3 runtime manifest was not supplied to parser"}
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle); writer.writerow(mt5opt.csv_header_for(inputs)); writer.writerows(rows)
        handle.flush()
    custom_index = mt5opt.CSV_HEADER.index("CustomFitness")
    require(any(float(row[custom_index]) >= 0.0 for row in rows), "Minato CustomFitness field is unreadable")
    optimized_input_ranges = {
        name: {"start": start, "step": step, "stop": stop}
        for name, (start, step, stop) in ini_optimization_ranges(ini_path).items()
    }
    evidence = {
        "schema_version": 2, "cycle_id": handoff["cycle_id"], "stage": 3, "timeframe": timeframe,
        "ea": handoff["ea"], "from_date": handoff["from_date"], "to_date": handoff["to_date"],
        **({"source": str(source_path.resolve()), "source_sha256": sha256(source_path),
            "stage1_spec": str(Path(handoff["stage1_spec"]).resolve()),
            "stage1_spec_sha256": handoff["stage1_spec_sha256"],
            "source_input_domains": source_contract} if source_contract else {}),
        "opt": str(opt_path.resolve()), "opt_sha256": sha256(opt_path), "csv": str(csv_path.resolve()),
        "csv_sha256": sha256(csv_path), "header_size": header_size, "record_size": record_size,
        "timeframe_event": str(timeframe_event_path.resolve()),
        "timeframe_event_sha256": sha256(timeframe_event_path),
        "tester_log": str(journal_path.resolve()) if runtime else None,
        "tester_log_start_offset": journal_start if runtime else None,
        "tester_log_end_offset": journal_end if runtime else None,
        "tester_log_sha256": journal_hash if runtime else None,
        "minato_diagnostics": minato_diagnostics,
        "records": len(rows), "duplicates_dropped": duplicates, "declared_parameters": sorted(declared),
        "stage1_parameters": sorted(stage1_expected),
        "stage2_derived_parameters": sorted(expected - stage1_expected),
        "optimization_contract_parameters": sorted(expected),
        "optimized_input_ranges": optimized_input_ranges,
        "ini": str(ini_path.resolve()), "ini_sha256": sha256(ini_path),
        "ini_optimized_parameters": sorted(ini_optimized), "ini_fixed_parameters": sorted(ini_fixed),
        "authenticated_fixed_source_parameters": sorted(fixed_source_metadata),
        "optimized_parameters": sorted(header_optimized),
        "mt5_grid_endpoint_adjustments": {
            item["name"]: {
                "declared_stop": float(item["stop"]),
                "last_step_value": mt5opt.effective_grid_stop(item),
            }
            for item in inputs
            if item["optimized"]
            and mt5opt.effective_grid_stop(item) > float(item["stop"]) + 1e-9
        },
        "fixed_parameters_omitted_from_opt_header": sorted(ini_fixed - declared),
        "nonoptimized_opt_header_extras": sorted(declared - ini_declared),
        "post_stage2_infrastructure_parameters": sorted(
            name for name in unexpected_in_ini if INFRASTRUCTURE_PARAM_RE.match(name)),
        "parsed_utc": utc_now(), "passed": True,
    }
    atomic_json(evidence_path, evidence)
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handoff", type=Path, required=True)
    parser.add_argument("--timeframe-event", type=Path, required=True)
    parser.add_argument("--opt", type=Path, required=True)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--timeframe", choices=("M5", "M15", "H1"), required=True)
    args = parser.parse_args()
    print(json.dumps(parse_opt(args.handoff, args.timeframe_event, args.opt, args.csv, args.evidence, args.timeframe)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
