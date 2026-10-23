#!/usr/bin/env python3
"""Prepare Stage 3 v2 artifacts without launching MT5 or a cron."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import psutil

from kcos_stage3_common_v2 import FROM_DATE, TIMEFRAMES, TO_DATE, atomic_bytes, atomic_json, load_object, require, sha256, utc_now
from kcos_tester_config_v2 import (apply_config_surface, config_from_spec, stage2_config_from_spec,
                                   stage3_config_from_spec, config_sha256,
                                   is_timing_control_input,
                                   validate_optimized_source_usage,
                                   apply_shared_input_surface)
from kcos_runtime_paths_v2 import assert_safe_root
from kcos_stage3_duration_policy_v2 import reduce_ini
from kcos_run_authorization_v2 import authorizes_stage3_admission, authorizes_stages_1_to_5
import normalize_opt_ini


ROOT = assert_safe_root()
STATE = ROOT / "state" / "kcos_cycle_state_v2.json"
STAGE2_HANDOFF = ROOT / "state" / "handoffs" / "Stage2_v2.json"
STAGE3_HANDOFF = ROOT / "state" / "handoffs" / "Stage3_v2.json"
LEASE = ROOT / "state" / "leases" / "stage3_v2.lock.json"
FIRST_PASS_BUDGET_SECONDS = 120
FIRST_PASS_HARD_LIMIT_SECONDS = 200
PROGRESS_STALL_BUDGET_SECONDS = 900
STARTUP_GRACE_SECONDS = 300
TIMEFRAME_BUDGET_SECONDS = 9 * 60 * 60
MAX_PASSES = 11000
PASS_LIMIT_SETTLE_SECONDS = 60
TIMEFRAMES = ("M5", "M15", "H1")
INI_PARAM_RE = re.compile(r"(?m)^\s*(Inp[A-Za-z0-9_]+)\s*=")


def consumption_insensitive_event_sha256(event_path: Path) -> str:
    """Hash immutable event identity while ignoring supervisor consumption bookkeeping."""
    identity = load_object(Path(event_path))
    for key in ("consumed", "consumed_utc", "consumed_action"):
        identity.pop(key, None)
    payload = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest().upper()


class Stage3InputContractError(ValueError):
    """A candidate EA cannot be optimized under its authenticated input contract."""

    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code


def record_input_contract_failure(state_path: Path, stage2_handoff_path: Path,
                                  code: str, detail: str) -> dict:
    """Persist a hash-bound Stage 3 input rejection with the correct recovery class."""
    state = load_object(state_path)
    stage2 = load_object(stage2_handoff_path)
    cycle_id = str(state.get("cycle_id", ""))
    require(bool(cycle_id) and stage2.get("cycle_id") == cycle_id,
            "cannot record input-contract failure without matching cycle evidence")
    stage1_spec = Path(stage2["stage1_spec"])
    stage2_event = Path(stage2["expected_event"])
    evidence = {}
    for key, path in (("stage1_spec", stage1_spec), ("stage2_event", stage2_event),
                      ("stage2_handoff", stage2_handoff_path)):
        require(path.is_file(), f"cannot record Stage 3 input-contract failure: missing {key}")
        evidence[key] = {"path": str(path.resolve()), "sha256": sha256(path)}
    failure_path = state_path.parent / "events" / f"{cycle_id}.stage3.input_contract_failure.json"
    # A bad optimization grid is a Stage 3 preparation/configuration failure.
    # It says nothing about the candidate EA and must not consume the rotation
    # queue. Source/default/type contract mismatches remain candidate-local.
    rotation_eligible = code != "INPUT_GRID_INVALID"
    failure = {
        "schema_version": 2,
        "event_id": f"{cycle_id}:stage3:input_contract_rejected",
        "cycle_id": cycle_id,
        "stage": 3,
        "status": "failed",
        "failure_kind": "STAGE3_INPUT_CONTRACT_FAILURE",
        "recovery_class": "candidate_input_contract" if rotation_eligible else "stage3_grid_policy",
        "ea": state.get("current_ea"),
        "failed_utc": utc_now(),
        "rotation_eligible": rotation_eligible,
        "consumed": False,
        "findings": [{"code": code, "detail": str(detail)}],
        "evidence": evidence,
    }
    atomic_json(failure_path, failure)
    state.update({
        "current_stage": "stage3_failed_held",
        "stage_status": "stage3_input_contract_rejected",
        "stage3_failure_class": "candidate_input_contract" if rotation_eligible else "stage3_grid_policy",
        "last_stage2_handoff": str(stage2_handoff_path.resolve()),
        "stage3_failure_event": str(failure_path.resolve()),
        "stage3_failure_detail": str(detail),
        "next_action": "rotate_stage1" if rotation_eligible else "repair_stage3_grid",
        "hold_after_stage": 5 if authorizes_stages_1_to_5(state) else min(int(state.get("hold_after_stage") or 3), 3),
        "updated_utc": utc_now(),
    })
    atomic_json(state_path, state)
    return failure


def reconcile_stale_lease(lease_path: Path, cycle_id: str, output_dir: Path) -> Path | None:
    """Archive only a lease whose Stage 3 runtime is identifiable and has no live owners."""
    if not lease_path.exists():
        return None
    lease = load_object(lease_path)
    lease_cycle = lease.get("cycle_id")
    require(bool(lease_cycle), "Stage 3 lease has no cycle identity")
    runtime_path = output_dir / f"{lease_cycle}.stage3.runtime.json"
    if not runtime_path.is_file():
        # A preflight can fail after lease creation but before a runtime manifest exists.
        # Recover only when the canonical cycle points to this exact non-rotatable failure
        # and no process from the isolated Stage 3 terminal tree is running.
        state_path = lease_path.parent.parent / "kcos_cycle_state_v2.json"
        require(state_path.is_file(), f"cannot prove orphan Stage 3 lease stale; state is missing: {state_path}")
        state = load_object(state_path)
        failure_path = Path(str(state.get("stage3_failure_event") or ""))
        require(state.get("cycle_id") == lease_cycle == cycle_id
                and state.get("stage3_failure_class") == "infrastructure"
                and failure_path.is_file(),
                "cannot prove orphan Stage 3 lease belongs to the current infrastructure failure")
        failure = load_object(failure_path)
        require(failure.get("cycle_id") == lease_cycle and failure.get("stage") == 3
                and failure.get("status") == "failed" and failure.get("rotation_eligible") is False,
                "cannot prove orphan Stage 3 lease failure is cycle-owned and non-rotatable")
        portable_root = output_dir.parent / "mt5_stage45_portable"
        expected_exes = {
            (portable_root / "terminal64.exe").resolve(),
            (portable_root / "metatester64.exe").resolve(),
        }
        live = []
        unknown = []
        for process in psutil.process_iter(["pid", "name", "exe"]):
            name = str(process.info.get("name") or "").casefold()
            if name not in {"terminal64.exe", "metatester64.exe"}:
                continue
            try:
                exe = Path(process.exe()).resolve()
                if exe in expected_exes:
                    live.append(int(process.pid))
            except psutil.NoSuchProcess:
                continue
            except (psutil.AccessDenied, OSError):
                unknown.append(int(process.pid))
        require(not live and not unknown,
                f"cannot recover orphan Stage 3 lease; isolated terminal PIDs live={live}, inaccessible={unknown}")
        stamp = utc_now().replace("-", "").replace(":", "")
        archive = lease_path.with_name(f"{lease_path.stem}.removed.{stamp}{lease_path.suffix}")
        require(not archive.exists(), f"orphan lease archive already exists: {archive}")
        atomic_bytes(archive, lease_path.read_bytes())
        lease_hash = sha256(archive)
        require(lease_hash == sha256(lease_path), "orphan Stage 3 lease archive hash mismatch")
        lease_path.unlink()
        output_dir.mkdir(parents=True, exist_ok=True)
        receipt_path = output_dir / f"{lease_cycle}.stage3.orphan_lease_recovery.{stamp}.json"
        atomic_json(receipt_path, {
            "schema_version": 1, "cycle_id": lease_cycle, "recovered_utc": utc_now(),
            "reason": "preflight_failure_before_runtime_manifest",
            "lease_archive": str(archive.resolve()), "lease_sha256": lease_hash,
            "failure_event": str(failure_path.resolve()), "failure_event_sha256": sha256(failure_path),
            "runtime_manifest_missing": str(runtime_path.resolve()),
            "isolated_executables_checked": [str(path) for path in sorted(expected_exes)],
            "matching_live_pids": [], "inaccessible_candidate_pids": [],
        })
        return archive
    runtime = load_object(runtime_path)
    require(runtime.get("schema_version") == 2 and runtime.get("stage") == 3 and
            runtime.get("cycle_id") == lease_cycle,
            "cannot prove Stage 3 lease belongs to its runtime manifest")
    worker_evidence = {
        int(item.get("pid")): item
        for item in runtime.get("worker_process_evidence", [])
        if isinstance(item, dict) and item.get("pid")
    }
    recorded_pids = list(dict.fromkeys([
        *runtime.get("worker_pids", []),
        *worker_evidence.keys(),
        runtime.get("terminal_pid"),
        runtime.get("watcher_pid"),
    ]))
    def owned_live_pid(raw_pid: int) -> bool:
        """Treat a PID as live only when it still matches this runtime's identity."""
        try:
            process = psutil.Process(int(raw_pid))
            runtime_pid = int(raw_pid)
            worker_identity = worker_evidence.get(runtime_pid)
            if isinstance(worker_identity, dict) and worker_identity.get("create_time") is not None:
                expected_created = float(worker_identity["create_time"])
                if abs(float(process.create_time()) - expected_created) > 1.0:
                    # Windows may reuse a completed MetaTester PID. Compare the
                    # captured process identity before reading its command line,
                    # which can be inaccessible for an unrelated system process.
                    return False
            if runtime_pid == int(runtime.get("terminal_pid") or 0):
                exe = Path(process.exe()).resolve()
                command = [str(value) for value in process.cmdline()]
                expected = [str(value) for value in runtime.get("command", [])]
                return (exe == Path(expected[0]).resolve() and
                        process.create_time() >= float(runtime.get("launched_epoch", 0)) - 1.0 and
                        command == expected)
            addresses = {int(item.get("worker_pid")): str(item.get("address"))
                         for item in runtime.get("agent_logs", [])
                         if isinstance(item, dict) and item.get("worker_pid") and item.get("address")}
            addresses.update({pid: str(item.get("address")) for pid, item in worker_evidence.items()
                              if item.get("address")})
            address = addresses.get(runtime_pid)
            if address:
                command = " ".join(str(value) for value in process.cmdline()).casefold()
                evidence = worker_evidence.get(runtime_pid, {})
                expected_worker_exe = evidence.get("exe") or str(Path(runtime.get("command", [""])[0]).with_name("metatester64.exe"))
                actual_worker_exe = Path(process.exe()).resolve()
                return (str(process.name()).casefold() == "metatester64.exe" and
                        actual_worker_exe == Path(expected_worker_exe).resolve() and
                        f"/address:{address.casefold()}" in command and
                        "/portable" in command and
                        process.create_time() >= float(runtime.get("launched_epoch", 0)) - 1.0)
            watcher = int(runtime.get("watcher_pid") or 0)
            command = " ".join(str(value) for value in process.cmdline()).casefold()
            return (runtime_pid == watcher and "kcos_stage3_watch_v2.py" in command and
                    process.create_time() >= float(runtime.get("launched_epoch", 0)) - 1.0)
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            return False
        except (psutil.AccessDenied, OSError, ValueError, IndexError):
            return True

    if lease_cycle == cycle_id:
        require(runtime.get("status") in {"START_REQUIRED", "STARTING", "OPTIMIZING",
                                           "INFRASTRUCTURE_FAILURE", "WATCHER_REGISTRATION_FAILED",
                                           "TIMEFRAME_COMPLETE", "COMPUTE_BUDGET_EXCEEDED",
                                           "STAGE3_COMPLETE_HELD", "ABANDONED", "LAUNCH_FAILED"},
                f"cannot classify current-cycle Stage 3 runtime as stale: {runtime.get('status')}")
        live_recorded_pids = []
        for raw_pid in recorded_pids:
            if not raw_pid:
                continue
            try:
                if owned_live_pid(int(raw_pid)):
                    live_recorded_pids.append(int(raw_pid))
            except (TypeError, ValueError):
                live_recorded_pids.append(raw_pid)
        require(not live_recorded_pids,
                f"current-cycle Stage 3 lease still has recorded live PIDs: {live_recorded_pids}")
        stamp = utc_now().replace("-", "").replace(":", "")
        archive = lease_path.with_name(f"{lease_path.stem}.removed.{stamp}{lease_path.suffix}")
        require(not archive.exists(), f"stale lease archive already exists: {archive}")
        atomic_bytes(archive, lease_path.read_bytes())
        require(sha256(archive) == sha256(lease_path), "stale lease archive hash mismatch")
        lease_path.unlink()
        return archive
    def authenticated_mt5_pid(pid: int) -> bool:
        try:
            process = psutil.Process(pid)
            name = (process.name() or "").lower()
            exe = str(process.exe() or "").lower()
            return name in {"terminal64.exe", "metatester64.exe"} or exe.endswith(("\\terminal64.exe", "/terminal64.exe", "\\metatester64.exe", "/metatester64.exe"))
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return False

    live_pids = [int(pid) for pid in recorded_pids if pid and authenticated_mt5_pid(int(pid))]
    require(not live_pids, f"old Stage 3 lease still owns authenticated MT5 processes: {live_pids}")
    terminal_states = {"TIMEFRAME_COMPLETE", "INFRASTRUCTURE_FAILURE", "STAGE3_COMPLETE_HELD", "ABANDONED"}
    require(runtime.get("status") in terminal_states or bool(recorded_pids),
            f"cannot prove old Stage 3 lease is terminal or abandoned: {runtime.get('status')}")
    stamp = utc_now().replace("-", "").replace(":", "")
    archive = lease_path.with_name(f"{lease_path.stem}.removed.{stamp}{lease_path.suffix}")
    require(not archive.exists(), f"stale lease archive already exists: {archive}")
    atomic_bytes(archive, lease_path.read_bytes())
    lease_path.unlink()
    return archive


def replace_tester_value(text: str, key: str, value: str) -> str:
    pattern = re.compile(rf"(?m)^{re.escape(key)}\s*=.*$")
    require(bool(pattern.search(text)), f"template is missing [Tester] {key}")
    return pattern.sub(lambda _match: f"{key}={value}", text, count=1)


def _optimization_intervals(start: float, step: float, stop: float) -> float:
    """Return a stable interval count for decimal INI domains."""
    if step == 0:
        return 0.0
    return round((stop - start) / step, 9)


def _integral_domain(config: dict, item: dict | None = None) -> bool:
    """Return true for authenticated integer/categorical TesterInputs."""
    values = []
    for key in ("default", "start", "step", "stop"):
        value = config.get(key) if isinstance(config, dict) else None
        if value is None and isinstance(item, dict):
            value = item.get(key)
        if value is None:
            return False
        try:
            number = float(value)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(number) or not number.is_integer():
            return False
        values.append(number)
    return True


def validate_tester_inputs(rendered: str, approved_spec: dict | None = None, grid_mode: str = "default",
                           *, validate_shared_surface: bool = True) -> None:
    """Reject stale/default-only inputs before they can reach the MT5 launcher."""
    if (validate_shared_surface and isinstance(approved_spec, dict)
            and isinstance(approved_spec.get("tester_config"), dict)):
        from kcos_tester_config_v2 import validate_shared_input_surface
        validate_shared_input_surface(rendered, approved_spec)
    source_types = _source_input_types(approved_spec or {})
    runtime_bounds = _runtime_source_bounds(approved_spec or {})
    def authenticated_regime_floor(name: str) -> float | None:
        """Return a Stage 1-authenticated positive floor for one regime input."""
        config = ((approved_spec or {}).get("tester_config", {})
                  if isinstance(approved_spec, dict) else {})
        constraints = config.get("input_constraints", []) if isinstance(config, dict) else []
        floors = []
        for constraint in constraints:
            if not isinstance(constraint, dict) or constraint.get("scope") != "single":
                continue
            coefficients = constraint.get("coefficients")
            if (constraint.get("operator") != ">=" or not isinstance(coefficients, dict)
                    or set(coefficients) != {name}):
                continue
            try:
                coefficient = float(coefficients[name])
                limit = float(constraint["limit"])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(coefficient) and coefficient > 0 and math.isfinite(limit):
                floors.append(limit / coefficient)
        return max(floors) if floors else None

    in_inputs = False
    input_count = 0
    ranged_count = 0
    for line in rendered.splitlines():
        stripped = line.strip()
        section = re.fullmatch(r"\[([^]]+)\]", stripped)
        if section:
            in_inputs = section.group(1).casefold() == "testerinputs"
            continue
        if not in_inputs or not stripped or stripped.startswith(";"):
            continue
        require("=" in stripped, f"invalid [TesterInputs] row: {stripped}")
        name, raw_value = (part.strip() for part in stripped.split("=", 1))
        fields = raw_value.split("||")
        if name in {"InpMinatoExpectedScoreVersion", "InpMinatoTestStart", "InpMinatoTestEnd"}:
            require(len(fields) == 1 and bool(raw_value),
                    f"[TesterInputs] {name} must be a plain fixed assignment")
            input_count += 1
            continue
        # Non-optimized MT5 inputs are scalar assignments. Optimized inputs
        # are rewritten by apply_config_surface() into the five-field form.
        if len(fields) == 1:
            require(bool(name) and bool(raw_value),
                    f"[TesterInputs] {name or '<unnamed>'} has an empty name/default")
            input_count += 1
            continue
        require(
            len(fields) == 5,
            f"[TesterInputs] {name} must use default||start||step||stop||Y/N fields",
        )
        default, start, step, stop, optimize = fields
        require(bool(name) and bool(default), f"[TesterInputs] {name or '<unnamed>'} has an empty name/default")
        require(optimize in {"Y", "N"}, f"[TesterInputs] {name} has invalid optimization flag {optimize!r}")
        if name.startswith("InpRegime_") and optimize == "Y" and grid_mode != "smoke":
            if grid_mode == "default":
                regime_ok = (float(start), float(step), float(stop)) == (0.0, 0.25, 5.0)
                # Some authenticated EA defaults (for example 0.125) fall
                # between the canonical zero-anchored 0.25 points. Keep the
                # same 0.25 step and [0,5] hard domain, but permit the aligned
                # 15-20 interval grid to shift phase so the executable default
                # remains an actual tested candidate.
                default_value = float(default)
                default_aligned_offset_grid = (
                    not regime_ok
                    and _default_grid_index(default_value, float(start), float(step), float(stop)) is not None
                    and _default_grid_index(default_value, 0.0, 0.25, 5.0) is None
                    and math.isclose(float(step), 0.25, rel_tol=0.0, abs_tol=1e-12)
                    and 0.0 <= float(start) <= default_value <= float(stop) <= 5.0
                    and 15.0 <= _optimization_intervals(float(start), float(step), float(stop)) <= 20.0
                )
                regime_ok = regime_ok or default_aligned_offset_grid
                floor = authenticated_regime_floor(name)
                # Stage 1 can prohibit zero exposure for a specific regime.
                # Honor that signed floor by accepting only the first 0.25
                # grid point at or above it; retain the same canonical step,
                # upper bound, and 15-20 interval wide surface.
                if (not regime_ok and name.endswith("_Lot_Mult") and floor is not None
                        and float(step) == 0.25 and float(stop) == 5.0
                        and float(start) >= floor - 1e-12
                        and float(start) - floor < 0.25 - 1e-12):
                    regime_ok = True
            else:
                regime_ok = (float(step) == 0.25 and 0.0 <= float(start) < float(stop) <= 3.0)
            require(regime_ok, f"[TesterInputs] {name} has invalid {grid_mode} regime grid")
        input_count += 1
        if optimize == "Y":
            try:
                default_value, start_value, step_value, stop_value = map(float, (default, start, step, stop))
            except ValueError:
                require(False, f"[TesterInputs] optimized parameter {name} has non-numeric range fields")
            require(all(math.isfinite(v) for v in (default_value, start_value, step_value, stop_value))
                    and step_value > 0 and start_value < stop_value,
                    f"[TesterInputs] optimized parameter {name} has no usable range")
            steps = _optimization_intervals(start_value, step_value, stop_value)
            authenticated_cfg = ((approved_spec or {}).get("tester_config", {}).get("inputs", {}).get(name, {})
                                 if isinstance(approved_spec, dict) else {})
            if isinstance(authenticated_cfg, dict) and authenticated_cfg.get("default") is not None:
                try:
                    authenticated_default = float(authenticated_cfg["default"])
                except (TypeError, ValueError):
                    authenticated_default = default_value
                require(math.isclose(default_value, authenticated_default, rel_tol=1e-10, abs_tol=1e-12),
                        f"[TesterInputs] {name} default {default_value:g} differs from authenticated default {authenticated_default:g}")
            require(_default_grid_index(default_value, start_value, step_value, stop_value) is not None,
                    f"[TesterInputs] {name} default {default_value:g} is not on optimization grid [{start_value:g},{step_value:g},{stop_value:g}]")
            typ = str(source_types.get(name, authenticated_cfg.get("type", ""))).lower()
            if typ in INTEGER_TYPES:
                require(all(v.is_integer() for v in (start_value, step_value, stop_value)),
                        f"[TesterInputs] integer parameter {name} has a fractional range")
            if name in runtime_bounds and not name.startswith("InpRegime_"):
                lo, hi = runtime_bounds[name]
                require(lo <= start_value < stop_value <= hi,
                        f"[TesterInputs] {name} escapes executable source bounds")
            discrete_values = _small_discrete_values(name, authenticated_cfg, approved_spec or {})
            discrete_default_in_domain = (discrete_values is not None and any(
                math.isclose(default_value, candidate, rel_tol=1e-10, abs_tol=1e-12)
                for candidate in discrete_values
            ))
            discrete_ok = False
            if discrete_default_in_domain and steps.is_integer():
                candidates = [round(start_value + index * step_value, 9)
                              for index in range(int(steps) + 1)]
                discrete_ok = bool(candidates) and all(v in discrete_values for v in candidates)
            require(discrete_ok if discrete_default_in_domain else
                    (grid_mode == "smoke"
                     or (10.0 <= steps <= 15.0 if grid_mode == "reduced" else 15.0 <= steps <= 20.0)),
                    f"[TesterInputs] optimized parameter {name} has {steps:g} steps; invalid for {grid_mode} profile")
            ranged_count += 1
    require(input_count > 0, "template is missing [TesterInputs] rows")
    require(ranged_count > 0, "template has no enabled [TesterInputs] optimization ranges")
    validate_input_constraints(rendered, approved_spec)


def validate_input_constraints(rendered: str, approved_spec: dict | None) -> None:
    """Check optional Stage 1 linear constraints across the whole MT5 grid.

    A constraint uses ``tester_config.input_constraints`` with a coefficient
    map, operator (``<=``, ``<``, ``>=``, or ``>``), and numeric limit. Since
    MT5 optimizes each input independently, testing the worst grid corner
    prevents a row-wise-valid grid from generating invalid combinations.
    """
    config = ((approved_spec or {}).get("tester_config", {})
              if isinstance(approved_spec, dict) else {})
    constraints = config.get("input_constraints", []) if isinstance(config, dict) else []
    require(isinstance(constraints, list), "tester_config input_constraints must be a list")
    if not constraints:
        return
    input_specs = config.get("inputs", {})
    ranges = {}
    in_inputs = False
    for line in rendered.splitlines():
        stripped = line.strip()
        section = re.fullmatch(r"\[([^]]+)\]", stripped)
        if section:
            in_inputs = section.group(1).casefold() == "testerinputs"
            continue
        if not in_inputs or "=" not in stripped or stripped.startswith(";"):
            continue
        name, value = (part.strip() for part in stripped.split("=", 1))
        fields = value.split("||")
        if len(fields) == 1:
            try:
                default = float(fields[0])
            except ValueError:
                continue
            ranges[name] = (default, default, default)
            continue
        if len(fields) != 5:
            continue
        try:
            default = float(fields[0])
            if fields[4].upper() == "Y":
                low, high = float(fields[1]), float(fields[3])
            else:
                low = high = default
        except ValueError:
            continue
        ranges[name] = (low, high, default)
    for index, constraint in enumerate(constraints):
        require(isinstance(constraint, dict), f"input constraint #{index + 1} must be an object")
        label = str(constraint.get("id", f"constraint_{index + 1}"))
        coefficients = constraint.get("coefficients")
        operator = constraint.get("operator")
        try:
            limit = float(constraint["limit"])
        except (KeyError, TypeError, ValueError):
            require(False, f"input constraint {label} has an invalid numeric limit")
        require(isinstance(coefficients, dict) and coefficients,
                f"input constraint {label} requires a non-empty coefficients map")
        require(operator in {"<=", "<", ">=", ">"},
                f"input constraint {label} has unsupported operator {operator!r}")
        min_value = max_value = default_value = 0.0
        for name, raw_coefficient in coefficients.items():
            require(name in input_specs and name in ranges,
                    f"input constraint {label} references missing input {name}")
            try:
                coefficient = float(raw_coefficient)
            except (TypeError, ValueError):
                require(False, f"input constraint {label} has invalid coefficient for {name}")
            require(math.isfinite(coefficient),
                    f"input constraint {label} has non-finite coefficient for {name}")
            low, high, default = ranges[name]
            min_value += coefficient * (low if coefficient >= 0 else high)
            max_value += coefficient * (high if coefficient >= 0 else low)
            default_value += coefficient * default
        if operator in {"<=", "<"}:
            worst = max_value
            valid = worst <= limit + 1e-12 if operator == "<=" else worst < limit - 1e-12
            default_valid = default_value <= limit + 1e-12 if operator == "<=" else default_value < limit - 1e-12
        else:
            worst = min_value
            valid = worst >= limit - 1e-12 if operator == ">=" else worst > limit + 1e-12
            default_valid = default_value >= limit - 1e-12 if operator == ">=" else default_value > limit + 1e-12
        require(default_valid,
                f"input constraint {label} rejects EA defaults: value {default_value:g} {operator} {limit:g} is false")
        require(valid,
                f"input constraint {label} rejects optimization grid: worst value {worst:g} {operator} {limit:g} is false")


def constrain_input_grid_ranges(rendered: str, approved_spec: dict | None,
                                grid_mode: str = "default") -> str:
    """Trim adverse grid edges and recover interval width on safe edges.

    Stage 1 ranges are estimates. If independent MT5 ranges would create
    invalid combinations, this contracts their adverse endpoints toward the
    authenticated defaults in proportion to each input's available slack.
    If that contraction leaves an input below the required interval count,
    recover only the favorable edge within authenticated Stage 1/source bounds.
    The existing aligner rebuilds the typed default-anchored grid afterward.
    """
    config = ((approved_spec or {}).get("tester_config", {})
              if isinstance(approved_spec, dict) else {})
    constraints = config.get("input_constraints", []) if isinstance(config, dict) else []
    if not constraints:
        return rendered
    def read_ranges(text: str) -> dict[str, tuple[float, float, float, float, str]]:
        ranges = {}
        in_inputs = False
        for line in text.splitlines():
            stripped = line.strip()
            section = re.fullmatch(r"\[([^]]+)\]", stripped)
            if section:
                in_inputs = section.group(1).casefold() == "testerinputs"
                continue
            if not in_inputs or "=" not in stripped or stripped.startswith(";"):
                continue
            name, value = (part.strip() for part in stripped.split("=", 1))
            fields = value.split("||")
            if len(fields) == 1:
                # Scalar TesterInputs rows are fixed defaults. Constraints
                # still apply to them; omitting one makes an authenticated
                # rule look as though it references a nonexistent input.
                try:
                    default = float(fields[0])
                except ValueError:
                    continue
                ranges[name] = (default, default, 0.0, default, "N")
                continue
            if len(fields) != 5 or fields[4].upper() not in {"Y", "N"}:
                continue
            try:
                default, start, step, stop = map(float, fields[:4])
            except ValueError:
                continue
            ranges[name] = (default, start, step, stop, fields[4].upper())
        return ranges

    def constraint_slacks(text: str) -> list[float]:
        parsed = read_ranges(text)
        slacks = []
        for constraint in constraints:
            operator = constraint["operator"]
            worst = 0.0
            for name, raw_coefficient in constraint["coefficients"].items():
                coefficient = float(raw_coefficient)
                default, start, _step, stop, optimize = parsed[name]
                low, high = (start, stop) if optimize == "Y" else (default, default)
                endpoint = ((high if coefficient >= 0 else low)
                            if operator in {"<=", "<"} else
                            (low if coefficient >= 0 else high))
                worst += coefficient * endpoint
            limit = float(constraint["limit"])
            slacks.append(limit - worst if operator in {"<=", "<"} else worst - limit)
        return slacks

    input_specs = config.get("inputs", {}) if isinstance(config, dict) else {}
    source_bounds = _runtime_source_bounds(approved_spec or {})

    def approved_bounds(name: str) -> tuple[float, float] | None:
        cfg = input_specs.get(name, {}) if isinstance(input_specs, dict) else {}
        if not isinstance(cfg, dict):
            return None
        try:
            low, high = float(cfg["start"]), float(cfg["stop"])
        except (KeyError, TypeError, ValueError):
            return None
        if name in source_bounds:
            low, high = max(low, source_bounds[name][0]), min(high, source_bounds[name][1])
        if not (math.isfinite(low) and math.isfinite(high) and low < high):
            return None
        return low, high

    result = rendered
    for _iteration in range(max(1, len(constraints) * 3)):
        changed = False
        ranges = read_ranges(result)
        for index, constraint in enumerate(constraints):
            require(isinstance(constraint, dict), f"input constraint #{index + 1} must be an object")
            label = str(constraint.get("id", f"constraint_{index + 1}"))
            coefficients = constraint.get("coefficients")
            operator = constraint.get("operator")
            try:
                limit = float(constraint["limit"])
            except (KeyError, TypeError, ValueError):
                require(False, f"input constraint {label} has an invalid numeric limit")
            require(isinstance(coefficients, dict) and bool(coefficients),
                    f"input constraint {label} requires a non-empty coefficients map")
            require(operator in {"<=", "<", ">=", ">"},
                    f"input constraint {label} has unsupported operator {operator!r}")

            default_value = 0.0
            worst_value = 0.0
            terms = []
            for name, raw_coefficient in coefficients.items():
                require(name in ranges,
                        f"input constraint {label} references missing TesterInputs row {name}")
                try:
                    coefficient = float(raw_coefficient)
                except (TypeError, ValueError):
                    require(False, f"input constraint {label} has invalid coefficient for {name}")
                require(math.isfinite(coefficient),
                        f"input constraint {label} has non-finite coefficient for {name}")
                default, start, step, stop, optimize = ranges[name]
                low, high = (start, stop) if optimize == "Y" else (default, default)
                default_value += coefficient * default
                worst_endpoint = (high if coefficient >= 0 else low) if operator in {"<=", "<"} else (
                    low if coefficient >= 0 else high)
                worst_value += coefficient * worst_endpoint
                if optimize == "Y" and coefficient != 0:
                    capacity = (coefficient * (worst_endpoint - default)
                                if operator in {"<=", "<"}
                                else coefficient * (default - worst_endpoint))
                    if capacity > 0:
                        terms.append((name, coefficient, default, start, step, stop,
                                      optimize, capacity, worst_endpoint))

            default_valid = (default_value <= limit + 1e-12 if operator == "<=" else
                             default_value < limit - 1e-12 if operator == "<" else
                             default_value >= limit - 1e-12 if operator == ">=" else
                             default_value > limit + 1e-12)
            require(default_valid,
                    f"input constraint {label} rejects EA defaults: {default_value:g} {operator} {limit:g} is false")
            grid_valid = (worst_value <= limit + 1e-12 if operator == "<=" else
                          worst_value < limit - 1e-12 if operator == "<" else
                          worst_value >= limit - 1e-12 if operator == ">=" else
                          worst_value > limit + 1e-12)
            if grid_valid:
                continue

            # Match the validator's strict-inequality tolerance. A larger
            # arbitrary margin can be impossible when authenticated defaults
            # sit close to (but still satisfy) the declared boundary.
            margin = 1e-12 if operator in {"<", ">"} else 0.0
            target = limit - margin if operator in {"<=", "<"} else limit + margin
            reduction_needed = (worst_value - target if operator in {"<=", "<"}
                                else target - worst_value)
            total_capacity = sum(term[7] for term in terms)
            require(total_capacity + 1e-12 >= reduction_needed and total_capacity > 0,
                    f"input constraint {label} cannot be satisfied while preserving EA defaults")
            remaining_fraction = max(0.0, 1.0 - reduction_needed / total_capacity)
            for name, coefficient, default, start, step, stop, _optimize, _capacity, endpoint in terms:
                proposed = default + (endpoint - default) * remaining_fraction
                # Snap inward to the current grid so no range expansion or
                # unsupported MT5 candidate is introduced by floating point.
                if endpoint > default:
                    snapped = start + math.floor((proposed - start) / step + 1e-10) * step
                    new_start, new_stop = start, max(default, min(stop, snapped))
                else:
                    snapped = start + math.ceil((proposed - start) / step - 1e-10) * step
                    new_start, new_stop = min(default, max(start, snapped)), stop
                require(new_start <= default <= new_stop,
                        f"input constraint {label} cannot retain the default for {name}")
                if not math.isclose(new_start, start, abs_tol=1e-12) or not math.isclose(new_stop, stop, abs_tol=1e-12):
                    row = f"{_clean_ini_number(default)}||{_clean_ini_number(new_start)}||{_clean_ini_number(step)}||{_clean_ini_number(new_stop)}||Y"
                    result = re.sub(rf"(?im)^({re.escape(name)}=)[^\r\n]*$",
                                    lambda match, row=row: match.group(1) + row, result, count=1)
                    changed = True
            if changed:
                # Constraint trimming can make an integer grid too short even
                # when the signed inequality leaves room to extend its safe
                # edge. Restore only the minimum width needed, and only when
                # no declared constraint loses slack as a result.
                minimum_intervals = 10 if grid_mode == "reduced" else 15
                current_ranges = read_ranges(result)
                for name, raw_coefficient in coefficients.items():
                    default, start, step, stop, optimize = current_ranges[name]
                    intervals = _optimization_intervals(start, step, stop)
                    if optimize != "Y" or intervals >= minimum_intervals or step <= 0:
                        continue
                    coefficient = float(raw_coefficient)
                    favorable_low = ((operator in {"<=", "<"} and coefficient > 0)
                                     or (operator in {">=", ">"} and coefficient < 0))
                    bounds = approved_bounds(name)
                    if bounds is None:
                        continue
                    deficit = minimum_intervals - math.floor(intervals + 1e-9)
                    if favorable_low:
                        candidate_start = max(bounds[0], start - deficit * step)
                        candidate_stop = stop
                    else:
                        candidate_start = start
                        candidate_stop = min(bounds[1], stop + deficit * step)
                    if (candidate_start >= start - 1e-12 and candidate_stop <= stop + 1e-12):
                        continue
                    trial_row = (f"{name}={_clean_ini_number(default)}||"
                                 f"{_clean_ini_number(candidate_start)}||"
                                 f"{_clean_ini_number(step)}||"
                                 f"{_clean_ini_number(candidate_stop)}||Y")
                    trial = re.sub(rf"(?im)^({re.escape(name)}=)[^\r\n]*$",
                                   lambda match, row=trial_row: row, result, count=1)
                    if trial == result:
                        continue
                    try:
                        trial_slacks = constraint_slacks(trial)
                    except (KeyError, TypeError, ValueError):
                        continue
                    if all((after >= before - 1e-9 if before < -1e-9
                            else after >= -1e-9)
                           for before, after in zip(constraint_slacks(result), trial_slacks)):
                        result = trial
                        break
                result = align_optimized_defaults(result, approved_spec, grid_mode)
                break
        if not changed:
            break
    return result


def _source_paths(approved_spec: dict) -> list[Path]:
    """Resolve Stage 1 source references in either legacy-string or object form."""
    paths = []
    entries = approved_spec.get("source_paths", []) if isinstance(approved_spec, dict) else []
    for entry in entries if isinstance(entries, list) else []:
        raw_path = entry.get("path") if isinstance(entry, dict) else entry
        if isinstance(raw_path, (str, os.PathLike)) and str(raw_path).strip():
            paths.append(Path(raw_path))
    return paths


def _runtime_source_bounds(approved_spec: dict) -> dict[str, tuple[float, float]]:
    """Extract executable OnInit bounds, not the narrow Stage 2 annotations."""
    bounds: dict[str, tuple[float, float]] = {}
    numeric = r"([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)"
    guard = re.compile(
        rf"if\s*\(\s*(Inp\w+)\s*<\s*{numeric}\s*\|\|\s*\1\s*>\s*{numeric}"
    )
    for source_path in _source_paths(approved_spec):
        if not source_path.is_file():
            continue
        source = source_path.read_text(encoding="utf-8", errors="ignore")
        for match in guard.finditer(source):
            name, lo, hi = match.group(1), float(match.group(2)), float(match.group(3))
            prior = bounds.get(name)
            bounds[name] = (max(lo, prior[0]), min(hi, prior[1])) if prior else (lo, hi)
    return bounds


INTEGER_TYPES = {"int", "uint", "ulong", "long", "short", "uchar", "ushort"}


def _source_input_types(approved_spec: dict) -> dict[str, str]:
    types = {}
    for path in _source_paths(approved_spec):
        if path.is_file():
            source = path.read_text(encoding="utf-8-sig", errors="replace")
            types.update((name, typ) for typ, name in
                         re.findall(r"(?m)^\s*input\s+(\w+)\s+(\w+)\s*=", source))
    return types


def validate_optimized_input_source_contract(approved_config: dict, source_text: str) -> None:
    """Require Stage 1 optimized defaults and types to match EA declarations.

    The source declaration is the executable authority for the compiled input
    surface. Stage 1 may suggest search bounds, but a stale/default or wrong
    type must not be used to describe what "wide" means for that EA.
    """
    declarations: dict[str, tuple[str, str]] = {}
    declaration_re = re.compile(
        r"(?m)^\s*input\s+(\w+)\s+([A-Za-z_]\w*)\s*=\s*([^;]+);"
    )
    for match in declaration_re.finditer(source_text):
        source_type, name, raw_default = match.groups()
        raw_default = raw_default.split("//", 1)[0].strip()
        if name in declarations:
            raise Stage3InputContractError(
                "SOURCE_INPUT_DECLARATION_AMBIGUOUS",
                f"optimized input {name} has multiple EA declarations",
            )
        declarations[name] = (source_type.lower(), raw_default)

    integer_types = INTEGER_TYPES | {"integer", "byte", "sbyte"}
    real_types = {"double", "float", "real"}
    boolean_types = {"bool", "boolean"}

    def family(value: str) -> str:
        normalized = value.lower()
        if normalized in integer_types:
            return "integer"
        if normalized in real_types:
            return "real"
        if normalized in boolean_types:
            return "boolean"
        return normalized

    number_re = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?[fF]?$")
    errors = []
    for name, cfg in (approved_config.get("inputs", {}) or {}).items():
        if not isinstance(cfg, dict) or str(cfg.get("optimize", "N")).upper() != "Y":
            continue
        declaration = declarations.get(name)
        if declaration is None:
            errors.append(f"{name}: optimized by Stage 1 but has no EA input declaration")
            continue
        source_type, source_default = declaration
        declared_type = str(cfg.get("type", "")).strip().lower()
        if declared_type and family(declared_type) != family(source_type):
            errors.append(
                f"{name}: Stage 1 type {declared_type} conflicts with EA type {source_type}"
            )
        source_is_bool = family(source_type) == "boolean"
        if source_is_bool:
            source_value = source_default.lower()
            if source_value not in {"true", "false", "0", "1"}:
                errors.append(f"{name}: cannot verify EA boolean default {source_default!r}")
                continue
            executable_default = 1.0 if source_value in {"true", "1"} else 0.0
            try:
                stage1_raw = cfg["default"]
                if isinstance(stage1_raw, bool):
                    stage1_default = float(stage1_raw)
                elif str(stage1_raw).lower() in {"true", "false"}:
                    stage1_default = 1.0 if str(stage1_raw).lower() == "true" else 0.0
                else:
                    stage1_default = float(stage1_raw)
            except (KeyError, TypeError, ValueError):
                errors.append(f"{name}: Stage 1 boolean default is missing or invalid")
                continue
        elif not number_re.fullmatch(source_default):
            errors.append(
                f"{name}: cannot verify non-literal EA default {source_default!r}; "
                "Stage 1 optimized default must be reviewed against source"
            )
            continue
        else:
            try:
                executable_default = float(source_default.rstrip("fF"))
                stage1_default = float(cfg["default"])
            except (KeyError, TypeError, ValueError):
                errors.append(f"{name}: Stage 1 default is missing or non-numeric")
                continue
        if (not math.isfinite(executable_default) or not math.isfinite(stage1_default)
                or not math.isclose(executable_default, stage1_default, rel_tol=1e-12, abs_tol=1e-15)):
            errors.append(
                f"{name}: Stage 1 default {cfg.get('default')!r} conflicts with "
                f"EA default {source_default}"
            )
    if errors:
        raise Stage3InputContractError(
            "SOURCE_INPUT_CONTRACT_MISMATCH", "; ".join(errors)
        )


def build_input_surface_audit(approved_config: dict, source_text: str,
                              rendered_by_tf: dict[str, str], *, cycle_id: str,
                              ea: str, source_path: str, source_sha256: str,
                              stage1_spec: str, stage1_spec_sha256: str,
                              config_sha256: str,
                              ini_sha256_by_tf: dict[str, str] | None = None) -> dict:
    """Record source defaults separately from the Stage 3 search grid.

    The first numeric field in MT5's TesterInputs tuple is the EA default;
    start/step/stop describe the search domain. This report makes that
    distinction explicit and binds it to the source, Stage 1 contract, and
    exact per-timeframe INI bytes.
    """
    # Keep the audit builder fail-closed on its own. It can also be used by
    # diagnostics and tests outside the normal prepare path, so do not let it
    # describe a source/default or a signed grid that the runtime validators
    # would reject.
    validate_optimized_input_source_contract(approved_config, source_text)
    for rendered in rendered_by_tf.values():
        validate_input_constraints(rendered, {"tester_config": approved_config})
    declarations = {}
    declaration_re = re.compile(
        r"(?m)^\s*input\s+(\w+)\s+([A-Za-z_]\w*)\s*=\s*([^;]+);"
    )
    for match in declaration_re.finditer(source_text):
        source_type, name, raw_default = match.groups()
        raw_default = raw_default.split("//", 1)[0].strip()
        declarations[name] = (source_type.lower(), raw_default)

    input_configs = approved_config.get("inputs", {})
    source_bounds = _runtime_source_bounds(approved_config)

    def numeric_input_default(value, input_type: str) -> float:
        if input_type.lower() in {"bool", "boolean"}:
            if isinstance(value, bool):
                return float(value)
            normalized = str(value).strip().lower()
            if normalized in {"true", "1"}:
                return 1.0
            if normalized in {"false", "0"}:
                return 0.0
            raise ValueError("invalid boolean input default")
        return float(str(value).rstrip("fF"))

    by_timeframe = {}
    for timeframe, rendered in rendered_by_tf.items():
        rows = {}
        in_inputs = False
        for raw_line in rendered.splitlines():
            stripped = raw_line.strip()
            section = re.fullmatch(r"\[([^]]+)\]", stripped)
            if section:
                in_inputs = section.group(1).casefold() == "testerinputs"
                continue
            if not in_inputs or "=" not in raw_line or stripped.startswith(";"):
                continue
            name, raw_value = (part.strip() for part in raw_line.split("=", 1))
            rows[name] = raw_value.split("||")

        audited_inputs = {}
        for name, cfg in input_configs.items():
            if not isinstance(cfg, dict) or str(cfg.get("optimize", "N")).upper() != "Y":
                continue
            require(name in declarations,
                    f"input audit: optimized input {name} is absent from EA source")
            require(name in rows and len(rows[name]) == 5 and rows[name][4].upper() == "Y",
                    f"input audit: optimized input {name} is absent or malformed in {timeframe} INI")
            source_type, source_default = declarations[name]
            try:
                source_default_num = numeric_input_default(source_default, source_type)
                stage1_default_num = numeric_input_default(cfg["default"], source_type)
                ini_default, start, step, stop = map(float, rows[name][:4])
            except (KeyError, TypeError, ValueError) as exc:
                raise Stage3InputContractError(
                    "INPUT_SURFACE_AUDIT_INVALID",
                    f"{name} has a non-numeric or missing default/grid in {timeframe}",
                ) from exc
            require(all(math.isfinite(value) for value in
                        (source_default_num, stage1_default_num, ini_default, start, step, stop)),
                    f"input audit: {name} contains a non-finite value in {timeframe}")
            require(math.isclose(source_default_num, stage1_default_num, rel_tol=1e-12, abs_tol=1e-15)
                    and math.isclose(source_default_num, ini_default, rel_tol=1e-12, abs_tol=1e-15),
                    f"input audit: {name} default does not match EA source in {timeframe}")
            require(step > 0 and start < stop and start <= source_default_num <= stop,
                    f"input audit: {name} has an invalid grid in {timeframe}")
            intervals = _optimization_intervals(start, step, stop)
            discrete_values = _small_discrete_values(
                name, cfg, {"tester_config": approved_config,
                            "source_paths": [source_path]})
            require(discrete_values is not None or 15.0 <= intervals <= 20.0,
                    f"input audit: {name} has {intervals:g} intervals in {timeframe}; Stage 3 wide requires 15-20")
            require(_default_grid_index(source_default_num, start, step, stop) is not None,
                    f"input audit: {name} EA default is not a candidate in {timeframe}")
            default_grid_index = _default_grid_index(source_default_num, start, step, stop)
            provenance = cfg.get("domain_provenance", {}) if isinstance(cfg, dict) else {}
            stage2_derived_base_lot = (
                name == "InpBaseLot"
                and isinstance(provenance, dict)
                and provenance.get("method") == "kcos_stage2_gjr_base_lot"
            )
            estimate = None
            if (not stage2_derived_base_lot
                    and cfg.get("start") is not None and cfg.get("stop") is not None):
                estimate = [float(cfg["start"]), float(cfg["stop"])]
            bounds = source_bounds.get(name)
            domain_constraints = [
                str(rule.get("id", f"constraint_{index + 1}"))
                for index, rule in enumerate(approved_config.get("input_constraints", []))
                if isinstance(rule, dict)
                and name in (rule.get("coefficients") or {})
            ]
            if start < 0 < stop:
                search_sign_domain = "crosses_zero"
            elif start == 0 < stop:
                search_sign_domain = "nonnegative"
            elif start < stop == 0:
                search_sign_domain = "nonpositive"
            elif start > 0:
                search_sign_domain = "positive"
            elif stop < 0:
                search_sign_domain = "negative"
            else:
                search_sign_domain = "zero_only"
            audited_inputs[name] = {
                "type": source_type,
                "ea_source_default_literal": source_default,
                "ea_source_default": source_default_num,
                "stage1_default": None if stage2_derived_base_lot else stage1_default_num,
                "stage2_derived_default": stage1_default_num if stage2_derived_base_lot else None,
                "search_start": start,
                "search_step": step,
                "search_stop": stop,
                "interval_count": intervals,
                "search_crosses_zero": start < 0 < stop,
                "search_sign_domain": search_sign_domain,
                "stage1_estimated_bounds": estimate,
                "stage2_derived_bounds": (
                    [float(cfg["start"]), float(cfg["step"]), float(cfg["stop"])]
                    if stage2_derived_base_lot else None
                ),
                "executable_source_bounds": list(bounds) if bounds else None,
                "domain_basis": (
                    "stage2_derived_base_lot_grid" if stage2_derived_base_lot else
                    "stage1_estimate_and_input_constraints" if domain_constraints else
                    "stage1_estimate"
                ),
                "applicable_input_constraints": domain_constraints,
                "ea_default_grid_index": default_grid_index,
                "ea_default_grid_position": (source_default_num - start) / (stop - start),
                "ea_default_interval_distance_to_nearest_edge": min(
                    (source_default_num - start) / step,
                    (stop - source_default_num) / step,
                ),
                "default_is_optimization_candidate": True,
            }
        by_timeframe[timeframe] = {
            "ini_sha256": ((ini_sha256_by_tf or {}).get(timeframe)
                           or hashlib.sha256(rendered.encode("utf-8")).hexdigest().upper()),
            "optimized_inputs": audited_inputs,
        }
    timing_controls = {}
    for name, cfg in input_configs.items():
        kind = is_timing_control_input(name, cfg)
        if kind is None:
            continue
        require(str(cfg.get("optimize", "N")).upper() == "N" and
                float(cfg.get("default", -1)) == 0.0,
                f"input audit: {name} timing control is not fixed to zero")
        values_by_tf = {}
        for timeframe, rendered in rendered_by_tf.items():
            row = re.search(rf"(?im)^\s*{re.escape(name)}\s*=([^\r\n]+)$", rendered)
            require(row is not None and row.group(1).strip() == "0",
                    f"input audit: {name} must be a scalar zero in {timeframe}")
            values_by_tf[timeframe] = 0
        timing_controls[name] = {
            "kind": kind,
            "runtime_value": 0,
            "optimization": "disabled",
            "previous_stage2_contract": cfg.get("domain_provenance", {}).get(
                "previous_stage2_contract"),
            "tester_values_by_timeframe": values_by_tf,
        }
    return {
        "schema_version": 2,
        "cycle_id": cycle_id,
        "ea": ea,
        "policy": "Stage 1 remains immutable; Stage 2 derives and Stage 3 optimizes the GJR base lot on its bounded grid before regime scaling; time-stop and cooldown controls are fixed to zero; regime lot multipliers are optimized independently without an ordering rule; remaining explicit coupled constraints and executable source bounds are enforced before launch.",
        "optimizer_policy_overrides": approved_config.get("optimizer_policy_overrides", []),
        "timing_controls": timing_controls,
        "source": {"path": str(Path(source_path).resolve()), "sha256": source_sha256},
        "stage1_spec": {"path": str(Path(stage1_spec).resolve()), "sha256": stage1_spec_sha256,
                        "tester_config_sha256": config_sha256},
        "timeframes": by_timeframe,
    }


def _small_discrete_values(name: str, config: dict, approved_spec: dict) -> set[float] | None:
    """Allow fewer steps only for an explicitly bounded discrete domain."""
    typ = str(_source_input_types(approved_spec).get(name, config.get("type", ""))).lower()
    if typ == "bool":
        return {0.0, 1.0}
    if typ in INTEGER_TYPES:
        bounds = _runtime_source_bounds(approved_spec).get(name)
        if bounds:
            lo, hi = math.ceil(bounds[0]), math.floor(bounds[1])
            if 0 < hi - lo < 15:
                return {float(v) for v in range(lo, hi + 1)}
        else:
            # If the EA declares no executable min/max, the hash-bound Stage 1
            # tester domain is the only authenticated search range. Treat a
            # small integral range as exhaustive instead of demanding a wide
            # grid that cannot fit inside it.
            try:
                start = float(config["start"])
                step = float(config["step"])
                stop = float(config["stop"])
            except (KeyError, TypeError, ValueError):
                start = step = stop = math.nan
            if (all(math.isfinite(value) for value in (start, step, stop))
                    and step >= 1
                    and all(math.isclose(value, round(value), rel_tol=0, abs_tol=1e-9)
                            for value in (start, step, stop))):
                lo, stride, hi = int(round(start)), int(round(step)), int(round(stop))
                intervals, remainder = divmod(hi - lo, stride)
                if 0 < intervals < 15 and remainder == 0:
                    return {float(value) for value in range(lo, hi + 1, stride)}
    # Enum/categorical exceptions need enumerated values, rather than a short
    # Stage 2 numeric suggestion being treated as a declared domain.
    if typ in {"enum", "categorical"} or typ.startswith("enum_"):
        declared = config.get("enum_values", config.get("values"))
        if isinstance(declared, list) and 1 < len(declared) <= 15:
            try:
                values = {float(v) for v in declared}
                if all(math.isfinite(v) for v in values):
                    return values
            except (TypeError, ValueError):
                pass
    return None


def _legal_wide_grid(lo: float, hi: float, typ: str,
                     minimum_intervals: int = 15, maximum_intervals: int = 20) -> tuple[float, float, float]:
    """Cover the executable domain with a clean 15–20 interval grid."""
    span = hi - lo
    integral = typ.lower() in INTEGER_TYPES
    require(math.isfinite(lo) and math.isfinite(hi) and lo < hi,
            "executable optimization domain must be finite and increasing")
    if integral:
        lo, hi = float(math.ceil(lo)), float(math.floor(hi))
        span = hi - lo
        require(span >= 1, "integer optimization domain has fewer than two legal values")
        if span < minimum_intervals:
            return lo, 1.0, hi
    candidates = []
    exponent = math.floor(math.log10(span / maximum_intervals)) if span > 0 else 0
    for e in range(exponent - 1, exponent + 4):
        scale = 10.0 ** e
        candidates.extend(multiplier * scale for multiplier in
                          (1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 7.5, 8.0, 10.0))
    for step in sorted(set(candidates)):
        if integral:
            step = max(1.0, round(step))
        intervals = math.floor(span / step + 1e-9)
        if minimum_intervals <= intervals <= maximum_intervals:
            return lo, step, lo + intervals * step
    step = span / minimum_intervals
    if integral:
        step = max(1.0, math.floor(step))
    intervals = min(maximum_intervals, math.floor(span / step + 1e-9))
    require(intervals >= minimum_intervals, "no legal integral optimization grid meets interval policy")
    return lo, step, lo + intervals * step


def _default_grid_index(default: float, start: float, step: float, stop: float) -> float | None:
    """Return the candidate index when a default is represented by a uniform grid."""
    if step <= 0 or default < start - 1e-9 or default > stop + 1e-9:
        return None
    index = (default - start) / step
    nearest = round(index)
    if abs(index - nearest) > 1e-8:
        return None
    if nearest < 0 or nearest > round(_optimization_intervals(start, step, stop)):
        return None
    return float(nearest)


def align_optimized_defaults(rendered: str, approved_spec: dict | None = None,
                             grid_mode: str = "default") -> str:
    """Keep each numeric EA default as an actual optimization candidate.

    Stage 1 bounds are estimates for Stage 3's wide pass. When the chosen step
    misses the compiled EA default, shift the grid within its existing bounds
    or choose a nearby legal step. Signed domains stay signed; no positivity
    assumptions are made from an input's name.
    """
    if grid_mode == "reduced":
        minimum_intervals, maximum_intervals = 10, 15
    else:
        minimum_intervals, maximum_intervals = 15, 20
    inputs = ((approved_spec or {}).get("tester_config", {}).get("inputs", {})
              if isinstance(approved_spec, dict) else {})
    source_types = _source_input_types(approved_spec or {})
    in_inputs = False
    output = []
    for raw in rendered.splitlines():
        stripped = raw.strip()
        section = re.fullmatch(r"\[([^]]+)\]", stripped)
        if section:
            in_inputs = section.group(1).casefold() == "testerinputs"
            output.append(raw)
            continue
        if not in_inputs or "=" not in raw or stripped.startswith(";"):
            output.append(raw)
            continue
        name, value = (part.strip() for part in raw.split("=", 1))
        fields = value.split("||")
        if len(fields) != 5 or fields[-1].upper() != "Y":
            output.append(raw)
            continue
        try:
            default, start, step, stop = map(float, fields[:4])
        except ValueError:
            output.append(raw)
            continue
        cfg = inputs.get(name, {})
        cfg_default = cfg.get("default") if isinstance(cfg, dict) else None
        if cfg_default is not None:
            try:
                approved_default = float(cfg_default)
            except (TypeError, ValueError):
                approved_default = default
            require(math.isclose(default, approved_default, rel_tol=1e-10, abs_tol=1e-12),
                    f"{name} TesterInputs default {default:g} differs from authenticated EA default {approved_default:g}")
        typ = str(source_types.get(name, cfg.get("type", "double"))).lower()
        current_intervals = _optimization_intervals(start, step, stop)
        integral = typ in INTEGER_TYPES
        if (_default_grid_index(default, start, step, stop) is not None
                and minimum_intervals <= current_intervals <= maximum_intervals
                and (not integral or all(value.is_integer() for value in (start, step, stop)))):
            output.append(raw)
            continue
        require(start <= default <= stop,
                f"{name} EA default {default:g} is outside optimization bounds [{start:g},{stop:g}]")

        # Small authenticated discrete domains are exhaustive searches, not
        # undersized wide grids. Keep every legal value and the EA default;
        # do not force 15 intervals into a source-bounded integer domain that
        # contains fewer than 16 values.
        small_values = _small_discrete_values(name, cfg, approved_spec or {})
        if small_values is not None:
            values = sorted(v for v in small_values if start - 1e-9 <= v <= stop + 1e-9)
            deltas = [right - left for left, right in zip(values, values[1:])]
            uniform = bool(deltas) and all(math.isclose(delta, deltas[0], rel_tol=1e-10, abs_tol=1e-12)
                                           for delta in deltas)
            if (uniform and len(values) - 1 <= maximum_intervals
                    and any(math.isclose(default, value, rel_tol=1e-10, abs_tol=1e-12)
                            for value in values)):
                output.append(
                    f"{name}={_clean_ini_number(default)}||{_clean_ini_number(values[0])}||"
                    f"{_clean_ini_number(deltas[0])}||{_clean_ini_number(values[-1])}||Y"
                )
                continue

        span = stop - start
        step_candidates = {step}
        if name.startswith("InpRegime_") and grid_mode == "default":
            # The default regime policy fixes the candidate spacing at 0.25.
            # Rephase within the signed [0, 5] domain when needed; do not let
            # the generic aligner select a nearby step to improve coverage.
            step_candidates = {0.25}
        if not (name.startswith("InpRegime_") and grid_mode == "default"):
            exponent = math.floor(math.log10(span / maximum_intervals)) if span > 0 else 0
            for power in range(exponent - 1, exponent + 4):
                scale = 10.0 ** power
                step_candidates.update(multiplier * scale for multiplier in
                                       (1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 7.5, 8.0, 10.0))

        candidates = []
        integral = typ in INTEGER_TYPES
        for candidate_step in step_candidates:
            if integral:
                candidate_step = float(round(candidate_step))
                if candidate_step < 1:
                    continue
            if candidate_step <= 0 or not math.isfinite(candidate_step):
                continue
            for intervals in range(minimum_intervals, maximum_intervals + 1):
                min_offset = math.ceil((start - default) / candidate_step - 1e-9)
                max_offset = math.floor((stop - default - intervals * candidate_step) / candidate_step + 1e-9)
                low_offset, high_offset = max(min_offset, -intervals), min(max_offset, 0)
                for offset in range(low_offset, high_offset + 1):
                    candidate_start = default + offset * candidate_step
                    candidate_stop = candidate_start + intervals * candidate_step
                    if candidate_start < start - 1e-8 or candidate_stop > stop + 1e-8:
                        continue
                    coverage = intervals * candidate_step
                    endpoint_loss = (candidate_start - start) + (stop - candidate_stop)
                    candidates.append((coverage, -endpoint_loss, -abs(candidate_step - step),
                                       candidate_start, candidate_step, candidate_stop))
        require(candidates,
                f"{name} default {default:g} cannot fit a {minimum_intervals}-{maximum_intervals} interval grid inside [{start:g},{stop:g}]")
        _, _, _, new_start, new_step, new_stop = max(candidates)
        if integral:
            require(all(v.is_integer() for v in (new_start, new_step, new_stop)),
                    f"{name} default-aligned integer grid is fractional")
        fields[0] = _clean_ini_number(default)
        fields[1] = _clean_ini_number(new_start)
        fields[2] = _clean_ini_number(new_step)
        fields[3] = _clean_ini_number(new_stop)
        output.append(f"{name}=" + "||".join(fields))
    return "\n".join(output) + ("\n" if rendered.endswith("\n") else "")


def reduce_typed_ini(rendered: str, spec: dict, regime_bounds: dict) -> str:
    """Reduce using declared types, rather than guessing from tuple numbers."""
    reduced = reduce_ini(rendered, regime_bounds)
    source_types = _source_input_types(spec)
    inputs = spec.get("tester_config", {}).get("inputs", {})
    for name, value in re.findall(r"(?m)^\s*(\w+)\s*=([^\r\n]+)$", rendered):
        fields = value.strip().split("||")
        if len(fields) != 5 or fields[-1] != "Y":
            continue
        if name.startswith("InpRegime_"):
            reduced = upsert_fixed_tester_input(reduced, name, "||".join([fields[0], "0", "0.25", "3", "Y"]))
            continue
        cfg = inputs.get(name, {})
        if _small_discrete_values(name, cfg, spec) is not None:
            reduced = upsert_fixed_tester_input(reduced, name, value)
            continue
        typ = str(source_types.get(name, cfg.get("type", "double"))).lower()
        start, step, stop = _legal_wide_grid(float(fields[1]), float(fields[3]), typ, 10, 15)
        reduced = upsert_fixed_tester_input(
            reduced, name, "||".join([fields[0], _clean_ini_number(start),
                                     _clean_ini_number(step), _clean_ini_number(stop), "Y"]))
    return reduced


def apply_wide_grid_policy(text: str, approved_spec: dict) -> str:
    """Write the Stage 3 wide surface at the final writer.

    Stage 2 supplies defaults and a starting suggestion.  It does not own the
    Stage 3 search width.  Rebuild every optimized row from the canonical wide
    policy here, after all surface-restoration steps, so a narrow Stage 2 tuple
    cannot be reintroduced by a later writer.
    """
    inputs = ((approved_spec or {}).get("tester_config", {}).get("inputs", {})
              if isinstance(approved_spec, dict) else {})
    runtime_bounds = _runtime_source_bounds(approved_spec)
    source_types = _source_input_types(approved_spec)
    for name, cfg in inputs.items():
        if str(cfg.get("optimize", "N")).upper() != "Y":
            continue
        match = re.search(rf"(?im)^\s*{re.escape(name)}\s*=([^\r\n]+)$", text)
        if not match:
            continue
        fields = match.group(1).strip().split("||")
        if len(fields) != 5 or fields[-1].upper() != "Y":
            continue
        try:
            default = float(cfg["default"])
        except (KeyError, TypeError, ValueError):
            continue
        typ = str(source_types.get(name, cfg.get("type", "int" if _integral_domain(cfg) else "double"))).lower()
        if name.startswith("InpRegime_"):
            start, step, stop = 0.0, 0.25, 5.0
        elif (name.startswith("InpRegime") or name in {"InpBaseLot", "InpMinBarsBetweenTrades"}) \
                and cfg.get("start") is not None and cfg.get("stop") is not None:
            # Non-multiplier regime inputs include signed thresholds and an
            # integer lookback. Build their wide grid from the authenticated
            # Stage 1/Stage 2 domain so generic regime/magnitude heuristics cannot
            # erase the sign, change an integer step to fractional, or expand
            # base exposure/cooldown beyond the approved surface.
            start, step, stop = _legal_wide_grid(float(cfg["start"]), float(cfg["stop"]), typ)
        elif (discrete_values := _small_discrete_values(name, cfg, approved_spec)) is not None:
            start, stop = min(discrete_values), max(discrete_values)
            step = float(cfg.get("step", 1))
        elif name in runtime_bounds:
            start, step, stop = _legal_wide_grid(*runtime_bounds[name], typ)
        elif (cfg.get("start") is not None and cfg.get("stop") is not None
              and float(cfg["start"]) < 0 < float(cfg["stop"])
              and float(cfg["start"]) <= default <= float(cfg["stop"])):
            # An explicitly signed Stage 1 estimate is evidence that the
            # parameter may cross zero. Keep that sign surface rather than
            # replacing it with a name/default heuristic.
            start, step, stop = _legal_wide_grid(
                float(cfg["start"]), float(cfg["stop"]), typ
            )
        else:
            # Stage 1 bounds are estimates, but they are the only generic
            # search-surface evidence we have. Do not replace their sign or
            # scale with a name/default magnitude heuristic. Executable EA
            # bounds, when present, further restrict that estimate.
            start, step, stop = _canonical_stage3_wide_range(
                name, typ, default, cfg, runtime_bounds.get(name)
            )
            if not 15.0 <= _optimization_intervals(start, step, stop) <= 20.0:
                # A canonical fixed anchor may intentionally be degenerate;
                # if Stage 1 authorized optimization, use its estimate.
                approved_lo, approved_hi = float(cfg["start"]), float(cfg["stop"])
                if name in runtime_bounds:
                    approved_lo = max(approved_lo, runtime_bounds[name][0])
                    approved_hi = min(approved_hi, runtime_bounds[name][1])
                start, step, stop = _default_anchored_wide_grid(
                    name, approved_lo, approved_hi, default, typ
                )
        row = f"{name}={_clean_ini_number(default)}||{_clean_ini_number(start)}||{_clean_ini_number(step)}||{_clean_ini_number(stop)}||Y"
        text = re.sub(rf"(?im)^\s*{re.escape(name)}\s*=.*$", row, text)
    return align_optimized_defaults(text, approved_spec, "default")


def upsert_fixed_tester_input(text: str, name: str, value: str) -> str:
    pattern = re.compile(rf"(?m)^{re.escape(name)}\s*=.*$")
    row = f"{name}={value}"
    if pattern.search(text):
        return pattern.sub(row, text, count=1)
    marker = re.search(r"(?im)^\[TesterInputs\]\s*$", text)
    require(marker is not None, "template is missing [TesterInputs]")
    insert_at = marker.end()
    return text[:insert_at] + "\n" + row + text[insert_at:]


def apply_source_fixed_nflag_inputs(rendered: str, source_text: str) -> str:
    """Keep source-declared constant N-flag selectors fixed in Stage 3."""
    declaration = re.compile(
        r"(?m)^[ \t]*input[ \t]+"
        r"(?P<kind>char|uchar|short|ushort|int|uint|long|ulong|float|double)"
        r"[ \t]+(?P<name>[A-Za-z0-9_]+)[ \t]*=[ \t]*(?P<default>[^;]+);"
        r"[ \t]*//(?P<comment>[^\r\n]*)$",
        re.I,
    )
    for match in declaration.finditer(source_text):
        if re.search(r"\bconstant\s*,\s*N-flag\b", match.group("comment"), re.I) is None:
            continue
        raw_default = match.group("default").strip()
        try:
            default_value = float(raw_default)
        except ValueError as exc:
            raise ValueError(
                f"{match.group('name')} is marked constant N-flag but has a nonnumeric default"
            ) from exc
        kind = match.group("kind").casefold()
        if kind in {"char", "uchar", "short", "ushort", "int", "uint", "long", "ulong"}:
            require(default_value.is_integer(),
                    f"{match.group('name')} integer N-flag default is fractional")
        value = _clean_ini_number(default_value)
        row = "||".join((value, value, "1", value, "N"))
        rendered = upsert_fixed_tester_input(rendered, match.group("name"), row)
    return rendered


def _clean_ini_number(value) -> str:
    """Serialize numeric TesterInputs without binary-float artefacts."""
    if isinstance(value, bool):
        return str(int(value))
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        raise ValueError(f"non-finite TesterInputs number: {value!r}")
    if number.is_integer():
        return str(int(number))
    return format(number, ".12g")


def _clean_grid(lo: float, hi: float) -> tuple[float, float]:
    """Choose a human-clean step and aligned stop inside authenticated bounds."""
    span = float(hi) - float(lo)
    raw = span / 15.0
    exponent = math.floor(math.log10(raw))
    base = raw / (10 ** exponent)
    nice = 1.0 if base <= 1.5 else 2.0 if base <= 3.0 else 5.0 if base <= 7.0 else 10.0
    step = nice * (10 ** exponent)
    intervals = max(5, min(20, math.floor(span / step + 1e-9)))
    if intervals < 15:
        # Preserve authenticated bounds while meeting the mandatory minimum
        # surface for floating-point inputs whose source step is too coarse.
        step = span / 15.0
        intervals = 15
    return step, float(lo) + intervals * step


def _canonical_stage3_wide_range(name: str, typ: str, default,
                                 config: dict | None = None,
                                 executable_bounds: tuple[float, float] | None = None):
    """Build a typed wide grid only from explicit Stage 1/source bounds.

    Defaults identify the center value and must be present on the grid; they
    do not identify units, sign, or a legal search domain. Guessing bounds
    from an input name or the magnitude of its default is therefore unsafe.
    """
    try:
        numeric_default = float(default)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} has no numeric EA default for Stage 3") from exc
    require(math.isfinite(numeric_default), f"{name} has a non-finite EA default")

    estimated = None
    if isinstance(config, dict) and config.get("start") is not None and config.get("stop") is not None:
        try:
            estimated = (float(config["start"]), float(config["stop"]))
        except (TypeError, ValueError):
            estimated = None
    if estimated and executable_bounds:
        lo, hi = max(estimated[0], executable_bounds[0]), min(estimated[1], executable_bounds[1])
    elif estimated:
        lo, hi = estimated
    elif executable_bounds:
        lo, hi = executable_bounds
    else:
        raise ValueError(
            f"{name} has no authenticated Stage 1 estimate or executable source bounds; "
            "refusing to guess its Stage 3 sign/scale"
        )
    require(math.isfinite(lo) and math.isfinite(hi) and lo < hi,
            f"{name} has no usable search interval after applying executable bounds")
    require(lo <= numeric_default <= hi,
            f"{name} EA default {numeric_default:g} is outside approved search interval [{lo:g},{hi:g}]")
    return _default_anchored_wide_grid(name, lo, hi, numeric_default, typ)


def _default_anchored_wide_grid(name: str, lo: float, hi: float,
                                numeric_default: float, typ: str) -> tuple[float, float, float]:
    """Choose 15-20 legal intervals within bounds while retaining the default."""
    require(math.isfinite(lo) and math.isfinite(hi) and lo < hi
            and lo <= numeric_default <= hi,
            f"{name} has no usable default-containing interval [{lo:g},{hi:g}]")
    seed_step = (hi - lo) / 15.0
    seed = (f"[TesterInputs]\n{name}={_clean_ini_number(numeric_default)}||"
            f"{_clean_ini_number(lo)}||{_clean_ini_number(seed_step)}||"
            f"{_clean_ini_number(hi)}||Y\n")
    aligned = align_optimized_defaults(
        seed, {"tester_config": {"inputs": {name: {
            "default": numeric_default, "type": typ, "optimize": "Y"
        }}}}, "default"
    )
    match = re.search(rf"(?im)^\s*{re.escape(name)}\s*=([^\r\n]+)$", aligned)
    require(match is not None, f"{name} could not be aligned to its approved wide interval")
    fields = match.group(1).split("||")
    return tuple(float(value) for value in fields[1:4])


def ensure_stage1_parameter_surface(text: str, spec: dict) -> str:
    """Carry every Stage 1 parameter into TesterInputs, including regime fields."""
    existing, designs = set(INI_PARAM_RE.findall(text)), spec.get("strategy", {}).get("parameter_design", [])
    for item in designs:
        # Some authenticated Stage 1 specs use a name-only parameter surface
        # (list[str]) while the signed template carries the actual defaults and
        # ranges. Preserve those template rows; only dict entries contain
        # enough authenticated data to rewrite values/ranges.
        if isinstance(item, str):
            if item in existing:
                continue
            continue
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")); value = item.get("value", item.get("default")); rng = item.get("range", [])
        if not name or value is None:
            continue
        # Always rewrite existing rows from the authenticated Stage 1 design.
        # Reusing a stale template row can put values outside the compiled EA's
        # declared domain and MT5 rejects every genetic pass as invalid.
        tester_input = spec.get("tester_config", {}).get("inputs", {}).get(name, {})
        tester_optimized = str(tester_input.get("optimize", "N")).upper() == "Y"
        optimized_design = (item.get("role") == "optimized" or
                            (item.get("opt_lo") is not None and item.get("opt_hi") is not None) or
                            tester_optimized)
        existing_match = re.search(rf"(?im)^{re.escape(name)}\s*=([^\r\n]+)$", text)
        existing_value = existing_match.group(1).strip() if existing_match else ""
        # ``Lots`` is a common non-Inp identity/execution input. Treat both
        # spellings as fixed when the authenticated Stage 1 config does not
        # authorize optimization; otherwise a stale template can create an
        # invalid 10-step search surface and block autonomous Stage 3.
        fixed_names = {"Lots", "InpLots", "InpMagicNumber", "InpSlippagePts", "InpTradeComment"}
        if optimized_design and (name not in fixed_names or
                                 (name in {"Lots", "InpLots"} and tester_optimized)):
            cfg = (spec.get("tester_config", {}).get("inputs", {}) or {}).get(name, {})
            discrete = _integral_domain(cfg, item)

            # Regime controls are never inherited from a narrow Stage 2
            # suggestion.  Stage 3 must expose the canonical full surface.
            if name.startswith("InpRegime_"):
                row = f"{name}={_clean_ini_number(value)}||0||0.25||5||Y"
                text = re.sub(rf"(?im)^\s*{re.escape(name)}\s*=.*$", row, text)
                existing_value = row.split("=", 1)[1]

            if discrete and existing_value.endswith("||Y"):
                # Never preserve a fractional legacy template step for an
                # authenticated integer/categorical input.
                fields = existing_value.split("||")
                if len(fields) == 5:
                    lo_i, hi_i = int(float(fields[1])), int(float(fields[3]))
                    row = f"{name}={_clean_ini_number(value)}||{lo_i}||1||{hi_i}||Y"
                    text = re.sub(rf"(?im)^\s*{re.escape(name)}\s*=.*$", row, text)
                    existing_value = row.split("=", 1)[1]
            # Repair underspecified legacy Y rows from the authenticated
            # Stage 1 bounds. Choose 15 equal intervals (16 values), keeping
            # integer domains integral and leaving already-valid 15-20-step
            # signed surfaces untouched.
            fields = existing_value.split("||") if existing_value.endswith("||Y") else []
            try:
                old_steps = _optimization_intervals(float(fields[1]), float(fields[2]), float(fields[3])) if len(fields) == 5 else 0.0
            except (TypeError, ValueError, ZeroDivisionError):
                old_steps = 0.0
            if fields and old_steps != 1.0 and old_steps < 5.0:
                lo, hi = cfg.get("start"), cfg.get("stop")
                if lo is not None and hi is not None and float(hi) > float(lo):
                    step, hi = _clean_grid(float(lo), float(hi))
                    if all(float(v).is_integer() for v in (lo, hi, step)):
                        step = int(step)
                        lo, hi = int(lo), int(hi)
                    row = f"{name}={_clean_ini_number(lo)}||{_clean_ini_number(lo)}||{_clean_ini_number(step)}||{_clean_ini_number(hi)}||Y"
                    text = re.sub(rf"(?im)^\s*{re.escape(name)}\s*=.*$", row, text)
                    existing_value = row.split("=", 1)[1]
            # Stage 1 describes source-authorized bounds, but Stage 3 wide
            # optimization must use the signed wide template surface. Preserve
            # a valid 15-20-step template range; otherwise fail closed rather
            # than silently launching the old narrow domain.
            if existing_value.endswith("||Y") and len(existing_value.split("||")) == 5:
                try:
                    _, old_start, old_step, old_stop, _ = existing_value.split("||")
                    old_steps = _optimization_intervals(float(old_start), float(old_step), float(old_stop))
                except (TypeError, ValueError, ZeroDivisionError):
                    old_steps = -1
                if (old_steps == 1.0 or 15.0 <= old_steps <= 22.0) and not name.startswith("InpRegime_"):
                    row = f"{name}={existing_value}"
                    text = upsert_fixed_tester_input(text, name, str(row.split('=', 1)[1]))
                    existing.add(name)
                    continue
            typ = str(item.get("type", tester_input.get("type", "double"))).lower()
            discrete_values = _small_discrete_values(name, tester_input, spec)
            values = sorted(discrete_values) if discrete_values is not None else []
            deltas = [right - left for left, right in zip(values, values[1:])]
            uniform = bool(deltas) and all(
                math.isclose(delta, deltas[0], rel_tol=1e-10, abs_tol=1e-12)
                for delta in deltas
            )
            default_in_discrete_domain = any(
                math.isclose(float(value), candidate, rel_tol=1e-10, abs_tol=1e-12)
                for candidate in values
            )
            if discrete_values is not None and uniform and default_in_discrete_domain:
                lo, step, hi = values[0], deltas[0], values[-1]
            else:
                lo, step, hi = _canonical_stage3_wide_range(
                    name, typ, value, tester_input, _runtime_source_bounds(spec).get(name)
                )
            row = f"{name}={_clean_ini_number(value)}||{_clean_ini_number(lo)}||{_clean_ini_number(step)}||{_clean_ini_number(hi)}||Y"
        else:
            # Some authenticated legacy Stage 1 specs carry defaults only while
            # their signed template already contains the intended optimization
            # domain. Preserve that valid Y range rather than collapsing it to N.
            if (name not in {"Lots", "InpLots", "InpMagicNumber", "InpSlippagePts", "InpTradeComment"}
                    and existing_value.endswith("||Y") and len(existing_value.split("||")) == 5):
                row = f"{name}={existing_value}"
            else:
                row = f"{name}={value}||{value}||1||{value}||N"
        text = upsert_fixed_tester_input(text, name, str(row.split('=', 1)[1]))
        existing.add(name)
    ea_name = str(spec.get("ea", ""))
    regime_pattern = r"(?im)^(\s*InpRegime_[A-Za-z0-9_]+\s*=.*?\|\|)([YN])\s*$"
    if ea_name.startswith("B1_1_Donchian_ATR_Filter"):
        # Donchian declares regime knobs for compatibility, but its compiled EA
        # does not consume them; keep those metadata-only inputs fixed.
        text = re.sub(regime_pattern, r"\1N", text)
    elif ea_name.startswith("B1_1_Realized_Correlation_Regime"):
        # These twelve regime knobs are part of the Stage 1 contract. Never
        # infer their exposure from a stale/failed .opt and silently reduce the
        # search surface. The post-run .opt gate proves MT5 actually honored it.
        text = re.sub(regime_pattern, r"\1Y", text)
        required_regime = re.findall(r"(?im)^\s*InpRegime_[A-Za-z0-9_]+\s*=.*?\|\|Y\s*$", text)
        require(len(required_regime) == 12, "Realized Correlation Stage 3 requires all 12 regime inputs optimized")
    elif ea_name.startswith("B1_1_GARCH_MIDAS") or ea_name.startswith("B1_1_HAR_J_RealizedJumpVariation"):
        # These EAs consume their regime/JI surfaces; all declared search
        # dimensions must remain optimized even when the template is stale.
        text = re.sub(r"(?im)^(\s*(?:InpRegime_|InpJI_)[A-Za-z0-9_]+\s*=.*?\|\|)([YN])\s*$", r"\1Y", text)
        required_surface = re.findall(r"(?im)^\s*(?:InpRegime_|InpJI_)[A-Za-z0-9_]+\s*=.*?\|\|Y\s*$", text)
        require(len(required_surface) == 15, "HAR/J Stage 3 requires all 15 regime/JI inputs optimized")
    # Final fixed-input normalization: the signed Stage 1 config is the
    # authority even when a legacy template contains an optimized Y row for a
    # name absent from parameter_design (for example non-Inp ``Lots``).
    for fixed_name, fixed_cfg in spec.get("tester_config", {}).get("inputs", {}).items():
        if str(fixed_cfg.get("optimize", "N")).upper() != "N":
            continue
        fixed_value = fixed_cfg.get("default", fixed_cfg.get("value"))
        if fixed_value is not None:
            text = upsert_fixed_tester_input(text, fixed_name, _clean_ini_number(fixed_value))
    # Normalize optimized inputs omitted from parameter_design as well. Legacy
    # specs may still authorize them in tester_config (e.g. InpSpreadPts).
    for optimized_name, optimized_cfg in spec.get("tester_config", {}).get("inputs", {}).items():
        if str(optimized_cfg.get("optimize", "N")).upper() != "Y":
            continue
        match = re.search(rf"(?im)^\s*{re.escape(optimized_name)}\s*=([^\r\n]+)$", text)
        if not match:
            continue
        fields = match.group(1).strip().split("||")
        # A signed template's fixed row is authoritative; do not silently
        # re-enable it merely because a legacy Stage 1 spec lists the name.
        if len(fields) != 5 or fields[-1].upper() != "Y":
            continue
        try:
            steps = _optimization_intervals(float(fields[1]), float(fields[2]), float(fields[3])) if len(fields) == 5 else 0.0
        except (TypeError, ValueError, ZeroDivisionError):
            steps = 0.0
        if steps != 1.0 and 15.0 <= steps <= 22.0:
            if _integral_domain(optimized_cfg):
                lo_i, hi_i = int(float(optimized_cfg["start"])), int(float(optimized_cfg["stop"]))
                cfg_step = int(float(optimized_cfg.get("step", 1)))
                require(cfg_step > 0, f"integral optimized input {optimized_name} has non-positive authenticated step")
                row = f"{optimized_name}={_clean_ini_number(optimized_cfg.get('default', lo_i))}||{lo_i}||{cfg_step}||{hi_i}||Y"
                text = re.sub(rf"(?im)^\s*{re.escape(optimized_name)}\s*=.*$", row, text)
                continue
            continue
        lo, hi = optimized_cfg.get("start"), optimized_cfg.get("stop")
        if lo is None or hi is None or float(hi) <= float(lo):
            continue
        step, hi = _clean_grid(float(lo), float(hi))
        if all(float(v).is_integer() for v in (lo, hi, step)):
            lo, hi, step = int(lo), int(hi), int(step)
        row = f"{optimized_name}={_clean_ini_number(lo)}||{_clean_ini_number(lo)}||{_clean_ini_number(step)}||{_clean_ini_number(hi)}||Y"
        text = re.sub(rf"(?im)^\s*{re.escape(optimized_name)}\s*=.*$", row, text)
    # Last-writer invariant: no earlier surface-restoration path may leave a
    # fractional grid for an authenticated integral input. This pass is
    # deliberately unconditional and runs immediately before validation.
    for integral_name, integral_cfg in spec.get("tester_config", {}).get("inputs", {}).items():
        if str(integral_cfg.get("optimize", "N")).upper() != "Y" or not _integral_domain(integral_cfg):
            continue
        match = re.search(rf"(?im)^\s*{re.escape(integral_name)}\s*=([^\r\n]+)$", text)
        if not match:
            continue
        fields = match.group(1).strip().split("||")
        if len(fields) != 5 or fields[-1].upper() != "Y":
            continue
        lo_i = int(float(fields[1]))
        hi_i = int(float(fields[3]))
        default_i = integral_cfg.get("default", fields[0])
        cfg_step = int(float(integral_cfg.get("step", fields[2])))
        require(cfg_step > 0, f"integral optimized input {integral_name} has non-positive authenticated step")
        row = f"{integral_name}={_clean_ini_number(default_i)}||{lo_i}||{cfg_step}||{hi_i}||Y"
        text = re.sub(rf"(?im)^\s*{re.escape(integral_name)}\s*=.*$", row, text)
    for metadata_name in ("InpMagicNumber", "InpSlippagePts", "InpTradeComment"):
        metadata_pattern = rf"(?im)^(\s*{re.escape(metadata_name)}\s*=.*?\|\|)[YN]\s*$"
        text = re.sub(metadata_pattern, r"\1N", text)
    if ea_name.startswith("B1_1_MTFTrend"):
        # MTFTrend's non-Inp execution/identity parameters are not part of
        # the parser's InpXxx optimization surface; force them metadata-only.
        for fixed_name in ("HtfPeriod", "HtfFastEma", "HtfSlowEma", "LtfPullbackAtrMult", "StopLossAtr", "TakeProfitAtr", "Lots", "SlippagePts"):
            fixed_pattern = rf"(?im)^(\s*{re.escape(fixed_name)}\s*=.*?\|\|)[YN]\s*$"
            text = re.sub(fixed_pattern, r"\1N", text)
    return text


def preserve_signed_wide_surface(rendered: str, template: str, approved_config: dict) -> str:
    """Restore signed wide ranges after approved-config materialization.

    Stage 1 owns identity/defaults and lineage. Stage 3 wide optimization owns
    the search surface. ``apply_config_surface`` runs after template rendering,
    so this final-writer guard prevents a valid wide tuple being collapsed back
    to the narrow Stage 1 tuple.
    """
    template_rows = {}
    for raw in template.splitlines():
        m = re.match(r"^\s*(Inp[A-Za-z0-9_]+)\s*=\s*([^\r\n]+)$", raw)
        if not m:
            continue
        fields = m.group(2).split("||")
        if len(fields) != 5 or fields[-1].upper() != "Y":
            continue
        try:
            steps = (float(fields[3]) - float(fields[1])) / float(fields[2])
        except (ValueError, ZeroDivisionError):
            continue
        if steps == 1.0 or 15.0 <= steps <= 22.0:
            template_rows[m.group(1)] = fields
    for name, spec in approved_config.get("inputs", {}).items():
        if name.startswith("InpRegime_") or str(spec.get("optimize", "N")).upper() != "Y" or name not in template_rows:
            continue
        fields = template_rows[name]
        cfg = approved_config.get("inputs", {}).get(name, {})
        if cfg.get("start") is not None and cfg.get("stop") is not None:
            if float(fields[1]) < float(cfg["start"]) or float(fields[3]) > float(cfg["stop"]):
                continue
        # A signed template is not allowed to reintroduce fractional steps for
        # an authenticated integer/categorical input. MT5 rounds such a step
        # to zero and rejects the entire optimization before pass 1.
        if _integral_domain(cfg):
            lo_i, hi_i = int(float(fields[1])), int(float(fields[3]))
            cfg_step = int(float(cfg.get("step", 1)))
            require(cfg_step > 0, f"integral optimized input {name} has non-positive authenticated step")
            row = "||".join([_clean_ini_number(spec.get("default", fields[0])), str(lo_i), str(cfg_step), str(hi_i), "Y"])
        else:
            row = "||".join([_clean_ini_number(spec.get("default", fields[0])), *fields[1:]])
        rendered = upsert_fixed_tester_input(rendered, name, row)
    return rendered


def normalize_optimized_ini_numbers(rendered: str) -> str:
    """Remove binary-float artefacts without changing an approved grid."""
    rows = []
    for raw in rendered.splitlines():
        match = re.match(r"^(\s*Inp[A-Za-z0-9_]+\s*=)([^\r\n]+)$", raw)
        if not match:
            rows.append(raw)
            continue
        fields = match.group(2).split("||")
        if len(fields) != 5 or fields[-1].upper() != "Y":
            rows.append(raw)
            continue
        try:
            fields[:4] = [_clean_ini_number(value) for value in fields[:4]]
        except (TypeError, ValueError):
            rows.append(raw)
            continue
        rows.append(match.group(1) + "||".join(fields))
    return "\n".join(rows) + "\n"


def rebuild_smoke_inputs_from_source(rendered: str, source_path: Path, approved_config: dict) -> str:
    """Replace stale template inputs with the authenticated EA declaration surface."""
    source = source_path.read_text(encoding="utf-8-sig", errors="replace")
    declarations = []
    pattern = re.compile(r"^\s*input\s+(char|uchar|short|ushort|int|uint|long|ulong|float|double|bool|string|datetime)\s+([A-Za-z0-9_]+)\s*=\s*([^;]+);", re.M)
    for match in pattern.finditer(source):
        kind, name, default = match.groups()
        default = default.strip()
        if kind == "bool":
            value = "true" if default.lower() in {"true", "1"} else "false"
        else:
            value = default
        cfg = (approved_config.get("inputs", {}) if isinstance(approved_config, dict) else {}).get(name, {})
        if isinstance(cfg, dict) and str(cfg.get("optimize", "N")).upper() == "Y":
            row = f"{name}={cfg['default']}||{cfg['start']}||{cfg['step']}||{cfg['stop']}||Y"
        elif isinstance(cfg, dict) and "default" in cfg:
            value = cfg["default"]
            row = f"{name}={value}||{value}||1||{value}||N"
        else:
            row = f"{name}={value}||{value}||1||{value}||N"
        declarations.append(row)
    require(declarations, f"no input declarations found in authenticated source: {source_path}")
    before, sep, after = rendered.partition("[TesterInputs]")
    require(sep, "Stage 3 smoke INI is missing [TesterInputs]")
    return before + sep + "\n" + "\n".join(declarations) + "\n"


def apply_coupled_wide_constraints(rendered: str, grid_mode: str = "default") -> str:
    """Apply coupled constraints only to input families declared by this EA.

    Legacy families are optional across strategy families.  A partially declared
    family remains invalid because its coupled contract cannot be authenticated.
    """
    coupled = ({
        "InpYzLowPct": (0.0, 2.5, 30.0),
        "InpYzHighPct": (45.0, 3.0, 81.0),
        "InpEmaFastPeriod": (2.0, 3.0, 38.0),
        "InpEmaSlowPeriod": (55.0, 25.0, 355.0),
    } if grid_mode == "reduced" else {
        "InpYzLowPct": (0.0, 2.5, 40.0),
        "InpYzHighPct": (45.0, 3.0, 99.0),
        "InpEmaFastPeriod": (2.0, 3.0, 50.0),
        "InpEmaSlowPeriod": (55.0, 25.0, 480.0),
    })
    families = (("InpYzLowPct", "InpYzHighPct"), ("InpEmaFastPeriod", "InpEmaSlowPeriod"))
    for family in families:
        present = []
        for name in family:
            pattern = rf"(?im)^({re.escape(name)}=)([^|\r\n]+)\|\|[^|\r\n]+\|\|[^|\r\n]+\|\|[^|\r\n]+\|\|Y\s*$"
            present.append(bool(re.search(pattern, rendered)))
        if not any(present):
            continue
        require(all(present), f"coupled input family incomplete: {', '.join(family)}")
        for name in family:
            start, step, stop = coupled[name]
            pattern = rf"(?im)^({re.escape(name)}=)([^|\r\n]+)\|\|[^|\r\n]+\|\|[^|\r\n]+\|\|[^|\r\n]+\|\|Y\s*$"
            rendered, count = re.subn(
                pattern,
                lambda m, start=start, step=step, stop=stop: f"{m.group(1)}{m.group(2)}||{_clean_ini_number(start)}||{_clean_ini_number(step)}||{_clean_ini_number(stop)}||Y",
                rendered,
                count=1,
            )
            require(count == 1, f"optimized coupled input {name} is malformed or missing from Stage 3 INI")
    return rendered


def source_regime_bounds(source_path: str) -> dict[str, tuple[float, float]]:
    source = Path(source_path).read_text(encoding="utf-8", errors="ignore")
    bounds = {}
    for m in re.finditer(r"if\s*\(\s*(Inp\w+)\s*<\s*([-+]?\d*\.?\d+)\s*\|\|\s*\1\s*>\s*([-+]?\d*\.?\d+)", source):
        bounds[m.group(1)] = (float(m.group(2)), float(m.group(3)))
    for m in re.finditer(r"input\s+\w+\s+(InpRegime_[A-Za-z0-9_]+)\s*=.*?(?:\n|$)", source):
        b = re.search(r"\[lo\s*=\s*([-+]?\d*\.?\d+)\s*,\s*hi\s*=\s*([-+]?\d*\.?\d+)", m.group(0))
        if b:
            lo, hi = float(b.group(1)), float(b.group(2)); old = bounds.get(m.group(1))
            bounds[m.group(1)] = (max(lo, old[0]) if old else lo, min(hi, old[1]) if old else hi)
    return {k: v for k, v in bounds.items() if k.startswith("InpRegime_")}


def validate_source_regime_contract(rendered: str, source_path: str, grid_mode: str = "default") -> None:
    """Reject regime grids containing values the EA source declares invalid."""
    source = Path(source_path).read_text(encoding="utf-8", errors="ignore")
    bounds = {}
    for m in re.finditer(r"if\s*\(\s*(Inp\w+)\s*<\s*([-+]?\d*\.?\d+)\s*\|\|\s*\1\s*>\s*([-+]?\d*\.?\d+)", source):
        bounds[m.group(1)] = (float(m.group(2)), float(m.group(3)))
    for m in re.finditer(r"input\s+\w+\s+(InpRegime_[A-Za-z0-9_]+)\s*=.*?(?:\n|$)", source):
        line = m.group(0)
        b = re.search(r"\[lo\s*=\s*([-+]?\d*\.?\d+)\s*,\s*hi\s*=\s*([-+]?\d*\.?\d+)", line)
        if b:
            lo, hi = float(b.group(1)), float(b.group(2))
            old = bounds.get(m.group(1))
            bounds[m.group(1)] = (max(lo, old[0]) if old else lo, min(hi, old[1]) if old else hi)
    for raw in rendered.splitlines():
        if not raw.startswith("Inp") or "=" not in raw:
            continue
        name, value = raw.split("=", 1)
        if name not in bounds:
            continue
        fields = value.split("||")
        if len(fields) != 5 or fields[-1].upper() != "Y":
            continue
        lo, hi = bounds[name]
        actual_lo, actual_hi = float(fields[1]), float(fields[3])
        policy_stop = 3.0 if grid_mode == "reduced" else 5.0
        if name.startswith("InpRegime_") and (actual_lo, float(fields[2]), actual_hi) == (0.0, 0.25, policy_stop):
            continue
        require(lo <= actual_lo <= hi and lo <= actual_hi <= hi,
                f"{name} wide range [{actual_lo:g},{actual_hi:g}] escapes source contract [{lo:g},{hi:g}]")


def render_ini(template_text: str, ea: str, timeframe: str, spec: dict,
               from_date: str = FROM_DATE, to_date: str = TO_DATE) -> str:
    # Re-read the shared contract at the final writer so direct callers and
    # cron callers receive the same regime grid and fixed-input surface.
    approved_config = stage3_config_from_spec(spec)
    spec = dict(spec)
    spec["tester_config"] = approved_config
    rendered = template_text
    for key, value in (
        ("EA_NAME", ea), ("Expert", rf"Kiyosaki\{ea}.ex5"), ("Symbol", "ETHUSD"),
        ("Period", timeframe), ("FromDate", from_date), ("ToDate", to_date),
        ("Optimization", "2"), ("OptimizationCriterion", "6"), ("Visual", "0"),
    ):
        rendered = replace_tester_value(rendered, key, value)
    start_epoch = str(int(datetime.strptime(from_date, "%Y.%m.%d").replace(tzinfo=timezone.utc).timestamp()))
    end_epoch = str(int(datetime.strptime(to_date, "%Y.%m.%d").replace(tzinfo=timezone.utc).timestamp()))
    rendered = upsert_fixed_tester_input(rendered, "InpMinatoExpectedScoreVersion", "MINATO_BASE_SCORE_V2.0")
    rendered = ensure_stage1_parameter_surface(rendered, spec)
    rendered = apply_wide_grid_policy(rendered, spec)
    # The authenticated Stage 1 surface may carry date-string defaults for the
    # datetime inputs; MT5 requires epoch values in TesterInputs. Apply these
    # fixed fields after the surface rewrite so they cannot be overwritten.
    rendered = upsert_fixed_tester_input(rendered, "InpMinatoExpectedScoreVersion", "MINATO_BASE_SCORE_V2.0")
    rendered = upsert_fixed_tester_input(rendered, "InpMinatoTestStart", start_epoch)
    rendered = upsert_fixed_tester_input(rendered, "InpMinatoTestEnd", end_epoch)
    # MT5 rejects an empty string TesterInputs default even when the EA treats
    # it as "use the common fallback". Keep the input fixed and give it a
    # deterministic non-empty metadata value.
    rendered = re.sub(
        r'(?im)^InpCsvPath\s*=\s*(?:""\s*)?\|\|\s*(?:""\s*)?\|\|1\|\|\s*(?:""\s*)?\|\|N\s*$',
        'InpCsvPath="mt5_stage3.csv"||"mt5_stage3.csv"||1||"mt5_stage3.csv"||N',
        rendered,
        count=1,
    )
    # Replace the entire template input surface with the authenticated Stage 2
    # contract before validating; legacy template rows must not be validated as
    # if they were approved for this EA.
    rendered = apply_config_surface(rendered, approved_config)
    rendered = apply_wide_grid_policy(rendered, spec)
    rendered = apply_coupled_wide_constraints(rendered)
    if "EA_NAME=B1_1_MTFTrend_v4_Elder" in rendered:    # The compiled Elder tester surface does not expose these regime
        # fields to MT5 optimization; never advertise them as Y in the INI.
        for regime_name in ("InpRegime_GreedLot", "InpRegime_GreedSLMult", "InpRegime_GreedTPMult", "InpRegime_NeutralLot", "InpRegime_NeutralSLMult", "InpRegime_NeutralTPMult", "InpRegime_FearLot", "InpRegime_FearSLMult", "InpRegime_FearTPMult", "InpRegime_ExtFearLot", "InpRegime_ExtFearSLMult", "InpRegime_ExtFearTPMult"):
            rendered = re.sub(rf'(?im)^({re.escape(regime_name)}=.*\|\|)Y\s*$', r'\1N', rendered, count=1)
    rendered = align_optimized_defaults(rendered, spec)
    rendered = constrain_input_grid_ranges(rendered, spec)
    rendered = align_optimized_defaults(rendered, spec)
    validate_tester_inputs(rendered, spec)
    return rendered


def render_ini_for_candidate_contract(template_text: str, ea: str, timeframe: str,
                                      spec: dict, from_date: str, to_date: str) -> str:
    """Classify an infeasible authenticated grid as a candidate input rejection."""
    try:
        return render_ini(template_text, ea, timeframe, spec, from_date, to_date)
    except Stage3InputContractError:
        raise
    except ValueError as exc:
        raise Stage3InputContractError("INPUT_GRID_INVALID", str(exc)) from exc


def validate_stage2(state: dict, handoff: dict) -> tuple[dict, dict]:
    require(state.get("schema_version") == 2, "canonical state schema mismatch")
    restartable_stage3_hold = (
        state.get("current_stage") == "stage3_running"
        and state.get("stage_status") == "stage3_infrastructure_failure"
        and state.get("next_action") == "inspect_stage3_infrastructure_failure"
    )
    restartable_wide_duration_retry = (
        state.get("current_stage") == "stage3_running"
        and state.get("stage_status") == "stage3_duration_reduction_pending"
        and state.get("next_action") == "resume_stage3_wide_after_budget_update"
        and state.get("stage3_grid_mode", "default") == "default"
        and authorizes_stage3_admission(state)
    )
    require(
        state.get("current_stage") == "stage3_pending" or restartable_stage3_hold
        or restartable_wide_duration_retry,
        "Stage 3 requires stage3_pending or an authenticated cycle-owned restart hold",
    )
    require(
        state.get("stage_status") == "stage2_complete_held" or restartable_stage3_hold
        or restartable_wide_duration_retry
        or (state.get("stage_status") == "stage2_complete_ready"
            and state.get("current_stage") == "stage3_pending"
            and type(state.get("hold_after_stage")) is int and state["hold_after_stage"] in {3, 5}
            and state.get("next_action") == "run_stage3"
            and authorizes_stage3_admission(state)),
        "Stage 2 is not complete-held or this is not an authenticated Stage 3 restart",
    )
    require(handoff.get("schema_version") == 2 and handoff.get("stage") == 2, "Stage 2 handoff schema mismatch")
    require(handoff.get("cycle_id") == state.get("cycle_id"), "Stage 2 handoff cycle mismatch")
    require(handoff.get("ea") == state.get("current_ea"), "Stage 2 handoff EA mismatch")
    event_path = Path(handoff["expected_event"])
    verification_path = Path(handoff["expected_verification"])
    event, verification = load_object(event_path), load_object(verification_path)
    require(event.get("cycle_id") == state["cycle_id"] and event.get("stage") == 2 and event.get("status") == "complete", "invalid Stage 2 event")
    require(Path(event.get("verification", "")).resolve() == verification_path.resolve(), "Stage 2 event verification path mismatch")
    require(event.get("verification_sha256") == sha256(verification_path), "Stage 2 verification hash mismatch")
    require(verification.get("passed") is True, "Stage 2 verification did not pass")
    require(verification.get("score_version") == "MINATO_BASE_SCORE_V2.0",
            "Stage 2 verification did not attest MINATO_BASE_SCORE_V2.0")
    require(event.get("score_version") == "MINATO_BASE_SCORE_V2.0",
            "Stage 2 event did not attest MINATO_BASE_SCORE_V2.0")
    stage1_spec = Path(handoff.get("stage1_spec", ""))
    stage1_event = Path(handoff.get("stage1_event", ""))
    require(stage1_spec.is_file() and stage1_event.is_file(), "Stage 1 lineage artifacts are missing")
    stage1_event_value = load_object(stage1_event)
    stage1_hash = sha256(stage1_spec)
    require(stage1_hash == handoff.get("stage1_spec_sha256"), "Stage 1 specification changed after Stage 2 preparation")
    require(stage1_event_value.get("artifact_sha256") == stage1_hash, "Stage 1 event/spec lineage mismatch")
    require(verification.get("stage1_spec_sha256") == stage1_hash, "Stage 2 verification used a different Stage 1 specification")
    require(event.get("stage1_spec_sha256") == stage1_hash, "Stage 2 event used a different Stage 1 specification")
    stage1_spec_data = load_object(stage1_spec)
    source_text = Path(handoff["source"]).read_text(encoding="utf-8-sig", errors="replace")
    approved_config = stage2_config_from_spec(stage1_spec_data, source_text=source_text)
    validate_optimized_source_usage(approved_config, source_text)
    approved_config_hash = config_sha256(approved_config)
    require(handoff.get("approved_tester_config_sha256") == approved_config_hash,
            "Stage 2 handoff tester_config hash mismatch")
    require(verification.get("approved_tester_config_sha256") == approved_config_hash,
            "Stage 2 verification lacks approved tester_config")
    require(event.get("approved_tester_config_sha256") == approved_config_hash,
            "Stage 2 event lacks approved tester_config")
    for key, hash_key in (("source", "mq5_sha256"), ("ex5", "ex5_sha256"), ("canonical_minato", "canonical_minato_sha256")):
        artifact = Path(verification[key])
        require(artifact.is_file() and sha256(artifact) == event.get(hash_key), f"Stage 2 {key} hash mismatch")
    return event, verification


def build_stage2_release_authorization(state: dict, stage2_handoff_path: Path, *,
                                      authorized_by: str, reason: str,
                                      authorized_utc: str | None = None,
                                      authorization_nonce: str | None = None) -> dict:
    """Bind a user-approved, one-run Stage 3 release to current Stage 2 evidence."""
    stage2 = load_object(stage2_handoff_path)
    event, verification = validate_stage2(state, stage2)
    ordinary_release_state = (
        state.get("current_stage") == "stage3_pending"
        and state.get("stage_status") == "stage2_complete_held"
        and state.get("hold_after_stage") == 2
        and state.get("next_action") == "audit_stage2_before_stage3"
    )
    grid_retry_release_state = False
    if (state.get("current_stage") == "stage3_pending"
            and state.get("stage_status") == "stage2_complete_held"
            and state.get("hold_after_stage") == 2
            and state.get("next_action") == "run_stage3"
            and state.get("stage3_input_contract_retry_count") == 1
            and authorizes_stage3_admission(state)):
        try:
            import kcos_supervisor_v2 as supervisor
            grid_retry_release_state = supervisor.validate_stage3_input_contract_repair(state)[0]
        except (ImportError, OSError, ValueError, TypeError, KeyError, RuntimeError):
            grid_retry_release_state = False
    lineage_retry_release_state = False
    if (state.get("current_stage") == "stage3_pending"
            and state.get("stage_status") == "stage2_complete_held"
            and state.get("hold_after_stage") == 2
            and state.get("next_action") == "run_stage3"
            and state.get("stage3_launch_lineage_retry_count") == 1
            and authorizes_stage3_admission(state)):
        try:
            import kcos_supervisor_v2 as supervisor
            lineage_retry_release_state = supervisor.stage3_launch_lineage_retry_identity(state) is not None
        except (ImportError, OSError, ValueError, TypeError, KeyError, RuntimeError):
            lineage_retry_release_state = False
    ordinary_release_state = ordinary_release_state or grid_retry_release_state or lineage_retry_release_state
    retry_release_state = False
    retry = state.get("stage3_repair_retry")
    attempt = state.get("stage2_release_attempt")
    if (
        not ordinary_release_state
        and isinstance(retry, dict)
        and retry.get("status") == "consumed"
        and retry.get("repair_kind") == "stage3_journal_candidate_error_reclassification"
        and retry.get("cycle_id") == state.get("cycle_id")
        and retry.get("event_id") == state.get("stage3_journal_reclassification_retry_event_id")
        and state.get("stage3_journal_reclassification_retry_count") == 1
        and state.get("current_stage") == "stage3_pending"
        and state.get("stage_status") == "stage2_complete_held"
        and state.get("next_action") == "run_stage3"
        and isinstance(state.get("hold_after_stage"), int)
        and state.get("hold_after_stage") >= 3
        and isinstance(attempt, dict)
        and attempt.get("status") == "attempt_in_progress"
        and attempt.get("retry_reuse_kind") == retry.get("repair_kind")
        and attempt.get("retry_reuse_event_id") == retry.get("event_id")
        and attempt.get("retry_reuse_receipt_sha256") == retry.get("repair_receipt_sha256")
        and authorizes_stages_1_to_5(state)
    ):
        receipt_path = Path(str(retry.get("repair_receipt") or ""))
        try:
            receipt = load_object(receipt_path)
            retry_release_state = (
                receipt_path.is_file()
                and sha256(receipt_path) == retry.get("repair_receipt_sha256")
                and str(receipt_path.resolve()) == str(
                    state.get("stage3_journal_reclassification_recovery_receipt", ""))
                and receipt.get("record_kind") == "stage3_journal_candidate_reclassification_retry"
                and receipt.get("repair_complete") is True
                and receipt.get("cycle_id") == state.get("cycle_id")
                and receipt.get("retry_event_id") == retry.get("event_id")
            )
        except (OSError, ValueError, TypeError, KeyError):
            retry_release_state = False
    require(ordinary_release_state or retry_release_state,
            "Stage 2 release authorization requires the exact held Stage 3 admission state or its authenticated journal retry")
    require(authorized_by == "user_confirmed", "Stage 2 release must be explicitly user-confirmed")
    require(isinstance(reason, str) and len(reason.strip()) >= 16,
            "Stage 2 release authorization requires a specific recorded reason")
    event_path = Path(stage2["expected_event"])
    verification_path = Path(stage2["expected_verification"])
    spec_path = Path(stage2["stage1_spec"])
    # Supervisor consumption fields are bookkeeping, not event identity.
    stable_event_sha256 = consumption_insensitive_event_sha256(event_path)
    evidence = {
        "stage2_handoff_sha256": sha256(stage2_handoff_path),
        "stage2_event_sha256": stable_event_sha256,
        "stage2_verification_sha256": sha256(verification_path),
        "stage1_spec_sha256": sha256(spec_path),
        "source_sha256": sha256(Path(stage2["source"])),
        "ex5_sha256": sha256(Path(verification["ex5"])),
        "canonical_minato_sha256": sha256(Path(verification.get("canonical_minato") or verification["ex5"])),
        "approved_tester_config_sha256": str(stage2["approved_tester_config_sha256"]),
    }
    event_hash = evidence["stage2_event_sha256"]
    authorization = {
        "schema_version": 1,
        "authorization_id": f"{state['cycle_id']}:stage3:one-run:{event_hash[:12]}"
                            + (f":{authorization_nonce}" if authorization_nonce else ""),
        "cycle_id": state["cycle_id"],
        "target_stage": 3,
        "scope": "current_cycle_stage3_once",
        "authorized_by": authorized_by,
        "reason": reason.strip(),
        "authorized_utc": authorized_utc or utc_now(),
        "evidence": evidence,
    }
    if authorization_nonce:
        require(re.fullmatch(r"[A-Za-z0-9_-]{1,32}", authorization_nonce) is not None,
                "Stage 3 release authorization nonce is invalid")
        authorization["authorization_nonce"] = authorization_nonce
    return authorization


def validate_stage2_release_authorization(state: dict, stage2_handoff_path: Path,
                                          authorization: dict) -> dict:
    require(isinstance(authorization, dict), "one-run Stage 2 release authorization is required")
    expected = build_stage2_release_authorization(
        state, stage2_handoff_path,
        authorized_by=str(authorization.get("authorized_by", "")),
        reason=str(authorization.get("reason", "")),
        authorized_utc=str(authorization.get("authorized_utc", "")),
        authorization_nonce=(str(authorization["authorization_nonce"])
                             if authorization.get("authorization_nonce") else None),
    )
    expected_keys = ["schema_version", "authorization_id", "cycle_id", "target_stage", "scope",
                     "authorized_by", "reason", "authorized_utc", "evidence"]
    if "authorization_nonce" in expected:
        expected_keys.append("authorization_nonce")
    for key in expected_keys:
        require(authorization.get(key) == expected.get(key),
                f"one-run Stage 2 release authorization {key} mismatch")
    require(state.get("stage2_release_attempt", {}).get("authorization_id") == expected["authorization_id"],
            "one-run Stage 2 release authorization has no matching persisted attempt")
    require(state.get("stage2_release_attempt", {}).get("status") == "attempt_in_progress",
            "one-run Stage 2 release authorization was already consumed or failed")
    if authorization.get("hermes_provenance") is not None:
        expected["hermes_provenance"] = str(authorization["hermes_provenance"])
    return expected


def apply_smoke_surface(rendered: str) -> str:
    """Keep exactly one declared numeric input optimized for MT5 smoke tests."""
    rows = []
    in_inputs = False
    for line in rendered.splitlines():
        stripped = line.strip()
        if stripped == "[TesterInputs]":
            in_inputs = True
        elif in_inputs and stripped.startswith("[") and stripped.endswith("]"):
            in_inputs = False
        if in_inputs and "=" in line and not stripped.startswith(";"):
            name, value = line.split("=", 1)
            fields = value.split("||")
            if len(fields) == 5 and fields[-1].strip() == "Y":
                if name.strip() == "InpAtrPeriod":
                    line = f"{name.strip()}=14||5||1||23||Y"
                else:
                    default = fields[0].strip()
                    line = f"{name.strip()}={default}||{default}||{default}||{default}||N"
        rows.append(line)
    return "\n".join(rows) + ("\n" if rendered.endswith("\n") else "")


def prepare(state_path: Path, stage2_handoff_path: Path, stage3_handoff_path: Path,
            lease_path: Path, template_path: Path, output_dir: Path, dry_run: bool = False,
            grid_mode: str = "default", release_authorization: dict | None = None) -> dict:
    require(grid_mode in {"default", "reduced", "smoke"}, f"unsupported Stage 3 grid mode: {grid_mode}")
    state, stage2 = load_object(state_path), load_object(stage2_handoff_path)
    event, verification = validate_stage2(state, stage2)
    is_held_stage2 = state.get("stage_status") == "stage2_complete_held"
    if is_held_stage2:
        release_authorization = validate_stage2_release_authorization(state, stage2_handoff_path, release_authorization)
        if not dry_run:
            require(state.get("stage2_release_attempt", {}).get("status") == "attempt_in_progress",
                    "one-run Stage 2 release attempt is not active")
    else:
        require(release_authorization is None,
                "one-run Stage 2 release authorization is valid only for the exact held Stage 2 state")
    stage1_spec = Path(stage2["stage1_spec"])
    stage1_spec_data = load_object(stage1_spec)
    tester_contract = stage1_spec_data.get("tester_config")
    if (not isinstance(tester_contract, dict)
            or not isinstance(tester_contract.get("input_constraints"), list)):
        raise Stage3InputContractError(
            "INPUT_CONSTRAINTS_UNDECLARED",
            "Stage 1 tester_config.input_constraints is missing; use [] only when optimized inputs are independent",
        )
    stage1_hash = sha256(stage1_spec)
    source_path = Path(stage2["source"]).resolve()
    source_text = source_path.read_text(encoding="utf-8-sig", errors="replace")
    approved_config = stage2_config_from_spec(stage1_spec_data, source_text=source_text)
    approved_config_hash = config_sha256(approved_config)
    runtime_config = stage3_config_from_spec(stage1_spec_data)
    runtime_spec = dict(stage1_spec_data)
    runtime_spec["tester_config"] = runtime_config
    # Stage 1 source_paths are research citations, not the generated candidate.
    # Stage 3 grid/type inference must use the authenticated Stage 2 EA source.
    runtime_spec["source_paths"] = [
        *(runtime_spec.get("source_paths", []) if isinstance(runtime_spec.get("source_paths"), list) else []),
        str(source_path),
    ]
    try:
        validate_optimized_input_source_contract(approved_config, source_text)
    except Stage3InputContractError:
        raise
    except ValueError as exc:
        raise Stage3InputContractError("SOURCE_INPUT_CONTRACT_MISMATCH", str(exc)) from exc
    validate_optimized_source_usage(approved_config, source_text)
    canonical_minato = verification.get("canonical_minato") or verification["ex5"]
    canonical_minato_sha = event.get("canonical_minato_sha256") or verification.get("canonical_minato_sha256", "")
    template_text = template_path.read_text(encoding="utf-8-sig")
    from_date, to_date = approved_config["from_date"], approved_config["to_date"]
    cycle, ea = state["cycle_id"], state["current_ea"]
    ini_paths = {}
    rendered_inis = {}
    for tf in TIMEFRAMES:
        target = output_dir / f"{cycle}.{ea}.ETHUSD.{tf}.{from_date.replace('.', '')}_{to_date.replace('.', '')}.wide.ini"
        rendered = render_ini_for_candidate_contract(
            template_text, ea, tf, runtime_spec, from_date, to_date)
        rendered = apply_config_surface(rendered, runtime_config)
        rendered = preserve_signed_wide_surface(rendered, template_text, runtime_config)
        # Final-writer invariant: signed surface restoration may reintroduce
        # legacy ranges; reapply the authenticated Stage 1 optimize flags and
        # compliant interval normalization before any validation or launch.
        # Restore the source declaration surface before the final grid writer;
        # this helper starts with Stage 2 tuples and must never follow it.
        rendered = rebuild_smoke_inputs_from_source(rendered, Path(stage2["source"]), stage2["approved_tester_config"])
        rendered = ensure_stage1_parameter_surface(rendered, runtime_spec)
        rendered = apply_source_fixed_nflag_inputs(rendered, source_text)
        rendered = normalize_optimized_ini_numbers(rendered)
        rendered = apply_wide_grid_policy(rendered, runtime_spec)
        rendered = apply_coupled_wide_constraints(rendered, grid_mode)
        if grid_mode == "reduced":
            rendered = reduce_typed_ini(rendered, runtime_spec, source_regime_bounds(stage2["source"]))
            rendered = apply_coupled_wide_constraints(rendered, grid_mode)
        if grid_mode == "smoke":
            rendered = apply_smoke_surface(rendered)
        # Source rebuilding sees local declarations only. Restore every
        # configured shared fixed input from its authenticated runtime view.
        rendered = apply_shared_input_surface(rendered, runtime_spec)
        try:
            rendered = align_optimized_defaults(rendered, runtime_spec, grid_mode)
            rendered = constrain_input_grid_ranges(rendered, runtime_spec, grid_mode)
            rendered = align_optimized_defaults(rendered, runtime_spec, grid_mode)
            validate_source_regime_contract(rendered, stage2["source"], grid_mode)
            validate_tester_inputs(rendered, runtime_spec, grid_mode)
        except ValueError as exc:
            raise Stage3InputContractError("INPUT_GRID_INVALID", str(exc)) from exc
        rendered_inis[tf] = rendered
        ini_paths[tf] = str(target.resolve())
    input_audit_path = output_dir / f"{cycle}.input_surface_audit.json"
    input_audit_preview = build_input_surface_audit(
        runtime_config, source_text, rendered_inis, cycle_id=cycle, ea=ea,
        source_path=stage2["source"], source_sha256=event["mq5_sha256"],
        stage1_spec=str(stage1_spec.resolve()), stage1_spec_sha256=stage1_hash,
        config_sha256=approved_config_hash,
    )
    handoff = {
        "schema_version": 2, "cycle_id": cycle, "stage": 3, "ea": ea,
        "score_version": "MINATO_BASE_SCORE_V2.0",
        "score_include": canonical_minato,
        "score_include_sha256": canonical_minato_sha,
        "canonical_minato": canonical_minato,
        "canonical_minato_sha256": canonical_minato_sha,
        "template_ini": str(template_path.resolve()), "template_ini_sha256": sha256(template_path),
        "source": verification["source"], "source_sha256": event["mq5_sha256"],
        "ex5": verification["ex5"], "ex5_sha256": event["ex5_sha256"],
        "stage2_event": str(Path(stage2["expected_event"]).resolve()),
        "stage2_event_sha256": consumption_insensitive_event_sha256(Path(stage2["expected_event"])),
        "stage1_spec": str(stage1_spec.resolve()), "stage1_spec_sha256": stage1_hash,
        "approved_tester_config_sha256": approved_config_hash,
        "optimizer_policy_overrides": runtime_config.get("optimizer_policy_overrides", []),
        "configuration_authority": "immutable_stage1_spec_plus_stage2_derived_position_sizing_and_stage3_timing_control_policy_v2",
        "timing_control_policy": "time_stop_and_cooldown_inputs_fixed_to_zero_and_never_optimized",
        "market": f"EightCap {approved_config['symbol']}", "symbol": approved_config["symbol"], "from_date": from_date, "to_date": to_date,
        "timeframe_order": list(TIMEFRAMES), "ini_files": ini_paths,
        "ini_sha256": {tf: hashlib.sha256(rendered_inis[tf].encode("utf-8")).hexdigest().upper() for tf in TIMEFRAMES},
        "input_surface_audit": {"path": str(input_audit_path.resolve()), "schema_version": 2},
        "runtime_manifest": str((output_dir / f"{cycle}.stage3.runtime.json").resolve()),
        "lease": str(lease_path.resolve()),
        "decision": str((state_path.parent / "decisions" / f"{cycle}.stage3.decision.json").resolve()),
        "event": str((state_path.parent / "events" / f"{cycle}.stage3.complete.json").resolve()),
        "heartbeat": str((state_path.parent / "kcos_stage3_heartbeat_v2.json").resolve()),
        "failed_notes": str(ROOT / "state" / "reports" / "failed-strategies.md"),
        "source_notes": str(ROOT / "state" / "reports" / "sources-tracker.md"),
        "passing_notes": str(ROOT / "state" / "reports" / "passing-strategies.md"),
        "poll_seconds": 10, "normal_settle_seconds": 20, "fallback_settle_seconds": 60,
        "grid_mode": grid_mode, "max_passes": MAX_PASSES, "pass_limit_settle_seconds": PASS_LIMIT_SETTLE_SECONDS,
        "first_pass_budget_seconds": FIRST_PASS_BUDGET_SECONDS,
        "first_pass_hard_limit_seconds": FIRST_PASS_HARD_LIMIT_SECONDS,
        "progress_stall_budget_seconds": PROGRESS_STALL_BUDGET_SECONDS,
        "startup_grace_seconds": STARTUP_GRACE_SECONDS,
        "timeframe_budget_seconds": TIMEFRAME_BUDGET_SECONDS,
        "hold_after_stage": int(state.get("hold_after_stage", 3)), "legacy_state_write_allowed": False,
        "llm_polling_allowed": False, "auto_launch_allowed": True,
        "exclusive_mt5_control": True,
        "prelaunch_reconcile_existing_profile": False,
        "lease": str(lease_path.resolve()),
        "continuation_mode": "deterministic_zero_token_driver", "prepared_utc": utc_now(), "dry_run": dry_run,
    }
    if release_authorization is not None:
        handoff["stage2_release"] = dict(release_authorization, status="consumed", consumed_utc=utc_now())
    if dry_run:
        handoff["input_surface_audit_preview"] = input_audit_preview
        return handoff
    reconcile_stale_lease(lease_path, cycle, output_dir)
    require(not lease_path.exists(), f"Stage 3 single-flight lease already exists: {lease_path}")
    # Claim the stage before writing any run artifact so concurrent prepares
    # cannot race through the initial existence check.
    lease_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(str(lease_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump({"schema_version": 2, "cycle_id": cycle, "created_utc": utc_now()}, handle, indent=2)
        handle.write("\n")
        handle.flush(); os.fsync(handle.fileno())
    try:
        for tf, rendered in rendered_inis.items():
            ini_path = Path(ini_paths[tf])
            atomic_bytes(ini_path, rendered.encode("utf-8"))
            # Normalize the final on-disk artifact before hashing/signing it.
            # The launcher validates these fields fail-closed and must receive
            # the exact same normalized bytes that the handoff attests.
            normalize_opt_ini.normalize_opt_ini(ini_path)
            require(normalize_opt_ini.is_normalized(ini_path),
                    f"Stage 3 tester profile normalization failed for {tf}")
        handoff["ini_sha256"] = {tf: sha256(Path(ini_paths[tf])) for tf in TIMEFRAMES}
        final_inis = {
            tf: Path(ini_paths[tf]).read_text(encoding="utf-8-sig", errors="replace")
            for tf in TIMEFRAMES
        }
        input_audit = build_input_surface_audit(
            runtime_config, source_text, final_inis, cycle_id=cycle, ea=ea,
            source_path=stage2["source"], source_sha256=event["mq5_sha256"],
            stage1_spec=str(stage1_spec.resolve()), stage1_spec_sha256=stage1_hash,
            config_sha256=approved_config_hash,
            ini_sha256_by_tf=handoff["ini_sha256"],
        )
        atomic_json(input_audit_path, input_audit)
        handoff["input_surface_audit"]["sha256"] = sha256(input_audit_path)
        atomic_json(stage3_handoff_path, handoff)
        next_state = dict(state)
        next_state.update({"current_stage": "stage3_running", "stage_status": "stage3_m5_pending",
                           "hold_after_stage": (5 if authorizes_stages_1_to_5(state) else
                                                (3 if release_authorization is not None else int(state.get("hold_after_stage", 3)))),
                           "next_action": "start_stage3_m5" if release_authorization is not None else state.get("next_action"),
                           "last_stage3_handoff": str(stage3_handoff_path.resolve()), "updated_utc": utc_now()})
        if release_authorization is not None:
            attempt = dict(state["stage2_release_attempt"])
            attempt.update({"status": "released", "consumed_utc": utc_now()})
            next_state["stage2_release_attempt"] = attempt
            next_state["stage2_release_audit"] = dict(
                release_authorization, status="consumed", consumed_utc=attempt["consumed_utc"],
                stage3_handoff=str(stage3_handoff_path.resolve()),
                stage3_handoff_sha256=sha256(stage3_handoff_path))
            operator_authorization = next_state.get("stage3_one_run_release_authorization")
            if (isinstance(operator_authorization, dict)
                    and operator_authorization.get("authorization_id")
                    == release_authorization.get("authorization_id")):
                next_state["stage3_one_run_release_authorization"] = dict(
                    operator_authorization, status="consumed",
                    consumed_utc=attempt["consumed_utc"],
                    stage3_handoff=str(stage3_handoff_path.resolve()),
                    stage3_handoff_sha256=sha256(stage3_handoff_path))
            restart_authorization = next_state.get("stage3_restart_from_m5_authorization")
            if (isinstance(restart_authorization, dict)
                    and restart_authorization.get("status") == "authorized"
                    and restart_authorization.get("authorization_nonce")
                    == release_authorization.get("authorization_nonce")):
                next_state["stage3_restart_from_m5_authorization"] = dict(
                    restart_authorization, status="consumed",
                    consumed_utc=attempt["consumed_utc"],
                    stage3_handoff=str(stage3_handoff_path.resolve()),
                    stage3_handoff_sha256=sha256(stage3_handoff_path))
        atomic_json(state_path, next_state)
    except Exception:
        lease_path.unlink(missing_ok=True)
        raise
    return handoff


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, default=STATE)
    parser.add_argument("--stage2-handoff", type=Path, default=STAGE2_HANDOFF)
    parser.add_argument("--handoff", type=Path, default=STAGE3_HANDOFF)
    parser.add_argument("--lease", type=Path, default=LEASE)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "state" / "stage3_v2")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--grid-mode", choices=("default", "reduced", "smoke"), default="default")
    args = parser.parse_args()
    try:
        result = prepare(args.state, args.stage2_handoff, args.handoff, args.lease,
                         args.template, args.output_dir, args.dry_run, args.grid_mode)
    except Stage3InputContractError as exc:
        event = record_input_contract_failure(args.state, args.stage2_handoff, exc.code, str(exc))
        print(json.dumps({"status": "STAGE3_INPUT_CONTRACT_REJECTED",
                          "event_id": event["event_id"], "failure_kind": event["failure_kind"],
                          "detail": str(exc), "zero_token": True}, separators=(",", ":")))
        return 2
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
