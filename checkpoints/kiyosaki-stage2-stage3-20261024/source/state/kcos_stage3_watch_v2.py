#!/usr/bin/env python3
"""Zero-token Stage 3 optimization watcher.

The watcher never launches MT5. It observes a runtime manifest and only uses
the Tester Stop control after a validated counter proves the total is complete.
"""

from __future__ import annotations

import argparse
import ast
import ctypes
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import shutil
from datetime import datetime, timezone
from pathlib import Path

import psutil

from kcos_stage3_common_v2 import atomic_json, load_object, require, sha256, utc_now
# Stop support is used only for controlled pass-limit enforcement. GUI/OCR
# reads always run in a bounded helper process below: a hung Windows window
# call or Tesseract invocation must not pin the recurring Stage 3 cron.
from kcos_stage3_duration_policy_v2 import (
    UI_ETA_CHECKPOINT_SECONDS, UI_ETA_SKIP_SECONDS, eta_cutoff_triggered, project,
)
try:
    import kcos_stage3_stop_v2 as stop_control
except (ModuleNotFoundError, ImportError):
    stop_control = None


EARLY_COMPLETE_RE = re.compile(r"genetic calculation is over", re.I)
FINAL_COMPLETE_RE = re.compile(r"genetic optimization finished on pass", re.I)
CACHE_SAVED_RE = re.compile(r"(?:new records saved to cache file|optimization done in)", re.I)
PASS_COMPLETE_RE = re.compile(r"OnTester result .*? passed in", re.I)
STOP_ERROR_RE = re.compile(r"(?:fatal|crash|aborted|cannot be started|incorrect input parameters|init_parameters_incorrect)", re.I)
GENETIC_INVALID_PARAMETER_PASS_RE = re.compile(
    r"\bgenetic pass\s+\([^)]*\)\s+tested with error\s+['\"]incorrect input parameters['\"]",
    re.I,
)
DEFAULT_MAX_PASSES = 100000000
DEFAULT_PARTIAL_COMPLETION_RATIO = 0.90
UI_OBSERVATION_INTERVAL_SECONDS = 300


def _click_stop(terminal_pid: int) -> dict:
    if stop_control is not None:
        return stop_control.click_stop(int(terminal_pid))
    ps = r'''
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
$p = Get-Process -Id $env:KCOS_TERMINAL_PID -ErrorAction Stop
if ($p.MainWindowHandle -eq 0) { throw 'terminal has no main window' }
$root = [System.Windows.Automation.AutomationElement]::FromHandle($p.MainWindowHandle)
$condition = New-Object System.Windows.Automation.PropertyCondition -ArgumentList @([System.Windows.Automation.AutomationElement]::ControlTypeProperty, [System.Windows.Automation.ControlType]::Button)
$buttons = $root.FindAll([System.Windows.Automation.TreeScope]::Descendants, $condition)
$found = $false
for ($i=0; $i -lt $buttons.Count; $i++) { $b=$buttons.Item($i); if ($b.Current.Name -match '^(Stop|??????)$') { $b.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern).Invoke(); $found=$true; break } }
if (-not $found) { throw 'enabled MT5 Stop button not found' }
[pscustomobject]@{stopped=$true;pid=[int]$env:KCOS_TERMINAL_PID}|ConvertTo-Json -Compress
'''
    import os, subprocess
    env=dict(os.environ); env['KCOS_TERMINAL_PID']=str(int(terminal_pid))
    r=subprocess.run(['powershell','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-Command',ps],capture_output=True,text=True,timeout=30,env=env)
    if r.returncode == 0:
        return json.loads(r.stdout.strip())
    # MT5 can hide the Tester Stop control from UI Automation. If the caller
    # supplied the cycle-owned terminal PID, stop only that process tree.
    try:
        root_proc = psutil.Process(int(terminal_pid))
        children = root_proc.children(recursive=True)
        for child in children:
            child.terminate()
        psutil.wait_procs(children, timeout=5)
        if root_proc.is_running():
            root_proc.terminate()
        try:
            root_proc.wait(timeout=5)
        except psutil.TimeoutExpired:
            root_proc.kill()
        return {"stopped": True, "pid": int(terminal_pid), "method": "owned_process_tree_fallback",
                "gui_error": (r.stderr or r.stdout).strip()}
    except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
        raise RuntimeError((r.stderr or r.stdout).strip() or str(exc))


def read_gui_counter(pid: int) -> dict:
    helper = Path(__file__).with_name("kcos_stage3_ui_pass_watch_v2.py")
    route = "bounded_gui_helper_subprocess"
    if not helper.is_file():
        return {"ok": False, "detail": "GUI observer helper is missing", "observer_route": route}
    hermes_root = Path(os.environ.get("LOCALAPPDATA", r"C:\Users\Tyson\AppData\Local")) / "hermes"
    interpreters = [hermes_root / "hermes-agent" / "venv" / "Scripts" / "python.exe", Path(sys.executable)]
    seen = set()
    errors = []
    for interpreter in interpreters:
        key = str(interpreter.resolve()).casefold()
        if key in seen or not interpreter.is_file():
            continue
        seen.add(key)
        try:
            proc = subprocess.run([str(interpreter), str(helper), "--pid", str(pid), "--once", "--dry-run"],
                                  capture_output=True, text=True, timeout=30)
            for line in reversed(proc.stdout.splitlines()):
                try:
                    result = ast.literal_eval(line.strip())
                except (SyntaxError, ValueError):
                    continue
                if isinstance(result, dict) and "ok" in result:
                    result["observer_route"] = route
                    return result
            errors.append(proc.stderr.strip() or "GUI helper returned no result")
        except Exception as exc:
            errors.append(str(exc))
    return {"ok": False, "detail": "; ".join(errors) or "GUI observer interpreter unavailable",
            "observer_route": route}


def observe_start_required_eta(runtime: dict, runtime_path: Path,
                               now_epoch: float | None = None) -> dict:
    """Observe the bound Tester clock in START_REQUIRED without clicking UI tabs.

    Stage 3 cron can see launch-bound Tester log progress before it can register
    worker processes or promote the runtime to OPTIMIZING. This narrow path
    applies only to that verified state and samples the UI at most every five
    minutes, beginning after ten minutes of runtime.
    """
    now = time.time() if now_epoch is None else float(now_epoch)
    result = dict(runtime)
    require(result.get("schema_version") == 2 and result.get("stage") == 3
            and result.get("status") == "START_REQUIRED",
            "ETA observation requires the authenticated Stage 3 START_REQUIRED runtime")
    require(int(result.get("terminal_pid") or 0) > 0
            and pid_exists(int(result.get("terminal_pid") or 0)),
            "ETA observation requires the launch-bound terminal to remain alive")
    progress = result.get("terminal_log_progress") or {}
    progress_age = progress.get("progress_age_seconds")
    stall_budget = max(900, int(result.get("progress_stall_budget_seconds", 900)))
    require(progress.get("authenticated") is True
            and isinstance(progress_age, (int, float))
            and 0 <= float(progress_age) <= stall_budget,
            "ETA observation requires recent authenticated launch-bound Tester progress")
    launched = float(result.get("launched_epoch") or parse_utc(result["launched_utc"]))
    if now - launched < UI_ETA_CHECKPOINT_SECONDS:
        return result

    last_sample = float(result.get("tester_ui_sampled_epoch") or 0)
    if last_sample and now - last_sample < UI_OBSERVATION_INTERVAL_SECONDS:
        counter = result.get("tester_ui_counter") or {}
        if result.get("eta_cutoff_stop_pending") is True:
            return result
        if last_sample - launched < UI_ETA_CHECKPOINT_SECONDS:
            return result
    else:
        try:
            counter = read_gui_counter(int(result["terminal_pid"]))
        except Exception as exc:
            counter = {"ok": False, "detail": f"UI counter read failed: {exc}"}
        result.update({
            "tester_ui_sampled_epoch": now,
            "tester_ui_sampled_utc": utc_now(),
            "tester_ui_counter": counter,
            "tester_ui_sample_error": counter.get("detail") if not counter.get("ok") else None,
        })
        atomic_json(runtime_path, result)
    timing = counter.get("tester_timing") if isinstance(counter, dict) else None
    result["tester_ui_timing"] = (
        {"source": "authenticated_bound_tester_gui_bottom_right", **timing}
        if isinstance(timing, dict) else None
    )
    if not (counter.get("ok") is True and counter.get("button_state") == "STOP"
            and isinstance(timing, dict)
            and eta_cutoff_triggered(timing.get("elapsed_seconds", 0),
                                     timing.get("estimated_total_seconds", 0))):
        return result

    checkpoint = int(UI_ETA_CHECKPOINT_SECONDS)
    projection = {
        "elapsed_seconds": float(timing["elapsed_seconds"]),
        "runtime_elapsed_seconds": max(0.0, now - launched),
        "completed_passes": int(counter.get("current", 0) or 0),
        "total_passes": int(counter.get("total", 0) or 0),
        "projected_seconds": float(timing["estimated_total_seconds"]),
        "estimated_total_seconds": float(timing["estimated_total_seconds"]),
        "sample_qualified": True, "over_budget": True,
        "checkpoint_seconds": checkpoint,
        "limit_seconds": UI_ETA_SKIP_SECONDS,
        "source": "authenticated_bound_tester_gui_bottom_right",
        "tester_elapsed_seconds": timing["elapsed_seconds"],
        "tester_elapsed_text": timing.get("elapsed_text"),
        "tester_estimated_total_text": timing.get("estimated_total_text"),
    }
    result["eta_cutoff_projection"] = projection
    try:
        stop = _click_stop(int(result["terminal_pid"]))
    except Exception as exc:
        result.update({"eta_cutoff_stop_pending": True,
                       "eta_cutoff_stop_error": str(exc)[:500],
                       "completion_reason": "eta_over_4h_stop_unconfirmed"})
        atomic_json(runtime_path, result)
        return result
    if stop.get("stopped") is not True:
        result.update({"eta_cutoff_stop_pending": True,
                       "eta_cutoff_stop_error": "MT5 stop control did not confirm stop",
                       "completion_reason": "eta_over_4h_stop_unconfirmed"})
        atomic_json(runtime_path, result)
        return result

    result.update({
        "status": "COMPUTE_BUDGET_EXCEEDED",
        "completion_reason": "eta_over_4h_after_10m",
        "failure_mode": "compute_budget_exceeded",
        "duration_policy_action": "skip_eta_over_4h",
        "duration_projection": projection, "duration_stop": stop,
        "duration_stop_utc": utc_now(), "eta_cutoff_stop_pending": False,
    })
    event_path = Path(result["timeframe_event"])
    require(not event_path.exists(),
            "ETA skip will not overwrite an existing Stage 3 timeframe event")
    event = {
        "schema_version": 2,
        "event_id": f"{result['cycle_id']}:stage3:{result['timeframe']}:compute_budget_exceeded",
        "cycle_id": result["cycle_id"], "stage": 3,
        "status": "COMPUTE_BUDGET_EXCEEDED", "timeframe": result["timeframe"],
        "failure_mode": "compute_budget_exceeded",
        "completion_reason": "eta_over_4h_after_10m",
        "duration_policy_action": "skip_eta_over_4h",
        "duration_projection": projection, "duration_stop": stop,
        "evidence": {"tester_ui_timing": {
            "source": "authenticated_bound_tester_gui_bottom_right", **timing},
            "tester_ui_counter": counter,
            "terminal_log_progress": progress,
            "stop": stop,
        },
        "metrics": {
            "completed_passes": int(counter.get("current", 0) or 0),
            "elapsed_seconds": max(0.0, now - launched),
            "seconds_since_last_pass": None,
        },
        "completed_utc": utc_now(), "consumed": False,
    }
    atomic_json(event_path, event)
    atomic_json(runtime_path, result)
    return result


def parse_utc(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()


def pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name != "nt":
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
    process = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
    if not process:
        return False
    ctypes.windll.kernel32.CloseHandle(process)
    return True


def structurally_complete_opt(path: Path) -> tuple[bool, dict]:
    if not path.is_file() or path.stat().st_size < 436:
        return False, {"reason": "opt_missing_or_too_small"}
    with path.open("rb") as handle:
        handle.seek(428)
        raw = handle.read(8)
    if len(raw) != 8:
        return False, {"reason": "opt_header_truncated"}
    header_size = int.from_bytes(raw[:4], "little")
    record_size = int.from_bytes(raw[4:], "little")
    size = path.stat().st_size
    if header_size < 436 or record_size < 232 or header_size > size:
        return False, {"reason": "opt_header_invalid", "header_size": header_size, "record_size": record_size}
    payload = size - header_size
    complete = payload > 0 and payload % record_size == 0
    return complete, {"header_size": header_size, "record_size": record_size, "records": payload // record_size,
                      "trailing_bytes": payload % record_size}


def fresh_log_completion(log_path: Path, start_offset: int, launch_epoch: float) -> tuple[bool, dict]:
    if not log_path.is_file() or log_path.stat().st_mtime < launch_epoch or log_path.stat().st_size <= start_offset:
        return False, {"detail": "no fresh tester-log bytes", "early": False, "final": False,
                       "cache_saved": False, "sha256": None, "byte_count": 0,
                       "end_offset": start_offset}
    with log_path.open("rb") as handle:
        handle.seek(start_offset)
        raw = handle.read()
    candidates = (raw.decode("utf-16le", errors="ignore"), raw.decode("utf-8", errors="ignore"))
    text = max(candidates, key=lambda value: sum(bool(pattern.search(value)) for pattern in
                                                 (EARLY_COMPLETE_RE, FINAL_COMPLETE_RE, CACHE_SAVED_RE)))
    early = EARLY_COMPLETE_RE.search(text)
    final = FINAL_COMPLETE_RE.search(text)
    cache_saved = CACHE_SAVED_RE.search(text)
    authoritative = bool(final and cache_saved)
    markers = [match.group(0) for match in (early, final, cache_saved) if match]
    detail = "; ".join(markers) if markers else "no fresh completion line"
    diagnostic_lines = []
    error_lines = []
    for line in text.splitlines():
        if not STOP_ERROR_RE.search(line):
            continue
        # MT5 can reject an individual genetic candidate with INIT_PARAMETERS_INCORRECT
        # and continue optimization. Keep those lines as diagnostics, matching the
        # Stage 3 journal classifier; only execution-wide errors block completion.
        if GENETIC_INVALID_PARAMETER_PASS_RE.search(line):
            diagnostic_lines.append(line.strip())
        else:
            error_lines.append(line.strip())
    return authoritative, {"detail": detail, "early": bool(early), "final": bool(final),
                           "cache_saved": bool(cache_saved), "clean": not error_lines,
                           "error_lines": error_lines[-20:],
                           "diagnostic_error_lines": diagnostic_lines[-80:],
                           "sha256": hashlib.sha256(raw).hexdigest().upper(),
                           "byte_count": len(raw), "end_offset": start_offset + len(raw)}


def pass_limit_completion_proven(final: dict, max_passes: int) -> bool:
    """Require either the authenticated UI ceiling or this run's clean genetic receipt."""
    ui_proof = bool(
        final.get("ui_counter_valid") is True
        and final.get("ui_passes_complete") is True
        and int(final.get("ui_completed_passes", 0)) >= max_passes
    )
    if ui_proof:
        return True
    evidence = final.get("fresh_log_evidence")
    if not isinstance(evidence, dict):
        return False
    start = final.get("tester_log_start_offset")
    end = final.get("fresh_log_end_offset")
    byte_count = evidence.get("byte_count")
    digest = final.get("fresh_log_sha256")
    return bool(
        final.get("completion_basis") == "fresh_genetic_journal"
        and final.get("fresh_log_completion") is True
        and evidence.get("early") is True
        and evidence.get("final") is True
        and evidence.get("cache_saved") is True
        and evidence.get("clean") is True
        and not evidence.get("error_lines")
        and isinstance(digest, str) and len(digest) == 64
        and all(char in "0123456789abcdefABCDEF" for char in digest)
        and evidence.get("sha256") == digest
        and isinstance(start, int) and isinstance(end, int)
        and isinstance(byte_count, int) and byte_count > 0
        and end == start + byte_count
    )


def validate_monitor_only_recovery(runtime: dict) -> bool:
    """Accept a worker-less runtime only when its immutable receipt binds it."""
    recovery = runtime.get("monitor_only_recovery")
    if not isinstance(recovery, dict):
        return False
    try:
        receipt_path = Path(str(recovery["receipt_path"]))
        raw = receipt_path.read_bytes()
        if hashlib.sha256(raw).hexdigest().upper() != recovery.get("receipt_sha256"):
            return False
        receipt = json.loads(raw.decode("utf-8"))
        return bool(
            receipt.get("schema_version") == 1
            and receipt.get("status") == "monitor_only_recovery_inputs_verified"
            and receipt.get("cycle_id") == runtime.get("cycle_id")
            and receipt.get("ea") == runtime.get("ea")
            and receipt.get("timeframe") == runtime.get("timeframe")
            and receipt.get("terminal_pid") == runtime.get("terminal_pid")
            and receipt.get("tester_log") == runtime.get("tester_log")
            and receipt.get("tester_log_start_offset") == runtime.get("tester_log_start_offset")
            and receipt.get("opt_path") == runtime.get("opt_path")
            and receipt.get("opt_sha256") == recovery.get("opt_sha256")
            and receipt.get("launched_epoch") == runtime.get("launched_epoch")
        )
    except (OSError, ValueError, TypeError, KeyError):
        return False


def completed_agent_passes(agent_logs: list[dict]) -> tuple[int, list[dict]]:
    total, evidence = 0, []
    for registration in agent_logs:
        path = Path(registration["path"])
        start = int(registration.get("start_offset", 0))
        raw = b""
        if path.is_file() and path.stat().st_size > start:
            with path.open("rb") as handle:
                handle.seek(start)
                raw = handle.read()
        candidates = (raw.decode("utf-16le", errors="ignore"), raw.decode("utf-8", errors="ignore"))
        count = max(len(PASS_COMPLETE_RE.findall(text)) for text in candidates)
        total += count
        evidence.append({**registration, "current_size": path.stat().st_size if path.is_file() else 0,
                         "completed_passes": count})
    return total, evidence


def sample_active_workers(runtime: dict, alive: list[int]) -> tuple[list[int], dict[str, float]]:
    previous = {str(key): float(value) for key, value in runtime.get("worker_cpu_seconds", {}).items()}
    current, active = {}, []
    for pid in alive:
        try:
            cpu = psutil.Process(int(pid)).cpu_times()
            value = float(cpu.user + cpu.system)
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
        current[str(pid)] = value
        if str(pid) in previous and value > previous[str(pid)] + 0.001:
            active.append(pid)
    return active, current


def evaluate_snapshot(runtime: dict, now_epoch: float, alive_pids: list[int] | None = None,
                      active_pids: list[int] | None = None) -> dict:
    require(runtime.get("schema_version") == 2 and runtime.get("stage") == 3, "runtime schema mismatch")
    require(runtime.get("status") in {"STARTING", "OPTIMIZING", "TIMEFRAME_COMPLETE", "PARTIAL_TIMEFRAME_COMPLETE", "OPTIMIZATION_STOPPED", "COMPUTE_BUDGET_EXCEEDED"},
            "runtime is not optimizing")
    if runtime.get("status") == "COMPUTE_BUDGET_EXCEEDED":
        return runtime
    opt_path = Path(runtime["opt_path"])
    log_path = Path(runtime["tester_log"])
    launch_epoch = parse_utc(runtime["launched_utc"])
    pids = [int(value) for value in runtime.get("worker_pids", [])]
    recovery_verified = validate_monitor_only_recovery(runtime)
    require(bool(pids) or recovery_verified,
            "registered worker_pids or a hash-bound monitor-only recovery receipt are required")
    alive = alive_pids if alive_pids is not None else [pid for pid in pids if pid_exists(pid)]
    sampled_active, cpu_samples = sample_active_workers(runtime, alive)
    active = active_pids if active_pids is not None else sampled_active
    size = opt_path.stat().st_size if opt_path.is_file() else 0
    mtime = opt_path.stat().st_mtime if opt_path.is_file() else 0.0
    previous_size = int(runtime.get("last_opt_size", -1))
    last_growth = float(runtime.get("last_opt_growth_epoch", launch_epoch))
    if size != previous_size:
        # A watcher restart can miss the final write; use the file mtime to
        # account for settle time already elapsed while it was offline.
        last_growth = mtime if mtime else now_epoch
    stable_seconds = max(0.0, now_epoch - last_growth)
    log_done, log_evidence = fresh_log_completion(log_path, int(runtime.get("tester_log_start_offset", 0)), launch_epoch)
    structural, structure = structurally_complete_opt(opt_path)
    prior_ui_sample = runtime.get("tester_ui_counter") or runtime.get("ui_pass_counter")
    last_ui_sample_epoch = float(runtime.get("tester_ui_sampled_epoch") or 0)
    ui_sample_due = (
        not isinstance(prior_ui_sample, dict)
        or not last_ui_sample_epoch
        or now_epoch - last_ui_sample_epoch >= UI_OBSERVATION_INTERVAL_SECONDS
        or log_done
        or not alive
    )
    if ui_sample_due:
        try:
            ui_counter = read_gui_counter(int(runtime["terminal_pid"]))
        except Exception as exc:
            ui_counter = {"ok": False, "detail": f"UI counter read failed: {exc}"}
        ui_sample_epoch = now_epoch
    else:
        ui_counter = dict(prior_ui_sample)
        ui_sample_epoch = last_ui_sample_epoch
    ui_counter_valid = bool(
        ui_counter.get("ok")
        and int(ui_counter.get("current", 0)) > 0
        and int(ui_counter.get("total", 0)) > 0
        and int(ui_counter.get("current", 0)) <= int(ui_counter.get("total", 0))
    )
    # OCR can read an optimization-table number as the bottom-right Tester
    # counter. Reject impossible current > total pairs before they influence
    # completion, partial-completion, or pass-limit decisions.
    ui_passes = int(ui_counter.get("current", 0)) if ui_counter_valid else 0
    baseline_size = runtime.get("opt_size_before")
    baseline_mtime = runtime.get("opt_mtime_before")
    opt_fresh = structural and mtime >= launch_epoch and (
        baseline_size is None or size != baseline_size or baseline_mtime is None or mtime != baseline_mtime
    )
    workers_stopped = len(alive) == 0
    has_agent_logs = bool(runtime.get("agent_logs"))
    completed_passes, agent_evidence = completed_agent_passes(runtime.get("agent_logs", []))
    # Agent journal events and OPT records are progress telemetry. Distributed
    # workers can emit many records per Tester pass, so these counts are never
    # compared with the Tester-wide pass ceiling.
    completed_passes = max(completed_passes, int(structure.get("records", 0)), ui_passes)
    previous_passes = int(runtime.get("completed_passes", 0))
    last_pass_epoch = runtime.get("last_completed_pass_epoch")
    if completed_passes > previous_passes:
        last_pass_epoch = now_epoch
    elapsed = max(0.0, now_epoch - launch_epoch)
    first_budget = int(runtime.get("first_pass_budget_seconds", 120))
    first_hard_limit = int(runtime.get("first_pass_hard_limit_seconds", 200))
    stall_budget = int(runtime.get("progress_stall_budget_seconds", 180))
    total_budget = int(runtime.get("timeframe_budget_seconds", 14400))
    startup_grace = int(runtime.get("startup_grace_seconds", 300))
    seconds_since_last_pass = None if last_pass_epoch is None else max(0.0, now_epoch - float(last_pass_epoch))
    first_pass_seconds = runtime.get("first_pass_seconds")
    if first_pass_seconds is None and completed_passes > 0:
        first_pass_seconds = elapsed
    if first_pass_seconds is not None:
        first_pass_seconds = float(first_pass_seconds)
        compute_qualification = "qualified" if first_pass_seconds <= first_budget else "slow_first_pass"
        first_pass_censored_seconds = None
    elif elapsed >= max(first_hard_limit, startup_grace):
        compute_qualification = "censored_no_first_pass"
        first_pass_censored_seconds = first_hard_limit
    else:
        compute_qualification = "pending_first_pass"
        first_pass_censored_seconds = None
    ui_passes_complete = bool(
        ui_counter_valid
        and int(ui_counter.get("current", 0)) == int(ui_counter.get("total", 0))
    )
    gui_stopped = bool(ui_counter.get("ok") and ui_counter.get("button_state") == "START")
    user_stopped = bool(
        gui_stopped and structural and opt_fresh
        and stable_seconds >= int(runtime.get("normal_settle_seconds", 20))
    )
    # Genetic optimization has a dynamic stopping point. MT5's fresh journal
    # completion markers, finalized cache, clean log, and settled fresh OPT are
    # authoritative when the GUI's estimated pass total remains above the
    # actual genetic finish pass.
    genetic_journal_done = bool(
        log_done and log_evidence.get("early") is True
        and log_evidence.get("final") is True
        and log_evidence.get("cache_saved") is True
        and log_evidence.get("clean") is True
        and not log_evidence.get("error_lines")
    )
    # A visible Start button is authoritative Tester-stop evidence. Local MT5
    # agents may linger after the Tester has finalized the cache; retain their
    # PIDs as telemetry but do not let them block GUI-based settlement.
    # Full completion requires either the exact UI total or the fresh, clean
    # MT5 genetic-finish receipt; partial results remain a separate hold path.
    # MT5 may close the Tester UI before the watcher samples its Start button.
    # The run-bound genetic-finish + cache-save journal is authoritative once
    # the fresh OPT settles; disconnected local agents may linger as processes.
    journal_stopped = genetic_journal_done
    # A fully completed Tester counter with the bound UI showing Start is also
    # a complete run. Requiring a genetic-finish Journal line here strands
    # legitimate completed OPTs when MT5 saves the cache but omits that line.
    ui_full_run_stopped = bool(ui_counter_valid and ui_passes_complete and gui_stopped)
    normal_done = (
        structural and opt_fresh
        and stable_seconds >= int(runtime.get("normal_settle_seconds", 20))
        and (ui_full_run_stopped or (
            log_done and (ui_passes_complete or genetic_journal_done)
            and (gui_stopped or journal_stopped)
        ))
    )
    partial_ratio = float(runtime.get("partial_completion_ratio", DEFAULT_PARTIAL_COMPLETION_RATIO))
    journal_override = bool(runtime.get("journal_override") is True and runtime.get("journal_override_scope") == "this_run")
    partial_done = bool(
        runtime.get("allow_partial_completion", True)
        and 0.0 < partial_ratio <= 1.0
        and ui_counter_valid
        and (int(ui_counter.get("current", 0)) / int(ui_counter.get("total", 0))) >= partial_ratio
        and user_stopped
        and not genetic_journal_done
        and (bool(log_evidence.get("byte_count", 0)) and log_evidence.get("clean") is True or journal_override)
    )
    timed_out = now_epoch - launch_epoch >= int(runtime.get("timeout_seconds", 14400))
    # Worker journal lines and OPT records count distributed work/results, not
    # the Tester-wide pass counter. Never compare their aggregate to the GUI
    # ceiling or stop MT5 from that aggregate.
    pass_limit_reached = bool(
        ui_counter_valid
        and ui_passes >= int(runtime.get("max_passes", DEFAULT_MAX_PASSES))
        and ui_passes == int(ui_counter.get("total", 0))
    )
    active_ids = {int(value) for value in active if str(value).lstrip("-").isdigit()}
    alive_ids = {int(value) for value in alive if str(value).lstrip("-").isdigit()}
    active_workers_present = bool(active_ids & alive_ids)
    first_pass_budget_exceeded = (
        active_workers_present and compute_qualification == "censored_no_first_pass"
    )
    progress_stall_exceeded = (
        completed_passes > 0
        and seconds_since_last_pass is not None
        and seconds_since_last_pass >= stall_budget
    )
    # Completion evidence always outranks slow/stall/budget telemetry. MT5 can
    # have long initialization gaps or slow passes and still later emit the
    # authoritative tester-journal completion marker. Never strand that result
    # behind an earlier COMPUTE_BUDGET_EXCEEDED classification.
    if normal_done:
        status, reason = "TIMEFRAME_COMPLETE", (
            "authoritative_genetic_journal_opt_settled"
            if genetic_journal_done and not ui_passes_complete
            else "full_tester_pass_counter_opt_settled"
            if ui_full_run_stopped and not log_done
            else "authoritative_log_opt_settled"
        )
    elif partial_done:
        status, reason = "PARTIAL_TIMEFRAME_COMPLETE", "stopped_after_authenticated_gui_threshold"
    elif (user_stopped and log_done and ui_counter_valid
          and not ui_passes_complete):
        status, reason = "OPTIMIZATION_STOPPED", "journal_final_but_tester_counter_incomplete"
    elif user_stopped:
        status, reason = "OPTIMIZATION_STOPPED", "tester_gui_start_button_settled_artifact"
    elif pass_limit_reached:
        status, reason = "OPTIMIZING", "pass_limit_reached_controlled_stop_pending"
    elif first_pass_budget_exceeded:
        status, reason = "COMPUTE_BUDGET_EXCEEDED", "first_pass_hard_limit_exceeded_with_active_worker"
    elif progress_stall_exceeded:
        worker_state = "with_active_worker" if active_workers_present else "without_active_workers"
        status, reason = "COMPUTE_BUDGET_EXCEEDED", f"progress_stall_budget_exceeded_{worker_state}"
    elif timed_out and not log_done:
        status, reason = "COMPUTE_BUDGET_EXCEEDED", "timeframe_budget_exceeded"
    else:
        status, reason = "OPTIMIZING", "waiting_for_gui_pass_completion_or_qualified_duration_limit"
    return {
        **runtime, "status": status, "completion_reason": reason, "observed_utc": utc_now(),
        "zero_token": True, "alive_worker_pids": alive, "last_opt_size": size,
        "last_opt_mtime_epoch": mtime, "last_opt_growth_epoch": last_growth,
        "stable_seconds": stable_seconds, "fresh_log_completion": log_done,
        "fresh_log_detail": log_evidence["detail"], "fresh_log_evidence": log_evidence,
        "fresh_log_sha256": log_evidence.get("sha256"),
        "fresh_log_byte_count": log_evidence.get("byte_count", 0),
        "fresh_log_end_offset": log_evidence.get("end_offset"),
        "opt_structurally_complete": structural,
        "opt_fresh": opt_fresh, "opt_structure": structure,
        "completed_passes": completed_passes,
        "ui_pass_counter": ui_counter,
        "tester_ui_counter": ui_counter,
        "tester_ui_sampled_epoch": ui_sample_epoch,
        "tester_ui_sampled_utc": (
            datetime.fromtimestamp(ui_sample_epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            if ui_sample_epoch else None
        ),
        "tester_ui_sampled_this_tick": ui_sample_due,
        "tester_ui_timing": ({
            "source": "authenticated_bound_tester_gui_bottom_right",
            **ui_counter["tester_timing"],
        } if isinstance(ui_counter.get("tester_timing"), dict) else None),
        "ui_completed_passes": ui_passes,
        "ui_total_passes": int(ui_counter.get("total", 0)) if ui_counter_valid else None,
        "ui_button_state": ui_counter.get("button_state") if ui_counter.get("ok") else None,
        "ui_observer_route": ui_counter.get("observer_route"),
        "journal_override": journal_override,
        "journal_override_reason": runtime.get("journal_override_reason"),
        "ui_passes_complete": ui_passes_complete,
        "ui_counter_valid": ui_counter_valid,
        "partial_completion_ratio": partial_ratio,
        "partial_completion_eligible": partial_done,
        "completion_mode": "partial_90_percent" if partial_done else ("full" if normal_done else None),
        "completion_basis": (
            "fresh_genetic_journal" if normal_done and genetic_journal_done and not ui_passes_complete
            else "tester_pass_counter" if normal_done else None
        ),
        "ui_pass_limit_reached": pass_limit_reached,
        "last_completed_pass_epoch": last_pass_epoch,
        "active_worker_pids": active,
        "worker_cpu_seconds": cpu_samples,
        "agent_log_evidence": agent_evidence,
        "elapsed_seconds": elapsed,
        "seconds_since_last_pass": seconds_since_last_pass,
        "first_pass_budget_seconds": first_budget,
        "first_pass_hard_limit_seconds": first_hard_limit,
        "startup_grace_seconds": startup_grace,
        "first_pass_seconds": first_pass_seconds,
        "first_pass_censored_seconds": first_pass_censored_seconds,
        "compute_qualification": compute_qualification,
        "progress_stall_budget_seconds": stall_budget,
        "timeframe_budget_seconds": total_budget,
        "failure_mode": ("compute_stalled" if status == "COMPUTE_BUDGET_EXCEEDED" and
                         reason.startswith("progress_stall") else
                         "compute_budget_exceeded" if status == "COMPUTE_BUDGET_EXCEEDED" else
                         runtime.get("failure_mode")),
        "compute_budget_trigger": reason if status == "COMPUTE_BUDGET_EXCEEDED" else runtime.get("compute_budget_trigger"),
    }


def _stop_at_pass_limit(runtime: dict, result: dict, runtime_path: Path,
                        max_passes: int) -> dict:
    """Stop MT5 through its Stop button, settle the cache, and close the timeframe."""
    require(result.get("ui_counter_valid") is True
            and result.get("ui_pass_limit_reached") is True
            and result.get("ui_passes_complete") is True
            and int(result.get("ui_completed_passes", 0)) >= max_passes,
            "authenticated Tester UI counter did not reach the pass ceiling")
    stop_result = _click_stop(int(runtime["terminal_pid"]))
    deadline = time.time() + int(runtime.get("pass_limit_settle_seconds", 60))
    last_size = -1
    stable_since = time.time()
    last_ui_passes = None
    ui_stable_reads = 0
    opt_path = Path(runtime["opt_path"])
    while time.time() < deadline:
        size = opt_path.stat().st_size if opt_path.is_file() else 0
        if size != last_size:
            last_size, stable_since = size, time.time()
        try:
            settled_ui = read_gui_counter(int(runtime["terminal_pid"]))
        except Exception:
            settled_ui = {"ok": False}
        if settled_ui.get("ok"):
            observed = int(settled_ui["current"])
            if observed == last_ui_passes:
                ui_stable_reads += 1
            else:
                last_ui_passes, ui_stable_reads = observed, 1
        if (size > 0 and time.time() - stable_since >= int(runtime.get("normal_settle_seconds", 20))
                and ui_stable_reads >= 3):
            break
        time.sleep(2)
    require(opt_path.is_file(), "pass-limit stop left no optimization artifact")
    final = evaluate_snapshot(runtime, time.time(), alive_pids=[], active_pids=[])
    require(final.get("opt_structurally_complete") is True and final.get("opt_fresh") is True,
            "pass-limit artifact is not fresh and structurally complete")
    require(pass_limit_completion_proven(final, max_passes),
            "pass-limit stop lacks an authenticated UI ceiling or fresh clean genetic-finish receipt")
    require(final.get("fresh_log_completion") is True and final.get("fresh_log_sha256"),
            "pass-limit stop lacks authoritative fresh Tester Journal completion evidence")
    final.update({"status": "TIMEFRAME_COMPLETE", "completion_reason": "max_pass_limit_reached",
                  "pass_limit": max_passes, "pass_limit_stop": stop_result,
                  "pass_limit_stop_utc": utc_now()})
    return final


def immutable_opt_snapshot_path(runtime_path: Path, runtime: dict, source_opt: Path) -> Path:
    """Choose a content-bound OPT name without overwriting an earlier TF attempt."""
    parent = runtime_path.parent
    base = parent / f"{runtime['cycle_id']}.stage3.{runtime['timeframe']}.opt"
    source_hash = sha256(source_opt)
    if base.is_file() and sha256(base) != source_hash:
        base = parent / f"{runtime['cycle_id']}.stage3.{runtime['timeframe']}.{source_hash[:12]}.opt"
    return base


def run_once(runtime_path: Path, heartbeat_path: Path, now_epoch: float | None = None,
             alive_pids: list[int] | None = None, active_pids: list[int] | None = None,
             max_passes: int | None = None) -> dict:
    runtime = load_object(runtime_path)
    max_passes = int(runtime.get("max_passes", DEFAULT_MAX_PASSES) if max_passes is None else max_passes)
    result = evaluate_snapshot(runtime, time.time() if now_epoch is None else now_epoch, alive_pids, active_pids)
    timing = result.get("tester_ui_timing")
    pending_projection = runtime.get("eta_cutoff_projection")
    pending_stop = runtime.get("eta_cutoff_stop_pending") is True
    cutoff_already_stopped = (
        pending_stop and result.get("ui_button_state") == "START"
        and isinstance(pending_projection, dict)
    )
    cutoff_triggered = (
        result.get("status") == "OPTIMIZING" and isinstance(timing, dict)
        and eta_cutoff_triggered(timing.get("elapsed_seconds", 0),
                                 timing.get("estimated_total_seconds", 0))
    )
    cutoff_retry = (
        pending_stop and result.get("status") in {"OPTIMIZING", "OPTIMIZATION_STOPPED"}
        and isinstance(pending_projection, dict)
    )
    if cutoff_already_stopped or cutoff_triggered or cutoff_retry:
        projection = pending_projection if cutoff_retry else {
            "elapsed_seconds": float(timing["elapsed_seconds"]),
            "runtime_elapsed_seconds": float(result.get("elapsed_seconds", 0)),
            "completed_passes": int(result.get("completed_passes", 0)),
            "total_passes": int(result.get("ui_total_passes") or result.get("total_passes") or 0),
            "projected_seconds": float(timing["estimated_total_seconds"]),
            "estimated_total_seconds": float(timing["estimated_total_seconds"]),
            "sample_qualified": True,
            "over_budget": True,
            "checkpoint_seconds": UI_ETA_CHECKPOINT_SECONDS,
            "limit_seconds": UI_ETA_SKIP_SECONDS,
            "source": timing.get("source"),
            "tester_elapsed_seconds": timing.get("elapsed_seconds"),
            "tester_elapsed_text": timing.get("elapsed_text"),
            "tester_estimated_total_text": timing.get("estimated_total_text"),
        }
        stop_result = {"stopped": True, "method": "tester_already_stopped"} if cutoff_already_stopped else None
        if not cutoff_already_stopped:
            try:
                if result.get("ui_button_state") == "START":
                    stop_result = {"stopped": True, "method": "tester_already_stopped"}
                else:
                    stop_result = _click_stop(int(runtime["terminal_pid"]))
            except Exception as exc:
                result.update({"eta_cutoff_stop_pending": True,
                               "eta_cutoff_stop_error": str(exc)[:500],
                               "eta_cutoff_projection": projection,
                               "completion_reason": "eta_over_4h_stop_unconfirmed"})
        if stop_result is not None and stop_result.get("stopped") is True:
            result.update({
                "status": "COMPUTE_BUDGET_EXCEEDED",
                # Bind the reason label to the configured checkpoint. Historical
                # 4-minute events stay immutable; new 10-minute events say so.
                "completion_reason": f"eta_over_4h_after_{UI_ETA_CHECKPOINT_SECONDS // 60}m",
                "failure_mode": "compute_budget_exceeded",
                "duration_policy_action": "skip_eta_over_4h",
                "duration_projection": projection,
                "duration_stop": stop_result,
                "duration_stop_utc": utc_now(),
                "eta_cutoff_stop_pending": False,
                "eta_cutoff_projection": projection,
            })
        elif stop_result is not None:
            result.update({"eta_cutoff_stop_pending": True,
                           "eta_cutoff_stop_error": "MT5 stop control did not confirm stop",
                           "eta_cutoff_projection": projection,
                           "completion_reason": "eta_over_4h_stop_unconfirmed"})
    # Apply the qualified timeframe budget to the authenticated grid. The
    # production default-wide profile is never narrowed to satisfy the cap.
    if result["status"] == "OPTIMIZING" and runtime.get("grid_mode") in {None, "default", "reduced"}:
        projection = project(result.get("elapsed_seconds", 0), result.get("completed_passes", 0),
                             result.get("ui_total_passes") or result.get("total_passes", 0),
                             runtime.get("timeframe_budget_seconds", 14400))
        if projection.over_budget:
            stop_result = _click_stop(int(runtime["terminal_pid"]))
            reduced = runtime.get("grid_mode") == "reduced"
            result.update({
                "status": "COMPUTE_BUDGET_EXCEEDED" if reduced else "DURATION_REDUCTION_REQUIRED",
                "completion_reason": "timeframe_budget_exceeded",
                "failure_mode": "compute_budget_exceeded" if reduced else "projected_timeframe_over_budget",
                "duration_policy_action": "rotate_reduced_over_budget" if reduced else "resume_wide_or_review",
                "duration_projection": projection.__dict__,
                "duration_stop": stop_result,
                "duration_stop_utc": utc_now(),
            })
    fresh_clean_genetic_receipt = bool(
        result.get("fresh_log_completion") is True
        and isinstance(result.get("fresh_log_evidence"), dict)
        and result["fresh_log_evidence"].get("early") is True
        and result["fresh_log_evidence"].get("final") is True
        and result["fresh_log_evidence"].get("cache_saved") is True
        and result["fresh_log_evidence"].get("clean") is True
        and not result["fresh_log_evidence"].get("error_lines")
    )
    if (result["status"] == "OPTIMIZING"
            and result.get("ui_pass_limit_reached") is True
            and not fresh_clean_genetic_receipt):
        result = _stop_at_pass_limit(runtime, result, runtime_path, max_passes)
    atomic_json(heartbeat_path, result)
    atomic_json(runtime_path, result)
    if result["status"] in {"TIMEFRAME_COMPLETE", "PARTIAL_TIMEFRAME_COMPLETE"}:
        event_path = Path(runtime["timeframe_event"])
        source_opt = Path(result["opt_path"]).resolve()
        require(source_opt.is_file(), "completed timeframe has no optimization artifact")
        immutable_opt = immutable_opt_snapshot_path(runtime_path, runtime, source_opt)
        if not immutable_opt.is_file():
            shutil.copy2(source_opt, immutable_opt)
        require(sha256(immutable_opt) == sha256(source_opt),
                "immutable timeframe OPT snapshot hash mismatch")
        result["opt_path"] = str(immutable_opt.resolve())
        result["opt_sha256"] = sha256(immutable_opt)
        atomic_json(heartbeat_path, result)
        atomic_json(runtime_path, result)
        event = {
            "schema_version": 2,
            "event_id": f"{runtime['cycle_id']}:stage3:{runtime['timeframe']}:complete",
            "cycle_id": runtime["cycle_id"], "stage": 3,
            "status": "TIMEFRAME_COMPLETE", "timeframe": runtime["timeframe"],
            "completion_mode": result.get("completion_mode") or "full",
            "completion_basis": result.get("completion_basis"),
            "journal_override": bool(result.get("journal_override")),
            "journal_override_reason": result.get("journal_override_reason"),
            "completion_ratio": (
                int(result.get("ui_completed_passes", 0)) / int(result.get("ui_total_passes", 0))
                if result.get("ui_counter_valid") is True
                and int(result.get("ui_total_passes", 0)) > 0
                and 0 <= int(result.get("ui_completed_passes", 0)) <= int(result.get("ui_total_passes", 0))
                else None
            ),
            "ui_passes_complete": result.get("ui_passes_complete") is True,
            "ui_pass_counter": {
                "ok": result.get("ui_counter_valid") is True,
                "current": result.get("ui_completed_passes"),
                "total": result.get("ui_total_passes"),
                "button_state": result.get("ui_button_state"),
                "source": (result.get("ui_pass_counter") or {}).get("counter_source"),
            },
            "opt_path": str(immutable_opt.resolve()), "opt_sha256": sha256(immutable_opt),
            "tester_log": result["tester_log"],
            "tester_log_start_offset": result["tester_log_start_offset"],
            "tester_log_end_offset": result["fresh_log_end_offset"],
            "tester_log_sha256": result["fresh_log_sha256"],
            "tester_log_completion_evidence": result["fresh_log_evidence"],
            "completion_reason": result["completion_reason"], "completed_utc": utc_now(), "consumed": False,
        }
        if event_path.exists():
            existing = load_object(event_path)
            require(existing.get("cycle_id") == event["cycle_id"] and existing.get("timeframe") == event["timeframe"]
                    and existing.get("opt_sha256") == event["opt_sha256"], "conflicting timeframe event exists")
        else:
            atomic_json(event_path, event)
    elif result["status"] == "DURATION_REDUCTION_REQUIRED":
        event_path = Path(runtime["timeframe_event"])
        event = {
            "schema_version": 2,
            "event_id": f"{runtime['cycle_id']}:stage3:{runtime['timeframe']}:duration_reduction_required",
            "cycle_id": runtime["cycle_id"], "stage": 3,
            "status": "DURATION_REDUCTION_REQUIRED", "timeframe": runtime["timeframe"],
            "failure_mode": "projected_timeframe_over_budget",
            "projection": result["duration_projection"],
            "default_grid": runtime.get("grid_mode", "default"),
            "completed_utc": utc_now(), "consumed": False,
        }
        if not event_path.exists():
            atomic_json(event_path, event)
    elif result["status"] == "COMPUTE_BUDGET_EXCEEDED":
        event_path = Path(runtime["timeframe_event"])
        event = {
            "schema_version": 2,
            "event_id": f"{runtime['cycle_id']}:stage3:{runtime['timeframe']}:compute_budget_exceeded",
            "cycle_id": runtime["cycle_id"], "stage": 3,
            "status": "COMPUTE_BUDGET_EXCEEDED", "timeframe": runtime["timeframe"],
            "failure_mode": result["failure_mode"], "completion_reason": result["completion_reason"],
            "duration_policy_action": result.get("duration_policy_action"),
            "duration_projection": result.get("duration_projection"),
            "duration_stop": result.get("duration_stop"),
            "metrics": {"completed_passes": result["completed_passes"],
                        "elapsed_seconds": result["elapsed_seconds"],
                        "seconds_since_last_pass": result["seconds_since_last_pass"],
                        "compute_qualification": result["compute_qualification"],
                        "first_pass_seconds": result["first_pass_seconds"],
                        "first_pass_censored_seconds": result["first_pass_censored_seconds"]},
            "evidence": {"active_worker_pids": result["active_worker_pids"],
                         "agent_logs": result["agent_log_evidence"],
                         "tester_ui_timing": result.get("tester_ui_timing"),
                         "duration_stop": result.get("duration_stop"),
                         "budgets": {"first_pass_seconds": result["first_pass_budget_seconds"],
                                     "first_pass_hard_limit_seconds": result["first_pass_hard_limit_seconds"],
                                     "progress_stall_seconds": result["progress_stall_budget_seconds"],
                                     "timeframe_seconds": result["timeframe_budget_seconds"]}},
            "completed_utc": utc_now(), "consumed": False,
        }
        if event_path.exists():
            existing = load_object(event_path)
            require(existing.get("cycle_id") == event["cycle_id"] and
                    existing.get("timeframe") == event["timeframe"] and
                    existing.get("status") == event["status"], "conflicting timeframe event exists")
        else:
            atomic_json(event_path, event)
    return result


def wait_until_optimizing(runtime_path: Path, timeout_seconds: float = 30.0,
                          poll_seconds: float = 0.1, loader=load_object) -> dict:
    """Handshake with the launcher without observing partial ownership state."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        runtime = loader(runtime_path)
        status = runtime.get("status")
        if status == "OPTIMIZING":
            return runtime
        if status == "START_REQUIRED":
            terminal_pid = int(runtime.get("terminal_pid") or 0)
            evidence = runtime.get("worker_process_evidence") or []
            worker_pids = [int(item.get("pid")) for item in evidence if item.get("pid") and pid_exists(int(item["pid"]))]
            if terminal_pid and pid_exists(terminal_pid) and worker_pids:
                runtime["worker_pids"] = worker_pids
                runtime["status"] = "OPTIMIZING"
                runtime["handshake_ui_counter"] = {"ok": True, "source": "owned_worker_evidence"}
                atomic_json(runtime_path, runtime)
                return runtime
            time.sleep(poll_seconds)
            continue
        if status == "STARTING":
            try:
                counter = read_gui_counter(int(runtime.get("terminal_pid", 0)))
            except Exception:
                counter = {"ok": False}
            if counter.get("ok"):
                runtime["status"] = "OPTIMIZING"
                runtime["handshake_ui_counter"] = counter
                atomic_json(runtime_path, runtime)
                return runtime
            time.sleep(poll_seconds)
            continue
        if status == "COMPUTE_BUDGET_EXCEEDED" and runtime.get("completion_reason") == "timeframe_budget_exceeded":
            runtime["status"] = "OPTIMIZING"
            atomic_json(runtime_path, runtime)
            return runtime
        if status in {"TIMEFRAME_COMPLETE", "PARTIAL_TIMEFRAME_COMPLETE", "OPTIMIZATION_STOPPED", "COMPUTE_BUDGET_EXCEEDED"}:
            return runtime
        if (status == "WATCHER_REGISTRATION_FAILED" and runtime.get("worker_pids")
                and runtime.get("opt_path") and runtime.get("fresh_journal_sha256")):
            runtime["status"] = "OPTIMIZING"
            atomic_json(runtime_path, runtime)
            return runtime
        require(False, f"watcher handshake found invalid runtime status: {status}")
    raise ValueError("watcher registration handshake timed out in STARTING")


def publish_watcher_ready(runtime_path: Path, ready_path: Path,
                          timeout_seconds: float = 10.0) -> dict:
    deadline = time.time() + timeout_seconds
    while True:
        runtime = load_object(runtime_path)
        require(runtime.get("status") in {"STARTING", "OPTIMIZING"}, "watcher can register only against STARTING or OPTIMIZING")
        recorded_pid = int(runtime.get("watcher_pid") or 0)
        if recorded_pid in {0, os.getpid()} or not psutil.pid_exists(recorded_pid):
            runtime["watcher_pid"] = os.getpid()
            atomic_json(runtime_path, runtime)
            break
        if time.time() >= deadline:
            raise ValueError("watcher PID registration handshake timed out")
        time.sleep(0.05)
    ready = {
        "schema_version": 1, "status": "WATCHER_READY", "watcher_pid": os.getpid(),
        "runtime": str(runtime_path.resolve()), "ready_utc": utc_now(),
    }
    atomic_json(ready_path, ready)
    return ready


def wake_supervisor(supervisor_path: Path | None = None, runner=subprocess.run) -> dict:
    """Wake the zero-token supervisor immediately; its claim remains authoritative."""
    supervisor_path = supervisor_path or Path(__file__).with_name("kcos_supervisor_v2.py")
    completed = runner(
        [sys.executable, str(supervisor_path), "--show"],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    detail = (completed.stdout or completed.stderr or "").strip()
    return {"accepted": completed.returncode == 0, "detail": detail}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--heartbeat", type=Path, required=True)
    parser.add_argument("--ready", type=Path)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-passes", type=int)
    args = parser.parse_args()
    if args.ready:
        publish_watcher_ready(args.runtime, args.ready)
    wait_until_optimizing(args.runtime)
    while True:
        result = run_once(args.runtime, args.heartbeat, max_passes=args.max_passes)
        if args.once or result["status"] != "OPTIMIZING":
            wake = wake_supervisor() if result["status"] in {"TIMEFRAME_COMPLETE", "PARTIAL_TIMEFRAME_COMPLETE", "COMPUTE_BUDGET_EXCEEDED"} and not args.once else None
            print(json.dumps({"status": result["status"], "reason": result["completion_reason"],
                              "supervisor_wake": wake}))
            # Both outcomes are expected, successfully published timeframe
            # evidence. COMPUTE_BUDGET_EXCEEDED is a skip/continuation signal,
            # not a watcher process failure.
            return 0 if result["status"] in {"TIMEFRAME_COMPLETE", "PARTIAL_TIMEFRAME_COMPLETE", "COMPUTE_BUDGET_EXCEEDED"} else 2
        time.sleep(int(result.get("poll_seconds", 10)))


if __name__ == "__main__":
    raise SystemExit(main())
