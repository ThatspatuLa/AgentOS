#!/usr/bin/env python3
"""One-pass, non-optimizing Stage 2 Strategy Tester diagnostic.

The experiment identity binds the effective authenticated Stage 2 contract,
the exact source and approved Minato include, terminal settings, and the
available symbol history files. A matching completed identity is reused;
an incomplete identity is held for explicit reconciliation.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from kcos_runtime_paths_v2 import assert_safe_root, is_canary, mt5_data_root
from kcos_tester_config_v2 import config_sha256, stage2_spec_from_spec

ROOT = assert_safe_root()
CANARY_PORTABLE_ROOT = ROOT / "state" / "mt5_stage45_portable"
MT5_EXE = Path(os.environ.get("KCOS_TESTER_TERMINAL") or CANARY_PORTABLE_ROOT / "terminal64.exe")
DATA_ROOT = CANARY_PORTABLE_ROOT.resolve()
MAX_STARTUP_SECONDS = 200
MAX_RUNTIME_SECONDS = 3600
POLL_SECONDS = 1


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest().upper()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def canonical_hash(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return sha256_bytes(raw)


def read_json(path: Path) -> dict:
    obj = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"expected JSON object: {path}")
    return obj


def history_identity(data_root: Path, symbol: str) -> dict:
    """Bind all local history chunks for this symbol into the run signature."""
    roots = (data_root / "Bases", data_root / "Tester" / "history")
    files = []
    for base in roots:
        if not base.is_dir():
            continue
        for path in base.rglob("*"):
            in_symbol_folder = any(parent.name.casefold() == symbol.casefold()
                                   for parent in path.parents)
            if (path.is_file() and path.suffix.casefold() == ".hcc"
                    and (path.stem.casefold().startswith(symbol.casefold()) or in_symbol_folder)):
                files.append({"path": str(path.resolve()), "size": path.stat().st_size,
                              "sha256": sha256_file(path)})
    files.sort(key=lambda row: row["path"].casefold())
    return {"data_root": str(data_root.resolve()), "symbol": symbol, "history_files": files,
            "history_sha256": canonical_hash(files)}


def assert_history_readable(data_root: Path, symbol: str) -> None:
    """Fail early on a profile file held exclusively by a live MT5 process."""
    roots = (data_root / "Bases", data_root / "Tester" / "history")
    for base in roots:
        if not base.is_dir():
            continue
        for path in base.rglob("*"):
            in_symbol_folder = any(parent.name.casefold() == symbol.casefold()
                                   for parent in path.parents)
            if (path.is_file() and path.suffix.casefold() == ".hcc"
                    and (path.stem.casefold().startswith(symbol.casefold()) or in_symbol_folder)):
                with path.open("rb") as stream:
                    stream.read(1)


def effective_contract(handoff: dict, verification: dict) -> dict:
    cycle = str(handoff.get("cycle_id") or "")
    if not cycle or verification.get("cycle_id") != cycle or verification.get("stage") != 2 or verification.get("passed") is not True:
        raise RuntimeError("Stage 2 tester requires a passing same-cycle verification")
    handoff_ea = str(handoff.get("ea") or "")
    verified_ea = str(verification.get("ea") or "")
    if not handoff_ea or handoff_ea != verified_ea:
        raise RuntimeError("Stage 2 tester EA differs between handoff and verification")
    spec_path = Path(handoff.get("stage1_spec") or "")
    source_path = Path(handoff.get("source") or "")
    include_path = Path(handoff.get("canonical_minato") or "")
    for label, path in (("Stage 1 spec", spec_path), ("verified source", source_path), ("canonical Minato include", include_path)):
        if not path.is_file():
            raise RuntimeError(f"missing {label}: {path}")
    if sha256_file(spec_path) != str(handoff.get("stage1_spec_sha256") or "").upper():
        raise RuntimeError("Stage 1 spec hash changed after Stage 2 verification")
    if sha256_file(source_path) != str(verification.get("source_sha256") or "").upper():
        raise RuntimeError("verified Stage 2 source hash changed before diagnostic")
    include_hash = sha256_file(include_path)
    if include_hash != str(handoff.get("canonical_minato_sha256") or "").upper():
        raise RuntimeError("approved Minato include changed after Stage 2 verification")
    spec = read_json(spec_path)
    source_text = source_path.read_text(encoding="utf-8-sig", errors="replace")
    effective = stage2_spec_from_spec(spec, source_text=source_text)
    effective_config = effective.get("tester_config")
    approved = handoff.get("approved_tester_config")
    if not isinstance(approved, dict) or config_sha256(approved) != str(handoff.get("approved_tester_config_sha256") or "").upper():
        raise RuntimeError("handoff approved tester config is not self-authenticating")
    if config_sha256(effective_config) != config_sha256(approved):
        raise RuntimeError("effective Stage 2 tester contract changed since prepare")
    if approved.get("symbol") != "ETHUSD" or "M5" not in approved.get("timeframes", []):
        raise RuntimeError("Stage 2 tester contract must preserve ETHUSD/M5")
    ex5_path = Path(verification.get("ex5") or "")
    compile_record = verification.get("compile") or {}
    ex5_hash = str(verification.get("ex5_sha256") or compile_record.get("ex5_sha256") or "").upper()
    if (not ex5_path.is_file() or not ex5_hash or sha256_file(ex5_path) != ex5_hash
            or str(compile_record.get("ex5_sha256") or "").upper() != ex5_hash):
        raise RuntimeError("Stage 2 tester requires the exact authenticated compiled EX5")
    cycle_slug = re.sub(r"[^A-Za-z0-9_.-]", "_", cycle)
    ea = handoff_ea
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", ea):
        raise RuntimeError("Stage 2 tester EA name is not safe for a cycle-scoped profile path")
    expert_relative = f"Kiyosaki\\Stage2\\{cycle_slug}\\{ea}"
    return {"cycle_id": cycle, "ea": ea,
            "symbol": "ETHUSD", "timeframe": "M5", "from_date": approved["from_date"],
            "to_date": approved["to_date"], "effective_spec": effective,
            "effective_config": approved, "spec_path": str(spec_path.resolve()),
            "spec_sha256": sha256_file(spec_path), "source_path": str(source_path.resolve()),
            "source_sha256": sha256_file(source_path), "include_path": str(include_path.resolve()),
            "include_sha256": include_hash, "ex5_path": str(ex5_path.resolve()),
            "ex5_sha256": ex5_hash, "expert_relative": expert_relative}


def experiment_signature(contract: dict, settings: dict, data: dict) -> dict:
    identity = {"schema_version": 1, "cycle_id": contract["cycle_id"], "ea": contract["ea"],
                "symbol": contract["symbol"], "timeframe": contract["timeframe"],
                "from_date": contract["from_date"], "to_date": contract["to_date"],
                "effective_spec": contract["effective_spec"],
                "stage1_spec_sha256": contract["spec_sha256"],
                "source_path": contract["source_path"],
                "source_sha256": contract["source_sha256"],
                "minato_include_path": contract["include_path"],
                "minato_include_sha256": contract["include_sha256"],
                "verified_ex5_path": contract["ex5_path"],
                "verified_ex5_sha256": contract["ex5_sha256"],
                "expert_relative_path": contract["expert_relative"],
                "tester_settings": settings, "data_identity": data}
    return {"signature": canonical_hash(identity), "identity": identity}


def _ini_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        # MT5's [TesterInputs] accepts string parameters as a single plain
        # assignment. Appending optimization range fields makes the `||`
        # delimiters part of the EA string (the tester log confirms this),
        # even when Optimization=0.
        return value
    return str(value)


def build_ini(contract: dict, report_path: Path, ini_path: Path) -> dict:
    config = contract["effective_config"]
    settings = {"optimization": 0, "model": 0, "visual": 0, "forward_mode": 0,
                "deposit": int(config.get("deposit", 10000)),
                "currency": str(config.get("currency", "USD")),
                "leverage": int(config.get("leverage", 100)),
                "execution_mode": 0, "shutdown_terminal": 1}
    rows = ["[Tester]", f"Expert={contract['expert_relative']}", f"Symbol={contract['symbol']}",
            f"Period={contract['timeframe']}", f"Optimization={settings['optimization']}",
            f"Model={settings['model']}", f"FromDate={contract['from_date']}",
            f"ToDate={contract['to_date']}", f"ForwardMode={settings['forward_mode']}",
            f"Deposit={settings['deposit']}", f"Currency={settings['currency']}",
            f"Leverage={settings['leverage']}", f"ExecutionMode={settings['execution_mode']}",
            "Visual=0", f"Report={report_path.resolve()}", "ReplaceReport=1", "ShutdownTerminal=1",
            "", "[TesterInputs]"]
    # Optimization is disabled; every parameter is a single fixed value.
    for name, item in sorted(config["inputs"].items()):
        value = _ini_value(item["default"])
        if isinstance(item["default"], str):
            rows.append(f"{name}={value}")
        else:
            rows.append(f"{name}={value}||{value}||{value}||{value}||N")
    ini_path.parent.mkdir(parents=True, exist_ok=True)
    ini_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    text = ini_path.read_text(encoding="utf-8")
    if "Optimization=0" not in text or "Period=M5" not in text or "Visual=0" not in text:
        raise RuntimeError("Stage 2 tester INI failed its single-run settings gate")
    if re.search(r"(?im)^\s*Optimization\s*=\s*[1-9]", text):
        raise RuntimeError("Stage 2 tester INI enables optimization")
    return {**settings, "ini_path": str(ini_path.resolve()), "ini_sha256": sha256_file(ini_path),
            "expert_relative_path": contract["expert_relative"],
            "verified_ex5_sha256": contract["ex5_sha256"],
            "effective_config_sha256": config_sha256(config)}


def install_verified_ex5(contract: dict, data_root: Path, *, psutil_module=None,
                         baseline_pids=()) -> dict:
    """Copy only the verified EX5 into a cycle-scoped Experts directory."""
    if psutil_module is None:
        import psutil as psutil_module
    active = sorted(set(_active_tester_pids(psutil_module)) - {int(pid) for pid in baseline_pids})
    if active:
        raise RuntimeError(
            f"terminal/tester workers appeared after Stage 2 preflight; refusing profile install: {active}"
        )
    source = Path(contract["ex5_path"])
    expected = str(contract["ex5_sha256"]).upper()
    if not source.is_file() or sha256_file(source) != expected:
        raise RuntimeError("verified Stage 2 EX5 changed before cycle-profile installation")
    dest = data_root / "MQL5" / "Experts" / Path(*contract["expert_relative"].split("\\")).with_suffix(".ex5")
    dest.parent.mkdir(parents=True, exist_ok=True)
    previous = None
    backup = None
    if dest.is_file():
        previous = sha256_file(dest)
        if previous == expected:
            return {"path": str(dest.resolve()), "sha256": expected,
                    "disposition": "already_authenticated", "previous_sha256": previous,
                    "backup_path": None}
        backup = dest.with_name(dest.name + f".pre-stage2-{previous[:16]}.evidence")
        if backup.exists() and sha256_file(backup) != previous:
            raise RuntimeError("existing Stage 2 profile rollback evidence has a different hash")
        if not backup.exists():
            shutil.copy2(dest, backup)
            if sha256_file(backup) != previous:
                raise RuntimeError("Stage 2 profile rollback copy failed hash verification")
    temporary = dest.with_name(dest.name + ".stage2.tmp")
    shutil.copy2(source, temporary)
    if sha256_file(temporary) != expected:
        temporary.unlink(missing_ok=True)
        raise RuntimeError("cycle-scoped Stage 2 EX5 copy failed hash verification")
    temporary.replace(dest)
    return {"path": str(dest.resolve()), "sha256": expected,
            "disposition": "installed" if previous is None else "replaced_with_rollback",
            "previous_sha256": previous, "backup_path": str(backup.resolve()) if backup else None}


def journal_events(text: str) -> dict:
    folded = text.casefold()
    init_error = ("oninit_incorrect_input_parameters"
                  if "tester stopped because oninit reports incorrect input parameters" in folded
                  else None)
    return {"history_ready": any(token in folded for token in
                                  ("history synchronized", "history data synchronized", "history data loaded",
                                   "history cache updated", "history data begins from",
                                   "history cache allocated", "history data available")),
            "tester_initialized": any(token in folded for token in
                                       ("testing of", "test started", "tester initialized", "testing started")),
            "runtime_observed": any(token in folded for token in
                                     ("test passed in", "test stopped", "testing finished", "ontester result")),
            "completed": "test passed in" in folded and "ontester result" in folded,
            "initialization_error": init_error,
            "zero_trades": bool(re.search(r"\b0\s+trades\b", folded) or re.search(r"trades\s*[:=]\s*0\b", folded))}


def _matching_tst_paths(data_root: Path, contract: dict) -> list[Path]:
    cache = data_root / "Tester" / "cache"
    if not cache.is_dir():
        return []
    dates = (contract["from_date"].replace(".", ""), contract["to_date"].replace(".", ""))
    return [path for path in cache.rglob("*.tst")
            if contract["ea"].casefold() in path.name.casefold()
            and contract["symbol"].casefold() in path.name.casefold()
            and contract["timeframe"].casefold() in path.name.casefold()
            and all(date.casefold() in path.name.casefold() for date in dates)]


def snapshot_tst_artifacts(data_root: Path, contract: dict) -> dict:
    """Capture exact matching cache files immediately before launch."""
    snapshot = {}
    for path in _matching_tst_paths(data_root, contract):
        stat = path.stat()
        snapshot[str(path.resolve())] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns,
                                         "sha256": sha256_file(path)}
    return snapshot


def _locate_fresh_tst(data_root: Path, contract: dict, launch_epoch: float,
                      before: dict) -> tuple[Path, dict] | None:
    for path in _matching_tst_paths(data_root, contract):
        resolved = str(path.resolve())
        stat = path.stat()
        after = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "sha256": sha256_file(path)}
        prior = before.get(resolved)
        if prior is None:
            # A new file is known to have been created since the prelaunch snapshot.
            if stat.st_mtime >= launch_epoch - 1:
                return path, {"disposition": "created_after_launch", "before": None, "after": after}
        elif (prior.get("sha256") != after["sha256"] and stat.st_mtime >= launch_epoch - 1):
            return path, {"disposition": "content_changed_after_launch", "before": prior, "after": after}
    return None


def _owned_terminal(pid: int, expected_exe: Path, launch_epoch: float, process_lookup: Callable):
    proc = process_lookup(pid)
    if proc is None:
        return None
    if (str(Path(proc["exe"]).resolve()).casefold() != str(expected_exe.resolve()).casefold()
            or abs(float(proc["created"]) - launch_epoch) > 300
            or str(proc["name"]).casefold() != "terminal64.exe"):
        raise RuntimeError("Stage 2 terminal PID failed executable/creation-time ownership checks")
    return proc


def run_diagnostic(handoff: dict, verification: dict, *, launch: Callable | None = None,
                   process_lookup: Callable | None = None, sleep: Callable = time.sleep,
                   clock: Callable = time.monotonic, wall_clock: Callable = time.time,
                   data_root: Path | None = None, terminal_exe: Path | None = None,
                   existing_terminals: Callable[[], list[int]] | None = None,
                   process_census: Callable[[int], dict] | None = None,
                   install_binary: Callable | None = None,
                   close_terminal: Callable | None = None,
                   max_startup: int = MAX_STARTUP_SECONDS, max_runtime: int = MAX_RUNTIME_SECONDS) -> dict:
    """Run or reuse exactly one Stage 2 tester pass. Injectable seams support safe mocked tests."""
    root = ROOT
    data_root = (data_root or DATA_ROOT).resolve()
    terminal_exe = (terminal_exe or MT5_EXE).resolve()
    contract = effective_contract(handoff, verification)
    output = root / "state" / "stage2_tester_v1"
    output.mkdir(parents=True, exist_ok=True)
    cycle_slug = re.sub(r"[^A-Za-z0-9_.-]", "_", contract["cycle_id"])
    data = history_identity(data_root, contract["symbol"])
    if not data["history_files"]:
        raise RuntimeError("Stage 2 tester has no local ETHUSD history files; history readiness cannot be authenticated")
    report_path = output / f"{cycle_slug}.stage2.{contract['timeframe']}.report.html"
    ini_path = output / f"{cycle_slug}.stage2.{contract['timeframe']}.ini"
    settings = build_ini(contract, report_path, ini_path)
    signed = experiment_signature(contract, settings, data)
    sig = signed["signature"]
    lease_path = output / f"{cycle_slug}.{sig}.lease.json"
    result_path = output / f"{cycle_slug}.{sig}.result.json"
    if result_path.is_file():
        prior = read_json(result_path)
        if prior.get("signature") != sig or prior.get("status") != "COMPLETE":
            raise RuntimeError("matching Stage 2 tester identity already exists without a reusable completed result")
        return prior
    if lease_path.exists():
        raise RuntimeError("matching Stage 2 tester experiment has a prior lease; explicit recovery required")
    if launch is None:
        expected_terminal = (CANARY_PORTABLE_ROOT / "terminal64.exe").resolve()
        if is_canary() and os.environ.get("KCOS_CANARY_ALLOW_MT5") != "1":
            raise RuntimeError("canary MT5 launch requires KCOS_CANARY_ALLOW_MT5=1")
        if terminal_exe != expected_terminal or data_root != CANARY_PORTABLE_ROOT.resolve():
            raise RuntimeError("Stage 2 launch requires canonical project portable terminal and matching data root")
    if not terminal_exe.is_file():
        raise RuntimeError(f"MT5 executable missing: {terminal_exe}")
    try:
        import psutil
    except ImportError as exc:
        raise RuntimeError("psutil is required for Stage 2 tester process ownership") from exc
    lookup = process_lookup or (lambda pid: _psutil_process(pid, psutil))
    census = process_census
    existing = existing_terminals() if existing_terminals is not None else _active_tester_pids(psutil)
    baseline_pids = sorted(set(int(value) for value in existing))
    census = census or (lambda pid: verify_live_tester_process_census(
        pid, baseline_pids=baseline_pids, psutil_module=psutil))
    lease = {"schema_version": 1, "signature": sig, "status": "STARTING",
             "cycle_id": contract["cycle_id"], "created_utc": datetime.now(timezone.utc).isoformat()}
    try:
        with lease_path.open("x", encoding="utf-8") as fh:
            json.dump(lease, fh, sort_keys=True, indent=2)
    except FileExistsError as exc:
        raise RuntimeError("Stage 2 tester identity was concurrently claimed") from exc
    installer = install_binary or (lambda value, base: install_verified_ex5(
        value, base, psutil_module=psutil, baseline_pids=baseline_pids))
    profile_install = installer(contract, data_root)
    if (Path(profile_install.get("path") or "").resolve() !=
            (data_root / "MQL5" / "Experts" / Path(*contract["expert_relative"].split("\\")).with_suffix(".ex5")).resolve()
            or str(profile_install.get("sha256") or "").upper() != contract["ex5_sha256"]):
        raise RuntimeError("Stage 2 cycle-profile install evidence does not bind the verified EX5")
    launch_fn = launch or _launch
    journal = data_root / "Tester" / "logs" / (datetime.now().strftime("%Y%m%d") + ".log")
    baseline = journal.stat().st_size if journal.is_file() else 0
    cache_baseline = snapshot_tst_artifacts(data_root, contract)
    launch_epoch = wall_clock()
    started = clock()
    pid = int(launch_fn(terminal_exe, ini_path))
    runtime = {**signed, "status": "RUNNING", "terminal_pid": pid,
               "terminal_exe": str(terminal_exe), "launch_epoch": launch_epoch,
               "journal_path": str(journal.resolve()), "journal_start_offset": baseline,
               "ini_path": str(ini_path.resolve()), "ini_sha256": settings["ini_sha256"],
               "profile_install": profile_install, "cache_baseline": cache_baseline,
               "worker_baseline_pids": baseline_pids}
    closer = close_terminal or (lambda: close_owned_terminal(
        pid, terminal_exe, launch_epoch, ini_path, baseline_pids=baseline_pids, psutil_module=psutil))
    runtime_path = output / f"{cycle_slug}.{sig}.runtime.json"
    try:
        atomic_write(runtime_path, runtime)
        startup_deadline = started + max_startup
        overall_deadline = started + max_runtime
        history_ready = False
        events = {}
        artifact = None
        artifact_freshness = None
        while clock() < overall_deadline:
            owned = lookup(pid)
            if owned is not None:
                _owned_terminal(pid, terminal_exe, launch_epoch, lookup)
            live_census = census(pid)
            text = _journal_delta(journal, baseline)
            events = journal_events(text)
            if events["initialization_error"] == "oninit_incorrect_input_parameters":
                raise RuntimeError("Stage 2 tester journal reports OnInit incorrect input parameters")
            if events["history_ready"]:
                history_ready = True
            if not (history_ready and events["tester_initialized"]) and clock() > startup_deadline:
                raise RuntimeError(f"Stage 2 tester history/init did not become ready within {max_startup} seconds")
            if history_ready and events["tester_initialized"] and events["runtime_observed"] and events["completed"]:
                fresh = _locate_fresh_tst(data_root, contract, launch_epoch, cache_baseline)
                if fresh is not None:
                    artifact, artifact_freshness = fresh
                    break
            if owned is None and clock() >= started + 2:
                raise RuntimeError("owned Stage 2 terminal exited before fresh tester completion evidence")
            sleep(POLL_SECONDS)
        if artifact is None:
            raise RuntimeError(f"Stage 2 tester did not complete with a fresh owned .tst artifact within {max_runtime} seconds")
        artifact_info = {"path": str(artifact.resolve()), "size": artifact.stat().st_size,
                         "sha256": sha256_file(artifact), "mtime": artifact.stat().st_mtime,
                         "freshness": artifact_freshness}
        shutdown = closer()
        result = {"schema_version": 1, "cycle_id": contract["cycle_id"], "stage": 2,
                  "status": "COMPLETE", "signature": sig, "identity": signed["identity"],
                  "settings": settings, "data_identity": data, "terminal_pid": pid,
                  "profile_install": profile_install, "shutdown": shutdown,
                  "terminal_exe": str(terminal_exe), "launch_epoch": launch_epoch,
                  "startup_seconds": round(clock() - started, 3), "history_ready": history_ready,
                  "journal_path": str(journal.resolve()), "journal_start_offset": baseline,
                  "journal_sha256": sha256_bytes(_journal_delta_bytes(journal, baseline)),
                  "events": events, "trades_interpretation": "informational_zero_trades" if events.get("zero_trades") else "see_tester_report",
                  "artifact": artifact_info, "report_path": str(report_path.resolve()),
                  "process_census": live_census,
                  "ini_path": str(ini_path.resolve()), "completed_utc": datetime.now(timezone.utc).isoformat()}
        atomic_write(result_path, result)
        lease.update(status="COMPLETE", result_path=str(result_path.resolve()), artifact=artifact_info,
                     shutdown=shutdown)
        atomic_write(lease_path, lease)
        return result
    except Exception as exc:
        try:
            shutdown = closer()
            shutdown_error = None
        except Exception as close_exc:
            shutdown = None
            shutdown_error = str(close_exc)
        failure = {"schema_version": 1, "cycle_id": contract["cycle_id"], "stage": 2,
                   "status": "FAILED", "signature": sig, "error": str(exc),
                   "terminal_pid": pid, "terminal_exe": str(terminal_exe),
                   "launch_epoch": launch_epoch, "ini_path": str(ini_path.resolve()),
                   "profile_install": profile_install, "shutdown": shutdown,
                   "shutdown_error": shutdown_error, "journal_path": str(journal.resolve()),
                   "journal_start_offset": baseline,
                   "journal_sha256": sha256_bytes(_journal_delta_bytes(journal, baseline)),
                   "failed_utc": datetime.now(timezone.utc).isoformat()}
        failure_path = output / f"{cycle_slug}.{sig}.failure.json"
        atomic_write(failure_path, failure)
        status = "FAILED" if shutdown_error is None else "SHUTDOWN_FAILED"
        lease.update(status=status, failure_path=str(failure_path.resolve()),
                     shutdown=shutdown, shutdown_error=shutdown_error)
        atomic_write(lease_path, lease)
        runtime.update(status=status, failure_path=str(failure_path.resolve()),
                       shutdown=shutdown, shutdown_error=shutdown_error)
        atomic_write(runtime_path, runtime)
        if shutdown_error is not None:
            raise RuntimeError(f"{exc}; owned terminal cleanup also failed: {shutdown_error}") from exc
        raise


def _psutil_process(pid: int, psutil_module):
    try:
        proc = psutil_module.Process(pid)
        return {"name": proc.name(), "exe": proc.exe(), "created": proc.create_time()}
    except psutil_module.NoSuchProcess:
        return None


def close_owned_terminal(pid: int, expected_exe: Path, launch_epoch: float, ini_path: Path,
                         *, baseline_pids=(), psutil_module=None) -> dict:
    """Stop only the launch we authenticated and its MetaTester children."""
    if psutil_module is None:
        import psutil as psutil_module
    try:
        terminal = psutil_module.Process(int(pid))
    except psutil_module.NoSuchProcess:
        remaining = sorted(set(_active_tester_pids(psutil_module)) - set(baseline_pids))
        if remaining:
            raise RuntimeError(
                "Stage 2 terminal exited but terminal/tester processes remain without verifiable parent ownership: "
                + ",".join(str(value) for value in remaining)
            )
        return {"terminal_pid": int(pid), "terminal_status": "exited_after_completion",
                "worker_pids": [], "post_exit_process_census": "clear"}
    if (terminal.name().casefold() != "terminal64.exe"
            or Path(terminal.exe()).resolve() != expected_exe.resolve()
            or abs(terminal.create_time() - launch_epoch) > 300
            or str(ini_path.resolve()).casefold() not in " ".join(terminal.cmdline()).casefold()):
        raise RuntimeError("Stage 2 shutdown refused because terminal identity/config ownership changed")
    workers = []
    for child in terminal.children(recursive=True):
        if child.name().casefold() != "metatester64.exe":
            continue
        workers.append({"pid": child.pid, "name": child.name(), "exe": child.exe(),
                        "created": child.create_time(), "parent_pid": child.ppid()})
        child.terminate()
    terminal.terminate()
    try:
        terminal.wait(timeout=10)
    except psutil_module.TimeoutExpired as exc:
        raise RuntimeError("owned Stage 2 terminal did not exit after graceful shutdown") from exc
    for item in workers:
        try:
            psutil_module.Process(item["pid"]).wait(timeout=5)
        except psutil_module.NoSuchProcess:
            pass
        except psutil_module.TimeoutExpired as exc:
            raise RuntimeError(f"owned Stage 2 MetaTester worker {item['pid']} did not exit") from exc
    remaining = sorted(set(_active_tester_pids(psutil_module)) - set(baseline_pids))
    if remaining:
        raise RuntimeError(
            "owned terminal closed but terminal/tester processes remain after child waits without verifiable ownership: "
            + ",".join(str(value) for value in remaining)
        )
    return {"terminal_pid": int(pid), "terminal_status": "gracefully_exited",
            "worker_pids": [item["pid"] for item in workers], "workers": workers,
            "post_exit_process_census": "clear"}


def _active_tester_pids(psutil_module) -> list[int]:
    found = []
    try:
        processes = psutil_module.process_iter(["pid", "name"])
        for proc in processes:
            try:
                info = proc.info
                name = info.get("name")
                if not name:
                    name = proc.name()
                if str(name).casefold() in {"terminal64.exe", "metatester64.exe"}:
                    found.append(int(info["pid"]))
            except psutil_module.NoSuchProcess:
                continue
            except psutil_module.AccessDenied as exc:
                raise RuntimeError("process enumeration is incomplete; refusing Stage 2 tester launch") from exc
    except psutil_module.AccessDenied as exc:
        raise RuntimeError("process enumeration is incomplete; refusing Stage 2 tester launch") from exc
    return found


def verify_live_tester_process_census(terminal_pid: int, *, baseline_pids=(), psutil_module=None) -> dict:
    """Reject terminal or tester workers that cannot be tied to our terminal."""
    if psutil_module is None:
        import psutil as psutil_module
    census = set(_active_tester_pids(psutil_module))
    try:
        terminal = psutil_module.Process(int(terminal_pid))
    except psutil_module.NoSuchProcess:
        unknown = sorted(census - set(int(pid) for pid in baseline_pids))
        if unknown:
            raise RuntimeError("unowned terminal/tester process remained after Stage 2 terminal exit: "
                               + ",".join(str(value) for value in unknown))
        return {"observed_pids": sorted(census), "owned_pids": [],
                "status": "owned_terminal_exited", "baseline_pids": sorted(set(baseline_pids))}
    if int(terminal_pid) not in census or terminal.name().casefold() != "terminal64.exe":
        raise RuntimeError("owned Stage 2 terminal is missing from the live process census")
    owned = {int(terminal_pid)}
    for child in terminal.children(recursive=True):
        if child.name().casefold() == "metatester64.exe":
            owned.add(int(child.pid))
    unknown = sorted(census - owned - set(baseline_pids))
    if unknown:
        raise RuntimeError("unowned terminal/tester process appeared during Stage 2 run: "
                           + ",".join(str(value) for value in unknown))
    return {"observed_pids": sorted(census), "owned_pids": sorted(owned), "status": "clear"}


def _launch(executable: Path, ini: Path) -> int:
    flags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    command = [str(executable)]
    options = {}
    command.append("/portable")
    options["cwd"] = str(executable.resolve().parent)
    command.append(f"/config:{ini.resolve()}")
    return subprocess.Popen(command, stdin=subprocess.DEVNULL,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            creationflags=flags, **options).pid


def _journal_delta_bytes(path: Path, offset: int) -> bytes:
    return path.read_bytes()[offset:] if path.is_file() else b""


def _journal_delta(path: Path, offset: int) -> str:
    raw = _journal_delta_bytes(path, offset)
    # MT5 tester journals are commonly UTF-16LE; accept UTF-8 test fixtures too.
    return raw.decode("utf-16le", errors="ignore") if b"\x00" in raw[:64] else raw.decode("utf-8", errors="ignore")


def atomic_write(path: Path, value: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)
