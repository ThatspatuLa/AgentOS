#!/usr/bin/env python3
"""Run one auditable MetaEditor compile attempt for KCOS Stage 2."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


def load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain an object")
    return value


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def parse_compile_log(text: str) -> tuple[int, int]:
    match = re.search(r"(\d+)\s+errors?\s*,\s*(\d+)\s+warnings?", text, re.I)
    if not match:
        raise ValueError("compile log has no errors/warnings summary")
    return int(match.group(1)), int(match.group(2))


def wait_for_compile_log(path: Path, timeout: float = 8.0) -> str:
    deadline = time.monotonic() + timeout
    last_size = -1
    stable_reads = 0
    while time.monotonic() < deadline:
        if path.is_file():
            size = path.stat().st_size
            if size > 0 and size == last_size:
                text = path.read_text(encoding="utf-16", errors="replace")
                try:
                    parse_compile_log(text)
                    stable_reads += 1
                    if stable_reads >= 2:
                        return text
                except ValueError:
                    stable_reads = 0
            else:
                stable_reads = 0
            last_size = size
        time.sleep(0.2)
    return path.read_text(encoding="utf-16", errors="replace") if path.is_file() else ""


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def compile_once(handoff_path: Path, output_path: Path, timeout: int = 180) -> dict:
    output_path = output_path.resolve()
    handoff = load(handoff_path)
    source = Path(handoff["source"])
    current_source_sha = sha256(source)
    attempts_path = Path(handoff["compile_attempts_file"])
    prior_all = load(attempts_path).get("attempts", []) if attempts_path.is_file() else []
    # Compile attempts are bounded per immutable source revision. Preserve
    # historical attempts, but do not exhaust a repaired source's budget.
    prior = [item for item in prior_all if str(item.get("mq5_sha256", "")).upper() == current_source_sha]
    attempt = len(prior) + 1
    maximum = int(handoff["max_compile_attempts"])
    if attempt > maximum:
        raise ValueError(f"compile attempt limit exhausted ({maximum}) for source {current_source_sha}")
    ex5 = Path(handoff["ex5"])
    metaeditor = Path(handoff["metaeditor"])
    started = datetime.now(timezone.utc)
    log_path = output_path.with_suffix(f".attempt{attempt}.log")
    command = [str(metaeditor), f"/compile:{source}", f"/log:{log_path}"]
    timed_out = False
    try:
        process = subprocess.run(
            command,
            cwd=str(source.parent),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        exit_code = process.returncode
        stdout = process.stdout
        stderr = process.stderr
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        exit_code = None
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
    finished = datetime.now(timezone.utc)
    log_text = wait_for_compile_log(log_path)
    # MetaEditor may ignore /log and emit the durable compiler log beside the
    # source. Prefer the requested path, then use the source-local log only
    # when the requested path contains no parseable compiler summary.
    if not re.search(r"\d+\s+errors?\s*,\s*\d+\s+warnings?", log_text, re.I):
        source_log = source.with_suffix(".log")
        if source_log.is_file():
            fallback_text = wait_for_compile_log(source_log)
            if re.search(r"\d+\s+errors?\s*,\s*\d+\s+warnings?", fallback_text, re.I):
                log_text = fallback_text
                log_path = source_log
    try:
        errors, warnings = parse_compile_log(log_text)
    except ValueError:
        errors, warnings = 1, 0
    fresh_ex5 = ex5.is_file() and ex5.stat().st_mtime >= started.timestamp()
    # MetaEditor on this installation returns wrapper code 1 despite a
    # successful compile. Preserve the raw process code, but normalize the
    # contract exit code only when durable compiler evidence is clean.
    raw_exit_code = exit_code
    normalized_exit_code = 0 if (exit_code == 1 and not timed_out and errors == 0 and warnings == 0 and fresh_ex5) else exit_code
    result = {
        "schema_version": 2,
        "cycle_id": handoff["cycle_id"],
        "stage": 2,
        "ea": handoff["ea"],
        "attempt": attempt,
        "max_attempts": maximum,
        "started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_utc": finished.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "command": command,
        "working_directory": str(source.parent),
        "exit_code": normalized_exit_code,
        "raw_exit_code": raw_exit_code,
        "timed_out": timed_out,
        "errors": errors,
        "warnings": warnings,
        "log": str(log_path.resolve()),
        "stdout": stdout[-2000:],
        "stderr": stderr[-2000:],
        "mq5_sha256": sha256(source),
        "fresh_ex5": fresh_ex5,
        "ex5_sha256": sha256(ex5) if ex5.is_file() else None,
        "passed": normalized_exit_code == 0 and not timed_out and errors == 0 and warnings == 0 and fresh_ex5,
    }
    prior.append(result)
    atomic_json(attempts_path, {"schema_version": 2, "cycle_id": handoff["cycle_id"], "attempts": prior})
    atomic_json(output_path, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handoff", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    result = compile_once(args.handoff, args.output, args.timeout)
    print(json.dumps({"passed": result["passed"], "attempt": result["attempt"], "errors": result["errors"], "warnings": result["warnings"]}))
    return 0 if result["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
