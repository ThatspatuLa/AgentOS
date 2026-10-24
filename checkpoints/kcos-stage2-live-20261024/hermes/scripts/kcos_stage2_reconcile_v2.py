#!/usr/bin/env python3
"""Reconcile Stage 1 optimization ranges with the EA's declared source domain.

Stage 1 owns the intent; Stage 2 owns the executable contract.  When the
approved window overlaps the source domain but extends beyond it, narrow the
window to the legal intersection, preserve the source step, and update the
authenticated Stage 1 artifact event.  No range is widened and no input is
invented.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from kcos_runtime_paths_v2 import assert_safe_root

ROOT = assert_safe_root()
STATE = ROOT / "state" / "kcos_cycle_state_v2.json"
STAGE1 = ROOT / "state" / "handoffs" / "Stage1_v2.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2)
            handle.write("\n")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def reconcile() -> dict:
    handoff = json.loads(STAGE1.read_text(encoding="utf-8-sig"))
    spec_path = Path(handoff["expected_spec_path"])
    event_path = Path(handoff["expected_event_path"])
    candidate_source = handoff.get("candidate_source")
    if not candidate_source:
        raise ValueError("STAGE2_BUILD_REQUIRED: Stage 1 handoff has no candidate_source")
    source_path = Path(candidate_source)
    spec = json.loads(spec_path.read_text(encoding="utf-8-sig"))
    source = source_path.read_text(encoding="utf-8-sig", errors="replace")
    decl = re.compile(
        r"(?m)^\s*input\s+[A-Za-z_][A-Za-z0-9_<>]*\s+"
        r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=[^;]*;(?P<comment>[^\r\n]*)"
    )
    domain = re.compile(
        r"\[\s*lo\s*=\s*([-+0-9.eE]+)\s*,\s*hi\s*=\s*([-+0-9.eE]+)\s*,\s*step\s*=\s*([-+0-9.eE]+)\s*\]"
        r"|\[\s*([-+0-9.eE]+)\s*\.\.\s*([-+0-9.eE]+)\s*\]\s*step\s*([-+0-9.eE]+)"
    )
    domains = {}
    for match in decl.finditer(source):
        item = domain.search(match.group("comment"))
        if item:
            values = item.groups()
            values = values[:3] if values[0] is not None else values[3:]
            domains[match.group("name")] = tuple(map(float, values))
    changes = []
    for name, item in spec.get("tester_config", {}).get("inputs", {}).items():
        if str(item.get("optimize", "N")).upper() != "Y" or name not in domains:
            continue
        dlo, dhi, dstep = domains[name]
        lo, hi = sorted((dlo, dhi))
        old_start, old_stop = float(item["start"]), float(item["stop"])
        new_start, new_stop = max(lo, old_start), min(hi, old_stop)
        if new_start > new_stop:
            default = min(hi, max(lo, float(item.get("default", lo))))
            new_start = new_stop = default
        # Source step is authoritative; it is also required by the tester.
        if new_start == new_stop:
            # A one-point legal intersection is a fixed input. Preserve its
            # scalar default and never write a step into the stop field.
            item["optimize"] = "N"
            item["default"] = new_start
            item["start"], item["stop"], item["step"] = new_start, new_start, abs(dstep)
        else:
            item["start"], item["stop"], item["step"] = new_start, new_stop, abs(dstep)
        if (old_start, old_stop, float(item["step"])) != (float(item["start"]), float(item["stop"]), float(item["step"])):
            item["domain_provenance"] = {
                "method": "stage2_source_reconciliation",
                "basis": "intersection_with_source_declared_domain",
                "source_domain": [dlo, dhi, abs(dstep)],
                "previous_range": [old_start, old_stop],
                "reconciled_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            changes.append({"input": name, "from": [old_start, old_stop], "to": [item["start"], item["stop"]], "source_domain": [dlo, dhi]})
    if changes:
        backup = spec_path.with_name(spec_path.name + ".bak.stage2-reconcile")
        shutil.copy2(spec_path, backup)
        atomic(spec_path, spec)
        event = json.loads(event_path.read_text(encoding="utf-8-sig"))
        event["artifact_sha256"] = sha256(spec_path)
        event["stage2_reconciled"] = changes
        atomic(event_path, event)
        report = ROOT / "state" / "reports" / f"{handoff['cycle_id']}.stage2.reconciliation.json"
        atomic(report, {"schema_version": 1, "cycle_id": handoff["cycle_id"], "ea": handoff.get("candidate_ea"), "changes": changes, "backup": str(backup.resolve())})
    return {"changed": bool(changes), "changes": changes, "spec": str(spec_path.resolve())}


if __name__ == "__main__":
    print(json.dumps(reconcile(), separators=(",", ":")))
