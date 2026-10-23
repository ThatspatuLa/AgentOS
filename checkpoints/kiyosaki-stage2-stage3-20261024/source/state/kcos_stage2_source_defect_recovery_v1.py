"""Hash-bound, same-cycle Stage 3 source-defect recovery primitives.

This module only prepares a one-shot Stage 2 recovery ticket after validating
the current cycle evidence. It does not dispatch a job or execute a stage.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path


# A failed Stage 2 dispatch may be caused by pre-proposal recovery plumbing.
# Keep two bounded dispatch slots for those authenticated failures plus the
# existing three proposal slots; the repair episode ledger remains the
# authority limiting actual model proposals to three.
MAX_DISPATCH_ATTEMPTS = 8


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def consumption_insensitive_sha256(path: Path) -> str:
    """Hash immutable event content while excluding the supervisor's consumption stamp."""
    event = load(path)
    for key in ("consumed", "consumed_utc", "consumed_action"):
        event.pop(key, None)
    payload = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest().upper()


def load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def _bound_file(name: str, value: str | Path) -> dict:
    path = Path(value).resolve()
    if not path.is_file():
        raise ValueError(f"recovery evidence missing: {name}={path}")
    return {"path": str(path), "sha256": sha256(path), "size": path.stat().st_size}


def _check_bound_file(item: dict) -> bool:
    try:
        path = Path(item["path"])
        return path.is_file() and sha256(path) == item.get("sha256")
    except (OSError, KeyError, TypeError):
        return False


def classify_runtime_502_repair(spec_primitive: str, source_text: str, dcc_gate, *,
                                journal_lines: list[str] | None = None,
                                ea_name: str | None = None) -> str:
    """Classify an authenticated array-bounds crash at its reported source line."""
    if journal_lines is not None:
        source_lines = source_text.splitlines()
        exact_source_lines = []
        expected_filename = f"{ea_name}.mq5".casefold() if ea_name else None
        for entry in journal_lines:
            location = re.search(
                r"\bfile\s+([^,\r\n]*?\.mq5),\s*line\s+(\d+),\s*col\s+\d+",
                str(entry), re.I)
            if not location:
                continue
            filename = Path(location.group(1).strip().strip('"')).name.casefold()
            if expected_filename and filename != expected_filename:
                continue
            line_number = int(location.group(2))
            if 1 <= line_number <= len(source_lines):
                exact_source_lines.append(source_lines[line_number - 1])
        if not exact_source_lines:
            raise ValueError("runtime 502 journal lines do not identify a line in the authenticated candidate source")

        if "dcc" in str(spec_primitive).casefold() and not dcc_gate(source_text, required_dcc=True):
            return "stage3_tester_journal_dcc_closed_return_bounds"
        from kcos_stage2_verify_v2 import multidimensional_array_row_bounds_are_safe
        if (not multidimensional_array_row_bounds_are_safe(source_text)
                and any(re.search(r"\b[A-Za-z_]\w*\s*\[[^\]]+\]\s*\[[^\]]+\]", line)
                        for line in exact_source_lines)):
            return "stage3_tester_journal_multidimensional_array_row_bounds"
        quantile_clamp = re.search(
            r"if\s*\(\s*qi0\s*>=\s*filled\s*-\s*1\s*\)\s*\{\s*"
            r"qi0\s*=\s*filled\s*-\s*1\s*;\s*fracq\s*=\s*0(?:\.0)?\s*;\s*\}",
            source_text, re.I,
        )
        unsafe_interpolation = re.search(
            r"\btemp\s*\[\s*qi0\s*\]\s*\+\s*fracq\s*\*\s*\(\s*"
            r"temp\s*\[\s*qi0\s*\+\s*1\s*\]\s*-\s*temp\s*\[\s*qi0\s*\]\s*\)",
            source_text, re.I,
        )
        if (quantile_clamp and unsafe_interpolation
                and any(re.search(r"\btemp\s*\[\s*qi0\s*\+\s*1\s*\]", line, re.I)
                        for line in exact_source_lines)):
            return "stage3_tester_journal_quantile_interpolation_bounds"
        return "stage3_tester_journal_ea_runtime_array_bounds"

    # Keep the original evidence-free unit/API behavior for existing callers;
    # the live recovery path below always supplies the journal source location.
    if "dcc" in str(spec_primitive).casefold():
        if not dcc_gate(source_text, required_dcc=True):
            return "stage3_tester_journal_dcc_closed_return_bounds"
        raise ValueError("runtime 502 has no verifier-confirmed DCC closed-return bounds defect")
    from kcos_stage2_verify_v2 import multidimensional_array_row_bounds_are_safe
    if not multidimensional_array_row_bounds_are_safe(source_text):
        return "stage3_tester_journal_multidimensional_array_row_bounds"
    # A one-element warm-up window leaves qi0 on the last valid item while
    # interpolation still reads qi0 + 1. Admit only this proven source shape.
    quantile_clamp = re.search(
        r"if\s*\(\s*qi0\s*>=\s*filled\s*-\s*1\s*\)\s*\{\s*"
        r"qi0\s*=\s*filled\s*-\s*1\s*;\s*fracq\s*=\s*0(?:\.0)?\s*;\s*\}",
        source_text, re.I,
    )
    unsafe_interpolation = re.search(
        r"\btemp\s*\[\s*qi0\s*\]\s*\+\s*fracq\s*\*\s*\(\s*"
        r"temp\s*\[\s*qi0\s*\+\s*1\s*\]\s*-\s*temp\s*\[\s*qi0\s*\]\s*\)",
        source_text, re.I,
    )
    if quantile_clamp and unsafe_interpolation:
        return "stage3_tester_journal_quantile_interpolation_bounds"
    raise ValueError("runtime 502 has no verifier-confirmed multidimensional row-bounds defect")


def valid_dispatch_attempt_identity(ticket: dict, event: dict, state: dict,
                                    event_path: Path) -> bool:
    attempt = event.get("dispatch_attempt", 1)
    event_id = event.get("event_id")
    expected_parent = (ticket.get("ticket_id") if type(attempt) is int and attempt == 2
                       else f"{ticket.get('ticket_id')}:dispatch:{attempt - 1}" if type(attempt) is int else None)
    first_attempt = (type(attempt) is int and attempt == 1
                     and event_id == ticket.get("ticket_id"))
    retry_attempt = (type(attempt) is int and 2 <= attempt <= MAX_DISPATCH_ATTEMPTS
                     and event_id == f"{ticket.get('ticket_id')}:dispatch:{attempt}"
                     and event.get("parent_event_id") == expected_parent)
    return bool((first_attempt or retry_attempt)
                and Path(state.get("stage2_source_defect_event") or "").resolve() == event_path.resolve()
                and state.get("stage2_source_defect_event_id") == event_id)


def _find_parent_attempt_archive(ticket_path: Path, ticket: dict, cycle_id: str,
                                 expected_parent: str) -> dict | None:
    """Find and fully validate the immutable archive named by a retry's parent ID."""
    try:
        attempt = int(expected_parent.rsplit(":dispatch:", 1)[1])
    except (IndexError, ValueError):
        return None
    nonce = str(ticket.get("ticket_id") or "").rsplit(":", 1)[-1]
    archive_root = (ticket_path.parent.parent / "diagnostics" / "stage2_source_defect_attempts"
                    / cycle_id / nonce)
    if not archive_root.is_dir():
        return None
    for folder in archive_root.glob(f"attempt-{attempt:02d}-*"):
        manifest_path = folder / "manifest.json"
        if not manifest_path.is_file():
            continue
        manifest = load(manifest_path)
        if manifest.get("event_id") != expected_parent:
            continue
        receipt = {"path": str(manifest_path.resolve()), "sha256": sha256(manifest_path),
                   "event_id": expected_parent, "execution_id": manifest.get("execution_id")}
        try:
            validate_attempt_archive(receipt, ticket_id=str(ticket["ticket_id"]),
                                     cycle_id=cycle_id, event_id=expected_parent,
                                     execution_id=manifest.get("execution_id"))
        except (OSError, ValueError, KeyError, TypeError):
            continue
        return receipt
    return None


def _assert_archive_path_safe(path: Path, archive_root: Path) -> None:
    """Reject path redirection and escapes before creating supplement content."""
    root = archive_root.resolve(strict=True)
    candidate = path.absolute()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("supplement path escapes the immutable base bundle") from exc
    current = root
    parts = candidate.relative_to(root).parts
    for component in parts:
        current = current / component
        if current.exists() or current.is_symlink():
            metadata = current.lstat()
            if current.is_symlink() or getattr(metadata, "st_file_attributes", 0) & 0x400:
                raise ValueError("supplement path contains a symlink or reparse point")
            try:
                current.resolve(strict=True).relative_to(root)
            except (OSError, ValueError) as exc:
                raise ValueError("supplement path resolves outside the immutable base bundle") from exc


def _assert_no_reparse_components(path: Path) -> None:
    """Check the lexical path itself before resolving any archive trust root."""
    candidate = Path(os.path.abspath(path))
    current = Path(candidate.anchor)
    for component in candidate.parts[1:]:
        current = current / component
        if current.exists() or current.is_symlink():
            metadata = current.lstat()
            if current.is_symlink() or getattr(metadata, "st_file_attributes", 0) & 0x400:
                raise ValueError("archive path contains a symlink or reparse point")


def validate_archive_receipt(ticket: dict, ticket_path: Path, receipt: dict) -> dict:
    """Verify the immutable base bundle and any append-only missing-file supplements."""
    manifest_path = Path(receipt.get("path") or "")
    archive_dir = Path(receipt.get("archive_dir") or "")
    if (not manifest_path.is_file() or not archive_dir.is_dir()
            or manifest_path.parent.resolve() != archive_dir.resolve()
            or sha256(ticket_path) != str(receipt.get("ticket_sha256") or "").upper()
            or receipt.get("ticket_id") != ticket.get("ticket_id")
            or sha256(manifest_path) != str(receipt.get("sha256") or "").upper()):
        raise ValueError("original recovery archive receipt is missing or hash-mismatched")
    manifest = load(manifest_path)
    if manifest.get("ticket_id") != ticket.get("ticket_id") or manifest.get("cycle_id") != ticket.get("cycle_id"):
        raise ValueError("original recovery archive manifest has the wrong ticket or cycle")
    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        raise ValueError("original recovery archive manifest has no file inventory")
    combined = dict(files)
    for supplement in receipt.get("supplements", []):
        supplement_path = Path(supplement.get("path") or "")
        if (not supplement_path.is_file()
                or sha256(supplement_path) != str(supplement.get("sha256") or "").upper()):
            raise ValueError("supplemental recovery archive receipt is missing or changed")
        supplement_dir = supplement_path.parent
        try:
            supplement_dir.resolve().relative_to(archive_dir.resolve())
        except ValueError as exc:
            raise ValueError("supplemental archive escapes the immutable base bundle") from exc
        extra = load(supplement_path)
        if (extra.get("schema_version") != 1 or extra.get("ticket_id") != ticket.get("ticket_id")
                or extra.get("cycle_id") != ticket.get("cycle_id")
                or extra.get("ticket_sha256") != sha256(ticket_path)
                or extra.get("base_manifest_sha256") != sha256(manifest_path)
                or supplement.get("ticket_id") != ticket.get("ticket_id")):
            raise ValueError("supplemental archive identity does not bind ticket and base manifest")
        extra_files = extra.get("files")
        if not isinstance(extra_files, dict) or not extra_files:
            raise ValueError("supplemental archive has no file inventory")
        for name, item in extra_files.items():
            if name in combined:
                raise ValueError(f"supplemental archive duplicates existing evidence: {name}")
            combined[name] = item
    for name, bound in ticket.get("artifacts", {}).items():
        archived = combined.get(name)
        if not isinstance(archived, dict):
            raise ValueError(f"original recovery archive omits ticket evidence: {name}")
        path = Path(archived.get("path") or "")
        try:
            path.resolve().relative_to(archive_dir.resolve())
        except ValueError as exc:
            raise ValueError(f"archived evidence escapes immutable bundle: {name}") from exc
        if (not path.is_file() or path.stat().st_size != bound.get("size")
                or archived.get("size") != bound.get("size")
                or archived.get("sha256") != bound.get("sha256")
                or archived.get("original_path") != str(Path(bound.get("path") or "").resolve())
                or sha256(path) != str(bound.get("sha256") or "").upper()):
            raise ValueError(f"original archived evidence failed byte/hash validation: {name}")
    return manifest


def extend_archive_for_missing_ticket_artifacts(ticket: dict, ticket_path: Path,
                                                receipt: dict) -> dict:
    """Append a separate immutable bundle for ticket artifacts absent in a legacy base manifest."""
    manifest_path = Path(receipt.get("path") or "")
    archive_dir = Path(receipt.get("archive_dir") or "")
    _assert_no_reparse_components(archive_dir)
    _assert_no_reparse_components(manifest_path)
    if (not manifest_path.is_file() or not archive_dir.is_dir()
            or manifest_path.parent.resolve() != archive_dir.resolve()
            or sha256(manifest_path) != str(receipt.get("sha256") or "").upper()
            or receipt.get("ticket_id") != ticket.get("ticket_id")
            or sha256(ticket_path) != str(receipt.get("ticket_sha256") or "").upper()):
        raise ValueError("cannot supplement an invalid original recovery archive receipt")
    base = load(manifest_path)
    if base.get("ticket_id") != ticket.get("ticket_id") or base.get("cycle_id") != ticket.get("cycle_id"):
        raise ValueError("cannot supplement a base manifest for another ticket or cycle")
    base_files = base.get("files")
    if not isinstance(base_files, dict) or not base_files:
        raise ValueError("cannot supplement an empty or malformed base manifest")
    # Validate every existing entry before considering an extension. This never
    # repairs or masks a corrupted item already present in the immutable base.
    for name, archived in base_files.items():
        bound = ticket.get("artifacts", {}).get(name)
        if not isinstance(bound, dict):
            raise ValueError(f"base archive contains unbound evidence: {name}")
        path = Path(archived.get("path") or "")
        try:
            path.resolve().relative_to(archive_dir.resolve())
        except ValueError as exc:
            raise ValueError(f"base archive evidence escapes bundle: {name}") from exc
        if (not path.is_file() or archived.get("sha256") != bound.get("sha256")
                or archived.get("size") != bound.get("size")
                or archived.get("original_path") != str(Path(bound.get("path") or "").resolve())
                or path.stat().st_size != bound.get("size")
                or sha256(path) != str(bound.get("sha256") or "").upper()):
            raise ValueError(f"base archive evidence failed byte/hash validation: {name}")
    supplements = list(receipt.get("supplements") or [])
    combined = dict(base_files)
    # Validate existing supplements before reusing their inventory.
    if supplements:
        validate_archive_receipt(ticket, ticket_path, {**receipt, "supplements": supplements})
        for item in supplements:
            sub_manifest = load(Path(item["path"]))
            combined.update(sub_manifest["files"])
    missing = sorted(set(ticket.get("artifacts", {})) - set(combined))
    if not missing:
        return {**receipt, "supplements": supplements}
    records = {name: ticket["artifacts"][name] for name in missing}
    for name, record in records.items():
        source = Path(record.get("path") or "")
        if not _check_bound_file(record) or source.stat().st_size != record.get("size"):
            raise ValueError(f"missing ticket evidence source no longer matches its bound hash: {name}")
    # The directory name is deterministic so a crash after sealing the manifest
    # can be reconciled without overwriting any prior supplement.
    identity = hashlib.sha256(json.dumps(records, sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
    archive_root = archive_dir.resolve(strict=True)
    supplements_root = archive_root / "supplements"
    supplement_dir = supplements_root / identity
    _assert_archive_path_safe(supplements_root, archive_root)
    if not supplements_root.exists():
        supplements_root.mkdir()
    _assert_archive_path_safe(supplement_dir, archive_root)
    if not supplement_dir.exists():
        supplement_dir.mkdir()
    _assert_archive_path_safe(supplement_dir, archive_root)
    supplement_manifest = supplement_dir / "manifest.json"
    copied = {}
    if supplement_manifest.exists():
        provisional = {"path": str(supplement_manifest.resolve()), "sha256": sha256(supplement_manifest),
                       "ticket_id": ticket["ticket_id"]}
        manifest = load(supplement_manifest)
        if (manifest.get("schema_version") != 1 or manifest.get("ticket_id") != ticket["ticket_id"]
                or manifest.get("cycle_id") != ticket["cycle_id"]
                or manifest.get("ticket_sha256") != sha256(ticket_path)
                or manifest.get("base_manifest_sha256") != sha256(manifest_path)):
            raise ValueError("existing supplemental archive manifest identity is invalid")
        copied = manifest.get("files") or {}
        # Validate every existing file using the same full combined check below.
        validate_archive_receipt(ticket, ticket_path,
                                 {**receipt, "supplements": supplements + [provisional]})
    else:
        expected_names = {f"{name}--{Path(record['path']).name}" for name, record in records.items()}
        for existing in supplement_dir.iterdir():
            _assert_archive_path_safe(existing, archive_root)
            if existing.name not in expected_names:
                raise ValueError("partial supplemental archive contains unexpected content")
        for name in missing:
            record = records[name]
            source = Path(record.get("path") or "")
            destination = supplement_dir / f"{name}--{source.name}"
            _assert_archive_path_safe(destination, archive_root)
            if destination.exists():
                if (not destination.is_file() or destination.stat().st_size != record["size"]
                        or sha256(destination) != str(record["sha256"]).upper()):
                    raise ValueError(f"orphan supplemental copy failed byte/hash validation: {name}")
            else:
                with source.open("rb") as src, destination.open("xb") as dst:
                    shutil.copyfileobj(src, dst)
                    dst.flush()
                    os.fsync(dst.fileno())
            if sha256(destination) != record["sha256"] or destination.stat().st_size != record["size"]:
                raise ValueError(f"supplemental archive copy failed byte/hash validation: {name}")
            destination.chmod(0o444)
            copied[name] = {"path": str(destination.resolve()), "sha256": sha256(destination),
                            "size": destination.stat().st_size, "original_path": str(source.resolve())}
        manifest = {"schema_version": 1, "ticket_id": ticket["ticket_id"],
                    "cycle_id": ticket["cycle_id"], "ticket_sha256": sha256(ticket_path),
                    "base_manifest_sha256": sha256(manifest_path), "files": copied,
                    "created_utc": datetime.now(timezone.utc).isoformat()}
        with supplement_manifest.open("x", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        supplement_manifest.chmod(0o444)
    supplement_receipt = {"path": str(supplement_manifest.resolve()),
                          "sha256": sha256(supplement_manifest), "ticket_id": ticket["ticket_id"]}
    result = {**receipt, "supplements": supplements + [supplement_receipt]}
    validate_archive_receipt(ticket, ticket_path, result)
    return result


def recover_archive_receipt(ticket: dict, ticket_path: Path, archive_dir: Path, *,
                            allow_missing_ticket_artifacts: bool = False) -> dict:
    """Reconcile a complete immutable archive created before its state receipt."""
    manifest_path = archive_dir / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("archive directory exists without a complete manifest")
    manifest = load(manifest_path)
    receipt = {"path": str(manifest_path.resolve()), "sha256": sha256(manifest_path),
               "archive_dir": str(archive_dir.resolve()), "files": manifest.get("files"),
               "ticket_id": ticket.get("ticket_id"), "ticket_sha256": sha256(ticket_path)}
    if allow_missing_ticket_artifacts:
        if (manifest.get("ticket_id") != ticket.get("ticket_id")
                or manifest.get("cycle_id") != ticket.get("cycle_id")
                or not isinstance(manifest.get("files"), dict)
                or sha256(manifest_path) != receipt["sha256"]):
            raise ValueError("legacy archive manifest identity or digest is invalid")
    else:
        validate_archive_receipt(ticket, ticket_path, receipt)
    return receipt


def validate_attempt_archive(receipt: dict, *, ticket_id: str, cycle_id: str,
                             event_id: str, execution_id: str | None = None) -> dict:
    """Fail closed unless a failed-attempt manifest is complete and hash-bound."""
    manifest_path = Path(receipt.get("path") or "")
    if (not manifest_path.is_file()
            or sha256(manifest_path) != str(receipt.get("sha256") or "").upper()
            or receipt.get("event_id") != event_id):
        raise ValueError("failed-attempt archive receipt is missing or changed")
    manifest = load(manifest_path)
    if (manifest.get("schema_version") != 1 or manifest.get("ticket_id") != ticket_id
            or manifest.get("cycle_id") != cycle_id or manifest.get("event_id") != event_id
            or not manifest.get("execution_id")
            or receipt.get("execution_id") != manifest.get("execution_id")
            or execution_id is not None and manifest.get("execution_id") != execution_id):
        raise ValueError("failed-attempt manifest identity or Hermes execution binding is invalid")
    files = manifest.get("files")
    required = {"failure_event", "verification", "compile", "handoff", "source",
                "ex5", "build_receipt", "admission"}
    if not isinstance(files, dict) or not required <= set(files):
        raise ValueError("failed-attempt archive omits required mutable Stage 2 lineage")
    root = manifest_path.parent.resolve()
    for name, item in files.items():
        path = Path(item.get("path") or "")
        try:
            path.resolve().relative_to(root)
        except ValueError as exc:
            raise ValueError(f"failed-attempt archived file escapes its bundle: {name}") from exc
        digest = str(item.get("sha256") or "").upper()
        if (len(digest) != 64 or not path.is_file() or type(item.get("size")) is not int
                or path.stat().st_size != item["size"] or sha256(path) != digest
                or not item.get("original_path")):
            raise ValueError(f"failed-attempt archive hash/size validation failed: {name}")
    return manifest


def classify_and_prepare(
    state: dict,
    *,
    state_dir: Path,
    authorize_full_cycle,
    stage3_owner_live: bool,
    dcc_gate,
    stage2_attestation_validator,
) -> dict | None:
    """Validate the exact same-cycle Stage 3 source defect and create a ticket.

    Return None when this is not the narrow recovery case. Any partially
    specified or mismatched evidence raises ValueError so the supervisor holds.
    """
    if state.get("stage_status") not in {"stage3_infrastructure_failure", "stage3_failed_held"}:
        return None
    if state.get("stage3_failure_class") != "infrastructure":
        return None
    if not authorize_full_cycle(state) or stage3_owner_live:
        return None
    cycle, ea = state.get("cycle_id"), state.get("current_ea")
    if not isinstance(cycle, str) or not cycle or not isinstance(ea, str) or not ea:
        raise ValueError("current cycle/candidate identity is incomplete")
    failure_path = Path(state.get("stage3_failure_event") or "")
    handoff_path = Path(state.get("last_stage3_handoff") or "")
    if not failure_path.is_file() or not handoff_path.is_file():
        raise ValueError("Stage 3 failure or handoff evidence is missing")
    failure, stage3_handoff = load(failure_path), load(handoff_path)
    if (failure.get("cycle_id") != cycle or failure.get("stage") != 3
            or failure.get("status") != "failed" or failure.get("consumed") is not False
            or failure.get("rotation_eligible") is not False or failure.get("ea") != ea):
        raise ValueError("Stage 3 failure identity/consumption/rotation contract mismatch")
    runtime_path = Path(stage3_handoff.get("runtime_manifest") or "")
    if not runtime_path.is_file():
        raise ValueError("Stage 3 handoff does not identify its runtime manifest")
    runtime = load(runtime_path)
    if (runtime.get("cycle_id") != cycle or runtime.get("stage") != 3 or runtime.get("ea") != ea
            or runtime.get("status") != "INFRASTRUCTURE_FAILURE"
            or runtime.get("failure_kind") != "TESTER_JOURNAL_REJECTED"
            or runtime.get("infrastructure_only") is not True):
        raise ValueError("Stage 3 runtime is not the authenticated infrastructure-only Tester Journal defect")
    # Current failure receipts bind the exact runtime and timeframe. Accept
    # legacy receipts only through the same-cycle canonical handoff above, then
    # bind both immutable files into the recovery ticket below.
    if (failure.get("runtime_manifest")
            and Path(str(failure.get("runtime_manifest"))).resolve() != runtime_path.resolve()):
        raise ValueError("Stage 3 failure receipt points to a different runtime manifest")
    if (failure.get("runtime_manifest_sha256")
            and str(failure.get("runtime_manifest_sha256")).upper() != sha256(runtime_path)):
        raise ValueError("Stage 3 failure receipt runtime hash changed")
    if failure.get("timeframe") and failure.get("timeframe") != runtime.get("timeframe"):
        raise ValueError("Stage 3 failure receipt timeframe does not match its runtime")
    if (failure.get("runtime_failure_kind")
            and failure.get("runtime_failure_kind") != runtime.get("failure_kind")):
        raise ValueError("Stage 3 failure receipt runtime failure kind changed")
    diagnostic = runtime.get("journal_diagnostic") or {}
    lines = diagnostic.get("error_lines")
    if not isinstance(lines, list):
        raise ValueError("Stage 3 runtime has no journal error-line inventory")
    repair_kind = None
    invalid_stop_lines = [line for line in lines if isinstance(line, str)
                          and "invalid stops" in line.casefold()
                          and ("failed market buy" in line.casefold()
                               or "failed market sell" in line.casefold())]
    if (diagnostic.get("code") in {"TESTER_EA_ORDER_REJECTED", "TESTER_JOURNAL_ERROR"}
            and len(invalid_stop_lines) >= 2):
        matching = invalid_stop_lines
        repair_kind = "stage3_tester_journal_invalid_stops"
    else:
        matching = [line for line in lines if isinstance(line, str)
                    and "runtime error 502" in line.casefold()
                    and "array out of range" in line.casefold()
                    and ea.casefold() in line.casefold()]
        if len(matching) < 2:
            raise ValueError("fewer than two current-EA runtime 502 array-bounds journal lines")
        repair_kind = "stage3_tester_journal_dcc_closed_return_bounds"

    stage1_spec = Path(state.get("last_stage1_artifact") or "")
    stage1_event = Path(state.get("last_stage1_event") or "")
    stage2_event = Path(state.get("last_stage2_event") or "")
    verification_path = Path(state.get("last_stage2_verification") or "")
    stage2_handoff_path = Path(state.get("last_stage2_handoff") or
                               state_dir / "handoffs" / "Stage2_v2.json")
    for label, path in (("Stage 1 spec", stage1_spec), ("Stage 1 event", stage1_event),
                        ("Stage 2 event", stage2_event), ("Stage 2 verification", verification_path),
                        ("Stage 2 handoff", stage2_handoff_path)):
        if not path.is_file():
            raise ValueError(f"{label} missing: {path}")
    s1, s1event = load(stage1_spec), load(stage1_event)
    s2event, verification, s2handoff = load(stage2_event), load(verification_path), load(stage2_handoff_path)
    if (s1.get("cycle_id") != cycle or s1event.get("cycle_id") != cycle or s1event.get("stage") != 1
            or s1event.get("status") != "complete"
            or Path(s1event.get("artifact") or "").resolve() != stage1_spec.resolve()
            or s1event.get("artifact_sha256") != sha256(stage1_spec)):
        raise ValueError("Stage 1 spec/event lineage does not match the current cycle")
    source_path, ex5_path = Path(verification.get("source") or ""), Path(verification.get("ex5") or "")
    if (s2event.get("schema_version") != 2 or s2event.get("cycle_id") != cycle or s2event.get("stage") != 2
            or s2event.get("status") != "complete" or s2event.get("ea") != ea
            or s2event.get("consumed") is False
            or verification.get("cycle_id") != cycle or verification.get("stage") != 2
            or verification.get("passed") is not True or not source_path.is_file() or not ex5_path.is_file()
            or Path(s2event.get("verification") or "").resolve() != verification_path.resolve()
            or s2event.get("verification_sha256") != sha256(verification_path)
            or s2event.get("mq5_sha256") != sha256(source_path)
            or s2event.get("ex5_sha256") != sha256(ex5_path)
            or verification.get("source_sha256") != sha256(source_path)
            or verification.get("stage1_spec_sha256") != sha256(stage1_spec)
            or s2event.get("stage1_spec_sha256") != sha256(stage1_spec)
            or Path(s2handoff.get("source") or "").resolve() != source_path.resolve()
            or Path(s2handoff.get("ex5") or "").resolve() != ex5_path.resolve()
            or s2handoff.get("stage1_spec_sha256") != sha256(stage1_spec)):
        raise ValueError("prior Stage 2 event/verification/source/EX5 hashes do not authenticate")
    if s2handoff.get("cycle_id") != cycle or s2handoff.get("ea") != ea:
        raise ValueError("prior Stage 2 handoff identity mismatch")
    if not callable(stage2_attestation_validator):
        raise ValueError("Stage 2 execution attestation validator is required for recovery admission")
    event_hash = sha256(stage2_event)
    state_root = stage2_event.parent.parent if stage2_event.parent.name.casefold() == "events" else stage2_event.parent
    attestation_path = state_root / "attestations" / f"{cycle}.stage2.execution.{event_hash}.json"
    if not attestation_path.is_file():
        raise ValueError("prior Stage 2 DPAPI execution attestation is missing")
    attestation_ok, attestation_detail = stage2_attestation_validator(
        state, stage2_event, verification_path, s2event, attestation_path)
    if attestation_ok is not True:
        raise ValueError(f"prior Stage 2 execution attestation is invalid: {attestation_detail}")
    expected_stage2_event_sha256 = str(stage3_handoff.get("stage2_event_sha256") or "").upper()
    raw_stage2_event_sha256 = sha256(stage2_event)
    consumed_stage2_event = (
        s2event.get("consumed") is True and s2event.get("consumed_action") == "start_stage3"
        and isinstance(s2event.get("consumed_utc"), str) and s2event.get("consumed_utc"))
    allowed_stage2_event_hashes = {raw_stage2_event_sha256}
    if consumed_stage2_event or s2event.get("consumed") is None:
        # Stage 3 handoffs may record the stable canonical digest even when an
        # older Stage 2 publisher omitted the mutable consumption fields.
        allowed_stage2_event_hashes.add(consumption_insensitive_sha256(stage2_event))
    if (stage3_handoff.get("cycle_id") != cycle or stage3_handoff.get("ea") != ea
            or Path(stage3_handoff.get("stage2_event") or "").resolve() != stage2_event.resolve()
            or expected_stage2_event_sha256 not in allowed_stage2_event_hashes
            or Path(stage3_handoff.get("source") or "").resolve() != source_path.resolve()
            or stage3_handoff.get("source_sha256") != sha256(source_path)
            or Path(stage3_handoff.get("ex5") or "").resolve() != ex5_path.resolve()
            or stage3_handoff.get("ex5_sha256") != sha256(ex5_path)):
        raise ValueError("Stage 3 handoff is not bound to the current Stage 2 completion bundle")
    if s2event.get("consumed") is None and not (
            expected_stage2_event_sha256 in allowed_stage2_event_hashes
            and stage3_handoff.get("source_sha256") == sha256(source_path)
            and stage3_handoff.get("ex5_sha256") == sha256(ex5_path)):
        raise ValueError("missing Stage 2 consumed flag is not proven by the Stage 3 handoff")
    spec_primitive = (s1.get("primitive_selection") or {}).get("primitive") or s1.get("primitive")
    if repair_kind == "stage3_tester_journal_dcc_closed_return_bounds":
        source_text = source_path.read_text(encoding="utf-8-sig", errors="replace")
        repair_kind = classify_runtime_502_repair(
            str(spec_primitive or ""), source_text, dcc_gate,
            journal_lines=matching, ea_name=ea)

    compile_path = Path(verification.get("compile", {}).get("path") or "")
    if not compile_path.is_file():
        compile_path = stage2_event.parent.parent / "attempts" / f"{cycle}.stage2.compile.latest.json"
    build_value = s2handoff.get("source_build_receipt") or str(source_path.with_suffix(source_path.suffix + ".build.json"))
    build_path = Path(build_value)
    if not build_path.is_file():
        raise ValueError("prior Stage 2 source build receipt is missing")
    build = load(build_path)
    if (build.get("cycle_id") != cycle or build.get("candidate_ea") != ea
            or Path(build.get("output") or "").resolve() != source_path.resolve()
            or str(build.get("sha256") or "").upper() != sha256(source_path)
            or Path(build.get("source_spec") or "").resolve() != stage1_spec.resolve()
            or str(build.get("source_spec_sha256") or "").upper() != sha256(stage1_spec)
            or str(s2handoff.get("source_build_receipt_sha256") or "").upper() != sha256(build_path)):
        raise ValueError("prior source build receipt does not authenticate current cycle/source/spec")
    admission_ref = build.get("implementation_admission") or build.get("implementation_review")
    admission_path = Path((admission_ref or {}).get("path") or "")
    if not admission_path.is_file():
        raise ValueError("prior Stage 2 source-admission/review proof is missing")
    admission = load(admission_path)
    if (admission.get("cycle_id") != cycle or admission.get("ea") != ea
            or admission.get("passed") is not True
            or str(admission.get("source_sha256") or "").upper() != sha256(source_path)
            or str(admission.get("source_spec_sha256") or "").upper() != sha256(stage1_spec)
            or str(admission_ref.get("sha256") or "").upper() != sha256(admission_path)):
        raise ValueError("prior source-admission proof does not authenticate current source/spec")
    artifacts = {
        "stage1_spec": _bound_file("stage1_spec", stage1_spec),
        "stage1_event": _bound_file("stage1_event", stage1_event),
        "stage2_event": _bound_file("stage2_event", stage2_event),
        "stage2_verification": _bound_file("stage2_verification", verification_path),
        "stage2_handoff": _bound_file("stage2_handoff", stage2_handoff_path),
        "stage2_source": _bound_file("stage2_source", source_path),
        "stage2_original_source": _bound_file("stage2_original_source", source_path),
        "stage2_ex5": _bound_file("stage2_ex5", ex5_path),
        "stage2_build_receipt": _bound_file("stage2_build_receipt", build_path),
        "stage2_admission": _bound_file("stage2_admission", admission_path),
        "stage3_failure": _bound_file("stage3_failure", failure_path),
        "stage3_runtime": _bound_file("stage3_runtime", runtime_path),
        "stage3_handoff": _bound_file("stage3_handoff", handoff_path),
    }
    if not compile_path.is_file():
        raise ValueError("prior Stage 2 compile evidence is missing")
    compile_doc = load(compile_path)
    if (compile_doc.get("cycle_id") != cycle or compile_doc.get("stage") != 2
            or compile_doc.get("mq5_sha256") != sha256(source_path)
            or compile_doc.get("ex5_sha256") != sha256(ex5_path)
            or compile_doc.get("passed") is not True):
        raise ValueError("prior Stage 2 compile evidence does not bind source/EX5")
    artifacts["stage2_compile"] = _bound_file("stage2_compile", compile_path)
    canonical_minato_path = Path(verification.get("canonical_minato") or "")
    if not canonical_minato_path.is_file() or sha256(canonical_minato_path) != s2event.get("canonical_minato_sha256"):
        raise ValueError("prior canonical Minato artifact is missing or hash-mismatched")
    artifacts["stage2_canonical_minato"] = _bound_file("stage2_canonical_minato", canonical_minato_path)
    artifacts["stage2_execution_attestation"] = _bound_file("stage2_execution_attestation", attestation_path)
    authorization = state.get("run_authorization")
    auth_bytes = json.dumps(authorization, sort_keys=True, separators=(",", ":")).encode()
    ticket_core = {"schema_version": 1, "cycle_id": cycle, "ea": ea,
                   "candidate_source": str(source_path.resolve()), "artifacts": artifacts,
                   "authorization_sha256": hashlib.sha256(auth_bytes).hexdigest().upper(),
                   "repair_kind": repair_kind,
                   "parent_source_sha256": sha256(source_path),
                   "stage3_journal_diagnostic": {"code": ("TESTER_EA_ORDER_REJECTED"
                       if repair_kind == "stage3_tester_journal_invalid_stops" else diagnostic.get("code")),
                       "path": diagnostic.get("path"), "start_offset": diagnostic.get("start_offset"),
                       "matching_error_lines": matching}}
    ticket_hash = hashlib.sha256(json.dumps(ticket_core, sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
    nonce = uuid.uuid4().hex
    ticket_path = state_dir / "recovery" / f"{cycle}.stage2.source_defect.{nonce}.ticket.json"
    event_path = state_dir / "events" / f"{cycle}.stage2.source_defect.{nonce}.json"
    ticket = {**ticket_core, "ticket_id": f"{cycle}:stage2:source_defect:{nonce}",
              "ticket_sha256": ticket_hash, "created_utc": datetime.now(timezone.utc).isoformat()}
    ticket_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = ticket_path.parent / f"{cycle}.stage2.source_defect.admission.lock"
    descriptor = os.open(str(lock_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump({"ticket_id": ticket["ticket_id"], "created_utc": ticket["created_utc"]}, handle)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    ticket_path.write_text(json.dumps(ticket, indent=2) + "\n", encoding="utf-8")
    ticket_file_hash = sha256(ticket_path)
    event = {"schema_version": 1, "event_id": ticket["ticket_id"], "cycle_id": cycle,
             "stage": 2, "status": "repair_requested", "ea": ea,
             "ticket": str(ticket_path.resolve()), "ticket_sha256": ticket_file_hash,
             "rotation_eligible": False, "consumed": False,
             "created_utc": ticket["created_utc"]}
    event_path.write_text(json.dumps(event, indent=2) + "\n", encoding="utf-8")
    return {"ticket": str(ticket_path.resolve()), "ticket_sha256": ticket_file_hash,
            "event": str(event_path.resolve()), "event_id": ticket["ticket_id"],
            "event_sha256": sha256(event_path), "cycle_id": cycle,
            "repair_kind": repair_kind}


def validate_ticket(ticket_path: Path, event_path: Path, state: dict, *,
                    allow_consumed_digest_reconcile: bool = False,
                    allow_unconsumed_for_dispatch: bool = False) -> tuple[dict, dict]:
    ticket, event = load(ticket_path), load(event_path)
    ticket_core = {key: value for key, value in ticket.items()
                   if key not in {"ticket_id", "ticket_sha256", "created_utc"}}
    computed_ticket_hash = hashlib.sha256(
        json.dumps(ticket_core, sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
    attempt = event.get("dispatch_attempt", 1)
    event_id = event.get("event_id")
    expected_parent = (ticket.get("ticket_id") if type(attempt) is int and attempt == 2
                       else f"{ticket.get('ticket_id')}:dispatch:{attempt - 1}" if type(attempt) is int else None)
    valid_attempt_identity = valid_dispatch_attempt_identity(ticket, event, state, event_path)
    actual_event_hash = sha256(event_path)
    declared_event_hash = str(state.get("stage2_source_defect_event_sha256") or "").upper()
    digest_matches = bool(declared_event_hash) and declared_event_hash == actual_event_hash
    consumption_intent = state.get("stage2_source_defect_consumption_intent") or {}
    try:
        intent_timestamp_valid = bool(datetime.fromisoformat(
            str(consumption_intent.get("started_utc", "")).replace("Z", "+00:00")).tzinfo)
    except (ValueError, TypeError):
        intent_timestamp_valid = False
    reconcileable_keys = {"schema_version", "event_id", "cycle_id", "stage", "status", "consumed",
                          "rotation_eligible", "ticket", "ticket_sha256", "created_utc", "ea",
                          "dispatch_attempt", "parent_event_id", "parent_attempt_failed",
                          "consumed_utc", "consumed_action"}
    digest_reconcileable = (allow_consumed_digest_reconcile and event.get("consumed") is True
                            and event.get("consumed_action") == "dispatch_stage2"
                            and consumption_intent.get("event_id") == event_id
                            and intent_timestamp_valid
                            and set(event) <= reconcileable_keys)
    consumption_valid = (
        event.get("consumed") is True and event.get("consumed_action") == "dispatch_stage2"
        or allow_unconsumed_for_dispatch and event.get("consumed") is False
        and event.get("consumed_action") in {None, ""}
    )
    if (not digest_matches and not digest_reconcileable
            or ticket.get("schema_version") != 1 or event.get("schema_version") != 1
            or ticket.get("cycle_id") != state.get("cycle_id") or event.get("cycle_id") != state.get("cycle_id")
            or not valid_attempt_identity
            or event.get("stage") != 2 or event.get("status") != "repair_requested"
            or event.get("rotation_eligible") is not False
            or ticket.get("ea") != state.get("current_ea")
            or event.get("ticket_sha256") != sha256(ticket_path)
            or ticket.get("ticket_sha256") != computed_ticket_hash
            or event.get("ticket") != str(ticket_path.resolve())
            or not consumption_valid):
        raise ValueError("same-cycle Stage 2 recovery ticket/event is invalid, replayed, or unconsumed")
    if type(attempt) is int and attempt > 1:
        parent_failed = event.get("parent_attempt_failed")
        prior_attempt_archive = state.get("stage2_source_defect_prior_attempt_archive") or {}
        if prior_attempt_archive.get("event_id") != expected_parent:
            prior_attempt_archive = (_find_parent_attempt_archive(
                ticket_path, ticket, str(state.get("cycle_id") or ""), str(expected_parent)) or {})
        parent_failed_is_proven = prior_attempt_archive.get("event_id") == expected_parent
        if type(parent_failed) is not bool or parent_failed is not parent_failed_is_proven:
            raise ValueError("retry event does not bind its parent attempt's archived failure evidence")
        if parent_failed:
            validate_attempt_archive(prior_attempt_archive, ticket_id=ticket["ticket_id"],
                                     cycle_id=ticket["cycle_id"], event_id=expected_parent,
                                     execution_id=prior_attempt_archive.get("execution_id"))
    original_bundle = state.get("stage2_source_defect_archive")
    if original_bundle:
        validate_archive_receipt(ticket, ticket_path, original_bundle)
    else:
        for record in ticket.get("artifacts", {}).values():
            if not _check_bound_file(record):
                raise ValueError("a hash-bound recovery artifact changed before archive")
    auth = state.get("run_authorization")
    auth_hash = hashlib.sha256(json.dumps(auth, sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
    if auth_hash != ticket.get("authorization_sha256"):
        raise ValueError("current-cycle authorization changed after recovery ticket creation")
    return ticket, event


def archive_stage2_bundle(ticket: dict, archive_root: Path) -> dict:
    """Copy old Stage 2 fixed-path evidence byte-for-byte into exclusive archive."""
    cycle = str(ticket["cycle_id"])
    folder = archive_root / "stage2_source_defect_rebuild" / cycle / str(ticket["ticket_id"].rsplit(":", 1)[-1])
    folder.mkdir(parents=True, exist_ok=False)
    copied = {}
    try:
        for name in ("stage1_spec", "stage1_event", "stage2_event", "stage2_verification", "stage2_handoff", "stage2_compile",
                     "stage2_source", "stage2_ex5", "stage2_build_receipt", "stage2_admission",
                     "stage2_canonical_minato", "stage2_execution_attestation",
                     "stage3_failure", "stage3_runtime", "stage3_handoff", "stage2_original_source"):
            record = ticket["artifacts"].get(name)
            if not record:
                continue
            source = Path(record["path"])
            if not _check_bound_file(record):
                raise ValueError(f"Stage 2 artifact changed before archive: {name}")
            destination = folder / f"{name}--{source.name}"
            with source.open("rb") as src, destination.open("xb") as dst:
                shutil.copyfileobj(src, dst)
                dst.flush()
                os.fsync(dst.fileno())
            if sha256(destination) != record["sha256"] or destination.stat().st_size != record["size"]:
                raise ValueError(f"byte-for-byte Stage 2 archive verification failed: {name}")
            destination.chmod(0o444)
            copied[name] = {"path": str(destination.resolve()), "sha256": sha256(destination),
                            "size": destination.stat().st_size, "original_path": str(source.resolve())}
        manifest = {"schema_version": 1, "ticket_id": ticket["ticket_id"], "cycle_id": cycle,
                    "files": copied, "created_utc": datetime.now(timezone.utc).isoformat()}
        manifest_path = folder / "manifest.json"
        with manifest_path.open("x", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        manifest_path.chmod(0o444)
        return {"path": str(manifest_path.resolve()), "sha256": sha256(manifest_path),
                "archive_dir": str(folder.resolve()), "files": copied}
    except Exception:
        # Keep partial evidence visible for forensic review; never remove or retry
        # into the same exclusive ticket directory.
        raise
