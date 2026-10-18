"""Fail-closed raw-response P3M inventory and stage/verify-only adapter.

This module never changes raw payloads. It requires caller-supplied,
release-bound source authorization and complete descendant evidence because
the raw JSONL writer does not persist either proof in the response envelope.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from engine.lib.collection_retention import RetentionHold, expiry_fields, parse_capture_time
from engine.lib import nami_io
from engine.lib.contracts import canonical_json
from engine.lib.exact_root_lease import ExactRootQuiescenceLease

SCHEMA = "nami.raw_response_inventory.v1"
_SHA = re.compile(r"^[0-9a-f]{64}$")
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)


class RawResponseRetentionHold(ValueError):
    """Raw-response coverage or transaction binding is incomplete."""


def _is_reparse(info) -> bool:
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0) & _REPARSE_POINT)


def _resolve_state_root(path: Path | str) -> Path:
    lexical = Path(path).expanduser().absolute()
    if ".." in lexical.parts:
        raise RawResponseRetentionHold("raw_response_root_traversal_component")
    current = Path(lexical.anchor)
    for component in lexical.parts[1:]:
        current = current / component
        try:
            info = current.lstat()
        except OSError as exc:
            raise RawResponseRetentionHold("raw_response_root_component_unavailable") from exc
        if _is_reparse(info):
            raise RawResponseRetentionHold("raw_response_state_root_reparse_point")
    try:
        resolved = lexical.resolve(strict=True)
        info = resolved.lstat()
    except (OSError, RuntimeError) as exc:
        raise RawResponseRetentionHold("raw_response_root_unavailable") from exc
    if _is_reparse(info) or not stat.S_ISDIR(info.st_mode):
        raise RawResponseRetentionHold("raw_response_root_type_invalid")
    return resolved


def _resolve_child(root: Path, relative: str, *, directory: bool) -> Path:
    """lstat every child component before type checks, reads, or opens."""
    lexical = root / relative
    current = root
    parts = Path(relative).parts
    for index, component in enumerate(parts):
        current = current / component
        try:
            info = current.lstat()
        except OSError as exc:
            raise RawResponseRetentionHold("raw_response_bound_path_unavailable") from exc
        if _is_reparse(info):
            raise RawResponseRetentionHold("raw_response_bound_path_reparse_point")
        if index < len(parts) - 1 and not stat.S_ISDIR(info.st_mode):
            raise RawResponseRetentionHold("raw_response_bound_parent_not_directory")
    try:
        resolved_root = root.resolve(strict=True)
        resolved = lexical.resolve(strict=True)
        resolved.relative_to(resolved_root)
    except (OSError, RuntimeError, ValueError) as exc:
        raise RawResponseRetentionHold("raw_response_bound_path_escape") from exc
    info = resolved.lstat()
    if _is_reparse(info):
        raise RawResponseRetentionHold("raw_response_bound_path_reparse_point")
    if directory and not stat.S_ISDIR(info.st_mode):
        raise RawResponseRetentionHold("raw_response_bound_path_type_invalid")
    if not directory and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1):
        raise RawResponseRetentionHold("raw_response_bound_path_type_invalid")
    return resolved


# Compatibility name retained for the existing raw-response assessment API.
RawDatasetStoreLease = ExactRootQuiescenceLease


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _safe_source(value: str) -> str:
    name = re.sub(r"[^a-z0-9_-]+", "_", value.strip().lower()).strip("_")
    return name or "unknown"


def _regular_files(secure_io, relative_root: str, *, extra_suffixes=()):
    """Enumerate only entries opened and classified beneath retained root handles."""
    files = []
    directories = []
    enumerated_payloads = {}
    allow_manifest_locks = ".manifest.json" in extra_suffixes
    root_handle, root_chain = secure_io.open_handle(relative_root, directory=True)
    try:
        root_identity = secure_io.handle_identity(root_handle)
    finally:
        for opened in reversed(root_chain):
            from engine.lib.secure_state_io import _CloseHandle
            _CloseHandle(opened)
    directories.append({"path": ".", "identity": {
        "device": root_identity["device"], "file_id": root_identity["file_id"]}})
    def visit(directory_relative, directory_identity=None):
        for name, identity in secure_io.list_entries(
                directory_relative, expected_identity=directory_identity):
            relative = f"{directory_relative}/{name}"
            if secure_io.is_directory(relative, expected_identity=identity):
                directories.append({"path": relative[len(relative_root) + 1:],
                    "identity": {"device": identity["device"], "file_id": identity["file_id"]}})
                visit(relative, identity)
                continue
            # Open and read once during classification so missing, reparse, and
            # out-of-root entries cannot become catalog rows.
            secure_io.read_bytes(relative, expected_identity=identity)
            path = Path(name)
            is_jsonl_lock = name.endswith(".jsonl.lock")
            is_manifest_lock = name.endswith(".manifest.json.lock")
            if is_jsonl_lock or is_manifest_lock:
                if is_manifest_lock and not allow_manifest_locks:
                    raise RawResponseRetentionHold("raw_response_unknown_artifact")
                payload_relative = relative[:-5]
                if payload_relative not in enumerated_payloads:
                    raise RawResponseRetentionHold("raw_response_lock_payload_missing")
                expected_lock_bytes = b"" if nami_io._HAS_FCNTL else b"0"
                payload_identity = enumerated_payloads.get(payload_relative)
                if payload_identity is None:
                    raise RawResponseRetentionHold("raw_response_lock_payload_missing")
                secure_io.read_bytes(payload_relative, expected_identity=payload_identity)
                if secure_io.read_bytes(relative, expected_identity=identity) != expected_lock_bytes:
                    raise RawResponseRetentionHold("raw_response_lock_sidecar_invalid")
                files.append((relative[len(relative_root) + 1:], False, identity))
                continue
            if path.suffix != ".jsonl" and not any(name.endswith(suffix) for suffix in extra_suffixes):
                raise RawResponseRetentionHold("raw_response_unknown_artifact")
            enumerated_payloads[relative] = identity
            files.append((relative[len(relative_root) + 1:], True, identity))
    visit(relative_root, root_identity)
    return sorted(files, key=lambda row: row[0]), sorted(directories, key=lambda row: row["path"])


def _snapshot_artifacts(secure_io, relative_root, files, *, dataset=False):
    rows = []
    for relative, is_payload, identity in files:
        data = secure_io.read_bytes(f"{relative_root}/{relative}", expected_identity=identity)
        if dataset:
            kind = "dataset_jsonl" if is_payload else "writer_lock_sidecar"
        else:
            kind = "response_jsonl" if is_payload else "writer_lock_sidecar"
        rows.append({"path": relative, "size": len(data), "sha256": _sha(data),
                     "kind": kind, "identity": {"device": identity["device"],
                                                   "file_id": identity["file_id"]}})
    return rows


def _assert_final_raw_dataset_snapshot(secure_io, raw_files, raw_directories, dataset_catalog):
    """Re-enumerate IDs and bytes under the still-held exact-root writer gate."""
    final_raw, final_raw_dirs = _regular_files(secure_io, "raw")
    final_datasets, final_dataset_dirs = _regular_files(
        secure_io, "datasets", extra_suffixes=(".manifest.json",))
    raw_rows = _snapshot_artifacts(secure_io, "raw", final_raw)
    dataset_rows = _snapshot_artifacts(secure_io, "datasets", final_datasets, dataset=True)
    if (_canonical(raw_rows) != _canonical(raw_files)
            or _canonical(final_raw_dirs) != _canonical(raw_directories)
            or _canonical(dataset_rows) != _canonical(dataset_catalog["artifacts"])
            or _canonical(final_dataset_dirs) != _canonical(dataset_catalog["directories"])):
        raise RawResponseRetentionHold("raw_response_final_inventory_changed")


def _candidate_release_sha256() -> str:
    candidate_root = Path(__file__).resolve().parents[2]
    try:
        from scripts.nami_qualification import release_manifest
        value = release_manifest(candidate_root).get("sha256")
    except Exception as exc:
        raise RawResponseRetentionHold("raw_response_candidate_release_unverifiable") from exc
    if not isinstance(value, str) or not _SHA.fullmatch(value):
        raise RawResponseRetentionHold("raw_response_candidate_release_unverifiable")
    return value


def _verify_clearance(state_root: Path, release_sha256: str,
                      source_identity_to_connector: dict[str, str], identities: set[str],
                      now: datetime, secure_io=None):
    if not isinstance(source_identity_to_connector, dict):
        raise RawResponseRetentionHold("raw_response_authorization_mapping_missing")
    from engine.lib.source_clearance import (KNOWN_SOURCES, SourceClearanceError,
        _load_registry, require_sources, source_clearance_snapshot, source_selection_snapshot)
    used_connectors = {}
    try:
        for identity in sorted(identities):
            connector = source_identity_to_connector.get(identity)
            if (not isinstance(connector, str) or connector not in KNOWN_SOURCES
                    or not connector.strip()):
                raise RawResponseRetentionHold("raw_response_authorization_missing")
            used_connectors[identity] = connector
        if set(source_identity_to_connector) != identities:
            raise RawResponseRetentionHold("raw_response_authorization_mapping_extra_or_missing")
        connectors = sorted(set(used_connectors.values()))
        if secure_io is None:
            raise RawResponseRetentionHold("raw_response_handle_bound_io_unavailable")
        # Open every authorization root and file through retained root-bound
        # handles; the source-clearance validator consumes the same handle API.
        if not secure_io.is_directory("source_clearance_evidence"):
            raise RawResponseRetentionHold("raw_response_authorization_evidence_missing")
        registry = _load_registry(state_root, safe_io=secure_io)
        for connector in connectors:
            record = registry["records"].get(connector)
            evidence_name = record.get("evidence_file") if isinstance(record, dict) else None
            if not isinstance(evidence_name, str) or not evidence_name.strip():
                raise RawResponseRetentionHold("raw_response_authorization_evidence_missing")
            evidence_relative = Path(evidence_name)
            if evidence_relative.is_absolute() or ".." in evidence_relative.parts:
                raise RawResponseRetentionHold("raw_response_authorization_evidence_path_invalid")
            secure_io.read_bytes((Path("source_clearance_evidence") / evidence_relative).as_posix())
        selection = source_selection_snapshot(state_root, enforce_pin=True, safe_io=secure_io)
        clearance_pin = require_sources(state_root, connectors, now=now, safe_io=secure_io)
        snapshot = source_clearance_snapshot(state_root, connectors, now=now, safe_io=secure_io)
        if clearance_pin != snapshot["source_clearance_fingerprint"]:
            raise RawResponseRetentionHold("raw_response_clearance_pin_mismatch")
    except RawResponseRetentionHold:
        raise
    except (SourceClearanceError, OSError, ValueError, TypeError) as exc:
        raise RawResponseRetentionHold("raw_response_authorization_evidence_invalid") from exc
    # The verified source clearance fingerprints and their external evidence
    # bytes are now included in this release-bound inventory receipt.
    return {"candidate_release_sha256": release_sha256,
            "source_selection_fingerprint": selection["source_selection_fingerprint"],
            "source_selection_enabled": selection["source_selection_enabled"],
            "source_processing_profile": selection["source_processing_profile"],
            "active_run_clearance_pin": clearance_pin,
            "source_clearance_fingerprint": snapshot["source_clearance_fingerprint"],
            "source_clearance_sources": snapshot["source_clearance_sources"],
            "source_identity_to_connector": dict(sorted(used_connectors.items()))}


def _inventory_descendants(state_root: Path, release_sha256: str,
                           raw_records_by_id: dict[str, dict], now: datetime, secure_io):
    """Build a complete, deterministic catalog from every dataset artifact."""
    if not secure_io.is_directory("datasets"):
        raise RawResponseRetentionHold("raw_response_descendant_root_invalid")
    files, directories = _regular_files(secure_io, "datasets", extra_suffixes=(".manifest.json",))
    data_files = {}
    dataset_bytes = {}
    manifests = []
    artifacts = []
    for rel, is_payload, identity in files:
        path = Path(rel)
        if len(path.parts) != 1:
            raise RawResponseRetentionHold("raw_response_descendant_nested_directory_unknown")
        data = secure_io.read_bytes(f"datasets/{rel}", expected_identity=identity)
        artifacts.append({"path": rel, "size": len(data), "sha256": _sha(data),
                          "kind": "dataset_jsonl" if is_payload else "writer_lock_sidecar",
                          "identity": {"device": identity["device"], "file_id": identity["file_id"]}})
        if not is_payload:
            continue
        if path.name.endswith(".manifest.json"):
            try:
                item = json.loads(data.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
            except Exception as exc:
                raise RawResponseRetentionHold("raw_response_descendant_manifest_invalid") from exc
            if not isinstance(item, dict):
                raise RawResponseRetentionHold("raw_response_descendant_manifest_invalid")
            manifests.append((path, item))
            continue
        if path.suffix != ".jsonl":
            raise RawResponseRetentionHold("raw_response_descendant_artifact_unknown")
        dataset_id = path.stem
        if dataset_id in data_files:
            raise RawResponseRetentionHold("raw_response_descendant_dataset_duplicate")
        lines = []
        for ordinal, line in enumerate(data.splitlines(), 1):
            try:
                row = json.loads(line.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
            except Exception as exc:
                raise RawResponseRetentionHold("raw_response_descendant_record_invalid") from exc
            capture = row.get("_capture") if isinstance(row, dict) else None
            if not isinstance(capture, dict):
                raise RawResponseRetentionHold("raw_response_descendant_lineage_missing")
            raw_id, identity = capture.get("raw_response_id"), capture.get("source_identity")
            if (not isinstance(raw_id, str) or not _SHA.fullmatch(raw_id)
                    or not isinstance(identity, str) or not identity.strip()):
                raise RawResponseRetentionHold("raw_response_descendant_lineage_invalid")
            required_capture = {"hypothesis_id", "source", "source_identity", "raw_response_id",
                                "captured_at", "expires_at", "retention_policy",
                                "retention_lineage", "decision_cutoff"}
            if set(capture) != required_capture:
                raise RawResponseRetentionHold("raw_response_descendant_capture_schema_invalid")
            parent = raw_records_by_id.get(raw_id)
            if parent is None or identity != parent["source_identity"] or capture.get("source") != parent["source"]:
                raise RawResponseRetentionHold("raw_response_descendant_parent_identity_mismatch")
            try:
                captured = parse_capture_time(capture["captured_at"])
                expires = parse_capture_time(capture["expires_at"])
                parent_expiry = parse_capture_time(parent["expires_at"])
                expected_expiry = min(parse_capture_time(expiry_fields(captured)["expires_at"]), parent_expiry)
                cutoff = parse_capture_time(capture["decision_cutoff"])
            except (RetentionHold, TypeError, ValueError) as exc:
                raise RawResponseRetentionHold("raw_response_descendant_retention_invalid") from exc
            expected_policy = ("P3M calendar months from capture"
                               if capture["retention_policy"] == "P3M calendar months from capture"
                               else "inherited P3M calendar-month expiry")
            lineage = capture["retention_lineage"]
            if (captured > now or parent_expiry <= captured or cutoff != captured
                    or capture["captured_at"] != captured.isoformat()
                    or capture["expires_at"] != expires.isoformat()
                    or expires != expected_expiry
                    or capture["retention_policy"] != expected_policy
                    or not isinstance(capture["hypothesis_id"], str) or not capture["hypothesis_id"].strip()
                    or not isinstance(lineage, list) or len(lineage) != 1
                    or not isinstance(lineage[0], dict)
                    or set(lineage[0]) != {"role", "response_id", "expires_at"}
                    or lineage[0] != {"role": "raw_adapter_response", "response_id": raw_id,
                                      "expires_at": parent["expires_at"]}):
                raise RawResponseRetentionHold("raw_response_descendant_retention_or_lineage_mismatch")
            lines.append({"raw_response_id": raw_id, "source_identity": identity,
                          "source": capture["source"], "hypothesis_id": capture["hypothesis_id"],
                          "captured_at": capture["captured_at"], "expires_at": capture["expires_at"],
                          "retention_policy": capture["retention_policy"],
                          "retention_lineage": lineage,
                          "dataset_id": dataset_id, "record_ordinal": ordinal,
                          "record_sha256": _sha(line)})
        if not lines:
            raise RawResponseRetentionHold("raw_response_descendant_dataset_empty")
        data_files[dataset_id] = lines
        dataset_bytes[dataset_id] = data
    if not data_files or not manifests:
        raise RawResponseRetentionHold("raw_response_descendant_catalog_incomplete")
    manifest_ids = set()
    for path, metadata in manifests:
        manifest = metadata.get("dataset_manifest")
        if not isinstance(manifest, dict) or not isinstance(manifest.get("dataset_id"), str):
            raise RawResponseRetentionHold("raw_response_descendant_manifest_schema_invalid")
        dataset_id = manifest["dataset_id"]
        if dataset_id not in data_files:
            raise RawResponseRetentionHold("raw_response_descendant_manifest_orphan")
        hypothesis_id = metadata.get("hypothesis_id")
        expected_filename = f"{dataset_id}-{_safe_source(hypothesis_id)}.manifest.json" if isinstance(hypothesis_id, str) else ""
        if (path.parent != Path(".") or path.name != expected_filename
                or type(metadata.get("schema_version")) is not int or metadata.get("schema_version") != 1
                or not isinstance(hypothesis_id, str) or not hypothesis_id.strip()):
            raise RawResponseRetentionHold("raw_response_descendant_manifest_path_binding_invalid")
        if (type(manifest.get("schema_version")) is not int or manifest.get("schema_version") != 1
                or type(manifest.get("event_count")) is not int
                or not isinstance(manifest.get("dataset_hash"), str)
                or manifest["dataset_hash"] != _sha(dataset_bytes[dataset_id])
                or dataset_id != f"dataset-{manifest['dataset_hash'][:16]}"
                or manifest.get("event_count") != len(data_files[dataset_id])):
            raise RawResponseRetentionHold("raw_response_descendant_manifest_digest_mismatch")
        row_parents = {(row["raw_response_id"], row["source_identity"])
                       for row in data_files[dataset_id]}
        row_captures = {(row["hypothesis_id"], row["source"], row["source_identity"],
                         row["raw_response_id"], row["captured_at"], row["expires_at"],
                         row["retention_policy"], _canonical(row["retention_lineage"]))
                        for row in data_files[dataset_id]}
        if len(row_parents) != 1 or len(row_captures) != 1:
            raise RawResponseRetentionHold("raw_response_descendant_dataset_lineage_ambiguous")
        row_capture = data_files[dataset_id][0]
        allowed_manifest_keys = {"schema_version", "hypothesis_id", "dataset_manifest", "frozen_rule",
            "windows", "source_identity", "raw_response_id", "captured_at", "expires_at",
            "retention_policy", "retention_lineage"}
        if "evaluation_protocol" in metadata or "evaluation_protocol_hash" in metadata:
            allowed_manifest_keys |= {"evaluation_protocol", "evaluation_protocol_hash"}
            if (not isinstance(metadata.get("evaluation_protocol"), dict)
                    or metadata.get("evaluation_protocol_hash") != _sha(canonical_json(metadata["evaluation_protocol"]).encode("utf-8"))):
                raise RawResponseRetentionHold("raw_response_descendant_evaluation_protocol_invalid")
        if (set(metadata) != allowed_manifest_keys
                or metadata.get("hypothesis_id") != row_capture["hypothesis_id"]
                or metadata.get("source_identity") != row_capture["source_identity"]
                or metadata.get("raw_response_id") != row_capture["raw_response_id"]
                or metadata.get("captured_at") != row_capture["captured_at"]
                or metadata.get("expires_at") != row_capture["expires_at"]
                or metadata.get("retention_policy") != row_capture["retention_policy"]
                or metadata.get("retention_lineage") != row_capture["retention_lineage"]
                or manifest.get("market") != row_capture["source"]
                or manifest.get("decision_cutoff") != row_capture["captured_at"]):
            raise RawResponseRetentionHold("raw_response_descendant_manifest_metadata_mismatch")
        if any(row["hypothesis_id"] != hypothesis_id for row in data_files[dataset_id]):
            raise RawResponseRetentionHold("raw_response_descendant_manifest_hypothesis_mismatch")
        manifest_ids.add(dataset_id)
    if set(data_files) - manifest_ids:
        raise RawResponseRetentionHold("raw_response_descendant_manifest_missing")
    rows = [row for dataset_id in sorted(data_files) for row in data_files[dataset_id]]
    catalog = {"schema": "nami.raw_response_descendant_catalog.v1",
               "release_sha256": release_sha256,
        "root": str(state_root / "datasets"), "artifacts": artifacts,
               "directories": directories,
               "descendants": rows, "record_count": len(rows), "complete": True}
    catalog["catalog_sha256"] = _sha(_canonical(catalog))
    return catalog


def assess_raw_responses(
    root: Path | str,
    *,
    expected_root: Path | str,
    release_sha256: str,
    source_identity_to_connector: dict[str, str],
    isolation_guard,
    transaction_id: str,
    quiescence_attestation,
    now: datetime,
) -> dict:
    """Inventory every raw JSONL record under a quiesced exact-root lease.

    Dataset descendants and verified source-clearance evidence are inventoried
    from the exact state root under the same lease. No caller-supplied lineage
    rows or digest strings are accepted as authority.

    """
    if os.name != "nt":
        raise RawResponseRetentionHold("raw_response_posix_writer_gate_namespace_unpinned")
    from engine.lib.secure_state_io import SecureStateIOError, SecureStateRoot
    resolved = _resolve_state_root(root)
    expected = _resolve_state_root(expected_root)
    if resolved != expected:
        raise RawResponseRetentionHold("raw_response_root_mismatch")
    if (isolation_guard is None or not callable(getattr(isolation_guard, "__enter__", None))
            or not callable(getattr(isolation_guard, "__exit__", None))):
        raise RawResponseRetentionHold("raw_response_exact_root_lease_required")
    try:
        declared_root = _resolve_state_root(getattr(isolation_guard, "root", ""))
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise RawResponseRetentionHold("raw_response_lease_root_mismatch") from exc
    if declared_root != resolved:
        raise RawResponseRetentionHold("raw_response_lease_root_mismatch")
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise RawResponseRetentionHold("raw_response_assessment_time_invalid")
    if not isinstance(transaction_id, str) or not re.fullmatch(r"[0-9a-f]{32}", transaction_id):
        raise RawResponseRetentionHold("raw_response_transaction_id_invalid")
    instant = now.astimezone(timezone.utc)
    if not isinstance(release_sha256, str) or not _SHA.fullmatch(release_sha256):
        raise RawResponseRetentionHold("raw_response_release_identity_invalid")
    actual_release = _candidate_release_sha256()
    if release_sha256 != actual_release:
        raise RawResponseRetentionHold("raw_response_release_identity_mismatch")
    secure_io = None
    try:
        with isolation_guard as lease:
            owner = lease if lease is not None else isolation_guard
            if _resolve_state_root(getattr(owner, "root", resolved)) != resolved:
                raise RawResponseRetentionHold("raw_response_lease_root_mismatch")
            check = getattr(owner, "assert_bound_root", None)
            if not callable(check):
                raise RawResponseRetentionHold("raw_response_authenticated_lease_required")
            check(resolved)
            verify_attestation = getattr(owner, "verify_quiescence_attestation", None)
            if not callable(verify_attestation):
                raise RawResponseRetentionHold("raw_response_lease_quiescence_verifier_missing")
            try:
                verified = verify_attestation(quiescence_attestation, resolved, transaction_id)
            except Exception as exc:
                raise RawResponseRetentionHold("raw_response_lease_quiescence_attestation_invalid") from exc
            if verified is not True:
                raise RawResponseRetentionHold("raw_response_lease_quiescence_attestation_invalid")
            secure_io = getattr(owner, "_secure_root", None)
            if secure_io is None:
                raise RawResponseRetentionHold("raw_response_lease_root_handle_missing")
            opened_final = secure_io.root_final.removeprefix("\\\\?\\").replace("/", "\\")
            if opened_final.casefold() != str(expected).casefold():
                raise RawResponseRetentionHold("raw_response_root_handle_mismatch")
            if (not hasattr(owner, "_root_identity")
                    or secure_io.identity() != owner._root_identity):
                raise RawResponseRetentionHold("raw_response_lease_root_handle_mismatch")
            files, raw_directories = _regular_files(secure_io, "raw")
            file_rows = []
            records = []
            seen = set()
            for relative, is_payload, identity in files:
                path = Path(relative)
                raw = secure_io.read_bytes(f"raw/{relative}", expected_identity=identity)
                file_rows.append({"path": relative,
                                  "size": len(raw), "sha256": _sha(raw),
                                  "kind": "response_jsonl" if is_payload else "writer_lock_sidecar",
                                  "identity": {"device": identity["device"],
                                               "file_id": identity["file_id"]}})
                if not is_payload:
                    continue
                for line_no, line in enumerate(raw.splitlines(), 1):
                    if not line.strip():
                        raise RawResponseRetentionHold("raw_response_blank_line")
                    try:
                        row = json.loads(line.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
                    except Exception as exc:
                        raise RawResponseRetentionHold("raw_response_record_malformed") from exc
                    required = {"response_id", "source", "source_identity", "captured_at",
                                "expires_at", "retention_policy", "response"}
                    if not isinstance(row, dict) or set(row) != required:
                        raise RawResponseRetentionHold("raw_response_record_schema_invalid")
                    if (not isinstance(row["source"], str) or not row["source"].strip()
                            or not isinstance(row["source_identity"], str) or not row["source_identity"].strip()
                            or not isinstance(row["response"], dict)):
                        raise RawResponseRetentionHold("raw_response_record_fields_invalid")
                    try:
                        captured = parse_capture_time(row["captured_at"])
                        fields = expiry_fields(captured)
                    except (RetentionHold, TypeError, ValueError) as exc:
                        raise RawResponseRetentionHold("raw_response_timestamp_invalid") from exc
                    if (row["captured_at"] != fields["captured_at"]
                            or row["expires_at"] != fields["expires_at"]
                            or row["retention_policy"] != fields["retention_policy"]
                            or parse_capture_time(fields["captured_at"]) > instant):
                        raise RawResponseRetentionHold("raw_response_p3m_metadata_invalid")
                    captured_day = parse_capture_time(fields["captured_at"]).date().isoformat()
                    expected_relative = Path(_safe_source(row["source"])) / f"{captured_day}.jsonl"
                    if path != expected_relative:
                        raise RawResponseRetentionHold("raw_response_path_binding_invalid")
                    body = row["response"]
                    if body.get("market") != row["source"] or body.get("data_source") != row["source_identity"]:
                        raise RawResponseRetentionHold("raw_response_identity_mismatch")
                    expected_id = _sha(canonical_json({"source": row["source"],
                        "source_identity": row["source_identity"], "captured_at": fields["captured_at"],
                        "response": body}).encode("utf-8"))
                    if row["response_id"] != expected_id:
                        raise RawResponseRetentionHold("raw_response_id_digest_mismatch")
                    if expected_id in seen:
                        raise RawResponseRetentionHold("raw_response_duplicate_id")
                    seen.add(expected_id)
                    records.append({"response_id": expected_id, "source": row["source"],
                                    "source_identity": row["source_identity"],
                                    "captured_at": fields["captured_at"],
                                    "expires_at": fields["expires_at"],
                                    "record_sha256": _sha(line), "path": relative,
                                    "line": line_no})
            authorization = _verify_clearance(resolved, release_sha256,
                source_identity_to_connector, {row["source_identity"] for row in records}, instant,
                secure_io=secure_io)
            parent_by_id = {row["response_id"]: row for row in records}
            catalog = _inventory_descendants(resolved, release_sha256, parent_by_id, instant, secure_io)
            counts = {}
            for child in catalog["descendants"]:
                parent_id = child["raw_response_id"]
                if parent_id not in parent_by_id:
                    raise RawResponseRetentionHold("raw_response_descendant_parent_missing")
                if child["source_identity"] != parent_by_id[parent_id]["source_identity"]:
                    raise RawResponseRetentionHold("raw_response_descendant_identity_mismatch")
                counts[parent_id] = counts.get(parent_id, 0) + 1
            if any(counts.get(item["response_id"], 0) == 0 for item in records):
                raise RawResponseRetentionHold("raw_response_lineage_descendant_missing")
            check(resolved)
            _assert_final_raw_dataset_snapshot(
                secure_io, file_rows, raw_directories, catalog)
            final_authorization = _verify_clearance(resolved, release_sha256,
                source_identity_to_connector, {row["source_identity"] for row in records}, instant,
                secure_io=secure_io)
            if _canonical(final_authorization) != _canonical(authorization):
                raise RawResponseRetentionHold("raw_response_authorization_changed_during_assessment")
            inventory = {"schema": SCHEMA, "root": str(resolved),
                         "root_identity": secure_io.identity(),
                         "release_sha256": release_sha256, "transaction_id": transaction_id,
                         "files": file_rows, "directories": raw_directories, "records": records,
                         "authorization": authorization,
                         "descendant_catalog": catalog,
                         "descendant_catalog_sha256": catalog["catalog_sha256"],
                         "descendant_record_count": catalog["record_count"],
                         "descendant_counts": dict(sorted(counts.items())),
                         "raw_response_count": len(records), "complete": True,
                         "retention_ready": False}
            inventory["inventory_sha256"] = _sha(_canonical(inventory))
            return inventory
    except RawResponseRetentionHold:
        raise
    except SecureStateIOError as exc:
        if str(exc) == "secure_handle_reparse_point":
            raise RawResponseRetentionHold("raw_response_bound_path_reparse_point") from exc
        if str(exc) == "secure_enumerated_entry_identity_mismatch":
            raise RawResponseRetentionHold("raw_response_enumerated_entry_identity_mismatch") from exc
        raise RawResponseRetentionHold("raw_response_inventory_held") from exc
    except Exception as exc:
        raise RawResponseRetentionHold("raw_response_inventory_held") from exc


def _no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def stage_raw_response_assessment(root: Path | str, *, staging_dir: Path | str,
                                  expected_root: Path | str, release_sha256: str,
                                  source_identity_to_connector: dict[str, str],
                                  isolation_guard, transaction_id: str,
                                  quiescence_attestation, now: datetime) -> dict:
    """Return a digest-bound assessment receipt; intentionally writes nothing."""
    stage = Path(staging_dir).expanduser().absolute()
    if os.path.lexists(stage):
        raise RawResponseRetentionHold("raw_response_stage_path_must_be_absent")
    inventory = assess_raw_responses(root, expected_root=expected_root,
        release_sha256=release_sha256, source_identity_to_connector=source_identity_to_connector,
        isolation_guard=isolation_guard, transaction_id=transaction_id,
        quiescence_attestation=quiescence_attestation, now=now)
    return {"schema": "nami.raw_response_stage_assessment.v1", "status": "staged",
            "root": inventory["root"], "release_sha256": release_sha256,
            "transaction_id": transaction_id,
            "inventory_sha256": inventory["inventory_sha256"],
            "descendant_catalog_sha256": inventory["descendant_catalog_sha256"],
            "authorization_sha256": _sha(_canonical(inventory["authorization"])),
            "source_selection_fingerprint": inventory["authorization"]["source_selection_fingerprint"],
            "source_clearance_fingerprint": inventory["authorization"]["source_clearance_fingerprint"],
            "active_run_clearance_pin": inventory["authorization"]["active_run_clearance_pin"],
            "raw_response_count": inventory["raw_response_count"],
            "retention_ready": False, "payload_mutated": False,
            "global_commit_supported": False}


def verify_raw_response_assessment(root: Path | str, *, assessment: dict,
                                   expected_root: Path | str, release_sha256: str,
                                   source_identity_to_connector: dict[str, str],
                                   isolation_guard, transaction_id: str,
                                   quiescence_attestation, now: datetime) -> dict:
    """Reinventory and compare the immutable stage assessment binding."""
    required = {"schema", "status", "root", "release_sha256", "inventory_sha256",
                "transaction_id", "descendant_catalog_sha256", "authorization_sha256",
                "source_selection_fingerprint", "source_clearance_fingerprint",
                "active_run_clearance_pin",
                "raw_response_count", "retention_ready", "payload_mutated", "global_commit_supported"}
    if (not isinstance(assessment, dict) or set(assessment) != required
            or assessment.get("schema") != "nami.raw_response_stage_assessment.v1"
            or assessment.get("status") != "staged" or assessment.get("retention_ready") is not False
            or assessment.get("payload_mutated") is not False
            or assessment.get("global_commit_supported") is not False):
        raise RawResponseRetentionHold("raw_response_assessment_receipt_invalid")
    fresh = assess_raw_responses(root, expected_root=expected_root,
        release_sha256=release_sha256, source_identity_to_connector=source_identity_to_connector,
        isolation_guard=isolation_guard, transaction_id=transaction_id,
        quiescence_attestation=quiescence_attestation, now=now)
    if (assessment["root"] != fresh["root"] or assessment["release_sha256"] != release_sha256
            or assessment["transaction_id"] != transaction_id
            or assessment["inventory_sha256"] != fresh["inventory_sha256"]
            or assessment["descendant_catalog_sha256"] != fresh["descendant_catalog_sha256"]
            or assessment["authorization_sha256"] != _sha(_canonical(fresh["authorization"]))
            or assessment["source_selection_fingerprint"] != fresh["authorization"]["source_selection_fingerprint"]
            or assessment["source_clearance_fingerprint"] != fresh["authorization"]["source_clearance_fingerprint"]
            or assessment["active_run_clearance_pin"] != fresh["authorization"]["active_run_clearance_pin"]
            or assessment["raw_response_count"] != fresh["raw_response_count"]):
        raise RawResponseRetentionHold("raw_response_inventory_stale")
    return {"schema": "nami.raw_response_assessment_verification.v1", "verified": True,
            "inventory_sha256": fresh["inventory_sha256"], "retention_ready": False,
            "global_commit_supported": False}
