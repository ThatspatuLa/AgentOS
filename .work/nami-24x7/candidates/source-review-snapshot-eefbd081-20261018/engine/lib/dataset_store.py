"""Append raw market adapter responses and freeze immutable evaluation inputs."""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

from engine import config
from engine.lib import nami_io
from engine.lib.contracts import canonical_dataset_bytes, canonical_json, freeze_dataset
from engine.lib.collection_retention import derived_expiry_fields, expiry_fields, validate_capture_timestamp
from engine.lib.exact_root_lease import ExactRootQuiescenceLease


def _safe_source(value: str) -> str:
    name = re.sub(r"[^a-z0-9_-]+", "_", str(value).strip().lower()).strip("_")
    return name or "unknown"


def _write_immutable(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(data)
            stream.flush()
    except FileExistsError:
        if path.read_bytes() != data:
            raise ValueError(f"immutable artifact collision: {path}")


def _raw_dataset_write_gate(state_dir: Path) -> Path:
    """Shared cross-process gate used by raw and frozen dataset writers."""
    return Path(state_dir) / ".nami-raw-dataset-write-gate"


def persist_raw_response(response, *, state_dir: Path | None = None,
                         captured_at: str | None = None) -> dict:
    """Append the complete adapter envelope, including empty/error responses."""
    state_dir = Path(state_dir or config.STATE_DIR)
    captured_at = captured_at or datetime.now(timezone.utc).isoformat()
    captured_at = validate_capture_timestamp(captured_at).isoformat()
    retention = expiry_fields(captured_at)
    captured_at = retention["captured_at"]
    value = response.to_dict() if hasattr(response, "to_dict") else dict(response)
    source = str(value.get("market") or "unknown")
    identity = str(value.get("data_source") or "unknown")
    key = {"source": source, "source_identity": identity,
           "captured_at": captured_at, "response": value}
    response_id = hashlib.sha256(canonical_json(key).encode("utf-8")).hexdigest()
    record = {"response_id": response_id, "source": source,
              "source_identity": identity, **retention,
              "response": value}
    day = datetime.fromisoformat(captured_at.replace("Z", "+00:00")).astimezone(timezone.utc).date().isoformat()
    path = state_dir / "raw" / _safe_source(source) / f"{day}.jsonl"
    relative = path.relative_to(state_dir).as_posix()
    with ExactRootQuiescenceLease(state_dir) as lease:
        lease.ensure_directory(Path(relative).parent.as_posix())
        lease.assert_safe_file_or_missing(relative)
        lease.assert_safe_file_or_missing(relative + ".lock")
        with nami_io.file_lock(path):
            if not nami_io.safe_append_jsonl(path, record):
                raise OSError(f"could not append raw adapter response: {path}")
    return {"response_id": response_id, "source": source,
            "source_identity": identity, "captured_at": captured_at,
            "path": str(path)}


def persist_frozen_events(*, hypothesis_id: str, market: str, events: list[dict],
                          source_identity: str, raw_response_id: str,
                          captured_at: str, state_dir: Path | None = None,
                          parent_expiries: list[str] | None = None,
                          parent_lineage: list[dict] | None = None,
                          frozen_rule: dict | None = None,
                          windows: list[dict] | None = None,
                          evaluation_protocol: dict | None = None) -> dict:
    """Freeze a per-hypothesis snapshot; source fields remain unchanged."""
    state_dir = Path(state_dir or config.STATE_DIR)
    captured_at = validate_capture_timestamp(captured_at).isoformat()
    retention = (derived_expiry_fields(captured_at,parent_expiries)
                 if parent_expiries is not None else expiry_fields(captured_at))
    captured_at = retention["captured_at"]
    retention_lineage = list(parent_lineage) if parent_lineage is not None else [
        {"role":"raw_adapter_response","response_id":raw_response_id,
         "expires_at":retention["expires_at"]}]
    if any(not isinstance(row,dict) for row in retention_lineage):
        raise ValueError("dataset retention lineage must contain object records")
    capture = {"hypothesis_id": hypothesis_id, "source": market,
               "source_identity": source_identity, "raw_response_id": raw_response_id,
               **retention, "retention_lineage":retention_lineage,
               "decision_cutoff": captured_at}
    rows = []
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("adapter events must be objects")
        rows.append({**event, "_capture": capture})
    manifest = freeze_dataset(market=market, events=rows, decision_cutoff=captured_at)
    body = canonical_dataset_bytes(rows)
    if hashlib.sha256(body).hexdigest() != manifest["dataset_hash"]:
        raise ValueError("manifest hash does not match canonical JSONL event bytes")
    directory = state_dir / "datasets"
    path = directory / f"{manifest['dataset_id']}.jsonl"
    sidecar = directory / f"{manifest['dataset_id']}-{_safe_source(hypothesis_id)}.manifest.json"
    metadata = {"schema_version": 1, "hypothesis_id": hypothesis_id,
                "dataset_manifest": manifest, "frozen_rule": frozen_rule,
                "windows": list(windows or []), "source_identity": source_identity,
                "raw_response_id": raw_response_id, **retention,
                "retention_lineage":retention_lineage}
    if evaluation_protocol is not None:
        metadata["evaluation_protocol"] = evaluation_protocol
        metadata["evaluation_protocol_hash"] = hashlib.sha256(canonical_json(evaluation_protocol).encode()).hexdigest()
    relative = path.relative_to(state_dir).as_posix()
    manifest_relative = sidecar.relative_to(state_dir).as_posix()
    with ExactRootQuiescenceLease(state_dir) as lease:
        lease.ensure_directory("datasets")
        lease.assert_safe_file_or_missing(relative)
        lease.assert_safe_file_or_missing(relative + ".lock")
        lease.assert_safe_file_or_missing(manifest_relative)
        lease.assert_safe_file_or_missing(manifest_relative + ".lock")
        with nami_io.file_lock(path):
            _write_immutable(path, body)
            with nami_io.file_lock(sidecar):
                old = nami_io.safe_read_json(sidecar, None)
                if old is None:
                    if not nami_io.safe_write_json(sidecar, metadata):
                        raise OSError(f"could not persist dataset manifest: {sidecar}")
                elif old != metadata:
                    raise ValueError(f"immutable manifest collision: {sidecar}")
    return {"dataset_id": manifest["dataset_id"], "dataset_hash": manifest["dataset_hash"],
            "event_count": manifest["event_count"], "path": str(path),
            "manifest_path": str(sidecar), "manifest": manifest, "metadata": metadata}


def fetch_and_persist(hypothesis: dict, *, state_dir: Path | None = None,
                      captured_at: str | None = None) -> dict:
    """Fetch only through the existing market data adapter and persist its response."""
    from engine.lib import market_data
    from engine.lib.capability_registry import FAMILY
    if hypothesis.get("evaluation_family") == FAMILY:
        # Collection is a separate bounded public-data task. TEST reads only
        # captured evidence; it never invents depth from historic closing prices.
        from scripts.nami_data_capture import load_observations
        state_dir = Path(state_dir or config.STATE_DIR)
        events = load_observations(state_dir, period=(hypothesis.get("evaluation_protocol") or {}).get("period"),
                                   evaluation_ready_only=True)
        captured_at = captured_at or datetime.now(timezone.utc).isoformat()
        parent_expiries=[]
        parent_lineage=[]
        for event in events:
            parent_expiry=(event.get("_capture") or {}).get("expires_at")
            if not isinstance(parent_expiry,str):
                raise ValueError("Polymarket dataset parent retention evidence is missing")
            parent_expiries.append(parent_expiry)
            observation=(event.get("retention_lineage") or [])
            if not isinstance(observation,list) or not observation:
                raise ValueError("Polymarket observation parent lineage is missing")
            parent_lineage.append({"role":"observation","observation_id":event.get("id"),
                                  "expires_at":parent_expiry,"parents":observation})
        retention_parents=parent_expiries if events else None
        retention_lineage=parent_lineage if events else None
        return persist_frozen_events(
            hypothesis_id=str(hypothesis.get("hypothesis_id") or hypothesis.get("id") or ""),
            market="polymarket", events=events, source_identity="public-polymarket-v1",
            raw_response_id=hashlib.sha256(canonical_json([row.get("raw_evidence") for row in events]).encode()).hexdigest(),
            captured_at=captured_at, state_dir=state_dir, parent_expiries=retention_parents,
            parent_lineage=retention_lineage,
            evaluation_protocol=hypothesis.get("evaluation_protocol"),
            frozen_rule=hypothesis.get("frozen_rule"),
            windows=hypothesis.get("evaluation_windows"))
    market = str(hypothesis.get("market") or "unknown").strip().lower().replace(" ", "_")
    response = market_data.fetch_market_data(market)
    captured_at = captured_at or datetime.now(timezone.utc).isoformat()
    raw = persist_raw_response(response, state_dir=state_dir, captured_at=captured_at)
    value = response.to_dict() if hasattr(response, "to_dict") else dict(response)
    return persist_frozen_events(
        hypothesis_id=str(hypothesis.get("id") or hypothesis.get("hypothesis_id") or ""),
        market=str(value.get("market") or market), events=list(value.get("events") or []),
        source_identity=raw["source_identity"], raw_response_id=raw["response_id"],
        captured_at=captured_at, state_dir=state_dir,
        frozen_rule=hypothesis.get("frozen_rule"),
        windows=hypothesis.get("evaluation_windows"),
    ) | {"raw_response": raw}
