#!/usr/bin/env python3
"""Bounded Stage 2 repair loop with deterministic core and AI proposal lane.

The model may propose a one-file patch; only deterministic compile/contract
checks can accept it. All writes are candidate-local under the canary root.
"""
from __future__ import annotations
import argparse, hashlib, json, os, re, subprocess, sys, tempfile
import shutil
import base64, sqlite3
from datetime import datetime, timezone
from uuid import uuid4
from pathlib import Path
from kcos_runtime_paths_v2 import assert_safe_root
from kcos_stage2_adapters_v1 import family, constraints

ROOT = assert_safe_root()
REPAIR = ROOT / "state" / "kcos_stage2_repair_v2.py"
COMPILE = ROOT / "state" / "kcos_stage2_compile_v2.py"
VERIFY = ROOT / "state" / "kcos_stage2_verify_v2.py"

def sha(p: Path) -> str: return hashlib.sha256(p.read_bytes()).hexdigest().upper()
def run(cmd: list[str]) -> tuple[int,str]:
    p = subprocess.run([sys.executable, *cmd], cwd=ROOT, text=True, capture_output=True)
    return p.returncode, (p.stdout + "\n" + p.stderr).strip()[-4000:]


def decode_text_log(raw: bytes) -> str:
    """Decode MetaEditor logs without mistaking UTF-16 NULs for UTF-8 text."""
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16", errors="replace")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig", errors="replace")
    if raw.count(b"\x00") > len(raw) // 10:
        return raw.decode("utf-16le", errors="replace")
    return raw.decode("utf-8", errors="replace")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest().upper()


def write_json_atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    payload = (json.dumps(value, indent=2) + "\n").encode("utf-8")
    with temp.open("wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    temp.replace(path)


MAX_PROPOSALS = 4
_ACTIVE_EPISODE: dict | None = None


def authenticated_stage3_zero_trade_repair_anchor(handoff: dict, state: dict) -> tuple[str, str] | None:
    """Bind a Stage 2 repair episode to the same-cycle verified Stage 3 zero-trade receipt."""
    if handoff.get("stage3_zero_trade_repair") is not True:
        return None
    evidence = handoff.get("stage3_zero_trade_evidence")
    if not isinstance(evidence, dict):
        raise ValueError("Stage 2 zero-trade repair handoff lacks authenticated Stage 3 evidence")
    decision_path = _require_candidate_path(Path(evidence.get("decision") or ""))
    verification_path = _require_candidate_path(Path(evidence.get("verification") or ""))
    if not decision_path.is_file() or not verification_path.is_file():
        raise ValueError("Stage 3 zero-trade repair receipt is missing")
    decision = json.loads(decision_path.read_text(encoding="utf-8-sig"))
    verification = json.loads(verification_path.read_text(encoding="utf-8-sig"))
    decision_hash = sha(decision_path)
    if (decision_hash != str(evidence.get("decision_sha256") or "").upper()
            or sha(verification_path) != str(evidence.get("verification_sha256") or "").upper()
            or Path(state.get("stage3_zero_trade_decision") or "").resolve() != decision_path.resolve()
            or Path(state.get("stage3_zero_trade_verification") or "").resolve() != verification_path.resolve()
            or decision.get("cycle_id") != handoff.get("cycle_id")
            or decision.get("stage") != 3 or decision.get("ea") != handoff.get("ea")
            or decision.get("verdict") != "REPAIR_REQUIRED"
            or verification.get("cycle_id") != handoff.get("cycle_id")
            or verification.get("stage") != 3
            or Path(verification.get("decision") or "").resolve() != decision_path.resolve()
            or str(verification.get("decision_sha256") or "").upper() != decision_hash
            or verification.get("passed") is not True
            or not isinstance(verification.get("checks"), dict)
            or not verification["checks"]
            or any(value is not True for value in verification["checks"].values())):
        raise ValueError("Stage 3 zero-trade receipt is not authenticated for this Stage 2 repair")
    timeframes = decision.get("timeframes")
    order = ("M5", "M15", "H1")
    names = list(timeframes) if isinstance(timeframes, dict) else []
    if names != list(order[:len(names)]) or not names:
        raise ValueError("Stage 3 zero-trade decision lacks an ordered completed timeframe prefix")
    zero_trade_timeframes = []
    for timeframe, metrics in timeframes.items():
        if not isinstance(metrics, dict) or type(metrics.get("row_count")) is not int or metrics["row_count"] <= 0:
            raise ValueError("Stage 3 zero-trade decision lacks authenticated result rows")
        if (metrics.get("zero_trades") is True and metrics.get("trades") == 0
                and metrics.get("max_trades") == 0):
            zero_trade_timeframes.append(timeframe)
    trigger = decision.get("repair_trigger") or {}
    if trigger.get("kind") == "completed_timeframe_zero_trades":
        tf = trigger.get("timeframe")
        metrics = timeframes.get(tf) if tf else None
        if (tf not in names or not isinstance(metrics, dict)
                or metrics.get("zero_trades") is not True or metrics.get("trades") != 0
                or metrics.get("max_trades") != 0):
            raise ValueError("Stage 3 early repair trigger does not bind a completed zero-trade timeframe")
    elif trigger.get("kind") == "all_timeframes_below_minimum_trade_count":
        if (set(names) != set(order)
                or any(type(metrics.get("max_trades")) is not int or metrics["max_trades"] >= 10
                       for metrics in timeframes.values())):
            raise ValueError("Stage 3 all-timeframe repair lacks completed low-trade evidence")
    elif trigger:
        raise ValueError("Stage 3 repair receipt has an unknown repair trigger")
    elif set(names) != set(order) or set(zero_trade_timeframes) != set(order):
        raise ValueError("legacy Stage 3 repair receipt lacks full all-zero-trade evidence")
    # The verifier binds the decision hash, parse/CSV/OPT hashes, metrics, and
    # Stage 1/2 lineage. Its file hash is therefore a durable repair-episode anchor.
    return sha(verification_path), str(verification_path.resolve())


def canonical_stage2_failure_digest(handoff: dict, source: Path, *, state_path: Path | None = None) -> tuple[str, str]:
    """Use the existing cycle-state failure-event pointer as the trust anchor."""
    state_path = state_path or (ROOT / "state" / "kcos_cycle_state_v2.json")
    state = json.loads(state_path.read_text(encoding="utf-8-sig"))
    held_repair = (state.get("current_stage") == "stage2_pending"
                   and state.get("stage_status") == "stage2_repair_required")
    active_preflight = (state.get("current_stage") == "stage2_running"
                        and state.get("stage_status") == "stage2_preflight")
    if active_preflight:
        # prepare() claims the same-cycle Stage 2 lease and advances the state
        # to stage2_preflight before this repair loop authenticates the failure.
        # Accept that transition only while the durable lease still names this
        # cycle; an orphaned preflight state cannot authorize a repair episode.
        lease_path = state_path.parent / "leases" / "stage2_v2.lock.json"
        try:
            lease = json.loads(lease_path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError, json.JSONDecodeError):
            lease = {}
        active_preflight = lease.get("cycle_id") == handoff.get("cycle_id")
    if state.get("cycle_id") != handoff.get("cycle_id"):
        raise ValueError("cycle state does not authorize a Stage 2 repair episode")
    zero_trade_anchor = authenticated_stage3_zero_trade_repair_anchor(handoff, state)
    held_zero_trade_retry = (
        state.get("current_stage") == "stage3_pending"
        and state.get("stage_status") == "stage2_complete_held"
        and state.get("hold_after_stage") == 2
        and state.get("next_action") == "audit_stage2_before_stage3"
        and zero_trade_anchor is not None
    )
    if not (held_repair or active_preflight or held_zero_trade_retry):
        raise ValueError("cycle state does not authorize a Stage 2 repair episode")
    if zero_trade_anchor is not None:
        return zero_trade_anchor
    event_value = state.get("stage2_failure_event") or state.get("stage2_invalid_event")
    if not event_value:
        raise ValueError("cycle state has no canonical Stage 2 failure event")
    event_path = _require_candidate_path(Path(event_value))
    if not event_path.is_file():
        raise ValueError("canonical Stage 2 failure event is missing")
    raw_event = event_path.read_bytes()
    event = json.loads(raw_event.decode("utf-8-sig"))
    if (event.get("cycle_id") != handoff.get("cycle_id") or event.get("stage") != 2
            or event.get("status") != "failed"
            or not (event.get("failed_checks") or event.get("failure_kind") or event.get("error_sha256"))):
        raise ValueError("canonical failure event does not identify a Stage 2 failure")
    if str(event.get("ea") or "") != str(handoff.get("ea") or ""):
        raise ValueError("canonical failure event candidate does not match the Stage 2 handoff")
    spec_value = handoff.get("stage1_spec")
    if not spec_value:
        raise ValueError("Stage 2 handoff lacks its authenticated Stage 1 spec path")
    spec_path = _require_candidate_path(Path(spec_value))
    if not spec_path.is_file():
        raise ValueError("authenticated Stage 1 spec is missing")
    spec_hash = sha(spec_path)
    event_spec = event.get("stage1_spec")
    event_spec_hash = str(event.get("stage1_spec_sha256") or "").upper()
    event_has_spec_binding = bool(event_spec or event_spec_hash)
    if (str(handoff.get("stage1_spec_sha256") or "").upper() != spec_hash
            or (event_has_spec_binding and (not event_spec or event_spec_hash != spec_hash
                                            or Path(event_spec).resolve() != spec_path.resolve()))):
        raise ValueError("canonical failure event does not authenticate the Stage 1 spec lineage")
    # When the event points at a verifier report, bind that report to the same
    # candidate baseline. Other authenticated executor failures can be backed
    # by the canonical event's compile/failure fields without a verifier report.
    verification_value = event.get("verification")
    if not event_has_spec_binding and not verification_value:
        raise ValueError("canonical failure event has no Stage 1 spec lineage or verifier fallback")
    if verification_value:
        report_path = _require_candidate_path(Path(verification_value))
        if not report_path.is_file():
            raise ValueError("canonical failure event names a missing verifier report")
        report = json.loads(report_path.read_text(encoding="utf-8-sig"))
        if (report.get("cycle_id") != handoff.get("cycle_id")
                or report.get("stage") != 2
                or str(report.get("source_sha256") or "").upper() != sha(source)
                or str(report.get("source") or "") and Path(report["source"]).resolve() != source.resolve()
                or str(report.get("stage1_spec_sha256") or "").upper() != spec_hash
                or str(report.get("stage1_spec") or "") == ""
                or Path(report["stage1_spec"]).resolve() != spec_path.resolve()):
            raise ValueError("canonical failure event verifier report does not match baseline candidate and Stage 1 spec")
        checks = report.get("checks") if isinstance(report.get("checks"), dict) else {}
        event_failed = event.get("failed_checks") or []
        if event_failed and (not any(checks.get(name) is False for name in event_failed)
                             or any(checks.get(name) is True for name in event_failed)):
            raise ValueError("canonical failure event failed checks do not match verifier evidence")
    else:
        if (not event.get("source_sha256")
                or str(event.get("source_sha256")).upper() != sha(source)
                or (event.get("source") and Path(event["source"]).resolve() != source.resolve())):
            raise ValueError("canonical failure event does not authenticate the failed candidate source")
    digest = sha256_bytes(raw_event)
    claimed = str(handoff.get("authenticated_failure_digest") or handoff.get("stage2_failure_digest") or "").upper()
    if claimed and (not re.fullmatch(r"[A-F0-9]{64}", claimed) or claimed != digest):
        raise ValueError("authenticated_failure_digest does not match the canonical failure event")
    return digest, str(event_path)


def bind_stage3_runtime_repair_from_state(handoff: dict, *, state_path: Path | None = None) -> bool:
    """Attach the current cycle's hash-bound Stage 3 source-defect ticket to Stage 2."""
    state_path = state_path or (ROOT / "state" / "kcos_cycle_state_v2.json")
    state = json.loads(state_path.read_text(encoding="utf-8-sig"))
    ticket_value = state.get("stage2_source_defect_ticket")
    if not ticket_value:
        return False
    ticket_path = _require_candidate_path(Path(ticket_value))
    if not ticket_path.is_file():
        raise ValueError("current Stage 3 source-defect ticket is missing")
    ticket_hash = sha(ticket_path)
    ticket = json.loads(ticket_path.read_text(encoding="utf-8-sig"))
    if (state.get("cycle_id") != handoff.get("cycle_id")
            or state.get("current_ea") != handoff.get("ea")
            or ticket_hash != str(state.get("stage2_source_defect_ticket_sha256") or "").upper()
            or ticket.get("cycle_id") != handoff.get("cycle_id")
            or ticket.get("ea") != handoff.get("ea")
            or Path(ticket.get("candidate_source") or "").resolve()
            != Path(handoff.get("source") or "").resolve()):
        raise ValueError("current Stage 3 source-defect ticket does not bind this Stage 2 handoff")
    artifacts = ticket.get("artifacts") or {}
    runtime = artifacts.get("stage3_runtime")
    diagnostic = ticket.get("stage3_journal_diagnostic")
    if not isinstance(runtime, dict) or not isinstance(diagnostic, dict):
        raise ValueError("current Stage 3 source-defect ticket lacks runtime diagnostics")
    handoff["stage3_runtime_repair"] = {
        "kind": ticket.get("repair_kind"),
        "ticket": str(ticket_path.resolve()),
        "ticket_sha256": ticket_hash,
        "parent_source_sha256": ticket.get("parent_source_sha256"),
        "runtime": runtime,
        "diagnostic": diagnostic,
    }
    return True


def episode_identity(handoff: dict, source: Path, verification_path: Path,
                     explicit: str | None = None, *, state_path: Path | None = None) -> dict:
    """Bind retries to the canonical failure event, baseline source, and inputs."""
    source_hash = sha(source)
    runtime_repair = handoff.get("stage3_runtime_repair")
    if isinstance(runtime_repair, dict):
        ticket_path = Path(runtime_repair.get("ticket") or "")
        ticket_hash = str(runtime_repair.get("ticket_sha256") or "").upper()
        runtime_record = runtime_repair.get("runtime") or {}
        diagnostic = runtime_repair.get("diagnostic") or {}
        ticket = json.loads(ticket_path.read_text(encoding="utf-8-sig")) if ticket_path.is_file() else {}
        runtime_path = Path(runtime_record.get("path") or "")
        ticket_core = {key: value for key, value in ticket.items()
                       if key not in {"ticket_id", "ticket_sha256", "created_utc"}}
        computed_ticket_hash = hashlib.sha256(json.dumps(
            ticket_core, sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
        artifacts = ticket.get("artifacts") if isinstance(ticket.get("artifacts"), dict) else {}
        bound_runtime = artifacts.get("stage3_runtime") if isinstance(artifacts, dict) else None
        source_record = artifacts.get("stage2_source") if isinstance(artifacts, dict) else None
        stage3_failure_record = artifacts.get("stage3_failure") if isinstance(artifacts, dict) else None
        stage3_handoff_record = artifacts.get("stage3_handoff") if isinstance(artifacts, dict) else None
        runtime_doc = json.loads(runtime_path.read_text(encoding="utf-8-sig")) if runtime_path.is_file() else {}
        generic_array_bounds = runtime_repair.get("kind") in {
            "stage3_tester_journal_multidimensional_array_row_bounds",
            "stage3_tester_journal_quantile_interpolation_bounds",
            "stage3_tester_journal_ea_runtime_array_bounds",
        }
        generic_matching = diagnostic.get("matching_error_lines") or []
        if (not ticket_path.is_file() or sha(ticket_path) != ticket_hash
                or computed_ticket_hash != ticket.get("ticket_sha256")
                or ticket.get("cycle_id") != handoff.get("cycle_id")
                or ticket.get("ea") != handoff.get("ea")
                or Path(ticket.get("candidate_source") or "").resolve() != source.resolve()
                or not isinstance(source_record, dict) or source_record.get("sha256") != source_hash
                or not isinstance(stage3_failure_record, dict)
                or not Path(stage3_failure_record.get("path") or "").is_file()
                or not isinstance(stage3_handoff_record, dict)
                or runtime_doc.get("cycle_id") != handoff.get("cycle_id")
                or runtime_doc.get("stage") != 3 or runtime_doc.get("ea") != handoff.get("ea")
                or runtime_doc.get("status") != "INFRASTRUCTURE_FAILURE"
                or runtime_doc.get("failure_kind") != "TESTER_JOURNAL_REJECTED"
                or runtime_doc.get("journal_diagnostic", {}).get("code") not in {
                    "TESTER_EA_ORDER_REJECTED", "TESTER_JOURNAL_ERROR", "NO_EXPERT_IN_START_CONFIG"}
                or not set(diagnostic.get("matching_error_lines") or []).issubset(
                    set(runtime_doc.get("journal_diagnostic", {}).get("error_lines") or []))
                or runtime_repair.get("kind") not in {
                    "stage3_tester_journal_invalid_stops",
                    "stage3_tester_journal_multidimensional_array_row_bounds",
                    "stage3_tester_journal_quantile_interpolation_bounds",
                    "stage3_tester_journal_ea_runtime_array_bounds"}
                or ticket.get("repair_kind") != runtime_repair.get("kind")
                or diagnostic != ticket.get("stage3_journal_diagnostic")
                or (generic_array_bounds and (
                    diagnostic.get("code") not in {"TESTER_JOURNAL_ERROR", "NO_EXPERT_IN_START_CONFIG"}
                    or len(generic_matching) < 2
                    or any("runtime error 502" not in str(line).casefold()
                           or "array out of range" not in str(line).casefold()
                           for line in generic_matching)
                    or str(ticket.get("parent_source_sha256") or "").upper()
                    != str((ticket.get("artifacts", {}).get("stage2_original_source") or {}).get("sha256") or "").upper()
                ))
                or (not generic_array_bounds and (
                    diagnostic.get("code") != "TESTER_EA_ORDER_REJECTED"
                    or len(diagnostic.get("matching_error_lines") or []) < 2
                ))
                or not isinstance(bound_runtime, dict) or runtime_record != bound_runtime
                or not runtime_path.is_file() or sha(runtime_path) != runtime_record.get("sha256")):
            raise ValueError("Stage 3 runtime-repair evidence is incomplete, mismatched, or changed")
        failure_hash, failure_event_path = ticket_hash, str(ticket_path.resolve())
    else:
        failure_hash, failure_event_path = canonical_stage2_failure_digest(handoff, source, state_path=state_path)
    sig = locked_strategy_surface(source.read_text(encoding="utf-8-sig", errors="replace"))
    effective_inputs = handoff.get("effective_input_signature") or handoff.get("effective_inputs") or handoff.get("tester_config") or {}
    sig = hashlib.sha256(json.dumps({"source_surface": sig, "effective_inputs": effective_inputs,
                                    "stage1_spec_sha256": handoff.get("stage1_spec_sha256")},
                                   sort_keys=True, default=str).encode("utf-8")).hexdigest().upper()
    identity = {"cycle_id": str(handoff.get("cycle_id") or ""),
                "candidate": str(handoff.get("ea") or source.stem),
                "failure_digest": failure_hash, "baseline_source_sha256": source_hash,
                "failure_event_path": failure_event_path,
                "effective_input_signature": sig}
    if not identity["cycle_id"]:
        raise ValueError("handoff cycle_id is required")
    derived_episode_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest().upper()
    if explicit and str(explicit).upper() != derived_episode_id:
        raise ValueError("explicit episode_id does not match the canonical content identity")
    identity["episode_id"] = derived_episode_id
    return reuse_existing_episode_identity(identity)


def reuse_existing_episode_identity(identity: dict, *, ledger_path: Path | None = None) -> dict:
    """Keep a retry on its original persisted episode when code-derived fingerprints evolve."""
    ledger_path = ledger_path or ROOT / "state" / "ledgers" / "stage2_repair_episodes.sqlite3"
    if not ledger_path.is_file():
        return identity
    stable_fields = ("cycle_id", "candidate", "failure_digest", "baseline_source_sha256", "failure_event_path")
    try:
        connection = sqlite3.connect("file:" + ledger_path.resolve().as_posix() + "?mode=ro",
                                     uri=True, timeout=1.0)
        try:
            rows = connection.execute(
                "SELECT episode_id, identity_json, proposal_count, max_proposals FROM episodes").fetchall()
        finally:
            connection.close()
    except sqlite3.Error as exc:
        raise ValueError("cannot read persistent Stage 2 proposal ledger") from exc
    matches = []
    for episode_id, identity_json, proposal_count, max_proposals in rows:
        try:
            recorded = json.loads(identity_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("persistent Stage 2 proposal ledger contains malformed identity") from exc
        if all(recorded.get(field) == identity.get(field) for field in stable_fields):
            if (recorded.get("episode_id") != episode_id
                    or type(proposal_count) is not int or type(max_proposals) is not int
                    or not 1 <= max_proposals <= MAX_PROPOSALS
                    or not 0 <= proposal_count <= max_proposals):
                raise ValueError("persistent Stage 2 proposal ledger identity or budget is inconsistent")
            matches.append(recorded)
    if len(matches) > 1:
        raise ValueError("multiple persisted Stage 2 proposal episodes match this same ticket and source")
    return matches[0] if matches else identity


def admitted_bundle_paths(handoff_path: Path, handoff: dict, source: Path,
                          compile_path: Path, verification_path: Path) -> list[Path]:
    paths = [handoff_path, source, compile_path, verification_path, source.with_suffix(".ex5")]
    for raw in (handoff.get("source_build_receipt"),
                (handoff.get("source_implementation_review") or {}).get("path")):
        if raw:
            paths.append(Path(raw))
    # Compile receipts vary by compiler adapter; snapshot any named binary.
    try:
        receipt = json.loads(compile_path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, TypeError):
        receipt = {}
    for key in ("binary", "binary_path", "ex5", "ex5_path", "output_binary"):
        if receipt.get(key):
            paths.append(Path(receipt[key]))
    for key in ("binary", "binary_path", "ex5", "ex5_path", "compiled_binary"):
        if handoff.get(key):
            paths.append(Path(handoff[key]))
    return list(dict.fromkeys(_require_candidate_path(p) for p in paths))


def snapshot_bundle(paths: list[Path]) -> dict:
    return {str(path): (path.read_bytes() if path.is_file() else None) for path in paths}


def restore_bundle(snapshot: dict) -> None:
    for raw, content in snapshot.items():
        path = Path(raw)
        if content is None:
            path.unlink(missing_ok=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)


def _require_candidate_path(path: Path) -> Path:
    resolved = path.resolve()
    if not resolved.is_relative_to(ROOT.resolve()):
        raise ValueError(f"transaction artifact escapes candidate root: {resolved}")
    return resolved


def write_transaction_journal(path: Path, snapshot: dict, status: str = "PREPARED") -> None:
    journal = {"schema_version": 1, "status": status, "updated_at_utc": datetime.now(timezone.utc).isoformat(),
               "snapshot": {raw: None if data is None else base64.b64encode(data).decode("ascii")
                           for raw, data in snapshot.items()}}
    write_json_atomic(path, journal)


def track_transaction_artifact(path: Path) -> None:
    """Durably add generated proof/build files to rollback before creating them."""
    if _ACTIVE_EPISODE is None:
        return
    resolved = _require_candidate_path(path)
    snapshot = _ACTIVE_EPISODE["snapshot"]
    key = str(resolved)
    if key not in snapshot:
        snapshot[key] = resolved.read_bytes() if resolved.is_file() else None
        write_transaction_journal(_ACTIVE_EPISODE["journal_path"], snapshot, "PREPARED")


def recover_pending_transactions() -> list[str]:
    """Idempotently roll back transactions whose durable journal lacks COMMITTED."""
    root = ROOT / "state" / "diagnostics" / "stage2_repair_transactions"
    recovered = []
    if not root.is_dir():
        return recovered
    for journal_path in root.glob("*/*/transaction-journal.json"):
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
        if journal.get("status") in {"COMMITTED", "ROLLED_BACK"}:
            continue
        encoded_snapshot = journal.get("snapshot")
        if not isinstance(encoded_snapshot, dict):
            raise RuntimeError(f"transaction journal has no recoverable snapshot: {journal_path}")
        snapshot = {}
        for raw, encoded in encoded_snapshot.items():
            path = _require_candidate_path(Path(raw))
            snapshot[str(path)] = None if encoded is None else base64.b64decode(encoded, validate=True)
            if path.is_file():
                output_dir = journal_path.parent / "recovered-output"
                output_dir.mkdir(parents=True, exist_ok=True)
                archived = output_dir / (hashlib.sha256(str(path).encode("utf-8")).hexdigest()[:12] + "-" + path.name)
                if not archived.exists():
                    with archived.open("xb") as stream:
                        stream.write(path.read_bytes())
                        stream.flush()
                        os.fsync(stream.fileno())
        restore_bundle(snapshot)
        write_transaction_journal(journal_path, snapshot, "ROLLED_BACK")
        recovered.append(str(journal_path))
    return recovered


def reserve_proposal(ledger_path: Path, episode: dict) -> int | None:
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(ledger_path, timeout=30, isolation_level=None)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("CREATE TABLE IF NOT EXISTS episodes (episode_id TEXT PRIMARY KEY, identity_json TEXT NOT NULL, proposal_count INTEGER NOT NULL, max_proposals INTEGER NOT NULL, updated_at_utc TEXT NOT NULL)")
        connection.execute("BEGIN IMMEDIATE")
        key = episode["episode_id"]
        encoded = json.dumps(episode, sort_keys=True, separators=(",", ":"))
        row = connection.execute("SELECT identity_json, proposal_count, max_proposals FROM episodes WHERE episode_id=?", (key,)).fetchone()
        if row is None:
            connection.execute("INSERT INTO episodes VALUES (?, ?, 0, ?, ?)",
                               (key, encoded, MAX_PROPOSALS, datetime.now(timezone.utc).isoformat()))
            count, maximum = 0, MAX_PROPOSALS
        else:
            if row[0] != encoded:
                raise ValueError("episode identity collision")
            count, maximum = int(row[1]), int(row[2])
        if count >= min(MAX_PROPOSALS, maximum):
            connection.execute("COMMIT")
            return None
        count += 1
        connection.execute("UPDATE episodes SET proposal_count=?, updated_at_utc=? WHERE episode_id=?",
                           (count, datetime.now(timezone.utc).isoformat(), key))
        connection.execute("COMMIT")
        return count
    except Exception:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()


def episode_proposal_count(ledger_path: Path, episode_id: str) -> int:
    try:
        with sqlite3.connect(ledger_path, timeout=30) as connection:
            row = connection.execute("SELECT proposal_count FROM episodes WHERE episode_id=?", (episode_id,)).fetchone()
            return int(row[0]) if row else 0
    except (OSError, sqlite3.Error, ValueError, TypeError):
        return 0


def verification_passes_for_candidate(report: dict, cycle_id: str, source: Path) -> bool:
    checks = report.get("checks")
    return (report.get("passed") is True and report.get("cycle_id") == cycle_id
            and str(report.get("source_sha256") or "").upper() == sha(source)
            and isinstance(checks, dict) and bool(checks) and all(value is True for value in checks.values()))


def _archive_attempt(attempt_dir: Path, snapshot: dict, paths: list[Path],
                     compile_path: Path, verification_path: Path) -> None:
    attempt_dir.mkdir(parents=True, exist_ok=False)
    bundle = attempt_dir / "baseline"
    bundle.mkdir()
    for raw, data in snapshot.items():
        if data is None:
            continue
        name = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12] + "-" + Path(raw).name
        (bundle / name).write_bytes(data)
    for path in (compile_path, verification_path):
        if path.is_file():
            shutil.copy2(path, attempt_dir / ("evidence-" + path.name))


def archive_transaction_outputs(attempt_dir: Path, snapshot: dict) -> None:
    output_dir = attempt_dir / "candidate-output"
    output_dir.mkdir(exist_ok=True)
    for raw in snapshot:
        path = Path(raw)
        if path.is_file():
            name = hashlib.sha256(str(path).encode("utf-8")).hexdigest()[:12] + "-" + path.name
            target = output_dir / name
            if not target.exists():
                shutil.copy2(path, target)


def compiler_log_evidence(compile_receipt: Path) -> str:
    """Read bounded MetaEditor diagnostics from the compile receipt."""
    try:
        receipt = json.loads(compile_receipt.read_text(encoding="utf-8-sig"))
        log_path = Path(receipt["log"])
        if not log_path.is_file():
            return "COMPILER_LOG_MISSING"
        log = decode_text_log(log_path.read_bytes())
        diagnostic_lines = [line for line in log.splitlines()
                            if re.search(r"(?i)\b(?:error|warning)\s+\d+\s*:", line)]
        return "\n".join(diagnostic_lines[-80:]) if diagnostic_lines else log[-4000:]
    except (OSError, ValueError, KeyError, TypeError):
        return "COMPILER_RECEIPT_UNAVAILABLE"


def source_repair_context(source: str, diagnostics: str, *, source_name: str | None = None,
                          max_chars: int = 26000,
                          execution_contract: dict | None = None) -> str:
    """Supply bounded, complete runtime dependencies or compiler-error context."""
    lines = source.splitlines()
    error_lines = [int(value) for value in re.findall(r"\((\d+),\d+\)\s*:\s*error\s+\d+\s*:", diagnostics)]
    sections = []
    if source_name:
        sections.append(f"EA SOURCE FILE BASENAME: {Path(source_name).name}")
    if error_lines:
        windows = []
        for line_number in error_lines:
            start_line, end_line = max(1, line_number - 10), min(len(lines), line_number + 10)
            if windows and start_line <= windows[-1][1] + 1:
                windows[-1] = (windows[-1][0], max(windows[-1][1], end_line))
            else:
                windows.append((start_line, end_line))
        for start_line, end_line in windows:
            sections.append(f"SOURCE LINES {start_line}-{end_line}:\n" + "\n".join(
                f"{number}: {lines[number - 1]}" for number in range(start_line, end_line + 1)))
        inputs = [f"{number}: {line}" for number, line in enumerate(lines, 1)
                  if re.match(r"\s*input\b", line) and ";" in line]
        if inputs:
            sections.append("LOCKED INPUT DECLARATIONS:\n" + "\n".join(inputs))
        for enum in re.findall(r"(?is)\benum\s+[A-Za-z_]\w*\s*\{.*?\}\s*;", source):
            sections.append("LOCKED ENUM DECLARATION:\n" + enum)
    else:
        function_pattern = re.compile(
            r"(?m)^[ \t]*(?:(?:static|inline|virtual|const)\s+)*"
            r"(?:void|bool|int|long|ulong|uint|double|float|string|datetime|color|uchar|ushort|char|"
            r"ENUM_[A-Za-z_]\w*|C[A-Za-z_]\w*)\s+([A-Za-z_]\w*)\s*\([^;{}]*\)\s*\{"
        )
        functions = {}
        for match in function_pattern.finditer(source):
            opening = source.find("{", match.start())
            depth = 0
            state = "code"
            quote = ""
            escaped = False
            index = opening
            while index < len(source):
                char = source[index]
                next_char = source[index + 1] if index + 1 < len(source) else ""
                if state == "line_comment":
                    if char == "\n": state = "code"
                elif state == "block_comment":
                    if char == "*" and next_char == "/":
                        state = "code"; index += 1
                elif state == "string":
                    if escaped: escaped = False
                    elif char == "\\": escaped = True
                    elif char == quote: state = "code"
                elif char == "/" and next_char == "/":
                    state = "line_comment"; index += 1
                elif char == "/" and next_char == "*":
                    state = "block_comment"; index += 1
                elif char in ('"', "'"):
                    state = "string"; quote = char; escaped = False
                elif char == "{":
                    depth += 1
                elif char == "}":
                    depth -= 1
                    if depth == 0:
                        start_line = source.count("\n", 0, match.start()) + 1
                        end_line = source.count("\n", 0, index) + 1
                        functions.setdefault(match.group(1), []).append(
                            (match.start(), index + 1, start_line, end_line))
                        break
                index += 1

        roots = [name for name in ("OnInit", "OnTick", "OnTimer", "OnTradeTransaction", "OnTester")
                 if name in functions]
        if not roots:
            raise ValueError("runtime repair context has no recognized EA callback")
        selected = set()
        pending = list(roots)
        while pending:
            name = pending.pop()
            if name in selected or name not in functions:
                continue
            selected.add(name)
            for begin, finish, _start_line, _end_line in functions[name]:
                body = source[begin:finish]
                for called in re.findall(r"\b([A-Za-z_]\w*)\s*\(", body):
                    if called in functions and called not in selected:
                        pending.append(called)
        first_function_line = min(item[2] for bodies in functions.values() for item in bodies)
        preamble = [(number, line) for number, line in enumerate(lines, 1)
                    if number < first_function_line]
        if preamble:
            sections.append("EA PREAMBLE (includes, globals, types, and inputs):\n" + "\n".join(
                f"{number}: {line}" for number, line in preamble))
        for name, bodies in functions.items():
            if name not in selected:
                continue
            for begin, finish, start_line, end_line in bodies:
                sections.append(f"COMPLETE RUNTIME FUNCTION {name} (source lines {start_line}-{end_line}):\n"
                                + source[begin:finish])
        sections.append("RUNTIME CONTEXT METHOD: transitive call closure from EA callbacks")
    if diagnostics:
        bounded = diagnostics if len(diagnostics) <= 8000 else diagnostics[:4000] + "\n[diagnostic middle omitted]\n" + diagnostics[-4000:]
        sections.insert(1 if source_name else 0, "COMPILER OR RUNTIME EVIDENCE:\n" + bounded)
    if execution_contract:
        sections.append("AUTHENTICATED STAGE 1 EXECUTION CONTRACT (controlling repair authority):\n"
                        + json.dumps(execution_contract, sort_keys=True, ensure_ascii=False, separators=(",", ":")))
    result = "\n\n".join(sections)
    if len(result) > max_chars:
        raise ValueError(f"complete runtime dependency context exceeds bounded prompt size ({len(result)}>{max_chars})")
    return result


def mt5_journal_evidence() -> str:
    """Return bounded current MT5 journal evidence for technical repair only."""
    roots = [ROOT / "state" / "mt5_stage45_portable" / "logs",
             ROOT / "state" / "mt5_stage45_portable" / "Tester" / "logs"]
    files = [p for root in roots if root.is_dir() for p in root.glob("*.log") if p.is_file()]
    if not files:
        return "NO_MT5_JOURNAL_FILE_FOUND"
    path = max(files, key=lambda p: p.stat().st_mtime)
    raw = path.read_bytes()[-24000:]
    text = max((raw.decode("utf-16le", errors="ignore"),
                raw.decode("utf-8-sig", errors="ignore")), key=len)
    return f"JOURNAL={path.resolve()}\n{text[-6000:]}"


PERFORMANCE_MARKERS = ("drawdown", "profit", "fitness", "trades", "tpm", "cf_", "performance", "score")
TECHNICAL_MARKERS = ("compile", "journal", "tester", "terminal", "ini", "source", "missing", "error", "warning", "identity", "regime", "time_stop", "timing_control")


def repair_eligibility(failed: list[str], diagnostics: str) -> tuple[bool, str]:
    blob = (" ".join(failed) + " " + diagnostics).casefold()
    if any(marker in blob for marker in PERFORMANCE_MARKERS) and not any(marker in blob for marker in ("compile", "journal", "tester", "terminal")):
        return False, "performance_or_drawdown_failure_not_repairable"
    if not any(marker in blob for marker in TECHNICAL_MARKERS):
        return False, "no_technical_failure_evidence"
    return True, "technical_failure_with_journal_or_compile_evidence"
def safe_patch(work: Path, patch_text: str, source: Path) -> dict:
    lines = patch_text.splitlines()
    headers = [x[4:].strip() for x in lines if x.startswith(("--- ", "+++ "))]
    if len(headers) < 2 or any(".." in Path(x.removeprefix("a/").removeprefix("b/")).parts for x in headers):
        return {"status":"PATCH_REJECTED","reason":"invalid_headers"}
    if len(set(x.removeprefix("a/").removeprefix("b/") for x in headers)) != 1 or not any(re.match(r"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@",x) for x in lines):
        return {"status":"PATCH_REJECTED","reason":"scope_or_hunk"}
    target = headers[-1].removeprefix("b/")
    if Path(target).name != source.name: return {"status":"PATCH_REJECTED","reason":"wrong_file"}
    # AI diffs may use repo-relative or absolute source paths. The checks
    # above already require exactly one matching source basename and reject
    # traversal. Rewrite only the two file headers to the authenticated
    # candidate name, then apply from its parent directory. Hunk content is
    # still dry-run checked against the current source before any write.
    normalized_lines = list(lines)
    header_indexes = [i for i, line in enumerate(normalized_lines)
                      if line.startswith(("--- ", "+++ "))]
    normalized_lines[header_indexes[0]] = f"--- a/{source.name}"
    normalized_lines[header_indexes[1]] = f"+++ b/{source.name}"
    fd, name = tempfile.mkstemp(suffix=".patch", text=True)
    os.close(fd); pp=Path(name); pp.write_text("\n".join(normalized_lines) + "\n", encoding="utf-8")
    try:
        dry=subprocess.run(["patch","-p1","--dry-run","--forward","-i",str(pp)],cwd=work,capture_output=True,text=True)
        if dry.returncode: return {"status":"PATCH_REJECTED","reason":(dry.stdout+"\n"+dry.stderr)[-1000:]}
        applied=subprocess.run(["patch","-p1","--forward","-i",str(pp)],cwd=work,capture_output=True,text=True)
        return {"status":"APPLIED" if applied.returncode==0 else "PATCH_APPLY_FAILED","output":(applied.stdout+"\n"+applied.stderr)[-1000:]}
    finally: pp.unlink(missing_ok=True)


def _function_body_bounds(source_text: str, name: str) -> tuple[int, int] | None:
    match = re.search(rf"\bbool\s+{re.escape(name)}\s*\([^{{}};]*\)\s*\{{", source_text)
    if not match or len(re.findall(rf"\bbool\s+{re.escape(name)}\s*\(", source_text)) != 1:
        return None
    opening = source_text.find("{", match.start())
    depth = 0
    for offset in range(opening, len(source_text)):
        if source_text[offset] == "{":
            depth += 1
        elif source_text[offset] == "}":
            depth -= 1
            if depth == 0:
                return opening, offset + 1
    return None


def apply_dcc_closed_return_bounds_fix(source_path: Path) -> dict:
    """Correct only the captured v14 n-1 bounds pattern to reviewed n-2 bounds."""
    from kcos_stage2_verify_v2 import dcc_closed_return_arrays_are_bounded

    original = source_path.read_bytes()
    text = original.decode("utf-8-sig", errors="replace")
    if dcc_closed_return_arrays_are_bounded(text, required_dcc=True):
        return {"status": "ALREADY_BOUNDED", "changed": False,
                "source_sha256": sha256_bytes(original)}
    span = _function_body_bounds(text, "ComputeDCCSignals")
    if span is None:
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "expected exactly one recognizable ComputeDCCSignals function"}
    start, end = span
    body = text[start:end]
    required_semantics = (
        (r"MathLog\(g_eth_close\[k\s*\+\s*1\]\)\s*-\s*MathLog\(g_eth_close\[k\s*\+\s*2\]\)", 1),
        (r"MathLog\(g_btc_close\[k\s*\+\s*1\]\)\s*-\s*MathLog\(g_btc_close\[k\s*\+\s*2\]\)", 1),
    )
    if any(len(re.findall(pattern, body)) != count for pattern, count in required_semantics):
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "closed-bar return construction differs from authenticated shift-1/shift-2 v14 shape"}
    replacements = (
        ("ArrayResize(r_eth, n - 1);", "ArrayResize(r_eth, n - 2);", 1),
        ("ArrayResize(r_btc, n - 1);", "ArrayResize(r_btc, n - 2);", 1),
        ("for(int k = 0; k < n - 1; k++)", "for(int k = 0; k < n - 2; k++)", 2),
        ("if(seed_n > n - 1) seed_n = n - 1;", "if(seed_n > n - 2) seed_n = n - 2;", 1),
        ("ArrayResize(h_eth_hist, n - 1);", "ArrayResize(h_eth_hist, n - 2);", 1),
        ("ArrayResize(h_btc_hist, n - 1);", "ArrayResize(h_btc_hist, n - 2);", 1),
        ("int qbar_n = MathMin(InpDccLookback, n - 1);", "int qbar_n = MathMin(InpDccLookback, n - 2);", 1),
        ("for(int k = qbar_n; k < n - 1; k++)", "for(int k = qbar_n; k < n - 2; k++)", 1),
        ("if(src < n - 1)", "if(src < n - 2)", 1),
        ("int vol_start = (n - 1) - InpRegimeVolLookup;", "int vol_start = (n - 2) - InpRegimeVolLookup;", 1),
        ("if(src >= n - 1) src = n - 2;", "if(src >= n - 2) src = n - 3;", 1),
    )
    for old, _new, expected in replacements:
        if body.count(old) != expected:
            return {"status": "PATCH_REJECTED", "changed": False,
                    "reason": f"expected {expected} exact `{old}` occurrences in ComputeDCCSignals; found {body.count(old)}"}
    updated_body = body
    for old, new, expected in replacements:
        updated_body = updated_body.replace(old, new, expected)
    updated_text = text[:start] + updated_body + text[end:]
    if locked_strategy_surface(text) != locked_strategy_surface(updated_text):
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "DCC bounds repair changed locked inputs, regimes, or score callback"}
    if not dcc_closed_return_arrays_are_bounded(updated_text, required_dcc=True):
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "reviewed closed-return gate rejected transformed source"}
    # Preserve the source encoding marker, if present; the transformation itself
    # is ASCII-only and confined to the authenticated DCC function body.
    prefix = b"\xef\xbb\xbf" if original.startswith(b"\xef\xbb\xbf") else b""
    updated = prefix + updated_text.encode("utf-8")
    source_path.write_bytes(updated)
    return {"status": "APPLIED", "method": "deterministic_v14_dcc_n_minus_2",
            "changed": True, "before_sha256": sha256_bytes(original),
            "after_sha256": sha256_bytes(updated), "closed_return_gate": True,
            "replacements": {old: count for old, _new, count in replacements}}


def apply_multidimensional_array_row_bounds_fix(source_path: Path) -> dict:
    """Replace flattened ArraySize row bounds only for declared 2D arrays."""
    from kcos_stage2_verify_v2 import executable_source, multidimensional_array_row_bounds_are_safe

    original = source_path.read_bytes()
    text = original.decode("utf-8-sig", errors="replace")
    code = executable_source(text)
    names = set(re.findall(
        r"\b(?:double|float|int|long|bool|string)\s*(?:&\s*)?([A-Za-z_]\w*)\s*\[\s*\]\s*\[",
        code, re.I))
    if not names:
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "no declared two-dimensional arrays match the verifier rule"}
    updated = text
    replacements = {}
    for name in sorted(names):
        pattern = re.compile(rf"\bArraySize\s*\(\s*{re.escape(name)}\s*\)", re.I)
        updated, count = pattern.subn(f"ArrayRange({name}, 0)", updated)
        if count:
            replacements[name] = count
    if not replacements:
        return {"status": "ALREADY_BOUNDED" if multidimensional_array_row_bounds_are_safe(text)
                else "PATCH_REJECTED", "changed": False,
                "reason": "no offending ArraySize call matched a declared two-dimensional array"}
    if locked_strategy_surface(text) != locked_strategy_surface(updated):
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "row-bound repair changed the locked strategy surface"}
    if not multidimensional_array_row_bounds_are_safe(updated):
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "post-repair multidimensional row-bounds gate failed"}
    prefix = b"\xef\xbb\xbf" if original.startswith(b"\xef\xbb\xbf") else b""
    output = prefix + updated.encode("utf-8")
    source_path.write_bytes(output)
    return {"status": "APPLIED", "method": "deterministic_arrayrange_row_bounds",
            "changed": True, "before_sha256": sha256_bytes(original),
            "after_sha256": sha256_bytes(output), "row_bounds_gate": True,
            "replacements": replacements}


def apply_quantile_interpolation_bounds_fix(source_path: Path) -> dict:
    """Guard the upper interpolation read when the quantile window has one item."""
    original = source_path.read_bytes()
    text = original.decode("utf-8-sig", errors="strict")
    clamp = re.search(
        r"if\s*\(\s*qi0\s*>=\s*filled\s*-\s*1\s*\)\s*\{\s*"
        r"qi0\s*=\s*filled\s*-\s*1\s*;\s*fracq\s*=\s*0(?:\.0)?\s*;\s*\}",
        text, re.I,
    )
    pattern = re.compile(
        r"(?m)^(?P<indent>[ \t]*)double\s+u_t\s*=\s*temp\s*\[\s*qi0\s*\]"
        r"\s*\+\s*fracq\s*\*\s*\(\s*temp\s*\[\s*qi0\s*\+\s*1\s*\]"
        r"\s*-\s*temp\s*\[\s*qi0\s*\]\s*\)\s*;\s*$", re.I,
    )
    matches = list(pattern.finditer(text))
    if not clamp or len(matches) != 1:
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "expected one bounded quantile interpolation site"}
    match = matches[0]
    indent = match.group("indent")
    replacement = (
        f"{indent}double u_t = temp[qi0];\n"
        f"{indent}if(qi0 + 1 < filled)\n"
        f"{indent}   u_t += fracq * (temp[qi0 + 1] - temp[qi0]);"
    )
    updated = text[:match.start()] + replacement + text[match.end():]
    if locked_strategy_surface(text) != locked_strategy_surface(updated):
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "guarded interpolation changed the locked strategy surface"}
    prefix = b"\xef\xbb\xbf" if original.startswith(b"\xef\xbb\xbf") else b""
    output = prefix + updated.encode("utf-8")
    source_path.write_bytes(output)
    return {"status": "APPLIED", "method": "deterministic_quantile_interpolation_bounds",
            "changed": True, "before_sha256": sha256_bytes(original),
            "after_sha256": sha256_bytes(output), "upper_index_guard": "qi0 + 1 < filled",
            "replacements": 1}


def quantile_interpolation_bounds_are_safe(source: str) -> bool:
    """Confirm the identified interpolation read is guarded by the array length."""
    unsafe = re.search(
        r"\btemp\s*\[\s*qi0\s*\]\s*\+\s*fracq\s*\*\s*\(\s*"
        r"temp\s*\[\s*qi0\s*\+\s*1\s*\]\s*-\s*temp\s*\[\s*qi0\s*\]\s*\)",
        source, re.I,
    )
    guarded = re.search(
        r"if\s*\(\s*qi0\s*\+\s*1\s*<\s*filled\s*\)\s*"
        r"u_t\s*\+=\s*fracq\s*\*\s*\(\s*temp\s*\[\s*qi0\s*\+\s*1\s*\]\s*"
        r"-\s*temp\s*\[\s*qi0\s*\]\s*\)\s*;",
        source, re.I,
    )
    return not unsafe and bool(guarded)




def apply_rolling_volatility_history_window_fix(source_path: Path) -> dict:
    """Return enough samples for each requested overlapping rolling-volatility window."""
    original = source_path.read_bytes()
    text = original.decode("utf-8-sig", errors="replace")
    builder = _function_body_bounds(text, "BuildClosedReturns")
    rolling = _function_body_bounds(text, "BuildRollingVolatility")
    features = _function_body_bounds(text, "BuildFeatures")
    if not builder or not rolling or not features:
        return {"status": "PATCH_REJECTED", "changed": False, "reason": "ambiguous rolling-window helper"}
    start, end = builder
    body = text[start:end]
    rolling_body = text[rolling[0]:rolling[1]]
    feature_body = text[features[0]:features[1]]
    if "int return_count = 2 * lookback - 1;" in body and "ArrayResize(rets, return_count);" in body:
        return {"status": "ALREADY_FIXED", "changed": False, "source_sha256": sha256_bytes(original)}
    expected = (len(re.findall(r"ArrayResize\s*\(\s*rets\s*,\s*lookback\s*\)\s*;", body)) == 1
                and len(re.findall(r"for\s*\(\s*int\s+i\s*=\s*0\s*;\s*i\s*<\s*lookback\s*;\s*\+\+i\s*\)", body)) == 1
                and "if(lookback < 2) return false;" in body
                and len(re.findall(r"for\s*\(\s*int\s+t\s*=\s*0\s*;\s*t\s*<\s*lookback\s*;", rolling_body)) == 1
                and "idx = t + i" in rolling_body and "ArraySize(rets)" in rolling_body
                and "for(int i = 0; i < lookback; ++i)" in feature_body)
    if not expected:
        return {"status": "PATCH_REJECTED", "changed": False, "reason": "helpers do not match overlapping-window history shape"}
    updated_body = body.replace("ArrayResize(rets, lookback);",
        "int return_count = 2 * lookback - 1;\n   if(Bars(_Symbol, _Period) < return_count + 2) return false;\n   ArrayResize(rets, return_count);", 1)
    updated_body = updated_body.replace("for(int i = 0; i < lookback; ++i)", "for(int i = 0; i < return_count; ++i)", 1)
    updated_text = text[:start] + updated_body + text[end:]
    if updated_body == body or locked_strategy_surface(text) != locked_strategy_surface(updated_text):
        return {"status": "PATCH_REJECTED", "changed": False, "reason": "history repair changed protected strategy surface"}
    prefix = b"\xef\xbb\xbf" if original.startswith(b"\xef\xbb\xbf") else b""
    updated = prefix + updated_text.encode("utf-8")
    source_path.write_bytes(updated)
    return {"status": "APPLIED", "method": "rolling_volatility_overlapping_history_window",
            "changed": True, "before_sha256": sha256_bytes(original), "after_sha256": sha256_bytes(updated),
            "return_count": "2*lookback-1", "history_guard": "Bars >= return_count + 2"}


def apply_volatility_clustering_indexed_buffer_fix(source_path: Path) -> dict:
    """Repair the authenticated zero-trade cluster guard and its undersized close buffer."""
    original = source_path.read_bytes()
    try:
        text = original.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError as exc:
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": f"source encoding is not valid UTF-8: {exc}"}
    matches = list(re.finditer(r"\bvoid\s+OnTick\s*\([^{};]*\)\s*\{", text))
    if len(matches) != 1:
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "OnTick body is ambiguous"}
    start = matches[0].end() - 1
    depth = 0
    end = None
    for offset in range(start, len(text)):
        if text[offset] == "{":
            depth += 1
        elif text[offset] == "}":
            depth -= 1
            if depth == 0:
                end = offset + 1
                break
    if end is None:
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "OnTick brace structure is incomplete"}
    body = text[start:end]
    newline = "\r\n" if "\r\n" in body else "\n"
    declaration = "   double pcbuf[1];"
    prev_close_copy = (
        "   if(CopyClose(_Symbol, _Period, 2, 1, pcbuf) != 1) return;"
    )
    lookback_copy = (
        "   if(CopyClose(_Symbol, _Period, 1, InpVolStateLookback, pcbuf) "
        "!= InpVolStateLookback) return;"
    )
    cluster_copy = (
        "   if(CopyClose(_Symbol, _Period, 1, InpClusterWindow, pcbuf) "
        "!= InpClusterWindow) return;"
    )
    dead_guard = newline.join((
        "   int    start_idx = InpVolStateLookback - 1;",
        "   int    end_idx   = InpVolStateLookback - 1 + InpClusterWindow;",
        "   if(end_idx >= InpClusterWindow) return;",
    ))
    expected_counts = (body.count(declaration), body.count(prev_close_copy),
                       body.count(lookback_copy), body.count(cluster_copy), body.count(dead_guard))
    if expected_counts != (1, 1, 1, 1, 1):
        already_fixed = (
            body.count("double pcbuf[];") == 1
            and body.count("if(end_idx >= InpClusterWindow) return;") == 0
            and body.count("InpClusterWindow + InpVolStateLookback - 1, pcbuf") == 1
        )
        return {"status": "ALREADY_FIXED" if already_fixed else "PATCH_REJECTED",
                "changed": False, "expected_occurrences": [1, 1, 1, 1, 1],
                "observed_occurrences": list(expected_counts),
                "source_sha256": sha256_bytes(original)}

    cluster_samples = "InpClusterWindow + InpVolStateLookback - 1"
    updated_body = body.replace(declaration, "   double pcbuf[];", 1)
    updated_body = updated_body.replace(
        prev_close_copy,
        "   if(ArrayResize(pcbuf, 1) != 1) return;" + newline + prev_close_copy, 1)
    updated_body = updated_body.replace(
        lookback_copy,
        "   if(ArrayResize(pcbuf, InpVolStateLookback) != InpVolStateLookback) return;"
        + newline + lookback_copy, 1)
    updated_body = updated_body.replace(
        cluster_copy,
        f"   if(ArrayResize(pcbuf, {cluster_samples}) != {cluster_samples}) return;"
        + newline
        + "   if(CopyClose(_Symbol, _Period, 1, " + cluster_samples + ", pcbuf) != "
        + cluster_samples + ") return;", 1)
    updated_body = updated_body.replace(dead_guard + newline, "", 1)
    updated_text = text[:start] + updated_body + text[end:]

    # Reverse only these exact edits and require byte-for-byte source recovery.
    # This proves the rolling formula, regime selection, entry/exit signals,
    # and every other source line remain untouched.
    restored = updated_body.replace(
        "   if(ArrayResize(pcbuf, 1) != 1) return;" + newline, "", 1)
    restored = restored.replace(
        "   if(ArrayResize(pcbuf, InpVolStateLookback) != InpVolStateLookback) return;"
        + newline, "", 1)
    restored = restored.replace(
        f"   if(ArrayResize(pcbuf, {cluster_samples}) != {cluster_samples}) return;"
        + newline, "", 1)
    expanded_cluster_copy = (
        "   if(CopyClose(_Symbol, _Period, 1, " + cluster_samples + ", pcbuf) != "
        + cluster_samples + ") return;"
    )
    restored = restored.replace(expanded_cluster_copy, cluster_copy, 1)
    restored = restored.replace("   double pcbuf[];", declaration, 1)
    restored = restored.replace(cluster_copy,
                                cluster_copy + newline + dead_guard, 1)
    if restored != body or updated_text[:start] != text[:start]:
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "exact reverse check failed; protected strategy source would differ"}
    updated = (b"\xef\xbb\xbf" if original.startswith(b"\xef\xbb\xbf") else b"") + updated_text.encode("utf-8")
    source_path.write_bytes(updated)
    return {"status": "APPLIED", "method": "volatility_clustering_indexed_buffer_bounds",
            "changed": True, "before_sha256": sha256_bytes(original),
            "after_sha256": sha256_bytes(updated),
            "cluster_sample_count": cluster_samples,
            "removed_impossible_guard": "end_idx >= InpClusterWindow"}


def apply_semivariance_closed_bar_bounds_fix(source_path: Path) -> dict:
    """Repair only the captured impossible shift guard and reversed closed-bar returns."""
    original = source_path.read_bytes()
    text = original.decode("utf-8-sig", errors="replace")
    definitions = len(re.findall(r"\bbool\s+ComputeClosedBarMetrics\s*\(", text))
    if definitions != 1:
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": f"expected one ComputeClosedBarMetrics definition, found {definitions}"}
    span = _function_body_bounds(text, "ComputeClosedBarMetrics")
    if span is None:
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "ComputeClosedBarMetrics body is ambiguous"}
    start, end = span
    body = text[start:end]
    if ("Bars(_Symbol, _Period) <= shift + lookback" in body
            and "for(int i = 0; i < lookback; i++)" in body
            and "MathLog(c_newer / c_older)" in body):
        return {"status": "ALREADY_FIXED", "changed": False,
                "source_sha256": sha256_bytes(original)}
    required = (
        ("lookback input", r"int\s+lookback\s*=\s*InpSemivarianceLookback\s*;", 1),
        ("impossible shift guard", r"if\s*\(\s*shift\s*<\s*lookback\s*\+\s*1\s*\)\s*return\s+false\s*;", 1),
        ("reverse loop", r"for\s*\(\s*int\s+i\s*=\s*1\s*;\s*i\s*<=\s*lookback\s*;\s*i\+\+\s*\)", 1),
        ("reverse return inputs", r"MathLog\(\s*c_t1\s*/\s*c_t\s*\)", 1),
    )
    for label, pattern, count in required:
        if len(re.findall(pattern, body)) != count:
            return {"status": "PATCH_REJECTED", "changed": False,
                    "reason": f"{label} differs from the authenticated failing shape"}
    replacement = """{
   int lookback = InpSemivarianceLookback;
   if(lookback < 1 || shift < 1 || Bars(_Symbol, _Period) <= shift + lookback) return false;

   double rs_plus = 0.0, rs_minus = 0.0;
   for(int i = 0; i < lookback; i++)
   {
      int newer_shift = shift + i;
      int older_shift = newer_shift + 1;
      double c_newer = iClose(_Symbol, _Period, newer_shift);
      double c_older = iClose(_Symbol, _Period, older_shift);
      if(!MathIsValidNumber(c_newer) || !MathIsValidNumber(c_older)) return false;
      if(c_newer <= 0.0 || c_older <= 0.0) return false;
      double r = MathLog(c_newer / c_older);
      if(r > 0.0) rs_plus += r * r;
      else        rs_minus += r * r;
   }

   double a = MathSqrt((rs_minus + 1e-12) / (rs_plus + 1e-12));
   double rv = MathSqrt((rs_plus + rs_minus) / (double)lookback);
   if(!MathIsValidNumber(a) || !MathIsValidNumber(rv)) return false;

   asymmetry = a;
   realizedVol = rv;
   return true;
}"""
    updated_text = text[:start] + replacement + text[end:]
    if locked_strategy_surface(text) != locked_strategy_surface(updated_text):
        return {"status": "PATCH_REJECTED", "changed": False,
                "reason": "closed-bar repair changed locked inputs, regime definitions, or score callback"}
    prefix = b"\xef\xbb\xbf" if original.startswith(b"\xef\xbb\xbf") else b""
    updated = prefix + updated_text.encode("utf-8")
    source_path.write_bytes(updated)
    return {"status": "APPLIED", "changed": True,
            "method": "deterministic_semivariance_closed_bar_bounds",
            "before_sha256": sha256_bytes(original), "after_sha256": sha256_bytes(updated),
            "closed_bar_only": True}


def apply_kcos_regime_enum_order_fix(source: Path, diagnostics: str) -> dict | None:
    """Move the existing KCOS regime enum before a file-scope initializer that uses it."""
    if not re.search(r"(?i)undeclared identifier\s+'KCOS_REGIME_[A-Z0-9_]+'", diagnostics):
        return None
    original = source.read_bytes()
    text = original.decode("utf-8-sig", errors="replace")
    enum_pattern = re.compile(r"(?ms)^[ \t]*enum\s+ENUM_KCOS_REGIME\s*\{.*?^[ \t]*\};[ \t]*(?://[^\r\n]*)?(?:\r?\n|$)")
    enums = list(enum_pattern.finditer(text))
    if len(enums) != 1:
        return {"status": "PATCH_REJECTED", "reason": f"expected one exact ENUM_KCOS_REGIME declaration, found {len(enums)}"}
    enum_match = enums[0]
    use_pattern = re.compile(r"(?m)^[ \t]*(?:int|ENUM_KCOS_REGIME)\s+g_active_closed_bar_regime\s*=\s*(KCOS_REGIME_[A-Z0-9_]+)\s*;[^\r\n]*(?:\r?\n|$)")
    uses = list(use_pattern.finditer(text))
    if len(uses) != 1 or uses[0].start() >= enum_match.start():
        return {"status": "PATCH_REJECTED", "reason": "expected one file-scope regime initializer before the enum"}
    enumerator = uses[0].group(1)
    if len(re.findall(rf"\b{re.escape(enumerator)}\b", enum_match.group(0))) != 1:
        return {"status": "PATCH_REJECTED", "reason": "initializer enumerator is not uniquely declared by the existing enum"}
    block = enum_match.group(0)
    without_enum = text[:enum_match.start()] + text[enum_match.end():]
    target = use_pattern.search(without_enum)
    if not target:
        return {"status": "PATCH_REJECTED", "reason": "file-scope regime initializer disappeared during relocation"}
    updated_text = without_enum[:target.start()] + block + without_enum[target.start():]
    if locked_strategy_surface(text) != locked_strategy_surface(updated_text):
        return {"status": "PATCH_REJECTED", "reason": "enum relocation changed the locked strategy surface"}
    prefix = b"\xef\xbb\xbf" if original.startswith(b"\xef\xbb\xbf") else b""
    updated = prefix + updated_text.encode("utf-8")
    source.write_bytes(updated)
    return {"status": "APPLIED", "method": "deterministic_kcos_regime_enum_before_use",
            "changed": True, "before_sha256": sha256_bytes(original),
            "after_sha256": sha256_bytes(updated),
            "changes": ["moved the existing unchanged ENUM_KCOS_REGIME before its file-scope initializer"]}

def apply_known_mql5_compile_fix(source: Path, diagnostics: str) -> dict | None:
    """Apply only exact fixes for compiler-confirmed MQL5 API/type errors."""
    original = source.read_bytes()
    text = original.decode("utf-8-sig", errors="replace")
    updated = text
    changes = []
    # MathSign is not a built-in MQL5 function. Repair only a compiler-reported
    # undeclared call, with a one-to-one source path/line binding. Keeping this
    # helper outside strategy/scoring functions preserves their locked surface.
    if re.search(r"(?i)undeclared identifier\s+['\"]MathSign['\"]", diagnostics):
        rows = re.findall(
            r"(?im)^(.+?\.mq5)\((\d+),(\d+)\)\s*:\s*error\s+(\d+)\s*:\s*(.*)$",
            diagnostics)
        errors_total = len(re.findall(r"(?i):\s*error\s+\d+\s*:", diagnostics))
        expected_path = os.path.normcase(os.path.abspath(str(source)))
        mathsign_rows = []
        cascade_rows: dict[int, list[str]] = {}
        cascade_patterns = {
            "152": r"['\"]r_t['\"]\s*-\s*some operator expected",
            "154": r"semicolon expected",
            "150": r"['\"]\*['\"]\s*-\s*illegal operation use",
            "237": r"object pointer expected",
        }
        identity_ok = len(rows) == errors_total and bool(rows)
        for path, line_no, column_no, code, message in rows:
            line_number = int(line_no)
            if os.path.normcase(os.path.abspath(path)) != expected_path:
                identity_ok = False
            if code == "256" and re.search(r"(?i)undeclared identifier\s+['\"]MathSign['\"]", message):
                mathsign_rows.append((line_number, int(column_no)))
            elif code in cascade_patterns and re.fullmatch(cascade_patterns[code], message.strip(), re.I):
                cascade_rows.setdefault(line_number, []).append(code)
            else:
                identity_ok = False
        code = __import__("kcos_stage2_verify_v2")._mask_comments_and_strings(text)
        existing_definition = re.search(
            r"(?i)\b(?:int|double|long|float|bool|void)\s+MathSign\s*\(", code)
        calls = list(re.finditer(r"\bMathSign\s*\(", code))
        call_lines = [code.count("\n", 0, match.start()) + 1 for match in calls]
        reported_lines = [line for line, _column in mathsign_rows]
        one_call_per_line = len(call_lines) == len(set(call_lines))
        cascade_binding_ok = (
            all(line in reported_lines and len(codes) == len(set(codes))
                for line, codes in cascade_rows.items())
        )
        if (not identity_ok or existing_definition or not calls or not one_call_per_line
                or sorted(call_lines) != sorted(reported_lines) or not cascade_binding_ok):
            return {"status": "PATCH_REJECTED",
                    "reason": "MathSign diagnostics do not bind one-to-one to undeclared call sites in this source"}
        helper = ("\n// Deterministic sign helper for compiler-confirmed MathSign call sites.\n"
                  "int MathSign(double value)\n"
                  "{\n"
                  "   if(value > 0.0) return 1;\n"
                  "   if(value < 0.0) return -1;\n"
                  "   return 0;\n"
                  "}\n")
        updated += helper
        changes.append({"kind": "mql5_mathsign_sign_helper", "replacements": len(calls),
                        "diagnostic_lines": sorted(reported_lines),
                        "semantics": "positive=1, negative=-1, zero=0"})
    if re.search(r"(?i)undeclared identifier\s+'POSITION_PRICE'", diagnostics):
        updated, count = re.subn(r"\bPositionGetDouble\s*\(\s*POSITION_PRICE\s*\)",
                                 "PositionGetDouble(POSITION_PRICE_OPEN)", updated)
        if count != 1:
            return {"status": "PATCH_REJECTED", "reason": f"expected one exact POSITION_PRICE call, found {count}"}
        changes.append({"kind": "position_price_open", "replacements": count})
    position_direction_errors = re.findall(
        r"(?i)undeclared identifier\s+'(POSITION_LONG|POSITION_SHORT)'", diagnostics)
    if position_direction_errors:
        # MQL5 position direction uses POSITION_TYPE_BUY/SELL. Bind both
        # replacements to the exact compiler-reported call sites and expected
        # enum-typed helper so unrelated identifiers or partial repairs fail closed.
        error_rows = re.findall(
            r"(?im)^(.+?\.mq5)\((\d+),\d+\)\s*:\s*error\s+(256|262)\s*:", diagnostics)
        expected_source = os.path.normcase(os.path.abspath(str(source)))
        grouped_errors: dict[int, list[str]] = {}
        identities_match = len(error_rows) == 4 and len(position_direction_errors) == 2
        for path, line_no, code in error_rows:
            if os.path.normcase(os.path.abspath(path)) != expected_source:
                identities_match = False
            grouped_errors.setdefault(int(line_no), []).append(code)
        long_call = re.compile(r"\bCloseOpposite\s*\(\s*POSITION_LONG\s*\)")
        short_call = re.compile(r"\bCloseOpposite\s*\(\s*POSITION_SHORT\s*\)")
        long_matches, short_matches = list(long_call.finditer(updated)), list(short_call.finditer(updated))
        call_lines = {updated.count("\n", 0, m.start()) + 1 for m in long_matches + short_matches}
        expected_lines = {line for line, codes in grouped_errors.items()
                          if sorted(codes) == ["256", "262"]}
        enum_helper = re.search(r"\bvoid\s+CloseOpposite\s*\(\s*ENUM_POSITION_TYPE\b", updated)
        if (not identities_match or len(grouped_errors) != 2
                or any(sorted(codes) != ["256", "262"] for codes in grouped_errors.values())
                or expected_lines != call_lines or len(long_matches) != 1
                or len(short_matches) != 1 or not enum_helper):
            return {"status": "PATCH_REJECTED",
                    "reason": "compiler evidence does not bind both exact position-direction enum call sites"}
        updated, long_count = long_call.subn("CloseOpposite(POSITION_TYPE_BUY)", updated)
        updated, short_count = short_call.subn("CloseOpposite(POSITION_TYPE_SELL)", updated)
        if long_count != 1 or short_count != 1:
            return {"status": "PATCH_REJECTED", "reason": "expected both exact position-direction calls"}
        changes.append({"kind": "position_direction_enum_constants", "replacements": long_count + short_count})
    if re.search(r"(?i)undeclared identifier\s+'SetDeviations'", diagnostics):
        updated, count = re.subn(r"\bSetDeviations\s*\(\s*10\s*\)",
                                 "SetDeviationInPoints(10)", updated)
        if count != 1:
            return {"status": "PATCH_REJECTED", "reason": f"expected one SetDeviations(10), found {count}"}
        changes.append({"kind": "trade_set_deviation_in_points", "replacements": count})
    if re.search(r"(?i)cannot convert parameter 'datetime' to 'datetime&\[\]'", diagnostics):
        pattern = re.compile(
            r"(?m)^(\s*)datetime\s+(\w+)\s*;\s*\r?\n"
            r"\s*if\s*\(\s*CopyTime\(([^;]*?),\s*\2\)\s*==\s*1\s*\)\s*\r?\n"
            r"\s*g_entry_bar_time\s*=\s*\2\s*;"
        )
        updated, count = pattern.subn(
            lambda match: (f"{match.group(1)}datetime {match.group(2)}[1];\n"
                           f"{match.group(1)}if(CopyTime({match.group(3)}, {match.group(2)}) == 1)\n"
                           f"{match.group(1)}   g_entry_bar_time = {match.group(2)}[0];"), updated)
        if count != 2:
            return {"status": "PATCH_REJECTED", "reason": f"expected two exact scalar CopyTime paths, found {count}"}
        changes.append({"kind": "copy_time_datetime_array", "replacements": count})
    nan_errors = re.findall(
        r"(?im):\s*error\s+198\s*:.*division by zero in the constant expression", diagnostics)
    if nan_errors:
        from kcos_stage2_generate_source_v2 import repair_generated_mql5_runtime_patterns
        masked = __import__("kcos_stage2_verify_v2")._mask_comments_and_strings(text)
        sentinel_count = len(re.findall(r"(?<![\w.])0(?:\.0+)?\s*/\s*0(?:\.0+)?(?![\w.])", masked))
        repaired_text, repairs = repair_generated_mql5_runtime_patterns(text)
        nan_repairs = [item for item in repairs if item.get("kind") == "mql5_nan_sentinel_replaced"]
        if (len(nan_errors) != sentinel_count or len(nan_repairs) != 1
                or nan_repairs[0].get("count") != sentinel_count
                or len(repairs) != 1):
            return {"status": "PATCH_REJECTED", "reason":
                    f"compiler reports {len(nan_errors)} constant zero-division errors; source has {sentinel_count} matching sentinels"}
        updated = repaired_text
        changes.extend(nan_repairs)
    stop_level_errors = re.findall(
        r"(?im)^(.+?\.mq5)\((\d+),\d+\)\s*:\s*error\s+(199|262)\s*:", diagnostics)
    if stop_level_errors and re.search(r"\bSYMBOL_TRADE_STOPS_LEVEL\b", updated, re.I):
        stop_level_calls = re.compile(
            r"SymbolInfoDouble\s*\(\s*_Symbol\s*,\s*SYMBOL_TRADE_STOPS_LEVEL\s*\)", re.I)
        matches = list(stop_level_calls.finditer(updated))
        target_lines = {updated.count("\n", 0, match.start()) + 1 for match in matches}
        error_lines: dict[int, set[str]] = {}
        expected_source = os.path.normcase(os.path.abspath(str(source)))
        for path, line_no, code in stop_level_errors:
            if os.path.normcase(os.path.abspath(path)) != expected_source:
                return {"status": "PATCH_REJECTED", "reason": "stop-level compile errors reference a different source"}
            error_lines.setdefault(int(line_no), set()).add(code)
        expected_error_lines = {line_no for line_no, codes in error_lines.items()
                                if codes == {"199", "262"}}
        if (len(matches) != 2 or len(stop_level_errors) != 4
                or len(error_lines) != 2
                or any(codes != {"199", "262"} for codes in error_lines.values())
                or expected_error_lines != target_lines):
            return {"status": "PATCH_REJECTED",
                    "reason": "errors 199/262 do not map exactly to both trade-stops-level double-property calls"}
        updated, replacements = stop_level_calls.subn(
            "(double)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL)", updated)
        if replacements != 2:
            return {"status": "PATCH_REJECTED",
                    "reason": f"expected two exact trade-stops-level calls, replaced {replacements}"}
        changes.append({"kind": "trade_stops_level_integer_property", "replacements": replacements})
    warning_rows = re.findall(
        r"(?im)^(.+?\.mq5)\((\d+),\d+\)\s*:\s*warning\s+43\s*:"
        r"\s*possible loss of data due to type conversion from 'long' to 'uint'\s*$",
        diagnostics)
    if warning_rows:
        ticket_lookup = re.compile(
            r"(?P<prefix>\b[A-Za-z_]\w*\.PositionClose\s*\(\s*)"
            r"PositionGetTicket\s*\(\s*PositionGetInteger\s*\(\s*POSITION_TICKET\s*\)\s*\)"
            r"(?P<suffix>\s*\))", re.I)
        matches = list(ticket_lookup.finditer(updated))
        target_lines = {updated.count("\n", 0, match.start()) + 1 for match in matches}
        all_warning_43 = re.findall(r"(?i):\s*warning\s+43\s*:", diagnostics)
        expected_source = os.path.normcase(os.path.abspath(str(source)))
        warning_identity_matches = (
            len(matches) == 2 and len(warning_rows) == 2 and len(all_warning_43) == 2
            and {int(line_no) for path, line_no in warning_rows
                 if os.path.normcase(os.path.abspath(path)) == expected_source} == target_lines
            and all(os.path.normcase(os.path.abspath(path)) == expected_source
                    for path, _line_no in warning_rows)
        )
        if not warning_identity_matches:
            return {"status": "PATCH_REJECTED",
                    "reason": "warning 43 does not map exactly to both selected-position ticket close calls"}
        updated, replacements = ticket_lookup.subn(
            lambda match: match.group("prefix") + "(ulong)PositionGetInteger(POSITION_TICKET)"
                          + match.group("suffix"), updated)
        if replacements != 2:
            return {"status": "PATCH_REJECTED",
                    "reason": f"expected two selected-position ticket calls, replaced {replacements}"}
        changes.append({"kind": "selected_position_ticket_close", "replacements": replacements})
    if (re.search(r"(?i)undeclared identifier\s+'currentValue'", diagnostics)
            and re.search(r"(?i)':'\s*-\s*unexpected token", diagnostics)):
        compile_rows = re.findall(
            r"(?im)^(.+?\.mq5)\((\d+),\d+\)\s*:\s*error\s+(\d+)\s*:", diagnostics)
        named_rows = re.findall(
            r"(?im)^(.+?\.mq5)\((\d+),\d+\)\s*:\s*error\s+(149|256)\s*:", diagnostics)
        expected_source = os.path.normcase(os.path.abspath(str(source)))
        grouped_errors: dict[int, list[str]] = {}
        identities_match = len(compile_rows) == 6 and len(named_rows) == 6
        for path, line_no, code in compile_rows:
            if os.path.normcase(os.path.abspath(path)) != expected_source:
                identities_match = False
            grouped_errors.setdefault(int(line_no), []).append(code)
        expected_codes = {"149": 4, "256": 2}
        actual_codes = {code: sum(1 for _path, _line, row_code in compile_rows if row_code == code)
                        for code in {row_code for _path, _line, row_code in compile_rows}}
        target_lines = {line for line, codes in grouped_errors.items()
                        if sorted(codes) == ["149", "149", "256"]}
        long_call = re.compile(
            r"(?m)^(?P<indent>[ \t]*)TryEnterLong\(\s*ask,\s*atrValue,\s*"
            r"currentValue:\s*close1,\s*emaPrev\s*\);\s*$")
        short_call = re.compile(
            r"(?m)^(?P<indent>[ \t]*)TryEnterShort\(\s*bid,\s*atrValue,\s*"
            r"currentValue:\s*close1,\s*emaPrev\s*\);\s*$")
        long_matches = list(long_call.finditer(updated))
        short_matches = list(short_call.finditer(updated))
        call_lines = {updated.count("\n", 0, match.start()) + 1
                      for match in long_matches + short_matches}
        signatures_match = (
            len(re.findall(r"(?m)^\s*void\s+TryEnterLong\(double\s+ask,\s*double\s+atrValue,\s*double\s+priorClose,\s*double\s+emaPrev\)", updated)) == 1
            and len(re.findall(r"(?m)^\s*void\s+TryEnterShort\(double\s+bid,\s*double\s+atrValue,\s*double\s+priorClose,\s*double\s+emaPrev\)", updated)) == 1
        )
        if (not identities_match or actual_codes != expected_codes or len(grouped_errors) != 2
                or len(target_lines) != 2 or target_lines != call_lines
                or len(long_matches) != 1 or len(short_matches) != 1 or not signatures_match):
            return {"status": "PATCH_REJECTED",
                    "reason": "compiler evidence does not map exactly to both invalid currentValue labels"}
        updated, long_count = long_call.subn(
            lambda match: match.group("indent") + "TryEnterLong(ask, atrValue, close1, emaPrev);", updated)
        updated, short_count = short_call.subn(
            lambda match: match.group("indent") + "TryEnterShort(bid, atrValue, close1, emaPrev);", updated)
        if long_count != 1 or short_count != 1:
            return {"status": "PATCH_REJECTED", "reason": "expected both exact invalid call sites"}
        changes.append({"kind": "mql5_named_argument_label_removed", "replacements": long_count + short_count})
    if not changes:
        return None
    prefix = b"\xef\xbb\xbf" if original.startswith(b"\xef\xbb\xbf") else b""
    source.write_bytes(prefix + updated.encode("utf-8"))
    return {"status": "APPLIED", "method": "compiler_proven_mql5_api_fixes" if not nan_errors else "compiler_proven_mql5_nan_sentinel",
            "changes": changes, "before_sha256": sha256_bytes(original),
            "after_sha256": sha256_bytes(source.read_bytes())}


def apply_authenticated_ctrade_api_fix(source: Path, diagnostics: str) -> dict:
    """Fix only the two exact unsupported CTrade calls named by this source's compiler log."""
    original = source.read_bytes()
    text = original.decode("utf-8-sig", errors="replace")
    deviation = re.compile(r"\bSetDeviationsInPoints\s*\(\s*10\s*\)")
    filling = re.compile(r"\bSetTypeFillingByVolume\s*\(\s*_Symbol\s*,\s*"
                         r"SymbolInfoDouble\s*\(\s*_Symbol\s*,\s*SYMBOL_VOLUME_MIN\s*\)\s*\)")
    deviation_matches = list(deviation.finditer(text))
    filling_matches = list(filling.finditer(text))
    if len(deviation_matches) != 1 or len(filling_matches) != 1:
        return {"status": "PATCH_REJECTED", "reason": "expected one exact call for each unsupported CTrade API"}
    bad_names = ("SetDeviationsInPoints", "SetTypeFillingByVolume")
    if not all(re.search(r"(?i)undeclared identifier\s+'" + re.escape(name) + r"'", diagnostics)
               for name in bad_names):
        return {"status": "PATCH_REJECTED", "reason": "compiler evidence does not name both unsupported CTrade APIs"}
    source_lines = text.splitlines()
    target_lines = {text.count("\n", 0, match.start()) + 1
                    for match in (deviation_matches[0], filling_matches[0])}
    error_rows = re.findall(
        r"(?im)^(.+?\.mq5)\((\d+),\d+\)\s*:\s*error\s+\d+\s*:\s*(.*)$", diagnostics)
    all_errors = len(re.findall(r"(?i):\s*error\s+\d+\s*:", diagnostics))
    try:
        expected_source = os.path.normcase(os.path.abspath(str(source)))
        rows_match = (len(error_rows) == all_errors and bool(error_rows)
                      and all(os.path.normcase(os.path.abspath(path)) == expected_source
                              and int(line_no) in target_lines for path, line_no, _message in error_rows))
    except (OSError, TypeError, ValueError):
        rows_match = False
    if not rows_match:
        return {"status": "PATCH_REJECTED", "reason": "compiler log includes errors outside the two exact CTrade call sites"}
    updated = deviation.sub("SetDeviationInPoints(10)", text, count=1)
    updated = filling.sub("SetTypeFillingBySymbol(_Symbol)", updated, count=1)
    prefix = b"\xef\xbb\xbf" if original.startswith(b"\xef\xbb\xbf") else b""
    source.write_bytes(prefix + updated.encode("utf-8"))
    return {"status": "APPLIED", "method": "authenticated_exact_ctrade_api_repair",
            "changes": [{"kind": "trade_set_deviation_in_points", "replacements": 1},
                        {"kind": "trade_set_filling_by_symbol", "replacements": 1}],
            "before_sha256": sha256_bytes(original),
            "after_sha256": sha256_bytes(source.read_bytes())}


def reusable_failed_compile_evidence(compile_path: Path, source: Path, cycle_id: str) -> bool:
    """Reuse a failed receipt for identical source instead of exhausting retries before repair."""
    try:
        receipt = json.loads(compile_path.read_text(encoding="utf-8-sig"))
        return (receipt.get("cycle_id") == cycle_id
                and str(receipt.get("mq5_sha256") or "").upper() == sha(source)
                and receipt.get("passed") is not True
                and receipt.get("timed_out") is False)
    except (OSError, ValueError, TypeError):
        return False


def refresh_generated_source_admission(handoff_path: Path, source_path: Path,
                                       repair: dict) -> dict | None:
    """Re-run deterministic source admission and publish derived lineage receipts."""
    handoff = json.loads(handoff_path.read_text(encoding="utf-8-sig"))
    if handoff.get("source_implementation_mode") != "model_generated_source_admitted":
        return None
    proof_ref = handoff.get("source_implementation_review") or {}
    old_proof_path = Path(proof_ref.get("path") or "")
    old_build_path = Path(handoff.get("source_build_receipt") or "")
    if (not old_proof_path.is_file() or sha256_bytes(old_proof_path.read_bytes()) != str(proof_ref.get("sha256") or "").upper()
            or not old_build_path.is_file()
            or sha256_bytes(old_build_path.read_bytes()) != str(handoff.get("source_build_receipt_sha256") or "").upper()):
        raise RuntimeError("cannot attest repaired source: parent source-admission lineage changed")
    spec_path = Path(handoff["stage1_spec"])
    minato_path = Path(handoff["canonical_minato"])
    if (sha256_bytes(spec_path.read_bytes()) != str(handoff["stage1_spec_sha256"]).upper()
            or sha256_bytes(minato_path.read_bytes()) != str(handoff["canonical_minato_sha256"]).upper()):
        raise RuntimeError("cannot attest repaired source: Stage 1 spec or canonical Minato include changed")
    import kcos_stage2_generate_source_v2 as source_builder
    from kcos_input_contract_v2 import discover_inputs
    from kcos_tester_config_v2 import resolve_input_spec, stage2_spec_from_spec
    shared_sources = {str(minato_path.resolve()): minato_path.read_bytes().decode("utf-8-sig")}
    shared = discover_inputs(shared_sources)
    spec, _ = resolve_input_spec(json.loads(spec_path.read_text(encoding="utf-8-sig")), shared_sources=shared_sources)
    spec = stage2_spec_from_spec(spec)
    input_usage = source_builder.validate_source(source_path.read_text(encoding="utf-8-sig", errors="replace"), spec, shared)
    proof = json.loads(old_proof_path.read_text(encoding="utf-8-sig"))
    before_source_sha = str(proof.get("source_sha256") or "").upper()
    after_source_sha = sha256_bytes(source_path.read_bytes())
    if before_source_sha == after_source_sha:
        return None
    proof["source_sha256"] = after_source_sha
    proof["parent_admission_receipt"] = {"path": str(old_proof_path.resolve()), "sha256": str(proof_ref["sha256"]).upper()}
    proof["post_admission_repair"] = {
        **repair, "before_source_sha256": before_source_sha,
        "after_source_sha256": after_source_sha,
        "deterministic_source_admission_passed": True,
        "deterministic_input_usage": input_usage,
    }
    attempt_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:10]
    # Keep the derived proof beside its authenticated effective-spec and
    # input-resolution artifacts; the verifier requires all three together.
    repair_dir = old_proof_path.parent
    new_proof_path = repair_dir / f"source-admission-repair-{attempt_id}.json"
    track_transaction_artifact(new_proof_path)
    write_json_atomic(new_proof_path, proof)
    new_proof_ref = {"path": str(new_proof_path.resolve()), "sha256": sha256_bytes(new_proof_path.read_bytes())}
    old_build_bytes = old_build_path.read_bytes()
    build = json.loads(old_build_bytes.decode("utf-8-sig"))
    build["sha256"] = after_source_sha
    build["implementation_admission"] = new_proof_ref
    build["post_admission_repair"] = proof["post_admission_repair"]
    parent_build_path = repair_dir / f"candidate-parent-build-receipt-{attempt_id}.json"
    track_transaction_artifact(parent_build_path)
    write_json_atomic(parent_build_path, json.loads(old_build_bytes.decode("utf-8-sig")))
    new_build_path = old_build_path
    write_json_atomic(new_build_path, build)
    handoff["source_implementation_review"] = new_proof_ref
    handoff["source_build_receipt"] = str(new_build_path.resolve())
    handoff["source_build_receipt_sha256"] = sha256_bytes(new_build_path.read_bytes())
    write_json_atomic(handoff_path, handoff)
    return {"proof": new_proof_ref, "build_receipt": str(new_build_path.resolve()),
            "source_sha256": after_source_sha, "deterministic_source_admission_passed": True}


def locked_strategy_surface(source: str) -> str:
    """Fingerprint settings, strategy calculations/control flow, and scorer before AI edits."""
    # Stage 2's authenticated zero-trade repair for the volatility-clustering
    # EA changes only the pcbuf capacity/copy count and removes a provably
    # always-true bounds guard. Normalize that exact repaired shape back to
    # its pre-repair spelling for the strategy fingerprint; all remaining
    # OnTick code continues to be compared byte-for-byte by this surface.
    # This is intentionally shape-specific so arbitrary OnTick edits remain
    # rejected by the transaction lock.
    required_pcbuf_resizes = (
        r"(?m)^\s*if\(ArrayResize\(pcbuf,\s*1\)\s*!=\s*1\)\s*return;\s*$",
        r"(?m)^\s*if\(ArrayResize\(pcbuf,\s*InpVolStateLookback\)\s*!=\s*InpVolStateLookback\)\s*return;\s*$",
        r"(?m)^\s*if\(ArrayResize\(pcbuf,\s*InpClusterWindow\s*\+\s*InpVolStateLookback\s*-\s*1\)\s*!=\s*InpClusterWindow\s*\+\s*InpVolStateLookback\s*-\s*1\)\s*return;\s*$",
    )
    repaired_cluster_shape = (
        re.search(r"(?m)^\s*double\s+pcbuf\[\];\s*$", source)
        and all(len(re.findall(pattern, source)) == 1 for pattern in required_pcbuf_resizes)
        and re.search(
            r"CopyClose\(_Symbol,\s*_Period,\s*1,\s*InpClusterWindow\s*\+\s*InpVolStateLookback\s*-\s*1,\s*pcbuf\)",
            source,
        )
        and not re.search(r"if\s*\(\s*end_idx\s*>=\s*InpClusterWindow\s*\)\s*return\s*;", source)
    )
    if repaired_cluster_shape:
        source = re.sub(r"(?m)^(\s*)double\s+pcbuf\[\];\s*$", r"\1double pcbuf[1];", source, count=1)
        source = re.sub(r"(?m)^\s*if\(ArrayResize\(pcbuf,.*\)\s*!=.*\)\s*return;\s*\r?\n", "", source)
        source = re.sub(
            r"if\(CopyClose\(_Symbol,\s*_Period,\s*1,\s*InpClusterWindow\s*\+\s*InpVolStateLookback\s*-\s*1,\s*pcbuf\)\s*!=\s*InpClusterWindow\s*\+\s*InpVolStateLookback\s*-\s*1\)\s*return;",
            "if(CopyClose(_Symbol, _Period, 1, InpClusterWindow, pcbuf) != InpClusterWindow) return;",
            source,
            count=1,
        )
        source = re.sub(
            r"(?m)^(\s*)for\(int k = 0; k < InpClusterWindow; k\+\+\)",
            r"\1int    start_idx = InpVolStateLookback - 1;\n"
            r"\1int    end_idx   = InpVolStateLookback - 1 + InpClusterWindow;\n"
            r"\1if(end_idx >= InpClusterWindow) return;\n"
            r"\1for(int k = 0; k < InpClusterWindow; k++)",
            source,
            count=1,
        )
    # Canonicalize only the compiler-proven correction from looking up a
    # selected position ticket with an index-taking API. This leaves every
    # other part of OnTick under the existing strategy lock.
    selected_ticket_call = re.compile(
        r"PositionClose\s*\(\s*(?:"
        r"PositionGetTicket\s*\(\s*PositionGetInteger\s*\(\s*POSITION_TICKET\s*\)\s*\)"
        r"|\(\s*ulong\s*\)\s*PositionGetInteger\s*\(\s*POSITION_TICKET\s*\)"
        r")\s*\)", re.I)
    source = selected_ticket_call.sub("PositionClose(__SELECTED_POSITION_TICKET__)", source)
    # Canonicalize only the compiler-proven unsupported named-argument label
    # on the exact positional entry-call shape.
    source = re.sub(
        r"\b(TryEnter(?:Long|Short)\s*\(\s*(?:ask|bid)\s*,\s*atrValue\s*,\s*)"
        r"currentValue\s*:\s*(close1\s*,\s*emaPrev\s*\))",
        r"\1\2", source, flags=re.I,
    )
    inputs = [line.strip() for line in source.splitlines()
              if re.match(r"^\s*input\b", line) and ";" in line]
    enums = re.findall(r"(?is)\benum\s+[A-Za-z_]\w*\s*\{.*?\}\s*;", source)

    def function_body(name: str) -> str:
        match = re.search(rf"(?i)\b(?:double|int|void|bool|long|ENUM_[A-Za-z_]\w*)\s+{name}\s*\([^)]*\)\s*\{{", source)
        if not match:
            return ""
        opening = source.find("{", match.start())
        depth = 0
        for index in range(opening, len(source)):
            if source[index] == "{": depth += 1
            elif source[index] == "}":
                depth -= 1
                if depth == 0:
                    return source[match.start():index + 1]
        return source[match.start():]

    regime_functions = {name: function_body(name) for name in
                        re.findall(r"(?i)\b(?:double|int|bool|ENUM_[A-Za-z_]\w*)\s+([A-Za-z_]\w*(?:Regime|Classif)\w*)\s*\(", source)}
    declared_functions = re.findall(r"(?i)\b(?:double|int|bool|void|long|ENUM_[A-Za-z_]\w*)\s+([A-Za-z_]\w*)\s*\([^)]*\)\s*\{", source)
    strategy_functions = {name: function_body(name) for name in declared_functions
                          if any(token in name.casefold() for token in ("ontick", "signal", "classif"))}
    stop_equations = [re.sub(r"\s+", " ", line.strip()) for line in source.splitlines()
                      if re.search(r"(?i)\b(?:sl|tp|stoploss|takeprofit)\b", line)
                      and re.search(r"(?i)(?:=|\+=|-=)", line)]
    surface = {"inputs": inputs, "enums": enums,
               "regime_functions": regime_functions,
               "strategy_functions": strategy_functions,
               "stop_equations": stop_equations,
               "on_tester": function_body("OnTester")}
    return hashlib.sha256(json.dumps(surface, sort_keys=True).encode("utf-8")).hexdigest().upper()


def ai_proposal(source: Path, diagnostics: str, failed: list[str], *, handoff: dict | None = None) -> dict:
    if os.environ.get("KCOS_AI_REPAIR", "1") != "1": return {"status": "DISABLED"}
    if _ACTIVE_EPISODE is not None:
        slot = reserve_proposal(_ACTIVE_EPISODE["ledger"], _ACTIVE_EPISODE["identity"])
        if slot is None:
            _ACTIVE_EPISODE["exhausted"] = True
            return {"status": "EXHAUSTED", "reason": "episode proposal budget exhausted"}
    compile_failure = re.search(r"(?i):\s*error\s+\d+\s*:", diagnostics) is not None
    evidence = diagnostics
    evidence = evidence.replace("\x00", " ")
    task = ("A MetaEditor compile failure is present. Repair the listed compiler errors using the compiler log and source. "
            "Ignore tester journal, trading-enabled, account-balance, and performance evidence for this compile-only task. "
            if compile_failure else "Only propose a repair supported by the supplied technical evidence. ")
    if "time_stop_zero_disables_close" in failed:
        task += ("The Stage 2 verifier requires zero time-stop values to disable time-based exits. Guard every "
                 "expiry comparison with the specific input > 0. Preserve the approved input declaration/default "
                 "and all strategy logic beyond that guard.\n")
    execution_contract = {}
    if handoff and handoff.get("stage1_spec"):
        spec_path = Path(handoff["stage1_spec"])
        expected = str(handoff.get("stage1_spec_sha256") or "").upper()
        if not spec_path.is_file() or not expected or sha256_bytes(spec_path.read_bytes()) != expected:
            return {"status": "FAILED", "error": "authenticated Stage 1 spec missing or hash mismatch"}
        stage1_spec = json.loads(spec_path.read_text(encoding="utf-8-sig"))
        strategy = stage1_spec.get("strategy") if isinstance(stage1_spec.get("strategy"), dict) else {}
        execution_contract = strategy.get("execution_contract") or stage1_spec.get("execution_contract") or {}
    source_context = source_repair_context(source.read_text(encoding="utf-8-sig", errors="replace"), evidence,
                                          source_name=source.name,
                                          execution_contract=execution_contract)
    prompt=f'''Return JSON only: {{"patch":"unified diff or empty","diagnosis":"string","changes":["string"]}}.
You are proposing a bounded repair for one isolated KCOS MT5 EA after a technical failure.
{task}Only propose a change when it makes the EA logically consistent with its declared regime.
Preserve the authenticated Stage 1 strategy contract, signal concept, regime definition, Minato base score, score formula,
all regime thresholds and multipliers, every input name/type/default/range, custom maximums
(including InpMaxTradeBars), and timeframe contract. When failed gate time_stop_zero_disables_close is present,
make zero disable each time-stop exit by guarding expiry with the corresponding input > 0; do not change its
approved declaration or default. Do not repair poor profit, drawdown, fitness,
or ordinary trade count. However, when the handoff explicitly marks a technically evidenced
all-zero-trade failure, inspect the signal/order/initialization paths for a real code defect;
do not assume zero trades is merely poor performance. Do not change the cron lifecycle.
Change one EA source file only and refuse if the evidence is insufficient.
Return patch as a valid unified diff with exactly one matching source basename in
the --- and +++ headers. Every hunk header must include numeric old and new
line ranges (for example: @@ -12,4 +12,5 @@); bare @@ markers are invalid.
Keep patch context lines byte-for-byte aligned with the supplied source excerpt.
Failed gates: {failed}
Compiler and verification evidence:
{evidence}\nRELEVANT SOURCE AND LOCKED STRATEGY CONTEXT:\n{source_context}'''
    command_units = len(prompt.encode("utf-16-le")) // 2
    if command_units > 30000:
        return {"status": "FAILED",
                "error": f"repair prompt exceeds Windows-safe one-shot command budget ({command_units}>30000 UTF-16 units)"}
    call_artifacts = None
    if _ACTIVE_EPISODE is not None:
        call_index = _ACTIVE_EPISODE.get("call_index", 0) + 1
        _ACTIVE_EPISODE["call_index"] = call_index
        call_artifacts = _ACTIVE_EPISODE["attempt_dir"] / "proposals"
        call_artifacts.mkdir(parents=True, exist_ok=True)
        (call_artifacts / f"proposal-{call_index}.prompt.txt").write_text(prompt, encoding="utf-8")
    try:
        p=subprocess.run(["hermes","--cli","--safe-mode","--ignore-rules","--no-restore-cwd","-m","gpt-5.6-luna","--provider","openai-codex","-z",prompt],cwd=ROOT,capture_output=True,text=True,timeout=120)
        if call_artifacts is not None:
            (call_artifacts / f"proposal-{call_index}.stdout.txt").write_text(p.stdout or "", encoding="utf-8")
            (call_artifacts / f"proposal-{call_index}.stderr.txt").write_text(p.stderr or "", encoding="utf-8")
            (call_artifacts / f"proposal-{call_index}.call.json").write_text(json.dumps({
                "model": "gpt-5.6-luna", "provider": "openai-codex", "returncode": p.returncode,
                "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest().upper(),
                "stdout_sha256": hashlib.sha256((p.stdout or "").encode("utf-8")).hexdigest().upper(),
                "stderr_sha256": hashlib.sha256((p.stderr or "").encode("utf-8")).hexdigest().upper()}, indent=2) + "\n", encoding="utf-8")
        if p.returncode != 0:
            return {"status": "FAILED", "error": f"Hermes exited {p.returncode}",
                    "stdout": (p.stdout or "")[-4000:], "stderr": (p.stderr or "")[-4000:]}
        raw=(p.stdout or "").strip(); raw=re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.I|re.S).strip()
        obj=json.loads(raw); obj["status"]="PROPOSED"
        if _ACTIVE_EPISODE is not None:
            raw_dir = _ACTIVE_EPISODE["attempt_dir"] / "proposals"
            raw_dir.mkdir(parents=True, exist_ok=True)
            proposal_file = raw_dir / f"proposal-{_ACTIVE_EPISODE.get('call_index', 0)}.json"
            proposal_file.write_text(json.dumps({"response": obj, "diagnostics": diagnostics,
                                                 "failed_gates": failed}, indent=2) + "\n", encoding="utf-8")
        return obj
    except Exception as exc: return {"status":"FAILED","error":type(exc).__name__+": "+str(exc)}
def _main_impl() -> int:
    ap=argparse.ArgumentParser(); ap.add_argument("--handoff",type=Path,required=True); ap.add_argument("--compile",type=Path,required=True); ap.add_argument("--verification",type=Path,required=True); ap.add_argument("--episode-id"); args=ap.parse_args()
    h=json.loads(args.handoff.read_text(encoding="utf-8-sig")); source=Path(h["source"]); adapter=family(source, source.read_text(encoding="utf-8-sig",errors="replace")); attempts=[]
    admission_refresh = None
    # Repair this infrastructure/provenance defect only when the canonical
    # failure event and its same-cycle verifier report both identify the
    # admission receipt as the sole failed check. Never infer permission from
    # an error string or rewrite a receipt hash directly.
    if _ACTIVE_EPISODE is not None:
        identity = _ACTIVE_EPISODE["identity"]
        event_path = Path(identity["failure_event_path"])
        event = json.loads(event_path.read_text(encoding="utf-8-sig"))
        if event.get("failed_checks") == ["generated_source_review_current"]:
            error_text = str(event.get("error") or "")
            report_path = Path(event.get("verification") or "")
            report = json.loads(report_path.read_text(encoding="utf-8-sig")) if report_path.is_file() else {}
            checks = report.get("checks") if isinstance(report.get("checks"), dict) else {}
            failures = sorted(name for name, value in checks.items() if value is False)
            if (event.get("cycle_id") != h.get("cycle_id")
                    or event.get("ea") != h.get("ea")
                    or "generated source build receipt does not authenticate this cycle/spec/source" not in error_text
                    or report_path.resolve() != args.verification.resolve()
                    or report.get("cycle_id") != h.get("cycle_id")
                    or report.get("stage") != 2
                    or report.get("passed") is not False
                    or failures != ["generated_source_review_current"]
                    or str(report.get("source_sha256") or "").upper() != sha(source)
                    or Path(report.get("source") or "").resolve() != source.resolve()
                    or str(report.get("stage1_spec_sha256") or "").upper() != str(h.get("stage1_spec_sha256") or "").upper()):
                raise RuntimeError("stale-admission repair refused: authenticated event and verifier evidence do not agree")
            admission_refresh = refresh_generated_source_admission(
                args.handoff, source,
                {"kind": "generated_source_admission_refresh",
                 "failure_event_sha256": identity["failure_digest"],
                 "verifier_sha256": sha(report_path),
                 "reason": "authenticated generated_source_review_current failure"})
            if not admission_refresh:
                raise RuntimeError("stale-admission repair refused: current source is already identical to its admitted parent")
            _ACTIVE_EPISODE["admission_refreshed"] = admission_refresh
    # Establish a baseline before touching EA source. Clean verification and
    # performance-only failures are never repair triggers.
    if reusable_failed_compile_evidence(args.compile, source, h["cycle_id"]):
        crc, cout = 2, "Reused authenticated failed compile receipt for unchanged source; preserving compile budget for a changed repair."
    else:
        crc,cout=run([str(COMPILE),"--handoff",str(args.handoff),"--output",str(args.compile)])
    vrc,vout=run([str(VERIFY),"--handoff",str(args.handoff),"--compile",str(args.compile),"--output",str(args.verification)])
    verification=json.loads(args.verification.read_text(encoding="utf-8")) if args.verification.is_file() else {}
    failed=[k for k,v in verification.get("checks",{}).items() if v is False]
    eligible,reason=repair_eligibility(failed,cout+"\n"+vout+"\n"+mt5_journal_evidence())
    attempts.append({"attempt":0,"baseline":True,"before_sha256":sha(source),"compile_returncode":crc,"verify_returncode":vrc,"checks":verification.get("checks",{}),"verification_passed":verification.get("passed") is True,"repair_eligible":eligible,"repair_reason":reason,"source_admission_refresh":admission_refresh})
    zero_trade_mode = h.get("stage3_zero_trade_repair") is True
    runtime_repair_mode = isinstance(h.get("stage3_runtime_repair"), dict)
    runtime_repair = h.get("stage3_runtime_repair") or {}
    if zero_trade_mode:
        eligible, reason = True, "authenticated_stage3_all_zero_trade_technical_evidence"
    if runtime_repair_mode:
        eligible, reason = True, "authenticated_stage3_tester_runtime_defect"
    if runtime_repair.get("kind") == "stage3_tester_journal_multidimensional_array_row_bounds":
        parent_sha = str(runtime_repair.get("parent_source_sha256") or "").upper()
        current_sha = sha(source)
        from kcos_stage2_verify_v2 import multidimensional_array_row_bounds_are_safe
        bounds_safe = multidimensional_array_row_bounds_are_safe(
            source.read_text(encoding="utf-8-sig", errors="replace"))
        if (verification.get("passed") is True
                and all(value is True for value in (verification.get("checks") or {}).values())
                and current_sha != parent_sha
                and bounds_safe):
            report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
            report.parent.mkdir(parents=True, exist_ok=True)
            attempts.append({"attempt": 1, "kind": "authenticated_stage3_multidimensional_array_bounds_requalification",
                             "parent_source_sha256": parent_sha, "after_source_sha256": current_sha,
                             "verification_passed": True, "checks": verification.get("checks", {})})
            report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
                "ea": h["ea"], "adapter": adapter, "attempts": attempts,
                "max_attempts": MAX_PROPOSALS, "repair_reason": reason,
                "fresh_repair_verification_passed": True}, indent=2) + "\n", encoding="utf-8")
            print(json.dumps({"attempts": 1, "report": str(report), "passed": True,
                              "repair_reason": "authenticated repaired source requalified"}))
            return 0
        if verification.get("passed") is True:
            report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
                "ea": h["ea"], "adapter": adapter, "attempts": attempts,
                "max_attempts": MAX_PROPOSALS, "repair_reason": reason,
                "fresh_repair_verification_passed": False,
                "hold_reason": "passing source is not hash-distinct from the failing Stage 3 parent"}, indent=2) + "\n",
                encoding="utf-8")
            print(json.dumps({"attempts": 0, "report": str(report), "passed": False,
                              "repair_reason": "row-bounds runtime failure has no proven changed source"}))
            return 2
    if (verification.get("passed") is True and not zero_trade_mode and not runtime_repair_mode) or not eligible:
        report=ROOT/"state"/"reports"/f"{h['cycle_id']}.stage2.repair-loop.json"; report.parent.mkdir(parents=True,exist_ok=True); report.write_text(json.dumps({"schema_version":2,"cycle_id":h['cycle_id'],"ea":h['ea'],"adapter":adapter,"attempts":attempts,"max_attempts":MAX_PROPOSALS,"source_admission_refresh":admission_refresh},indent=2)+"\n",encoding="utf-8")
        print(json.dumps({"attempts":0,"report":str(report),"passed":verification.get("passed") is True,"repair_reason":reason}))
        return 0
    nan_compile_evidence = compiler_log_evidence(args.compile)
    ctrade_api_errors = ("SetDeviationsInPoints", "SetTypeFillingByVolume")
    if (failed == ["compile_clean"]
            and all(re.search(r"(?i)undeclared identifier\s+'" + re.escape(name) + r"'",
                              nan_compile_evidence) for name in ctrade_api_errors)):
        before_bytes = source.read_bytes()
        deterministic = apply_authenticated_ctrade_api_fix(source, nan_compile_evidence)
        archive_dir = ROOT / "state" / "diagnostics" / "stage2_source_repairs" / h["cycle_id"]
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive = archive_dir / f"candidate-before-{hashlib.sha256(before_bytes).hexdigest().upper()}.mq5"
        if not archive.exists():
            archive.write_bytes(before_bytes)
        if deterministic and deterministic.get("status") == "APPLIED":
            if locked_strategy_surface(before_bytes.decode("utf-8-sig", errors="replace")) != locked_strategy_surface(
                    source.read_text(encoding="utf-8-sig", errors="replace")):
                source.write_bytes(before_bytes)
                deterministic = {"status": "PATCH_REJECTED", "reason": "locked strategy surface changed"}
            else:
                try:
                    admission = refresh_generated_source_admission(
                        args.handoff, source, {"kind": "mql5_ctrade_api_repair",
                                               "changes": deterministic.get("changes", [])})
                except Exception as exc:
                    source.write_bytes(before_bytes)
                    deterministic = {"status": "PATCH_REJECTED", "reason":
                                     "source admission refresh failed: " + type(exc).__name__ + ":" + str(exc)}
                else:
                    crc, cout = run([str(COMPILE), "--handoff", str(args.handoff), "--output", str(args.compile)])
                    vrc, vout = run([str(VERIFY), "--handoff", str(args.handoff), "--compile", str(args.compile), "--output", str(args.verification)])
                    repaired_verification = json.loads(args.verification.read_text(encoding="utf-8")) if args.verification.is_file() else {}
                    passed = repaired_verification.get("passed") is True
                    attempts.append({"attempt": 1, "kind": "deterministic_mql5_ctrade_api_repair",
                                     "deterministic": deterministic,
                                     "before_sha256": hashlib.sha256(before_bytes).hexdigest().upper(),
                                     "after_sha256": sha(source), "source_admission_refresh": admission,
                                     "compile_returncode": crc, "verify_returncode": vrc,
                                     "checks": repaired_verification.get("checks", {}),
                                     "verification_passed": passed})
                    report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
                    report.parent.mkdir(parents=True, exist_ok=True)
                    report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
                                                  "ea": h["ea"], "adapter": adapter,
                                                  "attempts": attempts, "max_attempts": MAX_PROPOSALS}, indent=2) + "\n",
                                      encoding="utf-8")
                    print(json.dumps({"attempts": 1, "report": str(report), "passed": passed,
                                      "repair_reason": "compiler-proven unsupported CTrade API replacement"}))
                    return 0
    if failed == ["compile_clean"] and re.search(
            r"(?i):\s*error\s+198\s*:.*division by zero in the constant expression",
            nan_compile_evidence):
        before_bytes = source.read_bytes()
        deterministic = apply_known_mql5_compile_fix(source, nan_compile_evidence)
        archive_dir = ROOT / "state" / "diagnostics" / "stage2_source_repairs" / h["cycle_id"]
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive = archive_dir / f"candidate-before-{hashlib.sha256(before_bytes).hexdigest().upper()}.mq5"
        if not archive.exists():
            archive.write_bytes(before_bytes)
        if deterministic and deterministic.get("status") == "APPLIED":
            if locked_strategy_surface(before_bytes.decode("utf-8-sig", errors="replace")) != locked_strategy_surface(
                    source.read_text(encoding="utf-8-sig", errors="replace")):
                source.write_bytes(before_bytes)
                deterministic = {"status": "PATCH_REJECTED", "reason": "locked strategy surface changed"}
            else:
                deterministic["changed"] = True
                deterministic["kind"] = "mql5_nan_sentinel_replaced"
                try:
                    admission = refresh_generated_source_admission(
                        args.handoff, source, {"kind": deterministic["kind"],
                                               "changes": deterministic.get("changes", [])})
                except Exception as exc:
                    source.write_bytes(before_bytes)
                    deterministic = {"status": "PATCH_REJECTED", "reason":
                                     "source admission refresh failed: " + type(exc).__name__ + ":" + str(exc)}
                else:
                    crc, cout = run([str(COMPILE), "--handoff", str(args.handoff), "--output", str(args.compile)])
                    vrc, vout = run([str(VERIFY), "--handoff", str(args.handoff), "--compile", str(args.compile), "--output", str(args.verification)])
                    repaired_verification = json.loads(args.verification.read_text(encoding="utf-8")) if args.verification.is_file() else {}
                    checks = repaired_verification.get("checks", {})
                    passed = repaired_verification.get("passed") is True
                    attempts.append({"attempt": 1, "kind": "deterministic_mql5_nan_sentinel",
                                     "deterministic": deterministic,
                                     "before_sha256": hashlib.sha256(before_bytes).hexdigest().upper(),
                                     "after_sha256": sha(source), "source_admission_refresh": admission,
                                     "compile_returncode": crc, "verify_returncode": vrc,
                                     "checks": checks, "verification_passed": passed})
                    report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
                    report.parent.mkdir(parents=True, exist_ok=True)
                    report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
                                                  "ea": h["ea"], "adapter": adapter,
                                                  "attempts": attempts, "max_attempts": MAX_PROPOSALS}, indent=2) + "\n",
                                      encoding="utf-8")
                    print(json.dumps({"attempts": 1, "report": str(report), "passed": passed,
                                      "repair_reason": "compiler-proven 0/0 NaN sentinel portability repair"}))
                    return 0
    first_generic_attempt = 2 if zero_trade_mode else 1
    if (runtime_repair_mode
            and runtime_repair.get("kind") != "stage3_tester_journal_quantile_interpolation_bounds"):
        repair_evidence = h["stage3_runtime_repair"]["diagnostic"]
        evidence_text = ("Authenticated fresh Stage 3 Tester Journal evidence bound by the recovery ticket\n"
                         + json.dumps(repair_evidence, ensure_ascii=False, sort_keys=True)
                         + "\nBaseline Stage 2 compile/verification evidence\n" + cout + "\n" + vout)
        baseline_sha = sha(source)
        baseline_surface = locked_strategy_surface(source.read_text(encoding="utf-8-sig", errors="replace"))
        repair_passed = False
        feedback = evidence_text
        for attempt_no in range(1, MAX_PROPOSALS + 1):
            proposal = ai_proposal(source, feedback, ["stage3_authenticated_invalid_stops_order_rejection"], handoff=h)
            record = {"attempt": attempt_no, "kind": "stage3_authenticated_invalid_stops",
                      "ai": {k: v for k, v in proposal.items() if k != "patch"},
                      "input_source_sha256": sha(source)}
            attempts.append(record)
            if proposal.get("status") != "PROPOSED" or not proposal.get("patch"):
                feedback += "\nPrior proposal was unavailable or empty; return a new contract-supported repair or an explicit refusal."
                continue
            before_bytes = source.read_bytes()
            applied = safe_patch(source.parent, proposal["patch"], source)
            record["ai_application"] = applied
            if applied.get("status") != "APPLIED":
                feedback += "\nPrior patch was rejected: " + str(applied.get("reason") or applied.get("status"))
                continue
            current_text = source.read_text(encoding="utf-8-sig", errors="replace")
            if locked_strategy_surface(current_text) != baseline_surface:
                source.write_bytes(before_bytes)
                record["ai_application"] = {"status": "PATCH_REJECTED",
                    "reason": "locked strategy calculations, signal control flow, regimes, or score callback changed"}
                feedback += "\nPrior patch changed a locked strategy surface; keep contract math/signals unchanged and make only the evidenced execution repair."
                continue
            try:
                record["source_admission_refresh"] = refresh_generated_source_admission(
                    args.handoff, source, {"kind": "ai_stage3_invalid_stops_repair",
                        "ticket_sha256": h["stage3_runtime_repair"]["ticket_sha256"],
                        "patch_sha256": hashlib.sha256(proposal["patch"].encode("utf-8")).hexdigest().upper()})
            except Exception as exc:
                source.write_bytes(before_bytes)
                record["ai_application"] = {"status": "PATCH_REJECTED", "reason": "post-repair deterministic admission failed: " + str(exc)}
                feedback += "\nPrior patch failed deterministic source admission: " + str(exc)
                continue
            crc, cout = run([str(COMPILE), "--handoff", str(args.handoff), "--output", str(args.compile)])
            vrc, vout = run([str(VERIFY), "--handoff", str(args.handoff), "--compile", str(args.compile), "--output", str(args.verification)])
            repaired = json.loads(args.verification.read_text(encoding="utf-8-sig")) if args.verification.is_file() else {}
            record.update({"compile_returncode": crc, "verify_returncode": vrc,
                           "checks": repaired.get("checks", {}), "verification_passed": repaired.get("passed") is True,
                           "verified_source_sha256": repaired.get("source_sha256")})
            if (sha(source) != baseline_sha and crc == 0 and vrc == 0
                    and verification_passes_for_candidate(repaired, h["cycle_id"], source)):
                repair_passed = True
                break
            feedback += "\nFresh compile/verification did not pass for the changed source. Diagnostics:\n" + cout + "\n" + vout
        report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
            "ea": h["ea"], "adapter": adapter, "attempts": attempts,
            "max_attempts": MAX_PROPOSALS, "repair_reason": reason,
            "baseline_source_sha256": baseline_sha, "repair_source_changed": sha(source) != baseline_sha,
            "fresh_repair_verification_passed": repair_passed}, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"attempts": len(attempts), "report": str(report), "passed": repair_passed,
                          "repair_reason": reason}))
        return 0 if repair_passed else 2
    if "multidimensional_array_row_bounds_safe" in failed:
        before_bytes = source.read_bytes()
        deterministic = apply_multidimensional_array_row_bounds_fix(source)
        attempts.append({"attempt": 1, "kind": "deterministic_multidimensional_array_row_bounds",
                         "deterministic": deterministic,
                         "before_sha256": hashlib.sha256(before_bytes).hexdigest().upper(),
                         "after_sha256": sha(source)})
        if deterministic.get("status") != "APPLIED":
            attempts[-1]["verification_passed"] = False
            report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
                "ea": h["ea"], "adapter": adapter, "attempts": attempts,
                "max_attempts": MAX_PROPOSALS}, indent=2) + "\n", encoding="utf-8")
            print(json.dumps({"attempts": 1, "report": str(report), "passed": False,
                              "repair_reason": "deterministic multidimensional row-bounds patch rejected"}))
            return 2
        try:
            attempts[-1]["source_admission_refresh"] = refresh_generated_source_admission(
                args.handoff, source, {"kind": deterministic["method"],
                                       "before_sha256": deterministic["before_sha256"],
                                       "after_sha256": deterministic["after_sha256"],
                                       "replacements": deterministic["replacements"]})
        except Exception as exc:
            source.write_bytes(before_bytes)
            attempts[-1].update({"deterministic": {"status": "PATCH_REJECTED", "reason": str(exc)},
                                 "after_sha256": hashlib.sha256(before_bytes).hexdigest().upper(),
                                 "verification_passed": False})
            report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
                "ea": h["ea"], "adapter": adapter, "attempts": attempts,
                "max_attempts": MAX_PROPOSALS}, indent=2) + "\n", encoding="utf-8")
            print(json.dumps({"attempts": 1, "report": str(report), "passed": False,
                              "repair_reason": "source-admission refresh rejected row-bounds repair"}))
            return 2
        crc, cout = run([str(COMPILE), "--handoff", str(args.handoff), "--output", str(args.compile)])
        vrc, vout = run([str(VERIFY), "--handoff", str(args.handoff), "--compile", str(args.compile),
                         "--output", str(args.verification)])
        verification = json.loads(args.verification.read_text(encoding="utf-8")) if args.verification.is_file() else {}
        attempts[-1].update({"compile_returncode": crc, "verify_returncode": vrc,
                             "checks": verification.get("checks", {}),
                             "verification_passed": verification.get("passed") is True})
        report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
            "ea": h["ea"], "adapter": adapter, "attempts": attempts,
            "max_attempts": MAX_PROPOSALS}, indent=2) + "\n", encoding="utf-8")
        passed = verification.get("passed") is True and all(
            value is True for value in (verification.get("checks") or {}).values())
        print(json.dumps({"attempts": 1, "report": str(report), "passed": passed,
                          "repair_reason": "deterministic multidimensional row-bounds repair"}))
        return 0 if passed else 2
    if runtime_repair.get("kind") == "stage3_tester_journal_quantile_interpolation_bounds":
        before_bytes = source.read_bytes()
        deterministic = apply_quantile_interpolation_bounds_fix(source)
        attempts.append({"attempt": 1, "kind": "deterministic_quantile_interpolation_bounds",
                         "deterministic": deterministic,
                         "before_sha256": hashlib.sha256(before_bytes).hexdigest().upper(),
                         "after_sha256": sha(source)})
        if deterministic.get("status") != "APPLIED":
            attempts[-1]["verification_passed"] = False
            report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
                "ea": h["ea"], "adapter": adapter, "attempts": attempts,
                "max_attempts": MAX_PROPOSALS}, indent=2) + "\n", encoding="utf-8")
            print(json.dumps({"attempts": 1, "report": str(report), "passed": False,
                              "repair_reason": "quantile interpolation source shape rejected"}))
            return 2
        try:
            attempts[-1]["source_admission_refresh"] = refresh_generated_source_admission(
                args.handoff, source, {"kind": deterministic["method"],
                                       "before_sha256": deterministic["before_sha256"],
                                       "after_sha256": deterministic["after_sha256"],
                                       "upper_index_guard": deterministic["upper_index_guard"]})
        except Exception as exc:
            source.write_bytes(before_bytes)
            attempts[-1].update({"deterministic": {"status": "PATCH_REJECTED", "reason": str(exc)},
                                 "after_sha256": hashlib.sha256(before_bytes).hexdigest().upper(),
                                 "verification_passed": False})
            report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
                "ea": h["ea"], "adapter": adapter, "attempts": attempts,
                "max_attempts": MAX_PROPOSALS}, indent=2) + "\n", encoding="utf-8")
            print(json.dumps({"attempts": 1, "report": str(report), "passed": False,
                              "repair_reason": "quantile repair admission rejected"}))
            return 2
        crc, cout = run([str(COMPILE), "--handoff", str(args.handoff), "--output", str(args.compile)])
        vrc, vout = run([str(VERIFY), "--handoff", str(args.handoff), "--compile", str(args.compile),
                         "--output", str(args.verification)])
        repaired = json.loads(args.verification.read_text(encoding="utf-8-sig")) if args.verification.is_file() else {}
        passed = (crc == 0 and vrc == 0 and repaired.get("passed") is True
                  and all(value is True for value in (repaired.get("checks") or {}).values())
                  and quantile_interpolation_bounds_are_safe(
                      source.read_text(encoding="utf-8-sig", errors="replace")))
        attempts[-1].update({"compile_returncode": crc, "verify_returncode": vrc,
                             "checks": repaired.get("checks", {}),
                             "verification_passed": passed,
                             "verified_source_sha256": repaired.get("source_sha256")})
        report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
            "ea": h["ea"], "adapter": adapter, "attempts": attempts,
            "max_attempts": MAX_PROPOSALS, "repair_reason": reason,
            "fresh_repair_verification_passed": passed,
            "diagnostics": (cout + "\n" + vout)[-4000:]}, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"attempts": 1, "report": str(report), "passed": passed,
                          "repair_reason": "authenticated quantile interpolation array-bounds repair"}))
        return 0 if passed else 2
    if "dcc_closed_return_arrays_bounded" in failed:
        before_bytes = source.read_bytes()
        deterministic = apply_dcc_closed_return_bounds_fix(source)
        attempts.append({"attempt": 1, "kind": "deterministic_dcc_closed_return_bounds",
                         "deterministic": deterministic,
                         "before_sha256": hashlib.sha256(before_bytes).hexdigest().upper(),
                         "after_sha256": sha(source)})
        if deterministic.get("status") != "APPLIED":
            # This repair is shape-specific. An unrecognized source must stay
            # held; model proposals may not broaden this authenticated patch.
            attempts[-1]["verification_passed"] = False
            report=ROOT/"state"/"reports"/f"{h['cycle_id']}.stage2.repair-loop.json"
            report.parent.mkdir(parents=True,exist_ok=True)
            report.write_text(json.dumps({"schema_version":2,"cycle_id":h['cycle_id'],"ea":h['ea'],"adapter":adapter,"attempts":attempts,"max_attempts":MAX_PROPOSALS},indent=2)+"\n",encoding="utf-8")
            print(json.dumps({"attempts":1,"report":str(report),"passed":False,"repair_reason":"deterministic DCC source shape rejected"}))
            return 0
        try:
            attempts[-1]["source_admission_refresh"] = refresh_generated_source_admission(
                args.handoff, source, {"kind": deterministic["method"], "replacements": deterministic["replacements"]})
            from kcos_stage2_verify_v2 import dcc_closed_return_arrays_are_bounded
            if not dcc_closed_return_arrays_are_bounded(
                    source.read_text(encoding="utf-8-sig", errors="replace"), required_dcc=True):
                raise RuntimeError("post-admission DCC closed-return gate rejected source")
        except Exception as exc:
            source.write_bytes(before_bytes)
            attempts[-1].update({"deterministic": {"status": "PATCH_REJECTED", "reason": str(exc)},
                                 "after_sha256": hashlib.sha256(before_bytes).hexdigest().upper(),
                                 "verification_passed": False})
            report=ROOT/"state"/"reports"/f"{h['cycle_id']}.stage2.repair-loop.json"
            report.parent.mkdir(parents=True,exist_ok=True)
            report.write_text(json.dumps({"schema_version":2,"cycle_id":h['cycle_id'],"ea":h['ea'],"adapter":adapter,"attempts":attempts,"max_attempts":MAX_PROPOSALS},indent=2)+"\n",encoding="utf-8")
            print(json.dumps({"attempts":1,"report":str(report),"passed":False,"repair_reason":"post-repair DCC gate rejected source"}))
            return 0
        crc,cout=run([str(COMPILE),"--handoff",str(args.handoff),"--output",str(args.compile)])
        vrc,vout=run([str(VERIFY),"--handoff",str(args.handoff),"--compile",str(args.compile),"--output",str(args.verification)])
        verification=json.loads(args.verification.read_text(encoding="utf-8")) if args.verification.is_file() else {}
        attempts[-1].update({"compile_returncode":crc,"verify_returncode":vrc,
                             "checks":verification.get("checks",{}),
                             "verification_passed":verification.get("passed") is True})
        if verification.get("passed") is True:
            report=ROOT/"state"/"reports"/f"{h['cycle_id']}.stage2.repair-loop.json"
            report.parent.mkdir(parents=True,exist_ok=True)
            report.write_text(json.dumps({"schema_version":2,"cycle_id":h['cycle_id'],"ea":h['ea'],"adapter":adapter,"attempts":attempts,"max_attempts":MAX_PROPOSALS},indent=2)+"\n",encoding="utf-8")
            print(json.dumps({"attempts":1,"report":str(report),"passed":True,"repair_reason":reason}))
            return 0
        failed=[k for k,v in verification.get("checks",{}).items() if v is False]
        if "dcc_closed_return_arrays_bounded" in failed:
            # Never ask a model to bypass the deterministic closed-return gate.
            report=ROOT/"state"/"reports"/f"{h['cycle_id']}.stage2.repair-loop.json"
            report.parent.mkdir(parents=True,exist_ok=True)
            report.write_text(json.dumps({"schema_version":2,"cycle_id":h['cycle_id'],"ea":h['ea'],"adapter":adapter,"attempts":attempts,"max_attempts":MAX_PROPOSALS},indent=2)+"\n",encoding="utf-8")
            print(json.dumps({"attempts":1,"report":str(report),"passed":False,"repair_reason":"DCC closed-return gate failed after verification"}))
            return 0
        first_generic_attempt = 2
    if zero_trade_mode:
        before_bytes = source.read_bytes()
        deterministic = apply_rolling_volatility_history_window_fix(source)
        if deterministic.get("status") == "APPLIED":
            try:
                admission = refresh_generated_source_admission(args.handoff, source,
                    {"kind": deterministic["method"], "before_sha256": deterministic["before_sha256"],
                     "after_sha256": deterministic["after_sha256"],
                     "stage3_zero_trade_evidence": h.get("stage3_zero_trade_evidence")})
            except Exception as exc:
                source.write_bytes(before_bytes)
                deterministic.update({"status": "PATCH_REJECTED", "admission_error": type(exc).__name__ + ":" + str(exc)})
            if deterministic.get("status") == "APPLIED":
                crc, cout = run([str(COMPILE), "--handoff", str(args.handoff), "--output", str(args.compile)])
                vrc, vout = run([str(VERIFY), "--handoff", str(args.handoff), "--compile", str(args.compile), "--output", str(args.verification)])
                verification = json.loads(args.verification.read_text(encoding="utf-8")) if args.verification.is_file() else {}
                attempts.append({"attempt": 1, "kind": deterministic["method"], "deterministic": deterministic,
                    "source_admission_refresh": admission, "compile_returncode": crc, "verify_returncode": vrc,
                    "checks": verification.get("checks", {}), "verification_passed": verification.get("passed") is True})
                report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"], "ea": h["ea"],
                    "adapter": adapter, "attempts": attempts, "max_attempts": MAX_PROPOSALS}, indent=2) + "\n", encoding="utf-8")
                print(json.dumps({"attempts": 1, "report": str(report), "passed": verification.get("passed") is True,
                    "repair_reason": "authenticated_all_zero_trade_rolling_window_history_defect"}))
                return 0 if verification.get("passed") is True else 2
        before_bytes = source.read_bytes()
        deterministic = apply_semivariance_closed_bar_bounds_fix(source)
        if deterministic.get("status") == "APPLIED":
            try:
                admission = refresh_generated_source_admission(
                    args.handoff, source,
                    {"kind": deterministic["method"], "before_sha256": deterministic["before_sha256"],
                     "after_sha256": deterministic["after_sha256"],
                     "stage3_zero_trade_evidence": h.get("stage3_zero_trade_evidence")})
            except Exception as exc:
                source.write_bytes(before_bytes)
                deterministic.update({"status": "PATCH_REJECTED",
                                      "admission_error": type(exc).__name__ + ":" + str(exc)})
                attempts.append({"attempt": 1, "kind": "deterministic_semivariance_closed_bar",
                                 "deterministic": deterministic, "before_sha256": sha256_bytes(before_bytes),
                                 "after_sha256": sha(source)})
                admission = None
            if deterministic.get("status") == "APPLIED":
                crc, cout = run([str(COMPILE), "--handoff", str(args.handoff), "--output", str(args.compile)])
                vrc, vout = run([str(VERIFY), "--handoff", str(args.handoff), "--compile", str(args.compile),
                                 "--output", str(args.verification)])
                verification = json.loads(args.verification.read_text(encoding="utf-8")) if args.verification.is_file() else {}
                attempts.append({"attempt": 1, "kind": "deterministic_semivariance_closed_bar",
                                 "deterministic": deterministic, "source_admission_refresh": admission,
                                 "compile_returncode": crc, "verify_returncode": vrc,
                                 "checks": verification.get("checks", {}),
                                 "verification_passed": verification.get("passed") is True})
                report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
                                              "ea": h["ea"], "adapter": adapter,
                                              "attempts": attempts, "max_attempts": MAX_PROPOSALS}, indent=2) + "\n",
                                  encoding="utf-8")
                print(json.dumps({"attempts": 1, "report": str(report),
                                  "passed": verification.get("passed") is True,
                                  "repair_reason": "authenticated_all_zero_trade_closed_bar_indexing_defect"}))
                return 0 if verification.get("passed") is True else 2
        before_bytes = source.read_bytes()
        deterministic = apply_volatility_clustering_indexed_buffer_fix(source)
        if deterministic.get("status") == "APPLIED":
            try:
                admission = refresh_generated_source_admission(
                    args.handoff, source,
                    {"kind": deterministic["method"],
                     "before_sha256": deterministic["before_sha256"],
                     "after_sha256": deterministic["after_sha256"],
                     "stage3_zero_trade_evidence": h.get("stage3_zero_trade_evidence")})
            except Exception as exc:
                source.write_bytes(before_bytes)
                deterministic.update({"status": "PATCH_REJECTED",
                                      "admission_error": type(exc).__name__ + ":" + str(exc)})
            if deterministic.get("status") == "APPLIED":
                crc, cout = run([str(COMPILE), "--handoff", str(args.handoff), "--output", str(args.compile)])
                vrc, vout = run([str(VERIFY), "--handoff", str(args.handoff), "--compile", str(args.compile),
                                 "--output", str(args.verification)])
                verification = json.loads(args.verification.read_text(encoding="utf-8-sig")) \
                    if args.verification.is_file() else {}
                passed = (crc == 0 and vrc == 0
                          and verification_passes_for_candidate(verification, h["cycle_id"], source))
                attempts.append({"attempt": 1, "kind": deterministic["method"],
                                 "deterministic": deterministic,
                                 "source_admission_refresh": admission,
                                 "compile_returncode": crc, "verify_returncode": vrc,
                                 "checks": verification.get("checks", {}),
                                 "verification_passed": passed})
                report = ROOT / "state" / "reports" / f"{h['cycle_id']}.stage2.repair-loop.json"
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text(json.dumps({"schema_version": 2, "cycle_id": h["cycle_id"],
                    "ea": h["ea"], "adapter": adapter, "attempts": attempts,
                    "max_attempts": MAX_PROPOSALS,
                    "repair_reason": "authenticated_stage3_zero_trades_impossible_cluster_guard_and_buffer_bounds",
                    "fresh_repair_verification_passed": passed}, indent=2) + "\n", encoding="utf-8")
                print(json.dumps({"attempts": 1, "report": str(report), "passed": passed,
                    "repair_reason": "authenticated_stage3_zero_trades_impossible_cluster_guard_and_buffer_bounds"}))
                return 0 if passed else 2
        proposal=ai_proposal(source, cout+"\n"+vout, ["stage3_all_zero_trade_technical_evidence"], handoff=h)
        attempts.append({"attempt":1,"ai":{k:v for k,v in proposal.items() if k!="patch"}})
        if proposal.get("status")=="PROPOSED" and proposal.get("patch"):
            ai_before = source.read_bytes()
            ai_surface = locked_strategy_surface(ai_before.decode("utf-8-sig", errors="replace"))
            applied=safe_patch(source.parent,proposal["patch"],source)
            attempts[-1]["ai_application"]=applied
            if applied.get("status")=="APPLIED":
                if locked_strategy_surface(source.read_text(encoding="utf-8-sig", errors="replace")) != ai_surface:
                    source.write_bytes(ai_before)
                    applied={"status":"PATCH_REJECTED", "reason":"locked strategy inputs, regimes, or score callback changed"}
                else:
                    try:
                        attempts[-1]["source_admission_refresh"] = refresh_generated_source_admission(
                            args.handoff, source, {"kind":"ai_zero_trade_compile_repair",
                                                   "patch_sha256":hashlib.sha256(proposal["patch"].encode("utf-8")).hexdigest().upper()})
                    except Exception as exc:
                        source.write_bytes(ai_before)
                        applied={"status":"PATCH_REJECTED", "reason":"post-repair deterministic admission failed: "+str(exc)}
                attempts[-1]["ai_application"]=applied
            if applied.get("status")=="APPLIED":
                crc,cout=run([str(COMPILE),"--handoff",str(args.handoff),"--output",str(args.compile)])
                vrc,vout=run([str(VERIFY),"--handoff",str(args.handoff),"--compile",str(args.compile),"--output",str(args.verification)])
                ai_verification = json.loads(args.verification.read_text(encoding="utf-8-sig")) if args.verification.is_file() else {}
                attempts[-1].update({"compile_returncode":crc,"verify_returncode":vrc,"verification_passed":ai_verification.get("passed") is True})
                if attempts[-1]["verification_passed"]:
                    report=ROOT/"state"/"reports"/f"{h['cycle_id']}.stage2.repair-loop.json"; report.parent.mkdir(parents=True,exist_ok=True); report.write_text(json.dumps({"schema_version":2,"cycle_id":h['cycle_id'],"ea":h['ea'],"adapter":adapter,"attempts":attempts,"max_attempts":MAX_PROPOSALS},indent=2)+"\n",encoding="utf-8")
                    print(json.dumps({"attempts":1,"report":str(report),"passed":True,"repair_reason":reason})); return 0
    for n in range(first_generic_attempt, MAX_PROPOSALS + 1):
        before=sha(source); before_bytes=source.read_bytes(); rc,out=run([str(REPAIR),str(args.handoff)])
        deterministic={"returncode":rc,"output":out,"changed":sha(source)!=before}
        attempts.append({"attempt":n,"adapter":adapter,"adapter_constraints":constraints(adapter),"deterministic":deterministic,"before_sha256":before,"after_sha256":sha(source)})
        if rc: break
        if deterministic["changed"]:
            try:
                repair_record={"kind":"deterministic_stage2_repair_tool",
                              "tool_output_sha256":hashlib.sha256(out.encode("utf-8")).hexdigest().upper(),
                              "before_sha256":before,"after_sha256":sha(source)}
                attempts[-1]["source_admission_refresh"]=refresh_generated_source_admission(
                    args.handoff, source, repair_record)
            except Exception as exc:
                source.write_bytes(before_bytes)
                attempts[-1]["deterministic"].update({"status":"PATCH_REJECTED",
                                                       "admission_refresh_error":type(exc).__name__+":"+str(exc)})
                attempts[-1]["after_sha256"]=sha(source)
                attempts[-1]["verification_passed"]=False
                break
        crc,cout=run([str(COMPILE),"--handoff",str(args.handoff),"--output",str(args.compile)])
        vrc,vout=run([str(VERIFY),"--handoff",str(args.handoff),"--compile",str(args.compile),"--output",str(args.verification)])
        verification=json.loads(args.verification.read_text(encoding="utf-8")) if args.verification.is_file() else {}
        attempts[-1].update({"compile_returncode":crc,"verify_returncode":vrc,"checks":verification.get("checks",{}),"verification_passed":verification.get("passed") is True})
        if verification.get("passed") is True: break
        failed=[k for k,v in verification.get("checks",{}).items() if v is False]
        eligible,reason=repair_eligibility(failed,cout+"\n"+vout+"\n"+mt5_journal_evidence())
        attempts[-1].update({"repair_eligible":eligible,"repair_reason":reason})
        if not eligible: break
        compiler_evidence=compiler_log_evidence(args.compile)
        source_before_compile_fix = source.read_bytes()
        compile_fix = apply_known_mql5_compile_fix(source, compiler_evidence)
        if compile_fix is not None:
            attempts[-1]["deterministic_compile_fix"] = compile_fix
            if compile_fix.get("status") != "APPLIED":
                break
            if locked_strategy_surface(source.read_text(encoding="utf-8-sig", errors="replace")) != \
                    locked_strategy_surface(source_before_compile_fix.decode("utf-8-sig", errors="replace")):
                source.write_bytes(source_before_compile_fix)
                attempts[-1]["deterministic_compile_fix"].update(
                    {"status": "PATCH_REJECTED", "reason": "locked strategy inputs, regimes, or score callback changed"})
                break
            try:
                attempts[-1]["source_admission_refresh"] = refresh_generated_source_admission(
                    args.handoff, source,
                    {"kind": compile_fix.get("method", "compiler_proven_mql5_api_fixes"),
                     "changes": compile_fix.get("changes", [])})
            except Exception as exc:
                source.write_bytes(source_before_compile_fix)
                attempts[-1]["deterministic_compile_fix"].update(
                    {"status": "PATCH_REJECTED", "reason": "source admission refresh failed: "
                     + type(exc).__name__ + ":" + str(exc)})
                break
            fix_crc, fix_cout = run([str(COMPILE), "--handoff", str(args.handoff), "--output", str(args.compile)])
            fix_vrc, fix_vout = run([str(VERIFY), "--handoff", str(args.handoff), "--compile", str(args.compile),
                                     "--output", str(args.verification)])
            fixed_verification = json.loads(args.verification.read_text(encoding="utf-8-sig")) \
                if args.verification.is_file() else {}
            attempts[-1].update({"after_sha256": sha(source), "repair_compile_returncode": fix_crc,
                                 "repair_verify_returncode": fix_vrc,
                                 "checks": fixed_verification.get("checks", {}),
                                 "verification_passed": fixed_verification.get("passed") is True})
            if attempts[-1]["verification_passed"]:
                break
            # Keep this lane bounded: a compiler-proven type correction that
            # does not clear verification is preserved for diagnosis, not
            # followed by an unrelated speculative patch.
            break
        ai_evidence=(compiler_evidence if re.search(r"(?i):\s*error\s+\d+\s*:",compiler_evidence)
                     else cout+"\n"+vout+"\n"+compiler_evidence)
        proposal=ai_proposal(source,ai_evidence,failed,handoff=h); attempts[-1]["ai"]={k:v for k,v in proposal.items() if k!="patch"}
        if proposal.get("status")!="PROPOSED" or not proposal.get("patch"): break
        original_bytes=source.read_bytes()
        original_surface=locked_strategy_surface(original_bytes.decode("utf-8-sig",errors="replace"))
        applied=safe_patch(source.parent,proposal["patch"],source)
        attempts[-1]["ai"]["patch_sha256"]=hashlib.sha256(proposal["patch"].encode("utf-8")).hexdigest().upper()
        attempts[-1]["ai"]["patch_chars"]=len(proposal["patch"])
        if applied.get("status")!="APPLIED":
            fallback=apply_known_mql5_compile_fix(source,ai_evidence)
            if fallback and fallback.get("status")=="APPLIED":
                applied=fallback
            else:
                fallback=apply_kcos_regime_enum_order_fix(source,ai_evidence)
                if fallback and fallback.get("status")=="APPLIED":
                    applied=fallback
        if applied.get("status")=="APPLIED":
            patched_surface=locked_strategy_surface(source.read_text(encoding="utf-8-sig",errors="replace"))
            if patched_surface!=original_surface:
                source.write_bytes(original_bytes)
                applied={"status":"PATCH_REJECTED","reason":"locked strategy inputs, regimes, or score callback changed"}
            else:
                try:
                    attempts[-1]["source_admission_refresh"]=refresh_generated_source_admission(
                        args.handoff, source, {"kind":applied.get("method","ai_compile_repair"),
                                               "diagnosis":proposal.get("diagnosis"),
                                               "changes":proposal.get("changes"),
                                               "patch_sha256":attempts[-1]["ai"]["patch_sha256"]})
                except Exception as exc:
                    source.write_bytes(original_bytes)
                    applied={"status":"PATCH_REJECTED","reason":"post-repair deterministic admission failed: "+str(exc)}
        attempts[-1]["ai_application"]=applied
        if applied.get("status")!="APPLIED": break
        if n == 3:
            # A final proposal has no following loop iteration to validate it.
            crc, cout = run([str(COMPILE), "--handoff", str(args.handoff), "--output", str(args.compile)])
            vrc, vout = run([str(VERIFY), "--handoff", str(args.handoff), "--compile", str(args.compile), "--output", str(args.verification)])
            final_check = json.loads(args.verification.read_text(encoding="utf-8-sig")) if args.verification.is_file() else {}
            attempts[-1].update({"compile_returncode": crc, "verify_returncode": vrc,
                                 "checks": final_check.get("checks", {}),
                                 "verification_passed": final_check.get("passed") is True})
            if attempts[-1]["verification_passed"]:
                break
    report=ROOT/"state"/"reports"/f"{h['cycle_id']}.stage2.repair-loop.json"; report.parent.mkdir(parents=True,exist_ok=True); report.write_text(json.dumps({"schema_version":2,"cycle_id":h['cycle_id'],"ea":h['ea'],"adapter":adapter,"attempts":attempts,"max_attempts":MAX_PROPOSALS},indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"attempts":len(attempts)-1,"report":str(report),"passed":attempts[-1].get("verification_passed",False) if attempts else False}))
    return 0


def main() -> int:
    """Run all repair lanes inside one rollback-capable admitted-bundle transaction."""
    global _ACTIVE_EPISODE
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--handoff", type=Path, required=True)
    ap.add_argument("--compile", type=Path, required=True)
    ap.add_argument("--verification", type=Path, required=True)
    ap.add_argument("--episode-id")
    args, _ = ap.parse_known_args()
    try:
        recovered = recover_pending_transactions()
        args.handoff = _require_candidate_path(args.handoff)
        args.compile = _require_candidate_path(args.compile)
        args.verification = _require_candidate_path(args.verification)
        handoff = json.loads(args.handoff.read_text(encoding="utf-8-sig"))
        if bind_stage3_runtime_repair_from_state(handoff):
            write_json_atomic(args.handoff, handoff)
        source = _require_candidate_path(Path(handoff["source"]))
        identity = episode_identity(handoff, source, args.verification, args.episode_id)
        paths = admitted_bundle_paths(args.handoff, handoff, source, args.compile, args.verification)
        baseline = snapshot_bundle(paths)
        attempt_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:10]
        attempt_dir = ROOT / "state" / "diagnostics" / "stage2_repair_transactions" / identity["episode_id"] / attempt_id
        _archive_attempt(attempt_dir, baseline, paths, args.compile, args.verification)
        failure_event_path = Path(identity["failure_event_path"])
        if sha(failure_event_path) != identity["failure_digest"]:
            raise RuntimeError("canonical Stage 2 failure event changed before transaction archive")
        archived_failure = attempt_dir / "canonical-stage2-failure-event.json"
        with archived_failure.open("xb") as stream:
            stream.write(failure_event_path.read_bytes())
            stream.flush()
            os.fsync(stream.fileno())
        (attempt_dir / "episode.json").write_text(json.dumps(identity, indent=2) + "\n", encoding="utf-8")
        journal_path = attempt_dir / "transaction-journal.json"
        write_transaction_journal(journal_path, baseline, "PREPARED")
        proof_path = Path((handoff.get("source_implementation_review") or {}).get("path") or "")
        proof_dir = proof_path.parent if proof_path else None
        before_generated = set(proof_dir.glob("source-admission-repair-*.json")) | set(proof_dir.glob("candidate-parent-build-receipt-*.json")) if proof_dir and proof_dir.is_dir() else set()
        ledger_path = ROOT / "state" / "ledgers" / "stage2_repair_episodes.sqlite3"
        _ACTIVE_EPISODE = {"identity": identity, "ledger": ledger_path, "attempt_dir": attempt_dir,
                           "journal_path": journal_path, "snapshot": baseline,
                           "exhausted": False, "call_index": 0}
        original_surface = locked_strategy_surface(source.read_text(encoding="utf-8-sig", errors="replace"))
        original_adapter = family(source, source.read_text(encoding="utf-8-sig", errors="replace"))
        original_constraints = constraints(original_adapter)
        transaction_snapshot = baseline
        try:
            rc = _main_impl()
            archive_transaction_outputs(attempt_dir, _ACTIVE_EPISODE["snapshot"])
            report_path = ROOT / "state" / "reports" / f"{identity['cycle_id']}.stage2.repair-loop.json"
            if report_path.is_file():
                shutil.copy2(report_path, attempt_dir / "repair-loop-report.json")
            verification = json.loads(args.verification.read_text(encoding="utf-8-sig")) if args.verification.is_file() else {}
            passed = verification_passes_for_candidate(verification, identity["cycle_id"], source)
            current_text = source.read_text(encoding="utf-8-sig", errors="replace")
            if passed and (locked_strategy_surface(current_text) != original_surface
                           or family(source, current_text) != original_adapter
                           or constraints(family(source, current_text)) != original_constraints):
                passed = False
                outcome = "REJECTED"
                (attempt_dir / "strategy-lock-rejection.txt").write_text(
                    "passing verification did not preserve inputs, regime surface, score callback, or strategy constraints\n",
                    encoding="utf-8")
            elif (passed and handoff.get("stage3_zero_trade_repair") is True
                  and sha(source) == sha256_bytes(baseline[str(source.resolve())])):
                passed = False
                outcome = "REPAIR_REQUIRED_NO_CHANGE"
            else:
                exhausted = (_ACTIVE_EPISODE["exhausted"] or
                             episode_proposal_count(ledger_path, identity["episode_id"]) >= MAX_PROPOSALS)
                lineage_changed = bool(_ACTIVE_EPISODE.get("admission_refreshed"))
                outcome = "REPAIRED" if passed and (sha(source) != sha256_bytes(baseline[str(source.resolve())]) or lineage_changed) else "NO_CHANGE" if passed else "EXHAUSTED" if exhausted else "REJECTED"
        except Exception as exc:
            rc = 2
            passed = False
            outcome = "INFRASTRUCTURE_BLOCKED"
            (attempt_dir / "exception.txt").write_text(type(exc).__name__ + ": " + str(exc) + "\n", encoding="utf-8")
        finally:
            if _ACTIVE_EPISODE is not None:
                transaction_snapshot = _ACTIVE_EPISODE["snapshot"]
            _ACTIVE_EPISODE = None
        if not passed:
            if proof_dir and proof_dir.is_dir():
                new_generated = (set(proof_dir.glob("source-admission-repair-*.json")) | set(proof_dir.glob("candidate-parent-build-receipt-*.json"))) - before_generated
                generated_dir = attempt_dir / "generated-admission-evidence"
                if new_generated:
                    generated_dir.mkdir()
                    for item in new_generated:
                        shutil.copy2(item, generated_dir / item.name)
                        item.unlink()
            restore_bundle(transaction_snapshot)
            write_transaction_journal(journal_path, transaction_snapshot, "ROLLED_BACK")
        else:
            write_transaction_journal(journal_path, transaction_snapshot, "COMMITTED")
        (attempt_dir / "outcome.json").write_text(json.dumps({"outcome": outcome, "passed": passed,
            "episode_id": identity["episode_id"], "attempt_dir": str(attempt_dir), "returncode": rc,
            "recovered_prior_transactions": recovered}, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"outcome": outcome, "episode_id": identity["episode_id"],
                          "attempt_dir": str(attempt_dir), "returncode": rc}))
        return 0 if outcome in {"REPAIRED", "NO_CHANGE"} else rc or 2
    except Exception as exc:
        print(json.dumps({"outcome": "INFRASTRUCTURE_BLOCKED", "reason": type(exc).__name__ + ": " + str(exc)}))
        return 2
if __name__ == "__main__": raise SystemExit(main())
