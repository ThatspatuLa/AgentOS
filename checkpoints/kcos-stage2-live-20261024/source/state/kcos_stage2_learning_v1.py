"""Append-only Stage 2 failure observations and evidence-scoped lessons.

The caller must pass the existing repair episode ledger.  This module never
creates a new ledger and never imports or scans historical files unless their
paths are explicitly supplied in a manifest.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


LEDGER_SCHEMA_VERSION = 1
_LESSON_STATES = ("observed", "diagnosed", "validated", "reusable", "retired")
_STATE_RANK = {"observed": 0, "diagnosed": 1, "validated": 2, "reusable": 3, "retired": 4}


@dataclass(frozen=True)
class ObservationResult:
    occurrence_id: str
    failure_fingerprint: str
    inserted: bool


@dataclass(frozen=True)
class ImportReport:
    imported: int
    duplicates: int
    missing_policy_identity: int
    sources: tuple[str, ...]


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def _same_occurrence_payload(first: str, second: str) -> bool:
    """Ignore delivery timestamps when comparing the same stable event identity."""
    try:
        left, right = json.loads(first), json.loads(second)
    except ValueError:
        return False
    for payload in (left, right):
        for key in ("failed_utc", "recorded_at_utc", "created_utc", "occurrence_id",
                    "historical_source_path", "historical_source_sha256", "historical_record_type"):
            payload.pop(key, None)
    return _canonical(left) == _canonical(right)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest().upper()


def _connect_existing(path: Path) -> sqlite3.Connection:
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"existing Stage 2 repair episode ledger is required: {path}")
    connection = sqlite3.connect(str(path), timeout=10.0, isolation_level=None)
    connection.execute("PRAGMA busy_timeout=10000")
    return connection


def _ensure_schema(connection: sqlite3.Connection) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS stage2_learning_observations (
                occurrence_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                cycle_id TEXT,
                ea TEXT,
                ea_family TEXT,
                phase TEXT,
                failure_fingerprint TEXT NOT NULL,
                source_sha256 TEXT,
                spec_sha256 TEXT,
                verifier_policy_identity TEXT,
                verifier_code_sha256 TEXT,
                evidence_hashes_json TEXT NOT NULL DEFAULT '{}',
                imported INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL,
                recorded_at_utc TEXT NOT NULL
            )
        """)
        connection.execute("""
            CREATE TABLE IF NOT EXISTS stage2_learning_lesson_events (
                lesson_event_id TEXT PRIMARY KEY,
                lesson_id TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('observed','diagnosed','validated','reusable','retired')),
                payload_json TEXT NOT NULL,
                recorded_at_utc TEXT NOT NULL
            )
        """)
        connection.execute("CREATE INDEX IF NOT EXISTS stage2_learning_observations_fingerprint ON stage2_learning_observations(failure_fingerprint)")
        connection.execute("CREATE INDEX IF NOT EXISTS stage2_learning_lessons_lookup ON stage2_learning_lesson_events(lesson_id)")
        existing_columns = {row[1] for row in connection.execute("PRAGMA table_info(stage2_learning_observations)")}
        for name, declaration in (("ea_family", "TEXT"), ("phase", "TEXT"),
                                  ("verifier_code_sha256", "TEXT"),
                                  ("evidence_hashes_json", "TEXT NOT NULL DEFAULT '{}'")):
            if name not in existing_columns:
                connection.execute(f"ALTER TABLE stage2_learning_observations ADD COLUMN {name} {declaration}")
        for table in ("stage2_learning_observations", "stage2_learning_lesson_events"):
            for action in ("UPDATE", "DELETE"):
                trigger = f"{table}_deny_{action.lower()}"
                connection.execute(f"CREATE TRIGGER IF NOT EXISTS {trigger} BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT, 'Stage 2 learning records are append-only'); END")
        connection.execute("COMMIT")
    except Exception:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise


def _failure_fingerprint(event: Mapping[str, Any]) -> str:
    explicit = event.get("failure_fingerprint")
    if not explicit and isinstance(event.get("source_diagnosis"), Mapping):
        diagnoses = event["source_diagnosis"].get("diagnoses")
        if isinstance(diagnoses, list) and diagnoses and isinstance(diagnoses[-1], Mapping):
            explicit = diagnoses[-1].get("failure_fingerprint")
    if isinstance(explicit, str) and re.fullmatch(r"[A-Fa-f0-9]{64}", explicit):
        return explicit.upper()
    checks = event.get("failed_checks")
    if not isinstance(checks, list):
        checks = []
    kind = str(event.get("failure_kind") or "UNKNOWN_STAGE2_FAILURE").strip().upper()
    identity: dict[str, Any] = {"failure_kind": kind, "failed_checks": sorted({str(x) for x in checks})}
    explicit = event.get("diagnostic_code") or event.get("diagnostic")
    if isinstance(explicit, str) and explicit.strip():
        identity["diagnostic"] = explicit.strip().upper()
    elif not identity["failed_checks"]:
        detail = str(event.get("error") or event.get("detail") or "")
        # Remove obvious cycle, EA, path, and line-number variation while
        # retaining a bounded diagnostic signature for otherwise untyped errors.
        detail = re.sub(r"(?i)\b(?:kcos-[\w.-]+|cycle[-_ ]?\w+|[a-z]:\\[^\s:]+)\b", "<identity>", detail)
        detail = re.sub(r"\b\d+\b", "#", detail)
        identity["detail"] = re.sub(r"\s+", " ", detail).strip().casefold()[:500]
    return _sha256(_canonical(identity))


def source_characteristics(source_text: str) -> list[str]:
    """Return a small, stable set of source traits used only to scope lessons."""
    text = source_text.casefold()
    traits = set()
    for input_name in re.findall(r"\bInp[A-Za-z0-9_]+\b", source_text):
        traits.add("input:" + input_name.casefold())
    for token, trait in (("iobv", "uses:obv"), ("iatr", "uses:atr"),
                         ("iichimoku", "uses:ichimoku"), ("icci", "uses:cci"),
                         ("ifractals", "uses:fractals"), ("period_current", "uses:closed-bar"),
                         ("ckiyosakiregime", "uses:regime-helper"),
                         ("minatoscore", "uses:minato-score")):
        if token in text:
            traits.add(trait)
    return sorted(traits)


def _occurrence_id(event: Mapping[str, Any], fingerprint: str) -> str:
    if event.get("outcome_type") == "repair_transaction" and event.get("event_id"):
        return _sha256(_canonical({
            "event_id": str(event["event_id"]),
            "stage": event.get("stage", 2),
        }))
    # Executor-generated events carry a persisted invocation key. A scheduler
    # redelivery of that event keeps the key; a later failure in the same
    # cycle receives a new one. Historical events without the field retain the
    # conservative legacy identity below.
    occurrence_key = event.get("occurrence_key")
    if isinstance(occurrence_key, str) and occurrence_key.strip():
        return _sha256(_canonical({
            "occurrence_key": occurrence_key.strip(),
            "stage": event.get("stage", 2),
        }))
    identity = {
        "event_id": event.get("event_id"),
        "cycle_id": event.get("cycle_id"),
        "stage": event.get("stage", 2),
        "ea": event.get("ea") or event.get("candidate_ea"),
        "source_sha256": str(event.get("source_sha256") or "").upper() or None,
        "failure_fingerprint": fingerprint,
    }
    return _sha256(_canonical(identity))


def append_observation(ledger_path: Path, event: Mapping[str, Any], *, imported: bool = False,
                       source_path: Path | str | None = None) -> ObservationResult:
    """Append a typed Stage 2 observation; historical outcomes stay classified."""
    if not isinstance(event, Mapping):
        raise ValueError("observation must be a Stage 2 failure event unless explicitly imported")
    transaction_outcome = (event.get("outcome_type") == "repair_transaction"
                           and event.get("status") in {"committed", "rolled_back"})
    if (event.get("stage", 2) != 2
            or (event.get("status") != "failed" and not imported and not transaction_outcome)):
        raise ValueError("observation must be a Stage 2 failure event unless explicitly imported")
    payload = dict(event)
    evidence_hashes: dict[str, Any] = {}
    for key in ("source", "stage1_spec", "verification", "compile_attempt"):
        raw_path = payload.get(key)
        if not raw_path:
            continue
        path = Path(str(raw_path))
        if path.is_file():
            digest = hashlib.sha256(path.read_bytes()).hexdigest().upper()
            evidence_hashes[key] = {"path": str(path.resolve()), "sha256": digest}
        else:
            evidence_hashes[key] = {"path": str(path), "missing": True}
    verification_value = {}
    verifier_path = payload.get("verification")
    if verifier_path and Path(str(verifier_path)).is_file():
        try:
            verification_value = json.loads(Path(str(verifier_path)).read_text(encoding="utf-8-sig"))
        except (OSError, ValueError):
            verification_value = {}
    policy = (payload.get("verifier_policy_identity") or payload.get("verifier_identity")
              or payload.get("policy_identity") or verification_value.get("verification_policy_id"))
    payload.setdefault("verifier_policy_identity", policy)
    payload.setdefault("verifier_policy_sha256", verification_value.get("verification_policy_sha256"))
    payload.setdefault("verifier_code_sha256", verification_value.get("verifier_code_sha256"))
    payload["evidence_hashes"] = evidence_hashes
    phase = str(payload.get("phase") or payload.get("phase_name") or "").strip().lower()
    if not phase:
        category = str(payload.get("failure_kind") or "").casefold()
        phase = ("generation" if "generation" in category or "source" in category else
                 "compile" if "compile" in category else
                 "verification" if "verif" in category else
                 "repair" if "repair" in category or payload.get("transaction_id") else "execution")
    ea_family = str(payload.get("ea_family") or "").strip()
    if not ea_family and evidence_hashes.get("source", {}).get("path"):
        try:
            from kcos_stage2_adapters_v1 import family
            source_path = Path(evidence_hashes["source"]["path"])
            source_text = source_path.read_text(encoding="utf-8-sig", errors="replace")
            ea_family = family(source_path, source_text)
            payload.setdefault("source_characteristics", source_characteristics(source_text))
        except (OSError, ImportError):
            ea_family = "unknown"
    payload.setdefault("phase", phase)
    payload.setdefault("ea_family", ea_family or "unknown")
    fingerprint = _failure_fingerprint(payload)
    occurrence = _occurrence_id(event, fingerprint)
    payload["occurrence_id"] = occurrence
    payload["failure_fingerprint"] = fingerprint
    if source_path is not None:
        payload["historical_source_path"] = str(source_path)
    encoded = _canonical(payload)
    event_id = str(event.get("event_id") or occurrence)
    recorded_at = str(event.get("failed_utc") or event.get("recorded_at_utc") or datetime.now(timezone.utc).isoformat())
    connection = _connect_existing(ledger_path)
    try:
        _ensure_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        existing = connection.execute("SELECT payload_json FROM stage2_learning_observations WHERE occurrence_id=?", (occurrence,)).fetchone()
        if existing:
            if not _same_occurrence_payload(existing[0], encoded):
                raise ValueError("Stage 2 observation occurrence identity collision")
            connection.execute("COMMIT")
            return ObservationResult(occurrence, fingerprint, False)
        connection.execute("""
            INSERT INTO stage2_learning_observations
            (occurrence_id,event_id,cycle_id,ea,failure_fingerprint,source_sha256,spec_sha256,
             verifier_policy_identity,verifier_code_sha256,evidence_hashes_json,imported,payload_json,recorded_at_utc,
             ea_family,phase)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (occurrence, event_id, event.get("cycle_id"), event.get("ea") or event.get("candidate_ea"),
              fingerprint, event.get("source_sha256"), event.get("stage1_spec_sha256"),
              str(policy) if policy else None, payload.get("verifier_code_sha256"),
              _canonical(evidence_hashes), int(imported), encoded, recorded_at,
              payload.get("ea_family"), payload.get("phase")))
        connection.execute("COMMIT")
    except Exception:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    if not imported and payload.get("status") == "failed":
        ensure_observed_diagnosed_lesson(ledger_path, {
            "lesson_id": _lesson_candidate_id(payload, fingerprint),
            "defect_fingerprint": fingerprint,
            "ea_family": payload.get("ea_family") or "unknown",
            "contract_version": payload.get("contract_version") or payload.get("verifier_policy_identity") or "unknown",
            "source_characteristics": payload.get("source_characteristics") or [],
            "phase": payload.get("phase"),
            "first_occurrence_id": occurrence,
            "diagnostic": payload.get("diagnostic") or payload.get("error"),
        })
    return ObservationResult(occurrence, fingerprint, True)


def record_failure_event_file(event_path: Path, ledger_path: Path) -> ObservationResult:
    """Record the canonical executor event; parsing or persistence errors propagate."""
    event_path = Path(event_path)
    data = json.loads(event_path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise ValueError("canonical Stage 2 failure event must contain a JSON object")
    return append_observation(ledger_path, data, source_path=event_path)


def append_repair_outcome(ledger_path: Path, *, cycle_id: str, ea: str,
                          transaction_id: str, outcome: str, source_before_sha256: str,
                          source_after_sha256: str, stage1_spec_sha256: str,
                          verifier_policy_identity: str | None, evidence_path: Path) -> ObservationResult:
    """Append one committed/rolled-back transaction outcome by stable ID."""
    if outcome not in {"committed", "rolled_back"}:
        raise ValueError("repair transaction outcome must be committed or rolled_back")
    event = {
        "schema_version": 1, "event_id": f"{cycle_id}:stage2:transaction:{transaction_id}",
        "cycle_id": cycle_id, "stage": 2, "status": outcome, "outcome_type": "repair_transaction",
        "ea": ea, "transaction_id": transaction_id,
        "source_before_sha256": source_before_sha256,
        "source_after_sha256": source_after_sha256,
        "stage1_spec_sha256": stage1_spec_sha256,
        "verifier_policy_identity": verifier_policy_identity,
        "evidence_refs": [{"path": str(evidence_path.resolve()),
                           "sha256": hashlib.sha256(evidence_path.read_bytes()).hexdigest().upper()}],
        "failure_kind": "REPAIR_TRANSACTION_ROLLBACK" if outcome == "rolled_back" else "REPAIR_TRANSACTION_COMMIT",
        "failed_checks": [],
    }
    return append_observation(ledger_path, event, source_path=evidence_path)


def _latest_lesson_state(connection: sqlite3.Connection, lesson_id: str) -> str | None:
    row = connection.execute("SELECT state FROM stage2_learning_lesson_events WHERE lesson_id=? ORDER BY rowid DESC LIMIT 1", (lesson_id,)).fetchone()
    return str(row[0]) if row else None


def latest_lesson(ledger_path: Path, lesson_id: str) -> dict[str, Any] | None:
    """Read the current lesson state and payload without changing the ledger."""
    connection = _connect_existing(ledger_path)
    try:
        _ensure_schema(connection)
        row = connection.execute(
            "SELECT state,payload_json FROM stage2_learning_lesson_events "
            "WHERE lesson_id=? ORDER BY rowid DESC LIMIT 1", (lesson_id,)
        ).fetchone()
        if not row:
            return None
        value = json.loads(row[1])
        value["state"] = str(row[0])
        return value
    finally:
        connection.close()


def promote_runtime_validated_lesson(ledger_path: Path, lesson_id: str,
                                     receipt_path: Path) -> str:
    """Promote a validated runtime lesson only with a candidate-bound Stage 3 receipt."""
    receipt_path = Path(receipt_path)
    if not receipt_path.is_file():
        raise FileNotFoundError(f"Stage 3 runtime receipt is missing: {receipt_path}")
    current = latest_lesson(ledger_path, lesson_id)
    if current and current.get("state") == "reusable":
        return str(current.get("lesson_event_id") or current.get("lesson_id"))
    if not current or current.get("state") != "validated":
        raise ValueError("only a validated Stage 2 lesson can receive Stage 3 runtime evidence")
    if current.get("runtime_related") is not True:
        raise ValueError("Stage 3 evidence is only used to promote runtime-related lessons")
    _verify_stage3_runtime_attestation(receipt_path, current)
    ref = {"role": "stage3_runtime", "path": str(receipt_path.resolve()),
           "sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest().upper()}
    refs = [item for item in current.get("evidence_refs", [])
            if item.get("role") != "stage3_runtime"]
    updated = {**current, "evidence_refs": [*refs, ref], "state": "reusable",
               "stage3_runtime_validated": True}
    return append_lesson(ledger_path, updated)


def _lesson_candidate_id(event: Mapping[str, Any], fingerprint: str) -> str:
    characteristics = event.get("source_characteristics") or []
    scope = {"fingerprint": fingerprint, "family": event.get("ea_family") or "unknown",
             "contract": event.get("contract_version") or event.get("verifier_policy_identity") or "unknown",
             "characteristics": sorted(map(str, characteristics))}
    return "lesson-" + _sha256(_canonical(scope))[:32].lower()


def ensure_observed_diagnosed_lesson(ledger_path: Path, lesson: Mapping[str, Any]) -> str:
    """Create the append-only observed/diagnosed prefix for a failure lesson."""
    connection = _connect_existing(ledger_path)
    try:
        _ensure_schema(connection)
        latest = _latest_lesson_state(connection, str(lesson["lesson_id"]))
    finally:
        connection.close()
    if latest is None:
        append_lesson(ledger_path, {**dict(lesson), "state": "observed"})
        latest = "observed"
    if latest == "observed":
        append_lesson(ledger_path, {**dict(lesson), "state": "diagnosed"})
    return str(lesson["lesson_id"])


def _verify_lesson_evidence(connection: sqlite3.Connection, lesson: Mapping[str, Any],
                            *, require_runtime: bool) -> None:
    """Check the artifacts and committed transaction behind a reusable lesson."""
    refs = lesson.get("evidence_refs")
    if not isinstance(refs, list):
        raise ValueError("lesson evidence_refs must bind verification and protected-contract receipts")
    by_role: dict[str, dict[str, Any]] = {}
    for ref in refs:
        if not isinstance(ref, Mapping) or not ref.get("role") or not ref.get("path"):
            raise ValueError("lesson evidence references require role, path, and sha256")
        path = Path(str(ref["path"]))
        if not path.is_file():
            raise ValueError(f"lesson evidence is missing: {path}")
        actual = hashlib.sha256(path.read_bytes()).hexdigest().upper()
        if actual != str(ref.get("sha256") or "").upper():
            raise ValueError(f"lesson evidence hash changed: {path}")
        by_role[str(ref["role"])] = {**dict(ref), "_path": path}
    fixture = lesson.get("regression_fixture")
    if not isinstance(fixture, Mapping) or not fixture.get("path") or not fixture.get("sha256"):
        raise ValueError("lesson requires a hash-bound regression fixture")
    fixture_path = Path(str(fixture["path"]))
    if (not fixture_path.is_file()
            or hashlib.sha256(fixture_path.read_bytes()).hexdigest().upper() != str(fixture["sha256"]).upper()):
        raise ValueError("lesson regression fixture is missing or changed")
    verification_ref = by_role.get("verification")
    protected_ref = by_role.get("protected_contract")
    if not verification_ref or not protected_ref:
        raise ValueError("lesson requires hash-bound verification and protected-contract receipts")
    before_ref, after_ref = by_role.get("source_before"), by_role.get("source_after")
    if not before_ref or not after_ref:
        raise ValueError("lesson regression replay requires before and after source evidence")
    _execute_regression_fixture(fixture_path, before_ref["_path"], after_ref["_path"])
    verification = json.loads(verification_ref["_path"].read_text(encoding="utf-8-sig"))
    protected = json.loads(protected_ref["_path"].read_text(encoding="utf-8-sig"))
    if verification.get("passed") is not True or not verification.get("verification_policy_sha256"):
        raise ValueError("lesson verification receipt does not show a policy-bound PASS")
    if protected.get("passed") is not True or not protected.get("contract_id"):
        raise ValueError("lesson protected-contract receipt does not prove preservation")
    transaction_id = str(lesson.get("transaction_id") or "")
    before = str(lesson.get("source_before_sha256") or "").upper()
    after = str(lesson.get("source_after_sha256") or "").upper()
    if not transaction_id or not re.fullmatch(r"[A-F0-9]{64}", before) or not re.fullmatch(r"[A-F0-9]{64}", after):
        raise ValueError("lesson requires transaction identity and before/after source hashes")
    observations = connection.execute(
        "SELECT payload_json FROM stage2_learning_observations WHERE payload_json LIKE ?",
        (f'%"transaction_id":"{transaction_id}"%',)).fetchall()
    committed = False
    for (payload_json,) in observations:
        outcome = json.loads(payload_json)
        if (outcome.get("outcome_type") == "repair_transaction"
                and outcome.get("status") == "committed"
                and str(outcome.get("source_before_sha256") or "").upper() == before
                and str(outcome.get("source_after_sha256") or "").upper() == after
                and str(outcome.get("transaction_id") or "") == transaction_id):
            committed = True
            break
    if not committed:
        raise ValueError("lesson transaction has no matching committed Stage 2 ledger outcome")
    if (verification.get("cycle_id") != lesson.get("cycle_id")
            or verification.get("ea") != lesson.get("ea")
            or str(verification.get("source_sha256") or "").upper() != after
            or str(verification.get("verification_policy_id") or "") != str(lesson.get("verifier_policy_identity") or "")
            or str(verification.get("verification_policy_id") or "") != str(lesson.get("contract_version") or "")
            or protected.get("contract_id") != lesson.get("contract_version")):
        raise ValueError("lesson verification, candidate, policy, and protected-contract identities do not match")
    if require_runtime and lesson.get("runtime_related") is True:
        runtime_ref = by_role.get("stage3_runtime")
        if not runtime_ref:
            raise ValueError("runtime-related lesson requires an authenticated Stage 3 receipt")
        _verify_stage3_runtime_attestation(runtime_ref["_path"], lesson)


def _fixture_predicate(check: str, source: str) -> bool:
    """Run only named, deterministic source predicates from the Stage 2 verifier."""
    import kcos_stage2_verify_v2 as verifier
    if check == "closed_bar_calculation":
        return bool(verifier.closed_bar_check(source)
                    or verifier.diagnostic2_closed_bar_only(source)
                    or verifier.cusum_closed_bar_only(source)
                    or verifier.generic_closed_bar_audit(source))
    if check == "closed_bar_only":
        return bool(verifier.closed_bar_check(source)
                    or verifier.diagnostic2_closed_bar_only(source)
                    or verifier.cusum_closed_bar_only(source)
                    or verifier.generic_closed_bar_audit(source))
    if check == "indicator_reads_exclude_forming_bar":
        return bool(verifier.closed_bar_indicator_reads_are_safe(source))
    if check == "multidimensional_array_row_bounds_safe":
        return bool(verifier.multidimensional_array_row_bounds_are_safe(source))
    if check == "every_order_path_margin_quantity_tp_safe":
        code = verifier.executable_source(source)
        return bool(verifier.every_order_path_is_safe(code)
                    or verifier.diagnostic2_order_path_is_safe(code)
                    or verifier.cusum_order_path_is_safe(code))
    if check == "dcc_closed_return_arrays_bounded":
        return bool(verifier.dcc_closed_return_arrays_are_bounded(source))
    raise ValueError(f"no executable Stage 2 regression predicate is registered for {check}")


def _execute_regression_fixture(fixture_path: Path, before_path: Path, after_path: Path) -> None:
    fixture = json.loads(fixture_path.read_text(encoding="utf-8-sig"))
    checks = fixture.get("executable_checks")
    if not isinstance(checks, list) or not checks:
        raise ValueError("lesson regression fixture has no executable checks")
    before = before_path.read_text(encoding="utf-8-sig", errors="strict")
    after = after_path.read_text(encoding="utf-8-sig", errors="strict")
    for case in checks:
        if not isinstance(case, Mapping) or case.get("expected_before") is not False or case.get("expected_after") is not True:
            raise ValueError("regression fixture must require a failing before source and passing repaired source")
        name = str(case.get("check") or "")
        if _fixture_predicate(name, before) is not False or _fixture_predicate(name, after) is not True:
            raise ValueError(f"regression fixture did not reproduce and clear Stage 2 check: {name}")


def _verify_stage3_runtime_attestation(attestation_path: Path,
                                      lesson: Mapping[str, Any]) -> None:
    """Re-run the official Stage 3 verifier against hash-bound run artifacts."""
    attestation = json.loads(attestation_path.read_text(encoding="utf-8-sig"))
    if attestation.get("schema_version") != 1 or attestation.get("role") != "stage3_runtime_attestation":
        raise ValueError("Stage 3 runtime evidence must use the verified attestation schema")
    artifacts = attestation.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise ValueError("Stage 3 runtime attestation is missing handoff, decision, and verification artifacts")
    paths: dict[str, Path] = {}
    for role in ("handoff", "decision", "verification"):
        ref = artifacts.get(role)
        if not isinstance(ref, Mapping) or not ref.get("path") or not ref.get("sha256"):
            raise ValueError(f"Stage 3 runtime attestation lacks hash-bound {role}")
        path = Path(str(ref["path"]))
        if (not path.is_file()
                or hashlib.sha256(path.read_bytes()).hexdigest().upper() != str(ref["sha256"]).upper()):
            raise ValueError(f"Stage 3 runtime {role} artifact is missing or changed")
        paths[role] = path
    import kcos_stage3_verify_v2 as stage3_verify
    with tempfile.TemporaryDirectory(prefix="stage2-stage3-attestation-") as folder:
        replay_path = Path(folder) / "stage3-verification.json"
        replay = stage3_verify.verify(paths["handoff"], paths["decision"], replay_path)
    supplied = json.loads(paths["verification"].read_text(encoding="utf-8-sig"))
    comparable = ("schema_version", "cycle_id", "stage", "decision_sha256", "checks", "passed",
                  "stage1_spec_sha256", "stage2_event_sha256")
    if any(supplied.get(key) != replay.get(key) for key in comparable) or replay.get("passed") is not True:
        raise ValueError("Stage 3 runtime verification did not replay as an authenticated PASS")
    handoff = json.loads(paths["handoff"].read_text(encoding="utf-8-sig"))
    decision = json.loads(paths["decision"].read_text(encoding="utf-8-sig"))
    stage2_event_path = Path(str(handoff.get("stage2_event") or ""))
    if not stage2_event_path.is_file():
        raise ValueError("Stage 3 handoff has no authenticated Stage 2 completion event")
    stage2_event = json.loads(stage2_event_path.read_text(encoding="utf-8-sig"))
    source_hash = str(lesson.get("source_after_sha256") or "").upper()
    event_source_hash = str(stage2_event.get("mq5_sha256") or stage2_event.get("source_sha256") or "").upper()
    if (decision.get("stage") != 3 or decision.get("cycle_id") != lesson.get("cycle_id")
            or decision.get("ea") != lesson.get("ea")
            or handoff.get("cycle_id") != lesson.get("cycle_id")
            or handoff.get("ea") != lesson.get("ea")
            or stage2_event.get("stage") != 2 or stage2_event.get("status") != "complete"
            or stage2_event.get("cycle_id") != lesson.get("cycle_id")
            or stage2_event.get("ea") != lesson.get("ea") or event_source_hash != source_hash):
        raise ValueError("Stage 3 evidence does not match the committed Stage 2 candidate")
    results = decision.get("timeframes") or {}
    if not any(value.get("status") != "COMPUTE_SKIPPED" for value in results.values()
               if isinstance(value, Mapping)):
        raise ValueError("Stage 3 runtime evidence contains no authenticated tester execution")


def append_lesson(ledger_path: Path, lesson: Mapping[str, Any]) -> str:
    """Append a lesson state event, enforcing evidence and forward transitions."""
    state = lesson.get("state")
    lesson_id = str(lesson.get("lesson_id") or "")
    if state not in _LESSON_STATES or not lesson_id:
        raise ValueError("lesson requires a stable lesson_id and a supported state")
    if state in {"validated", "reusable"}:
        if not (lesson.get("evidence_refs") and lesson.get("regression_fixture")
                and lesson.get("committed_repair") is True and lesson.get("checks_passed") is True
                and lesson.get("protected_contract_preserved") is True
                and lesson.get("applicability") and lesson.get("negative_examples")):
            raise ValueError("validated lesson evidence requires fixture, evidence, committed repair, passing checks, protected contract proof, applicability, and negative examples")
    encoded = _canonical(dict(lesson))
    event_key = str(lesson.get("lesson_event_id") or _sha256(encoded))
    recorded_at = str(lesson.get("recorded_at_utc") or datetime.now(timezone.utc).isoformat())
    connection = _connect_existing(ledger_path)
    try:
        _ensure_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        if state in {"validated", "reusable"}:
            _verify_lesson_evidence(connection, lesson, require_runtime=state == "reusable")
        existing = connection.execute("SELECT payload_json FROM stage2_learning_lesson_events WHERE lesson_event_id=?", (event_key,)).fetchone()
        if existing:
            if existing[0] != encoded:
                raise ValueError("Stage 2 lesson event identity collision")
            connection.execute("COMMIT")
            return event_key
        previous = _latest_lesson_state(connection, lesson_id)
        if state == "retired":
            if previous is None:
                raise ValueError("a lesson must be observed before it can be retired")
            if not lesson.get("retirement_reason"):
                raise ValueError("retired lessons require a retirement reason")
        elif previous is None:
            if state != "observed":
                raise ValueError("lesson state must begin at observed")
        elif previous == "retired" or _STATE_RANK[state] != _STATE_RANK[previous] + 1:
            raise ValueError(f"invalid lesson state transition from {previous} to {state}")
        connection.execute("INSERT INTO stage2_learning_lesson_events VALUES (?,?,?,?,?)",
                           (event_key, lesson_id, state, encoded, recorded_at))
        connection.execute("COMMIT")
        return event_key
    except Exception:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()


def reusable_lessons(ledger_path: Path, *, defect_fingerprint: str, ea_family: str,
                     contract_version: str, source_characteristics: Sequence[str]) -> list[dict[str, Any]]:
    connection = _connect_existing(ledger_path)
    try:
        _ensure_schema(connection)
        rows = connection.execute("""
            SELECT e.payload_json FROM stage2_learning_lesson_events e
            JOIN (SELECT lesson_id, MAX(rowid) AS latest_rowid FROM stage2_learning_lesson_events GROUP BY lesson_id) latest
              ON latest.latest_rowid=e.rowid
            WHERE e.state='reusable'
            ORDER BY e.lesson_id
        """).fetchall()
        required_characteristics = set(map(str, source_characteristics))
        result = []
        for (encoded,) in rows:
            lesson = json.loads(encoded)
            if (lesson.get("defect_fingerprint") == defect_fingerprint
                    and lesson.get("ea_family") == ea_family
                    and lesson.get("contract_version") == contract_version
                    and set(map(str, lesson.get("preconditions") or [])).issubset(required_characteristics)):
                # Promotion proves the evidence was valid at that time. Recheck it
                # at the point of use so changed or missing artifacts cannot keep
                # authorizing automatic reuse.
                _verify_lesson_evidence(connection, lesson, require_runtime=True)
                result.append(lesson)
        return result
    finally:
        connection.close()


def compatibility_summary(ledger_path: Path) -> list[dict[str, Any]]:
    """Return legacy failure-summary rows without mutating the canonical ledger."""
    connection = _connect_existing(ledger_path)
    try:
        _ensure_schema(connection)
        rows = connection.execute("SELECT payload_json FROM stage2_learning_observations ORDER BY rowid").fetchall()
        decoded = [json.loads(row[0]) for row in rows]
        decoded = [item for item in decoded if item.get("status") == "failed"]
        exhausted_eas: dict[str, set[str]] = {}
        for item in decoded:
            if item.get("rotation_eligible") is True or item.get("exhausted") is True:
                exhausted_eas.setdefault(str(item.get("failure_kind") or "unknown_failure"), set()).add(
                    str(item.get("ea") or item.get("candidate_ea") or "unknown"))
        summary = []
        for item in decoded:
            category = re.sub(r"[^a-z0-9]+", "_", str(item.get("failure_kind") or "unknown_failure").lower()).strip("_")[:100] or "unknown_failure"
            exhausted = bool(item.get("rotation_eligible") is True or item.get("exhausted") is True)
            distinct = len(exhausted_eas.get(str(item.get("failure_kind") or "unknown_failure"), set()))
            summary.append({
                "event_id": item.get("event_id"),
                "utc": item.get("failed_utc") or item.get("recorded_at_utc"),
                "cycle_id": item.get("cycle_id"),
                "ea": item.get("ea") or item.get("candidate_ea"),
                "category": category,
                "detail": item.get("error") or item.get("detail") or "",
                "attempt": item.get("attempt", 0),
                "exhausted": exhausted,
                "distinct_strategy_count": distinct,
                "deeper_review_required": distinct >= 2,
                "occurrence_id": item.get("occurrence_id"),
                "failure_fingerprint": item.get("failure_fingerprint"),
            })
        return summary
    finally:
        connection.close()


def write_compatibility_views(ledger_path: Path, *, project_root: Path) -> dict[str, str]:
    """Regenerate legacy failure summaries from canonical append-only observations."""
    rows = compatibility_summary(ledger_path)
    registry = {"schema_version": 2, "generated_from": "stage2_repair_episodes.sqlite3",
                "failures": rows}
    exhausted = [row for row in rows if row.get("exhausted") is True]
    markdown = ["# Stage 2 structural failures", "",
                "Generated compatibility view of the append-only Stage 2 episode ledger.",
                "Do not edit this file; record new observations in the ledger.", ""]
    for row in exhausted:
        markdown.extend([
            f"## {row.get('ea') or 'unknown EA'} - {row.get('utc') or 'unknown time'}", "",
            f"- Cycle: `{row.get('cycle_id') or 'unknown'}`",
            f"- Failure: `{row.get('category') or 'unknown_failure'}`",
            f"- Attempts: {row.get('attempt', 0)}",
            f"- Detail: {str(row.get('detail') or '').replace(chr(10), ' ')}",
            f"- Distinct strategies with this failure: {row.get('distinct_strategy_count', 0)}",
            f"- Deeper system review: {'required' if row.get('deeper_review_required') else 'not yet'}",
            "- Notification: suppressed by operator policy", "",
        ])
    outputs = {
        Path(project_root) / "state" / "stage2_failure_patterns_v2.json": json.dumps(registry, indent=2) + "\n",
        Path(project_root) / "state" / "reports" / "stage2-structural-failures.md": "\n".join(markdown),
    }
    hashes = {}
    for path, content in outputs.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_text(content, encoding="utf-8")
        temp.replace(path)
        hashes[str(path.resolve())] = hashlib.sha256(path.read_bytes()).hexdigest().upper()
    return hashes


def observation_count(ledger_path: Path) -> int:
    connection = _connect_existing(ledger_path)
    try:
        _ensure_schema(connection)
        return int(connection.execute("SELECT COUNT(*) FROM stage2_learning_observations").fetchone()[0])
    finally:
        connection.close()


def import_historical_manifest(ledger_path: Path, manifest: Mapping[str, Sequence[Path | str]]) -> ImportReport:
    """Import only explicitly listed historical JSON records, never modifying them."""
    categories = {"canonical_events", "superseded_failures", "transaction_archives",
                  "legacy_failure_registry"}
    unknown = set(manifest) - categories
    if unknown:
        raise ValueError(f"unsupported historical manifest categories: {sorted(unknown)}")
    imported = duplicates = missing_policy = 0
    sources: list[str] = []
    for category in sorted(categories):
        for source in manifest.get(category, ()):
            if isinstance(source, Mapping):
                path = Path(str(source.get("path") or ""))
                expected_digest = str(source.get("sha256") or "").upper()
                if not re.fullmatch(r"[A-F0-9]{64}", expected_digest):
                    raise ValueError(f"manifest source has no valid sha256: {path}")
            else:
                path = Path(source)
                expected_digest = ""
            if not path.is_file():
                raise FileNotFoundError(f"manifest source is missing: {path}")
            source_digest = hashlib.sha256(path.read_bytes()).hexdigest().upper()
            if expected_digest and source_digest != expected_digest:
                raise ValueError(f"manifest source changed after inventory: {path}")
            raw = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(raw, dict):
                raise ValueError(f"historical source must be a JSON object: {path}")
            if category == "legacy_failure_registry":
                records = raw.get("failures")
                if raw.get("schema_version") != 2 or not isinstance(records, list):
                    raise ValueError(f"legacy failure registry schema is unsupported: {path}")
                normalized_records = []
                for index, record in enumerate(records):
                    if not isinstance(record, dict):
                        raise ValueError(f"legacy failure row {index} is not an object: {path}")
                    row_digest = _sha256(_canonical(record))
                    normalized_records.append({
                        "schema_version": 1, "event_id": f"legacy-failure:{row_digest}",
                        "cycle_id": record.get("cycle_id") or f"historical:{row_digest[:16]}",
                        "stage": 2, "status": "failed", "ea": record.get("ea"),
                        "failure_kind": record.get("category") or "LEGACY_STAGE2_FAILURE",
                        "error": record.get("detail") or "Imported legacy Stage 2 failure report",
                        "failed_checks": [], "failed_utc": record.get("utc"),
                        "attempt": record.get("attempt"), "exhausted": record.get("exhausted"),
                        "historical_row_sha256": row_digest,
                    })
                records_to_import = normalized_records
            else:
                records_to_import = [raw]
            for raw_record in records_to_import:
                event = dict(raw_record)
                row_digest = _sha256(_canonical(raw_record))
                event["historical_source_event_id"] = raw_record.get("event_id")
                if category == "legacy_failure_registry":
                    event["historical_outcome"] = "failed"
                    event["historical_source_sha256"] = row_digest
                event.setdefault("stage", 2)
                historical_outcome = raw_record.get("status") or raw_record.get("outcome")
                if category != "legacy_failure_registry":
                    event["historical_outcome"] = historical_outcome
                    event["status"] = (str(historical_outcome).lower()
                                       if str(historical_outcome).lower() in {
                                           "complete", "passed", "committed", "rolled_back", "rejected", "failed"
                                       } else "historical")
                fallback_identity = row_digest if category == "legacy_failure_registry" else source_digest
                event.setdefault("cycle_id", raw_record.get("cycle_id") or f"historical:{fallback_identity[:16]}")
                # Hash-bound IDs preserve distinct superseded versions while making
                # duplicate copies of the same immutable file idempotent.
                event["event_id"] = (f"legacy-failure:{row_digest}" if category == "legacy_failure_registry"
                                      else f"historical:{source_digest}")
                event.setdefault("failure_kind", raw_record.get("failure_kind") or f"HISTORICAL_{category.upper()}")
                event.setdefault("failed_checks", raw_record.get("failed_checks") if isinstance(raw_record.get("failed_checks"), list) else [])
                event.setdefault("error", raw_record.get("error") or raw_record.get("detail") or f"Imported historical {category} record")
                if not (event.get("verifier_policy_identity") or event.get("verifier_identity") or event.get("policy_identity")):
                    missing_policy += 1
                    event["verifier_policy_identity"] = None
                    event["historical_policy_identity_missing"] = True
                event["historical_record_type"] = category
                event.setdefault("historical_source_sha256", source_digest)
                event["historical_lesson_state"] = "observed"
                result = append_observation(ledger_path, event, imported=True, source_path=path)
                imported += int(result.inserted)
                duplicates += int(not result.inserted)
                sources.append(str(path.resolve()))
    return ImportReport(imported, duplicates, missing_policy, tuple(sources))

