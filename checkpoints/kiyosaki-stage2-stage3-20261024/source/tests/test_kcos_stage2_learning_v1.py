import hashlib
import json
import sqlite3
from unittest.mock import patch
from pathlib import Path

import pytest

import kcos_stage2_learning_v1 as learning


def make_ledger(path: Path) -> Path:
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE episodes (episode_id TEXT PRIMARY KEY, identity_json TEXT NOT NULL, "
            "proposal_count INTEGER NOT NULL, max_proposals INTEGER NOT NULL, updated_at_utc TEXT NOT NULL)"
        )
        connection.execute("INSERT INTO episodes VALUES ('keep', '{}', 2, 4, '2026-01-01T00:00:00Z')")
    return path


def event(**overrides):
    value = {
        "schema_version": 2,
        "event_id": "cycle-1:stage2:failed",
        "cycle_id": "cycle-1",
        "stage": 2,
        "status": "failed",
        "ea": "EA One",
        "failure_kind": "STAGE2_VERIFICATION_FAILURE",
        "failed_checks": ["closed_bar_calculation"],
        "error": "closed bar check failed",
        "source_sha256": "A" * 64,
        "stage1_spec_sha256": "B" * 64,
        "verification": "verification.json",
        "compile_attempt": "compile.json",
        "failed_utc": "2026-10-23T00:00:00Z",
    }
    value.update(overrides)
    return value


def verified_lesson_artifacts(db: Path, tmp_path: Path, *, transaction_outcome="committed") -> dict:
    fixture = tmp_path / "fixture.json"
    verification = tmp_path / "verification.json"
    protected = tmp_path / "protected-contract.json"
    transaction = tmp_path / "transaction-journal.json"
    fixture.write_text(json.dumps({"executable_checks": [{"check": "closed_bar_calculation",
                                                            "expected_before": False,
                                                            "expected_after": True}]}), encoding="utf-8")
    before_path, after_path = tmp_path / "before.mq5", tmp_path / "after.mq5"
    before_path.write_text("void OnTick() {}\n", encoding="utf-8")
    after_path.write_text("int g_prevBars=0;\nbool NewBarClosed(){if(Bars(_Symbol,PERIOD_CURRENT)>g_prevBars){g_prevBars=Bars(_Symbol,PERIOD_CURRENT);return true;}return false;}\nvoid OnTick(){if(!NewBarClosed()) return;}\n", encoding="utf-8")
    after = hashlib.sha256(after_path.read_bytes()).hexdigest().upper()
    verification.write_text(json.dumps({"passed": True, "cycle_id": "cycle-lesson",
                                        "ea": "OBV candidate", "source_sha256": after,
                                        "verification_policy_id": "stage2-contract-v1",
                                        "verification_policy_sha256": "C" * 64}), encoding="utf-8")
    protected.write_text(json.dumps({"passed": True, "contract_id": "stage2-contract-v1"}), encoding="utf-8")
    transaction.write_text('{"status":"COMMITTED"}\n', encoding="utf-8")
    before = hashlib.sha256(before_path.read_bytes()).hexdigest().upper()
    learning.append_repair_outcome(
        db, cycle_id="cycle-lesson", ea="OBV candidate", transaction_id="tx-lesson",
        outcome=transaction_outcome, source_before_sha256=before, source_after_sha256=after,
        stage1_spec_sha256="B" * 64, verifier_policy_identity="stage2-contract-v1",
        evidence_path=transaction)
    refs = []
    for role, path in (("verification", verification), ("protected_contract", protected),
                       ("source_before", before_path), ("source_after", after_path)):
        refs.append({"role": role, "path": str(path),
                     "sha256": hashlib.sha256(path.read_bytes()).hexdigest().upper()})
    return {
        "evidence_refs": refs,
        "regression_fixture": {"path": str(fixture),
                               "sha256": hashlib.sha256(fixture.read_bytes()).hexdigest().upper()},
        "transaction_id": "tx-lesson", "source_before_sha256": before,
        "source_after_sha256": after,
        "cycle_id": "cycle-lesson", "ea": "OBV candidate",
        "verifier_policy_identity": "stage2-contract-v1",
    }


def test_append_observation_is_idempotent_and_keeps_occurrence_separate_from_fingerprint(tmp_path):
    db = make_ledger(tmp_path / "episodes.sqlite3")
    first = learning.append_observation(db, event())
    duplicate = learning.append_observation(db, event())
    redelivered = learning.append_observation(db, event(failed_utc="2026-10-23T00:00:30Z"))
    recurrence = learning.append_observation(db, event(
        event_id="cycle-1:stage2:failed", failed_utc="2026-10-23T00:01:00Z",
        cycle_id="cycle-2", ea="EA One", source_sha256="C" * 64,
    ))

    assert first.occurrence_id == duplicate.occurrence_id
    assert first.occurrence_id == redelivered.occurrence_id
    assert first.failure_fingerprint == recurrence.failure_fingerprint
    assert first.occurrence_id != recurrence.occurrence_id
    assert learning.observation_count(db) == 2
    assert learning.compatibility_summary(db)[0]["cycle_id"] == "cycle-1"
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT proposal_count FROM episodes WHERE episode_id='keep'").fetchone() == (2,)


def test_same_cycle_repeat_is_distinct_but_replay_of_persisted_dispatch_is_idempotent(tmp_path):
    db = make_ledger(tmp_path / "episodes.sqlite3")
    first = learning.append_observation(db, event(occurrence_key="dispatch-1"))
    redelivery = learning.append_observation(db, event(
        occurrence_key="dispatch-1", failed_utc="2026-10-23T00:01:00Z"))
    later = learning.append_observation(db, event(
        occurrence_key="dispatch-2", failed_utc="2026-10-23T00:02:00Z"))
    assert first.occurrence_id == redelivery.occurrence_id
    assert first.inserted is True and redelivery.inserted is False
    assert later.occurrence_id != first.occurrence_id
    assert later.failure_fingerprint == first.failure_fingerprint
    assert learning.observation_count(db) == 2


def test_lesson_state_is_append_only_and_reuse_requires_evidence(tmp_path):
    db = make_ledger(tmp_path / "episodes.sqlite3")
    evidence = verified_lesson_artifacts(db, tmp_path)
    base = {
        "lesson_id": "lesson-obv-policy-change",
        "defect_fingerprint": "D" * 64,
        "ea_family": "OBV",
        "contract_version": "stage2-contract-v1",
        "source_characteristics": ["obv-divergence"],
        "applicability": ["same policy mismatch diagnosis"],
        "negative_examples": ["do not apply when source differs"],
        **evidence,
        "committed_repair": True,
        "checks_passed": True,
        "protected_contract_preserved": True,
        "stage3_runtime_validated": False,
    }
    learning.append_lesson(db, {**base, "state": "observed"})
    learning.append_lesson(db, {**base, "state": "diagnosed"})
    with pytest.raises(ValueError, match="validated lesson evidence"):
        learning.append_lesson(db, {**base, "state": "validated", "checks_passed": False})
    learning.append_lesson(db, {**base, "state": "validated"})
    learning.append_lesson(db, {**base, "state": "reusable"})
    rows = learning.reusable_lessons(db, defect_fingerprint="D" * 64,
                                    ea_family="OBV", contract_version="stage2-contract-v1",
                                    source_characteristics=["obv-divergence", "input:inpperiod"])
    assert [row["lesson_id"] for row in rows] == ["lesson-obv-policy-change"]
    # A different candidate/cycle can reuse the lesson when its declared
    # source preconditions hold; family and protected-contract drift exclude it.
    assert learning.reusable_lessons(db, defect_fingerprint="D" * 64,
                                     ea_family="OBV", contract_version="stage2-contract-v1",
                                     source_characteristics=["obv-divergence"])
    assert not learning.reusable_lessons(db, defect_fingerprint="D" * 64,
                                         ea_family="OBV", contract_version="stage2-contract-v2",
                                         source_characteristics=["obv-divergence"])
    assert not learning.reusable_lessons(db, defect_fingerprint="D" * 64,
                                         ea_family="MTF_ELDER", contract_version="stage2-contract-v1",
                                         source_characteristics=["obv-divergence"])
    # Evidence is revalidated at retrieval, not trusted forever because it
    # passed during promotion.
    fixture_path = Path(evidence["regression_fixture"]["path"])
    fixture_path.write_text('{"executable_checks": []}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="regression fixture is missing or changed"):
        learning.reusable_lessons(db, defect_fingerprint="D" * 64,
                                  ea_family="OBV", contract_version="stage2-contract-v1",
                                  source_characteristics=["obv-divergence"])
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT COUNT(*) FROM stage2_learning_lesson_events").fetchone()[0] == 4
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("DELETE FROM stage2_learning_lesson_events")


def test_runtime_lesson_has_hash_bound_stage3_promotion_path(tmp_path):
    db = make_ledger(tmp_path / "episodes.sqlite3")
    evidence = verified_lesson_artifacts(db, tmp_path)
    lesson = {
        "lesson_id": "runtime-lesson", "defect_fingerprint": "E" * 64,
        "ea_family": "OBV", "contract_version": "stage2-contract-v1",
        "source_characteristics": ["uses:obv"], "preconditions": ["uses:obv"],
        "applicability": ["same authenticated runtime ticket"],
        "negative_examples": ["do not reuse without Stage 3 runtime PASS"],
        "cycle_id": "cycle-lesson", "ea": "OBV candidate",
        "runtime_related": True, "stage3_runtime_validated": False,
        "committed_repair": True, "checks_passed": True,
        "protected_contract_preserved": True, **evidence,
    }
    learning.append_lesson(db, {**lesson, "state": "observed"})
    learning.append_lesson(db, {**lesson, "state": "diagnosed"})
    learning.append_lesson(db, {**lesson, "state": "validated"})
    stage2_event_path = tmp_path / "stage2-complete.json"
    stage2_event_path.write_text(json.dumps({
        "stage": 2, "status": "complete", "cycle_id": "cycle-lesson",
        "ea": "OBV candidate", "mq5_sha256": lesson["source_after_sha256"],
    }), encoding="utf-8")
    handoff_path, decision_path = tmp_path / "stage3-handoff.json", tmp_path / "stage3-decision.json"
    handoff_path.write_text(json.dumps({"cycle_id": "cycle-lesson", "ea": "OBV candidate",
                                       "stage2_event": str(stage2_event_path)}), encoding="utf-8")
    decision_path.write_text(json.dumps({"stage": 3, "cycle_id": "cycle-lesson",
                                        "ea": "OBV candidate",
                                        "timeframes": {"M5": {"status": "TIMEFRAME_COMPLETE"}}}),
                             encoding="utf-8")
    verification_path = tmp_path / "stage3-verification.json"
    stage3_verification = {
        "schema_version": 2, "cycle_id": "cycle-lesson", "stage": 3,
        "decision_sha256": hashlib.sha256(decision_path.read_bytes()).hexdigest().upper(),
        "checks": {"identity": True}, "passed": True,
        "stage1_spec_sha256": "B" * 64,
        "stage2_event_sha256": hashlib.sha256(stage2_event_path.read_bytes()).hexdigest().upper(),
    }
    verification_path.write_text(json.dumps(stage3_verification), encoding="utf-8")
    attestation = tmp_path / "stage3-attestation.json"
    def artifact_ref(path):
        return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest().upper()}
    attestation.write_text(json.dumps({
        "schema_version": 1, "role": "stage3_runtime_attestation",
        "artifacts": {"handoff": artifact_ref(handoff_path),
                      "decision": artifact_ref(decision_path),
                      "verification": artifact_ref(verification_path)},
    }), encoding="utf-8")
    with patch("kcos_stage3_verify_v2.verify", return_value=stage3_verification):
        learning.promote_runtime_validated_lesson(db, lesson["lesson_id"], attestation)
    promoted = learning.latest_lesson(db, lesson["lesson_id"])
    assert promoted["state"] == "reusable"
    assert any(ref["role"] == "stage3_runtime" for ref in promoted["evidence_refs"])


def test_markov_strategy_lock_rollback_never_creates_a_reusable_lesson(tmp_path):
    db = make_ledger(tmp_path / "episodes.sqlite3")
    evidence = verified_lesson_artifacts(db, tmp_path, transaction_outcome="rolled_back")
    lesson = {
        "lesson_id": "markov-strategy-lock", "defect_fingerprint": "F" * 64,
        "ea_family": "Markov", "contract_version": "stage2-contract-v1",
        "source_characteristics": ["uses:regime-helper"],
        "applicability": ["diagnosed strategy-lock rejection only"],
        "negative_examples": ["do not alter the lock from an inner verifier PASS"],
        "committed_repair": True, "checks_passed": True,
        "protected_contract_preserved": True, **evidence,
    }
    learning.append_lesson(db, {**lesson, "state": "observed"})
    learning.append_lesson(db, {**lesson, "state": "diagnosed"})
    with pytest.raises(ValueError, match="no matching committed Stage 2 ledger outcome"):
        learning.append_lesson(db, {**lesson, "state": "validated"})
    assert learning.latest_lesson(db, lesson["lesson_id"])["state"] == "diagnosed"
    assert not learning.reusable_lessons(db, defect_fingerprint="F" * 64,
                                         ea_family="Markov", contract_version="stage2-contract-v1",
                                         source_characteristics=["uses:regime-helper"])


def test_source_characteristics_are_stable_and_explicitly_scoped():
    traits = learning.source_characteristics(
        "input int InpPeriod=14; iOBV(_Symbol, PERIOD_CURRENT, VOLUME_TICK);"
        " CKiyosakiRegime regime; MinatoScore();")
    assert traits == ["input:inpperiod", "uses:closed-bar", "uses:minato-score",
                      "uses:obv", "uses:regime-helper"]


def test_historical_import_uses_only_manifest_paths_and_reports_missing_policy_identity(tmp_path):
    db = make_ledger(tmp_path / "episodes.sqlite3")
    canonical = tmp_path / "canonical.json"
    superseded = tmp_path / "superseded.json"
    transaction = tmp_path / "transaction.json"
    archive = tmp_path / "unlisted.json"
    canonical.write_text(json.dumps(event(status="complete", event_id="historical-pass")), encoding="utf-8")
    superseded.write_text(json.dumps(event(event_id="old-event", cycle_id="old-cycle")), encoding="utf-8")
    transaction.write_text(json.dumps({"cycle_id": "tx-cycle", "stage": 2, "status": "ROLLED_BACK",
                                       "transaction_id": "tx-1"}), encoding="utf-8")
    archive.write_text(json.dumps(event(event_id="must-not-import")), encoding="utf-8")
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in (canonical, superseded, transaction, archive)}

    report = learning.import_historical_manifest(db, {
        "canonical_events": [canonical],
        "superseded_failures": [superseded],
        "transaction_archives": [transaction],
    })

    assert report.imported == 3
    assert report.missing_policy_identity == 3
    assert learning.observation_count(db) == 3
    assert all(row["event_id"] != "historical-pass" for row in learning.compatibility_summary(db))
    assert all(hashlib.sha256(path.read_bytes()).hexdigest() == digest for path, digest in before.items())
    assert not any(row["event_id"] == "must-not-import" for row in learning.compatibility_summary(db))


def test_historical_legacy_failure_registry_import_stays_observation_only(tmp_path):
    db = make_ledger(tmp_path / "episodes.sqlite3")
    registry = tmp_path / "legacy.json"
    registry.write_text(json.dumps({"schema_version": 2, "failures": [{
        "cycle_id": "old-cycle", "ea": "Old EA", "category": "closed_bar_flow",
        "detail": "legacy failure", "attempt": 2, "exhausted": True,
    }]}), encoding="utf-8")
    digest = hashlib.sha256(registry.read_bytes()).hexdigest().upper()
    report = learning.import_historical_manifest(db, {"legacy_failure_registry": [
        {"path": str(registry), "sha256": digest}]})
    assert report.imported == 1
    assert learning.compatibility_summary(db)[0]["failure_fingerprint"]
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT COUNT(*) FROM stage2_learning_lesson_events").fetchone()[0] == 0


def test_existing_database_is_required_and_bad_event_is_rejected(tmp_path):
    with pytest.raises(FileNotFoundError):
        learning.append_observation(tmp_path / "do-not-create.sqlite3", event())
    db = make_ledger(tmp_path / "episodes.sqlite3")
    with pytest.raises(ValueError, match="Stage 2 failure"):
        learning.append_observation(db, event(status="complete"))


def test_canonical_event_file_remains_immutable_and_missing_ledger_fails_closed(tmp_path):
    event_path = tmp_path / "cycle.stage2.failed.json"
    event_path.write_text(json.dumps(event()), encoding="utf-8")
    before = hashlib.sha256(event_path.read_bytes()).hexdigest()
    missing_db = tmp_path / "missing-ledger.sqlite3"

    with pytest.raises(FileNotFoundError):
        learning.record_failure_event_file(event_path, missing_db)

    assert hashlib.sha256(event_path.read_bytes()).hexdigest() == before
    assert not missing_db.exists()
    db = make_ledger(tmp_path / "episodes.sqlite3")
    learning.record_failure_event_file(event_path, db)
    assert hashlib.sha256(event_path.read_bytes()).hexdigest() == before


def test_repair_transaction_outcome_is_idempotent_and_keeps_commits_out_of_failure_view(tmp_path):
    db = make_ledger(tmp_path / "episodes.sqlite3")
    evidence = tmp_path / "transaction-journal.json"
    evidence.write_text('{"status":"COMMITTED"}', encoding="utf-8")
    kwargs = dict(cycle_id="cycle-x", ea="EA X", transaction_id="tx-123",
                  outcome="committed", source_before_sha256="A" * 64,
                  source_after_sha256="C" * 64, stage1_spec_sha256="B" * 64,
                  verifier_policy_identity="stage2-v1", evidence_path=evidence)
    first = learning.append_repair_outcome(db, **kwargs)
    duplicate = learning.append_repair_outcome(db, **kwargs)
    assert first.occurrence_id == duplicate.occurrence_id
    assert first.inserted and not duplicate.inserted
    assert learning.observation_count(db) == 1
    assert learning.compatibility_summary(db) == []


def test_compatibility_failure_views_are_generated_from_ledger(tmp_path):
    db = make_ledger(tmp_path / "episodes.sqlite3")
    learning.append_observation(db, event(rotation_eligible=True, exhausted=True))
    hashes = learning.write_compatibility_views(db, project_root=tmp_path)
    registry = json.loads((tmp_path / "state/stage2_failure_patterns_v2.json").read_text())
    markdown = (tmp_path / "state/reports/stage2-structural-failures.md").read_text()
    assert len(registry["failures"]) == 1
    assert registry["failures"][0]["cycle_id"] == "cycle-1"
    assert "Deeper system review" in markdown
    assert len(hashes) == 2


def test_lesson_cannot_be_validated_without_explicit_scope_and_negative_examples(tmp_path):
    db = make_ledger(tmp_path / "episodes.sqlite3")
    base = {
        "lesson_id": "lesson-needs-scope", "state": "observed",
        "evidence_refs": ["event.json"], "regression_fixture": "fixture.json",
        "committed_repair": True, "checks_passed": True,
        "protected_contract_preserved": True,
    }
    learning.append_lesson(db, base)
    learning.append_lesson(db, {**base, "state": "diagnosed"})
    with pytest.raises(ValueError, match="negative examples"):
        learning.append_lesson(db, {**base, "state": "validated", "applicability": ["x"]})


def test_lesson_promotion_rejects_changed_verification_evidence(tmp_path):
    db = make_ledger(tmp_path / "episodes.sqlite3")
    evidence = verified_lesson_artifacts(db, tmp_path)
    base = {"lesson_id": "lesson-tampered", "state": "observed",
            "defect_fingerprint": "F" * 64, "ea_family": "OBV",
            "contract_version": "stage2-contract-v1", "source_characteristics": [],
            "applicability": ["only matching OBV policy failures"],
            "negative_examples": ["not a source defect"], "committed_repair": True,
            "checks_passed": True, "protected_contract_preserved": True, **evidence}
    learning.append_lesson(db, base)
    learning.append_lesson(db, {**base, "state": "diagnosed"})
    verification_path = Path(evidence["evidence_refs"][0]["path"])
    verification_path.write_text('{"passed":false}', encoding="utf-8")
    with pytest.raises(ValueError, match="evidence hash changed"):
        learning.append_lesson(db, {**base, "state": "validated"})

