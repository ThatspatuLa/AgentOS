"""Behavioral tests for cron reconciliation transactions; no live runtime writes."""
import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest
import kcos_stage2_reconcile_v1 as reconcile


@pytest.fixture
def runtime(tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    source = state_dir / "Fixture.mq5"
    source.write_text("legacy source", encoding="utf-8")
    binary = source.with_suffix(".ex5")
    binary.write_bytes(b"old binary")
    spec = state_dir / "spec.json"
    reconcile.write(spec, {"cycle_id": "cycle", "ea": "Fixture", "decision": "approved_for_stage2",
                           "strategy": {"family": "fixture"}})
    event = state_dir / "event.json"
    reconcile.write(event, {"cycle_id": "cycle", "ea": "Fixture", "stage": 1, "status": "complete",
                           "artifact": str(spec), "artifact_sha256": reconcile.digest(spec)})
    state = state_dir / "state.json"
    reconcile.write(state, {"cycle_id": "cycle", "candidate_ea": "Fixture", "candidate_source": str(source),
                           "last_stage1_artifact": str(spec), "last_stage1_event": str(event),
                           "budget_consumed": 7})
    handoff = state_dir / "handoff.json"
    reconcile.write(handoff, {"cycle_id": "cycle", "ea": "Fixture", "source": str(source),
        "ex5": str(binary), "stage1_spec": str(spec), "stage1_event": str(event),
        "stage1_spec_sha256": reconcile.digest(spec), "approved_tester_config": {"fixed": "grid"}})
    contract = state_dir / "contracts" / "kcos_stage2_protected_behavior_v1.json"
    reconcile.write(contract, {"contract_id": "fixture"})
    ledger = state_dir / "existing.sqlite3"
    sqlite3.connect(ledger).close()
    protected = {"passed": True, "contract_id": "fixture", "integrations": {"regime": "unchanged"}}
    factory = lambda: reconcile.ImplementationReconciliation(state, handoff, state_dir / "compile.json",
                                                            state_dir / "verification.json", ledger)
    handler = lambda source, brief: {"status": "already_reconciled" if source == "reconciled source" else "reconciled",
        "source_text": "reconciled source", "method": "fixture", "changes": ["cached calculation"]}
    with patch.object(reconcile, "verify_protected_contract", return_value=protected), \
         patch.object(reconcile, "resolve_implementation_brief", return_value={"resolved_intent": {"fixed": True}}), \
         patch.dict(reconcile.HANDLERS, {"fixture": handler}):
        yield factory


def verify(transaction):
    transaction.binary.write_bytes(b"compiled current candidate")
    compiled = {"cycle_id": "cycle", "ea": "Fixture", "passed": True, "fresh_ex5": True,
        "mq5_sha256": reconcile.digest(transaction.source), "ex5_sha256": reconcile.digest(transaction.binary),
        "exit_code": 0, "errors": 0, "warnings": 0}
    reconcile.write(transaction.compile_path, compiled)
    reconcile.write(transaction.verification_path, {"cycle_id": "cycle", "ea": "Fixture", "passed": True,
        "source": str(transaction.source), "ex5": str(transaction.binary),
        "source_sha256": reconcile.digest(transaction.source), "stage1_spec_sha256": reconcile.digest(transaction.spec_path),
        "protected_behavior_contract": transaction.journal["frozen"]["protected"],
        "verification_policy_sha256": transaction.journal["frozen"]["contract"],
        "verifier_code_sha256": transaction.journal["frozen"]["code"]["kcos_stage2_verify_v2.py"], "compile": compiled})


def outcomes(transaction):
    with sqlite3.connect(transaction.ledger_path) as connection:
        return [json.loads(row[0]) for row in connection.execute(
            "SELECT payload_json FROM stage2_learning_observations WHERE event_id LIKE '%:transaction:%'")]


def test_commits_before_publication_and_preserves_retry_budget(runtime):
    with runtime() as transaction:
        assert transaction.source.read_text() == "reconciled source"
        verify(transaction)
        transaction.commit(transaction.verification_path)
        transaction.commit(transaction.verification_path)
    assert transaction.journal["publication_status"] == "not_published"
    assert reconcile.load(transaction.state_path)["budget_consumed"] == 7
    assert [x["status"] for x in outcomes(transaction)] == ["committed"]
    assert not sqlite3.connect(transaction.ledger_path).execute(
        "SELECT * FROM stage2_learning_lesson_events WHERE state IN ('validated','reusable')").fetchall()


def test_stage1_dispatch_consumption_metadata_does_not_invalidate_reconciliation(runtime):
    with runtime() as transaction:
        verify(transaction)
        event_path = Path(transaction.handoff["stage1_event"])
        event = reconcile.load(event_path)
        reconcile.write(event_path, {**event, "consumed": True,
            "consumed_utc": "2026-01-01T00:01:00Z", "consumed_action": "dispatch_stage2"})
        transaction.commit(transaction.verification_path)
    assert transaction.journal["status"] == "committed"


def test_stage1_event_identity_change_still_rejects_commit(runtime):
    with pytest.raises(ValueError, match="Stage 1/candidate identity mismatch"):
        with runtime() as transaction:
            verify(transaction)
            event_path = Path(transaction.handoff["stage1_event"])
            event = reconcile.load(event_path)
            reconcile.write(event_path, {**event, "ea": "OtherEA"})
            transaction.commit(transaction.verification_path)
    assert [x["status"] for x in outcomes(transaction)] == ["rolled_back"]


def test_verifier_pass_then_outer_rejection_rolls_back(runtime):
    with pytest.raises(RuntimeError, match="strategy lock"):
        with runtime() as transaction:
            verify(transaction)
            raise RuntimeError("strategy lock rejected")
    assert transaction.source.read_text() == "legacy source"
    assert transaction.binary.read_bytes() == b"old binary"
    assert not transaction.compile_path.exists()
    assert [x["status"] for x in outcomes(transaction)] == ["rolled_back"]
    assert (transaction.transaction_dir / "rejected" / "0.Fixture.mq5").read_text() == "reconciled source"


@pytest.mark.parametrize("tamper", ["spec", "source", "binary", "policy", "brief", "code", "handoff"])
def test_rejects_tampered_candidate_or_contract(runtime, tamper):
    with pytest.raises((ValueError, KeyError)):
        with runtime() as transaction:
            verify(transaction)
            if tamper == "code":
                transaction.journal["frozen"]["code"]["kcos_stage2_verify_v2.py"] = "incorrect"
            else:
                target = {"spec": transaction.spec_path, "source": transaction.source, "binary": transaction.binary,
                    "policy": transaction.root / "state" / "contracts" / "kcos_stage2_protected_behavior_v1.json",
                    "brief": Path(transaction.journal["brief"]), "handoff": transaction.handoff_path}[tamper]
                target.write_text("{}", encoding="utf-8")
            transaction.commit(transaction.verification_path)
    assert transaction.source.read_text() == "legacy source"
    assert all(x["status"] != "committed" for x in outcomes(transaction))


def test_pending_crash_recovers_candidate_and_original_binary(runtime):
    interrupted = runtime().__enter__()
    verify(interrupted)
    with runtime() as recovered:
        # Recovery restored old artifact before applying a fresh deterministic candidate.
        assert recovered.binary.read_bytes() == b"old binary"
        assert recovered.journal["source_before_sha256"] == reconcile.digest(
            Path(interrupted.journal["snapshots"][0]["backup"]))
        verify(recovered)
        recovered.commit(recovered.verification_path)
    assert [x["status"] for x in outcomes(recovered)] == ["rolled_back", "committed"]


def test_crash_after_ledger_commit_recovers_as_committed(runtime):
    interrupted = runtime().__enter__()
    verify(interrupted)
    interrupted.commit(interrupted.verification_path)
    interrupted.journal["status"] = "commit_recording"
    interrupted._save()
    with runtime() as recovered:
        assert recovered.journal["changed"] is False
        verify(recovered)
        recovered.commit(recovered.verification_path)
    assert reconcile.load(interrupted.journal_path)["status"] == "committed"
    assert all(x["status"] == "committed" for x in outcomes(recovered))


def test_unavailable_learning_store_holds_and_keeps_rejected_candidate(runtime):
    transaction = runtime()
    with patch.object(reconcile, "append_observation", side_effect=OSError("store offline")), \
         patch.object(reconcile, "append_repair_outcome", side_effect=OSError("store offline")):
        with pytest.raises(OSError, match="store offline"):
            transaction.__enter__()
    assert transaction.source.read_text() == "legacy source"
    assert reconcile.load(transaction.journal_path)["status"] == "rollback_recording"
    assert (transaction.transaction_dir / "rejected" / "0.Fixture.mq5").exists()


def test_wrong_cycle_event_rejected_before_source_changes(runtime):
    transaction = runtime()
    state = reconcile.load(transaction.state_path)
    event_path = Path(state["last_stage1_event"])
    event = reconcile.load(event_path)
    reconcile.write(event_path, {**event, "cycle_id": "other-cycle"})
    with pytest.raises(ValueError, match="identity mismatch"):
        transaction.__enter__()
    assert Path(state["candidate_source"]).read_text() == "legacy source"


def test_unregistered_family_is_context_only(runtime):
    with patch.dict(reconcile.HANDLERS, {}, clear=True):
        with runtime() as transaction:
            assert transaction.result["status"] == "resolved_context_only"
            assert transaction.source.read_text() == "legacy source"
            verify(transaction)
            transaction.commit(transaction.verification_path)
    receipt = reconcile.load(transaction.transaction_dir / "outcome.committed.json")
    assert receipt["source_semantics_review"] == "resolved_context_only"


def test_generated_admission_artifacts_are_recovered_after_failed_verification(runtime):
    import kcos_stage2_repair_loop_v1 as repair_loop
    transaction = runtime()
    handoff = reconcile.load(transaction.handoff_path)
    build = transaction.handoff_path.parent / "build.json"
    reconcile.write(build, {"source": "legacy source"})
    reconcile.write(transaction.handoff_path, {**handoff,
        "source_implementation_mode": "model_generated_source_admitted", "source_build_receipt": str(build)})
    original_handoff = transaction.handoff_path.read_bytes()
    original_build = build.read_bytes()
    new_proof = build.parent / "new-source-admission.json"
    parent_proof = build.parent / "new-parent-build.json"

    def refresh(handoff_path, source_path, repair):
        for path in (new_proof, parent_proof):
            repair_loop.track_transaction_artifact(path)
            reconcile.write(path, {"source_sha256": reconcile.digest(source_path)})
        reconcile.write(build, {"source": "reconciled source"})
        current = reconcile.load(handoff_path)
        reconcile.write(handoff_path, {**current, "source_implementation_review": {
            "path": str(new_proof), "sha256": reconcile.digest(new_proof)},
            "source_build_receipt_sha256": reconcile.digest(build)})
        return {"deterministic_source_admission_passed": True}

    with patch.object(repair_loop, "refresh_generated_source_admission", side_effect=refresh):
        with pytest.raises(RuntimeError, match="verification rejected"):
            with transaction:
                assert new_proof.exists() and parent_proof.exists()
                raise RuntimeError("verification rejected")
    assert transaction.handoff_path.read_bytes() == original_handoff
    assert build.read_bytes() == original_build
    assert not new_proof.exists() and not parent_proof.exists()
    assert list((transaction.transaction_dir / "rejected").glob("*.new-source-admission.json"))


def test_noop_does_not_record_successful_repair(runtime):
    with runtime() as first:
        verify(first)
        first.commit(first.verification_path)
    before = outcomes(first)
    with runtime() as second:
        assert second.result["status"] == "already_reconciled"
        verify(second)
        second.commit(second.verification_path)
    assert outcomes(second) == before


def test_source_tamper_before_entry_rejected_when_prepared_hash_is_bound(runtime):
    transaction = runtime()
    handoff = reconcile.load(transaction.handoff_path)
    reconcile.write(transaction.handoff_path, {**handoff, "source_sha256_before": "incorrect"})
    with pytest.raises(ValueError, match="starting source"):
        transaction.__enter__()


def test_recovery_before_prepare_needs_no_valid_handoff(runtime):
    interrupted = runtime().__enter__()
    interrupted.handoff_path.write_text("broken handoff after crash", encoding="utf-8")
    recovered = reconcile.recover_pending_reconciliations(interrupted.state_path, interrupted.ledger_path)
    assert recovered[0]["status"] == "rolled_back"
    assert interrupted.source.read_text() == "legacy source"
    assert reconcile.load(interrupted.handoff_path)["cycle_id"] == "cycle"
    # Ordinary repair is now safe to run: a later recovery cannot overwrite it.
    interrupted.source.write_text("ordinary repair committed later", encoding="utf-8")
    assert reconcile.recover_pending_reconciliations(interrupted.state_path, interrupted.ledger_path) == []
    assert interrupted.source.read_text() == "ordinary repair committed later"
