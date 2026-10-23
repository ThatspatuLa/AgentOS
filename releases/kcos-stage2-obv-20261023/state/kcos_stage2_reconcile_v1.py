"""Cron-owned, recoverable reconciliation of Stage 1 intent and existing source.

This transaction changes candidate source only. Standard compilation, verification,
and publication remain executor operations. A commit is not a published result.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import uuid
from pathlib import Path

from kcos_stage2_implementation_brief_v1 import resolve_implementation_brief
from kcos_stage2_obv_reconcile_v1 import reconcile_obv_source
from kcos_stage2_protected_contract_v1 import verify_protected_contract
from kcos_stage2_learning_v1 import append_observation, append_repair_outcome


HANDLERS = {"PivotPrice_SignedTickVolume_Divergence": reconcile_obv_source}
CODE_FILES = ("kcos_stage2_reconcile_v1.py", "kcos_stage2_implementation_brief_v1.py",
              "kcos_stage2_obv_reconcile_v1.py", "kcos_stage2_verify_v2.py",
              "kcos_stage2_protected_contract_v1.py")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def recover_pending_reconciliations(state_path: Path, ledger_path: Path) -> list[dict]:
    """Run before PREPARE, generation, or ordinary repair; needs no active handoff.

    A crash candidate must be restored before any later operation can inspect or
    mutate it. Recovery does not change cycle ownership or retry counters.
    """
    recovery = ImplementationReconciliation(state_path, state_path, state_path, state_path, ledger_path)
    recovery.directory.mkdir(parents=True, exist_ok=True)
    return recovery._recover()


class ImplementationReconciliation:
    """Wrap executor compilation and verification; require explicit commit.

    Original Stage 1 artifacts and shared includes are immutable. Pending journal
    recovery always occurs before new work, including after a process crash.
    """

    def __init__(self, state_path: Path, handoff_path: Path, compile_path: Path,
                 verification_path: Path, ledger_path: Path):
        self.state_path, self.handoff_path = Path(state_path), Path(handoff_path)
        self.compile_path, self.verification_path = Path(compile_path), Path(verification_path)
        self.ledger_path = Path(ledger_path)
        self.root = self.state_path.resolve().parent.parent
        self.directory = self.root / "state" / "diagnostics" / "stage2_implementation_reconciliation"
        self.journal = None
        self.committed = False
        self.result = {}

    def _inside_root(self, path: Path) -> Path:
        path = path.resolve()
        path.relative_to(self.root)
        return path

    def _identity(self) -> None:
        self.state, self.handoff = load(self.state_path), load(self.handoff_path)
        self.cycle = self.handoff["cycle_id"]
        self.ea = self.handoff["ea"]
        self.source = self._inside_root(Path(self.handoff["source"]))
        self.binary = self._inside_root(Path(self.handoff["ex5"]))
        self.spec_path = self._inside_root(Path(self.handoff["stage1_spec"]))
        self.event_path = self._inside_root(Path(self.handoff["stage1_event"]))
        self.spec, event = load(self.spec_path), load(self.event_path)
        if (self.state.get("cycle_id") != self.cycle
                or (self.state.get("candidate_ea") or self.state.get("current_ea")) != self.ea
                or Path(self.state.get("candidate_source") or "").resolve() != self.source
                or Path(self.state.get("last_stage1_artifact") or "").resolve() != self.spec_path
                or Path(self.state.get("last_stage1_event") or "").resolve() != self.event_path
                or event.get("cycle_id") != self.cycle or event.get("ea") != self.ea
                or event.get("stage") != 1 or event.get("status") != "complete"
                or Path(event.get("artifact") or "").resolve() != self.spec_path
                or event.get("artifact_sha256") != digest(self.spec_path)
                or self.handoff.get("stage1_spec_sha256") != digest(self.spec_path)
                or self.spec.get("cycle_id") != self.cycle or self.spec.get("ea") != self.ea
                or self.spec.get("decision") != "approved_for_stage2"):
            raise ValueError("implementation reconciliation Stage 1/candidate identity mismatch")
        if self.source.suffix.lower() != ".mq5" or self.binary != self.source.with_suffix(".ex5"):
            raise ValueError("implementation reconciliation candidate artifact paths mismatch")

    def _code_hashes(self) -> dict:
        from kcos_stage2_verify_v2 import STAGE2_PIPELINE_CODE_FILES
        return {name: digest(Path(__file__).parent / name)
                for name in dict.fromkeys((*CODE_FILES, *STAGE2_PIPELINE_CODE_FILES))}

    def _freeze(self) -> dict:
        protected = verify_protected_contract(self.root)
        canonical = self.handoff.get("canonical_minato")
        return {"spec": digest(self.spec_path), "event": digest(self.event_path),
                "canonical_minato": digest(Path(canonical)) if canonical else None,
                "contract": digest(self.root / "state" / "contracts" /
                                   "kcos_stage2_protected_behavior_v1.json"),
                "protected": protected, "code": self._code_hashes()}

    def _save(self) -> None:
        write(self.journal_path, self.journal)

    def _snapshot(self, path: Path, label: str) -> dict:
        path = self._inside_root(path)
        backup = self.transaction_dir / (label + ".before")
        if path.is_file():
            shutil.copyfile(path, backup)
        return {"path": str(path), "backup": str(backup), "exists": path.is_file(),
                "sha256": digest(path) if path.is_file() else None}

    def _record_outcome(self, outcome: str, evidence: Path) -> None:
        if not self.journal.get("changed"):
            return
        append_repair_outcome(self.ledger_path, cycle_id=self.journal["cycle_id"],
            ea=self.journal["ea"], transaction_id=self.journal["transaction_id"], outcome=outcome,
            source_before_sha256=self.journal["source_before_sha256"],
            source_after_sha256=self.journal["source_after_sha256"],
            stage1_spec_sha256=self.journal["frozen"]["spec"],
            verifier_policy_identity=self.journal["frozen"]["protected"]["contract_id"],
            evidence_path=evidence)

    def _ledger_committed(self, journal: dict) -> bool:
        if not self.ledger_path.is_file():
            raise FileNotFoundError("existing Stage 2 episode ledger is unavailable")
        with sqlite3.connect(f"{self.ledger_path.resolve().as_uri()}?mode=ro", uri=True) as connection:
            table = connection.execute("SELECT name FROM sqlite_master WHERE name='stage2_learning_observations'").fetchone()
            if not table:
                return False
            rows = connection.execute("SELECT payload_json FROM stage2_learning_observations WHERE event_id=?",
                (f"{journal['cycle_id']}:stage2:transaction:{journal['transaction_id']}",)).fetchall()
            return any(json.loads(row[0]).get("status") == "committed" for row in rows)

    def _recover(self) -> list[dict]:
        recovered = []
        for path in sorted(self.directory.glob("*/journal.json")):
            journal = load(path)
            if journal.get("status") in {"committed", "rolled_back", "resolved_context_only"}:
                continue
            self.journal, self.journal_path, self.transaction_dir = journal, path, path.parent
            if journal.get("status") == "commit_recording" and self._ledger_committed(journal):
                journal["status"] = "committed"
                self._save()
            else:
                self._rollback("recovered interrupted reconciliation")
            recovered.append({"transaction_id": journal["transaction_id"], "status": journal["status"],
                              "journal": str(path)})
        self.journal = None
        return recovered

    def _authenticate_starting_source(self) -> None:
        """A previous ordinary repair must carry committed provenance, not just PASS."""
        expected = self.handoff.get("source_sha256_before")
        if not expected or expected == digest(self.source):
            return
        verified = load(self.verification_path) if self.verification_path.is_file() else {}
        if (verified.get("passed") is not True or verified.get("cycle_id") != self.cycle
                or verified.get("ea") != self.ea or verified.get("source_sha256") != digest(self.source)
                or verified.get("stage1_spec_sha256") != digest(self.spec_path)):
            raise ValueError("reconciliation starting source has no authenticated ordinary repair")
        if not self.ledger_path.is_file():
            raise ValueError("ordinary repair provenance ledger is unavailable")
        with sqlite3.connect(f"{self.ledger_path.resolve().as_uri()}?mode=ro", uri=True) as connection:
            rows = connection.execute("SELECT payload_json FROM stage2_learning_observations WHERE cycle_id=? AND ea=?",
                                      (self.cycle, self.ea)).fetchall()
        for row in rows:
            event = json.loads(row[0])
            if (event.get("status") != "committed" or event.get("outcome_type") != "repair_transaction"
                    or event.get("source_after_sha256") != digest(self.source)
                    or event.get("stage1_spec_sha256") != digest(self.spec_path)):
                continue
            for ref in event.get("evidence_refs", []):
                path = self._inside_root(Path(ref.get("path") or ""))
                if path.is_file() and digest(path) == ref.get("sha256"):
                    return
        raise ValueError("reconciliation starting source has no committed ordinary repair")

    def _track_admission_artifact(self, path: Path) -> None:
        if any(Path(item["path"]).resolve() == Path(path).resolve() for item in self.journal["snapshots"]):
            return
        label = f"admission-{len(self.journal['snapshots'])}"
        self.journal["snapshots"].append(self._snapshot(Path(path), label))
        self._save()

    def _refresh_admission(self) -> None:
        if self.handoff.get("source_implementation_mode") != "model_generated_source_admitted":
            return
        import kcos_stage2_repair_loop_v1 as repair_loop
        if repair_loop._ACTIVE_EPISODE is not None:
            raise ValueError("implementation reconciliation cannot nest an ordinary repair transaction")
        self._track_admission_artifact(Path(self.handoff["source_build_receipt"]))
        original_tracker = repair_loop.track_transaction_artifact
        try:
            repair_loop.track_transaction_artifact = self._track_admission_artifact
            receipt = repair_loop.refresh_generated_source_admission(self.handoff_path, self.source, {
                "method": self.journal["method"], "changes": self.journal["changes"],
                "implementation_brief": self.journal["brief"],
                "implementation_brief_sha256": self.journal["brief_sha256"]})
        finally:
            repair_loop.track_transaction_artifact = original_tracker
        refreshed = load(self.handoff_path)
        mutable = {"source_implementation_review", "source_build_receipt", "source_build_receipt_sha256"}
        if ({k: v for k, v in refreshed.items() if k not in mutable}
                != {k: v for k, v in self.handoff.items() if k not in mutable}):
            raise ValueError("source admission refresh altered unrelated handoff configuration")
        self.journal["source_admission_refresh"] = receipt
        self.journal["handoff_sha256"] = digest(self.handoff_path)
        self._save()

    def __enter__(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        self._recover()
        self._identity()
        self._authenticate_starting_source()
        frozen = self._freeze()
        self.brief = resolve_implementation_brief(self.spec)
        original = self.source.read_bytes()
        source_text = original.decode("utf-8-sig")
        family = (self.spec.get("strategy") or {}).get("family")
        handler = HANDLERS.get(family)
        self.result = handler(source_text, self.brief) if handler else {
            "status": "resolved_context_only", "source_text": source_text,
            "method": "no_registered_family_reconciliation", "changes": []}
        if self.result.get("status") == "not_applicable":
            self.result = {**self.result, "status": "resolved_context_only"}
        status = self.result.get("status")
        if status not in {"reconciled", "already_reconciled", "resolved_context_only"}:
            raise ValueError(f"implementation reconciliation held: {self.result}")
        candidate = self.result.get("source_text", source_text)
        changed = candidate != source_text
        if changed and not self.result.get("changes"):
            raise ValueError("implementation reconciliation handler omitted changed-block evidence")
        transaction_id = uuid.uuid4().hex
        self.transaction_dir = self.directory / transaction_id
        self.transaction_dir.mkdir()
        self.journal_path = self.transaction_dir / "journal.json"
        brief_path = self.transaction_dir / "implementation_brief.json"
        write(brief_path, {"cycle_id": self.cycle, "ea": self.ea,
                          "stage1_spec_sha256": frozen["spec"], "brief": self.brief})
        self.journal = {"schema_version": 1, "transaction_id": transaction_id,
            "cycle_id": self.cycle, "ea": self.ea, "status": "prepared",
            "frozen": frozen, "brief": str(brief_path), "brief_sha256": digest(brief_path),
            "source_before_sha256": digest(self.source), "source_after_sha256": digest(self.source),
            "changed": changed, "method": self.result.get("method"),
            "implementation_status": status,
            "handoff_sha256": digest(self.handoff_path),
            "changes": self.result.get("changes", []), "snapshots": [],
            "handler_evidence": self.result.get("evidence"),
            "publication_status": "not_published"}
        try:
            # All mutable transaction artifacts are inventoried before applying source.
            for label, artifact in (("source", self.source), ("binary", self.binary),
                    ("handoff", self.handoff_path), ("compile", self.compile_path),
                    ("verification", self.verification_path)):
                self.journal["snapshots"].append(self._snapshot(artifact, label))
            self._save()
            if changed:
                candidate_bytes = candidate.encode("utf-8")
                if original.startswith(b"\xef\xbb\xbf"):
                    candidate_bytes = b"\xef\xbb\xbf" + candidate_bytes
                self.journal["source_after_sha256"] = hashlib.sha256(candidate_bytes).hexdigest().upper()
                self.journal["status"] = "applying"
                self._save()
                self.source.write_bytes(candidate_bytes)
                self._refresh_admission()
                self.journal["status"] = "applied"
                self._save()
            # A genuine diagnosed mismatch is recorded against immutable before-source
            # evidence. Context-only/no-op work does not manufacture a failure or repair.
            if changed:
                append_observation(self.ledger_path, {
                    "event_id": f"{self.cycle}:reconcile:{transaction_id}:diagnosed",
                    "cycle_id": self.cycle, "ea": self.ea, "stage": 2, "status": "failed",
                    "failure_kind": "IMPLEMENTATION_MISMATCH", "phase": "implementation_reconciliation",
                    "source": self.journal["snapshots"][0]["backup"],
                    "source_sha256": self.journal["source_before_sha256"],
                    "stage1_spec": str(self.spec_path), "stage1_spec_sha256": frozen["spec"],
                    "verifier_policy_identity": frozen["protected"]["contract_id"],
                    "diagnostic": self.result.get("method"), "failed_checks": [],
                    "implementation_reconciliation_status": status})
            else:
                if not self.ledger_path.is_file():
                    raise FileNotFoundError("existing Stage 2 episode ledger is unavailable")
                with sqlite3.connect(str(self.ledger_path)) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute("SELECT name FROM sqlite_master").fetchall()
                    connection.rollback()
            return self
        except BaseException:
            self._rollback("implementation reconciliation entry failed")
            raise

    def commit(self, verification_path: Path) -> dict:
        if self.committed:
            return self.journal
        if Path(verification_path).resolve() != self.verification_path.resolve():
            raise ValueError("reconciliation verification receipt path changed")
        self._identity()
        if self._freeze() != self.journal["frozen"]:
            raise ValueError("reconciliation spec, executing code, or protected contract changed")
        if digest(Path(self.journal["brief"])) != self.journal["brief_sha256"]:
            raise ValueError("implementation brief changed during transaction")
        if digest(self.handoff_path) != self.journal["handoff_sha256"]:
            raise ValueError("reconciliation handoff/configuration changed")
        if digest(self.source) != self.journal["source_after_sha256"]:
            raise ValueError("reconciled candidate source changed")
        verification, compiled = load(self.verification_path), load(self.compile_path)
        for receipt in (verification, compiled):
            if (receipt.get("passed") is not True or receipt.get("cycle_id") != self.cycle
                    or receipt.get("ea") != self.ea):
                raise ValueError("reconciliation requires same-cycle passing compile and verification")
        if (verification.get("source_sha256") != digest(self.source)
                or verification.get("stage1_spec_sha256") != self.journal["frozen"]["spec"]
                or Path(verification.get("source") or "").resolve() != self.source
                or Path(verification.get("ex5") or "").resolve() != self.binary
                or compiled.get("mq5_sha256") != digest(self.source)
                or compiled.get("ex5_sha256") != digest(self.binary)
                or compiled.get("fresh_ex5") is not True
                or compiled.get("errors") != 0 or compiled.get("warnings") != 0
                or compiled.get("exit_code") != 0
                or verification.get("compile") != compiled
                or verification.get("protected_behavior_contract") != self.journal["frozen"]["protected"]
                or verification.get("verification_policy_sha256") != self.journal["frozen"]["contract"]
                or verification.get("verifier_code_sha256") != self.journal["frozen"]["code"]["kcos_stage2_verify_v2.py"]):
            raise ValueError("reconciliation current source/binary/receipt authentication failed")
        receipt_path = self.transaction_dir / "outcome.committed.json"
        write(receipt_path, {**self.journal, "status": "committed", "verification": str(self.verification_path),
              "verification_sha256": digest(self.verification_path), "compile_sha256": digest(self.compile_path),
              "source_semantics_review": self.result.get("status"), "repair_changed_source": self.journal["changed"],
              "publication_status": "not_published"})
        self.journal["status"] = "commit_recording"
        self._save()
        self._record_outcome("committed", receipt_path)
        self.committed = True
        self.journal["status"] = "committed"
        self._save()
        return self.journal

    def _rollback(self, reason: str) -> None:
        if not self.journal or self.committed:
            return
        archive = self.transaction_dir / "rejected"
        archive.mkdir(exist_ok=True)
        for index, item in enumerate(self.journal["snapshots"]):
            artifact = self._inside_root(Path(item["path"]))
            backup = Path(item["backup"]).resolve()
            backup.relative_to(self.transaction_dir.resolve())
            archived = archive / f"{index}.{artifact.name}"
            if artifact.is_file() and not archived.exists():
                shutil.copyfile(artifact, archived)
            if item["exists"]:
                if digest(backup) != item["sha256"]:
                    raise ValueError("reconciliation recovery snapshot corrupted")
                shutil.copyfile(backup, artifact)
            elif artifact.is_file():
                artifact.unlink()
        self.journal["status"] = "rollback_recording"
        self.journal["rejection_reason"] = reason
        self._save()
        receipt = self.transaction_dir / "outcome.rolled_back.json"
        write(receipt, {**self.journal, "status": "rolled_back", "proposal_accepted": False})
        self._record_outcome("rolled_back", receipt)
        self.journal["status"] = "rolled_back"
        self._save()

    def __exit__(self, kind, error, traceback):
        if not self.committed:
            self._rollback(str(error) if error else "verification did not commit reconciliation")
        return False
