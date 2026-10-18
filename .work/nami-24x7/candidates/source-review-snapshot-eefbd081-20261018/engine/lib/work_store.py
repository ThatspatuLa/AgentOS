"""Canonical local workflow ledger. SQLite transactions fence every writer.

JSON queues are import-only history. Results and their reporting outbox events
commit together; consumers use stable event IDs to acknowledge exactly once.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from contextlib import contextmanager, nullcontext
from pathlib import Path


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


_RETENTION_FIELDS = frozenset({
    "record_id", "captured_at", "expires_at", "parent_ids", "retention_policy",
})


def _verified_hypothesis_retention(db, hypothesis, now):
    """Verify hypothesis retention lineage against committed MentionStore rows."""
    from engine.lib.collection_retention import (
        RetentionHold, derived_expiry_fields, expiry_fields,
        validate_capture_timestamp,
    )
    from engine.lib.contracts import canonical_json
    from engine.lib.mention_identity import _resolve_nami_mention_identity

    identity = hypothesis.get("id")
    if not isinstance(identity, str) or not identity or hypothesis.get("record_id") != identity:
        raise ValueError("TEST hypothesis retention identity is unavailable")
    instant = datetime.fromtimestamp(now, timezone.utc)
    try:
        capture = validate_capture_timestamp(hypothesis.get("created_at"), now=instant)
        stored_capture = validate_capture_timestamp(hypothesis.get("captured_at"), now=instant)
    except RetentionHold as exc:
        raise ValueError("TEST hypothesis capture timestamp is unavailable") from exc
    if capture != stored_capture or hypothesis.get("captured_at") != capture.isoformat():
        raise ValueError("TEST hypothesis capture differs from created_at")

    mention_ids = hypothesis.get("mention_ids")
    parent_ids = hypothesis.get("parent_ids")
    if (not isinstance(mention_ids, list) or not mention_ids
            or any(not isinstance(value, str) or not value for value in mention_ids)
            or len(set(mention_ids)) != len(mention_ids)
            or not isinstance(parent_ids, list) or parent_ids != sorted(mention_ids)):
        raise ValueError("TEST hypothesis parent IDs are missing or inconsistent")

    parent_expiries = []
    expected_lineage = []
    for mention_id in parent_ids:
        row = db.execute("""SELECT m.payload,m.status,r.canonical_payload_sha256
            FROM mentions m JOIN mention_intake_receipts r ON r.mention_id=m.id
            WHERE m.id=?""", (mention_id,)).fetchone()
        if row is None:
            raise ValueError("TEST hypothesis parent is not in the committed MentionStore")
        try:
            if row["status"] != "done":
                raise ValueError("TEST hypothesis parent was not committed by HYPOTHESIZE")
            payload_hash = hashlib.sha256(row["payload"].encode()).hexdigest()
            if payload_hash != row["canonical_payload_sha256"]:
                raise ValueError("TEST hypothesis parent differs from its intake receipt")
            parent = json.loads(row["payload"])
            resolved = _resolve_nami_mention_identity(parent, row_primary_key=mention_id)
            parent_capture = validate_capture_timestamp(parent.get("captured_at"), now=instant)
            parent_retention = expiry_fields(parent_capture)
        except (ValueError, RetentionHold, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("TEST hypothesis parent retention evidence is invalid") from exc
        if resolved != mention_id or parent.get("expires_at") != parent_retention["expires_at"]:
            raise ValueError("TEST hypothesis parent identity or P3M expiry differs")
        if parent_capture > capture:
            raise ValueError("TEST hypothesis capture predates a retained parent")
        parent_expiries.append(parent["expires_at"])
        expected_lineage.append({
            "mention_id": mention_id,
            "captured_at": parent_retention["captured_at"],
            "expires_at": parent_retention["expires_at"],
            "payload_sha256": hashlib.sha256(canonical_json(parent).encode()).hexdigest(),
        })
    try:
        retention = derived_expiry_fields(capture, parent_expiries)
    except RetentionHold as exc:
        raise ValueError("TEST hypothesis inherited expiry is unavailable") from exc
    if (hypothesis.get("retention_policy") != retention["retention_policy"]
            or hypothesis.get("expires_at") != retention["expires_at"]
            or hypothesis.get("retention_lineage") != expected_lineage):
        raise ValueError("TEST hypothesis retention lineage or expiry differs from stored parents")
    if datetime.fromisoformat(retention["expires_at"]) <= instant:
        raise ValueError("TEST hypothesis retention has expired")
    return identity


def _test_retention_payload(db, result, hypothesis, committed_at):
    """Attach system-owned, parent-bounded metadata to a committed TEST result."""
    from engine.lib.collection_retention import (
        RetentionHold, derived_expiry_fields, validate_capture_timestamp,
    )

    result_id = result.get("result_id")
    attempt_id = result.get("attempt_id")
    hypothesis_id = hypothesis.get("id")
    if not isinstance(result_id, str) or not result_id:
        raise ValueError("TEST result lacks system-owned result identity")
    if not isinstance(attempt_id, str) or not attempt_id or result_id != "result-" + attempt_id:
        raise ValueError("TEST result identity does not match its system-owned attempt")
    if not isinstance(hypothesis_id, str) or not hypothesis_id:
        raise ValueError("TEST hypothesis identity is unavailable")
    if _verified_hypothesis_retention(db, hypothesis, committed_at) != hypothesis_id:
        raise ValueError("TEST hypothesis retention identity is unavailable")
    captured = datetime.fromtimestamp(committed_at, timezone.utc)
    try:
        captured = validate_capture_timestamp(captured, now=captured).isoformat()
        retention = derived_expiry_fields(captured, [hypothesis["expires_at"]])
    except RetentionHold as exc:
        raise ValueError("TEST result capture-time retention is unavailable") from exc
    metadata = {
        "record_id": result_id, "captured_at": captured,
        "parent_ids": [hypothesis_id], **retention,
    }
    for key, value in metadata.items():
        if key in result and result[key] != value:
            raise ValueError("TEST result retention metadata is system-owned")
    return {**result, **metadata}


class IdentityCollision(ValueError):
    pass


class StaleLease(RuntimeError):
    pass


class WorkStore:
    def __init__(self, path):
        self.path = Path(path)
        with self._db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS work (
                  id TEXT PRIMARY KEY, payload TEXT NOT NULL, payload_hash TEXT NOT NULL,
                  status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                  token TEXT UNIQUE, attempt_id TEXT, owner TEXT, lease_until REAL,
                  available_at REAL NOT NULL DEFAULT 0, legacy INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS results (
                  token TEXT PRIMARY KEY, hypothesis_id TEXT NOT NULL,
                  attempt_id TEXT UNIQUE NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events (
                  event_id TEXT PRIMARY KEY, hypothesis_id TEXT NOT NULL,
                  payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS acknowledgements (
                  consumer TEXT NOT NULL, event_id TEXT NOT NULL,
                  PRIMARY KEY (consumer,event_id));
                CREATE TABLE IF NOT EXISTS imports (path TEXT PRIMARY KEY, digest TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS data_recoveries (
                  hypothesis_id TEXT NOT NULL, version TEXT NOT NULL,
                  attached_at REAL NOT NULL, reason TEXT NOT NULL,
                  PRIMARY KEY(hypothesis_id,version));
            ''')
            columns = {row[1] for row in db.execute('PRAGMA table_info(work)')}
            if 'generation_attempts' not in columns:
                db.execute('ALTER TABLE work ADD COLUMN generation_attempts INTEGER NOT NULL DEFAULT 0')
                db.execute('UPDATE work SET generation_attempts=attempts')

    @contextmanager
    def _db(self):
        from engine.lib.exact_root_lease import ExactRootQuiescenceLease
        # Every WorkStore operation, including schema initialization and WAL
        # sidecar writes, holds the same exact directory lease. `_transaction`
        # delegates to `_db`, so this intentionally avoids nested acquisition.
        with ExactRootQuiescenceLease(self.path.parent) as lease:
            lease.assert_bound_root(self.path.parent)
            lease.assert_safe_file_or_missing(self.path.name)
            db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            db.row_factory = sqlite3.Row
            try:
                db.execute('PRAGMA journal_mode=WAL')
                db.execute('PRAGMA synchronous=FULL')
                yield db
            finally:
                db.close()

    @contextmanager
    def _transaction(self):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def enqueue_hypotheses(self, records, legacy=False, connection=None):
        inserted = 0
        with (self._transaction() if connection is None else nullcontext(connection)) as db:
            for record in records:
                if not isinstance(record, dict) or not isinstance(record.get('id'), str) or not record['id']:
                    raise ValueError('hypothesis requires nonempty system-owned id')
                payload = _json(record)
                digest = hashlib.sha256(payload.encode()).hexdigest()
                old = db.execute('SELECT payload_hash FROM work WHERE id=?', (record['id'],)).fetchone()
                if old:
                    if old['payload_hash'] != digest:
                        raise IdentityCollision(record['id'])
                    continue
                if legacy:
                    status = 'legacy_unverified'
                else:
                    status = record.get('status', 'queued')
                if not legacy and status != 'queued':
                    raise ValueError('new work must be queued')
                db.execute('INSERT INTO work(id,payload,payload_hash,status,legacy) VALUES (?,?,?,?,?)',
                           (record['id'], payload, digest, status, int(legacy)))
                inserted += 1
        return inserted

    def import_legacy(self, path):
        """One atomic import; later archival of the historical file is harmless."""
        path=Path(path)
        if not path.exists():
            return 0
        key=str(path.resolve())
        with self._transaction() as db:
            if db.execute('SELECT 1 FROM imports WHERE path=?',(key,)).fetchone():
                return 0
            raw=path.read_bytes()
            records=json.loads(raw)
            if not isinstance(records,list):
                raise ValueError('legacy queue must be an array')
            count=self.enqueue_hypotheses(records,legacy=True,connection=db)
            db.execute('INSERT INTO imports VALUES (?,?)',(key,hashlib.sha256(raw).hexdigest()))
            return count

    def pending_count(self):
        with self._db() as db:
            return db.execute("SELECT count(*) FROM work WHERE legacy=0 AND (status='queued' OR (status='retry_wait' AND available_at<=?) OR (status='leased' AND lease_until<=?))", (time.time(),time.time())).fetchone()[0]

    def claim(self, worker, lease_seconds=300, max_attempts=3):
        from engine.lib.runtime_safety import check_claim_allowed
        check_claim_allowed(self.path.parent)
        if lease_seconds <= 0 or max_attempts < 1:
            raise ValueError('positive lease and attempt budget required')
        now = time.time()
        with self._transaction() as db:
            db.execute("UPDATE work SET status='legacy_unverified',token=NULL,owner=NULL,lease_until=NULL WHERE legacy=1 AND status IN ('queued','retry_wait','leased')")
            eligible = db.execute("SELECT id,payload FROM work WHERE legacy=0 AND (status='queued' OR (status='retry_wait' AND available_at<=?) OR (status='leased' AND lease_until<=?)) ORDER BY available_at,rowid", (now,now)).fetchall()
            for item in eligible:
                try:
                    _verified_hypothesis_retention(db, json.loads(item['payload']), now)
                except (ValueError, TypeError, json.JSONDecodeError):
                    db.execute("UPDATE work SET status='retention_held',token=NULL,owner=NULL,lease_until=NULL WHERE id=?", (item['id'],))
            exhausted = db.execute("SELECT * FROM work WHERE status='leased' AND lease_until<=? AND generation_attempts>=?", (now,max_attempts)).fetchall()
            for row in exhausted:
                result = {'schema_version':1,'hypothesis_id':row['id'],'attempt_id':row['attempt_id'],
                          'result_id':'result-'+row['attempt_id'],'status':'rejected_evidence',
                          'failure_class':'runtime_error','reason':'lease retry budget exhausted',
                          'metrics':{},'gate_results':{}}
                self._commit(db,row,result,now,retry=False)
            row = db.execute("SELECT * FROM work WHERE (status='queued' OR (status='retry_wait' AND available_at<=?) OR (status='leased' AND lease_until<=?)) AND generation_attempts<? ORDER BY available_at,rowid LIMIT 1",(now,now,max_attempts)).fetchone()
            if row is None:
                return None
            token = uuid.uuid4().hex
            attempt = 'test-'+uuid.uuid4().hex
            db.execute("UPDATE work SET status='leased',attempts=attempts+1,generation_attempts=generation_attempts+1,token=?,attempt_id=?,owner=?,lease_until=? WHERE id=?", (token,attempt,worker,now+lease_seconds,row['id']))
            hyp=json.loads(row['payload'])
            hyp.update(status='leased',attempt_id=attempt,
                       retry_wait=row['status']=='retry_wait',
                       retried_count=max(0, row['generation_attempts']))
            return {'token':token,'hypothesis':hyp,'attempt_id':attempt,'lease_until':now+lease_seconds}

    def heartbeat(self, token, lease_seconds=300):
        if lease_seconds <= 0:
            raise ValueError('positive lease required')
        now=time.time()
        with self._transaction() as db:
            return db.execute("UPDATE work SET lease_until=? WHERE token=? AND status='leased' AND lease_until>?", (now+lease_seconds,token,now)).rowcount == 1

    def _commit(self,db,row,result,now,retry=False,retry_delay=60):
        original=json.loads(row['payload'])
        result=_test_retention_payload(db,result,original,now)
        payload=_json(result)
        event_id='result-'+row['attempt_id']
        db.execute('INSERT INTO results VALUES (?,?,?,?)',(row['token'],row['id'],row['attempt_id'],payload))
        event={'event_id':event_id,'timestamp':datetime.fromtimestamp(now, timezone.utc).isoformat(),'hypothesis_id':row['id'],'attempt_id':row['attempt_id'],'stage':'test','status':result['status'],'result':result}
        event.update({key:result[key] for key in _RETENTION_FIELDS})
        event.update({key:original.get(key) for key in ('source_type','source','edge_type','market')})
        event['source_identity'] = result.get('source_identity') or original.get('source_identity')
        db.execute('INSERT INTO events VALUES (?,?,?)',(event_id,row['id'],_json(event)))
        db.execute('UPDATE work SET status=?,lease_until=NULL,available_at=? WHERE id=?', ('retry_wait' if retry else result['status'],now+retry_delay,row['id']))

    def complete(self, token, result, retry=False, retry_delay=60, max_attempts=3):
        from engine.lib.contracts import validate_result
        errors=validate_result(result)
        if errors:
            raise ValueError('invalid result: '+'; '.join(errors))
        now=time.time()
        with self._transaction() as db:
            old=db.execute('SELECT payload FROM results WHERE token=?',(token,)).fetchone()
            if old:
                stored=json.loads(old['payload'])
                supplied_metadata={key:result[key] for key in _RETENTION_FIELDS if key in result}
                if any(stored.get(key) != value for key,value in supplied_metadata.items()):
                    raise IdentityCollision('completed attempt retention metadata differs')
                stored_base={key:value for key,value in stored.items() if key not in _RETENTION_FIELDS}
                result_base={key:value for key,value in result.items() if key not in _RETENTION_FIELDS}
                if stored_base != result_base:
                    raise IdentityCollision('completed attempt payload differs')
                return False
            row=db.execute("SELECT * FROM work WHERE token=? AND status='leased' AND lease_until>?",(token,now)).fetchone()
            if row is None:
                raise StaleLease('lease expired or superseded')
            if row['legacy']:
                db.execute("UPDATE work SET status='legacy_unverified',token=NULL,owner=NULL,lease_until=NULL WHERE id=?", (row['id'],))
                return False
            if result.get('hypothesis_id') != row['id'] or result.get('attempt_id') != row['attempt_id']:
                raise IdentityCollision('result lineage differs from claim')
            retryable = retry and result.get('status') == 'retry_wait'
            if retryable and row['generation_attempts'] >= max_attempts:
                result = dict(result, status='rejected_evidence', promotion_status='rejected')
                retryable = False
            self._commit(db,row,result,now,retry=retryable,retry_delay=retry_delay)
        return True

    def verify_test_stage_commit(self, *, hypothesis_id, attempt_id, result_id,
                                 event_id, invocation_id):
        """Verify one TEST commit is durably tied to its exact stage invocation."""
        if not all(isinstance(value, str) and value for value in
                   (hypothesis_id, attempt_id, result_id, event_id, invocation_id)):
            return False
        with self._db() as db:
            work = db.execute('SELECT 1 FROM work WHERE id=?', (hypothesis_id,)).fetchone()
            row = db.execute('SELECT hypothesis_id,attempt_id,payload FROM results WHERE attempt_id=?',
                             (attempt_id,)).fetchone()
            event_row = db.execute('SELECT hypothesis_id,payload FROM events WHERE event_id=?',
                                   (event_id,)).fetchone()
        if not work or not row or not event_row:
            return False
        try:
            result = json.loads(row['payload'])
            event = json.loads(event_row['payload'])
        except (ValueError, TypeError):
            return False
        return bool(
            row['hypothesis_id'] == hypothesis_id and row['attempt_id'] == attempt_id
            and result.get('hypothesis_id') == hypothesis_id
            and result.get('attempt_id') == attempt_id
            and result.get('result_id') == result_id
            and result.get('stage_invocation_id') == invocation_id
            and event_row['hypothesis_id'] == hypothesis_id
            and event.get('event_id') == event_id
            and event.get('stage') == 'test'
            and event.get('hypothesis_id') == hypothesis_id
            and event.get('attempt_id') == attempt_id
            and event.get('status') == result.get('status')
            and isinstance(event.get('result'), dict)
            and _json(event['result']) == _json(result)
        )

    def verified_test_stage_commits(self, invocation_id):
        """Enumerate every exact, internally consistent commit owned by invocation_id."""
        if not isinstance(invocation_id, str) or not invocation_id:
            return None
        with self._db() as db:
            rows = db.execute('SELECT hypothesis_id,attempt_id,payload FROM results ORDER BY rowid').fetchall()
        commits = []
        try:
            for row in rows:
                result = json.loads(row['payload'])
                if result.get('stage_invocation_id') != invocation_id:
                    continue
                record = {
                    'hypothesis_id': row['hypothesis_id'],
                    'attempt_id': row['attempt_id'],
                    'result_id': result.get('result_id'),
                    'event_id': 'result-' + row['attempt_id'],
                }
                if not self.verify_test_stage_commit(**record, invocation_id=invocation_id):
                    return None
                commits.append(record)
        except (ValueError, TypeError, KeyError):
            return None
        identities = [tuple(record[key] for key in
                            ('hypothesis_id','attempt_id','result_id','event_id'))
                      for record in commits]
        if len(identities) != len(set(identities)):
            return None
        return commits

    def verified_test_dataset_manifest_ref(self, attempt_id, invocation_id):
        """Return a content-addressed manifest ref only for a fully validated result."""
        with self._db() as db:
            row = db.execute('SELECT payload FROM results WHERE attempt_id=?', (attempt_id,)).fetchone()
        if not row:
            return None
        try:
            result = json.loads(row['payload'])
            if result.get('stage_invocation_id') != invocation_id:
                return None
            manifest = result.get('evidence_manifest')
            if not isinstance(manifest, dict):
                return None
            from engine.lib.contracts import validate_result
            if validate_result(result):
                return None
            path = Path(manifest['dataset_manifest_path']).resolve()
            raw = path.read_bytes()
            metadata = json.loads(raw)
            frozen = metadata.get('dataset_manifest', {})
            if (metadata.get('hypothesis_id') != result.get('hypothesis_id')
                    or frozen.get('dataset_hash') != result.get('dataset_hash')
                    or frozen.get('dataset_id') != result.get('dataset_id')):
                return None
            digest = hashlib.sha256(raw).hexdigest()
            return f'dataset_manifest:{path}#sha256:{digest}'
        except (OSError, ValueError, TypeError, KeyError):
            return None

    def requeue_data_unavailable(self, decision_log_path, restored_sources, *, verified_versions=None):
        """A source heartbeat cannot requeue work: require changed validated evidence."""
        if not verified_versions:
            return []
        path = Path(decision_log_path)
        restored = {str(value).strip().lower() for value in restored_sources if value}
        if not path.exists() or not restored:
            return []
        failed_ids = set()
        for line in path.read_text(encoding='utf-8').splitlines():
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            extra = record.get('extra') if isinstance(record.get('extra'), dict) else {}
            result = record.get('result') if isinstance(record.get('result'), dict) else {}
            failure = record.get('failure_class') or extra.get('failure_class') or result.get('failure_class')
            if failure != 'data_unavailable':
                continue
            sources = {record.get(key) for key in ('source_identity','source_type','market')}
            sources.update(extra.get(key) for key in ('source_identity','source_type','market'))
            sources.update(result.get(key) for key in ('source_identity','source_type','market'))
            sources = {str(value).strip().lower() for value in sources if value}
            if not sources.intersection(restored):
                continue
            identity = record.get('hypothesis_id') or result.get('hypothesis_id')
            if identity:
                failed_ids.add(str(identity))
        requeued = []
        with self._transaction() as db:
            for identity in sorted(failed_ids):
                version = verified_versions.get(identity)
                if not isinstance(version, str) or len(version) != 64:
                    continue
                if db.execute('SELECT 1 FROM data_recoveries WHERE hypothesis_id=? AND version=?', (identity,version)).fetchone():
                    continue
                changed = db.execute(
                    "UPDATE work SET status='queued',generation_attempts=0,token=NULL,attempt_id=NULL,owner=NULL,lease_until=NULL,available_at=0 WHERE id=? AND status='blocked_on_data'",
                    (identity,)).rowcount
                if changed:
                    db.execute('INSERT INTO data_recoveries VALUES(?,?,?,?)', (identity,version,time.time(),'validated dataset/capability version changed'))
                    requeued.append(identity)
        return requeued

    def attach_dataset(self, identity, dataset_path, manifest_path, version):
        """Explicit data attachment, authenticated against immutable content, once per version."""
        if not isinstance(version, str) or len(version) != 64:
            raise ValueError('SHA256 dataset version required')
        path = Path(dataset_path)
        if hashlib.sha256(path.read_bytes()).hexdigest() != version:
            raise ValueError('attached dataset hash differs')
        if not Path(manifest_path).is_file():
            raise ValueError('manifest required')
        with self._transaction() as db:
            row = db.execute('SELECT * FROM work WHERE id=?', (identity,)).fetchone()
            if row is None or row['status'] not in {'blocked_on_data','catalog_watch'}:
                return False
            if db.execute('SELECT 1 FROM data_recoveries WHERE hypothesis_id=? AND version=?',(identity,version)).fetchone():
                return False
            hyp = json.loads(row['payload'])
            hyp.update(evaluation_dataset_path=str(path.resolve()), evaluation_dataset_manifest_path=str(Path(manifest_path).resolve()))
            payload = _json(hyp)
            db.execute("UPDATE work SET payload=?,payload_hash=?,status='queued',generation_attempts=0,token=NULL,attempt_id=NULL,owner=NULL,lease_until=NULL,available_at=0 WHERE id=?",
                       (payload,hashlib.sha256(payload.encode()).hexdigest(),identity))
            db.execute('INSERT INTO data_recoveries VALUES(?,?,?,?)',(identity,version,time.time(),'explicit content-addressed attachment; preflight required'))
        return True

    def snapshot(self):
        with self._transaction() as db:
            hypotheses=[]
            for row in db.execute('SELECT * FROM work ORDER BY rowid'):
                item=json.loads(row['payload'])
                item['hypothesis_scope']={field:item[field] for field in
                    ('frozen_rule','evaluation_protocol_hash','dataset_id','dataset_hash') if field in item}
                item.update(status=row['status'],attempt_id=row['attempt_id'],legacy_import=bool(row['legacy']),
                            attempts=row['attempts'],retried_count=max(0,row['attempts']-1),
                            retry_wait=row['status']=='retry_wait',lease_until=row['lease_until'],
                            available_at=row['available_at'],owner=row['owner'])
                latest=db.execute('SELECT payload FROM results WHERE attempt_id=?',(row['attempt_id'],)).fetchone()
                if latest:
                    test_result=json.loads(latest[0])
                    item['test_result']=test_result
                    # Dataset identity is minted by TEST when the immutable
                    # capture is frozen; rule and protocol remain sourced from
                    # the original hypothesis payload for stale-scope checks.
                    for field in ('dataset_id','dataset_hash'):
                        if field in test_result:
                            item['hypothesis_scope'][field]=test_result[field]
                    # Surface the exact system-owned evaluation scope attached
                    # to the latest committed attempt for Overlooker comparison.
                    for field in ('frozen_rule','evaluation_protocol_hash','dataset_id','dataset_hash'):
                        if field in test_result:
                            item[field]=test_result[field]
                hypotheses.append(item)
            return {'hypotheses':hypotheses,'results':[json.loads(r[0]) for r in db.execute('SELECT payload FROM results ORDER BY rowid')], 'events':[json.loads(r[0]) for r in db.execute('SELECT payload FROM events ORDER BY rowid')]}

    def pending_events(self, consumer):
        with self._db() as db:
            return [json.loads(r[0]) for r in db.execute('SELECT payload FROM events WHERE event_id NOT IN (SELECT event_id FROM acknowledgements WHERE consumer=?) ORDER BY rowid',(consumer,))]

    def acknowledge(self, consumer, event_ids):
        with self._transaction() as db:
            for event_id in event_ids:
                if not db.execute('SELECT 1 FROM events WHERE event_id=?',(event_id,)).fetchone():
                    raise ValueError('unknown event')
                db.execute('INSERT OR IGNORE INTO acknowledgements VALUES (?,?)',(consumer,event_id))
