"""Durable JSONL ingestion and exact, transactional mention acknowledgement."""
import hashlib
import json
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from contextlib import contextmanager
from pathlib import Path
from engine import config
from engine.lib.contracts import canonical_json
from engine.lib.work_store import WorkStore


class MentionStore:
    def __init__(self, path=None):
        self.path = Path(path or config.STATE_DIR / 'workflow.sqlite3')
        self.work = WorkStore(self.path)
        with self.connect() as db:
            db.executescript('''
              CREATE TABLE IF NOT EXISTS mention_cursors(path TEXT PRIMARY KEY, offset INTEGER NOT NULL, digest TEXT, generation_id TEXT, generation_store_root TEXT, cursor_version INTEGER NOT NULL DEFAULT 1, tombstones_digest TEXT);
              CREATE TABLE IF NOT EXISTS mentions(id TEXT PRIMARY KEY,payload TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',token TEXT,lease_until REAL,action TEXT,attempts INTEGER NOT NULL DEFAULT 0,last_error_code TEXT,quarantined_at REAL);
              CREATE TABLE IF NOT EXISTS mention_intake_receipts(mention_id TEXT PRIMARY KEY,stream_path TEXT NOT NULL,start_offset INTEGER NOT NULL,end_offset INTEGER NOT NULL,record_sha256 TEXT NOT NULL,listen_invocation_id TEXT,source TEXT,canonical_payload_sha256 TEXT,generation_id TEXT,generation_store_root TEXT);
              CREATE TABLE IF NOT EXISTS mention_batches(token TEXT PRIMARY KEY,status TEXT NOT NULL,actions TEXT);
              CREATE TABLE IF NOT EXISTS mention_failures(token TEXT NOT NULL,mention_id TEXT NOT NULL,attempt_number INTEGER NOT NULL,error_code TEXT NOT NULL,error_details TEXT,created_at REAL NOT NULL,PRIMARY KEY(token,mention_id));
              CREATE TABLE IF NOT EXISTS retention_transactions(transaction_id TEXT PRIMARY KEY,schema_sha256 TEXT NOT NULL,database_identity TEXT NOT NULL,source_generation TEXT NOT NULL,post_row_count INTEGER NOT NULL,post_tombstone_count INTEGER NOT NULL,transition_sha256 TEXT NOT NULL,created_at TEXT NOT NULL);
            ''')
            if 'digest' not in {r[1] for r in db.execute('PRAGMA table_info(mention_cursors)')}:
                db.execute('ALTER TABLE mention_cursors ADD COLUMN digest TEXT')
            cursor_columns={r[1] for r in db.execute('PRAGMA table_info(mention_cursors)')}
            for name in ('generation_id','generation_store_root'):
                if name not in cursor_columns: db.execute(f'ALTER TABLE mention_cursors ADD COLUMN {name} TEXT')
            cursor_columns={r[1] for r in db.execute('PRAGMA table_info(mention_cursors)')}
            for name, declaration in (('cursor_version','INTEGER NOT NULL DEFAULT 1'),('tombstones_digest','TEXT')):
                if name not in cursor_columns: db.execute(f'ALTER TABLE mention_cursors ADD COLUMN {name} {declaration}')
            if 'canonical_payload_sha256' not in {r[1] for r in db.execute('PRAGMA table_info(mention_intake_receipts)')}:
                db.execute('ALTER TABLE mention_intake_receipts ADD COLUMN canonical_payload_sha256 TEXT')
            intake_columns={r[1] for r in db.execute('PRAGMA table_info(mention_intake_receipts)')}
            for name in ('generation_id','generation_store_root'):
                if name not in intake_columns: db.execute(f'ALTER TABLE mention_intake_receipts ADD COLUMN {name} TEXT')
            columns = {r[1] for r in db.execute('PRAGMA table_info(mentions)')}
            batch_columns = {r[1] for r in db.execute('PRAGMA table_info(mention_batches)')}
            for name, declaration in (('stage_invocation_id','TEXT'), ('committed_at','REAL'), ('claimed_inputs','TEXT'), ('quarantined_inputs','TEXT'), ('model_calls','TEXT NOT NULL DEFAULT \'[]\''), ('model_call_required','INTEGER NOT NULL DEFAULT 0')):
                if name not in batch_columns:
                    db.execute(f'ALTER TABLE mention_batches ADD COLUMN {name} {declaration}')
            for name, declaration in (
                ('attempts', 'INTEGER NOT NULL DEFAULT 0'),
                ('invalid_attempts', 'INTEGER NOT NULL DEFAULT 0'),
                ('available_at', 'REAL NOT NULL DEFAULT 0'),
                ('last_claim_at', 'REAL NOT NULL DEFAULT 0'),
                ('last_error_code', 'TEXT'),
                ('quarantined_at', 'REAL'),
            ):
                if name not in columns:
                    db.execute(f'ALTER TABLE mentions ADD COLUMN {name} {declaration}')

    @contextmanager
    def connect(self):
        from engine.lib.exact_root_lease import ExactRootQuiescenceLease
        # MentionStore shares workflow.sqlite3 with WorkStore. Hold the same
        # directory gate for schema, row, cursor, and WAL sidecar writes.
        # `commit_stage` passes its active connection into WorkStore, avoiding
        # a nested acquisition of this non-reentrant OS gate.
        with ExactRootQuiescenceLease(self.path.parent) as lease:
            lease.assert_bound_root(self.path.parent)
            lease.assert_safe_file_or_missing(self.path.name)
            db = sqlite3.connect(self.path, timeout=30)
            db.row_factory = sqlite3.Row
            try:
                with db:
                    yield db
            finally:
                db.close()

    def ingest(self, path, *, generation_id=None, generation_store_root=None, _generation_store=None):
        path = Path(path)
        if not path.exists():
            return 0
        generation_store=None
        if generation_id is not None or generation_store_root is not None:
            if not isinstance(generation_id,str) or not generation_id or generation_store_root is None:
                raise ValueError('generation identity and store root must be supplied together')
            from engine.lib.raw_generation_store import RawGenerationStore, GenerationStoreError
            try:
                generation_store=_generation_store or RawGenerationStore(generation_store_root,fast_open=True)
                if generation_store.generation_path(generation_id)!=path.resolve():
                    raise ValueError('generation stream path does not match catalog')
            except (GenerationStoreError, OSError) as exc:
                raise ValueError('unknown or invalid raw generation') from exc
        cursor_path=(generation_store.stream_identity(generation_id) if generation_store is not None
                     else str(path.resolve()))
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT offset,digest,generation_id,generation_store_root,cursor_version,tombstones_digest FROM mention_cursors WHERE path=?',(cursor_path,)).fetchone()
            if row and row['cursor_version'] != 1:
                raise ValueError('mention cursor version unsupported')
            if row and (row['generation_id']!=generation_id or row['generation_store_root']!=(str(Path(generation_store_root).resolve()) if generation_store_root is not None else None)):
                raise ValueError('mention cursor generation identity mismatch')
            offset = row['offset'] if row else 0
            if generation_store is not None and not generation_store.verify_cursor_offset(generation_id,offset,row['digest'] if row else None):
                raise ValueError('generation cursor is not at a verified record boundary')
            if path.stat().st_size < offset:
                raise ValueError('mention stream truncated; verified replay required')
            count = 0
            tombstones_digest=(row['tombstones_digest'] if row and row['tombstones_digest']
                               else hashlib.sha256(b'').hexdigest())
            with path.open('rb') as stream:
                prefix=hashlib.sha256()
                if generation_store is None:
                    remaining=offset
                    while remaining:
                        chunk=stream.read(min(1024*1024,remaining))
                        if not chunk:raise ValueError('mention stream truncated')
                        prefix.update(chunk);remaining-=len(chunk)
                    if row and offset and (not row['digest'] or prefix.hexdigest()!=row['digest']):
                        raise ValueError('consumed mention bytes changed; verified replay required')
                elif offset:
                    prefix.update((row['digest'] or '').encode())
                    stream.seek(offset)
                while True:
                    line_start=stream.tell()
                    raw = stream.readline()
                    if not raw or not raw.endswith(b'\n'):
                        break  # a concurrently appended partial line is not acknowledged
                    line_end=stream.tell()
                    if generation_store is None: prefix.update(raw)
                    if raw.strip():
                        generation_record=None
                        if generation_store is not None:
                            try:
                                generation_record=generation_store.verify_record_range(generation_id,line_start,len(raw),hashlib.sha256(raw).hexdigest())
                            except GenerationStoreError as exc:
                                raise ValueError('raw generation record mismatch') from exc
                            if generation_record is None:
                                raise ValueError('raw generation record mismatch')
                        item = json.loads(raw)
                        if not isinstance(item,dict):
                            raise ValueError('mention must be an object')
                        if item.get('purge_tombstone') is True:
                            from engine.lib.collection_retention import (
                                POLICY, RetentionHold, add_calendar_months, parse_capture_time,
                            )
                            required={'record_id','captured_at','expires_at','retention_policy','parent_ids','purge_tombstone','payload_sha256'}
                            digest=item.get('payload_sha256')
                            if (set(item)!=required or not isinstance(item.get('record_id'),str) or not item['record_id']
                                    or not isinstance(digest,str) or len(digest)!=64
                                    or any(c not in '0123456789abcdef' for c in digest)
                                    or not isinstance(item.get('parent_ids'),list)):
                                raise ValueError('mention tombstone invalid')
                            record_id=item['record_id']
                            parents=item['parent_ids']
                            if (len(record_id)!=64 or any(c not in '0123456789abcdef' for c in record_id)
                                    or any(not isinstance(parent,str) or len(parent)!=64
                                           or any(c not in '0123456789abcdef' for c in parent)
                                           for parent in parents)
                                    or len(set(parents))!=len(parents) or record_id in parents):
                                raise ValueError('mention tombstone lineage invalid')
                            try:
                                captured=parse_capture_time(item['captured_at'])
                                expires=parse_capture_time(item['expires_at'])
                                expected=add_calendar_months(captured)
                            except (RetentionHold,TypeError,ValueError) as exc:
                                raise ValueError('mention tombstone retention timestamp invalid') from exc
                            policy=item['retention_policy']
                            if (captured > datetime.now(timezone.utc) or expires > datetime.now(timezone.utc)
                                    or expires > expected
                                    or (not parents and (policy!=POLICY or expires!=expected))
                                    or (parents and policy not in {POLICY,'inherited P3M calendar-month expiry'})):
                                raise ValueError('mention tombstone retention policy invalid')
                            # Tombstones are consumed as retention evidence, never as live mentions.
                            tombstones_digest=hashlib.sha256(
                                tombstones_digest.encode('ascii')
                                + hashlib.sha256(raw).digest()).hexdigest()
                            offset=stream.tell()
                            continue
                        from engine.lib.collection_retention import RetentionHold, expiry_fields, validate_capture_timestamp
                        try:
                            validate_capture_timestamp(item.get('captured_at'))
                            retention=expiry_fields(item.get('captured_at'))
                        except RetentionHold as exc:
                            raise ValueError('explicit mention capture timestamp unavailable; retention held') from exc
                        if (item.get('expires_at') != retention['expires_at']
                                or item.get('retention_policy') != retention['retention_policy']):
                            raise ValueError('explicit mention expiry does not match its capture-time retention policy')
                        item={**item, 'captured_at':retention['captured_at'],
                              'expires_at':retention['expires_at'],
                              'retention_policy':retention['retention_policy']}
                        identity={k:item.get(k,'') for k in ('source','content_snippet','url')}
                        mid=hashlib.sha256(canonical_json(identity).encode()).hexdigest()
                        payload=canonical_json({**item,'mention_id':mid})
                        count += db.execute('INSERT OR IGNORE INTO mentions(id,payload) VALUES(?,?)',(mid,payload)).rowcount
                        db.execute('INSERT OR IGNORE INTO mention_intake_receipts(mention_id,stream_path,start_offset,end_offset,record_sha256,listen_invocation_id,source,canonical_payload_sha256,generation_id,generation_store_root) VALUES(?,?,?,?,?,?,?,?,?,?)',
                            (mid,cursor_path,line_start,line_end,hashlib.sha256(raw).hexdigest(),
                             item.get('listen_invocation_id') if isinstance(item.get('listen_invocation_id'),str) else None,
                             str(item.get('source',''))[:120],hashlib.sha256(payload.encode()).hexdigest(),
                             generation_id,str(Path(generation_store_root).resolve()) if generation_store_root is not None else None))
                    offset=stream.tell()
            db.execute('INSERT OR REPLACE INTO mention_cursors(path,offset,digest,generation_id,generation_store_root,cursor_version,tombstones_digest) VALUES(?,?,?,?,?,1,?)',
                       (cursor_path,offset,prefix.hexdigest() if generation_store is None else generation_store.cursor_digest(generation_id,offset),generation_id,
                        str(Path(generation_store_root).resolve()) if generation_store_root is not None else None,
                        tombstones_digest))
            return count

    def ingest_generation_store(self, root):
        """Ingest every catalogued generation while retaining legacy streams."""
        root=Path(root)
        if not root.exists():
            return 0
        from engine.lib.raw_generation_store import RawGenerationStore
        store=RawGenerationStore(root,fast_open=True)
        total=0
        for generation in store.generation_ids():
            total+=self.ingest(store.generation_path(generation),generation_id=generation,
                               generation_store_root=root,_generation_store=store)
        return total

    def build_retention_rebase(self, rewrite_mapping, *, source_generation_root,
                               staged_generation_root):
        """Derive cursor and intake-receipt replacements from one staged map.

        This is read-only: the global retention executor must apply the
        returned rows inside its durable transaction. A cursor that does not
        land on an inventoried source boundary, or an intake receipt that does
        not match the exact source record, is held rather than guessed.
        """
        if (not isinstance(rewrite_mapping, dict)
                or rewrite_mapping.get("schema") != "nami.raw_generation_rewrite_stage_result.v1"
                or rewrite_mapping.get("committed") is not False
                or not isinstance(rewrite_mapping.get("cursor_mapping"), dict)
                or not isinstance(rewrite_mapping.get("record_mapping"), dict)):
            raise ValueError("retention rewrite mapping is incomplete")
        source_root = str(Path(source_generation_root).resolve())
        staged_root = str(Path(staged_generation_root).resolve())
        if rewrite_mapping.get("staging_root") != staged_root:
            raise ValueError("retention staged root conflicts with mapping")
        from engine.lib.raw_generation_inventory import (
            _MAX_SCAN_POLICY, inventory_raw_generation_store,
        )
        source_inventory = inventory_raw_generation_store(
            source_root, expected_root=source_root,
            artifact_scan_policy=_MAX_SCAN_POLICY)
        staged_inventory = inventory_raw_generation_store(
            staged_root, expected_root=staged_root,
            artifact_scan_policy=_MAX_SCAN_POLICY)
        if (source_inventory["store_sha256"] != rewrite_mapping.get("source_store_sha256")
                or staged_inventory["store_sha256"] != rewrite_mapping.get("staged_store_sha256")):
            raise ValueError("retention source or staged store digest conflicts with mapping")
        if (set(rewrite_mapping["cursor_mapping"]) != set(source_inventory["generation_ids"])
                or set(rewrite_mapping["record_mapping"]) != {
                    row["_mention_id"] for row in source_inventory["records"]
                }):
            raise ValueError("retention mapping does not cover exact source generations and IDs")
        source_records = {(r["generation"], r["offset"]): r
                          for r in source_inventory["records"]}
        staged_records = {(r["generation"], r["offset"]): r
                          for r in staged_inventory["records"]}
        source_by_id = {r["_mention_id"]: r for r in source_inventory["records"]}
        staged_by_id = {r["_mention_id"]: r for r in staged_inventory["records"]}
        if len(source_by_id) != len(source_inventory["records"]):
            raise ValueError("source inventory contains duplicate mention IDs")
        tombstone_count = 0
        survivor_count = 0
        for identity, mapped in rewrite_mapping["record_mapping"].items():
            source = source_by_id[identity]
            if (not isinstance(mapped, dict)
                    or mapped.get("source_generation") != source["generation"]
                    or mapped.get("source_offset") != source["offset"]
                    or mapped.get("source_length") != source["length"]
                    or mapped.get("source_record_sha256") != source["record_sha256"]
                    or not isinstance(mapped.get("tombstone"), bool)):
                raise ValueError("retention record mapping conflicts with source inventory")
            if mapped["tombstone"]:
                tombstone_count += 1
                if identity in staged_by_id:
                    raise ValueError("expired mention remains in staged raw-generation payload")
            else:
                survivor_count += 1
                staged = staged_by_id.get(identity)
                if (staged is None
                        or mapped.get("staged_generation") != staged["generation"]
                        or mapped.get("staged_offset") != staged["offset"]
                        or mapped.get("staged_end_offset") != staged["offset"] + staged["length"]
                        or mapped.get("staged_record_sha256") != staged["record_sha256"]
                        or mapped.get("staged_canonical_sha256") != staged["canonical_sha256"]
                        or source["_content_fingerprint"] != staged["_content_fingerprint"]):
                    raise ValueError("survivor map conflicts with staged raw-generation inventory")
        mapped_staged_generations = {
            entry.get("new_generation")
            for entries in rewrite_mapping["cursor_mapping"].values()
            for entry in entries if isinstance(entry, dict)
        }
        if (set(staged_by_id) != {identity for identity, row in rewrite_mapping["record_mapping"].items()
                                 if not row.get("tombstone")}
                or mapped_staged_generations != set(staged_inventory["generation_ids"])
                or tombstone_count != rewrite_mapping.get("tombstone_count")
                or survivor_count != rewrite_mapping.get("survivor_count")):
            raise ValueError("retention staged counts or survivor set conflict with inventory")
        raw_cursors = rewrite_mapping["cursor_mapping"]
        cursor_lookup = {}
        source_boundaries = {(generation, 0): "0" * 64 for generation in raw_cursors}
        for record in source_inventory["records"]:
            source_boundaries[(record["generation"], record["offset"] + record["length"])] = record["cursor_digest"]
        staged_generation_ids = set(staged_inventory["generation_ids"])
        for generation, entries in raw_cursors.items():
            for entry in entries:
                if entry.get("new_generation") not in staged_generation_ids:
                    raise ValueError("retention cursor targets unknown staged generation")
        staged_boundaries = {(generation, 0): "0" * 64 for generation in staged_generation_ids}
        for record in staged_inventory["records"]:
            staged_boundaries[(record["generation"], record["offset"] + record["length"])] = record["cursor_digest"]
        for generation, entries in raw_cursors.items():
            if not isinstance(generation, str) or not isinstance(entries, list) or not entries:
                raise ValueError("retention cursor mapping is malformed")
            offsets = set()
            for entry in entries:
                if (not isinstance(entry, dict)
                        or isinstance(entry.get("old_offset"), bool)
                        or not isinstance(entry.get("old_offset"), int)
                        or entry["old_offset"] < 0
                        or entry["old_offset"] in offsets
                        or not isinstance(entry.get("source_digest"), str)
                        or not isinstance(entry.get("new_generation"), str)
                        or isinstance(entry.get("new_offset"), bool)
                        or not isinstance(entry.get("new_offset"), int)
                        or not isinstance(entry.get("new_digest"), str)):
                    raise ValueError("retention cursor mapping entry is invalid")
                if (source_boundaries.get((generation, entry["old_offset"])) != entry["source_digest"]
                        or staged_boundaries.get((entry["new_generation"], entry["new_offset"])) != entry["new_digest"]):
                    raise ValueError("retention cursor map is not backed by both verified stores")
                offsets.add(entry["old_offset"])
                cursor_lookup[(generation, entry["old_offset"])] = entry
        cursor_rows, intake_rows = [], []
        expired_ids = {identity for identity, mapped in rewrite_mapping["record_mapping"].items()
                       if mapped.get("tombstone") is True}
        with self.connect() as db:
            cursors = db.execute(
                "SELECT path,offset,digest,generation_id,generation_store_root "
                "FROM mention_cursors WHERE generation_store_root=? ORDER BY path",
                (source_root,)).fetchall()
            receipts = db.execute(
                "SELECT r.mention_id,r.stream_path,r.start_offset,r.end_offset,r.record_sha256,"
                "r.canonical_payload_sha256,r.generation_id,r.generation_store_root,m.payload AS mention_payload "
                "FROM mention_intake_receipts r LEFT JOIN mentions m ON m.id=r.mention_id "
                "WHERE r.generation_store_root=? ORDER BY r.mention_id",
                (source_root,)).fetchall()
            mention_rows = db.execute("SELECT id FROM mentions ORDER BY id").fetchall()
        for row in cursors:
            generation = row["generation_id"]
            if row["path"] != f"generation://{Path(source_root).as_posix()}/{generation}":
                raise ValueError("mention cursor stream identity conflicts with its root")
            mapped = cursor_lookup.get((generation, row["offset"]))
            if (mapped is None or row["digest"] != mapped["source_digest"]
                    or row["generation_store_root"] != source_root):
                raise ValueError("mention cursor has no exact verified source boundary")
            cursor_rows.append({"old_path": row["path"], "old_generation": generation,
                "old_offset": row["offset"], "old_digest": row["digest"],
                "path": f"generation://{Path(staged_root).as_posix()}/{mapped['new_generation']}",
                "offset": mapped["new_offset"], "digest": mapped["new_digest"],
                "generation_id": mapped["new_generation"],
                "generation_store_root": staged_root})
        for row in receipts:
            mapped = rewrite_mapping["record_mapping"].get(row["mention_id"])
            source_record = (source_records.get((row["generation_id"], row["start_offset"]))
                             if isinstance(row["generation_id"], str) else None)
            expected_stream = (f"generation://{Path(source_root).as_posix()}/{row['generation_id']}"
                               if isinstance(row["generation_id"], str) else None)
            if (mapped is None or row["generation_store_root"] != source_root
                    or row["stream_path"] != expected_stream
                    or row["generation_id"] != mapped["source_generation"]
                    or row["start_offset"] != mapped["source_offset"]
                    or row["end_offset"] != mapped["source_offset"] + mapped["source_length"]
                    or row["record_sha256"] != mapped["source_record_sha256"]
                    or source_record is None
                    or source_record["_mention_id"] != row["mention_id"]
                    or source_record["length"] != mapped["source_length"]
                    or source_record["record_sha256"] != mapped["source_record_sha256"]):
                raise ValueError("mention intake receipt conflicts with staged record lineage")
            payload_text = row["mention_payload"]
            if not isinstance(payload_text, str) or not isinstance(row["canonical_payload_sha256"], str):
                raise ValueError("mention intake receipt has no stored payload binding")
            if hashlib.sha256(payload_text.encode("utf-8")).hexdigest() != row["canonical_payload_sha256"]:
                raise ValueError("mention intake payload digest conflicts with stored payload")
            try:
                payload = json.loads(payload_text)
            except (ValueError, TypeError) as exc:
                raise ValueError("stored mention payload is invalid JSON") from exc
            if (not isinstance(payload, dict) or canonical_json(payload) != payload_text
                    or payload.get("mention_id") != row["mention_id"]):
                raise ValueError("stored mention payload identity or canonical form conflicts")
            from engine.lib.mention_identity import _resolve_nami_mention_identity
            if _resolve_nami_mention_identity(payload) != row["mention_id"]:
                raise ValueError("stored mention payload identity does not match receipt")
            fingerprint = hashlib.sha256(canonical_json(
                {key: value for key, value in payload.items() if key != "mention_id"}
            ).encode("utf-8")).hexdigest()
            if fingerprint != source_record["_content_fingerprint"]:
                raise ValueError("stored mention payload does not match authoritative source row")
            if mapped["tombstone"]:
                if any(record["_mention_id"] == row["mention_id"]
                       for record in staged_inventory["records"]):
                    raise ValueError("expired mention remains in staged raw-generation payload")
                continue
            staged_record = staged_records.get((mapped["staged_generation"], mapped["staged_offset"]))
            if (staged_record is None or staged_record["_mention_id"] != row["mention_id"]
                    or staged_record["length"] != mapped["staged_end_offset"] - mapped["staged_offset"]
                    or staged_record["record_sha256"] != mapped["staged_record_sha256"]
                    or staged_record["canonical_sha256"] != mapped["staged_canonical_sha256"]
                    or staged_record["_content_fingerprint"] != source_record["_content_fingerprint"]):
                raise ValueError("surviving intake row does not match staged record")
            intake_rows.append({"mention_id": row["mention_id"],
                "stream_path": f"generation://{Path(staged_root).as_posix()}/{mapped['staged_generation']}",
                "start_offset": mapped["staged_offset"], "end_offset": mapped["staged_end_offset"],
                "record_sha256": mapped["staged_record_sha256"],
                "canonical_payload_sha256": row["canonical_payload_sha256"],
                "generation_id": mapped["staged_generation"],
                "generation_store_root": staged_root})
        receipt_ids = {row["mention_id"] for row in receipts}
        for row in mention_rows:
            if row["id"] in rewrite_mapping["record_mapping"] and row["id"] not in receipt_ids:
                raise ValueError("stored mention row lacks authoritative intake receipt")
        return {"schema": "nami.mention_retention_rebase.v1",
                "source_generation_root": source_root,
                "staged_generation_root": staged_root,
                "cursors": cursor_rows, "intake_receipts": intake_rows,
                "expired_mention_ids": sorted(expired_ids),
                "applied": False}

    def pending_count(self, eligible_sources=None):
        max_attempts = max(1, int(getattr(config, 'HYPOTHESIZE_MAX_ATTEMPTS', 3)))
        source_clause, source_args = self._source_filter(eligible_sources)
        with self.connect() as db:
            return db.execute(
                "SELECT count(*) FROM mentions WHERE invalid_attempts<? AND available_at<=? AND (status='pending' OR (status='leased' AND lease_until<=?))" + source_clause,
                (max_attempts, time.time(), time.time(), *source_args),
            ).fetchone()[0]

    @staticmethod
    def _source_filter(eligible_sources):
        if eligible_sources is None: return "", ()
        sources = tuple(sorted(set(eligible_sources)))
        if not sources: return " AND 0", ()
        return " AND CASE WHEN instr(json_extract(payload,'$.source'),':')>0 THEN substr(json_extract(payload,'$.source'),1,instr(json_extract(payload,'$.source'),':')-1) ELSE json_extract(payload,'$.source') END IN (" + ",".join("?" for _ in sources) + ")", sources

    def quarantined_count(self):
        with self.connect() as db:
            return db.execute("SELECT count(*) FROM mentions WHERE status='quarantined'").fetchone()[0]

    def _record_failure(self, db, token, row, error_code, error_details, now):
        details = canonical_json(error_details or {})[:2000]
        db.execute(
            'INSERT OR IGNORE INTO mention_failures(token,mention_id,attempt_number,error_code,error_details,created_at) VALUES(?,?,?,?,?,?)',
            (token, row['id'], row['attempts'], error_code, details, now),
        )

    def claim(self, limit=10, lease_seconds=300, max_attempts=None, eligible_sources=None):
        from engine.lib.runtime_safety import check_claim_allowed
        check_claim_allowed(self.path.parent)
        limit = min(10, max(1, int(limit)))
        max_attempts = max(1, int(max_attempts or getattr(config, 'HYPOTHESIZE_MAX_ATTEMPTS', 3)))
        now = time.time()
        source_clause, source_args = self._source_filter(eligible_sources)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            expired = db.execute(
                "SELECT id,token,attempts FROM mentions WHERE status='leased' AND lease_until<=?" + source_clause + " ORDER BY rowid",
                (now, *source_args),
            ).fetchall()
            for row in expired:
                self._record_failure(db, row['token'], row, 'lease_expired', {'lease_expired': True}, now)
                terminal = False  # loss of a worker is an infrastructure event
                db.execute(
                    "UPDATE mentions SET status=?,token=NULL,lease_until=NULL,last_error_code='lease_expired',available_at=?,quarantined_at=CASE WHEN ? THEN ? ELSE quarantined_at END WHERE id=? AND token=?",
                    ('quarantined' if terminal else 'pending', now+60, int(terminal), now, row['id'], row['token']),
                )
                db.execute(
                    'UPDATE mention_batches SET status=? WHERE token=?',
                    ('quarantined' if terminal else 'lease_expired', row['token']),
                )
            rows = db.execute(
                "SELECT * FROM (SELECT *,rowid AS insertion_id,ROW_NUMBER() OVER (PARTITION BY json_extract(payload,'$.source') ORDER BY last_claim_at,rowid) AS source_rank FROM mentions WHERE status='pending' AND invalid_attempts<? AND available_at<=?" + source_clause + ") ORDER BY source_rank,last_claim_at,insertion_id LIMIT ?",
                (max_attempts, now, *source_args, limit),
            ).fetchall()
            if not rows:
                return None
            token = str(uuid.uuid4())
            db.execute("INSERT INTO mention_batches(token,status,actions,claimed_inputs,quarantined_inputs) VALUES(?,'leased',NULL,?,?)", (token,canonical_json({row['id']:hashlib.sha256(row['payload'].encode()).hexdigest() for row in rows}),canonical_json({})))
            db.executemany(
                "UPDATE mentions SET status='leased',token=?,lease_until=?,attempts=attempts+1,last_claim_at=? WHERE id=?",
                [(token, now + lease_seconds, now, row['id']) for row in rows],
            )
            return {'token': token, 'mentions': [json.loads(row['payload']) for row in rows]}


    @staticmethod
    def _verified_input_partition(db, batch, rows):
        """Bind both acknowledged and quarantined rows to the exact claimed payloads."""
        try:
            quarantine = json.loads(batch['quarantined_inputs'] or '{}')
            claimed = json.loads(batch['claimed_inputs']) if batch['claimed_inputs'] else None
            if not isinstance(quarantine,dict): return None
            if claimed is None:
                # Existing batches predate partition receipts and cannot add quarantine.
                return set() if not quarantine and all(r['status'] in {'leased','done'} for r in rows) else None
            if not isinstance(claimed,dict) or set(claimed)!={r['id'] for r in rows} or not set(quarantine).issubset(claimed): return None
            for row in rows:
                digest=hashlib.sha256(row['payload'].encode()).hexdigest()
                if claimed.get(row['id'])!=digest: return None
                receipt=quarantine.get(row['id'])
                if receipt is None:
                    if row['status'] not in {'leased','done'}: return None
                    continue
                if (not isinstance(receipt,dict) or row['status']!='quarantined' or row['action'] is not None
                        or row['lease_until'] is not None or receipt.get('payload_sha256')!=digest
                        or receipt.get('attempt_number')!=row['attempts']
                        or receipt.get('error_code')!=row['last_error_code']
                        or receipt.get('quarantined_at')!=row['quarantined_at']): return None
                failure=db.execute('SELECT * FROM mention_failures WHERE token=? AND mention_id=?', (batch['token'],row['id'])).fetchone()
                if (not failure or failure['attempt_number']!=receipt['attempt_number']
                        or failure['error_code']!=receipt['error_code'] or failure['created_at']!=receipt['quarantined_at']
                        or json.loads(failure['error_details'])!={'failure_kind':'input'}): return None
            return set(quarantine)
        except (ValueError,TypeError,KeyError): return None

    def quarantine_inputs(self, token, ids, error_code="invalid_numeric_signal"):
        """Retain invalid payloads with receipts distinct from healthy acknowledgements."""
        ids = set(ids)
        if not ids: return
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            batch=db.execute('SELECT * FROM mention_batches WHERE token=?',(token,)).fetchone()
            rows=db.execute("SELECT * FROM mentions WHERE token=?",(token,)).fetchall()
            if not batch or batch['status']!='leased' or self._verified_input_partition(db,batch,rows) is None:
                raise ValueError('invalid claimed input partition')
            leased={r['id']:r for r in rows if r['status']=='leased'}
            if not ids.issubset(leased) or any(leased[mid]['lease_until']<=now for mid in ids):
                raise ValueError('input quarantine lease unavailable')
            receipts=json.loads(batch['quarantined_inputs'] or '{}')
            for mid in sorted(ids):
                row=leased[mid]
                self._record_failure(db,token,row,error_code,{'failure_kind':'input'},now)
                receipts[mid]={'payload_sha256':hashlib.sha256(row['payload'].encode()).hexdigest(),
                    'attempt_number':row['attempts'],'error_code':error_code,'quarantined_at':now}
                db.execute("UPDATE mentions SET status='quarantined',lease_until=NULL,quarantined_at=?,last_error_code=? WHERE id=? AND token=?",(now,error_code,mid,token))
            db.execute('UPDATE mention_batches SET quarantined_inputs=? WHERE token=?',(canonical_json(receipts),token))
            if not set(leased)-ids:
                db.execute("UPDATE mention_batches SET status='quarantined' WHERE token=?",(token,))

    def complete(self, token, actions, hypotheses, *, invocation_id=None, model_calls=None, model_call_required=False):
        """Ack exact IDs and enqueue their proposals in the SAME transaction."""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            batch=db.execute('SELECT * FROM mention_batches WHERE token=?',(token,)).fetchone()
            all_rows=db.execute("SELECT id,payload FROM mentions WHERE token=?",(token,)).fetchall()
            parents={row['id']:json.loads(row['payload']) for row in all_rows}
            from engine.lib.collection_retention import (
                RetentionHold, derived_expiry_fields, expiry_fields,
                validate_capture_timestamp,
            )
            for hyp in hypotheses:
                parent_ids=hyp.get('mention_ids') if isinstance(hyp,dict) else None
                if (not isinstance(parent_ids,list) or not parent_ids
                        or any(not isinstance(mid,str) or mid not in parents for mid in parent_ids)
                        or len(set(parent_ids)) != len(parent_ids)):
                    raise ValueError('hypothesis lacks exact retained mention lineage')
                parent_expiries=[]
                parent_captures=[]
                for mid in parent_ids:
                    parent=parents[mid]
                    if not isinstance(parent.get('captured_at'),str) or not isinstance(parent.get('expires_at'),str):
                        raise ValueError('mention lacks explicit committed retention metadata')
                    try:
                        parent_capture=validate_capture_timestamp(parent['captured_at'])
                        expected_parent=expiry_fields(parent['captured_at'])
                    except RetentionHold as exc:
                        raise ValueError('mention capture metadata is invalid') from exc
                    if parent['expires_at'] != expected_parent['expires_at']:
                        raise ValueError('mention expiry differs from capture-time P3M')
                    parent_captures.append(parent_capture)
                    parent_expiries.append(parent['expires_at'])
                identity=hyp.get('id')
                if not isinstance(identity,str) or not identity:
                    raise ValueError('hypothesis lacks system-owned identity')
                try:
                    captured=validate_capture_timestamp(hyp.get('created_at')).isoformat()
                    retention=derived_expiry_fields(captured,parent_expiries)
                except RetentionHold as exc:
                    raise ValueError('hypothesis capture-time retention is unavailable') from exc
                if any(parent_capture > validate_capture_timestamp(captured)
                       for parent_capture in parent_captures):
                    raise ValueError('hypothesis capture time predates a retained parent')
                prior_expiry=hyp.get('expires_at')
                prior_capture=hyp.get('captured_at')
                prior_parents=hyp.get('parent_ids')
                if prior_expiry is not None and prior_expiry != retention['expires_at']:
                    raise ValueError('hypothesis retention expiry differs from its earliest parent')
                if prior_capture is not None and prior_capture != captured:
                    raise ValueError('hypothesis capture timestamp differs from created_at')
                expected_parents=sorted(parent_ids)
                if prior_parents is not None and prior_parents != expected_parents:
                    raise ValueError('hypothesis parent lineage changed during replay')
                if hyp.get('record_id') not in (None,identity):
                    raise ValueError('hypothesis record identity is system-owned')
                hyp.update({"record_id":identity, "captured_at":captured,
                            "parent_ids":expected_parents, **retention})
                expected_lineage=[{'mention_id':mid,
                    'captured_at':parents[mid]['captured_at'],
                    'expires_at':parents[mid]['expires_at'],
                    'payload_sha256':hashlib.sha256(canonical_json(parents[mid]).encode()).hexdigest()}
                    for mid in sorted(parent_ids)]
                if hyp.get('retention_lineage') not in (None,expected_lineage):
                    raise ValueError('hypothesis retention lineage changed during replay')
                hyp['retention_lineage']=expected_lineage
            serialized=canonical_json({"actions":actions,"hypotheses":hypotheses})
            if batch and batch['claimed_inputs']:
                serialized=canonical_json({'actions':actions,'hypotheses':hypotheses,'input_partition':{'claimed':json.loads(batch['claimed_inputs']),'quarantined':json.loads(batch['quarantined_inputs'] or '{}')}})
            if batch and batch['status']=='done':
                if batch['actions']!=serialized:raise ValueError('different replayed batch result')
                if invocation_id and batch['stage_invocation_id'] != invocation_id:
                    raise ValueError('different replayed stage invocation')
                if canonical_json(json.loads(batch['model_calls'] or '[]'))!=canonical_json(model_calls or []) or bool(batch['model_call_required'])!=bool(model_call_required):
                    raise ValueError('different replayed model call set')
                return True
            all_rows=db.execute("SELECT * FROM mentions WHERE token=?",(token,)).fetchall()
            partition=self._verified_input_partition(db,batch,all_rows) if batch else None
            if partition is None: raise ValueError('invalid claimed input partition')
            rows=[row for row in all_rows if row['status']=='leased']
            if not rows or any(r['lease_until']<=time.time() for r in rows):return False
            ids=[a.get('mention_id') for a in actions]
            if len(ids)!=len(set(ids)) or set(ids)!={r['id'] for r in rows}:
                raise ValueError('actions must acknowledge exactly the claimed mention IDs')
            for hyp in hypotheses:
                if not hyp.get('mention_ids') or not set(hyp['mention_ids']).issubset(set(ids)):
                    raise ValueError('hypothesis lacks claimed mention lineage')
            self.work.enqueue_hypotheses(hypotheses,connection=db)
            db.executemany("UPDATE mentions SET status='done',action=?,lease_until=NULL WHERE id=? AND token=?",[(canonical_json(a),a['mention_id'],token) for a in actions])
            db.execute("UPDATE mention_batches SET status='done',actions=?,stage_invocation_id=?,committed_at=?,model_calls=?,model_call_required=? WHERE token=?",
                       (serialized,invocation_id,time.time(),canonical_json(model_calls or []),int(bool(model_call_required)),token))
            return True

    def stage_lineage_for_invocation(self, invocation_id, *, not_before=0):
        """Return only lineage committed atomically with this stage invocation."""
        if not isinstance(invocation_id,str) or not invocation_id:return None
        with self.connect() as db:
            rows=db.execute("SELECT token,committed_at,model_calls,model_call_required FROM mention_batches WHERE status='done' AND stage_invocation_id=? AND committed_at>=? ORDER BY committed_at DESC",
                            (invocation_id,not_before)).fetchall()
        if len(rows)!=1:return None
        with self.connect() as db:
            append_rows=db.execute("SELECT DISTINCT listen_invocation_id FROM mention_intake_receipts WHERE mention_id IN (SELECT id FROM mentions WHERE token=?) AND listen_invocation_id IS NOT NULL ORDER BY 1",
                                   (rows[0]['token'],)).fetchall()
            batch=db.execute('SELECT claimed_inputs,quarantined_inputs FROM mention_batches WHERE token=?',(rows[0]['token'],)).fetchone()
            claimed=json.loads(batch['claimed_inputs'] or '{}')
            intake_rows=db.execute('''SELECT m.id,r.stream_path,r.start_offset,r.end_offset,r.record_sha256,r.canonical_payload_sha256,
                r.listen_invocation_id,r.source,r.generation_id,r.generation_store_root FROM mentions m LEFT JOIN mention_intake_receipts r ON r.mention_id=m.id
                WHERE m.token=? ORDER BY m.id''',(rows[0]['token'],)).fetchall()
            intake=[{'mention_id':r['id'],'stream_path':r['stream_path'],'start_offset':r['start_offset'],
                     'end_offset':r['end_offset'],'record_sha256':r['record_sha256'],
                     'canonical_payload_sha256':r['canonical_payload_sha256'],
                     'listen_invocation_id':r['listen_invocation_id'],'source':r['source'],
                     'generation_id':r['generation_id'],'generation_store_root':r['generation_store_root']}
                    for r in intake_rows if not claimed or r['id'] in claimed]
        return {'invocation_id':invocation_id,'batch_token':rows[0]['token'],
                'model_calls':json.loads(rows[0]['model_calls'] or '[]'),
                'model_call_required':bool(rows[0]['model_call_required']),
                'committed_at':rows[0]['committed_at'],
                'listen_invocation_ids':[r[0] for r in append_rows],
                'claimed_input_ids':sorted(claimed), 'claimed_input_hashes':claimed, 'intake_records':intake}

    def verify_stage_commit(self, lineage, *, not_before=0):
        """Validate this exact invocation's durable acknowledgements and proposals."""
        if not isinstance(lineage,dict) or not all(isinstance(lineage.get(k),str) and lineage[k]
                for k in ('invocation_id','batch_token')):
            return False
        with self.connect() as db:
            batch = db.execute('SELECT * FROM mention_batches WHERE token=?', (lineage['batch_token'],)).fetchone()
            if (not batch or batch['status']!='done' or batch['stage_invocation_id']!=lineage['invocation_id']
                    or batch['committed_at'] is None or batch['committed_at'] < not_before):
                return False
            try:
                payload = json.loads(batch['actions'])
                actions = payload['actions']
                if batch['claimed_inputs'] and payload.get('input_partition')!={'claimed':json.loads(batch['claimed_inputs']),'quarantined':json.loads(batch['quarantined_inputs'] or '{}')}: return False
                expected = {a['mention_id']:canonical_json(a) for a in actions}
                rows = db.execute('SELECT * FROM mentions WHERE token=?', (lineage['batch_token'],)).fetchall()
                partition=self._verified_input_partition(db,batch,rows)
                if partition is None: return False
                rows=[row for row in rows if row['id'] not in partition]
                if not expected or len(expected)!=len(actions) or len(rows)!=len(actions):
                    return False
                if any(row['status']!='done' or expected.get(row['id'])!=row['action'] for row in rows):
                    return False
                for hypothesis in payload['hypotheses']:
                    row = db.execute('SELECT payload FROM work WHERE id=?', (hypothesis['id'],)).fetchone()
                    if not row or not set(hypothesis['mention_ids']).issubset(expected):
                        return False
                    committed = json.loads(row['payload'])
                    if any(committed.get(key)!=value for key,value in hypothesis.items() if key!='status'):
                        return False
                return True
            except (ValueError,TypeError,KeyError):
                return False

    def release(self, token, error_code='unspecified_failure', error_details=None, max_attempts=None,
                failure_kind='input', retry_after_s=0):
        """Only invalid input consumes quarantine allowance; infrastructure preserves work."""
        max_attempts = max(1, int(max_attempts or getattr(config, 'HYPOTHESIZE_MAX_ATTEMPTS', 3)))
        error_code = str(error_code or 'unspecified_failure')[:80]
        now = time.time()
        released = pending = quarantined = 0
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute(
                "SELECT id,token,attempts,invalid_attempts FROM mentions WHERE token=? AND status='leased' AND lease_until>?",
                (token, now),
            ).fetchall()
            for row in rows:
                invalid = int(row['invalid_attempts']) + int(failure_kind == 'input')
                terminal = invalid >= max_attempts
                self._record_failure(db, token, row, error_code, error_details, now)
                db.execute(
                    'UPDATE mentions SET status=?,token=NULL,lease_until=NULL,last_error_code=?,invalid_attempts=?,available_at=?,quarantined_at=CASE WHEN ? THEN ? ELSE quarantined_at END WHERE id=? AND token=?',
                    ('quarantined' if terminal else 'pending', error_code, invalid, now+max(0,float(retry_after_s)), int(terminal), now, row['id'], token),
                )
                released += 1
                quarantined += int(terminal)
                pending += int(not terminal)
            if rows:
                batch_status = 'quarantined' if quarantined else 'released'
                db.execute(
                    'UPDATE mention_batches SET status=?,actions=? WHERE token=?',
                    (batch_status, canonical_json({'error_code': error_code, 'error_details': error_details or {}}), token),
                )
        return {'released': released, 'pending': pending, 'quarantined': quarantined}


