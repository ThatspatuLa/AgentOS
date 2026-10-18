# Under-90-day retention: candidate inventory pass

Date: 2026-10-18  
Candidate: `source-review-snapshot-eefbd081-20261018`  
Scope: read-only code and test inspection; no installed runtime, scheduler, external provider, or production data was accessed.

## Result

The candidate has six named store families and multiple partial readers, receipt writers, staging routines, and purge primitives. They do not yet form a complete root/writer inventory or a recoverable all-store purge. `verify_retention_control()` must remain false. This report records the audited code boundary; it is not a purge receipt, complete inventory, or release qualification.

## Root registry and unclassified copies

`resolve_candidate_root_registry()` registers the resolved Nami `state`, `logs`, `reports`, and `backups` trees; effective coordinator state; canary artifacts/state/Obsidian output; Hermes cron data; and two optional exact Nami-home evidence files. Worker request/result/lifecycle files are placed under `NAMI_HOME/state/worker-temp`, so they fall within the state tree. The inventory scanner rejects missing required roots, duplicate/overlapping roots, links/reparse points, unknown artifacts, incomplete scans, and conflicting duplicate records.

The registry is path configuration, not proof that every writer uses those roots or participates in one lease. Additional mutable operational outputs include model usage/workflow databases, documentation evidence, source clearance evidence, model dispatch receipts, worker failures, stage progress, dataset sidecars, raw-generation catalogs/indexes/checkpoints, retention staging, maintenance receipts, and compressed archives/SQLite backups. These are inside known trees in the usual layout, but the current inventory schema does not classify every such format. Unknown content-bearing formats remain held. The candidate resolver also accepts environment path overrides; this pass did not prove they match an installed release or an active runtime.

The Federal Reserve route is included only as route context: the registered adapter path is the public monetary-policy RSS feed `www.federalreserve.gov/feeds/press_monetary.xml`, and its pinned documentation references are the feeds index and disclaimer. Any older P3M source clearance for this route is not evidence of deletion within 89 days and cannot satisfy the newly authorized retention condition.

## Store and writer coverage

| Store family | Candidate writer and retained copies | Existing retention capability | Remaining hold |
|---|---|---|---|
| JSON/JSONL | `nami_io` append/write helpers; `dataset_store.persist_raw_response`; legacy mention/stage writers; qualification/report writers | Generic inventory plus a hash-guarded, single-file JSON/JSONL purge primitive | No global duplicate/copy writer map; no all-root transaction; operational logs/reports can contain content and have heterogeneous schemas |
| Workflow SQLite | `WorkStore`, `MentionStore`, dispatcher/model-usage stores; maintenance purge adapter | `WorkStore._db()` now holds an exact-root lease across schema setup and every connection, including WAL sidecar writes; explicit schema inventory and per-database rewrite/receipt paths exist for supported schemas | `MentionStore` and other SQLite writers do not yet share the lease; SQLite rows, WAL/SHM/journal sidecars and related JSONL/cursor transitions are not globally atomic; unsupported writers/schemas hold |
| Raw responses | `dataset_store.persist_raw_response`; `scripts.nami_data_capture.capture_response` writes separate public capture envelopes | Lease-bound raw-response inventory and assessment paths exist for the dataset raw directory | Response-envelope writer lacks persisted clearance/descendant proof; no authorized global duplicate and descendant purge commit |
| Raw generations | `RawGenerationStore.append` and intake/cursor writers | Hash-chained generation/catalog assessment and staged rewrite primitives exist | Catalog, event-index, cursor, intake receipts, and descendants lack one committed purge transition and global recovery |
| Frozen datasets | `dataset_store.persist_frozen_events`; manifests and evaluation descendants | Dataset raw/frozen records can be recognized and retention lineage inventoried | Descendant and duplicate-reference rewrite is incomplete; manifests, rows and raw parents cannot be committed as one recoverable unit |
| Archives/backups | archive chunk/index and SQLite backup writers; runtime backup helper | Read-only archive/backup assessment and isolated rewrite/SQLite staging exist | Shared gzip chunks, archive cursors, SQLite source generation/row lineage, and all backup copies are not globally committed or independently reinventoried |

## Lease and transaction findings

- Raw response, dataset, and `WorkStore` writers use `.nami-raw-dataset-write-gate.lock`; `RawDatasetStoreLease` and `WorkStore._db()` are Windows-only and check exact-root identity. The gate covers only call sites routed through these adapters. `MentionStore`, raw generations, archive/backups, logs/reports, and model-output writers remain outside it.
- Cross-process testing exposed that `file_lock_on_secure_root()` inspected the gate byte before taking its Windows byte lock. A waiting process could fail with `PermissionError`; it now acquires the lock before inspecting or initializing the one-byte gate. This ordering is covered by the WorkStore cross-process claim test.
- Archive rewrite and workflow purge adapters still accept caller-supplied lease seams; the archive rewrite path has not yet been hardened to require the concrete authenticated lease type.
- `global_retention_transaction` writes a create-only **held** plan, journal, and receipt under a caller-supplied lease-like object. It performs no payload mutation and explicitly cannot recover partial control writes. A method named `assert_bound_root` alone is not proof of exclusive quiescence.
- Maintenance is intentionally held and does not call the archive mutating sweep.
- No module produces an independent post-purge inventory proving zero expired payload copies and zero unclassified artifacts across the complete registered roots.

## Tests inspected

Relevant suites already present include `test_retention_inventory.py`, `test_collection_retention_purge.py`, `test_retention_transaction_contract.py`, `test_global_retention_transaction.py`, `test_raw_response_retention.py`, `test_raw_generation_retention.py`, `test_raw_generation_store.py`, `test_dataset_ingestion.py`, `test_retention_workflow_store.py`, `test_workflow_retention_propagation.py`, `test_archive_backup_retention.py`, `test_maintenance_retention.py`, and `test_automatic_source_clearance.py`. These encode important synthetic holds and per-store behaviors, but the suite set does not itself establish all-writer/root coverage or a crash-recoverable cross-store commit.

## Source hashes observed during this inventory pass

These hashes identify the inspected candidate snapshot. Any subsequent edits change the corresponding hash and require this evidence to be refreshed before qualification.

| Source | SHA-256 |
|---|---|
| `engine/lib/collection_retention_inventory.py` | `0f63323e8bc23a1633d078a4db2c775c1b7f7116d0535bbe68a4eea60d4d81ba` |
| `engine/lib/collection_retention_purge.py` | `4e312e43f74ea696da1b86c5427f08547cc5d2fcdd7f1ad8fcdb0fca3c554f4e` |
| `engine/lib/raw_response_retention.py` | `2a76411891fa53701ca7a11e7dc9124e0b84dbea03ca326476d5431077f07303` |
| `engine/lib/raw_generation_retention.py` | `a3c489b76d76538d3bd1e098532f1675f8b4282c9e2d542f36d0f1ba69aec6df` |
| `engine/lib/raw_generation_store.py` | `1af669e41666b548b121c4f7fcb00b29446deff2a2dc63a3401545ae5a74fd21` |
| `engine/lib/dataset_store.py` | `7f2ad7a54ad0705a99b4253d70352550077aabd6153e0030c0b43d396df73268` |
| `engine/lib/retention_workflow_store.py` | `d81e02a6ce0e2e3652a963c4c02f724ca9864a4959167c9fed81e47fae66e462` |
| `engine/lib/archive_backup_retention.py` | `1d113ae5ea09afba455c2d7477b97cb0a63e0ef91cea532c55587162b2ad1b37` |
| `engine/lib/global_retention_transaction.py` | `7ed997331212274a9d99424b32efb397b71144035877ffc52ca1122f9e246461` |
| `engine/lib/retention_transaction_contract.py` | `1fe82ca88132e4cc628d545f27a90f0551c4cf09d8b3f6557dc32c56836dc8b0` |
| `scripts/nami_maintenance.py` | `5e38a253bbda0c841bf9e967e513cceae16c5deaef9103b6c267a392ed1841c5` |
| `engine/lib/runtime_safety.py` | `96587cc20f20c8d2476310afd8329360de8bf5c4e06798b39cbede332f1f437c` |
| `engine/lib/worker_runtime.py` | `e3254d52c9f3ac8e1e028fb0f3c38b5ec786029f9c232e50c547365ce15588ee` |
| `engine/lib/automatic_source_clearance.py` | `86d7a29665673ff75091a5564bbb8d2e65fe8e343ad37b477eccc321dddabd90` |
| `engine/lib/exact_root_lease.py` | `bc655da38187ffe9f5b47b8522358a9d36d4a5bbab011fb28ccb6828334ec718` |
| `tests/test_exact_root_quiescence_lease.py` | `2f7c0ef53abb234974e01052d33f86c55f0bf6bc1e78e2a3160b59bdbfd869a7` |
| `tests/test_raw_response_retention.py` | `c588a1a55c269d620255d91ca5ae05ef448fd88a90cef0f1900bd3469b8f6209` |
| `engine/lib/startup_readiness.py` | `1e963a9c0bd55f7ddb135ee1c9da44e971ef5328b256f7fbd1d445170836f8c1` |

Current hashes for the workflow-lease slice (2026-10-18):

| Source | SHA-256 |
|---|---|
| `engine/lib/work_store.py` | `e0f075a05f01802c2397b2973dd6d143a259f3e259bd94f0ec0022ae1fcca03e` |
| `engine/lib/nami_io.py` | `a9d1cfbd4187dddcc2ae0093352fbfa91822d23eede3677c01748cd63de5218b` |
| `tests/test_work_store_exact_root_lease.py` | `e1e34fc59211f1c166c0680f7f69abc26779824646648cc147822316817af643` |

## Bounded implementation completed after this audit

`ExactRootQuiescenceLease` now supplies the authenticated Windows retained-handle lock for the raw response/dataset-store writer seam. Raw JSONL and frozen dataset writers acquire it before creating or touching their subdirectories and reject symlink/reparse destinations through the retained root. Raw-response assessment now consumes the lease's retained root handle instead of opening a conflicting second handle. Focused verification passed: 39 tests across `test_exact_root_quiescence_lease.py`, `test_raw_response_retention.py`, and `test_dataset_ingestion.py`.

This lease fences only cooperating writers routed through this seam. It is Windows-only, does not cover all Nami writers or the other four store families, and does not provide purge/recovery/post-inventory. Automatic source clearance remains held.

## Next bounded adapter: workflow SQLite

`WorkStore._db()` now enters `ExactRootQuiescenceLease(self.path.parent)`, verifies the exact database parent and rejects linked database leaves before connecting. Constructor/schema writes and normal reads/writes use this same context; `_transaction()` delegates once to `_db()`, and the audited call sites do not nest `_db()` on an open connection. No lease reentrancy redesign was needed. The adapter requires the database parent directory to exist before construction; failure to establish the lease aborts the operation. `MentionStore.connect()` remains a separate unleased writer even when pointed at the same database, so this is only a partial fence.

The focused tests cover missing lease failure, wrong-root rejection, same-root concurrent writer serialization, and the existing cross-process single-claim case. Scoped verification passed: 33 tests and 21 subtests across `test_work_store_exact_root_lease.py`, `test_durable_pipeline.py::DurablePipelineTests::test_cross_process_claim_has_one_owner`, and `test_retention_workflow_store.py`. The earlier all-suite attempt also selected two recovery tests from the adjacent `p3m-retention-transaction-20261017` candidate; their missing-claim failures are not treated as evidence for this candidate slice.

This does not qualify workflow retention or the global controller. A held lease file is operational control state, and root inventory may still classify it as unsupported evidence until explicitly registered. Automatic source clearance remains held.

## Remaining implementation boundary

The current gate excludes `MentionStore`, raw generations, archives/backups, logs/reports, and model-output writers. Before any purge work, the controller needs complete writer/root coverage, a release-bound registry, safe treatment of the gate control artifact, and recoverable global receipt/recovery plus independent post-inventory. `verify_retention_control()` remains false; no purge path or clearance activation was added.
