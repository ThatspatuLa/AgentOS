# KCOS Stage 2 OBV release snapshot

This release snapshot records the Stage 2 implementation and published EA source for the authenticated cycle in `release_manifest.json`. The operational project at the recorded source path is not a Git repository; this package was copied without modifying its live files.

The runtime files are copied from the Stage 2 transaction’s frozen code manifest and rechecked against those SHA-256 values. The EA source is bound to the published Stage 2 receipt. The package excludes the generated EX5 binary, tester data, logs, credentials, and mutable operational state.

The reconciliation replaces repeated full-history OBV and pivot rebuilds with bounded rolling work, fixes the historical array/shift alignment defects against the approved Stage 1 intent, and preserves the body-signed volume rule, strategy inputs, canonical regime tracker, Minato integration, and transaction/publisher flow.

Regression tests and source fixtures are included for review. Run them from the complete operational `state` tree because that tree supplies additional KCOS dependencies.
