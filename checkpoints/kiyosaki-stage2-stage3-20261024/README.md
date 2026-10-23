# Kiyosaki Stage 2 and Stage 3 checkpoint

Read-only archival snapshot captured 2026-10-24 from the live `kiyosaki_stage2b` project, which is outside Git. It contains current Stage 2/3 Python modules and tests, the EA source and protected integration includes, the exact Stage 2-published M5/M15/H1 INIs, and a hash-bound summary of Stage 2 and Stage 3 receipts.

The captured cycle completed Stage 3 for M5, M15, and H1. At capture it had advanced to `stage4_pending` with `next_action=run_stage4`. Its latest Stage 4 attempt failed the timing-input guard for `InpMinBarsBetweenTrades`, then raised a secondary `UnboundLocalError` while reporting the failure; this is recorded as observed evidence, not repaired or cleared by the snapshot.

Runtime databases, mutable tester caches, logs, compiled binaries, credentials, and market data are excluded. `manifest.json` lists each captured payload file with source path, size, and SHA-256.
