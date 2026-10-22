# Kiyosaki active cycle source snapshot

This checkpoint contains the live Stage 2 repair/executor/verifier and Stage 3/4/4.5 sizing cron sources captured from the external Kiyosaki runtime. Per-file hashes and exact source sizes are recorded in `manifest.json`.

Stage 3 searches base position size from 0.01 to 0.20 in 0.01 steps. Stage 4.5 can expand the GJR base lot above 0.20 when margin-qualified observations meet the 120% floor. This is a source snapshot only; it does not claim the current cycle completed or Stage 2 repair passed.
