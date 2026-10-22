# KCOS Stage 2 Donchian zero-trade repair — 2026-10-22

## Result

**Stage 2 repair verified; M5 results confirm trade generation.** The repair corrected the reported zero-trade defect in B1_1_Donchian_Breakout_Classic_v1 by computing the Donchian entry channel from prior closed bars (indices 1..N), excluding the signal candle at index 0.

## Stage 2 evidence

- Repair transaction: FE80268B9A19D4ED028651B0F80AF04C8E1C4A293C0BF44F02E75DB8EEE6A44D, attempt 20261022T104453Z-f7571bbdfe.
- Transaction outcome: REPAIRED; transaction journal: COMMITTED.
- Stage 2 verification: passed. MetaEditor compile: exit code 0, zero errors, zero warnings.
- Repaired MQ5 SHA-256: $hash.
- Fresh EX5 SHA-256: 4C79679ECDD913DFB04965F355BE79079B206EAD4A882FCAFED52184DA690664.

## Stage 3 confirmation

The authorized M5 rerun used the same repaired MQ5 and fresh EX5 hashes. The authenticated Stage 3 parser passed for OPT 524D3C630ACC437C3C67A03F482F935E99E23F51FDFBE2674548716B07ECA297 and CSV 0E95F0ACC32EF380668060498A25E65F788BA76882CFFD2051BAB6D85F4B7C64:

- 10,078 parsed optimization records
- 7,957 records with Trades > 0
- 2,121 records with Trades = 0
- Maximum observed Trades value: 4,990

This confirms the M5 optimization no longer produces zero trades across all results. These are optimizer rows, not a claim of Stage 3 completion or live trading.

## Held follow-on

The Stage 3 run did not complete the full timeframe sequence. Its M15 follow-on hit NO_EXPERT_IN_START_CONFIG; the fresh Journal says “no expert specified in the start config” and records 512 passes of 512 failed with incorrect input parameters. The state is now stage2_pending / stage2_repair_required with 
ext_action=dispatch_stage2 (while the separate stage2_repair_required flag is false). Stage 2 and Stage 3 crons are paused. No Stage 4 or later work ran.

The follow-on M15 admission/configuration issue is recorded separately from the verified Stage 2 no-trade repair.

## Preserved source

[Repaired MQ5 snapshot](../../checkpoints/kcos-stage2-donchian-zero-trades-20261022/B1_1_Donchian_Breakout_Classic_v1.mq5)
