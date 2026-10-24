# KCOS Stage 2 v2 — build, ranges, INI, and structural verification

Stage 2 consumes the deterministic handoff at `state/handoffs/Stage2_v2.json`.
It builds, repairs, compiles, and structurally verifies exactly one EA. After a
passing verification, Stage 2 finalizes optimization ranges and publishes
hash-bound M5/M15/H1 tester INI files alongside the authenticated handoff. It
never launches MT5, runs optimization, evaluates trading performance, rotates
the queue, researches the web, or starts Stage 3.

## Source authority

- When a valid Stage 1 specification exists, rebuild/reconcile the existing EA
  from that specification and treat the specification as strategy authority.
- Stage 1 owns strategy logic and the declared input identities. Stage 2 owns
  the executable optimization ranges and final tester INI files. Stage 2 must
  not invent optimization domains from Stage 1's suggested start/step/stop
  values; those values are hints unless a named protected contract explicitly
  requires an exact domain. Every optimized numeric input in the published
  INIs must have 15–20 MT5 steps (16–21 candidate values), including the default.
- Stage 3 validates the Stage 2 source/config/template/INI hashes and consumes
  the exact published INI bytes. It must not regenerate or widen optimizer
  domains.
- For a legacy queued EA without a Stage 1 specification, verify and repair its
  existing source without forcing a rebuild.
- The canonical name for this cycle is the `ea` value injected in the Stage 2 handoff.
- Preserve infrastructure that is consistent with the specification.
- Signals must use completed bars only. `NewBarClosed()` is a scheduling gate,
  but signal values must also come from closed shifts 1 and 2, never shift 0.
- Bind indicator periods to the authenticated Stage 1 execution contract. For
  DCC specs marked `must_use_ethusd_m5_closed_bars`, ETH/BTC signals and ATR
  remain on M5 with shift 1 even when Stage 3's Tester Period changes.
- Use the canonical `minato_base_score_v2.mqh`; do not copy or redefine its helpers.
- `OnInit()` must reset and validate the V2 score configuration, `OnTick()` must
  record endpoint equity/margin through `MinatoV2ObserveTick()`, and `OnTester()`
  must return `MinatoV2ScoreFromTester()`.
- Preserve `InpMarginKillLevel` exactly as declared by the authenticated Stage 1
  spec, including its optimizer flag and full start/step/stop range. Use the
  real MQL5 percentage property `ACCOUNT_MARGIN_LEVEL` for that entry gate and
  skip/log `margin_kill_skip` when the configured threshold is breached. Keep
  the separate canonical 120% emergency close/refuse path unchanged; it closes
  exposure and is not a replacement for the Stage 1 entry threshold.
- Size entries with `MinatoRegimeQuantity(...)`; when it returns zero, skip the
  trade before any lot-normalization call.
- For `B1_1_GJR_GARCH_AsymmetricVolatility_RegimeGate_v1`, Stage 2 derives a
  fixed `InpBaseLot=0.01` from the existing source fallback when Stage 1 omits
  the input. Clamp the requested base to `SYMBOL_VOLUME_MIN` before applying
  the regime lot multiplier in both long and short paths. Preserve the Stage 1
  specification and completion-event bytes and hashes; include the derived
  input in the Stage 2 config hash and Stage 3 input surface.
- Build fixed take profit with `MinatoTakeProfit(...)`; a zero regime TP
  multiplier must produce `tp=0` (no fixed TP), never a zero-distance order.
- Before every Buy/Sell order, require `MinatoAllowsProjectedMargin(...)` for
  the exact side, volume, and entry price. Reject the order when it is false.
- OnTick must close/refuse exposure below 120%. `MINATO_BASE_SCORE_V2.0` independently
  emits the severity-ordered `MARGIN_BREACH` penalty when the observed minimum
  margin level is below 120%.

## Compile and repair loop

1. Run the deterministic structural verifier without compile evidence first.
2. If its structural checks pass, do not reread or reinterpret the full source.
3. Make source corrections only for a named failed check, error, or warning.
4. Run `state/kcos_stage2_compile_v2.py` for one compile attempt.
5. Zero errors, zero warnings, and a fresh EX5 are required.
6. Any error or warning must be diagnosed and corrected by the LLM.
7. Maximum: five compile attempts for the cycle.
8. After exhaustion, call `state/kcos_stage2_failure_v2.py --exhausted` and stop
   silently. Do not notify the operator and do not start another stage.

For other repeated structural failures, diagnose and correct the first
occurrence. On the second occurrence, record it through the failure recorder
and stop silently. Two distinct strategies with the same normalized exhausted
failure category set `deeper_review_required=true`.

## Verification and completion

Run the deterministic structural verifier with the final compile evidence. It
must prove canonical identity, V2 score version and include hash, all three V2
hooks, source/EX5 hashes, the score-date contract, and both margin
gates, zero quantity/TP semantics, 12 regime parameters, completed-bar-only signals, clean compilation, and absence
of MT5/tester launch configuration. It must also prove that the published Stage
1 specification still matches its completion event. Stage 2 must never edit or
rehash published Stage 1 evidence.

Only the deterministic publisher may write the Stage 2 completion event and
advance canonical v2 state. It advances to `stage3_pending` with
`stage2_complete_held`. Legacy state is evidence during this rollout and must
not be modified. Stage 3 remains paused.

## Failure evidence and scoped learning

Record each diagnosed Stage 2 failure and repair transaction in the existing
`state/ledgers/stage2_repair_episodes.sqlite3` episode ledger. Keep observations
and lesson-state events append-only; generate the legacy JSON/Markdown failure
reports as compatibility views. Retrieve lessons only when the defect, EA
family, protected-contract version, and source preconditions match. A proposed
repair still passes the current deterministic checks and transaction commit
before its lesson can become reusable. Runtime-related lessons also require a
hash-bound, candidate-matched authenticated Stage 3 receipt.

The ledger and its explicitly enumerated historical-import manifest are the
only Stage 2-specific failure-learning store. This does not authorize changing
global/user memory, skills, rules, or project instructions. Imported historical
PASS messages and rejected proposals remain observations unless all current
lesson evidence requirements are independently met.

Use the paths injected in the handoff:

```text
python state/kcos_stage2_compile_v2.py --handoff state/handoffs/Stage2_v2.json --output <latest_compile_result>
python state/kcos_stage2_verify_v2.py --handoff state/handoffs/Stage2_v2.json --compile <latest_compile_result> --output <expected_verification>
python state/kcos_stage2_publish_v2.py --state state/kcos_cycle_state_v2.json --verification <expected_verification> --event <expected_event>
```

The publisher is the only success-state writer. Return a short local summary;
routine failures recorded under the approved policy remain silent.
Do not write to global/user memory, skills, rules, or project instructions.
Stage 2 may write only the project-specific, append-only failure observations
and evidence-scoped lesson records described above.
