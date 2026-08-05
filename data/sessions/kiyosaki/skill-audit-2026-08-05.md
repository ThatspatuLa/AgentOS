# Kiyosaki Skill Audit — 2026-08-05

## Scope

All 19 skills that touch the kiyosaki swarm + freqtrade + MT5 + scoring pipeline.
Categorised: **freqtrade** (4), **mt5** (2), **kiyosaki** (4), **scoring** (4), **infra** (3), **meta** (2).

Total payload: **395 KB across 19 SKILL.md files** (the 5 kiyosaki+swarm+confidence tiles alone = 278 KB).

---

## 1. THE 19 SKILLS — STATUS TABLE

| # | Skill | Cat | Size | Last edit | Used by kiyosaki? | Verdict |
|---|---|---|---|---|---|---|
| 1 | freqtrade-5-phase-hyperopt-pipeline | freqtrade | 55 KB | 25/07 | YES (entry point) | **KEEP** |
| 2 | freqtrade-parallel-orchestrator | freqtrade | 15 KB | 25/07 | YES (engine layer) | **KEEP** |
| 3 | kiyosaki-confidence-engine-phase25-v2-freqtrade-port | kiyosaki | 70 KB | 25/07 | YES (target port) | **KEEP** |
| 4 | kiyosaki-stage2d | kiyosaki | 34 KB | 25/07 | YES (Stage 2d) | **KEEP** |
| 5 | kiyosaki-swarm-mining | kiyosaki | 74 KB | 25/07 | YES (orchestrator) | **KEEP** |
| 6 | kiyosaki-stage2b-windows-migration-state | kiyosaki | 7 KB | 24/07 | YES (state ref) | **DISCARDED 2026-08-05** ✅ — 12-day stale, state in `AGENTS.md` |
| 7 | mt5-stage1-best-params-extraction | mt5 | 9 KB | 28/07 | YES (fresh) | **KEEP** |
| 8 | mt5-ea-backtest-deployment | mt5 | 14 KB | 27/07 | YES (headless test) | **KEEP** |
| 9 | trading-fitness-function-design | scoring | 13 KB | 30/07 | YES (just edited) | **KEEP** |
| 10 | strategy-repainting-audit | scoring | 12 KB | 25/07 | YES (ZigZag fix history) | **REFRAME** — fold "bar-close-first" invariant into scorer; keep deeper audit |
| 11 | subpool-stage-pipeline | kiyosaki | 9 KB | 25/07 | YES (Liquidity-100) | **KEEP, TIGHTEN** — currently describes mechanism, not workflow |
| 12 | cross-seed-optimization-diagnostics | scoring | 24 KB | 24/07 | YES (plateau analysis) | **KEEP** |
| 13 | cross-validation-gate-pattern | scoring | 2 KB | 28/07 | YES (gate discipline) | **KEEP** — already minimal |
| 14 | installed-skill-curation | meta | 18 KB | 05/08 | YES (this audit) | **KEEP** |
| 15 | skill-decision-framework | meta | 16 KB | 24/07 | YES (decision policy) | **KEEP** |
| 16 | third-party-research-venv | infra | 4 KB | 24/07 | YES (venv setup) | **KEEP** |
| 17 | shell-batch-runner-pattern | infra | 15 KB | 24/07 | YES (batches) | **KEEP** |
| 18 | check-no-owl-alpha | meta | 6 KB | 25/07 | n/a (Zen/Nami) | **DISCARDED 2026-08-05** ✅ — not kiyosaki-related |
| 19 | memory-sync | infra | 9 KB | 25/07 | YES (state sync) | **KEEP** |

---

## 2. OVERLAP CLUSTERS

### Cluster A: "Scoring / fitness / repainting" (4 skills, 51 KB)

| Skill | Job | Overlap |
|---|---|---|
| trading-fitness-function-design (13 KB) | Design/verify fitness formulas | The **naming table** (Kiyosaki Minato Base vs Minato DCA discovery_fitness vs freqtrade hyperopt) IS the unique value. Otherwise overlaps with #4 below. |
| strategy-repainting-audit (12 KB) | 2-layer audit: static scan + dynamic indicator-stability test | **Massive overlap with #4's "is this score real" discussion.** Both address "are the signals valid?" |
| cross-seed-optimization-diagnostics (24 KB) | Plateau / "what knobs matter" analysis | Distinct role (multi-seed robustness, not formula design). |
| cross-validation-gate-pattern (2 KB) | Hold cross-platform gate discipline | Already minimal. No overlap. |

**Verdict:** Merge **strategy-repainting-audit** into **trading-fitness-function-design** as a section. The "bar-close-first" invariant (your Tier-3 note) becomes the first paragraph of the merged skill. drop `strategy-repainting-audit` as a separate tile. Saves ~12 KB.

### Cluster B: "Kiyosaki stage pipeline" (5 skills, 194 KB)

| Skill | Job | Overlap |
|---|---|---|
| kiyosaki-stage2d (34 KB) | Live portfolio selector | Mostly the canonical reference; no overlap. |
| kiyosaki-confidence-engine-phase25-v2-freqtrade-port (70 KB) | The port itself | 70 KB is huge — most of it is **reference material** (TradeCounter, SMC state filter, walk-forward validation). |
| kiyosaki-swarm-mining (74 KB) | Orchestrator + Stage 2b | 74 KB — bundles Stage 1, Stage 2a, Stage 2b, Stage 2c, recovery templates. |
| kiyosaki-stage2b-windows-migration-state (7 KB) | Stale state doc | Useful for 1-2 weeks post-migration. Now obsolete. |
| subpool-stage-pipeline (9 KB) | Add new pool (e.g. Liquidity-100) | Tiny, distinct. |

**Verdict:**
- **Discard #6 (stage2b-windows-migration-state)** — state is in `AGENTS.md` now. Save 7 KB.
- **Compress #5 (kiyosaki-swarm-mining)** — split into 2 files: (a) the **workflow** (when to use which stage, current state) at 8 KB in the main SKILL.md, (b) **templates** moved to `references/` (recovery scripts, sync_queue_state, etc.). Saves ~50 KB on every load.
- **Compress #3 (kiyosaki-confidence-engine-phase25-v2-freqtrade-port)** — same: move the 70 KB of reference docs to `references/`, keep the **workflow** at ≤10 KB in SKILL.md. Saves ~50 KB.

### Cluster C: "MT5" (2 skills, 24 KB)

| Skill | Job | Overlap |
|---|---|---|
| mt5-stage1-best-params-extraction (9 KB) | Parse .opt cache binary | Distinct, narrow. |
| mt5-ea-backtest-deployment (14 KB) | Deploy .ex5 to tester headless | Distinct, narrow. |

**Verdict:** No overlap. Both **KEEP**. Total 24 KB is reasonable.

### Cluster D: "Freqtrade execution" (2 skills, 70 KB)

| Skill | Job | Overlap |
|---|---|---|
| freqtrade-5-phase-hyperopt-pipeline (55 KB) | 5-phase pipeline per bucket | 55 KB is large; most of it is "gotchas" table plus phase definitions. |
| freqtrade-parallel-orchestrator (15 KB) | Concurrent backtest/hyperopt engine | Distinct (engine layer). |

**Verdict:** **Compress #1** — the 55 KB gotchas table should live in `references/gotchas.md`. Keep the workflow + trigger phrases in SKILL.md at ≤15 KB. Saves ~40 KB.

### Cluster E: "Meta" (2 skills, 34 KB)

| Skill | Job | Overlap |
|---|---|---|
| installed-skill-curation (18 KB) | Periodic audit of installed skills | The skill THIS audit is using. |
| skill-decision-framework (16 KB) | Decide install/keep-conditional/drop | Subset of installed-skill-curation. |

**Verdict:** These two have **substantive overlap** — both are about deciding what to do with skills. Compress to **one** tile: keep `installed-skill-curation` (more general), drop `skill-decision-framework` as redundant. Saves ~16 KB.

### Cluster F: "Infra" (3 skills, 28 KB)

| Skill | Job | Verdict |
|---|---|---|
| third-party-research-venv (4 KB) | Install/upgrade third-party venvs | **KEEP** |
| shell-batch-runner-pattern (15 KB) | Bash pattern for N parallel commands | **KEEP** |
| memory-sync (9 KB) | Dual-write Hermes + Obsidian | **KEEP** |

No overlap. All 3 keep.

---

## 3. PROPOSED ACTIONS

### DISCARD (2 skills, -13 KB)
1. `kiyosaki-stage2b-windows-migration-state` — stale 12 days, state in AGENTS.md
2. `check-no-owl-alpha` — not kiyosaki-related (it's for Nami/Zen)

### COMPRESS (4 skills, -150 KB total)
3. `kiyosaki-swarm-mining` (74 KB → 24 KB): move templates/scripts to `references/`, keep workflow in SKILL.md
4. `kiyosaki-confidence-engine-phase25-v2-freqtrade-port` (70 KB → 20 KB): same — references to `references/`, workflow stays
5. `freqtrade-5-phase-hyperopt-pipeline` (55 KB → 15 KB): gotchas table → `references/gotchas.md`
6. `skill-decision-framework` (16 KB → 0): merge intent into `installed-skill-curation`

### MERGE (2 skills → 1, -12 KB)
7. `strategy-repainting-audit` (12 KB) → fold into `trading-fitness-function-design` as "Repainting gate" section. Add the "bar-close-first" invariant from your Tier-3 note as the first paragraph.

### TIGHTEN (1 skill, no size change)
8. `subpool-stage-pipeline` — currently describes mechanism; should describe workflow (when to use, decision tree, current subpools). Add a "current sub-pools" table at the top.

### KEEP (12 skills, no change)
- freqtrade-parallel-orchestrator
- kiyosaki-stage2d
- mt5-stage1-best-params-extraction
- mt5-ea-backtest-deployment
- trading-fitness-function-design (gets larger after merge)
- cross-seed-optimization-diagnostics
- cross-validation-gate-pattern
- installed-skill-curation (gets larger after merge)
- third-party-research-venv
- shell-batch-runner-pattern
- memory-sync
- platform-mechanism-explanations (general, not specific)

---

## 4. NET IMPACT

| Metric | Before | After | Change |
|---|---|---|---|
| Total skills for kiyosaki work | 19 | 17 | -2 |
| Total payload | 395 KB | ~220 KB | **-44%** |
| Average per-skill size | 20.8 KB | 12.9 KB | -38% |
| Skill halves needing rename | 0 | 0 | 0 |

The 4 largest skills (74+ KB each) are the same ones most likely to be **summarised away** by Hermes's session-start context loader — so the actual system-prompt cost is lower than the on-disk size suggests. But the on-disk size still matters for `skill_view()` speed and human maintenance.

---

## 5. BEFORE/AFTER FREQUENCY MAP (estimated)

How often each is actually invoked in a kiyosaki session:

| Skill | Est. freq | Reason |
|---|---|---|
| freqtrade-5-phase-hyperopt-pipeline | 1× per Stage 2b run | Mechanism only used on bucket init |
| freqtrade-parallel-orchestrator | 2× per Stage 2b run | Engine for 332 strategies |
| kiyosaki-confidence-engine-phase25-v2-freqtrade-port | 1× per port attempt | Template lookup |
| kiyosaki-stage2d | 0.5× per session | Live portfolio decisions |
| kiyosaki-swarm-mining | 2× per session | Orchestrator + stage transitions |
| mt5-stage1-best-params-extraction | 1× per MT5 opt | After every .opt run |
| mt5-ea-backtest-deployment | 1× per EA | After every recompile |
| trading-fitness-function-design | 3× per session | Formula design/discuss |
| strategy-repainting-audit | 2× per Stage 2b | Each new strategy needs audit |
| subpool-stage-pipeline | 0.1× per session | Only when adding pools |
| cross-seed-optimization-diagnostics | 0.5× per Stage 2b | Plateau diagnosis |
| cross-validation-gate-pattern | 0.2× per session | Gate enforcement |
| installed-skill-curation | 0.1× per session | This audit only |
| skill-decision-framework | 0 (overlap) | Replaced by installed-skill-curation |
| third-party-research-venv | 0.05× per quarter | Venv setup |
| shell-batch-runner-pattern | 1× per Stage 2b | Every MT5 batch |
| memory-sync | 1× per session | After every durable change |
| check-no-owl-alpha | 0 | **DISCARDED 2026-08-05** ✅ |
| kiyosaki-stage2b-windows-migration-state | 0 | **DISCARDED 2026-08-05** ✅ (also pre-emptively gone) |

---

## 6. RECOMMENDED ORDER OF WORK

1. **Get your approval on the 4 high-impact compressions** (clusters B, D, E) — these move a lot of content.
2. **Merge strategy-repainting-audit into trading-fitness-function-design** — small, clean, implements your Tier-3 bar-close-first directive.
3. **Discard the 2 dead skills** — trivial, no risk. ✅ **DONE 2026-08-05** (both skills removed)
4. **Tighten subpool-stage-pipeline** — add current-sub-pools table.
5. Re-verify total payload (target: ~220 KB).

---

## 7. STATUS AS OF 2026-08-05 (after cleanup)

| Action | Status |
|---|---|
| Owl-alpha cleanup (8 ZenNew files, 25 live refs → 0 live) | ✅ DONE |
| `check-no-owl-alpha` skill discarded | ✅ DONE |
| `kiyosaki-stage2b-windows-migration-state` skill discarded | ✅ DONE |
| 8 `.pre-owl-alpha-cleanup.*` backups removed | ✅ DONE (auto-cleaned) |
| 17 active kiyosaki skills remain (~382 KB) | Awaiting compressions |
| 4 compressions + 1 merge + 1 tighten (5 changes) | PENDING user approval |

**Next steps** (when ready to restart):
- Merge `strategy-repainting-audit` into `trading-fitness-function-design` (implements Tier-3 bar-close-first invariant)
- Compress `kiyosaki-swarm-mining` (74 → 24 KB) — move templates to `references/`
- Compress `kiyosaki-confidence-engine-phase25-v2-freqtrade-port` (70 → 20 KB) — same
- Compress `freqtrade-5-phase-hyperopt-pipeline` (55 → 15 KB) — gotchas → `references/gotchas.md`
- Merge `skill-decision-framework` → `installed-skill-curation` (subset; saves ~16 KB)
- Tighten `subpool-stage-pipeline` — add current-sub-pools table

**Target after all work**: ~220 KB (-44%), 16 skills (vs 19 start).

