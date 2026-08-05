# Kiyosaki Conversation Prep — Stage 2c/2d

## Context
- Pipeline: Kiyosaki (ETH/USDT 5m, freqtrade)
- Current phase: Stage 2.5 v2 reached fitness 1.8861
- Six wants to talk to Kiyosaki about CHANGING something — context: batch 6 was about fitness-function tuning (mutation/coverage plateau discussion)

## The Thing to Change
The thing Six flagged "41" on. From the conversation context, this is the mutation/branch-coverage discussion:

**Issue:** Mutation/hyperopt search has plateaued.
- Stage 2.5 v2 fitness = 1.8861
- Returns only +1.4% YoY
- Tree branches / coverage is too narrow — not exploring enough of the strategy space

**What Six wants changed:**
- Broaden the mutation strategy
- Better branch coverage
- Higher trade frequency (TPM) was the original goal
- Move from exploitation (3-5%) toward more discovery (15-20%) mode

## Key Numbers to Discuss
- Fitness 1.8861 (Stage 2.5 v2)
- +1.4% YoY return (insufficient)
- Mutation weights locked at 60/20/10/5/5 (no return component)
- Six already approved shift to 30/20/15/10/10/5 — adds return weight
- TPM primary metric, not win rate
- Discovery vs exploitation mode %

## Open Questions for Kiyosaki
1. Does the new mutation weight set (30/20/15/10/10/5) actually move fitness, or just mask poor candidates?
2. Branch coverage — how do we measure it? Tree depth? Operator diversity? Population entropy?
3. Can we evaluate Phase 3 on all failed strategies, or only top-K?
4. What's the discovery/exploitation budget? Default 18%/4% with midpoints, not 20%/5% max.

## Mode
- Discovery 15-20% (target 18% midpoint)
- Exploitation 3-5% (target 4% midpoint)
- Ceilings are hard limits — don't expand without explicit approval

## Stage Context
- Stage 2b: Windows migration complete
- Stage 2c: Portfolio selector work
- Stage 2d: Meta-strategy (current)
- Stage 2.5 v2: freqtrade port, fitness 1.8861
- Stage 3: evaluation on failed strategies (pending)

## Six's Style for the Conversation
- Terse, numbers-first
- Will redirect away from unproductive work
- Asks for real numbers from real files, not theory
- Primary metrics: TPM, profit %/wk, max DD
- Tables-with-percentages preferred format
- Don't pre-decide for him — present options, he calls keep/discard
