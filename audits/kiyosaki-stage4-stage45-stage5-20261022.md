# Kiyosaki Stage 4, Stage 4.5, and Stage 5 receipt — 2026-10-22

Cycle: `kcos-20261022T051345Z-research-new`  
EA: `B1_1_Donchian_Breakout_Classic_v1`

## Verified outcomes

- Stage 3 completed and passed verification (`READY_FOR_STAGE4`). Its implementation receipt is already recorded in commit `803332be` on this branch.
- Stage 4 emitted `STAGE4_PLANNED`; verification passed. It selected M15 with 4,046 qualifying rows.
- Stage 4.5 completed three narrow optimization runs; all three verifications passed. Qualifying rows were 5,243, 6,766, and 7,380. The retained best Minato scores were 60.8106, 64.1862, and 65.1788.
- Stage 5 completed and passed its verifier, but the forward-performance decision was `STAGE5_FORWARD_FAIL` (`HOLD_FOR_REVIEW`). This is a verified strategy failure, not a successful Stage 5 gate:
  - profit: -2,636.38; return: -26.36%
  - drawdown: 31.909%, above the 25% maximum
  - forward CF: 0.0, below half the Stage 4.5 score (32.5894)
  - passed checks: 773 forward trades (at least half the optimized 1,043), 83.14 trades/month, 397.03% minimum margin

## Evidence paths and hashes

All paths below are under `C:\Users\Tyson\Projects\kiyosaki_stage2b\state`.

- Stage 4 event `events\kcos-20261022T051345Z-research-new.stage4.complete.json`: `d88dfd0c38d7add91e0bf48639a8208f0efe660f22a382fb8da69f08bbc552a0`
- Stage 4 decision `decisions\kcos-20261022T051345Z-research-new.decision.json`: `225fdab3e67326d09e6e5b3dbe5e2c15893c54e53bb726c814fee4babc90bfbb`
- Stage 4 verification `verification\kcos-20261022T051345Z-research-new.stage4.verification.json`: `9e96a3f2aae7872474bb68d0be128f9729a5838e511971fd36e9b97b035a5800`
- Stage 4.5 run 1 event: `5b96340d500fc97cadf7070aa798e64535c348645cfcd60a5154f26a0a4590ef`; verification: `dc34aa322cc1dddd476a6b17bda19733f4c4c01e0d226f1b7fb055206319bb93`
- Stage 4.5 run 2 event: `5c0b26f981ea0b5438961b186a25a0a1bf0f4f3df7357b0b260bd0e151fa289a`; verification: `e18f702f26678846c4e528f061f91d1da92a02bbdd4f8e364925a680e8764e27`
- Stage 4.5 run 3 event: `9887f0e8c82baa4f8f83688120e904a5ff9b1bab10e9bada399c1efa8265c7e9`; verification: `37e2acf3ab6fe28e38b8ad2fcf5e24b8092c045b18bf750225c3adcb3f39973d`
- Stage 5 event `events\kcos-20261022T051345Z-research-new.stage5.complete.json`: `c40a463fc75bf7d5580fbfdcfaa7b1f48e37c487b11b986769f83b26402fcd1d`
- Stage 5 decision `decisions\kcos-20261022T051345Z-research-new.stage5.decision.json`: `8261e3e75ba3c6084ec0c423cd2173e93e662854a7f06ca809dbc74b363fa768`
- Stage 5 verification `verification\kcos-20261022T051345Z-research-new.stage5.verification.json`: `2e8cdf52e224ee70109b9c7331c9e9fa0a0441bb543a3f4c01890eb2991ff130`
- Stage 5 result `results\kcos-20261022T051345Z-research-new.stage5.result.json`: `a8010d0bb2a2f0a3880503a244d311a2773fcf0eefbac40844c128e96fe4f53b`

The cycle's Kiyosaki controls and all stage jobs were paused after the Stage 5 forward failure, at the requested halt boundary. No downstream run was started by this receipt.
