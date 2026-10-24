#!/usr/bin/env python3
"""Idempotent Stage 2 EA safety-contract repair before compile/verify."""
from __future__ import annotations
import hashlib, json, re, shutil, sys
from datetime import datetime, timezone
from pathlib import Path


def sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest().upper()


def repair(source: Path) -> dict:
    original = source.read_text(encoding="utf-8-sig")
    text = original
    changes: list[str] = []
    # Volatility-of-volatility uses the canonical regime functions but its
    # legacy order path normalized the raw regime quantity before the zero
    # check. That can bypass the Minato quantity contract. Keep the signal and
    # source-specific multipliers; only repair the safety flow.
    if source.name == "B1_1_VolatilityOfVolatility_StdDev_v1.mq5":
        text, n = re.subn(
            r"double lot = InpLots \* RegimeLotMult\(r\);",
            "double lot = MinatoRegimeQuantity(InpLots, RegimeLotMult(r));",
            text, count=1)
        if n: changes.append("vov_regime_quantity")
        text, n = re.subn(
            r"if \(sl_dist <= 0\.0 \|\| tp_dist <= 0\.0 \|\| lot <= 0\.0\) return;",
            "if (sl_dist <= 0.0 || tp_dist <= 0.0) return;\n   if (lot <= 0.0) return;",
            text, count=1)
        if n: changes.append("vov_zero_quantity_before_normalization")
        text, n = re.subn(r"\n\s*lot = NormalizeLot\(lot\);", "", text, count=1)
        if n: changes.append("vov_remove_unsafe_normalization")
    # MaxEntropy/HMM dual-regime order layout: preserve the researched
    # multipliers while routing final quantity through the canonical Minato
    # contract. Signature-gated to this exact expression.
    if ("double lot = InpLots * regime_lot * hmm_lot;" in text
            and "MinatoRegimeQuantity" not in text
            and "trade.Buy(lot, _Symbol, ask, sl, tp, \"HMM_L\")" in text
            and "trade.Sell(lot, _Symbol, bid, sl, tp, \"HMM_S\")" in text):
        text, n = re.subn(
            r"double lot = InpLots \* regime_lot \* hmm_lot;",
            "double lot = MinatoRegimeQuantity(InpLots, regime_lot * hmm_lot);",
            text, count=1)
        if n: changes.append("maxentropy_regime_quantity_contract")
        text, n = re.subn(r"\n\s*lot = NormalizeLot\(lot\);", "", text, count=1)
        if n: changes.append("maxentropy_remove_unsafe_lot_normalization")

    # Common regime order layout: replace raw sizing and normalization before
    # the existing projected-margin guards. This applies only when the source
    # already exposes the canonical 12 regime inputs/functions.
    if "double lot = InpLots * RegimeLotMult(" in text and "MinatoRegimeQuantity" not in text:
        text, n = re.subn(
            r"double lot = InpLots \* RegimeLotMult\(([^)]+)\);",
            r"double lot = MinatoRegimeQuantity(InpLots, RegimeLotMult(\1));",
            text, count=1)
        if n: changes.append("regime_quantity_contract")
    if "MinatoRegimeQuantity" in text:
        text, n = re.subn(
            r"if \(sl_dist <= 0\.0 \|\| tp_dist <= 0\.0 \|\| lot <= 0\.0\) return;",
            "if (sl_dist <= 0.0 || tp_dist <= 0.0) return;\n   if (lot <= 0.0) return;",
            text, count=1)
        if n: changes.append("zero_quantity_before_normalization")
        text, n = re.subn(r"\n\s*lot = NormalizeLot\(lot\);", "", text, count=1)
        if n: changes.append("remove_unsafe_lot_normalization")
    # WilliamsR has separate norm_lot locals around each order.
    if source.name == "B1_1_WilliamsR_MeanReversion.mq5":
        text, n = re.subn(r"double norm_lot = NormalizeLot\(lot\);", "double norm_lot = MinatoRegimeQuantity(InpLots, RegimeLotMult(regime));\n      if (norm_lot <= 0.0) return;", text)
        if n: changes.append("williams_regime_quantity")
        text = text.replace("if (norm_lot > 0.0 && trade.Buy(norm_lot, _Symbol, ask, sl, tp, \"WPR_Rev_L\"))", "if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, norm_lot, ask)) return;\n      if (trade.Buy(norm_lot, _Symbol, ask, sl, tp, \"WPR_Rev_L\"))")
        text = text.replace("if (norm_lot > 0.0 && trade.Sell(norm_lot, _Symbol, bid, sl, tp, \"WPR_Rev_S\"))", "if (!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, norm_lot, bid)) return;\n      if (trade.Sell(norm_lot, _Symbol, bid, sl, tp, \"WPR_Rev_S\"))")
        changes.append("williams_order_guards")
    text, n = re.subn(r"(InpMarginKillLevel\s*=\s*)90(?:\.0+)?(\s*;)", r"\g<1>120.0\2", text)
    if n: changes.append("margin_kill_120")
    # Legacy normalized-lot/order layout used by Keltner/MFI families.
    if "Keltner_Rev_L" in text or "MFI_Rev_L" in text:
        text, n = re.subn(r"double lot = InpLots \* RegimeLotMult\(regime\);", "double lot = MinatoRegimeQuantity(InpLots, RegimeLotMult(regime));", text, count=1)
        if n: changes.append("legacy_regime_quantity")
        text = text.replace("if (lot <= 0.0) lot = InpLots;", "if (lot <= 0.0) return;")
        buy_pattern = r'double tp = NormalizeDouble\(ask \+ tp_dist, _Digits\);\s+double norm_lot = NormalizeLot\(lot\);\s+if \(norm_lot > 0\.0 && trade\.Buy\(norm_lot, _Symbol, ask, sl, tp, "([A-Za-z0-9_]+)"\)\)'
        text, n = re.subn(buy_pattern, lambda m: 'double tp = MinatoTakeProfit(ask, tp_dist, 1.0, true);\n      tp = NormalizeDouble(tp, _Digits);\n\n      if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lot, ask)) return;\n\n      if (trade.Buy(lot, _Symbol, ask, sl, tp, "' + m.group(1) + '"))', text, count=1)
        if n: changes.append("legacy_buy_contract")
        sell_pattern = r'double tp = NormalizeDouble\(bid - tp_dist, _Digits\);\s+double norm_lot = NormalizeLot\(lot\);\s+if \(norm_lot > 0\.0 && trade\.Sell\(norm_lot, _Symbol, bid, sl, tp, "([A-Za-z0-9_]+)"\)\)'
        text, n = re.subn(sell_pattern, lambda m: 'double tp = MinatoTakeProfit(bid, tp_dist, 1.0, false);\n      tp = NormalizeDouble(tp, _Digits);\n\n      if (!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, lot, bid)) return;\n\n      if (trade.Sell(lot, _Symbol, bid, sl, tp, "' + m.group(1) + '"))', text, count=1)
        if n: changes.append("legacy_sell_contract")
    # MTFTrend v4 Elder contract: exact long-only percentage-TP layout.
    if ("B1_1_MTFTrend_v4_Elder" in text
            and "input double  InpTakeProfitPct" in text
            and "if (!trade.Buy(InpLots, _Symbol, ask, sl, tp, InpTradeComment))" in text):
        text, n = re.subn(r"(input double  InpLots\s*=\s*[^;]+;)", r"\1\ninput double  InpMarginKillLevel = 120.0;", text, count=1)
        if n: changes.append("mtftrend_elder_margin_input")
        text, n = re.subn(r"if \(!IsNewBar\(\)\) return;", "if (!NewBarClosed()) return;", text, count=1)
        if n: changes.append("mtftrend_elder_closed_bar_call")
        text, n = re.subn(r"(MinatoV2ObserveTick\(\);\s*\n)(\s*if \(!NewBarClosed\(\)\) return;)",
                          r"\1\n   double margin_level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);\n   if (margin_level < InpMarginKillLevel)\n   {\n      if (PositionSelect(_Symbol)) trade.PositionClose(_Symbol);\n      return;\n   }\n\n\2", text, count=1)
        if n: changes.append("mtftrend_elder_live_margin_kill")
        text, n = re.subn(r"double tp\s*=\s*\(InpTakeProfitPct\s*>\s*0\.0\)\s*\?\s*ask\s*\*\s*\(1\.0\s*\+\s*InpTakeProfitPct\)\s*:\s*0\.0;",
                          "double tp_dist = (InpTakeProfitPct > 0.0) ? ask * InpTakeProfitPct : 0.0;\n      double tp = MinatoTakeProfit(ask, tp_dist, 1.0, true);\n      tp = NormalizeDouble(tp, _Digits);", text, count=1)
        if n: changes.append("mtftrend_elder_take_profit")
        text, n = re.subn(r"(double ask = SymbolInfoDouble\(_Symbol, SYMBOL_ASK\);\s*\n\s*double sl\s*=.*?;\s*\n\s*double tp_dist =.*?;\s*\n\s*double tp = MinatoTakeProfit\([^;]+;\s*\n\s*tp = NormalizeDouble\(tp, _Digits\);)",
                          r"\1\n      double eff_lot = MinatoRegimeQuantity(InpLots, 1.0);\n      if (eff_lot <= 0.0) return;\n      if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, eff_lot, ask)) return;", text, count=1, flags=re.S)
        if n: changes.append("mtftrend_elder_quantity_margin_guard")
        text, n = re.subn(r"trade\.Buy\(InpLots, _Symbol, ask, sl, tp, InpTradeComment\)", "trade.Buy(eff_lot, _Symbol, ask, sl, tp, InpTradeComment)", text, count=1)
        if n: changes.append("mtftrend_elder_quantity")
    # MTFTrend legacy contract: one long-only order path with direct Lots/regime sizing.
    if "B1_1_MTFTrend" in text and "double eff_lot   = Lots * lot_mult;" in text:
        text, n = re.subn(r"(input double Lots\s*=\s*[^;]+;)", r"\1\ninput double InpMarginKillLevel = 120.0;", text, count=1)
        if n: changes.append("mtftrend_margin_input")
        text, n = re.subn(r"(void OnTick\(\)\s*\{\s*\n\s*MinatoV2ObserveTick\(\);)", r"\1\n\n   if (!NewBarClosed()) return;\n\n   double margin_level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);\n   if (margin_level < InpMarginKillLevel)\n   {\n      if (PositionSelect(_Symbol)) trade.PositionClose(_Symbol);\n      return;\n   }", text, count=1)
        if n: changes.append("mtftrend_closed_bar_margin_kill")
        text, n = re.subn(r"double eff_lot\s*=\s*Lots \* lot_mult;", "double eff_lot   = MinatoRegimeQuantity(Lots, lot_mult);", text, count=1)
        if n: changes.append("mtftrend_regime_quantity")
        text, n = re.subn(r"(double eff_lot\s*=\s*MinatoRegimeQuantity\(Lots, lot_mult\);)", r"\1\n         if (eff_lot <= 0.0) return;", text, count=1)
        if n: changes.append("mtftrend_zero_lot_refusal")
        text, n = re.subn(r"double tp = ask \+ eff_tp_atr \* atr1;", "double tp = MinatoTakeProfit(ask, eff_tp_atr * atr1, 1.0, true);\n         tp = NormalizeDouble(tp, _Digits);", text, count=1)
        if n: changes.append("mtftrend_take_profit")
        text, n = re.subn(r"(double sl = ask - eff_sl_atr \* atr1;\s*\n\s*double tp = MinatoTakeProfit\(ask, eff_tp_atr \* atr1, 1.0, true\);\s*\n\s*tp = NormalizeDouble\(tp, _Digits\);)", r"\1\n         if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, eff_lot, ask)) return;", text, count=1)
        if n: changes.append("mtftrend_projected_margin")
    # OBV Divergence v1: exact legacy ATR/regime layout.
    # Keyed to the canonical filename plus the distinctive raw lot expression;
    # do not apply this transformation to unrelated divergence EAs.
    if ("B1_1_OBV_Divergence_v1" in text
            and "double lot = InpLots * RegimeLotMult(r);" in text
            and "double tp_dist = atr1 * InpAtrTpMult * RegimeTPMult(r);" in text):
        text, n = re.subn(r"(input double InpMarginKillLevel\s*=\s*)90(?:\.0+)?(\s*;)", r"\g<1>120.0\2", text, count=1)
        if n: changes.append("obv_margin_kill_120")
        text, n = re.subn(r"double lot = InpLots \* RegimeLotMult\(r\);", "double lot = MinatoRegimeQuantity(InpLots, RegimeLotMult(r));", text, count=1)
        if n: changes.append("obv_regime_quantity")

        text, n = re.subn(
            r"if \(sl_dist <= 0\.0 \|\| tp_dist <= 0\.0 \|\| lot <= 0\.0\) return;",
            "if (sl_dist <= 0.0 || tp_dist <= 0.0) return;\n   if (lot <= 0.0) return;",
            text, count=1)
        if n: changes.append("obv_zero_lot_refusal")

    # RealizedCorrelation Regime ETH/BTC M5 v1: exact raw-Lots, dual-order
    # layout. This branch is filename/signature gated; it preserves the
    # correlation signal and adds only the canonical Minato safety contract.
    if (source.name == "B1_1_Realized_Correlation_Regime_ETH_BTC_M5_v1.mq5"
            and "input double Lots" in text
            and "bool IsNewBar()" in text
            and "trade.Buy(Lots, _Symbol, ask, sl, tp, TradeComment)" in text
            and "trade.Sell(Lots, _Symbol, bid, sl, tp, TradeComment)" in text
            and "MinatoV2ObserveTick();" in text):
        text, n = re.subn(
            r"(input double Lots\s*=\s*[^;]+;)",
            r"\1\ninput double InpMarginKillLevel = 120.0;",
            text, count=1)
        if n: changes.append("realized_corr_margin_input")
        text, n = re.subn(
            r"(MinatoV2ObserveTick\(\);\s*\n)(\s*if \(!IsNewBar\(\)\) return;)",
            r"\1\n   double margin_level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);\n   if (margin_level > 0.0 && margin_level < InpMarginKillLevel)\n   {\n      g_margin_breach = true;\n      CloseAllPositions();\n      return;\n   }\n\n   if (!NewBarClosed()) return;",
            text, count=1)
        if n: changes.append("realized_corr_closed_bar_margin_kill")
        text, n = re.subn(
            r"(double tp_distance = atr_val \* InpAtrTpMult;)",
            r"\1\n\n   double eff_lot = MinatoRegimeQuantity(Lots, 1.0);\n   if (eff_lot <= 0.0) return;",
            text, count=1)
        if n: changes.append("realized_corr_regime_quantity")
        text, n = re.subn(
            r"double sl = ask - sl_distance;\s*\n\s*double tp = ask \+ tp_distance;\s*\n\s*opened = trade\.Buy\(Lots, _Symbol, ask, sl, tp, TradeComment\);",
            "double sl = NormalizeDouble(ask - sl_distance, _Digits);\n      double tp = MinatoTakeProfit(ask, tp_distance, 1.0, true);\n      tp = NormalizeDouble(tp, _Digits);\n      if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, eff_lot, ask)) return;\n      opened = trade.Buy(eff_lot, _Symbol, ask, sl, tp, TradeComment);",
            text, count=1)
        if n: changes.append("realized_corr_buy_contract")
        text, n = re.subn(
            r"double sl = bid \+ sl_distance;\s*\n\s*double tp = bid - tp_distance;\s*\n\s*opened = trade\.Sell\(Lots, _Symbol, bid, sl, tp, TradeComment\);",
            "double sl = NormalizeDouble(bid + sl_distance, _Digits);\n      double tp = MinatoTakeProfit(bid, tp_distance, 1.0, false);\n      tp = NormalizeDouble(tp, _Digits);\n      if (!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, eff_lot, bid)) return;\n      opened = trade.Sell(eff_lot, _Symbol, bid, sl, tp, TradeComment);",
            text, count=1)
        if n: changes.append("realized_corr_sell_contract")

    # RealizedSemivariance UpDown AsymRegimeGate v1: exact dual-path
    # semivariance layout. Gate by canonical filename plus distinctive regime
    # sizing/order comments so unrelated volatility EAs remain untouched.
    if (source.name == "B1_1_RealizedSemivariance_UpDown_AsymRegimeGate_v1.mq5"
            and "double lot = InpLots * regime_lot;" in text
            and "trade.Buy(lot, _Symbol, ask, sl, tp, \"RSEMI_L\")" in text
            and "trade.Sell(lot, _Symbol, bid, sl, tp, \"RSEMI_S\")" in text
            and "MinatoAllowsProjectedMargin" in text):
        text, n = re.subn(
            r"double lot = InpLots \* regime_lot;",
            "double lot = MinatoRegimeQuantity(InpLots, regime_lot);",
            text, count=1)
        if n: changes.append("rsemi_regime_quantity")
        text, n = re.subn(
            r"if \(sl_dist <= 0\.0 \|\| tp_dist <= 0\.0 \|\| lot <= 0\.0\) return;",
            "if (sl_dist <= 0.0 || tp_dist <= 0.0) return;\n   if (lot <= 0.0) return;",
            text, count=1)
        if n: changes.append("rsemi_zero_lot_refusal")
        text, n = re.subn(r"\n\s*lot = NormalizeLot\(lot\);", "", text, count=1)
        if n: changes.append("rsemi_remove_unsafe_normalization")

    # RSI2 MTF Bollinger: exact dual-path layout with raw regime sizing and
    # normalized volume. Keep this gated by the canonical filename plus the
    # distinctive signal/order structure; do not rewrite unrelated RSI EAs.
    if (source.name == "B1_1_RSI2_MTF_Bollinger.mq5"
            and "bool long_signal" in text
            and "bool short_signal" in text
            and "RegimeLotMult(regime)" in text
            and "trade.Buy(norm_lot" in text
            and "trade.Sell(norm_lot" in text):
        text, n = re.subn(
            r"double lot = InpLots \* RegimeLotMult\(regime\);\s*"
            r"if \(lot <= 0\.0\) lot = InpLots;",
            "double norm_lot = MinatoRegimeQuantity(InpLots, RegimeLotMult(regime));\n   if (norm_lot <= 0.0) return;",
            text, count=1)
        if n: changes.append("rsi2_regime_quantity")
        text, n = re.subn(r"\s*double norm_lot = NormalizeLot\(lot\);", "", text)
        if n: changes.append("rsi2_remove_unsafe_normalization")
        text, n = re.subn(
            r"if \(norm_lot > 0\.0 && trade\.Buy\(norm_lot, _Symbol, ask, sl, tp, \"RSI2_Rev_L\"\)\)",
            "if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, norm_lot, ask)) return;\n\n      if (trade.Buy(norm_lot, _Symbol, ask, sl, tp, \"RSI2_Rev_L\"))",
            text, count=1)
        if n: changes.append("rsi2_buy_projected_margin")
        text, n = re.subn(
            r"if \(norm_lot > 0\.0 && trade\.Sell\(norm_lot, _Symbol, bid, sl, tp, \"RSI2_Rev_S\"\)\)",
            "if (!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, norm_lot, bid)) return;\n\n      if (trade.Sell(norm_lot, _Symbol, bid, sl, tp, \"RSI2_Rev_S\"))",
            text, count=1)
        if n: changes.append("rsi2_sell_projected_margin")

    # Yang-Zhang RangeVol v1: exact dual-path layout. The source already
    # has closed-bar, TP, and projected-margin guards; replace only its raw
    # regime sizing so the canonical quantity contract is proven.
    if (source.name == "B1_1_Yang_Zhang_RangeVol_Filter_v1.mq5"
            and "double lot = InpLots * RegimeLotMult(r);" in text
            and "trade.Buy(lot, _Symbol, ask, sl, tp, \"YZRV_L\")" in text
            and "trade.Sell(lot, _Symbol, bid, sl, tp, \"YZRV_S\")" in text
            and "MinatoAllowsProjectedMargin" in text):
        text, n = re.subn(
            r"double lot = InpLots \* RegimeLotMult\(r\);",
            "double lot = MinatoRegimeQuantity(InpLots, RegimeLotMult(r));",
            text, count=1)
        if n: changes.append("yang_zhang_regime_quantity")
        text, n = re.subn(
            r"(double lot = MinatoRegimeQuantity\(InpLots, RegimeLotMult\(r\)\);)",
            r"\1\n   if (lot <= 0.0) return;",
            text, count=1)
        if n: changes.append("yang_zhang_explicit_zero_quantity_guard")
        text, n = re.subn(r"\n\s*lot = NormalizeLot\(lot\);", "", text, count=1)
        text = text.replace("if (sl_dist <= 0.0 || tp_dist <= 0.0 || lot <= 0.0) return;", "if (sl_dist <= 0.0 || tp_dist <= 0.0) return;")
        if n: changes.append("yang_zhang_remove_unsafe_normalization")

    # Turtle Channel 20/55: convert its implicit bar-count throttle and raw
    # regime/order sizing into the canonical contracts without changing signals.
    if (source.name == "B1_1_Turtle_Channel_System_20_55.mq5"
            and "trade.Buy(lots, _Symbol, ask, sl, tp, \"Turtle_Long\")" in text
            and "trade.Sell(lots, _Symbol, bid, sl, tp, \"Turtle_Short\")" in text):
        # NewBarClosed is supplied by minato_base_score_v2.mqh; never redefine it.
        text, n = re.subn(
            r"bool NewBarClosed\(\)\s*\{[\s\S]*?\n\}\s*\n\s*",
            "", text, count=1)
        if n: changes.append("turtle_remove_duplicate_closed_bar_helper")
        text, n = re.subn(
            r"   int bars = Bars\(_Symbol, PERIOD_CURRENT\);\s*\n"
            r"   if \(bars <= 0\) return;\s*\n"
            r"   if \(g_prevBars == bars\) return;\s*\n"
            r"   g_prevBars = bars;",
            "   if (!NewBarClosed()) return;", text, count=1)
        if n: changes.append("turtle_closed_bar_gate")
        text = text.replace("double lot = MinatoRegimeQuantity(InpBaseLot, RegimeLotMult(reg));\n   if (lot <= 0.0) return;", "double lot = LotForRegime(reg);")
        changes.append("turtle_regime_quantity")
        text = text.replace("double tp  = ask + tpPrice;\n      double lots = NormalizeLot(lot);\n      if (lots > 0) {", "double tp  = MinatoTakeProfit(ask, tpPrice, 1.0, true);\n      tp = NormalizeDouble(tp, _Digits);\n      double lots = MinatoRegimeQuantity(lot, 1.0);\n      if (lots <= 0.0) return;\n      lots = NormalizeLot(lots);\n      if (lots > 0) {")
        text = text.replace("if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lot, ask)) return;", "if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lots, ask)) return;")
        text = text.replace("double tp  = bid - tpPrice;\n      double lots = NormalizeLot(lot);\n      if (lots > 0) {", "double tp  = MinatoTakeProfit(bid, tpPrice, 1.0, false);\n      tp = NormalizeDouble(tp, _Digits);\n      double lots = MinatoRegimeQuantity(lot, 1.0);\n      if (lots <= 0.0) return;\n      lots = NormalizeLot(lots);\n      if (lots > 0) {")
        text = text.replace("if (!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, lot, bid)) return;", "if (!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, lots, bid)) return;")
        changes.append("turtle_order_quantity_tp_margin_contract")
    # bar with g_prevBars, but the verifier requires the canonical explicit
    # NewBarClosed() contract. This is a semantics-preserving refactor: retain
    # the exact bar-count behavior and only expose it through the approved gate.
    if (source.name == "B1_1_AroonTrendStrength_v1.mq5"
            and "if (g_prevBars == bars) return;" in text
            and "g_prevBars = bars;" in text
            and "double AroonUpClosed" in text
            and "double AroonDownClosed" in text):
        text, n = re.subn(
            r"   int bars = Bars\(_Symbol, PERIOD_CURRENT\);\s*\n"
            r"   if \(bars <= 0\) return;\s*\n"
            r"   if \(g_prevBars == bars\) return;\s*\n"
            r"   g_prevBars = bars;",
            "   if (!NewBarClosed()) return;",
            text, count=1)
        if n: changes.append("aroon_canonical_closed_bar_gate")

    # SuperTrend v2 legacy contract: trend-change strategy with one Buy/Sell
    # pair. Add the canonical live margin kill and closed-bar gate while
    # preserving its signal logic; the order paths already use `lot` and the
    # shared projected-margin helper.
    if (source.name == "B1_1_SuperTrend.mq5"
            and "void OnTick()" in text
            and "MinatoV2ObserveTick();" in text
            and "double lot = MinatoRegimeQuantity(InpLots, 1.0);" in text):
        if "InpMarginKillLevel" not in text:
            text, n = re.subn(
                r"(input double\s+InpLots\s*=\s*[^;]+;[^\r\n]*)",
                r"\1\ninput double InpMarginKillLevel = 120.0;",
                text, count=1)
            if n: changes.append("supertrend_margin_input")
        if "NewBarClosed()" not in text:
            text, n = re.subn(r"if \(!IsNewBar\(\)\) return;", "if (!NewBarClosed()) return;", text, count=1)
            if n: changes.append("supertrend_closed_bar")
        if "double margin_level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);" not in text:
            text, n = re.subn(
                r"(MinatoV2ObserveTick\(\);\s*\n)",
                r"\1\n   double margin_level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);\n   if (margin_level > 0.0 && margin_level < InpMarginKillLevel)\n   {\n      if (PositionSelect(_Symbol)) trade.PositionClose(_Symbol);\n      return;\n   }\n",
                text, count=1)
            if n: changes.append("supertrend_live_margin_kill")


        text, n = re.subn(
            r"double sl = NormalizeDouble\(ask \* \(1\.0 - InpStopLossPct\), digits\);\s*\n\s*double tp = NormalizeDouble\(ask \* \(1\.0 \+ InpTakeProfitPct\), digits\);",
            "double sl = NormalizeDouble(ask * (1.0 - InpStopLossPct), digits);\n      double tp_dist = ask * InpTakeProfitPct;\n      double tp = MinatoTakeProfit(ask, tp_dist, 1.0, true);\n      tp = NormalizeDouble(tp, digits);",
            text, count=1)
        if n: changes.append("supertrend_buy_take_profit")
        text, n = re.subn(
            r"double sl = NormalizeDouble\(bid \* \(1\.0 \+ InpStopLossPct\), digits\);\s*\n\s*double tp = NormalizeDouble\(bid \* \(1\.0 - InpTakeProfitPct\), digits\);",
            "double sl = NormalizeDouble(bid * (1.0 + InpStopLossPct), digits);\n      double tp_dist = bid * InpTakeProfitPct;\n      double tp = MinatoTakeProfit(bid, tp_dist, 1.0, false);\n      tp = NormalizeDouble(tp, digits);",
            text, count=1)
        if n: changes.append("supertrend_sell_take_profit")

        text = text.replace("if (trade.Buy(lot, _Symbol, ask, sl, tp, comment))", "bool buy_ok = trade.Buy(lot, _Symbol, ask, sl, tp, comment);\n      if (buy_ok)")
        text = text.replace("if (trade.Sell(lot, _Symbol, bid, sl, tp, comment))", "bool sell_ok = trade.Sell(lot, _Symbol, bid, sl, tp, comment);\n      if (sell_ok)")
        changes.append("supertrend_explicit_order_results")

    # Generic order-path quantity contract: any family using normalized raw
    # regime lots must pass the effective volume through Minato first. This is
    # signature-gated and idempotent; it does not alter signal selection.
    raw_lots = len(re.findall(r"double lots = NormalizeLot\(lot\);", text))
    if raw_lots:
        text = text.replace(
            "double lots = NormalizeLot(lot);",
            "double lots = MinatoRegimeQuantity(lot, 1.0);\n"
            "      if (lots <= 0.0) return;\n"
            "      lots = NormalizeLot(lots);")
        text = text.replace("MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lot, ask)", "MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lots, ask)")
        text = text.replace("MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, lot, bid)", "MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, lots, bid)")
        changes.append(f"generic_order_quantity_contract_{raw_lots}_paths")

    text, n = re.subn(
        r"double\s+tp\s*=\s*ask\s*\+\s*([A-Za-z_]\w*)\s*;",
        r"double tp = MinatoTakeProfit(ask, \1, 1.0, true);\n      tp = NormalizeDouble(tp, _Digits);",
        text)
    if n: changes.append("generic_long_take_profit_contract")
    text, n = re.subn(
        r"double\s+tp\s*=\s*bid\s*-\s*([A-Za-z_]\w*)\s*;",
        r"double tp = MinatoTakeProfit(bid, \1, 1.0, false);\n      tp = NormalizeDouble(tp, _Digits);",
        text)
    if n: changes.append("generic_short_take_profit_contract")

    # Replace direct ATR TP expressions in the two standard order branches.
    text, n = re.subn(r"double tp = NormalizeDouble\(ask\s*\+\s*tp_dist,\s*_Digits\);", "double tp = MinatoTakeProfit(ask, tp_dist, 1.0, true);\n      tp = NormalizeDouble(tp, _Digits);", text)
    if n: changes.append("long_take_profit")
    text, n = re.subn(r"double tp = NormalizeDouble\(bid\s*-\s*tp_dist,\s*_Digits\);", "double tp = MinatoTakeProfit(bid, tp_dist, 1.0, false);\n      tp = NormalizeDouble(tp, _Digits);", text)
    if n: changes.append("short_take_profit")
    # Add projected margin checks immediately before raw Buy/Sell calls when absent.
    if "MinatoAllowsProjectedMargin" not in text:
        text, n = re.subn(r"(?m)^(\s*)(if \(trade\.Buy\()", r"\1if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lot, ask)) return;\n\1\2", text)
        if n: changes.append("buy_projected_margin")
        text, n = re.subn(r"(?m)^(\s*)(if \(trade\.Sell\()", r"\1if (!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, lot, bid)) return;\n\1\2", text)
        if n: changes.append("sell_projected_margin")
    if text != original:
        backup = source.with_name(source.name + ".bak.stage2-auto-repair-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        shutil.copy2(source, backup)
        source.write_text(text, encoding="utf-8", newline="")
        return {"changed": True, "changes": changes, "backup": str(backup), "source_sha256": sha256(source)}
    return {"changed": False, "changes": [], "source_sha256": sha256(source)}


def main() -> int:
    handoff = Path(sys.argv[1])
    data = json.loads(handoff.read_text(encoding="utf-8-sig"))
    result = repair(Path(data["source"]))
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
