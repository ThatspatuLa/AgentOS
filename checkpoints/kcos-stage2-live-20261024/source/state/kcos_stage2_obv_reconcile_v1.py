"""Pure source reconciliation for the approved body-signed-volume strategy.

The executor authenticates the Stage 1 brief and owns staging/compilation/commit.
This module has no filesystem, scheduler, model, or MT5 side effects. Only the
reviewed legacy implementation shape is transformed; unknown variants are held.
"""
from __future__ import annotations

import hashlib
import re

METHOD = "body_signed_volume_bounded_rolling_v1"
_OLD = {
    "BuildPrecomputedArrays": "89ba8f0ebaad33c45765b5341558bb2468936e308b6ac27f4c4cc2ed5697046c",
    "RefreshLatestBar": "0f05f67aad1fc75a0b7682d2540ec1178fb6a8f7235c1c41af9b8d637d2c9117",
    "OnInit": "43212f2df7f128827dd8f8c323dc143f70f2f3c365904c0687283ddb7f0ab8ff",
    "OnTick": "b0d94b2c50abcacadf323d15968d05b96b1a843e4761fe9a0883e3f7bc846b2b",
    "CalcRegime": "489c17c16928d40524f39be8e003e9c987a72186cea6b3d205fe3cae0f15b26e",
    "FindMaxHigh": "249cc73116b36c361169a1208df7595d0b177bd3394dfe264c894201f7978c86",
    "FindMinLow": "39b780e747abcbcb3fde5329e1150b648d86acb46d7a1db19e9eea0c9a166e60",
}

# The finite window proof depends on BOTH searches spanning at most lookback
# bars from the starting pivot. Hold changed helpers instead of guessing their
# revised data requirements or silently truncating a wider search.
_PIVOT_SEARCH = {
    "FindPrevPivotHigh": "93ed4ec048a8f269dbf361b893cba5bb683f576f8132d2450b468390502711cf",
    "FindPrevPivotLow": "6d15dbaea691b6a0f9fe33d7547e32fbd765e0514b586bc76b6b7e69b4f267e8",
}

BUILD = r'''bool BuildPrecomputedArrays()
{
   // A common cumulative prefix cancels in every compared OBV pivot pair.
   // Keep both lookback searches plus pivot neighbours, never full history.
   if (InpObvDivLookback < 1 || InpPivotStrength < 1 ||
       InpEmaTrendPeriod < 1 || InpAtrPeriod < 1) return false;
   int window = MathMax(100, 2 * InpObvDivLookback + 2 * InpPivotStrength + 8);
   window = MathMax(window, MathMax(InpEmaTrendPeriod + 3, InpAtrPeriod + 3));
   if (Bars(_Symbol, PERIOD_CURRENT) < window) return false;
   int n = window;
   double o[], c[], hi_s[], lo_s[];
   long v[];
   ArraySetAsSeries(o, false);
   ArraySetAsSeries(c, false);
   ArraySetAsSeries(v, false);
   ArraySetAsSeries(hi_s, true);
   ArraySetAsSeries(lo_s, true);
   // All copies must describe the same window before any indexed access.
   if (CopyOpen(_Symbol, PERIOD_CURRENT, 0, n, o) != n) return false;
   if (CopyClose(_Symbol, PERIOD_CURRENT, 0, n, c) != n) return false;
   if (CopyTickVolume(_Symbol, PERIOD_CURRENT, 0, n, v) != n) return false;
   if (CopyHigh(_Symbol, PERIOD_CURRENT, 0, n, hi_s) != n) return false;
   if (CopyLow(_Symbol, PERIOD_CURRENT, 0, n, lo_s) != n) return false;
   ArraySetAsSeries(g_obv, false);
   if (ArrayResize(g_obv, n) != n) return false;
   long cum = 0;
   for (int i = 0; i < n; i++)
   {
      double op = o[i];
      double cl = c[i];
      long vt = v[i];
      if (cl > op) cum += vt;
      else if (cl < op) cum -= vt;
      g_obv[i] = (double)cum;
   }
   // Physical oldest-first storage, logical shift access. No second reversal.
   ArraySetAsSeries(g_obv, true);
   // These ordinary arrays are explicitly keyed by shift, not copy destinations.
   ArraySetAsSeries(g_is_pivot_high, false);
   ArraySetAsSeries(g_is_pivot_low, false);
   if (ArrayResize(g_is_pivot_high, n) != n) return false;
   if (ArrayResize(g_is_pivot_low, n) != n) return false;
   ArrayInitialize(g_is_pivot_high, false);
   ArrayInitialize(g_is_pivot_low, false);
   int len = InpPivotStrength;
   for (int shift = len + 1; shift < n - len - 1; shift++)
   {
      bool is_h = true, is_l = true;
      double pivot_h = hi_s[shift];
      double pivot_l = lo_s[shift];
      for (int j = 1; j <= len; j++)
      {
         if (hi_s[shift - j] > pivot_h) is_h = false;
         if (hi_s[shift + j] > pivot_h) is_h = false;
         if (lo_s[shift - j] < pivot_l) is_l = false;
         if (lo_s[shift + j] < pivot_l) is_l = false;
      }
      g_is_pivot_high[shift] = is_h;
      g_is_pivot_low[shift] = is_l;
   }
   g_obv_size = n;
   return true;
}'''

REFRESH = r'''bool RefreshLatestBar()
{
   // OnTick calls this once behind NewBarClosed, even at a Bars-count plateau.
   return BuildPrecomputedArrays();
}'''

CHANGES = [
    "Replace full-history volume/pivot rebuild with bounded rolling window; retain body-signed increments and pivot comparison rules.",
    "Use chronological OBV writes with a single series flag, without manual reversal; pivot flags stay explicitly shift-keyed.",
    "Rebuild after every closed-bar gate, including fixed Bars counts, and stop that evaluation if refresh fails.",
    "Read exactly one closed EMA, ATR and close value; reject partial OHLCV, extrema and H1 copies.",
    "Sample 60 completed H1 bars from shift 1; preserve regime classifier, mappings, tracker, Minato and input declarations.",
    "Update obsolete complexity comments only; no signal, order, exit, sizing, grid or shared include changes.",
]


def _digest(value: str) -> str:
    return hashlib.sha256(re.sub(r"\s+", "", value).encode()).hexdigest()


def _function(source: str, name: str) -> tuple[int, int, str]:
    matches = list(re.finditer(r"\b(?:bool|int|void|ENUM_REGIME)\s+" + name + r"\s*\([^)]*\)\s*\{", source))
    if len(matches) != 1:
        raise ValueError("missing_or_duplicate_function:" + name)
    match = matches[0]
    depth, end = 1, match.end()
    while depth and end < len(source):
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    if depth:
        raise ValueError("unterminated_function:" + name)
    return match.start(), end, source[match.start():end]


def _sub_once(text: str, pattern: str, replacement: str) -> str:
    result, count = re.subn(pattern, lambda _: replacement, text)
    if count != 1:
        raise ValueError("unsupported_source_shape:" + pattern)
    return result


def _transform(name: str, body: str) -> str:
    if name == "BuildPrecomputedArrays":
        return BUILD
    if name == "RefreshLatestBar":
        return REFRESH
    if name == "OnInit":
        return body.replace("O(bars) at init; O(1) per bar thereafter", "O(window) at init and each closed bar")
    if name == "OnTick":
        body = body.replace("Refresh precomputed arrays if bar count grew", "Refresh bounded arrays on every closed bar")
        body = _sub_once(body, r"(?m)^\s*RefreshLatestBar\(\);", "   if (!RefreshLatestBar()) return;")
        body = _sub_once(body, r"CopyBuffer\(g_ema_handle,\s*0,\s*1,\s*3,\s*g_ema_buf\)\s*<=\s*0", "CopyBuffer(g_ema_handle, 0, 1, 1, g_ema_buf) != 1")
        body = _sub_once(body, r"double ema1 = g_ema_buf\[1\];", "double ema1 = g_ema_buf[0];")
        body = _sub_once(body, r"CopyBuffer\(g_atr_handle,\s*0,\s*1,\s*1,\s*g_atr_buf\)\s*<=\s*0", "CopyBuffer(g_atr_handle, 0, 1, 1, g_atr_buf) != 1")
        return _sub_once(body, r"CopyClose\(_Symbol,\s*PERIOD_CURRENT,\s*1,\s*2,\s*g_close_buf\)\s*<=\s*0", "CopyClose(_Symbol, PERIOD_CURRENT, 1, 1, g_close_buf) != 1")
    if name == "CalcRegime":
        body = _sub_once(body, r"h1_total < 60", "h1_total < 61")
        for kind, array in (("High", "h1high"), ("Low", "h1low"), ("Close", "h1close")):
            body = _sub_once(body, r"Copy" + kind + r"\s*\(_Symbol,\s*PERIOD_H1,\s*0,\s*60,\s*" + array + r"\)\s*<=\s*0", f"Copy{kind}(_Symbol, PERIOD_H1, 1, 60, {array}) != 60")
        return body
    kind, array = ("High", "highs") if name == "FindMaxHigh" else ("Low", "lows")
    return _sub_once(body, r"Copy" + kind + r"\(_Symbol,\s*PERIOD_CURRENT,\s*from_shift \+ 1,\s*lookback,\s*" + array + r"\)\s*<=\s*0", f"Copy{kind}(_Symbol, PERIOD_CURRENT, from_shift + 1, lookback, {array}) != lookback")


def _approved_intent(brief: dict) -> bool:
    """The caller authenticates the brief; do not infer strategy from EA names."""
    intent = brief.get("resolved_intent", brief)
    return (intent.get("volume_sign_rule") == "close_vs_open"
            and intent.get("signal_shift") == 1
            and intent.get("h1_regime_shift") == 1)


def reconcile_obv_source(source: str, brief: dict) -> dict:
    result = {"status": "not_applicable", "source_text": source, "method": METHOD,
              "changes": [], "evidence": {"source_before_sha256": hashlib.sha256(source.encode()).hexdigest()}}
    if not isinstance(brief, dict) or not _approved_intent(brief):
        result["reason"] = "resolved_body_signed_closed_bar_intent_required"
        return result
    try:
        for name, expected in _PIVOT_SEARCH.items():
            if _digest(_function(source, name)[2]) != expected:
                raise ValueError("unsupported_pivot_search_shape:" + name)
        regions = {name: _function(source, name) for name in _OLD}
        # Derive reviewed output hashes from the immutable fixture signatures at
        # module build time, rather than accepting a marker as proof of shape.
        states = []
        for name, (_, _, body) in regions.items():
            digest = _digest(body)
            if digest == _OLD[name]:
                states.append("old")
            elif digest == _NEW[name]:
                states.append("new")
            else:
                raise ValueError("unsupported_function_shape:" + name)
        if all(state == "new" for state in states):
            result.update(status="already_reconciled", reason="reviewed_bounded_shape_present")
            return result
        if any(state != "old" for state in states):
            raise ValueError("mixed_reconciliation_state")
        revised = source
        newline = "\r\n" if "\r\n" in source else "\n"
        for name, (start, end, body) in sorted(regions.items(), key=lambda item: item[1][0], reverse=True):
            transformed = _transform(name, body)
            if name in {"BuildPrecomputedArrays", "RefreshLatestBar"}:
                transformed = transformed.replace("\n", newline)
            revised = revised[:start] + transformed + revised[end:]
        revised = revised.replace("// size of precomputed arrays (== Bars(_Symbol, PERIOD_CURRENT) at last build)", "// bounded rolling window size")
        revised = revised.replace("// Pre-compute OBV and pivot flags for the entire visible history.", "// Compute body-signed volume and pivots for a bounded rolling window.")
        revised = revised.replace("// Called once at OnInit, and on every new bar to refresh the current bar.", "// Called once at OnInit and once per closed chart bar.")
        revised = revised.replace("// Memory: O(bars) doubles + bools. For M5 over 5y (~250k bars) ~2 MB.", "// Work and memory: O(window), independent of total tester history.")
        result.update(status="reconciled", source_text=revised, changes=list(CHANGES))
        result["evidence"].update(source_after_sha256=hashlib.sha256(revised.encode()).hexdigest(),
                                  complexity="O(window) per closed bar; O(window) memory",
                                  equivalence="Common cumulative prefix cancels in compared pivot-volume differences; index corrections implement Stage 1 intent, not legacy buggy output parity.")
    except (ValueError, KeyError) as exc:
        result.update(status="held", reason=str(exc))
    return result


# Reviewed outputs for the exact legacy function shapes. No runtime file reads.
_NEW = {'BuildPrecomputedArrays': '49e373f4dcf812e40095ee41794a149c54fc4f77d11a4e5b675791314585b0ce', 'RefreshLatestBar': '69c6b028a76b0429a56ed722b6e9b4519dd17f68f6a8354d96e9317f822ef809', 'OnInit': 'fc35ad77bf775b1637cfc613b3534d21b969b5cf2ff35b8bf1f72c872e9efd19', 'OnTick': 'd4a119c9510fa988e18cb58d973095405a6ed21af00315121fa3a0559367a952', 'CalcRegime': '8f810f1002fda98644b4fff5103d203335275c2982df6cf4e0e815e6fc235d50', 'FindMaxHigh': '4c0e9b5fa2c00e4ca82955b39ea4aca1cd71a27845e73c2117ca53e477ae361f', 'FindMinLow': 'caeecbf554c37273c1591200c2c9d1d8c1cbcaf154cedd08369293dc70a487cd'}
