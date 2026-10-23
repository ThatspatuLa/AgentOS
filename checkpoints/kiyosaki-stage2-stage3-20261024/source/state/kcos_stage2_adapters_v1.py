"""Small source-family adapter registry for Stage 2 repair."""
from pathlib import Path

def family(source: Path, text: str) -> str:
    name = source.name.casefold()
    if "elder" in name: return "elder_control_contract"
    if any(x in name for x in ("williamsr", "vwma", "vwap")): return "standard_regime_order"
    if "bollinger" in name: return "bollinger_closed_bar"
    if any(x in name for x in ("asianrange", "camarilla")): return "session_reversal"
    return "generic_minato_ea"

def constraints(adapter: str) -> list[str]:
    return {
        "elder_control_contract": [
            "Preserve the authenticated Elder/MACD entry and exit signals and the Stage 1 input surface.",
            "The authenticated Stage 1 execution contract requires one selected regime state but records that the source has no selector. Use the existing project include <kiyosaki_regime.mqh> and CKiyosakiRegime API as the selector; do not invent a new classifier or thresholds. The exact API is bool Init(string symbol, ENUM_TIMEFRAMES tf), void Deinit(), and ENUM_REGIME Calc(const double &high[], const double &low[], const double &close[], int totalBars). The enum values are REGIME_GREED=1, REGIME_NEUTRAL=0, REGIME_FEAR=-1, and REGIME_EXTREME_FEAR=-2.",
            "Use the closed-bar regime result to select exactly one GREED, NEUTRAL, FEAR, or EXTREME_FEAR lot/SL/TP triplet from the twelve existing InpRegime_* inputs. Apply the selected lot multiplier through MinatoRegimeQuantity(InpLots, multiplier), and apply the selected SL/TP multipliers through the existing Minato helpers.",
            "Keep every existing signal declaration, predicate, entry guard, and exit path byte-for-byte equivalent. Preserve the existing [1]/[2] signal indices and MACD slope equation. Implement these exact MQL5 helper signatures: bool CopyClosedBuffer(const int handle, const int buffer, double &target[]), bool CopyClosedHigh(double &target[]), and bool CopyClosedLow(double &target[]). In each helper declare double shift1[1] and shift2[1], read shifts 1 and 2 separately with count 1, return false immediately if either CopyBuffer/CopyHigh/CopyLow result is less than 1, then assign shift1[0] to target[1] and shift2[0] to target[2] and return true. Call CopyClosedBuffer(handle, buffer, target) with exactly three arguments; never pass a shift argument. Never multi-copy directly into the series arrays used by those signal indices. Keep ATR on shift 1. Regime history is separate and must use only completed bars.",
            "For regime history, declare fixed 64-element g_regime_high, g_regime_low, and g_regime_close arrays and implement void UpdateRegimeSeries() with exactly for(int shift=1; shift<64; shift++) assigning iHigh/iLow/iClose(_Symbol, PERIOD_CURRENT, shift) to the same array index. Call that helper before CKiyosakiRegime.Calc. Do not add CopyRegimeHistory or read shift 0.",
            "Preserve InpLots, all Stage 1 input names/types/defaults/ranges, the declared entry margin threshold, the separate 120 percent emergency margin guard, and the existing strategy signals. Repair only named failed Stage 2 checks; never edit Stage 1 spec or completion evidence.",
        ],
        "standard_regime_order": ["preserve existing signals", "repair quantity before normalization and both order paths"],
        "bollinger_closed_bar": ["preserve prior-bar calculations", "never use active signal bar"],
        "session_reversal": ["preserve session window", "retain closed-bar and margin guards"],
    }.get(adapter, ["preserve signal logic", "change only the failed contract"])
