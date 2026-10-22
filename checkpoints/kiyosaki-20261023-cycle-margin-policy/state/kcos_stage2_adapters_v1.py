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
            "The authenticated Stage 1 execution contract requires one selected regime state but records that the source has no selector. Use the existing project include <kiyosaki_regime.mqh> and CKiyosakiRegime API as the selector; do not invent a new classifier or thresholds.",
            "Use the closed-bar regime result to select exactly one GREED, NEUTRAL, FEAR, or EXTREME_FEAR lot/SL/TP triplet from the twelve existing InpRegime_* inputs. Apply the selected lot multiplier through MinatoRegimeQuantity(InpLots, multiplier), and apply the selected SL/TP multipliers through the existing Minato helpers.",
            "Keep every existing signal declaration, predicate, entry guard, and exit path byte-for-byte equivalent. Preserve the existing [1]/[2] signal indices and MACD slope equation. Use bounded CopyClosedBuffer, CopyClosedHigh, and CopyClosedLow helpers: each must read completed shifts 1 and 2 separately, assign shift 1 into target[1] and shift 2 into target[2], and return false when either read is short. Never multi-copy directly into the series arrays used by those signal indices. Keep ATR on shift 1. Regime history is separate and must use only completed bars.",
            "Preserve InpLots, all Stage 1 input names/types/defaults/ranges, the declared entry margin threshold, the separate 120 percent emergency margin guard, and the existing strategy signals. Repair only named failed Stage 2 checks; never edit Stage 1 spec or completion evidence.",
        ],
        "standard_regime_order": ["preserve existing signals", "repair quantity before normalization and both order paths"],
        "bollinger_closed_bar": ["preserve prior-bar calculations", "never use active signal bar"],
        "session_reversal": ["preserve session window", "retain closed-bar and margin guards"],
    }.get(adapter, ["preserve signal logic", "change only the failed contract"])
