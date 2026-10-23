#!/usr/bin/env python3
"""Stage 3 duration admission/reduction policy.

Stage 3 wide grids preserve their authenticated parameter bounds. The per-timeframe
budget must allow a forecasted wide run to finish; do not silently switch to a
range-narrowing profile when a wide optimization is merely expensive.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass

BUDGET_SECONDS = 9 * 60 * 60
# Generic zero-token MT5 Tester checkpoint. The visible second time value is
# the tester's estimated total duration for the active timeframe.
UI_ETA_CHECKPOINT_SECONDS = 10 * 60
UI_ETA_SKIP_SECONDS = 4 * 60 * 60
# Do not stop a wide grid from a noisy startup sample. The throughput estimate
# becomes eligible only after both elapsed-time and completed-pass minimums.
MIN_PROJECTION_ELAPSED_SECONDS = 300
MIN_PROJECTION_PASSES = 200
REDUCED_REGIME_STOP = 3.0
REDUCED_INTERVALS = 12
EXCLUDED = {"InpMagicNumber", "InpSlippagePts", "InpTradeComment", "InpCsvPath"}


def parse_mt5_elapsed_estimate(text: str) -> dict | None:
    """Parse MT5's bottom-right ``elapsed / estimated total`` clock text."""
    match = re.search(
        r"(?<!\d)(\d+):(\d{2}):(\d{2})\s*/\s*(\d+):(\d{2}):(\d{2})(?!\d)",
        str(text or ""),
    )
    if not match:
        return None
    elapsed_h, elapsed_m, elapsed_s, total_h, total_m, total_s = map(int, match.groups())
    if elapsed_m >= 60 or elapsed_s >= 60 or total_m >= 60 or total_s >= 60:
        return None
    return {
        "elapsed_seconds": elapsed_h * 3600 + elapsed_m * 60 + elapsed_s,
        "estimated_total_seconds": total_h * 3600 + total_m * 60 + total_s,
        "elapsed_text": f"{elapsed_h:02d}:{elapsed_m:02d}:{elapsed_s:02d}",
        "estimated_total_text": f"{total_h:02d}:{total_m:02d}:{total_s:02d}",
    }


def eta_cutoff_triggered(elapsed_seconds: float, estimated_total_seconds: float,
                         checkpoint_seconds: int = UI_ETA_CHECKPOINT_SECONDS,
                         limit_seconds: int = UI_ETA_SKIP_SECONDS) -> bool:
    """Skip only after ten elapsed minutes and an ETA above four hours."""
    try:
        elapsed, estimate = float(elapsed_seconds), float(estimated_total_seconds)
    except (TypeError, ValueError):
        return False
    return elapsed >= checkpoint_seconds and estimate > limit_seconds


def validate_eta_skip(projection: dict, timing: dict, stop: dict) -> bool:
    """Validate a generic, zero-token MT5 ETA skip from persisted UI evidence."""
    if not all(isinstance(item, dict) for item in (projection, timing, stop)):
        return False
    try:
        elapsed = float(timing.get("elapsed_seconds"))
        estimate = float(timing.get("estimated_total_seconds"))
        projected_elapsed = float(projection.get("elapsed_seconds"))
        projected_estimate = float(projection.get("estimated_total_seconds"))
    except (TypeError, ValueError):
        return False
    checkpoint = projection.get("checkpoint_seconds")
    try:
        checkpoint = int(checkpoint)
    except (TypeError, ValueError):
        return False
    # Keep exact historical evidence readable while requiring the current
    # policy for new events. Old events are immutable and record 240 seconds.
    valid_checkpoint = checkpoint in {240, UI_ETA_CHECKPOINT_SECONDS}
    return (
        timing.get("source") == "authenticated_bound_tester_gui_bottom_right"
        and projection.get("source") == timing.get("source")
        and valid_checkpoint
        and elapsed >= checkpoint
        and estimate > UI_ETA_SKIP_SECONDS
        and projected_elapsed >= checkpoint
        and projected_estimate > UI_ETA_SKIP_SECONDS
        and abs(projected_estimate - estimate) <= 1
        and stop.get("stopped") is True
    )

@dataclass(frozen=True)
class Projection:
    elapsed_seconds: float
    completed_passes: int
    total_passes: int
    projected_seconds: float | None
    sample_qualified: bool
    over_budget: bool


def project(elapsed_seconds: float, completed_passes: int, total_passes: int,
            budget_seconds: float = BUDGET_SECONDS) -> Projection:
    elapsed_seconds = float(elapsed_seconds)
    completed_passes = int(completed_passes)
    total_passes = int(total_passes)
    projected = None
    if elapsed_seconds > 0 and completed_passes > 0 and total_passes > 0:
        projected = elapsed_seconds * total_passes / completed_passes
    sample_qualified = (elapsed_seconds >= MIN_PROJECTION_ELAPSED_SECONDS
                        and completed_passes >= MIN_PROJECTION_PASSES)
    return Projection(elapsed_seconds, completed_passes, total_passes, projected,
                      sample_qualified,
                      sample_qualified and projected is not None
                      and projected > float(budget_seconds))


def _num(value: str) -> float:
    return float(value.strip().strip('"'))


def _fmt(value: float) -> str:
    return str(int(round(value))) if float(value).is_integer() else format(value, ".12g")


def _reduced_bounds(name: str, lo: float, step: float, hi: float,
                    regime_bounds: dict[str, tuple[float, float]] | None = None) -> tuple[float, float, float]:
    if name.startswith("InpRegime_"):
        source_lo, source_hi = (regime_bounds or {}).get(name, (0.0, REDUCED_REGIME_STOP))
        return max(lo, source_lo), 0.25, min(REDUCED_REGIME_STOP, hi, source_hi)
    span = max(0.0, hi - lo)
    if span <= 0 or step <= 0:
        return lo, step, hi
    # Preserve the legal lower bound and reduce the number of intervals to 12.
    new_step = span / REDUCED_INTERVALS
    if all(float(v).is_integer() for v in (lo, step, hi)):
        new_step = max(1.0, math.ceil(new_step))
        new_hi = min(hi, lo + REDUCED_INTERVALS * new_step)
        return lo, new_step, new_hi
    return lo, new_step, min(hi, lo + REDUCED_INTERVALS * new_step)


def reduce_ini(rendered: str, regime_bounds: dict[str, tuple[float, float]] | None = None) -> str:
    """Return a reduced, still-optimized TesterInputs surface."""
    rows = []
    for raw in rendered.splitlines():
        match = re.match(r"^(\s*(Inp[A-Za-z0-9_]+)\s*=)([^\r\n]+)$", raw)
        if not match:
            rows.append(raw)
            continue
        fields = match.group(3).split("||")
        name = match.group(2)
        if len(fields) != 5 or fields[-1].upper() != "Y" or name in EXCLUDED:
            rows.append(raw)
            continue
        try:
            default, lo, step, hi = (_num(v) for v in fields[:4])
        except ValueError:
            rows.append(raw)
            continue
        new_lo, new_step, new_hi = _reduced_bounds(name, lo, step, hi, regime_bounds)
        # Keep the authenticated default inside the reduced rectangle by
        # centering the reduced window around it when the legal bounds allow.
        if default < new_lo or default > new_hi:
            interval_count = min(REDUCED_INTERVALS, max(1, int(math.floor((hi - lo) / new_step))))
            new_lo = max(lo, min(default - (interval_count / 2) * new_step, hi - interval_count * new_step))
            new_hi = min(hi, new_lo + interval_count * new_step)
        rows.append(f"{match.group(1)}{_fmt(default)}||{_fmt(new_lo)}||{_fmt(new_step)}||{_fmt(new_hi)}||Y")
    return "\n".join(rows)
