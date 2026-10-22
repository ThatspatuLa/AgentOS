"""Deterministic, side-effect-free KCOS Stage 4/5 policy helpers.

These functions intentionally do not read/write live state, launch MT5, or
dispatch cron jobs. Callers must supply verified, run-scoped results.
"""

from __future__ import annotations

import math
import re
from statistics import median
from typing import Iterable

from parse_mt5_opt_v2 import effective_grid_stop


MARGIN_FLOOR_PCT = 120.0
STAGE5_FORWARD_CF_RATIO = 0.50
STAGE5_MIN_TPM = 35.0
STAGE5_MAX_DRAWDOWN_PCT = 25.0
STAGE5_MIN_FORWARD_TRADE_RATIO = 0.50
NARROW_FALLBACK_RUNS = 3
MIN_REFINEMENT_INTERVALS = 15
MAX_REFINEMENT_INTERVALS = 20
TARGET_REFINEMENT_INTERVALS = 16
MIN_STAGE45_MT5_VALUES = 15
MAX_STAGE45_MT5_VALUES = 20
MIN_STAGE45_STRONG_MT5_VALUES = 8
TARGET_STAGE45_MT5_VALUES = 16


def _finite(value: float, name: str) -> float:
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


def _ceil_grid(value: float, origin: float, step: float) -> int:
    return math.ceil((value - origin) / step - 1e-12)


def _floor_grid(value: float, origin: float, step: float) -> int:
    return math.floor((value - origin) / step + 1e-12)


def _round_grid(value: float, origin: float, step: float) -> int:
    scaled = (value - origin) / step
    return math.floor(scaled + 0.5) if scaled >= 0 else math.ceil(scaled - 0.5)


def _clean_step(step: float) -> float:
    """Normalize decimal steps without changing authenticated integer grids."""
    step = _finite(step, "step")
    if step.is_integer():
        return step
    scale = 10 ** math.floor(math.log10(step))
    bases = (1.0, 2.0, 2.5, 5.0, 10.0)
    return min((base * scale for base in bases), key=lambda value: abs(value - step))


def select_top_records(records: Iterable[dict], parameter_names: Iterable[str],
                       limit: int = 50) -> dict:
    """Select top valid rows by margin-adjusted Minato, retaining cutoff ties.

    A valid row must be a mapping with finite ``minato_score`` and
    ``min_margin_pct`` and must not explicitly set ``valid`` to false. Margin
    below 120% gives effective score zero; exactly 120% keeps its score. Each
    requested input cluster is every observed value for that input in the
    selected rows, including every valid tie at the cutoff.
    """
    if limit <= 0:
        raise ValueError("limit must be positive")
    params = tuple(parameter_names)
    if len(set(params)) != len(params):
        raise ValueError("parameter_names must be unique")
    ranked: list[tuple[int, float, dict]] = []
    skipped_invalid = 0
    for index, row in enumerate(records):
        if not isinstance(row, dict) or row.get("valid", True) is False:
            skipped_invalid += 1
            continue
        try:
            score = _finite(row["minato_score"], "minato_score")
            min_margin = _finite(row["min_margin_pct"], "min_margin_pct")
        except (KeyError, TypeError, ValueError):
            skipped_invalid += 1
            continue
        if min_margin < MARGIN_FLOOR_PCT:
            score = 0.0
        ranked.append((index, score, row))
    if len(ranked) < limit:
        raise ValueError(
            f"only {len(ranked)} valid optimization records; need at least {limit}"
        )
    ranked.sort(key=lambda item: (-item[1], item[0]))
    cutoff = ranked[limit - 1][1]
    selected = [(index, score, row) for index, score, row in ranked if score >= cutoff]
    clusters: dict[str, list[float]] = {name: [] for name in params}
    selected_rows = []
    for index, score, row in selected:
        for name in params:
            if name not in row:
                raise ValueError(f"selected optimization record lacks input {name}")
            clusters[name].append(_finite(row[name], f"{name} value"))
        selected_rows.append({
            "source_index": index,
            "effective_minato_score": score,
            "record": row,
        })
    return {
        "action": "selected_top_records",
        "limit": limit,
        "valid_record_count": len(ranked),
        "selected_record_count": len(selected_rows),
        "skipped_invalid_count": skipped_invalid,
        "cutoff_effective_minato_score": cutoff,
        "selected_records": selected_rows,
        "input_clusters": clusters,
    }


def evaluate_input_range(
    *,
    start: float,
    stop: float,
    step: float,
    domain_min: float,
    domain_max: float,
    cluster: Iterable[float],
    centered_streak: int = 0,
    broad_run: bool = True,
    allow_first_pass_narrow: bool = False,
    require_current_range_in_domain: bool = True,
) -> dict:
    """Classify an input's best-result cluster and propose one range action.

    Centered ranges narrow to the smallest step-aligned interval that contains
    the full qualifying cluster plus one current step of padding on each side.
    This is deliberately data-derived rather than a fixed shrink percentage.
    If that interval cannot be smaller than the current interval, the function
    keeps the range broad for review instead of dropping qualifying results.
    """
    start = _finite(start, "start")
    stop = _finite(stop, "stop")
    # Stage 3 and prior Stage 4.5 INIs are authenticated grids. Preserve their
    # exact step while measuring cluster position; rounding can extend the
    # reachable stop and silently broaden a follow-up range.
    step = _finite(step, "step")
    new_streak = int(centered_streak)
    if new_streak < 0:
        raise ValueError("centered_streak cannot be negative")
    if not broad_run:
        # A narrow optimization interrupts the sequence of consecutive broad
        # runs required to establish centering.
        new_streak = 0
    if domain_min is None or domain_max is None:
        return _result("hold_unknown_domain", start, stop, new_streak,
                       "unknown", "authoritative valid input domain is missing")
    domain_min = _finite(domain_min, "domain_min")
    domain_max = _finite(domain_max, "domain_max")
    values = sorted(_finite(v, "cluster value") for v in cluster)
    if step <= 0 or start >= stop or domain_min >= domain_max:
        raise ValueError("range, domain, or step is invalid")
    # The incoming Stage 3 interval is already authenticated by its INI and
    # parser evidence. A movement envelope may be narrower than that interval
    # (for example, a positive-input floor), so only enforce containment when
    # the caller explicitly supplies a true hard domain.
    if require_current_range_in_domain and (start < domain_min or stop > domain_max):
        raise ValueError("current range exceeds its valid hard domain")
    if len(values) < 2:
        raise ValueError("at least two qualifying cluster results are required")
    reachable_stop = effective_grid_stop({"start": start, "step": step, "stop": stop})
    if values[0] < start - 1e-9 or values[-1] > reachable_stop + 1e-9:
        raise ValueError("qualifying cluster values must lie within the current range")

    # MT5 may evaluate the final step above a non-aligned configured stop.
    # Use that reachable endpoint for cluster-position analysis so valid OPT
    # rows remain eligible for refinement planning.
    width = reachable_stop - start
    normalized = [(value - start) / width for value in values]
    cluster_median = median(normalized)
    # A shift is justified only when the *whole* qualifying cluster is inside
    # one outer band. A low/high median alone can hide a broad or mixed cluster.
    if normalized[-1] <= 0.20:
        direction = "lower"
    elif normalized[0] >= 0.80:
        direction = "upper"
    else:
        direction = "center"

    if direction in {"lower", "upper"}:
        if broad_run:
            new_streak = 0
        # Translate both guards by the same grid-aligned amount. Align the
        # cluster median with the old range midpoint, clamping only at domain.
        desired_delta = median(values) - (start + reachable_stop) / 2
        requested_steps = _round_grid(start + desired_delta, start, step)
        min_steps = _ceil_grid(domain_min, start, step)
        max_steps = _floor_grid(domain_max - width, start, step)
        if min_steps > max_steps:
            return _result("hold_domain_limit", start, stop, new_streak,
                           direction, "no full-width range fits the valid domain")
        actual_steps = min(max(requested_steps, min_steps), max_steps)
        shifted_start = start + actual_steps * step
        shifted_stop = shifted_start + width
        shifted_width = shifted_stop - shifted_start
        shift_centered = (
            shifted_width > 0 and
            (values[0] - shifted_start) / shifted_width >= 0.20 - 1e-9 and
            (values[-1] - shifted_start) / shifted_width <= 0.80 + 1e-9 and
            0.30 - 1e-9 <= (median(values) - shifted_start) / shifted_width <= 0.70 + 1e-9
        )
        if actual_steps == 0 or not shift_centered:
            padding = max(step, (values[-1] - values[0]) / 3.0)
            low = max(domain_min, values[0] - padding)
            high = min(domain_max, values[-1] + padding)
            lower_index = max(_ceil_grid(domain_min, start, step),
                              _floor_grid(low, start, step))
            upper_index = min(_floor_grid(domain_max, start, step),
                              _ceil_grid(high, start, step))
            narrowed_start = start + lower_index * step
            narrowed_stop = start + upper_index * step
            if (narrowed_start <= values[0] and narrowed_stop >= values[-1]
                    and narrowed_stop > narrowed_start
                    and narrowed_stop - narrowed_start < width - 1e-12):
                narrow_width = narrowed_stop - narrowed_start
                narrow_low = (values[0] - narrowed_start) / narrow_width
                narrow_high = (values[-1] - narrowed_start) / narrow_width
                narrow_median = (median(values) - narrowed_start) / narrow_width
                if (narrow_low >= 0.20 - 1e-9 and narrow_high <= 0.80 + 1e-9
                        and 0.30 - 1e-9 <= narrow_median <= 0.70 + 1e-9):
                    return _result("narrow", narrowed_start, narrowed_stop, 0,
                                   direction, "recenter edge cluster in a smaller valid range")
            return _result("hold_domain_limit", start, stop, new_streak,
                           direction, "cluster is at the movement boundary; no centered range fits")
        return _result("shift", shifted_start, shifted_stop, new_streak,
                       direction, "translate the whole range toward the result cluster")

    # A centered cluster must be wholly outside both outer 20% bands, with
    # its median in the middle 40%. This rejects clusters that straddle edges.
    fully_inside_inner_bands = normalized[0] >= 0.20 and normalized[-1] <= 0.80
    median_in_middle_40 = 0.30 <= cluster_median <= 0.70
    centered = fully_inside_inner_bands and median_in_middle_40
    if not centered:
        if broad_run:
            new_streak = 0
        return _result("keep_broad", start, reachable_stop, new_streak, "mixed",
                       "qualifying cluster is not centered within the inner range")

    if broad_run:
        new_streak += 1
    if not allow_first_pass_narrow and (not broad_run or new_streak < 2):
        return _result("keep_broad", start, reachable_stop, new_streak, "center",
                       "centered condition needs two consecutive broad runs")

    # Formula: cluster min/max plus one old-grid step of padding on both
    # sides, snapped outward to the existing grid and clipped to current and
    # hard-domain bounds. This preserves every qualifying result and step.
    grid_origin = start
    lower_index = _floor_grid(values[0] - step, grid_origin, step)
    upper_index = math.ceil((values[-1] + step - grid_origin) / step - 1e-12)
    min_index = max(0, _ceil_grid(domain_min, grid_origin, step))
    max_index = min(_floor_grid(reachable_stop, grid_origin, step),
                    _floor_grid(domain_max, grid_origin, step))
    lower_index = max(lower_index, min_index)
    upper_index = min(upper_index, max_index)
    narrowed_start = grid_origin + lower_index * step
    narrowed_stop = grid_origin + upper_index * step
    if narrowed_start > values[0] or narrowed_stop < values[-1] or narrowed_stop <= narrowed_start:
        return _result("keep_broad", start, reachable_stop, new_streak, "center",
                       "no valid step-aligned range contains the qualifying cluster")
    if narrowed_stop - narrowed_start >= width - 1e-12:
        return _result("keep_broad", start, reachable_stop, new_streak, "center",
                       "cluster_does_not_fit_narrow_range")
    return _result("narrow", narrowed_start, narrowed_stop, 0, "center",
                   "narrow to qualifying cluster plus one grid step of padding")


def _result(action: str, start: float, stop: float, streak: int,
            position: str, reason: str) -> dict:
    return {
        "action": action,
        "start": start,
        "stop": stop,
        "centered_streak": streak,
        "cluster_position": position,
        "reason": reason,
    }


def range_movement_bounds(start: float, stop: float, *,
                          lower_bound: float | None = None,
                          upper_bound: float | None = None) -> tuple[float, float]:
    """Allow an outer-band result cluster to translate one full range width.

    The Stage 3/4.5 range is the current search window, not the movement
    envelope. Using it as both caused every edge shift to clamp back to itself.
    One window of travel on either side is sufficient to center any cluster
    that lies within the current window while preserving the current width.
    """
    start = _finite(start, "start")
    stop = _finite(stop, "stop")
    if start >= stop:
        raise ValueError("range bounds must be increasing")
    width = stop - start
    move_min, move_max = start - width, stop + width
    if lower_bound is not None:
        move_min = max(move_min, _finite(lower_bound, "lower_bound"))
    if upper_bound is not None:
        move_max = min(move_max, _finite(upper_bound, "upper_bound"))
    if move_min >= move_max:
        raise ValueError("movement bounds must be increasing")
    return move_min, move_max


def source_init_input_bounds(source_text: str,
                             input_types: dict[str, str] | None = None) -> dict[str, dict[str, float | None]]:
    """Read simple numeric input guards that reject parameters in OnInit.

    Only comparisons inside an ``if`` that directly returns
    ``INIT_PARAMETERS_INCORRECT`` are treated as constraints. This avoids
    confusing search estimates or strategy logic with authenticated input
    validity rules.
    """
    source = re.sub(r"/\*.*?\*/|//[^\r\n]*", "", source_text, flags=re.S)
    on_init = re.search(r"\bint\s+OnInit\s*\(\s*\)\s*\{", source)
    if not on_init:
        return {}
    open_brace = source.find("{", on_init.start())
    depth = 0
    close_brace = None
    for index in range(open_brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                close_brace = index
                break
    if close_brace is None:
        return {}
    body = source[open_brace + 1:close_brace]
    declaration_types = {
        name: kind.lower()
        for kind, name in re.findall(
            r"\binput\s+([A-Za-z_]\w*)\s+(Inp[A-Za-z_]\w*)\s*=", source)
    }
    declaration_types.update({name: str(kind).lower()
                              for name, kind in (input_types or {}).items()})
    numeric = re.compile(r"\b(Inp[A-Za-z_]\w*)\s*(<=|>=|<|>)\s*"
                         r"([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)[fF]?\b")
    bounds: dict[str, dict[str, float | None]] = {}
    for guard in re.finditer(
            r"\bif\s*\((.*?)\)\s*return\s+INIT_PARAMETERS_INCORRECT\s*;",
            body, flags=re.S):
        condition = guard.group(1)
        # A conjunction needs Boolean reasoning; leave it unaudited rather
        # than turning one conjunct into an over-restrictive scalar bound.
        if "&&" in condition:
            continue
        for match in numeric.finditer(condition):
            name, operator, literal = match.groups()
            value = float(literal)
            integer = declaration_types.get(name, "") in {
                "int", "uint", "ulong", "long", "short", "uchar",
                "ushort", "integer", "byte", "sbyte",
            }
            item = bounds.setdefault(name, {"min": None, "max": None})
            if operator == "<":
                item["min"] = max(value, item["min"]) if item["min"] is not None else value
            elif operator == "<=":
                minimum = float(math.floor(value) + 1) if integer else math.nextafter(value, math.inf)
                item["min"] = max(minimum, item["min"]) if item["min"] is not None else minimum
            elif operator == ">":
                item["max"] = min(value, item["max"]) if item["max"] is not None else value
            else:  # x >= c is invalid, so valid values are strictly below c.
                maximum = float(math.ceil(value) - 1) if integer else math.nextafter(value, -math.inf)
                item["max"] = min(maximum, item["max"]) if item["max"] is not None else maximum
    return {name: value for name, value in bounds.items()
            if value["min"] is not None or value["max"] is not None}


def input_movement_bounds(name: str, start: float, stop: float,
                          domain: tuple[float, float] | None = None,
                          safety_bounds: dict[str, float | None] | None = None) -> tuple[float, float]:
    """Apply an authenticated domain when it contains the current wide range.

    Stage 3 deliberately owns the wide optimization surface, so Stage 2 range
    estimates cannot reject a Stage 3 interval that extends beyond them. When
    the current interval is inside the authenticated domain, use it to prevent
    a Stage 4/4.5 shift from producing an invalid interval. Otherwise preserve
    the wide interval and use the input's safety bounds below.
    """
    lower = upper = None
    if domain is not None:
        lower, upper = (_finite(domain[0], "domain_min"),
                        _finite(domain[1], "domain_max"))
        if lower >= upper:
            raise ValueError("authoritative input domain must be increasing")
    if safety_bounds:
        if safety_bounds.get("min") is not None:
            value = _finite(safety_bounds["min"], f"{name}.source_min")
            lower = value if lower is None else max(lower, value)
        if safety_bounds.get("max") is not None:
            value = _finite(safety_bounds["max"], f"{name}.source_max")
            upper = value if upper is None else min(upper, value)
        if lower is not None and upper is not None and lower >= upper:
            raise ValueError(f"{name} source constraints have no usable range")
    if domain is not None:
        return lower, upper
    if name.startswith("InpRegime_"):
        # The Stage 2 regime grid's 0..5 range is a search estimate, not a
        # hard source domain. Keep multipliers nonnegative, but allow a result
        # cluster at the edge to move the next window beyond the old stop.
        lower = 0.0 if lower is None else max(lower, 0.0)
        return range_movement_bounds(start, stop, lower_bound=lower, upper_bound=upper)
    positive_floors = {
        "InpZRegimeGreed": 0.0,
        "InpDccLookback": 50.0,
        # Positivity is a safety constraint; Stage 1 search starts are only
        # estimates and must not become fabricated hard minima.
        "InpDccEmaQ": 1e-12,
        "InpDccEmaP": 1e-12,
        "InpUnivGarchOmega": 0.000001,
        "InpUnivGarchAlpha": 0.01,
        "InpUnivGarchBeta": 0.8,
        "InpRegimeVolLookup": 20.0,
        "InpAtrPeriod": 5.0,
        "InpEmaPeriod": 50.0,
        "InpBaseLot": 0.0,
        "InpATRPeriod": 1.0,
        "InpGKLookbackBars": 2.0,
        "InpMaxTradeBars": 12.0,
        "InpMinBarsBetweenTrades": 1.0,
        "InpMarginKillLevel": 100.0,
        # Stage 3's authenticated wide ranges may extend below Stage 2's
        # estimates. Keep the movement envelope strictly positive without
        # rejecting that existing wide search surface.
        "InpAtrSlMult": 1e-9,
        "InpAtrTpMult": 1e-9,
        "InpSL_ATR_Mult": 0.0,
        "InpTP_ATR_Mult": 0.0,
    }
    input_lower = positive_floors.get(name)
    upper_bounds = {
        "InpDccLookback": 600.0,
        "InpDccEmaQ": 0.2,
        "InpDccEmaP": 0.99,
        "InpUnivGarchOmega": 0.001,
        "InpUnivGarchAlpha": 0.2,
        "InpUnivGarchBeta": 0.99,
        "InpRegimeVolLookup": 400.0,
        # Match the authenticated EA OnInit bound: periods above 100 are
        # rejected by the EA, while Stage 3 may validly search through 99.
        "InpAtrPeriod": 100.0,
        "InpEmaPeriod": 500.0,
        "InpMaxTradeBars": 200.0,
        "InpMinBarsBetweenTrades": 96.0,
        "InpMarginKillLevel": 800.0,
    }
    input_upper = upper_bounds.get(name)
    if input_lower is not None:
        lower = input_lower if lower is None else max(lower, input_lower)
    if input_upper is not None:
        upper = input_upper if upper is None else min(upper, input_upper)
    return range_movement_bounds(start, stop, lower_bound=lower, upper_bound=upper)


def refined_step(step: float, input_type: str = "double") -> float:
    """Halve a grid step while preserving the EA input's numeric type."""
    step = _finite(step, "step")
    integer = input_type.casefold() in {
        "int", "integer", "long", "ulong", "uint", "short", "ushort",
        "byte", "uchar", "sbyte",
    }
    if integer:
        if not step.is_integer():
            raise ValueError("integer refinement input has a fractional current step")
        return max(1.0, float(math.floor(step / 2.0)))
    step = _clean_step(step)
    if step.is_integer():
        return max(1.0, float(math.floor(step / 2.0)))
    return step / 2.0



def _round_sig(value: float, digits: int = 2, *, upward: bool = False) -> float:
    """Round a positive grid step to significant digits without binary tails."""
    from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

    value = _finite(value, "step candidate")
    if value <= 0:
        raise ValueError("step candidate must be positive")
    decimal_value = Decimal(str(value))
    quantum = Decimal(1).scaleb(decimal_value.adjusted() - digits + 1)
    rounding = ROUND_CEILING if upward else ROUND_FLOOR
    units = (decimal_value / quantum).to_integral_value(rounding=rounding)
    return float(units * quantum)


def _fit_clean_grid(*, start, stop, step, cluster, input_type, action,
                    domain_min, domain_max, minimum_mt5_steps,
                    maximum_mt5_steps, target_mt5_steps):
    integer = input_type.casefold() in {
        "int", "integer", "long", "ulong", "uint", "short", "ushort",
        "byte", "uchar", "sbyte",
    }
    if integer:
        start, stop = float(math.floor(start + 1e-9)), float(math.ceil(stop - 1e-9))
        if not step.is_integer() or any(not value.is_integer() for value in cluster):
            raise ValueError("integer refinement input has a fractional step or cluster value")
        step = float(max(1, math.ceil(step)))
        if domain_min is not None:
            domain_min = float(math.ceil(_finite(domain_min, "domain_min") - 1e-9))
        if domain_max is not None:
            domain_max = float(math.floor(_finite(domain_max, "domain_max") + 1e-9))
    if domain_min is not None and domain_max is not None and domain_min >= domain_max:
        raise ValueError("refinement hard domain is invalid")

    low, high = min(cluster), max(cluster)
    old_width = stop - start
    if old_width <= 0:
        raise ValueError("refinement range has no width")
    narrowing = action in {"narrow", "shift"}
    # If the prior step alone would make the minimum inclusive MT5 grid wider
    # than the current range, reduce the clean increment to preserve both the
    # requested value count and the narrowing contract.
    allow_finer_step = narrowing and (minimum_mt5_steps - 1) * step >= old_width - 1e-9
    required_width = (max((high - low) / 0.60, step * 8.0) if narrowing
                      else max(high - low + 2.0 * step, step * 8.0))
    if narrowing and required_width >= old_width - 1e-12:
        narrowing = False
        required_width = max(high - low + 2.0 * step, step * 8.0)
    broad_target_width = old_width
    if not narrowing and domain_min is not None and domain_max is not None:
        domain_width = domain_max - domain_min
        required_width = min(required_width, domain_width)
        broad_target_width = min(old_width, domain_width)

    candidates = []
    for mt5_steps in range(minimum_mt5_steps, maximum_mt5_steps + 1):
        intervals = mt5_steps - 1
        target_width = (required_width if narrowing
                        else max(broad_target_width, required_width))
        ideal_step = target_width / intervals
        raw_step = ideal_step if allow_finer_step else max(step, ideal_step)
        clean_steps = [_round_sig(raw_step, 2, upward=True)]
        # A downward rounded increment can be the only clean fit inside a hard
        # domain (for example 5.0 / 18 cannot be represented upward to 2 sigs).
        lower_step = _round_sig(raw_step, 2)
        if (allow_finer_step or lower_step >= step - 1e-12) and lower_step not in clean_steps:
            clean_steps.append(lower_step)
        for clean_step in clean_steps:
            if integer:
                clean_step = float(max(1, math.ceil(clean_step - 1e-12)))
            width = intervals * clean_step
            places = max(0, -math.floor(math.log10(clean_step)) + 1)
            # MT5 starts a grid at the supplied start value and then adds the
            # step; the start does not need to be a multiple of that step.
            # Align starts to their representable decimal precision instead,
            # so a valid centered cluster is not rejected by an artificial
            # zero-origin step lattice.
            start_quantum = 1.0 if integer else 10.0 ** (-places)
            if width + 1e-9 < required_width:
                continue
            if narrowing and width >= old_width - 1e-9:
                continue
            if not narrowing and width + 1e-9 < min(broad_target_width, max(high - low, step * 8.0)):
                continue
            if domain_min is not None and domain_max is not None and width > domain_max - domain_min + 1e-9:
                continue

            if narrowing:
                feasible_low, feasible_high = high - width, low
                # A generated refinement must leave enough room around the
                # full selected cluster. Mere inclusion can put an edge
                # cluster back on the edge (especially after hard-domain
                # clamping), causing the emitted range to fail its own plan
                # validator. Keep the cluster inside the middle 60%.
                feasible_low = max(feasible_low, high - 0.80 * width)
                feasible_high = min(feasible_high, low - 0.20 * width)
                if domain_min is not None:
                    feasible_low = max(feasible_low, domain_min)
                if domain_max is not None:
                    feasible_high = min(feasible_high, domain_max - width)
                first = math.ceil(feasible_low / start_quantum - 1e-10)
                last = math.floor(feasible_high / start_quantum + 1e-10)
                if first > last:
                    continue
                desired = ((low + high) / 2.0 - width / 2.0) / start_quantum
                new_start = min(max(int(round(desired)), first), last) * start_quantum
            else:
                if action == "shift":
                    # The minimum MT5 grid can be wider than a proposed
                    # refinement (common for integer inputs whose step cannot
                    # be halved). Refit that fallback around the selected
                    # cluster instead of anchoring it at the old lower edge.
                    feasible_low = max(high - width, high - 0.80 * width)
                    feasible_high = min(low, low - 0.20 * width)
                    if domain_min is not None:
                        feasible_low = max(feasible_low, domain_min)
                    if domain_max is not None:
                        feasible_high = min(feasible_high, domain_max - width)
                    first = math.ceil(feasible_low / start_quantum - 1e-10)
                    last = math.floor(feasible_high / start_quantum + 1e-10)
                    if first > last:
                        continue
                    desired = ((low + high) / 2.0 - width / 2.0) / start_quantum
                    new_start = min(max(int(round(desired)), first), last) * start_quantum
                else:
                    new_start = float(start)
                    if domain_min is not None and new_start < domain_min - 1e-9:
                        new_start = float(domain_min)
                    if integer:
                        new_start = float(math.floor(new_start + 1e-9))
                    if domain_max is not None and new_start + width > domain_max + 1e-9:
                        shifted_start = float(domain_max - width)
                        if shifted_start < (domain_min if domain_min is not None else -math.inf) - 1e-9:
                            continue
                        if shifted_start > low + 1e-9 or shifted_start + width < high - 1e-9:
                            continue
                        new_start = shifted_start
                    if new_start > low + 1e-9 or new_start + width < high - 1e-9:
                        continue
            rounded_start = round(new_start, places)
            if (rounded_start <= low + 1e-9 and
                    rounded_start + width >= high - 1e-9 and
                    (domain_min is None or rounded_start >= domain_min - 1e-9) and
                    (domain_max is None or rounded_start + width <= domain_max + 1e-9)):
                new_start = rounded_start
            from decimal import Decimal
            new_stop = float(Decimal(str(new_start)) + Decimal(str(clean_step)) * Decimal(intervals))
            if low < new_start - 1e-9 or high > new_stop + 1e-9:
                continue
            candidates.append(((width - required_width, abs(mt5_steps - target_mt5_steps), mt5_steps),
                               new_start, clean_step, new_stop, intervals, mt5_steps))
    if not candidates:
        raise ValueError("no clean step grid fits the cluster and valid domain")
    _, new_start, clean_step, new_stop, intervals, mt5_steps = min(candidates, key=lambda row: row[0])
    if integer and not all(float(v).is_integer() for v in (new_start, clean_step, new_stop)):
        raise ValueError("integer refinement bounds/step are fractional")
    return {"start": float(new_start), "step": float(clean_step),
            "stop": float(new_stop), "intervals": float(intervals),
            "mt5_steps": float(mt5_steps)}


def fit_refinement_grid(*, start: float, stop: float, step: float,
                        cluster: Iterable[float], input_type: str = "double",
                        action: str = "narrow", domain_min: float | None = None,
                        domain_max: float | None = None, clean_grid: bool = False,
                        allow_sub_15: bool = False) -> dict[str, float]:
    """Fit a refinement grid while retaining its cluster and hard domain.

    Legacy Stage 4 uses 15-20 intervals. Stage 4.5 clean grids instead count
    MT5's inclusive start/stop values, using 15-20 normally or 8-20 for a
    strongly converged cluster. Integer inputs retain integral bounds/steps.
    """
    start, stop, step = (_finite(start, "start"), _finite(stop, "stop"),
                         _finite(step, "step"))
    values = sorted(_finite(value, "cluster value") for value in cluster)
    if start >= stop or step <= 0 or not values:
        raise ValueError("refinement grid needs an increasing range, positive step, and cluster")
    if values[0] < start - 1e-9 or values[-1] > stop + 1e-9:
        raise ValueError("refinement range excludes selected cluster")
    if clean_grid:
        minimum = MIN_STAGE45_STRONG_MT5_VALUES if allow_sub_15 else MIN_STAGE45_MT5_VALUES
        return _fit_clean_grid(
            start=start, stop=stop, step=step, cluster=values,
            input_type=input_type, action=action,
            domain_min=domain_min, domain_max=domain_max,
            minimum_mt5_steps=minimum,
            maximum_mt5_steps=MAX_STAGE45_MT5_VALUES,
            target_mt5_steps=minimum if allow_sub_15 else TARGET_STAGE45_MT5_VALUES,
        )
    integer = input_type.casefold() in {
        "int", "integer", "long", "ulong", "uint", "short", "ushort",
        "byte", "uchar", "sbyte",
    }
    if integer:
        if not step.is_integer() or any(not value.is_integer() for value in values):
            raise ValueError("integer refinement input has a fractional step or cluster value")
        # Range analysis may yield fractional provisional bounds around an
        # integer-valued cluster. Expand them outward before fitting the final
        # integer grid so every selected result remains inside the range.
        start = float(math.floor(start + 1e-9))
        stop = float(math.ceil(stop - 1e-9))
        if domain_min is not None and domain_max is not None:
            domain_min = float(math.ceil(_finite(domain_min, "domain_min") - 1e-9))
            domain_max = float(math.floor(_finite(domain_max, "domain_max") + 1e-9))
            if domain_min >= domain_max:
                raise ValueError("integer refinement hard domain contains no usable interval")

    old_width = stop - start
    cluster_low, cluster_high = values[0], values[-1]
    cluster_span = cluster_high - cluster_low
    if action == "keep_broad":
        new_start, new_stop = start, stop
    else:
        # Half a previous step per side is not enough for stable refinement;
        # eight old steps across the new range avoids an excessively fine grid.
        needed_width = max(cluster_span / 0.60, step * 8.0)
        if integer:
            new_step = max(1, math.ceil(needed_width / TARGET_REFINEMENT_INTERVALS))
            width = float(new_step * TARGET_REFINEMENT_INTERVALS)
        else:
            width = needed_width
        if width >= old_width - 1e-12:
            new_start, new_stop = start, stop
        else:
            center = (cluster_low + cluster_high) / 2.0
            new_start, new_stop = center - width / 2.0, center + width / 2.0

    if domain_min is not None:
        domain_min = _finite(domain_min, "domain_min")
        width = new_stop - new_start
        if new_start < domain_min:
            new_start, new_stop = domain_min, domain_min + width
    if domain_max is not None:
        domain_max = _finite(domain_max, "domain_max")
        width = new_stop - new_start
        if domain_min is not None and domain_min >= domain_max:
            raise ValueError("refinement hard domain is invalid")
        if domain_min is not None and width > domain_max - domain_min + 1e-9:
            raise ValueError("15-20 interval refinement window cannot fit inside the hard domain")
        if new_stop > domain_max:
            new_start, new_stop = domain_max - width, domain_max
    if domain_min is not None and new_start < domain_min - 1e-9:
        raise ValueError("refinement grid cannot fit above its lower bound")

    if not integer and action in {"narrow", "shift"}:
        width = new_stop - new_start
        if (width <= 0 or
                (cluster_low - new_start) / width < 0.20 - 1e-9 or
                (cluster_high - new_start) / width > 0.80 + 1e-9):
            raise ValueError(
                "refinement grid cannot center the selected cluster inside its authenticated domain"
            )

    if integer:
        if action == "keep_broad":
            width = stop - start
            exact_counts = [count for count in range(MIN_REFINEMENT_INTERVALS,
                                                     MAX_REFINEMENT_INTERVALS + 1)
                            if math.isclose(width / count, round(width / count), abs_tol=1e-9)]
            if exact_counts:
                intervals = min(exact_counts, key=lambda count: (abs(count - TARGET_REFINEMENT_INTERVALS), -count))
                new_start, new_stop = float(round(start)), float(round(stop))
                result_step = width / intervals
            else:
                result_step = float(max(1, math.ceil(width / TARGET_REFINEMENT_INTERVALS)))
                width = result_step * TARGET_REFINEMENT_INTERVALS
                new_start = float(math.floor(start))
                new_stop = new_start + width
        elif action in {"narrow", "shift"}:
            needed_width = max(cluster_span / 0.60, step * 8.0)
            center = (cluster_low + cluster_high) / 2.0
            feasible_grids = []
            for candidate_intervals in range(MIN_REFINEMENT_INTERVALS,
                                             MAX_REFINEMENT_INTERVALS + 1):
                candidate_step = float(max(
                    1, math.ceil(needed_width / candidate_intervals)))
                candidate_width = candidate_step * candidate_intervals
                feasible_low = cluster_high - 0.80 * candidate_width
                feasible_high = cluster_low - 0.20 * candidate_width
                if domain_min is not None:
                    feasible_low = max(feasible_low, domain_min)
                if domain_max is not None:
                    feasible_high = min(feasible_high,
                                        domain_max - candidate_width)
                first = math.ceil(feasible_low - 1e-9)
                last = math.floor(feasible_high + 1e-9)
                if first > last:
                    continue
                candidate_start = min(max(math.floor(
                    center - candidate_width / 2.0 + 0.5), first), last)
                feasible_grids.append((
                    abs(candidate_intervals - TARGET_REFINEMENT_INTERVALS),
                    candidate_width, candidate_intervals, candidate_start,
                    candidate_step,
                ))
            if not feasible_grids:
                raise ValueError(
                    "integer refinement grid cannot center the selected cluster "
                    "inside its authenticated domain"
                )
            _, width, intervals, new_start, result_step = min(feasible_grids)
            new_start = float(new_start)
            new_stop = new_start + width
        else:
            # A hard-domain hold is an explicit boundary decision. Preserve
            # the cluster and let the all-input validator enforce the hard
            # domain; requiring a centered cluster here would turn a valid
            # domain-edge hold into a false Stage 4 rejection.
            needed_width = max(cluster_span / 0.60, step * 8.0)
            result_step = float(max(
                1, math.ceil(needed_width / TARGET_REFINEMENT_INTERVALS)))
            width = result_step * TARGET_REFINEMENT_INTERVALS
            center = (cluster_low + cluster_high) / 2.0
            new_start = float(math.floor(center - width / 2.0))
            new_stop = new_start + width
            if domain_min is not None and new_start < domain_min:
                new_start, new_stop = float(domain_min), float(domain_min) + width
            if domain_max is not None and new_stop > domain_max:
                new_start, new_stop = float(domain_max) - width, float(domain_max)
        if (new_start > cluster_low or new_stop < cluster_high or
                new_start < (domain_min if domain_min is not None else new_start) - 1e-9 or
                new_stop > (domain_max if domain_max is not None else new_stop) + 1e-9):
            raise ValueError("integer refinement grid cannot contain the selected cluster in its hard domain")
    else:
        width = new_stop - new_start
        if width <= 0:
            raise ValueError("refinement grid has no width")
        result_step = width / TARGET_REFINEMENT_INTERVALS

    result = {"start": float(new_start), "step": float(result_step),
              "stop": float(new_stop)}
    intervals = (result["stop"] - result["start"]) / result["step"]
    if (not MIN_REFINEMENT_INTERVALS - 1e-9 <= intervals <= MAX_REFINEMENT_INTERVALS + 1e-9 or
            not math.isclose(intervals, round(intervals), abs_tol=1e-8)):
        raise ValueError("fitted refinement grid violates the 15-20 interval policy")
    if cluster_low < result["start"] - 1e-9 or cluster_high > result["stop"] + 1e-9:
        raise ValueError("fitted refinement grid excludes selected cluster")
    return result


def evaluate_refinement_range(*, start: float, stop: float, step: float,
                              cluster: Iterable[float], centered_streak: int = 0,
                              broad_run: bool = True,
                              allow_first_pass_narrow: bool = False,
                              movement_bounds: tuple[float, float] | None = None) -> dict:
    """Evaluate a cluster with movement bounds separate from its current grid."""
    move_min, move_max = movement_bounds or input_movement_bounds("", start, stop)
    return evaluate_input_range(
        start=start, stop=stop, step=step,
        domain_min=move_min, domain_max=move_max,
        cluster=cluster, centered_streak=centered_streak,
        broad_run=broad_run,
        allow_first_pass_narrow=allow_first_pass_narrow,
        require_current_range_in_domain=False,
    )


def validate_input_range_plan(input_actions: dict,
                              actual_ranges: dict[str, dict[str, float]]) -> list[str]:
    """Check the emitted INI against every planned input range and moved cluster."""
    failures = []
    if set(input_actions) != set(actual_ranges):
        return ["optimized input set does not match the range plan"]
    for name, action in input_actions.items():
        observed = actual_ranges[name]
        try:
            expected = (
                _finite(action.get("next_start", action["start"]), f"{name}.next_start"),
                _finite(action.get("next_step", action.get("step", observed["step"])),
                        f"{name}.next_step"),
                _finite(action.get("next_stop", action["stop"]), f"{name}.next_stop"),
            )
            actual = tuple(_finite(observed[key], f"{name}.{key}")
                           for key in ("start", "step", "stop"))
            if any(abs(left - right) > 1e-9 for left, right in zip(expected, actual)):
                failures.append(f"{name}: emitted INI differs from planned start/step/stop")
                continue
            intervals = (actual[2] - actual[0]) / actual[1]
            if not math.isclose(intervals, round(intervals), abs_tol=1e-8):
                failures.append(f"{name}: refinement grid has a non-integral interval count ({intervals:g})")
                continue
            if action.get("step_policy") == "two_significant_digits":
                mt5_steps = int(round(intervals)) + 1
                minimum_mt5_steps = int(action.get("minimum_mt5_steps", 15))
                if not minimum_mt5_steps <= mt5_steps <= 20:
                    failures.append(f"{name}: MT5 grid has {mt5_steps} values; expected {minimum_mt5_steps}-20")
                    continue
            else:
                minimum_intervals = int(action.get("minimum_intervals", MIN_REFINEMENT_INTERVALS))
                if not minimum_intervals - 1e-9 <= intervals <= MAX_REFINEMENT_INTERVALS + 1e-9:
                    failures.append(f"{name}: refinement grid has {intervals:g} intervals; expected {minimum_intervals}-20")
                    continue
            if action.get("step_policy") == "two_significant_digits":
                if not (math.isclose(actual[1], _round_sig(actual[1], 2), rel_tol=1e-9, abs_tol=1e-15) or
                        math.isclose(actual[1], _round_sig(actual[1], 2, upward=True), rel_tol=1e-9, abs_tol=1e-15)):
                    failures.append(f"{name}: step is not a clean two-significant-digit increment")
                    continue
            input_type = str(action.get("input_type", "double")).casefold()
            if input_type in {"int", "integer", "long", "ulong", "uint", "short", "ushort", "byte", "uchar", "sbyte"} and not all(
                    value.is_integer() for value in actual):
                failures.append(f"{name}: integer refinement bounds/step are fractional")
                continue
            start, _, stop = actual
            metadata = action.get("source_metadata")
            runtime_bounds = action.get("source_runtime_bounds")
            if isinstance(metadata, dict) and metadata.get("hard_domain") is True:
                domain_min = _finite(metadata["min"], f"{name}.domain_min")
                domain_max = _finite(metadata["max"], f"{name}.domain_max")
                if start < domain_min - 1e-9 or stop > domain_max + 1e-9:
                    failures.append(f"{name}: emitted range exceeds its authenticated hard domain")
                    continue
            elif name.startswith("InpRegime_"):
                if start < -1e-9:
                    failures.append(f"{name}: regime multiplier range must stay nonnegative")
                    continue
            else:
                movement_min, movement_max = input_movement_bounds(
                    name, _finite(action["start"], f"{name}.start"),
                    _finite(action["stop"], f"{name}.stop"),
                    safety_bounds=runtime_bounds if isinstance(runtime_bounds, dict) else None)
                if start < movement_min - 1e-9 or stop > movement_max + 1e-9:
                    failures.append(f"{name}: emitted range exceeds computed input movement bounds")
                    continue
            if isinstance(runtime_bounds, dict):
                runtime_min, runtime_max = runtime_bounds.get("min"), runtime_bounds.get("max")
                if ((runtime_min is not None and start < float(runtime_min) - 1e-9) or
                        (runtime_max is not None and stop > float(runtime_max) + 1e-9)):
                    failures.append(f"{name}: emitted range violates EA OnInit parameter constraints")
                    continue
            cluster_min = action.get("cluster_min")
            cluster_max = action.get("cluster_max")
            if cluster_min is not None and cluster_max is not None:
                cluster_min = _finite(cluster_min, f"{name}.cluster_min")
                cluster_max = _finite(cluster_max, f"{name}.cluster_max")
                if cluster_min < start - 1e-9 or cluster_max > stop + 1e-9:
                    failures.append(f"{name}: emitted range excludes its selected cluster")
                elif str(action.get("action", "")).lower() == "shift":
                    width = stop - start
                    if width <= 0 or (cluster_min - start) / width < 0.20 - 1e-9 or \
                            (cluster_max - start) / width > 0.80 + 1e-9:
                        failures.append(f"{name}: shifted cluster is not within the middle 60%")
                elif (str(action.get("action", "")).lower() == "narrow" and
                      "recenter edge cluster" in str(action.get("reason", "")).lower()):
                    width = stop - start
                    if width <= 0 or (cluster_min - start) / width < 0.20 - 1e-9 or \
                            (cluster_max - start) / width > 0.80 + 1e-9:
                        failures.append(f"{name}: recentered edge cluster is outside the middle 60%")
        except (KeyError, TypeError, ValueError):
            failures.append(f"{name}: invalid or incomplete range plan")
    return failures


def evaluate_narrow_fallback(*, narrow_runs_completed: int,
                             latest_score: float | None,
                             previous_score: float | None,
                             latest_valid: bool,
                             fallback_eligible: bool = False) -> dict:
    """Apply the three-narrow-run fallback with score regression fail-closed."""
    if not latest_valid or latest_score is None or previous_score is None:
        return {
            "action": "hold_for_narrowing_review",
            "reason": "every completed run requires latest and prior valid effective Minato scores",
            "latest_score": latest_score,
            "previous_score": previous_score,
            "runs_completed": narrow_runs_completed,
        }
    latest_score = _finite(latest_score, "latest_score")
    previous_score = _finite(previous_score, "previous_score")
    if latest_score < previous_score:
        return {
            "action": "hold_for_narrowing_review",
            "reason": "latest Minato Base Score regressed from the prior run",
            "latest_score": latest_score,
            "previous_score": previous_score,
            "runs_completed": narrow_runs_completed,
        }
    if not fallback_eligible:
        return {
            "action": "continue_narrowing",
            "reason": "fallback conditions are not met",
            "latest_score": latest_score,
            "previous_score": previous_score,
            "runs_completed": narrow_runs_completed,
        }
    if narrow_runs_completed < NARROW_FALLBACK_RUNS:
        return {
            "action": "continue_narrowing",
            "reason": "latest effective Minato score did not regress; fallback run count not reached",
            "latest_score": latest_score,
            "previous_score": previous_score,
            "runs_completed": narrow_runs_completed,
        }
    return {
        "action": "use_latest_best",
        "reason": "three narrow runs completed; latest valid score is non-regressing",
        "candidate_score": latest_score,
        "latest_score": latest_score,
        "previous_score": previous_score,
    }


def evaluate_stage4_run(*, inputs: dict[str, dict], run_completed: bool,
                        latest_minato_score: float | None,
                        latest_min_margin_pct: float | None,
                        previous_minato_score: float | None,
                        previous_min_margin_pct: float | None,
                        latest_score_valid: bool,
                        stage4_completed: bool,
                        stage5_enabled: bool) -> dict:
    """Coordinate one verified Stage 4 result without external side effects.

    Scores supplied here are raw Minato values paired with their observed
    minimum margins. The effective scores are compared on every completed
    run. Every input supplies its own persisted ``narrow_runs_completed``
    counter. An unresolved input is fallback-eligible only after that same
    input has completed three narrow runs; another input's history cannot
    satisfy its counter.
    """
    if not run_completed:
        return {
            "action": "wait_for_completed_run",
            "input_actions": {},
            "fallback": {"action": "not_evaluated"},
            "stage5_transition": "wait_for_stage4",
        }

    input_run_counts: dict[str, int] = {}
    try:
        for name, spec in inputs.items():
            count_value = spec["narrow_runs_completed"]
            count = int(count_value)
            if count < 0 or float(count_value) != count:
                raise ValueError(f"{name}: narrow_runs_completed must be a non-negative integer")
            input_run_counts[name] = count
    except (KeyError, TypeError, ValueError) as exc:
        return {
            "action": "hold_invalid_input_run_count",
            "reason": str(exc),
            "input_actions": {},
            "fallback": {"action": "hold_for_narrowing_review"},
            "stage5_transition": "hold_disabled",
        }

    if latest_min_margin_pct is None or previous_min_margin_pct is None:
        return {
            "action": "hold_for_narrowing_review",
            "reason": "completed runs require recorded minimum-margin evidence for both scores",
            "input_actions": {},
            "fallback": {"action": "hold_for_narrowing_review"},
            "stage5_transition": "hold_disabled",
        }

    try:
        effective_latest = (None if latest_minato_score is None else
                            minato_with_margin_floor(latest_minato_score, latest_min_margin_pct))
        effective_previous = (None if previous_minato_score is None else
                              minato_with_margin_floor(previous_minato_score, previous_min_margin_pct))
    except (TypeError, ValueError) as exc:
        return {
            "action": "hold_for_narrowing_review",
            "reason": f"invalid Minato or margin evidence: {exc}",
            "input_actions": {},
            "fallback": {"action": "hold_for_narrowing_review"},
            "stage5_transition": "hold_disabled",
        }

    score_check = evaluate_narrow_fallback(
        narrow_runs_completed=0,
        latest_score=effective_latest,
        previous_score=effective_previous,
        latest_valid=latest_score_valid,
        fallback_eligible=False,
    )
    if score_check["action"] == "hold_for_narrowing_review":
        return {
            "action": "hold_for_narrowing_review",
            "reason": score_check["reason"],
            "effective_latest_score": effective_latest,
            "effective_previous_score": effective_previous,
            "input_actions": {},
            "narrow_runs_by_input": input_run_counts,
            "fallback": score_check,
            "stage5_transition": "hold_disabled",
        }

    input_actions: dict[str, dict] = {}
    try:
        for name, spec in inputs.items():
            range_spec = {key: value for key, value in spec.items()
                          if key != "narrow_runs_completed"}
            input_actions[name] = evaluate_input_range(**range_spec)
            input_actions[name]["narrow_runs_completed"] = input_run_counts[name]
    except (TypeError, ValueError) as exc:
        return {
            "action": "hold_invalid_input_domain_or_range",
            "reason": str(exc),
            "effective_latest_score": effective_latest,
            "effective_previous_score": effective_previous,
            "input_actions": input_actions,
            "narrow_runs_by_input": input_run_counts,
            "fallback": score_check,
            "stage5_transition": "hold_disabled",
        }

    invalid_domain_actions = {
        name: result for name, result in input_actions.items()
        if result["action"] == "hold_unknown_domain"
    }
    if invalid_domain_actions:
        return {
            "action": "hold_invalid_input_domain_or_range",
            "reason": "authoritative valid domain is missing for one or more inputs",
            "effective_latest_score": effective_latest,
            "effective_previous_score": effective_previous,
            "input_actions": input_actions,
            "narrow_runs_by_input": input_run_counts,
            "fallback": score_check,
            "stage5_transition": "hold_disabled",
        }

    unresolved_inputs = {
        name: result for name, result in input_actions.items()
        if result["action"] in {"shift", "keep_broad", "hold_domain_limit"}
    }
    domain_limit_inputs = [
        name for name, result in input_actions.items()
        if result["action"] == "hold_domain_limit"
    ]
    fallback_inputs = {
        name: result for name, result in unresolved_inputs.items()
        if input_run_counts[name] >= NARROW_FALLBACK_RUNS
    }
    pending_inputs = {
        name: result for name, result in unresolved_inputs.items()
        if input_run_counts[name] < NARROW_FALLBACK_RUNS
    }
    # Stage 4 can finish only when every unresolved input has independently
    # exhausted its own three narrow runs; a younger input blocks Stage 5.
    fallback_context = bool(unresolved_inputs) and not pending_inputs
    fallback = evaluate_narrow_fallback(
        narrow_runs_completed=max((input_run_counts[name] for name in fallback_inputs), default=0),
        latest_score=effective_latest,
        previous_score=effective_previous,
        latest_valid=latest_score_valid,
        fallback_eligible=fallback_context,
    )
    fallback["unresolved_inputs"] = sorted(unresolved_inputs)
    fallback["unresolved_narrow_runs_by_input"] = {
        name: input_run_counts[name] for name in sorted(unresolved_inputs)
    }
    fallback["fallback_inputs"] = sorted(fallback_inputs)
    fallback["pending_inputs"] = sorted(pending_inputs)
    fallback_selected = (
        bool(fallback_inputs) and not pending_inputs and
        fallback["action"] == "use_latest_best"
    )
    if fallback_selected:
        stage4_completed = True
        fallback["context"] = (
            "authoritative_domain_limit_reached"
            if any(name in domain_limit_inputs for name in fallback_inputs)
            else "unresolved_input_after_three_narrow_runs"
        )
        fallback["fallback_narrow_runs_by_input"] = {
            name: input_run_counts[name] for name in sorted(fallback_inputs)
        }
    if pending_inputs:
        stage4_completed = False
    stage5 = stage5_transition(stage4_completed, stage5_enabled)
    if fallback_selected:
        action = "select_latest_nonregressing_score"
    elif stage4_completed:
        action = "stage4_complete"
    else:
        action = "continue_stage4"
    return {
        "action": action,
        "effective_latest_score": effective_latest,
        "effective_previous_score": effective_previous,
        "input_actions": input_actions,
        "narrow_runs_by_input": input_run_counts,
        "fallback": fallback,
        "fallback_selected": fallback_selected,
        "pending_inputs": sorted(pending_inputs),
        "stage5_transition": stage5,
        "stage5_enabled": bool(stage5_enabled),
    }


def minato_with_margin_floor(score: float, min_margin_pct: float | None) -> float:
    """Return zero when minimum observed margin is missing or below 120%."""
    score = _finite(score, "score")
    if min_margin_pct is None or _finite(min_margin_pct, "min_margin_pct") < MARGIN_FLOOR_PCT:
        return 0.0
    return score


def evaluate_stage5_gates(*, forward_cf: float | None, stage4_cf: float | None,
                          profit: float | None, tpm: float | None,
                          drawdown_pct: float | None,
                          forward_trades: float | None,
                          optimization_trades: float | None,
                          min_margin_pct: float | None) -> dict:
    """Evaluate only confirmed Stage 5 forward gates; failures hold."""
    fields = {
        "forward_cf": forward_cf,
        "stage4_cf": stage4_cf,
        "profit": profit,
        "tpm": tpm,
        "drawdown_pct": drawdown_pct,
        "forward_trades": forward_trades,
        "optimization_trades": optimization_trades,
        "min_margin_pct": min_margin_pct,
    }
    missing = [name for name, value in fields.items() if value is None]
    values = {}
    invalid = []
    for name, value in fields.items():
        if value is None:
            continue
        try:
            values[name] = _finite(value, name)
        except (TypeError, ValueError, OverflowError):
            invalid.append(name)
    if "forward_cf" in values and "min_margin_pct" in values:
        effective_score = minato_with_margin_floor(
            values["forward_cf"], values["min_margin_pct"]
        )
    else:
        effective_score = 0.0
    checks = {
        "forward_cf_at_least_half_stage4": (
            "forward_cf" in values and "stage4_cf" in values and
            values["forward_cf"] >= STAGE5_FORWARD_CF_RATIO * values["stage4_cf"]
        ),
        "profit_positive": "profit" in values and values["profit"] > 0,
        "tpm_at_least_35": "tpm" in values and values["tpm"] >= STAGE5_MIN_TPM,
        "drawdown_at_most_25_pct": (
            "drawdown_pct" in values and values["drawdown_pct"] <= STAGE5_MAX_DRAWDOWN_PCT
        ),
        "trade_samples_positive": (
            "forward_trades" in values and "optimization_trades" in values and
            values["forward_trades"] > 0 and values["optimization_trades"] > 0
        ),
        "forward_trades_at_least_half_optimization": (
            "forward_trades" in values and "optimization_trades" in values and
            values["forward_trades"] > 0 and values["optimization_trades"] > 0 and
            values["forward_trades"] >= STAGE5_MIN_FORWARD_TRADE_RATIO * values["optimization_trades"]
        ),
        "minimum_margin_at_least_120_pct": (
            "min_margin_pct" in values and values["min_margin_pct"] >= MARGIN_FLOOR_PCT
        ),
    }
    failures = [name for name, passed in checks.items() if not passed]
    if missing:
        failures.extend(f"missing_{name}" for name in missing)
    if invalid:
        failures.extend(f"invalid_{name}" for name in invalid)
    passed = not failures
    if invalid:
        reason = "invalid non-finite or non-numeric metrics: " + ", ".join(sorted(invalid))
    elif missing:
        reason = "missing required metrics: " + ", ".join(sorted(missing))
    elif failures:
        reason = "one or more confirmed Stage 5 gates failed"
    else:
        reason = "all confirmed Stage 5 gates passed"
    return {
        "action": "record_pass" if passed else "hold",
        "passed": passed,
        "checks": checks,
        "failures": failures,
        "invalid_fields": sorted(invalid),
        "missing_fields": sorted(missing),
        "reason": reason,
        "effective_minato_score": effective_score,
        "auto_rotate": False,
        "code_repair": False,
        "deployment_authorized": False,
    }


def stage5_transition(stage4_completed: bool, stage5_enabled: bool) -> str:
    """Return the transition only after completion and explicit enablement."""
    if not stage4_completed:
        return "wait_for_stage4"
    if not stage5_enabled:
        return "hold_disabled"
    return "start_stage5"

