#!/usr/bin/env python3
"""Deterministically verify Stage 2 source structure and compile evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

from kcos_tester_config_v2 import (config_from_spec, config_sha256,
                                   validate_config_against_source,
                                   resolve_input_spec, is_timing_control_input)


def load(value) -> dict:
    if isinstance(value, dict):
        return value
    result = json.loads(Path(value).read_text(encoding="utf-8-sig"))
    if not isinstance(result, dict):
        raise ValueError("handoff must contain an object")
    return result


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def closed_bar_check(source: str) -> bool:
    local_newbar = bool(re.search(r"\bbool\s+NewBarClosed\s*\(", source))
    calls_newbar = bool(re.search(r"if\s*\(\s*!\s*NewBarClosed\s*\(\s*\)\s*\)\s*return", source))
    uses_current_signal_slot = bool(re.search(r"(?:close|mid|std)\w*\s*=\s*g_\w+_buf\[2\]", source, re.I))
    if "prior_upper" in source or "prior_lower" in source:
        bollinger_prior_ok = all(
            re.search(pattern, source) is not None
            for pattern in (
                r"(?:iClose\s*\([^;]*,\s*2\s*\)|CopyClose\s*\([^;]*,\s*2\s*,\s*1\s*,)",
                r"CopyBuffer\s*\(\s*g_bb_mid_handle\s*,\s*0\s*,\s*2\s*,\s*1\s*,",
                r"CopyBuffer\s*\(\s*g_bb_std_handle\s*,\s*0\s*,\s*2\s*,\s*1\s*,",
                r"prior_upper\s*=\s*prior_mid\s*\+\s*InpBbStdDev\s*\*\s*prior_std",
                r"prior_lower\s*=\s*prior_mid\s*-\s*InpBbStdDev\s*\*\s*prior_std",
            )
        )
    else:
        bollinger_prior_ok = True
    local_bar_helper_ok = (not local_newbar) or (
        re.search(r"bool\s+NewBarClosed\s*\(\s*\)\s*\{[\s\S]{0,500}?Bars\s*\(\s*_Symbol\s*,\s*PERIOD_CURRENT\s*\)[\s\S]{0,500}?g_prevBars[\s\S]{0,500}?return\s+true\s*;", source, re.I) is not None
    )
    return calls_newbar and local_bar_helper_ok and not uses_current_signal_slot and bollinger_prior_ok


def executable_source(source: str) -> str:
    """Remove comments so policy tokens cannot be satisfied by prose."""
    source = re.sub(r"/\*.*?\*/", "", source, flags=re.S)
    return re.sub(r"//[^\r\n]*", "", source)


def zero_disables_time_stop_controls(source: str) -> bool:
    """Require explicit positive guards on time-stop expiry comparisons."""
    control_names = []
    declaration_re = re.compile(
        r"(?im)^\s*input\s+[A-Za-z_]\w*\s+([A-Za-z_]\w*)\s*=\s*([^;]+);"
    )
    for match in declaration_re.finditer(source):
        declaration = match.group(0)
        name = match.group(1)
        comment = declaration.split("//", 1)[1] if "//" in declaration else ""
        if is_timing_control_input(name, {"description": comment}) == "time_stop":
            control_names.append(name)
    if not control_names:
        return True
    code = executable_source(source)
    for name in control_names:
        conditions = re.findall(r"\bif\s*\(([^;{}]{1,800})\)", code, re.I)
        uses = [condition for condition in conditions
                if re.search(rf"\b{re.escape(name)}\b", condition)]
        expiry_checks = [condition for condition in uses
                         if re.search(rf"\b{re.escape(name)}\b\s*(?:>=|>)", condition)
                         or re.search(rf"(?:>=|>)\s*\b{re.escape(name)}\b", condition)]
        if not expiry_checks:
            return False
        zero_guard = re.compile(
            rf"\b{re.escape(name)}\b\s*(?:>|>=|!=)\s*(?:0(?:\.0*)?|1)\s*&&", re.I
        )
        if any(not zero_guard.search(condition) for condition in expiry_checks):
            return False
    return True


def _normalized_expression(value: str) -> str:
    return re.sub(r"\s+", "", value).strip("()")


def regime_parameters_used(source: str, code: str) -> bool:
    """Require the declared regime surface to affect executable logic.

    Diagnostic2 intentionally uses one bounded regime selector rather than the
    legacy 12-field regime surface. Accept that explicit form only when its
    validator and both regime-derived controls are present.
    """
    declared = sorted(set(re.findall(r"\bInpRegime_[A-Za-z0-9_]+", source)))
    executable = re.sub(r"^\s*input\b[^\r\n]*$", "", code, flags=re.I | re.M)
    if len(declared) >= 12:
        return all(len(re.findall(rf"\b{re.escape(name)}\b", executable)) > 0 for name in declared)
    diagnostic2 = (
        re.search(r"\binput\s+int\s+InpRegime\s*=\s*0\s*;", source) is not None
        and re.search(r"\bbool\s+RegimeParametersValid\s*\(", source) is not None
        and re.search(r"\bdouble\s+RegimeMarginThreshold\s*\(", source) is not None
        and re.search(r"\bint\s+RegimeTakeProfitPoints\s*\(", source) is not None
        and len(re.findall(r"\bInpRegime\b", executable)) >= 2
    )
    return diagnostic2


def every_order_path_is_safe(code: str, diagnostics: list[dict] | None = None) -> bool:
    """Recognize a bounded quantity/refusal/normalization/margin/order flow."""
    scan_code = _mask_comments_and_strings(code)
    prohibited_tp_override = bool(re.search(
        r'\b(?!(?:return|new|delete)\b)[A-Za-z_]\w*\s+(?:[*&]\s*)?MinatoTakeProfit\s*\('
        r'|#\s*(?:define|undef)\s+MinatoTakeProfit\b', scan_code))
    # Accept standalone calls and both MQL5 conditional forms:
    # `if(!trade.Buy(...)) return;` and `if(trade.Buy(...)) { ... }`.
    # The closing parenthesis of an if condition follows the call's own
    # closing parenthesis; the rest of this validator still checks the same
    # quantity, normalization, margin, TP, scope, and submitted-value
    # invariants for every recognized call.
    orders = list(re.finditer(
        r"\.\s*(Buy|Sell)\s*\(([^;]*?)\)\s*(?:;|\{|\)\s*(?:return\s*;|\{))",
        scan_code, re.I | re.S))
    if not orders:
        return False
    # Ignore quoted brace tokens when checking whether a recognized guard
    # dominates the order in the same lexical block.
    masked = scan_code
    def scope_at(position):
        stack = []
        for index, char in enumerate(masked[:position]):
            if char == '{':
                stack.append(index)
            elif char == '}':
                if not stack:
                    return None
                stack.pop()
        return tuple(stack)
    def direct_statement(position, scope, ancestor=False):
        before = masked[:position].rstrip()
        current = scope_at(position)
        # A statement in the order's block may be nested in a guard block;
        # its scope stack must begin with the order scope.
        dominates = current == scope or (ancestor and current is not None and scope is not None
                                         and current[:len(scope)] == scope)
        return dominates and (not before or before[-1] in '{;}')
    def writes(text, name):
        return bool(re.search(rf'\b{re.escape(name)}\s*(?:[+\-*/%&|^]?=(?!=)|\+\+|--)'
                              rf'|(?:\+\+|--)\s*\b{re.escape(name)}\b', text))
    safe_value_helpers = {'freemarginallowsnotional'} if free_margin_notional_helper_is_safe(scan_code) else set()
    def unknown_reference_call(text, name):
        calls = re.finditer(rf'(?=\b([A-Za-z_]\w*)\s*\([^;{{}}]*\b{re.escape(name)}\b[^;{{}}]*\))', text)
        # These verified value-only helpers cannot mutate order values.
        return any(call[1].casefold() not in {'if', 'switch', 'normalizedouble', 'buy', 'sell'} | safe_value_helpers for call in calls)
    for order in orders:
        side = order.group(1).upper()
        args = [item.strip() for item in order.group(2).split(',')]
        if not args:
            if diagnostics is not None:
                diagnostics.append({"code": "ORDER_VOLUME_UNIDENTIFIABLE",
                                    "reason": "order call has no submitted volume argument"})
            return False
        volume = _normalized_expression(args[0])
        if not re.fullmatch(r'[A-Za-z_]\w*', volume):
            if diagnostics is not None:
                diagnostics.append({"code": "ORDER_VOLUME_UNIDENTIFIABLE",
                                    "reason": "submitted order volume argument is not a single identifier"})
            return False
        # Bound analysis to the enclosing function so a same-named volume or
        # another order in a previous entry function cannot contaminate checks.
        function_starts = list(re.finditer(
            r"\b(?:void|bool|int|double|long|ulong|datetime|ENUM_[A-Za-z0-9_]+)\s+"
            r"[A-Za-z_]\w*\s*\([^;{}]*\)\s*\{",
            scan_code[:order.start()], re.I | re.S))
        base = function_starts[-1].end() - 1 if function_starts else max(0, order.start() - 1600)
        window = scan_code[base:order.start()]
        scope = scope_at(order.start())
        quantity = list(re.finditer(
            rf'(?:\bdouble\s+)?\b{re.escape(volume)}\s*=\s*MinatoRegimeQuantity\s*\([^;]+;',
            window, re.I | re.S))
        quantity = quantity[-1] if quantity else None
        if quantity is None:
            if diagnostics is not None:
                diagnostics.append({"code": "ORDER_QUANTITY_UNIDENTIFIABLE",
                                    "side": side, "volume_variable": volume,
                                    "reason": "no matching MinatoRegimeQuantity assignment was identifiable before this order"})
            return False
        search_start = quantity.end() if quantity is not None else 0
        normalizers = list(re.finditer(
            rf'\b{re.escape(volume)}\s*=\s*NormalizeLot\s*\(\s*{re.escape(volume)}\s*\)\s*;',
            window[search_start:], re.I))
        normalizer_spans = [(search_start + match.start(), search_start + match.end()) for match in normalizers]
        guards = list(re.finditer(
            r'if\s*\(\s*!\s*MinatoAllowsProjectedMargin\s*\(\s*ORDER_TYPE_(BUY|SELL)\s*,'
            r'\s*([^,]+),\s*([^\)]+)\)\s*\)', window, re.I | re.S))
        submitted_price = _normalized_expression(args[2]) if len(args) > 2 else ""
        guard_price = submitted_price
        # Some source proposals capture the exact tick quote once as
        # `entryPrice = ask/bid`, run both guards on that same quote, and pass
        # the captured value to CTrade. Admit only this proven one-hop alias;
        # reject arbitrary expressions or values that can be changed later.
        if re.fullmatch(r'[A-Za-z_]\w*', submitted_price) and guards:
            candidate = guards[-1]
            candidate_end = _simple_refusal_end(window, candidate.end())
            alias_rows = list(re.finditer(
                rf'(?:\bdouble\s+)?\b{re.escape(submitted_price)}\s*=\s*(ask|bid)\s*;',
                window, re.I))
            expected_quote = 'ask' if side == 'BUY' else 'bid'
            if (len(alias_rows) == 1 and candidate_end is not None
                    and alias_rows[0].start() >= candidate_end
                    and alias_rows[0].group(1).casefold() == expected_quote
                    and direct_statement(base + alias_rows[0].start(), scope, ancestor=True)
                    and not writes(window[alias_rows[0].end():], submitted_price)):
                guard_price = alias_rows[0].group(1)
        parseable_price = bool(re.fullmatch(r'[A-Za-z_]\w*', submitted_price))
        matching_guard = None
        matching_guard_end = None
        if parseable_price and guards:
            candidate_guard = guards[-1]
            candidate_end = _simple_refusal_end(window, candidate_guard.end())
            if (candidate_guard.group(1).upper() == side
                    and _normalized_expression(candidate_guard.group(2)) == volume
                    and _normalized_expression(candidate_guard.group(3)) == guard_price
                    and candidate_end is not None
                    and direct_statement(base + candidate_guard.start(), scope)):
                matching_guard = candidate_guard
                matching_guard_end = candidate_end
        if matching_guard is None:
            if diagnostics is not None:
                diagnostics.append({"code": "ORDER_PRICE_GUARD_MISMATCH", "side": side,
                                    "submitted_price": submitted_price,
                                    "reason": "projected-margin guard must use the exact price identifier submitted to Buy/Sell; aliases are not admitted"})
            return False
        helper_ok = normalize_lot_helper_is_authenticated(scan_code)
        normalizer_before_margin = (matching_guard is None or
                                    (len(normalizer_spans) == 1 and normalizer_spans[0][0] < matching_guard.start()))
        if len(normalizer_spans) != 1 or not helper_ok or not normalizer_before_margin:
            span_start = quantity.start() if quantity is not None else 0
            span_end = matching_guard_end if matching_guard is not None else len(window)
            span_kind = ("quantity_to_margin_refusal" if matching_guard is not None
                         else "quantity_to_order_fallback")
            relevant = window[span_start:span_end]
            misplaced = normalizer_spans[0] if len(normalizer_spans) == 1 and matching_guard is not None \
                and normalizer_spans[0][0] >= matching_guard.start() else None
            if diagnostics is not None:
                diagnostics.append({
                    "code": "NORMALIZATION_NOT_RECOGNIZED",
                    "side": side,
                    "volume_variable": volume,
                    "source_line_start": code.count("\n", 0, base + span_start) + 1,
                    "source_line_end": code.count("\n", 0, base + span_end) + 1,
                    "quantity_to_margin_sha256": hashlib.sha256(
                        re.sub(r"\s+", " ", relevant).strip().encode("utf-8")).hexdigest().upper(),
                    "span_kind": span_kind,
                    "normalizer_line_start": (code.count("\n", 0, base + misplaced[0]) + 1) if misplaced else None,
                    "normalizer_line_end": (code.count("\n", 0, base + misplaced[1]) + 1) if misplaced else None,
                    "helper_authenticated": helper_ok,
                    "normalizer_call_count": len(normalizer_spans),
                    "matching_margin_guard_found": matching_guard is not None,
                })
            return False
        if len(args) < 5:
            if diagnostics is not None:
                diagnostics.append({"code": "ORDER_TP_PRICE_UNIDENTIFIABLE", "side": side,
                                    "reason": "Buy/Sell must submit both a verified stop and take-profit price"})
            return False
        price, tp = guard_price, _normalized_expression(args[4])
        if not all(re.fullmatch(r'[A-Za-z_]\w*', value) for value in (price, tp)):
            if diagnostics is not None:
                diagnostics.append({"code": "ORDER_TP_PRICE_UNIDENTIFIABLE", "side": side,
                                    "submitted_sl": args[3], "submitted_tp": args[4],
                                    "reason": "each order must submit the same calculated TP and SL variables; zero-price fallback order paths are not admitted"})
            return False
        if not guards:
            return False
        guard = guards[-1]
        guard_end = _simple_refusal_end(window, guard.end())
        if (guard_end is None or guard.group(1).upper() != side
                or _normalized_expression(guard.group(2)) != volume
                or _normalized_expression(guard.group(3)) != price
                or not direct_statement(base + guard.start(), scope)):
            return False
        notional_guards = list(re.finditer(
            r'if\s*\(\s*!\s*FreeMarginAllowsNotional\s*\(\s*([^,]+),\s*([^\)]+)\)\s*\)',
            window, re.I))
        valid_notional_guard = False
        if free_margin_notional_helper_is_safe(scan_code):
            for check in notional_guards:
                end = _simple_refusal_end(window, check.end())
                if (end is not None and check.start() >= guard_end and end <= order.start() - base
                        and _normalized_expression(check.group(1)) == volume
                        and _normalized_expression(check.group(2)) == price
                        and direct_statement(base + check.start(), scope)):
                    valid_notional_guard = True
                    break
        if not valid_notional_guard:
            if diagnostics is not None:
                diagnostics.append({"code": "ORDER_MARGIN_NOTIONAL_MISSING", "side": side,
                                    "volume_variable": volume, "price_variable": price,
                                    "reason": "the order path needs a direct FreeMarginAllowsNotional(volume, price) refusal after the canonical projected-margin guard"})
            return False
        # The order must use the very values approved by both margin checks.
        # Even unknown reference-taking calls mentioning those values are
        # held rather than assumed harmless.
        after_guard = window[guard_end:]
        assignment_pattern = re.compile(
            rf'(?:\bdouble\s+)?\b{re.escape(tp)}\s*=\s*MinatoTakeProfit\s*\(',
            re.I,
        )
        assignments = []
        for candidate in assignment_pattern.finditer(window):
            closing = _balanced_call_end(window, candidate.end() - 1)
            if closing is None:
                continue
            terminator = re.match(r'\s*;', window[closing + 1:])
            if terminator is None:
                continue
            arguments_text = window[candidate.end():closing]
            arguments = _split_top_level_arguments(arguments_text)
            if (arguments is not None and len(arguments) == 4
                    and all(argument for argument in arguments)):
                assignments.append((candidate, arguments, closing + 1 + terminator.end(), arguments_text))
        if (not assignments
                or not direct_statement(base + assignments[-1][0].start(), scope, ancestor=True)):
            if diagnostics is not None:
                diagnostics.append({"code": "ORDER_TP_CALCULATION_UNAUTHENTICATED", "side": side,
                                    "reason": "assign the submitted TP from MinatoTakeProfit(entry, ATR*InpTP_ATR_Mult, the active regime TP multiplier, is_buy) after both canonical margin refusals"})
            return False
        tp_assignment, tp_arguments, tp_assignment_end, tp_arguments_text = assignments[-1]
        # Nested calls in TP arguments are supported only when they do not
        # receive or mutate the live order price or take-profit output.
        if (writes(tp_arguments_text, price) or writes(tp_arguments_text, tp)
                or unknown_reference_call(tp_arguments_text, price)
                or unknown_reference_call(tp_arguments_text, tp)):
            return False
        # The authenticated canonical global call takes values, not references.
        # Exclude only its concrete four-argument assignment, never a method,
        # overload, macro, or other invocation merely sharing the helper name.
        price_scan = after_guard
        if tp_assignment.start() >= guard_end:
            begin, end = tp_assignment.start() - guard_end, tp_assignment_end - guard_end
            price_scan = price_scan[:begin] + ' ' * (end - begin) + price_scan[end:]
        if (writes(after_guard, volume) or writes(after_guard, price)
                or unknown_reference_call(after_guard, volume)
                or unknown_reference_call(price_scan, price)):
            return False
        if not quantity:
            return False
        if quantity.end() >= guard.start() or not direct_statement(base + quantity.start(), scope, ancestor=True):
            return False
        def normalization_failure(helper_ok: bool, normalizer_count: int,
                                  normalizer_span: tuple[int, int] | None = None) -> bool:
            guard_authenticated = matching_guard is not None
            snippet_end = guard_end if guard_authenticated else len(window)
            relevant = window[quantity.start():snippet_end]
            if diagnostics is not None:
                diagnostics.append({
                    "code": "NORMALIZATION_NOT_RECOGNIZED",
                    "side": side,
                    "volume_variable": volume,
                    "source_line_start": code.count("\n", 0, base + quantity.start()) + 1,
                    "source_line_end": code.count("\n", 0, base + snippet_end) + 1,
                    "quantity_to_margin_sha256": hashlib.sha256(
                        re.sub(r"\s+", " ", relevant).strip().encode("utf-8")).hexdigest().upper(),
                    "span_kind": ("quantity_to_margin_refusal" if guard_authenticated
                                  else "quantity_to_order_fallback"),
                    "normalizer_line_start": (code.count("\n", 0, base + normalizer_span[0]) + 1)
                        if normalizer_span else None,
                    "normalizer_line_end": (code.count("\n", 0, base + normalizer_span[1]) + 1)
                        if normalizer_span else None,
                    "helper_authenticated": helper_ok,
                    "normalizer_call_count": normalizer_count,
                    "matching_margin_guard_found": guard_authenticated,
                })
            return False
        normalizers = list(re.finditer(
            rf'\b{re.escape(volume)}\s*=\s*NormalizeLot\s*\(\s*{re.escape(volume)}\s*\)\s*;',
            window[quantity.end():], re.I))
        helper_ok = normalize_lot_helper_is_authenticated(scan_code)
        if len(normalizers) != 1 or not helper_ok:
            norm_span = ((quantity.end() + normalizers[0].start(), quantity.end() + normalizers[0].end())
                         if len(normalizers) == 1 else None)
            return normalization_failure(helper_ok, len(normalizers), norm_span)
        # The approved quantity helper scales volume; it does not normalize.
        # Require explicit normalization of that volume before margin approval.
        n_start, n_end = guard.start(), quantity.end()
        if normalizers:
            normalization = normalizers[0]
            n_start, n_end = quantity.end() + normalization.start(), quantity.end() + normalization.end()
            if n_end >= guard.start() or not direct_statement(base + n_start, scope, ancestor=True):
                return normalization_failure(helper_ok, len(normalizers), (n_start, n_end))
        post_normalization_refusals = []
        for zero in re.finditer(rf'if\s*\(\s*{re.escape(volume)}\s*<=\s*0(?:\.0+)?\s*\)',
                                window[n_end:guard.start()], re.I):
            z_start = n_end + zero.start()
            z_end = _simple_refusal_end(window, n_end + zero.end())
            if (z_end is not None and z_end <= guard.start()
                    and direct_statement(base + z_start, scope)):
                post_normalization_refusals.append((z_start, z_end))
        if not post_normalization_refusals:
            return False
        zeros = list(re.finditer(rf'if\s*\(\s*{re.escape(volume)}\s*<=\s*0(?:\.0+)?\s*\)',
                                 window[quantity.end():n_start], re.I))
        valid_zero = None
        for zero in zeros:
            z_start, z_condition_end = quantity.end() + zero.start(), quantity.end() + zero.end()
            z_end = _simple_refusal_end(window, z_condition_end)
            if z_end is not None and z_end <= n_start and direct_statement(base + z_start, scope, ancestor=True):
                valid_zero = (z_start, z_end)
                break
        if valid_zero is None:
            return False
        z_start, z_end = valid_zero
        if writes(window[quantity.end():z_start] + window[z_end:n_start], volume):
            return False
        # Remove only recognized post-normalization refusal blocks before
        # checking for intervening writes and unknown reference-taking calls.
        after_normalization = window[n_end:guard.start()]
        removals = [(start - n_end, end - n_end) for start, end in post_normalization_refusals]
        for start, end in reversed(removals):
            after_normalization = after_normalization[:start] + after_normalization[end:]
        if writes(after_normalization, volume):
            return False
        # Exclude only the already-proven refusal and normalization spans.
        # Unknown calls in the remaining approved path might take references
        # and change the canonical quantity before its margin check.
        remaining = window[quantity.end():guard.start()]
        excluded = [(z_start, z_end)] + [(n_end + start, n_end + end) for start, end in removals]
        if normalizers:
            excluded.append((n_start, n_end))
        for start, end in sorted(excluded, reverse=True):
            start, end = start - quantity.end(), end - quantity.end()
            remaining = remaining[:start] + ' ' * (end - start) + remaining[end:]
        if unknown_reference_call(remaining, volume):
            return False
        tp_tail = window[tp_assignment_end:]
        # Permit only direct, value-only normalization of the already
        # calculated TP. Other post-calculation writes and custom calls stay
        # rejected so the submitted TP remains the authenticated value.
        tp_normalization = re.compile(
            rf'(?:if\s*\(\s*{re.escape(tp)}\s*>\s*0(?:\.0+)?\s*\)\s*)?'
            rf'\b{re.escape(tp)}\s*=\s*NormalizeDouble\s*\(\s*{re.escape(tp)}\s*,\s*_Digits\s*\)\s*;',
            re.I,
        )
        normalizations = list(tp_normalization.finditer(tp_tail))
        if len(normalizations) > 1:
            return False
        if normalizations:
            item = normalizations[0]
            if not direct_statement(base + tp_assignment_end + item.start(), scope, ancestor=True):
                return False
            start, end = item.span()
            tp_tail = tp_tail[:start] + ' ' * (end - start) + tp_tail[end:]
        if writes(tp_tail, tp) or unknown_reference_call(tp_tail, tp):
            return False
    return not prohibited_tp_override


def live_margin_path_is_safe(code: str) -> bool:
    return bool(re.search(
        r"AccountInfoDouble\s*\(\s*ACCOUNT_MARGIN_LEVEL\s*\)[\s\S]{0,500}?"
        r"if\s*\([^)]*<\s*InpMarginKillLevel[^)]*\)[\s\S]{0,500}?"
        r"(?:PositionClose\s*\([^;]+;|CloseAllPositions\s*\(\s*\);)[\s\S]{0,250}?return\s*;",
    code, re.I)) or bool(re.search(r"if\s*\(\s*MarginBreached\s*\(\s*\)\s*\)[\s\S]{0,300}?CloseAllPositions\s*\(\s*\);[\s\S]{0,300}?return\s*;", code, re.I))


def diagnostic2_order_path_is_safe(code: str) -> bool:
    """Recognize Diagnostic2's explicit SafeBuy execution chain."""
    body = re.search(r"bool\s+SafeBuy\s*\([^)]*\)\s*\{([\s\S]*?)\n\}", code, re.I)
    if body is None:
        return False
    text = body.group(1)
    required = ("MarginKillAndRefuse()", "SafeVolume(", "MarginSafeForBuy(",
                "SafeBuyTakeProfit(", "OrderCheck(", "trade.Buy(")
    if not all(token in text for token in required):
        return False
    return all(re.search(pattern, text, re.I) is not None for pattern in (
        r"!\s*SafeVolume\s*\([^)]*\)[\s\S]{0,180}?return\s+false",
        r"MarginSafeForBuy\s*\([^)]*\)[\s\S]{0,120}?return\s+false",
        r"!\s*SafeBuyTakeProfit\s*\([^)]*\)[\s\S]{0,120}?return\s+false",
        r"!\s*OrderCheck\s*\([^)]*\)[\s\S]{0,100}?return\s+false",
    ))


def diagnostic2_live_margin_path_is_safe(code: str) -> bool:
    """Recognize Diagnostic2's wrapped live margin close/refuse path."""
    return all(token in code for token in (
        "AccountInfoDouble(ACCOUNT_MARGIN_LEVEL)", "bool MarginKillActive()",
        "bool CloseOwnPositions()", "bool MarginKillAndRefuse()",
        "CloseOwnPositions();", "return true;",
    )) and re.search(
        r"bool\s+MarginKillAndRefuse\s*\(\s*\)\s*\{[\s\S]{0,900}?"
        r"if\s*\(\s*!\s*MarginKillActive\s*\(\s*\)\s*\)[\s\S]{0,120}?return\s+false;[\s\S]{0,500}?"
        r"CloseOwnPositions\s*\(\s*\);[\s\S]{0,300}?return\s+true;",
        code, re.I) is not None


def diagnostic2_closed_bar_only(source: str) -> bool:
    """Recognize Diagnostic2's renamed closed-bar helper and shift-1 reads."""
    return (
        "bool Diag2NewBarClosed()" in source and "bool IsNewBar()" in source
        and "return Diag2NewBarClosed();" in source
        and re.search(r"if\s*\(!IsNewBar\s*\(\s*\)\s*\)\s*return\s*;", source) is not None
        and "iClose(_Symbol, PERIOD_CURRENT, 1)" in source
        and "CopyBuffer(ema_fast_handle, 0, 1, 1" in source
        and "CopyBuffer(ema_slow_handle, 0, 1, 1" in source
    )


def generic_closed_bar_audit(source: str) -> bool:
    """Require a timestamp gate and reject live-bar signal reads."""
    timestamp_gate = re.search(r"datetime\s+\w+\s*=\s*iTime\s*\([^;]*,\s*1\s*\)", source, re.I)
    gate_call = re.search(r"if\s*\(!\s*\w*NewBarClosed\s*\(\s*\)\s*\)\s*return", source, re.I)
    live_ohlc = re.search(r"Copy(?:Open|High|Low|Close)\s*\([^;]*,\s*0\s*,", source, re.I)
    live_buffer = re.search(r"CopyBuffer\s*\([^;]*,\s*0\s*,\s*0\s*,", source, re.I)
    live_scalar = re.search(r"i(?:Open|High|Low|Close)\s*\([^;]*,\s*0\s*\)", source, re.I)
    return bool(timestamp_gate and gate_call and not (live_ohlc or live_buffer or live_scalar))


def cusum_order_path_is_safe(code: str) -> bool:
    """Recognize the CUSUM bounded execution chain."""
    body_buy = re.search(r"bool\s+SafeBuy\s*\([^)]*\)\s*\{([\s\S]*?)\n\}", code, re.I)
    body_sell = re.search(r"bool\s+SafeSell\s*\([^)]*\)\s*\{([\s\S]*?)\n\}", code, re.I)
    if body_buy is None or body_sell is None:
        return False
    buy, sell = body_buy.group(1), body_sell.group(1)
    required_buy = ("MarginKillAndRefuse()", "SafeVolume(", "MarginSafeForBuy(",
                    "SafeBuyTakeProfit(", "OrderCheck(", "trade.Buy(")
    required_sell = ("MarginKillAndRefuse()", "SafeVolume(", "MarginSafe(", "SafeTakeProfit(", "trade.Sell(")
    return all(token in buy for token in required_buy) and all(token in sell for token in required_sell) and \
        re.search(r"!\s*SafeVolume\s*\([^)]*\)[\s\S]{0,180}?return\s+false", buy, re.I) is not None and \
        re.search(r"!\s*OrderCheck\s*\([^)]*\)[\s\S]{0,140}?return\s+false", buy, re.I) is not None


def cusum_closed_bar_only(source: str) -> bool:
    """Require CUSUM to gate on a newly opened bar and read prior bars."""
    return (
        re.search(r"bool\s+IsNewBar\s*\(\s*\)", source) is not None
        and "Bars(_Symbol, PERIOD_CURRENT)" in source
        and re.search(r"iClose\s*\([^;]*,\s*1\s*\)", source) is not None
        and re.search(r"iClose\s*\([^;]*,\s*2\s*\)", source) is not None
        and re.search(r"CopyBuffer\s*\([^;]*,\s*1\s*,\s*1\s*,", source) is not None
        and re.search(r"if\s*\(!IsNewBar\s*\(\s*\)\s*\)\s*return", source) is not None
    )


def generated_source_review_is_current(handoff: dict, source_path: Path, spec_path: Path) -> bool:
    """Authenticate the exact generated source-admission proof and its lineage."""
    proof_ref = handoff.get("source_implementation_review")
    mode = handoff.get("source_implementation_mode")
    if mode not in {"model_reviewed", "model_generated_source_admitted"} and proof_ref is None:
        return True
    try:
        if not isinstance(proof_ref, dict): return False
        proof_path=Path(proof_ref.get("path") or "")
        proof_path.resolve().relative_to(source_path.resolve().parent)
        if not proof_path.is_file() or sha256(proof_path)!=str(proof_ref.get("sha256") or "").upper(): return False
        proof=load(proof_path)
        build_path=Path(handoff.get("source_build_receipt") or "")
        if not build_path.is_file() or sha256(build_path)!=str(handoff.get("source_build_receipt_sha256") or "").upper(): return False
        build=load(build_path)
        effective,resolution=resolve_input_spec(load(spec_path))
        for field,expected in (("effective_spec",effective),("input_resolution",resolution)):
            artifact=Path(proof.get(field) or "")
            artifact.resolve().relative_to(proof_path.resolve().parent)
            if not artifact.is_file() or sha256(artifact)!=str(proof.get(field+"_sha256") or "").upper() or load(artifact)!=expected:
                return False
        include=Path(handoff.get("canonical_minato") or "")
        if not include.is_file() or sha256(include)!=str(proof.get("canonical_minato_sha256") or "").upper(): return False
        identity=(proof.get("schema_version")==1 and proof.get("passed") is True
                  and proof.get("cycle_id")==handoff.get("cycle_id") and proof.get("ea")==handoff.get("ea")
                  and str(proof.get("source_spec_sha256") or "").upper()==sha256(spec_path)
                  and sha256(spec_path)==str(handoff.get("stage1_spec_sha256") or "").upper()
                  and str(proof.get("source_sha256") or "").upper()==sha256(source_path))
        if mode=="model_generated_source_admitted":
            identity=identity and proof.get("status")=="DETERMINISTIC_SOURCE_ADMISSION"
            build_proof=build.get("implementation_admission")
        else:
            identity=identity and bool(proof.get("reviewer_provider")) and bool(proof.get("reviewer_model"))
            build_proof=build.get("implementation_review")
        return bool(identity and build.get("implementation_mode")==mode
                    and build.get("cycle_id")==handoff.get("cycle_id") and build.get("candidate_ea")==handoff.get("ea")
                    and build_proof==proof_ref and str(build.get("sha256") or "").upper()==sha256(source_path)
                    and str(build.get("source_spec_sha256") or "").upper()==sha256(spec_path)
                    and Path(build.get("output") or "").resolve()==source_path.resolve()
                    and Path(build.get("source_spec") or "").resolve()==spec_path.resolve())
    except (OSError,ValueError,TypeError,KeyError):
        return False


def verify(handoff_value, compile_value=None) -> dict:
    handoff = load(handoff_value)
    source_path = Path(handoff["source"])
    ex5_path = Path(handoff["ex5"])
    minato_path = Path(handoff["canonical_minato"])
    source = source_path.read_text(encoding="utf-8-sig", errors="replace")
    current_source_sha = sha256(source_path)
    current_ex5_sha = sha256(ex5_path) if ex5_path.is_file() else None
    minato_source = minato_path.read_text(encoding="utf-8-sig")
    code = executable_source(source)
    stage1_spec_path = Path(handoff.get("stage1_spec", ""))
    stage1_event_path = Path(handoff.get("stage1_event", ""))
    stage1_event = load(stage1_event_path) if stage1_event_path.is_file() else {}
    stage1_lineage = (
        stage1_spec_path.is_file()
        and stage1_event_path.is_file()
        and sha256(stage1_spec_path) == handoff.get("stage1_spec_sha256")
        and stage1_event.get("cycle_id") == handoff.get("cycle_id")
        and Path(stage1_event.get("artifact", "")).resolve() == stage1_spec_path.resolve()
        and stage1_event.get("artifact_sha256") == sha256(stage1_spec_path)
    )
    stage1_spec = load(stage1_spec_path)
    stage1_config = config_from_spec(stage1_spec, source_text=source)
    spec_input_names = set((stage1_spec.get("tester_config") or {}).get("inputs", {}))
    atr_period_input = "InpAtrPeriod" if "InpAtrPeriod" in spec_input_names else "InpATRPeriod"
    approved_config_sha = config_sha256(stage1_config)
    approved_config_ok = handoff.get("approved_tester_config_sha256") == approved_config_sha
    checks = {
        "approved_tester_config": approved_config_ok,
        "stage1_lineage": stage1_lineage,
        "generated_source_review_current": generated_source_review_is_current(handoff, source_path, stage1_spec_path),
        "canonical_name": source_path.stem == handoff["ea"],
        "canonical_minato_hash": sha256(minato_path) == handoff["canonical_minato_sha256"],
        "score_version": handoff.get("score_version") == "MINATO_BASE_SCORE_V2.0",
        "minato_v2_include": "#include <minato_base_score_v2.mqh>" in source,
        "no_legacy_minato_include": "#include <minato_base_score.mqh>" not in source,
        "minato_v2_score_call": "MinatoV2ScoreFromTester()" in source,
        "minato_v2_tick_capture": "MinatoV2ObserveTick();" in source,
        "minato_v2_reset_and_config_gate": (
            "MinatoV2ResetCapture();" in source and "MinatoV2ConfigurationValid()" in source),
        "minato_v2_formula_contract": all(token in minato_source for token in (
            "MINATO_BASE_SCORE_V2_VERSION", "STAT_EQUITY_DDREL_PERCENT",
            "STAT_MIN_MARGINLEVEL", "ACCOUNT_EQUITY",
            "MinatoV2ScoreFromTester", "MinatoV2ObserveTick",
            "MinatoV2ResetCapture", "MinatoV2ConfigurationValid")),
        "minato_margin_gate": all(token in minato_source for token in (
            "MINATO_MIN_MARGIN_LEVEL", "120.0",
            "g_minato_v2_runtime_margin_breach")),
        "margin_kill_120": margin_kill_120(source, handoff),
        "every_order_path_margin_quantity_tp_safe": (
            every_order_path_is_safe(code) or diagnostic2_order_path_is_safe(code) or cusum_order_path_is_safe(code)),
        "live_margin_close_and_refuse": (
            live_margin_path_is_safe(code) or diagnostic2_live_margin_path_is_safe(code)),
        "regime_parameters": regime_parameters_used(source, code),
        "time_stop_zero_disables_close": zero_disables_time_stop_controls(source),
        "closed_bar_only": (
            closed_bar_check(source) or diagnostic2_closed_bar_only(source) or cusum_closed_bar_only(source) or generic_closed_bar_audit(source)),
        "gk_sample_update_after_closed_bar_gate": gk_sample_update_is_closed_bar_gated(source),
        "indicator_reads_exclude_forming_bar": closed_bar_indicator_reads_are_safe(source),
        "atr_uses_closed_period_buffer": atr_uses_authenticated_closed_buffer(
            source, atr_period_input,
            "PERIOD_M5" if stage1_spec.get("stage2_requirements", {}).get(
                "must_use_ethusd_m5_closed_bars") else "_Period"),
        "no_mt5_configuration": not any(token in source for token in ("terminal64.exe", "Tester\\", "Optimization=")),
    }
    compile_result = load(compile_value) if compile_value is not None else None
    if compile_result is not None:
        checks["compile_clean"] = (
            compile_result.get("cycle_id") == handoff["cycle_id"]
            and compile_result.get("ea") == handoff["ea"]
            and compile_result.get("exit_code") == 0
            and compile_result.get("errors") == 0
            and compile_result.get("warnings") == 0
            and bool(compile_result.get("fresh_ex5"))
            and compile_result.get("passed") is True
            and compile_result.get("mq5_sha256") == current_source_sha
            and compile_result.get("ex5_sha256") == current_ex5_sha
            and 1 <= int(compile_result.get("attempt", 0)) <= 5
        )

    # Stage-2/3 tester config must be normalized to the agreed fast INI shape.
    ini_path = handoff.get("ini")
    ini_normalize_report = {}
    if ini_path:
        ini_path_obj = Path(ini_path)
        if not ini_path_obj.is_file():
            checks["mt5_tester_config_normalized"] = False
        else:
            import normalize_opt_ini as _norm
            ini_normalize_report = _norm.normalize_opt_ini(ini_path_obj)
            checks["mt5_tester_config_normalized"] = _norm.is_normalized(ini_path_obj)
    else:
        # No INI attached: treat as passing (Stage-2 only enforces when present).
        checks["mt5_tester_config_normalized"] = True
    return {
        "schema_version": 2,
        "cycle_id": handoff["cycle_id"],
        "stage": 2,
        "ea": handoff["ea"],
        "score_version": handoff.get("score_version"),
        "passed": all(checks.values()),
        "checks": checks,
        "compile": compile_result,
        "ini_normalize_report": ini_normalize_report,
        "source": str(source_path.resolve()),
        "ex5": str(ex5_path.resolve()),
        "canonical_minato": str(minato_path.resolve()),
        "source_sha256": current_source_sha,
        "canonical_minato_sha256": sha256(minato_path),
        "stage1_spec": str(stage1_spec_path.resolve()),
        "stage1_spec_sha256": sha256(stage1_spec_path) if stage1_spec_path.is_file() else None,
        "approved_tester_config_sha256": approved_config_sha,
        "stage1_event": str(stage1_event_path.resolve()),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handoff", required=True, type=Path)
    parser.add_argument("--compile", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = verify(args.handoff, args.compile)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"passed": result["passed"], "output": str(args.output.resolve())}))
    return 0 if result["passed"] else 2


def normalize_lot_helper_is_authenticated(code: str) -> bool:
    """Require the single canonical helper with an ordered, bound dataflow."""
    code = _mask_comments_and_strings(code)
    declarations = []
    for occurrence in re.finditer(r"\bNormalizeLot\s*\(", code):
        opening = occurrence.end() - 1
        closing = _balanced_call_end(code, opening)
        if closing is None:
            continue
        terminator = closing + 1
        while terminator < len(code) and code[terminator].isspace():
            terminator += 1
        if terminator >= len(code) or code[terminator] not in ";{":
            continue
        boundary = max(code.rfind(";", 0, occurrence.start()),
                       code.rfind("{", 0, occurrence.start()),
                       code.rfind("}", 0, occurrence.start()))
        prefix = code[boundary + 1:occurrence.start()]
        prefix = re.sub(r"(?m)^\s*#.*(?:\r?\n|$)", " ", prefix).strip()
        declaration_prefix = re.fullmatch(
            r"(?:(?:static|inline|extern|const|virtual|explicit)\s+)*"
            r"(?:[A-Za-z_]\w*(?:::\w+)*\s+)*[A-Za-z_]\w*(?:::\w+)*\s*[*&]?\s*", prefix)
        if declaration_prefix and prefix.casefold() not in {
                "return", "if", "while", "for", "switch", "case", "sizeof"}:
            declarations.append((occurrence, terminator))
    if len(declarations) not in {1, 2} or re.search(r"#\s*(?:define|undef)\s+NormalizeLot\b", code):
        return False
    conditional_depth = 0
    for line in code[:declarations[0][0].start()].splitlines():
        directive = re.match(r"\s*#\s*(if|ifdef|ifndef|endif|else|elif)\b", line, re.I)
        if directive:
            keyword = directive[1].casefold()
            if keyword in {"if", "ifdef", "ifndef"}:
                conditional_depth += 1
            elif keyword == "endif":
                conditional_depth = max(0, conditional_depth - 1)
    if conditional_depth:
        return False
    signatures = list(re.finditer(r"\bdouble\s+NormalizeLot\s*\(\s*double\s+requestedVolume\s*\)\s*\{", code))
    if len(signatures) != 1:
        return False
    definitions = [item for item in declarations if code[item[1]] == "{"]
    prototypes = [item for item in declarations if code[item[1]] == ";"]
    if len(definitions) != 1 or len(prototypes) != len(declarations) - 1:
        return False
    if prototypes:
        prototype, prototype_terminator = prototypes[0]
        prototype_start = prototype.start()
        prefix_boundary = max(code.rfind(";", 0, prototype_start),
                              code.rfind("{", 0, prototype_start),
                              code.rfind("}", 0, prototype_start))
        prefix = code[prefix_boundary + 1:prototype_start].strip()
        prefix = re.sub(r"(?m)^\s*//[^\r\n]*", "", prefix).strip()
        prototype_text = code[prototype_start:prototype_terminator + 1]
        if (prefix != "double"
                or not re.fullmatch(r"NormalizeLot\s*\(\s*double\s+requestedVolume\s*\);", prototype_text)
                or prototype.start() >= signatures[0].start()):
            return False
    brace_depth = 0
    for char in code[:signatures[0].start()]:
        if char == "{":
            brace_depth += 1
        elif char == "}":
            brace_depth -= 1
            if brace_depth < 0:
                return False
    if brace_depth != 0:
        return False
    opening = signatures[0].end() - 1
    depth, closing = 0, None
    for index in range(opening, len(code)):
        depth += (code[index] == "{") - (code[index] == "}")
        if depth == 0:
            closing = index
            break
    if closing is None:
        return False
    body = code[opening + 1:closing]
    canonical = """double minVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
double maxVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
double volumeStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
if (requestedVolume <= 0.0) return 0.0;
if (minVolume <= 0.0) return 0.0;
if (maxVolume < minVolume) return 0.0;
if (volumeStep <= 0.0) return 0.0;
double cappedVolume = MathMin(requestedVolume, maxVolume);
double flooredVolume = MathFloor(cappedVolume / volumeStep) * volumeStep;
if (flooredVolume < minVolume) return 0.0;
double normalizedVolume = NormalizeDouble(flooredVolume, 8);
if (normalizedVolume < minVolume) return 0.0;
if (normalizedVolume > maxVolume) return 0.0;
return normalizedVolume;"""
    return _mql_token_sequence(body) == _mql_token_sequence(canonical)


def canonical_initialization_hooks_present(code: str) -> bool:
    """Require capture reset, then fail-closed validation at the start of OnInit."""
    executable = _mask_comments_and_strings(code)
    body = _function_body(executable, "int", "OnInit")
    if body is None:
        return False
    return re.match(
        r"\s*MinatoV2ResetCapture\s*\(\s*\)\s*;\s*"
        r"if\s*\(\s*!\s*MinatoV2ConfigurationValid\s*\(\s*\)\s*\)\s*"
        r"return\s+INIT_PARAMETERS_INCORRECT\s*;",
        body,
    ) is not None


def margin_kill_120(source: str, handoff: dict) -> bool:
    """Preserve the legacy input branch or prove the approved constant branch.

    The new branch binds actual include bytes to both handoff and operator
    approval, then requires a concrete close-and-refuse flow using its 120%
    constant. No source mutation or strategy repair occurs here.
    """
    if re.search(r'InpMarginKillLevel\s*=\s*120(?:\.0+)?\s*;', source):
        return True  # Existing explicit-input behavior is unchanged.
    try:
        from kcos_runtime_paths_v2 import assert_safe_root, mt5_data_root
        include = mt5_data_root() / 'MQL5/Include/minato_base_score_v2.mqh'
        if Path(handoff.get('canonical_minato') or '').resolve() != include.resolve():
            return False
        raw = include.read_bytes()
        actual_hash = hashlib.sha256(raw).hexdigest().upper()
        approval_path = assert_safe_root() / 'state/contracts/minato_base_score_approved_v2.json'
        approval = load(approval_path)
        approved = approval.get('approved', bool(approval.get('score_include_sha256')))
        expected_hash = approval.get('sha256', approval.get('score_include_sha256'))
        if (approved is not True or actual_hash != str(expected_hash).upper()
                or actual_hash != str(handoff.get('canonical_minato_sha256')).upper()
                or approval.get('score_version') != 'MINATO_BASE_SCORE_V2.0'):
            return False
        include_code = executable_source(raw.decode('utf-8-sig'))
        definitions = re.findall(r'(?m)^\s*#\s*define\s+MINATO_MIN_MARGIN_LEVEL\s+([^\r\n]+)', include_code)
        if len(definitions) != 1 or not re.fullmatch(r'120(?:\.0+)?', definitions[0].strip()):
            return False
        code = executable_source(source)
        if '#include <minato_base_score_v2.mqh>' not in code:
            return False
        # No preprocessing or local shadow can reinterpret the proven flow.
        # Regime enum defines are local strategy constants and do not alter
        # the authenticated include's margin threshold or control flow.
        # Conditional compilation can hide the kill path, so it remains held.
        if re.search(r'(?m)^\s*#\s*(?:if|ifdef|ifndef|elif|else)\b', code):
            return False
        if re.search(r'\b(?:double|float|char|uchar|short|ushort|int|uint|long|ulong|bool|datetime|string|const)\s+MINATO_MIN_MARGIN_LEVEL\b', code):
            return False
        return _canonical_margin_wrapper_is_concrete(code)
    except (OSError, ValueError, TypeError, KeyError):
        return False


def _canonical_margin_wrapper_is_concrete(code: str) -> bool:
    """Recognize a bounded executable wrapper, not helper-name promises.

    This deliberately accepts one simple representation of account-margin
    refusal and a descending owned-position close loop. Other equivalent
    implementations remain held for review rather than guessed safe.
    """
    # Quoted text cannot provide executable evidence, including brace tokens.
    code = re.sub(r'"(?:\\.|[^"\\])*"', '""', code)
    margin = _function_body(code, 'bool', 'MarginBreached')
    close = _function_body(code, 'void', 'CloseAllPositions')
    tick = _function_body(code, 'void', 'OnTick')
    if margin is None or close is None or tick is None:
        return False
    compact = lambda text: re.sub(r'\s+', '', text)
    margin_match = re.fullmatch(
        r'double(?P<used>[A-Za-z_]\w*)=AccountInfoDouble\(ACCOUNT_MARGIN\);'
        r'double(?P<level>[A-Za-z_]\w*)=AccountInfoDouble\(ACCOUNT_MARGIN_LEVEL\);'
        r'return\(*(?P=used)>0(?:\.0+)?&&(?:MathIsValidNumber\((?P=level)\)&&)?'
        r'(?P=level)>0(?:\.0+)?&&(?P=level)<MINATO_MIN_MARGIN_LEVEL\)*;', compact(margin))
    if margin_match is None:
        return False
    if margin_match['used'] == margin_match['level']:
        return False
    close_code = compact(close)
    magic_setup = re.match(
        r'(?P<trade>[A-Za-z_]\w*)\.SetExpertMagicNumber\(\(ulong\)InpMagicNumber\);', close_code)
    if magic_setup:
        close_code = close_code[magic_setup.end():]
    loop = re.fullmatch(
        r'for\(int(?P<index>[A-Za-z_]\w*)=PositionsTotal\(\)-1;(?P=index)>=0;'
        r'(?:--(?P=index)|(?P=index)--)\)\{'
        r'ulong(?P<ticket>[A-Za-z_]\w*)=PositionGetTicket\((?P=index)\);'
        r'if\((?P=ticket)==0\)continue;'
        r'if\(PositionGetString\(POSITION_SYMBOL\)!=_Symbol\)continue;'
        r'if\(PositionGetInteger\(POSITION_MAGIC\)!=(?:\(long\))?InpMagicNumber\)continue;'
        r'(?P<trade>[A-Za-z_]\w*)\.PositionClose\((?P=ticket)\);}'
        r'(?:g_[A-Za-z_]\w*=(?:false|true|0(?:\.0+)?|(?:(?:MINATO|KCOS)_)?REGIME_[A-Za-z_]\w*);|g_entry_bar_index=-1;)*', close_code)
    if loop is None or (magic_setup and magic_setup['trade'] != loop['trade']):
        return False
    if not re.search(r'\bCTrade\s+' + re.escape(loop['trade']) + r'\s*;', code):
        return False
    return bool(re.match(
        r'MinatoV2ObserveTick\(\);if\(MarginBreached\(\)\)\{CloseAllPositions\(\);'
        r'(?:(?:g_[A-Za-z_]\w*)=(?:false|true|-1|0(?:\.0+)?|(?:(?:MINATO|KCOS)_)?REGIME_[A-Za-z_]\w*);)*return;\}',
        compact(tick)))


def _function_body(code: str, result_type: str, name: str) -> str | None:
    """Extract one parameterless definition; ambiguous definitions fail closed."""
    matches = list(re.finditer(r"\b" + result_type + r"\s+" + name +
                               r"\s*\(\s*\)\s*\{", code))
    if len(matches) != 1:
        return None
    start, depth = matches[0].end(), 1
    for index in range(start, len(code)):
        depth += (code[index] == '{') - (code[index] == '}')
        if depth == 0:
            return code[start:index]
    return None


def _simple_refusal_end(code: str, start: int) -> int | None:
    """Recognize bounded scalar bookkeeping followed by unconditional refusal.

    A braced body may assign literals or scalar identifiers and then return
    or continue. Calls, expressions, nested control flow, compound writes and
    statements after refusal remain unsupported. Unbraced refusal must be one
    immediate return/continue statement. This is a bounded recognizer, not a
    general MQL control-flow analyzer.
    """
    tail = code[start:start + 1024]
    scalar = r'(?:[A-Za-z_]\w*|[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|"(?:\\.|[^"\\])*"|D\'[^\'{}]*\')'
    terminal = r'(?:return(?:\s+' + scalar + r')?\s*;|continue\s*;)'
    if tail.lstrip().startswith('{'):
        body = re.match(r'\s*\{([^{}]{0,900})\}', tail, re.S)
        if body is None:
            return None
        assignments = r'(?:\s*[A-Za-z_]\w*\s*=\s*' + scalar + r'\s*;)*'
        if re.fullmatch(assignments + r'\s*' + terminal + r'\s*', body[1]) is None:
            return None
        return start + body.end()
    statement = re.match(r'\s*' + terminal, tail)
    return start + statement.end() if statement else None


def _mask_comments_and_strings(code: str) -> str:
    chars=list(code); i=0
    while i<len(code):
        if code.startswith('//',i):
            end=code.find('\n',i); end=len(code) if end<0 else end
            for k in range(i,end): chars[k]=' '
            i=end
        elif code.startswith('/*',i):
            end=code.find('*/',i+2); end=len(code) if end<0 else end+2
            for k in range(i,end):
                if code[k] not in '\r\n': chars[k]=' '
            i=end
        elif code[i] in ('"', "'"):
            q=code[i]; start=i; i+=1
            while i<len(code):
                if code[i]=='\\': i+=2; continue
                if code[i]==q: i+=1; break
                i+=1
            for k in range(start,min(i,len(code))):
                if code[k] not in '\r\n': chars[k]=' '
        else: i+=1
    return ''.join(chars)


def _balanced_call_end(code: str, opening: int) -> int | None:
    if opening>=len(code) or code[opening]!='(': return None
    depth=0; quote=None; escaped=False
    for i in range(opening,len(code)):
        c=code[i]
        if quote:
            if escaped: escaped=False
            elif c=='\\': escaped=True
            elif c==quote: quote=None
            continue
        if c in ('"',"'"): quote=c
        elif c=='(': depth+=1
        elif c==')':
            depth-=1
            if depth==0:return i
            if depth<0:return None
    return None


def _split_top_level_arguments(text: str) -> list[str] | None:
    result=[]; start=0; paren=bracket=brace=0; quote=None; escaped=False
    for i,c in enumerate(text):
        if quote:
            if escaped: escaped=False
            elif c=='\\': escaped=True
            elif c==quote: quote=None
            continue
        if c in ('"',"'"): quote=c
        elif c=='(': paren+=1
        elif c==')': paren-=1
        elif c=='[': bracket+=1
        elif c==']': bracket-=1
        elif c=='{': brace+=1
        elif c=='}': brace-=1
        elif c==',' and paren==bracket==brace==0:
            result.append(text[start:i].strip()); start=i+1
        if min(paren,bracket,brace)<0:return None
    if quote or paren or bracket or brace:return None
    result.append(text[start:].strip())
    return result


def _mql_token_sequence(value: str) -> list[str]:
    return re.findall(r'(?:[A-Za-z_]\w*|\d+(?:\.\d*)?(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?|==|!=|<=|>=|\+\+|--|\+=|-=|\*=|/=|&&|\|\||::|[^\s])',value)


def free_margin_notional_helper_is_safe(code: str) -> bool:
    masked = _mask_comments_and_strings(code)
    signature = re.search(r"bool\s+FreeMarginAllowsNotional\s*\([^)]*\)\s*\{", masked)
    if signature is None or "&" in signature.group(0) or "*" in signature.group(0):
        return False
    opening = signature.end() - 1
    depth = 0
    for index in range(opening, len(masked)):
        depth += (masked[index] == "{") - (masked[index] == "}")
        if depth == 0:
            body = masked[opening + 1:index]
            break
    else:
        return False
    compact = re.sub(r"\s+", "", body)
    if not re.search(r"if\(volume<=0(?:\.0+)?\|\|price<=0(?:\.0+)?\)returnfalse;", compact):
        return False
    notional = re.search(r"(?:double)?([A-Za-z_]\w*)=volume\*price;", compact)
    free = re.search(r"(?:double)?([A-Za-z_]\w*)=AccountInfoDouble\(ACCOUNT_MARGIN_FREE\);", compact)
    if notional is None or free is None or notional.group(1) == free.group(1):
        return False
    return re.search(r"return\(*" + re.escape(free.group(1)) + r">=2(?:\.0+)?\*" + re.escape(notional.group(1)) + r"\)*;", compact) is not None


def gk_sample_update_is_closed_bar_gated(source: str) -> bool:
    code=_mask_comments_and_strings(source); body=_function_body(code,"void","OnTick")
    if body is None:return False
    gate=re.search(r"if\s*\(\s*!\s*NewBarClosed\s*\(\s*\)\s*\)\s*return\s*;",body)
    updates=list(re.finditer(r"\b(?:PushGK\w*|UpdateGKWindow)\s*\(",body,re.I))
    return not updates or (gate is not None and all(x.start()>=gate.end() for x in updates))

def closed_bar_indicator_reads_are_safe(source: str) -> bool:
    return re.search(r"CopyBuffer\s*\([^;]*,\s*0\s*,\s*0\s*,",_mask_comments_and_strings(source),re.I) is None

def atr_uses_authenticated_closed_buffer(source: str, period_input: str = "InpATRPeriod",
                                         period_expression: str = "_Period") -> bool:
    code=_mask_comments_and_strings(source)
    created=re.search(r"\b([A-Za-z_]\w*)\s*=\s*iATR\s*\(\s*_Symbol\s*,\s*" + re.escape(period_expression) + r"\s*,\s*" + re.escape(period_input) + r"\s*\)",code,re.I)
    if created is None:return not re.search(r"\b(?:InpATRPeriod|InpAtrPeriod)\b|\biATR\s*\(",code,re.I)
    handle = re.escape(created.group(1))
    if re.search(rf"CopyBuffer\s*\(\s*{handle}\s*,\s*0\s*,\s*1\s*,\s*1\s*,",code,re.I):
        return True
    # Also accept a small authenticated wrapper when it is called with this
    # exact iATR handle and buffer 0, and the wrapper itself reads only shift 1.
    for call in re.finditer(rf"\b([A-Za-z_]\w*)\s*\(\s*{handle}\s*,\s*0\s*\)",code):
        helper_name = call.group(1)
        helper = re.escape(helper_name)
        signature = re.search(rf"\bdouble\s+{helper}\s*\(\s*int\s+handle\s*,\s*int\s+buffer\s*\)\s*\{{",code,re.I)
        if signature is None:
            continue
        start = signature.end()
        depth = 1
        end = None
        for index in range(start, len(code)):
            depth += (code[index] == "{") - (code[index] == "}")
            if depth == 0:
                end = index
                break
        if end is None:
            continue
        body = code[start:end]
        if (re.search(r"CopyBuffer\s*\(\s*handle\s*,\s*buffer\s*,\s*1\s*,\s*1\s*,",body,re.I)
                and re.search(r"return\s+[A-Za-z_]\w*\s*\[\s*0\s*\]\s*;",body)):
            return True
    return False


if __name__ == "__main__":
    raise SystemExit(main())
