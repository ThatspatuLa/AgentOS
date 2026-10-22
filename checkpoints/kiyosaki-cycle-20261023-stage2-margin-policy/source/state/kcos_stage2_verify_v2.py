#!/usr/bin/env python3
"""Deterministically verify Stage 2 source structure and compile evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

from kcos_tester_config_v2 import (config_from_spec, stage2_config_from_spec, config_sha256,
                                   validate_config_against_source,
                                   resolve_input_spec, is_timing_control_input,
                                   stage2_spec_from_spec)
from kcos_input_contract_v2 import discover_inputs


# Fixed output contract used by the bounded DCC recovery classifier. Recovery
# requires the full verifier run with a compile receipt and normalized-config
# check, not a self-consistent subset supplied by an artifact.
STAGE2_RECOVERY_REQUIRED_CHECK_KEYS = frozenset({
    "approved_tester_config", "timing_controls_zero_and_fixed", "stage1_lineage",
    "generated_source_review_current", "canonical_name", "canonical_minato_hash",
    "score_version", "minato_v2_include", "no_legacy_minato_include",
    "minato_v2_score_call", "minato_v2_tick_capture", "minato_v2_reset_and_config_gate",
    "minato_v2_formula_contract", "minato_margin_gate", "margin_kill_120",
    "every_order_path_margin_quantity_tp_safe", "live_margin_close_and_refuse",
    "regime_parameters", "time_stop_zero_disables_close", "cooldown_controls_absent",
    "closed_bar_only", "dcc_closed_return_arrays_bounded",
    "multidimensional_array_row_bounds_safe",
    "gk_sample_update_after_closed_bar_gate", "indicator_reads_exclude_forming_bar",
    "atr_uses_closed_period_buffer", "no_mt5_configuration", "compile_clean",
    "mt5_tester_config_normalized",
})


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


def spec_requires_dcc_garch(spec: dict) -> bool:
    """Read the strategy identity from an already authenticated Stage 1 spec."""
    selection = spec.get("primitive_selection")
    primitive = selection.get("primitive") if isinstance(selection, dict) else None
    primitive = primitive or spec.get("primitive") or ""
    return "dcc-garch" in str(primitive).casefold().replace("_", "-")



def multidimensional_array_row_bounds_are_safe(source: str) -> bool:
    """Reject ArraySize as a row bound for arrays declared with 2+ dimensions."""
    code = executable_source(source)
    names = set(re.findall(r"\b(?:double|float|int|long|bool|string)\s*(?:&\s*)?([A-Za-z_]\w*)\s*\[\s*\]\s*\[", code, re.I))
    names.update(re.findall(r"\b(?:double|float|int|long|bool|string)\s*&\s*([A-Za-z_]\w*)\s*\[\s*\]\s*\[", code, re.I))
    return not any(re.search(rf"\bArraySize\s*\(\s*{re.escape(name)}\s*\)", code, re.I) for name in names)

def dcc_closed_return_arrays_are_bounded(source: str, required_dcc: bool = False) -> bool:
    """Prove bounds for the recognized DCC/GARCH history dataflow.

    MQL5 source admission fails closed for DCC implementations outside this
    deliberately narrow shape. Every access is audited, then the exact loops,
    clamps, allocations and RollingZScore calls that justify those indices are
    checked. Returns omit shift 0 and use shifts 1/2, so their count is n-2.
    """
    code = executable_source(source)
    tracked_names = (
        "g_eth_close", "g_btc_close", "r_eth", "r_btc", "h_eth_hist",
        "h_btc_hist", "corr_hist", "vol_hist",
    )
    bound_names = (
        "eth_total", "btc_total", "n", "k", "seed_n", "qbar_n", "src",
        "vol_start", "InpDccLookback", "InpRegimeVolLookup",
    )
    for directive in re.findall(r"(?im)^\s*#\s*(?:define|undef)\b[^\r\n]*", code):
        if any(re.search(rf"\b{name}\b", directive, re.I) for name in tracked_names + bound_names):
            return False
    # Single-asset strategies may name a generic ETH return `r_eth`; that
    # identifier alone does not establish a DCC implementation. Trigger the
    # DCC-specific proof only from explicit DCC code/dataflow markers.
    dcc_marker = re.search(
        r"\b(?:DCC[_-]GARCH\w*|ComputeDCCSignals|g_eth_close|g_btc_close)\b",
        source, re.I,
    )
    if not required_dcc and not dcc_marker:
        return True
    if not dcc_marker:
        return False
    match = re.search(r"\bbool\s+ComputeDCCSignals\s*\([^{};]*\)\s*\{", code, re.I)
    if not match:
        return False
    oninit_signatures = list(re.finditer(r"\bint\s+OnInit\s*\(\s*\)\s*\{", code, re.I))
    if len(oninit_signatures) != 1:
        return False
    oninit_start = oninit_signatures[0].end()
    oninit_depth = 1
    oninit_end = oninit_start
    while oninit_end < len(code) and oninit_depth:
        oninit_depth += (code[oninit_end] == "{") - (code[oninit_end] == "}")
        oninit_end += 1
    if oninit_depth:
        return False
    oninit_body = code[oninit_start:oninit_end - 1]
    if (re.search(r"(?im)^\s*#\s*(?:if|ifdef|ifndef|elif|else|endif)\b", oninit_body)
            or re.search(r"\bgoto\b", oninit_body, re.I)
            or re.search(r"(?m)^\s*[A-Za-z_]\w*\s*:", oninit_body)):
        return False
    if re.search(
            r"(?im)^\s*#\s*(?:define|undef)\s+"
            r"(?:INIT_SUCCEEDED|INIT_FAILED|INIT_PARAMETERS_INCORRECT|INIT_AGENT_NOT_SUITABLE)\b",
            code):
        return False
    defined_macros = set(re.findall(r"(?im)^\s*#\s*define\s+([A-Za-z_]\w*)\b", code))
    if defined_macros and re.search(
            r"\b(?:" + "|".join(re.escape(name) for name in sorted(defined_macros)) + r")\b",
            oninit_body):
        return False
    returns = list(re.finditer(r"\breturn\s+([^;]+);", oninit_body, re.I))
    allowed_failures = {"INIT_FAILED", "INIT_PARAMETERS_INCORRECT", "INIT_AGENT_NOT_SUITABLE"}
    success_returns = []
    for item in returns:
        expression = re.sub(r"\s+", "", item.group(1)).casefold()
        if expression == "init_succeeded":
            success_returns.append(item)
        elif expression not in {value.casefold() for value in allowed_failures}:
            return False
    if not success_returns:
        return False
    for array in ("g_eth_close", "g_btc_close"):
        setups = list(re.finditer(rf"\bArraySetAsSeries\s*\(\s*{array}\s*,\s*true\s*\)\s*;", oninit_body, re.I))
        all_setups = list(re.finditer(rf"\bArraySetAsSeries\s*\(\s*{array}\s*,\s*[^)]*\)", code, re.I))
        if len(setups) != 1 or len(all_setups) != 1:
            return False
        setup = setups[0]
        depth = 0
        for char in oninit_body[:setup.start()]:
            depth += (char == "{") - (char == "}")
        prior_boundary = max(oninit_body.rfind(";", 0, setup.start()),
                             oninit_body.rfind("{", 0, setup.start()),
                             oninit_body.rfind("}", 0, setup.start()))
        statement_prefix = oninit_body[prior_boundary + 1:setup.start()]
        if (depth != 0 or any(setup.start() >= success.start() for success in success_returns)
                or re.search(r"\b(?:if|else|for|while|switch)\b", statement_prefix, re.I)):
            return False
    if oninit_end > match.start():
        return False
    start = match.end()
    depth = 1
    end = start
    while end < len(code) and depth:
        depth += (code[end] == "{") - (code[end] == "}")
        end += 1
    if depth:
        return False
    body = code[start:end - 1]
    compact = re.sub(r"\s+", "", body).casefold()
    calls = set(re.findall(r"\b([A-Za-z_]\w*)\s*\(", body))
    allowed_calls = {
        "if", "for", "CopyClose", "ArrayResize", "MathMin", "MathLog",
        "MathMax", "MathSqrt", "RollingZScore", "ClassifyRegime",
    }
    if calls - allowed_calls:
        return False
    required_fragments = (
        "intn=mathmin(eth_total,btc_total);",
        "if(n<inpdcclookback+10)returnfalse;",
        "arrayresize(r_eth,n-2);", "arrayresize(r_btc,n-2);",
        "for(intk=0;k<n-2;k++)",
        "intseed_n=inpdcclookback;",
        "if(seed_n>n-2)seed_n=n-2;",
        "for(intk=0;k<seed_n;k++){mean_eth+=r_eth[k];mean_btc+=r_btc[k];}",
        "for(intk=0;k<seed_n;k++)",
        "arrayresize(h_eth_hist,n-2);", "arrayresize(h_btc_hist,n-2);",
        "for(intk=0;k<n-2;k++)",
        "h_eth=omega_eth+alpha*r_eth[k]*r_eth[k]+beta*h_eth;",
        "h_btc=omega_btc+alpha*r_btc[k]*r_btc[k]+beta*h_btc;",
        "h_eth_hist[k]=h_eth;", "h_btc_hist[k]=h_btc;",
        "intqbar_n=mathmin(inpdcclookback,n-2);",
        "for(intk=0;k<qbar_n;k++)",
        "for(intk=qbar_n;k<n-2;k++)",
        "arrayresize(corr_hist,inpdcclookback);",
        "for(intk=0;k<inpdcclookback;k++)",
        "intsrc=k+1;", "if(src<n-2)",
        "z_corr=rollingzscore(corr_hist,inpdcclookback,inpdcclookback,corr);",
        "arrayresize(vol_hist,inpregimevollookup);",
        "intvol_start=(n-2)-inpregimevollookup;",
        "for(intk=0;k<inpregimevollookup;k++)",
        "intsrc=vol_start+k;", "if(src<0)src=0;", "if(src>=n-2)src=n-3;",
        "vol_hist[k]=mathsqrt(mathmax(h_eth_hist[src],1e-12));",
        "doublez_vol=rollingzscore(vol_hist,inpregimevollookup,inpregimevollookup,mathsqrt(mathmax(h_eth,1e-12)));",
    )
    if any(fragment not in compact for fragment in required_fragments):
        return False
    vol_loop_single_clamp = re.search(
        r"for\(intk=0;k<inpregimevollookup;k\+\+\)\{"
        r"intsrc=vol_start\+k;if\(src<0\)src=0;"
        r"if\(src>=n-2\)src=n-3;"
        r"vol_hist\[k\]=mathsqrt\(mathmax\(h_eth_hist\[src\],1e-12\)\);\}",
        compact,
    )
    vol_loop_redundant_lower_clamp = re.search(
        r"for\(intk=0;k<inpregimevollookup;k\+\+\)\{"
        r"intsrc=vol_start\+k;if\(src<0\)src=0;"
        r"if\(src>=n-2\)src=n-3;if\(src<0\)src=0;"
        r"vol_hist\[k\]=mathsqrt\(mathmax\(h_eth_hist\[src\],1e-12\)\);\}",
        compact,
    )
    if (vol_loop_single_clamp is None) == (vol_loop_redundant_lower_clamp is None):
        return False
    direct_return_loop = re.search(
        r"for\(intk=0;k<n-2;k\+\+\)\{r_eth\[k\]=mathlog\(g_eth_close\[k\+1\]\)-mathlog\(g_eth_close\[k\+2\]\);r_btc\[k\]=mathlog\(g_btc_close\[k\+1\]\)-mathlog\(g_btc_close\[k\+2\]\);\}",
        compact,
    )
    alias_return_loop = re.search(
        r"for\(intk=0;k<n-2;k\+\+\)\{"
        r"intidx_a=k\+1;intidx_b=k\+2;"
        r"if\(idx_a>=n\)idx_a=n-1;if\(idx_b>=n\)idx_b=n-1;"
        r"if\(idx_a<1\)idx_a=1;if\(idx_b<2\)idx_b=2;"
        r"r_eth\[k\]=mathlog\(g_eth_close\[idx_a\]\)-mathlog\(g_eth_close\[idx_b\]\);"
        r"r_btc\[k\]=mathlog\(g_btc_close\[idx_a\]\)-mathlog\(g_btc_close\[idx_b\]\);\}",
        compact,
    )
    if (direct_return_loop is None) == (alias_return_loop is None):
        return False
    corr_zero_fallback = re.search(
        r"if\(se<=0\.0\|\|sb<=0\.0\)\{corr_hist\[k\]=0\.0;continue;\}",
        compact,
    ) is not None

    # Validate each bounds-driving definition/loop/clamp exactly, then remove
    # those authorized writes and reject any remaining mutation (including a
    # hidden reassignment inserted inside an otherwise safe block).
    expected_loops = {
        "for(intk=0;k<n-2;k++)": 2,
        "for(intk=0;k<seed_n;k++)": 2,
        "for(intk=0;k<qbar_n;k++)": 1,
        "for(intk=qbar_n;k<n-2;k++)": 1,
        "for(intk=0;k<InpDccLookback;k++)": 1,
        "for(intk=0;k<inpregimevollookup;k++)": 1,
    }
    if any(compact.count(loop.casefold()) != count for loop, count in expected_loops.items()):
        return False
    allowed_writes = (
        ("inteth_total=CopyClose(_Symbol,_Period,0,InpDccLookback+260,g_eth_close);", 1),
        ("intbtc_total=CopyClose(InpBTCCrossSymbol,_Period,0,InpDccLookback+260,g_btc_close);", 1),
        ("intn=MathMin(eth_total,btc_total);", 1),
        ("intseed_n=InpDccLookback;", 1),
        ("if(seed_n>n-2)seed_n=n-2;", 1),
        ("intqbar_n=MathMin(InpDccLookback,n-2);", 1),
        ("intsrc=k+1;", 1),
        ("intvol_start=(n-2)-InpRegimeVolLookup;", 1),
        ("if(vol_start<0)vol_start=0;", 1),
        ("intsrc=vol_start+k;", 1),
        ("if(src<0)src=0;", 2 if vol_loop_redundant_lower_clamp is not None else 1),
        ("if(src>=n-2)src=n-3;", 1),
    )
    if alias_return_loop is not None:
        allowed_writes += (
            ("intidx_a=k+1;", 1), ("intidx_b=k+2;", 1),
            ("if(idx_a>=n)idx_a=n-1;", 1), ("if(idx_b>=n)idx_b=n-1;", 1),
            ("if(idx_a<1)idx_a=1;", 1), ("if(idx_b<2)idx_b=2;", 1),
        )
    mutation_check = compact
    for allowed, count in allowed_writes:
        if mutation_check.count(allowed.casefold()) != count:
            return False
        mutation_check = mutation_check.replace(allowed.casefold(), "", count)
    for loop, count in expected_loops.items():
        mutation_check = mutation_check.replace(loop.casefold(), "", count)
    assignment = r"(?:\+\+|--|<<=|>>=|\+=|-=|\*=|/=|%=|=(?!=))"
    if any(re.search(rf"(?:\+\+|--)\s*{name}\b|\b{name}\s*{assignment}", mutation_check, re.I)
           for name in bound_names):
        return False
    expected_resizes = {
        "r_eth": "n-2", "r_btc": "n-2",
        "h_eth_hist": "n-2", "h_btc_hist": "n-2",
        "corr_hist": "InpDccLookback", "vol_hist": "InpRegimeVolLookup",
    }
    for array, length in expected_resizes.items():
        calls = re.findall(rf"ArrayResize\s*\(\s*{array}\s*,\s*([^)]*)\)", body, re.I)
        if len(calls) != 1 or re.sub(r"\s+", "", calls[0]).casefold() != length.casefold():
            return False
    if (len(re.findall(r"\bRollingZScore\s*\(\s*corr_hist\s*,", body, re.I)) != 1
            or len(re.findall(r"\bRollingZScore\s*\(\s*vol_hist\s*,", body, re.I)) != 1):
        return False

    # Exact access inventories reject added reads/writes even when an added
    # access happens to reuse an otherwise accepted index expression.
    expected_accesses = {
        "g_eth_close": ({"idx_a": 1, "idx_b": 1} if alias_return_loop is not None
                         else {"k+1": 1, "k+2": 1}),
        "g_btc_close": ({"idx_a": 1, "idx_b": 1} if alias_return_loop is not None
                         else {"k+1": 1, "k+2": 1}),
        "r_eth": {"k": 8, "src": 1},
        "r_btc": {"k": 8, "src": 1},
        "h_eth_hist": {"k": 3, "src": 2},
        "h_btc_hist": {"k": 3, "src": 1},
        "corr_hist": {"k": 2 if corr_zero_fallback else 1},
        "vol_hist": {"k": 1},
    }
    for array, expected in expected_accesses.items():
        found = re.findall(rf"\b{array}\s*\[([^\]]+)\]", body, re.I)
        normalized = [re.sub(r"\s+", "", index).casefold() for index in found]
        counts = {index: normalized.count(index) for index in set(normalized)}
        if counts != expected:
            return False

    # Tracked arrays may be indexed directly only in the blocks proven above.
    # They may be passed by whole-array reference only to the exact rolling
    # consumer calls checked below. Reject helper calls, aliases, address or
    # reference expressions, and any other bare identifier use before those
    # removals so an unmodelled callee cannot hide an out-of-range access.
    residual = re.sub(r"\s+", "", body)
    residual = re.sub(r"\bdoubler_eth\[\],r_btc\[\];", "", residual, flags=re.I)
    for local_array in ("h_eth_hist", "h_btc_hist", "corr_hist", "vol_hist"):
        residual = re.sub(rf"\bdouble\s+{local_array}\[\];", "", residual, flags=re.I)
    for array, length in expected_resizes.items():
        resize_length = length.casefold().replace(" ", "")
        residual = re.sub(
            rf"\bArrayResize\({array},{re.escape(resize_length)}\)",
            "", residual, count=1, flags=re.I,
        )
    for array in expected_accesses:
        if re.search(rf"&{array}\[", residual, re.I):
            return False
        residual = re.sub(rf"\b{array}\[[^\]]+\]", "", residual, flags=re.I)
    residual = re.sub(
        r"\bRollingZScore\(corr_hist,InpDccLookback,InpDccLookback,corr\)",
        "", residual, count=1, flags=re.I,
    )
    residual = re.sub(
        r"\bRollingZScore\(vol_hist,InpRegimeVolLookup,InpRegimeVolLookup,MathSqrt\(MathMax\(h_eth,1e-12\)\)\)",
        "", residual, count=1, flags=re.I,
    )
    residual = re.sub(
        r"\bCopyClose\(_Symbol,_Period,0,InpDccLookback\+260,g_eth_close\)",
        "", residual, count=1, flags=re.I,
    )
    residual = re.sub(
        r"\bCopyClose\(InpBTCCrossSymbol,_Period,0,InpDccLookback\+260,g_btc_close\)",
        "", residual, count=1, flags=re.I,
    )
    if re.search(r"\b(?:g_eth_close|g_btc_close|r_eth|r_btc|h_eth_hist|h_btc_hist|corr_hist|vol_hist)\b", residual, re.I):
        return False

    # The close buffers are global. No out-of-function helper may read, alias,
    # or otherwise consume any tracked array; the recognized in-function body
    # is the only place with admitted accesses. Keep only the canonical global
    # declarations and series setup outside it.
    outside = code[:match.start()] + code[end:]
    outside = re.sub(r"\bdouble\s+g_eth_close\s*\[\s*\]\s*;", "", outside, flags=re.I)
    outside = re.sub(r"\bdouble\s+g_btc_close\s*\[\s*\]\s*;", "", outside, flags=re.I)
    for array in ("g_eth_close", "g_btc_close"):
        outside = re.sub(rf"\bArraySetAsSeries\s*\(\s*{array}\s*,\s*true\s*\)\s*;", "", outside, flags=re.I)
    if re.search(r"\b(?:g_eth_close|g_btc_close|r_eth|r_btc|h_eth_hist|h_btc_hist|corr_hist|vol_hist)\b", outside, re.I):
        return False

    # The consumer is admitted only when its own range is 0..total-1 and both
    # calls pass totals equal to the allocated history length.
    rolling_signature = re.search(
        r"\bdouble\s+RollingZScore\s*\(\s*const\s+double\s*&values\[\]\s*,\s*int\s+total\s*,\s*int\s+lookback\s*,\s*double\s+current\s*\)",
        code, re.I,
    )
    if (not rolling_signature
            or len(re.findall(r"\bdouble\s+RollingZScore\s*\(", code, re.I)) != 1):
        return False
    rolling_open = code.find("{", rolling_signature.end())
    rolling_depth = 1
    rolling_end = rolling_open + 1
    while rolling_end < len(code) and rolling_depth:
        rolling_depth += (code[rolling_end] == "{") - (code[rolling_end] == "}")
        rolling_end += 1
    if rolling_depth:
        return False
    rolling_body = re.sub(r"\s+", "", code[rolling_open + 1:rolling_end - 1]).casefold()
    rolling_calls = set(re.findall(r"\b([A-Za-z_]\w*)\s*\(", code[rolling_open + 1:rolling_end - 1]))
    if rolling_calls - {"if", "for", "return", "MathSqrt"}:
        return False
    if ("if(lookback<=0||total<lookback)return0.0;" not in rolling_body
            or rolling_body.count("for(intk=total-lookback;k<total;k++)") != 2
            or rolling_body.count("values[k]") != 2
            or "sum+=values[k];" not in rolling_body
            or "d=values[k]-mean;" not in rolling_body
            or "var+=d*d;" not in rolling_body):
        return False
    rolling_mutation = rolling_body.replace("if(lookback<=0||total<lookback)return0.0;", "", 1)
    rolling_mutation = rolling_mutation.replace("for(intk=total-lookback;k<total;k++)", "", 2)
    rolling_mutation = rolling_mutation.replace("sum+=values[k];", "", 1)
    rolling_mutation = rolling_mutation.replace("d=values[k]-mean;", "", 1)
    rolling_mutation = rolling_mutation.replace("var+=d*d;", "", 1)
    rolling_mutation = rolling_mutation.replace("values[k]", "", 2)
    if re.search(r"\bvalues\b", rolling_mutation, re.I):
        return False
    if any(re.search(rf"(?:\+\+|--)\s*{name}\b|\b{name}\s*{assignment}", rolling_mutation, re.I)
           for name in ("total", "lookback", "k")):
        return False
    values_outside = code[:rolling_signature.start()] + code[rolling_end:]
    if re.search(r"\bvalues\b", values_outside, re.I):
        return False
    return True


def executable_source(source: str) -> str:
    """Remove comments so policy tokens cannot be satisfied by prose."""
    source = re.sub(r"/\*.*?\*/", "", source, flags=re.S)
    return re.sub(r"//[^\r\n]*", "", source)


def _function_spans(code: str):
    """Yield function body ranges with a simple balanced-brace scan."""
    signature = re.compile(
        r"\b(?:void|bool|int|long|double|datetime|string|ulong)\s+"
        r"[A-Za-z_]\w*\s*\([^{};]*\)\s*\{", re.I
    )
    for match in signature.finditer(code):
        start = match.end()
        depth = 1
        for index in range(start, len(code)):
            depth += (code[index] == "{") - (code[index] == "}")
            if depth == 0:
                yield start, index, code[start:index], match.group(0).split(None, 1)[0].lower()
                break


def _if_block_end(code: str, match: re.Match) -> int:
    """Return the end of an if body, or the first statement semicolon."""
    cursor = match.end()
    while cursor < len(code) and code[cursor].isspace():
        cursor += 1
    if cursor >= len(code):
        return cursor
    if code[cursor] != "{":
        end = code.find(";", cursor)
        return len(code) if end < 0 else end + 1
    depth = 1
    for index in range(cursor + 1, len(code)):
        depth += (code[index] == "{") - (code[index] == "}")
        if depth == 0:
            return index + 1
    return len(code)


def _is_unconditional_top_level_guard(code: str, match: re.Match) -> bool:
    """Accept an early guard only when earlier statements cannot skip it."""
    prefix = code[:match.start()]
    prefix = re.sub(r'"(?:\\.|[^"\\])*"', lambda item: " " * len(item.group(0)), prefix)
    brace_depth = 0
    paren_depth = 0
    last_statement_end = 0
    for index, char in enumerate(prefix):
        if char == "(":
            paren_depth += 1
        elif char == ")":
            paren_depth = max(0, paren_depth - 1)
        elif char == "{":
            brace_depth += 1
        elif char == "}":
            brace_depth = max(0, brace_depth - 1)
            if brace_depth == 0 and paren_depth == 0:
                last_statement_end = index + 1
        elif char == ";" and brace_depth == 0 and paren_depth == 0:
            last_statement_end = index + 1
    return brace_depth == 0 and not prefix[last_statement_end:].strip()


def _positive_guard_is_necessary(condition: str, name: str) -> bool:
    """Require a positive-input atom on every supported route through a guard."""
    expression = re.sub(r'"(?:\\.|[^"\\])*"', lambda item: " " * len(item.group(0)), condition)
    if "||" in expression:
        # OR expressions require full Boolean path analysis; fail closed.
        return False
    depth = 0
    start = 0
    conjuncts = []
    index = 0
    while index < len(expression):
        char = expression[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif expression.startswith("&&", index) and depth == 0:
            conjuncts.append(expression[start:index].strip())
            index += 1
            start = index + 1
        index += 1
    conjuncts.append(expression[start:].strip())
    pattern = re.compile(rf"{re.escape(name)}\s*>\s*0(?:\.0*)?", re.I)
    return any(pattern.fullmatch(conjunct.strip(" ()\t\r\n")) for conjunct in conjuncts)


def zero_disables_time_stop_controls(source: str) -> bool:
    """Require zero to return before any executable time-stop expiry check."""
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
    function_spans = list(_function_spans(code))
    for name in control_names:
        comparisons = list(re.finditer(
            rf"(?:\b{re.escape(name)}\s*(?:>=|>)|(?:>=|>)\s*\b{re.escape(name)}\b)",
            code, re.I))
        for comparison in comparisons:
            span = next(((start, body, return_type) for start, end, body, return_type in function_spans
                         if start <= comparison.start() < end), None)
            if span is None:
                return False
            function_start, enclosing, return_type = span
            relative = comparison.start() - function_start
            # A direct positive guard in the expiry condition is sufficient.
            positive_same_condition = False
            positive_guard_test = False
            for condition_match in re.finditer(r"\bif\s*\(([^;{}]{1,800})\)", enclosing, re.I):
                if condition_match.start() <= relative < condition_match.end():
                    condition = condition_match.group(1).strip()
                    if re.fullmatch(rf"{re.escape(name)}\s*>\s*0(?:\.0*)?", condition, re.I):
                        positive_guard_test = True
                    if _positive_guard_is_necessary(condition, name):
                        positive_same_condition = True
                    break
            # Accept a positive guard whose body encloses the expiry comparison.
            positive_enclosing_guard = False
            if not positive_same_condition:
                for guard in re.finditer(r"\bif\s*\(([^;{}]{1,800})\)", enclosing, re.I):
                    guard_condition = guard.group(1).strip()
                    has_positive_guard = _positive_guard_is_necessary(guard_condition, name)
                    if not has_positive_guard:
                        continue
                    if guard.start() <= relative < _if_block_end(enclosing, guard):
                        positive_enclosing_guard = True
                        break
            # Also accept the common early-return idiom in the same function.
            early_guard = re.search(
                rf"if\s*\(\s*{re.escape(name)}\s*(?:<=|==)\s*0(?:\.0*)?\s*\)\s*return\s*(?P<value>false)?\s*;",
                enclosing[:relative], re.I)
            if early_guard is not None:
                return_value = early_guard.group("value")
                if (not _is_unconditional_top_level_guard(enclosing, early_guard)
                        or return_type not in {"void", "bool"}
                        or (return_type == "void" and return_value is not None)
                        or (return_type == "bool" and return_value is None)):
                    early_guard = None
            if not (positive_guard_test or positive_same_condition or positive_enclosing_guard or early_guard):
                return False
    return True


def _safe_zero_bypass_ranges(code: str, control_name: str) -> list[tuple[int, int]]:
    """Find boolean cooldown gates that explicitly bypass on a zero value."""
    def strip_outer_parentheses(value: str) -> str:
        value=value.strip()
        while value.startswith("(") and value.endswith(")"):
            depth=0; encloses=True
            for index,char in enumerate(value):
                depth += (char == "(") - (char == ")")
                if depth == 0 and index != len(value)-1:
                    encloses=False; break
            if not encloses or depth != 0:
                break
            value=value[1:-1].strip()
        return value

    def top_level_or_terms(expression: str) -> list[str]:
        expression=strip_outer_parentheses(expression)
        terms=[]; start=0; depth=0; index=0
        while index < len(expression):
            char=expression[index]
            depth += (char == "(") - (char == ")")
            if depth == 0 and expression[index:index+2] == "||":
                terms.append(expression[start:index].strip())
                start=index+2; index+=2; continue
            index+=1
        terms.append(expression[start:].strip())
        return terms

    ranges=[]
    boolean_assignment=re.compile(r"\bbool\s+([A-Za-z_]\w*)\s*=\s*([^;]+);", re.I)
    zero_term=re.compile(rf"{re.escape(control_name)}\s*(?:<=|==)\s*0(?:\.0*)?", re.I)
    for assignment in boolean_assignment.finditer(code):
        expression=assignment.group(2)
        if not any(zero_term.fullmatch(strip_outer_parentheses(term))
                   for term in top_level_or_terms(expression)):
            continue
        gate_name=assignment.group(1)
        for guard in re.finditer(rf"\bif\s*\(\s*!\s*{re.escape(gate_name)}\s*\)", code, re.I):
            block_end=_if_block_end(code, guard)
            body=code[guard.end():block_end]
            # A direct refusal on a false cooldown gate is also a valid zero
            # bypass: the explicit `control <= 0` OR term makes the gate true
            # at zero, so execution cannot hit this return because of cooldown.
            if re.fullmatch(r"\s*return\s*;\s*", body):
                ranges.append(assignment.span(2))
                break
            blocked_flags=set(re.findall(r"\b([A-Za-z_]\w*)\s*=\s*(?:false|0)\s*;", body, re.I))
            if not blocked_flags:
                continue
            later=code[block_end:]
            entry_flags=set(re.findall(
                r"\bif\s*\(\s*([A-Za-z_]\w*)\s*\)\s*(?:[A-Za-z_]\w*(?:(?:->|\.)[A-Za-z_]\w*)*)\s*\(",
                later, re.I))
            if entry_flags and entry_flags.issubset(blocked_flags):
                ranges.append(assignment.span(2))
                break
    return ranges


def zero_disables_cooldown_controls(source: str) -> bool:
    """Allow cooldown logic only behind a positive-value guard.

    An unused input is valid: zero has no effect when no cooldown code reads it.
    Any conditional use must be gated by ``control > 0`` before an early return
    or other cooldown action can run.
    """
    code = _mask_comments_and_strings(executable_source(source))
    # A telemetry-only mention does not apply a cooldown. Ignore standalone
    # diagnostic statements while retaining the existing positive-guard check
    # for every executable use.
    masked = list(code)
    for call in re.finditer(r"(?m)^[ \t]*(?:Print|PrintFormat|Comment|Alert)\s*\(", code):
        opening = code.find("(", call.start(), call.end())
        closing = _balanced_call_end(code, opening)
        if closing is None:
            continue
        terminator = closing + 1
        while terminator < len(code) and code[terminator].isspace() and code[terminator] not in "\r\n":
            terminator += 1
        if terminator >= len(code) or code[terminator] != ";":
            continue
        for index in range(call.start(), terminator + 1):
            if code[index] not in "\r\n":
                masked[index] = " "
    code = "".join(masked)
    declaration_re = re.compile(
        r"(?im)^\s*input\s+[A-Za-z_]\w*\s+([A-Za-z_]\w*)\s*=\s*([^;]+);"
    )
    for match in declaration_re.finditer(source):
        name = match.group(1)
        comment = match.group(0).split("//", 1)[1] if "//" in match.group(0) else ""
        if is_timing_control_input(name, {"description": comment}) != "cooldown":
            continue
        stripped = re.sub(r"\binput\s+[A-Za-z_]\w*\s+[A-Za-z_]\w*\s*=\s*[^;]+;", "", code, flags=re.I)
        protected = []
        zero_bypass_ranges = _safe_zero_bypass_ranges(stripped, name)
        alias_assignment = re.compile(
            rf"\b(?:const\s+)?(?:int|long|short)\s+([A-Za-z_]\w*)\s*=\s*{re.escape(name)}\s*;",
            re.I)
        for alias in alias_assignment.finditer(stripped):
            alias_name = alias.group(1)
            assignments = list(re.finditer(rf"\b{re.escape(alias_name)}\s*=", stripped))
            if len(assignments) != 1:
                continue
            alias_guards = []
            for guard in re.finditer(r"\bif\s*\(([^)]{1,1200})\)", stripped, re.I):
                condition = guard.group(1)
                if re.fullmatch(rf"\s*{re.escape(alias_name)}\s*>\s*0(?:\.0*)?\s*", condition, re.I):
                    alias_guards.append((guard.start(), _if_block_end(stripped, guard)))
            alias_reads = [item for item in re.finditer(rf"\b{re.escape(alias_name)}\b", stripped)
                           if item.start() != alias.start(1)]
            if alias_reads and all(any(start <= item.start() < end for start, end in alias_guards)
                                   for item in alias_reads):
                zero_bypass_ranges.append(alias.span(0))
        for guard in re.finditer(r"\bif\s*\(([^)]{1,1200})\)", stripped, re.I):
            condition = guard.group(1)
            positive_only_guard = re.fullmatch(
                rf"\s*{re.escape(name)}\s*>\s*0(?:\.0*)?\s*", condition, re.I)
            positive_conjunct_guard = re.match(
                rf"(?:^|&&)\s*{re.escape(name)}\s*>\s*0(?:\.0*)?\s*(?:&&|$)", condition, re.I)
            if positive_only_guard or positive_conjunct_guard:
                protected.append((guard.start(), _if_block_end(stripped, guard)))
        function_spans = list(_function_spans(stripped))
        early_true_guards = []
        guard_pattern = re.compile(
            rf"if\s*\(\s*{re.escape(name)}\s*(?:<=|==)\s*0(?:\.0*)?\s*\)\s*return\s+(?:true|1)\s*;",
            re.I)
        for function_start, function_end, body, _return_type in function_spans:
            for guard in guard_pattern.finditer(body):
                early_true_guards.append((function_start + guard.start(),
                                          function_start + guard.end(), function_start, function_end))
        for occurrence in re.finditer(rf"\b{re.escape(name)}\b", stripped):
            if any(start <= occurrence.start() < end for start, end in zero_bypass_ranges):
                continue
            if any(start <= occurrence.start() < end for start, end in protected):
                continue
            # In a function, an unconditional zero/nonpositive early return
            # makes subsequent cooldown reads reachable only for positive values.
            if any(fn_start <= occurrence.start() < fn_end and
                   (guard_start <= occurrence.start() < guard_end or occurrence.start() >= guard_end)
                   for guard_start, guard_end, fn_start, fn_end in early_true_guards):
                continue
            return False
    return True


def cooldown_controls_absent(source: str) -> bool:
    """Require generated EA source to contain no cooldown input or logic."""
    code = executable_source(source)
    if re.search(r"\bInpMinBarsBetweenTrades\b", code, re.I):
        return False
    if re.search(r"\b(?:[A-Za-z_]\w*(?:cooldown|coolingdown|barsbetween)\w*|cooldown\w*|coolingdown\w*|barsbetween\w*)\b", code, re.I):
        return False
    declaration_re = re.compile(
        r"(?im)^\s*input\s+[A-Za-z_]\w*\s+([A-Za-z_]\w*)\s*=\s*([^;]+);"
    )
    for match in declaration_re.finditer(source):
        comment = match.group(0).split("//", 1)[1] if "//" in match.group(0) else ""
        if is_timing_control_input(match.group(1), {"description": comment}) == "cooldown":
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
    # Recognize direct order statements and the supported `if (order-call)`
    # forms. Other expression contexts fail closed instead of being treated as
    # a validated order path.
    order_candidates = list(re.finditer(
        r"\.\s*(Buy|Sell)\s*\(([^;]*?)\)", scan_code, re.I | re.S))
    orders = []
    for candidate in order_candidates:
        start, end = candidate.span()
        statement_start = max(scan_code.rfind(token, 0, start) for token in (";", "{", "}")) + 1
        prefix = scan_code[statement_start:start].strip()
        suffix = scan_code[end:].lstrip()
        if re.fullmatch(r"[A-Za-z_]\w*", prefix) and suffix.startswith(";"):
            orders.append(candidate)
            continue
        if re.fullmatch(r"if\s*\(\s*!?\s*[A-Za-z_]\w*", prefix, re.I):
            if suffix.startswith(")"):
                body = suffix[1:].lstrip()
                if body.startswith("{") or (body and not any(ch in body.split(";", 1)[0] for ch in "{}") and ";" in body):
                    orders.append(candidate)
                    continue
        if diagnostics is not None:
            diagnostics.append({"code": "ORDER_CONTEXT_UNSUPPORTED",
                                "reason": "Buy/Sell must be a direct statement or the complete condition of a supported if statement"})
        return False
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
        related_scope = (current is not None and scope is not None and
                         (scope[:len(current)] == current or current[:len(scope)] == scope))
        dominates = current == scope or (ancestor and related_scope)
        return dominates and (not before or before[-1] in '{;}')
    def writes(text, name):
        return bool(re.search(rf'\b{re.escape(name)}\s*(?:[+\-*/%&|^]?=(?!=)|\+\+|--)'
                              rf'|(?:\+\+|--)\s*\b{re.escape(name)}\b', text))
    # MQL5 MathAbs(double) and MathIsValidNumber(double) read [in] values; neither mutates an order value.
    safe_value_helpers = {'mathabs', 'mathisvalidnumber'} | ({'freemarginallowsnotional'} if free_margin_notional_helper_is_safe(scan_code) else set())
    # The authenticated canonical include defines this guard with value parameters.
    # validate_source separately binds the include to the approved Minato hash.
    if re.search(r'(?im)^\s*#\s*include\s*[<"]minato_base_score_v2\.mqh[>"]', scan_code):
        safe_value_helpers.add('minatoallowsprojectedmargin')
    def unknown_reference_call(text, name):
        calls = re.finditer(rf'(?=\b([A-Za-z_]\w*)\s*\([^;{{}}]*\b{re.escape(name)}\b[^;{{}}]*\))', text)
        # These verified value-only helpers cannot mutate order values.
        return any(call[1].casefold() not in {
            'if', 'switch', 'normalizedouble', 'buy', 'sell',
            'print', 'printformat', 'comment', 'alert',
        } | safe_value_helpers for call in calls)
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
        normalizer_spans = [
            (search_start + match.start(), search_start + match.end()) for match in normalizers
            if direct_statement(base + search_start + match.start(), scope, ancestor=True)
        ]
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
        notional_helper_safe = free_margin_notional_helper_is_safe(scan_code)
        if notional_helper_safe:
            for check in notional_guards:
                end = _simple_refusal_end(window, check.end())
                if (end is not None and check.start() >= guard_end and end <= order.start() - base
                        and _normalized_expression(check.group(1)) == volume
                        and _normalized_expression(check.group(2)) == price
                        and direct_statement(base + check.start(), scope)):
                    valid_notional_guard = True
                    break
        # MinatoAllowsProjectedMargin is the canonical risk bound: it calculates
        # the requested order's margin and requires projected account margin
        # level to remain at or above 120%. A separate notional reserve is
        # optional; when present, it must be authenticated and bound to this
        # order's exact volume and price.
        if not valid_notional_guard and notional_guards:
            if diagnostics is not None:
                if notional_guards and not notional_helper_safe:
                    diagnostics.append({"code": "ORDER_MARGIN_NOTIONAL_HELPER_UNAUTHENTICATED", "side": side,
                                        "volume_variable": volume, "price_variable": price,
                                        "reason": "an optional FreeMarginAllowsNotional guard must match the authenticated read-only helper and bind the exact submitted volume and price"})
                else:
                    diagnostics.append({"code": "ORDER_MARGIN_NOTIONAL_MISSING", "side": side,
                                        "volume_variable": volume, "price_variable": price,
                                        "reason": "an optional FreeMarginAllowsNotional guard is present but not bound to the exact order; the canonical projected-margin guard remains required"})
            return False
        # The order must use the values approved by the canonical projected-margin check
        # and any optional, authenticated notional reserve check.
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
        normalizers = [match for match in normalizers
                       if direct_statement(base + quantity.end() + match.start(), scope, ancestor=True)]
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
                    and direct_statement(base + z_start, scope, ancestor=True)):
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
        before_zero_guard = window[quantity.end():z_start]
        # A single authenticated MaxLotForLeverage cap may adjust the requested
        # volume before the zero check. All other writes remain rejected, and
        # the normalized result still must pass the canonical 120% margin gate.
        cap_writes = list(re.finditer(
            r"if\s*\(\s*lot\s*>\s*cap_lot\s*\)\s*\{\s*"
            r"lot\s*=\s*cap_lot\s*;[^{}]*\}",
            before_zero_guard, re.I | re.S))
        if len(cap_writes) > 1:
            return False
        if cap_writes:
            cap_write = cap_writes[0]
            if not re.search(
                    r"\bdouble\s+cap_lot\s*=\s*MaxLotForLeverage\s*\([^;]+\)\s*;",
                    before_zero_guard[:cap_write.start()], re.I | re.S):
                return False
            before_zero_guard = (before_zero_guard[:cap_write.start()]
                                 + before_zero_guard[cap_write.end():])
        if writes(before_zero_guard + window[z_end:n_start], volume):
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
        minato_path = Path(handoff.get("canonical_minato") or "")
        shared_sources = ({str(minato_path.resolve()): minato_path.read_text(encoding="utf-8-sig")}
                          if minato_path.is_file() else {})
        effective,resolution=resolve_input_spec(load(spec_path), shared_sources=shared_sources)
        effective=stage2_spec_from_spec(effective)
        resolution["kcos_runtime_policy"] = {
            "timing_controls": effective.get("stage2_runtime_policy", {}).get("timing_controls", {})
        }
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
    stage1_config = stage2_config_from_spec(stage1_spec, source_text=source)
    spec_input_names = set((stage1_spec.get("tester_config") or {}).get("inputs", {}))
    source_inputs = discover_inputs({str(source_path.resolve()): source}).get("inputs", {})
    timing_names = [name for name, item in stage1_config["inputs"].items()
                    if is_timing_control_input(name, item) is not None]
    timing_controls_zero_fixed = all(
        stage1_config["inputs"][name].get("default") == 0
        and stage1_config["inputs"][name].get("start") == 0
        and stage1_config["inputs"][name].get("step") == 0
        and stage1_config["inputs"][name].get("stop") == 0
        and stage1_config["inputs"][name].get("optimize") == "N"
        and source_inputs.get(name, {}).get("source_default") == 0
        for name in timing_names
    )
    atr_period_input = atr_period_input_name(stage1_spec)
    stage1_inputs = ((stage1_spec.get("tester_config") or {}).get("inputs")
                     if isinstance((stage1_spec.get("tester_config") or {}).get("inputs"), dict) else {})
    approved_config_sha = config_sha256(stage1_config)
    approved_config_ok = handoff.get("approved_tester_config_sha256") == approved_config_sha
    compact_ea = re.sub(r"[^a-z0-9]", "", str(handoff.get("ea") or "").casefold())
    gjr_base_lot_required = "gjr" in compact_ea and "garch" in compact_ea
    checks = {
        "approved_tester_config": approved_config_ok,
        "timing_controls_zero_and_fixed": timing_controls_zero_fixed,
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
        "gjr_base_lot_sizing": (not gjr_base_lot_required or gjr_base_lot_sizing_contract(source)),
        "time_stop_zero_disables_close": zero_disables_time_stop_controls(source),
        "cooldown_controls_absent": cooldown_controls_absent(source),
        "closed_bar_only": (
            closed_bar_check(source) or diagnostic2_closed_bar_only(source) or cusum_closed_bar_only(source) or generic_closed_bar_audit(source)),
        "dcc_closed_return_arrays_bounded": dcc_closed_return_arrays_are_bounded(
            source, required_dcc=bool(stage1_lineage and spec_requires_dcc_garch(stage1_spec))),
        "multidimensional_array_row_bounds_safe": multidimensional_array_row_bounds_are_safe(source),
        "gk_sample_update_after_closed_bar_gate": gk_sample_update_is_closed_bar_gated(source),
        "indicator_reads_exclude_forming_bar": closed_bar_indicator_reads_are_safe(source),
        "atr_uses_closed_period_buffer": atr_uses_authenticated_closed_buffer(
            source, atr_period_input,
            ("PERIOD_M5" if stage1_spec.get("stage2_requirements", {}).get(
                "must_use_ethusd_m5_closed_bars") and stage1_spec.get("timeframes") == ["M5"]
             else "_Period"),
            allow_literal_period=atr_period_input not in stage1_inputs),
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
    local_trade = re.match(r'CTrade(?P<trade>[A-Za-z_]\w*);', close_code)
    declared_trade = local_trade['trade'] if local_trade else None
    if local_trade:
        close_code = close_code[local_trade.end():]
    magic_setup = re.match(
        r'(?P<trade>[A-Za-z_]\w*)\.SetExpertMagicNumber\(\(ulong\)InpMagicNumber\);', close_code)
    configured_trade = magic_setup['trade'] if magic_setup else None
    if magic_setup:
        close_code = close_code[magic_setup.end():]
    loop = re.fullmatch(
        r'for\(int(?P<index>[A-Za-z_]\w*)=PositionsTotal\(\)-1;(?P=index)>=0;'
        r'(?:--(?P=index)|(?P=index)--|\(--(?P=index)\)|\((?P=index)--\))\)\{'
        r'ulong(?P<ticket>[A-Za-z_]\w*)=PositionGetTicket\((?P=index)\);'
        r'if\((?P=ticket)==0\)continue;'
        r'if\(PositionGetString\(POSITION_SYMBOL\)!=_Symbol\)continue;'
        r'if\(PositionGetInteger\(POSITION_MAGIC\)!=(?:\(long\))?InpMagicNumber\)continue;'
        r'(?P<trade>[A-Za-z_]\w*)\.PositionClose\((?P=ticket)\);}'
        r'(?:g_[A-Za-z_]\w*=(?:false|true|0(?:\.0+)?|(?:[A-Za-z_]\w*_)?REGIME_[A-Za-z_]\w*);|g_(?:entry_bar_index|last_entry_bar)=-1;)*', close_code)
    if (loop is None or (configured_trade and configured_trade != loop['trade'])
            or (declared_trade and declared_trade != loop['trade'])):
        return False
    if not re.search(r'\bCTrade\s+' + re.escape(loop['trade']) + r'\s*;', code):
        return False
    return bool(re.match(
        r'MinatoV2ObserveTick\(\);if\(MarginBreached\(\)\)\{CloseAllPositions\(\);'
        r'(?:(?:g_[A-Za-z_]\w*)=(?:false|true|-1|0(?:\.0+)?|(?:[A-Za-z_]\w*_)?REGIME_[A-Za-z_]\w*);)*return;\}',
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
    if not re.match(r"if\(volume<=0(?:\.0+)?\|\|price<=0(?:\.0+)?\)returnfalse;", compact):
        return False

    def assignment(pattern: str):
        return re.search(r"(?:double(?P<typed>[A-Za-z_]\w*)|(?P<plain>[A-Za-z_]\w*))=" + pattern, compact)

    def name(match):
        return match.group("typed") or match.group("plain")

    notional = assignment(r"volume\*price;")
    free = assignment(r"AccountInfoDouble\(ACCOUNT_MARGIN_FREE\);")
    if notional is None or free is None or name(notional) == name(free):
        return False
    notional_name, free_name = name(notional), name(free)
    variable_guard = re.search(
        r"return\(*" + re.escape(free_name) + r">=2(?:\.0+)?\*"
        + re.escape(notional_name) + r"\)*;", compact)
    equivalent_guard = re.search(
        r"return\(*2(?:\.0+)?\*" + re.escape(notional_name)
        + r"\)*<=" + re.escape(free_name) + r"\)*;", compact)

    # Also accept the equivalent fail-closed branch form, but only when the
    # whole helper consists of these exact read-only declarations and checks.
    required = assignment(r"2(?:\.0+)?\*" + re.escape(notional_name) + r";")
    rejecting_form = False
    if required is not None:
        required_name = name(required)
        equity = assignment(r"AccountInfoDouble\(ACCOUNT_EQUITY\);")
        notional_decl = assignment(r"volume\*price;")
        free_decl = assignment(r"AccountInfoDouble\(ACCOUNT_MARGIN_FREE\);")
        prefix_start = re.match(
            r"if\(volume<=0(?:\.0+)?\|\|price<=0(?:\.0+)?\)returnfalse;", compact)
        if prefix_start and notional_decl and free_decl:
            prefix = compact[prefix_start.end():required.start()]
            notional_in_prefix = re.search(
                r"(?:double" + re.escape(notional_name) + r"|" + re.escape(notional_name)
                + r")=volume\*price;", prefix)
            free_in_prefix = re.search(
                r"(?:double" + re.escape(free_name) + r"|" + re.escape(free_name)
                + r")=AccountInfoDouble\(ACCOUNT_MARGIN_FREE\);", prefix)
            spans = [item for item in (notional_in_prefix, free_in_prefix) if item is not None]
            if equity is not None:
                equity_decl = re.search(
                    r"(?:double" + re.escape(name(equity)) + r"|" + re.escape(name(equity))
                    + r")=AccountInfoDouble\(ACCOUNT_EQUITY\);", prefix)
                if equity_decl is not None:
                    spans.append(equity_decl)
            for declaration in sorted(spans, key=lambda item: item.start(), reverse=True):
                prefix = prefix[:declaration.start()] + prefix[declaration.end():]
            positive_hold = (
                prefix == ""
                or re.fullmatch(r"if\(" + re.escape(free_name) + r"<=0(?:\.0+)?\)returnfalse;", prefix)
                or (equity is not None and re.fullmatch(
                    r"if\(" + re.escape(name(equity)) + r"<=0(?:\.0+)?\|\|"
                    + re.escape(free_name) + r"<=0(?:\.0+)?\)returnfalse;", prefix))
            )
            expected_tail = (f"if({free_name}<{required_name})returnfalse;returntrue;")
            rejecting_form = (
                bool(positive_hold) and compact[required.end():] == expected_tail
                and compact.count(notional.group(0)) == 1
                and compact.count(free.group(0)) == 1
                and compact.count(required.group(0)) == 1
                and (equity is None or compact.count(equity.group(0)) == 1)
            )
    return variable_guard is not None or equivalent_guard is not None or rejecting_form

def gk_sample_update_is_closed_bar_gated(source: str) -> bool:
    code=_mask_comments_and_strings(source); body=_function_body(code,"void","OnTick")
    if body is None:return False
    gate=re.search(r"if\s*\(\s*!\s*NewBarClosed\s*\(\s*\)\s*\)\s*return\s*;",body)
    updates=list(re.finditer(r"\b(?:PushGK\w*|UpdateGKWindow)\s*\(",body,re.I))
    return not updates or (gate is not None and all(x.start()>=gate.end() for x in updates))

def closed_bar_indicator_reads_are_safe(source: str) -> bool:
    return re.search(r"CopyBuffer\s*\([^;]*,\s*0\s*,\s*0\s*,",_mask_comments_and_strings(source),re.I) is None

def atr_period_input_name(spec: dict) -> str:
    """Resolve the ATR period/length input from the authenticated tester spec."""
    tester = spec.get("tester_config") if isinstance(spec.get("tester_config"), dict) else {}
    inputs = tester.get("inputs") if isinstance(tester.get("inputs"), dict) else {}
    candidates = [str(name) for name in inputs
                  if re.search(r"atr", str(name), re.I)
                  and re.search(r"(?:period|length)", str(name), re.I)]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        raise ValueError("ambiguous authenticated ATR period/length inputs: " + ", ".join(sorted(candidates)))
    # Some strategies name the ATR lookback by its role in their range model.
    # Accept that explicit alias only when the authenticated tester spec has
    # exactly one such period/length input; the source check still requires
    # that same input to drive iATR and reads only closed-bar buffer data.
    range_estimator_candidates = [
        str(name) for name in inputs
        if re.search(r"range", str(name), re.I)
        and re.search(r"estimator", str(name), re.I)
        and re.search(r"(?:period|length)", str(name), re.I)
    ]
    if len(range_estimator_candidates) == 1:
        return range_estimator_candidates[0]
    if len(range_estimator_candidates) > 1:
        raise ValueError(
            "ambiguous authenticated range-estimator period/length inputs: "
            + ", ".join(sorted(range_estimator_candidates))
        )
    # Keep legacy DCC/non-research specs compatible; full admission still
    # requires the chosen identifier to be declared and used in the source.
    return "InpAtrPeriod" if "InpAtrPeriod" in inputs else "InpATRPeriod"


def atr_uses_authenticated_closed_buffer(source: str, period_input: str = "InpATRPeriod",
                                         period_expression: str = "_Period",
                                         allow_literal_period: bool = False) -> bool:
    code=_mask_comments_and_strings(source)
    period_argument = re.escape(period_input)
    period_pattern = (r"(?:_Period|PERIOD_CURRENT)" if period_expression == "_Period"
                      else re.escape(period_expression))
    int_input = re.search(r"\binput\s+int\s+" + re.escape(period_input) + r"\s*=", code, re.I)
    if int_input is not None:
        # A redundant cast preserves the authenticated integer input exactly.
        period_argument = r"(?:" + period_argument + r"|\(\s*int\s*\)\s*" + period_argument + r")"
    created=re.search(r"\b([A-Za-z_]\w*)\s*=\s*iATR\s*\(\s*_Symbol\s*,\s*" + period_pattern + r"\s*,\s*" + period_argument + r"\s*\)",code,re.I)
    if created is None and allow_literal_period and int_input is None:
        literal = re.search(
            r"\b([A-Za-z_]\w*)\s*=\s*iATR\s*\(\s*_Symbol\s*,\s*" + period_pattern
            + r"\s*,\s*([1-9]\d*)\s*\)", code, re.I)
        if literal is not None:
            literal_handle = re.escape(literal.group(1))
            if re.search(rf"CopyBuffer\s*\(\s*{literal_handle}\s*,\s*0\s*,\s*1\s*,\s*1\s*,", code, re.I):
                return True
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
    # Also accept a bool helper that copies buffer 0/shift 1 into an output
    # parameter and returns success, as used by generated EAs for ATR/EMA reads.
    helper_signature = re.compile(
        r"\bbool\s+([A-Za-z_]\w*)\s*\(\s*int\s+([A-Za-z_]\w*)\s*,"
        r"\s*double\s*&\s*([A-Za-z_]\w*)\s*\)\s*\{", re.I
    )
    for signature in helper_signature.finditer(code):
        helper_name, handle_arg, output_arg = signature.groups()
        helper = re.escape(helper_name)
        handle_arg = re.escape(handle_arg)
        output_arg = re.escape(output_arg)
        call_pattern = rf"\b{helper}\s*\(\s*{handle}\s*,\s*[A-Za-z_]\w*\s*\)"
        if not re.search(call_pattern, code):
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
        if (re.search(rf"CopyBuffer\s*\(\s*{handle_arg}\s*,\s*0\s*,\s*1\s*,\s*1\s*,",body,re.I)
                and re.search(rf"\b{output_arg}\s*=\s*[A-Za-z_]\w*\s*\[\s*0\s*\]",body)
                and re.search(r"return\s+true\s*;",body,re.I)):
            return True
    # Accept a bool wrapper that owns the authenticated ATR handle and forwards
    # a shift argument to CopyBuffer, when every call site requests closed bar 1.
    # Generated EAs commonly expose this shape as ReadClosedATR(1, value_out).
    for signature in helper_signature.finditer(code):
        helper_name, shift_arg, output_arg = signature.groups()
        helper = re.escape(helper_name)
        shift_arg = re.escape(shift_arg)
        output_arg = re.escape(output_arg)
        calls = []
        for call in re.finditer(rf"\b{helper}\s*\(([^()]*)\)", code):
            arguments = call.group(1).strip()
            # Ignore the function declaration matched by the call-shaped scan.
            if re.match(r"int\s+\w+\s*,\s*double\s*&", arguments, re.I):
                continue
            if not arguments:
                continue
            calls.append(arguments.split(",", 1)[0].strip())
        if not calls or any(argument != "1" for argument in calls):
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
        if re.search(rf"\b{shift_arg}\s*(?:\+\+|--|(?:[+\-*/%]?=)(?!=))", body):
            continue
        copy_pattern = (rf"CopyBuffer\s*\(\s*{handle}\s*,\s*0\s*,\s*{shift_arg}"
                        rf"\s*,\s*1\s*,\s*([A-Za-z_]\w*)\s*\)")
        copy_match = re.search(copy_pattern, body, re.I)
        if copy_match is None:
            continue
        array_arg = re.escape(copy_match.group(1))
        guarded_copy = (rf"if\s*\(\s*{copy_pattern}\s*!=\s*1\s*\)\s*"
                        rf"(?:\{{\s*)?return\s+false\s*;\s*(?:\}})?")
        if (re.search(guarded_copy, body, re.I)
                and re.search(rf"\b{output_arg}\s*=\s*{array_arg}\s*\[\s*0\s*\]", body)
                and re.search(r"return\s+true\s*;", body, re.I)):
            return True

    # Accept a guarded multi-bar ATR reader used to calculate rolling ATR
    # windows. Each CopyBuffer read must still use buffer 0/count 1, and the
    # helper must reject shifts below 1. Authenticate the forwarded handle at
    # every caller and require the rolling loop to begin at closed shift 1.
    atr_handles = {
        match.group(1)
        for match in re.finditer(
            r"\b([A-Za-z_]\w*)\s*=\s*iATR\s*\(\s*_Symbol\s*,\s*"
            + re.escape(period_expression)
            + r"\s*,\s*" + period_argument + r"\s*\)",
            code, re.I
        )
    }
    if atr_handles:
        multi_reader_signature = re.compile(
            r"\bbool\s+([A-Za-z_]\w*)\s*\(\s*int\s+([A-Za-z_]\w*)\s*,"
            r"\s*int\s+([A-Za-z_]\w*)\s*,\s*double\s*&\s*([A-Za-z_]\w*)"
            r"\s*\)\s*\{", re.I
        )

        def body_end(open_brace):
            depth = 1
            for pos in range(open_brace + 1, len(code)):
                depth += (code[pos] == "{") - (code[pos] == "}")
                if depth == 0:
                    return pos
            return None

        function_signature = re.compile(
            r"\b(?:bool|double|int|void)\s+([A-Za-z_]\w*)\s*\(([^{}]*)\)\s*\{", re.I
        )
        for reader_signature in multi_reader_signature.finditer(code):
            reader_name, reader_handle, reader_shift, reader_output = reader_signature.groups()
            reader_end = body_end(reader_signature.end() - 1)
            if reader_end is None:
                continue
            reader_body = code[reader_signature.end():reader_end]
            shift_guard = re.search(
                rf"if\s*\(\s*{re.escape(reader_shift)}\s*<\s*1\s*\)\s*"
                rf"(?:\{{\s*)?return\s+false\s*;\s*(?:\}})?", reader_body, re.I
            )
            copy_match = re.search(
                rf"CopyBuffer\s*\(\s*{re.escape(reader_handle)}\s*,\s*0\s*,\s*"
                rf"{re.escape(reader_shift)}\s*,\s*1\s*,\s*([A-Za-z_]\w*)\s*\)",
                reader_body, re.I
            )
            if (shift_guard is None or copy_match is None
                    or shift_guard.start() > copy_match.start()
                    or re.search(rf"\b{re.escape(reader_shift)}\s*(?:\+\+|--|(?:[+\-*/%]?=)(?!=))", reader_body)
                    or not re.search(
                        rf"if\s*\(\s*CopyBuffer\s*\(\s*{re.escape(reader_handle)}\s*,\s*0\s*,\s*"
                        rf"{re.escape(reader_shift)}\s*,\s*1\s*,\s*{re.escape(copy_match.group(1))}\s*\)"
                        rf"\s*!=\s*1\s*\)\s*(?:\{{\s*)?return\s+false\s*;\s*(?:\}})?",
                        reader_body, re.I
                    )
                    or not re.search(
                        rf"\b{re.escape(reader_output)}\s*=\s*{re.escape(copy_match.group(1))}\s*\[\s*0\s*\]",
                        reader_body
                    )
                    or not re.search(r"return\s+true\s*;", reader_body, re.I)):
                continue

            reader_name_re = re.escape(reader_name)
            reader_calls = list(re.finditer(rf"\b{reader_name_re}\s*\(([^()]*)\)", code))
            actual_reader_calls = [
                call for call in reader_calls
                if not (reader_signature.start() <= call.start() < reader_signature.end())
            ]
            if not actual_reader_calls:
                continue

            # Find the single rolling-window helper that forwards an ATR
            # handle and calls the guarded reader from a loop starting at 1.
            parent = None
            for candidate in function_signature.finditer(code):
                candidate_name, candidate_args = candidate.groups()
                if candidate_name == reader_name:
                    continue
                candidate_end = body_end(candidate.end() - 1)
                if candidate_end is None:
                    continue
                candidate_body = code[candidate.end():candidate_end]
                first_param = re.match(r"\s*int\s+([A-Za-z_]\w*)\s*,", candidate_args)
                if first_param is None:
                    continue
                forwarded_handle = first_param.group(1)
                candidate_calls = [call for call in actual_reader_calls
                                   if candidate.end() <= call.start() < candidate_end]
                if not candidate_calls:
                    continue
                looped = True
                for call in candidate_calls:
                    args = [part.strip() for part in call.group(1).split(",")]
                    if len(args) != 3 or args[0] != forwarded_handle:
                        looped = False
                        break
                    shift_name = re.escape(args[1])
                    inside_closed_loop = False
                    for loop in re.finditer(
                        rf"for\s*\(\s*int\s+{shift_name}\s*=\s*1\s*;[^;]*;\s*"
                        rf"(?:\+\+{shift_name}|{shift_name}\s*\+\+|{shift_name}\s*\+=\s*1)\s*\)\s*\{{",
                        candidate_body
                    ):
                        loop_end = body_end(candidate.end() + loop.end() - 1)
                        if loop_end is not None and loop.end() <= call.start() - candidate.end() < loop_end - candidate.end():
                            inside_closed_loop = True
                            break
                    if not inside_closed_loop:
                        looped = False
                        break
                if looped:
                    parent = (candidate, candidate_end, forwarded_handle)
                    break
            if parent is None:
                continue

            parent_signature, parent_end, forwarded_handle = parent
            parent_name = re.escape(parent_signature.group(1))
            parent_calls = list(re.finditer(rf"\b{parent_name}\s*\(([^()]*)\)", code))
            authenticated_parent_calls = []
            for call in parent_calls:
                if parent_signature.start() <= call.start() < parent_signature.end():
                    continue
                args = [part.strip() for part in call.group(1).split(",")]
                if not args or args[0] not in atr_handles:
                    break
                authenticated_parent_calls.append(call)
            else:
                if authenticated_parent_calls:
                    # Reject any direct reader call outside the authenticated
                    # rolling helper; no unreviewed handle path is accepted.
                    if all(parent_signature.end() <= call.start() < parent_end
                           for call in actual_reader_calls):
                        return True
    return False


def gjr_base_lot_sizing_contract(source: str) -> bool:
    """Require both GJR order paths to clamp InpBaseLot before regime scaling."""
    compact_source = re.sub(r"\s+", " ", _mask_comments_and_strings(source))
    declaration = re.search(
        r"\binput\s+double\s+InpBaseLot\s*=\s*0\.01\s*;", compact_source)
    if declaration is None:
        return False
    code = _mask_comments_and_strings(source)
    for function_name, order_call in (("TryBuy", "trade.Buy"), ("TrySell", "trade.Sell")):
        matches = list(re.finditer(r"\bvoid\s+" + function_name + r"\s*\([^{};]*\)\s*\{", code))
        if len(matches) != 1:
            return False
        start, depth = matches[0].end(), 1
        end = None
        for index in range(start, len(code)):
            depth += (code[index] == "{") - (code[index] == "}")
            if depth == 0:
                end = index
                break
        if end is None:
            return False
        body = code[start:end]
        normalized = re.sub(r"\s+", " ", body)
        if not re.search(
                r"double\s+minVolume\s*=\s*SymbolInfoDouble\(_Symbol,\s*SYMBOL_VOLUME_MIN\);"
                r"\s*if\s*\(minVolume\s*<=\s*0\.0\)\s*return\s*;"
                r"\s*double\s+baseQty\s*=\s*MathMax\(InpBaseLot,\s*minVolume\);",
                normalized):
            return False
        if not re.search(
                r"double\s+volume\s*=\s*MinatoRegimeQuantity\(baseQty,\s*RegimeLotMult\(g_active_regime\)\);",
                normalized):
            return False
        if not re.search(r"\b" + re.escape(order_call) + r"\s*\(\s*volume\s*,", normalized):
            return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
