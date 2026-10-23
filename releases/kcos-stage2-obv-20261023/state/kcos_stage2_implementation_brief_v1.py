"""Resolve Stage 1 intent once for the Stage 2 creation and repair prompts.

The original spec remains immutable evidence. This view separates known legacy
programming observations from approved equations and bar timing; it does not
authorize a proposal, bypass a transaction, or modify a protected contract.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json


BRIEF_VERSION = "stage2-implementation-brief-v1"
_SIGNED_VOLUME_EQUATION = (
    "Cumulative sum: +tick_volume[i] if close[i]>open[i], "
    "-tick_volume[i] if close[i]<open[i], unchanged if equal")
_SIGNED_VOLUME_LEGACY_SUFFIX = (
    "; source physically reverses the array and then sets series indexing.")


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _is_signed_tick_volume_contract(spec: dict, contract: dict) -> bool:
    strategy = spec.get("strategy") if isinstance(spec.get("strategy"), dict) else {}
    series = contract.get("series")
    if strategy.get("family") != "PivotPrice_SignedTickVolume_Divergence" or not isinstance(series, list):
        return False
    by_id = {item.get("id"): item for item in series if isinstance(item, dict)}
    signed = by_id.get("custom_signed_tick_volume", {})
    equation = "".join(str(signed.get("equation") or "").split())
    approved_equations = {"".join(value.split()) for value in (
        _SIGNED_VOLUME_EQUATION, _SIGNED_VOLUME_EQUATION + ".",
        _SIGNED_VOLUME_EQUATION + _SIGNED_VOLUME_LEGACY_SUFFIX)}
    closed_policy = contract.get("closed_bar_policy")
    closed_policy = closed_policy if isinstance(closed_policy, dict) else {}
    # Name/family alone cannot authorize a resolution against a future contract
    # with different mathematics or timing. Match the approved audited intent.
    return (equation in approved_equations
            and closed_policy.get("signal_shift") == 1
            and closed_policy.get("allow_forming_bar") is False
            and all(by_id.get(name, {}).get("bar_shift") == 1
                    for name in ("custom_signed_tick_volume", "ema_trend", "atr", "h1_regime")))


def resolve_implementation_brief(spec: dict) -> dict:
    """Return a provenance-bound derived instruction without changing ``spec``.

    Unknown families retain their execution contract exactly. Known signed-volume
    resolutions remove only the explicitly audited legacy mechanics and apply the
    existing Stage 2 cooldown-absence policy. No inference selects a new strategy.
    ``source_spec_canonical_sha256`` hashes the parsed spec, not its file encoding;
    the caller must continue authenticating the original Stage 1 file hash.
    """
    strategy = spec.get("strategy") if isinstance(spec.get("strategy"), dict) else {}
    raw_contract = strategy.get("execution_contract") or spec.get("execution_contract")
    if not isinstance(raw_contract, dict) or not raw_contract:
        raise ValueError("authenticated Stage 1 execution contract is missing")
    contract = deepcopy(raw_contract)
    observations = []
    resolutions = []
    resolved_intent = {}
    if _is_signed_tick_volume_contract(spec, contract):
        resolved_intent = {"volume_sign_rule": "close_vs_open", "signal_shift": 1,
                           "h1_regime_shift": 1}
        for item in contract["series"]:
            if not isinstance(item, dict):
                continue
            equation = item.get("equation")
            if not isinstance(equation, str):
                continue
            # These exact separators identify the audited source observations,
            # not arbitrary strategy prose. Unrecognized equations are preserved.
            for separator in ("; source physically reverses", "; source copies",
                              "; the legacy source currently requests"):
                if separator in equation:
                    intended, observed = equation.split(separator, 1)
                    observations.append({"series_id": item.get("id"),
                                         "legacy_observation": separator[2:] + observed})
                    item["equation"] = intended.rstrip(". ") + "."
                    break
        execution = contract.get("execution")
        if isinstance(execution, dict) and "cooldown" in execution:
            observations.append({"path": "execution.cooldown", "legacy_observation": execution["cooldown"]})
            execution["cooldown"] = "No cooldown input, elapsed-time gate, or cooldown behavior under canonical Stage 2 policy."
        signals = contract.get("signals")
        if isinstance(signals, dict) and isinstance(signals.get("conflict_policy"), str):
            signals["conflict_policy"] = signals["conflict_policy"].replace(
                ", cooldown has not elapsed", "")
        bindings = contract.get("input_bindings")
        if isinstance(bindings, dict):
            bindings.pop("InpMinBarsBetweenTrades", None)
        resolutions = [
            {"id": "signed_volume_identity", "instruction":
             "Preserve cumulative candle-body-signed tick volume: +volume for close>open, -volume for close<open, zero increment for a doji. Do not substitute conventional close-to-prior-close OBV or iOBV."},
            {"id": "closed_bar_alignment", "instruction":
             "The approved bar_shift=1 and signal equations control close, EMA and ATR timing. Read the same completed chart bar for all three; copied-buffer offsets and array layout are implementation details, not alternative signal timing. Keep each confirmed pivot price and signed-volume observation aligned to the same timestamp."},
            {"id": "closed_h1_regime", "instruction":
             "Use the approved completed H1 OHLC observations for the existing CKiyosakiRegime selector and tracker. Preserve regime definitions, all twelve regime inputs and multipliers, and Minato hooks, formula and scoring dates."},
            {"id": "canonical_cooldown_policy", "instruction":
             "Canonical Stage 2 policy omits cooldown inputs and behavior. Historical InpMinBarsBetweenTrades references describe the legacy source and do not restore that gate or input."},
            {"id": "incremental_history", "instruction":
             "Avoid rebuilding all growing history each new bar. Use incremental completed-bar updates with initialization/history reset rebuilding, or a bounded rolling recalculation derived from existing pivot strength and lookback whose cost is independent of total history. For comparisons between pivots, a common cumulative-volume offset cancels; a bounded implementation must retain every signed increment between compared pivots. Preserve approved pivot inequalities, confirmation delay and lookback; introduce no strategy parameter. Equivalent storage and indexing may change; demonstrate equivalent intended observations on fixed history, including multi-bar catch-up and history reset."},
        ]
    requirements = spec.get("stage2_requirements")
    tasks = {key: deepcopy(spec[key]) for key in ("implementation_tasks", "stage2_implementation_tasks")
             if key in spec}
    if isinstance(requirements, dict):
        tasks["stage2_requirements"] = deepcopy(requirements)
    return {
        "brief_version": BRIEF_VERSION,
        "source_spec_canonical_sha256": hashlib.sha256(_canonical_json(spec).encode("utf-8")).hexdigest().upper(),
        "execution_contract": contract,
        "resolved_intent": resolved_intent,
        "legacy_observations": observations,
        "canonical_resolutions": resolutions,
        "stage1_implementation_context": tasks,
    }


def render_implementation_guidance(spec: dict) -> str:
    """The identical strategy instruction used by both Stage 2 model lanes."""
    brief = resolve_implementation_brief(spec)
    return (
        "\nRESOLVED STAGE 2 IMPLEMENTATION INSTRUCTION (shared by creation and repair):\n"
        "Implement the approved mathematical signals, exits, input bindings and bar timing in execution_contract. "
        "Canonical resolutions below explicitly distinguish the approved intent from audited legacy programming mechanics. "
        "Legacy observations and Stage 1 implementation context describe evidence/tasks, not instructions to preserve defective buffer layouts "
        "or replace the approved strategy. For other families the execution contract is unchanged. "
        "Preserve regime selector/tracker, regime definitions and twelve inputs, Minato scoring/hooks/dates, "
        "approved input grid, sizing and margin protections. Do not introduce strategy parameters. "
        "Repairs remain limited to the evidenced task and protected constraints; this instruction does not waive the "
        "strategy lock, compilation, verification, transaction commitment or publication requirements.\n"
        + _canonical_json(brief) + "\n")
