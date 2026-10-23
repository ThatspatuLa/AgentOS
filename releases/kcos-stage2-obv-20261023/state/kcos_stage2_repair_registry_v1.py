"""Typed registry for bounded deterministic Stage 2 repair handlers.

The registry is descriptive and dispatch-enforcing: repair lanes retain their
existing evidence gates, while every deterministic handler is named here with
its diagnostic family, applicability contract, verification gates, and locks.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


PROTECTED_CONSTRAINTS = (
    "regime_selector_and_tracker",
    "minato_score_callbacks_and_dates",
    "closed_bar_strategy_signals_and_inputs",
    "sizing_zero_tp_and_margin_contracts",
    "stage1_lineage_and_downstream_authority",
)


@dataclass(frozen=True)
class RepairDefinition:
    rule_id: str
    diagnostic: tuple[str, ...]
    applicability: str
    handler: str
    verification_checks: tuple[str, ...]
    protected_constraints: tuple[str, ...] = PROTECTED_CONSTRAINTS


_DEFINITIONS = (
    RepairDefinition("family_safety_contract", ("every_order_path_margin_quantity_tp_safe", "atr_uses_closed_period_buffer", "closed_bar_calculation"), "Only the existing authenticated Stage 1 EA source and the exact family/signature gates implemented by the common source-repair pass; the OBV closed-ATR/quantity replay requires its named EA identity and exact legacy helper/order shape.", "_repair_impl", ("compile_clean", "atr_uses_closed_period_buffer", "every_order_path_margin_quantity_tp_safe", "protected_behavior_contract")),
    RepairDefinition("authenticated_ctrade_api", ("compile_clean", "SetTypeFillingByVolume", "SetDeviationsInPoints"), "Only authenticated MQL5 CTrade API compile diagnostics for the current source.", "apply_authenticated_ctrade_api_fix", ("compile_clean", "stage2_verifier")),
    RepairDefinition("known_mql5_compile", ("compile_clean",), "Only a supported exact diagnostic admitted by the known-compiler-fix classifier.", "apply_known_mql5_compile_fix", ("compile_clean", "stage2_verifier")),
    RepairDefinition("cooldown_controls", ("cooldown_zero_disables_entry", "cooldown input"), "Only authenticated Stage 2 contract checks that prohibit cooldown controls.", "apply_stage2_cooldown_removal", ("cooldown_zero_disables_entry", "time_stop_zero_disables_close", "stage2_verifier")),
    RepairDefinition("stage2_order_quantity", ("every_order_path_margin_quantity_tp_safe", "NormalizeLot"), "Only the authenticated order-path quantity gate and exact lot-assignment signatures.", "apply_stage2_order_quantity_normalization", ("every_order_path_margin_quantity_tp_safe", "stage2_verifier")),
    RepairDefinition("stage2_regime_quantity", ("every_order_path_margin_quantity_tp_safe", "MinatoRegimeQuantity"), "Only when order quantity normalization reports its exact not-applicable signature and regime sizing remains unchanged.", "apply_stage2_regime_quantity_normalization", ("every_order_path_margin_quantity_tp_safe", "stage2_verifier")),
    RepairDefinition("arrayrange_row_bounds", ("ArraySize", "array out of range"), "Only declared two-dimensional arrays with the verifier's flattened row-bound defect.", "apply_multidimensional_array_row_bounds_fix", ("multidimensional_array_row_bounds", "stage2_verifier")),
    RepairDefinition("quantile_interpolation_bounds", ("quantile", "array out of range"), "Only authenticated quantile interpolation journal evidence matching its exact helper.", "apply_quantile_interpolation_bounds_fix", ("quantile_interpolation_bounds", "stage2_verifier")),
    RepairDefinition("dcc_closed_return_bounds", ("dcc", "closed return", "array out of range"), "Only the authenticated DCC function with a proven n-minus-two closed-return indexing defect.", "apply_dcc_closed_return_bounds_fix", ("dcc_closed_return_arrays", "stage2_verifier")),
    RepairDefinition("stage3_timeframe_bounds", ("stage3 timeframe", "array bounds"), "Only a hash-bound Stage 3 runtime ticket with the supported timeframe defect.", "apply_authenticated_stage3_timeframe_fix", ("stage3_runtime_ticket", "stage2_verifier")),
    RepairDefinition("donchian_closed_bar_indexing", ("donchian", "closed bar", "array bounds"), "Only authenticated Donchian source and runtime evidence satisfying the exact index signature.", "apply_donchian_closed_bar_indexing_fix", ("closed_bar_calculation", "stage2_verifier")),
    RepairDefinition("rolling_volatility_history", ("rolling volatility", "history window"), "Only the supported rolling-volatility source signature and matching Stage 3 diagnostic.", "apply_rolling_volatility_history_window_fix", ("closed_bar_calculation", "stage2_verifier")),
    RepairDefinition("semivariance_closed_bar_bounds", ("semivariance", "closed bar", "array bounds"), "Only the supported semivariance closed-bar source signature.", "apply_semivariance_closed_bar_bounds_fix", ("closed_bar_calculation", "stage2_verifier")),
    RepairDefinition("volatility_clustering_buffer", ("volatility clustering", "indexed buffer"), "Only the supported volatility-clustering indexed-buffer source signature.", "apply_volatility_clustering_indexed_buffer_fix", ("closed_bar_calculation", "stage2_verifier")),
    RepairDefinition("regime_enum_order", ("regime enum", "order"), "Only authenticated compiler evidence for the supported regime enum/order type defect.", "apply_kcos_regime_enum_order_fix", ("compile_clean", "stage2_verifier")),
)

REPAIR_REGISTRY = {definition.rule_id: definition for definition in _DEFINITIONS}
HANDLER_TO_RULE = {definition.handler: definition.rule_id for definition in _DEFINITIONS}


def registered_repair(rule_id: str, handler: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    definition = REPAIR_REGISTRY.get(rule_id)
    if definition is None:
        raise ValueError(f"unregistered Stage 2 deterministic repair: {rule_id}")
    if getattr(handler, "__name__", "") != definition.handler:
        raise ValueError(f"Stage 2 repair handler mismatch for {rule_id}")
    return handler(*args, **kwargs)


def validate_registry() -> None:
    if len(HANDLER_TO_RULE) != len(REPAIR_REGISTRY):
        raise ValueError("Stage 2 deterministic repair registry has duplicate handlers")
    for definition in _DEFINITIONS:
        if not (definition.diagnostic and definition.applicability and definition.handler
                and definition.verification_checks and definition.protected_constraints):
            raise ValueError(f"incomplete Stage 2 repair registry definition: {definition.rule_id}")


validate_registry()
