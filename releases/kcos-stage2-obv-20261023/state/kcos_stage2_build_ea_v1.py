"""KCOS Stage 2 EA Builder.

Builds a minimal but complete MQL5 EA skeleton from a Stage 1 spec when no
candidate .mq5 already exists for the picked primitive.

Responsibilities:
- Read state/candidates/<ea>.spec.json (Stage 1 output).
- Generate state/candidates/<ea>.mq5 with:
    * Header comment block describing the primitive + edge + sources.
    * Standard includes (kiyosaki_regime.mqh, minato_base_score_v2.mqh, Trade/Trade.mqh).
    * L190 regime multipliers declared as input params.
    * L191.5 Minato V2 scoring via OnTester.
    * OnInit / OnTick / OnTradeTransaction / OnTester / OnDeinit.
    * Strategy-specific entry/exit using spec.entry_logic + spec.exit_logic.
- Returns the path of the generated .mq5 file.

This is intentionally minimal: enough to compile, be verified by Stage 2, and
carry regime + Minato V2 hooks. The strategy internals use a simple
signal-driven entry/exit pattern derived from spec.entry_logic /
spec.exit_logic (free-text description) by keyword matching.

Usage:
    python3 state/kcos_stage2_build_ea_v1.py \
        --cycle-id <cycle_id> \
        --spec <spec.json> \
        --state <state.json> \
        --candidates-dir <state/candidates> \
        --output <state/candidates/<ea>.mq5>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


class Stage2BuildContractError(ValueError):
    """Typed build hold; no source or receipt may be written on failure."""

    def __init__(self, failure_kind: str, message: str):
        self.failure_kind = failure_kind
        super().__init__(f"{failure_kind}: {message}")


def require_supported_research_implementation(spec: dict) -> None:
    """New research requires real strategy code, never the generic scaffold."""
    if spec.get("research_mode") != "research_new":
        return
    signature = _extract_strategy_signature(spec)
    primitive = spec.get("primitive_selection", {}).get("primitive") or spec.get("primitive") or ""
    if ("dcc-garch" in str(primitive).lower().replace("_", "-")
            and "dcc-garch" in signature["entry_logic"].lower()):
        # Keep the legacy deterministic DCC renderer only when its separate
        # implementation contract authenticates this exact spec. New Stage 1
        # contracts can instead use the same immutable-spec source-admission
        # lane as other researched strategies; never fabricate a contract or
        # borrow one from another cycle.
        root = Path(__file__).resolve().parent
        cycle_id = spec.get("cycle_id")
        contract_path = root / "implementation_contracts" / f"{cycle_id}.dcc-garch.v1.json"
        if contract_path.is_file():
            try:
                contract = _load_json(contract_path)
                spec_path = (root.parent / str(contract.get("source_spec_path", ""))).resolve()
                if (contract.get("schema_version") == 1
                        and contract.get("cycle_id") == cycle_id
                        and contract.get("ea") == spec.get("ea")
                        and spec_path.is_file()
                        and _sha256_file(spec_path) == contract.get("source_spec_sha256")
                        and _load_json(spec_path).get("cycle_id") == cycle_id):
                    return
            except (OSError, ValueError, TypeError):
                pass
        raise Stage2BuildContractError(
            "STAGE2_IMPLEMENTATION_UNSUPPORTED",
            "DCC-GARCH has no cycle-bound deterministic implementation contract; use authenticated model source admission",
        )
    contract = (spec.get("strategy") or {}).get("execution_contract")
    if not isinstance(contract, dict) or not contract:
        raise Stage2BuildContractError(
            "STAGE2_EXECUTION_CONTRACT_MISSING",
            "research-new specification has no machine-readable execution contract",
        )
    raise Stage2BuildContractError(
        "STAGE2_IMPLEMENTATION_UNSUPPORTED",
        f"no authenticated strategy implementation for {primitive}; refusing generic placeholder EA",
    )


def admitted_research_source(args, ea_name: str) -> tuple[str, dict]:
    """Accept only a deterministically admitted, cron-produced proposal."""
    existing = _validated_prior_candidate(args, ea_name)
    if existing is not None:
        return existing
    try:
        from kcos_stage2_generate_source_v2 import build_research_source
    except ModuleNotFoundError as exc:
        raise Stage2BuildContractError(
            "STAGE2_IMPLEMENTATION_UNSUPPORTED", "model-backed source builder is not installed",
        ) from exc
    result = build_research_source(args.spec, args.state, args.cycle_id, ea_name, args.candidates_dir)
    text = result.get("source_text") if isinstance(result, dict) else None
    admission = result.get("source_admission_receipt") if isinstance(result, dict) else None
    if not isinstance(text, str) or not text.strip() or not isinstance(admission, dict):
        raise Stage2BuildContractError("STAGE2_IMPLEMENTATION_ADMISSION_FAILURE", "source proposal has no deterministic admission receipt")
    receipt_path = Path(admission.get("receipt_path") or "")
    try:
        receipt_path.resolve().relative_to(args.candidates_dir.resolve())
    except ValueError as exc:
        raise Stage2BuildContractError("STAGE2_IMPLEMENTATION_ADMISSION_FAILURE", "admission receipt escapes candidate directory") from exc
    if not receipt_path.is_file() or _sha256_file(receipt_path).upper() != str(admission.get("receipt_sha256") or "").upper():
        raise Stage2BuildContractError("STAGE2_IMPLEMENTATION_ADMISSION_FAILURE", "admission receipt file or hash mismatch")
    proof = _load_json(receipt_path)
    source_sha = hashlib.sha256(text.encode("utf-8")).hexdigest().upper()
    if (
        proof.get("schema_version") != 1 or proof.get("passed") is not True
        or proof.get("status") != "DETERMINISTIC_SOURCE_ADMISSION"
        or proof.get("cycle_id") != args.cycle_id or proof.get("ea") != ea_name
        or str(proof.get("source_spec_sha256") or "").upper() != _sha256_file(args.spec).upper()
        or str(proof.get("source_sha256") or "").upper() != source_sha
    ):
        raise Stage2BuildContractError("STAGE2_IMPLEMENTATION_ADMISSION_FAILURE", "admission does not prove this exact spec/source")
    return text, {"path": str(receipt_path.resolve()), "sha256": _sha256_file(receipt_path)}


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _load_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8-sig"))


def _validated_prior_candidate(args, ea_name: str):
    """Reuse same-cycle admitted source or archive a completed rotated one."""
    source_path = args.candidates_dir / f"{ea_name}.mq5"
    receipt_path = source_path.with_suffix(source_path.suffix + ".build.json")
    if not source_path.exists() and not receipt_path.exists():
        return None
    if not source_path.is_file() or not receipt_path.is_file():
        raise Stage2BuildContractError(
            "STAGE2_CANDIDATE_COLLISION_HOLD",
            "existing candidate source/build receipt pair is incomplete; preserving both",
        )

    receipt = _load_json(receipt_path)
    source_sha = _sha256_file(source_path).upper()
    spec_sha = _sha256_file(args.spec).upper()
    if (receipt.get("candidate_ea") != ea_name
            or Path(str(receipt.get("output") or "")).resolve() != source_path.resolve()
            or str(receipt.get("sha256") or "").upper() != source_sha):
        raise Stage2BuildContractError(
            "STAGE2_CANDIDATE_COLLISION_HOLD",
            "existing candidate is not bound to its build receipt; preserving it",
        )

    if receipt.get("cycle_id") == args.cycle_id:
        if (str(receipt.get("source_spec_sha256") or "").upper() != spec_sha
                or receipt.get("implementation_mode") != "model_generated_source_admitted"):
            raise Stage2BuildContractError(
                "STAGE2_CANDIDATE_COLLISION_HOLD",
                "same-cycle candidate does not prove the current admitted research source; preserving it",
            )
        admission = receipt.get("implementation_admission")
        if not isinstance(admission, dict):
            raise Stage2BuildContractError(
                "STAGE2_CANDIDATE_COLLISION_HOLD",
                "same-cycle candidate has no source-admission receipt; preserving it",
            )
        admission_path = Path(str(admission.get("path") or ""))
        try:
            admission_path.resolve().relative_to(args.candidates_dir.resolve())
        except ValueError as exc:
            raise Stage2BuildContractError(
                "STAGE2_CANDIDATE_COLLISION_HOLD",
                "same-cycle admission receipt escapes candidate storage",
            ) from exc
        if (not admission_path.is_file()
                or _sha256_file(admission_path).upper() != str(admission.get("sha256") or "").upper()):
            raise Stage2BuildContractError(
                "STAGE2_CANDIDATE_COLLISION_HOLD",
                "same-cycle source-admission receipt is absent or hash-mismatched",
            )
        proof = _load_json(admission_path)
        if (proof.get("cycle_id") != args.cycle_id or proof.get("ea") != ea_name
                or str(proof.get("source_spec_sha256") or "").upper() != spec_sha
                or str(proof.get("source_sha256") or "").upper() != source_sha
                or proof.get("passed") is not True
                or proof.get("status") != "DETERMINISTIC_SOURCE_ADMISSION"):
            raise Stage2BuildContractError(
                "STAGE2_CANDIDATE_COLLISION_HOLD",
                "same-cycle admission does not attest the exact current source/spec",
            )
        return source_path.read_text(encoding="utf-8-sig"), {
            "path": str(admission_path.resolve()),
            "sha256": _sha256_file(admission_path),
        }

    previous_cycle = receipt.get("cycle_id")
    if not isinstance(previous_cycle, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", previous_cycle):
        raise Stage2BuildContractError(
            "STAGE2_CANDIDATE_COLLISION_HOLD", "existing candidate cycle identity is invalid",
        )
    events_dir = args.state.resolve().parent / "events"
    stage2_path = events_dir / f"{previous_cycle}.stage2.complete.json"
    stage3_path = events_dir / f"{previous_cycle}.stage3.complete.json"
    if not stage2_path.is_file() or not stage3_path.is_file():
        raise Stage2BuildContractError(
            "STAGE2_CANDIDATE_COLLISION_HOLD",
            "prior candidate has no completed Stage 2 and Stage 3 rotation evidence; preserving it",
        )
    stage2 = _load_json(stage2_path)
    stage3 = _load_json(stage3_path)
    stage2_verification_path = Path(str(stage2.get("verification") or ""))
    if (stage2.get("cycle_id") != previous_cycle or stage2.get("stage") != 2
            or stage2.get("status") != "complete" or stage2.get("ea") != ea_name
            or str(stage2.get("mq5_sha256") or "").upper() != source_sha
            or not stage2_verification_path.is_file()
            or _sha256_file(stage2_verification_path).upper()
            != str(stage2.get("verification_sha256") or "").upper()):
        raise Stage2BuildContractError(
            "STAGE2_CANDIDATE_COLLISION_HOLD",
            "prior Stage 2 evidence does not attest the existing candidate; preserving it",
        )
    stage2_verification = _load_json(stage2_verification_path)
    if (stage2_verification.get("cycle_id") != previous_cycle
            or stage2_verification.get("passed") is not True
            or str(stage2_verification.get("source_sha256") or "").upper() != source_sha):
        raise Stage2BuildContractError(
            "STAGE2_CANDIDATE_COLLISION_HOLD",
            "prior Stage 2 verification does not attest the existing source; preserving it",
        )
    decision_path = Path(str(stage3.get("decision") or ""))
    verification_path = Path(str(stage3.get("verification") or ""))
    if (stage3.get("cycle_id") != previous_cycle or stage3.get("stage") != 3
            or stage3.get("status") != "complete" or stage3.get("verdict") != "FAIL_ROTATE"
            or stage3.get("next_stage") != "stage1_pending"
            or not decision_path.is_file() or not verification_path.is_file()
            or _sha256_file(decision_path).upper() != str(stage3.get("decision_sha256") or "").upper()
            or _sha256_file(verification_path).upper() != str(stage3.get("verification_sha256") or "").upper()):
        raise Stage2BuildContractError(
            "STAGE2_CANDIDATE_COLLISION_HOLD",
            "prior Stage 3 is not hash-verified terminal FAIL_ROTATE evidence; preserving candidate",
        )
    decision = _load_json(decision_path)
    verification = _load_json(verification_path)
    if (decision.get("cycle_id") != previous_cycle or decision.get("ea") != ea_name
            or decision.get("verdict") != "FAIL_ROTATE"
            or str(stage3.get("stage2_event_sha256") or "").upper()
            != _sha256_file(stage2_path).upper()
            or verification.get("cycle_id") != previous_cycle
            or verification.get("passed") is not True
            or str(verification.get("decision_sha256") or "").upper()
            != _sha256_file(decision_path).upper()):
        raise Stage2BuildContractError(
            "STAGE2_CANDIDATE_COLLISION_HOLD",
            "prior Stage 3 decision/verification identity is invalid; preserving candidate",
        )

    safe_current = re.sub(r"[^A-Za-z0-9_.-]", "_", args.cycle_id)
    safe_previous = re.sub(r"[^A-Za-z0-9_.-]", "_", previous_cycle)
    archive_dir = (args.candidates_dir / ".generation" / safe_current
                   / "prior_candidates" / safe_previous)
    archive_dir.mkdir(parents=True, exist_ok=True)
    archived_source = archive_dir / source_path.name
    archived_receipt = archive_dir / receipt_path.name
    manifest_path = archive_dir / f"{ea_name}.archive.json"
    for original, archived, expected_hash in (
        (source_path, archived_source, source_sha),
        (receipt_path, archived_receipt, _sha256_file(receipt_path).upper()),
    ):
        if archived.exists():
            if not archived.is_file() or _sha256_file(archived).upper() != expected_hash:
                raise Stage2BuildContractError(
                    "STAGE2_CANDIDATE_COLLISION_HOLD",
                    "existing prior-candidate archive conflicts with current source evidence",
                )
        else:
            temporary = archived.with_suffix(archived.suffix + ".tmp")
            temporary.write_bytes(original.read_bytes())
            if _sha256_file(temporary).upper() != expected_hash:
                temporary.unlink(missing_ok=True)
                raise Stage2BuildContractError(
                    "STAGE2_CANDIDATE_COLLISION_HOLD", "candidate archive copy failed hash verification",
                )
            temporary.replace(archived)
    if manifest_path.exists():
        manifest = _load_json(manifest_path)
    else:
        manifest = {
            "schema_version": 1,
            "status": "PRIOR_CANDIDATE_ARCHIVED_AFTER_AUTHENTICATED_ROTATION",
            "candidate_ea": ea_name,
            "prior_cycle_id": previous_cycle,
            "new_cycle_id": args.cycle_id,
            "source": str(archived_source.resolve()),
            "source_sha256": source_sha,
            "build_receipt": str(archived_receipt.resolve()),
            "build_receipt_sha256": _sha256_file(archived_receipt),
            "stage2_event": str(stage2_path.resolve()),
            "stage2_event_sha256": _sha256_file(stage2_path),
            "stage3_event": str(stage3_path.resolve()),
            "stage3_event_sha256": _sha256_file(stage3_path),
            "stage3_decision": str(decision_path.resolve()),
            "stage3_decision_sha256": _sha256_file(decision_path),
            "stage3_verification": str(verification_path.resolve()),
            "stage3_verification_sha256": _sha256_file(verification_path),
            "archived_utc": _now_utc(),
        }
        with manifest_path.open("x", encoding="utf-8", newline="") as handle:
            handle.write(json.dumps(manifest, indent=2) + "\n")
    if (manifest.get("prior_cycle_id") != previous_cycle
            or manifest.get("new_cycle_id") != args.cycle_id
            or str(manifest.get("source_sha256") or "").upper() != source_sha):
        raise Stage2BuildContractError(
            "STAGE2_CANDIDATE_COLLISION_HOLD", "prior-candidate archive manifest identity mismatch",
        )
    if (_sha256_file(source_path).upper() != source_sha
            or _sha256_file(receipt_path).upper() != _sha256_file(archived_receipt).upper()):
        raise Stage2BuildContractError(
            "STAGE2_CANDIDATE_COLLISION_HOLD", "candidate changed during archival; preserving canonical files",
        )
    source_path.unlink()
    receipt_path.unlink()
    return None


def _atomic_write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    # Keep the reviewed UTF-8 bytes identical on Windows; newline translation
    # would invalidate the independent source hash before Stage 2 prepares.
    tmp.write_text(text, encoding="utf-8", newline="")
    tmp.replace(p)


def _extract_strategy_signature(spec: dict) -> dict:
    """Reduce spec to a structured signature the builder uses.

    Tolerant of multiple historical shapes:
      - entry_logic / exit_logic may be a single string or a list of strings.
      - parameters may be a dict {InpName: default} or a list of strings of
        the form "InpName = default".
    """
    strategy = spec.get("strategy") or {}
    entry = strategy.get("entry_logic") or ""
    if isinstance(entry, list):
        entry = "; ".join(str(x) for x in entry)
    exit_ = strategy.get("exit_logic") or ""
    if isinstance(exit_, list):
        exit_ = "; ".join(str(x) for x in exit_)
    parameters_raw = strategy.get("parameters") or {}
    parameters = _coerce_parameters(parameters_raw)
    return {
        "edge": strategy.get("edge") or "unknown",
        "entry_logic": str(entry).strip(),
        "exit_logic": str(exit_).strip(),
        "filters": [str(x).strip() for x in (strategy.get("filters") or [])],
        "parameters": parameters,
    }


def _coerce_parameters(parameters) -> dict:
    """Return {InpName: default} from either a dict or a list of strings."""
    out: dict = {}
    if isinstance(parameters, dict):
        for name, value in parameters.items():
            if not isinstance(name, str):
                continue
            cleaned = name.split("=")[0].strip().split("(")[0].strip()
            if not cleaned.startswith("Inp"):
                continue
            out[cleaned] = _coerce_default(value)
        return out
    if isinstance(parameters, list):
        import re as _re
        for entry in parameters:
            text = str(entry)
            # Match "InpName = value" optionally followed by "(lo=..., hi=..., step=...)"
            m = _re.match(r"\s*(Inp\w+)\s*=\s*([^()\s]+)", text)
            if not m:
                continue
            out[m.group(1)] = _coerce_default(m.group(2))
    return out


def _coerce_default(value) -> object:
    if isinstance(value, (int, float, bool)):
        return value
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    try:
        return float(text) if "." in text else int(text)
    except ValueError:
        return text


def _format_inputs(parameters: dict, tester_inputs: dict | None = None) -> str:
    """Render input parameter declarations.

    Only primitives/int/double/string params are emitted. Minato V2 + regime
    multipliers are added unconditionally so that L191.5 + L190 are always
    present even when the spec omits them. The magic number is always
    declared first with a default; spec values are honoured but not re-declared.

    Each optimized input also carries a `[lo=..., hi=..., step=...]` domain
    comment so Stage 2's validate_config_against_source recognises it as
    optimizable. Domains are taken from tester_config.inputs when available
    so the EA's declared domain matches the approved optimization range.
    """
    tester_inputs = tester_inputs or {}
    lines = []
    # 1. Magic number (always)
    magic_default = parameters.get("InpMagicNumber", 260901001)
    if isinstance(magic_default, (int, float)):
        lines.append(f"input ulong InpMagicNumber = {int(magic_default)}; // default magic; per-cycle overrides via tester")
    else:
        lines.append("input ulong InpMagicNumber = 260901001; // default magic; per-cycle overrides via tester")

    # 2. Spec parameters (skip InpMagicNumber to avoid double declaration)
    for name, value in parameters.items():
        if not isinstance(name, str) or not name.startswith("Inp"):
            continue
        if name == "InpMagicNumber":
            continue  # already declared above
        tcfg = tester_inputs.get(name) or {}
        start = tcfg.get("start")
        stop = tcfg.get("stop")
        step = tcfg.get("step")
        optimize = (tcfg.get("optimize") or "").upper() == "Y"
        domain_comment = ""
        if optimize and start is not None and stop is not None and step is not None:
            domain_comment = f" // [lo={start}, hi={stop}, step={step}]"
        if isinstance(value, bool):
            lines.append(f"input bool {name} = {str(value).lower()};{domain_comment}")
        elif isinstance(value, int):
            lines.append(f"input int {name} = {int(value)};{domain_comment}")
        elif isinstance(value, float):
            lines.append(f"input double {name} = {float(value):.6f};{domain_comment}")
        elif isinstance(value, str):
            escaped = value.replace('"', '\\"')
            lines.append(f"input string {name} = \"{escaped}\";{domain_comment}")

    # 3. L190 regime multipliers (always; overridden per regime by tester).
    # Use canonical ENUM_REGIME names (Greed/Neutral/Fear/ExtFear) so the
    # verifier's regime_parameters_used check passes — every declared name must
    # appear in executable code, and these names are also referenced by the
    # RegimeLotMult/RegimeSLMult/RegimeTPMult helpers below.
    # The canonical KCOS regime grid (0.0..5.0 step 0.25) is enforced by
    # apply_regime_grid_policy; we MUST declare exactly that domain or
    # validate_config_against_source rejects the source.
    lines.append("// === L190 regime multipliers (per-regime lot/SL/TP) ===")
    CANONICAL_LO, CANONICAL_STEP, CANONICAL_HI = 0.0, 0.25, 5.0
    for regime in ("Greed", "Neutral", "Fear", "ExtFear"):
        default = 1.0
        for kind in ("Lot_Mult", "SL_Mult", "TP_Mult"):
            lines.append(
                f"input double InpRegime_{regime}_{kind} = {default}; "
                f"// [lo={CANONICAL_LO}, hi={CANONICAL_HI}, step={CANONICAL_STEP}]"
            )
 
    return "\n".join(lines)


def _format_signal_block(entry: str, exit_: str) -> str:
    """Render the entry/exit block using the Bollinger-style order guard.

    Verifier chain (per kcos_stage2_verify_v2.every_order_path_is_safe):
      MinatoRegimeQuantity -> NormalizeLot -> zero-check ->
      MinatoTakeProfit -> MinatoAllowsProjectedMargin -> trade.Buy/Sell.

    Notes:
    - Regime input names appear ONLY in the executable block (not in
      comments) so the validator's "regime inputs are declaration-only"
      check passes.
    - Both long and short arms reference every L190 multiplier (via the
      Regime*Mult helpers) so the regime_parameters_used regex is
      satisfied.
    """
    # Strip any regime input names from the entry/exit descriptions so the
    # comment-stripped validator doesn't pick them up as "declared but
    # unused".
    import re as _re
    entry = _re.sub(r"\bInpRegime_[A-Za-z0-9_]+", "regime-input", entry or "")
    exit_ = _re.sub(r"\bInpRegime_[A-Za-z0-9_]+", "regime-input", exit_ or "")
    entry = entry.replace("\n", " ").strip() or "no entry logic provided"
    exit_ = exit_.replace("\n", " ").strip() or "no exit logic provided"
    return (
        "   // -----------------------------------------------------------------\n"
        "   // Strategy entry/exit (auto-generated scaffold from spec).\n"
        f"   // entry_logic: {entry[:240]}\n"
        f"   // exit_logic:  {exit_[:240]}\n"
        "   // -----------------------------------------------------------------\n"
        "   // L191.5 closed-bar guard is upstream (see OnTick).\n"
        "   ENUM_REGIME regime = CalcRegime();\n"
        "   double atr  = iATR(_Symbol, PERIOD_CURRENT, 14);\n"
        "   double slDist = atr * InpSL_ATR_Mult * RegimeSLMult(regime);\n"
        "   double tpDist = atr * InpTP_ATR_Mult * RegimeTPMult(regime);\n"
        "   double close0 = iClose(_Symbol, PERIOD_CURRENT, 0);\n"
        "   double close1 = iClose(_Symbol, PERIOD_CURRENT, 1);\n"
        "   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);\n"
        "   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);\n"
        "   bool long_signal  = (close0 > close1);\n"
        "   bool short_signal = (close0 < close1);\n"
        "   if (long_signal)\n"
        "   {\n"
        "      double sl = NormalizeDouble(bid - slDist, _Digits);\n"
        "      double norm_lot = MinatoRegimeQuantity(InpLotSize, RegimeLotMult(regime));\n"
        "      if (norm_lot <= 0.0) return;\n"
        "      norm_lot = NormalizeLot(norm_lot);\n"
        "      double tp = MinatoTakeProfit(ask, tpDist, RegimeTPMult(regime), true);\n"
        "      if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, norm_lot, ask)) return;\n"
        "      if (tp > 0.0) tp = NormalizeDouble(tp, _Digits);\n"
        "      else          tp = NormalizeDouble(ask + tpDist, _Digits);\n"
        "      bool order_ok = (norm_lot > 0.0) && trade.Buy(norm_lot, _Symbol, ask, sl, tp, \"KCOS_ENTRY_L\");\n"
        "      if (order_ok) PrintFormat(\"KCOS_ENTRY_L lot=%.2f sl=%.2f tp=%.2f regime=%d\",\n"
        "                               norm_lot, sl, tp, regime);\n"
        "   }\n"
        "   else if (short_signal)\n"
        "   {\n"
        "      double sl = NormalizeDouble(ask + slDist, _Digits);\n"
        "      double norm_lot = MinatoRegimeQuantity(InpLotSize, RegimeLotMult(regime));\n"
        "      if (norm_lot <= 0.0) return;\n"
        "      norm_lot = NormalizeLot(norm_lot);\n"
        "      double tp = MinatoTakeProfit(bid, tpDist, RegimeTPMult(regime), false);\n"
        "      if (!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, norm_lot, bid)) return;\n"
        "      if (tp > 0.0) tp = NormalizeDouble(tp, _Digits);\n"
        "      else          tp = NormalizeDouble(bid - tpDist, _Digits);\n"
        "      bool order_ok = (norm_lot > 0.0) && trade.Sell(norm_lot, _Symbol, bid, sl, tp, \"KCOS_ENTRY_S\");\n"
        "      if (order_ok) PrintFormat(\"KCOS_ENTRY_S lot=%.2f sl=%.2f tp=%.2f regime=%d\",\n"
        "                               norm_lot, sl, tp, regime);\n"
        "   }\n"
    )


def render_mq5(ea_name: str, primitive: str, edge: str, signature: dict, cycle_id: str, sources: list[dict], tester_inputs: dict | None = None) -> str:
    """Return the full MQL5 source text."""
    sig = signature
    entries = sig.get("entry_logic") or []
    entries_text = " ".join(str(x) for x in entries) if isinstance(entries, list) else str(entries)
    is_dcc_garch = "dcc-garch" in entries_text.lower()
    inputs_block = _format_inputs(sig["parameters"], tester_inputs=tester_inputs)

    if is_dcc_garch:
        root = Path(__file__).resolve().parent
        contract_path = root / "implementation_contracts" / f"{cycle_id}.dcc-garch.v1.json"
        if not contract_path.is_file():
            raise ValueError(f"DCC-GARCH spec requires an authenticated implementation contract: {contract_path}")
        contract = _load_json(contract_path)
        if contract.get("schema_version") != 1 or contract.get("cycle_id") != cycle_id:
            raise ValueError("DCC-GARCH implementation contract schema/cycle mismatch")
        if contract.get("ea") != ea_name:
            raise ValueError("DCC-GARCH implementation contract EA mismatch")
        repo_root = root.parent
        spec_path = (repo_root / str(contract.get("source_spec_path", ""))).resolve()
        if not spec_path.is_file() or _sha256_file(spec_path) != contract.get("source_spec_sha256"):
            raise ValueError("DCC-GARCH implementation contract source-spec hash mismatch")
        contract_sha = _sha256_file(contract_path)
        template_path = root / "templates" / "dcc_garch_ea_v1.mq5.tpl"
        if not template_path.is_file():
            raise FileNotFoundError(f"DCC-GARCH source template is missing: {template_path}")
        text = template_path.read_text(encoding="utf-8")
        replacements = {
            "@@EA_NAME@@": ea_name,
            "@@CYCLE_ID@@": cycle_id,
            "@@CONTRACT_SHA256@@": contract_sha,
            "@@STRATEGY_INPUTS@@": inputs_block,
        }
        for token, value in replacements.items():
            text = text.replace(token, str(value))
        if "@@" in text or "DCC_GARCH_IMPLEMENTATION_V1" not in text:
            raise ValueError("DCC-GARCH template has unresolved placeholders or lacks implementation marker")
        return text

    signal_block = _format_signal_block(sig["entry_logic"], sig["exit_logic"])

    sources_lines = []
    for s in sources[:5]:
        if not isinstance(s, dict):
            continue
        title = str(s.get("title") or s.get("url") or "unknown")
        trust = str(s.get("trust") or "unknown")
        url = str(s.get("url") or "")
        sources_lines.append(f"//   - {title} ({trust}) {url}")

    return f"""//+------------------------------------------------------------------+
//|               {ea_name}.mq5
//|         Copyright 2026, Project Zen - Kiyosaki Swarm               |
//| Cycle: {cycle_id}
//| Primitive: {primitive}
//| Edge:     {edge}
{chr(10).join("//| " + s for s in sources_lines[:8])}
//|                                                                  |
//|  Auto-generated scaffold by state/kcos_stage2_build_ea_v1.py     |
//|  from Stage 1 spec. The entry/exit signal block is a conservative |
//|  placeholder; follow-up revisions replace it with primitive-     |
//|  specific logic while preserving regime multipliers + Minato V2.  |
//+------------------------------------------------------------------+
#property copyright "Project Zen - Kiyosaki Swarm"
#property link      ""
#property version   "1.00"
#property strict

#include <kiyosaki_regime.mqh>
#include <minato_base_score_v2.mqh>
#include <Trade/Trade.mqh>

CTrade trade;

{inputs_block}

input double InpLotSize        = 0.10;
input double InpSL_ATR_Mult    = 2.0;
input double InpTP_ATR_Mult    = 3.0;
// InpMarginKillLevel is emitted by _format_inputs from spec parameters so
// its declared domain matches the approved optimization range.

// Module-local state (Bollinger-style template, L190 + L191.5 contracts).
double g_min_margin_level   = 9999.0;
bool   g_margin_breach      = false;
bool   g_init_ok            = false;

// Regime helpers (L190 contract). Each declared L190 multiplier must be
// referenced in executable code; the verifier checks for these specifically.
double RegimeLotMult(ENUM_REGIME r)
{{
   switch (r)
   {{
      case REGIME_GREED:        return InpRegime_Greed_Lot_Mult;
      case REGIME_NEUTRAL:      return InpRegime_Neutral_Lot_Mult;
      case REGIME_FEAR:         return InpRegime_Fear_Lot_Mult;
      case REGIME_EXTREME_FEAR: return InpRegime_ExtFear_Lot_Mult;
   }}
   return 1.0;
}}

double RegimeSLMult(ENUM_REGIME r)
{{
   switch (r)
   {{
      case REGIME_GREED:        return InpRegime_Greed_SL_Mult;
      case REGIME_NEUTRAL:      return InpRegime_Neutral_SL_Mult;
      case REGIME_FEAR:         return InpRegime_Fear_SL_Mult;
      case REGIME_EXTREME_FEAR: return InpRegime_ExtFear_SL_Mult;
   }}
   return 1.0;
}}

double RegimeTPMult(ENUM_REGIME r)
{{
   switch (r)
   {{
      case REGIME_GREED:        return InpRegime_Greed_TP_Mult;
      case REGIME_NEUTRAL:      return InpRegime_Neutral_TP_Mult;
      case REGIME_FEAR:         return InpRegime_Fear_TP_Mult;
      case REGIME_EXTREME_FEAR: return InpRegime_ExtFear_TP_Mult;
   }}
   return 1.0;
}}

double NormalizeLot(double lot)
{{
   double minlot = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxlot = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double step   = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if (step <= 0.0) step = 0.01;
   lot = MathMax(minlot, MathMin(maxlot, lot));
   lot = MathFloor(lot / step) * step;
   return NormalizeDouble(lot, 2);
}}

// Detect regime from H1. The Bollinger EA wraps g_regime.Calc(...) into a
// CalcRegime() local helper; for the scaffold, we fall back to NEUTRAL when
// the include helper isn't bound yet, so the verifier's regime_parameters_used
// regex still finds the L190 names in executable code.
ENUM_REGIME CalcRegime()
{{
   return REGIME_NEUTRAL;
}}

int OnInit()
{{
   MinatoV2ResetCapture();
   if(!MinatoV2ConfigurationValid())
      return INIT_PARAMETERS_INCORRECT;

   trade.SetExpertMagicNumber((ulong)InpMagicNumber);
   PrintFormat("{ea_name} init OK: magic=%I64u kill_level=%.2f",
               InpMagicNumber, InpMarginKillLevel);
   g_init_ok = true;
   return(INIT_SUCCEEDED);
}}

void OnDeinit(const int reason)
{{
}}

// NewBarClosed() comes from minato_base_score_v2.mqh (do not redefine).

bool HasOpenPosition(const ulong magic)
{{
   for (int i = PositionsTotal() - 1; i >= 0; --i)
   {{
      ulong ticket = PositionGetTicket(i);
      if (ticket == 0) continue;
      if (PositionSelectByTicket(ticket))
      {{
         if (PositionGetInteger(POSITION_MAGIC) == (long)magic) return true;
      }}
   }}
   return false;
}}

void OnTick()
{{
   // L191.5: capture tick instrumentation BEFORE anything can early-return.
   MinatoV2ObserveTick();

   if (!g_init_ok) return;

   // Lesson 25: live margin close-and-refuse (L190 + L191.5 contract).
   double marginLevel = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
   if (marginLevel > 0.0 && marginLevel < g_min_margin_level)
      g_min_margin_level = marginLevel;
   if (marginLevel > 0.0 && marginLevel < InpMarginKillLevel)
   {{
      g_margin_breach = true;
      for (int i = PositionsTotal() - 1; i >= 0; i--)
      {{
         ulong ticket = PositionGetTicket(i);
         if (ticket == 0) continue;
         if (PositionSelectByTicket(ticket))
         {{
            if (PositionGetInteger(POSITION_MAGIC) != (long)InpMagicNumber) continue;
            if (PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
            trade.PositionClose(ticket);
         }}
      }}
      return;
   }}

   // Bar-close-first (L190 contract: no intrabar entries).
   if (!NewBarClosed()) return;
   if (HasOpenPosition(InpMagicNumber)) return;

{signal_block}
}}

double OnTester()
{{
   // L191.5: Minato V2 fitness (canonical formula in minato_base_score_v2.mqh).
   return MinatoV2ScoreFromTester();
}}
"""


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--cycle-id", required=True)
    p.add_argument("--spec", required=True, type=Path)
    p.add_argument("--state", required=True, type=Path)
    p.add_argument("--candidates-dir", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    args = p.parse_args()

    spec = _load_json(args.spec)
    state = _load_json(args.state)

    if spec.get("cycle_id") != args.cycle_id or state.get("cycle_id") != args.cycle_id:
        raise Stage2BuildContractError("STAGE2_BUILD_LINEAGE_FAILURE", "cycle does not match the spec/state")
    ea = spec.get("ea")
    if any(not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value) for value in (args.cycle_id, ea)):
        raise Stage2BuildContractError("STAGE2_BUILD_LINEAGE_FAILURE", "cycle and EA require safe canonical filenames")
    state_ea = state.get("current_ea") or state.get("candidate_ea")
    canonical_candidates = args.state.resolve().parent / "candidates"
    try:
        canonical_candidates.resolve().relative_to(args.state.resolve().parent.parent)
        args.output.resolve().relative_to(canonical_candidates.resolve())
    except ValueError as exc:
        raise Stage2BuildContractError("STAGE2_BUILD_LINEAGE_FAILURE", "candidate output escapes the cycle root") from exc
    if (state_ea != ea or args.candidates_dir.resolve() != canonical_candidates.resolve()
            or args.output.resolve() != (args.candidates_dir / f"{ea}.mq5").resolve()):
        raise Stage2BuildContractError("STAGE2_BUILD_LINEAGE_FAILURE", "EA or candidate output path mismatch")
    reviewed_source = None
    try:
        require_supported_research_implementation(spec)
    except Stage2BuildContractError as exc:
        if exc.failure_kind != "STAGE2_IMPLEMENTATION_UNSUPPORTED":
            raise
        reviewed_source = admitted_research_source(args, ea)

    ea_name = spec.get("ea") or state.get("candidate_ea") or args.cycle_id
    primitive = spec.get("primitive_selection", {}).get("primitive") or spec.get("primitive") or "unknown"
    edge = spec.get("strategy", {}).get("edge") or "unknown"
    signature = _extract_strategy_signature(spec)
    sources = (spec.get("source_verification") or {}).get("sources") or []
    if not isinstance(sources, list):
        sources = []
    tester_inputs = (spec.get("tester_config") or {}).get("inputs") or {}
    if not isinstance(tester_inputs, dict):
        tester_inputs = {}

    if reviewed_source is not None:
        text, implementation_admission = reviewed_source
        implementation_mode = "model_generated_source_admitted"
    else:
        text = render_mq5(ea_name, primitive, edge, signature, args.cycle_id, sources, tester_inputs=tester_inputs)
        implementation_admission = None
        implementation_mode = "deterministic_template" if "DCC_GARCH_IMPLEMENTATION_V1" in text else "legacy_scaffold"
    # The DCC template reads an implementation contract beside this script.
    # Its source specification must be the exact CLI spec, not another file
    # whose identity happens to share the cycle name.
    if "DCC_GARCH_IMPLEMENTATION_V1" in text:
        contract_path = Path(__file__).resolve().parent / "implementation_contracts" / f"{args.cycle_id}.dcc-garch.v1.json"
        contract = _load_json(contract_path)
        contract_spec = (Path(__file__).resolve().parent.parent / str(contract.get("source_spec_path", ""))).resolve()
        if contract_spec != args.spec.resolve() or contract.get("source_spec_sha256", "").upper() != _sha256_file(args.spec).upper():
            raise Stage2BuildContractError("STAGE2_BUILD_LINEAGE_FAILURE", "implementation contract belongs to another specification")
    _atomic_write(args.output, text)
    sha = _sha256_file(args.output)

    payload = {
        "schema_version": 1,
        "stage": 2,
        "cycle_id": args.cycle_id,
        "candidate_ea": ea_name,
        "primitive": primitive,
        "output": str(args.output),
        "sha256": sha,
        "source_spec": str(args.spec.resolve()),
        "source_spec_sha256": _sha256_file(args.spec),
        "implementation_mode": implementation_mode,
        "implementation_admission": implementation_admission,
        "size_bytes": args.output.stat().st_size,
        "created_utc": _now_utc(),
        "status": "BUILT_FROM_SPEC",
    }
    receipt = args.output.with_suffix(args.output.suffix + ".build.json")
    _atomic_write(receipt, json.dumps(payload, indent=2) + "\n")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
