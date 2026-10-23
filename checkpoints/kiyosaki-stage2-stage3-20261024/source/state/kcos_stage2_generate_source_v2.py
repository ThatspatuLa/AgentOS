"""Cron-owned source proposal/review; never compiles or launches a tester.

The model receives no tools. Acceptance here is a reviewed source proposal,
not compilation, strategy parity proof, backtest evidence, or Stage 2 success.
"""
from __future__ import annotations
import hashlib
import inspect
import json
import math
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kcos_execution_contract_v2 import validate_execution_contract
from kcos_stage2_implementation_brief_v1 import render_implementation_guidance
from kcos_tester_config_v2 import (resolve_input_spec, stage2_spec_from_spec, REGIME_GRID_START,
                                   REGIME_GRID_STEP, REGIME_GRID_STOP)
from kcos_input_contract_v2 import discover_inputs, assert_no_shared_input_shadowing, coerce_value
from kcos_runtime_paths_v2 import assert_safe_root, mt5_data_root
import kcos_stage2_learning_v1 as stage2_learning
from kcos_stage2_verify_v2 import (closed_bar_check, generic_closed_bar_audit,
                                 gk_sample_update_is_closed_bar_gated,
                                 closed_bar_indicator_reads_are_safe,
                                 dcc_closed_return_arrays_are_bounded,
                                 spec_requires_dcc_garch,
                                 atr_period_input_name,
                                 atr_uses_authenticated_closed_buffer,
                                 gjr_base_lot_sizing_contract,
                                 every_order_path_is_safe, live_margin_path_is_safe,
                                 normalize_lot_helper_is_authenticated,
                                  canonical_initialization_hooks_present,
                                  regime_parameters_used, margin_kill_120,
                                 _canonical_margin_wrapper_is_concrete, _function_body, _mask_comments_and_strings, _simple_refusal_end,
                                 _normalized_expression)

DCC_BOUNDED_IMPLEMENTATION_EXAMPLE = 'bool ComputeDCCSignals(double &z_corr, int &regime_out)\n{\n   int eth_total  = CopyClose(_Symbol, _Period, 0, InpDccLookback + 260, g_eth_close);\n   int btc_total  = CopyClose(InpBTCCrossSymbol, _Period, 0, InpDccLookback + 260, g_btc_close);\n   if(eth_total <= 0 || btc_total <= 0) return false;\n\n   int n = MathMin(eth_total, btc_total);\n   if(n < InpDccLookback + 10) return false;\n\n   // Build log-returns arrays from closed bars (shift 1 onward)\n   double r_eth[], r_btc[];\n   ArrayResize(r_eth, n - 2);\n   ArrayResize(r_btc, n - 2);\n   for(int k = 0; k < n - 2; k++)\n   {\n      r_eth[k] = MathLog(g_eth_close[k + 1]) - MathLog(g_eth_close[k + 2]);\n      r_btc[k] = MathLog(g_btc_close[k + 1]) - MathLog(g_btc_close[k + 2]);\n   }\n\n   // Sample variance seed over InpDccLookback initial returns\n   double var_seed_eth = 0.0, var_seed_btc = 0.0;\n   int seed_n = InpDccLookback;\n   if(seed_n > n - 2) seed_n = n - 2;\n   double mean_eth = 0.0, mean_btc = 0.0;\n   for(int k = 0; k < seed_n; k++) { mean_eth += r_eth[k]; mean_btc += r_btc[k]; }\n   mean_eth /= seed_n; mean_btc /= seed_n;\n   for(int k = 0; k < seed_n; k++)\n   {\n      var_seed_eth += (r_eth[k] - mean_eth) * (r_eth[k] - mean_eth);\n      var_seed_btc += (r_btc[k] - mean_btc) * (r_btc[k] - mean_btc);\n   }\n   var_seed_eth /= seed_n; var_seed_btc /= seed_n;\n   if(var_seed_eth <= 0.0 || var_seed_btc <= 0.0) return false;\n\n   // Univariate GARCH(1,1) recursions (forward pass through history)\n   double h_eth = var_seed_eth, h_btc = var_seed_btc;\n   double omega_eth = var_seed_eth * 0.05;\n   double omega_btc = var_seed_btc * 0.05;\n   double alpha = 0.06, beta = 0.92;\n   double h_eth_hist[];\n   double h_btc_hist[];\n   ArrayResize(h_eth_hist, n - 2);\n   ArrayResize(h_btc_hist, n - 2);\n   for(int k = 0; k < n - 2; k++)\n   {\n      h_eth = omega_eth + alpha * r_eth[k] * r_eth[k] + beta * h_eth;\n      h_btc = omega_btc + alpha * r_btc[k] * r_btc[k] + beta * h_btc;\n      h_eth_hist[k] = h_eth;\n      h_btc_hist[k] = h_btc;\n   }\n   if(h_eth <= 0.0 || h_btc <= 0.0) return false;\n\n   // Q_bar (unconditional outer product) using z_eth/z_btc from full window\n   double qbar_ee = 0.0, qbar_bb = 0.0, qbar_eb = 0.0;\n   int qbar_n = MathMin(InpDccLookback, n - 2);\n   for(int k = 0; k < qbar_n; k++)\n   {\n      double z_e = r_eth[k] / MathSqrt(MathMax(h_eth_hist[k], 1e-12));\n      double z_b = r_btc[k] / MathSqrt(MathMax(h_btc_hist[k], 1e-12));\n      qbar_ee += z_e * z_e;\n      qbar_bb += z_b * z_b;\n      qbar_eb += z_e * z_b;\n   }\n   qbar_ee /= qbar_n; qbar_bb /= qbar_n; qbar_eb /= qbar_n;\n   if(qbar_ee <= 0.0 || qbar_bb <= 0.0) return false;\n\n   // DCC Q_t recursion on closed bars - run forward through history\n   double q_ee = qbar_ee, q_bb = qbar_bb, q_eb = qbar_eb;\n   double one_minus = 1.0 - InpDccEmaQ - InpDccEmaP;\n   if(one_minus < 0.0) one_minus = 0.0;\n   for(int k = qbar_n; k < n - 2; k++)\n   {\n      double z_e = r_eth[k] / MathSqrt(MathMax(h_eth_hist[k], 1e-12));\n      double z_b = r_btc[k] / MathSqrt(MathMax(h_btc_hist[k], 1e-12));\n      q_ee = one_minus * qbar_ee + InpDccEmaQ * z_e * z_e + InpDccEmaP * q_ee;\n      q_bb = one_minus * qbar_bb + InpDccEmaQ * z_b * z_b + InpDccEmaP * q_bb;\n      q_eb = one_minus * qbar_eb + InpDccEmaQ * z_e * z_b + InpDccEmaP * q_eb;\n   }\n\n   double sqrt_ee = MathSqrt(MathMax(q_ee, 1e-12));\n   double sqrt_bb = MathSqrt(MathMax(q_bb, 1e-12));\n   double corr = q_eb / (sqrt_ee * sqrt_bb);\n   corr = MathMax(-0.999, MathMin(0.999, corr));\n\n   // Build correlation history over lookback window for z_corr_t\n   double corr_hist[];\n   ArrayResize(corr_hist, InpDccLookback);\n   double q_ee_h = qbar_ee, q_bb_h = qbar_bb, q_eb_h = qbar_eb;\n   for(int k = 0; k < InpDccLookback; k++)\n   {\n      int src = k + 1;\n      if(src < n - 2)\n      {\n         double z_e = r_eth[src] / MathSqrt(MathMax(h_eth_hist[src], 1e-12));\n         double z_b = r_btc[src] / MathSqrt(MathMax(h_btc_hist[src], 1e-12));\n         q_ee_h = one_minus * qbar_ee + InpDccEmaQ * z_e * z_e + InpDccEmaP * q_ee_h;\n         q_bb_h = one_minus * qbar_bb + InpDccEmaQ * z_b * z_b + InpDccEmaP * q_bb_h;\n         q_eb_h = one_minus * qbar_eb + InpDccEmaQ * z_e * z_b + InpDccEmaP * q_eb_h;\n      }\n      double se = MathSqrt(MathMax(q_ee_h, 1e-12));\n      double sb = MathSqrt(MathMax(q_bb_h, 1e-12));\n      double c  = q_eb_h / (se * sb);\n      corr_hist[k] = MathMax(-0.999, MathMin(0.999, c));\n   }\n   z_corr = RollingZScore(corr_hist, InpDccLookback, InpDccLookback, corr);\n\n   // Volatility z-score using sqrt(h) over InpRegimeVolLookup window\n   double vol_hist[];\n   ArrayResize(vol_hist, InpRegimeVolLookup);\n   int vol_start = (n - 2) - InpRegimeVolLookup;\n   if(vol_start < 0) vol_start = 0;\n   for(int k = 0; k < InpRegimeVolLookup; k++)\n   {\n      int src = vol_start + k;\n      if(src < 0) src = 0;\n      if(src >= n - 2) src = n - 3;\n      vol_hist[k] = MathSqrt(MathMax(h_eth_hist[src], 1e-12));\n   }\n   double z_vol = RollingZScore(vol_hist, InpRegimeVolLookup, InpRegimeVolLookup,\n                                MathSqrt(MathMax(h_eth, 1e-12)));\n   regime_out = ClassifyRegime(z_vol);\n   return true;\n}'
ROLLING_ZSCORE_BOUNDED_REFERENCE = """double RollingZScore(const double &values[], int total, int lookback, double current)
{
   if(lookback <= 0 || total < lookback) return 0.0;
   double sum = 0.0;
   for(int k = total - lookback; k < total; k++) sum += values[k];
   double mean = sum / (double)lookback;
   double var = 0.0;
   for(int k = total - lookback; k < total; k++)
   {
      double d = values[k] - mean;
      var += d * d;
   }
   double std = MathSqrt(var / (double)lookback);
   if(std <= 0.0) return 0.0;
   return (current - mean) / std;
}"""

ROOT = assert_safe_root()
HERMES_ROOT = Path(r"C:\Users\Tyson\AppData\Local\hermes\hermes-agent")
HERMES_PYTHON = HERMES_ROOT / "venv" / "Scripts" / "python.exe"
MODEL, PROVIDER = "MiniMax-M3", "minimax"
TIMEOUT_SECONDS = 240
MAX_PROPOSAL_ATTEMPTS = 4
ONTESTER_TOKEN = "@@KCOS_CANONICAL_ONTESTER@@"
ONTESTER = "double OnTester()\n{\n   return MinatoV2ScoreFromTester();\n}\n"
SOURCE_PROTOCOL = "KCOS_SOURCE_PROPOSAL_V1"


def is_dcc_garch_spec(spec: dict) -> bool:
    """Recognize authenticated DCC specs across Stage1/effective-spec shapes."""
    selection = spec.get("primitive_selection")
    selected = selection.get("primitive") if isinstance(selection, dict) else selection
    strategy = spec.get("strategy") if isinstance(spec.get("strategy"), dict) else {}
    labels = (selected, spec.get("primitive"), spec.get("candidate_ea"), spec.get("ea"),
              spec.get("rationale"), strategy.get("edge"), strategy.get("name"))
    if any(re.search(r"dcc[\s_-]*garch", str(value or ""), re.I) for value in labels):
        return True

    requirements = spec.get("stage2_requirements")
    requirements = requirements if isinstance(requirements, dict) else {}
    declared = requirements.get("expected_inp_declarations")
    declared = set(declared) if isinstance(declared, list) else set()
    if not declared:
        tester = spec.get("tester_config")
        inputs = tester.get("inputs") if isinstance(tester, dict) else None
        declared = set(inputs) if isinstance(inputs, dict) else set()
    return {"InpDccLookback", "InpBTCCrossSymbol"}.issubset(declared)


def authenticated_contract_guidance(instructions: str, spec: dict, contract: dict) -> str:
    """Append the same resolved strategy instruction used by the repair lane."""
    strategy = spec.get("strategy") if isinstance(spec.get("strategy"), dict) else {}
    execution_contract = strategy.get("execution_contract")
    if not isinstance(execution_contract, dict) or not execution_contract:
        execution_contract = contract if isinstance(contract, dict) else {}
    if not execution_contract:
        raise ValueError("authenticated Stage 1 execution contract is missing")
    # Some callers already pass a validated top-level contract. Keep that
    # compatibility without mutating their authenticated spec.
    instruction_spec = spec
    if not (strategy.get("execution_contract") or spec.get("execution_contract")):
        instruction_spec = {**spec, "execution_contract": execution_contract}
    return (instructions +
        "\n\nAUTHENTICATED STRATEGY CONTRACT (controlling strategy authority):\n"
        "Implement only the signals, regime transitions, exits, warmup, and order refusal rules in this JSON. "
        "All strategy-specific prose or examples elsewhere in these instructions are subordinate and apply only "
        "when the authenticated contract explicitly specifies the same behavior. In particular, do not copy GK, "
        "DCC, neutral-only, transition, pending-reversal, or regime-triggered-close behavior from another family. "
        "Preserve every equation and input binding exactly. When the contract requires refusing an invalid broker "
        "stop distance, keep the computed SL/TP unchanged and skip the order; never widen or clamp them.\n"
        + render_implementation_guidance(instruction_spec))


def remove_foreign_strategy_guidance(instructions: str) -> str:
    """Strip legacy family-specific rules from the shared non-DCC creation prompt.

    These passages used to give every non-DCC EA neutral-only transitions and
    a GK rolling-window signal, then rely on the contract appended afterward to
    override them. The contract must be the only source of trading behavior.
    """
    replacements = (
        (
            re.compile(r"- NewBarClosed detects the first tick of a new bar\. On that tick, shift 1 is the just-closed bar and a market order at the current ask/bid is the next-bar-open entry\. This satisfies the closed-bar and next-open requirements\.\n"),
            "- NewBarClosed detects the first tick of a new bar. Treat shift 1 as the most recently completed bar. "
            "Use market entry at the next-bar price only when the authenticated execution_contract specifies "
            "that timing; otherwise preserve its exact entry timing and order type.\n",
        ),
        (
            re.compile(r"- With no open position, entries are only .*?do not invent extra neutral exits\.", re.S),
            "- Derive entries, exits, regime transitions, and conflict handling only from the authenticated "
            "Stage 1 execution_contract. Do not add neutral-only entries, delayed reversals, or regime-triggered "
            "closes unless that exact behavior is specified by the contract.",
        ),
        (
            re.compile(r"- The exit contract names the current closed-bar regime for SL/TP multipliers\..*?do not substitute entry-time regime\.(?: Opposite-direction conflict handling must not require prev==NEUTRAL\.)?\n"),
            "- Apply the authenticated execution_contract's exact exit timing, regime binding, and SL/TP "
            "formula. Do not substitute entry-time or current regime unless that contract specifies it.\n",
        ),
        (
            re.compile(r"OnTick ordering:.*?Current valid closed-bar observations are part of the rolling last-N values used by the contract z-score\.", re.S),
            "OnTick ordering: capture Minato state and enforce live margin close/refusal before new entries. "
            "Manage positions, calculate indicators, update rolling state, and evaluate signals only as the "
            "authenticated execution_contract specifies. Use completed bars wherever the contract requires them; "
            "do not add GK sampling or any other strategy calculation absent from the contract.",
        ),
        (
            re.compile(r"- Keep an explicit InpTP_ATR_Mult >= 0\.5 refusal as specified, even though the current optimization grid starts at 1\.5\.\n"),
            "- Implement input refusals only when the authenticated execution_contract requires them.\n",
        ),
    )
    result = instructions
    for pattern, replacement in replacements:
        result, count = pattern.subn(replacement, result, count=1)
        if count != 1:
            raise ValueError("shared Stage 2 source instructions no longer match the audited family-specific passages")
    return result

# An empty explicit built-in toolset is supported by the installed resolver.
# Register it only inside the ephemeral CLI child; change no installed files.
# Verify the final schemas as well, so future Hermes drift cannot add tools.
CLI_BOOTSTRAP = r'''
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import toolsets
toolsets.TOOLSETS["kcos_source_only"] = {"description":"No tools", "tools":[]}
import model_tools
original = model_tools.get_tool_definitions
def fenced(*args, **kwargs):
    result = original(*args, **kwargs)
    if result:
        raise RuntimeError("KCOS_SOURCE_ONLY_TOOL_SCHEMA_NOT_EMPTY")
    return result
model_tools.get_tool_definitions = fenced
import run_agent
run_agent.get_tool_definitions = fenced
import hashlib
import json
original_agent = run_agent.AIAgent
source_prompt_path = Path(sys.argv[2])
WORKER_ROLE = """You are a text-only MQL5 source proposal and independent review worker.
The caller supplies authenticated contracts and include bytes. Your task is to return one fenced MQL5 source block or the requested JSON review text,
not execute MT5, inspect files, or write files. Source proposals are not claims of compiled results.
The cron owns filesystem publication, compiler checks and tester runs after your response.
The cron validates the source and input use deterministically. The absence of filesystem tools does not prevent
generating source text or reviewing supplied source. Keep include declarations intact; use only
the authenticated Minato include and Trade/Trade.mqh when CTrade is used. Do not add unused
standard-library includes. Approved
tester overrides need no change to compiled defaults. Never claim execution or fabricate results.
If genuinely unsupported, return the required structured reason. Otherwise return the exact requested response format.
"""
class source_worker(original_agent):
    # Preserve Hermes class constants/methods consulted by its conversation loop.
    def __init__(self, *args, **kwargs):
        kwargs.update(skip_context_files=True, load_soul_identity=False, skip_memory=True,
                      prefill_messages=[], ephemeral_system_prompt=WORKER_ROLE,
                      platform="kcos_source_worker", session_db=None, max_iterations=1,
                      enabled_toolsets=["kcos_source_only"], save_trajectories=False)
        super().__init__(*args, **kwargs)
        if self.tools or self.valid_tool_names:
            raise RuntimeError("KCOS_SOURCE_ONLY_AGENT_TOOLS_NOT_EMPTY")
        if (not self.skip_context_files or self.load_soul_identity or self._memory_enabled
                or self._user_profile_enabled or self._memory_store is not None
                or self._memory_manager is not None):
            raise RuntimeError("KCOS_SOURCE_WORKER_CONTEXT_NOT_ISOLATED")
        metadata = {"schema_version":1, "platform":self.platform,
                    "skip_context_files":self.skip_context_files, "skip_memory":kwargs["skip_memory"],
                    "memory_enabled":self._memory_enabled, "user_profile_enabled":self._user_profile_enabled,
                    "load_soul_identity":self.load_soul_identity, "tools":[],
                    "role_sha256":hashlib.sha256(WORKER_ROLE.encode()).hexdigest().upper()}
        target = source_prompt_path.with_name(source_prompt_path.name.replace(".prompt.txt", ".worker-context.json"))
        with target.open("x", encoding="utf-8") as stream:
            json.dump(metadata, stream, indent=2)
# Hermes --oneshot imports this constructor directly and bypasses cli.py.
run_agent.AIAgent = source_worker
from hermes_cli.main import main
prompt = source_prompt_path.read_text(encoding="utf-8")
sys.argv = ["hermes", "--cli", "--safe-mode", "--ignore-rules", "--no-restore-cwd",
            "--toolsets", "kcos_source_only", "--provider", "minimax", "-m", "MiniMax-M3", "-z", prompt]
main()
'''


def fail(message):
    raise ValueError("STAGE2_MODEL_SOURCE_HOLD: " + message)


class ReturnedProposalFormatError(ValueError):
    """A completed model call returned invalid text; never a launcher failure."""


def response_fail(message):
    raise ReturnedProposalFormatError("STAGE2_MODEL_SOURCE_HOLD: " + message)


def require(value, message):
    if not value:
        fail(message)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest().upper()


def sha(path: Path) -> str:
    return digest(path.read_bytes())


def stage1_event_lineage_sha256(path: Path) -> str:
    """Hash the immutable Stage 1 event payload, ignoring supervisor consumption metadata."""
    event = load(path)
    for key in ("consumed", "consumed_utc", "consumed_action"):
        event.pop(key, None)
    return digest(json.dumps(event, sort_keys=True, separators=(",", ":"),
                            ensure_ascii=False).encode("utf-8"))


def load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    require(isinstance(value, dict), "artifact must contain an object")
    return value


def immutable(path: Path, value: str) -> None:
    with path.open("x", encoding="utf-8", newline="") as handle:
        handle.write(value)


def write_json(path: Path, value: dict) -> None:
    immutable(path, json.dumps(value, indent=2) + "\n")


def authenticate(spec_path: Path, state_path: Path, cycle_id: str, ea_name: str, candidates_dir: Path):
    state, spec = load(state_path), load(spec_path)
    require(re.fullmatch(r"[A-Za-z0-9_-]+", ea_name), "candidate EA filename is unsafe")
    require(type(state.get("schema_version")) is int and state["schema_version"] == 2,
            "state schema is invalid")
    require(state.get("cycle_id") == cycle_id and state.get("current_ea") == ea_name,
            "canonical state cycle/EA mismatch")
    require(state.get("current_stage") in ("stage2_pending", "stage2_running"), "Stage 2 is not admitted")
    require(spec.get("cycle_id") == cycle_id and spec.get("ea") == ea_name and spec.get("stage") == 1
            and spec.get("schema_version") == 2 and spec.get("decision") == "approved_for_stage2"
            and spec.get("research_mode") == "research_new", "Stage 1 approved research identity mismatch")
    event_path = Path(state.get("last_stage1_event") or "")
    require(event_path.is_file(), "completed Stage 1 event is absent")
    event = load(event_path)
    require(event.get("cycle_id") == cycle_id and event.get("ea") == ea_name
            and event.get("stage") == 1 and event.get("status") == "complete", "Stage 1 completion identity mismatch")
    require(Path(event.get("artifact", "")).resolve() == spec_path.resolve()
            and event.get("artifact_sha256") == sha(spec_path), "Stage 1 event does not attest this exact spec")
    require(candidates_dir.resolve() == (ROOT / "state" / "candidates").resolve(), "candidate directory is not canonical")
    contract = validate_execution_contract(spec)
    approval_path = ROOT / "state" / "contracts" / "minato_base_score_approved_v2.json"
    approval = load(approval_path)
    minato = mt5_data_root() / "MQL5" / "Include" / "minato_base_score_v2.mqh"
    require(approval.get("approved") is True and sha(minato) == approval.get("sha256", approval.get("score_include_sha256")),
            "canonical Minato include is not operator-approved")
    require(approval.get("score_version") == "MINATO_BASE_SCORE_V2.0", "approved score version is invalid")
    return state, spec, contract, event_path, minato


def code_only(source: str) -> str:
    def blank(match):
        return "".join(char if char in "\r\n" else " " for char in match[0])
    return re.sub(r"/\*.*?\*/|//[^\r\n]*", blank, source, flags=re.S)


def source_input_usage(source: str, spec: dict, shared_contract: dict) -> dict:
    """Build deterministic input-use evidence from executable source text."""
    code = _mask_comments_and_strings(source)
    shared_inputs = shared_contract.get("inputs", {})
    candidate_inputs = discover_inputs({"__kcos_candidate__.mq5": source}).get("inputs", {})
    config = spec.get("tester_config") or {}
    plan = spec.get("test_plan") or {}

    def fixed_window_metadata(name: str, item: dict) -> dict | None:
        """Authenticate the two fixed date labels as tester metadata, not EA signals."""
        field = {"InpFromDate": "from_date", "InpToDate": "to_date"}.get(name)
        if field is None:
            return None
        expected = config.get(field)
        declaration = candidate_inputs.get(name) or {}
        if (not isinstance(expected, str) or not expected
                or plan.get(field) != expected
                or config.get("from_date") != plan.get("from_date")
                or config.get("to_date") != plan.get("to_date")
                or not isinstance(item, dict)
                or any(item.get(key) != expected for key in ("default", "start", "step", "stop"))
                or str(item.get("optimize", "N")).upper() != "N"
                or str(declaration.get("mql_type") or "").casefold() != "string"
                or declaration.get("source_default") != expected):
            return None
        return {"owner": "tester_config.test_window", "shared": False,
                "metadata_only": True, "bound_field": field, "value": expected}

    usage = {}
    failures = []
    for name, item in spec["tester_config"]["inputs"].items():
        declaration = shared_inputs.get(name)
        if declaration:
            usage[name] = {"owner": declaration["owner"], "shared": True}
            continue
        if not re.search(r"\binput\s+\w+\s+" + re.escape(name) + r"\b", code):
            failures.append(f"{name}: declared input is missing")
            continue
        metadata_usage = fixed_window_metadata(name, item)
        if metadata_usage is not None:
            usage[name] = metadata_usage
            continue
        occurrences = len(re.findall(r"\b" + re.escape(name) + r"\b", code))
        if occurrences < 2:
            failures.append(f"{name}: declaration-only input")
            continue
        usage[name] = {"owner": "ea_source", "shared": False,
                       "executable_occurrences": occurrences}
    require(not failures, "input usage failures: " + "; ".join(failures))
    return usage


def strategy_input_lock_manifest(spec: dict) -> dict:
    """Snapshot the authenticated Stage 1 strategy input values and ranges."""
    inputs = spec.get("tester_config", {}).get("inputs", {})
    require(isinstance(inputs, dict), "STRATEGY_INPUT_LOCK: tester_config.inputs must be an object")
    fields = ("type", "default", "start", "step", "stop", "optimize", "domain_provenance")
    return {name: {field: item.get(field) for field in fields}
            for name, item in sorted(inputs.items()) if isinstance(item, dict)}


def validate_strategy_input_defaults(source: str, spec: dict, shared_contract: dict) -> dict:
    """Reject generated source that changes an approved input default or type."""
    candidate_owner = "__kcos_candidate__.mq5"
    candidate = discover_inputs({candidate_owner: source})
    shared_inputs = shared_contract.get("inputs", {})
    locked = {}
    failures = []
    for name, item in spec.get("tester_config", {}).get("inputs", {}).items():
        if name in shared_inputs:
            continue  # Authenticated include ownership and runtime bindings govern these.
        declaration = candidate["inputs"].get(name)
        if declaration is None or not declaration.get("supported"):
            failures.append(f"{name}: configured input has no supported source declaration")
            continue
        mql_type = declaration["mql_type"]
        expected_type = item.get("type")
        if expected_type and str(expected_type).casefold() != mql_type.casefold():
            failures.append(f"{name}: source type {mql_type} differs from approved type {expected_type}")
            continue
        try:
            raw_default = item.get("default")
            # Stage 1 stores string defaults as serialized MQL literals in
            # tester_config (for example, the two quote characters around
            # BTCUSD are part of the JSON value).  Compare the runtime string
            # value to the source declaration, not that literal syntax.
            if (mql_type.casefold() == "string" and isinstance(raw_default, str)
                    and len(raw_default) >= 2 and raw_default[0] == '"'
                    and raw_default[-1] == '"'):
                expected_default = json.loads(raw_default)
                require(isinstance(expected_default, str),
                        f"{name}: serialized string default is not a string literal")
            else:
                expected_default = coerce_value(raw_default, mql_type)
        except (TypeError, ValueError) as exc:
            failures.append(f"{name}: approved default cannot be represented as {mql_type}: {exc}")
            continue
        actual_default = declaration.get("source_default")
        if type(actual_default) is not type(expected_default) or actual_default != expected_default:
            failures.append(f"{name}: source default {actual_default!r} differs from approved default {expected_default!r}")
            continue
        locked[name] = {"type": mql_type, "default": actual_default}
    require(not failures, "STRATEGY_INPUT_LOCK: " + "; ".join(failures))
    return locked


def repair_local_regime_macro_namespace(source: str, shared_contract: dict) -> tuple[str, list[dict]]:
    """Rename only local regime labels that accidentally use Minato's namespace.

    The canonical Minato include owns its MINATO_* macros. Some model proposals
    used MINATO_REGIME_* for the EA's own four-state enum, which trips the
    fail-closed no-override guard even though those names are absent from the
    approved include. Rename those identifiers only after authenticating that
    they do not collide with any macro in the exact approved include bytes.
    Numeric values and executable logic are unchanged.
    """
    code = code_only(source)
    local_names = sorted(set(re.findall(
        r"(?im)^\s*#\s*define\s+(MINATO_REGIME_[A-Za-z0-9_]+)\b", code)))
    if not local_names:
        return source, []
    canonical_macros: set[str] = set()
    include_sources = (shared_contract or {}).get("sources", {})
    minato_includes = [(Path(path), digest_value) for path, digest_value in include_sources.items()
                       if Path(path).name.casefold() == "minato_base_score_v2.mqh"]
    require(len(minato_includes) == 1, "cannot authenticate the single approved Minato include for regime namespace repair")
    include_path, expected_sha = minato_includes[0]
    require(include_path.is_file() and sha(include_path).upper() == str(expected_sha).upper(),
            "approved Minato include changed before regime namespace repair")
    include_text = include_path.read_text(encoding="utf-8-sig", errors="replace")
    canonical_macros.update(re.findall(
        r"(?im)^\s*#\s*define\s+([A-Za-z_][A-Za-z0-9_]*)\b", code_only(include_text)))
    collisions = sorted(set(local_names) & canonical_macros)
    require(not collisions, "local regime namespace collides with approved Minato macro(s): " + ", ".join(collisions))
    replacements = {name: name.replace("MINATO_REGIME_", "KCOS_REGIME_", 1) for name in local_names}
    updated = source
    for old, new in replacements.items():
        updated = re.sub(r"\b" + re.escape(old) + r"\b", new, updated)
    return updated, [{"kind": "local_regime_macro_namespace_renamed",
                      "reason": "avoid reserved Minato namespace without changing regime values or behavior",
                      "renamed_identifiers": replacements}]


def ensure_trade_class_include(source: str) -> tuple[str, list[dict]]:
    """Add the standard MQL5 CTrade dependency when the source uses CTrade."""
    code = code_only(source)
    includes = re.findall(r"(?im)^\s*#\s*include\s*[<\"]([^>\"]+)[>\"]", code)
    uses_trade = re.search(r"\bCTrade\b", code) is not None
    if not uses_trade or any(name.replace("\\", "/").casefold() == "trade/trade.mqh" for name in includes):
        return source, []
    marker = re.search(r"(?im)^\s*#\s*include\s*[<\"][^>\"]+[>\"][^\r\n]*$", source)
    require(marker is not None, "CTrade is used but source has no include insertion point")
    repaired = source[:marker.end()] + "\n#include <Trade/Trade.mqh>" + source[marker.end():]
    return repaired, [{"kind": "standard_trade_include_added", "reason": "CTrade is used by the EA"}]


def remove_unused_position_info_include(source: str) -> tuple[str, list[dict]]:
    """Remove Trade/PositionInfo.mqh only when its CPositionInfo class is unused."""
    code = code_only(source)
    if re.search(r"\bCPositionInfo\b", code) is not None:
        return source, []
    pattern = re.compile(
        r"(?im)^[ \t]*#\s*include\s*[<\"]Trade[/\\]PositionInfo\.mqh[>\"][^\r\n]*(?:\r?\n|$)")
    repaired, count = pattern.subn("", source)
    if not count:
        return source, []
    return repaired, [{"kind": "unused_position_info_include_removed",
                       "reason": "CPositionInfo is not used by the EA"}]


def ensure_approved_optimization_domain_comments(source: str, spec: dict,
                                                 shared_contract: dict | None = None) -> tuple[str, list[dict]]:
    """Restore only missing source-domain comments from authenticated Stage 1 metadata."""
    shared_inputs = (shared_contract or {}).get("inputs", {})
    inputs = spec.get("tester_config", {}).get("inputs", {})
    declaration_pattern = lambda name: re.compile(
        rf"(?m)^[ \t]*input\s+[A-Za-z_][A-Za-z0-9_<>]*\s+{re.escape(name)}\s*=[^;\r\n]*;[^\r\n]*$")
    domain_pattern = re.compile(
        r"\[\s*lo\s*=\s*([-+0-9.eE]+)\s*,\s*hi\s*=\s*([-+0-9.eE]+)\s*,\s*step\s*=\s*([-+0-9.eE]+)\s*\]"
        r"|\[\s*([-+0-9.eE]+)\s*\.\.\s*([-+0-9.eE]+)\s*\]\s*step\s*([-+0-9.eE]+)")
    repairs = []
    insertions = []
    for name, item in inputs.items():
        provenance = item.get("domain_provenance", {}) if isinstance(item, dict) else {}
        if name in shared_inputs or item.get("optimize") != "Y":
            continue
        method = provenance.get("method")
        if method == "source_comment":
            domain = provenance.get("source_domain")
            reason = "authenticated Stage 1 source_comment metadata"
            if (domain is None and provenance.get("research_status") == "researched"
                    and isinstance(provenance.get("basis"), str) and provenance["basis"].strip()):
                try:
                    start, step, stop = (float(item[key]) for key in ("start", "step", "stop"))
                    default = float(item["default"])
                except (KeyError, TypeError, ValueError):
                    fail(f"OPTIMIZATION_DOMAIN_LOCK: {name} researched range metadata is incomplete")
                if (not all(math.isfinite(value) for value in (start, step, stop, default))
                        or step <= 0 or stop <= start or default < start or default > stop):
                    fail(f"OPTIMIZATION_DOMAIN_LOCK: {name} researched range metadata is invalid")
                steps = (stop - start) / step
                default_steps = (default - start) / step
                if (not math.isclose(steps, round(steps), rel_tol=0.0, abs_tol=1e-9)
                        or not math.isclose(default_steps, round(default_steps), rel_tol=0.0, abs_tol=1e-9)):
                    fail(f"OPTIMIZATION_DOMAIN_LOCK: {name} researched range is not aligned to its authenticated step")
                domain = [start, stop, step]
                reason = "researched Stage 1 source_comment range bounds and step"
        elif method == "stage2_regime_grid_policy":
            domain = [REGIME_GRID_START, REGIME_GRID_STOP, REGIME_GRID_STEP]
            try:
                configured = (float(item["start"]), float(item["stop"]), float(item["step"]))
            except (KeyError, TypeError, ValueError):
                fail(f"OPTIMIZATION_DOMAIN_LOCK: {name} has incomplete regime-grid settings")
            approved = (REGIME_GRID_START, REGIME_GRID_STOP, REGIME_GRID_STEP)
            if configured != approved:
                fail(f"OPTIMIZATION_DOMAIN_LOCK: {name} regime settings {configured!r} differ from canonical grid {approved!r}")
            reason = "canonical Stage 2 regime-grid policy"
        else:
            continue
        if not isinstance(domain, (list, tuple)) or len(domain) != 3:
            fail(f"OPTIMIZATION_DOMAIN_LOCK: {name} has no authenticated three-value source_domain")
        try:
            values = tuple(float(value) for value in domain)
        except (TypeError, ValueError):
            fail(f"OPTIMIZATION_DOMAIN_LOCK: {name} source_domain is not numeric")
        if not all(math.isfinite(value) for value in values) or values[0] == values[1] or values[2] <= 0:
            fail(f"OPTIMIZATION_DOMAIN_LOCK: {name} source_domain is invalid")
        fmt = lambda value: str(int(value)) if float(value).is_integer() else format(float(value), ".12g")
        marker = f"[lo={fmt(values[0])},hi={fmt(values[1])},step={fmt(values[2])}]"
        matches = list(declaration_pattern(name).finditer(source))
        if len(matches) != 1:
            fail(f"OPTIMIZATION_DOMAIN_LOCK: expected one source declaration for {name}, found {len(matches)}")
        match = matches[0]
        existing = domain_pattern.search(match.group(0))
        if existing:
            groups = existing.groups()
            actual = tuple(float(value) for value in (groups[:3] if groups[0] is not None else groups[3:]))
            if actual != values:
                fail(f"OPTIMIZATION_DOMAIN_LOCK: {name} source comment {actual!r} differs from authenticated domain {values!r}")
            continue
        declaration_line = match.group(0)
        semicolon = declaration_line.find(";")
        if semicolon < 0:
            fail(f"OPTIMIZATION_DOMAIN_LOCK: {name} declaration has no terminating semicolon")
        comment_start = declaration_line.find("//", semicolon + 1)
        if comment_start >= 0:
            insertions.append((match.end(), " " + marker))
        else:
            insertions.append((match.start() + semicolon + 1, " // " + marker))
        repairs.append({"kind": "approved_optimization_domain_comment_added", "input": name,
                        "source_domain": [fmt(value) for value in values],
                        "reason": reason})
    # Apply offsets against the original source from right to left. Input
    # metadata order is independent of declaration order, so reversing the
    # insertion list alone can shift later offsets into declaration tokens.
    for position, text in sorted(insertions, key=lambda item: item[0], reverse=True):
        source = source[:position] + text + source[position:]
    return source, repairs


def surgical_repair_feedback(exc: ValueError, diagnosis: dict, rejected_source: str | None) -> str:
    """Ask the model to repair rejected code while retaining the locked strategy surface."""
    feedback = ("\nPRIOR PROPOSAL REJECTED; its immutable output is preserved in the attempt artifacts.\n"
                "VALIDATION FAILURE: " + str(exc)
                + "\nCRON DIAGNOSIS: " + json.dumps(diagnosis, ensure_ascii=False)
                + "\nRepair only the failing implementation code. Preserve every approved strategy input name, type, default, regime value, custom maximum, scoring rule, and optimization range exactly. Do not tune, substitute, clamp, or remove strategy settings. Acceptance requires deterministic source admission, compilation, and Stage 2 verification.\n")
    if isinstance(rejected_source, str) and len(rejected_source) <= 160000:
        feedback += ("\nREJECTED COMPLETE SOURCE FOR SURGICAL REPAIR (preserve its approved input declarations; fix only the diagnosed code defect):\n"
                     "```mql5\n" + rejected_source + "\n```\n")
    return feedback


def normalize_canonical_ontester_hook(source: str) -> str:
    """Accept the exact canonical scorer body or its cron-owned insertion marker."""
    marker_comment = re.compile(r"(?m)^[ \t]*//[ \t]*" + re.escape(ONTESTER_TOKEN) + r"[ \t]*$")
    marker_comments = list(marker_comment.finditer(source))
    # A model may put the requested marker on a comment line. Promote that
    # sole marker to the cron-owned insertion token rather than deleting it.
    # If the exact canonical scorer body is also present, remove the redundant
    # banner and normalize that body to the one canonical hook.
    if len(marker_comments) == 1 and source.count(ONTESTER_TOKEN) == 1:
        without_banner = marker_comment.sub("", source)
        code_without_banner = _mask_comments_and_strings(without_banner)
        canonical = re.compile(
            r"\bdouble\s+OnTester\s*\(\s*\)\s*\{\s*"
            r"return\s+MinatoV2ScoreFromTester\s*\(\s*\)\s*;\s*\}", re.I)
        matches_without_banner = list(canonical.finditer(code_without_banner))
        if len(matches_without_banner) == 1:
            match = matches_without_banner[0]
            return without_banner[:match.start()] + ONTESTER_TOKEN + without_banner[match.end():]
        return marker_comment.sub(ONTESTER_TOKEN, source, count=1)
    if source.count(ONTESTER_TOKEN) == 1:
        return source
    if ONTESTER_TOKEN in source:
        return source
    code = _mask_comments_and_strings(source)
    pattern = re.compile(
        r"\bdouble\s+OnTester\s*\(\s*\)\s*\{\s*"
        r"return\s+MinatoV2ScoreFromTester\s*\(\s*\)\s*;\s*\}", re.I)
    matches = list(pattern.finditer(code))
    if len(matches) != 1:
        return source
    match = matches[0]
    return source[:match.start()] + ONTESTER_TOKEN + source[match.end():]


FREE_MARGIN_NOTIONAL_HELPER = """bool FreeMarginAllowsNotional(double volume, double price)
{
   if(volume <= 0.0 || price <= 0.0) return false;
   double notional = volume * price;
   double free_margin = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   if(!MathIsValidNumber(free_margin) || free_margin <= 0.0) return false;
   return (free_margin >= 2.0 * notional);
}
"""


def ensure_free_margin_notional_guards(source: str) -> tuple[str, list[dict]]:
    """Add only the deterministic Stage 1 2x free-margin guard after canonical projected-margin checks."""
    code = _mask_comments_and_strings(source)
    guard_pattern = re.compile(
        r"if\s*\(\s*!\s*MinatoAllowsProjectedMargin\s*\(\s*"
        r"ORDER_TYPE_(BUY|SELL)\s*,\s*([A-Za-z_]\w*)\s*,\s*([A-Za-z_]\w*)\s*\)\s*\)\s*return\s*;",
        re.I)
    guards = list(guard_pattern.finditer(code))
    if not guards:
        return source, []
    repairs = []
    insertions = []
    for guard in guards:
        volume, price = guard.group(2), guard.group(3)
        tail = code[guard.end():guard.end() + 300]
        existing = re.match(
            r"\s*if\s*\(\s*!\s*FreeMarginAllowsNotional\s*\(\s*"
            + re.escape(volume) + r"\s*,\s*" + re.escape(price)
            + r"\s*\)\s*\)\s*return\s*;", tail, re.I)
        if existing:
            continue
        indent_start = source.rfind("\n", 0, guard.start()) + 1
        indent = re.match(r"[ \t]*", source[indent_start:guard.start()]).group(0)
        statement = f"\n{indent}if(!FreeMarginAllowsNotional({volume}, {price})) return;"
        insertions.append((guard.end(), statement))
        repairs.append({"kind": "projected_margin_followup_added", "side": guard.group(1).upper(),
                        "volume": volume, "price": price,
                        "reason": "authenticated 2x free-margin/notional requirement"})
    for position, statement in reversed(insertions):
        source = source[:position] + statement + source[position:]
    masked = _mask_comments_and_strings(source)
    definition_pattern = re.compile(
        r"\bbool\s+FreeMarginAllowsNotional\s*\(\s*double\s+volume\s*,\s*double\s+price\s*\)", re.I)
    definitions = list(definition_pattern.finditer(masked))
    if not definitions:
        include = re.search(r"(?m)^\s*#\s*include\s*<minato_base_score_v2\.mqh>\s*$", source)
        if include is None:
            return source, repairs
        source = source[:include.end()] + "\n\n" + FREE_MARGIN_NOTIONAL_HELPER.rstrip() + source[include.end():]
        repairs.insert(0, {"kind": "free_margin_notional_helper_added",
                           "reason": "authenticated 2x free-margin/notional requirement"})
    else:
        source, correction = normalize_free_margin_notional_helper(source)
        if correction is not None:
            repairs.append(correction)
    return source, repairs


def normalize_free_margin_notional_helper(source: str) -> tuple[str, dict | None]:
    """Replace one model-authored helper with the cron's exact fixed implementation."""
    masked = _mask_comments_and_strings(source)
    named_definitions = list(re.finditer(r"\bbool\s+FreeMarginAllowsNotional\s*\(", masked))
    signature = re.compile(
        r"\bbool\s+FreeMarginAllowsNotional\s*\(\s*double\s+volume\s*,\s*double\s+price\s*\)\s*\{",
        re.I,
    )
    if len(named_definitions) != 1:
        return source, None
    match = signature.search(masked)
    if match is None:
        return source, None
    opening = match.end() - 1
    depth = 0
    end = None
    for index in range(opening, len(masked)):
        depth += (masked[index] == "{") - (masked[index] == "}")
        if depth == 0:
            end = index
            break
    if end is None:
        return source, None
    helper_end = end + 1
    canonical = FREE_MARGIN_NOTIONAL_HELPER.rstrip()
    if re.sub(r"\s+", "", source[match.start():helper_end]) == re.sub(r"\s+", "", canonical):
        return source, None
    source = source[:match.start()] + canonical + source[helper_end:]
    return source, {
        "kind": "model_authored_free_margin_notional_helper_replaced",
        "reason": "replace the model-authored helper with the exact fixed Stage 1 free-margin/notional implementation",
    }


def validate_source(source: str, spec: dict, shared_contract: dict | None = None):
    code = code_only(source)
    require(len(source) <= 160000 and "\x00" not in source, "invalid source size/encoding")
    # The approved hook replaces a required insertion token before this gate.
    # Describing that canonical hook in a line comment is not unfinished code.
    # Exempt only that word in that documentation; all other markers, including
    # unfinished strategy comments and executable identifiers, remain rejected.
    marker_text = source
    if ONTESTER.strip() in code:
        marker_text = re.sub(
            r"(?im)^[ \t]*//[ \t]*(?:File-scope[ \t]+)?placeholder[ \t]+"
            r"(?:where the canonical OnTester implementation is|for the canonical OnTester "
            r"(?:injection|insertion) point)[.]?[ \t]*$",
            lambda match: re.sub(r"\bplaceholder\b", "insertion point", match[0], flags=re.I),
            marker_text)
    require(not re.search(r"placeholder|scaffold|TODO|FIXME|@@|not implemented", marker_text, re.I), "placeholder source is prohibited")
    require("#include <minato_base_score_v2.mqh>" in code and "#include <minato_base_score.mqh>" not in code,
            "canonical score include is required")
    includes = re.findall(r"#\s*include\s*[<\"]([^>\"]+)[>\"]", code)
    require(set(includes) <= {"minato_base_score_v2.mqh", "kiyosaki_regime.mqh", "Trade/Trade.mqh", "Trade\\Trade.mqh"}
            and not re.search(r"#\s*import|\b(?:WebRequest|SendMail|SendNotification|ShellExecute)\s*\(", code),
            "source has unsupported external dependencies or side effects")
    require(not re.search(r"#\s*(?:define|undef).*MINATO|(?:double|bool|void|int)\s+Minato\w*\s*\(", code, re.I),
            "source may not override canonical Minato definitions")
    require(len(re.findall(r"\bOnTester\s*\(", code)) == 1 and ONTESTER.strip() in code,
            "source must use the injected canonical OnTester")
    require(re.search(r"void\s+OnTick\s*\([^)]*\)\s*\{\s*MinatoV2ObserveTick\(\);", code),
            "Minato tick capture must precede any OnTick early return")
    require(canonical_initialization_hooks_present(code),
            "OnInit must reset Minato capture, then return INIT_PARAMETERS_INCORRECT when MinatoV2ConfigurationValid() is false")
    shared_inputs = (shared_contract or {}).get("inputs", {})
    if shared_contract is not None:
        assert_no_shared_input_shadowing(source, shared_contract)
    input_usage = source_input_usage(source, spec, shared_contract or {})
    validate_strategy_input_defaults(source, spec, shared_contract or {})
    require(regime_parameters_used(source, code), "regime multipliers are not all used")
    identity = " ".join(str(value or "") for value in (
        spec.get("ea"), spec.get("candidate_ea"),
        (spec.get("tester_config") or {}).get("ea"),
    )).casefold()
    compact_identity = re.sub(r"[^a-z0-9]", "", identity)
    if "gjr" in compact_identity and "garch" in compact_identity:
        require(gjr_base_lot_sizing_contract(source),
                "GJR sizing must clamp InpBaseLot to SYMBOL_VOLUME_MIN before applying the regime lot multiplier in both order paths")
    require(closed_bar_check(source) or generic_closed_bar_audit(source), "closed-bar source gate failed")
    require(dcc_closed_return_arrays_are_bounded(source, required_dcc=spec_requires_dcc_garch(spec)),
            "DCC closed-return history arrays must preserve shift-1/shift-2 semantics and prove every return index is in bounds")
    require(gk_sample_update_is_closed_bar_gated(source), "GK rolling-window updates must occur only after the direct NewBarClosed early-return gate; never insert the same shift-1 candle on every tick")
    require(closed_bar_indicator_reads_are_safe(source), "indicator CopyBuffer reads must use closed bars; shift 0 is the forming candle")
    input_names = set((spec.get("tester_config") or {}).get("inputs", {}))
    atr_period_input = atr_period_input_name(spec)
    atr_period_expression = ("PERIOD_M5" if spec.get("stage2_requirements", {}).get(
        "must_use_ethusd_m5_closed_bars") else "_Period")
    require(atr_uses_authenticated_closed_buffer(source, atr_period_input, atr_period_expression),
            f"use iATR(_Symbol, {atr_period_expression}, {atr_period_input}) and CopyBuffer on buffer 0, shift 1, count 1; do not hand-roll ATR or read the forming bar")
    order_diagnosis = []
    order_safe = every_order_path_is_safe(code, order_diagnosis)
    if not order_safe and order_diagnosis and order_diagnosis[0].get("code") == "NORMALIZATION_NOT_RECOGNIZED":
        require(False, "NORMALIZATION_NOT_RECOGNIZED " + json.dumps(order_diagnosis[0], sort_keys=True))
    if not order_safe and order_diagnosis and order_diagnosis[0].get("code") == "ORDER_VOLUME_UNIDENTIFIABLE":
        require(False, "Minato order safety source gate failed: " + order_diagnosis[0].get(
            "reason", "submitted order volume identifier could not be parsed"))
    if not order_safe and order_diagnosis and order_diagnosis[0].get("code") == "ORDER_QUANTITY_UNIDENTIFIABLE":
        require(False, "Minato order safety source gate failed: " + order_diagnosis[0].get(
            "reason", "actual submitted order quantity could not be authenticated"))
    if not order_safe and order_diagnosis and order_diagnosis[0].get("code") == "ORDER_PRICE_GUARD_MISMATCH":
        require(False, "projected-margin and Buy/Sell must use the exact same price variable; pass ask or bid directly and do not create submittedPrice aliases")
    if not order_safe and order_diagnosis and order_diagnosis[0].get("code") == "ORDER_MARGIN_NOTIONAL_MISSING":
        require(False, "each order path must call if (!FreeMarginAllowsNotional(volume, ask_or_bid)) return; after MinatoAllowsProjectedMargin; implement the authenticated 2x free-margin/notional helper")
    if not order_safe and order_diagnosis and order_diagnosis[0].get("code") == "ORDER_MARGIN_NOTIONAL_HELPER_UNAUTHENTICATED":
        require(False, "FreeMarginAllowsNotional helper does not match the authenticated implementation: notional = volume * price; free_margin = AccountInfoDouble(ACCOUNT_MARGIN_FREE); require free_margin >= 2.0 * notional")
    if not order_safe and order_diagnosis and order_diagnosis[0].get("code") == "ORDER_TP_PRICE_UNIDENTIFIABLE":
        require(False, "every Buy/Sell path must submit the same calculated nonzero SL and MinatoTakeProfit variables; remove branches that submit 0.0 for either price and return instead when either price is invalid")
    if not order_safe and order_diagnosis and order_diagnosis[0].get("code") == "ORDER_TP_CALCULATION_UNAUTHENTICATED":
        require(False, "each order path must assign its submitted TP directly from MinatoTakeProfit(entryPrice, ATR * InpTP_ATR_Mult, RegimeTPMult(activeRegime), isBuy) after both margin refusals; do not replace the canonical helper with a handwritten TP formula")
    require(order_safe, "Minato order safety source gate failed: assign volume from MinatoRegimeQuantity; use direct `if (volume <= 0.0) return;` refusals before and after NormalizeLot; use direct `if (!MinatoAllowsProjectedMargin(...)) return;`; then calculate TP and submit that same volume/price. Empty refusal blocks and else-if refusal chains are not accepted.")
    require(live_margin_path_is_safe(code), "Minato live-margin safety source gate failed: check executable close-and-return flow before signal gates. "
            "The supported MarginBreached()/CloseAllPositions() wrapper path needs no extra margin input; see live_margin_path_is_safe.")
    owners = [owner for owner in (shared_contract or {}).get('sources', {})
              if Path(owner).name.casefold() == 'minato_base_score_v2.mqh']
    margin_handoff = ({'canonical_minato': owners[0], 'canonical_minato_sha256': sha(Path(owners[0])),
                       'canonical_minato_approval': str(ROOT / 'state/contracts/minato_base_score_approved_v2.json')}
                      if len(owners) == 1 else {})
    require(margin_kill_120(source, margin_handoff),
            "authenticated 120% margin threshold and executable early close-and-refuse path are required; CloseAllPositions must use a descending PositionsTotal()-1 to zero loop with i-- or --i and close only matching symbol/magic tickets")
    return input_usage


def decode_source_proposal(raw: str, label: str) -> dict | None:
    """Accept one complete MQL5 block; optional legacy metadata stays authenticated."""
    if not re.fullmatch(r"proposal(?:-retry-\d+)?", label):
        return None
    pattern = (
        r"(?:" + re.escape(SOURCE_PROTOCOL) + r"\r?\n)?"
        r"```mql5\r?\n((?:(?!```).)*?)\r?\n```"
        r"(?:\r?\n```json\r?\n((?:(?!```).)*?)\r?\n```)?"
    )
    match = re.fullmatch(pattern, raw, re.S)
    if match is None:
        if raw.startswith(SOURCE_PROTOCOL):
            response_fail("source response must contain one complete MQL5 block and optional filename metadata, with no surrounding text")
        return None
    metadata_text = match.group(2)
    metadata = {}
    if metadata_text is not None:
        try:
            metadata = json.loads(metadata_text)
        except json.JSONDecodeError as exc:
            context = metadata_text[max(0, exc.pos - 90):exc.pos + 90]
            response_fail(f"{label} source filename metadata is malformed: {exc.msg}; "
                          f"line={exc.lineno}, column={exc.colno}, offset={exc.pos}, length={len(metadata_text)}; "
                          f"error_context={json.dumps(context)}")
        if not isinstance(metadata, dict) or set(metadata) != {"filename"}:
            response_fail("optional source metadata must contain exactly filename")
    return {**metadata, "source_text": match.group(1)}


def proposal_diagnosis(exc: ValueError, proposal: dict | None, previous: list[dict]) -> dict:
    """Describe a returned-proposal defect; never authorize edits to contracts."""
    detail = str(exc)
    normalization = None
    if detail.startswith("NORMALIZATION_NOT_RECOGNIZED "):
        try:
            normalization = json.loads(detail.removeprefix("NORMALIZATION_NOT_RECOGNIZED "))
        except (TypeError, json.JSONDecodeError):
            normalization = None
    rules = (
        ("STRATEGY_INPUT_LOCK", "STRATEGY_INPUT_LOCK", "Restore the exact approved input declaration type and default. Repair only the implementation; do not change regime values, custom maximums, scoring, or ranges."),
        ("declaration-only input", "INPUT_USAGE", "Use the existing approved input in its required calculation; preserve its name, type, default, regime values, and custom maximum."),
        ("FreeMarginAllowsNotional helper is invalid", "ORDER_MARGIN_NOTIONAL_HELPER", "Keep a direct FreeMarginAllowsNotional(volume, price) refusal after MinatoAllowsProjectedMargin. Its helper must compute notional = volume * price, read free_margin from ACCOUNT_MARGIN_FREE, and require free_margin >= 2.0 * notional. Do not reverse this to 2.0 * free_margin >= notional."),
        ("Minato order safety", "ORDER_REFUSAL_FLOW", "Correct matching quantity, normalization, zero-refusal, margin and TP flow; follow each executable predicate."),
        ("closed-bar source", "CLOSED_BAR_FLOW", "Use the authenticated closed-bar policy and correct signal shifts."),
        ("live-margin safety", "LIVE_MARGIN_FLOW", "Correct early live-margin close-and-refuse flow using the approved include."),
        ("schemas cannot be combined", "RESPONSE_SCHEMA", "Return one fenced MQL5 block or one JSON refusal, never both."),
        ("OnTester injection point", "CANONICAL_HOOK", "Keep exactly one file-scope canonical hook token; include no extra markers in comments."),
        ("generator refused", "IMPLEMENTATION_REFUSAL", "Implement the unchanged contract or identify the precise unsupported requirement."),
    )
    if normalization is not None:
        kind = "NORMALIZATION_NOT_RECOGNIZED"
        misplaced_call = ""
        if normalization.get("normalizer_line_start") is not None:
            misplaced_call = (f" Misplaced NormalizeLot call is on lines "
                              f"{normalization.get('normalizer_line_start')}-"
                              f"{normalization.get('normalizer_line_end')}.")
        action = (f"For {normalization.get('side')} order volume `{normalization.get('volume_variable')}`, define the authenticated "
                  "global double NormalizeLot(double requestedVolume) helper using SYMBOL_VOLUME_MIN/MAX/STEP, invalid-input and "
                  "invalid-bound/step refusals, floor-to-step rounding, below-minimum refusal and step-derived NormalizeDouble precision. "
                  f"Then assign `{normalization.get('volume_variable')} = NormalizeLot({normalization.get('volume_variable')});` after its "
                  "MinatoRegimeQuantity assignment and first zero refusal, check zero again, and place projected margin last. "
                  f"Relevant source lines {normalization.get('source_line_start')}-{normalization.get('source_line_end')}; "
                  f"snippet SHA256 {normalization.get('quantity_to_margin_sha256')}." + misplaced_call)
    elif isinstance(exc, ReturnedProposalFormatError):
        kind, action = "RESPONSE_FORMAT", "Return exactly one fenced mql5 code block with the complete source and no metadata."
    else:
        kind, action = next(((code, repair) for token, code, repair in rules if token in detail),
                            ("SOURCE_ADMISSION", "Correct the reported source-admission defect against the unchanged contract."))
    stable_detail = re.sub(r'\bproposal(?:-retry-\d+)?\b', 'proposal', detail)
    if normalization is not None:
        fingerprint = digest((kind + "\n" + str(normalization.get("quantity_to_margin_sha256"))).encode("utf-8"))
    else:
        fingerprint = digest((kind + "\n" + stable_detail).encode("utf-8"))
    consecutive_matches = 0
    for earlier in reversed(previous):
        if earlier.get("failure_fingerprint") != fingerprint:
            break
        if normalization is not None:
            earlier_normalization = earlier.get("normalization_diagnosis")
            if (not isinstance(earlier_normalization, dict)
                    or earlier_normalization.get("quantity_to_margin_sha256")
                        != normalization.get("quantity_to_margin_sha256")):
                break
        consecutive_matches += 1
    result = {"failure_kind": kind, "failed_check": detail, "repair_action": action,
            "repair_scope": "source_proposal_only", "failure_fingerprint": fingerprint,
            "same_failure_count": 1 + consecutive_matches,
            "proposed_source_sha256": digest(proposal['source_text'].encode('utf-8'))
            if isinstance(proposal, dict) and isinstance(proposal.get('source_text'), str) else None}
    if normalization is not None:
        result["normalization_diagnosis"] = normalization
    return result


def reusable_lessons_for_rejected_source(diagnosis: dict, source_text: str,
                                         source_path: Path, ledger_path: Path | None = None) -> list[dict]:
    """Load only verified lessons matching a rejected proposal and current contract."""
    fingerprint = str(diagnosis.get("failure_fingerprint") or "").upper()
    if not re.fullmatch(r"[A-F0-9]{64}", fingerprint):
        return []
    ledger = ledger_path or ROOT / "state" / "ledgers" / "stage2_repair_episodes.sqlite3"
    contract_path = ROOT / "state" / "contracts" / "kcos_stage2_protected_behavior_v1.json"
    contract = load(contract_path)
    from kcos_stage2_adapters_v1 import family
    return stage2_learning.reusable_lessons(
        ledger, defect_fingerprint=fingerprint,
        ea_family=family(source_path, source_text),
        contract_version=str(contract.get("contract_id") or ""),
        source_characteristics=stage2_learning.source_characteristics(source_text))


def model_json(prompt: str, attempt: Path, label: str, runner=None) -> dict:
    prompt_path = attempt / (label + ".prompt.txt")
    immutable(prompt_path, prompt)
    require(HERMES_PYTHON.is_file(), "installed Hermes Python launcher is unavailable")
    try:
        result = (runner or subprocess.run)([str(HERMES_PYTHON), "-c", CLI_BOOTSTRAP, str(HERMES_ROOT), str(prompt_path)],
                cwd=attempt, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=TIMEOUT_SECONDS)
        immutable(attempt / (label + ".stdout.txt"), result.stdout or "")
        immutable(attempt / (label + ".stderr.txt"), result.stderr or "")
        require(result.returncode == 0, f"{label} Hermes CLI failed with exit {result.returncode}")
        raw = (result.stdout or "").strip()
        require(len(raw) <= 300000, "model response exceeds bounded source payload")
        envelope = decode_source_proposal(raw, label)
        if envelope is not None:
            return envelope
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as strict_error:
            if raw.lstrip().startswith(('{', '[')):
                response_fail(label + " returned malformed JSON: " + str(strict_error))
            # Some CLI providers surround their single JSON answer with prose.
            # Recover one explicitly fenced object; never choose among objects
            # or scrape arbitrary braces from source text or explanations.
            blocks = re.findall(r"```([^\r\n`]*)\r?\n([\s\S]*?)```", raw)
            if not (len(blocks) == 1 and blocks[0][0].strip().casefold() in {"", "json"}):
                response_fail("model response must contain one unambiguous JSON object")
            value = json.loads(blocks[0][1].strip())
        if not isinstance(value, dict):
            response_fail("model response is not a JSON object")
        return value
    except subprocess.TimeoutExpired as exc:
        for suffix, value in (("stdout", exc.stdout), ("stderr", exc.stderr)):
            text = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else (value or "")
            immutable(attempt / f"{label}.timeout-{suffix}.txt", text)
        fail(label + " model call timed out")
    except json.JSONDecodeError as exc:
        response_fail(label + " returned malformed JSON: " + str(exc))
    except OSError as exc:
        fail(label + " response/launcher failure: " + type(exc).__name__)


def review_issue_is_explicitly_nonblocking(issue: object) -> bool:
    """Drop only reviewer entries that explicitly conclude there is no violation."""
    if not isinstance(issue, str):
        return False
    tail = issue.casefold().strip()[-420:]
    conclusions = (
        r"no\s+(?:double[- ]multiplier\s+)?violation\s+(?:is|was)\s+present\b[^.]*[.!]?\s*$",
        r"no\s+issue(?:\s+(?:here|raised|found))?\b[^.]*[.!]?\s*$",
        r"not\s+(?:a\s+)?(?:blocking\s+)?issue\b[^.]*[.!]?\s*$",
        r"not\s+a\s+violation\b[^.]*[.!]?\s*$",
        r"retract(?:ed)?\b[^.]*[.!]?\s*$",
    )
    return any(re.search(pattern, tail, re.I) for pattern in conclusions)


def prior_repair_feedback(history_root: Path, bound: dict) -> dict | None:
    """Return recent prior holds only when their source lineage is identical."""
    if not history_root.is_dir():
        return None
    keys = ("cycle_id", "ea", "source_spec_sha256", "stage1_event_sha256", "canonical_minato_sha256")
    records = []
    for directory in sorted((item for item in history_root.iterdir() if item.is_dir()),
                            key=lambda item: item.name, reverse=True):
        hold_path = directory / "hold.json"
        if not hold_path.is_file():
            continue
        try:
            hold = json.loads(hold_path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            continue
        if (hold.get("status") != "MODEL_SOURCE_HOLD"
                or hold.get("phase") not in {"proposal", "deterministic_source_admission"}
                or any(hold.get(key) != bound.get(key) for key in keys)):
            continue
        reason = hold.get("reason")
        if isinstance(reason, str) and reason.strip():
            records.append({"attempt": directory.name, "hold_sha256": sha(hold_path),
                            "reason": reason[:10000], "truncated": len(reason) > 10000})
            if len(records) == 2:
                break
    return {"prior_holds": records} if records else None


def split_dcc_history_array_declaration(source: str, spec: dict) -> tuple[str, list[dict]]:
    """Split one safe combined history-array declaration into verifier-recognized declarations."""
    if not is_dcc_garch_spec(spec):
        return source, []
    pattern = re.compile(
        r"(?m)^(?P<indent>[ \t]*)double[ \t]+h_eth_hist[ \t]*\[[ \t]*\][ \t]*,[ \t]*"
        r"h_btc_hist[ \t]*\[[ \t]*\][ \t]*;[ \t]*$"
    )
    matches = list(pattern.finditer(source))
    if len(matches) != 1:
        return source, []
    match = matches[0]
    newline = "\r\n" if "\r\n" in source else "\n"
    replacement = (match.group("indent") + "double h_eth_hist[];" + newline
                   + match.group("indent") + "double h_btc_hist[];")
    repaired = source[:match.start()] + replacement + source[match.end():]
    return repaired, [{"kind": "dcc_history_array_declarations_split",
                       "arrays": ["h_eth_hist", "h_btc_hist"],
                       "reason": "separate declarations preserve the unchanged closed-return bounds audit"}]


def reorder_exact_dcc_reference_after_oninit(source: str, spec: dict) -> tuple[str, list[dict]]:
    """Move only the exact bounded DCC reference body after OnInit when needed."""
    if not is_dcc_garch_spec(spec):
        return source, []
    masked = _mask_comments_and_strings(source)
    function_pattern = (r"\bbool\s+ComputeDCCSignals\s*\(\s*double\s*&\s*z_corr\s*,"
                        r"\s*int\s*&\s*regime_out\s*\)\s*\{")
    oninit_pattern = r"\bint\s+OnInit\s*\(\s*\)\s*\{"
    functions = list(re.finditer(function_pattern, masked, re.I))
    oninits = list(re.finditer(oninit_pattern, masked, re.I))
    if len(functions) != 1 or len(oninits) != 1:
        return source, []
    function, oninit = functions[0], oninits[0]
    if oninit.start() < function.start():
        return source, []

    def block_end(masked_text: str, match: re.Match[str]) -> int | None:
        opening = match.end() - 1
        depth = 0
        for index in range(opening, len(masked_text)):
            if masked_text[index] == "{":
                depth += 1
            elif masked_text[index] == "}":
                depth -= 1
                if depth == 0:
                    return index + 1
        return None

    function_end = block_end(masked, function)
    oninit_end = block_end(masked, oninit)
    if function_end is None or oninit_end is None or function_end > oninit.start():
        return source, []

    reference = _mask_comments_and_strings(DCC_BOUNDED_IMPLEMENTATION_EXAMPLE)
    reference_match = re.search(function_pattern, reference, re.I)
    reference_end = block_end(reference, reference_match) if reference_match else None
    if reference_match is None or reference_end is None:
        return source, []
    compact = lambda value: re.sub(r"\s+", "", value).casefold()
    if compact(masked[function.end():function_end - 1]) != compact(reference[reference_match.end():reference_end - 1]):
        return source, []

    prototype_pattern = (r"\bbool\s+ComputeDCCSignals\s*\(\s*double\s*&\s*z_corr\s*,"
                         r"\s*int\s*&\s*regime_out\s*\)\s*;")
    if re.search(prototype_pattern, masked, re.I):
        return source, []

    function_start = function.start()
    # Carry an immediately preceding DCC comment with the function definition.
    line_start = source.rfind("\n", 0, function_start) + 1
    previous_end = max(0, line_start - 1)
    previous_start = source.rfind("\n", 0, previous_end) + 1
    if source[previous_start:previous_end].strip().startswith("//"):
        function_start = previous_start
    moved_function = source[function_start:function_end]
    without_function = source[:function_start] + source[function_end:]
    masked_without = _mask_comments_and_strings(without_function)
    oninit = re.search(oninit_pattern, masked_without, re.I)
    if oninit is None:
        return source, []
    oninit_end = block_end(masked_without, oninit)
    if oninit_end is None:
        return source, []

    # Place the forward declaration before any OnInit banner comment.
    insert_at = oninit.start()
    oninit_line = without_function.rfind("\n", 0, oninit.start()) + 1
    prior_end = max(0, oninit_line - 1)
    prior_start = without_function.rfind("\n", 0, prior_end) + 1
    if without_function[prior_start:prior_end].strip().startswith("//"):
        insert_at = prior_start
    prototype = "bool ComputeDCCSignals(double &z_corr, int &regime_out);\n\n"
    without_function = without_function[:insert_at] + prototype + without_function[insert_at:]
    masked_without = _mask_comments_and_strings(without_function)
    oninit = re.search(oninit_pattern, masked_without, re.I)
    if oninit is None:
        return source, []
    oninit_end = block_end(masked_without, oninit)
    if oninit_end is None:
        return source, []
    newline = "\r\n" if "\r\n" in source else "\n"
    moved_function = moved_function.lstrip("\r\n")
    repaired = without_function[:oninit_end] + newline + newline + moved_function + without_function[oninit_end:]
    return repaired, [{"kind": "exact_dcc_reference_moved_after_oninit",
                       "reason": "only the canonical bounded DCC body matched; added its forward declaration and preserved the body unchanged"}]


def cycle_magic_number(spec: dict) -> int:
    """Return the established YYMMDD001 operational identity for a cycle."""
    cycle_id = str(spec.get("cycle_id") or (spec.get("tester_config") or {}).get("cycle_id") or "")
    match = re.search(r"(?:^|-)kcos-(\d{4})(\d{2})(\d{2})T", cycle_id, re.I)
    if match is None:
        match = re.search(r"kcos-(\d{4})(\d{2})(\d{2})T", cycle_id, re.I)
    require(match is not None, "cannot derive the reserved InpMagicNumber identity from the authenticated cycle date")
    year, month, day = match.groups()
    return int(year[-2:] + month + day + "001")


def ensure_magic_number_identity_control(source: str, spec: dict) -> tuple[str, list[dict]]:
    """Bind only position ownership fields to a deterministic, non-optimized input."""
    masked = _mask_comments_and_strings(source)
    input_pattern = re.compile(r"(?im)^[ \t]*input[ \t]+(?:int|long|ulong)[ \t]+InpMagicNumber\b[^;\r\n]*;")
    declarations = list(input_pattern.finditer(masked))
    all_decl_pattern = re.compile(
        r"\b(?:(?:input|const|static)[ \t]+)?(?:int|long|ulong)[ \t]+InpMagicNumber\b|^\s*#\s*define\s+InpMagicNumber\b",
        re.I | re.M)
    all_declarations = list(all_decl_pattern.finditer(masked))
    require(len(declarations) <= 1 and len(all_declarations) == len(declarations),
            "InpMagicNumber has an ambiguous or unsupported declaration; preserve source for review")
    require(not declarations or "=" in declarations[0].group(0),
            "InpMagicNumber input is missing its stable default; preserve source for review")

    repairs: list[dict] = []
    if not declarations:
        cycle_magic = cycle_magic_number(spec)
        top_level_inputs = []
        input_line_pattern = re.compile(r"(?m)^[ \t]*input[ \t]+[^;\r\n]+;")
        for match in input_line_pattern.finditer(masked):
            prefix = masked[:match.start()]
            depth = prefix.count("{") - prefix.count("}")
            if depth == 0:
                top_level_inputs.append(match)
        require(bool(top_level_inputs), "cannot place reserved InpMagicNumber input safely: no top-level input declaration")
        insert_at = top_level_inputs[-1].end()
        newline = "\r\n" if "\r\n" in source else "\n"
        source = (source[:insert_at] + newline
                  + f"input ulong InpMagicNumber = {cycle_magic}; // KCOS execution identity; not optimized"
                  + source[insert_at:])
        repairs.append({"kind": "reserved_magic_number_input_added", "default": cycle_magic,
                        "reason": "the authenticated cycle date supplies the non-optimized operational position identity; it is not a strategy input"})
        masked = _mask_comments_and_strings(source)

    edits: list[tuple[int, int, str]] = []
    set_magic_pattern = re.compile(
        r"(?P<receiver>\b[A-Za-z_]\w*)[ \t]*\.[ \t]*SetExpertMagicNumber[ \t]*\([ \t]*(?:\([ \t]*ulong[ \t]*\)[ \t]*)?0(?:\.0+)?[ \t]*\)", re.I)
    for match in set_magic_pattern.finditer(masked):
        edits.append((match.start(), match.end(),
                      f"{match.group('receiver')}.SetExpertMagicNumber((ulong)InpMagicNumber)"))
    position_magic_pattern = re.compile(
        r"PositionGetInteger[ \t]*\([ \t]*POSITION_MAGIC[ \t]*\)[ \t]*(==|!=)[ \t]*(?:\([ \t]*long[ \t]*\)[ \t]*)?0(?:\.0+)?", re.I)
    for match in position_magic_pattern.finditer(masked):
        edits.append((match.start(), match.end(),
                      f"PositionGetInteger(POSITION_MAGIC) {match.group(1)} (long)InpMagicNumber"))
    changed = len(edits)
    for start, end, replacement in sorted(edits, reverse=True):
        source = source[:start] + replacement + source[end:]
    if changed:
        repairs.append({"kind": "literal_position_magic_bound_to_input", "references": changed,
                        "reason": "use the same reserved identity input for CTrade tagging and symbol/magic ownership checks"})
    return source, repairs


def prepare_source_for_admission(source: str, spec: dict, shared_contract: dict) -> tuple[str, list[dict], dict]:
    """Apply only the existing deterministic repairs, then run full admission."""
    gjr_base_lot_repairs = []
    identity = " ".join(str(value or "") for value in (
        spec.get("ea"), spec.get("candidate_ea"),
        (spec.get("tester_config") or {}).get("ea"),
    )).casefold()
    compact_identity = re.sub(r"[^a-z0-9]", "", identity)
    if "gjr" in compact_identity and "garch" in compact_identity:
        from kcos_stage2_repair_v2 import repair_gjr_base_lot
        source, gjr_base_lot_repairs = repair_gjr_base_lot(source)
    source, magic_identity_repairs = ensure_magic_number_identity_control(source, spec)
    source = normalize_canonical_ontester_hook(source)
    source, dcc_order_repairs = reorder_exact_dcc_reference_after_oninit(source, spec)
    source, generated_runtime_repairs = repair_generated_mql5_runtime_patterns(source, spec)
    require(source.count(ONTESTER_TOKEN) == 1,
            "canonical OnTester hook is invalid: provide one marker or the exact canonical scorer body; "
            "the cron replaces the marker before source validation.")
    source = source.replace(ONTESTER_TOKEN, ONTESTER)
    source, dcc_declaration_repairs = split_dcc_history_array_declaration(source, spec)
    source, regime_repairs = repair_local_regime_macro_namespace(source, shared_contract)
    source, unused_include_repairs = remove_unused_position_info_include(source)
    source, trade_include_repairs = ensure_trade_class_include(source)
    source, domain_repairs = ensure_approved_optimization_domain_comments(source, spec, shared_contract)
    source, margin_repairs = ensure_free_margin_notional_guards(source)
    repairs = (gjr_base_lot_repairs + magic_identity_repairs + dcc_order_repairs + generated_runtime_repairs + dcc_declaration_repairs + regime_repairs + unused_include_repairs
               + trade_include_repairs + domain_repairs + margin_repairs)
    usage = validate_source(source, spec, shared_contract)
    return source, repairs, usage


def repair_generated_mql5_runtime_patterns(source: str, spec: dict | None = None) -> tuple[str, list[dict]]:
    """Repair exact, semantics-preserving generated MQL5 portability patterns."""
    repairs: list[dict] = []
    from kcos_tester_config_v2 import is_timing_control_input
    declaration = re.compile(
        r"(?im)^[ \t]*input[ \t]+[A-Za-z_]\w*[ \t]+([A-Za-z_]\w*)[ \t]*=[^;]*;[^\r\n]*(?:\r?\n|$)"
    )
    cooldown_declarations = []
    for match in reversed(list(declaration.finditer(source))):
        line = match.group(0)
        comment = line.split("//", 1)[1] if "//" in line else ""
        name = match.group(1)
        if is_timing_control_input(name, {"description": comment}) == "cooldown":
            cooldown_declarations.append(name)
            source = source[:match.start()] + source[match.end():]
    if cooldown_declarations:
        repairs.append({"kind": "stage2_cooldown_input_declaration_removed",
                        "inputs": sorted(set(cooldown_declarations)),
                        "reason": "cooldown inputs are excluded from the Stage 2 execution contract"})
    masked = _mask_comments_and_strings(source)
    # Canonicalize only equivalent current-period aliases to the period fixed
    # by the authenticated execution contract (M5 for the locked ETHUSD path).
    stage2_requirements = (spec or {}).get("stage2_requirements") or {}
    required_atr_period = ("PERIOD_M5" if stage2_requirements.get(
        "must_use_ethusd_m5_closed_bars") else "_Period")
    atr_period_alias = re.compile(
        r"\biATR\s*\(\s*_Symbol\s*,\s*(PERIOD_CURRENT|_Period)\s*,", re.I
    )
    for match in reversed(list(atr_period_alias.finditer(masked))):
        start, end = match.span(1)
        if match.group(1) == required_atr_period:
            continue
        source = source[:start] + required_atr_period + source[end:]
        repairs.append({"kind": "current_period_atr_alias_normalized",
                        "reason": f"canonicalize the equivalent current-period iATR argument to the authenticated {required_atr_period} period required by source admission"})
    masked = _mask_comments_and_strings(source)
    const_ints = {}
    for match in re.finditer(r"(?im)^\s*const\s+int\s+([A-Za-z_]\w*)\s*=\s*(\d+)\s*;", masked):
        const_ints[match.group(1)] = (int(match.group(2)), match.start(), match.end())
    replacements = []
    for name, (bound, _, _) in const_ints.items():
        for array in re.finditer(rf"\[\s*{re.escape(name)}\s*\]", masked):
            replacements.append((array.start(), array.end(), f"[{bound}]"))
            repairs.append({"kind": "mql5_const_array_bound_literalized",
                            "constant": name, "bound": bound,
                            "reason": "MQL5 requires a literal compile-time fixed-array dimension"})
    for start, end, replacement in reversed(replacements):
        source = source[:start] + replacement + source[end:]

    # Generated MQL5 sometimes uses the constant expression 0.0/0.0 as a
    # NaN sentinel. MetaEditor diagnoses that exact constant division as a
    # compile error. MathArcsin(2.0) yields a runtime invalid real value in
    # MQL5, preserving the sentinel behavior checked by MathIsValidNumber.
    masked = _mask_comments_and_strings(source)
    nan_sentinel = re.compile(r"(?<![\w.])0(?:\.0+)?\s*/\s*0(?:\.0+)?(?![\w.])")
    nan_matches = list(nan_sentinel.finditer(masked))
    for match in reversed(nan_matches):
        source = source[:match.start()] + "MathArcsin(2.0)" + source[match.end():]
    if nan_matches:
        repairs.append({"kind": "mql5_nan_sentinel_replaced",
                        "count": len(nan_matches),
                        "source_lines": [source.count("\n", 0, match.start()) + 1
                                         for match in nan_matches],
                        "reason": "replace compiler-rejected 0/0 NaN sentinels with the MQL5 invalid-result sentinel MathArcsin(2.0)"})

    # The generic source scaffold touched fixed-zero timing inputs in an
    # initializer/combined condition, which the strict verifier correctly
    # refused. Keep their required input references behind explicit positive
    # guards; KCOS fixes both inputs at zero, so these branches cannot change
    # entry, exit, or management behavior.
    fixed_zero_controls = re.search(
        r"(?im)^\s*input\s+int\s+InpTimeStopBars\s*=\s*0\s*;", source
    )
    if fixed_zero_controls:
        touch_block = re.compile(
            r"(?ms)^\s*// Touches KCOS-fixed timing inputs.*?^\s*\}\s*\n")
        if touch_block.search(source):
            replacement = (
                '   PrintFormat("kcos_time_stop_fixed_input=%d", InpTimeStopBars);\n'
            )
            source = touch_block.sub(replacement, source, count=1)
            repairs.append({"kind": "fixed_time_stop_evidence_logging",
                            "inputs": ["InpTimeStopBars"],
                            "reason": "retain fixed time-stop evidence without introducing a cooldown input or reference"})
        unsafe_noop_guard = re.compile(
            r"(?m)^\s*// Zero KCOS-fixed time_stop is never used to delay or close a position\.\s*\n"
            r"\s*if\s*\(\s*InpTimeStopBars\s*>\s*0\s*\)\s*return\s*;\s*\n")
        if unsafe_noop_guard.search(source):
            source = unsafe_noop_guard.sub("", source, count=1)
            repairs.append({"kind": "remove_fixed_time_stop_management_bypass",
                            "input": "InpTimeStopBars",
                            "reason": "the guard disabled all open-position management instead of implementing an expiry stop"})
    return source, repairs


def replay_prior_admissible_proposal(history_root: Path, bound: dict, ea_name: str,
                                     spec: dict, shared_contract: dict) -> dict | None:
    """Revalidate exact same-lineage model output after deterministic gates improve.

    The proposal and prior hold must both be hash-bound to this cycle/spec/include.
    Replay skips only a redundant model call; it cannot waive any source check.
    """
    lineage_keys = ("cycle_id", "ea", "source_spec_sha256", "stage1_event_sha256",
                    "canonical_minato_sha256", "effective_spec_sha256",
                    "input_resolution_sha256", "strategy_input_lock_sha256",
                    "shared_input_contract_sha256")
    for attempt in sorted((path for path in history_root.iterdir() if path.is_dir()),
                          key=lambda path: path.name, reverse=True) if history_root.is_dir() else []:
        hold_path = attempt / "hold.json"
        if not hold_path.is_file():
            continue
        try:
            hold = json.loads(hold_path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            continue
        if (hold.get("status") != "MODEL_SOURCE_HOLD"
                or hold.get("phase") not in {"proposal", "deterministic_source_admission"}
                or any(hold.get(key) != bound.get(key) for key in lineage_keys)):
            continue
        for rejection_path in sorted(attempt.glob("proposal-rejection-*.json"),
                                     key=lambda path: path.name, reverse=True):
            try:
                rejection = json.loads(rejection_path.read_text(encoding="utf-8-sig"))
                raw_path = Path(rejection["raw_proposal"]).resolve()
                raw_path.relative_to(attempt.resolve())
                label = raw_path.name.removesuffix(".stdout.txt")
                raw = raw_path.read_text(encoding="utf-8-sig")
                envelope = decode_source_proposal(raw.strip(), label)
                if isinstance(envelope, dict) and set(envelope) == {"source_text"}:
                    envelope = {"filename": ea_name + ".mq5", **envelope}
                diagnosis = rejection.get("diagnosis", {})
                if (not isinstance(envelope, dict) or envelope.get("filename") != ea_name + ".mq5"
                        or not isinstance(envelope.get("source_text"), str)
                        or digest(envelope["source_text"].encode("utf-8"))
                            != str(diagnosis.get("proposed_source_sha256") or "").upper()):
                    continue
                source, repairs, usage = prepare_source_for_admission(
                    envelope["source_text"], spec, shared_contract)
                return {"source_text": source, "source_repairs": repairs, "source_usage": usage,
                        "raw_proposal": str(raw_path), "raw_proposal_sha256": sha(raw_path),
                        "proposal_source_sha256": diagnosis["proposed_source_sha256"],
                        "prior_attempt": attempt.name, "prior_hold_sha256": sha(hold_path),
                        "prior_rejection_sha256": sha(rejection_path)}
            except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
                continue
    return None



def build_research_source(spec_path: Path, state_path: Path, cycle_id: str, ea_name: str,
                          candidates_dir: Path, *, runner=None) -> dict:
    state, spec, contract, event_path, minato = authenticate(spec_path, state_path, cycle_id, ea_name, candidates_dir)
    shared_sources = {str(minato.resolve()): minato.read_bytes().decode("utf-8-sig")}
    shared_contract = discover_inputs(shared_sources)
    spec, input_resolution = resolve_input_spec(spec, shared_sources=shared_sources)
    spec = stage2_spec_from_spec(spec)
    input_resolution["kcos_runtime_policy"] = {
        "timing_controls": spec.get("stage2_runtime_policy", {}).get("timing_controls", {})
    }
    require(not (candidates_dir / (ea_name + ".mq5")).exists(), "refusing to overwrite an existing candidate")
    bound = {"cycle_id": cycle_id, "ea": ea_name, "source_spec": str(spec_path.resolve()),
             "source_spec_sha256": sha(spec_path), "stage1_event_sha256": stage1_event_lineage_sha256(event_path),
             "canonical_minato_sha256": sha(minato)}
    safe_cycle = re.sub(r"[^A-Za-z0-9_.-]", "_", cycle_id)
    history_root = candidates_dir / ".generation" / safe_cycle
    prior_feedback = prior_repair_feedback(history_root, bound)
    strategy_lock = strategy_input_lock_manifest(spec)
    attempt = history_root / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12])
    attempt.mkdir(parents=True, exist_ok=False)
    write_json(attempt / "strategy-input-lock.json", strategy_lock)
    write_json(attempt / "effective-spec.json", spec)
    write_json(attempt / "input-resolution.json", input_resolution)
    bound.update(effective_spec=str(attempt / "effective-spec.json"),
                 effective_spec_sha256=sha(attempt / "effective-spec.json"),
                 input_resolution=str(attempt / "input-resolution.json"),
                 input_resolution_sha256=sha(attempt / "input-resolution.json"),
                 strategy_input_lock=str(attempt / "strategy-input-lock.json"),
                 strategy_input_lock_sha256=sha(attempt / "strategy-input-lock.json"),
                 shared_input_contract_sha256=shared_contract["sha256"])
    write_json(attempt / "lineage.json", bound)
    replay = replay_prior_admissible_proposal(history_root, bound, ea_name, spec, shared_contract)
    if replay is not None:
        source = replay["source_text"]
        if replay["source_repairs"]:
            write_json(attempt / "source-repairs-replay.json", replay["source_repairs"])
        replay_record = {key: value for key, value in replay.items() if key != "source_text"}
        write_json(attempt / "proposal-replay.json", {
            "status": "SAME_LINEAGE_PROPOSAL_REVALIDATED",
            "reason": "prior deterministic hold was caused by a corrected source-admission rule",
            **replay_record,
        })
        source_hash = digest(source.encode("utf-8"))
        receipt = {"schema_version": 1, **bound, "source_sha256": source_hash, "passed": True,
                   "status": "DETERMINISTIC_SOURCE_ADMISSION",
                   "implementation_mode": "model_generated_source_admitted",
                   "proposal_attempts": 0,
                   "strategy_input_lock": bound["strategy_input_lock"],
                   "strategy_input_lock_sha256": bound["strategy_input_lock_sha256"],
                   "raw_proposal": replay["raw_proposal"],
                   "raw_proposal_sha256": replay["raw_proposal_sha256"],
                   "replayed_prior_proposal": str(attempt / "proposal-replay.json"),
                   "replayed_prior_proposal_sha256": sha(attempt / "proposal-replay.json"),
                   "deterministic_source_repairs": replay["source_repairs"],
                   "implementation_evidence": {"input_usage": replay["source_usage"],
                                                "source_admission_passed": True,
                                                "independent_model_review": "not_required"},
                   "limitations": "Same-lineage proposal replayed through current deterministic source admission; compilation and full Stage 2 verification still required."}
        receipt_path = attempt / "source-admission-receipt.json"
        write_json(receipt_path, receipt)
        return {"source_text": source,
                "source_admission_receipt": {**receipt, "receipt_path": str(receipt_path),
                                              "receipt_sha256": sha(receipt_path)}}
    dependencies = [{"owner": owner, "sha256": sha(Path(owner)),
                     "include_directive": f"#include <{Path(owner).name}>",
                     "verified_by": "cron authenticated approved bytes before model invocation"}
                    for owner in shared_sources]
    context = json.dumps({"identity": bound, "spec": spec, "input_resolution": input_resolution,
                          "verified_shared_dependencies": dependencies,
                          "canonical_on_tester_injection": {
                              "marker": ONTESTER_TOKEN, "replacement": ONTESTER,
                              "owner": "cron", "applied": "before source validation, independent review and compilation"},
                          "source_admission_checks": {
                              check.__name__: inspect.getsource(check) for check in
                              (closed_bar_check, generic_closed_bar_audit,
                               gk_sample_update_is_closed_bar_gated,
                               closed_bar_indicator_reads_are_safe,
                               atr_uses_authenticated_closed_buffer,
                               every_order_path_is_safe, normalize_lot_helper_is_authenticated,
                                canonical_initialization_hooks_present,
                               live_margin_path_is_safe, regime_parameters_used, margin_kill_120,
                               _canonical_margin_wrapper_is_concrete, _function_body, _simple_refusal_end,
                               _normalized_expression)}})
    instructions = """Implement this exact reproducible execution contract as one complete MQL5 EA.
No tools or filesystem changes. Return exactly one fenced mql5 code block containing the complete literal EA source.
Do not include a filename, JSON metadata, protocol label, explanation, or surrounding prose; the cron binds the authenticated EA filename.
The executable source is the evidence; cron runs deterministic source checks and an independent source review.
If an actual requirement is unsupported, return exactly {\"unsupported\":\"specific reason\"} and no source block. Never use placeholder signals or constant neutral regime.
Stage 1 supplies the authenticated strategy contract and tester_config, not a source template. Building the full EA from that contract is this task; no prebuilt template is required. Declare every EA-owned tester input with its MQL type and approved default from the effective spec. Omit only inputs explicitly owned by the authenticated include. The tester INI applies optimization bounds; do not invent code-side property assignments.
Use every declared EA-owned strategy input in executable logic; preserve four mutually exclusive regimes and
all twelve lot/SL/TP mappings. The exact fixed fields InpFromDate and InpToDate are tester-window metadata only
when their defaults/start/step/stop match tester_config.from_date/to_date, optimize is N, and test_plan carries
the same dates. Declare those two fields as strings with the authenticated date values, but do not use them to
gate trading or calculations; Stage 3/4.5 tester windows and Stage 5 forward windows are supplied separately.
All other EA-owned inputs still require executable strategy use. Include <minato_base_score_v2.mqh>; never define or replace any Minato formula.
For CTrade settings, use the installed MQL5 API: `SetDeviationInPoints(ulong)` is the deviation method and `SetTypeFillingBySymbol(_Symbol)` selects the symbol fill policy. Keep the declared CTrade object name, and use a deviation value only when the authenticated execution contract already specifies one; do not invent a deviation value or infer fill policy from minimum volume. `SetDeviationsInPoints` and `SetTypeFillingByVolume` are not CTrade methods.
KCOS omits cooldown inputs and all cooldown behavior entirely. Do not declare or reference
InpMinBarsBetweenTrades or any other cooldown control, and do not add a substitute entry throttle.
Time-stop inputs remain fixed at zero; guard every time-stop expiry comparison by input > 0.
Do not add a fallback time stop.
Input ownership and MQL types are provided by input_resolution.shared_inputs, parsed from the approved include.
Do not redeclare include-owned inputs. Their compiled defaults may differ from the approved runtime defaults:
the tester sets those runtime values before OnInit. A date serialized as UTC epoch is still an MQL datetime.
Use the effective spec and runtime values; original metadata aliases are resolved deterministically in provenance.
Include-owned inputs are verified from the authenticated include and its exact hash. Do not redeclare them.
The cron verifies each EA-owned input has a declaration and executable use; do not produce a separate evidence map.
Copy each verified_shared_dependencies.include_directive into your proposed EA.
The cron already verified that dependency's existence, hash and ownership; filesystem tools are unnecessary.
You are not being asked to change compiled include defaults or implement the external tester's runtime bindings.
tester_config describes the cron's testing context; the separate runs are performed by the tester, not source-generation tools.
source_admission_checks contains the current executable verification predicates, not strategy equations.
Implement real control flow that satisfies these guards as well as the research contract; comments and dummy calls are invalid.
Reuse canonical include helpers, including its NewBarClosed, without defining conflicting local functions.
Call the include's NewBarClosed directly in the OnTick early-return gate; the current admission checks require this direct call.
Each order side needs its own matching projected-margin guard, canonical quantity with immediate zero refusal,
and canonical nonzero stop and take-profit values. The cron supplies the 2x free-margin/notional check after source generation. If either calculated price is invalid/nonpositive, return without submitting an order; never submit a fallback Buy/Sell with `0.0` SL or TP. Express every refusal as a direct terminating `return;` in the void entry function:
`if (volume <= 0.0) return;`, `volume = NormalizeLot(volume);`, `if (volume <= 0.0) return;`,
`if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, volume, ask)) return;` (use SELL/bid on the sell path).
Do not use empty refusal bodies, `else if` chains, or comment-only blocks; the source gate authenticates a direct terminating refusal.
Live margin refusal and closing positions must run before signal early returns.
The order volume variable itself must receive MinatoRegimeQuantity and immediate <=0 refusal before normalization.
Use exactly this global helper body (formatting may vary, executable statements and their order must match):
`double NormalizeLot(double requestedVolume) { double minVolume=SymbolInfoDouble(_Symbol,SYMBOL_VOLUME_MIN); double maxVolume=SymbolInfoDouble(_Symbol,SYMBOL_VOLUME_MAX); double volumeStep=SymbolInfoDouble(_Symbol,SYMBOL_VOLUME_STEP); if(requestedVolume<=0.0)return 0.0; if(minVolume<=0.0)return 0.0; if(maxVolume<minVolume)return 0.0; if(volumeStep<=0.0)return 0.0; double cappedVolume=MathMin(requestedVolume,maxVolume); double flooredVolume=MathFloor(cappedVolume/volumeStep)*volumeStep; if(flooredVolume<minVolume)return 0.0; double normalizedVolume=NormalizeDouble(flooredVolume,8); if(normalizedVolume<minVolume)return 0.0; if(normalizedVolume>maxVolume)return 0.0; return normalizedVolume; }`.
This exact ordered body binds each symbol field to its matching local, validates before division, caps before flooring,
floors by the validated step, and returns the eight-digit normalized floored value. On each Buy/Sell
path: assign its actual submitted volume from MinatoRegimeQuantity, immediately refuse <=0, assign that SAME
variable with `volume = NormalizeLot(volume);`, immediately refuse <=0 again, then make the final projected-margin
check with that same variable and exact submitted entry price. No inline normalization arithmetic or alternate
normalizer is accepted by the verifier.
Pass the exact same `ask` or `bid` identifier directly to MinatoAllowsProjectedMargin and CTrade.Buy/Sell; do not create submittedPrice or other aliases. Call `if (!FreeMarginAllowsNotional(volume, ask_or_bid)) return;` after MinatoAllowsProjectedMargin using that same volume and price. Keep each returned TP variable direct in the order call; no self-assignment aliases. After those two margin refusals, assign the submitted TP directly from `MinatoTakeProfit(entryPrice, ATR_value * InpTP_ATR_Mult, RegimeTPMult(active_closed_bar_regime), is_buy)`; do not hand-code or algebraically rearrange the TP helper result. The SL is `entryPrice +/- ATR_value * InpSL_ATR_Mult * RegimeSLMult(active_closed_bar_regime)` and both calculated nonzero prices are submitted as identifiers.
Contract interpretation for this MQL5 EA:
- The specification's `SYMBOL_TRADE_EXECUTION_VOLUME` label is not an MQL5 symbol property. Use the actual SYMBOL_VOLUME_MIN, SYMBOL_VOLUME_MAX and SYMBOL_VOLUME_STEP properties in the locked NormalizeLot helper; do not reference a nonexistent enum.
- NewBarClosed detects the first tick of a new bar. On that tick, shift 1 is the just-closed bar and a market order at the current ask/bid is the next-bar-open entry. This satisfies the closed-bar and next-open requirements.
- With no open position, entries are only the declared NEUTRAL->GREED buy and NEUTRAL->EXTREME_FEAR sell. With an open short, a transition into GREED closes it and schedules a buy for the next NewBarClosed event; with an open long, a transition into EXTREME_FEAR closes it and schedules a sell for the next NewBarClosed event. That existing-position conflict rule explicitly permits the delayed opposite entry without inventing a new regime transition. Never close and reopen on the same event. Otherwise use only the stated GREED->NEUTRAL long exit and EXTREME_FEAR->FEAR short exit; do not invent extra neutral exits.
- The exit contract names the current closed-bar regime for SL/TP multipliers. Recalculate with that current regime; retain the most recently classified closed-bar regime between NewBarClosed events for per-tick SL/TP checks; do not substitute entry-time regime. Opposite-direction conflict handling must not require prev==NEUTRAL.
- MinatoTakeProfit computes `entry +/- base_distance * regime_multiplier`. Pass `ATR * InpTP_ATR_Mult` as base_distance and the selected regime multiplier once; never multiply it twice.
- Keep an explicit InpTP_ATR_Mult >= 0.5 refusal as specified, even though the current optimization grid starts at 1.5.
OnTick ordering: Minato tick capture; live margin close/refuse; manage any open position on every tick using the latest classified closed-bar regime and ATR shift 1; then `if (!NewBarClosed()) return;`; only after that one-shot gate compute/push exactly one shift-1 GK sample and evaluate regimes/signals. Never update the GK ring before the gate or insert the same closed candle on every tick. Use built-in `iATR(_Symbol, _Period, InpATRPeriod)` and `CopyBuffer(handle, 0, 1, 1, values)`; release the handle in OnDeinit. Never read CopyBuffer shift 0 or hand-roll ATR. Maintain the last-N valid GK samples; the mean and sample stdev are order-independent, so summing all N occupied ring slots is correct regardless of physical order. Invalid OHLC bars are skipped and do not increment valid-sample count or push sentinels. Do not signal until the authenticated GK and ATR data are ready. Initialize the first eligible prior regime to NEUTRAL. Current valid closed-bar observations are part of the rolling last-N values used by the contract z-score.
- The cron deterministically inserts a read-only 2x free-margin/notional refusal immediately after each valid MinatoAllowsProjectedMargin call when missing, and records that repair. Do not add your own FreeMarginAllowsNotional helper or duplicate guard. Keep the canonical projected-margin guard in every order path.
For the existing live-margin admission wrapper, OnTick checks MarginBreached(), calls CloseAllPositions(), then returns. CloseAllPositions must iterate owned positions in descending order from PositionsTotal()-1 to zero and decrement the index (`i--` or `--i`), skipping zero tickets and nonmatching symbol/magic before closing the ticket.
Implement both helpers with real account-margin detection against the include's MINATO_MIN_MARGIN_LEVEL and closing owned positions.
This close-all wrapper uses the canonical fixed 120% emergency threshold and must not be rewritten to use an input. Separately, the authenticated Stage 1 spec may declare a non-optimized InpMarginKillLevel for refusing new entries; preserve and use that exact input as an additional entry gate when the spec requires it. MQL5 has no ACCOUNT_MARGIN_LEVEL_FREE property: use AccountInfoDouble(ACCOUNT_MARGIN_LEVEL) for the percentage threshold. When the per-entry threshold is breached, log `margin_kill_skip` and refuse that entry without changing the fixed 120% close-all wrapper. Merely naming helpers is insufficient.
The same margin_kill_120 check runs here and in full Stage 2 verification. Match its concrete wrapper representation:
MarginBreached reads used margin and margin level into distinct doubles and returns used>0, level>0 and level<the approved constant.
CloseAllPositions uses a descending position-ticket loop, skips zero tickets and foreign symbol/magic positions, then calls CTrade.PositionClose.
After MinatoV2ObserveTick, OnTick immediately checks the margin wrapper, closes owned positions and returns before any other signal gate.
The cron authenticates the include and approval bytes; those filesystem checks are not work the source worker must perform.
CTrade Buy/Sell are new-entry paths. Use its PositionClose API for closing an existing position;
Their argument order is volume,symbol,price,sl,tp,comment. The symbol argument is a string such as _Symbol;
never put numeric ask or bid in that slot. The third argument is the entry price used by the projected-margin guard.
the entry Buy/Sell admission predicate does not apply to PositionClose. Closing does not require opening a reverse trade.
OnInit must start with this exact fail-closed sequence before other initialization: `int OnInit() { MinatoV2ResetCapture(); if (!MinatoV2ConfigurationValid()) return INIT_PARAMETERS_INCORRECT; ... }`. Keep the remaining EA initialization after that check. Do not merely call ConfigurationValid and ignore its result.
Use the exact canonical scorer `double OnTester() { return MinatoV2ScoreFromTester(); }` or one file-scope @@KCOS_CANONICAL_ONTESTER@@ marker. The cron normalizes the exact scorer body to the marker and applies the same canonical implementation before validation; do not create or alter Minato scoring logic.
All signals use closed bars, preserve legal bounds, margin kill 120, projected margin, normalized quantity,
zero-lot refusal and MinatoTakeProfit. No strategy substitution, timeframe reinterpretation or unsupported assumptions.
"""
    atr_input_name = atr_period_input_name(spec)
    instructions = instructions.replace("InpATRPeriod", atr_input_name)
    gjr_identity = " ".join(str(value or "") for value in (
        spec.get("ea"), spec.get("candidate_ea"),
        (spec.get("tester_config") or {}).get("ea"),
    )).casefold()
    compact_gjr_identity = re.sub(r"[^a-z0-9]", "", gjr_identity)
    if "gjr" in compact_gjr_identity and "garch" in compact_gjr_identity:
        instructions += (
            "\nGJR STAGE-2 POSITION SIZE POLICY: the effective Stage 2 contract adds `InpBaseLot=0.01` "
            "with Stage 3 optimization grid [lo=0.01,hi=0.20,step=0.01], "
            "derived by Stage 2 from the existing GJR source fallback because Stage 1 omitted this input. "
            "Declare it exactly once with that default and range comment. In both TryBuy and TrySell, read "
            "`double minVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);`, return when minVolume <= 0, "
            "set `double baseQty = MathMax(InpBaseLot, minVolume);`, then set the submitted volume using "
            "`MinatoRegimeQuantity(baseQty, RegimeLotMult(g_active_regime))`. Preserve the existing regime "
            "multipliers and all signal, stop, target, and position-management logic unchanged.\n")
    if is_dcc_garch_spec(spec):
        # The shared source lane used to carry GK-only directions and legacy
        # ATR names. Bind its implementation guidance to this authenticated
        # DCC spec; all input values and ranges remain locked by the same
        # deterministic admission checks below.
        instructions = (instructions
            .replace("InpATRPeriod", "InpAtrPeriod")
            .replace("InpTP_ATR_Mult", "InpAtrTpMult")
            .replace("InpSL_ATR_Mult", "InpAtrSlMult"))
        instructions += ("\nDeclare local EA regime constants in an enum using the KCOS_REGIME_* namespace, not with #define; "
                         "the deterministic verifier rejects OnInit references to preprocessor macros. Preserve every "
                         "authenticated regime name and value. "
                         "Do not declare any MINATO_* macro or redefine an identifier from the approved Minato include. "
                         "Keep the four regime values and all strategy behavior exactly as specified.\n")
        instructions = re.sub(
            r"OnTick ordering:.*?Current valid closed-bar observations are part of the rolling last-N values used by the contract z-score\.",
            "OnTick ordering: Minato tick capture; live margin close/refuse; manage open positions; "
            "then call NewBarClosed and return on non-new bars. Compute the exact DCC-GARCH, z_rho, "
            "realized-volatility z-score, four-state Kiyosaki regime and ETH EMA/ATR signals from the "
            "authenticated execution_contract using only completed bars and the declared inputs. "
            "Do not implement GK sampling, substitute a different strategy, or infer missing constants.",
            instructions, flags=re.S)
        instructions = re.sub(
            r"- With no open position, entries are only .*?do not invent extra neutral exits\.",
            "- Implement only the authenticated DCC signals: long when z_rho >= InpRegimeZLongEntry and "
            "z_vol <= InpRegimeVolZEntry with the specified ETH EMA/ATR confirmation; short when "
            "z_rho <= InpRegimeZShortEntry and z_vol > InpRegimeVolZEntry with its specified confirmation. "
            "Skip simultaneous/conflicting signals and apply the spec's dominant GREED/EXTREME_FEAR gate. "
            "Preserve TP and SL behavior. KCOS omits cooldown inputs and behavior entirely; explicitly guard "
            "every time-stop expiry with input > 0.",
            instructions, flags=re.S)
        instructions += ("\nDCC CLOSED-RETURN ARRAY CONTRACT (the unchanged deterministic verifier accepts this bounded form):\n"
                         "Implement ComputeDCCSignals with the following exact history shape; do not invent aliases, helper consumers, alternate loop bounds, or extra indexed reads. "
                         "Declare h_eth_hist[] and h_btc_hist[] on separate double declaration lines; do not combine them. "
                         "Define the complete OnInit function before the ComputeDCCSignals function definition; "
                         "a ComputeDCCSignals prototype may precede OnInit if required. This order is required by "
                         "the current bounded verifier, while file-scope buffer declarations may precede both. "
                         "For ComputeDCCSignals, copy DCC_BOUNDED_IMPLEMENTATION_EXAMPLE verbatim: preserve its exact declarations, assignments, early returns, loop guards, and MathMin/clamp forms. Do not add defensive branches or equivalent rewrites; the verifier admits this bounded representation only. "
                         "Copy both close series with "
                         "CopyClose(..., 0, InpDccLookback + 260, ...), set n = MathMin(eth_total, btc_total), and "
                         "return false unless each copy is positive and n >= InpDccLookback + 10. Arrays r_eth and r_btc "
                         "must each be resized to n - 2. Their sole fill loop is k=0; k<n-2; k++, with "
                         "r_eth[k] = MathLog(g_eth_close[k+1]) - MathLog(g_eth_close[k+2]) and the same expression "
                         "for BTC. This maps only closed-bar shifts 1 and 2; never index shift 0. Set seed_n = "
                         "InpDccLookback, clamp it down to n-2, and use only k < seed_n for both seed passes. Resize "
                         "h_eth_hist and h_btc_hist to n-2 and fill them in exactly one k<n-2 forward GARCH loop. "
                         "Set qbar_n = MathMin(InpDccLookback, n-2); use k<qbar_n for Q-bar and k=qbar_n; k<n-2 "
                         "for the DCC tail recursion. Resize corr_hist to InpDccLookback. Its sole loop is "
                         "k=0; k<InpDccLookback; k++; set src=k+1 and read r_*/h_* at src only inside if(src<n-2). "
                         "Resize vol_hist to InpRegimeVolLookup. Set vol_start=(n-2)-InpRegimeVolLookup; clamp "
                         "vol_start to zero when negative. In the sole k<InpRegimeVolLookup loop set src=vol_start+k, "
                         "clamp src below zero to zero and src >= n-2 to n-3 before any history access. Do not access "
                         "these tracked arrays anywhere else or pass them to any other helper. Use the exact "
                         "RollingZScore consumers: corr_hist with both lookbacks InpDccLookback and current corr; "
                         "vol_hist with both lookbacks InpRegimeVolLookup and current sqrt(max(h_eth,1e-12)). "
                          "Define RollingZScore exactly once with the canonical signature and body below; do not add a forward declaration or alter its arguments. The bounded verifier requires one definition and explicit total/lookback parameters.\n"
                          + ROLLING_ZSCORE_BOUNDED_REFERENCE + "\n"
                         "These concrete bounds preserve shift-1/shift-2 closed-return semantics and are mandatory. "
                         "For the DCC calculation, explicitly use one_minus=1.0-InpDccEmaQ-InpDccEmaP and update "
                         "each Q component as one_minus*qbar_component + InpDccEmaQ*z_component_product + "
                         "InpDccEmaP*previous_q_component; use the same recursion for the correlation history. "
                         "This is required executable use of both authenticated EMA inputs, not optional tuning. "
                         "The source admission verifier remains authoritative; this prompt does not waive any check.\n")
        instructions += ("\nVERIFIER-APPROVED DCC BOUNDS REFERENCE (dataflow only; Stage 1 remains the strategy authority):\n"
                         "The complete function below is copied from a prior authenticated Stage2-verified EA. Use its global-buffer declarations/series setup pattern and its ComputeDCCSignals history allocation, shift mapping, seed clamps, loop bounds, and double RollingZScore consumers exactly. Do not re-declare g_eth_close/g_btc_close inside the function, resize those close buffers, switch CopyClose to PERIOD_CURRENT, replace double histories with integers, hand-calculate rolling z-scores, or add nested source-index loops. Keep this reference function's bounded history dataflow intact. Align calls and any signal/regime logic outside this function with the current authenticated Stage 1 execution_contract; do not copy the old strategy's entry/exit rules or override current inputs. The deterministic source gate remains authoritative.\n"
                         "File-scope declarations and one-time OnInit setup required by this dataflow: double g_eth_close[]; double g_btc_close[]; ArraySetAsSeries(g_eth_close,true); ArraySetAsSeries(g_btc_close,true).\n"
                         "```mql5\n" + DCC_BOUNDED_IMPLEMENTATION_EXAMPLE + "\n```\n")
    else:
        instructions = remove_foreign_strategy_guidance(instructions)
    atr_period_input = atr_period_input_name(spec)
    atr_period_expression = ("PERIOD_M5" if spec.get("stage2_requirements", {}).get(
        "must_use_ethusd_m5_closed_bars") else "_Period")
    instructions += (
        "\nMQL5 TYPE SAFETY: represent categorical regime IDs with the authenticated enum/int type throughout. "
        "Do not store a regime ID as double, cast an int regime ID to double for a helper call, or rely on "
        "lossy double-to-int conversions. Keep continuous scores and prices as double.\n"
        "\nVERIFIER-REQUIRED ATR SOURCE BINDING (applies to every strategy): create the indicator handle with "
        f"iATR(_Symbol, {atr_period_expression}, {atr_period_input}) and read its closed-bar value using "
        "CopyBuffer(handle, 0, 1, 1, values). Check that exactly one value was copied and that it is valid "
        f"before using it. Pass {atr_period_input} directly as the handle period; do not assign it to an alias. "
        "Do not hand-calculate ATR, use buffer shift 0, or substitute another timeframe. "
        "This period input and timeframe are bound to the authenticated spec and deterministic verifier.\n")
    instructions += (
        "\nOPERATIONAL POSITION IDENTITY (reserved metadata, not a strategy parameter): declare exactly "
        f"`input ulong InpMagicNumber = {cycle_magic_number(spec)};` and do not optimize it. "
        "Use InpMagicNumber for CTrade.SetExpertMagicNumber and every POSITION_MAGIC ownership check; "
        "never hardcode zero or another literal for position identity. This value is derived from the authenticated cycle date.\n")
    # The authenticated Stage 1 execution contract is the sole authority for
    # strategy behavior. Shared instructions above describe platform and
    # deterministic admission requirements only.
    instructions = authenticated_contract_guidance(instructions, spec, contract)
    phase = "proposal"
    try:
        feedback = ("\nPRIOR SAME-LINEAGE HALT FEEDBACK (verified immutable artifacts):\n"
                    + json.dumps(prior_feedback["prior_holds"], ensure_ascii=False)
                    + "\nRepair these specific findings under the unchanged authenticated contract.\n") if prior_feedback else ""
        if prior_feedback:
            write_json(attempt / "prior-repair-feedback.json", prior_feedback)
        diagnoses = []
        for proposal_number in range(1, MAX_PROPOSAL_ATTEMPTS + 1):
            proposal_label = "proposal" if proposal_number == 1 else f"proposal-retry-{proposal_number}"
            # Timeouts/launcher failures propagate immediately. Only returned
            # proposals rejected by these deterministic gates get a bounded retry.
            authenticate(spec_path, state_path, cycle_id, ea_name, candidates_dir)
            proposal = None
            source = None
            returned_proposal = False
            try:
                proposal = model_json(instructions + context + feedback
                                  + "\nIMMUTABLE STRATEGY INPUT LOCK (authoritative names, defaults, and ranges; repair implementation code only):\n"
                                  + json.dumps(strategy_lock, ensure_ascii=False, sort_keys=True)
                                  + "\nFor optimized inputs with domain_provenance.method=source_comment, retain the exact source_domain comment from the approved spec. The cron may add only a missing comment from that authenticated metadata; it rejects any conflicting domain.\n"
                                  + "\nCanonical approved Minato include (read-only):\n"
                                  + minato.read_text(encoding="utf-8-sig")
                                  + "\nEND OF DEPENDENCY. Return exactly one fenced mql5 block containing the complete source, or the specified single-key JSON refusal. "
                                  "Do not return a plan, a promise to generate later, or an unsupported reason that says the contract is implementable. "
                                  "If an actual requirement cannot be satisfied, identify that requirement precisely. "
                                  "The complete EA source is your output; cron publication and testing follow afterwards.\n",
                                      attempt, proposal_label, runner)
                returned_proposal = True
                if 'unsupported' in proposal:
                    require(set(proposal) == {'unsupported'} and isinstance(proposal['unsupported'], str)
                            and proposal['unsupported'].strip(),
                            "response schemas cannot be combined: a successful proposal has only filename and source_text; "
                            "a refusal has only a nonempty unsupported reason. Remove empty refusal fields from a complete proposal.")
                    fail('generator refused: ' + proposal['unsupported'])
                # Bind code-only responses to the authenticated Stage 1 EA name.
                # Accept legacy evidence only as a compatibility response field.
                if set(proposal) == {"source_text"}:
                    proposal = {"filename": ea_name + ".mq5", **proposal}
                if set(proposal) == {"filename", "source_text", "evidence"}:
                    proposal = {"filename": proposal["filename"], "source_text": proposal["source_text"]}
                require(set(proposal) == {"filename", "source_text"} and proposal.get("filename") == ea_name + ".mq5",
                        "proposal must contain exactly one correctly named source")
                source = proposal.get("source_text")
                require(isinstance(source, str), "proposal source_text must be a string")
                source, source_repairs, source_usage = prepare_source_for_admission(
                    source, spec, shared_contract)
                if source_repairs:
                    write_json(attempt / f"source-repairs-{proposal_number}.json", source_repairs)
                phase = "deterministic_source_admission"
                source_hash = digest(source.encode("utf-8"))
            except ValueError as exc:
                if not returned_proposal and not isinstance(exc, ReturnedProposalFormatError):
                    raise  # Actual timeout/launcher/provider errors stay held.
                diagnosis = proposal_diagnosis(exc, proposal, diagnoses)
                diagnoses.append(diagnosis)
                write_json(attempt / f"proposal-rejection-{proposal_number}.json",
                           {"attempt": proposal_number, "limit": MAX_PROPOSAL_ATTEMPTS, "reason": str(exc),
                            "diagnosis": diagnosis,
                            "raw_proposal": str(attempt / (proposal_label + ".stdout.txt"))})
                if proposal_number == MAX_PROPOSAL_ATTEMPTS:
                    raise
                rejected_source = source if isinstance(source, str) else (
                    proposal.get("source_text") if isinstance(proposal, dict) else None)
                feedback = surgical_repair_feedback(exc, diagnosis, rejected_source)
                compatible_lessons = []
                if isinstance(rejected_source, str):
                    compatible_lessons = reusable_lessons_for_rejected_source(
                        diagnosis, rejected_source, candidates_dir / (ea_name + ".mq5"))
                lesson_artifact = attempt / f"compatible-lessons-proposal-{proposal_number}.json"
                write_json(lesson_artifact, {
                    "schema_version": 1, "failure_fingerprint": diagnosis.get("failure_fingerprint"),
                    "ea_family": (compatible_lessons[0].get("ea_family") if compatible_lessons else None),
                    "contract_id": json.loads((ROOT / "state" / "contracts" / "kcos_stage2_protected_behavior_v1.json").read_text(encoding="utf-8-sig")).get("contract_id"),
                    "lesson_ids": [item.get("lesson_id") for item in compatible_lessons],
                    "lessons": compatible_lessons,
                })
                if compatible_lessons:
                    feedback += (
                        "\nVERIFIED REUSABLE LESSONS FOR THIS EXACT SOURCE-ADMISSION DEFECT, FAMILY, CONTRACT, AND SOURCE:\n"
                        + json.dumps(compatible_lessons, ensure_ascii=False, indent=2, sort_keys=True)
                        + "\nUse only when each lesson applicability and precondition matches. Respect negative examples. "
                        "The current deterministic admission, compilation, and Stage 2 checks remain authoritative.\n")
                continue
            break
        # The accepted proposal has passed deterministic source admission.
        # Verify immutable lineage again after source generation; do not publish
        # a proposal against a changed spec/include/event or rotated state.
        authenticate(spec_path, state_path, cycle_id, ea_name, candidates_dir)
        require(sha(spec_path) == bound["source_spec_sha256"]
                and stage1_event_lineage_sha256(event_path) == bound["stage1_event_sha256"]
                and sha(minato) == bound["canonical_minato_sha256"], "lineage changed while model generated source")
        require(sha(Path(bound["effective_spec"])) == bound["effective_spec_sha256"]
                and sha(Path(bound["input_resolution"])) == bound["input_resolution_sha256"],
                "resolved input evidence changed while model generated source")
        receipt = {"schema_version": 1, **bound, "source_sha256": source_hash, "passed": True,
                   "status": "DETERMINISTIC_SOURCE_ADMISSION", "implementation_mode": "model_generated_source_admitted",
                   "proposal_attempts": proposal_number,
                   "strategy_input_lock": str(attempt / "strategy-input-lock.json"),
                   "strategy_input_lock_sha256": sha(attempt / "strategy-input-lock.json"),
                   "deterministic_source_repairs": source_repairs,
                   "raw_proposal": str(attempt / (proposal_label + ".stdout.txt")),
                   "raw_proposal_sha256": sha(attempt / (proposal_label + ".stdout.txt")),
                   "implementation_evidence": {"input_usage": source_usage,
                                                "source_admission_passed": True,
                                                "independent_model_review": "not_required"},
                   "limitations": "Deterministic source admission; compilation and full Stage 2 verification still required."}
        receipt_path = attempt / "source-admission-receipt.json"
        write_json(receipt_path, receipt)
        return {"source_text": source, "source_admission_receipt": {**receipt, "receipt_path": str(receipt_path), "receipt_sha256": sha(receipt_path)}}
    except Exception as exc:
        hold_path = attempt / "hold.json"
        write_json(hold_path, {**bound, "status": "MODEL_SOURCE_HOLD", "reason": str(exc), "phase": phase,
                                          "proposal_diagnoses": diagnoses,
                                          "proposal_limit": MAX_PROPOSAL_ATTEMPTS,
                                          "proposal_attempts": len(diagnoses),
                                          "stop_reason": "PROPOSAL_BUDGET_EXHAUSTED"})
        exc.add_note("KCOS_SOURCE_HOLD_ARTIFACT=" + str(hold_path))
        exc.add_note("KCOS_SOURCE_HOLD_SHA256=" + sha(hold_path))
        raise
