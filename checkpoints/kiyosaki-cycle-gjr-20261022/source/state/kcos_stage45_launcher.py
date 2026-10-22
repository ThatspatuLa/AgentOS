"""Six 2026-09-15: Stage 4.5 narrow opt launcher (deterministic).

Builds narrow INI from Stage 4 decision, launches MT5 with watcher, parses
results, writes Stage 4.5 evidence, advances state.

Steps:
  1. Read Stage 4 decision + wide INI
  2. Build narrow INI from input_actions (first run: use wide bounds)
  3. Write narrow INI + stage4.5 runtime manifest
  4. Launch MT5 with narrow grid
  5. Watcher monitors until journal completion
  6. Parse narrow .opt → narrow CSV
  7. Write stage4.5 decision + verification + event
  8. Advance v2 state to narrow_opt_complete
"""
import ctypes
from ctypes import wintypes
import csv
import json
import shutil
import subprocess
import sys
import time
import hashlib
import math
import re
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from kcos_runtime_paths_v2 import assert_safe_root, mt5_data_root
from kcos_stage45_workflow_v2 import (
    evaluate_refinement_range, fit_refinement_grid, input_movement_bounds, refined_step,
    select_top_records,
    validate_input_range_plan,
)

ROOT = assert_safe_root()
STATE = ROOT / 'state'
CYCLE_STATE = STATE / 'kcos_cycle_state_v2.json'
QUEUE = STATE / 'stage4_pending_queue_v2.json'
STAGE3_HANDOFF = STATE / 'handoffs' / 'Stage3_v2.json'


def authenticated_stage3_input_handoff() -> dict:
    """Return the current hash-bound Stage 3 source contract for Tester binding."""
    handoff = json.loads(STAGE3_HANDOFF.read_text(encoding='utf-8'))
    if (handoff.get('cycle_id') != CYCLE or handoff.get('ea') != EA
            or TF not in handoff.get('timeframe_order', [])):
        raise RuntimeError('Stage 4.5 input schema authority does not match the active Stage 3 cycle')
    return handoff


def _active_stage45_identity() -> tuple[str, str, str, str, str, str, Path]:
    """Resolve Stage 4.5 identity only from the active v2 handoff."""
    state = json.loads(CYCLE_STATE.read_text(encoding='utf-8'))
    cycle = state.get('cycle_id')
    ea = state.get('current_ea')
    if state.get('stage_status') not in {
        'stage4_complete_held', 'stage4_5_start_required',
        'stage4_5_running', 'narrow_opt_complete', 'narrow_opt_failed'
    } or not cycle or not ea:
        raise RuntimeError('Stage 4.5 requires an active authenticated Stage 4 v2 state')
    queue = json.loads(QUEUE.read_text(encoding='utf-8'))
    entry = next((item for item in queue.get('entries', [])
                  if item.get('cycle_id') == cycle and item.get('ea') == ea
                  and item.get('status') == 'stage4_pending_narrow_opt'), None)
    if entry is None:
        raise RuntimeError('no verified active-cycle Stage 4.5 queue entry')
    decision_path = Path(entry.get('stage4_decision', ''))
    if not decision_path.is_file():
        raise RuntimeError('active Stage 4 decision is missing')
    decision = json.loads(decision_path.read_text(encoding='utf-8'))
    if decision.get('cycle_id') != cycle or decision.get('ea') != ea:
        raise RuntimeError('Stage 4.5 identity mismatch with canonical decision')
    tf = decision.get('selected_tf')
    if tf not in {'M5', 'M15', 'H1'}:
        raise RuntimeError(f'invalid Stage 4.5 timeframe: {tf!r}')
    window = state.get('window') or {}
    from_date = decision.get('from_date') or window.get('from_date')
    to_date = decision.get('to_date') or window.get('to_date')
    if not from_date or not to_date:
        raise RuntimeError('active Stage 4.5 dates are missing from decision and canonical state')
    return (cycle, ea, tf, decision.get('symbol', 'ETHUSD'),
            from_date, to_date, decision_path)


try:
    CYCLE, EA, TF, SYMBOL, FROM_DATE, TO_DATE, STAGE4_DECISION = _active_stage45_identity()
    ACTIVE_STAGE45_RESOLUTION_ERROR = None
except RuntimeError as exc:
    # Ineligible stages are a normal cron no-op, not a cron failure.
    CYCLE = EA = TF = SYMBOL = None
    FROM_DATE = TO_DATE = ''
    STAGE4_DECISION = None
    ACTIVE_STAGE45_RESOLUTION_ERROR = str(exc)
CYCLE_STATE_BACKUP = STATE / 'kcos_cycle_state_v2.json.bak.pre-stage45-launch'

def _current_run_number() -> int:
    """Return next unused run, reconciling verified immutable completion evidence."""
    try:
        state = json.loads(CYCLE_STATE.read_text(encoding='utf-8'))
        value = int(state.get('narrow_run_number', 1))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        state, value = {}, 1
    if value < 1 or value > 3:
        raise RuntimeError(f'invalid Stage 4.5 refinement run number: {value}')
    completed = 0
    for run in (1, 2, 3):
        paths = {
            'decision': STATE / 'decisions' / f'{CYCLE}.stage45.run{run}.decision.json',
            'verification': STATE / 'verification' / f'{CYCLE}.stage45.run{run}.verification.json',
            'event': STATE / 'events' / f'{CYCLE}.stage45.run{run}.complete.json',
            'complete': STATE / 'stage45_v2' / f'{CYCLE}.stage45.{TF}.run{run}.complete.json',
        }
        try:
            decision, verification, event, complete = (
                json.loads(paths[key].read_text(encoding='utf-8'))
                for key in ('decision', 'verification', 'event', 'complete'))
            csv_hash = hashlib.sha256(Path(decision['csv']).read_bytes()).hexdigest()
            decision_hash = hashlib.sha256(paths['decision'].read_bytes()).hexdigest()
            verification_hash = hashlib.sha256(paths['verification'].read_bytes()).hexdigest()
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            break
        if not (
            decision.get('cycle_id') == CYCLE and decision.get('stage') == '4.5'
            and decision.get('run_number') == run and decision.get('run_count_required') == 3
            and decision.get('verdict') == 'NARROW_OPT_COMPLETE'
            and int(decision.get('qualifying_row_count', 0)) > 0
            and (int(decision.get('passes_observed') or 0) > 0
                 or int(decision.get('parsed_opt_pass_records') or 0) > 0)
            and (int(complete.get('passes') or 0) > 0
                 or int(complete.get('parsed_opt_pass_records') or 0) > 0)
            and csv_hash.casefold() == str(decision.get('csv_sha256', '')).casefold()
            and verification.get('passed') is True
            and event.get('cycle_id') == CYCLE and event.get('run_number') == run
            and event.get('verification_passed') is True
            and str(event.get('decision_sha256', '')).casefold() == decision_hash.casefold()
            and str(event.get('verification_sha256', '')).casefold() == verification_hash.casefold()
            and complete.get('status') in {'TIMEFRAME_COMPLETE', 'TIMEOUT_SETTLING'}
        ):
            break
        completed = run
    next_value = max(value, completed + 1)
    if next_value > 3:
        # Sentinel consumed by main() as a successful terminal state; the
        # every-two-minute cron must become a no-op after verified run 3.
        return 4
    if next_value > value:
        state['narrow_run_number'] = next_value
        state['stage45_run_number'] = completed
        state['current_stage'] = 'stage4_complete_held'
        state['stage_status'] = 'stage4_complete_held'
        state['next_action'] = 'run_stage45_next'
        if completed:
            for key, path in (
                ('last_stage45_decision', STATE / 'decisions' / f'{CYCLE}.stage45.run{completed}.decision.json'),
                ('last_stage45_verification', STATE / 'verification' / f'{CYCLE}.stage45.run{completed}.verification.json'),
                ('last_stage45_event', STATE / 'events' / f'{CYCLE}.stage45.run{completed}.complete.json'),
            ):
                state[key] = str(path.resolve())
            state['narrow_opt_decision'] = state['last_stage45_decision']
            state['narrow_opt_event'] = state['last_stage45_event']
        archive = STATE / 'repair_archive'
        archive.mkdir(parents=True, exist_ok=True)
        backup = archive / f'{CYCLE}.cycle_state.before_run_cursor_reconcile.{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}.json'
        backup.write_bytes(CYCLE_STATE.read_bytes())
        CYCLE_STATE.write_text(json.dumps(state, indent=2), encoding='utf-8')
        print(f'[RECOVERY] verified Stage 4.5 run {completed}/3; continuing with run {next_value}/3')
    return next_value

RUN_NUMBER = _current_run_number() if CYCLE else 0

WIDE_INI = STATE / 'stage3_v2' / f'{CYCLE}.{EA}.{SYMBOL}.{TF}.{FROM_DATE.replace(".","")}_{TO_DATE.replace(".","")}.wide.ini'
# STAGE4_DECISION is resolved from the authenticated queue entry above.

# narrow INI goes into state/stage3_v2 alongside wide INI
NARROW_INI = STATE / 'stage3_v2' / f'{CYCLE}.{EA}.{SYMBOL}.{TF}.{FROM_DATE.replace(".","")}_{TO_DATE.replace(".","")}.run{RUN_NUMBER}.narrow.ini'
STARTUP_INI = NARROW_INI.with_name(NARROW_INI.stem + '.no_autostart.ini')

STAGE45_DIR = STATE / 'stage45_v2'
STAGE45_DIR.mkdir(parents=True, exist_ok=True)
NARROW_RUNTIME = STAGE45_DIR / f'{CYCLE}.stage45.run{RUN_NUMBER}.runtime.json'
LEASE = STAGE45_DIR / f'{CYCLE}.stage45.{TF}.run{RUN_NUMBER}.lease'
NARROW_COMPLETE = STAGE45_DIR / f'{CYCLE}.stage45.{TF}.run{RUN_NUMBER}.complete.json'
NARROW_CSV = STAGE45_DIR / f'{CYCLE}.stage45.{TF}.run{RUN_NUMBER}.csv'
NARROW_OPT = None  # filled by MT5 launch
NARROW_EVIDENCE = STAGE45_DIR / f'{CYCLE}.stage45.{TF}.run{RUN_NUMBER}.evidence.json'
RUN_SCREENSHOT = STAGE45_DIR / f'{CYCLE}.stage45.{TF}.run{RUN_NUMBER}.mt5.png'
NARROW_DECISION = STATE / 'decisions' / f'{CYCLE}.stage45.run{RUN_NUMBER}.decision.json'
NARROW_VERIFICATION = STATE / 'verification' / f'{CYCLE}.stage45.run{RUN_NUMBER}.verification.json'


def _authenticated_stage3_ex5_deployment() -> dict:
    """Carry forward the active cycle's verified Stage 3 EX5 identity."""
    stage3_dir = STATE / 'stage3_v2'
    candidates = [stage3_dir / f'{CYCLE}.stage3.runtime.json']
    candidates.extend(stage3_dir.glob(f'{CYCLE}.stage3.runtime.json.archive.*'))
    for path in sorted(candidates, key=lambda item: item.stat().st_mtime if item.exists() else 0,
                       reverse=True):
        try:
            stage3 = json.loads(path.read_text(encoding='utf-8'))
            deployment = stage3.get('deployment_reconciliation') or {}
            expected = str(deployment.get('authenticated_sha256') or '').casefold()
            authenticated = Path(deployment.get('authenticated_ex5') or '')
            portable = Path(deployment.get('portable_ex5') or '')
            if (stage3.get('cycle_id') != CYCLE or stage3.get('ea') != EA
                    or deployment.get('verified') is not True
                    or deployment.get('status') != 'MATCHED'
                    or len(expected) != 64 or not authenticated.is_file()
                    or not portable.is_file()
                    or sha256(authenticated).casefold() != expected
                    or sha256(portable).casefold() != expected
                    or str(deployment.get('portable_after_sha256') or '').casefold() != expected):
                continue
            return {**deployment, 'stage3_runtime': str(path.resolve())}
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    raise RuntimeError('no verified active-cycle Stage 3 EX5 deployment is available for Stage 4.5 binding')
NARROW_EVENT = STATE / 'events' / f'{CYCLE}.stage45.run{RUN_NUMBER}.complete.json'


def fixed_input_retry_receipt_path() -> Path:
    return STAGE45_DIR / f'{CYCLE}.stage45.{TF}.run{RUN_NUMBER}.fixed_input_retry.json'
class TesterInputRejected(RuntimeError):
    """MT5 rejected the configured optimization surface before starting."""

# Stage 4.5 must never share the live EightCap terminal/data singleton.
# The portable copy has its own executable, MQL5, Tester cache, and logs;
# only the large market-history `bases` tree is junctioned read-only-style.
ISOLATED_MT5_ROOT = ROOT / 'state' / 'mt5_stage45_portable'
MT5_PATH = Path(os.environ.get('KCOS_MT5_TERMINAL', str(ISOLATED_MT5_ROOT / 'terminal64.exe')))
MT5_DATA = Path(os.environ.get('KCOS_MT5_DATA_ROOT', str(ISOLATED_MT5_ROOT)))
MT5_CACHE = MT5_DATA / 'Tester' / 'cache'
MT5_LOG = MT5_DATA / 'Tester' / 'logs'

def tester_roots() -> list[Path]:
    """Return only the isolated Stage 4.5 tester root.

    Never scan the live terminal's shared data directory: a stale or reused
    live `.opt` must not be mistaken for this run's completion artifact.
    """
    return [MT5_DATA]


def sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest().upper()

def runtime_has_observed_passes(runtime: dict) -> bool:
    """A result is usable after a journal pass or authenticated .opt record."""
    try:
        passes = int(runtime.get('passes') or 0)
        parsed_records = int(runtime.get('parsed_opt_pass_records') or 0)
    except (TypeError, ValueError):
        return False
    return ((passes > 0 or (runtime.get('timed_out') and parsed_records > 0))
            and runtime.get('status') in {'TIMEFRAME_COMPLETE', 'TIMEOUT_SETTLING'})


def format_clean_grid_value(value: float, step: float) -> str:
    """Serialize bounds at the precision required by the clean increment."""
    from decimal import Decimal
    step_value = Decimal(str(step)).normalize()
    places = max(0, -step_value.adjusted() + 1)
    quantum = Decimal(1).scaleb(-places)
    rendered = format(Decimal(str(value)).quantize(quantum), 'f')
    return rendered.rstrip('0').rstrip('.') if '.' in rendered else rendered


def format_plain_numeric_text(value: str) -> str:
    """Render an exact INI decimal as plain notation without changing its value."""
    from decimal import Decimal
    rendered = format(Decimal(value.strip()), 'f')
    return rendered.rstrip('0').rstrip('.') if '.' in rendered else rendered


def matching_opt_files() -> list[Path]:
    start = FROM_DATE.replace('.', '')
    end = TO_DATE.replace('.', '')
    prefixes = {EA, EA[:59]}
    return sorted({p for root in tester_roots() for prefix in prefixes
                   for p in (root / 'Tester' / 'cache').glob(
                       f'{prefix}.{SYMBOL}.{TF}.{start}.{end}.*.opt') if p.is_file()})


def opt_baseline() -> dict:
    """Hash matching cache files before launch so old MT5 results cannot be reused."""
    return {str(path.resolve()).casefold(): sha256(path) for path in matching_opt_files()}




def backup_cycle_state():
    if not CYCLE_STATE_BACKUP.exists():
        CYCLE_STATE_BACKUP.write_text(CYCLE_STATE.read_text())


def _exact_terminal_for_current_ini(config_ini: Path | None = None) -> int | None:
    """Return a live terminal PID only when its /config path is exact."""
    try:
        import psutil
        token = str((config_ini or STARTUP_INI).resolve()).lower()
        for p in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if (p.info.get('name') or '').lower() == 'terminal64.exe' and token in ' '.join(p.info.get('cmdline') or []).lower():
                    return int(p.info['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
    except ImportError:
        return None
    return None


def _runtime_owned_live_terminal_pid(prior: dict) -> int | None:
    """Retain a live Stage 4.5 owner when MT5 normalizes its command line."""
    try:
        import psutil
        runtime = json.loads(NARROW_RUNTIME.read_text(encoding='utf-8'))
        pid = int(runtime.get('terminal_pid') or 0)
        startup_ini = Path(runtime.get('startup_ini') or '')
        if not pid or pid != int(prior.get('terminal_pid') or 0):
            return None
        if (runtime.get('cycle_id') != CYCLE or runtime.get('ea') != EA
                or runtime.get('timeframe') != TF
                or runtime.get('status') not in {'STARTING', 'OPTIMIZING'}):
            return None
        if str(Path(runtime.get('terminal_path') or '').resolve()).casefold() != str(MT5_PATH.resolve()).casefold():
            return None
        if startup_ini.resolve() != STARTUP_INI.resolve():
            return None
        actual_ini_hash = sha256(startup_ini)
        launch_identity = runtime.get('launch_identity') or {}
        if (runtime.get('startup_ini_sha256') != actual_ini_hash
                or runtime.get('narrow_ini_sha256') != sha256(NARROW_INI)
                or launch_identity.get('terminal_pid') != pid
                or Path(launch_identity.get('terminal_path') or '').resolve() != MT5_PATH.resolve()
                or Path(launch_identity.get('config_path') or '').resolve() != startup_ini.resolve()
                or launch_identity.get('config_sha256') != actual_ini_hash):
            return None
        proc = psutil.Process(pid)
        if Path(proc.exe()).resolve() != MT5_PATH.resolve():
            return None
        return pid
    except ImportError:
        return None
    except (OSError, ValueError, TypeError, KeyError):
        return None
    except psutil.Error:
        return None


def _archive_stale_lease(prior: dict) -> Path:
    """Preserve a dead startup/optimization claim before allowing one retry."""
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    archive = STATE / 'repair_archive' / f'stage45_stale_lease_{RUN_NUMBER}_{stamp}'
    archive.mkdir(parents=True, exist_ok=True)
    shutil.copy2(LEASE, archive / LEASE.name)
    if NARROW_RUNTIME.exists():
        shutil.copy2(NARROW_RUNTIME, archive / NARROW_RUNTIME.name)
    LEASE.unlink()
    print(f'[LEASE] archived stale unowned {prior.get("status")} lease: {archive}')
    return archive


def _fresh_stage45_worker_pids(runtime: dict) -> list[int] | None:
    """Find MetaTester workers from this Stage 4.5 launch; None means uncertain."""
    try:
        import psutil
        started = datetime.fromisoformat(
            str(runtime.get('started_utc') or '').replace('Z', '+00:00'))
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        worker_path = (MT5_PATH.parent / 'metatester64.exe').resolve()
        found: list[int] = []
        for proc in psutil.process_iter(['pid', 'name', 'exe', 'create_time']):
            info = proc.info
            if str(info.get('name') or '').casefold() != 'metatester64.exe':
                continue
            exe = info.get('exe')
            created = info.get('create_time')
            if not exe or created is None:
                return None
            if Path(exe).resolve() == worker_path and datetime.fromtimestamp(
                    float(created), timezone.utc) >= started:
                found.append(int(info['pid']))
        return found
    except (ImportError, OSError, ValueError, TypeError, KeyError):
        return None
    except psutil.Error:
        return None


def _fresh_optimizer_passes(runtime: dict, expected_cycle: str,
                             expected_symbol: str, expected_timeframe: str) -> int | None:
    """Return a positive completion count only from this launch's journal tail."""
    if (runtime.get('cycle_id') != expected_cycle or
            runtime.get('symbol') != expected_symbol or
            runtime.get('timeframe') != expected_timeframe):
        return None
    try:
        journal = Path(runtime.get('journal_path') or runtime.get('tester_log') or '')
        offset = int(runtime.get('journal_start_offset', -1))
        if offset < 0 or not journal.is_file():
            return None
        chunks = [journal.read_bytes()[offset:]]
        # MT5 opens a new YYYYMMDD.log at midnight. A run launched before
        # midnight may finish in the next day's file, so include that fresh
        # journal while retaining the launch offset on the original file.
        match = re.fullmatch(r'(\d{8})', journal.stem)
        if match:
            try:
                day = datetime.strptime(match.group(1), '%Y%m%d').date()
                next_journal = journal.with_name(f'{(day + timedelta(days=1)):%Y%m%d}{journal.suffix}')
                if next_journal.is_file():
                    chunks.append(next_journal.read_bytes())
            except (OverflowError, ValueError):
                pass
        text = b''.join(chunks).decode('utf-16le', errors='ignore')
    except (OSError, TypeError, ValueError):
        return None
    if re.search(r'no optimized parameter selected', text, re.I):
        return None
    passes = re.findall(r'genetic optimization finished on pass (\d+)', text, re.I)
    if not passes or 'optimization done in ' not in text.lower():
        return None
    count = int(passes[-1])
    return count if count > 0 else None


def _fresh_optimizer_start_seen(runtime: dict) -> bool:
    """Require this launch's Tester journal to prove that optimization began."""
    try:
        journal = Path(runtime.get('journal_path') or runtime.get('tester_log') or '')
        offset = int(runtime.get('journal_start_offset', -1))
        if offset < 0 or not journal.is_file():
            return False
        text = journal.read_bytes()[offset:].decode('utf-16le', errors='ignore')
    except (OSError, TypeError, ValueError):
        return False
    return bool(re.search(
        r'(?:genetic optimization started|automatic testing started|optimization started)',
        text, re.I))


def acquire_lease() -> str | bool:
    """Atomically claim a run; self-heal only dead, unowned startup claims."""
    if LEASE.exists():
        try:
            prior = json.loads(LEASE.read_text())
        except (OSError, json.JSONDecodeError):
            raise RuntimeError(f'corrupt Stage 4.5 lease: {LEASE}')
        status = prior.get('status')
        if prior.get('cycle_id') != CYCLE or prior.get('timeframe') != TF:
            raise RuntimeError('Stage 4.5 lease identity mismatch; refusing launch')
        exact_pid = _exact_terminal_for_current_ini()
        if exact_pid is None and status in {'STARTING', 'OPTIMIZING'}:
            exact_pid = _runtime_owned_live_terminal_pid(prior)
        try:
            lease_age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                (prior.get('updated_utc') or prior.get('created_utc')).replace('Z', '+00:00'))).total_seconds()
        except (AttributeError, TypeError, ValueError):
            lease_age = 10**9
        runtime_exists = NARROW_RUNTIME.exists()
        if status == 'INFRASTRUCTURE_FAILURE' and runtime_exists:
            # Retry one dead, zero-Start Stage 4.5 schema failure only when
            # authenticated source reconciliation proves a fixed input was
            # omitted and the exact Tester row count was the sole blocker.
            try:
                runtime = json.loads(NARROW_RUNTIME.read_text(encoding='utf-8'))
                state = json.loads(CYCLE_STATE.read_text(encoding='utf-8'))
                retry_key = f'{TF}:run{RUN_NUMBER}'
                retry_count = int((state.get('stage45_retry_counts') or {}).get(retry_key, 0))
                retry_receipt = fixed_input_retry_receipt_path()
                if retry_receipt.exists():
                    raise RuntimeError('bounded fixed-input retry was already consumed; refusing another launch')
                terminal_pid = int(runtime.get('terminal_pid') or prior.get('terminal_pid') or 0)
                import psutil
                terminal_gone = terminal_pid > 0 and not psutil.pid_exists(terminal_pid)
                fresh_workers = _fresh_stage45_worker_pids(runtime)
                handoff = authenticated_stage3_input_handoff()
                from kcos_stage3_launch_v2 import (
                    _read_profile_text, authenticated_input_surface_reconciliation,
                )
                reconciliation = authenticated_input_surface_reconciliation(
                    handoff, _read_profile_text(NARROW_INI))
                fixed_schema_failure = bool(
                    state.get('cycle_id') == CYCLE
                    and state.get('current_ea') == EA
                    and state.get('stage_status') == 'stage4_complete_held'
                    and (state.get('run_authorization') or {}).get('cycle_id') == CYCLE
                    and float((state.get('run_authorization') or {}).get('hold_after_stage', 0)) >= 5
                    and 5 in (state.get('run_authorization') or {}).get('allowed_stages', [])
                    and (not state.get('allowed_stages') or state.get('allowed_stages') ==
                         (state.get('run_authorization') or {}).get('allowed_stages'))
                    and runtime.get('cycle_id') == CYCLE
                    and runtime.get('ea') == EA
                    and runtime.get('timeframe') == TF
                    and runtime.get('run_number') == RUN_NUMBER
                    and runtime.get('status') == 'INFRASTRUCTURE_FAILURE'
                    and runtime.get('failure_kind') == 'TESTER_SCHEMA_BINDING_FAILED'
                    and runtime.get('stopped_after_schema_binding_failure') is True
                    and not runtime.get('start_button_invoked')
                    and int(runtime.get('input_schema_binding_attempts', 0)) >= 8
                    and (runtime.get('input_schema_binding') or {}).get('status') == 'INPUTS_NOT_READY'
                    and (runtime.get('input_schema_binding') or {}).get('visible_input_rows')
                        == (runtime.get('input_schema_binding') or {}).get('expected_input_rows') + 2
                    and Path(runtime.get('narrow_ini', '')).resolve() == NARROW_INI.resolve()
                    and runtime.get('narrow_ini_sha256') == sha256(NARROW_INI)
                    and terminal_gone and not runtime.get('worker_pids')
                    and fresh_workers == []
                    and _fresh_optimizer_start_seen(runtime) is False
                    and _fresh_optimizer_passes(runtime, CYCLE, SYMBOL, TF) is None
                    and not NARROW_COMPLETE.exists() and not NARROW_CSV.exists()
                    and bool(reconciliation.get('missing_fixed_source_defaults'))
                    and (runtime.get('retry_allowed') is True and retry_count == 1
                         or 'retry_allowed' not in runtime and retry_count == 0)
                )
                if fixed_schema_failure:
                    retry_receipt_payload = {
                        'schema_version': 1, 'cycle_id': CYCLE, 'stage': '4.5',
                        'timeframe': TF, 'run_number': RUN_NUMBER,
                        'attempt': 1,
                        'reason': 'authenticated_fixed_input_surface_zero_start_retry',
                        'runtime_sha256': hashlib.sha256(NARROW_RUNTIME.read_bytes()).hexdigest().upper()
                        if NARROW_RUNTIME.is_file() else None,
                        'narrow_ini_sha256': sha256(NARROW_INI),
                        'source_reconciliation': reconciliation,
                        'created_utc': datetime.now(timezone.utc).isoformat(),
                    }
                    try:
                        with retry_receipt.open('x', encoding='utf-8') as handle:
                            json.dump(retry_receipt_payload, handle, indent=2)
                    except FileExistsError:
                        raise RuntimeError('bounded fixed-input retry was already consumed')
                    archive = _archive_stale_lease(prior)
                    retry_receipt_payload['archived_attempt'] = str(archive.resolve())
                    retry_receipt.write_text(json.dumps(retry_receipt_payload, indent=2), encoding='utf-8')
                    if retry_count == 0:
                        state.setdefault('stage45_retry_counts', {})[retry_key] = 1
                    state['stage45_schema_retry_archive'] = str(archive.resolve())
                    state['stage45_schema_retry_reason'] = {
                        'cycle_id': CYCLE, 'timeframe': TF, 'run_number': RUN_NUMBER,
                        'reason': 'authenticated_fixed_input_surface_zero_start_retry',
                        'source_reconciliation': reconciliation,
                        'prepared_utc': datetime.now(timezone.utc).isoformat(),
                    }
                    state['current_stage'] = 'stage4_complete_held'
                    state['stage_status'] = 'stage4_complete_held'
                    state['next_action'] = 'run_stage45_next'
                    CYCLE_STATE.write_text(json.dumps(state, indent=2), encoding='utf-8')
                    return acquire_lease()
            except RuntimeError:
                raise
            except (OSError, ValueError, TypeError, KeyError):
                pass
            if fixed_input_retry_receipt_path().exists():
                raise RuntimeError(
                    'fixed-input retry receipt exists but lease transition did not finish; refusing another launch')
        if status in {'STARTING', 'OPTIMIZING'} and exact_pid and runtime_exists:
            try:
                runtime = json.loads(NARROW_RUNTIME.read_text(encoding='utf-8'))
                owned_pid = int(runtime.get('terminal_pid') or prior.get('terminal_pid') or 0)
                if owned_pid == exact_pid:
                    passes = _fresh_optimizer_passes(runtime, CYCLE, SYMBOL, TF)
                    if passes is not None:
                        runtime.update({
                            'status': 'TIMEFRAME_COMPLETE',
                            'passes': passes,
                            'lease_recovered_completion': True,
                            'lease_recovered_completion_utc': datetime.now(timezone.utc).isoformat(),
                        })
                        NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2), encoding='utf-8')
                        update_lease('TIMEFRAME_COMPLETE', terminal_pid=exact_pid,
                                     passes=passes, lease_recovered_completion=True)
                        print(f'[LEASE] recovered completed optimizer from fresh launch journal '
                              f'while lease was {status}: {passes} passes')
                        return 'SETTLE'
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                pass
        if status in {'STARTING', 'OPTIMIZING'} and exact_pid and runtime_exists:
            try:
                runtime = json.loads(NARROW_RUNTIME.read_text(encoding='utf-8'))
                binding = runtime.get('input_schema_binding') or {}
                retry_count = int(runtime.get('startup_recovery_retry_count') or 0)
                exact_identity = (
                    int(runtime.get('terminal_pid') or 0) == exact_pid
                    and runtime.get('cycle_id') == CYCLE
                    and runtime.get('ea') == EA
                    and runtime.get('timeframe') == TF
                    and runtime.get('startup_ini_sha256') == sha256(STARTUP_INI)
                    and runtime.get('narrow_ini_sha256') == sha256(NARROW_INI)
                )
                schema_wait = (binding.get('status') == 'INPUTS_BOUND'
                               and str(runtime.get('start_required_reason') or '').startswith(
                                   'waiting for exact Tester input schema:'))
                missing_start_marker = not _fresh_optimizer_start_seen(runtime)
                if exact_identity and (schema_wait or missing_start_marker) and retry_count < 2:
                    from kcos_stage3_launch_v2 import prepare_tester_schema_start
                    runtime['deployment_reconciliation'] = _authenticated_stage3_ex5_deployment()
                    runtime['startup_recovery_retry_count'] = retry_count + 1
                    runtime['startup_recovery_retry_utc'] = datetime.now(timezone.utc).isoformat()
                    runtime['startup_recovery_retry_trigger'] = (
                        'authenticated_live_starting_terminal' if status == 'STARTING'
                        else 'authenticated_live_terminal_without_tester_start_marker')
                    # Re-read the visible Tester controls instead of trusting
                    # a possibly stale hidden-window schema snapshot.
                    runtime.pop('input_schema_binding', None)
                    clicked = prepare_tester_schema_start(
                        runtime, NARROW_INI, handoff=authenticated_stage3_input_handoff())
                    runtime['startup_recovery_retry_start_control_invoked'] = bool(clicked)
                    NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2), encoding='utf-8')
                    update_lease('STARTING', terminal_pid=exact_pid,
                                 startup_recovery_retry_count=retry_count + 1,
                                 startup_recovery_retry_start_control_invoked=bool(clicked))
                    print('[LEASE] retried the exact Tester Start handoff once for the authenticated '
                          f'STARTING terminal (control_invoked={bool(clicked)}); awaiting journal evidence')
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                pass
        if status in {'OPTIMIZING', 'INFRASTRUCTURE_FAILURE'} and not exact_pid and runtime_exists:
            # A Tester validation error can leave its terminal alive while the
            # runner is later stopped or crashes. If that exact owned PID is
            # now gone, preserve the lease/runtime and retry immediately; the
            # normal ten-minute age threshold only applies when ownership is
            # ambiguous.
            try:
                runtime = json.loads(NARROW_RUNTIME.read_text(encoding='utf-8'))
                terminal_pid = int(runtime.get('terminal_pid') or prior.get('terminal_pid') or 0)
                if terminal_pid > 0:
                    import psutil
                    if not psutil.pid_exists(terminal_pid):
                        _archive_stale_lease(prior)
                        return acquire_lease()
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                pass
        if status == 'STARTING' and not exact_pid and lease_age > 120:
            _archive_stale_lease(prior)
            return acquire_lease()
        if status == 'OPTIMIZING' and not exact_pid and lease_age > 600:
            _archive_stale_lease(prior)
            return acquire_lease()
        if status in {'STARTING', 'OPTIMIZING', 'NARROW_OPT_COMPLETE'}:
            print(f'[LEASE] existing {status}; refusing duplicate launch')
            return False
        if status in {'TIMEFRAME_COMPLETE', 'TIMEOUT_SETTLING', 'RUN_COMPLETE'}:
            print(f'[LEASE] existing {status}; entering settlement')
            return 'SETTLE'
        raise RuntimeError(f'Stage 4.5 lease requires manual recovery: {status}')
    payload = {'schema_version': 1, 'cycle_id': CYCLE, 'timeframe': TF,
               'ea': EA, 'status': 'STARTING',
               'created_utc': datetime.now(timezone.utc).isoformat()}
    try:
        fd = os.open(LEASE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, 'w') as f:
            json.dump(payload, f, indent=2)
    except FileExistsError:
        return acquire_lease()
    return 'NEW'


def update_lease(status: str, **extra):
    if LEASE.exists():
        d = json.loads(LEASE.read_text())
        d.update({'status': status, **extra, 'updated_utc': datetime.now(timezone.utc).isoformat()})
        LEASE.write_text(json.dumps(d, indent=2))


def read_decision() -> dict:
    d = json.loads(STAGE4_DECISION.read_text())
    required = {
        'cycle_id': CYCLE, 'ea': EA, 'selected_tf': TF,
        'verdict': 'STAGE4_PLANNED',
    }
    for key, expected in required.items():
        if d.get(key) != expected:
            raise RuntimeError(f'Stage 4 decision mismatch: {key}={d.get(key)!r}, expected {expected!r}')
    if not WIDE_INI.exists():
        raise FileNotFoundError(WIDE_INI)
    if d.get('wide_ini_sha256', '').upper() != sha256(WIDE_INI):
        raise RuntimeError('Stage 4 wide INI SHA mismatch; refusing launch')
    stage4_verification = STATE / 'verification' / f'{CYCLE}.stage4.verification.json'
    if not stage4_verification.is_file():
        raise RuntimeError('Stage 4 verification missing; refusing Stage 4.5 launch')
    verification = json.loads(stage4_verification.read_text())
    if verification.get('cycle_id') != CYCLE or verification.get('stage') != 4 or verification.get('passed') is not True:
        raise RuntimeError('Stage 4 verification is not passed for the active cycle')
    if verification.get('decision_sha256') != sha256(STAGE4_DECISION):
        raise RuntimeError('Stage 4 verification decision hash mismatch; refusing launch')
    return d


def parse_wide_ini() -> dict[str, dict]:
    """Read [TesterInputs] section, parse InpName=default||start||step||stop||Y/N."""
    return read_ini_params(WIDE_INI)


def read_ini_params(path: Path) -> dict[str, dict]:
    """Read optimized TesterInputs from any authenticated stage INI."""
    text = path.read_text(encoding='utf-8-sig')
    params: dict[str, dict] = {}
    in_section = False
    for line in text.splitlines():
        s = line.strip()
        if s.startswith('['):
            in_section = s == '[TesterInputs]'
            continue
        if not in_section or '=' not in s:
            continue
        name, value = s.split('=', 1)
        if not name.startswith('Inp'):
            continue
        parts = value.split('||')
        if len(parts) < 5:
            continue
        try:
            params[name] = {
                'default': float(parts[0]),
                'start': float(parts[1]),
                'step': float(parts[2]),
                'stop': float(parts[3]),
                'optimize': parts[4].upper() == 'Y',
            }
        except ValueError:
            continue
    return {name: spec for name, spec in params.items() if spec['optimize']}


def build_narrow_ini(wide: dict, decision: dict) -> Path:
    """Materialize the authenticated Stage 4 range plan into a new INI."""
    # Runs 2 and 3 are chained from the immediately preceding authenticated
    # narrow grid.  This prevents a retry from silently jumping back to wide
    # bounds or mixing a different cycle's range plan.
    prior_ini = STATE / 'stage3_v2' / f'{CYCLE}.{EA}.{SYMBOL}.{TF}.{FROM_DATE.replace(".","")}_{TO_DATE.replace(".","")}.run{RUN_NUMBER - 1}.narrow.ini'
    source_ini = prior_ini if RUN_NUMBER > 1 and prior_ini.is_file() else WIDE_INI
    text = source_ini.read_text(encoding='utf-8-sig')
    input_actions = decision.get('input_actions') or {}
    optimized_names = set(read_ini_params(source_ini))
    if set(input_actions) != optimized_names:
        raise RuntimeError('Stage 4.5 range plan does not cover every optimized input')
    compact_ea = re.sub(r'[^a-z0-9]', '', EA.casefold())
    gjr_base_lot = 'gjr' in compact_ea and 'garch' in compact_ea
    new_lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith('Inp') and '=' in stripped:
            name, value = stripped.split('=', 1)
            if re.search(r'(time[_ ]?stop|cool[_ ]?down|cooldown|max.?trade.?bars|min.?bars.?between.?trades)', name, re.I):
                timing_parts = value.split('||')
                if len(timing_parts) >= 5:
                    value = '0||0||0||0||N'
                else:
                    value = '0'
                prefix = line[:len(line) - len(line.lstrip())]
                new_lines.append(prefix + name + '=' + value)
                continue
            parts = value.split('||')
            action = input_actions.get(name, {})
            if gjr_base_lot and name == 'InpBaseLot' and RUN_NUMBER == 1:
                # The first Stage 4.5 run confirms the persistent Stage 3 ETH
                # grid. Higher sizes require margin-qualified Stage 4.5 results.
                action = {**action, 'next_start': 0.01, 'next_step': 0.01,
                          'next_stop': 0.20}
            if len(parts) >= 5 and parts[-1].strip().upper() == 'Y' and action:
                try:
                    start = action.get('next_start', action.get('start', float(parts[1])))
                    step = action.get('next_step', action.get('step', float(parts[2])))
                    stop = action.get('next_stop', action.get('stop', float(parts[3])))
                    if RUN_NUMBER > 1:
                        action_name = str(action.get('action', '')).lower()
                        if action_name not in {'narrow', 'shift', 'keep_broad', 'hold_domain_limit'}:
                            raise RuntimeError(
                                f'run {RUN_NUMBER} has no explicit refinement plan for {name}'
                            )
                        prior_start, prior_step, prior_stop = map(float, parts[1:4])
                        bounds_refined = (float(start) >= prior_start and
                                          float(stop) <= prior_stop and
                                          (float(start) > prior_start or float(stop) < prior_stop))
                        shifted = (action_name == 'shift' and
                                   (float(start) != prior_start or float(stop) != prior_stop))
                        step_refined = float(step) < prior_step
                        unchanged_broad = (
                            action_name in {'keep_broad', 'hold_domain_limit'} and
                            float(start) == prior_start and float(stop) == prior_stop and
                            float(step) <= prior_step
                        )
                        cluster_min = float(action.get('cluster_min', float('nan')))
                        cluster_max = float(action.get('cluster_max', float('nan')))
                        intervals = (float(stop) - float(start)) / float(step)
                        clean_grid_compatible = (
                            action.get('step_policy') == 'two_significant_digits' and
                            math.isfinite(cluster_min) and math.isfinite(cluster_max) and
                            float(start) <= cluster_min + 1e-9 and
                            float(stop) >= cluster_max - 1e-9 and
                            float(stop) - float(start) <= prior_stop - prior_start + 2 * prior_step + 1e-9 and
                            (int(action.get('minimum_mt5_steps', 15)) - 1e-9 <= intervals + 1 <= 20 + 1e-9 if action.get('step_policy') == 'two_significant_digits' else int(action.get('minimum_intervals', 15)) - 1e-9 <= intervals <= 20 + 1e-9)
                        )
                        # Integer inputs cannot use a fractional MT5 step. When
                        # the prior grid is outside the valid 15-20-value band,
                        # allow only the minimum expansion needed to enter it,
                        # while keeping the derived grid connected to the prior
                        # range and covering the selected result cluster.
                        # A hard source domain remains binding; otherwise use the
                        # input's semantic movement/safety bounds, not Stage 1's
                        # non-authoritative search estimate.
                        integer_types = {'int', 'integer', 'long', 'ulong', 'uint', 'short', 'ushort', 'byte', 'uchar', 'sbyte'}
                        integer_minimum_grid_expansion = False
                        if (action_name in {'narrow', 'shift', 'keep_broad'} and
                                str(action.get('input_type', '')).casefold() in integer_types and
                                action.get('step_policy') == 'two_significant_digits' and
                                prior_step > 0 and float(step) >= prior_step - 1e-9):
                            prior_intervals = (prior_stop - prior_start) / prior_step
                            prior_values = int(round(prior_intervals)) + 1
                            minimum_values = max(15, int(action.get('minimum_mt5_steps', 15)))
                            source_domain = action.get('source_metadata') or {}
                            hard_domain = (
                                (float(source_domain['min']), float(source_domain['max']))
                                if source_domain.get('basis') == 'source_declared_domain' and
                                source_domain.get('hard_domain') is True and
                                {'min', 'max'}.issubset(source_domain) else None
                            )
                            movement_min, movement_max = input_movement_bounds(
                                name, prior_start, prior_stop, domain=hard_domain)
                            movement_bounds_ok = (
                                float(start) >= movement_min - 1e-9 and
                                float(stop) <= movement_max + 1e-9
                            )
                            expansion = float(stop) - float(start) - (prior_stop - prior_start)
                            minimum_expansion = max(
                                0.0, (minimum_values - 1) * float(step) - (prior_stop - prior_start))
                            integer_minimum_grid_expansion = (
                                movement_bounds_ok and
                                (prior_values > 20 or prior_values < minimum_values) and
                                math.isclose(prior_intervals, round(prior_intervals), abs_tol=1e-8) and
                                math.isclose(intervals, round(intervals), abs_tol=1e-8) and
                                # A data-derived cluster can move one bound inward
                                # while the opposite bound expands just enough to
                                # meet MT5's minimum integer grid. Require overlap
                                # with the prior grid, selected-cluster coverage,
                                # and the minimum expansion budget below.
                                float(start) <= prior_stop + 1e-9 and
                                float(stop) >= prior_start - 1e-9 and
                                expansion > 1e-9 and
                                expansion <= minimum_expansion + 1e-9 and
                                minimum_values <= intervals + 1 <= 20 and
                                math.isfinite(cluster_min) and math.isfinite(cluster_max) and
                                float(start) <= cluster_min + 1e-9 and
                                float(stop) >= cluster_max - 1e-9
                            )
                        if not (bounds_refined or shifted or step_refined or unchanged_broad or clean_grid_compatible or integer_minimum_grid_expansion):
                            raise RuntimeError(
                                f'run {RUN_NUMBER} plan for {name} does not refine or move its prior INI range'
                            )
                    if float(start) < float(stop) and float(step) > 0:
                        parts[0] = format_plain_numeric_text(parts[0])
                        if action.get('step_policy') == 'two_significant_digits':
                            # Keep the planned endpoints exact. Rounding them to
                            # the increment's display precision can move the
                            # actual grid off the authenticated plan (and make
                            # the strict verifier reject an otherwise valid
                            # clean-step grid).
                            rendered_grid = [
                                format_plain_numeric_text(str(start)),
                                format_clean_grid_value(step, step),
                                format_plain_numeric_text(str(stop)),
                            ]
                        else:
                            # Stage 4 emits exact legacy refinement decimals.
                            # Preserve them on run 1; rounding them to the
                            # later Stage 4.5 clean-step policy changes the
                            # authenticated range and correctly fails validation.
                            rendered_grid = [format_plain_numeric_text(str(value))
                                             for value in (start, step, stop)]
                        parts[1:4] = rendered_grid
                        prefix = line[:len(line) - len(line.lstrip())]
                        new_lines.append(prefix + name + '=' + '||'.join(parts))
                        continue
                except (TypeError, ValueError):
                    raise RuntimeError(f'invalid Stage 4 range plan for {name}')
        if 'wide opt' in line.lower() or 'wide scan' in line.lower():
            new_lines.append(line.replace('wide opt', 'narrow opt').replace('wide scan', 'narrow run'))
        else:
            new_lines.append(line)
    NARROW_INI.write_text('\n'.join(new_lines), encoding='utf-8')
    for line in new_lines:
        stripped = line.strip()
        if '=' not in stripped:
            continue
        name, value = stripped.split('=', 1)
        if re.search(r'(time[_ ]?stop|cool[_ ]?down|cooldown|max.?trade.?bars|min.?bars.?between.?trades)', name, re.I):
            timing_parts = value.split('||')
            try:
                default_value = float(timing_parts[0])
            except (TypeError, ValueError):
                raise RuntimeError(f'{name} is not a numeric zero-valued timing control')
            if default_value != 0 or (len(timing_parts) >= 5 and timing_parts[-1].strip().upper() == 'Y'):
                raise RuntimeError(f'{name} must be zero and excluded from optimization')
    failures = validate_input_range_plan(input_actions, read_ini_params(NARROW_INI))
    if failures:
        raise RuntimeError('Stage 4.5 emitted range plan failed verification: ' + '; '.join(failures))
    return NARROW_INI


def derive_followup_range_plan(previous_csv: Path, prior_ini: Path, base_decision: dict) -> dict:
    """Recompute the next Stage 4.5 ranges from the prior run's winners."""
    params = read_ini_params(prior_ini)
    audited_inputs = ((base_decision.get('input_surface_audit') or {}).get('optimized_inputs') or {})
    source_runtime_bounds = {
        name: item.get('source_runtime_bounds')
        for name, item in audited_inputs.items()
        if isinstance(item, dict) and isinstance(item.get('source_runtime_bounds'), dict)
    }
    start_date = datetime.strptime(FROM_DATE, '%Y.%m.%d')
    end_date = datetime.strptime(TO_DATE, '%Y.%m.%d')
    months = max((end_date - start_date).days / (365.2425 / 12.0), 1.0)
    compact_ea = re.sub(r'[^a-z0-9]', '', EA.casefold())
    is_gjr_garch = 'gjr' in compact_ea and 'garch' in compact_ea
    records = []
    with previous_csv.open(newline='', encoding='utf-8-sig') as handle:
        for raw in csv.DictReader(handle):
            try:
                profit = float(raw['Profit'])
                fitness = float(raw['CustomFitness'])
                trades = float(raw['Trades'])
                drawdown = float(raw['EquityDrawdownPercent'])
                margin = float(raw['MarginLevel'])
                row = {
                    name: float(raw[name]) for name in params
                }
            except (KeyError, TypeError, ValueError):
                continue
            regime_inputs_valid = all(
                0.0 <= float(row[name]) <= 5.0
                for name in params if name.startswith('InpRegime_'))
            source_inputs_valid = all(
                (bounds.get('min') is None or float(row[name]) >= float(bounds['min']) - 1e-9) and
                (bounds.get('max') is None or float(row[name]) <= float(bounds['max']) + 1e-9)
                for name, bounds in source_runtime_bounds.items() if name in row)
            row.update({
                'minato_score': fitness,
                'min_margin_pct': margin,
                'regime_domain_valid': regime_inputs_valid and source_inputs_valid,
                'source_input_constraints_valid': source_inputs_valid,
                'valid': (profit > 0 and fitness >= 15 and
                          trades / months >= 35 and drawdown <= 35 and
                          margin >= 120 and regime_inputs_valid and source_inputs_valid),
            })
            records.append(row)
    valid_count = sum(1 for row in records if row.get('valid') is not False)
    if valid_count >= 2:
        selection = select_top_records(records, params.keys(), limit=min(50, valid_count))
        selection_mode = 'stage3_gate_qualified'
    else:
        # A completed exploratory run with no gate-qualified rows still needs
        # to inform the next of the three cluster-search passes. Rank its finite
        # results by raw Minato score, retaining cutoff ties; Stage 3 gates stay
        # authoritative for the final Stage 4.5 verdict.
        if len(records) < 2:
            raise RuntimeError(f'not enough parseable Stage 4.5 records in {previous_csv}')
        eligible = [row for row in records if row.get('regime_domain_valid', True)]
        if len(eligible) < 2:
            raise RuntimeError('fewer than two parseable results remain inside the regime multiplier 0..5 domain')
        ranked = sorted(enumerate(eligible), key=lambda item: (-item[1]['minato_score'], item[0]))
        limit = min(50, len(ranked))
        cutoff = ranked[limit - 1][1]['minato_score']
        selected = [row for _, row in ranked if row['minato_score'] >= cutoff]
        selection = {
            'selected_record_count': len(selected),
            'valid_record_count': len(records),
            'input_clusters': {
                name: [row[name] for row in selected] for name in params
            },
        }
        selection_mode = 'exploratory_raw_minato_no_stage3_gates'
    actions = {}
    for name, spec in params.items():
        prior_action = (base_decision.get('input_actions') or {}).get(name, {})
        gjr_base_lot = is_gjr_garch and name == 'InpBaseLot'
        source_metadata = prior_action.get('source_metadata')
        domain = None
        if (isinstance(source_metadata, dict) and
                source_metadata.get('basis') == 'source_declared_domain' and
                source_metadata.get('hard_domain') is True and
                {'min', 'max'}.issubset(source_metadata)):
            domain = (float(source_metadata['min']), float(source_metadata['max']))
        runtime_bounds = source_runtime_bounds.get(name)
        cluster = selection['input_clusters'][name]
        movement_bounds = input_movement_bounds(
            name, spec['start'], spec['stop'], domain=domain,
            safety_bounds=runtime_bounds)
        margin_qualified_expansion = False
        if gjr_base_lot:
            margin_rows = [row for row in records
                           if row.get('regime_domain_valid', True)
                           and float(row.get('min_margin_pct', 0.0)) >= 120.0]
            if len(margin_rows) >= 2:
                ranked_margin = sorted(
                    enumerate(margin_rows),
                    key=lambda item: (-item[1]['minato_score'], item[0]))
                margin_limit = min(50, len(ranked_margin))
                margin_cutoff = ranked_margin[margin_limit - 1][1]['minato_score']
                margin_selected = [row for _, row in ranked_margin
                                   if row['minato_score'] >= margin_cutoff]
                cluster = [row[name] for row in margin_selected]
                margin_qualified_expansion = True
            else:
                # Without at least two observations above the 120% margin
                # floor, Stage 4.5 may refine the current range but not raise
                # its upper edge.
                movement_bounds = (max(0.01, movement_bounds[0]),
                                   min(float(spec['stop']), movement_bounds[1]))
        action = evaluate_refinement_range(
            start=spec['start'], stop=spec['stop'], step=spec['step'],
            cluster=cluster,
            centered_streak=0, broad_run=True,
            allow_first_pass_narrow=True,
            movement_bounds=movement_bounds,
        )
        if gjr_base_lot:
            action['margin_qualified_upward_expansion'] = margin_qualified_expansion
            movement_bounds = (max(0.01, movement_bounds[0]), movement_bounds[1])
        if isinstance(source_metadata, dict):
            action['source_metadata'] = source_metadata
        if gjr_base_lot:
            # Stage 2's 0.20 value is the Stage 3 ceiling, not a permanent
            # Stage 4.5 hard limit. Keep the current ceiling hard until there
            # are margin-qualified observations to justify expansion.
            lot_metadata = dict(source_metadata or {})
            lot_metadata.update({
                'min': 0.01,
                'hard_domain': not margin_qualified_expansion,
                'basis': 'stage2_derived_policy',
                'margin_qualified_upward_expansion': margin_qualified_expansion,
            })
            if not margin_qualified_expansion:
                lot_metadata['max'] = float(spec['stop'])
            action['source_metadata'] = lot_metadata
        input_type = str(prior_action.get('input_type', 'double'))
        hard_domain = (domain if isinstance(source_metadata, dict) and
                       source_metadata.get('basis') == 'source_declared_domain' else None)
        if runtime_bounds:
            hard_domain = movement_bounds
        if name.startswith('InpRegime_'):
            hard_domain = (0.0, 5.0)
        width = float(spec['stop']) - float(spec['start'])
        cluster_span = max(cluster) - min(cluster)
        strong = width > 0 and cluster_span <= width * 0.20 + 1e-12
        # A compact cluster touching a hard-domain edge cannot be recentered
        # inside the required middle 60% without excluding selected results.
        # Keep its broad/domain-limited action; otherwise the clean-grid fitter
        # has no feasible narrow range and aborts the whole Stage 4.5 handoff.
        cluster_at_hard_boundary = (
            hard_domain is not None and
            (min(cluster) <= hard_domain[0] + 1e-12 or
             max(cluster) >= hard_domain[1] - 1e-12)
        )
        if cluster_at_hard_boundary and action['action'] == 'narrow':
            # The requested middle-60% placement cannot contain a cluster
            # touching a hard input boundary. Keep the authenticated broad
            # interval so boundary-valid results remain eligible next pass.
            action.update({
                'action': 'keep_broad',
                'start': spec['start'],
                'stop': spec['stop'],
                'reason': 'retain broad range because the selected cluster touches a hard input boundary',
            })
        if (strong and not cluster_at_hard_boundary and
                action['action'] in {'keep_broad', 'hold_domain_limit'}):
            action.update({'action': 'narrow', 'cluster_position': 'converged',
                           'reason': 'strong convergence: qualifying cluster spans at most 20% of the current range'})
        if (name.startswith('InpRegime_') and not cluster_at_hard_boundary and
                (spec['start'] < 0 or spec['stop'] > 5)):
            action.update({'action': 'narrow', 'start': spec['start'], 'stop': spec['stop'],
                           'reason': 'repair regime multiplier range to the required 0..5 domain'})
        fit_action = action['action']
        if (fit_action == 'narrow' and
                ('recenter edge cluster' in str(action.get('reason', '')).lower() or
                 'strong convergence' in str(action.get('reason', '')).lower())):
            # If the inclusive MT5 minimum makes the narrow window too small,
            # the fitter falls back to a wider grid. Preserve the recentering
            # intent so that fallback is placed around the cluster and still
            # satisfies the middle-60% guard.
            fit_action = 'shift'
        fit_domain_min, fit_domain_max = (
            hard_domain if hard_domain is not None else movement_bounds)
        if (name.startswith('InpRegime_') and
                not (isinstance(source_metadata, dict) and
                     source_metadata.get('basis') == 'source_declared_domain')):
            # Regime estimates remain soft upper bounds; preserve only their
            # semantic nonnegative floor during follow-up refinement.
            fit_domain_max = None
        try:
            refinement_step = refined_step(spec['step'], input_type=input_type)
            if gjr_base_lot:
                refinement_step = max(0.01, refinement_step)
            fitted = fit_refinement_grid(
                start=action['start'], stop=action['stop'],
                step=refinement_step,
                cluster=cluster, input_type=input_type,
                action=fit_action,
                domain_min=max(0.01, fit_domain_min) if gjr_base_lot else fit_domain_min,
                domain_max=fit_domain_max,
                clean_grid=True, allow_sub_15=strong,
            )
        except ValueError as exc:
            raise ValueError(
                f'{name}: {exc}; action={fit_action}, '
                f'range={action["start"]}..{action["stop"]} step={spec["step"]}, '
                f'cluster={min(cluster)}..{max(cluster)} count={len(cluster)}, '
                f'domain={fit_domain_min}..{fit_domain_max}, input_type={input_type}, '
                f'strong={strong}'
            ) from exc
        action.update(fitted)
        action['strong_convergence'] = strong
        action['minimum_mt5_steps'] = 8 if strong else 15
        action['step_policy'] = 'two_significant_digits'
        action['narrow_runs_completed'] = int(base_decision.get('run_number', RUN_NUMBER))
        action['next_start'] = fitted['start']
        action['next_stop'] = fitted['stop']
        action['next_step'] = fitted['step']
        action['input_type'] = input_type
        action['source_runtime_bounds'] = runtime_bounds
        action['cluster_min'] = min(cluster)
        action['cluster_max'] = max(cluster)
        action['cluster_count'] = len(cluster)
        actions[name] = action
    plan = dict(base_decision)
    plan.update({
        'input_actions': actions,
        'convergence_mode': 'stage45_result_cluster',
        'derived_from_run': RUN_NUMBER - 1,
        'derived_from_csv': str(previous_csv.resolve()),
        'derived_from_csv_sha256': sha256(previous_csv),
        'selected_record_count': selection['selected_record_count'],
        'valid_record_count': selection['valid_record_count'],
        'cluster_selection_mode': selection_mode,
    })
    plan_path = STAGE45_DIR / f'{CYCLE}.stage45.run{RUN_NUMBER}.range_plan.json'
    plan_path.write_text(json.dumps(plan, indent=2))
    print(f'[RANGE PLAN] derived from {previous_csv.name}: {plan_path}')
    return plan


def altered_input_names(source_ini: Path, narrow_ini: Path) -> list[str]:
    """Return optimized inputs whose start/step/stop changed in this run."""
    def values(path: Path) -> dict[str, list[str]]:
        out = {}
        for line in path.read_text(encoding='utf-8-sig').splitlines():
            if '=' not in line or not line.strip().startswith('Inp'):
                continue
            name, value = line.strip().split('=', 1)
            parts = value.split('||')
            if len(parts) >= 5 and parts[-1].strip().upper() == 'Y':
                out[name] = parts[1:4]
        return out
    before, after = values(source_ini), values(narrow_ini)
    changed = sorted(name for name in set(before) & set(after) if before[name] != after[name])
    if not changed:
        raise RuntimeError('Stage 4.5 narrow inputs are unchanged; refusing to parse or advance')
    return changed


def write_runtime():
    journal_name = f'{datetime.now().strftime("%Y%m%d")}.log'
    log_candidates = [root / 'Tester' / 'logs' / journal_name
                      for root in tester_roots()
                      if (root / 'Tester' / 'logs' / journal_name).is_file()]
    journal_file = max(log_candidates, key=lambda p: p.stat().st_mtime) if log_candidates \
        else MT5_LOG / journal_name
    started_utc = datetime.now(timezone.utc).isoformat()
    rt = {
        'schema_version': 2,
        'cycle_id': CYCLE,
        'stage': '4.5',
        'ea': EA,
        'timeframe': TF,
        'run_number': RUN_NUMBER,
        'run_count_required': 3,
        'symbol': SYMBOL,
        'from_date': FROM_DATE,
        'to_date': TO_DATE,
        'narrow_ini': str(NARROW_INI.resolve()),
        'narrow_ini_sha256': sha256(NARROW_INI),
        'started_utc': started_utc,
        'status': 'STARTING',
        'terminal_pid': None,
        'worker_pids': [],
        'journal_path': str(journal_file.resolve()),
        'journal_start_offset': journal_file.stat().st_size if journal_file.is_file() else 0,
        'opt_baseline_sha256': opt_baseline(),
        'screenshot_path': str(RUN_SCREENSHOT.resolve()),
    }
    NARROW_RUNTIME.write_text(json.dumps(rt, indent=2))
    return rt


def capture_run_screenshot() -> str:
    """Persist a non-invasive desktop screenshot when Pillow is available.

    Screenshots are advisory evidence; missing Pillow must never abort a tester
    launch or leave a live runtime lease half-initialized.
    """
    try:
        from PIL import ImageGrab
        image = ImageGrab.grab()
        image.save(RUN_SCREENSHOT, format='PNG')
        return str(RUN_SCREENSHOT.resolve())
    except Exception as exc:
        print(f"[SCREENSHOT] skipped: {exc}", file=sys.stderr)
        return ''


def find_completion_line(timeout_seconds: float = 14400.0, start_offset: int = 0,
                         terminal_pid: int | None = None) -> tuple[bool, int | None]:
    """Poll journal for narrow completion. Default 4h timeout."""
    deadline = time.time() + timeout_seconds
    last_passes = 0
    while time.time() < deadline:
        if terminal_pid is not None:
            try:
                import psutil
                if not psutil.pid_exists(terminal_pid):
                    return False, last_passes or None
            except ImportError:
                pass
        runtime_data = json.loads(NARROW_RUNTIME.read_text()) if NARROW_RUNTIME.is_file() else {}
        configured_log = Path(runtime_data.get('journal_path', ''))
        log_files = [configured_log] if configured_log.is_file() else [
            root / 'Tester' / 'logs' / f'{datetime.now().strftime("%Y%m%d")}.log'
            for root in tester_roots()]
        for log_file in log_files:
            if not log_file.exists():
                continue
            raw = log_file.read_bytes()
            baseline = int(runtime_data.get('journal_start_offset', start_offset))
            text = raw[baseline:].decode('utf-16le', errors='ignore')
            rejected = re.search(r'no optimized parameter selected', text, re.I)
            if rejected:
                raise TesterInputRejected(
                    f'MT5 rejected the Stage 4.5 optimization inputs in {log_file}: '
                    f'{rejected.group(0)}')
            # Look for the latest completion belonging to this exact symbol and
            # timeframe.  A hard-coded M15 check could settle the wrong run.
            finishes = []
            for m in re.finditer(r'genetic optimization finished on pass (\d+)', text):
                finishes.append(int(m.group(1)))
            context_pattern = (rf'.*?{re.escape(SYMBOL)},{re.escape(TF)}.*?\n'
                               rf'.*?genetic optimization finished on pass (\d+)')
            for m in re.finditer(context_pattern, text):
                last_passes = int(m.group(1))
            if finishes:
                # use most recent finish
                latest = finishes[-1]
                if 'optimization done in ' in text and terminal_pid:
                    # MT5 may keep terminal64.exe open after the tester finishes.
                    # The authenticated finish+duration pair is the completion
                    # signal; ownership and exact artifact identity are checked
                    # before any parse or state advancement.
                    try:
                        import psutil
                        return True, latest
                    except ImportError:
                        pass
                    return True, latest
        time.sleep(10)
    return False, last_passes if last_passes else None


def stop_owned_mt5(pid: int | None) -> bool:
    """Stop only the terminal process this run launched after the hard limit."""
    if not pid:
        return False
    try:
        import psutil
        proc = psutil.Process(int(pid))
        name = (proc.name() or '').lower()
        if name != 'terminal64.exe':
            print(f'[STOP] refusing pid {pid}: process is {name!r}')
            return False
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except psutil.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=30)
        print(f'[STOP] terminated owned terminal pid {pid}')
        return True
    except ImportError:
        print('[STOP] psutil is unavailable; ownership cannot be verified')
        return False
    except psutil.NoSuchProcess:
        return True
    except (psutil.AccessDenied, ValueError, OSError) as exc:
        print(f'[STOP] unable to terminate owned pid {pid}: {exc}')
        return False


def auto_click_strategy_tester_start(terminal_pid: int) -> bool:
    """Invoke the exact visible MT5 Strategy Tester Start control."""
    if os.name != "nt":
        return False
    u = ctypes.windll.user32
    tops = []
    @ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
    def top_cb(hwnd, _):
        owner = wintypes.DWORD()
        u.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
        if owner.value == int(terminal_pid) and u.IsWindowVisible(hwnd):
            tops.append(hwnd)
        return True
    u.EnumWindows(top_cb, 0)
    for parent in tops:
        buttons = []
        @ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
        def child_cb(hwnd, _):
            if u.IsWindowVisible(hwnd):
                text = ctypes.create_unicode_buffer(64)
                u.GetWindowTextW(hwnd, text, 64)
                if text.value.strip().lower() == "start":
                    buttons.append(hwnd)
            return True
        u.EnumChildWindows(parent, child_cb, 0)
        if buttons:
            u.SendMessageW(buttons[0], 0x00F5, 0, 0)  # BM_CLICK
            return True
    return False


def tester_identity_matches(terminal_pid: int, ea: str, symbol: str, timeframe: str) -> bool:
    """Require the tester pane to identify the exact launched workload."""
    if os.name != "nt":
        return False
    u = ctypes.windll.user32
    tops = []
    CB = ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
    @CB
    def top_cb(hwnd, _):
        owner = wintypes.DWORD()
        u.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
        if owner.value == int(terminal_pid) and u.IsWindowVisible(hwnd):
            tops.append(hwnd)
        return True
    u.EnumWindows(top_cb, 0)
    needle = f"{ea}, {symbol} {timeframe}".lower()
    for parent in tops:
        texts = []
        @CB
        def child_cb(hwnd, _):
            text = ctypes.create_unicode_buffer(256)
            u.GetWindowTextW(hwnd, text, 256)
            if text.value.strip():
                texts.append(text.value.strip().lower())
            return True
        u.EnumChildWindows(parent, child_cb, 0)
        if any(needle in text for text in texts):
            return True
    return False


def launch_mt5(config_ini: Path = STARTUP_INI) -> int:
    """Launch MT5 with the authenticated Tester config and return its exact PID."""
    import os
    import time
    import psutil
    if not MT5_PATH.exists():
        raise FileNotFoundError(MT5_PATH)
    config_token = str(config_ini.resolve()).lower()
    config_name = config_ini.name.lower()
    cycle_token = CYCLE.lower()
    cmd = [str(MT5_PATH), '/portable', f'/config:{config_ini.resolve()}']
    # MT5 is single-instance. Retire only this cycle's prior exact terminal
    # before starting the next sequential run; never touch protected/unowned PIDs.
    try:
        import psutil
        for old in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                old_cmd = ' '.join(old.info.get('cmdline') or []).lower()
                if (old.info.get('name') or '').lower() == 'terminal64.exe' and cycle_token in old_cmd and config_name not in old_cmd:
                    old.terminate()
                    try:
                        old.wait(timeout=10)
                    except psutil.TimeoutExpired:
                        old.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
    except ImportError:
        pass
    creationflags = 0
    if os.name == 'nt':
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, creationflags=creationflags)
    # MT5 may replace the detached bootstrap PID with a new terminal64 PID.
    # Resolve the real process by its exact /config argument before UI checks.
    deadline = time.time() + 120
    while time.time() < deadline:
        for candidate in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                name = (candidate.info.get('name') or '').lower()
                cmdline = ' '.join(candidate.info.get('cmdline') or []).lower()
                if name == 'terminal64.exe' and config_token in cmdline:
                    candidate_pid = int(candidate.info['pid'])
                    time.sleep(2)
                    try:
                        stable = psutil.Process(candidate_pid)
                        stable_cmd = ' '.join(stable.cmdline()).lower()
                        if stable.is_running() and config_token in stable_cmd:
                            return candidate_pid
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        time.sleep(0.5)
    raise RuntimeError('MT5 did not expose a persistent terminal for the exact /config path')


def find_new_opt(timeout_seconds: float = 180.0) -> Path | None:
    """Wait only for a matching .opt created/changed by this exact run."""
    try:
        runtime = json.loads(NARROW_RUNTIME.read_text(encoding='utf-8'))
        started_mtime = datetime.fromisoformat(runtime['started_utc']).timestamp()
        baseline = {str(k).casefold(): str(v).upper()
                    for k, v in (runtime.get('opt_baseline_sha256') or {}).items()}
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, OSError):
        return None
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        for candidate in matching_opt_files():
            try:
                resolved = str(candidate.resolve()).casefold()
                if candidate.stat().st_size <= 0 or candidate.stat().st_mtime < started_mtime:
                    continue
                current_hash = sha256(candidate)
                if baseline.get(resolved) == current_hash:
                    continue
                return candidate
            except OSError:
                continue
        time.sleep(5)
    return None


def wait_for_stable_opt(opt_path: Path, timeout_seconds: float = 180.0,
                        stable_seconds: float = 5.0) -> str | None:
    """Return the hash only after a timed-out run's fresh .opt stops changing."""
    deadline = time.time() + timeout_seconds
    prior = None
    while time.time() < deadline:
        try:
            stat = opt_path.stat()
            current = (stat.st_size, stat.st_mtime_ns, sha256(opt_path))
        except OSError:
            prior = None
            time.sleep(stable_seconds)
            continue
        if stat.st_size > 0 and current == prior:
            return current[2]
        prior = current
        time.sleep(stable_seconds)
    return None


def parse_narrow_opt(opt_path: Path) -> Path:
    """Run parse_mt5_opt_v2.py to convert .opt to CSV."""
    cmd = [sys.executable, 'state/parse_mt5_opt_v2.py', str(opt_path),
           str(NARROW_CSV), str(NARROW_INI)]
    print(f'[PARSE] running: {" ".join(cmd)}')
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    print(result.stdout)
    if result.returncode != 0:
        print(f'[PARSE] stderr: {result.stderr[:500]}')
    if result.returncode != 0 or not NARROW_CSV.exists():
        raise RuntimeError(f'parser failed rc={result.returncode}: {result.stderr[:500]}')
    return NARROW_CSV


def count_parsed_opt_passes(csv_path: Path) -> int:
    """Count distinct MT5 Pass IDs in a parsed, run-authenticated .opt CSV."""
    import csv as _csv
    seen = set()
    with csv_path.open(newline='', encoding='utf-8-sig') as handle:
        reader = _csv.DictReader(handle)
        if 'Pass' not in (reader.fieldnames or []):
            raise RuntimeError('parsed Stage 4.5 CSV has no Pass column')
        for row in reader:
            try:
                value = int(row['Pass'])
            except (KeyError, TypeError, ValueError):
                continue
            if value >= 0:
                seen.add(value)
    return len(seen)


def record_execution_failure(runtime: dict, terminal_pid: int | None, kind: str, *, retry_safe: bool = True) -> int:
    """Record a failed run and allow at most one safe automatic retry per refinement."""
    state = json.loads(CYCLE_STATE.read_text(encoding='utf-8'))
    retry_counts = state.setdefault('stage45_retry_counts', {})
    key = f'{TF}:run{RUN_NUMBER}'
    retry_receipt = fixed_input_retry_receipt_path()
    consumed_fixed_input_retry = retry_receipt.is_file()
    attempts = max(int(retry_counts.get(key, 0)), int(consumed_fixed_input_retry)) + 1
    retry_counts[key] = attempts
    retry_allowed = attempts < 2 and retry_safe
    runtime.update({'status': 'INFRASTRUCTURE_FAILURE',
                    'failure_kind': kind,
                    'failure_attempt': attempts,
                    'retry_allowed': retry_allowed})
    NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
    if retry_allowed:
        state['stage_status'] = 'stage4_complete_held'
        state['current_stage'] = 'stage4_complete_held'
        state['next_action'] = 'run_stage45_next'
        update_lease('INFRASTRUCTURE_FAILURE', terminal_pid=terminal_pid,
                     failure_kind=kind, failure_attempt=attempts)
    else:
        state['stage_status'] = 'narrow_opt_failed'
        state['current_stage'] = 'stage4_complete_held'
        state['next_action'] = 'hold_for_review'
        state['halt_reason'] = f'Stage 4.5 run {RUN_NUMBER} failed twice ({kind}); held without accepting cached results.'
        update_lease('FAILED_HELD', terminal_pid=terminal_pid,
                     failure_kind=kind, failure_attempt=attempts)
    CYCLE_STATE.write_text(json.dumps(state, indent=2))
    failure_path = STATE / 'diagnostics' / f'{CYCLE}.stage45.run{RUN_NUMBER}.execution_failure.json'
    failure_path.parent.mkdir(parents=True, exist_ok=True)
    failure_path.write_text(json.dumps({
        'schema_version': 1, 'cycle_id': CYCLE, 'stage': '4.5',
        'timeframe': TF, 'run_number': RUN_NUMBER, 'failure_kind': kind,
        'failure_attempt': attempts, 'retry_allowed': retry_allowed,
        'runtime': str(NARROW_RUNTIME.resolve()),
        'created_utc': datetime.now(timezone.utc).isoformat(),
    }, indent=2))
    print(f'[HOLD] {kind}; attempt {attempts}/2; retry_allowed={retry_allowed}')
    return attempts


def write_complete_event(runtime: dict, passes: int, opt_hash: str) -> Path:
    evt = {
        'schema_version': 2,
        'stage': '4.5',
        'cycle_id': CYCLE,
        'timeframe': TF,
        'status': runtime.get('status') if runtime.get('status') in {'TIMEFRAME_COMPLETE', 'TIMEOUT_SETTLING'} else 'INFRASTRUCTURE_FAILURE',
        'consumed': False,
        'narrow_ini': str(NARROW_INI.resolve()),
        'narrow_ini_sha256': runtime['narrow_ini_sha256'],
        'opt_sha256': opt_hash,  # authenticated before publication
        'passes': passes,
        'passes_source': runtime.get('passes_source', 'tester_completion_marker'),
        'parsed_opt_pass_records': runtime.get('parsed_opt_pass_records'),
        'timed_out': bool(runtime.get('timed_out')),
        'completed_utc': datetime.now(timezone.utc).isoformat(),
    }
    NARROW_COMPLETE.write_text(json.dumps(evt, indent=2))
    return NARROW_COMPLETE


def assess_minato_growth_potential(current_best_score: float | None) -> dict:
    """Record whether rising Stage 4.5 scores plus a shifted input suggest more headroom."""
    run_decisions = {}
    evidence = {}
    for run_number in (1, 2, 3):
        path = STATE / 'decisions' / f'{CYCLE}.stage45.run{run_number}.decision.json'
        if run_number == RUN_NUMBER:
            run_decisions[run_number] = {
                'cycle_id': CYCLE, 'run_number': RUN_NUMBER,
                'verdict': 'NARROW_OPT_COMPLETE', 'best_minato_score': current_best_score,
            }
            evidence[f'run{run_number}_decision'] = {
                'path': str(path.resolve()), 'sha256': None,
                'note': 'current decision is being written with this assessment',
            }
            continue
        try:
            run_decisions[run_number] = json.loads(path.read_text(encoding='utf-8'))
            evidence[f'run{run_number}_decision'] = {
                'path': str(path.resolve()), 'sha256': sha256(path),
            }
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            run_decisions[run_number] = None

    scores = {}
    decisions_complete = True
    for run_number, item in run_decisions.items():
        try:
            identity_matches = (item and item.get('cycle_id') == CYCLE and
                                int(item.get('run_number', 0)) == run_number and
                                item.get('verdict') == 'NARROW_OPT_COMPLETE')
        except (TypeError, ValueError):
            identity_matches = False
        if not identity_matches:
            decisions_complete = False
            continue
        value = current_best_score if run_number == RUN_NUMBER else item.get('best_minato_score')
        try:
            value = float(value)
            if not (value == value and abs(value) != float('inf')):
                raise ValueError('non-finite score')
            scores[run_number] = value
        except (TypeError, ValueError):
            decisions_complete = False

    shifted_inputs = set()
    plan_paths = {1: STAGE4_DECISION}
    for run_number in (2, 3):
        plan_paths[run_number] = STAGE45_DIR / f'{CYCLE}.stage45.run{run_number}.range_plan.json'
    plans_complete = True
    for run_number, path in plan_paths.items():
        try:
            plan = json.loads(path.read_text(encoding='utf-8'))
            if plan.get('cycle_id') != CYCLE:
                raise ValueError('range plan belongs to another cycle')
            actions = plan.get('input_actions') or {}
            shifted_inputs.update(
                f'run{run_number}:{name}' for name, action in actions.items()
                if isinstance(action, dict) and action.get('action') == 'shift'
            )
            evidence[f'run{run_number}_range_plan'] = {
                'path': str(path.resolve()), 'sha256': sha256(path),
            }
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            plans_complete = False

    score_growth = (decisions_complete and len(scores) == 3 and
                    scores[1] < scores[2] < scores[3])
    has_shift = bool(shifted_inputs)
    assessable = decisions_complete and plans_complete and len(scores) == 3
    potential = bool(assessable and score_growth and has_shift)
    if not assessable:
        status = 'INSUFFICIENT_EVIDENCE'
        reason = 'All three run decisions and their input action plans are required.'
    elif potential:
        status = 'POTENTIAL_MINATO_SCORE_GROWTH'
        reason = ('The best Minato score increased on each of the three runs, and at least one '
                  'input used the shift action; the score may have further headroom.')
    else:
        status = 'NO_POTENTIAL_GROWTH_SIGNAL'
        reason = ('The three-run increasing-score and shifted-input conditions were not both met.')
    return {
        'schema_version': 1,
        'status': status,
        'minato_score_could_potentially_be_bigger': potential if assessable else None,
        'reason': reason,
        'criteria': {
            'best_minato_scores_by_run': {f'run{n}': scores.get(n) for n in (1, 2, 3)},
            'strictly_increasing_across_all_three_runs': bool(score_growth) if assessable else None,
            'shift_action_present': has_shift if plans_complete else None,
            'shifted_inputs': sorted(shifted_inputs),
        },
        'evidence': evidence,
        'assessed_utc': datetime.now(timezone.utc).isoformat(),
    }


def write_decision(csv_path: Path) -> Path:
    """Read narrow CSV and apply the shared Stage 3 qualification gates."""
    import csv as _csv
    from datetime import date
    rows = list(_csv.DictReader(csv_path.open()))
    start = date.fromisoformat(FROM_DATE.replace('.', '-'))
    end = date.fromisoformat(TO_DATE.replace('.', '-'))
    months = (end - start).days / (365.2425 / 12.0)
    qualifying = []
    max_trades = 0
    for r in rows:
        try:
            profit = float(r.get('Profit', 'nan'))
            cf = float(r.get('CustomFitness', 'nan'))
            trades = float(r.get('Trades', 'nan'))
            dd = float(r.get('EquityDrawdownPercent', 'nan'))
            margin = float(r.get('MarginLevel', 'nan'))
            if not all(map(lambda value: value == value and abs(value) != float('inf'),
                           (profit, cf, trades, dd, margin))):
                continue
            max_trades = max(max_trades, int(trades))
            if (profit > 0 and cf >= 15 and trades / months >= 35 and
                    dd <= 35 and margin >= 120):
                qualifying.append({'cf': cf, 'profit': profit, 'trades': int(trades),
                                   'drawdown_percent': dd, 'margin_level': margin})
        except (TypeError, ValueError, ZeroDivisionError):
            continue
    best_score = max((item['cf'] for item in qualifying), default=None)
    state = json.loads(CYCLE_STATE.read_text(encoding='utf-8'))
    try:
        runtime = json.loads(NARROW_RUNTIME.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        runtime = {}
    prior_score = state.get('stage45_best_score')
    retained_score = best_score if prior_score is None or (best_score is not None and best_score >= float(prior_score)) else prior_score
    verdict = 'NARROW_OPT_COMPLETE' if qualifying else 'NARROW_OPT_FAILED'
    decision = {
        'schema_version': 2,
        'cycle_id': CYCLE,
        'stage': '4.5',
        'ea': EA,
        'selected_tf': TF,
        'run_number': RUN_NUMBER,
        'run_count_required': 3,
        'verdict': verdict,
        'reason': f'{len(qualifying)} records passed all Stage 3 gates; best Minato score={best_score}',
        'qualifying_row_count': len(qualifying),
        'max_trades': max_trades,
        'best_minato_score': best_score,
        'prior_best_minato_score': prior_score,
        'retained_best_minato_score': retained_score,
        'timed_out': bool(runtime.get('timed_out')),
        'passes_observed': runtime.get('passes'),
        'passes_source': runtime.get('passes_source', 'tester_completion_marker'),
        'parsed_opt_pass_records': runtime.get('parsed_opt_pass_records'),
        'csv': str(csv_path.resolve()),
        'csv_sha256': sha256(csv_path),
        'effective_latest_score': best_score,
        'previous_score_provenance': 'authenticated Stage 3 or prior Stage 4.5 best',
        'decided_utc': datetime.now(timezone.utc).isoformat(),
    }
    if RUN_NUMBER == 3:
        decision['minato_growth_potential'] = assess_minato_growth_potential(best_score)
    NARROW_DECISION.write_text(json.dumps(decision, indent=2))
    return NARROW_DECISION


def write_verification(decision: dict) -> Path:
    valid = decision['verdict'] == 'NARROW_OPT_COMPLETE'
    runtime = {}
    plan = {}
    try:
        runtime = json.loads(NARROW_RUNTIME.read_text(encoding='utf-8'))
        plan_path = (STAGE45_DIR / f'{CYCLE}.stage45.run{RUN_NUMBER}.range_plan.json'
                     if RUN_NUMBER > 1 else STAGE4_DECISION)
        plan = json.loads(plan_path.read_text(encoding='utf-8'))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        pass
    range_failures = validate_input_range_plan(
        plan.get('input_actions') or {}, read_ini_params(NARROW_INI)
    ) if plan else ['authenticated range plan is missing']
    ini_hash_ok = (NARROW_INI.is_file() and
                   sha256(NARROW_INI) == runtime.get('narrow_ini_sha256'))
    try:
        complete = json.loads(NARROW_COMPLETE.read_text(encoding='utf-8'))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        complete = {}
    opt_path = Path(runtime.get('opt_path') or '')
    opt_hash_ok = (opt_path.is_file() and runtime.get('opt_sha256') and
                   sha256(opt_path) == runtime.get('opt_sha256') == complete.get('opt_sha256'))
    passes_ok = (runtime_has_observed_passes(runtime) and
                 (int(complete.get('passes') or 0) > 0 or
                  int(complete.get('parsed_opt_pass_records') or 0) > 0))
    ver = {
        'schema_version': 2,
        'cycle_id': CYCLE,
        'stage': '4.5',
        'passed': valid,
        'checks': [
            {'name': 'decision exists', 'passed': NARROW_DECISION.is_file()},
            {'name': 'csv exists', 'passed': NARROW_CSV.is_file()},
            {'name': 'csv sha256 matches decision', 'passed': sha256(NARROW_CSV) == decision['csv_sha256']},
            {'name': 'verdict', 'passed': valid},
            {'name': 'all Stage 3 gates applied', 'passed': decision['qualifying_row_count'] > 0 and decision.get('best_minato_score') is not None},
            {'name': 'run number in three-run sequence', 'passed': 1 <= int(decision.get('run_number', 0)) <= 3},
            {'name': 'launched INI hash matches runtime', 'passed': ini_hash_ok},
            {'name': 'current run observed optimization passes', 'passed': passes_ok,
             'passes_observed': runtime.get('passes'), 'complete_passes': complete.get('passes'),
             'parsed_opt_pass_records': runtime.get('parsed_opt_pass_records')},
            {'name': 'current run .opt is fresh and hash-bound', 'passed': opt_hash_ok},
            {'name': 'all optimized ranges match plan and include clusters', 'passed': not range_failures,
             'failures': range_failures},
        ],
        'verified_utc': datetime.now(timezone.utc).isoformat(),
    }
    ver['passed'] = all(c['passed'] for c in ver['checks'])
    NARROW_VERIFICATION.write_text(json.dumps(ver, indent=2))
    return NARROW_VERIFICATION


def write_event(decision: dict, verification: dict) -> Path:
    evt = {
        'schema_version': 2,
        'cycle_id': CYCLE,
        'stage': '4.5',
        'ea': EA,
        'timeframe': TF,
        'verdict': decision['verdict'],
        'verification_passed': verification['passed'],
        'decision': str(NARROW_DECISION.resolve()),
        'decision_sha256': sha256(NARROW_DECISION),
        'verification': str(NARROW_VERIFICATION.resolve()),
        'verification_sha256': sha256(NARROW_VERIFICATION),
        'csv': decision.get('csv'),
        'csv_sha256': decision.get('csv_sha256'),
        'run_number': decision.get('run_number'),
        'run_count_required': decision.get('run_count_required', 3),
        'best_minato_score': decision.get('best_minato_score'),
        'retained_best_minato_score': decision.get('retained_best_minato_score'),
        'minato_growth_potential': decision.get('minato_growth_potential'),
        'qualifying_row_count': decision['qualifying_row_count'],
        'consumed': False,
        'completed_utc': datetime.now(timezone.utc).isoformat(),
    }
    NARROW_EVENT.write_text(json.dumps(evt, indent=2))
    return NARROW_EVENT


def dispatch_stage5_cron() -> dict:
    """Resume/run the actual Stage 5 job only after Stage 4.5 publication."""
    listing = subprocess.run(['hermes', 'cron', 'list', '--all'], capture_output=True, text=True,
                             cwd=str(ROOT), check=False)
    match = re.search(r'([0-9a-f]{12}).*?Name:\s+kcos_stage5_forward(?:_test)?\b',
                      listing.stdout, re.S | re.I)
    if not match:
        print('[DISPATCH] HOLD: actual kcos_stage5_forward cron was not found')
        return {'dispatched': False, 'reason': 'job_not_found'}
    job_id = match.group(1)
    resume = subprocess.run(['hermes', 'cron', 'resume', job_id], capture_output=True, text=True, check=False)
    run = subprocess.run(['hermes', 'cron', 'run', job_id], capture_output=True, text=True, check=False)
    result = {'job_id': job_id, 'resume_rc': resume.returncode, 'run_rc': run.returncode,
              'dispatched': run.returncode == 0, 'run_stdout': run.stdout[-500:]}
    print(f"[DISPATCH] Stage 5 cron {job_id}: run_rc={run.returncode}")
    return result


def advance_state(decision: dict):
    v = json.loads(CYCLE_STATE.read_text())
    if v.get('cycle_id') != CYCLE or v.get('current_ea') != EA:
        print('[ACTIVE-CYCLE-GUARD] HOLD: refusing stale Stage 4.5 state write')
        return False
    verification = json.loads(NARROW_VERIFICATION.read_text(encoding='utf-8'))
    completed = (decision['verdict'] == 'NARROW_OPT_COMPLETE' and
                 verification.get('passed') is True and
                 runtime_has_observed_passes(json.loads(NARROW_RUNTIME.read_text(encoding='utf-8'))))
    final_run = RUN_NUMBER == 3
    structural_checks = [
        check for check in verification.get('checks', [])
        if check.get('name') not in {'verdict', 'all Stage 3 gates applied'}
    ]
    complete_evidence = json.loads(NARROW_COMPLETE.read_text(encoding='utf-8'))
    continue_refinement = (
        RUN_NUMBER < 3 and
        complete_evidence.get('status') in {'TIMEFRAME_COMPLETE', 'TIMEOUT_SETTLING'} and
        int(decision.get('qualifying_row_count', 0)) > 0 and
        bool(structural_checks) and all(check.get('passed') is True for check in structural_checks)
    )
    try:
        hold_after = float(v.get('hold_after_stage'))
        allowed_stages = {int(value) for value in v.get('allowed_stages', [])}
        stage5_authorized = hold_after >= 5 and 5 in allowed_stages
    except (TypeError, ValueError):
        stage5_authorized = False
    v['stage_status'] = ('narrow_opt_complete' if completed and final_run and stage5_authorized else
                         'stage4_complete_held' if (completed or continue_refinement) else 'narrow_opt_failed')
    v['narrow_opt_decision'] = str(NARROW_DECISION.resolve())
    v['last_stage45_decision'] = str(NARROW_DECISION.resolve())
    v['last_stage45_verification'] = str(NARROW_VERIFICATION.resolve())
    v['narrow_opt_event'] = str(NARROW_EVENT.resolve())
    v['last_stage45_event'] = str(NARROW_EVENT.resolve())
    v['stage45_run_number'] = RUN_NUMBER
    if continue_refinement:
        v['narrow_run_number'] = RUN_NUMBER + 1
        v['next_action'] = 'run_stage45_next'
        v['stage_status'] = 'stage4_complete_held'
    elif completed and final_run and not stage5_authorized:
        v['next_action'] = 'inspect_stage45_before_stage5'
        v['stage_status'] = 'stage4_complete_held'
    else:
        v['next_action'] = 'dispatch_stage5' if completed else 'hold_for_review'
    if completed and decision.get('retained_best_minato_score') is not None:
        v['stage45_best_score'] = decision['retained_best_minato_score']
    # A verified successful refinement supersedes an earlier Stage 4.5 halt.
    # Keep unrelated operator holds and the configured Stage 5 boundary intact.
    if completed and str(v.get('halt_reason') or '').startswith('Stage 4.5 '):
        v.pop('halt_reason', None)
    CYCLE_STATE.write_text(json.dumps(v, indent=2))


def main():
    # This legacy implementation is cycle-specific. Never let it emit stale
    # ADLine evidence after the canonical cycle has rotated.
    try:
        active = json.loads(CYCLE_STATE.read_text(encoding='utf-8'))
    except Exception as exc:
        print(f'[ACTIVE-CYCLE-GUARD] HOLD: cannot read canonical state: {exc}')
        return 0
    if active.get('cycle_id') != CYCLE or active.get('current_ea') != EA:
        print('[ACTIVE-CYCLE-GUARD] HOLD: legacy ADLine executor is not the active cycle')
        return 0
    # Respect the canonical operator boundary before reading a Stage 4 plan
    # or launching MT5. A hold at 4.5 permits these refinements but stops the
    # separate Stage 5 forward test.
    try:
        hold_after = active.get('hold_after_stage')
        if active.get('stage_status') == 'stage4_pending' or (
                hold_after is not None and float(hold_after) <= 4):
            print('[HOLD] authenticated cycle boundary holds after Stage 4; Stage 4.5 is not authorized')
            return 0
    except (TypeError, ValueError):
        print('[HOLD] invalid stage authorization/boundary; refusing Stage 4.5 launch')
        return 0
    if RUN_NUMBER > 3:
        print('[COMPLETE] all three Stage 4.5 refinements are verified; no further optimization')
        return 0
    if active.get('stage_status') == 'narrow_opt_failed' and active.get('next_action') == 'hold_for_review':
        print('[HOLD] Stage 4.5 has exhausted its bounded retry; waiting for operator review')
        return 0
    backup_cycle_state()
    decision = read_decision()
    if RUN_NUMBER > 1:
        prior_csv = STAGE45_DIR / f'{CYCLE}.stage45.{TF}.run{RUN_NUMBER - 1}.csv'
        prior_ini = STATE / 'stage3_v2' / f'{CYCLE}.{EA}.{SYMBOL}.{TF}.{FROM_DATE.replace(".","")}_{TO_DATE.replace(".","")}.run{RUN_NUMBER - 1}.narrow.ini'
        if not prior_csv.is_file() or not prior_ini.is_file():
            raise RuntimeError('Stage 4.5 follow-up requires the prior authenticated CSV and INI')
        decision = derive_followup_range_plan(prior_csv, prior_ini, decision)
    lease_mode = acquire_lease()
    if not lease_mode:
        return 0
    if lease_mode == 'SETTLE':
        runtime = json.loads(NARROW_RUNTIME.read_text())
        passes = runtime.get('passes')
        print('[SETTLE] reusing TIMEFRAME_COMPLETE runtime; no MT5 launch')
    else:
        wide = parse_wide_ini()
        print(f'[WIDE] parsed {len(wide)} inputs from {WIDE_INI.name}')
        build_narrow_ini(wide, decision)
        source_ini = (STATE / 'stage3_v2' / f'{CYCLE}.{EA}.{SYMBOL}.{TF}.{FROM_DATE.replace(".","")}_{TO_DATE.replace(".","")}.run{RUN_NUMBER - 1}.narrow.ini'
                      if RUN_NUMBER > 1 else WIDE_INI)
        altered = altered_input_names(source_ini, NARROW_INI)
        print(f'[NARROW INI] wrote {NARROW_INI}')
        # MT5 can prefer a cached, truncated-name .ini/.set profile to the
        # /config TesterInputs. Stage 3 already solved this exact failure by
        # backing up and synchronizing matching profiles before launch.
        from kcos_stage3_launch_v2 import sync_cached_tester_profiles
        profile_sync = sync_cached_tester_profiles(MT5_DATA, EA, NARROW_INI)
        if not profile_sync:
            raise RuntimeError(
                'Stage 4.5 cached Tester profile sync produced no verified INI/.set profile; refusing launch')
        print(f'[PROFILE SYNC] synchronized {len(profile_sync)} cached Tester profiles')
        runtime = write_runtime()
        runtime['altered_input_names'] = altered
        runtime['deployment_reconciliation'] = _authenticated_stage3_ex5_deployment()
        runtime['cached_tester_profile_sync'] = profile_sync
        # Keep the authenticated Expert and optimization settings in /config.
        # MT5 may autostart from this file; after launch, accept that state only
        # when the exact config path, file hash, and terminal executable match.
        from kcos_stage3_launch_v2 import (
            create_non_autostart_ini, prepare_tester_schema_start,
        )
        startup_ini = create_non_autostart_ini({}, TF, NARROW_INI)
        runtime['startup_ini'] = str(startup_ini.resolve())
        runtime['startup_ini_sha256'] = sha256(startup_ini)
        runtime['terminal_path'] = str(MT5_PATH.resolve())
        runtime['tester_log'] = runtime['journal_path']
        runtime['tester_log_start_offset'] = runtime['journal_start_offset']
        print(f'[RUNTIME] {NARROW_RUNTIME}')
        import psutil
        print('[LAUNCH] starting MT5 from the authenticated Tester INI; verifying exact EA/timeframe')
        pid = launch_mt5(startup_ini)
        runtime['terminal_pid'] = pid
        runtime['launch_identity'] = {
            'terminal_pid': pid,
            'terminal_path': str(MT5_PATH.resolve()),
            'config_path': str(startup_ini.resolve()),
            'config_sha256': sha256(startup_ini),
            'config_sha256_matches_narrow_ini': sha256(startup_ini) == sha256(NARROW_INI),
        }
        # Persist ownership immediately; UIA is advisory and may be slow while
        # MT5 initializes. The journal remains the completion authority.
        NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
        update_lease('STARTING', terminal_pid=pid, runtime=str(NARROW_RUNTIME.resolve()))
        runtime['input_schema_binding_attempts'] = 0
        runtime['start_button_invoked'] = False
        start_confirmed = False
        for attempt in range(8):
            runtime['start_button_invoked'] = bool(
                prepare_tester_schema_start(
                    runtime, NARROW_INI, handoff=authenticated_stage3_input_handoff()))
            runtime['journal_start_offset'] = int(
                runtime.get('tester_log_start_offset', runtime['journal_start_offset']))
            NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
            if runtime['start_button_invoked']:
                runtime['start_confirmation'] = 'gui_start_button'
                start_confirmed = True
                break
            binding = runtime.get('input_schema_binding') or {}
            if (binding.get('status') == 'ALREADY_RUNNING'
                    and runtime.get('startup_ini_sha256') == sha256(NARROW_INI)
                    and sha256(startup_ini) == sha256(NARROW_INI)):
                # The authenticated config includes Expert and Optimization=2;
                # MT5 can start the optimization while loading /config. Treat
                # Stop as a running launch only after the exact process,
                # executable and byte-identical narrow config match this run.
                # UI title text is advisory because MT5 may hide/omit it while
                # the optimization is starting.
                try:
                    exact_executable = Path(psutil.Process(pid).exe()).resolve() == MT5_PATH.resolve()
                except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                    exact_executable = False
                if not exact_executable:
                    runtime['config_autostart_check'] = {
                        'terminal_pid': pid,
                        'launch_matched_exact_config_path': True,
                        'exact_ini_hash': True,
                        'exact_executable': False,
                        'tester_identity_ui_match': tester_identity_matches(pid, EA, SYMBOL, TF),
                    }
                    NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
                    if attempt < 7:
                        time.sleep(3)
                    continue
                runtime['config_autostart_detected'] = True
                runtime['config_autostart_check'] = {
                    'terminal_pid': pid,
                    'launch_matched_exact_config_path': True,
                    'exact_ini_hash': True,
                    'exact_executable': True,
                    'tester_identity_ui_match': tester_identity_matches(pid, EA, SYMBOL, TF),
                }
                runtime['start_confirmation'] = 'authenticated_config_process_and_ini'
                start_confirmed = True
                NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
                break
            if attempt < 7:
                time.sleep(3)
        if not start_confirmed:
            stopped = stop_owned_mt5(pid)
            runtime.update({
                'status': 'INFRASTRUCTURE_FAILURE',
                'failure_kind': 'TESTER_SCHEMA_BINDING_FAILED',
                'failure_detail': runtime.get(
                    'start_required_reason', 'exact Tester schema binding or Start handoff failed'),
                'stopped_after_schema_binding_failure': stopped,
            })
            NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
            update_lease('INFRASTRUCTURE_FAILURE', terminal_pid=pid,
                         failure_kind='TESTER_SCHEMA_BINDING_FAILED')
            from kcos_stage3_launch_v2 import (
                _fresh_optimizer_passes, _fresh_optimizer_start_seen,
            )
            fresh_workers = _fresh_stage45_worker_pids(runtime)
            retry_safe = bool(
                stopped and not runtime.get('worker_pids')
                and fresh_workers == []
                and not _fresh_optimizer_start_seen(runtime)
                and _fresh_optimizer_passes(runtime, CYCLE, SYMBOL, TF) is None
                and not NARROW_COMPLETE.exists() and not NARROW_CSV.exists()
            )
            record_execution_failure(runtime, pid, 'TESTER_SCHEMA_BINDING_FAILED',
                                     retry_safe=retry_safe)
            raise RuntimeError(runtime['failure_detail'])
        runtime['start_required_reason'] = None
        for _ in range(20):
            if tester_identity_matches(pid, EA, SYMBOL, TF):
                runtime['identity_verified_from'] = 'exact Tester UI identity'
                break
            time.sleep(1)
        else:
            # UIA and process command lines may become unavailable or change
            # while MT5 is busy. Retain the launch-time config attestation and
            # require the live PID to still resolve to this exact executable.
            launch_identity = runtime.get('launch_identity') or {}
            launch_identity_matches = (
                launch_identity.get('terminal_pid') == pid
                and launch_identity.get('terminal_path') == str(MT5_PATH.resolve())
                and launch_identity.get('config_path') == str(startup_ini.resolve())
                and launch_identity.get('config_sha256') == sha256(startup_ini)
            )
            try:
                exact_executable = Path(psutil.Process(pid).exe()).resolve() == MT5_PATH.resolve()
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                exact_executable = False
            if not launch_identity_matches or not exact_executable:
                raise RuntimeError(f'launched terminal {pid} did not expose exact tester identity {EA}, {SYMBOL} {TF}')
            runtime['identity_verified_from'] = 'authenticated launch config + exact terminal executable'

        runtime['screenshot_path'] = capture_run_screenshot()
        runtime['screenshot_sha256'] = sha256(RUN_SCREENSHOT)
        runtime['status'] = 'OPTIMIZING'
        update_lease('OPTIMIZING', terminal_pid=pid, runtime=str(NARROW_RUNTIME.resolve()))
        NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
        print(f'[LAUNCH] terminal pid {pid}')
        print('[WATCH] polling journal for narrow completion (this may take hours)')
        try:
            done, passes = find_completion_line(
                start_offset=runtime.get('journal_start_offset', 0), terminal_pid=pid)
        except TesterInputRejected as exc:
            stopped = stop_owned_mt5(pid)
            runtime.update({"status": "INFRASTRUCTURE_FAILURE",
                            "failure_kind": "MT5_TESTER_INPUT_REJECTED",
                            "failure_detail": str(exc),
                            "stopped_after_input_rejection": stopped})
            NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
            update_lease('INFRASTRUCTURE_FAILURE', terminal_pid=pid,
                         failure_kind='MT5_TESTER_INPUT_REJECTED')
            raise
        if not done:
            print('[WATCH] timeout — stopping owned MT5 and settling any flushed best result')
            stopped = stop_owned_mt5(pid)
            runtime['status'] = 'TIMEOUT_SETTLING' if stopped else 'INFRASTRUCTURE_FAILURE'
            runtime['timed_out'] = True
            runtime['stopped_for_timeout'] = stopped
            NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
            update_lease(runtime['status'], terminal_pid=pid)
            if not stopped:
                return 1
            passes = passes or 0
            if passes <= 0:
                runtime['timeout_completion_marker_missing'] = True
                runtime['timeout_detail'] = 'No final optimization marker was observed before timeout; checking only a fresh, parseable .opt from this run.'
                runtime.pop('failure_detail', None)
                NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
        if int(passes or 0) <= 0 and not runtime.get('timed_out'):
            stopped = stop_owned_mt5(pid)
            runtime['stopped_after_zero_passes'] = stopped
            runtime['failure_detail'] = 'Completion was reported without a positive pass number; refusing to parse any .opt.'
            record_execution_failure(runtime, pid, 'ZERO_PASSES_REPORTED')
            return 1
        if int(passes or 0) > 0:
            print(f'[WATCH] narrow opt finished at pass {passes}')
        elif runtime.get('timed_out'):
            print('[WATCH] no final completion marker; attempting fresh .opt settlement')
        if runtime.get('status') != 'TIMEOUT_SETTLING':
            runtime['status'] = 'TIMEFRAME_COMPLETE'
        runtime['passes'] = passes
        update_lease('TIMEFRAME_COMPLETE' if runtime['status'] == 'TIMEFRAME_COMPLETE' else 'TIMEOUT_SETTLING', passes=passes)
        NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))

    # Find and parse .opt before publishing any completion artifact.
    opt = find_new_opt()
    if opt is None:
        runtime['failure_detail'] = 'No changed matching .opt was written after this run started.'
        record_execution_failure(runtime, runtime.get('terminal_pid'), 'FRESH_OPT_NOT_FOUND')
        print('[HOLD] no fresh current-run .opt found; no report published')
        return 1
    print(f'[PARSE] found {opt}')
    runtime['opt_path'] = str(opt.resolve())
    runtime['opt_sha256'] = sha256(opt)
    if runtime.get('timeout_completion_marker_missing'):
        settled_hash = wait_for_stable_opt(opt)
        if settled_hash is None:
            runtime['failure_detail'] = 'Fresh .opt kept changing after the timed-out terminal stopped; refusing an unstable parse.'
            NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
            attempts = record_execution_failure(
                runtime, runtime.get('terminal_pid'), 'OPT_UNSTABLE_AFTER_TIMEOUT',
                retry_safe=bool(runtime.get('stopped_for_timeout')))
            return 0 if runtime.get('stopped_for_timeout') and attempts < 2 else 1
        runtime['opt_sha256'] = settled_hash
    NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
    try:
        parse_narrow_opt(opt)
        if runtime.get('timeout_completion_marker_missing') and sha256(opt) != runtime.get('opt_sha256'):
            raise RuntimeError('fresh .opt changed while parsing the timed-out run')
    except Exception as exc:
        runtime['failure_detail'] = f'{type(exc).__name__}: {exc}'[:1000]
        runtime['parse_failure_utc'] = datetime.now(timezone.utc).isoformat()
        stopped = stop_owned_mt5(runtime.get('terminal_pid'))
        runtime['stopped_after_parse_failure'] = stopped
        attempts = record_execution_failure(
            runtime, runtime.get('terminal_pid'), 'OPT_PARSE_FAILURE', retry_safe=stopped)
        # A failed parse is retried only when the exact owned MT5 process is
        # confirmed closed. Return success for the first safe retry so Hermes
        # leaves the two-minute Stage 4.5 cron enabled; the second failure holds.
        return 0 if stopped and attempts < 2 else 1
    if int(passes or 0) <= 0 and runtime.get('timed_out'):
        # Worker agents can flush valid result records without the terminal
        # journal writing its final optimization-finished marker. The .opt has
        # already passed exact-run freshness and stability checks plus strict parsing.
        parsed_passes = count_parsed_opt_passes(NARROW_CSV)
        if parsed_passes <= 0:
            runtime['failure_detail'] = 'Timed out without a completion marker and the fresh .opt contained no parseable pass records.'
            NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
            attempts = record_execution_failure(
                runtime, runtime.get('terminal_pid'), 'ZERO_RESULT_PASSES_TIMEOUT',
                retry_safe=bool(runtime.get('stopped_for_timeout')))
            return 0 if runtime.get('stopped_for_timeout') and attempts < 2 else 1
        runtime['passes_source'] = 'fresh_parsed_opt_pass_records'
        runtime['parsed_opt_pass_records'] = parsed_passes
        NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
        update_lease('TIMEOUT_SETTLING', parsed_opt_pass_records=parsed_passes,
                     passes_source=runtime['passes_source'])
    runtime['terminal_closed_after_run'] = stop_owned_mt5(runtime.get('terminal_pid'))
    runtime['terminal_closed_utc'] = datetime.now(timezone.utc).isoformat()
    NARROW_RUNTIME.write_text(json.dumps(runtime, indent=2))
    if not runtime['terminal_closed_after_run']:
        raise RuntimeError('owned MT5 terminal did not close after Stage 4.5 completion')
    # Publish completion only after the owned MT5 terminal is confirmed closed.
    write_complete_event(runtime, passes, sha256(opt))

    # Decision + verification + event
    dec_path = write_decision(NARROW_CSV)
    print(f'[DECISION] {dec_path}')
    ver_path = write_verification(json.loads(dec_path.read_text()))
    print(f'[VERIFICATION] passed={json.loads(ver_path.read_text())["passed"]}')
    write_event(json.loads(dec_path.read_text()), json.loads(ver_path.read_text()))

    # Advance state only after decision/verification/event are on disk.  The
    # zero-token supervisor owns the exactly-once Stage 5 dispatch; this
    # executor must not also resume/run the cron (duplicate launch race).
    final_decision = json.loads(dec_path.read_text())
    advance_state(final_decision)
    update_lease('NARROW_OPT_COMPLETE' if RUN_NUMBER == 3 else 'RUN_COMPLETE',
                 decision=str(NARROW_DECISION.resolve()),
                 verification=str(NARROW_VERIFICATION.resolve()),
                 run_number=RUN_NUMBER, supervisor_dispatch_required=True)
    print(f'[STATE] completed Stage 4.5 run {RUN_NUMBER}/3; supervisor_dispatch_required=True')

    return 0


if __name__ == '__main__':
    sys.exit(main())


