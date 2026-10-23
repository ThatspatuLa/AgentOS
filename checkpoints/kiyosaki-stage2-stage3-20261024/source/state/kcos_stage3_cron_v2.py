#!/usr/bin/env python3
"""Recurring Stage 3 entry: bind/start once, resume, and hold unrelated work."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import shutil
import sys
import psutil

ROOT = Path(os.environ.get('KCOS_ROOT', r'C:\Users\Tyson\Projects\kiyosaki_stage2b')).resolve()
STATE = ROOT / 'state'

def recover_overwritten_h1_runtime(state: dict, handoff: dict, runtime: dict | None,
                                  runtime_path: Path, terminal: Path,
                                  terminal_pids: list[int], driver) -> dict | None:
    """Rebuild an H1 monitor registration from exact, still-verifiable launch evidence.

    This path never launches or declares completion. The regular Stage 3 driver
    and watcher still have to settle the OPT, publish the timeframe event, parse,
    verify, and publish the Stage 3 handoff.
    """
    cycle = str(state.get('cycle_id') or '')
    common_identity = bool(
        cycle and handoff.get('cycle_id') == cycle
        and state.get('current_ea') == handoff.get('ea')
        and runtime and runtime.get('cycle_id') == cycle
        and runtime.get('ea') == handoff.get('ea'))
    legacy_overwrite = bool(
        common_identity and state.get('current_stage') == 'stage3_running'
        and state.get('stage_status') == 'stage3_h1_running'
        and state.get('next_action') == 'watch_stage3_h1'
        and runtime.get('status') == 'TIMEFRAME_COMPLETE'
        and runtime.get('timeframe') == 'M15')
    failed_no_expert_h1 = bool(
        common_identity and state.get('current_stage') == 'stage2_pending'
        and state.get('stage_status') == 'stage2_repair_required'
        and state.get('next_action') == 'dispatch_stage2'
        and state.get('stage2_repair_source') == 'stage3_tester_journal'
        and state.get('stage2_repair_journal', {}).get('code') == 'NO_EXPERT_IN_START_CONFIG'
        and runtime.get('status') == 'INFRASTRUCTURE_FAILURE'
        and runtime.get('timeframe') == 'H1'
        and runtime.get('failure_kind') == 'TESTER_JOURNAL_REJECTED'
        and runtime.get('journal_diagnostic', {}).get('code') == 'NO_EXPERT_IN_START_CONFIG'
        and runtime.get('start_required_reason') == 'authenticated auto-click sent; waiting for fresh tester workers'
        and driver.authorizes_full_cycle_stage3(state))
    if not (legacy_overwrite or failed_no_expert_h1):
        return None

    m15_event_path = runtime_path.parent / f'{cycle}.stage3.M15.complete.json'
    h1_event_path = runtime_path.parent / f'{cycle}.stage3.H1.complete.json'
    if h1_event_path.exists():
        raise ValueError('H1 completion event already exists; recovery will not replace it')
    if not m15_event_path.is_file():
        raise ValueError('authenticated M15 completion event is missing')
    m15_event = driver.load_object(m15_event_path)
    if not (m15_event.get('cycle_id') == cycle and m15_event.get('timeframe') == 'M15'
            and m15_event.get('status') == 'TIMEFRAME_COMPLETE'
            and m15_event.get('consumed') is True
            and m15_event.get('consumed_action') == 'continue_stage3'
            and (not legacy_overwrite
                 or runtime.get('timeframe_event') == str(m15_event_path.resolve()))):
        raise ValueError('stale runtime is not bound to the consumed M15 continuation')
    m15_hash = driver.sha256(m15_event_path)
    try:
        m15_log_offset = int(m15_event['tester_log_end_offset'])
        m15_completed = datetime.fromisoformat(str(m15_event['completed_utc']).replace('Z', '+00:00')).timestamp()
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError('M15 completion event lacks a usable H1 log baseline') from exc
    log_path = Path(str(runtime.get('tester_log') if failed_no_expert_h1
                         else m15_event.get('tester_log') or '')).resolve()
    try:
        log_offset = (int(runtime['tester_log_start_offset'])
                      if failed_no_expert_h1 else m15_log_offset)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError('H1 runtime lacks its authenticated Tester Journal offset') from exc
    if (log_offset <= 0 or not log_path.is_file()
            or (failed_no_expert_h1 and (log_path != Path(str(m15_event.get('tester_log') or '')).resolve()
                                         or log_offset != m15_log_offset))):
        raise ValueError('authenticated M15 tester Journal baseline is unavailable')

    failure_path = None
    failure_hash = None
    if failed_no_expert_h1:
        failure_path = Path(str(state.get('stage3_failure_event') or '')).resolve()
        expected_failure = (STATE / 'events' / f'{cycle}.stage3.failed.json').resolve()
        failure = driver.load_object(failure_path) if failure_path.is_file() else {}
        journal_errors = runtime.get('journal_diagnostic', {}).get('error_lines', [])
        repair_errors = state.get('stage2_repair_journal', {}).get('error_lines', [])
        no_expert_only = bool(
            journal_errors and repair_errors
            and all(driver.launcher.NO_EXPERT_IN_START_CONFIG_RE.search(str(line)) for line in journal_errors)
            and all(driver.launcher.NO_EXPERT_IN_START_CONFIG_RE.search(str(line)) for line in repair_errors))
        if (failure_path != expected_failure
                or failure.get('cycle_id') != cycle or failure.get('stage') != 3
                or failure.get('status') != 'failed'
                or failure.get('rotation_eligible') is not False
                or failure.get('error') != runtime.get('start_required_reason')
                or not no_expert_only):
            raise ValueError('H1 false-startup-notice failure is not bound to its exact Stage 3 event')
        failure_hash = driver.sha256(failure_path)

    if len(terminal_pids) != 1:
        raise ValueError('H1 recovery requires exactly one portable terminal process')
    pid = int(terminal_pids[0])
    process = psutil.Process(pid)
    if Path(process.exe()).resolve() != terminal.resolve():
        raise ValueError('live terminal executable does not match the isolated portable terminal')
    handoff_ini = Path(str(handoff.get('ini_files', {}).get('H1') or '')).resolve()
    startup_ini = handoff_ini.with_name(handoff_ini.stem + '.no_autostart.ini')
    if (not handoff_ini.is_file()
            or driver.sha256(handoff_ini) != handoff.get('ini_sha256', {}).get('H1')
            or not startup_ini.is_file()
            or startup_ini.read_bytes() != driver.launcher.non_autostart_ini_bytes(handoff_ini)):
        raise ValueError('H1 canonical/derived startup INI identity failed')
    config_args = [arg.split(':', 1)[1] for arg in process.cmdline()
                   if str(arg).casefold().startswith('/config:')]
    if len(config_args) != 1 or Path(config_args[0]).resolve() != startup_ini.resolve():
        raise ValueError('live portable terminal is not bound to this cycle H1 startup profile')
    launched_epoch = (float(runtime.get('launched_epoch')) if failed_no_expert_h1
                      else float(process.create_time()))
    if failed_no_expert_h1 and abs(float(process.create_time()) - launched_epoch) > 3.0:
        raise ValueError('H1 failed runtime launch time does not match the portable terminal process')
    if launched_epoch <= m15_completed:
        raise ValueError('H1 terminal predates the consumed M15 completion')

    launch_utc = datetime.fromtimestamp(launched_epoch, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    log_done, log_evidence = driver.watcher.fresh_log_completion(log_path, log_offset, launched_epoch)
    if not (log_done and log_evidence.get('clean') is True and log_evidence.get('final') is True
            and log_evidence.get('cache_saved') is True and not log_evidence.get('error_lines')):
        raise ValueError('fresh H1 Journal completion evidence is not clean and final')
    with log_path.open('rb') as handle:
        handle.seek(log_offset)
        journal_bytes = handle.read()
    journal_text = max((journal_bytes.decode('utf-16le', errors='ignore'),
                        journal_bytes.decode('utf-8', errors='ignore')),
                       key=lambda value: len(re.findall(r'genetic optimization finished on pass', value, re.I)))
    profile_marker = (f"{handoff.get('ea')}.ex5 on {handoff.get('symbol')},H1 from "
                      f"{handoff.get('from_date')} 00:00 to {handoff.get('to_date')} 00:00")
    if profile_marker.casefold() not in journal_text.casefold():
        raise ValueError('fresh tester Journal does not name this H1 expert, symbol, and date range')
    saved_matches = list(re.finditer(r"new records saved to cache file\s+'([^']+)'", journal_text, re.I))
    pass_matches = list(re.finditer(r'genetic optimization finished on pass\s+(\d+)', journal_text, re.I))
    saved = saved_matches[-1] if saved_matches else None
    pass_match = pass_matches[-1] if pass_matches else None
    if not saved or not pass_match or int(pass_match.group(1)) <= 0:
        raise ValueError('H1 Journal lacks a bound cache filename or final pass count')
    relative_cache = Path(saved.group(1).replace('/', os.sep))
    expected_cache_root = (driver.launcher.STAGE3_PORTABLE_DATA_DIR / 'Tester' / 'cache').resolve()
    opt_path = (driver.launcher.STAGE3_PORTABLE_DATA_DIR / relative_cache).resolve()
    candidates = [item.resolve() for item in driver.launcher.opt_candidates(
        handoff, 'H1', driver.launcher.STAGE3_PORTABLE_DATA_DIR)]
    if (opt_path.parent != expected_cache_root or opt_path not in candidates
            or not opt_path.is_file() or opt_path.stat().st_mtime < launched_epoch):
        raise ValueError('H1 cache output is absent, stale, or outside the authenticated candidate set')
    structurally_complete, opt_structure = driver.watcher.structurally_complete_opt(opt_path)
    if not structurally_complete:
        raise ValueError('H1 cache OPT is not structurally complete')
    opt_hash = driver.sha256(opt_path)
    source_runtime_hash = driver.sha256(runtime_path)
    handoff_hash = driver.sha256(driver.HANDOFF)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    receipt_path = STATE / 'diagnostics' / (
        f'{cycle}.stage3.H1.'
        f"{'noexpert-monitor-only-recovery' if failed_no_expert_h1 else 'monitor-only-recovery'}.json")
    backup_path = runtime_path.with_name(
        runtime_path.name + f'.pre_h1_{"noexpert_" if failed_no_expert_h1 else ""}recovery.{stamp}')
    superseded_failure_path = None
    if failed_no_expert_h1:
        superseded_failure_path = (STATE / 'diagnostics' / 'superseded' /
                                   f'{cycle}.stage3.failed.prior_h1_startup_notice.{stamp}.json')
        if superseded_failure_path.exists():
            raise ValueError('H1 prior failure archive already exists')
    if receipt_path.exists():
        prior_receipt = driver.load_object(receipt_path)
        for key, value in {
            'cycle_id': cycle, 'ea': handoff.get('ea'), 'timeframe': 'H1',
            'source_runtime_prior_sha256': source_runtime_hash,
            'handoff_sha256': handoff_hash, 'm15_event_sha256': m15_hash,
            'terminal_pid': pid, 'startup_ini_sha256': driver.sha256(startup_ini),
            'launched_epoch': launched_epoch, 'tester_log_segment_sha256': log_evidence['sha256'],
            'opt_sha256': opt_hash,
        }.items():
            if prior_receipt.get(key) != value:
                raise ValueError('an existing H1 recovery receipt conflicts with current evidence')
        if failed_no_expert_h1 and (prior_receipt.get('recovery_kind') != 'h1_noexpert_notice_with_authenticated_run'
                                    or prior_receipt.get('failure_event_sha256') != failure_hash):
            raise ValueError('an existing H1 no-expert recovery receipt conflicts with the failure event')
        receipt = prior_receipt
        backup_path = Path(receipt['source_runtime'])
    else:
        if backup_path.exists():
            raise ValueError('H1 recovery backup path already exists')
        shutil.copy2(runtime_path, backup_path)
        receipt = None
    new_receipt = {
        'schema_version': 1,
        'status': 'monitor_only_recovery_inputs_verified',
        'cycle_id': cycle, 'ea': handoff.get('ea'), 'timeframe': 'H1',
        'source_runtime': str(backup_path.resolve()),
        'source_runtime_sha256': driver.sha256(backup_path),
        'source_runtime_prior_sha256': source_runtime_hash,
        'handoff': str(driver.HANDOFF.resolve()), 'handoff_sha256': handoff_hash,
        'm15_event': str(m15_event_path.resolve()), 'm15_event_sha256': m15_hash,
        'terminal_pid': pid, 'terminal_exe': str(Path(process.exe()).resolve()),
        'startup_ini': str(startup_ini.resolve()),
        'startup_ini_sha256': driver.sha256(startup_ini),
        'canonical_ini': str(handoff_ini), 'canonical_ini_sha256': driver.sha256(handoff_ini),
        'launched_epoch': launched_epoch, 'launched_utc': launch_utc,
        'tester_log': str(log_path), 'tester_log_start_offset': log_offset,
        'tester_log_end_offset': log_evidence['end_offset'],
        'tester_log_segment_sha256': log_evidence['sha256'],
        'tester_log_completion_evidence': log_evidence,
        'opt_path': str(opt_path), 'opt_sha256': opt_hash,
        'opt_size': opt_path.stat().st_size, 'opt_mtime_epoch': opt_path.stat().st_mtime,
        'opt_structure': opt_structure,
        'final_pass_count': int(pass_match.group(1)),
        'worker_registration': ('captured_in_failed_runtime' if failed_no_expert_h1
                                else 'unavailable_after_concurrent_runtime_overwrite'),
        'worker_evidence_count': len(runtime.get('worker_process_evidence', [])) if failed_no_expert_h1 else 0,
        'recovery_kind': ('h1_noexpert_notice_with_authenticated_run'
                          if failed_no_expert_h1 else 'overwritten_h1_runtime'),
        'failure_event': str(failure_path) if failure_path else None,
        'failure_event_sha256': failure_hash,
        'superseded_failure_archive': str(superseded_failure_path) if superseded_failure_path else None,
        'verified_utc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    if receipt is None:
        driver.atomic_json(receipt_path, new_receipt)
        receipt = new_receipt
    recovery = {'receipt_path': str(receipt_path.resolve()),
                'receipt_sha256': driver.sha256(receipt_path), 'opt_sha256': opt_hash}
    runtime_new = {
        'schema_version': 2, 'cycle_id': cycle, 'stage': 3, 'ea': handoff.get('ea'),
        'symbol': handoff.get('symbol'), 'timeframe': 'H1', 'status': 'OPTIMIZING',
        'command': driver.launcher.build_command(handoff, 'H1', terminal,
                                                 startup_ini_override=startup_ini),
        'grid_mode': handoff.get('grid_mode', 'default'),
        'ini': str(handoff_ini), 'ini_sha256': driver.sha256(handoff_ini),
        'startup_ini': str(startup_ini), 'startup_ini_sha256': driver.sha256(startup_ini),
        'opt_path': str(opt_path), 'tester_log': str(log_path),
        'tester_log_start_offset': log_offset,
        'worker_pids': [], 'worker_baseline_pids': [], 'worker_process_evidence': [],
        'agent_logs': [], 'worker_cpu_seconds': {},
        'launched_epoch': launched_epoch, 'launched_utc': launch_utc,
        'terminal_pid': pid, 'start_button_invoked': True,
        'startup_identity_evidence': {'status': 'H1_profile_and_Journal_match',
                                      'timeframe': 'H1', 'ini_sha256': driver.sha256(handoff_ini)},
        'last_opt_size': opt_path.stat().st_size,
        'last_opt_mtime_epoch': opt_path.stat().st_mtime,
        'last_opt_growth_epoch': opt_path.stat().st_mtime,
        'completed_passes': int(pass_match.group(1)),
        'max_passes': int(handoff.get('max_passes', 100000000)),
        'first_pass_budget_seconds': int(handoff.get('first_pass_budget_seconds', 120)),
        'first_pass_hard_limit_seconds': int(handoff.get('first_pass_hard_limit_seconds', 200)),
        'progress_stall_budget_seconds': int(handoff.get('progress_stall_budget_seconds', 180)),
        'timeframe_budget_seconds': int(handoff.get('timeframe_budget_seconds', 7200)),
        'poll_seconds': int(handoff.get('poll_seconds', 10)),
        'normal_settle_seconds': int(handoff.get('normal_settle_seconds', 20)),
        'fallback_settle_seconds': int(handoff.get('fallback_settle_seconds', 60)),
        'timeout_seconds': int(handoff.get('timeframe_budget_seconds', 7200)),
        'pass_limit_settle_seconds': int(handoff.get('pass_limit_settle_seconds', 60)),
        'timeframe_event': str(h1_event_path.resolve()),
        'monitor_only_recovery': recovery,
        'zero_token': True,
    }
    driver.atomic_json(runtime_path, runtime_new)
    if failed_no_expert_h1:
        if not failure_path.is_file() or driver.sha256(failure_path) != failure_hash:
            raise ValueError('H1 failure event changed before it could be superseded')
        superseded_failure_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(failure_path), str(superseded_failure_path))
        state.setdefault('stage3_superseded_failures', []).append({
            'path': str(superseded_failure_path.resolve()),
            'sha256': failure_hash,
            'timeframe': 'H1',
            'reason': 'exact expert-free startup notice was followed by authenticated H1 optimization completion',
            'recovery_receipt': str(receipt_path.resolve()),
            'recovery_receipt_sha256': driver.sha256(receipt_path),
            'superseded_utc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        })
        state.update({
            'current_stage': 'stage3_running',
            'stage_status': 'stage3_h1_running',
            'next_action': 'watch_stage3_h1',
            'hold_after_stage': None,
            'stage3_h1_monitor_recovery_receipt': str(receipt_path.resolve()),
            'stage3_h1_monitor_recovery_receipt_sha256': driver.sha256(receipt_path),
            'updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        })
        for key in ('stage3_failure_event', 'stage3_failure_class', 'stage3_failure_detail',
                    'stage2_repair_source', 'stage2_repair_reason', 'stage2_repair_journal'):
            state.pop(key, None)
        driver.atomic_json(driver.STATE, state)
    return runtime_new

def runtime_has_live_owned_worker(runtime: dict) -> bool:
    """Match recorded worker identity so Windows PID reuse cannot block a restart.

    Missing evidence or inaccessible live-process metadata remains fail-closed.
    """
    worker_pids = runtime.get('worker_pids', [])
    evidence = runtime.get('worker_process_evidence', [])
    if not isinstance(worker_pids, list):
        return True
    if not isinstance(evidence, list):
        evidence = []
    by_pid = {}
    for item in evidence:
        if isinstance(item, dict):
            try:
                by_pid[int(item.get('pid'))] = item
            except (TypeError, ValueError):
                continue
    for raw_pid in worker_pids:
        try:
            pid = int(raw_pid)
            process = psutil.Process(pid)
        except (TypeError, ValueError, psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
        item = by_pid.get(pid)
        if not item:
            return True
        try:
            if abs(float(process.create_time()) - float(item['create_time'])) > 1.0:
                continue
            expected_exe = os.path.normcase(os.path.abspath(str(item['exe'])))
            actual_exe = os.path.normcase(os.path.abspath(process.exe()))
        except (KeyError, TypeError, ValueError, OSError, psutil.AccessDenied, psutil.Error):
            return True
        if actual_exe == expected_exe:
            return True
    return False


def choose_action(state: dict, runtime: dict | None, terminal_pids: list[int],
                  driver_module=None, handoff: dict | None = None) -> str:
    if state.get('current_stage') not in {'stage3_pending','stage3_running'}:
        return 'NOOP_OTHER_STAGE'
    if (state.get('stage_status') == 'stage3_infrastructure_failure'
            and state.get('next_action') == 'inspect_stage3_infrastructure_failure'):
        retry = getattr(driver_module, 'stage3_repair_retry_is_ready', None)
        if callable(retry) and retry(state) is True:
            return 'START'
        else:
            return 'HOLD_STAGE3_INFRASTRUCTURE_FAILURE'
    unstarted_retry = getattr(driver_module, 'unstarted_terminal_retry_is_ready', None)
    if (driver_module is not None and runtime and handoff and callable(unstarted_retry)
            and unstarted_retry(state, handoff, runtime, terminal_pids)):
        return 'RETRY_UNSTARTED_TERMINAL'
    if terminal_pids:
        if runtime and runtime.get('terminal_pid') in terminal_pids:
            return 'RESUME'
        return 'ALREADY_RUNNING_EXTERNAL'
    if (driver_module is not None and runtime
            and runtime.get('status') == 'INFRASTRUCTURE_FAILURE'
            and driver_module.stage3_repair_retry_is_ready(state)):
        return 'START'
    stale_restart = getattr(driver_module, 'stale_runtime_restart_is_ready', None)
    if (runtime and handoff and callable(stale_restart)
            and stale_restart(state, handoff, runtime, terminal_pids)):
        return 'RESTART_STALE'
    if state.get('current_stage') == 'stage3_pending':
        return 'START'
    duration_retry_ready = getattr(driver_module, 'stage3_duration_retry_is_ready', None)
    if (driver_module is not None and runtime and callable(duration_retry_ready)
            and duration_retry_ready(state, runtime) is True):
        return 'START'
    if not runtime and state.get('stage_status') == 'stage3_infrastructure_failure' and state.get('next_action') == 'inspect_stage3_infrastructure_failure':
        return 'START'
    if runtime and runtime.get('status') == 'START_REQUIRED':
        # A START_REQUIRED manifest can be relaunched only while its exact
        # authenticated terminal is still present (handled above), or after a
        # separate one-shot infrastructure repair receipt. A dead terminal is
        # resumed for failure classification; it is never silently re-started.
        return 'RESUME'
    return 'RESUME'

def cleanup_consumed_stale_runtime(state: dict, handoff: dict, terminal_pids: list[int],
                                   driver_module) -> bool:
    """Close a prior cycle's owned MT5 run after canonical rotation.

    A live terminal from a consumed Stage 3 failure must not block the next
    admitted cycle. Cleanup is allowed only when the failure event records the
    durable rotate action and the process identity is present in that runtime;
    the launcher revalidates every PID before stopping it.
    """
    if (state.get('current_stage') != 'stage3_pending'
            or not handoff.get('cycle_id')
            or handoff.get('cycle_id') == state.get('cycle_id')):
        return False
    runtime_path = Path(str(handoff.get('runtime_manifest', '')))
    if not runtime_path.is_file():
        return False
    runtime = json.loads(runtime_path.read_text(encoding='utf-8'))
    if (runtime.get('cycle_id') != handoff.get('cycle_id')
            or runtime.get('ea') != handoff.get('ea')
            or runtime.get('status') not in {'STARTING', 'OPTIMIZING', 'START_REQUIRED'}
            or int(runtime.get('terminal_pid') or 0) not in terminal_pids):
        return False
    failure_path = STATE / 'events' / f"{handoff['cycle_id']}.stage3.failed.json"
    if not failure_path.is_file():
        return False
    failure = json.loads(failure_path.read_text(encoding='utf-8'))
    if not (failure.get('cycle_id') == handoff.get('cycle_id')
            and failure.get('status') == 'failed'
            and failure.get('consumed') is True
            and failure.get('consumed_action') == 'rotate_stage1'):
        return False

    # cleanup_previous_runtime only permits cleanup for terminal statuses.
    # Use a copy with a terminal status; keep the preserved manifest marked as
    # stale-cycle cleanup after ownership-checked termination completes.
    cleanup_record = dict(runtime, status='INFRASTRUCTURE_FAILURE')
    terminal = Path(driver_module.launcher.DEFAULT_TERMINAL)
    driver_module.launcher.cleanup_previous_runtime(
        cleanup_record, terminal, runtime_path, include_worker_process_evidence=True)
    runtime['status'] = 'STALE_CYCLE_CLEANED'
    runtime['stale_cleanup_utc'] = driver_module.utc_now()
    runtime['stale_cleanup_reason'] = 'canonical Stage 3 failure was consumed and the cycle rotated'
    driver_module.atomic_json(runtime_path, runtime)
    return True

def main() -> int:
    os.chdir(ROOT)
    sys.path.insert(0,str(STATE))
    from kcos_runtime_paths_v2 import assert_safe_root
    assert_safe_root(ROOT)
    import kcos_stage3_driver_v2 as driver
    state = driver.load_object(driver.STATE)
    attempt = state.get('stage2_release_attempt')
    if (state.get('current_stage') == 'stage3_pending'
            and state.get('stage_status') == 'stage2_complete_held'
            and state.get('hold_after_stage') == 2
            and state.get('next_action') == 'audit_stage2_before_stage3'
            and isinstance(attempt, dict)
            and attempt.get('cycle_id') == state.get('cycle_id')
            and attempt.get('status') in {'preflight_in_progress', 'attempt_in_progress'}):
        print(json.dumps({
            'status': 'STAGE2_RELEASE_IN_PROGRESS',
            'cycle_id': state.get('cycle_id'),
            'authorization_id': attempt.get('authorization_id'),
            'detail': 'another Stage 3 cron invocation owns the one-run Stage 2 release preflight',
            'zero_token': True,
        }, separators=(',', ':')))
        return 0
    runtime = None
    h = driver.load_object(driver.HANDOFF) if driver.HANDOFF.is_file() else {}
    if h:
        if h.get('cycle_id') == state.get('cycle_id') and h.get('ea') == state.get('current_ea'):
            path = Path(h.get('runtime_manifest',''))
            if path.is_file():
                candidate = driver.load_object(path)
                if candidate.get('cycle_id') == state.get('cycle_id') and candidate.get('ea') == state.get('current_ea'):
                    runtime = candidate
    terminal = STATE/'mt5_stage45_portable'/'terminal64.exe'
    pids=[]
    for p in psutil.process_iter(['pid','name','exe']):
        if str(p.info.get('name') or '').casefold() == 'terminal64.exe':
            try:
                if Path(p.exe()).resolve() == terminal.resolve():pids.append(p.pid)
            except psutil.Error:
                # Ambiguous portable ownership must never trigger a restart.
                print(json.dumps({'status':'OWNERSHIP_UNAVAILABLE','zero_token':True}))
                return 0
    try:
        recovered = recover_overwritten_h1_runtime(
            state, h, runtime,
            Path(h.get('runtime_manifest', '')) if h else Path(),
            terminal, pids, driver)
        if recovered is not None:
            runtime = recovered
    except (OSError, ValueError, TypeError, KeyError, psutil.Error) as exc:
        print(json.dumps({'status':'H1_RUNTIME_RECOVERY_HELD',
                          'cycle_id':state.get('cycle_id'),
                          'detail':str(exc),'zero_token':True}))
        return 0
    if (runtime and h
            and driver.classify_dead_optimizing_runtime(driver.STATE, driver.HANDOFF, runtime, pids)):
        state = driver.load_object(driver.STATE)
        runtime_path = Path(h.get('runtime_manifest', ''))
        runtime = driver.load_object(runtime_path) if runtime_path.is_file() else runtime
        print(json.dumps({'status':'STAGE3_DEAD_RUNTIME_HELD',
                          'cycle_id':state.get('cycle_id'),
                          'timeframe':runtime.get('timeframe'),
                          'zero_token':True}))
        return 0
    try:
        cleanup_consumed_stale_runtime(state, h, pids, driver)
    except (OSError, ValueError, psutil.Error) as exc:
        print(json.dumps({'status':'STALE_RUNTIME_CLEANUP_HELD','detail':str(exc),
                          'cycle_id':state.get('cycle_id'),'zero_token':True}))
        return 0
    # Refresh after any verified cleanup; a prior-cycle terminal is no longer
    # allowed to shadow the currently admitted Stage 3 cycle.
    pids = []
    for p in psutil.process_iter(['pid','name','exe']):
        if str(p.info.get('name') or '').casefold() == 'terminal64.exe':
            try:
                if Path(p.exe()).resolve() == terminal.resolve(): pids.append(p.pid)
            except psutil.Error:
                print(json.dumps({'status':'OWNERSHIP_UNAVAILABLE','zero_token':True}))
                return 0
    action=choose_action(state,runtime,pids,driver,h)
    if action in {'NOOP_OTHER_STAGE','ALREADY_RUNNING_EXTERNAL'}:
        print(json.dumps({'status':action,'cycle_id':state.get('cycle_id'),'terminal_pids':pids,'zero_token':True}))
        return 0
    if action == 'HOLD_STAGE3_INFRASTRUCTURE_FAILURE':
        print(json.dumps({'status':action,'cycle_id':state.get('cycle_id'),
                          'detail':'supervisor must authorize recovery before Stage 3 can start again',
                          'zero_token':True}))
        return 0
    if action=='START' and runtime and runtime_has_live_owned_worker(runtime):
        print(json.dumps({'status':'ALREADY_RUNNING','detail':'a worker matching the runtime PID, creation time, and executable is still alive','zero_token':True}))
        return 0
    driver_args = (['--restart-stale-timeframe'] if action == 'RESTART_STALE' else
                   ['--retry-unstarted-terminal'] if action == 'RETRY_UNSTARTED_TERMINAL' else
                   ['--start'] if action == 'START' else [])
    if action == 'START' and driver.should_attempt_stage2_release(state):
        driver_args.append('--release-stage2-hold')
    sys.argv=[str(STATE/'kcos_stage3_driver_v2.py')]+driver_args
    return driver.main()

if __name__=='__main__':raise SystemExit(main())
