"""Six 2026-09-15: recover Stage 3 timeframe from journal evidence.

Generic version: takes timeframe as argument. Reads journal completion
line, writes TIMEFRAME_COMPLETE event, updates runtime status."""
import json
import hashlib
import re
import shutil
import sys
from pathlib import Path

ROOT = Path('C:/Users/Tyson/Projects/kiyosaki_stage2b')
STATE = ROOT / 'state'
CYCLE = 'kcos-20260915T174500AEST-bollingerbandreversion_v1-restart-batch'
TIMEFRAME = sys.argv[1] if len(sys.argv) > 1 else 'M5'
RUNTIME_PATH = STATE / 'stage3_v2' / (CYCLE + '.stage3.runtime.json')
EVENT_PATH = STATE / 'stage3_v2' / (CYCLE + '.stage3.' + TIMEFRAME + '.complete.json')
JOURNAL = Path('C:/Users/Tyson/AppData/Roaming/MetaQuotes/Terminal/608AB61EFF9C7B3585EC08B8CF6800E3/Tester/logs/20260915.log')
CACHE = Path('C:/Users/Tyson/AppData/Roaming/MetaQuotes/Terminal/608AB61EFF9C7B3585EC08B8CF6800E3/Tester/cache')

journal_text = JOURNAL.read_text(encoding='utf-16le', errors='ignore')
m = re.search(r'genetic optimization finished on pass (\d+)', journal_text)
if not m:
    raise SystemExit('journal: no completion line found')
completed_passes = int(m.group(1))

m2 = re.search(r'shortest pass (\d+):(\d+):(\d+\.\d+), longest pass (\d+):(\d+):(\d+\.\d+), average pass (\d+):(\d+):(\d+\.\d+)', journal_text)
if m2:
    def hms(h, mi, s):
        return int(h) * 3600 + int(mi) * 60 + float(s)
    elapsed_seconds = hms(m2.group(7), m2.group(8), m2.group(9))
else:
    elapsed_seconds = None

# Find .opt for this timeframe
opt_candidates = list(CACHE.glob('B1_1_BollingerBandReversion_v1.ETHUSD.' + TIMEFRAME + '.20250105.20260105.*.opt'))
opt_path = max(opt_candidates, key=lambda p: p.stat().st_mtime)
opt_sha = hashlib.sha256(opt_path.read_bytes()).hexdigest().upper()

if EVENT_PATH.exists():
    shutil.copy(EVENT_PATH, EVENT_PATH.with_suffix('.json.bak'))

event = {
    'schema_version': 2,
    'event_id': CYCLE + ':stage3:' + TIMEFRAME + ':complete',
    'cycle_id': CYCLE,
    'stage': 3,
    'timeframe': TIMEFRAME,
    'status': 'TIMEFRAME_COMPLETE',
    'ea': 'B1_1_BollingerBandReversion_v1',
    'opt': str(opt_path.resolve()),
    'opt_sha256': opt_sha,
    'completed_passes': completed_passes,
    'elapsed_seconds': elapsed_seconds,
    'completed_utc': '2026-09-15T12:00:51Z',
    'consumed': False,
    'recovery_note': 'Six 2026-09-15: recovered from watcher handshake timeout; ' + TIMEFRAME + ' actually ran to completion per journal evidence.',
    'source': 'kcos_stage3_recover.py',
}
EVENT_PATH.write_text(json.dumps(event, indent=2))
print('wrote', TIMEFRAME, 'complete event:', EVENT_PATH)

runtime = json.loads(RUNTIME_PATH.read_text())
runtime['status'] = 'TIMEFRAME_COMPLETE'
runtime['timeframe_event'] = str(EVENT_PATH.resolve())
runtime['completion_reason'] = 'genetic_optimization_finished'
runtime['completed_passes'] = completed_passes
runtime['elapsed_seconds'] = elapsed_seconds
runtime['opt_path'] = str(opt_path.resolve())
runtime['opt_sha256'] = opt_sha
runtime['timeframe'] = TIMEFRAME
runtime['recovered_by'] = 'kcos_stage3_recover.py at 2026-09-15T22:15 AEST'
RUNTIME_PATH.write_text(json.dumps(runtime, indent=2))
print('runtime updated for', TIMEFRAME, '(passes=' + str(completed_passes) + ')')
