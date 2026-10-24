'''Hermes shim for canonical KCOS code; runtime imports resolve from state first.'''
from pathlib import Path
import os
import runpy
import sys

CANONICAL_ROOT = Path(os.environ.get("KCOS_ROOT", r"C:\Users\Tyson\Projects\kiyosaki_stage2b")).resolve()
CANONICAL_STATE = CANONICAL_ROOT / "state"
CANONICAL_ENTRYPOINT = CANONICAL_STATE / "kcos_stage2_autonomous_executor.py"
# This no-agent cron's pinned route is otherwise not inherited by the inner
# bounded repair proposal call. Keep that call aligned with the Stage 2 job.
os.environ.setdefault("KCOS_STAGE2_REPAIR_MODEL", "gpt-6-luna")
os.environ.setdefault("KCOS_STAGE2_REPAIR_PROVIDER", "openai-codex")
sys.path.insert(0, str(CANONICAL_STATE))
runpy.run_path(str(CANONICAL_ENTRYPOINT), run_name="__main__")
