"""Load the canonical KCOS Stage2 generator with its matching state modules."""
from __future__ import annotations

import sys
from importlib.util import spec_from_file_location
from pathlib import Path


_CANONICAL = Path(
    r"C:\Users\Tyson\Projects\kiyosaki_stage2b\state\kcos_stage2_generate_source_v2.py"
)
if not _CANONICAL.is_file():
    raise ImportError(f"canonical KCOS Stage2 generator is missing: {_CANONICAL}")

_STATE_DIR = str(_CANONICAL.parent)
if _STATE_DIR in sys.path:
    sys.path.remove(_STATE_DIR)
sys.path.insert(0, _STATE_DIR)

_MODULE = sys.modules[__name__]
_SPEC = spec_from_file_location(__name__, _CANONICAL)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError(f"cannot load canonical KCOS Stage2 generator: {_CANONICAL}")
_MODULE.__file__ = str(_CANONICAL)
_MODULE.__spec__ = _SPEC
_SPEC.loader.exec_module(_MODULE)
