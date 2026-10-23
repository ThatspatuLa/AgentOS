import ast
from pathlib import Path

import pytest

import kcos_stage2_repair_loop_v1 as repair_loop
import kcos_stage2_repair_registry_v1 as registry


def test_every_active_deterministic_handler_is_registered():
    tree = ast.parse(Path(repair_loop.__file__).read_text(encoding="utf-8-sig"))
    active = {
        node.func.id for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id.startswith("apply_")
    }
    assert active <= set(registry.HANDLER_TO_RULE)
    registry.validate_registry()


def test_registry_rejects_handler_mismatch():
    definition = registry.REPAIR_REGISTRY["known_mql5_compile"]
    with pytest.raises(ValueError, match="handler mismatch"):
        registry.registered_repair("known_mql5_compile", repair_loop.apply_kcos_regime_enum_order_fix,
                                   object(), "diagnostics")
    assert definition.handler == "apply_known_mql5_compile_fix"
