from __future__ import annotations
import importlib.util
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(r"C:\Users\Tyson\Projects\ZenNew")
TMP = Path(tempfile.mkdtemp(prefix="zen_memory_integration_"))
HERMES_HOME = TMP / "hermes"
ZEN_DATA = TMP / "data"
OBSIDIAN = TMP / "vault"
os.environ["HERMES_HOME"] = str(HERMES_HOME)
os.environ["ZEN_DATA_ROOT"] = str(ZEN_DATA)
os.environ["OBSIDIAN_VAULT_DIR"] = str(OBSIDIAN)
sys.path.insert(0, str(ROOT / "projects" / "_lib"))
import memory_bus

spec = importlib.util.spec_from_file_location("tier1_compactor", ROOT / "scripts" / "tier1_compactor.py")
compactor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compactor)

results = {}
project = "zen"
t2 = memory_bus.memory_path(project)
t3 = memory_bus.obsidian_memory_path(project)

# Tier routing: real note -> Tier 2 only; Obsidian remains unchanged.
t3.parent.mkdir(parents=True, exist_ok=True)
t3.write_text("OBSIDIAN_SENTINEL", encoding="utf-8")
r = memory_bus.append_memory_note(project, "Durable integration decision: use three-tier memory routing.")
results["tier2_only"] = bool(r["data"] and not r["obsidian"] and "three-tier" in t2.read_text(encoding="utf-8") and t3.read_text(encoding="utf-8") == "OBSIDIAN_SENTINEL")

# Noise filter writes nowhere.
before_t2 = t2.read_bytes()
r = memory_bus.append_memory_note(project, "PONG")
results["noise_filter"] = bool(r["filtered"] and t2.read_bytes() == before_t2 and t3.read_text(encoding="utf-8") == "OBSIDIAN_SENTINEL")

# Tier 1 write and content-hash dedup.
fact = "Integration sentinel alpha-4827 survives cold-start injection."
r1 = memory_bus.persist_fact(fact, source="integration")
r2 = memory_bus.persist_fact(fact, source="integration")
results["tier1_write"] = bool(r1["written"] and fact in memory_bus.HERMES_MEMORY_PATH.read_text(encoding="utf-8"))
results["tier1_dedup"] = bool(r2["duplicate"] and not r2["written"])

# Watchdog refuses a normal write at >=85%.
fill = "**load (2026-07-01):** " + ("x" * 1900) + "\n§\n"
memory_bus.HERMES_MEMORY_PATH.write_text(fill, encoding="utf-8")
r = memory_bus.persist_fact("must be refused", source="integration")
results["watchdog"] = bool(not r["written"] and "compactor required" in (r["reason"] or ""))

# Compactor: sentinel survives, oldest disposable notes removed, under 60%.
entries = [
    "**Obsidian routing (2026-07-01):** protected sentinel",
    "**old (2026-07-01):** " + "a" * 500,
    "**old (2026-07-02):** " + "b" * 500,
    "**new (2026-08-03):** newest fact one",
    "**new (2026-08-04):** newest fact two",
]
memory_bus.HERMES_MEMORY_PATH.write_text("".join(e + "\n§\n" for e in entries), encoding="utf-8")
cr = compactor.compact("memory", cap=1200, target_pct=0.6, dry_run=False)
after = memory_bus.HERMES_MEMORY_PATH.read_text(encoding="utf-8")
results["compactor"] = bool(cr["applied"] and len(after) <= 720 and "protected sentinel" in after and "newest fact one" in after and "newest fact two" in after)

# Explicit Tier 3 path works only when called.
results["explicit_tier3"] = bool(memory_bus.patch_obsidian_project_memory(project, "CURATED_BLOCK") and "CURATED_BLOCK" in t3.read_text(encoding="utf-8"))

out = {"temp_root": str(TMP), "results": results, "passed": sum(results.values()), "total": len(results), "compactor": cr}
print(json.dumps(out, indent=2))
shutil.rmtree(TMP)
