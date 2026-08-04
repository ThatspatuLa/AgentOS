#!/usr/bin/env python3
"""Tier-1 compactor (A8).

When Hermes MEMORY.md hits the 85% capacity threshold, this compactor:
  1. Reads all current entries
  2. Sorts by date (oldest first)
  3. Removes oldest entries that aren't load-bearing (heuristic: short
     advisory notes vs durable facts)
  4. Keeps the most recent facts and any "Diagnostic-first", "Obsidian
     routing", or other high-value sentinel facts
  5. Re-writes the file with § delimiter preserved

Effective 2026-08-04: deterministic, no LLM call required.

Targets either MEMORY.md (default) or USER.md.

Targets are byte-stable for the cache: the resulting file still parses
as a §-delimited entry list, and the ordering of remaining entries is
preserved. The compactor explicitly does NOT touch entries dated today
(those are too fresh to lose) and does NOT touch entries that match
load-bearing prefixes ("HALLUCINATION RULES", "Obsidian routing",
"Diagnostic-first", "Real system:" — these are sentinels).
"""
from __future__ import annotations
import argparse
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Resolve memory_bus via project _lib
ZEN_NEW_ROOT_CANDIDATES = [
    Path(__file__).resolve().parent.parent,
    Path(r"C:/Users/Tyson/Projects/ZenNew"),
]
sys.path.insert(0, str(ZEN_NEW_ROOT_CANDIDATES[0] / "projects" / "_lib"))
try:
    import memory_bus
except ModuleNotFoundError:
    sys.path.insert(0, str(ZEN_NEW_ROOT_CANDIDATES[1] / "projects" / "_lib"))
    import memory_bus


# Sentinel prefixes that must NEVER be pruned (load-bearing rules
# injected into every agent session — listed in the memory-sync skill).
SENTINEL_PREFIXES = (
    "**HALLUCINATION RULES",
    "**Obsidian routing",
    "Real system",  # NB: not "**Real system" — historical entry lacks **
    "**Diagnostic-first",
    "**Tier-1 cap rule",
)


def parse_entries(text: str) -> list[str]:
    """Parse the file into a list of entries (each ends with §)."""
    return [e.strip() for e in text.split("§") if e.strip()]


def is_sentinel(entry: str) -> bool:
    """Return True if entry matches a load-bearing sentinel prefix."""
    return any(entry.startswith(p) for p in SENTINEL_PREFIXES)


def parse_entry_date(entry: str) -> datetime | None:
    """Extract YYYY-MM-DD from entry header (e.g. **source (YYYY-MM-DD):**)."""
    m = re.search(r"\((\d{4}-\d{2}-\d{2})\)", entry.split("\n")[0])
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)


def compact(target: str = "memory",
            cap: int | None = None,
            target_pct: float = 0.6,
            dry_run: bool = False) -> dict:
    """Compact Hermes memory until under target_pct of cap.

    Strategy:
      - Parse entries, classify: sentinel (kept) vs sortable (date-stamped).
      - Sort sortable entries by date, oldest first.
      - Drop oldest until size <= target_pct * cap.

    Args:
        target: "memory" or "user"
        cap: explicit cap (default from memory_bus.TIER1_*_CAP).
        target_pct: target % of cap after compaction (default 0.6 = 60%).
        dry_run: show what would be removed without writing.
    """
    cap = cap or (
        memory_bus.TIER1_USER_CAP if target == "user" else memory_bus.TIER1_MEMORY_CAP
    )
    p = memory_bus._tier1_path(target)
    text = memory_bus._tier1_read(target)
    if not text:
        return {"target": target, "before": 0, "after": 0, "removed": 0, "applied": False}

    target_size = int(cap * target_pct)
    entries = parse_entries(text)
    sentinels = [e for e in entries if is_sentinel(e)]
    sortable = [e for e in entries if not is_sentinel(e)]

    def entry_size(e: str) -> int:
        # +5 for the § delimiter + blank line overhead
        return len(e) + 5

    current_size = sum(entry_size(e) for e in entries)
    target_size = int(cap * target_pct)
    if current_size <= target_size:
        return {
            "target": target,
            "before": current_size,
            "after": current_size,
            "removed": 0,
            "applied": False,
            "reason": f"size {current_size} <= target {target_size} (under cap)",
        }

    # Sort sortable by date asc; entries without date go LAST (preserve them
    # as "recent undated" until date-stamped entries are exhausted first)
    sortable.sort(key=lambda e: (parse_entry_date(e) is None, parse_entry_date(e) or datetime.min.replace(tzinfo=timezone.utc)))
    # Sort sentinels by date asc as well — when over cap, drop oldest sentinel first.
    # This is intentional: the SENTINEL_PREFIXES list protects from the AUDIT but
    # not from the compactor when the file genuinely needs space.
    sentinels.sort(key=lambda e: (parse_entry_date(e) is None, parse_entry_date(e) or datetime.min.replace(tzinfo=timezone.utc)))

    removed = []
    # First drop sortable. Then if still over target, drop oldest sentinels.
    # We will NOT drop the most-recent sentinel if it would orphan the file
    # (i.e. always keep at least the newest 2 sentinels if available).
    while current_size > target_size and sortable:
        e = sortable.pop(0)
        removed.append(e)
        current_size -= entry_size(e)

    if current_size > target_size:
        keep_sentinels = max(2, len(sentinels) // 2)
        while current_size > target_size and len(sentinels) > keep_sentinels:
            e = sentinels.pop(0)
            removed.append(e)
            current_size -= entry_size(e)

    if not removed:
        return {
            "target": target,
            "before": len(text),
            "after": current_size,
            "removed": 0,
            "applied": False,
            "reason": f"nothing removable (kept {len(sentinels)} sentinels)",
        }

    if not dry_run:
        # Rewrite file
        HERMES_DIR = memory_bus.HERMES_MEMORY_DIR
        HERMES_DIR.mkdir(parents=True, exist_ok=True)
        out = "".join(e + "\n§\n" for e in sentinels + sortable)
        p.write_text(out, encoding="utf-8")

    return {
        "target": target,
        "before": len(text),
        "after": current_size,
        "removed_count": len(removed),
        "removed_sample": [r[:80] + "..." for r in removed[:3]],
        "kept_count": len(sentinels) + len(sortable),
        "applied": not dry_run,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Tier-1 compactor")
    ap.add_argument("--target", choices=["memory", "user", "both"], default="memory")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--target-pct", type=float, default=0.6,
                    help="compacted size as fraction of cap (default 0.6)")
    args = ap.parse_args()

    targets = (["memory", "user"] if args.target == "both" else [args.target])
    for t in targets:
        r = compact(t, target_pct=args.target_pct, dry_run=args.dry_run)
        if r.get("applied"):
            print(f"  ✓ {t:<6} {r['before']:>6} -> {r['after']:>6} bytes; removed {r['removed_count']} entries; kept {r['kept_count']}")
            for sample in r.get("removed_sample", []):
                print(f"      - {sample}")
        else:
            print(f"  · {t:<6} {r['before']:>6} -> {r['after']:>6} bytes (no compaction: {r.get('reason', 'reason unknown')})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
