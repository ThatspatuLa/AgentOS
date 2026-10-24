#!/usr/bin/env python3
"""Record silent Stage 2 failure patterns without dispatching notifications."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from kcos_runtime_paths_v2 import assert_safe_root

ROOT = assert_safe_root()
REGISTRY = ROOT / "state" / "stage2_failure_patterns_v2.json"
OBSIDIAN = ROOT / "state" / "reports" / "stage2-structural-failures.md"


def normalized(value: str) -> str:
    result = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return result[:100] or "unknown_failure"


def record(cycle: str, ea: str, category: str, detail: str, attempt: int, exhausted: bool) -> dict:
    key = normalized(category)
    registry = json.loads(REGISTRY.read_text(encoding="utf-8")) if REGISTRY.is_file() else {"schema_version": 2, "failures": []}
    entry = {
        "utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cycle_id": cycle,
        "ea": ea,
        "category": key,
        "detail": detail,
        "attempt": attempt,
        "exhausted": exhausted,
    }
    registry["failures"].append(entry)
    distinct = sorted({item["ea"] for item in registry["failures"] if item["category"] == key and item.get("exhausted")})
    entry["distinct_strategy_count"] = len(distinct)
    entry["deeper_review_required"] = len(distinct) >= 2
    REGISTRY.write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")
    if exhausted:
        OBSIDIAN.parent.mkdir(parents=True, exist_ok=True)
        with OBSIDIAN.open("a", encoding="utf-8") as handle:
            handle.write(
                f"\n## {ea} — {entry['utc']}\n\n"
                f"- Cycle: `{cycle}`\n- Failure: `{key}`\n- Attempts: {attempt}\n"
                f"- Detail: {detail}\n- Distinct strategies with this failure: {len(distinct)}\n"
                f"- Deeper system review: {'required' if entry['deeper_review_required'] else 'not yet'}\n"
                "- Notification: suppressed by operator policy\n"
            )
    return entry


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycle", required=True)
    parser.add_argument("--ea", required=True)
    parser.add_argument("--category", required=True)
    parser.add_argument("--detail", required=True)
    parser.add_argument("--attempt", required=True, type=int)
    parser.add_argument("--exhausted", action="store_true")
    args = parser.parse_args()
    print(json.dumps(record(args.cycle, args.ea, args.category, args.detail, args.attempt, args.exhausted)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
