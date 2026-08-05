#!/usr/bin/env python3
"""Daily Tier-2 -> Tier-1 consolidator.

Effective 2026-08-04 (A4): prevent per-project memory.md from growing
forever by promoting the most-important recent note per project to
Tier 1 (Hermes MEMORY.md).

Strategy: for each project, find the most recent Tier-2 note that's
longer than 200 chars (skips ack-style noise), run a quick importance
heuristic (mentions key terms like "decided", "phase 2b", "config",
"crashed", "outage"), and promote to MEMORY.md via persist_fact().

Conservative: only promotes ONE note per project per day, and persist_fact
has its own dedup so re-promotes of the same fact are silently dropped.
"""
from __future__ import annotations
import re
import sys
from pathlib import Path

# Resolve memory_bus — check both the canonical ZenNew location and the
# scripts-dir fallback (in case this was copied into ~/.hermes/scripts/).
ZEN_NEW_ROOT_CANDIDATES = [
    Path(__file__).resolve().parent.parent,                   # standard ZenNew/scripts
    Path(r"C:/Users/Tyson/Projects/ZenNew"),                   # absolute fallback
]
sys.path.insert(0, str(ZEN_NEW_ROOT_CANDIDATES[0] / "projects" / "_lib"))
try:
    import memory_bus  # noqa: E402
except ModuleNotFoundError:
    sys.path.insert(0, str(ZEN_NEW_ROOT_CANDIDATES[1] / "projects" / "_lib"))
    import memory_bus  # noqa: E402

# Importance heuristic — words that signal durable knowledge worth promoting
IMPORTANCE_KEYWORDS = re.compile(
    r"\b(decided|decision|step|phase|staged|config|configured|tuned|"
    r"crashed|outage|fixed|resolved|broke|broken|rollback|deploy|deployed|"
    r"version|v\d+\.\d+|schema|migration|completed|finished|"
    r"gates|budget|threshold|cap|ceiling|won|lost|killed|"
    r"freqtrade|minato|kiyosaki|zenbot|hermes|cron|session|"
    r"promote|demote|shipped|merged)\b",
    re.IGNORECASE,
)

NOTE_BLOCK = re.compile(
    r"^### (\d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC)\s*$(.+?)(?=^### |\Z)",
    re.MULTILINE | re.DOTALL,
)


def _best_note(notes: list[tuple[str, str]]) -> tuple[str, str] | None:
    """Pick the most important recent note. Returns (ts, body) or None."""
    if not notes:
        return None
    # Score by importance keyword hits + length (longer = more substance)
    scored = []
    for ts, body in notes:
        score = len(body) + len(IMPORTANCE_KEYWORDS.findall(body)) * 50
        scored.append((score, ts, body))
    scored.sort(key=lambda x: -x[0])
    return (scored[0][1], scored[0][2])


def _extract_notes(mem_path: Path) -> list[tuple[str, str]]:
    """Return [(timestamp, body)] for all note blocks in a memory.md."""
    if not mem_path.exists():
        return []
    text = mem_path.read_text(encoding="utf-8")
    notes = []
    for m in NOTE_BLOCK.finditer(text):
        body = m.group(2).strip()
        notes.append((m.group(1), body))
    return notes


def consolidate_project(project: str, *, dry_run: bool = False,
                        min_length: int = 60) -> dict:
    """Consolidate Tier-2 notes for one project. Returns summary stats."""
    mem_path = memory_bus.memory_path(project)
    notes = _extract_notes(mem_path)
    if not notes:
        return {"project": project, "promoted": False, "reason": "no notes"}

    # Skip the oldest 90% to prefer recent context, then pick best of top
    recent = notes[-max(1, len(notes) // 10):]

    # Filter out noise + short acks before scoring
    filtered = [
        (ts, body) for ts, body in recent
        if not memory_bus.is_test_noise(body)
        and len(body) >= min_length
    ]
    if not filtered:
        return {"project": project, "promoted": False, "reason": "no qualified candidate"}

    chosen = _best_note(filtered)
    if not chosen:
        return {"project": project, "promoted": False, "reason": "no candidate"}

    ts, body = chosen
    # Truncate the body for Tier-1 (memory is size-constrained). Take the
    # middle of the note (skip prefix/suffix noise).
    if len(body) > 240:
        body = body[:80] + " ... " + body[-80:]

    fact = f"[{project}] ({ts}) {body}"

    if dry_run:
        return {"project": project, "promoted": False, "dry_run_candidate": fact}

    result = memory_bus.persist_fact(fact, source=f"consolidator/{project}")
    return {
        "project": project,
        "promoted": result["written"],
        "duplicate": result["duplicate"],
        "reason": result["reason"],
    }


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Consolidate Tier-2 to Tier-1")
    ap.add_argument("--project", help="only run for this project")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--all", action="store_true", default=True,
                    help="consolidate all projects (default when no --project given)")
    args = ap.parse_args()

    if args.project:
        targets = [args.project]
    else:
        # --all is default; covers every project
        targets = (
            [args.project] if args.project else memory_bus.KNOWN_PROJECTS
        )
    promoted = 0
    duplicates = 0
    skipped = 0
    for proj in targets:
        r = consolidate_project(proj, dry_run=args.dry_run)
        if r.get("dry_run_candidate"):
            print(f"  [{proj}] WOULD promote: {r['dry_run_candidate'][:120]}...")
        elif r.get("promoted"):
            print(f"  [{proj}] ✓ promoted")
            promoted += 1
        elif r.get("duplicate"):
            print(f"  [{proj}] ✓ already known (duplicate)")
            duplicates += 1
        else:
            print(f"  [{proj}] → skip ({r.get('reason','?')})")
            skipped += 1

    print()
    print(f"Summary: promoted={promoted} duplicates={duplicates} skipped={skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
