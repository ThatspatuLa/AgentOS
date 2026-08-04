#!/usr/bin/env python3
"""Memory hygiene — weekly Sunday scan for memory drift + auto-fix.

Compares:
  - Hermes MEMORY.md (auto-injected system prompt, Tier 1)
  - Obsidian Project Memories/*.md (human-readable Tier 3)
  - data/sessions/*/memory.md (per-project Tier 2 log)

Reports:
  - Files last-modified > 7 days ago (stale)
  - Files with size 0 or 1 char (empty)
  - Capacity: Tier-1 MEMORY.md % of budget

Effective 2026-08-04 (A5):
  --fix mode now does:
    a) Deduplicate consecutive identical entries in Tier 2 (data/sessions)
    b) Drop Tier 2 entries that match the noise filter
    c) Print a Tier-3 proposal block (Six runs --propose to apply)

Output is concise table. Six reads it once on Sunday morning.
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Paths (override via env)
HERMES_HOME = Path(os.environ.get("HERMES_HOME", r"C:\Users\Tyson\AppData\Local\hermes"))
OBSIDIAN_VAULT = Path(os.environ.get("OBSIDIAN_VAULT_DIR", r"C:\Users\Tyson\Obsidian\ZenVault"))
ZEN_DATA = Path(os.environ.get("ZEN_DATA_ROOT", r"C:\Users\Tyson\Projects\ZenNew\data"))

HERMES_MEMORY = HERMES_HOME / "memories" / "MEMORY.md"
OBSIDIAN_MEMORY_DIR = OBSIDIAN_VAULT / "00_System" / "Project Memories"
DATA_MEMORY_DIR = ZEN_DATA / "sessions"

KNOWN_PROJECTS = [
    "zen", "zen-os", "kiyosaki", "minato",
    "nami", "rin", "toji", "kazuki",
]

# Same noise filter as memory_bus — keep in sync.
NOISE_PATTERNS = [
    re.compile(r"^reply with just (?:pong|ping|test|cache|zen)\.?$", re.IGNORECASE),
    re.compile(r"^reply with (?:word\s+)?(pong|ping|test|cache|zen)\.?$", re.IGNORECASE),
    re.compile(r"^pong\s*test", re.IGNORECASE),
    re.compile(r"^(?:pong|ping)\.?$", re.IGNORECASE),
    re.compile(r"^(?:pong|ping)$", re.IGNORECASE),
    re.compile(r"^test\s+\d*$", re.IGNORECASE),
    re.compile(r"^test$", re.IGNORECASE),
    re.compile(r"^(?:zen|cache)$", re.IGNORECASE),
]


def _is_noise_body(body: str) -> bool:
    """Same logic as memory_bus.is_test_noise but standalone."""
    if not body:
        return True
    stripped = body.strip()
    m = re.search(r"^>\s*(.+)$", stripped, re.MULTILINE)
    payload = m.group(1).strip() if m else stripped
    # Also strip "Discord user X said:" prefix
    return any(p.match(payload) for p in NOISE_PATTERNS)


NOTE_HEADER = re.compile(r"^### (\d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC)\s*$")


def _file_age_days(p: Path) -> Optional[float]:
    if not p.exists():
        return None
    mtime = p.stat().st_mtime
    return (datetime.now(timezone.utc).timestamp() - mtime) / 86400


def _fmt_age(days: Optional[float], stale_threshold: float = 7.0) -> str:
    if days is None:
        return "missing"
    if days < 1:
        return f"{days * 24:.1f}h"
    return f"{days:.0f}d" + (" [STALE]" if days > stale_threshold else "")


def _dedupe_consecutive(text: str) -> tuple[str, int]:
    """Remove consecutive duplicate note blocks.

    Two blocks are duplicates if their bodies (excluding the timestamp
    header) are byte-identical. Returns (new_text, removed_count).
    """
    lines = text.split("\n")
    blocks: list[tuple[str, list[str]]] = []  # (header, body_lines)
    cur_header: Optional[str] = None
    cur_body: list[str] = []
    for line in lines:
        if NOTE_HEADER.match(line):
            if cur_header is not None:
                blocks.append((cur_header, cur_body))
            cur_header = line
            cur_body = []
        elif cur_header is not None:
            cur_body.append(line)
        else:
            # Lines before any header — pass through unchanged
            pass
    if cur_header is not None:
        blocks.append((cur_header, cur_body))

    out_lines: list[str] = []
    prev_body_text: Optional[str] = None
    removed = 0
    for header, body_lines in blocks:
        body_text = "\n".join(body_lines).strip()
        if prev_body_text is not None and body_text == prev_body_text:
            removed += 1
            continue
        out_lines.append(header)
        out_lines.extend(body_lines)
        # Preserve a blank separator between blocks
        out_lines.append("")
        prev_body_text = body_text
    # Re-emit without the trailing extra blank
    if out_lines and out_lines[-1] == "":
        out_lines.pop()
    return "\n".join(out_lines), removed


def _drop_noise(text: str) -> tuple[str, int]:
    """Remove note blocks whose payload matches NOISE_PATTERNS."""
    out_lines = []
    i = 0
    removed = 0
    lines = text.split("\n")
    while i < len(lines):
        line = lines[i]
        if NOTE_HEADER.match(line):
            header = line
            body_lines = []
            i += 1
            while i < len(lines) and not NOTE_HEADER.match(lines[i]):
                body_lines.append(lines[i])
                i += 1
            body = "\n".join(body_lines)
            if _is_noise_body(body):
                removed += 1
                continue
            out_lines.append(header)
            out_lines.extend(body_lines)
        else:
            out_lines.append(line)
            i += 1
    return "\n".join(out_lines), removed


def fix_project(proj: str) -> dict:
    """Apply --fix to a single project's Tier 2 memory. Returns summary."""
    p = DATA_MEMORY_DIR / proj / "memory.md"
    if not p.exists():
        return {"project": proj, "removed_dups": 0, "removed_noise": 0, "applied": False}

    text = p.read_text(encoding="utf-8")
    text2, n_dups = _dedupe_consecutive(text)
    text3, n_noise = _drop_noise(text2)
    if text3 != text:
        p.write_text(text3, encoding="utf-8")
        return {"project": proj, "removed_dups": n_dups, "removed_noise": n_noise, "applied": True}
    return {"project": proj, "removed_dups": 0, "removed_noise": 0, "applied": False}


def cmd_report() -> int:
    print("# Memory Hygiene Report")
    print(f"_Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}_\n")

    print("## 1. Per-project memory files")
    print()
    print(f"{'Project':<10} {'Tier 3 Obsidian (age)':<22} {'Tier 2 data/sessions (age)':<26} {'Notes':<8}")
    print("-" * 75)
    for proj in KNOWN_PROJECTS:
        obs_path = OBSIDIAN_MEMORY_DIR / f"{proj.title()} Memory.md"
        dat_path = DATA_MEMORY_DIR / proj / "memory.md"
        obs_age = _file_age_days(obs_path)
        dat_age = _file_age_days(dat_path)
        n_notes = 0
        if dat_path.exists():
            n_notes = dat_path.read_text(encoding="utf-8").count("### ")
        status = "⚠" if (obs_age is None or obs_age > 7) and proj != "zen-os" else "✓"
        print(f"{status} {proj:<8} "
              f"{_fmt_age(obs_age):<22} "
              f"{_fmt_age(dat_age):<26} "
              f"{n_notes}")

    print()
    print("## 2. Tier 1 Hermes MEMORY.md (auto-injected)")
    print()
    if HERMES_MEMORY.exists():
        age = _file_age_days(HERMES_MEMORY)
        size = HERMES_MEMORY.stat().st_size
        cap = 2200
        pct = size / cap * 100
        lines = HERMES_MEMORY.read_text(encoding="utf-8").count("\n")
        print(f"  age: {_fmt_age(age)}")
        print(f"  size: {size}/{cap} bytes ({pct:.0f}%)")
        print(f"  entries: {lines // 2 if lines else 0}")
        if pct >= 85:
            print(f"  ⚠ [WARN] At capacity — A8 auto-summarizer should run.")
    else:
        print("  [MISSING]")
    print()
    return 0


def cmd_fix() -> int:
    print("# Memory Hygiene --fix Mode")
    print(f"_Run at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}_\n")

    total_applied = 0
    total_dups = 0
    total_noise = 0
    for proj in KNOWN_PROJECTS:
        r = fix_project(proj)
        if r["applied"]:
            print(f"  ✓ {proj:<10} removed_dups={r['removed_dups']:>3}  removed_noise={r['removed_noise']:>3}")
            total_applied += 1
            total_dups += r["removed_dups"]
            total_noise += r["removed_noise"]
        else:
            print(f"  · {proj:<10} (clean)")

    print()
    print(f"Summary: {total_applied} files modified, {total_dups} dup blocks removed, {total_noise} noise blocks removed")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Memory hygiene")
    ap.add_argument("--report", action="store_true", default=True, help="show drift report (default)")
    ap.add_argument("--fix", action="store_true", help="apply safe cleanup fixes")
    args = ap.parse_args()
    if args.fix:
        return cmd_fix()
    return cmd_report()


if __name__ == "__main__":
    sys.exit(main())