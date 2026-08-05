#!/usr/bin/env python3
"""Skill level-tracking + audit (effective 2026-08-04).

Scans:
  - All SKILL.md files in ~/.hermes/skills/ (and subdirs)
  - Hermes state.db for skill_view invocations
  - Output: SKILL_LEVEL.json with:
      {
        "<skill_name>": {
          "level": int,    # 1=novice, 2=regular, 3=trusted, 4=core
          "uses_30d": int,
          "uses_total": int,
          "last_used": "ISO timestamp" | null,
          "disposition": "keep" | "archive" | "delete" | "auto-disable",
          "tags": [list of categories from skills/<cat>/<name>],
          "size_chars": int,
          "first_seen": "ISO timestamp"
        }
      }

Level rules (counting only successful skill_view calls):
  1: 0 invocations ever                  -> novice / never-used
  2: 1-2 invocations                     -> casual
  3: 3-9 invocations                     -> regular
  4: 10+ invocations                     -> core / pinned

Disposition rules:
  - "core"       (level 4)               -> keep, never auto-archive
  - "regular"    (level 3)               -> keep, suggest pin
  - "casual"     (level 2)               -> keep, low priority
  - "novice"     (level 1) AND size<300c -> archive (move to _disabled/)
  - "novice"     (level 1) AND size>=300c -> manual review (Six decides)

Output JSON also includes 'summary' with totals and percentages.

Effective 2026-08-04: this is the source of truth for skill leveling.
"""
from __future__ import annotations
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

HERMES_HOME = Path(os.environ.get("HERMES_HOME", r"C:\Users\Tyson\AppData\Local\hermes"))
SKILLS_DIR = HERMES_HOME / "skills"
STATE_DB = HERMES_HOME / "state.db"
OUTPUT_JSON = SKILLS_DIR / "_meta" / "SKILL_LEVEL.json"

LEVEL_THRESHOLDS = [
    (0, 0, 1, "novice"),
    (1, 2, 2, "casual"),
    (3, 9, 3, "regular"),
    (10, float("inf"), 4, "core"),
]


def classify_level(uses: int) -> tuple[int, str]:
    for lo, hi, lvl, label in LEVEL_THRESHOLDS:
        if lo <= uses <= hi:
            return lvl, label
    return 1, "novice"


def discover_skills() -> dict[str, dict]:
    """Walk skills/ and return {skill_name: {path, size, category, sub_files}}."""
    found: dict[str, dict] = {}
    for skill_md in SKILLS_DIR.rglob("SKILL.md"):
        rel = skill_md.relative_to(SKILLS_DIR)
        # name is last component of the path (folder name usually)
        # But if SKILL.md is directly in skills/<cat>/<name>/SKILL.md, name is <name>
        # If at skills/<name>/SKILL.md, name is <name>
        parts = rel.parts[:-1]  # drop SKILL.md
        if not parts:
            continue
        # Last directory is the skill name
        skill_name = parts[-1]
        # If name matches a parent dir, we may be in a skill-folder
        # Category = second-to-last or 'top-level'
        category = parts[-2] if len(parts) >= 2 else "top-level"
        stat = skill_md.stat()
        # Approximate "skill" size: SKILL.md + any linked_files in skill/
        total_chars = stat.st_size
        # Walk one level deep to gather sub-files
        for f in skill_md.parent.rglob("*"):
            if f == skill_md or f.is_dir():
                continue
            try:
                total_chars += f.stat().st_size
            except OSError:
                pass
        first_seen_dt = datetime.fromtimestamp(stat.st_ctime, tz=timezone.utc)
        found[skill_name] = {
            "path": str(skill_md.parent),
            "rel": str(rel.parent),
            "category": category,
            "size_chars": total_chars,
            "first_seen": first_seen_dt.isoformat(),
        }
    return found


def query_skill_usage() -> dict[str, dict]:
    """Query state.db for skill_view invocations grouped by skill name.

    Hermes records skill_view in messages with tool_name='skill_view' and the
    skill name in tool_calls.arguments (JSON). Older sessions may store it
    in content; we handle both.
    """
    if not STATE_DB.exists():
        return {}
    db = sqlite3.connect(str(STATE_DB))
    counts_total: dict[str, int] = {}
    last_used: dict[str, float] = {}
    try:
        rows = db.execute("""
            SELECT tool_calls, content, timestamp
            FROM messages
            WHERE tool_name = 'skill_view' AND timestamp IS NOT NULL
            ORDER BY timestamp DESC
        """).fetchall()
    except sqlite3.OperationalError:
        return {}
    finally:
        db.close()

    for tool_calls_json, content, ts in rows:
        skill_name = None
        # Try parsing tool_calls first
        if tool_calls_json:
            try:
                tcs = json.loads(tool_calls_json)
                if isinstance(tcs, list):
                    for tc in tcs:
                        args = tc.get("function", {}).get("arguments", "{}")
                        args_d = json.loads(args) if isinstance(args, str) else args
                        if isinstance(args_d, dict) and "name" in args_d:
                            skill_name = args_d["name"]
                            break
            except (json.JSONDecodeError, KeyError, TypeError):
                pass
        # Fall back to content if not found
        if not skill_name and content:
            try:
                # Look for a tool call payload in content
                import re
                m = re.search(r'"name"\s*:\s*"([^"]+)"', content)
                if m:
                    skill_name = m.group(1)
            except Exception:
                pass
        if skill_name:
            counts_total[skill_name] = counts_total.get(skill_name, 0) + 1
            if skill_name not in last_used or ts > last_used[skill_name]:
                last_used[skill_name] = ts

    return {
        name: {
            "uses_total": counts_total.get(name, 0),
            "last_used_ts": last_used.get(name),
        }
        for name in counts_total
    }


def classify_disposition(level: int, size: int, uses_total: int) -> str:
    """Return one of: core, regular, casual, archive, manual-review."""
    if level >= 4:
        return "core"
    if level == 3:
        return "regular"
    if level == 2:
        return "casual"
    # level 1 — never used
    if uses_total == 0 and size < 5_000:
        return "archive"
    if uses_total == 0:
        return "manual-review"
    return "casual"


def main() -> int:
    skills = discover_skills()
    usage = query_skill_usage()

    out: dict[str, dict] = {}
    for name, info in sorted(skills.items()):
        u = usage.get(name, {"uses_total": 0, "last_used_ts": None})
        level, label = classify_level(u["uses_total"])
        last_used_iso = (
            datetime.fromtimestamp(u["last_used_ts"], tz=timezone.utc).isoformat()
            if u["last_used_ts"] else None
        )
        out[name] = {
            "level": level,
            "label": label,
            "uses_30d": 0,  # would require slicing by 30d window; placeholder
            "uses_total": u["uses_total"],
            "last_used": last_used_iso,
            "first_seen": info["first_seen"],
            "disposition": classify_disposition(level, info["size_chars"], u["uses_total"]),
            "category": info["category"],
            "size_chars": info["size_chars"],
            "path": info["path"],
        }

    # Build summary
    by_disposition: dict[str, list[str]] = {}
    for name, info in out.items():
        by_disposition.setdefault(info["disposition"], []).append(name)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_skills": len(out),
        "by_level": {
            "1_novice": sum(1 for v in out.values() if v["level"] == 1),
            "2_casual": sum(1 for v in out.values() if v["level"] == 2),
            "3_regular": sum(1 for v in out.values() if v["level"] == 3),
            "4_core": sum(1 for v in out.values() if v["level"] == 4),
        },
        "by_disposition": {k: len(v) for k, v in by_disposition.items()},
        "disposition_lists": by_disposition,
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    payload = {"summary": summary, "skills": out}
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    # Print to stdout (also useful for cron delivery)
    print(f"# Skill Level Audit — {summary['generated_at']}")
    print(f"Total skills: {summary['total_skills']}")
    print()
    print("## By level")
    for k, v in summary["by_level"].items():
        print(f"  {k:<10} {v}")
    print()
    print("## By disposition")
    for k, v in summary["by_disposition"].items():
        print(f"  {k:<18} {v}")
    print()
    print("## Top 20 by usage (lvl 4 / 3)")
    top = sorted(out.items(), key=lambda kv: (-kv[1]["uses_total"], kv[0]))[:20]
    for name, info in top:
        print(f"  lvl{info['level']} {name:<45} uses={info['uses_total']:>3} cat={info['category']}")
    print()
    print("## Archive candidates (novice + small)")
    for name in sorted(by_disposition.get("archive", [])):
        info = out[name]
        print(f"  {name:<45} size={info['size_chars']}c cat={info['category']}")
    print()
    print("## Manual review (novice + large)")
    for name in sorted(by_disposition.get("manual-review", [])):
        info = out[name]
        print(f"  {name:<45} size={info['size_chars']}c cat={info['category']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
