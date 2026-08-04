"""
memory_bus.py — read/write project memories across the ZenNew system.

Three-tier memory architecture (locked 2026-08-04):
  Tier 1 (auto-injected): ~/.hermes/memories/MEMORY.md
    Hermes reads this at session start, freezes the snapshot, and injects
    it into every system prompt. ONLY `persist_fact()` writes here — and
    it does so via the `memory` tool's file-format conventions, so the
    next agent sees the fact on cold start.
  Tier 2 (per-project log): data/sessions/<project>/memory.md
    Append-only conversation log. This is DEFAULT for bot/overlay traffic.
    The dashboard and the bot producer read this file.
  Tier 3 (Obsidian project memory): ~/Obsidian/.../Project Memories/*.md
    NOT auto-updated by bot traffic. Use `patch_obsidian_project_memory()` for
    explicit curation (when Six asks for a project-level fact, or an agent
    decides a fact is worth surfacing to Six's reading list).

This module is the cross-project symbiosis layer. Project agents use it to:
  - read another project's memory (e.g. Rin reads Minato's trades)
  - write a note into their own memory.md after a session (Tier 2 only)
  - promote a durable fact to Tier 1 via `persist_fact()`
  - record bot events (errors, decisions) via `note_bot_event()`
  - list all projects' headlines for the Zen meta-agent

Reads are tolerant: missing files / no project memory yet just returns an
empty string. Writes are append-only by default to avoid clobbering history.

Noise filtering (effective 2026-08-04):
  Test traffic ("PONG", "Reply with PONG", bare acknowledgment pings) is
  skipped to avoid polluting memory with debug noise. Real user prompts
  and bot responses still land.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from datetime import datetime, timezone

# Default ZenNew data root; can be overridden via env var.
ZEN_DATA_ROOT = Path(
    os.environ.get("ZEN_DATA_ROOT",
                   r"C:\Users\Tyson\Projects\ZenNew\data")
)
SESSIONS_DIR = ZEN_DATA_ROOT / "sessions"

# Obsidian vault root — overrides via OBSIDIAN_VAULT_DIR. Defaults to the
# canonical Six ZenVault location.
OBSIDIAN_VAULT = Path(
    os.environ.get("OBSIDIAN_VAULT_DIR",
                   r"C:\Users\Tyson\Obsidian\ZenVault")
)
PROJECT_MEMORIES_DIR = OBSIDIAN_VAULT / "00_System" / "Project Memories"


# Project name -> session directory name. Both are the same in ZenNew.
KNOWN_PROJECTS = [
    "zen", "zen-os", "kiyosaki", "minato",
    "nami", "rin", "toji", "kazuki",
]


# --- Noise filter: skip test/ping traffic so memory.md stays useful ---------

# Tests/ping patterns observed in our traffic. These will NOT be written to
# either memory.md location — silently dropped to keep memory useful.
_NOISE_PATTERNS = [
    # PONG/PING-style verification traffic from token-audit tests
    re.compile(r"^reply with just (?:pong|ping|test|cache|zen)\.?$", re.IGNORECASE),
    re.compile(r"^reply with (?:word\s+)?(pong|ping|test|cache|zen)\.?$", re.IGNORECASE),
    re.compile(r"^pong\s*test", re.IGNORECASE),
    re.compile(r"^(?:pong|ping)\.?$", re.IGNORECASE),
    re.compile(r"^(?:pong|ping)$", re.IGNORECASE),
    re.compile(r"^test\s+\d*$", re.IGNORECASE),
    re.compile(r"^test$", re.IGNORECASE),
    # Single-word short acks
    re.compile(r"^(?:zen|cache)$", re.IGNORECASE),
] 


def is_test_noise(note: str) -> bool:
    """True if the note matches any known test/ping pattern.

    Keeps memory.md focused on real conversation turns, not debug artillery
    from token-audit testing.
    """
    if not note:
        return True
    stripped = note.strip()
    # Strip "Overlay user said:\n> Reply with just PONG" wrapper — look at payload only
    m = re.search(r"^>\s*(.+)$", stripped, re.MULTILINE)
    payload = m.group(1).strip() if m else stripped
    return any(p.match(payload) for p in _NOISE_PATTERNS)


def memory_path(project: str) -> Path:
    return SESSIONS_DIR / project.lower() / "memory.md"


def obsidian_memory_path(project: str) -> Path:
    """Path to the Obsidian project memory file, e.g. 'Project Memories/Zen Memory.md'."""
    return PROJECT_MEMORIES_DIR / f"{project.title()} Memory.md"


def read_memory(project: str) -> str:
    """Return the project's memory.md contents (may be empty string)."""
    p = memory_path(project)
    try:
        return p.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""
    except OSError:
        return ""


def read_summary(project: str) -> str:
    """Return the project's one-line summary line from summary.json."""
    import json
    p = SESSIONS_DIR / project.lower() / "summary.json"
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        s = (data.get("summary") or "").strip()
        if s:
            return s
        last = data.get("lastActive")
        mc = data.get("messageCount")
        return f"(no summary; {mc} messages, last active {last})"
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return ""


def all_projects_snapshot() -> dict[str, dict[str, str]]:
    """Return all known projects' summary + memory status.

    Used by the Zen meta-agent ("what's everyone up to?") and by Rin
    when pressure-testing cross-project decisions.

    Returns:
        {
          "minato": {"summary": "Stage 10 gen 350, ...",
                     "memory_excerpt": "...first 300 chars of memory.md...",
                     "message_count": "12",
                     "last_active": "2026-08-01T12:34:56Z"},
          ...
        }
    """
    import json
    out: dict[str, dict[str, str]] = {}
    for proj in KNOWN_PROJECTS:
        rec: dict[str, str] = {
            "summary": read_summary(proj),
            "memory_excerpt": (read_memory(proj)[:300]).replace("\n", " "),
        }
        summary_json = SESSIONS_DIR / proj / "summary.json"
        try:
            data = json.loads(summary_json.read_text(encoding="utf-8"))
            rec["message_count"] = str(data.get("messageCount", 0))
            rec["last_active"] = str(data.get("lastActive", ""))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            rec["message_count"] = "?"
            rec["last_active"] = "?"
        out[proj] = rec
    return out


def _append_to_file(p: Path, content: str, header_if_new: str = "") -> bool:
    """Append `content` to file `p`, creating with header_if_new if absent."""
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists() and header_if_new:
            p.write_text(header_if_new, encoding="utf-8")
        with p.open("a", encoding="utf-8") as f:
            f.write(content)
        return True
    except OSError:
        return False


def _format_note_block(project: str, note: str) -> tuple[str, str]:
    """Build the timestamped content block for both memory.md files.

    Returns (data_sessions_block, obsidian_block). Both include project context
    so a Six or agent reading either file alone has enough context.
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    block = f"\n### {ts}\n{note.strip()}\n"
    return block, block


def append_memory_note(project: str, note: str,
                       *, header: str = "## Notes",
                       dual_write_obsidian: bool = False) -> dict[str, bool]:
    """Append a note to Tier 2 (per-project log) only.

    Effective 2026-08-04: Three-tier memory architecture.
      - Tier 1 (auto-injected system prompt): ~/.hermes/memories/MEMORY.md
        Updated only via `persist_fact()` (promoted for durable facts).
      - Tier 2 (per-project log): data/sessions/<project>/memory.md
        Default destination for raw bot/overlay traffic. THIS FUNCTION.
      - Tier 3 (Obsidian project memory): Obsidian/.../Project Memories/*.md
        NOT auto-updated. Use `patch_obsidian_project_memory()` for explicit
        writes from agents.

    `dual_write_obsidian` is kept as an opt-in for tests/admin overrides
    but defaults to False. Bot conversation logs are noise to Six's
    Obsidian reading list — agent_os dashboard already shows them.

    Noise filter: notes matching test/ping patterns are silently dropped
    BEFORE writing anywhere, so neither file accumulates debug traffic.

    Returns a dict with per-target success:
        {"data": True/False, "obsidian": True/False, "filtered": True/False}
    """
    if is_test_noise(note):
        return {"data": False, "obsidian": False, "filtered": True}

    data_block, _ = _format_note_block(project, note)

    # Tier 2: data/sessions/<project>/memory.md (immutable conversation log)
    p_data = memory_path(project)
    data_header = (
        f"# {project.title()} Memory\n\n"
        f"_Started {datetime.now(timezone.utc).isoformat()}._\n\n"
    ) if not p_data.exists() else ""
    data_ok = _append_to_file(p_data, data_block, data_header)

    # Tier 3: Obsidian only when explicitly requested (rare)
    obsidian_ok = False
    if dual_write_obsidian:
        p_obs = obsidian_memory_path(project)
        obsidian_header = (
            f"# {project.title()} Memory\n\n"
            f"_Auto-synced from ZenNew bot. Last sync: "
            f"{datetime.now(timezone.utc).isoformat()}._\n\n"
        ) if not p_obs.exists() else ""
        obsidian_block, = _format_note_block(project, note)[:1]  # already built
        obsidian_ok = _append_to_file(p_obs, data_block, obsidian_header)  # same block

    return {"data": data_ok, "obsidian": obsidian_ok, "filtered": False}


def projects_other_than(this_project: str) -> list[str]:
    """Return the OTHER projects for cross-project memory reads."""
    me = this_project.lower()
    return [p for p in KNOWN_PROJECTS if p != me]


# ============================================================================
# Tier 1: Hermes MEMORY.md / USER.md (auto-injected system prompt)
# ============================================================================

# Default Hermes paths — override via HERMES_HOME env var.
HERMES_HOME = Path(os.environ.get("HERMES_HOME",
                                 r"C:\Users\Tyson\AppData\Local\hermes"))
HERMES_MEMORY_DIR = HERMES_HOME / "memories"
HERMES_MEMORY_PATH = HERMES_MEMORY_DIR / "MEMORY.md"
HERMES_USER_PATH = HERMES_MEMORY_DIR / "USER.md"

# Capacity limits (chars). The Hermes memory store is bounded; explicit
# in tools/memory_tool.py — keeps a snapshot of entries in the system
# prompt. Practical budget ~2,200 chars (memory target) / 1,375 (user target).
TIER1_MEMORY_CAP = 2_200
TIER1_USER_CAP = 1_375
TIER1_CAPACITY_WATCHDOG_PCT = 0.85  # 85% triggers A8 auto-summarize


def _tier1_path(target: str = "memory") -> Path:
    """Return the Tier-1 path for a target ('memory' or 'user')."""
    if target == "user":
        return HERMES_USER_PATH
    return HERMES_MEMORY_PATH


def _tier1_read(target: str = "memory") -> str:
    p = _tier1_path(target)
    try:
        return p.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return ""


def _tier1_append(target: str, entry: str) -> bool:
    """Append a §-delimited entry to Hermes MEMORY.md or USER.md.

    MEMORY.md format (matches tools/memory_tool.py):
        <fact>\n§\n

    USER.md format: freeform sections, but one fact per line works.
    """
    p = _tier1_path(target)
    try:
        HERMES_MEMORY_DIR.mkdir(parents=True, exist_ok=True)
        suffix = "\n§\n"
        with p.open("a", encoding="utf-8") as f:
            f.write(entry.rstrip() + suffix)
        return True
    except OSError:
        return False


def _content_hash(text: str) -> str:
    """Stable hash for duplicate-detection on Tier-1 entries."""
    import hashlib
    norm = " ".join(text.lower().split())
    return hashlib.md5(norm.encode("utf-8")).hexdigest()[:12]


def _tier1_duplicate_check(target: str, entry: str) -> bool:
    """Return True if entry's content (without date prefix) already exists.

    We strip the auto-generated date source tag before hashing so re-runs
    on different days don't bypass dedup.
    """
    import re
    # Strip our "<source> (YYYY-MM-DD): " or "**<source> (YYYY-MM-DD):** "
    # prefix from the entry before comparing.
    cleaned_entry = re.sub(
        r"^\**\(?[\w\- ]+\)?\s*\(?\d{4}-\d{2}-\d{2}\)?\s*[:\.\-]\s*\**\s*",
        "", entry,
    ).strip()
    if not cleaned_entry:
        return False
    new_hash = _content_hash(cleaned_entry)

    text = _tier1_read(target)
    if not text:
        return False
    for prev_raw in text.split("§"):
        prev = prev_raw.strip()
        if not prev:
            continue
        # Apply same prefix-strip to existing entries
        prev_clean = re.sub(
            r"^\**\(?[\w\- ]+\)?\s*\(?\d{4}-\d{2}-\d{2}\)?\s*[:\.\-]\s*\**\s*",
            "", prev,
        ).strip()
        if prev_clean and _content_hash(prev_clean) == new_hash:
            return True
    return False


def persist_fact(fact: str, *, target: str = "memory",
                  source: str = "memory_bus",
                  allow_dup: bool = False) -> dict[str, bool]:
    """Promote a durable fact to Tier 1 (Hermes MEMORY.md or USER.md).

    Effective 2026-08-04 — use this for facts that should be visible to
    every future agent session (auto-injected system prompt).

    Args:
        fact: short declarative statement (not imperative).
            Good: "Minato cron runs every 1m self-chaining 2026-08-03."
            Bad: "Always check cron schedule." (re-read as directive)
        target: "memory" (cross-session facts) or "user" (user prefs).
        source: tag appended for provenance / grep.
        allow_dup: bypass duplicate detection (rare, for explicit resets).

    Returns:
        {"written": bool, "duplicate": bool, "reason": str|None}

    Capacity watchdog (A8): if target file is >=85% of its cap, refuses
    new writes and surfaces a "compaction needed" reason. Caller (or
    A8 cron) should run memory_hygiene to free space before retrying.
    """
    if not fact or not fact.strip():
        return {"written": False, "duplicate": False, "reason": "empty fact"}

    cap = TIER1_USER_CAP if target == "user" else TIER1_MEMORY_CAP
    current = len(_tier1_read(target))
    if current >= TIER1_CAPACITY_WATCHDOG_PCT * cap and not allow_dup:
        return {
            "written": False,
            "duplicate": False,
            "reason": (
                f"{target} at {current}/{cap} chars "
                f"({current/cap*100:.0f}%); compactor required (run A8)"
            ),
        }

    entry = f"**{source} ({datetime.now(timezone.utc).strftime('%Y-%m-%d')}):** {fact.strip()}"
    if not allow_dup and _tier1_duplicate_check(target, entry):
        return {"written": False, "duplicate": True, "reason": "content hash matches existing entry"}

    ok = _tier1_append(target, entry)
    return {
        "written": ok,
        "duplicate": False,
        "reason": None if ok else "OSError",
    }


def note_bot_event(event_type: str, message: str, *,
                   source: str = "bot") -> dict[str, bool]:
    """Capture a bot-side event (error, decision, timeout) into Tier 1.

    Effective 2026-08-04 (A7). Bot producers should call this when:
      - chatgpt_client / hermes chat fails or times out
      - a config file is updated by the bot
      - a deployment or restart happens
      - any non-trivial decision is made on Six's behalf

    The event goes through persist_fact(), so it respects Tier-1's
    dedup and capacity watchdog. If memory is full, the event is
    silently dropped (the bot log already shows the failure).

    Args:
        event_type: short tag (e.g. "chatgpt_timeout", "config_change",
                    "deploy", "gateway_reconnect", "schema_upgrade").
        message: 1-2 sentence description, declarative.
        source: producer name (e.g. "zen-bot", "kiyosaki-bot").

    Returns:
        dict with persist_fact keys plus a "event" tag.
    """
    if not event_type or not message:
        return {"event": event_type or "?", "written": False, "reason": "empty"}
    fact = f"bot event [{event_type}] from {source}: {message.strip()}"
    result = persist_fact(fact, target="memory", source=f"bot/{source}")
    result["event"] = event_type
    return result


# ============================================================================
# Tier 3: Obsidian Project Memories (curated)
# ============================================================================

def patch_obsidian_project_memory(project: str, block: str,
                                   *, mode: str = "append") -> bool:
    """Tier 3 — explicit writes to Obsidian project memory.

    Use only when an agent decides this is project-level state worth
    surfacing to Six (e.g. a decision, a status note). Bot conversation
    traffic should NOT use this path.

    Args:
        project: project key
        block: markdown block to add (one or more paragraphs)
        mode: "append" (default), "replace" (only if Six explicitly asks
              for a full rewrite — never default)

    Returns True on success.
    """
    p_obs = obsidian_memory_path(project)
    try:
        p_obs.parent.mkdir(parents=True, exist_ok=True)
        if mode == "replace":
            p_obs.write_text(block, encoding="utf-8")
        else:
            with p_obs.open("a", encoding="utf-8") as f:
                f.write("\n" + block.strip() + "\n")
        return True
    except OSError:
        return False 
