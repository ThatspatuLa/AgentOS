"""
chatgpt_client.py — bridge to MiniMax-M3 via Hermes CLI.

v0.3 (2026-08-03)
-----------------
Replaces the v0.1 persona-greeting stub with a real call to the Hermes
CLI (MiniMax-M3). The bot's existing produce_response() now routes
through MiniMax-M3 with project philosophy injected as system context.

Public surface (stable):
    answer_with_project_persona(prompt, project, *, channel_id=None) -> str
    second_opinion(question, project) -> str

These are called by `bot/main.py`'s response-producer hook.
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

# Where the project philosophy files live (Obsidian).
OBSIDIAN_VAULT = Path(
    os.environ.get("OBSIDIAN_VAULT_DIR",
                   r"C:\Users\Tyson\Obsidian\ZenVault")
)
INSTRUCTIONS_DIR = OBSIDIAN_VAULT / "00_System" / "Project Instructions"


def _resolve_hermes_dir() -> Path:
    """Locate the Hermes install directory.

    Hermes installs to %LOCALAPPDATA%\\hermes\\hermes-agent on Windows,
    but HERMES_HOME may point to either %LOCALAPPDATA%\\hermes or
    %LOCALAPPDATA%\\hermes\\hermes-agent. Handle both, plus legacy
    ~/.hermes/hermes-agent.
    """
    candidates: list[Path] = []
    hh = os.environ.get("HERMES_HOME")
    if hh:
        candidates.append(Path(hh) / "hermes-agent")
        candidates.append(Path(hh))
    candidates.append(Path.home() / "AppData" / "Local" / "hermes" / "hermes-agent")
    candidates.append(Path.home() / ".hermes" / "hermes-agent")
    for c in candidates:
        if c.exists() and (c / "cli.py").exists():
            return c
    # Fallback — most likely path even if missing.
    return Path.home() / "AppData" / "Local" / "hermes" / "hermes-agent"


HERMES_AGENT_DIR = _resolve_hermes_dir()
HERMES_PYTHON = HERMES_AGENT_DIR / "venv" / "Scripts" / "python.exe"
HERMES_EXE = HERMES_AGENT_DIR / "venv" / "Scripts" / "hermes.exe"


# Tiny per-project voice indicator — prepended to real LLM replies so
# the project's voice stays consistent across the system, and used
# as fallback if Hermes is unreachable.
_VOICE = {
    "zen":     "Zen here. Coordinator and governance.",
    "kiyosaki":"Kiyosaki here. Survivability-first.",
    "minato":  "Minato here. DCA evolution pipeline.",
    "nami":    "Nami here. Multi-market edge research.",
    "toji":    "Toji here. Body discipline and progression.",
    "rin":     "Rin here. Tactical sparring and pressure-testing.",
    "kazuki":  "Kazuki here. Guitar practice and creative discipline.",
    "zen-os":  "Zen-OS here. System operator.",
}


def _persona_greeting(project: str) -> str:
    return _VOICE.get(project.lower(), f"{project} here.")


# Channel → project map (mirrors bot/main.py:CHANNEL_SESSION_MAP).
_PROJECT_CHANNELS = {
    "zen":      "1513076747430793289",
    "zen-os":   "1516644810834841672",
    "kiyosaki": "1500437316379086880",
    "minato":   "1500437358934233219",
    "nami":     "1523539567792099339",
    "rin":      "1500437397916356608",
    "toji":     "1500437438680793209",
    "kazuki":   "1500437483429564476",
}


# 1-line persona summary per project — replaces the old 4K philosophy block.
# Kept tiny (~200 chars max) so it fits in the cached system-prompt prefix.
_PROJECT_PERSONAS: dict[str, str] = {
    "zen":     "Zen is the COO and second-in-command of Project Zen. Connects user, Hermes, tools, Obsidian, Git, services. Lean ops, terse, action-first.",
    "zen-os":  "Zen-OS is the Operator OS twin of Zen — same voice, same rules, hosts the agent-os UI and operator workflow.",
    "nami":    "Nami is the research executor. Reads papers, synthesises, runs cron research jobs every 15 min.",
    "minato":  "Minato is the BTC/USDT H1 adaptive RL trading bot. Reads windows of price data, picks entries, evaluates exits.",
    "kiyosaki":"Kiyosaki is the ETH/USDT 5m freqtrade bucket. Confluence trades, multi-strategy portfolio.",
    "rin":     "Rin is the Phase 2d portfolio selector / meta-strategy. Reads Phase 2b/2c output, picks winners.",
    "toji":    "Toji is the trading-system evaluator. Diagnoses underperforming strategies.",
    "kazuki":  "Kazuki is the test/orchestration agent for ZenFleet.",
}


# Map of project → hermes session ID (looked up from
# data/sessions/index.json). Used for `--resume <id>` to maintain
# per-project session continuity. Projects without an ID (zen-os,
# nami, minato, kazuki) fall back to fresh-session mode.
_SESSION_INDEX_PATH = Path(__file__).resolve().parent.parent / "data" / "sessions" / "index.json"


def _load_project_session_map() -> dict[str, str]:
    """Read data/sessions/index.json and return {project_id: hermes_session_id}."""
    try:
        idx = json.loads(_SESSION_INDEX_PATH.read_text(encoding="utf-8"))
        return {
            s["id"]: s["hermesSessionId"]
            for s in idx.get("sessions", [])
            if s.get("hermesSessionId")
        }
    except (OSError, json.JSONDecodeError, KeyError):
        return {}


_PROJECT_SESSION_IDS: dict[str, str] = _load_project_session_map()


def _project_persona_summary(project: str) -> str:
    """Return the 1-line persona summary for a project."""
    return _PROJECT_PERSONAS.get(project, f"Project agent for {project}.")


def _read_project_philosophy(project: str, max_chars: int = 4000) -> str:
    """Read the project's philosophy/instruction file (truncated).

    Legacy helper — kept for callers that explicitly want the full file.
    `answer_with_project_persona` no longer uses this (see Step 1 of the
    2026-08-03 token audit: re-injecting 4K chars every call was wasteful).
    """
    candidates = [
        INSTRUCTIONS_DIR / f"{project.title()} Instructions.md",
        INSTRUCTIONS_DIR / "Zen Instructions.md",  # fallback for zen-os etc.
    ]
    for path in candidates:
        try:
            if path.exists():
                text = path.read_text(encoding="utf-8")
                if len(text) > max_chars:
                    text = text[:max_chars] + "\n\n[...truncated for context budget...]"
                return text
        except OSError:
            continue
    return ""


def _strip_hermes_noise(stdout: str) -> str:
    """Strip `session_id:` / `Query:` prefix lines from Hermes output."""
    if not stdout:
        return ""
    lines = stdout.splitlines()
    content: list[str] = []
    past_header = False
    for line in lines:
        if past_header:
            content.append(line)
            continue
        stripped = line.strip()
        if not stripped:
            past_header = True
            continue
        if stripped.startswith("session_id:") or stripped.startswith("Query:"):
            continue
        content.append(line)
        past_header = True
    return "\n".join(content).strip()


def _call_hermes(prompt: str, *, timeout: int = 90,
                 project: str | None = None,
                 resume: bool = True) -> tuple[bool, str]:
    """Call Hermes CLI synchronously and return (ok, response_text).

    v0.7: per-project session resumption via `--resume <session-id>`.
    When `project` is supplied AND it has a hermes session ID mapped
    in `data/sessions/index.json`, we resume that session so the LLM
    keeps multi-turn context (the project's voice, prior decisions).
    The trade-off: `--resume` loads the full session transcript into
    the model context, so long-running project sessions accumulate
    tokens (28M+ in the worst historical case). Set `resume=False` to
    force fresh-session for a single call.

    Why this works for Zen traffic:
    - Each project's session ID is short and tied to project work
      (e.g. zen=20260607_193056_683caea5, ~6K messages max).
    - Past session content IS project context we want preserved
      (prior decisions, voice patterns).
    - Cache hits compound: first call cold, subsequent fast and
      cheaper on cached prefix.

    Projects without a session_id (zen-os, nami, minato, kazuki)
    fall through to fresh-session mode automatically.
    """
    if not HERMES_EXE.exists():
        return False, f"hermes.exe not found at {HERMES_EXE}"

    cmd = [
        str(HERMES_EXE),
        "chat",
        "-Q",                # quiet: only emit final response
        "-q", prompt,
        "-t", "",            # no toolsets (chat only, no agent tool schemas)
        "-s", "",            # no skills loaded (project persona in prompt itself)
        "--ignore-rules",    # skip Hermes SOUL/rules injection
        "--ignore-user-config",
        "--no-restore-cwd",
    ]

    # Resume the project's persistent session if we have its ID.
    session_id = _PROJECT_SESSION_IDS.get(project, "") if project else ""
    if resume and session_id:
        cmd += ["--resume", session_id]

    env = {
        **os.environ,
        "NO_COLOR": "1",
        "TERM": "dumb",
        "PYTHONUNBUFFERED": "1",
        "HERMES_QUIET": "1",
    }

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, env=env,
            cwd=str(HERMES_AGENT_DIR),
        )
        if result.returncode == 0:
            return True, _strip_hermes_noise(result.stdout)
        err = (result.stderr or result.stdout or "").strip()
        return False, err or f"hermes exit {result.returncode}"
    except subprocess.TimeoutExpired:
        return False, f"hermes timed out after {timeout}s"
    except FileNotFoundError:
        return False, "hermes.exe not found"
    except Exception as e:
        return False, f"hermes call failed: {e}"


def answer_with_project_persona(prompt: str, project: str,
                                 *, channel_id: int | None = None) -> str:
    """Return a project-voiced answer.

    v0.5: minimal persona marker. The full project philosophy is
    already loaded by Hermes' default system prompt (Obsidian path
    ~00_System/Project Instructions/) — re-injecting it on every call
    was -4K input tokens per call. We keep only a one-line persona
    hint so the LLM knows which voice to use. Falls back to error
    indicator if Hermes is unreachable. NO persona greeting prefix —
    the project is already obvious from the @<project> selector.
    """
    # 1-line persona marker — no philosophy file injection.
    persona = _project_persona_summary(project)
    full_prompt = f"[Project: {project}] {persona}\n\n{(prompt or '').strip()}".strip()

    ok, response = _call_hermes(full_prompt)
    if ok and response:
        return response

    # Fallback: just Hermes error indicator (no greeting prefix)
    excerpt = (prompt or "").strip()
    if len(excerpt) > 200:
        excerpt = excerpt[:197] + "..."
    return (
        f"(Hermes call failed: {response[:200]})\n\n"
        f"> {excerpt}"
    )


def second_opinion(question: str, project: str) -> str:
    """Get Hermes' reflection on a question a project-agent formed.

    Returns a short paragraph. Routes through Hermes CLI like the main
    answer path but with a shorter context budget.
    """
    ok, response = _call_hermes(
        f"[second-opinion request] {question[:600]}",
        timeout=60,
    )
    if ok and response:
        return response
    return (
        f"[second-opinion:fallback] Hermes unavailable ({response[:100]}). "
        f"Project {project} should reason as if MiniMax-M3 had agreed."
    )