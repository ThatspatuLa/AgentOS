# Multi-LLM Pipeline — Configuration Summary

## Architecture

```
User sends message to Zen
         ↓
    Zen (Owl Alpha)
    Decides: Thinking task or Doing task?
         ↓                ↓
   CLOUD PATH         LOCAL PATH
   Owl Alpha/Codex    Aider + Ollama gpt-oss:20b
   Research, discuss,  File mutations, artifact creation
   review, plan        HTML, CSS, JS, Python, configs, markdown
         ↓                ↓
   Direct output    Aider executes locally
                     ↓
                 Cloud Review (Owl Alpha)
                     ↓
                 Pass? → Done
                 Fail? → Tweak → Aider re-runs (max 3)
```

## Token Isolation Guarantee

- Local worker (Aider/Ollama) NEVER touches cloud API keys
- Cloud models NEVER process raw file generation prompts
- Ollama runs at `http://127.0.0.1:11434` only
- Cloud models run through OpenRouter (Owl Alpha) or Codex

## Model Priority

1. **Owl Alpha** — primary cloud for thinking/research/review
2. **Codex** — fallback when Owl Alpha hits limits
3. **gpt-oss:20b (Ollama)** — all file mutations (default, tested best June 2026)

## Local Worker — Tested June 2026

| Model | Time | Lines | Quality | Status |
|-------|------|-------|---------|--------|
| **gpt-oss:20b** | **78s** | **381** | **Excellent** | ✅ **DEFAULT** |
| devstral-small-2:24b | 149s | 254 | Medium | ❌ Removed (2x slower) |
| qwen2.5-coder:14b | 219s | ~350 | Good | ❌ Removed (3x slower) |
| codellama:13b | DNF | — | Poor | ❌ Incompatible with Aider |
| deepseek-coder-v2:16b | DNF | — | N/A | ❌ Incompatible with Aider |
| qwen3-coder:30b | 307s+ | ~205 | Unknown | ❌ Too slow, removed |
| qwen2.5-coder:7b | 163s+ | ~157 | Medium | ❌ Slower than 14b, stray files |

**Ollama is clean**: only gpt-oss:20b (13GB) on disk. All incompatible/slower models removed.

## Manual Override

User can override automatic routing:
- "use local" / "use Aider" → force local
- "use cloud" / "use Owl" / "use Codex" → force cloud

## File Locations

| File | Purpose |
|------|---------|
| `~/.hermes/skills/autonomous-ai-agents/multi-llm-pipeline/SKILL.md` | Main pipeline skill |
| `~/.hermes/skills/autonomous-ai-agents/aider/SKILL.md` | Aider worker skill |
| `~/Projects/ZenNew/token-tracker.py` | Token tracking system |
| `~/.hermes/sandbox/token-usage.json` | Token usage log |
| `~/Projects/ZenNew/generate-data.py` | Dashboard data generator (updated) |

## Pipeline Stages

1. **Zen Plans** — analyze task, write Aider prompt
2. **Aider Executes** — local file mutations (10 min timeout)
3. **Cloud Review** — Owl Alpha reviews output vs spec
4. **Auto-Tweak** — up to 3 fix iterations
5. **Promote** — production-ready

## Cloud Token Savings

Before: Every task used Owl Alpha/Codex (including file generation)
After: Only thinking/review uses cloud; file generation is free (local)

Estimated savings: 60-80% reduction in cloud token usage.
