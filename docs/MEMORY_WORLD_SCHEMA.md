# Memory World — Graph Schema Specification (M2)

**Version:** 1.0
**Status:** Draft — awaiting review before M3 implementation
**Owner:** Zen (governance layer)
**Dependencies:** M1 System Inspection complete

---

## 1. Design Principles

| Principle | Description |
|-----------|-------------|
| **Single Source of Truth** | `scripts/zen-graph-exporter.py` → `data/memory-world.json` |
| **Deterministic Validation** | Every node/edge must pass schema before entering graph |
| **Visual Identity First** | Node/edge types map 1:1 to visual tokens (color, shape, sigil, motion) |
| **Traceable Provenance** | Every node carries `source_path`, `confidence`, `evidence[]` |
| **Temporal Awareness** | `created_at`, `updated_at`, `expires_at` on all nodes |
| **Cluster/Layer Architecture** | Nodes belong to semantic clusters + render layers for depth cues |
| **Extensible but Governed** | New types require Zen approval + schema update + visual token assignment |

---

## 2. Canonical Node Types

### 2.1 Core System (Zen Core Cluster)

| Type | Label | Description | Cluster | Layer |
|------|-------|-------------|---------|-------|
| `zen_core` | Zen Core | Governance/COO agent — supreme authority | `zen_core` | 0 (background) |
| `hermes_runtime` | Hermes Runtime | Agent host process, gateway, cron, memory, skills | `zen_core` | 1 |
| `discord_gateway` | Discord Gateway | Bridge: channels ↔ agent sessions | `zen_core` | 1 |
| `obsidian_vault` | Obsidian Vault | ZenVault at `~/Obsidian/ZenVault` — git-backed | `zen_core` | 1 |

### 2.2 Agent Roster (Agents Cluster)

| Type | Label | Description | Cluster | Layer |
|------|-------|-------------|---------|-------|
| `agent` | Agent | Specialized agent (Zen, Rin, Kiyosaki, Toji, Minato, Kazuki) | `agents` | 2 |
| `agent_session` | Agent Session | Active Discord session for an agent | `agents` | 3 |

### 2.3 Model Layer (Models Cluster)

| Type | Label | Description | Cluster | Layer |
|------|-------|-------------|---------|-------|
| `model_cloud` | Cloud Model | Owl Alpha, Codex, Minimax, Nemotron, etc. | `models` | 2 |
| `model_local` | Local Model | gpt-oss:20b via Ollama | `models` | 2 |
| `model_fallback` | Fallback Model | Configured but not primary | `models` | 3 |

### 2.4 Routing & Policy (Rules Cluster)

| Type | Label | Description | Cluster | Layer |
|------|-------|-------------|---------|-------|
| `routing_rule` | Routing Rule | Local vs Cloud decision policy | `rules` | 2 |
| `safety_gate` | Safety Gate | Refusal/abliteration rules, hard stops | `rules` | 2 |
| `review_gate` | Review Gate | Acceptance criteria, promotion thresholds | `rules` | 2 |
| `resource_quota` | Resource Quota | Token budget, rate limits, concurrent caps | `rules` | 3 |

### 2.5 Operational Resources (Resources Cluster)

| Type | Label | Description | Cluster | Layer |
|------|-------|-------------|---------|-------|
| `token_budget` | Token Budget | Daily spend tracking, cost avoidance | `resources` | 2 |
| `worker_launcher` | Worker Launcher | `zen-worker` binary, Aider orchestration | `resources` | 2 |
| `sandbox_engine` | Sandbox Engine | Prompt pipeline, review queue, promotion log | `resources` | 2 |
| `skill_registry` | Skill Registry | `~/.hermes/skills/` — downloaded + authored | `resources` | 2 |
| `script_layer` | Script Layer | `~/.hermes/scripts/` — prospector, outreach, etc. | `resources` | 3 |
| `cron_scheduler` | Cron Scheduler | Hermes cron jobs, autonomous pipelines | `resources` | 3 |
| `session_store` | Session Store | SQLite DB, messages, cross-session memory | `resources` | 3 |

### 2.6 Projects & Work (Projects Cluster)

| Type | Label | Description | Cluster | Layer |
|------|-------|-------------|---------|-------|
| `project` | Project | Top-level project (Kiyosaki, Minato, Zen, etc.) | `projects` | 2 |
| `project_memory` | Project Memory | Durable facts, decisions, preferences per project | `projects` | 3 |
| `project_instructions` | Project Instructions | Behavioral rules, persona, voice per project | `projects` | 3 |
| `work_unit` | Work Unit | Sandbox run, backtest, skill creation, deliverable | `projects` | 3 |

### 2.7 Execution & Events (Events Cluster)

| Type | Label | Description | Cluster | Layer |
|------|-------|-------------|---------|-------|
| `sandbox_run` | Sandbox Run | Individual SB-XXXX execution with review status | `events` | 4 |
| `backtest_run` | Backtest Run | Lean/QuantConnect strategy backtest | `events` | 4 |
| `skill_creation` | Skill Creation | New Hermes skill authored + registered | `events` | 4 |
| `promotion_event` | Promotion | Sandbox → Skill/File/Obsidian | `events` | 4 |
| `deployment_event` | Deployment | Code/artifact pushed to production | `events` | 4 |
| `route_decision` | Route Decision | Cloud vs Local choice for a task | `events` | 4 |
| `gate_evaluation` | Gate Evaluation | Review gate pass/fail with evidence | `events` | 4 |

### 2.8 Memory & Knowledge (Memory Cluster)

| Type | Label | Description | Cluster | Layer |
|------|-------|-------------|---------|-------|
| `memory_fact` | Memory Fact | Durable cross-session fact (Hermes memory) | `memory` | 3 |
| `memory_preference` | User Preference | Six's preferences, corrections, habits | `memory` | 3 |
| `learned_pattern` | Learned Pattern | Auto-discovered pattern from execution history | `memory` | 4 |
| `failed_route` | Failed Route | Route that errored/blocked — for learning | `memory` | 4 |

### 2.9 Risk & Health (Risk Cluster)

| Type | Label | Description | Cluster | Layer |
|------|-------|-------------|---------|-------|
| `system_risk` | System Risk | Identified risk (token exhaustion, model deprecation, etc.) | `risk` | 2 |
| `health_check` | Health Check | Component health (Git, Obsidian, Hermes, Models) | `risk` | 2 |
| `alert` | Alert | Active alert requiring attention | `risk` | 4 |

---

## 3. Canonical Edge Types

| Edge Type | Source → Target | Semantic | Visual Token |
|-----------|-----------------|----------|--------------|
| `governs` | `zen_core` → `agent` | Authority/hierarchy | Solid, authority color |
| `routes_to` | `agent` → `model_cloud` | Planning/review delegation | Dashed, model color |
| `delegates_to` | `agent` → `model_local` | File edits, artifact creation | Dotted, local color |
| `enforces` | `zen_core` → `routing_rule` | Policy enforcement | Solid, rule color |
| `consumes` | `model_cloud` → `token_budget` | Spend tracking | Dashed, warning color |
| `configures` | `zen_core` → `worker_launcher` | Ownership/config | Solid, resource color |
| `launches` | `worker_launcher` → `model_local` | Execution via Aider | Solid, local color |
| `participates_in` | `zen_core` → `agent_session` | Conversation membership | Dotted, session color |
| `manages` | `zen_core` → `sandbox_engine` | Orchestration | Solid, sandbox color |
| `oversees` | `zen_core` → `project` | Governance | Dashed, project color |
| `owns` | `agent` → `project` | Domain ownership | Solid, agent color |
| `owns` | `agent` → `project_memory` | Memory stewardship | Dashed, memory color |
| `owns` | `agent` → `project_instructions` | Instruction authorship | Dotted, memory color |
| `produces` | `agent` → `work_unit` | Execution output | Solid, event color |
| `triggers` | `cron_scheduler` → `sandbox_run` | Scheduled execution | Dotted, cron color |
| `validates` | `review_gate` → `sandbox_run` | Acceptance check | Dashed, gate color |
| `promotes` | `sandbox_run` → `skill_creation` | Promotion pipeline | Solid, success color |
| `blocks` | `safety_gate` → `route_decision` | Refusal/hard stop | Solid, danger color |
| `derives_from` | `learned_pattern` → `memory_fact` | Pattern discovery | Dotted, memory color |
| `supersedes` | `memory_fact` → `memory_fact` | Fact replacement | Dashed, warning color |
| `depends_on` | `work_unit` → `work_unit` | Execution dependency | Dotted, neutral |
| `reports_to` | `health_check` → `zen_core` | Health reporting | Solid, health color |
| `alerts` | `alert` → `zen_core` | Alert propagation | Solid, danger color |
| `invokes` | `agent` → `skill_registry` | Skill usage | Dotted, skill color |
| `generates` | `script_layer` → `work_unit` | Script output | Dashed, script color |
| `executes` | `hermes_runtime` → `cron_scheduler` | Runtime ownership | Solid, runtime color |
| `bridges` | `discord_gateway` → `agent_session` | Channel binding | Dotted, gateway color |
| `stores_in` | `session_store` → `memory_fact` | Persistence | Dashed, store color |
| `assigns_to` | `routing_rule` → `model_cloud` | Model assignment | Dotted, rule color |
| `falls_back_to` | `model_cloud` → `model_fallback` | Fallback chain | Dotted, fallback color |

---

## 4. Required Fields — All Nodes

```json
{
  "id": "string (UUID or deterministic slug)",
  "type": "string (from canonical node types above)",
  "label": "string (human-readable, max 60 chars)",
  "cluster": "string (from canonical clusters above)",
  "layer": "integer (0-4, render depth)",
  "active": "boolean",
  "risk": "string (none|low|medium|high|critical)",
  "confidence": "number (0.0-1.0) — source reliability",
  "evidence": [
    {
      "source": "string (file path, URL, log line, API response)",
      "type": "string (file|log|api|manual|inferred)",
      "excerpt": "string (max 200 chars)",
      "timestamp": "ISO8601"
    }
  ],
  "summary": "string (1-2 sentences, what this node represents)",
  "source_path": "string (primary source file/path)",
  "created_at": "ISO8601",
  "updated_at": "ISO8601",
  "expires_at": "ISO8601? (optional, for time-bounded nodes)",
  "data": "object (type-specific payload — see Section 5)",
  "visual": {
    "sigil": "string (emoji/unicode identifier)",
    "color_override": "string? (hex, overrides cluster default)",
    "size_modifier": "number (default 1.0, 0.5-2.0)",
    "glow": "boolean (pulse if active/risk)",
    "pulse": "string? (none|slow|fast|heartbeat)"
  }
}
```

---

## 5. Type-Specific `data` Payloads

### 5.1 `agent`
```json
{
  "channel": "string (Discord channel)",
  "domain": "string",
  "authority": "string (root|managed)",
  "instructions_path": "string (Obsidian path)",
  "memory_path": "string (Obsidian path)",
  "last_active": "ISO8601",
  "message_count_24h": "integer"
}
```

### 5.2 `model_cloud` / `model_local`
```json
{
  "full_name": "string (provider/model)",
  "provider": "string",
  "endpoint": "string",
  "cost_per_million_in": "number",
  "cost_per_million_out": "number",
  "context_limit": "integer",
  "is_default": "boolean",
  "is_free_tier": "boolean",
  "calls_24h": "integer",
  "tokens_24h": "integer",
  "cache_hit_rate": "number (0.0-1.0)"
}
```

### 5.3 `routing_rule`
```json
{
  "cloud_models": "string[]",
  "local_models": "string[]",
  "boundary_description": "string",
  "decision_logic": "string (natural language)",
  "enforcement_level": "string (strict|advisory)"
}
```

### 5.4 `sandbox_run`
```json
{
  "run_id": "string (SB-YYYYMMDD-NNN)",
  "project": "string",
  "task_type": "string",
  "review_status": "string (pending|accepted|rejected|blocked)",
  "quality": "string (high|medium|low)",
  "failure_mode": "string",
  "genericness": "string",
  "file_path": "string (Obsidian path)",
  "tokens_in": "integer",
  "tokens_out": "integer",
  "model_used": "string",
  "duration_ms": "integer"
}
```

### 5.5 `gate_evaluation`
```json
{
  "gate_id": "string",
  "gate_type": "string (review|safety|promotion|quality)",
  "target_id": "string (node being evaluated)",
  "result": "string (pass|fail|conditional)",
  "criteria": "string[]",
  "evidence_refs": "string[] (evidence IDs)",
  "evaluator": "string (agent_id or 'system')"
}
```

### 5.6 `route_decision`
```json
{
  "task_id": "string",
  "chosen_model": "string",
  "reason": "string",
  "alternative_models": "string[]",
  "confidence": "number"
}
```

### 5.7 `health_check`
```json
{
  "component": "string (git|obsidian|hermes|models|zen_bot)",
  "status": "string (ok|warning|critical|unknown)",
  "details": "object",
  "last_check": "ISO8601",
  "stale": "boolean"
}
```

### 5.8 `project`
```json
{
  "path": "string (Obsidian relative)",
  "file_count": "integer",
  "active_work_units": "integer",
  "acceptance_rate": "number (0.0-1.0)",
  "last_activity": "ISO8601"
}
```

### 5.9 `memory_fact`
```json
{
  "category": "string (preference|decision|fact|constraint)",
  "scope": "string (global|project|agent)",
  "deprecated": "boolean",
  "superseded_by": "string? (node_id)"
}
```

### 5.10 `system_risk`
```json
{
  "severity": "string (low|medium|high|critical)",
  "mitigation": "string",
  "detected_at": "ISO8601",
  "auto_resolvable": "boolean"
}
```

---

## 6. Required Fields — All Edges

```json
{
  "id": "string (UUID or deterministic: source-type-target)",
  "type": "string (from canonical edge types above)",
  "source": "string (node_id)",
  "target": "string (node_id)",
  "label": "string (max 40 chars, human-readable)",
  "confidence": "number (0.0-1.0)",
  "evidence": [
    {
      "source": "string",
      "type": "string",
      "excerpt": "string",
      "timestamp": "ISO8601"
    }
  ],
  "created_at": "ISO8601",
  "updated_at": "ISO8601",
  "data": "object (edge-type specific, optional)",
  "visual": {
    "style": "string (solid|dashed|dotted|tapered)",
    "color_override": "string? (hex)",
    "width_modifier": "number (default 1.0, 0.5-3.0)",
    "animated": "boolean (flow animation if active)",
    "arrowhead": "string (none|standard|hollow|diamond)"
  }
}
```

---

## 7. Visual Identity Tokens (Per Cluster)

| Cluster | Base Color | Sigil Prefix | Shape | Layer | Motion |
|---------|------------|--------------|-------|-------|--------|
| `zen_core` | `#4da3ff` (accent) | 🧠 | Hexagon | 0 | Slow pulse (heartbeat) |
| `agents` | `#3fc960` (success) | 👤 | Circle | 2 | Idle drift |
| `models` | `#bc8cff` (rin) | 🤖 | Diamond | 2 | Data-flow shimmer |
| `rules` | `#f0883e` (warning) | ⚖️ | Shield | 2 | Steady glow |
| `resources` | `#58d4e8` (info) | ⚙️ | Square | 2-3 | Subtle rotation |
| `projects` | `#f778ba` (kazuki) | 📁 | Folder | 2 | Activity ripple |
| `events` | `#e8b84d` (warning) | ⚡ | Bolt | 4 | Flash on occurrence |
| `memory` | `#8b9bb3` (text-secondary) | 💾 | Cylinder | 3 | Slow fade-in |
| `risk` | `#f06258` (danger) | ⚠️ | Triangle | 2-4 | Fast pulse + screen shake |

**Node Visual Defaults by Type** (overrides cluster where needed):

| Type | Shape | Base Size | Sigil | Special |
|------|-------|-----------|-------|---------|
| `zen_core` | Hexagon | 1.5x | 🧠 | Permanent glow, center anchor |
| `agent` | Circle | 1.0x | 👤 | Orbit around zen_core |
| `model_cloud` | Diamond | 0.9x | ☁️ | Cloud shimmer |
| `model_local` | Diamond | 0.9x | 💻 | Local pulse |
| `routing_rule` | Shield | 1.1x | ⚖️ | Golden border |
| `safety_gate` | Shield | 1.1x | 🛡️ | Red accent |
| `review_gate` | Shield | 1.1x | 📋 | Green accent |
| `sandbox_run` | Bolt | 0.8x | ⚡ | Fades after 24h |
| `gate_evaluation` | Bolt | 0.7x | ✅/❌ | Check/X overlay |
| `route_decision` | Diamond | 0.7x | 🔀 | Arrow indicator |
| `health_check` | Circle | 0.6x | 💚/💛/❤️ | Status color |
| `alert` | Triangle | 1.2x | ⚠️ | Screen shake, persistent |
| `memory_fact` | Cylinder | 0.7x | 💾 | Archives to background |
| `project` | Folder | 1.0x | 📁 | Expands to show children |

---

## 8. Validation Rules (Exporter Must Enforce)

| Rule | Severity | Description |
|------|----------|-------------|
| `node.type.in_canonical` | Error | Node type must be in canonical list |
| `node.cluster.matches_type` | Error | Cluster must match type's canonical cluster |
| `node.layer.in_range` | Error | Layer 0-4 |
| `node.confidence.range` | Error | 0.0 ≤ confidence ≤ 1.0 |
| `node.evidence.required_if_low_confidence` | Warning | If confidence < 0.7, evidence[] must not be empty |
| `node.source_path.exists` | Warning | Source path should exist on filesystem |
| `node.id.unique` | Error | No duplicate node IDs |
| `node.label.length` | Warning | Label ≤ 60 chars |
| `edge.type.in_canonical` | Error | Edge type must be in canonical list |
| `edge.source.exists` | Error | Source node ID must exist |
| `edge.target.exists` | Error | Target node ID must exist |
| `edge.confidence.range` | Error | 0.0 ≤ confidence ≤ 1.0 |
| `edge.id.unique` | Error | No duplicate edge IDs |
| `graph.connected_components ≤ 3` | Warning | Graph should be mostly connected |
| `graph.orphan_nodes = 0` | Warning | Every node should have ≥1 edge |
| `schema.version.present` | Error | Root must have `schema_version` |
| `stats.match_actual` | Error | Stats object must match actual node/edge counts |

---

## 9. Root Document Schema

```json
{
  "schema_version": "1.0",
  "generated_at": "ISO8601",
  "exporter_version": "string",
  "exporter_git_sha": "string",
  "stats": {
    "total_nodes": "integer",
    "total_edges": "integer",
    "node_types": "Record<string, integer>",
    "edge_types": "Record<string, integer>",
    "clusters": "Record<string, integer>",
    "layers": "Record<string, integer>",
    "avg_confidence": "number",
    "orphan_nodes": "integer",
    "validation_warnings": "integer"
  },
  "nodes": "GraphNode[]",
  "edges": "GraphEdge[]"
}
```

---

## 10. Migration Notes (from v0.1.0 → v1.0)

| v0.1.0 Field | v1.0 Mapping | Notes |
|--------------|--------------|-------|
| `nodes[].type` | `nodes[].type` | Expand enum |
| `nodes[].label` | `nodes[].label` | Keep |
| `nodes[].data` | `nodes[].data` | Restructure per type |
| `nodes[].active` | `nodes[].active` | Keep |
| `edges[].type` | `edges[].type` | Expand enum, add labels |
| `edges[].label` | `edges[].label` | Required now |
| `stats` | `stats` | Add clusters, layers, confidence |
| — | `schema_version` | New required |
| — | `exporter_git_sha` | New required |
| — | `visual` on nodes/edges | New required |
| — | `evidence[]` | New required |
| — | `confidence` | New required |

---

## 11. Approval Gate

**This schema must be approved before M3 (Exporter Hardening) begins.**

| Reviewer | Role | Status |
|----------|------|--------|
| Zen | Governance / Schema Owner | ⏳ Pending |
| Six | User / Final Authority | ⏳ Pending |

---

**Next:** M3 — Exporter Hardening (implements this schema with deterministic validation, incremental updates, file watcher)