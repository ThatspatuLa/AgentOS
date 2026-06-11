#!/usr/bin/env python3
"""
Zen Graph Exporter — M3 Hardened (Schema v1.0)
Reads Zen system data and produces memory-world.json for the Memory World visual graph.
Pure Python 3.11, stdlib only. No dependencies.
Read-only: does not modify any source files.

Features:
- Canonical node/edge types per MEMORY_WORLD_SCHEMA.md v1.0
- Deterministic validation with error/warning reporting
- Confidence (0-1) + evidence[] on all nodes/edges
- Temporal fields (created_at, updated_at, expires_at)
- Visual identity tokens (sigil, color, shape, layer, motion)
- Incremental updates via file mtime tracking
- Watch mode for hot reload
- Schema version + exporter git SHA traceability
"""

import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION & PATHS
# ═══════════════════════════════════════════════════════════════════════════

HOME = Path.home()
REPO_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = REPO_ROOT / "data"
OUTPUT_FILE = OUTPUT_DIR / "memory-world.json"
STATE_FILE = OUTPUT_DIR / ".exporter-state.json"
SCHEMA_VERSION = "1.0"

# Source paths
HERMES_CONFIG = HOME / ".hermes/config.yaml"
STATE_DB = HOME / ".hermes/state.db"
AGENT_LOG = HOME / ".hermes/logs/agent.log"
LOCAL_WORKER_USAGE = HOME / ".hermes/sandbox/local-worker-usage.json"
TOKEN_USAGE = HOME / ".hermes/sandbox/token-usage.json"
VAULT = HOME / "Obsidian/ZenVault"
SOUL_FILE = VAULT / "00_System/SOUL.md"
ZEN_MEMORY = VAULT / "00_System/Project Memories/Zen Memory.md"
SANDBOX_RUNS = VAULT / "Sandbox/Runs"
AGENT_OS_DATA = REPO_ROOT / "agent-os-data.json"
ZEN_WORKER = HOME / ".local/bin/zen-worker"

# ═══════════════════════════════════════════════════════════════════════════
# CANONICAL TYPE DEFINITIONS (from MEMORY_WORLD_SCHEMA.md v1.0)
# ═══════════════════════════════════════════════════════════════════════════

NODE_CLUSTERS = {
    # (type, cluster, layer, base_color, sigil, shape)
    "zen_core": ("zen_core", 0, "#4da3ff", "🧠", "hexagon"),
    "hermes_runtime": ("zen_core", 1, "#4da3ff", "⚙️", "square"),
    "discord_gateway": ("zen_core", 1, "#4da3ff", "💬", "circle"),
    "obsidian_vault": ("zen_core", 1, "#4da3ff", "📓", "cylinder"),
    "agent": ("agents", 2, "#3fc960", "👤", "circle"),
    "agent_session": ("agents", 3, "#3fc960", "💬", "circle"),
    "model_cloud": ("models", 2, "#bc8cff", "☁️", "diamond"),
    "model_local": ("models", 2, "#bc8cff", "💻", "diamond"),
    "model_fallback": ("models", 3, "#bc8cff", "🔄", "diamond"),
    "routing_rule": ("rules", 2, "#f0883e", "⚖️", "shield"),
    "safety_gate": ("rules", 2, "#f0883e", "🛡️", "shield"),
    "review_gate": ("rules", 2, "#f0883e", "📋", "shield"),
    "resource_quota": ("rules", 3, "#f0883e", "📊", "shield"),
    "token_budget": ("resources", 2, "#58d4e8", "💰", "square"),
    "worker_launcher": ("resources", 2, "#58d4e8", "🚀", "square"),
    "sandbox_engine": ("resources", 2, "#58d4e8", "🧪", "square"),
    "skill_registry": ("resources", 2, "#58d4e8", "🧠", "square"),
    "script_layer": ("resources", 3, "#58d4e8", "🐍", "square"),
    "cron_scheduler": ("resources", 3, "#58d4e8", "⏰", "square"),
    "session_store": ("resources", 3, "#58d4e8", "💾", "cylinder"),
    "worker_activity": ("resources", 3, "#58d4e8", "💻", "square"),
    "project": ("projects", 2, "#f778ba", "📁", "folder"),
    "project_memory": ("projects", 3, "#f778ba", "💭", "cylinder"),
    "project_instructions": ("projects", 3, "#f778ba", "📜", "cylinder"),
    "work_unit": ("projects", 3, "#f778ba", "⚡", "bolt"),
    "sandbox_run": ("events", 4, "#e8b84d", "⚡", "bolt"),
    "backtest_run": ("events", 4, "#e8b84d", "📈", "bolt"),
    "skill_creation": ("events", 4, "#e8b84d", "✨", "bolt"),
    "promotion_event": ("events", 4, "#e8b84d", "⬆️", "bolt"),
    "deployment_event": ("events", 4, "#e8b84d", "🚀", "bolt"),
    "route_decision": ("events", 4, "#e8b84d", "🔀", "diamond"),
    "gate_evaluation": ("events", 4, "#e8b84d", "✅", "bolt"),
    "memory_fact": ("memory", 3, "#8b9bb3", "💾", "cylinder"),
    "memory_preference": ("memory", 3, "#8b9bb3", "❤️", "cylinder"),
    "learned_pattern": ("memory", 4, "#8b9bb3", "🔍", "cylinder"),
    "failed_route": ("memory", 4, "#8b9bb3", "❌", "cylinder"),
    "system_risk": ("risk", 2, "#f06258", "⚠️", "triangle"),
    "health_check": ("risk", 2, "#f06258", "💚", "circle"),
    "alert": ("risk", 4, "#f06258", "⚠️", "triangle"),
}

EDGE_TYPES = {
    "governs": {"style": "solid", "color": "#4da3ff", "arrowhead": "standard"},
    "routes_to": {"style": "dashed", "color": "#bc8cff", "arrowhead": "standard"},
    "delegates_to": {"style": "dotted", "color": "#3fc960", "arrowhead": "standard"},
    "enforces": {"style": "solid", "color": "#f0883e", "arrowhead": "standard"},
    "consumes": {"style": "dashed", "color": "#e8b84d", "arrowhead": "standard"},
    "configures": {"style": "solid", "color": "#58d4e8", "arrowhead": "standard"},
    "launches": {"style": "solid", "color": "#3fc960", "arrowhead": "standard"},
    "participates_in": {"style": "dotted", "color": "#8b9bb3", "arrowhead": "standard"},
    "manages": {"style": "solid", "color": "#e8b84d", "arrowhead": "standard"},
    "oversees": {"style": "dashed", "color": "#f778ba", "arrowhead": "standard"},
    "owns": {"style": "solid", "color": "#3fc960", "arrowhead": "standard"},
    "produces": {"style": "solid", "color": "#e8b84d", "arrowhead": "standard"},
    "triggers": {"style": "dotted", "color": "#58d4e8", "arrowhead": "standard"},
    "validates": {"style": "dashed", "color": "#f0883e", "arrowhead": "standard"},
    "promotes": {"style": "solid", "color": "#3fc960", "arrowhead": "standard"},
    "blocks": {"style": "solid", "color": "#f06258", "arrowhead": "diamond"},
    "derives_from": {"style": "dotted", "color": "#8b9bb3", "arrowhead": "standard"},
    "supersedes": {"style": "dashed", "color": "#e8b84d", "arrowhead": "standard"},
    "depends_on": {"style": "dotted", "color": "#8b9bb3", "arrowhead": "standard"},
    "reports_to": {"style": "solid", "color": "#3fc960", "arrowhead": "standard"},
    "alerts": {"style": "solid", "color": "#f06258", "arrowhead": "standard"},
    "invokes": {"style": "dotted", "color": "#58d4e8", "arrowhead": "standard"},
    "generates": {"style": "dashed", "color": "#e8b84d", "arrowhead": "standard"},
    "executes": {"style": "solid", "color": "#4da3ff", "arrowhead": "standard"},
    "bridges": {"style": "dotted", "color": "#8b9bb3", "arrowhead": "standard"},
    "stores_in": {"style": "dashed", "color": "#58d4e8", "arrowhead": "standard"},
    "assigns_to": {"style": "dotted", "color": "#f0883e", "arrowhead": "standard"},
    "falls_back_to": {"style": "dotted", "color": "#bc8cff", "arrowhead": "standard"},
    "available_to": {"style": "dotted", "color": "#8b9bb3", "arrowhead": "standard"},
    "contributes_to": {"style": "dashed", "color": "#e8b84d", "arrowhead": "standard"},
}

VALID_NODE_TYPES = set(NODE_CLUSTERS.keys())
VALID_EDGE_TYPES = set(EDGE_TYPES.keys())
VALID_CLUSTERS = {"zen_core", "agents", "models", "rules", "resources", "projects", "events", "memory", "risk"}

# ═══════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Evidence:
    source: str
    type: str  # file|log|api|manual|inferred
    excerpt: str
    timestamp: str

@dataclass
class VisualToken:
    sigil: str
    color_override: Optional[str] = None
    size_modifier: float = 1.0
    glow: bool = False
    pulse: str = "none"  # none|slow|fast|heartbeat

@dataclass
class NodeData:
    """Base node data - extended by type-specific payloads"""
    pass

@dataclass
class AgentData(NodeData):
    channel: str
    domain: str
    authority: str
    instructions_path: str
    memory_path: str
    last_active: str
    message_count_24h: int = 0

@dataclass
class ModelData(NodeData):
    full_name: str
    provider: str
    endpoint: str
    cost_per_million_in: float = 0.0
    cost_per_million_out: float = 0.0
    context_limit: int = 128000
    is_default: bool = False
    is_free_tier: bool = False
    calls_24h: int = 0
    tokens_24h: int = 0
    cache_hit_rate: float = 0.0

@dataclass
class RoutingRuleData(NodeData):
    cloud_models: list
    local_models: list
    boundary_description: str
    decision_logic: str
    enforcement_level: str = "strict"

@dataclass
class SandboxRunData(NodeData):
    run_id: str
    project: str
    task_type: str
    review_status: str
    quality: str
    failure_mode: str
    genericness: str
    file_path: str
    tokens_in: int
    tokens_out: int
    model_used: str
    duration_ms: int

@dataclass
class GateEvaluationData(NodeData):
    gate_id: str
    gate_type: str
    target_id: str
    result: str
    criteria: list
    evidence_refs: list
    evaluator: str

@dataclass
class RouteDecisionData(NodeData):
    task_id: str
    chosen_model: str
    reason: str
    alternative_models: list
    confidence: float

@dataclass
class HealthCheckData(NodeData):
    component: str
    status: str
    details: dict
    last_check: str
    stale: bool = False

@dataclass
class ProjectData(NodeData):
    path: str
    file_count: int
    active_work_units: int = 0
    acceptance_rate: float = 0.0
    last_activity: str = ""

@dataclass
class MemoryFactData(NodeData):
    category: str
    scope: str
    deprecated: bool = False
    superseded_by: Optional[str] = None

@dataclass
class SystemRiskData(NodeData):
    severity: str
    mitigation: str
    detected_at: str
    auto_resolvable: bool = False

# ═══════════════════════════════════════════════════════════════════════════
# VALIDATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ValidationIssue:
    severity: str  # error|warning
    code: str
    message: str
    node_id: Optional[str] = None
    edge_id: Optional[str] = None

class Validator:
    def __init__(self):
        self.issues: list[ValidationIssue] = []
        self.node_index: dict[str, dict] = {}
        self.edge_index: list[dict] = []

    def validate_node(self, node: dict) -> bool:
        ok = True
        nid = node.get("id", "unknown")

        # Type must be canonical
        if node.get("type") not in VALID_NODE_TYPES:
            self.issues.append(ValidationIssue("error", "node.type.in_canonical",
                f"Node type '{node.get('type')}' not in canonical list", node_id=nid))
            ok = False

        # Cluster must match type
        if node.get("type") in NODE_CLUSTERS:
            expected_cluster = NODE_CLUSTERS[node["type"]][0]
            if node.get("cluster") != expected_cluster:
                self.issues.append(ValidationIssue("error", "node.cluster.matches_type",
                    f"Node type '{node['type']}' expects cluster '{expected_cluster}', got '{node.get('cluster')}'", node_id=nid))
                ok = False

        # Layer 0-4
        layer = node.get("layer")
        if not isinstance(layer, int) or layer < 0 or layer > 4:
            self.issues.append(ValidationIssue("error", "node.layer.in_range",
                f"Layer must be integer 0-4, got {layer}", node_id=nid))
            ok = False

        # Confidence 0-1
        conf = node.get("confidence")
        if not isinstance(conf, (int, float)) or conf < 0.0 or conf > 1.0:
            self.issues.append(ValidationIssue("error", "node.confidence.range",
                f"Confidence must be 0.0-1.0, got {conf}", node_id=nid))
            ok = False

        # Evidence required if confidence < 0.7
        if isinstance(conf, (int, float)) and conf < 0.7:
            ev = node.get("evidence", [])
            if not ev or not isinstance(ev, list) or len(ev) == 0:
                self.issues.append(ValidationIssue("warning", "node.evidence.required_if_low_confidence",
                    f"Confidence {conf} < 0.7 but evidence[] is empty", node_id=nid))

        # Source path exists (warning)
        sp = node.get("source_path")
        if sp and not Path(sp).exists():
            self.issues.append(ValidationIssue("warning", "node.source_path.exists",
                f"Source path does not exist: {sp}", node_id=nid))

        # Label length
        label = node.get("label", "")
        if len(label) > 60:
            self.issues.append(ValidationIssue("warning", "node.label.length",
                f"Label exceeds 60 chars: {len(label)}", node_id=nid))

        # Required fields
        required = ["id", "type", "label", "cluster", "layer", "active", "risk",
                    "confidence", "evidence", "summary", "source_path",
                    "created_at", "updated_at", "data", "visual"]
        for field in required:
            if field not in node:
                self.issues.append(ValidationIssue("error", f"node.missing_field.{field}",
                    f"Missing required field: {field}", node_id=nid))
                ok = False

        self.node_index[nid] = node
        return ok

    def validate_edge(self, edge: dict) -> bool:
        ok = True
        eid = edge.get("id", "unknown")

        if edge.get("type") not in VALID_EDGE_TYPES:
            self.issues.append(ValidationIssue("error", "edge.type.in_canonical",
                f"Edge type '{edge.get('type')}' not in canonical list", edge_id=eid))
            ok = False

        source = edge.get("source")
        target = edge.get("target")
        if source not in self.node_index:
            self.issues.append(ValidationIssue("error", "edge.source.exists",
                f"Source node '{source}' does not exist", edge_id=eid))
            ok = False
        if target not in self.node_index:
            self.issues.append(ValidationIssue("error", "edge.target.exists",
                f"Target node '{target}' does not exist", edge_id=eid))
            ok = False

        conf = edge.get("confidence")
        if not isinstance(conf, (int, float)) or conf < 0.0 or conf > 1.0:
            self.issues.append(ValidationIssue("error", "edge.confidence.range",
                f"Confidence must be 0.0-1.0, got {conf}", edge_id=eid))
            ok = False

        required = ["id", "type", "source", "target", "label", "confidence",
                    "evidence", "created_at", "updated_at"]
        for field in required:
            if field not in edge:
                self.issues.append(ValidationIssue("error", f"edge.missing_field.{field}",
                    f"Missing required field: {field}", edge_id=eid))
                ok = False

        self.edge_index.append(edge)
        return ok

    def validate_graph(self):
        # Connected components
        if self.node_index:
            # Simple connectivity check
            adj = {nid: set() for nid in self.node_index}
            for e in self.edge_index:
                adj[e["source"]].add(e["target"])
                adj[e["target"]].add(e["source"])

            visited = set()
            components = 0
            for nid in self.node_index:
                if nid not in visited:
                    components += 1
                    stack = [nid]
                    while stack:
                        cur = stack.pop()
                        if cur in visited:
                            continue
                        visited.add(cur)
                        stack.extend(adj[cur] - visited)

            if components > 3:
                self.issues.append(ValidationIssue("warning", "graph.connected_components",
                    f"Graph has {components} connected components (expected ≤3)"))

            # Orphan nodes
            orphans = [nid for nid, neighbors in adj.items() if len(neighbors) == 0]
            if orphans:
                self.issues.append(ValidationIssue("warning", "graph.orphan_nodes",
                    f"{len(orphans)} orphan nodes: {orphans[:5]}"))

    def get_summary(self) -> dict:
        errors = [i for i in self.issues if i.severity == "error"]
        warnings = [i for i in self.issues if i.severity == "warning"]
        return {
            "valid": len(errors) == 0,
            "error_count": len(errors),
            "warning_count": len(warnings),
            "issues": [
                {"severity": i.severity, "code": i.code, "message": i.message,
                 "node_id": i.node_id, "edge_id": i.edge_id}
                for i in self.issues
            ]
        }

# ═══════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def read_file_safe(path: Path, default: str = "") -> str:
    try:
        return path.read_text(errors="replace")
    except Exception:
        return default

def grep_yaml_value(text: str, key: str) -> Optional[str]:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(f"{key}:"):
            val = stripped.split(":", 1)[1].strip().strip('"').strip("'")
            return val
    return None

def redact_secrets(text: str) -> str:
    text = re.sub(r'(sk-[a-zA-Z0-9]{20,})', '[REDACTED]', text)
    text = re.sub(r'(Bearer\s+[a-zA-Z0-9\-\._~+/]+)', 'Bearer [REDACTED]', text)
    return text

def get_git_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            return result.stdout.strip()[:12]
    except Exception:
        pass
    return "unknown"

def file_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except Exception:
        return 0.0

def file_hash(path: Path) -> str:
    try:
        content = path.read_bytes()
        return hashlib.sha256(content).hexdigest()[:16]
    except Exception:
        return "missing"

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"source_mtimes": {}, "source_hashes": {}}

def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))

def source_changed(path: Path, state: dict) -> bool:
    key = str(path)
    current_mtime = file_mtime(path)
    current_hash = file_hash(path)
    return (state["source_mtimes"].get(key) != current_mtime or
            state["source_hashes"].get(key) != current_hash)

def update_source_state(path: Path, state: dict):
    key = str(path)
    state["source_mtimes"][key] = file_mtime(path)
    state["source_hashes"][key] = file_hash(path)

def make_evidence(source: str, etype: str, excerpt: str) -> dict:
    return {
        "source": source,
        "type": etype,
        "excerpt": excerpt[:200],
        "timestamp": now_iso()
    }

def make_visual(sigil: str, color: Optional[str] = None, size: float = 1.0,
                glow: bool = False, pulse: str = "none") -> dict:
    return {
        "sigil": sigil,
        "color_override": color,
        "size_modifier": size,
        "glow": glow,
        "pulse": pulse
    }

def base_node(node_type: str, node_id: str, label: str, risk: str = "none",
              active: bool = True, confidence: float = 0.9,
              summary: str = "", source_path: str = "") -> dict:
    cluster, layer, base_color, sigil, shape = NODE_CLUSTERS[node_type]
    return {
        "id": node_id,
        "type": node_type,
        "label": label,
        "cluster": cluster,
        "layer": layer,
        "active": active,
        "risk": risk,
        "confidence": confidence,
        "evidence": [],
        "summary": summary or f"{label} ({node_type})",
        "source_path": source_path,
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "expires_at": None,
        "data": {},
        "visual": make_visual(sigil, base_color, 1.0, False, "none")
    }

def base_edge(edge_type: str, source: str, target: str, label: str,
              confidence: float = 0.9) -> dict:
    edge_info = EDGE_TYPES.get(edge_type, {})
    return {
        "id": f"{source}-{edge_type}-{target}",
        "type": edge_type,
        "source": source,
        "target": target,
        "label": label,
        "confidence": confidence,
        "evidence": [],
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "data": {},
        "visual": {
            "style": edge_info.get("style", "solid"),
            "color_override": edge_info.get("color"),
            "width_modifier": 1.0,
            "animated": False,
            "arrowhead": edge_info.get("arrowhead", "standard")
        }
    }

# ═══════════════════════════════════════════════════════════════════════════
# SOURCE EXTRACTORS (Schema v1.0 Compliant)
# ═══════════════════════════════════════════════════════════════════════════

def extract_models_from_config(state: dict) -> tuple[list, list]:
    nodes, edges = [], []
    if source_changed(HERMES_CONFIG, state) or "hermes-config" not in state.get("source_hashes", {}):
        config_text = read_file_safe(HERMES_CONFIG)
        config_text = redact_secrets(config_text)

        default_model = grep_yaml_value(config_text, "default") or "unknown"
        provider = grep_yaml_value(config_text, "provider") or "unknown"

        # Default cloud model
        cloud_model_id = "model-default-cloud"
        n = base_node("model_cloud", cloud_model_id,
            default_model.split("/")[-1] if "/" in default_model else default_model,
            risk="none", active=True, confidence=0.95,
            summary=f"Default cloud model: {default_model}",
            source_path=str(HERMES_CONFIG))
        n["data"] = {
            "full_name": default_model,
            "provider": provider,
            "endpoint": f"api.{provider}.com",
            "cost_per_million_in": 0.0,
            "cost_per_million_out": 0.0,
            "context_limit": 128000,
            "is_default": True,
            "is_free_tier": False,
            "calls_24h": 0,
            "tokens_24h": 0,
            "cache_hit_rate": 0.0
        }
        n["evidence"].append(make_evidence(str(HERMES_CONFIG), "file",
            f"default: {default_model}, provider: {provider}"))
        nodes.append(n)

        # All referenced models
        model_names = set()
        for line in config_text.splitlines():
            line = line.strip()
            if "model" in line.lower() and ":" in line and not line.startswith("#"):
                val = line.split(":", 1)[1].strip().strip('"').strip("'")
                if val and "/" in val and not val.startswith("http"):
                    model_names.add(val)

        for mn in sorted(model_names):
            mn_id = f"model-{mn.replace('/', '-').replace(':', '-').replace('.', '-')}"
            role = "local" if mn.startswith("ollama/") or "gpt-oss" in mn else "cloud"
            node_type = "model_local" if role == "local" else "model_cloud"

            n = base_node(node_type, mn_id,
                mn.split("/")[-1] if "/" in mn else mn,
                risk="none", active=(role == "local"), confidence=0.9,
                summary=f"Configured {role} model: {mn}",
                source_path=str(HERMES_CONFIG))
            n["data"] = {
                "full_name": mn,
                "provider": mn.split("/")[0] if "/" in mn else "unknown",
                "endpoint": f"api.{mn.split('/')[0]}.com" if "/" in mn else "unknown",
                "cost_per_million_in": 0.0,
                "cost_per_million_out": 0.0,
                "context_limit": 128000,
                "is_default": False,
                "is_free_tier": role == "local",
                "calls_24h": 0,
                "tokens_24h": 0,
                "cache_hit_rate": 0.0
            }
            n["evidence"].append(make_evidence(str(HERMES_CONFIG), "file", f"model: {mn}"))
            nodes.append(n)

            edges.append(base_edge("available_to", mn_id, cloud_model_id, "configured"))

        # Local worker model
        worker_model_id = "model-worker-local"
        n = base_node("model_local", worker_model_id, "gpt-oss:20b",
            risk="none", active=True, confidence=0.95,
            summary="Local execution model via Ollama",
            source_path=str(HERMES_CONFIG))
        n["data"] = {
            "full_name": "ollama/gpt-oss:20b",
            "provider": "ollama",
            "endpoint": "localhost:11434",
            "cost_per_million_in": 0.0,
            "cost_per_million_out": 0.0,
            "context_limit": 128000,
            "is_default": False,
            "is_free_tier": True,
            "calls_24h": 0,
            "tokens_24h": 0,
            "cache_hit_rate": 0.0
        }
        n["evidence"].append(make_evidence(str(HERMES_CONFIG), "file", "ollama/gpt-oss:20b for local worker"))
        nodes.append(n)

        update_source_state(HERMES_CONFIG, state)

    return nodes, edges

def extract_agent_roster(state: dict) -> tuple[list, list]:
    nodes, edges = [], []
    if source_changed(SOUL_FILE, state):
        soul_text = read_file_safe(SOUL_FILE)

        # Zen Core
        zen_id = "zen-core"
        n = base_node("zen_core", zen_id, "Zen Core",
            risk="high", active=True, confidence=0.99,
            summary="Governance/COO agent — supreme authority, system law, safety",
            source_path=str(SOUL_FILE))
        n["data"] = {
            "channel": "zen-chat",
            "domain": "Governance, safety, system law",
            "authority": "root",
            "instructions_path": "00_System/Project Instructions/Zen Instructions.md",
            "memory_path": "00_System/Project Memories/Zen Memory.md",
            "last_active": now_iso(),
            "message_count_24h": 0
        }
        n["evidence"].append(make_evidence(str(SOUL_FILE), "file", "SOUL.md Agent Roster: Zen"))
        n["visual"] = make_visual("🧠", "#4da3ff", 1.5, True, "heartbeat")
        nodes.append(n)

        # Parse agent table
        agent_section = False
        for line in soul_text.splitlines():
            if "## Project Zen — Agent Roster" in line:
                agent_section = True
                continue
            if agent_section and line.strip().startswith("|"):
                parts = [p.strip() for p in line.split("|")]
                if all(re.match(r'^-+$', p.strip()) for p in parts[1:-1] if p.strip()):
                    continue
                if len(parts) >= 4 and parts[1] and parts[1] != "Agent":
                    agent_name = parts[1]
                    if agent_name == "Zen":
                        continue
                    channel = parts[2] if len(parts) > 2 else ""
                    domain = parts[3] if len(parts) > 3 else ""
                    agent_id = f"agent-{agent_name.lower()}"

                    authority = "managed"
                    risk = "high" if any(k in domain.lower() for k in ["trading", "automation"]) else "medium"

                    n = base_node("agent", agent_id, agent_name,
                        risk=risk, active=True, confidence=0.9,
                        summary=f"Agent: {agent_name} — {domain}",
                        source_path=str(SOUL_FILE))
                    n["data"] = {
                        "channel": f"{agent_name.lower()}-chat" if not channel else channel,
                        "domain": domain,
                        "authority": authority,
                        "instructions_path": f"00_System/Project Instructions/{agent_name} Instructions.md",
                        "memory_path": f"00_System/Project Memories/{agent_name} Memory.md",
                        "last_active": now_iso(),
                        "message_count_24h": 0
                    }
                    n["evidence"].append(make_evidence(str(SOUL_FILE), "file",
                        f"SOUL.md Agent Roster: {agent_name}"))
                    nodes.append(n)

                    edges.append(base_edge("governs", zen_id, agent_id, "authority"))
                    edges.append(base_edge("routes_to", agent_id, "model-default-cloud", "planning & review"))
                    edges.append(base_edge("delegates_to", agent_id, "model-worker-local", "file edits"))

            if agent_section and line.strip().startswith("## ") and "Agent Roster" not in line:
                break

        update_source_state(SOUL_FILE, state)

    return nodes, edges

def extract_system_facts(state: dict) -> tuple[list, list]:
    nodes, edges = [], []
    if source_changed(ZEN_MEMORY, state):
        memory_text = read_file_safe(ZEN_MEMORY)
        memory_text = redact_secrets(memory_text)

        # Routing rule
        rule_id = "rule-local-vs-cloud"
        n = base_node("routing_rule", rule_id, "Local vs Cloud Routing",
            risk="medium", active=True, confidence=0.95,
            summary="Routes planning/review to cloud (Owl Alpha + Codex); execution to local (Aider + Ollama gpt-oss:20b)",
            source_path=str(ZEN_MEMORY))
        n["data"] = {
            "cloud_models": ["openrouter/owl-alpha", "openai-codex"],
            "local_models": ["ollama/gpt-oss:20b"],
            "boundary_description": "Local never touches cloud keys. Cloud never processes raw file generation.",
            "decision_logic": "Cloud for planning/research/review; Local for file edits/artifact creation",
            "enforcement_level": "strict"
        }
        n["evidence"].append(make_evidence(str(ZEN_MEMORY), "file", "Zen Memory: Local vs Cloud Routing rule"))
        n["visual"] = make_visual("⚖️", "#f0883e", 1.1, True, "slow")
        nodes.append(n)
        edges.append(base_edge("enforces", "zen-core", rule_id, "policy"))

        # Token budget
        resource_id = "resource-token-budget"
        n = base_node("token_budget", resource_id, "Cloud Token Budget",
            risk="medium", active=True, confidence=0.9,
            summary="Daily cloud token spend tracking and cost avoidance",
            source_path=str(ZEN_MEMORY))
        n["data"] = {
            "description": "Daily cloud token spend tracking",
            "warning": "Cloud tokens are finite — local worker should handle execution"
        }
        n["evidence"].append(make_evidence(str(ZEN_MEMORY), "file", "Zen Memory: Token budget"))
        nodes.append(n)
        edges.append(base_edge("consumes", "model-default-cloud", resource_id, "spend"))

        # Worker config
        worker_res_id = "resource-worker-config"
        n = base_node("worker_launcher", worker_res_id, "zen-worker",
            risk="low", active=True, confidence=0.9,
            summary="Guarded local-only worker launcher at ~/.local/bin/zen-worker",
            source_path=str(ZEN_MEMORY))
        n["data"] = {
            "path": "~/.local/bin/zen-worker",
            "description": "Guarded local-only worker launcher",
            "safety": "Refuses cloud model args. Ollama only."
        }
        n["evidence"].append(make_evidence(str(ZEN_MEMORY), "file", "Zen Memory: zen-worker config"))
        nodes.append(n)
        edges.append(base_edge("configures", "zen-core", worker_res_id, "owns"))
        edges.append(base_edge("launches", worker_res_id, "model-worker-local", "executes via Aider"))

        update_source_state(ZEN_MEMORY, state)

    return nodes, edges

def extract_session_graph(state: dict) -> tuple[list, list]:
    nodes, edges = [], []
    if STATE_DB.exists() and source_changed(STATE_DB, state):
        try:
            conn = sqlite3.connect(f"file:{STATE_DB}?mode=ro", uri=True)
            cur = conn.cursor()

            cur.execute("SELECT COUNT(*) FROM sessions")
            session_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages")
            msg_count = cur.fetchone()[0]

            cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            cur.execute("SELECT title, created_at FROM sessions WHERE created_at >= ? ORDER BY created_at DESC LIMIT 10", (cutoff,))
            recent = cur.fetchall()

            session_node_id = "resource-session-store"
            n = base_node("session_store", session_node_id, "Conversation Sessions",
                risk="low", active=True, confidence=0.9,
                summary=f"SQLite session store: {session_count} sessions, {msg_count} messages",
                source_path=str(STATE_DB))
            n["data"] = {
                "total_sessions": session_count,
                "total_messages": msg_count,
                "recent_7d": len(recent),
                "recent_titles": [r[0][:40] if r[0] else "(untitled)" for r in recent[:5]]
            }
            n["evidence"].append(make_evidence(str(STATE_DB), "api", f"{session_count} sessions, {msg_count} messages"))
            nodes.append(n)
            edges.append(base_edge("participates_in", "zen-core", session_node_id, "conversations"))

            conn.close()
        except Exception:
            pass

        update_source_state(STATE_DB, state)

    return nodes, edges

def extract_token_activity(state: dict) -> tuple[list, list]:
    nodes, edges = [], []
    if source_changed(AGENT_LOG, state):
        try:
            result = subprocess.run(
                ["tail", "-500", str(AGENT_LOG)],
                capture_output=True, text=True, errors="replace", timeout=5
            )
            lines = result.stdout.splitlines()

            model_usage = {}
            for line in lines:
                if "agent.conversation_loop" in line:
                    for model_name in ["owl-alpha", "codex", "gpt-oss", "minimax", "nemotron"]:
                        if model_name in line.lower():
                            model_usage[model_name] = model_usage.get(model_name, 0) + 1

            if model_usage:
                activity_node_id = "resource-token-activity"
                n = base_node("health_check", activity_node_id, "Inference Activity",
                    risk="low", active=True, confidence=0.8,
                    summary=f"Recent model calls from agent log: {sum(model_usage.values())} total",
                    source_path=str(AGENT_LOG))
                n["data"] = {
                    "component": "inference_activity",
                    "status": "ok",
                    "details": {"recent_calls_by_model": model_usage, "source": "last 500 log lines"},
                    "last_check": now_iso(),
                    "stale": False
                }
                n["evidence"].append(make_evidence(str(AGENT_LOG), "log",
                    f"Model usage: {model_usage}"))
                n["visual"] = make_visual("💚", "#3fc960", 1.0, False, "none")
                nodes.append(n)

                # Only add edges for models that exist in canonical types
                existing_model_ids = {"model-default-cloud", "model-worker-local"}
                for model_name, count in model_usage.items():
                    model_id = f"model-{model_name}"
                    if model_id in existing_model_ids:
                        edges.append(base_edge("contributes_to", model_id, activity_node_id, "activity"))
                    else:
                        # Create a generic model node for unknown models
                        if model_id not in [n["id"] for n in nodes]:
                            mn = base_node("model_cloud", model_id, model_name,
                                risk="none", active=False, confidence=0.7,
                                summary=f"Detected model from log activity: {model_name}",
                                source_path=str(AGENT_LOG))
                            mn["data"] = {
                                "full_name": model_name,
                                "provider": "unknown",
                                "endpoint": "unknown",
                                "cost_per_million_in": 0.0,
                                "cost_per_million_out": 0.0,
                                "context_limit": 128000,
                                "is_default": False,
                                "is_free_tier": False,
                                "calls_24h": count,
                                "tokens_24h": 0,
                                "cache_hit_rate": 0.0
                            }
                            mn["evidence"].append(make_evidence(str(AGENT_LOG), "log", f"Detected in agent log: {count} calls"))
                            nodes.append(mn)
                        edges.append(base_edge("contributes_to", model_id, activity_node_id, "activity"))

        except Exception:
            pass

        update_source_state(AGENT_LOG, state)

    return nodes, edges

def extract_projects(state: dict) -> tuple[list, list]:
    nodes, edges = [], []
    if VAULT.exists() and source_changed(VAULT / "01_Projects", state):
        projects_dir = VAULT / "01_Projects"
        if projects_dir.exists():
            # Known agent IDs that actually exist in the graph
            known_agent_ids = {"agent-rin", "agent-kiyosaki", "agent-toji", "agent-minato", "agent-kazuki"}

            for item in sorted(projects_dir.iterdir()):
                if item.is_dir():
                    project_id = f"project-{item.name.lower().replace(' ', '-')}"
                    md_files = list(item.rglob("*.md"))

                    n = base_node("project", project_id, item.name,
                        risk="none", active=len(md_files) > 0, confidence=0.9,
                        summary=f"Project: {item.name} — {len(md_files)} files",
                        source_path=str(item))
                    n["data"] = {
                        "path": str(item.relative_to(VAULT)),
                        "file_count": len(md_files),
                        "active_work_units": 0,
                        "acceptance_rate": 0.0,
                        "last_activity": now_iso()
                    }
                    n["evidence"].append(make_evidence(str(item), "file",
                        f"{len(md_files)} markdown files"))
                    n["visual"] = make_visual("📁", "#f778ba", 1.0, False, "none")
                    nodes.append(n)

                    edges.append(base_edge("oversees", "zen-core", project_id, "governance"))

                    # Connect project to its agent if the agent exists
                    agent_name = item.name.split()[0].lower()  # e.g., "swarm" from "Swarm Project"
                    agent_id = f"agent-{agent_name}"
                    if agent_id in known_agent_ids:
                        edges.append(base_edge("owns", agent_id, project_id, "domain"))

        update_source_state(VAULT / "01_Projects", state)

    return nodes, edges
def extract_sandbox_summary(state: dict) -> tuple[list, list]:
    nodes, edges = [], []
    if SANDBOX_RUNS.exists() and source_changed(SANDBOX_RUNS, state):
        total_runs = 0
        status_counts = {}
        review_counts = {"pending": 0, "accepted": 0, "rejected": 0}

        for run_file in SANDBOX_RUNS.rglob("SB-*.md"):
            total_runs += 1
            content = read_file_safe(run_file, "")
            status_match = re.search(r'^status:\s*(\w+)', content, re.M)
            review_match = re.search(r'^review_status:\s*(\w+)', content, re.M)
            if status_match:
                s = status_match.group(1)
                status_counts[s] = status_counts.get(s, 0) + 1
            if review_match:
                r = review_match.group(1)
                if r in review_counts:
                    review_counts[r] += 1

        sb_node_id = "resource-sandbox-engine"
        n = base_node("sandbox_engine", sb_node_id, "Sandbox Engine",
            risk="low", active=True, confidence=0.9,
            summary=f"Sandbox orchestration: {total_runs} runs, {review_counts['accepted']} accepted, {review_counts['rejected']} rejected",
            source_path=str(SANDBOX_RUNS))
        n["data"] = {
            "total_runs": total_runs,
            "status_breakdown": status_counts,
            "review_breakdown": review_counts
        }
        n["evidence"].append(make_evidence(str(SANDBOX_RUNS), "file",
            f"{total_runs} runs, review: {review_counts}"))
        nodes.append(n)
        edges.append(base_edge("manages", "zen-core", sb_node_id, "orchestrator"))

        update_source_state(SANDBOX_RUNS, state)

    return nodes, edges

def extract_worker_activity(state: dict) -> tuple[list, list]:
    nodes, edges = [], []
    if source_changed(LOCAL_WORKER_USAGE, state):
        try:
            wu = json.loads(LOCAL_WORKER_USAGE.read_text())
            runs = wu.get("runs", [])

            worker_node_id = "resource-local-worker"
            n = base_node("worker_activity", worker_node_id, "Local Worker Activity",
                risk="low", active=True, confidence=0.9,
                summary=f"Local worker: {len(runs)} Aider runs via Ollama gpt-oss:20b",
                source_path=str(LOCAL_WORKER_USAGE))
            n["visual"] = make_visual("💻", "#58d4e8", 1.0, False, "none")
            n["data"] = {
                "total_runs": len(runs),
                "model": "gpt-oss:20b",
                "tool": "Aider"
            }
            n["evidence"].append(make_evidence(str(LOCAL_WORKER_USAGE), "file",
                f"{len(runs)} local worker runs"))
            nodes.append(n)
            edges.append(base_edge("executes", "model-worker-local", worker_node_id, "work"))
            edges.append(base_edge("reports_to", worker_node_id, "zen-core", "results"))
        except Exception:
            pass

        update_source_state(LOCAL_WORKER_USAGE, state)

    return nodes, edges

def extract_agent_os_data(state: dict) -> tuple[list, list]:
    nodes, edges = [], []
    if source_changed(AGENT_OS_DATA, state):
        try:
            aos = json.loads(AGENT_OS_DATA.read_text())
            token_usage = aos.get("token_usage", {})
            sandbox = aos.get("sandbox", {})

            today = token_usage.get("today", {})
            if today:
                token_node_id = "resource-tokens-today"
                n = base_node("token_budget", token_node_id, "Today's Token Usage",
                    risk="medium", active=True, confidence=0.9,
                    summary=f"Today: {today.get('calls',0)} calls, ${today.get('cost',0):.4f}, {today.get('in',0):,} in / {today.get('out',0):,} out",
                    source_path=str(AGENT_OS_DATA))
                n["data"] = {
                    "calls": today.get("calls", 0),
                    "cost": today.get("cost", 0),
                    "in_tokens": today.get("in", 0),
                    "out_tokens": today.get("out", 0)
                }
                n["evidence"].append(make_evidence(str(AGENT_OS_DATA), "file",
                    f"Agent OS Data: today's token usage"))
                nodes.append(n)
                edges.append(base_edge("generates", "model-default-cloud", token_node_id, "usage"))

            total_sb = sandbox.get("total", 0)
            if total_sb:
                node_id = "resource-sandbox-summary"
                n = base_node("resource", node_id, "Sandbox Summary",
                    risk="low", active=True, confidence=0.9,
                    summary=f"Sandbox: {total_sb} total, {sandbox.get('accepted',0)} accepted, {sandbox.get('rejected',0)} rejected",
                    source_path=str(AGENT_OS_DATA))
                n["type"] = "resource"
                n["cluster"] = "resources"
                n["layer"] = 3
                n["visual"] = make_visual("📊", "#58d4e8", 1.0, False, "none")
                n["data"] = {
                    "total": total_sb,
                    "accepted": sandbox.get("accepted", 0),
                    "rejected": sandbox.get("rejected", 0),
                    "pending": sandbox.get("needs_review", 0)
                }
                n["evidence"].append(make_evidence(str(AGENT_OS_DATA), "file",
                    f"Agent OS Sandbox summary"))
                nodes.append(n)
        except Exception:
            pass

        update_source_state(AGENT_OS_DATA, state)

    return nodes, edges

def extract_worker_config(state: dict) -> tuple[list, list]:
    nodes, edges = [], []
    if source_changed(ZEN_WORKER, state):
        content = read_file_safe(ZEN_WORKER)
        content = redact_secrets(content)

        worker_node_id = "resource-zen-worker"
        n = base_node("script_layer", worker_node_id, "zen-worker",
            risk="low", active=True, confidence=0.95,
            summary="Guarded local-only worker: Aider + Ollama gpt-oss:20b at ~/.local/bin/zen-worker",
            source_path=str(ZEN_WORKER))
        n["data"] = {
            "path": str(ZEN_WORKER),
            "description": "Guarded local-only worker launcher",
            "safety": "Refuses non-Ollama models and cloud-looking args"
        }
        n["evidence"].append(make_evidence(str(ZEN_WORKER), "file",
            "zen-worker script content"))
        nodes.append(n)
        edges.append(base_edge("configures", "zen-core", worker_node_id, "owns"))
        edges.append(base_edge("invokes", worker_node_id, "model-worker-local", "Aider → Ollama"))

        update_source_state(ZEN_WORKER, state)

    return nodes, edges

# ═══════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

EXTRACTORS = [
    ("models_from_config", extract_models_from_config),
    ("agent_roster", extract_agent_roster),
    ("system_facts", extract_system_facts),
    ("session_graph", extract_session_graph),
    ("token_activity", extract_token_activity),
    ("projects", extract_projects),
    ("sandbox_summary", extract_sandbox_summary),
    ("worker_activity", extract_worker_activity),
    ("agent_os_data", extract_agent_os_data),
    ("worker_config", extract_worker_config),
]

def run_export(incremental: bool = False) -> dict:
    state = load_state()
    all_nodes = []
    all_edges = []
    seen_ids = set()
    extractor_stats = {}

    def add_nodes_edges(new_nodes, new_edges, name):
        for n in new_nodes:
            if n["id"] not in seen_ids:
                all_nodes.append(n)
                seen_ids.add(n["id"])
        all_edges.extend(new_edges)
        extractor_stats[name] = {"nodes": len(new_nodes), "edges": len(new_edges)}

    print("🔄 Running extractors..." + (" (incremental)" if incremental else ""))
    for name, extractor in EXTRACTORS:
        try:
            n, e = extractor(state)
            add_nodes_edges(n, e, name)
            if n or e:
                print(f"   ✅ {name}: {len(n)} nodes, {len(e)} edges")
        except Exception as ex:
            print(f"   ❌ {name}: {ex}")
            extractor_stats[name] = {"error": str(ex)}

    # Validation
    print("🔍 Validating graph...")
    validator = Validator()
    for n in all_nodes:
        validator.validate_node(n)
    for e in all_edges:
        validator.validate_edge(e)
    validator.validate_graph()

    validation_result = validator.get_summary()

    if not validation_result["valid"]:
        print(f"❌ Validation failed: {validation_result['error_count']} errors, {validation_result['warning_count']} warnings")
        for issue in validation_result["issues"]:
            if issue["severity"] == "error":
                print(f"   ERROR [{issue['code']}]: {issue['message']}")
    else:
        print(f"✅ Validation passed: {validation_result['warning_count']} warnings")
        if validation_result["issues"]:
            for issue in validation_result["issues"]:
                print(f"   WARN [{issue['code']}]: {issue['message']}")

    # Build output
    node_types = {}
    for n in all_nodes:
        t = n.get("type", "unknown")
        node_types[t] = node_types.get(t, 0) + 1

    edge_types = {}
    for e in all_edges:
        t = e.get("type", "unknown")
        edge_types[t] = edge_types.get(t, 0) + 1

    clusters = {}
    for n in all_nodes:
        c = n.get("cluster", "unknown")
        clusters[c] = clusters.get(c, 0) + 1

    layers = {}
    for n in all_nodes:
        l = n.get("layer", -1)
        layers[str(l)] = layers.get(str(l), 0) + 1

    confidences = [n.get("confidence", 0) for n in all_nodes if "confidence" in n]
    avg_confidence = sum(confidences) / len(confidences) if confidences else 0

    orphan_adj = {n["id"]: set() for n in all_nodes}
    for e in all_edges:
        orphan_adj[e["source"]].add(e["target"])
        orphan_adj[e["target"]].add(e["source"])
    orphans = [nid for nid, neigh in orphan_adj.items() if len(neigh) == 0]

    output = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "exporter_version": "1.0.0",
        "exporter_git_sha": get_git_sha(),
        "stats": {
            "total_nodes": len(all_nodes),
            "total_edges": len(all_edges),
            "node_types": node_types,
            "edge_types": edge_types,
            "clusters": clusters,
            "layers": layers,
            "avg_confidence": round(avg_confidence, 3),
            "orphan_nodes": len(orphans),
            "validation_warnings": validation_result["warning_count"],
            "validation_errors": validation_result["error_count"],
            "extractor_stats": extractor_stats
        },
        "nodes": all_nodes,
        "edges": all_edges
    }

    # Write output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))

    # Save state
    save_state(state)

    print(f"\n✅ Memory World exported: {OUTPUT_FILE}")
    print(f"   Nodes: {len(all_nodes)} | Edges: {len(all_edges)}")
    print(f"   Clusters: {clusters}")
    print(f"   Avg confidence: {avg_confidence:.2f}")
    if orphans:
        print(f"   ⚠️  Orphans: {orphans}")

    return output

# ═══════════════════════════════════════════════════════════════════════════
# WATCH MODE
# ═══════════════════════════════════════════════════════════════════════════

WATCH_PATHS = [
    HERMES_CONFIG,
    SOUL_FILE,
    ZEN_MEMORY,
    STATE_DB,
    AGENT_LOG,
    LOCAL_WORKER_USAGE,
    TOKEN_USAGE,
    VAULT / "01_Projects",
    SANDBOX_RUNS,
    AGENT_OS_DATA,
    ZEN_WORKER,
]

def watch_mode(interval: int = 5):
    print(f"👀 Watch mode started (interval: {interval}s)")
    print("   Press Ctrl+C to stop")
    print("   Watching:")
    for p in WATCH_PATHS:
        status = "✅" if p.exists() else "❌"
        print(f"   {status} {p}")

    last_hashes = {}
    for p in WATCH_PATHS:
        if p.exists():
            last_hashes[str(p)] = file_hash(p)

    try:
        while True:
            time.sleep(interval)
            changed = False
            for p in WATCH_PATHS:
                if p.exists():
                    current = file_hash(p)
                    if last_hashes.get(str(p)) != current:
                        print(f"\n🔄 Change detected: {p}")
                        last_hashes[str(p)] = current
                        changed = True

            if changed:
                print("   Re-running export...")
                run_export(incremental=True)
                print("   ✅ Export complete, watching...")

    except KeyboardInterrupt:
        print("\n👋 Watch mode stopped")

# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Zen Graph Exporter — M3 Hardened")
    parser.add_argument("--watch", action="store_true", help="Run in watch mode (hot reload)")
    parser.add_argument("--interval", type=int, default=5, help="Watch interval in seconds")
    parser.add_argument("--validate-only", action="store_true", help="Only validate existing output")
    args = parser.parse_args()

    if args.validate_only:
        if OUTPUT_FILE.exists():
            data = json.loads(OUTPUT_FILE.read_text())
            print(f"Validating {OUTPUT_FILE}...")
            validator = Validator()
            for n in data.get("nodes", []):
                validator.validate_node(n)
            for e in data.get("edges", []):
                validator.validate_edge(e)
            validator.validate_graph()
            result = validator.get_summary()
            print(f"Valid: {result['valid']} | Errors: {result['error_count']} | Warnings: {result['warning_count']}")
            for issue in result["issues"]:
                print(f"  {issue['severity'].upper()} [{issue['code']}]: {issue['message']}")
            return 0 if result["valid"] else 1
        else:
            print(f"Output file not found: {OUTPUT_FILE}")
            return 1

    if args.watch:
        watch_mode(args.interval)
        return 0

    return 0 if run_export()["stats"]["validation_errors"] == 0 else 1

if __name__ == "__main__":
    sys.exit(main())