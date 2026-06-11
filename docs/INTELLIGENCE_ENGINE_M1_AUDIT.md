# Intelligence Engine — M1 System Inspection Audit

**Date:** 2026-06-11
**Status:** Approved — Proceed to M2
**Author:** Minato (via Zen)

---

## Executive Summary

**Primary Finding:** Monetisation assets exist (skills, scripts, Obsidian project files, lead gen pipeline), but **Intelligence Engine infrastructure does not exist**. No PostgreSQL, no pgvector, no scraping framework, no monitoring stack, no cron orchestration.

**Conclusion:** Greenfield infrastructure build required. Existing Minato assets are reusable as *product modules* but not as *engine components*.

---

## Asset Inventory (Actual Audit Results)

### Hermes Skills (81 total)

| Category | Count | Reusable for Intelligence Engine |
|----------|-------|----------------------------------|
| Custom Minato skills | 3 | **YES** — local-business-prospector, fiverr-proposal-writer, ai-video-script-writer |
| Custom Zen skills | 2 | **YES** — zen-agent-architecture, zen-sandbox |
| Autonomous AI agents | 7 | NO — delegation layer only |
| Creative | 22 | NO |
| Data science | 1 | NO |
| Email | 1 | **YES** — himalaya (IMAP/SMTP for M8) |
| GitHub | 6 | **YES** — versioning engine components |
| Media | 4 | NO |
| MLOps | 12 | NO |
| Note-taking | 1 | **YES** — obsidian (vault integration) |
| Productivity | 10 | PARTIAL — agent-os-dashboard needs upgrade |
| Quantitative trading | 1 | NO |
| Red-teaming | 1 | NO |
| Research | 6 | NO |
| Smart-home | 1 | NO |
| Social-media | 1 | NO |
| Software-dev | 13 | PARTIAL — plan, spike, systematic-debugging useful |

**Key Gap:** Zero skills for: PostgreSQL, pgvector, Crawl4AI, Playwright, Scrapy, Firecrawl, changedetection.io, Uptime Kuma, APScheduler, sentence-transformers, FastAPI.

---

### Scripts (`~/.hermes/scripts/`)

| Script | Lines | Reuse Classification | Required Work |
|--------|-------|---------------------|---------------|
| `local_prospector.py` | 418 | **UPGRADE** | Replace Google Maps API with Crawl4AI/Playwright; add async; PostgreSQL sink; embedding generation; rate limiting; error handling; structured logging |
| `email_outreach.py` | 298 | **REUSE AS-IS** | Template engine upgrade for structured alerts (M8) |

---

### Cron Jobs

**Active: 0** — Clean slate for M3 orchestration design.

---

### Databases

| Type | Status |
|------|--------|
| SQLite | None found |
| PostgreSQL | Not installed/configured |
| Vector store (Chroma/FAISS/pgvector) | Not installed |
| JSON/CSV | Ad-hoc outputs only |

**Verdict:** Greenfield. M2 must design PostgreSQL + pgvector schema from scratch.

---

### Obsidian Vault (`~/Obsidian/ZenVault/`)

| Path | Role in Intelligence Engine |
|------|----------------------------|
| `00_System/SOUL.md` | Master index → Engine metadata registry |
| `00_System/Project Instructions/*.md` | Agent configs → Engine domain configs |
| `00_System/Project Memories/*.md` | Durable facts → Engine state persistence |
| `01_Projects/` | 10+ project files → Product catalog (M9) |
| `Sandbox/Runs/` | 100+ runs → Training data for gap detection (M5) |
| `Kiyosaki/Swarm/` | Trading intelligence → Reference pattern for ensemble scoring |

**Integration Pattern:** Intelligence Engine writes structured `.md` with frontmatter to `01_Projects/Intelligence/` for Dataview queries; PostgreSQL remains primary query layer.

---

### Scraping / Monitoring / Lead Gen Capabilities

| Capability | Current State | Gap |
|------------|---------------|-----|
| Web scraping | urllib + BeautifulSoup (basic) | Need: Playwright, Crawl4AI, Firecrawl, Scrapy |
| Google Maps/Places | Google Places API + DuckDuckGo fallback | Need: Rate-limited crawler or Apify alternative |
| Website analysis | Basic HTTP + content scan | Need: Lighthouse CI, PageSpeed API, Wappalyzer, mobile emulation |
| Email finding | Regex on homepage + contact pages | Need: Hunter.io/Apollo API or robust crawler |
| Monitoring | **None** | Need: changedetection.io, Uptime Kuma, ntfy/Apprise |
| Lead generation | Single-niche, single-city script | Need: Multi-niche, multi-geo, continuous, enriched |
| Vector search/embeddings | **None** | Need: sentence-transformers + pgvector |

---

## Gap Analysis (Prioritized for M3 Build)

| Priority | Missing Capability | M3 Target |
|----------|-------------------|-----------|
| **P0** | PostgreSQL + pgvector database | Provision + schema (M2) |
| **P0** | Async scraping framework | Crawl4AI + Playwright collection pipeline |
| **P0** | Continuous scheduler | Hermes cron + APScheduler orchestration |
| **P1** | Local embeddings | sentence-transformers (all-MiniLM-L6-v2) |
| **P1** | Monitoring stack | changedetection.io + Uptime Kuma + ntfy |
| **P1** | Alert email templates | Structured alert templates (M8) |
| **P2** | Technographic detection | Wappalyzer/builtwith integration |
| **P2** | Lighthouse CI / PageSpeed | Report engine integration (M6) |
| **P2** | Google Maps API management | Lead gen source adapter (M4) |
| **P3** | Apify/ScrapingBee | Future scale |

---

## Approved Architecture (Post-Audit)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        INTELLIGENCE ENGINE ARCHITECTURE                      │
│                                                                              │
│  SOURCES          COLLECTION           PROCESSING            STORAGE         │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │Google Bus│───▶│  Crawl4AI    │───▶│  Cleaning    │───▶│ PostgreSQL   │  │
│  │Directories    │  Playwright  │    │  Classification│    │ + pgvector  │  │
│  │Reviews   │    │  Firecrawl   │    │  Enrichment  │    │ Obsidian    │  │
│  │Public DBs│    │  Scrapy      │    │  Embeddings  │    │ (MD files)  │  │
│  │Industry  │    │  Hermes cron │    │  Scoring     │    │ Vector idx  │  │
│  └──────────┘    └──────────────┘    └──────────────┘    └──────┬──────┘  │
│                                                                   │         │
│                                                                   ▼         │
│  PRODUCTS          INTELLIGENCE       QUERY              ACCESS          │
│  ┌──────────┐    ┌──────────────┐   ┌──────────┐    ┌──────────────┐    │
│  │Leads DB  │◀───│ Gap detect   │◀──│ SQL      │◀── │ API (FastAPI)│    │
│  │Reports   │    │ Scoring      │   │ Vector   │    │ Dashboard    │    │
│  │Alerts    │    │ Clustering   │   │ Graph    │    │ Email        │    │
│  │Datasets  │    │ Trends       │   │ LLM RAG  │    │ Obsidian     │    │
│  │APIs      │    │ Summarize    │   │          │    │ (Dataview)   │    │
│  └──────────┘    └──────────────┘   └──────────┘    └──────────────┘    │
│                                                                              │
│  ORCHESTRATION: Hermes Cron (declarative) + APScheduler (dynamic)          │
│  MONITORING: changedetection.io + Uptime Kuma + ntfy/Apprise               │
│  EMBEDDINGS: sentence-transformers (local, free)                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Decisions (Locked)

| Component | Decision | Rationale |
|-----------|----------|-----------|
| Primary DB | PostgreSQL + pgvector | ACID, SQL + vector unified, free, scales |
| Vector embeddings | sentence-transformers (all-MiniLM-L6-v2) | Local, free, 384-dim, fast, good for business text |
| Scraping (primary) | Crawl4AI | LLM-friendly markdown output, async, free |
| Scraping (JS-heavy) | Playwright | Full browser automation for dynamic sites |
| Scraping (fallback) | Firecrawl API | Managed, handles anti-bot |
| Scheduling (simple) | Hermes cron | Declarative, integrated, visible |
| Scheduling (complex) | APScheduler | Dynamic schedules, job stores, persistence |
| Monitoring | changedetection.io + Uptime Kuma + ntfy | Self-hosted, webhook-friendly, multi-channel alerts |
| API layer | FastAPI | Async, auto-docs, Pydantic, modern |
| Dashboard (ops) | Agent OS Dashboard (vanilla HTML/JS) | Existing, browser-verified |
| Dashboard (data) | Streamlit | Rapid ML/data tooling |
| Obsidian sync | SQL → Markdown + frontmatter (scheduled) | Human-readable, queryable, version-controlled |

---

## Integration Map (Existing → Engine)

| Existing Asset | Engine Module | Integration Method |
|----------------|---------------|-------------------|
| local-business-prospector skill | Lead Gen (M4) | Wrap as callable module; cron-triggered |
| fiverr-proposal-writer skill | Delivery (M9) | Auto-generate proposals from scored leads |
| ai-video-script-writer skill | Content (M9) | Generate scripts from opportunity reports |
| local_prospector.py | Collection (M3) | Refactor → Google Maps source adapter |
| email_outreach.py | Alert Delivery (M8) | Refactor → structured alert email backend |
| Obsidian vault | Knowledge Layer | Write structured `.md` to `01_Projects/Intelligence/` |
| Hermes cron | Orchestration | All pipelines registered as cron jobs |
| zen-agent-architecture | Governance | Engine operates as Minato-domain service |

---

## M1 Checkpoint: APPROVED

**Decision:** Proceed to M2 — Intelligence Schema Design with PostgreSQL + pgvector as locked target architecture.

**Next:** Design complete schema for Businesses, Leads, Competitors, Reviews, Monitoring Targets, Websites, Reports, Alerts, Opportunities with relationships, source attribution, confidence scoring, update history, change tracking.