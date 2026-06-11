# Intelligence Engine — M2 Schema Design

**Date:** 2026-06-11
**Status:** Draft — Pending Review
**Target:** PostgreSQL 16+ with pgvector 0.7+
**Author:** Minato (via Zen)

---

## Design Principles

1. **Source attribution on every row** — Know where data came from, when, and with what confidence
2. **Change tracking by default** — Every mutable column has history via `*_history` tables
3. **Confidence scoring** — All derived/enriched fields carry 0.0–1.0 confidence
4. **Soft deletes** — `deleted_at` timestamp, never hard delete
5. **Composite primary keys** — Natural keys where possible (source + source_id)
6. **Partitioning ready** — Time-series tables (monitoring, alerts) designed for partitioning
7. **Vector columns alongside SQL** — pgvector for embeddings, SQL for structured queries

---

## Core Tables

### 1. sources — Data Source Registry

```sql
CREATE TABLE sources (
    source_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               VARCHAR(100) NOT NULL UNIQUE,      -- 'google_maps', 'yelp', 'companies_house'
    type               VARCHAR(30) NOT NULL,              -- 'api', 'scraper', 'feed', 'manual'
    base_url           VARCHAR(500),
    api_key_name       VARCHAR(100),                      -- env var name for secrets
    rate_limit_rpm     INT DEFAULT 60,                    -- requests per minute
    rate_limit_rph     INT DEFAULT 1000,                  -- requests per hour
    config             JSONB DEFAULT '{}',                -- source-specific config
    is_active          BOOLEAN DEFAULT TRUE,
    priority           INT DEFAULT 10,                    -- lower = higher priority
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sources_type_active ON sources(type, is_active);
```

---

### 2. businesses — Core Entity

```sql
CREATE TABLE businesses (
    business_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Natural key: source + source_id (one business per source)
    source_id          UUID NOT NULL REFERENCES sources(source_id),
    source_business_id VARCHAR(200) NOT NULL,             -- place_id, company_number, etc.
    
    -- Canonical fields (best available across sources)
    name               VARCHAR(300) NOT NULL,
    normalized_name    VARCHAR(300) GENERATED ALWAYS AS (
        lower(regexp_replace(name, '[^a-z0-9]+', '', 'g'))
    ) STORED,
    
    -- Address (structured)
    address_line1      VARCHAR(300),
    address_line2      VARCHAR(300),
    city               VARCHAR(100),
    region             VARCHAR(100),                       -- state/province/county
    postal_code        VARCHAR(20),
    country            VARCHAR(2) DEFAULT 'GB',           -- ISO 3166-1 alpha-2
    lat                DECIMAL(10, 8),
    lng                DECIMAL(11, 8),
    
    -- Contact
    phone              VARCHAR(50),
    phone_normalized   VARCHAR(20) GENERATED ALWAYS AS (
        regexp_replace(phone, '[^0-9+]', '', 'g')
    ) STORED,
    email              VARCHAR(255),
    website            VARCHAR(500),
    website_normalized VARCHAR(500) GENERATED ALWAYS AS (
        lower(regexp_replace(coalesce(website, ''), '^https?://(www\.)?', ''))
    ) STORED,
    
    -- Classification
    primary_category   VARCHAR(100),                      -- 'barber', 'dentist', 'saas'
    categories         TEXT[],                            -- all categories from sources
    sic_code           VARCHAR(10),                       -- Standard Industrial Classification
    naics_code         VARCHAR(10),                       -- North American Industry Classification
    
    -- Metadata
    rating             DECIMAL(3, 2),                     -- 0.00–5.00
    review_count       INT DEFAULT 0,
    price_level        INT,                               -- 1–4 (Google style)
    is_permanently_closed BOOLEAN DEFAULT FALSE,
    
    -- Source attribution & confidence
    confidence         DECIMAL(3, 2) DEFAULT 0.50,        -- 0.00–1.00 overall confidence
    first_seen_at      TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at       TIMESTAMPTZ DEFAULT NOW(),
    last_enriched_at   TIMESTAMPTZ,
    deleted_at         TIMESTAMPTZ,
    
    -- Vector embedding (384-dim for all-MiniLM-L6-v2)
    embedding          VECTOR(384),
    
    UNIQUE (source_id, source_business_id)
);

CREATE INDEX idx_businesses_normalized_name ON businesses(normalized_name);
CREATE INDEX idx_businesses_city_region ON businesses(city, region);
CREATE INDEX idx_businesses_category ON businesses(primary_category);
CREATE INDEX idx_businesses_website_norm ON businesses(website_normalized) WHERE website_normalized != '';
CREATE INDEX idx_businesses_embedding ON businesses USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 200);
CREATE INDEX idx_businesses_active ON businesses(deleted_at) WHERE deleted_at IS NULL;
```

---

### 3. business_source_refs — Multi-Source Linkage

```sql
CREATE TABLE business_source_refs (
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    source_id          UUID NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    source_business_id VARCHAR(200) NOT NULL,
    match_confidence   DECIMAL(3, 2) NOT NULL,            -- 0.00–1.00 match quality
    match_method       VARCHAR(30) NOT NULL,              -- 'exact_name_addr', 'fuzzy', 'website', 'phone', 'manual'
    matched_at         TIMESTAMPTZ DEFAULT NOW(),
    matched_by         VARCHAR(50) DEFAULT 'system',      -- 'system', 'llm', 'human'
    is_primary         BOOLEAN DEFAULT FALSE,             -- one primary per business
    
    PRIMARY KEY (business_id, source_id)
);

CREATE INDEX idx_bsr_source ON business_source_refs(source_id, source_business_id);
CREATE INDEX idx_bsr_confidence ON business_source_refs(match_confidence DESC);
```

---

### 4. leads — Qualified Prospects

```sql
CREATE TABLE leads (
    lead_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    
    -- Qualification
    status             VARCHAR(30) NOT NULL DEFAULT 'new', -- 'new', 'qualified', 'contacted', 'responded', 'quoted', 'won', 'lost', 'dead'
    priority           VARCHAR(10) NOT NULL DEFAULT 'medium', -- 'high', 'medium', 'low'
    score              DECIMAL(5, 2) DEFAULT 0.00,         -- 0–100 opportunity score (M5)
    score_breakdown    JSONB DEFAULT '{}',                 -- component scores
    
    -- Gaps detected (M5)
    gaps               JSONB DEFAULT '{}',                  -- {"no_website": true, "weak_seo": 0.8, "no_booking": true, ...}
    gap_score          DECIMAL(4, 2) DEFAULT 0.00,         -- 0–100 gap severity
    
    -- Enrichment
    contact_email      VARCHAR(255),
    contact_phone      VARCHAR(50),
    contact_name       VARCHAR(200),
    contact_role       VARCHAR(100),                       -- 'owner', 'manager', 'marketing'
    socials            JSONB DEFAULT '{}',                 -- {"facebook": "...", "instagram": "...", "linkedin": "..."}
    website_analysis   JSONB DEFAULT '{}',                 -- from M6: lighthouse, psi, technographics
    
    -- Outreach tracking
    outreach_count     INT DEFAULT 0,
    last_outreach_at   TIMESTAMPTZ,
    last_response_at   TIMESTAMPTZ,
    next_followup_at   TIMESTAMPTZ,
    assigned_to        VARCHAR(100),                       -- agent/persona
    
    -- Attribution
    source_id          UUID REFERENCES sources(source_id),  -- originating source
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW(),
    qualified_at       TIMESTAMPTZ,
    deleted_at         TIMESTAMPTZ
);

CREATE INDEX idx_leads_status_priority ON leads(status, priority, score DESC);
CREATE INDEX idx_leads_business ON leads(business_id);
CREATE INDEX idx_leads_assigned ON leads(assigned_to);
CREATE INDEX idx_leads_followup ON leads(next_followup_at) WHERE next_followup_at IS NOT NULL;
CREATE INDEX idx_leads_active ON leads(deleted_at) WHERE deleted_at IS NULL;
```

---

### 5. competitors — Competitor Tracking

```sql
CREATE TABLE competitors (
    competitor_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    competitor_business_id UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    
    relationship_type  VARCHAR(30) NOT NULL,               -- 'direct', 'indirect', 'aspirational', 'disruptor'
    similarity_score   DECIMAL(3, 2) DEFAULT 0.00,         -- 0.00–1.00 (embedding + category + geo)
    detected_via       VARCHAR(30) NOT NULL,               -- 'embedding', 'category', 'keyword', 'manual'
    notes              TEXT,
    
    -- Tracking
    is_monitored       BOOLEAN DEFAULT TRUE,
    monitor_frequency  INTERVAL DEFAULT '1 day',
    last_compared_at   TIMESTAMPTZ,
    
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW(),
    deleted_at         TIMESTAMPTZ,
    
    UNIQUE (business_id, competitor_business_id)
);

CREATE INDEX idx_competitors_business ON competitors(business_id);
CREATE INDEX idx_competitors_monitored ON competitors(is_monitored) WHERE is_monitored AND deleted_at IS NULL;
```

---

### 6. reviews — Review Aggregation

```sql
CREATE TABLE reviews (
    review_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    source_id          UUID NOT NULL REFERENCES sources(source_id),
    source_review_id   VARCHAR(200) NOT NULL,
    
    author_name        VARCHAR(200),
    author_profile_url VARCHAR(500),
    rating             DECIMAL(3, 2) NOT NULL,             -- 1.00–5.00
    text               TEXT,
    language           VARCHAR(10) DEFAULT 'en',
    published_at       TIMESTAMPTZ,
    retrieved_at       TIMESTAMPTZ DEFAULT NOW(),
    
    -- Sentiment (derived)
    sentiment_score    DECIMAL(3, 2),                      -- -1.00 to 1.00
    sentiment_label    VARCHAR(20),                        -- 'positive', 'neutral', 'negative'
    key_topics         TEXT[],                             -- extracted topics
    
    -- Attribution
    confidence         DECIMAL(3, 2) DEFAULT 1.00,
    deleted_at         TIMESTAMPTZ,
    
    UNIQUE (source_id, source_review_id)
);

CREATE INDEX idx_reviews_business ON reviews(business_id, published_at DESC);
CREATE INDEX idx_reviews_rating ON reviews(business_id, rating);
CREATE INDEX idx_reviews_sentiment ON reviews(sentiment_label);
```

---

### 7. monitoring_targets — What We Watch (M7)

```sql
CREATE TABLE monitoring_targets (
    target_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID REFERENCES businesses(business_id) ON DELETE SET NULL,
    competitor_id      UUID REFERENCES competitors(competitor_id) ON DELETE SET NULL,
    
    target_type        VARCHAR(30) NOT NULL,               -- 'website', 'reviews', 'pricing', 'hiring', 'social', 'news', 'filings'
    source_id          UUID REFERENCES sources(source_id), -- primary source for this target
    source_target_id   VARCHAR(200),                       -- URL, page ID, feed ID
    
    -- Configuration
    check_frequency    INTERVAL NOT NULL DEFAULT '1 hour',
    check_method       VARCHAR(30) NOT NULL,               -- 'http', 'api', 'rss', 'changedetection', 'playwright'
    check_config       JSONB DEFAULT '{}',                 -- selectors, headers, auth, etc.
    
    -- State
    last_checked_at    TIMESTAMPTZ,
    last_changed_at    TIMESTAMPTZ,
    last_content_hash  VARCHAR(64),                        -- SHA256 of content
    last_status_code   INT,
    consecutive_failures INT DEFAULT 0,
    is_active          BOOLEAN DEFAULT TRUE,
    is_paused          BOOLEAN DEFAULT FALSE,
    pause_reason       TEXT,
    
    -- Notification
    alert_on_change    BOOLEAN DEFAULT TRUE,
    alert_threshold    JSONB DEFAULT '{}',                 -- e.g., {"price_change_pct": 5, "rating_drop": 0.5}
    notification_channels TEXT[] DEFAULT ARRAY['email'],   -- 'email', 'dashboard', 'webhook', 'telegram'
    
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW(),
    deleted_at         TIMESTAMPTZ,
    
    CHECK (business_id IS NOT NULL OR competitor_id IS NOT NULL)
);

CREATE INDEX idx_mt_business ON monitoring_targets(business_id);
CREATE INDEX idx_mt_competitor ON monitoring_targets(competitor_id);
CREATE INDEX idx_mt_active_due ON monitoring_targets(is_active, is_paused, last_checked_at)
    WHERE is_active AND NOT is_paused AND deleted_at IS NULL;
CREATE INDEX idx_mt_type ON monitoring_targets(target_type);
```

---

### 8. monitoring_events — Change Events (M7)

```sql
CREATE TABLE monitoring_events (
    event_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target_id          UUID NOT NULL REFERENCES monitoring_targets(target_id) ON DELETE CASCADE,
    
    event_type         VARCHAR(30) NOT NULL,               -- 'content_change', 'status_change', 'new_item', 'removed_item', 'metric_threshold'
    severity           VARCHAR(10) NOT NULL DEFAULT 'info', -- 'info', 'warning', 'critical'
    
    -- Change details
    old_value          JSONB,
    new_value          JSONB,
    diff_summary       TEXT,                               -- human-readable summary
    change_score       DECIMAL(3, 2),                      -- 0.00–1.00 magnitude of change
    
    -- Content (for content changes)
    old_content_hash   VARCHAR(64),
    new_content_hash   VARCHAR(64),
    extracted_data     JSONB,                              -- structured data from change
    
    detected_at        TIMESTAMPTZ DEFAULT NOW(),
    processed_at       TIMESTAMPTZ,
    acknowledged_at    TIMESTAMPTZ,
    acknowledged_by    VARCHAR(100),
    
    -- Partitioning: by month on detected_at
    -- (Use pg_partman or native partitioning in production)
    
    deleted_at         TIMESTAMPTZ
);

CREATE INDEX idx_me_target ON monitoring_events(target_id, detected_at DESC);
CREATE INDEX idx_me_type_severity ON monitoring_events(event_type, severity);
CREATE INDEX idx_me_unprocessed ON monitoring_events(processed_at) WHERE processed_at IS NULL;
```

---

### 9. websites — Website Analysis (M6)

```sql
CREATE TABLE websites (
    website_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    url                VARCHAR(500) NOT NULL,
    url_normalized     VARCHAR(500) GENERATED ALWAYS AS (
        lower(regexp_replace(url, '^https?://(www\.)?', ''))
    ) STORED,
    
    -- Technical
    status_code        INT,
    final_url          VARCHAR(500),                       -- after redirects
    response_time_ms   INT,
    ssl_valid          BOOLEAN,
    ssl_expiry         TIMESTAMPTZ,
    
    -- Content
    title              VARCHAR(500),
    meta_description   TEXT,
    h1_tags            TEXT[],
    word_count         INT,
    has_contact_form   BOOLEAN DEFAULT FALSE,
    has_booking        BOOLEAN DEFAULT FALSE,
    has_analytics      BOOLEAN DEFAULT FALSE,
    has_chat           BOOLEAN DEFAULT FALSE,
    
    -- Technographics (Wappalyzer/builtwith)
    cms                VARCHAR(50),                        -- 'WordPress', 'Shopify', 'Wix', 'Squarespace', 'custom'
    cms_version        VARCHAR(30),
    framework          VARCHAR(50),                        -- 'React', 'Vue', 'Next.js', 'Laravel'
    hosting            VARCHAR(100),                       -- 'AWS', 'Cloudflare', 'GoDaddy', 'DigitalOcean'
    analytics          TEXT[],                             -- 'Google Analytics', 'Matomo', 'Plausible'
    cdn                VARCHAR(50),
    technologies       TEXT[],                             -- all detected
    
    -- Performance (Lighthouse/PageSpeed)
    lighthouse_score   DECIMAL(4, 1),                      -- 0–100
    psi_mobile_score   DECIMAL(4, 1),
    psi_desktop_score  DECIMAL(4, 1),
    cli_score          DECIMAL(4, 1),                      -- CLI Lighthouse
    core_web_vitals    JSONB DEFAULT '{}',                 -- LCP, FID, CLS, INP, TTFB
    
    -- Mobile
    is_mobile_friendly BOOLEAN,
    viewport_configured BOOLEAN,
    tap_targets_ok     BOOLEAN,
    
    -- SEO
    has_ssl            BOOLEAN,
    has_sitemap        BOOLEAN,
    has_robots_txt     BOOLEAN,
    meta_robots        VARCHAR(50),
    canonical_url      VARCHAR(500),
    structured_data    JSONB DEFAULT '{}',                 -- JSON-LD, Microdata
    
    -- Accessibility
    a11y_score         DECIMAL(4, 1),
    a11y_violations    INT DEFAULT 0,
    
    -- Analysis timestamp
    analyzed_at        TIMESTAMPTZ DEFAULT NOW(),
    analyzer_version   VARCHAR(20) DEFAULT '1.0',
    
    deleted_at         TIMESTAMPTZ,
    
    UNIQUE (business_id, url_normalized)
);

CREATE INDEX idx_websites_business ON websites(business_id);
CREATE INDEX idx_websites_url_norm ON websites(url_normalized);
CREATE INDEX idx_websites_cms ON websites(cms);
CREATE INDEX idx_websites_analyzed ON websites(analyzed_at DESC);
```

---

### 10. reports — Generated Reports (M6)

```sql
CREATE TABLE reports (
    report_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID REFERENCES businesses(business_id) ON DELETE SET NULL,
    lead_id            UUID REFERENCES leads(lead_id) ON DELETE SET NULL,
    competitor_id      UUID REFERENCES competitors(competitor_id) ON DELETE SET NULL,
    
    report_type        VARCHAR(30) NOT NULL,               -- 'website_audit', 'seo_audit', 'competitor_audit', 'ai_opportunity', 'growth_opportunity'
    title              VARCHAR(300) NOT NULL,
    summary            TEXT,                               -- executive summary
    
    -- Content (structured + markdown)
    sections           JSONB NOT NULL,                     -- [{"id": "...", "title": "...", "content_md": "...", "score": 0.0, "findings": [...]}]
    markdown_content   TEXT,                               -- full rendered markdown
    html_content       TEXT,                               -- full rendered HTML
    
    -- Scores
    overall_score      DECIMAL(4, 1),                      -- 0–100
    category_scores    JSONB DEFAULT '{}',                 -- {"technical": 85, "content": 70, "seo": 60, "conversion": 45}
    
    -- Generation
    generated_by       VARCHAR(50) NOT NULL,               -- 'intelligence_engine', 'llm_pipeline', 'manual'
    generation_model   VARCHAR(100),                       -- 'gpt-oss:20b', 'owl-alpha'
    generation_prompt_hash VARCHAR(64),                   -- for reproducibility
    generation_time_ms INT,
    
    -- Delivery
    status             VARCHAR(20) DEFAULT 'draft',        -- 'draft', 'review', 'final', 'delivered', 'archived'
    delivery_channels  TEXT[] DEFAULT ARRAY['dashboard'],  -- 'dashboard', 'email', 'pdf', 'api', 'obsidian'
    delivered_at       TIMESTAMPTZ,
    delivered_to       VARCHAR(200),                       -- email, client_id, etc.
    
    -- Versioning
    version            INT DEFAULT 1,
    parent_report_id   UUID REFERENCES reports(report_id),
    change_log         JSONB DEFAULT '[]',                 -- [{"version": 1, "changed_by": "...", "changes": "..."}]
    
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW(),
    deleted_at         TIMESTAMPTZ,
    
    CHECK (business_id IS NOT NULL OR lead_id IS NOT NULL OR competitor_id IS NOT NULL)
);

CREATE INDEX idx_reports_business ON reports(business_id, created_at DESC);
CREATE INDEX idx_reports_lead ON reports(lead_id);
CREATE INDEX idx_reports_type_status ON reports(report_type, status);
CREATE INDEX idx_reports_delivered ON reports(delivered_at) WHERE delivered_at IS NOT NULL;
```

---

### 11. alerts — Alert Engine Output (M8)

```sql
CREATE TABLE alerts (
    alert_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID REFERENCES businesses(business_id) ON DELETE SET NULL,
    lead_id            UUID REFERENCES leads(lead_id) ON DELETE SET NULL,
    monitoring_event_id UUID REFERENCES monitoring_events(event_id) ON DELETE SET NULL,
    report_id          UUID REFERENCES reports(report_id) ON DELETE SET NULL,
    
    alert_type         VARCHAR(30) NOT NULL,               -- 'daily_digest', 'weekly_summary', 'monthly_intel', 'threshold_breach', 'new_opportunity', 'competitor_move'
    severity           VARCHAR(10) NOT NULL DEFAULT 'info', -- 'info', 'warning', 'critical'
    title              VARCHAR(300) NOT NULL,
    summary            TEXT,                               -- 1-2 sentence summary
    body_md            TEXT,                               -- full markdown body
    body_html          TEXT,                               -- full HTML body
    
    -- Data references
    data_refs          JSONB DEFAULT '{}',                 -- {"business_ids": [...], "event_ids": [...], "report_ids": [...]}
    
    -- Delivery
    channels           TEXT[] NOT NULL DEFAULT ARRAY['dashboard'], -- 'email', 'dashboard', 'webhook', 'telegram', 'slack'
    email_recipients   TEXT[],                             -- specific emails
    email_sent_at      TIMESTAMPTZ,
    email_status       VARCHAR(20),                        -- 'pending', 'sent', 'failed', 'skipped'
    dashboard_pushed_at TIMESTAMPTZ,
    webhook_sent_at    TIMESTAMPTZ,
    
    -- Scheduling
    scheduled_for      TIMESTAMPTZ NOT NULL,               -- when it should be delivered
    generated_at       TIMESTAMPTZ DEFAULT NOW(),
    generated_by       VARCHAR(50) DEFAULT 'alert_engine',
    
    -- Deduplication
    dedupe_key         VARCHAR(200),                       -- prevents duplicate alerts
    is_dedupe          BOOLEAN DEFAULT FALSE,
    original_alert_id  UUID REFERENCES alerts(alert_id),
    
    status             VARCHAR(20) DEFAULT 'pending',      -- 'pending', 'delivered', 'failed', 'cancelled'
    acknowledged_at    TIMESTAMPTZ,
    acknowledged_by    VARCHAR(100),
    
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW(),
    deleted_at         TIMESTAMPTZ
);

CREATE INDEX idx_alerts_scheduled ON alerts(scheduled_for, status) WHERE status = 'pending';
CREATE INDEX idx_alerts_business ON alerts(business_id, created_at DESC);
CREATE INDEX idx_alerts_type_severity ON alerts(alert_type, severity);
CREATE INDEX idx_alerts_dedupe ON alerts(dedupe_key) WHERE dedupe_key IS NOT NULL;
```

---

### 12. opportunities — Scored Opportunities (M5/M9)

```sql
CREATE TABLE opportunities (
    opportunity_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    lead_id            UUID REFERENCES leads(lead_id) ON DELETE SET NULL,
    
    opportunity_type   VARCHAR(30) NOT NULL,               -- 'website_build', 'website_rebuild', 'seo_fix', 'booking_system', 'ai_chatbot', 'ai_automation', 'review_management', 'local_seo', 'paid_ads', 'content_marketing'
    title              VARCHAR(300) NOT NULL,
    description        TEXT,
    
    -- Scoring
    score              DECIMAL(5, 2) NOT NULL,             -- 0–100
    score_components   JSONB NOT NULL,                     -- {"gap_severity": 40, "market_size": 30, "competition": 15, "budget_fit": 15}
    revenue_estimate   DECIMAL(10, 2),                     -- estimated project value
    revenue_confidence DECIMAL(3, 2) DEFAULT 0.50,
    effort_estimate    VARCHAR(20),                        -- 'xs', 's', 'm', 'l', 'xl'
    effort_hours       DECIMAL(6, 1),
    
    -- Market context
    niche              VARCHAR(100),                       -- 'barber', 'dentist', 'saas'
    location           VARCHAR(200),                       -- city, region
    competitors_count  INT DEFAULT 0,
    market_saturation  DECIMAL(3, 2),                      -- 0.00–1.00
    
    -- Product matching (M9)
    product_matches    JSONB DEFAULT '[]',                 -- [{"product_id": "...", "fit_score": 0.85, "reasoning": "..."}]
    
    -- Status
    status             VARCHAR(20) DEFAULT 'identified',   -- 'identified', 'validated', 'pitched', 'quoted', 'won', 'lost', 'deferred'
    priority           VARCHAR(10) DEFAULT 'medium',
    
    identified_at      TIMESTAMPTZ DEFAULT NOW(),
    validated_at       TIMESTAMPTZ,
    pitched_at         TIMESTAMPTZ,
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW(),
    deleted_at         TIMESTAMPTZ
);

CREATE INDEX idx_opp_business ON opportunities(business_id);
CREATE INDEX idx_opp_lead ON opportunities(lead_id);
CREATE INDEX idx_opp_score_status ON opportunities(score DESC, status);
CREATE INDEX idx_opp_niche_location ON opportunities(niche, location);
CREATE INDEX idx_opp_product_match ON opportunities USING gin (product_matches);
```

---

## Change History Tables (Audit Trail)

### Pattern: One history table per mutable entity

```sql
-- Example: businesses_history
CREATE TABLE businesses_history (
    history_id         BIGSERIAL PRIMARY KEY,
    business_id        UUID NOT NULL,
    changed_at         TIMESTAMPTZ DEFAULT NOW(),
    changed_by         VARCHAR(100),                       -- 'system', 'llm', 'human', 'api'
    operation          VARCHAR(10) NOT NULL,               -- 'INSERT', 'UPDATE', 'DELETE'
    old_values         JSONB,
    new_values         JSONB,
    changed_fields     TEXT[],
    change_reason      VARCHAR(200)
);

CREATE INDEX idx_bh_business ON businesses_history(business_id, changed_at DESC);
CREATE INDEX idx_bh_changed_at ON businesses_history(changed_at DESC);

-- Repeat for: leads, competitors, reviews, monitoring_targets, monitoring_events, websites, reports, alerts, opportunities
```

---

## Views (Common Query Patterns)

```sql
-- Active businesses with latest enrichment
CREATE VIEW v_active_businesses AS
SELECT b.*, 
       l.status as lead_status,
       l.score as lead_score,
       w.lighthouse_score,
       w.cms,
       w.analyzed_at as website_analyzed_at
FROM businesses b
LEFT JOIN leads l ON l.business_id = b.business_id AND l.deleted_at IS NULL
LEFT JOIN (
    SELECT DISTINCT ON (business_id) * 
    FROM websites 
    WHERE deleted_at IS NULL 
    ORDER BY business_id, analyzed_at DESC
) w ON w.business_id = b.business_id
WHERE b.deleted_at IS NULL;

-- Leads ready for outreach
CREATE VIEW v_leads_ready_for_outreach AS
SELECT l.*, b.name, b.city, b.website, b.email as business_email, b.phone
FROM leads l
JOIN businesses b ON b.business_id = l.business_id
WHERE l.deleted_at IS NULL
  AND l.status IN ('new', 'qualified')
  AND (l.next_followup_at IS NULL OR l.next_followup_at <= NOW())
  AND l.outreach_count < 3
ORDER BY l.priority DESC, l.score DESC, l.created_at ASC;

-- Monitoring targets due for check
CREATE VIEW v_monitoring_due AS
SELECT mt.*, b.name as business_name, b.website
FROM monitoring_targets mt
LEFT JOIN businesses b ON b.business_id = mt.business_id
WHERE mt.deleted_at IS NULL
  AND mt.is_active
  AND NOT mt.is_paused
  AND (mt.last_checked_at IS NULL OR mt.last_checked_at + mt.check_frequency <= NOW())
ORDER BY mt.check_frequency ASC, mt.last_checked_at ASC NULLS FIRST;

-- Alerts pending delivery
CREATE VIEW v_alerts_pending AS
SELECT a.*
FROM alerts a
WHERE a.deleted_at IS NULL
  AND a.status = 'pending'
  AND a.scheduled_for <= NOW()
ORDER BY a.severity DESC, a.scheduled_for ASC;
```

---

## Vector Search Functions

```sql
-- Semantic search on businesses
CREATE OR REPLACE FUNCTION search_businesses_semantic(
    query_embedding VECTOR(384),
    match_threshold FLOAT DEFAULT 0.75,
    match_count INT DEFAULT 10,
    filter_category VARCHAR(100) DEFAULT NULL,
    filter_city VARCHAR(100) DEFAULT NULL
)
RETURNS TABLE (
    business_id UUID,
    name VARCHAR(300),
    city VARCHAR(100),
    primary_category VARCHAR(100),
    similarity FLOAT
) LANGUAGE sql STABLE AS $$
    SELECT b.business_id, b.name, b.city, b.primary_category,
           1 - (b.embedding <=> query_embedding) as similarity
    FROM businesses b
    WHERE b.deleted_at IS NULL
      AND b.embedding IS NOT NULL
      AND (filter_category IS NULL OR b.primary_category = filter_category)
      AND (filter_city IS NULL OR b.city = filter_city)
      AND (1 - (b.embedding <=> query_embedding)) >= match_threshold
    ORDER BY b.embedding <=> query_embedding
    LIMIT match_count;
$$;

-- Find similar businesses (for competitor detection)
CREATE OR REPLACE FUNCTION find_similar_businesses(
    business_id UUID,
    match_threshold FLOAT DEFAULT 0.80,
    match_count INT DEFAULT 20
)
RETURNS TABLE (
    competitor_id UUID,
    name VARCHAR(300),
    city VARCHAR(100),
    primary_category VARCHAR(100),
    similarity FLOAT
) LANGUAGE sql STABLE AS $$
    SELECT b.business_id, b.name, b.city, b.primary_category,
           1 - (b.embedding <=> source.embedding) as similarity
    FROM businesses b
    CROSS JOIN (SELECT embedding FROM businesses WHERE business_id = $1) source
    WHERE b.business_id != $1
      AND b.deleted_at IS NULL
      AND b.embedding IS NOT NULL
      AND (1 - (b.embedding <=> source.embedding)) >= match_threshold
    ORDER BY b.embedding <=> source.embedding
    LIMIT match_count;
$$;
```

---

## Indexes Summary (Performance Critical)

| Table | Index | Purpose |
|-------|-------|---------|
| businesses | `hnsw` on `embedding` | Vector similarity search |
| businesses | `normalized_name` | Fuzzy name matching |
| businesses | `city, region` | Geo queries |
| businesses | `primary_category` | Niche filtering |
| businesses | `website_normalized` | Website-based dedup |
| leads | `status, priority, score` | Outreach queue ordering |
| leads | `next_followup_at` | Due follow-ups |
| monitoring_targets | `is_active, is_paused, last_checked_at` | Scheduler query |
| monitoring_events | `target_id, detected_at` | Event timeline |
| monitoring_events | `processed_at` (partial) | Unprocessed events |
| alerts | `scheduled_for, status` (partial) | Pending delivery queue |
| opportunities | `score, status` | Opportunity ranking |
| opportunities | `niche, location` | Market analysis |

---

## Migration Strategy

| Phase | Action |
|-------|--------|
| **M2a** | Create `sources`, `businesses`, `business_source_refs` — core entity layer |
| **M2b** | Create `leads`, `competitors`, `reviews` — intelligence layer |
| **M2c** | Create `monitoring_targets`, `monitoring_events` — monitoring layer (M7) |
| **M2d** | Create `websites`, `reports` — analysis layer (M6) |
| **M2e** | Create `alerts`, `opportunities` — product layer (M8/M9) |
| **M2f** | Create history tables, views, vector functions |
| **M2g** | Load seed data: sources registry, initial niches, test businesses |

---

## M2 Checkpoint: PENDING REVIEW

**Review Required:**
1. Schema completeness — any missing entities for M3-M10?
2. Index strategy — partitioned tables for monitoring_events/alerts?
3. Vector dimensions — 384 confirmed for all-MiniLM-L6-v2?
4. Soft delete pattern — consistent across all tables?
5. Source attribution — sufficient for audit trail?
6. Confidence scoring — granular enough?

**Next:** M3 — Collection Architecture (pipeline implementation against this schema)