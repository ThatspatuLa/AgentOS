-- Intelligence Engine M2 Schema
-- PostgreSQL 16+ with pgvector
-- Run: psql -h localhost -U intelligence -d intelligence -f schema_m2.sql

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- ============================================================================
-- 1. SOURCES - Data Source Registry
-- ============================================================================
CREATE TABLE sources (
    source_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               VARCHAR(100) NOT NULL UNIQUE,
    type               VARCHAR(30) NOT NULL,
    base_url           VARCHAR(500),
    api_key_name       VARCHAR(100),
    rate_limit_rpm     INT DEFAULT 60,
    rate_limit_rph     INT DEFAULT 1000,
    config             JSONB DEFAULT '{}',
    is_active          BOOLEAN DEFAULT TRUE,
    priority           INT DEFAULT 10,
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sources_type_active ON sources(type, is_active);

-- ============================================================================
-- 2. BUSINESSES - Core Entity
-- ============================================================================
CREATE TABLE businesses (
    business_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id          UUID NOT NULL REFERENCES sources(source_id),
    source_business_id VARCHAR(200) NOT NULL,
    name               VARCHAR(300) NOT NULL,
    normalized_name    VARCHAR(300) GENERATED ALWAYS AS (
        lower(regexp_replace(name, '[^a-z0-9]+', '', 'g'))
    ) STORED,
    address_line1      VARCHAR(300),
    address_line2      VARCHAR(300),
    city               VARCHAR(100),
    region             VARCHAR(100),
    postal_code        VARCHAR(20),
    country            VARCHAR(2) DEFAULT 'AU',
    lat                DECIMAL(10, 8),
    lng                DECIMAL(11, 8),
    phone              VARCHAR(50),
    phone_normalized   VARCHAR(20) GENERATED ALWAYS AS (
        regexp_replace(phone, '[^0-9+]', '', 'g')
    ) STORED,
    email              VARCHAR(255),
    website            VARCHAR(500),
    website_normalized VARCHAR(500) GENERATED ALWAYS AS (
        lower(regexp_replace(coalesce(website, ''), '^https?://(www\.)?', ''))
    ) STORED,
    primary_category   VARCHAR(100),
    categories         TEXT[],
    sic_code           VARCHAR(10),
    naics_code         VARCHAR(10),
    rating             DECIMAL(3, 2),
    review_count       INT DEFAULT 0,
    price_level        INT,
    is_permanently_closed BOOLEAN DEFAULT FALSE,
    confidence         DECIMAL(3, 2) DEFAULT 0.50,
    first_seen_at      TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at       TIMESTAMPTZ DEFAULT NOW(),
    last_enriched_at   TIMESTAMPTZ,
    deleted_at         TIMESTAMPTZ,
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

-- ============================================================================
-- 3. BUSINESS_SOURCE_REFS - Multi-Source Linkage
-- ============================================================================
CREATE TABLE business_source_refs (
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    source_id          UUID NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    source_business_id VARCHAR(200) NOT NULL,
    match_confidence   DECIMAL(3, 2) NOT NULL,
    match_method       VARCHAR(30) NOT NULL,
    matched_at         TIMESTAMPTZ DEFAULT NOW(),
    matched_by         VARCHAR(50) DEFAULT 'system',
    is_primary         BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (business_id, source_id)
);

CREATE INDEX idx_bsr_source ON business_source_refs(source_id, source_business_id);
CREATE INDEX idx_bsr_confidence ON business_source_refs(match_confidence DESC);

-- ============================================================================
-- 4. LEADS - Qualified Prospects
-- ============================================================================
CREATE TABLE leads (
    lead_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    status             VARCHAR(30) NOT NULL DEFAULT 'new',
    priority           VARCHAR(10) NOT NULL DEFAULT 'medium',
    score              DECIMAL(5, 2) DEFAULT 0.00,
    score_breakdown    JSONB DEFAULT '{}',
    gaps               JSONB DEFAULT '{}',
    gap_score          DECIMAL(4, 2) DEFAULT 0.00,
    contact_email      VARCHAR(255),
    contact_phone      VARCHAR(50),
    contact_name       VARCHAR(200),
    contact_role       VARCHAR(100),
    socials            JSONB DEFAULT '{}',
    website_analysis   JSONB DEFAULT '{}',
    outreach_count     INT DEFAULT 0,
    last_outreach_at   TIMESTAMPTZ,
    last_response_at   TIMESTAMPTZ,
    next_followup_at   TIMESTAMPTZ,
    assigned_to        VARCHAR(100),
    source_id          UUID REFERENCES sources(source_id),
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

-- ============================================================================
-- 5. COMPETITORS - Competitor Tracking
-- ============================================================================
CREATE TABLE competitors (
    competitor_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    competitor_business_id UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    relationship_type  VARCHAR(30) NOT NULL,
    similarity_score   DECIMAL(3, 2) DEFAULT 0.00,
    detected_via       VARCHAR(30) NOT NULL,
    notes              TEXT,
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

-- ============================================================================
-- 6. REVIEWS - Review Aggregation
-- ============================================================================
CREATE TABLE reviews (
    review_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    source_id          UUID NOT NULL REFERENCES sources(source_id),
    source_review_id   VARCHAR(200) NOT NULL,
    author_name        VARCHAR(200),
    author_profile_url VARCHAR(500),
    rating             DECIMAL(3, 2) NOT NULL,
    text               TEXT,
    language           VARCHAR(10) DEFAULT 'en',
    published_at       TIMESTAMPTZ,
    retrieved_at       TIMESTAMPTZ DEFAULT NOW(),
    sentiment_score    DECIMAL(3, 2),
    sentiment_label    VARCHAR(20),
    key_topics         TEXT[],
    confidence         DECIMAL(3, 2) DEFAULT 1.00,
    deleted_at         TIMESTAMPTZ,
    UNIQUE (source_id, source_review_id)
);

CREATE INDEX idx_reviews_business ON reviews(business_id, published_at DESC);
CREATE INDEX idx_reviews_rating ON reviews(business_id, rating);
CREATE INDEX idx_reviews_sentiment ON reviews(sentiment_label);

-- ============================================================================
-- 7. MONITORING_TARGETS - What We Watch (M7)
-- ============================================================================
CREATE TABLE monitoring_targets (
    target_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID REFERENCES businesses(business_id) ON DELETE SET NULL,
    competitor_id      UUID REFERENCES competitors(competitor_id) ON DELETE SET NULL,
    target_type        VARCHAR(30) NOT NULL,
    source_id          UUID REFERENCES sources(source_id),
    source_target_id   VARCHAR(200),
    check_frequency    INTERVAL NOT NULL DEFAULT '1 hour',
    check_method       VARCHAR(30) NOT NULL,
    check_config       JSONB DEFAULT '{}',
    last_checked_at    TIMESTAMPTZ,
    last_changed_at    TIMESTAMPTZ,
    last_content_hash  VARCHAR(64),
    last_status_code   INT,
    consecutive_failures INT DEFAULT 0,
    is_active          BOOLEAN DEFAULT TRUE,
    is_paused          BOOLEAN DEFAULT FALSE,
    pause_reason       TEXT,
    alert_on_change    BOOLEAN DEFAULT TRUE,
    alert_threshold    JSONB DEFAULT '{}',
    notification_channels TEXT[] DEFAULT ARRAY['email'],
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

-- ============================================================================
-- 8. MONITORING_EVENTS - Change Events (M7)
-- ============================================================================
CREATE TABLE monitoring_events (
    event_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target_id          UUID NOT NULL REFERENCES monitoring_targets(target_id) ON DELETE CASCADE,
    event_type         VARCHAR(30) NOT NULL,
    severity           VARCHAR(10) NOT NULL DEFAULT 'info',
    old_value          JSONB,
    new_value          JSONB,
    diff_summary       TEXT,
    change_score       DECIMAL(3, 2),
    old_content_hash   VARCHAR(64),
    new_content_hash   VARCHAR(64),
    extracted_data     JSONB,
    detected_at        TIMESTAMPTZ DEFAULT NOW(),
    processed_at       TIMESTAMPTZ,
    acknowledged_at    TIMESTAMPTZ,
    acknowledged_by    VARCHAR(100),
    deleted_at         TIMESTAMPTZ
);

CREATE INDEX idx_me_target ON monitoring_events(target_id, detected_at DESC);
CREATE INDEX idx_me_type_severity ON monitoring_events(event_type, severity);
CREATE INDEX idx_me_unprocessed ON monitoring_events(processed_at) WHERE processed_at IS NULL;

-- ============================================================================
-- 9. WEBSITES - Website Analysis (M6)
-- ============================================================================
CREATE TABLE websites (
    website_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    url                VARCHAR(500) NOT NULL,
    url_normalized     VARCHAR(500) GENERATED ALWAYS AS (
        lower(regexp_replace(url, '^https?://(www\.)?', ''))
    ) STORED,
    status_code        INT,
    final_url          VARCHAR(500),
    response_time_ms   INT,
    ssl_valid          BOOLEAN,
    ssl_expiry         TIMESTAMPTZ,
    title              VARCHAR(500),
    meta_description   TEXT,
    h1_tags            TEXT[],
    word_count         INT,
    has_contact_form   BOOLEAN DEFAULT FALSE,
    has_booking        BOOLEAN DEFAULT FALSE,
    has_analytics      BOOLEAN DEFAULT FALSE,
    has_chat           BOOLEAN DEFAULT FALSE,
    cms                VARCHAR(50),
    cms_version        VARCHAR(30),
    framework          VARCHAR(50),
    hosting            VARCHAR(100),
    analytics          TEXT[],
    cdn                VARCHAR(50),
    technologies       TEXT[],
    lighthouse_score   DECIMAL(4, 1),
    psi_mobile_score   DECIMAL(4, 1),
    psi_desktop_score  DECIMAL(4, 1),
    cli_score          DECIMAL(4, 1),
    core_web_vitals    JSONB DEFAULT '{}',
    is_mobile_friendly BOOLEAN,
    viewport_configured BOOLEAN,
    tap_targets_ok     BOOLEAN,
    has_ssl            BOOLEAN,
    has_sitemap        BOOLEAN,
    has_robots_txt     BOOLEAN,
    meta_robots        VARCHAR(50),
    canonical_url      VARCHAR(500),
    structured_data    JSONB DEFAULT '{}',
    a11y_score         DECIMAL(4, 1),
    a11y_violations    INT DEFAULT 0,
    analyzed_at        TIMESTAMPTZ DEFAULT NOW(),
    analyzer_version   VARCHAR(20) DEFAULT '1.0',
    deleted_at         TIMESTAMPTZ,
    UNIQUE (business_id, url_normalized)
);

CREATE INDEX idx_websites_business ON websites(business_id);
CREATE INDEX idx_websites_url_norm ON websites(url_normalized);
CREATE INDEX idx_websites_cms ON websites(cms);
CREATE INDEX idx_websites_analyzed ON websites(analyzed_at DESC);

-- ============================================================================
-- 10. REPORTS - Generated Reports (M6)
-- ============================================================================
CREATE TABLE reports (
    report_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID REFERENCES businesses(business_id) ON DELETE SET NULL,
    lead_id            UUID REFERENCES leads(lead_id) ON DELETE SET NULL,
    competitor_id      UUID REFERENCES competitors(competitor_id) ON DELETE SET NULL,
    report_type        VARCHAR(30) NOT NULL,
    title              VARCHAR(300) NOT NULL,
    summary            TEXT,
    sections           JSONB NOT NULL,
    markdown_content   TEXT,
    html_content       TEXT,
    overall_score      DECIMAL(4, 1),
    category_scores    JSONB DEFAULT '{}',
    generated_by       VARCHAR(50) NOT NULL,
    generation_model   VARCHAR(100),
    generation_prompt_hash VARCHAR(64),
    generation_time_ms INT,
    status             VARCHAR(20) DEFAULT 'draft',
    delivery_channels  TEXT[] DEFAULT ARRAY['dashboard'],
    delivered_at       TIMESTAMPTZ,
    delivered_to       VARCHAR(200),
    version            INT DEFAULT 1,
    parent_report_id   UUID REFERENCES reports(report_id),
    change_log         JSONB DEFAULT '[]',
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW(),
    deleted_at         TIMESTAMPTZ,
    CHECK (business_id IS NOT NULL OR lead_id IS NOT NULL OR competitor_id IS NOT NULL)
);

CREATE INDEX idx_reports_business ON reports(business_id, created_at DESC);
CREATE INDEX idx_reports_lead ON reports(lead_id);
CREATE INDEX idx_reports_type_status ON reports(report_type, status);
CREATE INDEX idx_reports_delivered ON reports(delivered_at) WHERE delivered_at IS NOT NULL;

-- ============================================================================
-- 11. ALERTS - Alert Engine Output (M8)
-- ============================================================================
CREATE TABLE alerts (
    alert_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID REFERENCES businesses(business_id) ON DELETE SET NULL,
    lead_id            UUID REFERENCES leads(lead_id) ON DELETE SET NULL,
    monitoring_event_id UUID REFERENCES monitoring_events(event_id) ON DELETE SET NULL,
    report_id          UUID REFERENCES reports(report_id) ON DELETE SET NULL,
    alert_type         VARCHAR(30) NOT NULL,
    severity           VARCHAR(10) NOT NULL DEFAULT 'info',
    title              VARCHAR(300) NOT NULL,
    summary            TEXT,
    body_md            TEXT,
    body_html          TEXT,
    data_refs          JSONB DEFAULT '{}',
    channels           TEXT[] NOT NULL DEFAULT ARRAY['dashboard'],
    email_recipients   TEXT[],
    email_sent_at      TIMESTAMPTZ,
    email_status       VARCHAR(20),
    dashboard_pushed_at TIMESTAMPTZ,
    webhook_sent_at    TIMESTAMPTZ,
    scheduled_for      TIMESTAMPTZ NOT NULL,
    generated_at       TIMESTAMPTZ DEFAULT NOW(),
    generated_by       VARCHAR(50) DEFAULT 'alert_engine',
    dedupe_key         VARCHAR(200),
    is_dedupe          BOOLEAN DEFAULT FALSE,
    original_alert_id  UUID REFERENCES alerts(alert_id),
    status             VARCHAR(20) DEFAULT 'pending',
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

-- ============================================================================
-- 12. OPPORTUNITIES - Scored Opportunities (M5/M9)
-- ============================================================================
CREATE TABLE opportunities (
    opportunity_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id        UUID NOT NULL REFERENCES businesses(business_id) ON DELETE CASCADE,
    lead_id            UUID REFERENCES leads(lead_id) ON DELETE SET NULL,
    opportunity_type   VARCHAR(30) NOT NULL,
    title              VARCHAR(300) NOT NULL,
    description        TEXT,
    score              DECIMAL(5, 2) NOT NULL,
    score_components   JSONB NOT NULL,
    revenue_estimate   DECIMAL(10, 2),
    revenue_confidence DECIMAL(3, 2) DEFAULT 0.50,
    effort_estimate    VARCHAR(20),
    effort_hours       DECIMAL(6, 1),
    niche              VARCHAR(100),
    location           VARCHAR(200),
    competitors_count  INT DEFAULT 0,
    market_saturation  DECIMAL(3, 2),
    product_matches    JSONB DEFAULT '[]',
    status             VARCHAR(20) DEFAULT 'identified',
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

-- ============================================================================
-- HISTORY TABLES (Audit Trail)
-- ============================================================================
CREATE TABLE businesses_history (
    history_id         BIGSERIAL PRIMARY KEY,
    business_id        UUID NOT NULL,
    changed_at         TIMESTAMPTZ DEFAULT NOW(),
    changed_by         VARCHAR(100),
    operation          VARCHAR(10) NOT NULL,
    old_values         JSONB,
    new_values         JSONB,
    changed_fields     TEXT[],
    change_reason      VARCHAR(200)
);
CREATE INDEX idx_bh_business ON businesses_history(business_id, changed_at DESC);

CREATE TABLE leads_history (
    history_id         BIGSERIAL PRIMARY KEY,
    lead_id            UUID NOT NULL,
    changed_at         TIMESTAMPTZ DEFAULT NOW(),
    changed_by         VARCHAR(100),
    operation          VARCHAR(10) NOT NULL,
    old_values         JSONB,
    new_values         JSONB,
    changed_fields     TEXT[],
    change_reason      VARCHAR(200)
);
CREATE INDEX idx_lh_lead ON leads_history(lead_id, changed_at DESC);

-- ============================================================================
-- VIEWS
-- ============================================================================
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

CREATE VIEW v_leads_ready_for_outreach AS
SELECT l.*, b.name, b.city, b.website, b.email as business_email, b.phone
FROM leads l
JOIN businesses b ON b.business_id = l.business_id
WHERE l.deleted_at IS NULL
  AND l.status IN ('new', 'qualified')
  AND (l.next_followup_at IS NULL OR l.next_followup_at <= NOW())
  AND l.outreach_count < 3
ORDER BY l.priority DESC, l.score DESC, l.created_at ASC;

CREATE VIEW v_monitoring_due AS
SELECT mt.*, b.name as business_name, b.website
FROM monitoring_targets mt
LEFT JOIN businesses b ON b.business_id = mt.business_id
WHERE mt.deleted_at IS NULL
  AND mt.is_active
  AND NOT mt.is_paused
  AND (mt.last_checked_at IS NULL OR mt.last_checked_at + mt.check_frequency <= NOW())
ORDER BY mt.check_frequency ASC, mt.last_checked_at ASC NULLS FIRST;

-- ============================================================================
-- VECTOR SEARCH FUNCTIONS
-- ============================================================================
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

-- ============================================================================
-- SEED DATA: Sources
-- ============================================================================
INSERT INTO sources (name, type, base_url, rate_limit_rpm, rate_limit_rph, config, is_active, priority) VALUES
('google_maps_au', 'scraper', 'https://www.google.com/maps', 30, 500, '{"country": "AU", "cities": ["Melbourne", "Sydney", "Brisbane", "Perth", "Adelaide"], "niches": ["dentist", "plumber", "electrician"]}', TRUE, 1),
('companies_house_au', 'api', 'https://api.asic.gov.au', 60, 1000, '{}', FALSE, 10),
('abr_lookup', 'api', 'https://abr.business.gov.au', 60, 1000, '{}', FALSE, 10)
ON CONFLICT (name) DO NOTHING;

-- ============================================================================
-- RAW STORAGE TABLE
-- ============================================================================
CREATE TABLE raw_collection_events (
    id                 BIGSERIAL PRIMARY KEY,
    source_id          UUID NOT NULL REFERENCES sources(source_id),
    source_record_id   VARCHAR(200) NOT NULL,
    raw_data           JSONB NOT NULL,
    collected_at       TIMESTAMPTZ NOT NULL,
    collector_version  VARCHAR(20) NOT NULL,
    metadata           JSONB DEFAULT '{}',
    status             VARCHAR(20) NOT NULL DEFAULT 'pending',
    error              TEXT,
    processed_at       TIMESTAMPTZ,
    failed_at          TIMESTAMPTZ,
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source_id, source_record_id, collected_at)
);

CREATE INDEX idx_rce_source_status ON raw_collection_events(source_id, status, collected_at);
CREATE INDEX idx_rce_pending ON raw_collection_events(source_id, collected_at) WHERE status = 'pending';

-- ============================================================================
-- DONE
-- ============================================================================
SELECT 'M2 Schema applied successfully' as status;