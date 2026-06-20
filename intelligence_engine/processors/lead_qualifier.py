"""
intelligence_engine.processors.lead_qualifier
============================================

M4 — Lead Qualification Engine.

Reads processed_leads, applies multi-factor scoring, assigns quality tiers,
and produces qualified lead rankings and gap analysis reports.

Scoring model (0-100):
  - Website presence      (25 pts)  — has website +25
  - Rating quality        (20 pts)  — 4.5+ = 20, 4.0+ = 15, 3.5+ = 10, <3.5 = 5
  - Review volume         (15 pts)  — 50+ = 15, 20+ = 12, 10+ = 8, <10 = 3
  - Contact completeness  (15 pts)  — phone + address + website = 15, 2/3 = 10, 1/3 = 5
  - Opportunity gap       (25 pts)  — inverted PGR: high gap = high opportunity

Quality tiers:
  - A (75-100): Premium leads — high value, multiple gaps, strong candidates
  - B (50-74):  Standard leads — decent gaps, worth including
  - C (25-49):  Basic leads — few gaps, lower priority
  - D (0-24):   Low quality — minimal gaps, deprioritised

Run from the host:
    docker run --rm --network host \
        -v $(pwd)/intelligence_engine:/app/intelligence_engine \
        intel-collector python3 -u -m intelligence_engine.processors.lead_qualifier
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from typing import Any

import asyncpg

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

LOG = logging.getLogger("m4.lead_qualifier")

DSN = (
    "postgresql://intelligence:***@127.0.0.1:5433/intelligence"
)

# --------------------------------------------------------------------------- #
# Scoring engine
# --------------------------------------------------------------------------- #

def score_website(lead: dict) -> int:
    """25 pts max. Website presence is the single biggest gap signal."""
    website = (lead.get("website") or "").strip()
    return 25 if website else 0


def score_rating(lead: dict) -> int:
    """20 pts max. Higher-rated businesses are more established but also
    more likely to be approached — the sweet spot is 3.5-4.5."""
    rating = lead.get("rating")
    if rating is None:
        return 0
    if rating >= 4.5:
        return 20
    if rating >= 4.0:
        return 15
    if rating >= 3.5:
        return 10
    return 5


def score_reviews(lead: dict) -> int:
    """15 pts max. Low review count = opportunity for reputation management.
    NOTE: review_count is currently 0 for all records (not in Google Maps DOM).
    When real data is available, this scoring will activate automatically."""
    reviews = lead.get("review_count") or 0
    if reviews == 0:
        # No data — neutral score (don't penalise, don't reward)
        return 8
    if reviews >= 50:
        return 15
    if reviews >= 20:
        return 12
    if reviews >= 10:
        return 10
    return 5


def score_contact(lead: dict) -> int:
    """15 pts max. Complete contact info = more usable lead."""
    has_phone = bool((lead.get("phone") or "").strip())
    has_address = bool((lead.get("address") or "").strip())
    has_website = bool((lead.get("website") or "").strip())
    count = has_phone + has_address + has_website
    if count == 3:
        return 15
    if count == 2:
        return 10
    if count == 1:
        return 5
    return 0


def score_opportunity(lead: dict) -> int:
    """25 pts max. Higher PGR = more gaps = more monetisable."""
    pgr = float(lead.get("primary_gap_rate") or 0)
    return round(pgr * 25)


def compute_quality_score(lead: dict) -> int:
    """Total quality score 0-100."""
    return (
        score_website(lead)
        + score_rating(lead)
        + score_reviews(lead)
        + score_contact(lead)
        + score_opportunity(lead)
    )


def assign_tier(score: int) -> str:
    """Map quality score to tier.
    With current data (review_count=0), effective range is ~55-85.
    A = top ~15%  (score >= 80) — best leads, multiple monetisable gaps
    B = next ~35%  (score >= 65) — solid leads, at least one gap
    C = next ~35%  (score >= 50) — usable but fewer gaps
    D = bottom     (score < 50)  — minimal opportunity"""
    if score >= 75:
        return "A"
    if score >= 60:
        return "B"
    if score >= 45:
        return "C"
    return "D"


def classify_lead(lead: dict) -> dict:
    """Enrich a lead dict with quality_score, tier, and score breakdown."""
    score = compute_quality_score(lead)
    tier = assign_tier(score)
    return {
        **lead,
        "quality_score": score,
        "tier": tier,
        "score_website": score_website(lead),
        "score_rating": score_rating(lead),
        "score_reviews": score_reviews(lead),
        "score_contact": score_contact(lead),
        "score_opportunity": score_opportunity(lead),
    }


# --------------------------------------------------------------------------- #
# Gap analysis report
# --------------------------------------------------------------------------- #

def gap_analysis(leads: list[dict]) -> dict:
    """Produce a per-niche, per-city gap summary."""
    niches: dict[str, dict] = {}
    for lead in leads:
        niche = lead.get("niche", "unknown")
        city = lead.get("city", "unknown")
        if niche not in niches:
            niches[niche] = {"total": 0, "no_website": 0, "low_rating": 0, "low_reviews": 0, "no_phone": 0, "avg_pgr": 0, "avg_quality": 0, "cities": {}}
        n = niches[niche]
        n["total"] += 1
        if not lead.get("website"):
            n["no_website"] += 1
        if (lead.get("rating") or 0) < 4.0:
            n["low_rating"] += 1
        if (lead.get("review_count") or 0) < 10:
            n["low_reviews"] += 1
        if not lead.get("phone"):
            n["no_phone"] += 1
        n["avg_pgr"] += float(lead.get("primary_gap_rate") or 0)
        n["avg_quality"] += lead.get("quality_score", 0)

        if city not in n["cities"]:
            n["cities"][city] = {"total": 0, "no_website": 0, "low_rating": 0, "avg_pgr": 0}
        c = n["cities"][city]
        c["total"] += 1
        if not lead.get("website"):
            c["no_website"] += 1
        if (lead.get("rating") or 0) < 4.0:
            c["low_rating"] += 1
        c["avg_pgr"] += float(lead.get("primary_gap_rate") or 0)

    # Averages
    for niche, n in niches.items():
        if n["total"] > 0:
            n["avg_pgr"] = round(n["avg_pgr"] / n["total"], 3)
            n["avg_quality"] = round(n["avg_quality"] / n["total"], 1)
        for city, c in n["cities"].items():
            if c["total"] > 0:
                c["avg_pgr"] = round(c["avg_pgr"] / c["total"], 3)

    return niches


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #

DDL_QUALITY_TABLE = """
CREATE TABLE IF NOT EXISTS lead_quality_scores (
    lead_id              BIGINT PRIMARY KEY REFERENCES processed_leads(id) ON DELETE CASCADE,
    quality_score        INTEGER NOT NULL DEFAULT 0,
    tier                 TEXT NOT NULL DEFAULT 'D',
    score_website        INTEGER NOT NULL DEFAULT 0,
    score_rating         INTEGER NOT NULL DEFAULT 0,
    score_reviews        INTEGER NOT NULL DEFAULT 0,
    score_contact        INTEGER NOT NULL DEFAULT 0,
    score_opportunity    INTEGER NOT NULL DEFAULT 0,
    qualified_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_lead_quality_tier
    ON lead_quality_scores (tier);
CREATE INDEX IF NOT EXISTS idx_lead_quality_score
    ON lead_quality_scores (quality_score DESC);
"""

DDL_REPORTS_TABLE = """
CREATE TABLE IF NOT EXISTS gap_analysis_reports (
    id                   BIGSERIAL PRIMARY KEY,
    report_type          TEXT NOT NULL,
    niche                TEXT,
    city                 TEXT,
    total_leads          INTEGER NOT NULL DEFAULT 0,
    no_website           INTEGER NOT NULL DEFAULT 0,
    low_rating           INTEGER NOT NULL DEFAULT 0,
    low_reviews          INTEGER NOT NULL DEFAULT 0,
    no_phone             INTEGER NOT NULL DEFAULT 0,
    avg_pgr              NUMERIC(4,3),
    avg_quality          NUMERIC(4,1),
    tier_a_count         INTEGER NOT NULL DEFAULT 0,
    tier_b_count         INTEGER NOT NULL DEFAULT 0,
    tier_c_count         INTEGER NOT NULL DEFAULT 0,
    tier_d_count         INTEGER NOT NULL DEFAULT 0,
    generated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_gap_reports_type
    ON gap_analysis_reports (report_type, niche, city);
"""


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def ensure_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(DDL_QUALITY_TABLE)
    await conn.execute(DDL_REPORTS_TABLE)
    LOG.info("M4 tables ready")


async def fetch_all_leads(conn: asyncpg.Connection) -> list[dict]:
    rows = await conn.fetch(
        "SELECT id, name, address, phone, website, rating, review_count, "
        "primary_gap_rate, opportunity_tags, niche, city FROM processed_leads ORDER BY id"
    )
    return [dict(r) for r in rows]


async def upsert_quality(conn: asyncpg.Connection, lead: dict) -> None:
    sql = """
    INSERT INTO lead_quality_scores (
        lead_id, quality_score, tier,
        score_website, score_rating, score_reviews, score_contact, score_opportunity
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (lead_id) DO UPDATE
        SET quality_score     = EXCLUDED.quality_score,
            tier              = EXCLUDED.tier,
            score_website     = EXCLUDED.score_website,
            score_rating      = EXCLUDED.score_rating,
            score_reviews     = EXCLUDED.score_reviews,
            score_contact     = EXCLUDED.score_contact,
            score_opportunity = EXCLUDED.score_opportunity,
            qualified_at      = now();
    """
    await conn.execute(
        sql,
        lead["id"],
        lead["quality_score"],
        lead["tier"],
        lead["score_website"],
        lead["score_rating"],
        lead["score_reviews"],
        lead["score_contact"],
        lead["score_opportunity"],
    )


async def save_report(
    conn: asyncpg.Connection,
    report_type: str,
    niche: str | None,
    city: str | None,
    stats: dict,
) -> None:
    sql = """
    INSERT INTO gap_analysis_reports (
        report_type, niche, city, total_leads, no_website, low_rating,
        low_reviews, no_phone, avg_pgr, avg_quality,
        tier_a_count, tier_b_count, tier_c_count, tier_d_count
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);
    """
    await conn.execute(
        sql,
        report_type,
        niche,
        city,
        stats.get("total", 0),
        stats.get("no_website", 0),
        stats.get("low_rating", 0),
        stats.get("low_reviews", 0),
        stats.get("no_phone", 0),
        stats.get("avg_pgr", 0),
        stats.get("avg_quality", 0),
        stats.get("tier_a", 0),
        stats.get("tier_b", 0),
        stats.get("tier_c", 0),
        stats.get("tier_d", 0),
    )


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #

async def run() -> dict[str, Any]:
    LOG.info("connecting to %s", DSN.split("@")[-1])
    conn = await asyncpg.connect(DSN)
    await _init_connection(conn)
    try:
        await ensure_schema(conn)

        # 1. Fetch all processed leads
        leads = await fetch_all_leads(conn)
        LOG.info("fetched %d leads", len(leads))
        if not leads:
            return {"qualified": 0, "tier_a": 0, "tier_b": 0, "tier_c": 0, "tier_d": 0}

        # 2. Classify each lead
        classified = [classify_lead(lead) for lead in leads]

        # 3. Upsert quality scores
        for lead in classified:
            await upsert_quality(conn, lead)
        LOG.info("quality scores written for %d leads", len(classified))

        # 4. Tier counts
        tier_counts = {"A": 0, "B": 0, "C": 0, "D": 0}
        for lead in classified:
            tier_counts[lead["tier"]] += 1
        LOG.info("tier distribution: %s", tier_counts)

        # 5. Gap analysis — global
        report = gap_analysis(classified)

        # Clear old reports to prevent duplicates on re-run
        await conn.execute("DELETE FROM gap_analysis_reports")

        # Save global report
        global_stats = {
            "total": len(classified),
            "no_website": sum(1 for l in classified if not l.get("website")),
            "low_rating": sum(1 for l in classified if (l.get("rating") or 0) < 4.0),
            "low_reviews": sum(1 for l in classified if (l.get("review_count") or 0) < 10 and (l.get("review_count") or 0) > 0),
            "no_phone": sum(1 for l in classified if not l.get("phone")),
            "avg_pgr": round(sum(float(l.get("primary_gap_rate") or 0) for l in classified) / max(len(classified), 1), 3),
            "avg_quality": round(sum(l["quality_score"] for l in classified) / max(len(classified), 1), 1),
            "tier_a": tier_counts["A"],
            "tier_b": tier_counts["B"],
            "tier_c": tier_counts["C"],
            "tier_d": tier_counts["D"],
        }
        await save_report(conn, "global", None, None, global_stats)

        # Save per-niche reports
        for niche, n in report.items():
            niche_stats = {
                "total": n["total"],
                "no_website": n["no_website"],
                "low_rating": n["low_rating"],
                "low_reviews": n["low_reviews"],
                "no_phone": n["no_phone"],
                "avg_pgr": n["avg_pgr"],
                "avg_quality": n["avg_quality"],
                "tier_a": sum(1 for l in classified if l.get("niche") == niche and l["tier"] == "A"),
                "tier_b": sum(1 for l in classified if l.get("niche") == niche and l["tier"] == "B"),
                "tier_c": sum(1 for l in classified if l.get("niche") == niche and l["tier"] == "C"),
                "tier_d": sum(1 for l in classified if l.get("niche") == niche and l["tier"] == "D"),
            }
            await save_report(conn, "niche", niche, None, niche_stats)

            # Per-city reports
            for city, c in n["cities"].items():
                city_leads = [l for l in classified if l.get("niche") == niche and l.get("city") == city]
                avg_q = round(sum(l["quality_score"] for l in city_leads) / max(len(city_leads), 1), 1) if city_leads else 0
                city_stats = {
                    "total": c["total"],
                    "no_website": c["no_website"],
                    "low_rating": c["low_rating"],
                    "low_reviews": 0,
                    "no_phone": 0,
                    "avg_pgr": c["avg_pgr"],
                    "avg_quality": avg_q,
                    "tier_a": sum(1 for l in classified if l.get("niche") == niche and l.get("city") == city and l["tier"] == "A"),
                    "tier_b": sum(1 for l in classified if l.get("niche") == niche and l.get("city") == city and l["tier"] == "B"),
                    "tier_c": sum(1 for l in classified if l.get("niche") == niche and l.get("city") == city and l["tier"] == "C"),
                    "tier_d": sum(1 for l in classified if l.get("niche") == niche and l.get("city") == city and l["tier"] == "D"),
                }
                await save_report(conn, "niche_city", niche, city, city_stats)

        LOG.info("gap analysis reports saved")

        # 6. Print summary
        print("\n" + "=" * 60)
        print("M4 — Lead Qualification Engine — Results")
        print("=" * 60)
        print(f"\nTotal leads qualified: {len(classified)}")
        print(f"Tier distribution: A={tier_counts['A']}  B={tier_counts['B']}  C={tier_counts['C']}  D={tier_counts['D']}")
        print(f"\nGlobal gap analysis:")
        print(f"  No website:  {global_stats['no_website']}/{global_stats['total']} ({round(global_stats['no_website']/max(global_stats['total'],1)*100)}%)")
        print(f"  Low rating:  {global_stats['low_rating']}/{global_stats['total']} ({round(global_stats['low_rating']/max(global_stats['total'],1)*100)}%)")
        print(f"  Low reviews: {global_stats['low_reviews']}/{global_stats['total']} ({round(global_stats['low_reviews']/max(global_stats['total'],1)*100)}%)")
        print(f"  No phone:    {global_stats['no_phone']}/{global_stats['total']} ({round(global_stats['no_phone']/max(global_stats['total'],1)*100)}%)")
        print(f"  Avg PGR:     {global_stats['avg_pgr']}")
        print(f"  Avg quality: {global_stats['avg_quality']}")

        print(f"\nPer-niche breakdown:")
        for niche, n in sorted(report.items()):
            print(f"  {niche}: {n['total']} leads, avg PGR={n['avg_pgr']}, avg quality={n['avg_quality']}")
            for city, c in sorted(n["cities"].items()):
                print(f"    {city}: {c['total']} leads, {c['no_website']} no-website, {c['low_rating']} low-rating")

        # Top 10 leads
        top_leads = sorted(classified, key=lambda l: l["quality_score"], reverse=True)[:10]
        print(f"\nTop 10 leads by quality score:")
        for i, lead in enumerate(top_leads, 1):
            print(f"  {i}. [{lead['tier']}] {lead['name']} ({lead['niche']}/{lead['city']}) — score={lead['quality_score']}, PGR={lead['primary_gap_rate']}, tags={lead.get('opportunity_tags', [])}")

        print("=" * 60)

        return {
            "qualified": len(classified),
            "tier_a": tier_counts["A"],
            "tier_b": tier_counts["B"],
            "tier_c": tier_counts["C"],
            "tier_d": tier_counts["D"],
        }
    finally:
        await conn.close()


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )
    asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
