"""
intelligence_engine.processors.product_packager
============================================

M5 — Product Packaging Engine.

Reads qualified leads and packages them into sellable products:
- Niche-specific lead packs (A/B tier leads per niche)
- City-specific lead packs (A/B tier leads per niche+city)
- CSV export for each pack
- Product descriptions and pricing suggestions
- Sample datasets (top 5 leads per pack)

Products created:
1. Dentist Lead Pack (AU) — all dentist leads, A/B tier
2. Electrician Lead Pack (AU) — all electrician leads, A/B tier
3. Plumber Lead Pack (AU) — all plumber leads, A/B tier
4. City-specific packs: Dentist Melbourne, Plumber Perth, etc.
5. Opportunity-specific packs: No Website leads, Low Rating leads

Output dir default: /tmp/intelligence_products/ (override with --output-dir).
On the host, mount a host path to /tmp/intelligence_products to persist files:

docker run --rm --network host \\
    -v $(pwd)/intelligence_engine:/workspace/intelligence_engine \\
    -v $(pwd)/products:/tmp/intelligence_products \\
    intel-collector python3 -u -m intelligence_engine.processors.product_packager
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

LOG = logging.getLogger("m5.product_packager")

DSN = (
    "postgresql://intelligence:***@127.0.0.1:5433/intelligence"
)

OUTPUT_DIR = Path("/tmp/intelligence_products")
OUTPUT_DIR.mkdir(exist_ok=True)

CSV_FIELDS = [
    "name", "address", "phone", "website", "rating",
    "niche", "city", "quality_score", "tier",
    "primary_gap_rate", "opportunity_tags",
]

NICHE_TIERS = ["A", "B"]  # Only premium + standard leads in paid packs

# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #

async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def fetch_qualified_leads(
    conn: asyncpg.Connection,
    niche: str | None = None,
    city: str | None = None,
    tiers: list[str] | None = None,
) -> list[dict]:
    """Fetch leads with quality scores, filtered by niche/city/tier."""
    sql = """
        SELECT p.name, p.address, p.phone, p.website, p.rating,
               p.niche, p.city, p.primary_gap_rate, p.opportunity_tags,
               q.quality_score, q.tier
        FROM processed_leads p
        JOIN lead_quality_scores q ON q.lead_id = p.id
        WHERE 1=1
    """
    params: list[Any] = []
    idx = 1

    if niche:
        sql += f" AND p.niche = ${idx}"
        params.append(niche)
        idx += 1
    if city:
        sql += f" AND p.city = ${idx}"
        params.append(city)
        idx += 1
    if tiers:
        placeholders = ", ".join(f"${i}" for i in range(idx, idx + len(tiers)))
        sql += f" AND q.tier IN ({placeholders})"
        params.extend(tiers)
        idx += len(tiers)

    sql += " ORDER BY q.quality_score DESC"

    rows = await conn.fetch(sql, *params)
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------- #
# CSV export
# --------------------------------------------------------------------------- #

def write_csv(filepath: Path, leads: list[dict]) -> int:
    """Write leads to CSV. Returns row count."""
    if not leads:
        return 0
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            row = {**lead}
            # Convert list to string for CSV
            row["opportunity_tags"] = "; ".join(row.get("opportunity_tags") or [])
            writer.writerow(row)
    return len(leads)


# --------------------------------------------------------------------------- #
# Product manifest
# --------------------------------------------------------------------------- #

def now_str() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def save_manifest(products: list[dict], output_dir: Path, run_id: str) -> Path:
    """Save product manifest as JSON."""
    manifest_path = output_dir / f"manifest_{run_id}.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump({"run_id": run_id, "generated_at": datetime.utcnow().isoformat(), "products": products}, f, indent=2, ensure_ascii=False)
    return manifest_path


# --------------------------------------------------------------------------- #
# Pricing model (informational — for product descriptions)
# --------------------------------------------------------------------------- #

def suggested_price(tier: str, lead_count: int) -> str:
    """Return a suggested price range based on tier and count."""
    if tier == "A":
        if lead_count >= 50:
            return "$49-99"
        if lead_count >= 20:
            return "$29-49"
        return "$19-29"
    if tier == "B":
        if lead_count >= 50:
            return "$29-49"
        if lead_count >= 20:
            return "$19-29"
        return "$9-19"
    return "$9-14"


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #

async def run(dry_run: bool = False) -> dict[str, Any]:
    LOG.info("connecting to %s", DSN.split("@")[-1])
    conn = await asyncpg.connect(DSN)
    await _init_connection(conn)
    try:
        run_id = now_str()
        products: list[dict[str, Any]] = []
        total_leads_exported = 0

        # 1. Per-niche packs (A/B tier)
        niches = ["dentist", "electrician", "plumber"]
        for niche in niches:
            leads = await fetch_qualified_leads(conn, niche=niche, tiers=NICHE_TIERS)
            if not leads:
                LOG.info("niche pack %s: no leads, skipping", niche)
                continue

            # Split by tier for separate packs
            for tier in NICHE_TIERS:
                tier_leads = [l for l in leads if l["tier"] == tier]
                if not tier_leads:
                    continue

                filename = f"{niche}_{tier.lower()}_tier_{run_id}.csv"
                filepath = OUTPUT_DIR / filename

                if not dry_run:
                    count = write_csv(filepath, tier_leads)
                else:
                    count = len(tier_leads)

                product = {
                    "product_type": "niche_tier",
                    "niche": niche,
                    "tier": tier,
                    "lead_count": count,
                    "filename": filename,
                    "filepath": str(filepath),
                    "suggested_price": suggested_price(tier, count),
                    "description": f"{niche.title()} Lead Pack — {count} {tier}-tier leads across Australia. Includes name, address, phone, website, rating, opportunity tags.",
                    "filters": {"niche": niche, "tiers": [tier]},
                }
                products.append(product)
                total_leads_exported += count
                LOG.info("%s %s-tier: %d leads -> %s", niche, tier, count, filename)

        # 2. Niche + city packs (A/B tier, only for niches+city with 5+ leads)
        for niche in niches:
            for city in ["Melbourne", "Sydney", "Brisbane", "Perth", "Adelaide"]:
                leads = await fetch_qualified_leads(conn, niche=niche, city=city, tiers=NICHE_TIERS)
                if len(leads) < 5:
                    continue

                for tier in NICHE_TIERS:
                    tier_leads = [l for l in leads if l["tier"] == tier]
                    if not tier_leads:
                        continue

                    city_slug = city.lower()
                    filename = f"{niche}_{city_slug}_{tier.lower()}_{run_id}.csv"
                    filepath = OUTPUT_DIR / filename

                    if not dry_run:
                        count = write_csv(filepath, tier_leads)
                    else:
                        count = len(tier_leads)

                    product = {
                        "product_type": "niche_city_tier",
                        "niche": niche,
                        "city": city,
                        "tier": tier,
                        "lead_count": count,
                        "filename": filename,
                        "filepath": str(filepath),
                        "suggested_price": suggested_price(tier, count),
                        "description": f"{niche.title()} Lead Pack — {city}: {count} {tier}-tier leads. City-specific targeting for local agencies.",
                        "filters": {"niche": niche, "city": city, "tiers": [tier]},
                    }
                    products.append(product)
                    total_leads_exported += count
                    LOG.info("%s %s %s-tier: %d leads -> %s", niche, city, tier, count, filename)

        # 3. Opportunity-specific packs
        # No-website leads (across all niches)
        sql_no_web = """
            SELECT p.name, p.address, p.phone, p.website, p.rating,
                   p.niche, p.city, p.primary_gap_rate, p.opportunity_tags,
                   q.quality_score, q.tier
            FROM processed_leads p
            JOIN lead_quality_scores q ON q.lead_id = p.id
            WHERE p.website IS NULL OR p.website = ''
            ORDER BY q.quality_score DESC
        """
        no_web_leads = [dict(r) for r in await conn.fetch(sql_no_web)]
        if no_web_leads:
            filename = f"opportunity_no_website_{run_id}.csv"
            filepath = OUTPUT_DIR / filename
            count = write_csv(filepath, no_web_leads) if not dry_run else len(no_web_leads)
            products.append({
                "product_type": "opportunity",
                "opportunity": "no_website",
                "lead_count": count,
                "filename": filename,
                "filepath": str(filepath),
                "suggested_price": "$29-49",
                "description": f"No-Website Lead Pack — {count} businesses without a website. Highest-value opportunity for web dev and digital agencies.",
                "filters": {"opportunity": "no_website"},
            })
            total_leads_exported += count
            LOG.info("no-website pack: %d leads -> %s", count, filename)

        # Low-rating leads
        sql_low_rated = """
            SELECT p.name, p.address, p.phone, p.website, p.rating,
                   p.niche, p.city, p.primary_gap_rate, p.opportunity_tags,
                   q.quality_score, q.tier
            FROM processed_leads p
            JOIN lead_quality_scores q ON q.lead_id = p.id
            WHERE p.rating < 4.0 AND p.rating > 0
            ORDER BY q.quality_score DESC
        """
        low_rated_leads = [dict(r) for r in await conn.fetch(sql_low_rated)]
        if low_rated_leads:
            filename = f"opportunity_low_rating_{run_id}.csv"
            filepath = OUTPUT_DIR / filename
            count = write_csv(filepath, low_rated_leads) if not dry_run else len(low_rated_leads)
            products.append({
                "product_type": "opportunity",
                "opportunity": "low_rating",
                "lead_count": count,
                "filename": filename,
                "filepath": str(filepath),
                "suggested_price": "$19-39",
                "description": f"Low-Rating Lead Pack — {count} businesses with rating below 4.0. Opportunity for reputation management and SEO agencies.",
                "filters": {"opportunity": "low_rating"},
            })
            total_leads_exported += count
            LOG.info("low-rating pack: %d leads -> %s", count, filename)

        # 4. Save manifest
        manifest_path = save_manifest(products, OUTPUT_DIR, run_id)

        # 5. "All-in-one" master pack
        all_leads = await fetch_qualified_leads(conn, tiers=NICHE_TIERS)
        if all_leads:
            filename = f"master_all_leads_{run_id}.csv"
            filepath = OUTPUT_DIR / filename
            count = write_csv(filepath, all_leads) if not dry_run else len(all_leads)
            products.append({
                "product_type": "master",
                "lead_count": count,
                "filename": filename,
                "filepath": str(filepath),
                "suggested_price": "$99-199",
                "description": f"Master Lead Pack — {count} qualified leads across all niches (dentist, electrician, plumber) and all Australian capitals. A/B tier only.",
                "filters": {"tiers": NICHE_TIERS},
            })
            total_leads_exported += count
            LOG.info("master pack: %d leads -> %s", count, filename)

        # 6. Print summary
        print("\n" + "=" * 60)
        print("M5 — Product Packaging Engine — Results")
        print("=" * 60)
        print(f"\nProducts created: {len(products)}")
        print(f"Total lead rows exported: {total_leads_exported}")
        print(f"Output directory: {OUTPUT_DIR}")
        print(f"Manifest: {manifest_path}")

        print(f"\n{'Type':<20} {'Filter':<25} {'Leads':>6} {'Price':<12} {'File'}")
        print("-" * 90)
        for p in products:
            ptype = p["product_type"]
            if ptype == "niche_tier":
                filt = f"{p['niche']} / {p['tier']}-tier"
            elif ptype == "niche_city_tier":
                filt = f"{p['niche']} / {p['city']} / {p['tier']}"
            elif ptype == "opportunity":
                filt = f"opp: {p['opportunity']}"
            elif ptype == "master":
                filt = "all leads"
            else:
                filt = str(p.get("filters", ""))
            print(f"{ptype:<20} {filt:<25} {p['lead_count']:>6} {p['suggested_price']:<12} {p['filename']}")

        print("=" * 60)

        return {
            "products": len(products),
            "total_leads_exported": total_leads_exported,
            "output_dir": str(OUTPUT_DIR),
            "manifest": str(manifest_path),
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Count only, don't write files")
    parser.add_argument("--output-dir", default=None, help="Output directory for CSV files")
    args = parser.parse_args()
    if args.output_dir:
        global OUTPUT_DIR
        OUTPUT_DIR = Path(args.output_dir)
        OUTPUT_DIR.mkdir(exist_ok=True)
    asyncio.run(run(dry_run=args.dry_run))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
