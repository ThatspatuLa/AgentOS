"""
intelligence_engine.processors.revenue_validator
============================================

M6 — Revenue Validation Engine (information-gathering mode).

NO LIVE SENDS. All output is preparation for when Six approves outreach.

Outputs:
1. Target agency database — who buys lead lists, per niche
2. Outreach list — top-priority targets
3. Email templates — per-agency-type outreach copy
4. Pricing matrix — product x suggested price x target buyer
5. Sales playbook — full outreach workflow

Run:
    docker run --rm --network host \
        -v $(pwd)/intelligence_engine:/workspace/intelligence_engine \
        -v $(pwd)/products:/tmp/intelligence_products \
        intel-collector python3 -u -m intelligence_engine.processors.revenue_validator
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

LOG = logging.getLogger("m6.revenue_validator")

DSN = "postgresql://intelligence:***@127.0.0.1:5433/intelligence"

# --------------------------------------------------------------------------- #
# Target agency database — who buys what
# --------------------------------------------------------------------------- #

# Agencies that actively buy or use business lead lists
TARGET_AGENCIES: list[dict[str, Any]] = [
    # Digital agencies — buy no-website leads
    {
        "agency_type": "digital_agency",
        "agency_name": "Generic Digital Agency (AU)",
        "buys": ["no_website", "low_rating", "low_reviews"],
        "niches": ["dentist", "electrician", "plumber"],
        "contact_channel": "email",
        "priority": 1,
        "notes": "Digital agencies need leads of businesses without websites or with poor online presence. They sell web design, SEO, and digital marketing services. High volume buyers.",
    },
    # SEO agencies — buy low-rating, low-review leads
    {
        "agency_type": "seo_agency",
        "agency_name": "Generic SEO Agency (AU)",
        "buys": ["low_rating", "low_reviews"],
        "niches": ["dentist", "electrician", "plumber"],
        "contact_channel": "email",
        "priority": 1,
        "notes": "SEO agencies target businesses with poor Google ratings and low review counts. They pitch reputation management and local SEO services.",
    },
    # Web dev agencies — buy no-website leads
    {
        "agency_type": "web_dev",
        "agency_name": "Generic Web Dev Agency (AU)",
        "buys": ["no_website"],
        "niches": ["dentist", "electrician", "plumber"],
        "contact_channel": "email",
        "priority": 2,
        "notes": "Web development agencies need businesses that don't have a website. They pitch website builds. Smaller volume but higher conversion.",
    },
    # Recruitment agencies — buy all leads for employer branding
    {
        "agency_type": "recruitment",
        "agency_name": "Generic Recruitment Agency (AU)",
        "buys": ["all_leads"],
        "niches": ["dentist", "electrician", "plumber"],
        "contact_channel": "email",
        "priority": 3,
        "notes": "Recruitment agencies use lead lists for employer branding and candidate sourcing. They contact businesses to offer recruitment services.",
    },
    # B2B data brokers — buy master lists
    {
        "agency_type": "data_broker",
        "agency_name": "Generic B2B Data Broker (AU)",
        "buys": ["master_list"],
        "niches": ["dentist", "electrician", "plumber"],
        "contact_channel": "email",
        "priority": 2,
        "notes": "Data brokers buy curated lead lists in bulk. They resell to agencies and sales teams. Lower price per lead but high volume.",
    },
    # Trade-specific agencies — niche-focused
    {
        "agency_type": "dental_supply",
        "agency_name": "Dental Supply / Equipment Co (AU)",
        "buys": ["niche_specific"],
        "niches": ["dentist"],
        "contact_channel": "email",
        "priority": 2,
        "notes": "Dental supply companies, practice management software vendors, and dental equipment sellers need dentist-specific lead lists.",
    },
    {
        "agency_type": "trade_supply",
        "agency_name": "Trade Supply / Tool Co (AU)",
        "buys": ["niche_specific"],
        "niches": ["electrician", "plumber"],
        "contact_channel": "email",
        "priority": 3,
        "notes": "Trade supply companies, tool manufacturers, and parts distributors need electrician/plumber lead lists for B2B sales.",
    },
]

# --------------------------------------------------------------------------- #
# Outreach templates
# --------------------------------------------------------------------------- #

TEMPLATES: dict[str, dict[str, str]] = {
    "digital_agency": {
        "subject": "Lead pack: {count} {niche}s without a website in {city}",
        "body": """Hi {name},

I'm reaching out because we've compiled a list of {count} {niche} businesses in {city} that currently don't have a website — or have a very poor online presence.

This is exactly the type of lead list that digital agencies use to prospect for web design and digital marketing clients.

What's included:
- Business name, address, phone
- Current website status (none / poor)
- Google rating and review count
- Quality tier ranking

We're offering this as a curated CSV lead pack for ${price}. Happy to send a sample of 5 leads first.

Would this be useful for your outreach team?

Best,
[Your name]""",
    },
    "seo_agency": {
        "subject": "{count} {niche}s in {city} with low Google ratings — lead pack",
        "body": """Hi {name},

We've identified {count} {niche} businesses in {city} with Google ratings below 4.0 and low review counts.

These are prime candidates for local SEO and reputation management services.

Lead pack includes:
- Business name, address, phone, website
- Current Google rating and review count
- Opportunity gap score
- Quality tier (A/B)

Curated list, ready for outreach. ${price} for the full pack, or I can send 5 sample leads first.

Interested?

Best,
[Your name]""",
    },
    "web_dev": {
        "subject": "{count} {niche}s in {city} with no website",
        "body": """Hi {name},

I have a list of {count} {niche} businesses in {city} that don't have a website.

If you build websites for local businesses, these are your highest-probability prospects — they're established businesses (they have Google listings and ratings) but no web presence.

Pack is ${price} and includes full contact details. Sample of 5 available on request.

Best,
[Your name]""",
    },
    "data_broker": {
        "subject": "Curated B2B lead list: {total} qualified AU businesses",
        "body": """Hi {name},

We have a curated database of {total} qualified business leads across Australia — dentists, electricians, and plumbers in Melbourne, Sydney, Brisbane, Perth, and Adelaide.

Each lead is scored and tiered:
- Quality score (0-100) based on web presence, rating, reviews, contact completeness
- Opportunity gap rate — identifies which businesses need digital services
- A/B tier filtering for targeted outreach

Data fields: name, address, phone, website, rating, niche, city, quality score, tier, opportunity tags.

Bulk pricing available. Can provide sample dataset.

Best,
[Your name]""",
    },
    "recruitment": {
        "subject": "{niche} business contacts in {city} — recruitment prospecting list",
        "body": """Hi {name},

We've compiled a list of {count} {niche} businesses in {city} with full contact details.

If your recruitment firm covers the {niche} trade, this list gives you direct contacts for employer branding and candidate sourcing outreach.

Pack is ${price}. Sample available.

Best,
[Your name]""",
    },
    "niche_specific": {
        "subject": "{count} {niche} business leads in {city} — verified contacts",
        "body": """Hi {name},

We have a verified list of {count} {niche} businesses in {city} with:
- Business name and address
- Phone number
- Website (if any)
- Google rating

Useful for B2B sales, supply partnerships, and service outreach.

${price} for the full list. Happy to send a sample.

Best,
[Your name]""",
    },
}

# --------------------------------------------------------------------------- #
# Pricing matrix
# --------------------------------------------------------------------------- #

def build_pricing_matrix(products_summary: list[dict]) -> list[dict]:
    """Build product x buyer x price recommendations from DB product summary."""
    matrix = []
    for product in products_summary:
        niche = product.get("niche", "all")
        tier = product.get("tier", "")
        city = product.get("city", "")
        count = product["lead_count"]

        # Determine best buyers
        buyers = ["digital_agency", "data_broker"]
        if niche == "dentist":
            buyers.append("dental_supply")
        if niche in ("electrician", "plumber"):
            buyers.append("trade_supply")

        if count >= 20:
            price = "$29-49"
        elif count >= 10:
            price = "$19-29"
        else:
            price = "$9-19"

        matrix.append({
            "niche": niche,
            "tier": tier,
            "city": city,
            "lead_count": count,
            "suggested_price": price,
            "target_buyers": buyers,
        })

    return matrix


# --------------------------------------------------------------------------- #
# Database — save outreach targets
# --------------------------------------------------------------------------- #

DDL_OUTREACH_TABLE = """
CREATE TABLE IF NOT EXISTS outreach_targets (
    id                   BIGSERIAL PRIMARY KEY,
    agency_type          TEXT NOT NULL,
    agency_name          TEXT NOT NULL,
    buys                 JSONB NOT NULL DEFAULT '[]'::jsonb,
    niches               JSONB NOT NULL DEFAULT '[]'::jsonb,
    contact_channel      TEXT,
    priority             INTEGER NOT NULL DEFAULT 3,
    template_key         TEXT,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outreach_priority ON outreach_targets (priority);
"""

DDL_PLAYBOOK_TABLE = """
CREATE TABLE IF NOT EXISTS sales_playbook (
    id                   BIGSERIAL PRIMARY KEY,
    step_number          INTEGER NOT NULL,
    step_title           TEXT NOT NULL,
    step_detail          TEXT NOT NULL,
    template_key         TEXT,
    status               TEXT NOT NULL DEFAULT 'pending',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog",
    )


async def ensure_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(DDL_OUTREACH_TABLE)
    await conn.execute(DDL_PLAYBOOK_TABLE)
    LOG.info("M6 tables ready")


async def save_targets(conn: asyncpg.Connection) -> int:
    """Save target agency database. Returns count."""
    sql = """
    INSERT INTO outreach_targets (agency_type, agency_name, buys, niches, contact_channel, priority, template_key, notes)
    VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6, $7, $8)
    ON CONFLICT DO NOTHING;
    """
    count = 0
    for agency in TARGET_AGENCIES:
        template_key = agency["agency_type"]
        await conn.execute(
            sql,
            agency["agency_type"],
            agency["agency_name"],
            agency["buys"],
            agency["niches"],
            agency["contact_channel"],
            agency["priority"],
            template_key,
            agency["notes"],
        )
        count += 1
    return count


async def save_playbook(conn: asyncpg.Connection) -> int:
    """Save sales playbook steps. Returns count."""
    steps = [
        (1, "Prepare product samples", "Select top 5 leads from each product pack. Create sample CSVs with name, address, phone, website, rating. These go in the first email — no pitch, just value.", None),
        (2, "Build outreach list", "Search for digital agencies, SEO agencies, and web dev studios in Australia. Use Google Maps, LinkedIn, and Clutch.co to find contact emails. Target 50-100 agencies.", None),
        (3, "Personalise templates", "Customise the email template for each agency type. Insert the right niche, city, count, and price. Keep it short — under 150 words.", "digital_agency"),
        (4, "Send sample email (batch 1)", "Send to 10-15 priority-1 agencies. Include 5 sample leads as attachment. Track opens and replies in a spreadsheet.", "digital_agency"),
        (5, "Follow up non-responders (Day 3)", "Send a brief follow-up to agencies that didn't reply. One sentence: 'Just checking if the lead pack is useful — happy to adjust the niche or city.'", None),
        (6, "Send sample email (batch 2)", "Send to next 15-20 agencies. Adjust messaging based on batch 1 feedback. If no opens, test a different subject line.", "seo_agency"),
        (7, "Handle replies and objections", "Common objections: 'Too expensive' (offer smaller pack), 'Not the right niche' (offer custom list), 'Already have leads' (offer gap analysis report). Track all responses.", None),
        (8, "Close first sale", "When an agency agrees, send the full CSV via email. Invoice via PayPal, Stripe, or bank transfer. Record revenue event.", None),
        (9, "Ask for referral", "After first sale, ask: 'Know any other agencies that might need leads?' Offer 20% discount on next pack for referrals.", None),
        (10, "Refresh data monthly", "Re-run collector to get updated ratings, new businesses, and remove closed ones. Offer existing customers the update at 50% price.", None),
    ]
    sql = """
    INSERT INTO sales_playbook (step_number, step_title, step_detail, template_key)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT DO NOTHING;
    """
    for step in steps:
        await conn.execute(sql, *step)
    return len(steps)


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #

async def run() -> dict[str, Any]:
    LOG.info("connecting to %s", DSN.split("@")[-1])
    conn = await asyncpg.connect(DSN)
    await _init_connection(conn)
    try:
        await ensure_schema(conn)

        # 1. Get product summary from DB
        products_sql = """
            SELECT p.niche, p.city, q.tier, COUNT(*) as lead_count
            FROM processed_leads p
            JOIN lead_quality_scores q ON q.lead_id = p.id
            WHERE q.tier IN ('A', 'B')
            GROUP BY p.niche, p.city, q.tier
            ORDER BY p.niche, q.tier, COUNT(*) DESC
        """
        product_rows = await conn.fetch(products_sql)
        products_summary = [dict(r) for r in product_rows]

        # 2. Save target agencies
        target_count = await save_targets(conn)
        LOG.info("saved %d target agencies", target_count)

        # 3. Save playbook
        playbook_count = await save_playbook(conn)
        LOG.info("saved %d playbook steps", playbook_count)

        # 4. Build pricing matrix
        pricing_matrix = build_pricing_matrix(products_summary)

        # 5. Save everything to JSON for easy reference
        output = {
            "run_id": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
            "generated_at": datetime.utcnow().isoformat(),
            "target_agencies": TARGET_AGENCIES,
            "outreach_templates": TEMPLATES,
            "pricing_matrix": pricing_matrix,
            "playbook_steps": [
                {"step": s[0], "title": s[1], "detail": s[2], "template": s[3]}
                for s in [
                    (1, "Prepare product samples", "Select top 5 leads from each product pack. Create sample CSVs.", None),
                    (2, "Build outreach list", "Search for digital agencies, SEO agencies, web dev studios in AU.", None),
                    (3, "Personalise templates", "Customise email template for each agency type.", "digital_agency"),
                    (4, "Send sample email (batch 1)", "Send to 10-15 priority-1 agencies with 5 sample leads.", "digital_agency"),
                    (5, "Follow up non-responders (Day 3)", "Brief follow-up to non-repliers.", None),
                    (6, "Send sample email (batch 2)", "Send to next 15-20 agencies. Adjust messaging.", "seo_agency"),
                    (7, "Handle replies and objections", "Track responses. Handle price/niche objections.", None),
                    (8, "Close first sale", "Send full CSV. Invoice. Record revenue.", None),
                    (9, "Ask for referral", "Offer 20% discount for referrals.", None),
                    (10, "Refresh data monthly", "Re-run collector. Offer updates at 50%.", None),
                ]
            ],
            "product_summary": products_summary,
        }

        # Save to products directory
        output_path = Path("/tmp/intelligence_products/sales_playbook.json")
        output_path.parent.mkdir(exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        LOG.info("sales playbook saved to %s", output_path)

        # 6. Print summary
        print("\n" + "=" * 60)
        print("M6 — Revenue Validation Engine — Results")
        print("=" * 60)

        print(f"\nTarget agencies: {target_count}")
        for agency in sorted(TARGET_AGENCIES, key=lambda a: a["priority"]):
            buys = ", ".join(agency["buys"])
            niches = ", ".join(agency["niches"])
            print(f"  P{agency['priority']} [{agency['agency_type']}] {agency['agency_name']}")
            print(f"     Buys: {buys} | Niches: {niches}")

        print(f"\nOutreach templates: {len(TEMPLATES)}")
        for key in TEMPLATES:
            print(f"  - {key}: {TEMPLATES[key]['subject']}")

        print(f"\nPricing matrix: {len(pricing_matrix)} product-buyer combinations")

        print(f"\nSales playbook: {playbook_count} steps")
        for step_num, title, _, _ in [
            (1, "Prepare product samples", None, None),
            (2, "Build outreach list", None, None),
            (3, "Personalise templates", None, None),
            (4, "Send sample email (batch 1)", None, None),
            (5, "Follow up non-responders", None, None),
            (6, "Send sample email (batch 2)", None, None),
            (7, "Handle replies and objections", None, None),
            (8, "Close first sale", None, None),
            (9, "Ask for referral", None, None),
            (10, "Refresh data monthly", None, None),
        ]:
            print(f"  {step_num}. {title}")

        print(f"\nSales playbook JSON: {output_path}")
        print("=" * 60)

        return {
            "target_agencies": target_count,
            "templates": len(TEMPLATES),
            "pricing_entries": len(pricing_matrix),
            "playbook_steps": playbook_count,
            "output": str(output_path),
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
