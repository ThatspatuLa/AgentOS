"""
intelligence_engine.processors.lead_processor
============================================

M3 — Lead Processing Engine (information-gathering mode).

Reads raw business records from `raw_collection_events`, normalises them,
deduplicates by place_id (falling back to name+address), and writes
structured lead rows into `processed_leads`.

Design principles:
- Local DB only. No outbound calls.
- Idempotent. Re-running on already-processed rows is safe.
- Each step is independently testable via small functions.
- Failures on a single row never abort the whole batch.

Run from the host:
    docker run --rm --network host \\
        -v $(pwd)/intelligence_engine:/app/intelligence_engine \\
        intel-collector python3 -u -m intelligence_engine.processors.lead_processor
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from typing import Any, Iterable
from uuid import UUID

import asyncpg

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

LOG = logging.getLogger("m3.lead_processor")

DSN = (
    "postgresql://intelligence:"
    "changeme"
    "@127.0.0.1:5433/intelligence"
)

# Google Maps AU source — matches `sources.source_id` seeded in the DB.
GOOGLE_MAPS_AU_SOURCE_ID = UUID("3fb0b74d-f774-4267-ba37-456018574b58")

BATCH_SIZE = 200

# Regex helpers ----------------------------------------------------------------

_WS = re.compile(r"\s+")
_PHONE_CLEAN = re.compile(r"[^\d+]")
_URL_PREFIX = re.compile(r"^[^\w]+")


# --------------------------------------------------------------------------- #
# Field-level normalisers
# --------------------------------------------------------------------------- #

def norm_text(value: Any) -> str:
    """Lower-case, collapse whitespace, strip."""
    if value is None:
        return ""
    s = str(value).strip().lower()
    s = _WS.sub(" ", s)
    return s


def norm_phone(value: Any) -> str:
    """Keep digits and leading +. AU numbers get a +61 prefix when missing."""
    if not value:
        return ""
    raw = str(value)
    cleaned = _PHONE_CLEAN.sub("", raw)
    if not cleaned:
        return ""
    if cleaned.startswith("0") and len(cleaned) == 10:
        # Australian local number: 03 9999 9703 -> +61399999703
        return "+61" + cleaned[1:]
    if cleaned.startswith("61") and len(cleaned) == 11:
        return "+" + cleaned
    if cleaned.startswith("+"):
        return cleaned
    return cleaned


def norm_website(value: Any) -> str:
    """Strip leading junk, lowercase host, keep scheme."""
    if not value:
        return ""
    s = _URL_PREFIX.sub("", str(value)).strip().lower()
    s = s.replace("http://", "https://")
    if s.startswith("www."):
        s = "https://" + s
    return s


def norm_rating(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        f = float(str(value).replace(",", "."))
        if f < 0 or f > 5:
            return None
        return round(f, 2)
    except (TypeError, ValueError):
        return None


def norm_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        n = int(float(str(value).replace(",", "")))
        return max(n, 0)
    except (TypeError, ValueError):
        return None


def parse_place_id(raw_data: dict) -> str | None:
    """Try real place_id from raw_data, fall back to a derived id."""
    pid = raw_data.get("place_id")
    if pid:
        return str(pid).strip()
    # Fallback: hash name + address to create a stable synthetic id.
    name = norm_text(raw_data.get("name"))
    address = norm_text(raw_data.get("address"))
    if not name:
        return None
    fallback = (name + "|" + address).replace(" ", "+")
    return fallback[:200]


# --------------------------------------------------------------------------- #
# Gap / opportunity scoring
# --------------------------------------------------------------------------- #

def primary_gap_rate(row: dict) -> float:
    """
    Primary Gap Rate (PGR) — 0.0 (no gap) -> 1.0 (every gap present).

    A higher score = more monetisable opportunity for an agency.
    """
    score = 0.0
    weight_total = 0.0

    # No website
    weight_total += 0.35
    if not row.get("website"):
        score += 0.35

    # Few / no reviews
    weight_total += 0.20
    reviews = row.get("review_count") or 0
    if reviews == 0:
        score += 0.20
    elif reviews < 10:
        score += 0.10

    # Low rating
    weight_total += 0.15
    rating = row.get("rating")
    if rating is None:
        score += 0.15
    elif rating < 3.5:
        score += 0.15
    elif rating < 4.0:
        score += 0.07

    # Phone missing
    weight_total += 0.10
    if not row.get("phone"):
        score += 0.10

    # Address missing
    weight_total += 0.10
    if not row.get("address"):
        score += 0.10

    # Rating present but reviews missing — partial credit
    weight_total += 0.10
    if rating is not None and reviews == 0:
        score += 0.10

    return round(score / weight_total, 3) if weight_total else 0.0


def detect_opportunities(row: dict) -> list[str]:
    """Return a list of short tags describing what this lead is missing."""
    tags: list[str] = []
    if not row.get("website"):
        tags.append("no_website")
    if row.get("rating") is None:
        tags.append("no_rating")
    elif (row.get("rating") or 0) < 4.0:
        tags.append("low_rating")
    if (row.get("review_count") or 0) < 10:
        tags.append("low_reviews")
    if not row.get("phone"):
        tags.append("no_phone")
    if not row.get("address"):
        tags.append("no_address")
    return tags


# --------------------------------------------------------------------------- #
# Row transformation
# --------------------------------------------------------------------------- #

def transform(raw_row: dict, metadata: dict) -> dict | None:
    """Turn one raw_collection_events row into a processed_leads row.

    Returns None when the row is too empty to be useful (e.g. blank name).
    """
    raw_data = raw_row.get("raw_data") or {}
    name = norm_text(raw_data.get("name"))
    if not name:
        return None

    lead = {
        "name": raw_data.get("name", "").strip(),
        "address": (raw_data.get("address") or "").strip(),
        "phone": norm_phone(raw_data.get("phone")),
        "website": norm_website(raw_data.get("website")),
        "rating": norm_rating(raw_data.get("rating")),
        "review_count": norm_int(raw_data.get("review_count")),
        "categories": raw_data.get("categories") or [],
        "lat": raw_data.get("lat"),
        "lng": raw_data.get("lng"),
        "place_id": parse_place_id(raw_data),
        "niche": (metadata or {}).get("niche"),
        "city": (metadata or {}).get("city"),
    }
    lead["primary_gap_rate"] = primary_gap_rate(lead)
    lead["opportunity_tags"] = detect_opportunities(lead)
    return lead


# --------------------------------------------------------------------------- #
# Database access
# --------------------------------------------------------------------------- #

DDL_PROCESSED_LEADS = """
CREATE TABLE IF NOT EXISTS processed_leads (
    id                   BIGSERIAL PRIMARY KEY,
    source_record_id     TEXT NOT NULL,
    source_id            UUID NOT NULL,
    place_id             TEXT,
    name                 TEXT NOT NULL,
    address              TEXT,
    phone                TEXT,
    website              TEXT,
    rating               NUMERIC(3,2),
    review_count         INTEGER,
    categories           JSONB NOT NULL DEFAULT '[]'::jsonb,
    lat                  DOUBLE PRECISION,
    lng                  DOUBLE PRECISION,
    niche                TEXT,
    city                 TEXT,
    primary_gap_rate     NUMERIC(4,3) NOT NULL DEFAULT 0,
    opportunity_tags     JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_event_id      BIGINT REFERENCES raw_collection_events(id) ON DELETE SET NULL,
    processed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT processed_leads_source_unique
        UNIQUE (source_id, source_record_id)
);
CREATE INDEX IF NOT EXISTS idx_processed_leads_niche_city
    ON processed_leads (niche, city);
CREATE INDEX IF NOT EXISTS idx_processed_leads_gap_rate
    ON processed_leads (primary_gap_rate DESC);
CREATE INDEX IF NOT EXISTS idx_processed_leads_place_id
    ON processed_leads (place_id);
"""


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Auto-decode JSONB columns to Python dicts/list."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def ensure_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(DDL_PROCESSED_LEADS)
    LOG.info("processed_leads table ready")


async def fetch_pending_raw(
    conn: asyncpg.Connection,
    source_id: UUID,
    limit: int,
) -> list[asyncpg.Record]:
    """Rows that have not been processed yet."""
    sql = """
        SELECT id, source_record_id, raw_data, metadata
        FROM raw_collection_events
        WHERE source_id = $1
          AND status = 'pending'
          AND id NOT IN (SELECT source_event_id FROM processed_leads
                         WHERE source_event_id IS NOT NULL)
        ORDER BY id
        LIMIT $2;
    """
    return await conn.fetch(sql, source_id, limit)


async def upsert_processed(
    conn: asyncpg.Connection,
    source_id: UUID,
    lead: dict,
    raw_event_id: int,
    source_record_id: str,
) -> None:
    sql = """
    INSERT INTO processed_leads (
        source_record_id, source_id, place_id, name, address, phone, website,
        rating, review_count, categories, lat, lng, niche, city,
        primary_gap_rate, opportunity_tags, source_event_id
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7,
        $8, $9, $10::jsonb, $11, $12, $13, $14,
        $15, $16::jsonb, $17
    )
    ON CONFLICT (source_id, source_record_id) DO UPDATE
        SET place_id         = EXCLUDED.place_id,
            name             = EXCLUDED.name,
            address          = EXCLUDED.address,
            phone            = EXCLUDED.phone,
            website          = EXCLUDED.website,
            rating           = EXCLUDED.rating,
            review_count     = EXCLUDED.review_count,
            categories       = EXCLUDED.categories,
            lat              = EXCLUDED.lat,
            lng              = EXCLUDED.lng,
            niche            = EXCLUDED.niche,
            city             = EXCLUDED.city,
            primary_gap_rate = EXCLUDED.primary_gap_rate,
            opportunity_tags = EXCLUDED.opportunity_tags,
            source_event_id  = EXCLUDED.source_event_id,
            processed_at     = now();
    """
    await conn.execute(
        sql,
        source_record_id,
        source_id,
        lead.get("place_id"),
        lead["name"],
        lead.get("address"),
        lead.get("phone") or None,
        lead.get("website") or None,
        lead.get("rating"),
        lead.get("review_count"),
        lead.get("categories") or [],
        lead.get("lat"),
        lead.get("lng"),
        lead.get("niche"),
        lead.get("city"),
        lead.get("primary_gap_rate", 0.0),
        lead.get("opportunity_tags") or [],
        raw_event_id,
    )


async def mark_processed(
    conn: asyncpg.Connection,
    raw_event_id: int,
) -> None:
    await conn.execute(
        "UPDATE raw_collection_events "
        "SET status = 'processed', processed_at = now() "
        "WHERE id = $1;",
        raw_event_id,
    )


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #

async def process_batch(
    conn: asyncpg.Connection,
    rows: Iterable[asyncpg.Record],
) -> tuple[int, int, int]:
    """Returns (processed, skipped, failed) counts."""
    processed = skipped = failed = 0
    for r in rows:
        raw_event_id = r["id"]
        source_record_id = r["source_record_id"]
        raw_data = r["raw_data"]
        metadata = r["metadata"] or {}
        # asyncpg returns JSONB as a JSON string by default. Decode.
        if isinstance(raw_data, str):
            raw_data = json.loads(raw_data)
        if isinstance(metadata, str):
            metadata = json.loads(metadata) if metadata else {}
        try:
            lead = transform({"raw_data": raw_data}, metadata)
            if lead is None:
                skipped += 1
                LOG.debug("skip %s: empty name", source_record_id)
                continue
            await upsert_processed(
                conn,
                GOOGLE_MAPS_AU_SOURCE_ID,
                lead,
                raw_event_id,
                source_record_id,
            )
            await mark_processed(conn, raw_event_id)
            processed += 1
        except Exception as exc:
            failed += 1
            LOG.warning("row %s failed: %s", raw_event_id, exc)
    return processed, skipped, failed


async def run(limit: int | None = None) -> dict[str, int]:
    LOG.info("connecting to %s", DSN.split("@")[-1])
    conn = await asyncpg.connect(DSN)
    await _init_connection(conn)
    try:
        await ensure_schema(conn)
        total_p = total_s = total_f = 0
        while True:
            rows = await fetch_pending_raw(
                conn, GOOGLE_MAPS_AU_SOURCE_ID, BATCH_SIZE,
            )
            if not rows:
                break
            p, s, f = await process_batch(conn, rows)
            total_p += p
            total_s += s
            total_f += f
            LOG.info(
                "batch processed=%d skipped=%d failed=%d (running total %d/%d/%d)",
                p, s, f, total_p, total_s, total_f,
            )
            if limit and total_p >= limit:
                break
        return {"processed": total_p, "skipped": total_s, "failed": total_f}
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
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Stop after this many rows processed (default: all).",
    )
    args = parser.parse_args()
    summary = asyncio.run(run(limit=args.limit))
    LOG.info(
        "DONE processed=%d skipped=%d failed=%d",
        summary["processed"], summary["skipped"], summary["failed"],
    )
    return 0 if summary["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
