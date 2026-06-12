#!/usr/bin/env python3
"""
Google Maps AU Collector — M3d MVP
Collects dentists, plumbers, electricians in 5 AU capitals.
Outputs raw JSONL for processing pipeline.
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright
from uuid import UUID

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from intelligence_engine.storage.raw_storage import RawStorage
import asyncpg

# ============================================================================
# CONFIGURATION
# ============================================================================

NICHES = ["dentist", "plumber", "electrician"]
CITIES = ["Melbourne", "Sydney", "Brisbane", "Perth", "Adelaide"]

SOURCE_ID = UUID("3fb0b74d-f774-4267-ba37-456018574b58")  # google_maps_au from sources table
SOURCE_NAME = "google_maps_au"

DB_DSN = os.environ.get("INTELLIGENCE_DB_DSN", "postgresql://intelligence:password@localhost:5432/intelligence")

# Collector settings
MAX_PER_SEARCH = 50
SCROLL_PAUSE = 2.0
DETAIL_PAUSE = 1.0
REQUEST_TIMEOUT = 60000  # Increased to 60s
NAVIGATION_WAIT = "domcontentloaded"  # Less strict than networkidle

# Output
RAW_OUTPUT_DIR = Path("/tmp/intelligence_raw")
RAW_OUTPUT_DIR.mkdir(exist_ok=True)

# ============================================================================
# GOOGLE MAPS COLLECTOR
# ============================================================================

class GoogleMapsAUCollector:
    """Playwright-based Google Maps collector for AU niches × cities."""
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
        self.raw_storage = RawStorage(db_pool)
        self.collected_count = 0
        self.error_count = 0
        
    async def collect_all(self) -> int:
        """Run collection for all niche × city combos."""
        async with async_playwright() as p:
            # Use Playwright's bundled Chromium (no hardcoded path)
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 720},
                locale="en-AU"
            )
            page = await context.new_page()
            
            for niche in NICHES:
                for city in CITIES:
                    try:
                        count = await self._collect_niche_city(page, niche, city)
                        self.collected_count += count
                        print(f"  ✓ {niche} in {city}: {count} businesses")
                        
                        # Respectful delay between searches
                        await asyncio.sleep(2)
                    except Exception as e:
                        self.error_count += 1
                        print(f"  ✗ {niche} in {city}: {e}")
                        continue
            
            await browser.close()
        
        return self.collected_count
    
    async def _collect_niche_city(self, page, niche: str, city: str) -> int:
        """Collect businesses for one niche in one city."""
        query = f"{niche} in {city}"
        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        
        await page.goto(url, wait_until=NAVIGATION_WAIT, timeout=REQUEST_TIMEOUT)
        
        # Wait for results feed
        try:
            await page.wait_for_selector('[role="feed"]', timeout=15000)
        except:
            return 0
        
        # Scroll to load more results
        feed = page.locator('[role="feed"]')
        for _ in range(8):
            await feed.evaluate("el => el.scrollTop = el.scrollHeight")
            await asyncio.sleep(SCROLL_PAUSE)
        
        results = []
        consecutive_failures = 0
        
        while len(results) < MAX_PER_SEARCH:
            # Re-get cards each iteration (page may have changed after go_back)
            cards = await page.locator('[role="feed"] > div').all()
            
            # Filter to valid business cards (have a.hfpxzc link and are visible)
            valid_cards = []
            for card in cards:
                link = card.locator('a.hfpxzc').first
                if await link.count() > 0:
                    valid_cards.append(card)
            
            if not valid_cards:
                print(f"    No valid cards found")
                break
            
            # Try each valid card we haven't processed yet
            progress = False
            for i, card in enumerate(valid_cards):
                if len(results) >= MAX_PER_SEARCH:
                    break
                try:
                    data = await self._extract_card(page, card, niche, city)
                    if data:
                        results.append(data)
                        progress = True
                        consecutive_failures = 0
                        print(f"    ✓ {data['name']}")
                        # Write immediately to avoid losing data on timeout
                        await self.raw_storage.write(
                            source_id=SOURCE_ID,
                            source_record_id=data["place_id"],
                            raw_data=data,
                            metadata={"niche": niche, "city": city, "collector": "google_maps_au"}
                        )
                    else:
                        consecutive_failures += 1
                except Exception as e:
                    print(f"    Card extraction failed: {e}")
                    consecutive_failures += 1
            
            if not progress:
                if consecutive_failures >= 3:
                    print(f"    Too many failures, stopping")
                    break
                # Try scrolling more
                await feed.evaluate("el => el.scrollTop = el.scrollHeight")
                await asyncio.sleep(SCROLL_PAUSE)
        
        return len(results)
    
    async def _extract_card(self, page, card, niche: str, city: str) -> dict | None:
        """Extract data from a business card by clicking and reading detail panel."""
        # Click the card div with force (bypasses feed container interception)
        await card.click(force=True)
        await asyncio.sleep(DETAIL_PAUSE)
        
        # Wait for detail panel - wait for name element
        try:
            await page.wait_for_selector('h1.DUwDvf', timeout=10000)
        except:
            return None
        
        # Extract from detail panel
        # Name
        name_el = page.locator('h1.DUwDvf, h1.fontHeadlineLarge').first
        name = await name_el.text_content() if await name_el.count() > 0 else ""
        name = name.strip() if name else ""
        
        if not name:
            await page.go_back()
            await asyncio.sleep(1)
            return None
        
        # Address
        address = ""
        addr_els = await page.locator('[data-item-id="address"] .Io6YTe, .rogA2c .Io6YTe').all()
        for el in addr_els:
            text = await el.text_content()
            if text and text.strip():
                address = text.strip()
                break
        
        # Phone
        phone = ""
        phone_els = await page.locator('[data-item-id^="phone"] .Io6YTe').all()
        for el in phone_els:
            text = await el.text_content()
            if text and text.strip():
                phone = text.strip()
                break
        
        # Website
        website = ""
        web_els = await page.locator('[data-item-id="authority"] .Io6YTe, a[data-item-id="authority"]').all()
        for el in web_els:
            text = await el.text_content()
            href = await el.get_attribute("href")
            if text and text.strip():
                website = text.strip()
            elif href and href.startswith("http"):
                website = href
            if website:
                break
        
        # Rating
        rating = 0.0
        rating_el = page.locator('[role="img"][aria-label*="stars"]').first
        if await rating_el.count() > 0:
            aria = await rating_el.get_attribute("aria-label")
            if aria:
                import re
                match = re.search(r"(\d+\.?\d*)\s*out of 5", aria)
                if match:
                    rating = float(match.group(1))
        
        # Review count
        review_count = 0
        review_els = await page.locator('.fontBodyMedium .Io6YTe, button[aria-label*="review"]').all()
        for el in review_els:
            text = await el.text_content()
            if text:
                import re
                match = re.search(r"(\d[\d,]*)", text.replace(",", ""))
                if match:
                    review_count = int(match.group(1))
                    break
        
        # Place ID (from URL)
        place_id = ""
        current_url = page.url
        import re
        match = re.search(r"place/([^/]+)", current_url)
        if match:
            place_id = match.group(1)
        else:
            # Fallback: generate from name + city
            place_id = f"{name.lower().replace(' ', '_')}_{city.lower()}"
        
        # Categories
        categories = [niche]
        cat_els = await page.locator('[data-item-id="category"] .Io6YTe, .DkEaL .Io6YTe').all()
        for el in cat_els:
            text = await el.text_content()
            if text and text.strip() and text.strip() != niche:
                categories.append(text.strip())
        
        # Try to get lat/lng from URL
        lat, lng = None, None
        match = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", current_url)
        if match:
            lat = float(match.group(1))
            lng = float(match.group(2))
        
        # Go back to search results
        await page.go_back()
        await asyncio.sleep(1)
        
        return {
            "place_id": place_id,
            "name": name,
            "address": address,
            "phone": phone,
            "website": website,
            "rating": rating,
            "reviews": review_count,
            "categories": categories,
            "lat": lat,
            "lng": lng,
            "source_url": current_url,
            "collected_at": datetime.utcnow().isoformat()
        }


# ============================================================================
# MAIN
# ============================================================================

async def main():
    print(f"🚀 Google Maps AU Collector — M3d MVP")
    print(f"   Niches: {NICHES}")
    print(f"   Cities: {CITIES}")
    print(f"   Total searches: {len(NICHES) * len(CITIES)}")
    
    # Password from .env.intelligence
    pg_password = "changeme"
    dsn = f"postgresql://intelligence:***@localhost:5433/intelligence"
    print(f"   DB: localhost:5433/intelligence")
    print()
    
    # Connect to DB
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=5)
    
    try:
        collector = GoogleMapsAUCollector(pool)
        total = await collector.collect_all()
        print(f"\n✅ Collection complete: {total} businesses collected")
        print(f"   Errors: {collector.error_count}")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())