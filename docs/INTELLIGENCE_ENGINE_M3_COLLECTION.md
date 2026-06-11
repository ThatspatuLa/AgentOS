# Intelligence Engine — M3 Collection Architecture

**Date:** 2026-06-11
**Status:** Draft — Pending Review
**Target:** PostgreSQL 16+ with pgvector, Crawl4AI, Playwright, Firecrawl, Scrapy, Hermes Cron + APScheduler
**Author:** Minato (via Zen)

---

## Design Principles

1. **Idempotent by default** — Same input → same output; safe to re-run
2. **Source attribution on every record** — Traceable to origin
3. **Async-first** — High throughput via asyncio, connection pooling
4. **Resilient** — Exponential backoff, circuit breakers, dead letter queues
5. **Observable** — Structured logging, metrics, health checks
6. **Config-driven** — Sources, schedules, selectors in DB/config, not code

---

## Source Taxonomy

| Source Type | Examples | Collection Method | Frequency |
|-------------|----------|-------------------|-----------|
| **Business Directories** | Google Maps, Yelp, Yellow Pages, Thomson Local | Playwright (JS) + Crawl4AI | Daily |
| **Official Registries** | Companies House, ABN Lookup, SEC EDGAR, AusTender, GrantConnect | API (official) + Firecrawl fallback | Hourly/Daily |
| **Review Platforms** | Google Reviews, Trustpilot, Yelp, Facebook Reviews | Playwright + Crawl4AI | 6-hourly |
| **Industry Feeds** | RSS/Atom (blogs, news), Product Hunt, HN, GitHub Trending | Crawl4AI + Feedparser | 15-min/1hr |
| **Websites (targeted)** | Competitor sites, client sites, prospect sites | Playwright (full render) + Lighthouse CI | On-demand / Daily |
| **Job Boards** | LinkedIn, Indeed, company career pages | Playwright + API where available | 6-hourly |
| **Social Signals** | Twitter/X (Nitter), LinkedIn, Facebook | Nitter/Invidious instances + API | 15-min/1hr |

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           COLLECTION PIPELINE                                    │
│                                                                                  │
│  ┌──────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────────┐   │
│  │ SCHEDULER│───▶│  ORCHESTRATOR│───▶│  COLLECTORS │───▶│  RAW STORAGE     │   │
│  │ (Cron)   │    │  (APScheduler│    │  (Adapters) │    │  (JSONL/MinIO)   │   │
│  └──────────┘    └──────────────┘    └─────────────┘    └────────┬─────────┘   │
│                                                                    │             │
│                                                                    ▼             │
│  ┌──────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────────┐   │
│  │  LOAD    │◀───│  ENRICHMENT  │◀───│  PROCESSING │◀───│  VALIDATION      │   │
│  │ (PG)     │    │  (Embeddings,│    │ (Clean,     │    │ (Schema, Dedup)  │   │
│  │          │    │   Classification)    │  Normalize) │    │                  │   │
│  └──────────┘    └──────────────┘    └─────────────┘    └──────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Component 1: Scheduler Layer

### Hermes Cron (Declarative, Simple)

```yaml
# Registered via Hermes cronjob tool
# Run via: cronjob(action='create', schedule='0 * * * *', ...)

collection_jobs:
  - name: "collect-google-maps-daily"
    schedule: "0 6 * * *"           # 6 AM daily
    payload: {"source": "google_maps", "mode": "incremental"}
    
  - name: "collect-companies-house-hourly"
    schedule: "0 * * * *"           # Hourly
    payload: {"source": "companies_house", "mode": "incremental"}
    
  - name: "collect-reviews-6hourly"
    schedule: "0 */6 * * *"         # Every 6 hours
    payload: {"source": "google_reviews", "mode": "incremental"}
    
  - name: "collect-industry-feeds-15min"
    schedule: "*/15 * * * *"        # Every 15 min
    payload: {"source": "industry_feeds", "mode": "incremental"}
```

### APScheduler (Dynamic, Complex)

```python
# intelligence_engine/scheduler/collection_scheduler.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from datetime import datetime, timedelta
import asyncio

class CollectionScheduler:
    def __init__(self, db_url: str):
        jobstores = {
            'default': SQLAlchemyJobStore(url=db_url)
        }
        executors = {
            'default': ThreadPoolExecutor(20),
            'asyncio': {'type': 'asyncio'}
        }
        self.scheduler = AsyncIOScheduler(jobstores=jobstores, executors=executors)
        
    async def register_source_jobs(self, sources: list[dict]):
        """Register collection jobs for active sources from DB."""
        for source in sources:
            if not source['is_active']:
                continue
                
            # Parse frequency (supports cron, interval, custom)
            freq = source.get('collection_frequency', '1 day')
            
            if freq.endswith('min') or freq.endswith('hour') or freq.endswith('day'):
                # Interval trigger
                interval = self._parse_interval(freq)
                self.scheduler.add_job(
                    self._run_collection,
                    'interval',
                    minutes=interval,
                    args=[source['source_id']],
                    id=f"collect-{source['source_id']}",
                    replace_existing=True,
                    max_instances=1,
                    coalesce=True
                )
            else:
                # Cron trigger
                self.scheduler.add_job(
                    self._run_collection,
                    'cron',
                    **self._parse_cron(freq),
                    args=[source['source_id']],
                    id=f"collect-{source['source_id']}",
                    replace_existing=True
                )
    
    async def _run_collection(self, source_id: str):
        """Execute collection for a source."""
        from intelligence_engine.collectors.factory import get_collector
        collector = get_collector(source_id)
        await collector.run_incremental()
    
    def start(self):
        self.scheduler.start()
    
    def shutdown(self):
        self.scheduler.shutdown()
```

---

## Component 2: Collector Base Class

```python
# intelligence_engine/collectors/base.py

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncIterator
from uuid import UUID
import asyncio
import logging
import json

logger = logging.getLogger(__name__)

@dataclass
class CollectionResult:
    """Result of a single collection operation."""
    source_id: UUID
    source_record_id: str                    # Unique ID from source
    raw_data: dict                           # Full raw response
    collected_at: datetime = field(default_factory=datetime.utcnow)
    collector_version: str = "1.0"
    metadata: dict = field(default_factory=dict)  # HTTP status, latency, etc.
    
    def to_jsonl(self) -> str:
        return json.dumps({
            'source_id': str(self.source_id),
            'source_record_id': self.source_record_id,
            'raw_data': self.raw_data,
            'collected_at': self.collected_at.isoformat(),
            'collector_version': self.collector_version,
            'metadata': self.metadata
        }, default=str)

@dataclass
class CollectorConfig:
    """Configuration for a collector."""
    source_id: UUID
    name: str
    base_url: str
    rate_limit_rpm: int = 60
    rate_limit_rph: int = 1000
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff: float = 2.0
    headers: dict = field(default_factory=dict)
    auth: dict | None = None
    selectors: dict = field(default_factory=dict)  # CSS/XPath selectors
    pagination: dict = field(default_factory=dict)
    custom: dict = field(default_factory=dict)

class BaseCollector(ABC):
    """Abstract base collector with common functionality."""
    
    def __init__(self, config: CollectorConfig, db_pool, http_session):
        self.config = config
        self.db_pool = db_pool
        self.session = http_session
        self.semaphore = asyncio.Semaphore(config.rate_limit_rpm // 60 + 1)
        self._request_times: list[float] = []
        
    async def _rate_limit(self):
        """Enforce rate limits."""
        async with self.semaphore:
            now = asyncio.get_event_loop().time()
            # Remove requests older than 1 minute
            self._request_times = [t for t in self._request_times if now - t < 60]
            if len(self._request_times) >= self.config.rate_limit_rpm:
                wait = 60 - (now - self._request_times[0])
                if wait > 0:
                    await asyncio.sleep(wait)
            self._request_times.append(now)
    
    async def _request_with_retry(self, method: str, url: str, **kwargs) -> Any:
        """HTTP request with exponential backoff retry."""
        last_exception = None
        for attempt in range(self.config.max_retries + 1):
            try:
                await self._rate_limit()
                async with self.session.request(
                    method, url, 
                    timeout=aiohttp.ClientTimeout(total=self.config.timeout_seconds),
                    headers={**self.config.headers, **kwargs.pop('headers', {})},
                    **kwargs
                ) as resp:
                    if resp.status == 429:  # Rate limited
                        retry_after = int(resp.headers.get('Retry-After', 60))
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    return await resp.json()
            except Exception as e:
                last_exception = e
                if attempt < self.config.max_retries:
                    wait = self.config.retry_backoff ** attempt
                    logger.warning(f"Request failed (attempt {attempt+1}): {e}, retrying in {wait}s")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"Request failed after {self.config.max_retries} retries: {e}")
                    raise
        raise last_exception
    
    @abstractmethod
    async def collect_incremental(self) -> AsyncIterator[CollectionResult]:
        """Collect new/updated records since last run."""
        pass
    
    @abstractmethod
    async def collect_full(self) -> AsyncIterator[CollectionResult]:
        """Collect all records (initial backfill)."""
        pass
    
    async def run_incremental(self):
        """Run incremental collection and persist to raw storage."""
        from intelligence_engine.storage.raw_storage import RawStorage
        raw_storage = RawStorage(self.db_pool)
        
        async for result in self.collect_incremental():
            await raw_storage.write(result)
    
    async def run_full(self):
        """Run full collection and persist to raw storage."""
        from intelligence_engine.storage.raw_storage import RawStorage
        raw_storage = RawStorage(self.db_pool)
        
        async for result in self.collect_full():
            await raw_storage.write(result)
```

---

## Component 3: Specific Collectors

### 3.1 Google Maps Collector (Playwright)

```python
# intelligence_engine/collectors/google_maps.py

from intelligence_engine.collectors.base import BaseCollector, CollectorConfig, CollectionResult
from playwright.async_api import async_playwright
from datetime import datetime
from typing import AsyncIterator
from uuid import UUID
import asyncio
import json

class GoogleMapsCollector(BaseCollector):
    """Collect businesses from Google Maps via Playwright."""
    
    async def collect_incremental(self) -> AsyncIterator[CollectionResult]:
        """Collect businesses for configured niches/cities since last run."""
        # Get last run timestamp from DB
        last_run = await self._get_last_run_timestamp()
        
        niches = self.config.custom.get('niches', ['barber', 'gym', 'dentist', 'plumber', 'restaurant'])
        cities = self.config.custom.get('cities', ['London', 'Manchester', 'Birmingham', 'Leeds', 'Glasgow'])
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            for niche in niches:
                for city in cities:
                    try:
                        results = await self._search_niche_city(page, niche, city, last_run)
                        for result in results:
                            yield result
                        await asyncio.sleep(2)  # Be respectful
                    except Exception as e:
                        logger.error(f"Failed {niche} in {city}: {e}")
                        continue
            
            await browser.close()
    
    async def _search_niche_city(self, page, niche: str, city: str, since: datetime | None) -> list[CollectionResult]:
        """Search Google Maps for niche in city."""
        query = f"{niche} in {city}"
        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await page.wait_for_selector('[role="feed"]', timeout=15000)
        
        # Scroll to load more results
        feed = page.locator('[role="feed"]')
        for _ in range(5):  # Scroll 5 times
            await feed.evaluate('el => el.scrollTop = el.scrollHeight')
            await asyncio.sleep(1)
        
        # Extract business cards
        cards = await page.locator('[role="feed"] > div').all()
        results = []
        
        for card in cards:
            try:
                data = await self._extract_card(card, niche, city)
                if data:
                    results.append(CollectionResult(
                        source_id=self.config.source_id,
                        source_record_id=data['place_id'],
                        raw_data=data,
                        metadata={'niche': niche, 'city': city}
                    ))
            except Exception as e:
                logger.warning(f"Failed to extract card: {e}")
                continue
        
        return results
    
    async def _extract_card(self, card, niche: str, city: str) -> dict | None:
        """Extract data from a business card."""
        # Click to open details
        await card.click()
        await card.page.wait_for_selector('[data-result-index]', timeout=5000)
        
        # Extract from detail panel
        # ... implementation details
        return {
            'place_id': '...',
            'name': '...',
            'address': '...',
            'phone': '...',
            'website': '...',
            'rating': 4.5,
            'reviews': 120,
            'categories': [niche],
            'lat': 51.5,
            'lng': -0.1
        }
    
    async def collect_full(self) -> AsyncIterator[CollectionResult]:
        """Full backfill - same as incremental but without since filter."""
        async for result in self.collect_incremental():
            yield result
```

### 3.2 Official Registry Collector (API + Firecrawl Fallback)

```python
# intelligence_engine/collectors/official_registry.py

from intelligence_engine.collectors.base import BaseCollector, CollectorConfig, CollectionResult
from datetime import datetime, timedelta
from typing import AsyncIterator
from uuid import UUID
import aiohttp
import asyncio

class OfficialRegistryCollector(BaseCollector):
    """Collect from official registries (Companies House, ABN, SEC, etc.)."""
    
    REGISTRY_CONFIGS = {
        'companies_house': {
            'base_url': 'https://api.company-information.service.gov.uk',
            'search_endpoint': '/search/companies',
            'detail_endpoint': '/company/{company_number}',
            'rate_limit_rpm': 600,
            'auth_type': 'basic',  # API key as username, empty password
        },
        'abn_lookup': {
            'base_url': 'https://abr.business.gov.au/json/AbnDetails.aspx',
            'search_endpoint': '/search',
            'rate_limit_rpm': 100,
            'auth_type': 'api_key',
        },
        'sec_edgar': {
            'base_url': 'https://data.sec.gov',
            'search_endpoint': '/submissions/CIK{cik}.json',
            'rate_limit_rpm': 10,
            'auth_type': 'none',
        }
    }
    
    async def collect_incremental(self) -> AsyncIterator[CollectionResult]:
        registry = self.config.custom.get('registry', 'companies_house')
        config = self.REGISTRY_CONFIGS[registry]
        
        # Get last run timestamp
        since = await self._get_last_run_timestamp()
        if since:
            since_str = since.strftime('%Y-%m-%d')
        else:
            since_str = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Search for new/updated entities
        async for result in self._search_registry(registry, config, since_str):
            yield result
    
    async def _search_registry(self, registry: str, config: dict, since: str) -> AsyncIterator[CollectionResult]:
        """Search registry for new/updated entities."""
        if registry == 'companies_house':
            async for result in self._search_companies_house(config, since):
                yield result
        elif registry == 'abn_lookup':
            async for result in self._search_abn(config, since):
                yield result
        # ... other registries
    
    async def _search_companies_house(self, config: dict, since: str) -> AsyncIterator[CollectionResult]:
        """Search Companies House for new incorporations/updates."""
        url = f"{config['base_url']}{config['search_endpoint']}"
        params = {
            'q': f'incorporated:{since}',  # Companies House search syntax
            'items_per_page': 100,
            'start_index': 0
        }
        
        auth = aiohttp.BasicAuth(self.config.auth.get('api_key', ''), '')
        
        while True:
            async with self.session.get(url, params=params, auth=auth) as resp:
                if resp.status == 429:
                    await asyncio.sleep(60)
                    continue
                data = await resp.json()
            
            for item in data.get('items', []):
                yield CollectionResult(
                    source_id=self.config.source_id,
                    source_record_id=item['company_number'],
                    raw_data=item,
                    metadata={'registry': 'companies_house'}
                )
            
            # Pagination
            if data.get('items_per_page', 0) * (data.get('start_index', 0) + 1) >= data.get('total_results', 0):
                break
            params['start_index'] += params['items_per_page']
            await asyncio.sleep(0.1)  # Rate limit
    
    async def collect_full(self) -> AsyncIterator[CollectionResult]:
        """Full backfill - collect all entities."""
        async for result in self.collect_incremental():
            yield result
```

### 3.3 Review Collector (Playwright + Crawl4AI)

```python
# intelligence_engine/collectors/reviews.py

from intelligence_engine.collectors.base import BaseCollector, CollectorConfig, CollectionResult
from crawl4ai import AsyncWebCrawler
from playwright.async_api import async_playwright
from datetime import datetime
from typing import AsyncIterator
from uuid import UUID

class ReviewCollector(BaseCollector):
    """Collect reviews from Google Reviews, Trustpilot, Yelp."""
    
    PLATFORM_CONFIGS = {
        'google_reviews': {
            'base_url': 'https://www.google.com/maps',
            'requires_js': True,
            'selectors': {
                'review_list': 'div[data-review-id]',
                'author': '.d4r55',
                'rating': 'span[role="img"]',
                'text': '.wiI7pd',
                'date': '.rsqaWe'
            }
        },
        'trustpilot': {
            'base_url': 'https://www.trustpilot.com',
            'requires_js': False,
            'selectors': {
                'review_list': 'article[data-service-review-card-paper]',
                'author': '.typography_heading-xxs__QKBS8',
                'rating': '[data-service-review-rating]',
                'text': '.typography_body-l__KUYFJ',
                'date': '.typography_body-m__xgxZ_'
            }
        }
    }
    
    async def collect_incremental(self) -> AsyncIterator[CollectionResult]:
        platform = self.config.custom.get('platform', 'google_reviews')
        business_ids = self.config.custom.get('business_ids', [])  # From DB
        
        if platform == 'google_reviews':
            async for result in self._collect_google_reviews(business_ids):
                yield result
        elif platform == 'trustpilot':
            async for result in self._collect_trustpilot(business_ids):
                yield result
    
    async def _collect_google_reviews(self, business_ids: list[str]) -> AsyncIterator[CollectionResult]:
        """Collect Google reviews via Playwright."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            for biz_id in business_ids:
                try:
                    url = f"https://www.google.com/maps/place/?q=place_id:{biz_id}"
                    await page.goto(url, wait_until='networkidle')
                    await page.click('button:has-text("Reviews")')
                    await page.wait_for_selector('div[data-review-id]')
                    
                    # Scroll to load more
                    for _ in range(10):
                        await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                        await asyncio.sleep(0.5)
                    
                    reviews = await page.locator('div[data-review-id]').all()
                    for review in reviews:
                        data = await self._extract_google_review(review, biz_id)
                        if data:
                            yield CollectionResult(
                                source_id=self.config.source_id,
                                source_record_id=f"{biz_id}_{data['review_id']}",
                                raw_data=data,
                                metadata={'platform': 'google_reviews', 'business_id': biz_id}
                            )
                except Exception as e:
                    logger.error(f"Failed to collect reviews for {biz_id}: {e}")
            
            await browser.close()
    
    async def _collect_trustpilot(self, business_ids: list[str]) -> AsyncIterator[CollectionResult]:
        """Collect Trustpilot reviews via Crawl4AI (no JS needed)."""
        async with AsyncWebCrawler() as crawler:
            for biz_id in business_ids:
                try:
                    url = f"https://www.trustpilot.com/review/{biz_id}"
                    result = await crawler.arun(url=url)
                    
                    # Parse with CSS selectors
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(result.html, 'html.parser')
                    
                    for review_el in soup.select('article[data-service-review-card-paper]'):
                        data = self._extract_trustpilot_review(review_el, biz_id)
                        if data:
                            yield CollectionResult(
                                source_id=self.config.source_id,
                                source_record_id=f"{biz_id}_{data['review_id']}",
                                raw_data=data,
                                metadata={'platform': 'trustpilot', 'business_id': biz_id}
                            )
                except Exception as e:
                    logger.error(f"Failed Trustpilot for {biz_id}: {e}")
    
    async def collect_full(self) -> AsyncIterator[CollectionResult]:
        async for result in self.collect_incremental():
            yield result
```

### 3.4 Industry Feed Collector (RSS/Atom + Crawl4AI)

```python
# intelligence_engine/collectors/industry_feeds.py

from intelligence_engine.collectors.base import BaseCollector, CollectorConfig, CollectionResult
from crawl4ai import AsyncWebCrawler
import feedparser
from datetime import datetime
from typing import AsyncIterator
from uuid import UUID
import hashlib

class IndustryFeedCollector(BaseCollector):
    """Collect from RSS/Atom feeds, Product Hunt, HN, GitHub Trending."""
    
    FEED_SOURCES = {
        'rss_feeds': {
            'type': 'rss',
            'urls': [
                'https://techcrunch.com/feed/',
                'https://www.theverge.com/rss/index.xml',
                'https://venturebeat.com/feed/',
                # Niche feeds added via config
            ]
        },
        'product_hunt': {
            'type': 'rss',
            'urls': ['https://www.producthunt.com/feed']
        },
        'hacker_news': {
            'type': 'json',
            'urls': ['https://hacker-news.firebaseio.com/v0/topstories.json']
        },
        'github_trending': {
            'type': 'crawl4ai',
            'urls': ['https://github.com/trending']
        }
    }
    
    async def collect_incremental(self) -> AsyncIterator[CollectionResult]:
        for source_name, source_config in self.FEED_SOURCES.items():
            if source_config['type'] == 'rss':
                async for result in self._collect_rss(source_name, source_config['urls']):
                    yield result
            elif source_config['type'] == 'json':
                async for result in self._collect_json(source_name, source_config['urls']):
                    yield result
            elif source_config['type'] == 'crawl4ai':
                async for result in self._collect_crawl4ai(source_name, source_config['urls']):
                    yield result
    
    async def _collect_rss(self, source_name: str, urls: list[str]) -> AsyncIterator[CollectionResult]:
        for url in urls:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                # Generate deterministic ID from link
                record_id = hashlib.sha256(entry.get('link', '').encode()).hexdigest()[:16]
                
                yield CollectionResult(
                    source_id=self.config.source_id,
                    source_record_id=record_id,
                    raw_data={
                        'title': entry.get('title', ''),
                        'link': entry.get('link', ''),
                        'summary': entry.get('summary', ''),
                        'published': entry.get('published', ''),
                        'author': entry.get('author', ''),
                        'tags': [t.term for t in entry.get('tags', [])],
                        'feed_url': url
                    },
                    metadata={'source_type': 'rss', 'feed_source': source_name}
                )
    
    async def _collect_json(self, source_name: str, urls: list[str]) -> AsyncIterator[CollectionResult]:
        for url in urls:
            async with self.session.get(url) as resp:
                data = await resp.json()
            
            if source_name == 'hacker_news':
                # Get top 50 story IDs, then fetch each
                story_ids = data[:50]
                for story_id in story_ids:
                    story_url = f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
                    async with self.session.get(story_url) as resp:
                        story = await resp.json()
                    
                    yield CollectionResult(
                        source_id=self.config.source_id,
                        source_record_id=f"hn_{story_id}",
                        raw_data=story,
                        metadata={'source_type': 'hacker_news'}
                    )
                    await asyncio.sleep(0.05)
    
    async def _collect_crawl4ai(self, source_name: str, urls: list[str]) -> AsyncIterator[CollectionResult]:
        async with AsyncWebCrawler() as crawler:
            for url in urls:
                result = await crawler.arun(url=url)
                
                # Extract structured data from GitHub trending
                if source_name == 'github_trending':
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(result.html, 'html.parser')
                    
                    for repo in soup.select('article.Box-row'):
                        data = self._extract_github_repo(repo)
                        if data:
                            yield CollectionResult(
                                source_id=self.config.source_id,
                                source_record_id=f"github_{data['owner']}_{data['repo']}",
                                raw_data=data,
                                metadata={'source_type': 'github_trending'}
                            )
    
    def _extract_github_repo(self, repo_el) -> dict | None:
        # ... extract repo name, stars, description, language
        pass
    
    async def collect_full(self) -> AsyncIterator[CollectionResult]:
        async for result in self.collect_incremental():
            yield result
```

### 3.5 Website Analysis Collector (Playwright + Lighthouse)

```python
# intelligence_engine/collectors/website_analysis.py

from intelligence_engine.collectors.base import BaseCollector, CollectorConfig, CollectionResult
from playwright.async_api import async_playwright
from datetime import datetime
from typing import AsyncIterator
from uuid import UUID
import subprocess
import json
import tempfile
import os

class WebsiteAnalysisCollector(BaseCollector):
    """Collect website technical data: Lighthouse, technographics, SEO."""
    
    async def collect_incremental(self) -> AsyncIterator[CollectionResult]:
        """Analyze websites for businesses due for re-analysis."""
        # Get business IDs due for analysis from DB
        business_urls = await self._get_due_websites()
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            for business_id, url in business_urls:
                try:
                    result = await self._analyze_website(browser, business_id, url)
                    if result:
                        yield result
                except Exception as e:
                    logger.error(f"Failed to analyze {url}: {e}")
            
            await browser.close()
    
    async def _analyze_website(self, browser, business_id: str, url: str) -> CollectionResult | None:
        """Run full website analysis."""
        # 1. Playwright: DOM analysis, technographics, content
        page = await browser.new_page()
        await page.goto(url, wait_until='networkidle', timeout=30000)
        
        # Technographics via DOM inspection
        technographics = await self._extract_technographics(page)
        
        # Content analysis
        content = await self._extract_content(page)
        
        # SEO signals
        seo = await self._extract_seo_signals(page)
        
        await page.close()
        
        # 2. Lighthouse CI (CLI)
        lighthouse = await self._run_lighthouse(url)
        
        # 3. PageSpeed Insights API (if configured)
        psi = await self._run_psi(url) if self.config.custom.get('psi_api_key') else {}
        
        return CollectionResult(
            source_id=self.config.source_id,
            source_record_id=business_id,
            raw_data={
                'business_id': business_id,
                'url': url,
                'technographics': technographics,
                'content': content,
                'seo': seo,
                'lighthouse': lighthouse,
                'psi': psi,
                'analyzed_at': datetime.utcnow().isoformat()
            },
            metadata={'collector': 'website_analysis'}
        )
    
    async def _run_lighthouse(self, url: str) -> dict:
        """Run Lighthouse CLI and return JSON."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            output_path = f.name
        
        try:
            cmd = [
                'npx', 'lighthouse', url,
                '--output=json',
                f'--output-path={output_path}',
                '--chrome-flags="--headless --no-sandbox"',
                '--preset=desktop',
                '--quiet'
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            
            with open(output_path) as f:
                return json.load(f)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)
    
    async def collect_full(self) -> AsyncIterator[CollectionResult]:
        async for result in self.collect_incremental():
            yield result
```

---

## Component 4: Processing Pipeline

```python
# intelligence_engine/processing/pipeline.py

from intelligence_engine.storage.raw_storage import RawStorage
from intelligence_engine.processing.cleaner import DataCleaner
from intelligence_engine.processing.classifier import BusinessClassifier
from intelligence_engine.processing.enricher import DataEnricher
from intelligence_engine.processing.embedder import EmbeddingGenerator
from intelligence_engine.storage.postgres_writer import PostgresWriter
from datetime import datetime
from uuid import UUID
import asyncio
import logging

logger = logging.getLogger(__name__)

class ProcessingPipeline:
    """Process raw collected data through cleaning, classification, enrichment, embedding, storage."""
    
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.raw_storage = RawStorage(db_pool)
        self.cleaner = DataCleaner()
        self.classifier = BusinessClassifier()
        self.enricher = DataEnricher(db_pool)
        self.embedder = EmbeddingGenerator()
        self.writer = PostgresWriter(db_pool)
    
    async def process_source(self, source_id: UUID, batch_size: int = 100):
        """Process all unprocessed raw records for a source."""
        async for batch in self.raw_storage.read_unprocessed(source_id, batch_size):
            processed = []
            
            for raw_record in batch:
                try:
                    # 1. Clean
                    cleaned = self.cleaner.clean(raw_record)
                    
                    # 2. Classify
                    classified = self.classifier.classify(cleaned)
                    
                    # 3. Enrich
                    enriched = await self.enricher.enrich(classified)
                    
                    # 4. Embed
                    embedded = await self.embedder.embed(enriched)
                    
                    # 5. Write to PostgreSQL
                    await self.writer.upsert_business(embedded)
                    
                    processed.append(raw_record['id'])
                    
                except Exception as e:
                    logger.error(f"Failed to process record {raw_record['id']}: {e}")
                    # Move to dead letter queue
                    await self.raw_storage.mark_failed(raw_record['id'], str(e))
            
            # Mark processed
            if processed:
                await self.raw_storage.mark_processed(processed)
            
            logger.info(f"Processed batch of {len(processed)} records for source {source_id}")
```

---

## Component 5: Processing Modules

### 5.1 Data Cleaner

```python
# intelligence_engine/processing/cleaner.py

import re
from datetime import datetime
from typing import Any

class DataCleaner:
    """Clean and normalize raw collected data."""
    
    def clean(self, raw_record: dict) -> dict:
        data = raw_record['raw_data'].copy()
        metadata = raw_record.get('metadata', {})
        
        # Normalize based on source type
        source_type = metadata.get('source_type', 'unknown')
        
        if source_type == 'google_maps':
            return self._clean_google_maps(data, metadata)
        elif source_type == 'companies_house':
            return self._clean_companies_house(data, metadata)
        elif source_type == 'reviews':
            return self._clean_reviews(data, metadata)
        elif source_type == 'rss':
            return self._clean_rss(data, metadata)
        elif source_type == 'website_analysis':
            return data  # Already structured
        
        return data
    
    def _clean_google_maps(self, data: dict, metadata: dict) -> dict:
        return {
            'source_record_id': data.get('place_id', ''),
            'name': self._clean_text(data.get('name', '')),
            'address': self._clean_address(data.get('address', '')),
            'phone': self._normalize_phone(data.get('phone', '')),
            'website': self._normalize_url(data.get('website', '')),
            'rating': float(data.get('rating', 0)) if data.get('rating') else None,
            'review_count': int(data.get('reviews', 0)) if data.get('reviews') else 0,
            'categories': data.get('categories', []),
            'lat': float(data.get('lat', 0)) if data.get('lat') else None,
            'lng': float(data.get('lng', 0)) if data.get('lng') else None,
            'metadata': {**metadata, 'niche': metadata.get('niche'), 'city': metadata.get('city')}
        }
    
    def _clean_companies_house(self, data: dict, metadata: dict) -> dict:
        return {
            'source_record_id': data.get('company_number', ''),
            'name': self._clean_text(data.get('company_name', '')),
            'address': self._format_ch_address(data.get('registered_office_address', {})),
            'incorporation_date': data.get('date_of_creation'),
            'company_status': data.get('company_status'),
            'company_type': data.get('type'),
            'sic_codes': [c.get('code') for c in data.get('sic_codes', [])],
            'metadata': metadata
        }
    
    def _clean_reviews(self, data: dict, metadata: dict) -> dict:
        return {
            'source_record_id': data.get('review_id', ''),
            'business_id': metadata.get('business_id'),
            'author_name': self._clean_text(data.get('author', '')),
            'rating': float(data.get('rating', 0)) if data.get('rating') else None,
            'text': self._clean_text(data.get('text', '')),
            'published_at': self._parse_date(data.get('date', '')),
            'platform': metadata.get('platform'),
            'metadata': metadata
        }
    
    def _clean_rss(self, data: dict, metadata: dict) -> dict:
        return {
            'source_record_id': data.get('source_record_id', ''),
            'title': self._clean_text(data.get('title', '')),
            'url': data.get('link', ''),
            'summary': self._clean_text(data.get('summary', '')),
            'published_at': self._parse_date(data.get('published', '')),
            'author': self._clean_text(data.get('author', '')),
            'tags': data.get('tags', []),
            'feed_url': data.get('feed_url'),
            'metadata': metadata
        }
    
    def _clean_text(self, text: str) -> str:
        if not text:
            return ''
        # Remove extra whitespace, normalize unicode
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def _clean_address(self, address: str) -> str:
        return self._clean_text(address)
    
    def _normalize_phone(self, phone: str) -> str:
        if not phone:
            return ''
        # Keep only digits, +, spaces, dashes, parentheses
        cleaned = re.sub(r'[^\d+\-\s()]', '', phone)
        return cleaned.strip()
    
    def _normalize_url(self, url: str) -> str:
        if not url:
            return ''
        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url
    
    def _format_ch_address(self, addr: dict) -> str:
        parts = [
            addr.get('address_line_1', ''),
            addr.get('address_line_2', ''),
            addr.get('locality', ''),
            addr.get('region', ''),
            addr.get('postal_code', ''),
            addr.get('country', '')
        ]
        return ', '.join(p for p in parts if p)
    
    def _parse_date(self, date_str: str) -> str | None:
        if not date_str:
            return None
        # Try multiple formats
        formats = [
            '%a, %d %b %Y %H:%M:%S %z',
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%d',
            '%d %b %Y'
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).isoformat()
            except ValueError:
                continue
        return date_str  # Return original if unparseable
```

### 5.2 Business Classifier

```python
# intelligence_engine/processing/classifier.py

from typing import Any

class BusinessClassifier:
    """Classify businesses into niches, categories, and types."""
    
    NICHE_KEYWORDS = {
        'barber': ['barber', 'barbershop', 'haircut', 'mens hair', 'gentlemen'],
        'gym': ['gym', 'fitness', 'crossfit', 'personal training', 'health club'],
        'dentist': ['dentist', 'dental', 'orthodontist', 'oral surgery'],
        'plumber': ['plumber', 'plumbing', 'heating', 'boiler', 'pipe'],
        'restaurant': ['restaurant', 'cafe', 'bistro', 'takeaway', 'food'],
        'salon': ['salon', 'hairdresser', 'beauty', 'spa', 'nails'],
        'saas': ['software', 'saas', 'platform', 'api', 'cloud', 'app'],
        'agency': ['agency', 'marketing', 'digital', 'creative', 'advertising'],
        'recruiter': ['recruitment', 'recruiter', 'staffing', 'employment', 'headhunter'],
        'trade': ['electrician', 'builder', 'carpenter', 'roofer', 'decorator'],
    }
    
    def classify(self, data: dict) -> dict:
        """Add classification to business data."""
        name = data.get('name', '').lower()
        categories = [c.lower() for c in data.get('categories', [])]
        sic_codes = data.get('sic_codes', [])
        
        # Keyword-based niche detection
        niche = self._detect_niche(name, categories)
        
        # Business type detection
        biz_type = self._detect_business_type(name, categories, sic_codes)
        
        # Size estimation
        size = self._estimate_size(data)
        
        return {
            **data,
            'primary_category': niche,
            'business_type': biz_type,
            'size_estimate': size,
            'classification_confidence': self._calculate_confidence(niche, name, categories)
        }
    
    def _detect_niche(self, name: str, categories: list[str]) -> str:
        scores = {}
        for niche, keywords in self.NICHE_KEYWORDS.items():
            score = 0
            for kw in keywords:
                if kw in name:
                    score += 3
                for cat in categories:
                    if kw in cat:
                        score += 2
            if score > 0:
                scores[niche] = score
        
        if scores:
            return max(scores, key=scores.get)
        return 'other'
    
    def _detect_business_type(self, name: str, categories: list[str], sic_codes: list) -> str:
        # SIC code mapping
        if sic_codes:
            first_sic = str(sic_codes[0])[:2]
            if first_sic in ['62', '58']:  # Computer programming, software
                return 'saas'
            elif first_sic in ['69', '70', '71', '72', '73', '74']:  # Professional services
                return 'agency'
            elif first_sic in ['86', '87', '88']:  # Health, social
                return 'healthcare'
        
        # Fallback to niche
        niche = self._detect_niche(name, categories)
        if niche in ['barber', 'salon', 'gym']:
            return 'local_service'
        elif niche in ['plumber', 'trade']:
            return 'trade'
        elif niche in ['saas', 'agency', 'recruiter']:
            return 'b2b_service'
        return 'unknown'
    
    def _estimate_size(self, data: dict) -> str:
        review_count = data.get('review_count', 0)
        if review_count > 500:
            return 'large'
        elif review_count > 100:
            return 'medium'
        elif review_count > 20:
            return 'small'
        return 'micro'
    
    def _calculate_confidence(self, niche: str, name: str, categories: list[str]) -> float:
        if niche == 'other':
            return 0.1
        # Simple heuristic
        niche_kws = self.NICHE_KEYWORDS.get(niche, [])
        matches = sum(1 for kw in niche_kws if kw in name or any(kw in c for c in categories))
        return min(0.9, 0.3 + matches * 0.15)
```

### 5.3 Data Enricher

```python
# intelligence_engine/processing/enricher.py

from intelligence_engine.storage.postgres_writer import PostgresWriter
from typing import Any
import asyncio

class DataEnricher:
    """Enrich business data with cross-references, competitors, existing leads."""
    
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.writer = PostgresWriter(db_pool)
    
    async def enrich(self, data: dict) -> dict:
        """Enrich with database lookups and computed fields."""
        enriched = data.copy()
        
        # 1. Check for existing business (deduplication)
        existing = await self._find_existing_business(enriched)
        if existing:
            enriched['existing_business_id'] = existing['business_id']
            enriched['match_confidence'] = existing['match_confidence']
        
        # 2. Find competitors (embedding similarity)
        competitors = await self._find_competitors(enriched)
        enriched['competitors'] = competitors
        
        # 3. Check for existing lead
        lead = await self._find_existing_lead(enriched)
        if lead:
            enriched['existing_lead_id'] = lead['lead_id']
            enriched['lead_status'] = lead['status']
        
        # 4. Compute opportunity signals
        enriched['opportunity_signals'] = self._compute_opportunity_signals(enriched)
        
        return enriched
    
    async def _find_existing_business(self, data: dict) -> dict | None:
        """Find existing business via website, phone, or name+address match."""
        # Check website
        website = data.get('website')
        if website:
            # query businesses where website_normalized = normalized(website)
            pass
        
        # Check phone
        phone = data.get('phone')
        if phone:
            # query businesses where phone_normalized = normalized(phone)
            pass
        
        # Fuzzy name + address
        name = data.get('name', '')
        address = data.get('address', '')
        if name and address:
            # Use vector similarity on embeddings
            pass
        
        return None
    
    async def _find_competitors(self, data: dict) -> list[dict]:
        """Find competitors via embedding similarity."""
        # Query pgvector for similar businesses in same category/location
        return []
    
    async def _find_existing_lead(self, data: dict) -> dict | None:
        """Check if lead already exists for this business."""
        return None
    
    def _compute_opportunity_signals(self, data: dict) -> dict:
        """Compute M5 gap detection signals."""
        signals = {}
        
        # Website signals
        website = data.get('website')
        signals['has_website'] = bool(website)
        signals['no_website'] = not website
        
        # From website analysis (if available)
        wa = data.get('website_analysis', {})
        if wa:
            signals['poor_mobile'] = not wa.get('is_mobile_friendly', True)
            signals['weak_seo'] = (wa.get('lighthouse_score', 100) or 100) < 70
            signals['no_booking'] = not wa.get('has_booking', False)
            signals['no_analytics'] = not wa.get('has_analytics', False)
            signals['outdated_cms'] = wa.get('cms') in ['WordPress'] and wa.get('cms_version', '').startswith('4.')
        
        # Review signals
        review_count = data.get('review_count', 0)
        rating = data.get('rating', 5)
        signals['low_reviews'] = review_count < 10
        signals['poor_rating'] = rating < 4.0
        
        # Contact signals
        signals['no_email'] = not data.get('email')
        signals['no_phone'] = not data.get('phone')
        
        return signals
```

### 5.4 Embedding Generator

```python
# intelligence_engine/processing/embedder.py

from sentence_transformers import SentenceTransformer
from typing import Any
import numpy as np

class EmbeddingGenerator:
    """Generate vector embeddings for businesses and content."""
    
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2'):
        self.model = SentenceTransformer(model_name)
        self.dimension = 384  # all-MiniLM-L6-v2 output
    
    async def embed(self, data: dict) -> dict:
        """Generate embedding for business record."""
        # Build text representation for embedding
        text_parts = []
        
        if data.get('name'):
            text_parts.append(data['name'])
        if data.get('primary_category'):
            text_parts.append(data['primary_category'])
        if data.get('address'):
            text_parts.append(data['address'])
        if data.get('website'):
            text_parts.append(data['website'].replace('https://', '').replace('http://', ''))
        if data.get('opportunity_signals'):
            signals = data['opportunity_signals']
            active_signals = [k for k, v in signals.items() if v]
            text_parts.extend(active_signals)
        
        text = ' | '.join(text_parts)
        
        # Generate embedding (run in thread pool to avoid blocking)
        import asyncio
        loop = asyncio.get_event_loop()
        embedding = await loop.run_in_executor(None, self.model.encode, text)
        
        return {
            **data,
            'embedding': embedding.tolist(),
            'embedding_model': 'all-MiniLM-L6-v2',
            'embedding_text': text[:500]  # Store truncated for debugging
        }
    
    async def embed_batch(self, records: list[dict]) -> list[dict]:
        """Generate embeddings for multiple records efficiently."""
        texts = []
        for data in records:
            text_parts = []
            if data.get('name'): text_parts.append(data['name'])
            if data.get('primary_category'): text_parts.append(data['primary_category'])
            if data.get('address'): text_parts.append(data['address'])
            texts.append(' | '.join(text_parts))
        
        import asyncio
        loop = asyncio.get_event_loop()
        embeddings = await loop.run_in_executor(None, self.model.encode, texts)
        
        for i, data in enumerate(records):
            data['embedding'] = embeddings[i].tolist()
            data['embedding_model'] = 'all-MiniLM-L6-v2'
        
        return records
```

---

## Component 6: PostgreSQL Writer

```python
# intelligence_engine/storage/postgres_writer.py

from intelligence_engine.storage.raw_storage import RawStorage
from datetime import datetime
from uuid import UUID
from typing import Any
import asyncpg
import json

class PostgresWriter:
    """Write processed data to PostgreSQL with upserts."""
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.pool = db_pool
    
    async def upsert_business(self, data: dict):
        """Upsert business with all related data."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # 1. Upsert source
                source_id = await self._ensure_source(conn, data)
                
                # 2. Upsert business
                business_id = await self._upsert_business(conn, data, source_id)
                
                # 3. Upsert business_source_ref
                await self._upsert_business_source_ref(conn, business_id, source_id, data)
                
                # 4. Upsert lead if opportunity signals present
                if data.get('opportunity_signals'):
                    await self._upsert_lead(conn, business_id, data)
                
                # 5. Upsert website if analysis present
                if data.get('website_analysis') or data.get('website'):
                    await self._upsert_website(conn, business_id, data)
                
                return business_id
    
    async def _ensure_source(self, conn, data: dict) -> UUID:
        metadata = data.get('metadata', {})
        source_name = metadata.get('source', 'unknown')
        
        row = await conn.fetchrow("""
            INSERT INTO sources (name, type, base_url, is_active)
            VALUES ($1, $2, $3, TRUE)
            ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
            RETURNING source_id
        """, source_name, metadata.get('source_type', 'scraper'), metadata.get('base_url', ''))
        
        return row['source_id']
    
    async def _upsert_business(self, conn, data: dict, source_id: UUID) -> UUID:
        source_record_id = data.get('source_record_id', '')
        
        row = await conn.fetchrow("""
            INSERT INTO businesses (
                source_id, source_business_id, name, normalized_name,
                address_line1, city, region, postal_code, country,
                lat, lng, phone, phone_normalized, email, website, website_normalized,
                primary_category, categories, sic_code, naics_code,
                rating, review_count, confidence, embedding,
                first_seen_at, last_seen_at, last_enriched_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15, $16,
                $17, $18, $19, $20,
                $21, $22, $23, $24,
                NOW(), NOW(), NOW()
            )
            ON CONFLICT (source_id, source_business_id) DO UPDATE SET
                name = EXCLUDED.name,
                normalized_name = EXCLUDED.normalized_name,
                address_line1 = EXCLUDED.address_line1,
                city = EXCLUDED.city,
                region = EXCLUDED.region,
                postal_code = EXCLUDED.postal_code,
                country = EXCLUDED.country,
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng,
                phone = EXCLUDED.phone,
                phone_normalized = EXCLUDED.phone_normalized,
                email = EXCLUDED.email,
                website = EXCLUDED.website,
                website_normalized = EXCLUDED.website_normalized,
                primary_category = EXCLUDED.primary_category,
                categories = EXCLUDED.categories,
                sic_code = EXCLUDED.sic_code,
                naics_code = EXCLUDED.naics_code,
                rating = EXCLUDED.rating,
                review_count = EXCLUDED.review_count,
                confidence = EXCLUDED.confidence,
                embedding = EXCLUDED.embedding,
                last_seen_at = NOW(),
                last_enriched_at = NOW(),
                deleted_at = NULL
            RETURNING business_id
        """,
            source_id, source_record_id,
            data.get('name'), data.get('normalized_name'),
            data.get('address'), data.get('city'), data.get('region'), data.get('postal_code'), data.get('country', 'GB'),
            data.get('lat'), data.get('lng'),
            data.get('phone'), data.get('phone_normalized'), data.get('email'), data.get('website'), data.get('website_normalized'),
            data.get('primary_category'), data.get('categories', []), data.get('sic_code'), data.get('naics_code'),
            data.get('rating'), data.get('review_count'), data.get('confidence', 0.5),
            data.get('embedding')
        )
        
        return row['business_id']
    
    async def _upsert_business_source_ref(self, conn, business_id: UUID, source_id: UUID, data: dict):
        """Upsert cross-reference between business and source."""
        await conn.execute("""
            INSERT INTO business_source_refs (business_id, source_id, source_business_id, match_confidence, match_method, is_primary)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (business_id, source_id) DO UPDATE SET
                match_confidence = EXCLUDED.match_confidence,
                match_method = EXCLUDED.match_method,
                is_primary = EXCLUDED.is_primary
        """,
            business_id, source_id,
            data.get('source_record_id', ''),
            data.get('match_confidence', 1.0),
            data.get('match_method', 'exact'),
            True
        )
    
    async def _upsert_lead(self, conn, business_id: UUID, data: dict):
        """Upsert lead from opportunity signals."""
        signals = data.get('opportunity_signals', {})
        
        # Calculate gap score
        gap_score = sum(1 for v in signals.values() if v) * 10  # Simple scoring
        
        # Determine priority
        if gap_score >= 70:
            priority = 'high'
        elif gap_score >= 40:
            priority = 'medium'
        else:
            priority = 'low'
        
        await conn.execute("""
            INSERT INTO leads (business_id, status, priority, score, score_breakdown, gaps, gap_score, source_id)
            VALUES ($1, 'new', $2, $3, $4, $5, $6, $7)
            ON CONFLICT (business_id) DO UPDATE SET
                status = CASE 
                    WHEN leads.status IN ('won', 'lost', 'dead') THEN leads.status
                    ELSE 'new'
                END,
                priority = EXCLUDED.priority,
                score = EXCLUDED.score,
                score_breakdown = EXCLUDED.score_breakdown,
                gaps = EXCLUDED.gaps,
                gap_score = EXCLUDED.gap_score,
                updated_at = NOW()
        """,
            business_id, priority, gap_score,
            json.dumps({'gap_score': gap_score}),
            json.dumps(signals),
            gap_score,
            data.get('metadata', {}).get('source_id')
        )
    
    async def _upsert_website(self, conn, business_id: UUID, data: dict):
        """Upsert website analysis."""
        url = data.get('website')
        if not url:
            return
        
        wa = data.get('website_analysis', {})
        
        await conn.execute("""
            INSERT INTO websites (business_id, url, url_normalized, cms, framework, hosting, technologies,
                lighthouse_score, psi_mobile_score, psi_desktop_score, core_web_vitals,
                is_mobile_friendly, viewport_configured, tap_targets_ok,
                has_ssl, has_sitemap, has_robots_txt, structured_data,
                a11y_score, a11y_violations, analyzed_at, analyzer_version)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, NOW(), '1.0')
            ON CONFLICT (business_id, url_normalized) DO UPDATE SET
                cms = EXCLUDED.cms,
                framework = EXCLUDED.framework,
                hosting = EXCLUDED.hosting,
                technologies = EXCLUDED.technologies,
                lighthouse_score = EXCLUDED.lighthouse_score,
                psi_mobile_score = EXCLUDED.psi_mobile_score,
                psi_desktop_score = EXCLUDED.psi_desktop_score,
                core_web_vitals = EXCLUDED.core_web_vitals,
                is_mobile_friendly = EXCLUDED.is_mobile_friendly,
                viewport_configured = EXCLUDED.viewport_configured,
                tap_targets_ok = EXCLUDED.tap_targets_ok,
                has_ssl = EXCLUDED.has_ssl,
                has_sitemap = EXCLUDED.has_sitemap,
                has_robots_txt = EXCLUDED.has_robots_txt,
                structured_data = EXCLUDED.structured_data,
                a11y_score = EXCLUDED.a11y_score,
                a11y_violations = EXCLUDED.a11y_violations,
                analyzed_at = NOW()
        """,
            business_id, url, data.get('website_normalized'),
            wa.get('cms'), wa.get('framework'), wa.get('hosting'), wa.get('technologies', []),
            wa.get('lighthouse_score'), wa.get('psi_mobile_score'), wa.get('psi_desktop_score'),
            json.dumps(wa.get('core_web_vitals', {})),
            wa.get('is_mobile_friendly'), wa.get('viewport_configured'), wa.get('tap_targets_ok'),
            wa.get('has_ssl'), wa.get('has_sitemap'), wa.get('has_robots_txt'),
            json.dumps(wa.get('structured_data', {})),
            wa.get('a11y_score'), wa.get('a11y_violations', 0)
        )
```

---

## Component 7: Raw Storage (JSONL + MinIO/S3 compatible)

```python
# intelligence_engine/storage/raw_storage.py

from datetime import datetime
from uuid import UUID
from typing import AsyncIterator
import asyncpg
import json
import asyncio

class RawStorage:
    """Store raw collection results before processing."""
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.pool = db_pool
    
    async def write(self, result: 'CollectionResult'):
        """Write collection result to raw storage."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO raw_collection_events (
                    source_id, source_record_id, raw_data, collected_at,
                    collector_version, metadata, status
                ) VALUES ($1, $2, $3, $4, $5, $6, 'pending')
                ON CONFLICT (source_id, source_record_id, collected_at) DO NOTHING
            """,
                result.source_id,
                result.source_record_id,
                json.dumps(result.raw_data),
                result.collected_at,
                result.collector_version,
                json.dumps(result.metadata)
            )
    
    async def read_unprocessed(self, source_id: UUID, batch_size: int = 100) -> AsyncIterator[list[dict]]:
        """Read unprocessed records in batches."""
        async with self.pool.acquire() as conn:
            while True:
                rows = await conn.fetch("""
                    SELECT id, source_id, source_record_id, raw_data, collected_at,
                           collector_version, metadata
                    FROM raw_collection_events
                    WHERE source_id = $1 AND status = 'pending'
                    ORDER BY collected_at ASC
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                """, source_id, batch_size)
                
                if not rows:
                    break
                
                yield [dict(r) for r in rows]
    
    async def mark_processed(self, ids: list[int]):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE raw_collection_events
                SET status = 'processed', processed_at = NOW()
                WHERE id = ANY($1)
            """, ids)
    
    async def mark_failed(self, id: int, error: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE raw_collection_events
                SET status = 'failed', error = $1, failed_at = NOW()
                WHERE id = $2
            """, error, id)
```

---

## Database Schema Additions (Raw Storage)

```sql
-- Raw collection events table (staging)
CREATE TABLE raw_collection_events (
    id                 BIGSERIAL PRIMARY KEY,
    source_id          UUID NOT NULL REFERENCES sources(source_id),
    source_record_id   VARCHAR(200) NOT NULL,
    raw_data           JSONB NOT NULL,
    collected_at       TIMESTAMPTZ NOT NULL,
    collector_version  VARCHAR(20) NOT NULL,
    metadata           JSONB DEFAULT '{}',
    status             VARCHAR(20) NOT NULL DEFAULT 'pending',  -- 'pending', 'processed', 'failed'
    error              TEXT,
    processed_at       TIMESTAMPTZ,
    failed_at          TIMESTAMPTZ,
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE (source_id, source_record_id, collected_at)
);

CREATE INDEX idx_rce_source_status ON raw_collection_events(source_id, status, collected_at);
CREATE INDEX idx_rce_pending ON raw_collection_events(source_id, collected_at) WHERE status = 'pending';
```

---

## Deployment & Operations

### Docker Compose (Local Dev)

```yaml
# docker-compose.intelligence.yml
version: '3.8'
services:
  postgres:
    image: pgvector/pgvector:pg16
    environment:
      POSTGRES_DB: intelligence
      POSTGRES_USER: intelligence
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./migrations:/docker-entrypoint-initdb.d
    ports:
      - "5432:5432"
  
  changedetection:
    image: dgtlmoon/changedetection.io:latest
    environment:
      - PLAYWRIGHT_DRIVER_URL=ws://playwright:3000
    volumes:
      - changedetection_data:/datastore
    ports:
      - "5000:5000"
    depends_on:
      - playwright
  
  playwright:
    image: mcr.microsoft.com/playwright:v1.40.0-focal
    command: ["npx", "playwright", "run-server", "--port", "3000"]
    ports:
      - "3000:3000"
  
  uptime-kuma:
    image: louislam/uptime-kuma:1
    volumes:
      - uptime_kuma_data:/app/data
    ports:
      - "3001:3001"
  
  ntfy:
    image: binwiederhier/ntfy:latest
    volumes:
      - ntfy_data:/var/cache/ntfy
      - ntfy_config:/etc/ntfy
    ports:
      - "3002:80"
  
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

volumes:
  postgres_data:
  changedetection_data:
  uptime_kuma_data:
  ntfy_data:
  ntfy_config:
```

### Environment Variables

```bash
# .env.intelligence
POSTGRES_PASSWORD=changeme
DATABASE_URL=postgresql://intelligence:changeme@localhost:5432/intelligence
GOOGLE_MAPS_API_KEY=your_key
COMPANIES_HOUSE_API_KEY=your_key
HUNTER_IO_API_KEY=your_key
PAGESPEED_API_KEY=your_key
FIRECRAWL_API_KEY=your_key
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email
SMTP_PASS=your_app_password
FROM_NAME=Zen Intelligence
```

---

## M3 Implementation Plan

| Phase | Task | Deliverable |
|-------|------|-------------|
| **M3a** | Provision PostgreSQL + pgvector, run M2 migrations | Running DB with schema |
| **M3b** | Deploy monitoring stack (changedetection.io, Uptime Kuma, ntfy) | Running monitoring services |
| **M3c** | Implement base collector classes, raw storage, processing pipeline | Core framework |
| **M3d** | Implement Google Maps collector (Playwright) | Working collector |
| **M3e** | Implement Official Registry collectors (Companies House, ABN, SEC) | Working collectors |
| **M3f** | Implement Review collectors (Google, Trustpilot, Yelp) | Working collectors |
| **M3g** | Implement Industry Feed collector (RSS, HN, GitHub, Product Hunt) | Working collector |
| **M3h** | Implement Website Analysis collector (Playwright + Lighthouse + PSI) | Working collector |
| **M3i** | Implement Processing Pipeline (clean → classify → enrich → embed → write) | End-to-end pipeline |
| **M3j** | Register Hermes cron jobs + APScheduler integration | Scheduled collection |
| **M3k** | Integration testing, monitoring, alerting | Verified pipeline |

---

## M3 Checkpoint: PENDING REVIEW

**Review Required:**
1. Collector coverage — all M4–M6 sources addressed?
2. Rate limiting strategy — respectful, sustainable?
3. Error handling — dead letter queue, retry logic sufficient?
4. Processing pipeline — idempotent, exactly-once semantics?
5. Schema alignment — writes match M2 schema exactly?
6. Monitoring — observability built in from start?
7. Deployment — Docker Compose sufficient for MVP?

**Next:** M4 — Lead Generation Engine (build on this collection infrastructure)