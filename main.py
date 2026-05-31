Advanced Web Scraper with async support, rate limiting, and data pipeline.
"""
 
import asyncio
import aiohttp
import json
import csv
import logging
import time
from dataclasses import dataclass, field, asdict
from typing import Optional, Callable, Any
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
 
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log"),
    ],
)
logger = logging.getLogger("scraper")
 
 
@dataclass
class ScraperConfig:
    base_url: str
    max_pages: int = 10
    concurrency: int = 5
    delay_seconds: float = 1.0
    timeout_seconds: int = 30
    retries: int = 3
    user_agent: str = (
        "Mozilla/5.0 (compatible; AdvancedScraper/1.0; +https://github.com/yourusername)"
    )
    output_format: str = "json"  # "json" | "csv"
    output_dir: str = "output"
 
 
@dataclass
class PageResult:
    url: str
    status_code: int
    title: str = ""
    links: list[str] = field(default_factory=list)
    data: dict[str, Any] = field(default_factory=dict)
    scraped_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    error: Optional[str] = None
 
 
class RateLimiter:
    """Token-bucket rate limiter for async requests."""
 
    def __init__(self, rate: float):
        self.rate = rate
        self._tokens = rate
        self._last_check = time.monotonic()
        self._lock = asyncio.Lock()
 
    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_check
            self._tokens = min(self.rate, self._tokens + elapsed * self.rate)
            self._last_check = now
            if self._tokens < 1:
                sleep_time = (1 - self._tokens) / self.rate
                await asyncio.sleep(sleep_time)
                self._tokens = 0
            else:
                self._tokens -= 1
 
 
class DataPipeline:
    """Process, validate, and export scraped data."""
 
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.results: list[PageResult] = []
        Path(config.output_dir).mkdir(parents=True, exist_ok=True)
 
    def add(self, result: PageResult):
        self.results.append(result)
        logger.info(f"[Pipeline] Added: {result.url} | status={result.status_code}")
 
    def export(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if self.config.output_format == "json":
            self._export_json(timestamp)
        elif self.config.output_format == "csv":
            self._export_csv(timestamp)
        logger.info(f"[Pipeline] Exported {len(self.results)} records.")
 
    def _export_json(self, timestamp: str):
        path = Path(self.config.output_dir) / f"results_{timestamp}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump([asdict(r) for r in self.results], f, indent=2, ensure_ascii=False)
        logger.info(f"[Pipeline] JSON saved → {path}")
 
    def _export_csv(self, timestamp: str):
        path = Path(self.config.output_dir) / f"results_{timestamp}.csv"
        if not self.results:
            return
        fieldnames = ["url", "status_code", "title", "scraped_at", "error"]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in self.results:
                writer.writerow({k: getattr(r, k) for k in fieldnames})
        logger.info(f"[Pipeline] CSV saved → {path}")
 
 
class AsyncWebScraper:
    """
    High-performance async web scraper with:
    - Concurrency control via semaphore
    - Rate limiting (token bucket)
    - Automatic retries with exponential backoff
    - Pluggable page parser callbacks
    - Data pipeline for export


    def __init__(
        self,
        config: ScraperConfig,
        page_parser: Optional[Callable[[str, BeautifulSoup], dict]] = None,
    ):
        self.config = config
        self.page_parser = page_parser or self._default_parser
        self.pipeline = DataPipeline(config)
        self._semaphore = asyncio.Semaphore(config.concurrency)
        self._rate_limiter = RateLimiter(1.0 / config.delay_seconds)
        self._visited: set[str] = set()
 
    @staticmethod
    def _default_parser(url: str, soup: BeautifulSoup) -> dict:
       
        headings = [h.get_text(strip=True) for h in soup.find_all(["h1", "h2", "h3"])]
        paragraphs = [p.get_text(strip=True) for p in soup.find_all("p")][:5]
        return {"headings": headings, "paragraphs": paragraphs}
 
    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        links = []
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            full = urljoin(base_url, href)
            parsed = urlparse(full)
            if parsed.scheme in ("http", "https") and parsed.netloc == urlparse(base_url).netloc:
                links.append(full)
        return list(set(links))
 
    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> tuple[int, str]:
        headers = {"User-Agent": self.config.user_agent}
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        for attempt in range(1, self.config.retries + 1):
            try:
                await self._rate_limiter.acquire()
                async with session.get(url, headers=headers, timeout=timeout) as resp:
                    text = await resp.text(errors="replace")
                    return resp.status, text
            except Exception as exc:
                wait = 2 ** attempt
                logger.warning(f"Attempt {attempt}/{self.config.retries} failed for {url}: {exc}. Retrying in {wait}s...")
                await asyncio.sleep(wait)
        raise RuntimeError(f"All {self.config.retries} attempts failed for {url}")
 
    async def _scrape_page(self, session: aiohttp.ClientSession, url: str) -> PageResult:
        async with self._semaphore:
            try:
                status, html = await self._fetch(session, url)
                soup = BeautifulSoup(html, "html.parser")
                title = soup.title.string.strip() if soup.title else ""
                links = self._extract_links(soup, url)
                data = self.page_parser(url, soup)
                return PageResult(url=url, status_code=status, title=title, links=links, data=data)
            except Exception as exc:
                logger.error(f"Error scraping {url}: {exc}")
                return PageResult(url=url, status_code=0, error=str(exc))
 
    async def run(self) -> list[PageResult]:
        queue = asyncio.Queue()
        await queue.put(self.config.base_url)
 
        connector = aiohttp.TCPConnector(limit=self.config.concurrency)
        async with aiohttp.ClientSession(connector=connector) as session:
            while not queue.empty() and len(self._visited) < self.config.max_pages:
                batch = []
                while not queue.empty() and len(self._visited) + len(batch) < self.config.max_pages:
                    url = await queue.get()
                    if url not in self._visited:
                        self._visited.add(url)
                        batch.append(url)
 
                tasks = [self._scrape_page(session, u) for u in batch]
                results = await asyncio.gather(*tasks)
 
                for result in results:
                    self.pipeline.add(result)
                    for link in result.links:
                        if link not in self._visited:
                            await queue.put(link)
 
        self.pipeline.export()
        return self.pipeline.results
 
 
def run_scraper(base_url: str, **kwargs):
    config = ScraperConfig(base_url=base_url, **kwargs)
    scraper = AsyncWebScraper(config)
    return asyncio.run(scraper.run())
 
 
if __name__ == "__main__":
    results = run_scraper(
        base_url="https://example.com",
        max_pages=5,
        concurrency=3,
        delay_seconds=1.5,
        output_format="json",
    )
    print(f"\n✅ Scraped {len(results)} pages.")
 