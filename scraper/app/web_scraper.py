# =================================================================
# scraper/web_scraper.py - Simple web scraping
# =================================================================

import json
from typing import Optional, List, Dict
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(self, config):
        self.config = config
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def extract_data(self, url: str, schema: dict) -> Optional[List[Dict]]:
        """Extract data from URL using schema"""
        try:
            extraction_strategy = JsonCssExtractionStrategy(schema, verbose=True)
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                extraction_strategy=extraction_strategy,
                excluded_tags=["script", "style"]
            )

            async with AsyncWebCrawler(verbose=True) as crawler:
                logger.info(f"🔍 Scraping {url}")
                result = await crawler.arun(url=url, config=config)

                if not result.success:
                    logger.error(f"❌ Failed to scrape {url}: {result.error_message}")
                    return None

                data = json.loads(result.extracted_content)
                return data if data else None

        except Exception as e:
            logger.error(f"❌ Error scraping {url}: {str(e)}")
            return None
