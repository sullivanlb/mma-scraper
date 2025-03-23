import json
import asyncio
import os
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

with open('./schemas/schema_events.json', 'r') as file:
    schema_events = json.load(file)
    
async def extract_event_details(url):
    extraction_strategy = JsonCssExtractionStrategy(schema_events, verbose=True)
    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        extraction_strategy=extraction_strategy,
        excluded_tags=["script", "style"]
    )

    event_data = []

    async with AsyncWebCrawler(verbose=True) as crawler:
        print(f"url: {url}")
        result = await crawler.arun(url=url, config=config)

        if not result.success:
            print(f"Crawl failed for {url}: {result.error_message}")
            return

        data = json.loads(result.extracted_content)
        if data:
            event_data.append(data[0])
            print(f"Extracted data for {url}")
            return event_data

    return event_data

async def main():
    url = "https://www.tapology.com/fightcenter/events/122989-ufc-314"

    event_details = await extract_event_details(url)

    print("Event extracted :")
    print(event_details)

if __name__ == "__main__":
    asyncio.run(main())