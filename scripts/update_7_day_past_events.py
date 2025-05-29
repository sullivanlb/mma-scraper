import json
import asyncio
import os
import re
import hashlib
import logging
import traceback
from typing import List, Dict, Optional
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from urllib.parse import urljoin
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.tapology.com"

# Load schemas with error handling
def load_schema(filename: str) -> dict:
    try:
        with open(filename, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        logger.error(f"Schema file {filename} not found")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {filename}: {e}")
        raise

schema_events_urls = load_schema('./schemas/schema_events_urls.json')
schema_events = load_schema('./schemas/schema_events.json')
schema_profiles = load_schema('./schemas/schema_profiles.json')

# Configure retry policy for network operations
RETRY_POLICY = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((TimeoutError, ConnectionError)),
    reraise=True
)

async def extract(url: str, schema: dict) -> Optional[List[Dict]]:
    """Extract data from a URL with error handling and retries."""
    @RETRY_POLICY
    async def _extract():
        extraction_strategy = JsonCssExtractionStrategy(schema, verbose=True)
        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=extraction_strategy,
            excluded_tags=["script", "style"]
        )

        try:
            async with AsyncWebCrawler(verbose=True) as crawler:
                logger.info(f"Extracting from {url}")
                result = await crawler.arun(url=url, config=config)

                if not result.success:
                    logger.error(f"Crawl failed for {url}: {result.error_message}")
                    return None

                try:
                    data = json.loads(result.extracted_content)
                    return data if data else None
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error for {url}: {e}")
                    return None

        except Exception as e:
            logger.error(f"Error during extraction from {url}: {str(e)}")
            return None

    try:
        return await _extract()
    except Exception as e:
        logger.error(f"Failed after retries for {url}: {str(e)}")
        return None

def sanitize_int(value, default=0):
    """Convert value to integer, replacing '-' with 0."""
    return default if value == '-' else int(value)

def sanitize_fight(fight):
    """Clean and transform fight data."""
    if fight.get("details") == "Cancelled Bout":
        fight["result"] = "Cancelled"
        fight["finishBy"] = None
    elif not fight.get("result"):
        fight["result"] = "Unknown"
        fight["finishBy"] = None
    return fight

def parse_listing_date(date_str: str) -> datetime:
    """Parse all date formats found in Tapology listings into a UTC datetime."""
    # Normalize whitespace and remove extra commas
    clean_date = re.sub(r'\s{2,}', ' ', date_str.strip()).replace(',', '').strip()
    
    # Define all possible date formats in order of specificity
    formats = [
        # Formats including year and full month name
        '%B %d %Y %I%p ET',              # "September 28 2024 6pm ET"
        '%B %d %Y %I%p',                 # "September 28 2024 6pm"
        '%B %d %Y %H:%M',                # "September 28 2024 18:00"
        '%B %d %Y',                      # "September 28 2024"
        '%a %B %d %Y %I%p ET',           # "Sat September 28 2024 6pm ET"
        '%a %B %d %Y %I%M%p ET',         # "Sat September 28 2024 6:30pm ET"
        
        # Formats including year with abbreviated month
        '%a %b %d %Y %I%p ET',          # "Sat Sep 28 2024 6pm ET"
        '%a %b %d %Y %I%M%p ET',        # "Sat Sep 28 2024 6:30pm ET"
        '%b %d %Y %I%p ET',             # "Sep 28 2024 6pm ET"
        '%b %d %Y %I%p',                # "Sep 28 2024 6pm"
        '%b %d %Y %H:%M',               # "Sep 28 2024 18:00"
        
        # Formats without year (require appending current year)
        '%a %b %d %I%p ET',             # "Sat Sep 28 6pm ET"
        '%a %b %d %I%M%p ET',           # "Sat Sep 28 6:30pm ET"
        '%b %d %I%p ET',                # "Sep 28 6pm ET"
        '%b %d %I%p',                   # "Sep 28 6pm"
        '%b %d %H:%M',                  # "Sep 28 18:00"
    ]
    
    current_year = datetime.now().year
    us_eastern = pytz.timezone('US/Eastern')
    
    # First, attempt to parse formats that include the year
    for fmt in formats:
        try:
            dt = datetime.strptime(clean_date, fmt)
            localized_dt = us_eastern.localize(dt) if '%Y' in fmt else us_eastern.localize(dt.replace(year=current_year))
            return localized_dt.astimezone(pytz.UTC)
        except ValueError:
            continue
    
    # Fallback: try appending current_year to formats without year
    formats_without_year = [
        '%a %b %d %I%p ET',
        '%a %b %d %I%M%p ET',
        '%b %d %I%p ET',
        '%b %d %I%p',
        '%b %d %H:%M',
    ]
    for fmt in formats_without_year:
        try:
            dt = datetime.strptime(f"{clean_date} {current_year}", f"{fmt} %Y")
            localized_dt = us_eastern.localize(dt)
            return localized_dt.astimezone(pytz.UTC)
        except ValueError:
            continue
    
    logger.warning(f"Failed to parse date: {date_str}")
    return None

def format_date(date_to_format):
    """Robust date formatting with sanitization and multiple format support"""
    if not date_to_format or date_to_format == "N/A":
        return None

    # Clean the input string
    cleaned_date = re.sub(
        r"(\n.*|inUFC|ET|PT|CT|MT|UTC|GMT| at )",  # Remove unwanted phrases
        "", 
        date_to_format.strip()
    ).strip()

    # Define all possible date formats
    formats = [
        "%B %d, %Y %I:%M %p",    # December 16, 2023 6:00 PM
        "%B %d, %Y",             # December 16, 2023
        "%A %m.%d.%Y %I:%M %p",  # Saturday 12.16.2023 6:00 PM
        "%Y-%m-%dT%H:%M:%S%z"    # ISO 8601 format
    ]

    # Try parsing with different formats
    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned_date, fmt)
            # Convert to UTC timezone
            return dt.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S%z")
        except ValueError:
            continue

    logger.warning(f"Failed to parse date: {date_to_format}")
    return None  # or raise exception if preferred

async def process_event_url(event_url: str):
    """Update existing events with safety checks"""
    try:
        # 1. Check existing event
        existing_event = supabase.table('events')\
            .select('id,hash')\
            .eq('tapology_url', event_url)\
            .execute().data
        
        if not existing_event:
            logger.warning(f"Event not in DB: {event_url}")
            return

        # 2. Extract event details
        event_details = await extract(event_url, schema_events)
        if not event_details or not isinstance(event_details, list):
            logger.error(f"Invalid event data for {event_url}")
            return

        # 3. Validate header data
        try:
            header = event_details[0]['Header'][0]
        except (KeyError, IndexError) as e:
            logger.error(f"Missing header data in {event_url}: {str(e)}")
            return

        # 4. Generate hash safely
        event_json = json.dumps(event_details, sort_keys=True, ensure_ascii=False)
        current_hash = hashlib.sha256(event_json.encode('utf-8')).hexdigest()

        # 5. Skip if no changes
        if existing_event[0].get('hash') == current_hash:
            logger.info(f"Event header unchanged, checking fight updates")
            await process_fight_updates(event_details, existing_event[0]['id'])
            return

        # 6. Update event with validation
        update_data = {
            'hash': current_hash,
            'mma_bouts': header.get('mma_bouts', 0)
        }

        if 'datetime' in header:
            update_data['datetime'] = format_date(header['datetime'])

        supabase.table('events')\
            .update(update_data)\
            .eq('id', existing_event[0]['id'])\
            .execute()

        # 7. Update fights
        await process_fight_updates(event_details, existing_event[0]['id'])

    except Exception as e:
        logger.error(f"Event update failed for {event_url}: {str(e)}\n{traceback.format_exc()}")

async def extract_event_urls(base_url: str) -> List[str]:
    """Extract event URLs from the last 7 days"""
    event_urls = []
    page = 1
    cutoff_date = datetime.now(pytz.UTC) - timedelta(days=7)
    
    while True:
        url = f"{base_url}?page={page}"
        logger.info(f"Processing page {page}")
        
        # Extract data WITH EVENT DATES from listing page
        data = await extract(url, schema_events_urls)  # Modified schema
        
        if not data or not data[0]["URLs"]:
            break
            
        # Process events in reverse chronological order
        for event in data[0]["URLs"]:
            event_date = parse_listing_date(event['date'])  # NEW: Date from listing page
            event_url = urljoin(BASE_URL, event['url'])

            if event_date is None:
                logger.warning(f"Skipping event with unparseable date: {event['date']} ({event_url})")
                continue

            if event_date < cutoff_date:
                logger.info("Reached events older than 7 days, stopping pagination")
                return event_urls

            if event_date >= cutoff_date:
                event_urls.append(event_url)
                
        page += 1
        
    return event_urls

async def process_fight_updates(event_details: dict, event_id: int):
    """Update fight outcomes and fighter records"""
    for fight in event_details[0]["Fight Card"]:
        try:
            # Get full fighter URLs
            fighter1_url = fight.get('url_fighter_1', '')
            fighter2_url = fight.get('url_fighter_2', '')

            # Get fighter IDs from database
            fighter1 = supabase.table('fighters')\
                .select('id')\
                .eq('tapology_url', fighter1_url)\
                .execute().data
            fighter2 = supabase.table('fighters')\
                .select('id')\
                .eq('tapology_url', fighter2_url)\
                .execute().data

            if not fighter1 or not fighter2:
                print('Fighter not found')
                continue

            # Find the specific fight
            fight_record = supabase.table('fights')\
                .select('id')\
                .eq('id_event', event_id)\
                .eq('id_fighter_1', fighter1[0]['id'])\
                .eq('id_fighter_2', fighter2[0]['id'])\
                .execute().data

            if not fight_record:
                print('No fight found in db', event_id, fighter1[0]['id'], fighter2[0]['id'])
                continue

            # Update only the specific fight
            supabase.table('fights')\
                .update({
                    'result_fighter_1': fight.get('result_fighter_1'),
                    'result_fighter_2': fight.get('result_fighter_2'),
                    'finish_by': fight.get('finish_by'),
                    'finish_by_details': fight.get('finish_by_details'),
                    'rounds': fight.get('rounds')
                })\
                .eq('id', fight_record[0]['id'])\
                .execute()
            
            
            fighter1_url = urljoin(BASE_URL, fighter1_url)
            fighter2_url = urljoin(BASE_URL, fighter2_url)

            # Update fighter records
            for fighter_url in [fighter1_url, fighter2_url]:
                await update_fighter_record(fighter_url)
                
        except Exception as e:
            logger.error(f"Fight update failed: {str(e)}")

async def update_fighter_record(fighter_url: str):
    """Update fighter's MMA record and stats"""
    fighter_data = await extract(fighter_url, schema_profiles)
    if not fighter_data:
        return
        
    current_record = fighter_data[0]['Basic Infos'][0]['pro_mma_record']
    
    supabase.table('fighters')\
        .update({
            'pro_mma_record': current_record,
            'last_fight_date': format_date(fighter_data[0]['Basic Infos'][0]['last_fight_date'])
        })\
        .eq('tapology_url', fighter_url)\
        .execute()

async def main():
    """Main function to update past 7 days' events"""
    try:
        # Initialize with UFC events listing URL
        url = "https://www.tapology.com/fightcenter/promotions/1-ultimate-fighting-championship-ufc"
        
        # Get event URLs from last 7 days
        event_urls = await extract_event_urls(url)
        logger.info(f"Found {len(event_urls)} events to update")
        
        # Process events concurrently with concurrency limit
        semaphore = asyncio.Semaphore(5)  # 5 concurrent updates
        
        async def limited_processing(url):
            async with semaphore:
                await process_event_url(url)
        
        tasks = [limited_processing(url) for url in event_urls]
        await asyncio.gather(*tasks)
        
        logger.info(f"Completed updates for {len(event_urls)} events")
        
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    load_dotenv()
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    
    # Configure async event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()