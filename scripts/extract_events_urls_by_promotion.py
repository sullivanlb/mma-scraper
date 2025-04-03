import json
import asyncio
import os
import re
import hashlib
import logging
from typing import List, Dict, Optional
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from urllib.parse import urljoin
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
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

def format_date(date_to_format):
    if not date_to_format or date_to_format == "N/A":
        return ""

    # Remove the timezone abbreviation for parsing
    input_str_without_tz = date_to_format.replace(" ET", "")

    # Parse the datetime
    dt = datetime.strptime(input_str_without_tz, "%A %m.%d.%Y at %I:%M %p")

    # Map the timezone abbreviation to a valid IANA timezone
    timezone_mapping = {
        "ET": "America/New_York",  # Eastern Time
        "CT": "America/Chicago",  # Central Time
        "MT": "America/Denver",  # Mountain Time
        "PT": "America/Los_Angeles",  # Pacific Time
        "AKT": "America/Anchorage",  # Alaska Time
        "HT": "Pacific/Honolulu",  # Hawaii-Aleutian Time
        "GMT": "Europe/London",  # Greenwich Mean Time
        "UTC": "UTC",  # Coordinated Universal Time
        "CET": "Europe/Berlin",  # Central European Time
        "EET": "Europe/Athens",  # Eastern European Time
        "AEST": "Australia/Sydney",  # Australian Eastern Standard Time
        "IST": "Asia/Kolkata",  # Indian Standard Time
        "JST": "Asia/Tokyo",  # Japan Standard Time
        "CST": "Asia/Shanghai",  # China Standard Time
        "KST": "Asia/Seoul",  # Korea Standard Time
        "BRT": "America/Sao_Paulo",  # Brasília Time
        "ART": "America/Argentina/Buenos_Aires",  # Argentina Time
        "NZST": "Pacific/Auckland",  # New Zealand Standard Time
        "SAST": "Africa/Johannesburg",  # South Africa Standard Time
        "MSK": "Europe/Moscow",  # Moscow Standard Time
    }

    # Get the timezone from the mapping
    tz_abbreviation = "ET"  # Extract this dynamically if needed
    tz = pytz.timezone(timezone_mapping.get(tz_abbreviation, "UTC"))  # Default to UTC if not found

    # Localize the datetime to the specified timezone
    dt_with_tz = tz.localize(dt)

    # Format the datetime for PostgreSQL
    # Use the offset in the format ±HH:MM
    formatted_timestamp = dt_with_tz.strftime("%Y-%m-%d %H:%M:%S%z")  # Output: 2025-05-31 18:00:00-0400

    # Fix the offset format for PostgreSQL (replace -0400 with -04:00)
    return formatted_timestamp[:-2] + ":" + formatted_timestamp[-2:]

async def insert_fighter(fighter_details: List[Dict], tapology_url: str, small_img_url: str) -> bool|int:
    """Process and insert fighter data with validation."""
    try:
        if not fighter_details or not isinstance(fighter_details, list):
            logger.error("Invalid fighter details format")
            return False

        fighter_info = fighter_details[0]['Basic Infos'][0]
        if not fighter_info:
            logger.error("Missing Basic Infos in fighter details")
            return False

        required_fields = ['name', 'pro_mma_record', 'weight_class']
        for field in required_fields:
            if field not in fighter_info:
                logger.error(f"Missing required field {field} in fighter info")
                return False
        
        total_fights = len([fight for fight in fighter_details[0]["Fights"] if fight["result"] != ""])

        # Use regex to extract the date part
        match = re.search(r"[A-Za-z]+ \d{1,2}, \d{4}", fighter_info['last_fight_date'])
        if match:
            date_str_cleaned = match.group(0)
            last_fight_date = datetime.strptime(date_str_cleaned, "%B %d, %Y").isoformat()
        else:
            print("No date found in the string.")

        unique_data = (
            f"{fighter_info['pro_mma_record']}{fighter_info['weight_class']}"
            f"{fighter_info['affiliation']}{last_fight_date}{total_fights}"
        )
        profile_hash = hashlib.sha256(unique_data.encode()).hexdigest()

        fighter_data = {
            'name': fighter_info['name'],
            'nickname': fighter_info['nickname'],
            'age': fighter_info['age'],
            'date_of_birth': None if fighter_info['date_of_birth'] == 'N/A' else fighter_info['date_of_birth'],
            'height': fighter_info.get('height', ''),
            'weight_class': fighter_info.get('weight_class', ''),
            'last_weight_in': fighter_info.get('last_weight_in', ''),
            'born': fighter_info.get('born', ''),
            'head_coach': fighter_info.get('head_coach', ''),
            'other_coaches': fighter_info.get('other_coaches', ''),
            'pro_mma_record': fighter_info.get('pro_mma_record', ''),
            'current_mma_streak': fighter_info.get('current_mma_streak', ''),
            'affiliation': fighter_info.get('affiliation', ''),
            'profile_img_url': fighter_details[0].get('profile_img_url', ''),
            'small_img_url': small_img_url,
            'last_fight_date': last_fight_date,
            'tapology_url': tapology_url,
            'total_fights': total_fights,
            'profile_hash': profile_hash,
        }

        new_fighter_id = await insert_data("fighters", [fighter_data])
            
        if new_fighter_id == False:
            logger.error('Error while inserting fighter')
            return False

        # Insert fighter records
        for record_by_promotion in fighter_details[0]['Records']:
            await insert_data("records_by_promotion", [{
                'id_fighter': new_fighter_id,
                'promotion': record_by_promotion.get('promotion', ''),
                'broadcast': record_by_promotion.get('broadcast', ''),
                'from': record_by_promotion.get('from', ''),
                'to': record_by_promotion.get('to', ''),
                'win': sanitize_int(record_by_promotion.get('win', 0)),
                'loss': sanitize_int(record_by_promotion.get('loss', 0)),
                'draw': sanitize_int(record_by_promotion.get('draw', 0)),
                'no_contest': sanitize_int(record_by_promotion.get('noContest', 0)),
                'win_ko': sanitize_int(record_by_promotion.get('winKo', 0)),
                'win_sub': sanitize_int(record_by_promotion.get('winSub', 0)),
                'win_decision': sanitize_int(record_by_promotion.get('winDecision', 0)),
                'win_dq': sanitize_int(record_by_promotion.get('winDq', 0)),
                'loss_ko': sanitize_int(record_by_promotion.get('lossKo', 0)),
                'loss_sub': sanitize_int(record_by_promotion.get('lossSub', 0)),
                'loss_decision': sanitize_int(record_by_promotion.get('lossDecision', 0)),
                'loss_dq': sanitize_int(record_by_promotion.get('lossDq', 0)),
            }])

        return new_fighter_id


    except KeyError as e:
        logger.error(f"Missing key in fighter data: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error processing fighter data: {str(e)}")
        return False

async def insert_fight(id_event: int, id_fighter_1: int, id_fighter_2: int, fight_details: List[Dict]) -> bool|int:
    # Insert fights
    fight = sanitize_fight(fight_details)

    print("### Insert Fight :")
    print(fight)
    print("### End Insert")

    fight_type = 0
    match fight.get('fight_type'):
        case "Main Event":
            fight_type = 1
        case "Co-Main":
            fight_type = 2
        case "Main Card":
            fight_type = 3
        case "Prelim":
            fight_type = 4




    new_fight_id = await insert_data("fights", [{
        'fight_type'        : fight_type,
        'id_fighter_1'      : id_fighter_1,
        'id_fighter_2'      : id_fighter_2,
        'result_fighter_1'  : fight.get('result_fighter_1', ''),
        'result_fighter_2'  : fight.get('result_fighter_2', ''),
        'finish_by'         : fight.get('finish_by', ''),
        'finish_by_details' : fight.get('finish_by_details', ''),
        'rounds'            : fight.get('rounds', ''),
        'minutes_per_round' : fight.get('minutes_per_round', ''),
        'id_event'          : id_event,
    }])

    if new_fight_id == False:
        logger.error('Error while inserting fight')
        return False

    return new_fight_id

@RETRY_POLICY
async def insert_data(table: str, data: List[Dict]) -> Optional[str]:
    """Insert data into Supabase with transaction support."""
    try:
        response = supabase.table(table).insert(data).execute()

        if not response.data:
                return None
                
        return response.data[0]['id']
    except Exception as e:
        logger.error(f"Database operation failed: {str(e)}")
        return False

async def process_event_url(event_url: str):
    """Process individual event URL with error handling."""
    try:
        logger.info(f"Processing event: {event_url}")
        event_details = await extract(event_url, schema_events)
        if not event_details:
            print("No event details data")
            return
        
        event_header = event_details[0]['Header'][0]

        if not event_header:
            print("No event header data")
            return
        
        datetime = format_date(event_header['datetime'])
        
        # Insert event
        event_data = {
            'name': event_details[0]['name'],
            'datetime': datetime,
            'promotion': event_header.get('promotion', ''),
            'venue': event_header.get('venue', ''),
            'location': event_header.get('location', ''),
            'mma_bouts': event_header.get('mma_bouts', 0),
            'img_url': event_header.get('img_url', ''),
        }

        new_event_id = await insert_data("events", [event_data])

        for fight in event_details[0]["Fight Card"]:
            try:
                # Insert Fighter 1
                if fight["url_fighter_1"]:
                    fighter_url = urljoin(BASE_URL, fight["url_fighter_1"])
                    fighter_details = await extract(fighter_url, schema_profiles)
                    
                    if fighter_details:
                        new_id_fighter_1 = await insert_fighter(fighter_details, fight["url_fighter_1"], fight["img_fighter_1"])
                        if new_id_fighter_1:
                            logger.info(f"Processed fighter: {fight['name_fighter_1']}")
                        else:
                            logger.warning(f"Failed to insert fighter: {fight['name_fighter_1']}")

                # Insert Fighter 2
                if fight["url_fighter_2"]:
                    fighter_url = urljoin(BASE_URL, fight["url_fighter_2"])
                    fighter_details = await extract(fighter_url, schema_profiles)
                    
                    if fighter_details:
                        new_id_fighter_2 = await insert_fighter(fighter_details, fight["url_fighter_2"], fight["img_fighter_2"])
                        if new_id_fighter_2:
                            logger.info(f"Processed fighter: {fight['name_fighter_2']}")
                        else:
                            logger.warning(f"Failed to insert fighter: {fight['name_fighter_2']}")

                # Insert Fight
                if new_id_fighter_1 and new_id_fighter_2:
                    await insert_fight(new_event_id, new_id_fighter_1, new_id_fighter_2, fight)

                
            except Exception as e:
                logger.error(f"Error processing fighter {fight['name_fighter_1']} or {fight['name_fighter_2']}: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"Error processing event {event_url}: {str(e)}")

async def extract_event_urls(base_url: str) -> List[str]:
    """Extract paginated event URLs with error handling."""
    event_urls = []
    page = 1
    max_retries = 3

    while True:
        url = f"{base_url}?page={page}"
        logger.info(f"Processing page {page}")

        for attempt in range(max_retries):
            try:
                data = await extract(url, schema_events_urls)
                if not data:
                    logger.error("No data extracted")
                    break

                print(data)

                # extracted_urls = [urljoin(BASE_URL, u['url']) for u in data[0]["URLs"]]
                # if not extracted_urls:
                #     logger.info("No more URLs found, ending pagination")
                #     return event_urls

                # event_urls.extend(extracted_urls)
                # logger.info(f"Page {page} contained {len(extracted_urls)} URLs")

                # Process events concurrently but limit concurrency
                # processing_tasks = []
                # for event_url in extracted_urls:
                #     task = asyncio.create_task(process_event_url(event_url))
                #     processing_tasks.append(task)

                #     # Limit concurrent processing to 5 at a time
                #     if len(processing_tasks) >= 5:
                #         await asyncio.gather(*processing_tasks)
                #         processing_tasks = []

                # # Process any remaining tasks
                # if processing_tasks:
                #     await asyncio.gather(*processing_tasks)

                # page += 1
                # break  # Break out of retry loop if successful

            except Exception as e:
                logger.error(f"Error processing page {page} (attempt {attempt+1}): {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to process page {page} after {max_retries} attempts")
                    return event_urls
                await asyncio.sleep(2 ** attempt)

async def main():
    """Main function with top-level error handling."""
    try:
        url = "https://www.tapology.com/fightcenter/promotions/1-ultimate-fighting-championship-ufc"
        event_urls = await extract_event_urls(url)
        logger.info(f"Completed processing. Total events processed: {len(event_urls)}")
    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    load_dotenv()
    try:
        supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Initialization failed: {str(e)}")