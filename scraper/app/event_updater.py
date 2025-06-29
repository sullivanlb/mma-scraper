
# =================================================================
# scraper/event_updater.py - Handle event updates
# =================================================================

import asyncio
import hashlib
import json
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pytz
from urllib.parse import urljoin
from .web_scraper import WebScraper
from .schemas import load_schema
from .utils import parse_listing_date, get_or_create_fighter, calculate_hash, parse_result
import logging

logger = logging.getLogger(__name__)

class EventUpdater:
    def __init__(self, config, database):
        self.config = config
        self.db = database
        self.scraper = WebScraper(config)
        self.schema_events_urls = load_schema('./schemas/schema_events_urls.json')
        self.schema_events = load_schema('./schemas/schema_events.json')
        self.schema_profiles = load_schema('./schemas/schema_profiles.json')  # or whatever the correct path is
    
    async def update_recent_events(self):
        """Update events from the last N days and next N days"""
        logger.info(f"🚀 Updating events from last {self.config.days_offset} days and next {self.config.days_offset} days")
        
        # Get event URLs for both past and future
        event_urls = await self._get_event_urls_range()
        logger.info(f"📅 Found {len(event_urls)} events to check (past + upcoming)")
        
        # Update events concurrently
        semaphore = asyncio.Semaphore(self.config.concurrent_requests)
        tasks = [self._update_single_event(semaphore, url) for url in event_urls]
        await asyncio.gather(*tasks)
    
    async def _get_event_urls_range(self) -> List[str]:
        """Get event URLs from past N days and next N days"""
        event_urls = []
        page = 1
        now = datetime.now(pytz.UTC)
        
        # Define date range: past N days to future N days
        start_date = now - timedelta(days=self.config.days_offset)
        end_date = now + timedelta(days=self.config.days_offset)
        
        logger.info(f"📅 Searching events from {start_date.date()} to {end_date.date()}")
        
        while True:
            url = f"{self.config.ufc_url}?page={page}"
            data = await self.scraper.extract_data(url, self.schema_events_urls)
            
            if not data or not data[0]["URLs"]:
                break
                
            found_events_in_range = False
            
            for event in data[0]["URLs"]:
                event_date = parse_listing_date(event['date'])
                if not event_date:
                    continue
                
                # Skip events too far in the past
                if event_date < start_date:
                    continue
                
                # Stop if we've gone too far into the future
                if event_date > end_date:
                    continue
                
                found_events_in_range = True
                event_urls.append(urljoin(self.config.base_url, event['url']))
            
            # If we didn't find any events in our date range on this page,
            # and we're looking at past events, we can stop
            if not found_events_in_range and page > 1:
                # Check if the latest event on this page is older than our start date
                latest_event_date = None
                for event in data[0]["URLs"]:
                    event_date = parse_listing_date(event['date'])
                    if event_date and (not latest_event_date or event_date > latest_event_date):
                        latest_event_date = event_date
                
                if latest_event_date and latest_event_date < start_date:
                    break
            
            page += 1
        
        # Sort by date to process in chronological order
        return event_urls

    async def get_upcoming_event_urls(self) -> List[str]:
        """Get URLs for all upcoming events."""
        event_urls = []
        page = 1
        now = datetime.now(pytz.UTC)

        logger.info("📅 Searching for all upcoming events...")

        while True:
            url = f"{self.config.ufc_url}?page={page}"
            data = await self.scraper.extract_data(url, self.schema_events_urls)

            if not data or not data[0]["URLs"]:
                break

            for event in data[0]["URLs"]:
                event_date = parse_listing_date(event['date'])
                if event_date and event_date >= now:
                    event_urls.append(urljoin(self.config.base_url, event['url']))

            # If the last event on the page is in the past, we can stop
            last_event_date = parse_listing_date(data[0]["URLs"][-1]['date'])
            if last_event_date and last_event_date < now:
                break

            page += 1

        return event_urls
    
    async def _update_single_event(self, semaphore, event_url: str):
        """Update a single event if needed, create if doesn't exist"""
        async with semaphore:
            try:
                # Check if event exists
                existing_event = self.db.get_event_by_url(event_url)
                
                # Get fresh event data (needed for both update and create)
                event_data = await self.scraper.extract_data(event_url, self.schema_events)
                if not event_data:
                    logger.error(f"❌ No data extracted from {event_url}")
                    return

                current_hash = calculate_hash(event_data)
                print (f"Current hash for {event_url}: {current_hash}")

                if not existing_event:
                    # 🆕 CREATE NEW EVENT (could be past or future)
                    logger.info(f"🆕 Creating new event: {event_url}")
                    event_id = await self._create_new_event(event_url, event_data, current_hash)
                    if event_id:
                        await self._update_fights(event_data, event_id)
                        logger.info(f"✅ Created event with {len(event_data[0].get('Fight Card', []))} fights")
                    
                    return

                # 🔄 UPDATE EXISTING EVENT
                if existing_event.get('hash') != current_hash:
                    # Update event data as the hash is different
                    await self._update_event_data(existing_event['id'], event_data, current_hash)
                    logger.info(f"🔄 Event data changed, updating: {event_url}")
                else:
                    logger.info(f"✅ Event basics unchanged, checking for fight updates for {event_url}")

                # Always check for fight updates, regardless of the main event hash
                # Update fights (results for past events, card changes for future events)
                await self._update_fights(event_data, existing_event['id'])
                
                logger.info(f"✅ Finished processing event: {event_url}")
                

            except Exception as e:
                logger.error(f"❌ Failed to process event {event_url}: {str(e)}")
    
    async def _update_event_data(self, event_id: int, event_data: List[Dict], hash_value: str):
        """Update event header information"""
        try:
            header = event_data[0]['Header'][0]
            update_data = {
                'hash': hash_value,
                'mma_bouts': header.get('mma_bouts', 0),
                'datetime': parse_listing_date(header.get('datetime')).isoformat() \
                    if header.get('datetime') else None,
                'broadcast': header.get('broadcast', ''),
                'promotion': header.get('promotion', ''),
                'broadcast': header.get('broadcast', ''),
                'venue': '' if header.get('venue', '') == 'N/A' else header.get('venue', ''),
                'location': header.get('location', ''),
                'img_url': header.get('img_url', '')
            }
            
            self.db.update_event(event_id, update_data)
            
        except (KeyError, IndexError) as e:
            logger.error(f"Invalid event header data: {str(e)}")
    
    async def _update_fights(self, event_data: List[Dict], event_id: int):
        """Update, add, or remove fights as needed to match the scraped data."""
        scraped_fights = event_data[0].get("Fight Card", [])
        existing_fights = self.db.get_fights_by_event_id(event_id)

        # Create sets of fighter pairs for easy comparison
        scraped_fighter_pairs = set()
        for fight in scraped_fights:
            fighter1_id = await get_or_create_fighter(self, fight.get('url_fighter_1'), fight.get('name_fighter_1'))
            fighter2_id = await get_or_create_fighter(self, fight.get('url_fighter_2'), fight.get('name_fighter_2'))
            if fighter1_id and fighter2_id:
                scraped_fighter_pairs.add(tuple(sorted((fighter1_id, fighter2_id))))

        existing_fighter_pairs = {tuple(sorted((f['id_fighter_1'], f['id_fighter_2']))) for f in existing_fights}

        # 1. Fights to DELETE (in DB but not in scraped data)
        fights_to_delete = existing_fighter_pairs - scraped_fighter_pairs
        for pair in fights_to_delete:
            fight_to_delete = next((f for f in existing_fights if tuple(sorted((f['id_fighter_1'], f['id_fighter_2']))) == pair), None)
            if fight_to_delete:
                self.db.delete_fight(fight_to_delete['id'])
                logger.info(f"- Removed fight: {fight_to_delete['id']}")

        # 2. Fights to ADD or UPDATE
        for fight_data in scraped_fights:
            fighter1_id = await get_or_create_fighter(self, fight_data.get('url_fighter_1'), fight_data.get('name_fighter_1'))
            fighter2_id = await get_or_create_fighter(self, fight_data.get('url_fighter_2'), fight_data.get('name_fighter_2'))

            if not fighter1_id or not fighter2_id:
                continue

            fight_pair = tuple(sorted((fighter1_id, fighter2_id)))
            existing_fight = next((f for f in existing_fights if tuple(sorted((f['id_fighter_1'], f['id_fighter_2']))) == fight_pair), None)

            update_data = {
                'result_fighter_1': fight_data.get('result_fighter_1'),
                'result_fighter_2': fight_data.get('result_fighter_2'),
                'finish_by': fight_data.get('finish_by'),
                'finish_by_details': fight_data.get('finish_by_details'),
                'fight_type': fight_data.get('fight_type'),
                'rounds': fight_data.get('rounds')
            }

            if existing_fight:
                # UPDATE existing fight
                self.db.update_fight(existing_fight['id'], update_data)
                # Flag both fighters for a profile update
                self.db.flag_fighter_for_update(fighter1_id)
                self.db.flag_fighter_for_update(fighter2_id)
            else:
                # ADD new fight
                update_data.update({
                    'id_event': event_id,
                    'id_fighter_1': fighter1_id,
                    'id_fighter_2': fighter2_id,
                    'created_at': datetime.now(pytz.UTC).isoformat()
                })
                self.db.create_fight(update_data)
                logger.info(f"+ Added new fight between {fighter1_id} and {fighter2_id}")

    async def _create_new_event(self, event_url: str, event_data: List[Dict], hash_value: str) -> Optional[int]:
        """Create a new event in the database"""
        try:
            header = event_data[0]['Header'][0]
            name = event_data[0]['name']
            
            # Prepare event data
            event_record = {
                'tapology_url': event_url,
                'hash': hash_value,
                'name': name if name else 'Unknown Event',
                'promotion': header.get('promotion', 'UFC'),
                'broadcast': header.get('broadcast', ''),
                'location': header.get('location', ''),
                'venue': header.get('venue', ''),
                'datetime': parse_listing_date(header.get('datetime')).isoformat(),
                'mma_bouts': header.get('mma_bouts', 0),
                'img_url': header.get('img_url', ''),
                'created_at': datetime.now(pytz.UTC).isoformat()
            }
            
            # Insert and get the new event ID
            result = self.db.create_event(event_record)
            return result['id'] if result else None
            
        except (KeyError, IndexError) as e:
            logger.error(f"Invalid event header for creation: {str(e)}")
            return None

    

    

    async def _get_all_event_urls(self) -> List[str]:
        """Get ALL event URLs from UFC, page by page without stopping"""
        event_urls = []
        page = 22
        
        logger.info(f"🔍 Starting to scrape ALL UFC events, page by page...")
        
        while True:
            url = f"{self.config.ufc_url}?page={page}"
            logger.info(f"📄 Scraping page {page}: {url}")
            
            try:
                data = await self.scraper.extract_data(url, self.schema_events_urls)
                
                # If no data or no URLs found, we've reached the end
                if not data or not data[0].get("URLs") or len(data[0]["URLs"]) == 0:
                    logger.info(f"✅ No more events found on page {page}. Stopping.")
                    break
                
                page_events = data[0]["URLs"]
                logger.info(f"📋 Found {len(page_events)} events on page {page}")
                
                # Add all events from this page
                for event in page_events:
                    if event.get('url'):
                        full_url = urljoin(self.config.base_url, event['url'])
                        event_urls.append(full_url)
                        
                        # Optional: Log event details for debugging
                        event_date = parse_listing_date(event.get('date', ''))
                        logger.debug(f"  📅 {event.get('date', 'No date')} - {event.get('url', 'No URL')}")
                page += 1
                
                # Optional: Add small delay to be respectful to the server
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ Error scraping page {page}: {e}")
                # You might want to retry or break depending on your needs
                break
        
        logger.info(f"🎯 Total events found: {len(event_urls)} across {page-1} pages")
        return event_urls