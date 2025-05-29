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
from .utils import format_date, parse_listing_date
import logging

logger = logging.getLogger(__name__)

class EventUpdater:
    def __init__(self, config, database):
        self.config = config
        self.db = database
        self.scraper = WebScraper(config)
        self.schema_events_urls = load_schema('./schemas/schema_events_urls.json')
        self.schema_events = load_schema('./schemas/schema_events.json')
    
    async def update_recent_events(self):
        """Update events from the last N days"""
        logger.info(f"🚀 Updating events from last {self.config.days_lookback} days")
        
        # Get event URLs
        event_urls = await self._get_recent_event_urls()
        logger.info(f"📅 Found {len(event_urls)} events to check")
        
        # Update events concurrently
        semaphore = asyncio.Semaphore(self.config.concurrent_requests)
        tasks = [self._update_single_event(semaphore, url) for url in event_urls]
        await asyncio.gather(*tasks)
    
    async def _get_recent_event_urls(self) -> List[str]:
        """Get event URLs from recent days"""
        event_urls = []
        page = 1
        cutoff_date = datetime.now(pytz.UTC) - timedelta(days=self.config.days_lookback)
        
        while True:
            url = f"{self.config.ufc_url}?page={page}"
            data = await self.scraper.extract_data(url, self.schema_events_urls)
            
            if not data or not data[0]["URLs"]:
                break
                
            for event in data[0]["URLs"]:
                event_date = parse_listing_date(event['date'])
                if not event_date:
                    continue
                    
                if event_date < cutoff_date:
                    return event_urls
                    
                event_urls.append(urljoin(self.config.base_url, event['url']))
            
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

                current_hash = self._calculate_hash(event_data)

                if not existing_event:
                    # 🆕 CREATE NEW EVENT
                    logger.info(f"🆕 Creating new event: {event_url}")
                    event_id = await self._create_new_event(event_url, event_data, current_hash)
                    if event_id:
                        await self._create_fights(event_data, event_id)
                        logger.info(f"✅ Created event with {len(event_data[0].get('Fight Card', []))} fights")
                    return

                # 🔄 UPDATE EXISTING EVENT
                if existing_event.get('hash') == current_hash:
                    logger.info(f"✅ Event unchanged: {event_url}")
                    return

                # Update event data
                await self._update_event_data(existing_event['id'], event_data, current_hash)
                
                # Update fights (results, finishes, etc.)
                await self._update_fights(event_data, existing_event['id'])
                
                logger.info(f"🔄 Updated event: {event_url}")

            except Exception as e:
                logger.error(f"❌ Failed to process event {event_url}: {str(e)}")
    
    def _calculate_hash(self, data) -> str:
        """Calculate hash for change detection"""
        json_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(json_str.encode('utf-8')).hexdigest()
    
    async def _update_event_data(self, event_id: int, event_data: List[Dict], hash_value: str):
        """Update event header information"""
        try:
            header = event_data[0]['Header'][0]
            update_data = {
                'hash': hash_value,
                'mma_bouts': header.get('mma_bouts', 0)
            }
            
            if 'datetime' in header:
                update_data['datetime'] = format_date(header['datetime'])
            
            self.db.update_event(event_id, update_data)
            
        except (KeyError, IndexError) as e:
            logger.error(f"Invalid event header data: {str(e)}")
    
    async def _update_fights(self, event_data: List[Dict], event_id: int):
        """Update fight results"""
        for fight in event_data[0]["Fight Card"]:
            try:
                # Get fighters
                fighter1 = self.db.get_fighter_by_url(fight.get('url_fighter_1', ''))
                fighter2 = self.db.get_fighter_by_url(fight.get('url_fighter_2', ''))
                
                if not fighter1 or not fighter2:
                    continue
                
                # Get fight record
                fight_record = self.db.get_fight(event_id, fighter1['id'], fighter2['id'])
                if not fight_record:
                    continue
                
                # Update fight
                fight_data = {
                    'result_fighter_1': fight.get('result_fighter_1'),
                    'result_fighter_2': fight.get('result_fighter_2'),
                    'finish_by': fight.get('finish_by'),
                    'finish_by_details': fight.get('finish_by_details'),
                    'rounds': fight.get('rounds')
                }
                
                self.db.update_fight(fight_record['id'], fight_data)
                
            except Exception as e:
                logger.error(f"Failed to update fight: {str(e)}")

    async def _create_new_event(self, event_url: str, event_data: List[Dict], hash_value: str) -> Optional[int]:
        """Create a new event in the database"""
        try:
            header = event_data[0]['Header'][0]
            
            # Prepare event data
            event_record = {
                'tapology_url': event_url,
                'hash': hash_value,
                'name': header.get('event_name', 'Unknown Event'),
                'promotion': header.get('promotion', 'UFC'),
                'location': header.get('location', ''),
                'datetime': format_date(header.get('datetime')),
                'mma_bouts': header.get('mma_bouts', 0),
                'created_at': datetime.now(pytz.UTC).isoformat()
            }
            
            # Insert and get the new event ID
            result = self.db.create_event(event_record)
            return result['id'] if result else None
            
        except (KeyError, IndexError) as e:
            logger.error(f"Invalid event header for creation: {str(e)}")
            return None

    async def _create_fights(self, event_data: List[Dict], event_id: int):
        """Create fights for a new event"""
        fight_cards = event_data[0].get("Fight Card", [])
        
        for fight in fight_cards:
            try:
                # Get or create fighters first
                fighter1_id = await self._get_or_create_fighter(fight.get('url_fighter_1', ''), fight.get('fighter_1', ''))
                fighter2_id = await self._get_or_create_fighter(fight.get('url_fighter_2', ''), fight.get('fighter_2', ''))
                
                if not fighter1_id or not fighter2_id:
                    logger.warning(f"⚠️ Skipping fight - missing fighters: {fight.get('fighter_1')} vs {fight.get('fighter_2')}")
                    continue
                
                # Create fight record
                fight_record = {
                    'id_event': event_id,
                    'id_fighter_1': fighter1_id,
                    'id_fighter_2': fighter2_id,
                    'fighter_1_name': fight.get('fighter_1', ''),
                    'fighter_2_name': fight.get('fighter_2', ''),
                    'weight_class': fight.get('weight_class', ''),
                    'bout_order': fight.get('bout_order', 0),
                    'result_fighter_1': fight.get('result_fighter_1'),
                    'result_fighter_2': fight.get('result_fighter_2'),
                    'finish_by': fight.get('finish_by'),
                    'finish_by_details': fight.get('finish_by_details'),
                    'rounds': fight.get('rounds'),
                    'created_at': datetime.now(pytz.UTC).isoformat()
                }
                
                self.db.create_fight(fight_record)
                
            except Exception as e:
                logger.error(f"Failed to create fight: {str(e)}")

    async def _get_or_create_fighter(self, fighter_url: str, fighter_name: str) -> Optional[int]:
        """Get fighter ID or create new fighter if doesn't exist"""
        if not fighter_url:
            return None
            
        # Check if fighter exists
        existing_fighter = self.db.get_fighter_by_url(fighter_url)
        if existing_fighter:
            return existing_fighter['id']
        
        # Create new fighter
        logger.info(f"🆕 Creating new fighter: {fighter_name}")
        full_url = urljoin(self.config.base_url, fighter_url)
        
        # Try to get fighter profile data
        fighter_data = await self.scraper.extract_data(full_url, self.schema_profiles)
        
        fighter_record = {
            'tapology_url': full_url,
            'name': fighter_name,
            'created_at': datetime.now(pytz.UTC).isoformat()
        }
        
        # Add profile data if available
        if fighter_data and fighter_data[0].get('Basic Infos'):
            basic_info = fighter_data[0]['Basic Infos'][0]
            fighter_record.update({
                'pro_mma_record': basic_info.get('pro_mma_record'),
                'last_fight_date': format_date(basic_info.get('last_fight_date')),
                'hash': self._calculate_hash(fighter_data)
            })
        
        result = self.db.create_fighter(fighter_record)
        return result['id'] if result else None