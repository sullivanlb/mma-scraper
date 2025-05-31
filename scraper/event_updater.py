
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
from .utils import format_date, parse_listing_date, calculate_total_fights
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
                    # 🆕 CREATE NEW EVENT (could be past or future)
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
                
                # Update fights (results for past events, card changes for future events)
                await self._update_fights(event_data, existing_event['id'])
                
                # Check for new fights that might have been added to the card
                await self._check_and_add_new_fights(event_data, existing_event['id'])
                
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
                'mma_bouts': header.get('mma_bouts', 0),
                'datetime': parse_listing_date(header.get('datetime')).isoformat() \
                    if header.get('datetime') else None,
                'broadcast': header.get('broadcast', ''),
                'promotion': header.get('promotion', ''),
                'venue': header.get('venue', ''),
                'location': header.get('location', ''),
                'img_url': header.get('img_url', '')
            }
            
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
        
        # Initialize fighter record with basic data
        fighter_record = {
            'tapology_url': full_url,
            'name': fighter_name,
            'created_at': datetime.now(pytz.UTC).isoformat()
        }
        
        # Add comprehensive profile data if available
        if fighter_data and len(fighter_data) > 0 and fighter_data[0].get('Basic Infos'):
            try:
                basic_info = fighter_data[0]['Basic Infos'][0]
                
                # Map all available fields from the schema to database columns
                fighter_record.update({
                    'nickname': basic_info.get('nickname'),
                    'age': basic_info.get('age'),
                    'date_of_birth': parse_listing_date(basic_info.get('date_of_birth')),
                    'height': basic_info.get('height'),
                    'weight_class': basic_info.get('weight_class'),
                    'last_weight_in': basic_info.get('last_weight_in'),
                    'last_fight_date': parse_listing_date(basic_info.get('last_fight_date')),
                    'born': basic_info.get('born'),
                    'head_coach': basic_info.get('head_coach'),
                    'pro_mma_record': basic_info.get('pro_mma_record'),
                    'current_mma_streak': basic_info.get('current_mma_streak'),
                    'affiliation': basic_info.get('affiliation'),
                    'other_coaches': basic_info.get('other_coaches'),
                    'hash': self._calculate_hash(fighter_data)
                })
                
                # Add profile image URL if available
                if fighter_data[0].get('profile_img_url'):
                    fighter_record['profile_img_url'] = fighter_data[0]['profile_img_url']
                
                # Calculate total fights from the record if available
                if basic_info.get('pro_mma_record'):
                    fighter_record['total_fights'] = calculate_total_fights(basic_info['pro_mma_record'])
                    
            except (KeyError, IndexError, TypeError) as e:
                logger.warning(f"⚠️ Could not parse complete fighter profile for {fighter_name}: {str(e)}")
        else:
            logger.warning(f"⚠️ No profile data available for {fighter_name}, creating with basic info only")
        
        try:
            result = self.db.create_fighter(fighter_record)
            return result['id'] if result else None
        except Exception as e:
            logger.error(f"❌ Failed to create fighter {fighter_name}: {str(e)}")
            return None

    async def _check_and_add_new_fights(self, event_data: List[Dict], event_id: int):
        """Check for new fights added to an existing event card"""
        try:
            fight_cards = event_data[0].get("Fight Card", [])
            existing_fights = self.db.get_fights_by_event_id(event_id)  # You'll need to implement this method
            
            # Create a set of existing fight pairs for quick lookup
            existing_fight_pairs = set()
            for fight in existing_fights:
                # Create a tuple of fighter IDs (sorted to handle either order)
                pair = tuple(sorted([fight['id_fighter_1'], fight['id_fighter_2']]))
                existing_fight_pairs.add(pair)
            
            new_fights_added = 0
            for fight in fight_cards:
                # Get or create fighters
                fighter1_id = await self._get_or_create_fighter(fight.get('url_fighter_1', ''), fight.get('fighter_1', ''))
                fighter2_id = await self._get_or_create_fighter(fight.get('url_fighter_2', ''), fight.get('fighter_2', ''))
                
                if not fighter1_id or not fighter2_id:
                    continue
                
                # Check if this fight already exists
                fight_pair = tuple(sorted([fighter1_id, fighter2_id]))
                if fight_pair not in existing_fight_pairs:
                    # This is a new fight, add it
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
                    new_fights_added += 1
                    logger.info(f"+ Added new fight: {fight.get('fighter_1')} vs {fight.get('fighter_2')}")
            
            if new_fights_added > 0:
                logger.info(f"✅ Added {new_fights_added} new fights to existing event")
                
        except Exception as e:
            logger.error(f"Failed to check for new fights: {str(e)}")