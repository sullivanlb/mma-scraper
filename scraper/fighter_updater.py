# =================================================================
# scraper/fighter_updater.py - Handle fighter updates
# =================================================================

import asyncio
from .web_scraper import WebScraper
from .schemas import load_schema
from .utils import format_date
from typing import Dict
import logging
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class FighterUpdater:
    def __init__(self, config, database):
        self.config = config
        self.db = database
        self.scraper = WebScraper(config)
        self.schema_profiles = load_schema('./schemas/schema_profiles.json')
    
    async def update_all_fighters(self):
        """Update all fighters that need updating"""
        logger.info("🥊 Starting fighter updates")
        
        fighters = self.db.get_all_fighters_needing_update()
        logger.info(f"👥 Found {len(fighters)} fighters to update")
        
        # Update fighters concurrently
        semaphore = asyncio.Semaphore(self.config.concurrent_requests)
        tasks = [self._update_single_fighter(semaphore, fighter) for fighter in fighters]
        await asyncio.gather(*tasks)
        
        logger.info("✅ Fighter updates completed")
    
    async def _update_single_fighter(self, semaphore, fighter: Dict):
        """Update a single fighter"""
        async with semaphore:
            try:
                fighter_data = await self.scraper.extract_data(
                    urljoin(self.config.base_url, fighter['tapology_url']), 
                    self.schema_profiles
                )
                
                if not fighter_data:
                    return
                
                # Check if changed
                current_hash = self._calculate_hash(fighter_data)
                if fighter.get('hash') == current_hash:
                    return
                
                # Update fighter
                basic_info = fighter_data[0]['Basic Infos'][0]
                update_data = {
                    'hash': current_hash,
                    'pro_mma_record': basic_info.get('pro_mma_record'),
                    'last_fight_date': format_date(basic_info.get('last_fight_date')),
                    'age': basic_info.get('age'),
                    'weight_class': basic_info.get('weight_class'),
                    'last_weight_in': basic_info.get('last_weight_in'),
                    'head_coach': basic_info.get('head_coach'),
                    'current_mma_streak': basic_info.get('current_mma_streak'),
                    'affiliation': basic_info.get('affiliation'),
                    'other_coaches': basic_info.get('other_coaches'),
                }
                
                self.db.update_fighter(fighter['id'], update_data)

                # Update fighter fights
                for fight in fighter_data[0].get('Fights', []):
                    fight_data = {
                        'id_event': fight.get('event_id'),
                        'id_fighter_1': fighter['id'],
                        'id_fighter_2': fight.get('opponent_id'),
                        'result': fight.get('result'),
                        'date': format_date(fight.get('date')),
                        'weight_class': fight.get('weight_class'),
                    }
                    
                    existing_fight = self.db.get_fight(
                        fighter['id'], 
                        fighter['id'], 
                        fight.get('opponent_id')
                    )
                    
                    if existing_fight:
                        self.db.update_fight(existing_fight['id'], fight_data)
                    else:
                        self.db.create_fight(fight_data)

                logger.info(f"🔄 [Base info] Updated fighter: {fighter['tapology_url']}")
                
            except Exception as e:
                logger.error(f"❌ Failed to update fighter {fighter.get('tapology_url')}: {str(e)}")
    
    def _calculate_hash(self, data) -> str:
        """Calculate hash for change detection"""
        import json
        import hashlib
        json_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(json_str.encode('utf-8')).hexdigest()