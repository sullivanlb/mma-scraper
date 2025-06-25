# =================================================================
# scraper/fighter_updater.py - Handle fighter updates
# =================================================================

import asyncio
from .web_scraper import WebScraper
from .schemas import load_schema
from .utils import parse_listing_date, get_or_create_fighter, calculate_hash
from typing import Dict, Optional
import logging
import pytz
from urllib.parse import urljoin
from datetime import datetime

logger = logging.getLogger(__name__)

class FighterUpdater:
    def __init__(self, config, database):
        self.config = config
        self.db = database
        self.scraper = WebScraper(config)
        self.schema_profiles = load_schema('./schemas/schema_profiles.json')
        self.schema_events = load_schema('./schemas/schema_events.json')
    
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
                current_hash = calculate_hash(fighter_data)
                if fighter.get('hash') == current_hash:
                    return
                
                # Update fighter basic info
                basic_info = fighter_data[0]['Basic Infos'][0]

                if basic_info.get('last_fight_date') != "N/A":
                    basic_info['last_fight_date'] = parse_listing_date(basic_info.get('last_fight_date')).isoformat()

                update_data = {
                    'hash': current_hash,
                    'pro_mma_record': basic_info.get('pro_mma_record'),
                    'last_fight_date': basic_info.get('last_fight_date'),
                    'age': basic_info.get('age'),
                    'weight_class': basic_info.get('weight_class'),
                    'last_weight_in': basic_info.get('last_weight_in'),
                    'head_coach': basic_info.get('head_coach'),
                    'current_mma_streak': basic_info.get('current_mma_streak'),
                    'affiliation': basic_info.get('affiliation'),
                    'other_coaches': basic_info.get('other_coaches'),
                }
                
                self.db.update_fighter(fighter['id'], update_data)

                # Update fighter fights avec matching d'events
                fights_processed = 0
                for fight in fighter_data[0].get('Fights', []):
                    if not fight.get('event_url'):
                        logger.warning(f"⚠️ Combat sans event pour le fighter {fighter['name']}: {fight}")
                        continue

                    event_url = urljoin(self.config.base_url, fight.get("event_url"))
                    
                    event = self.db.get_event_by_url(event_url)
                    
                    if not event:
                        logger.warning(f"⚠️ Impossible de trouver/créer l'event pour le combat: {fight}")
                        continue

                    event_id = event['id']
                    
                    # Créer ou récupérer l'opponent
                    opponent_id = await get_or_create_fighter(self, fight.get('opponent_tapology_url'), fight.get('opponent'))
                    
                    if not opponent_id:
                        logger.warning(f"⚠️ Impossible de créer l'opponent: {fight.get('opponent')}")
                        continue

                    fight_data = {
                        'id_event': event_id,
                        'id_fighter_1': fighter['id'],
                        'id_fighter_2': opponent_id,
                        'result_fighter_1': fight.get('result_fighter_1'),
                        'result_fighter_2': fight.get('result_fighter_2'),
                        'fight_type': fight.get('fight_type'),
                        'finish_by': fight.get('finish_by'),
                        'finish_by_details': fight.get('finish_by_details'),
                        'rounds': fight.get('rounds'),
                        'minutes_per_round': fight.get('minutes_per_round', 5),
                        'opponent_tapology_url': fight.get('url_fighter_2', ''),
                        'created_at': datetime.now(pytz.UTC).isoformat()
                    }
                    
                    # Vérifier si le combat existe déjà
                    print(fighter['id'], opponent_id, event_id)
                    existing_fight = self.db.get_fight_by_fighters_and_event(
                        fighter['id'], 
                        opponent_id,
                        event_id
                    )
                    
                    if existing_fight:
                        self.db.update_fight(existing_fight[0]['id'], fight_data)
                        logger.debug(f"🔄 Combat mis à jour: {fighter['name']} vs {fight.get('opponent')}")
                    else:
                        print(f"Fighter updater: {fight_data}")
                        self.db.create_fight(fight_data)
                        logger.debug(f"🆕 Nouveau combat créé: {fighter['name']} vs {fight.get('opponent')}")
                        fights_processed += 1

                logger.info(f"🔄 [Historique] Fighter {fighter['name']}: {fights_processed} nouveaux combats ajoutés")
                
            except Exception as e:
                logger.error(f"❌ Failed to update fighter {fighter.get('tapology_url')}: {str(e)}")
    
    def _find_or_create_event(self, fight_date: datetime, event_name: str) -> Optional[int]:
        """
        Trouve l'event dans la base ou le crée si nécessaire
        """
        try:
            if not fight_date or not event_name:
                logger.warning(f"Données d'event incomplètes: date={fight_date}, name={event_name}")
                return None
            
            # 1. Recherche exacte par nom et date
            event = self.db.get_event_by_name_and_date(event_name, fight_date)
            if event:
                return event['id']
            
            return -1
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la recherche/création d'event: {str(e)}")
            return None