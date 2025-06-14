# =================================================================
# scraper/database.py - Clean database operations
# =================================================================

from supabase import create_client, Client
from typing import List, Dict, Optional
from .utils import calculate_hash
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, config):
        self.client = create_client(config.supabase_url, config.supabase_key)
    
    def get_event_by_url(self, url: str) -> Optional[Dict]:
        """Get event by Tapology URL"""
        result = self.client.table('events')\
            .select('id,hash')\
            .eq('tapology_url', url)\
            .execute()
        return result.data[0] if result.data else None
    
    def update_event(self, event_id: int, data: Dict):
        """Update event data"""
        self.client.table('events').update(data).eq('id', event_id).execute()
    
    def get_fighter_by_url(self, url: str) -> Optional[Dict]:
        """Get fighter by Tapology URL"""
        result = self.client.table('fighters')\
            .select('id,hash')\
            .eq('tapology_url', url)\
            .execute()
            
        return result.data[0] if result.data else None
    
    def get_fighter_by_id(self, id: int) -> Optional[Dict]:
        """Get fighter by ID"""
        result = self.client.table('fighters')\
            .select('*')\
            .eq('id', id)\
            .execute()
        return result.data[0] if result.data else None
    
    def update_fighter(self, fighter_id: int, data: Dict):
        """Update fighter data"""
        fighter = self.get_fighter_by_id(fighter_id)
        
        if fighter is None:
            print(f"Fighter with ID {fighter_id} not found")
            return
        
        for key, value in data.items():
            fighter[key] = value 

        # Calculate hash of the updated fighter
        fighter_hash = calculate_hash(fighter)
        
        # Add the hash to the update data
        update_data = data.copy()
        update_data['hash'] = fighter_hash

        try:
            result = self.client.table('fighters').update(update_data).eq('id', fighter_id).execute()
            # print(f"Update result: {result}")
        except Exception as e:
            print(f"Error updating fighter: {e}")
    
    def get_fight(self, event_id: int, fighter1_id: int, fighter2_id: int) -> Optional[Dict]:
        """Get specific fight"""
        result = self.client.table('fights')\
            .select('id')\
            .eq('id_event', event_id)\
            .eq('id_fighter_1', fighter1_id)\
            .eq('id_fighter_2', fighter2_id)\
            .execute()
        return result.data[0] if result.data else None
    
    def update_fight(self, fight_id: int, data: Dict):
        """Update fight data"""
        self.client.table('fights').update(data).eq('id', fight_id).execute()
    
    def get_all_fighters_needing_update(self) -> List[Dict]:
        """Get fighters that haven't been updated recently"""
        # You can add logic here to check last_updated timestamp
        result = self.client.table('fighters')\
            .select('*')\
            .execute()
        return result.data
    
    def get_fight_by_fighters_and_event(self, id_fighter_1, id_fighter_2, id_event) -> List[Dict]:
        """Get fight by fighters and by event"""
        result = self.client.table('fights')\
            .select('*')\
            .eq('id_event', id_event)\
            .or_(f'and(id_fighter_1.eq.{id_fighter_1},id_fighter_2.eq.{id_fighter_2}),and(id_fighter_1.eq.{id_fighter_2},id_fighter_2.eq.{id_fighter_1})')\
            .execute()
        
        return result.data

    def create_event(self, event_data: Dict) -> Optional[Dict]:
        """Create new event"""
        try:
            result = self.client.table('events').insert(event_data).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Failed to create event: {str(e)}")
            return None
    
    def create_fight(self, fight_data: Dict) -> Optional[Dict]:
        """Create new fight"""
        try:
            result = self.client.table('fights').insert(fight_data).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Failed to create fight: {str(e)}")
            return None
    
    def create_fighter(self, fighter_data: Dict) -> Optional[Dict]:
        """Create new fighter"""
        try:
            result = self.client.table('fighters').insert(fighter_data).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Failed to create fighter: {str(e)}")
            return None
        
    def get_fights_by_event_id(self, event_id: int) -> List[Dict]:
        """Get all fights for a specific event"""
        result = self.client.table('fights')\
            .select('*')\
            .eq('id_event', event_id)\
            .execute()
        return result.data if result.data else []
    
    def get_event_by_name_and_date(self, name: str, date: str) -> Optional[Dict]:
        """Get event by name and date"""
        result = self.client.table('events')\
            .select('*')\
            .eq('name', name)\
            .eq('date', date)\
            .execute()
        return result.data[0] if result.data else None