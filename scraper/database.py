# =================================================================
# scraper/database.py - Clean database operations
# =================================================================

from supabase import create_client, Client
from typing import List, Dict, Optional
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
    
    def update_fighter(self, fighter_id: int, data: Dict):
        """Update fighter data"""
        self.client.table('fighters').update(data).eq('id', fighter_id).execute()
    
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
            .select('id,tapology_url,hash')\
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