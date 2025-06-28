# =================================================================
# scraper/database.py - Clean database operations
# =================================================================

from supabase import create_client, Client
from typing import List, Dict, Optional
from .utils import calculate_hash
import logging
from datetime import datetime, timedelta
import pytz
from datetime import datetime, timedelta
import pytz

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
        try:
            # The hash should be pre-calculated and included in `data`
            self.client.table('fighters').update(data).eq('id', fighter_id).execute()
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
    
    def delete_fight(self, fight_id: int):
        """Delete a fight by its ID."""
        try:
            self.client.table('fights').delete().eq('id', fight_id).execute()
            logger.info(f"Deleted fight with ID: {fight_id}")
        except Exception as e:
            logger.error(f"Failed to delete fight {fight_id}: {str(e)}")

    def flag_fighter_for_update(self, fighter_id: int):
        """Flag a fighter to be updated in the next run."""
        self.update_fighter(fighter_id, {'needs_update': True})

    def get_fighters_to_update(self, days_since_last_fight: int) -> List[Dict]:
        """Get fighters who fought recently or are flagged for an update."""
        # Calculate the cutoff date
        cutoff_date = (datetime.now(pytz.UTC) - timedelta(days=days_since_last_fight)).isoformat()

        # Build the `or` condition string
        or_conditions = f"needs_update.eq.true,last_fight_date.gte.{cutoff_date}"

        # Fetch fighters who either need a manual update or have fought recently
        result = self.client.table('fighters').select('*').or_(or_conditions).execute()
        
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