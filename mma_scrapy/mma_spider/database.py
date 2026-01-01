from supabase import create_client
from typing import Dict, Optional
import os
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, supabase_url, supabase_key):
        self.client = create_client(supabase_url, supabase_key)

    def get_event_by_url(self, url: str) -> Optional[Dict]:
        response = self.client.table('events').select('id,hash').eq('tapology_url', url).execute()
        return response.data[0] if response.data else None

    def create_event(self, data: Dict) -> Optional[Dict]:
        response = self.client.table('events').insert(data).execute()
        return response.data[0] if response.data else None

    def update_event(self, event_id: int, data: Dict):
        self.client.table('events').update(data).eq('id', event_id).execute()

    def update_fighter(self, fighter_id: int, data: Dict):
        try:
             self.client.table('fighters').update(data).eq('id', fighter_id).execute()
        except Exception as e:
             logger.error(f"Error updating fighter {fighter_id}: {e}")

    def get_fighters_to_update(self):
        # Fetch fighters where needs_update is true
        response = self.client.table('fighters').select('*').eq('needs_update', True).execute()
        return response.data if response.data else []

    def get_fighter_by_url(self, url: str) -> Optional[Dict]:
        response = self.client.table('fighters').select('id,hash').eq('tapology_url', url).execute()
        return response.data[0] if response.data else None

    def create_fighter(self, data: Dict) -> Optional[Dict]:
        response = self.client.table('fighters').insert(data).execute()
        return response.data[0] if response.data else None

    def get_fight_by_holders(self, event_id, fighter1_id, fighter2_id):
        # Check fight existence
        # Supabase OR syntax for (f1=A AND f2=B) OR (f1=B AND f2=A)
        or_cond = f"and(id_fighter_1.eq.{fighter1_id},id_fighter_2.eq.{fighter2_id}),and(id_fighter_1.eq.{fighter2_id},id_fighter_2.eq.{fighter1_id})"
        response = self.client.table('fights').select('id').eq('id_event', event_id).or_(or_cond).execute()
        return response.data[0] if response.data else None

    def create_fight(self, data: Dict):
        self.client.table('fights').insert(data).execute()

    def update_fight(self, fight_id, data: Dict):
        self.client.table('fights').update(data).eq('id', fight_id).execute()
