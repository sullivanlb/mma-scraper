from itemadapter import ItemAdapter
from .database import Database
from .items import EventItem, FightItem, FighterItem
from datetime import datetime
import pytz
import logging

class SupabasePipeline:
    def __init__(self, supabase_url, supabase_key):
        self.db = Database(supabase_url, supabase_key)
        self.event_cache = {} # url -> id
        self.fighter_cache = {} # url -> id

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            supabase_url=crawler.settings.get('SUPABASE_URL'),
            supabase_key=crawler.settings.get('SUPABASE_KEY')
        )

    def process_item(self, item, spider):
        if isinstance(item, EventItem):
            self.process_event(item)
        elif isinstance(item, FightItem):
            self.process_fight(item)
        elif isinstance(item, FighterItem):
            self.process_fighter(item)
        return item

    def process_fighter(self, item):
        url = item['tapology_url']
        existing = self.db.get_fighter_by_url(url)

        data = ItemAdapter(item).asdict()
        data['needs_update'] = False

        if existing:
            # We don't always update hash in DB so skipping checks might be tricky if we want to force update
            # But usually we want to update if hash differs or if we just scraped it.
            # FighterSpider runs on 'needs_update=True', so we definitely want to update.
            self.db.update_fighter(existing['id'], data)
            logging.info(f"Updated fighter {item['name']}")
        else:
            data['created_at'] = datetime.now(pytz.UTC).isoformat()
            self.db.create_fighter(data)
            logging.info(f"Created fighter {item['name']}")


    def process_event(self, item):
        url = item['tapology_url']
        # Check cache first to save DB call if we are reprocessing same url (unlikely for event list but possible)
        # Actually correct is to check DB always for updates
        existing = self.db.get_event_by_url(url)

        data = ItemAdapter(item).asdict()

        if not existing:
             data['created_at'] = datetime.now(pytz.UTC).isoformat()
             res = self.db.create_event(data)
             if res:
                 self.event_cache[url] = res['id']
                 logging.info(f"Created event {url}")
        else:
            self.event_cache[url] = existing['id']
            if existing.get('hash') != item['hash']:
                self.db.update_event(existing['id'], data)
                logging.info(f"Updated event {url}")
            else:
                logging.debug(f"Event {url} unchanged")

    def process_fight(self, item):
        event_url = item['event_tapology_url']
        event_id = self.event_cache.get(event_url)

        if not event_id:
            # Fallback lookup
            evt = self.db.get_event_by_url(event_url)
            if evt:
                event_id = evt['id']
                self.event_cache[event_url] = event_id
            else:
                logging.warning(f"Event not found for fight: {event_url}")
                return

        # Ensure fighters
        f1_id = self.ensure_fighter(item['fighter_1_url'], item['fighter_1_name'], item['fighter_1_img'])
        f2_id = self.ensure_fighter(item['fighter_2_url'], item['fighter_2_name'], item['fighter_2_img'])

        if not f1_id or not f2_id:
            logging.warning("Could not ensure fighters for fight")
            return

        # Prepare fight data
        # Mapping Item fields to DB columns
        fight_data = {
            'id_event': event_id,
            'id_fighter_1': f1_id,
            'id_fighter_2': f2_id,
            'fight_type': item.get('fight_type'),
            'finish_by': item.get('finish_by'),
            'finish_by_details': item.get('finish_by_details'),
            'rounds': item.get('rounds'),
            'minutes_per_round': item.get('minutes_per_round'),
            'result_fighter_1': item.get('fighter_1_result'),
            'result_fighter_2': item.get('fighter_2_result'),
        }

        existing_fight = self.db.get_fight_by_holders(event_id, f1_id, f2_id)
        if existing_fight:
            self.db.update_fight(existing_fight['id'], fight_data)
        else:
            fight_data['created_at'] = datetime.now(pytz.UTC).isoformat()
            self.db.create_fight(fight_data)

    def ensure_fighter(self, url, name, img_url):
        if not url: return None
        if url in self.fighter_cache: return self.fighter_cache[url]

        existing = self.db.get_fighter_by_url(url)
        if existing:
            self.fighter_cache[url] = existing['id']
            return existing['id']

        # Create stub
        data = {
            'tapology_url': url,
            'name': name,
            'profile_img_url': img_url,
            'needs_update': True,
            'created_at': datetime.now(pytz.UTC).isoformat()
        }
        res = self.db.create_fighter(data)
        if res:
            self.fighter_cache[url] = res['id']
            return res['id']
        return None
