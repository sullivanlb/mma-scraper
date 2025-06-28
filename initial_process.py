# main.py - Simple entry point
import asyncio
from scraper.event_updater import EventUpdater
from scraper.fighter_updater import FighterUpdater
from scraper.config import Config
from scraper.database import Database
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """Main entry point - simple and clear"""
    config = Config()
    db = Database(config)
    
    logger.info(f"🚀 Get ALL events")

    # Update events from last 7 days
    event_updater = EventUpdater(config, db)

    # Get event URLs for both past and future
    event_urls = await event_updater._get_all_event_urls()
    logger.info(f"📅 Found {len(event_urls)} events to insert")
    
    # Update events concurrently
    semaphore = asyncio.Semaphore(event_updater.config.concurrent_requests)
    tasks = [event_updater._update_single_event(semaphore, url) for url in event_urls]
    await asyncio.gather(*tasks)
    
    # Update all fighters that need updating
    # fighter_updater = FighterUpdater(config, db)
    # await fighter_updater.update_all_fighters()
    
    logger.info("✅ All inserts completed")

if __name__ == "__main__":
    asyncio.run(main())