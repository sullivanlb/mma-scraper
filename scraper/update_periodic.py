# main.py - Simple entry point
import asyncio
from app.event_updater import EventUpdater
from app.fighter_updater import FighterUpdater
from app.config import Config
from app.database import Database
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """Main entry point - simple and clear"""
    config = Config()
    db = Database(config)
    
    # Update events from last 7 days
    event_updater = EventUpdater(config, db)
    await event_updater.update_recent_events()
    
    # Update all fighters that need updating
    fighter_updater = FighterUpdater(config, db)
    await fighter_updater.update_fighters()
    
    logger.info("✅ All updates completed")

if __name__ == "__main__":
    asyncio.run(main())