# main.py - Simple entry point
import asyncio
from app.event_updater import EventUpdater
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
    upcoming_events = await event_updater.get_upcoming_event_urls()

    if not upcoming_events:
        logger.info("No upcoming events found.")
        return

    logger.info(f"Found {len(upcoming_events)} upcoming events to check.")

    # Update events concurrently
    semaphore = asyncio.Semaphore(event_updater.config.concurrent_requests)
    tasks = [event_updater._update_single_event(semaphore, url) for url in upcoming_events]
    await asyncio.gather(*tasks)
    
    logger.info("✅ All updates completed")

if __name__ == "__main__":
    asyncio.run(main())