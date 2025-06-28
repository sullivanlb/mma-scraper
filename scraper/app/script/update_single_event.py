# main.py - Simple entry point
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import asyncio
import argparse
from app.event_updater import EventUpdater
from app.fighter_updater import FighterUpdater
from app.config import Config
from app.database import Database
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """Main entry point - simple and clear"""
    parser = argparse.ArgumentParser(description='Update a single event from URL')
    parser.add_argument('url', type=str, help='The URL of the event to update')
    
    args = parser.parse_args()
    print("url:", args.url)

    config = Config()
    db = Database(config)  # Create database instance
    
    semaphore = asyncio.Semaphore(config.concurrent_requests)
    event_updater = EventUpdater(config, db)  # Create EventUpdater instance
    await event_updater._update_single_event(semaphore, args.url)

if __name__ == "__main__":
    asyncio.run(main())