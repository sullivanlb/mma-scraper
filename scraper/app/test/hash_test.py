import sys
from pathlib import Path
import asyncio
import json
sys.path.append(str(Path(__file__).parent.parent.parent))

from app import config
from app import database
from app import fighter_updater
from app import web_scraper
from app import schemas
from app import utils
from app import event_updater

async def main():

    test_data_path = Path(__file__).parent / 'test_data' / 'event.json'
    with open(test_data_path, 'r') as f:
        data = json.load(f) 

    print(data)

    hash = utils.calculate_hash(data)

    print(hash)

if __name__ == "__main__":
    asyncio.run(main())