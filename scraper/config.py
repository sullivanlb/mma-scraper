# =================================================================
# scraper/config.py - All configuration in one place
# =================================================================

import os
from dotenv import load_dotenv

class Config:
    def __init__(self):
        load_dotenv()
        
        # Database
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # Scraping
        self.base_url = "https://www.tapology.com"
        self.days_lookback = int(os.getenv('DAYS_LOOKBACK', 7))
        self.concurrent_requests = int(os.getenv('CONCURRENT_REQUESTS', 5))
        self.retry_attempts = int(os.getenv('RETRY_ATTEMPTS', 3))
        
        # UFC specific
        self.ufc_url = f"{self.base_url}/fightcenter/promotions/1-ultimate-fighting-championship-ufc"