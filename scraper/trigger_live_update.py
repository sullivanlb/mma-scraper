
import asyncio
import os
import json
import requests
from datetime import datetime, timedelta
import pytz

from app.config import Config
from app.web_scraper import WebScraper
from app.schemas import load_schema
from app.utils import parse_listing_date

# --- Configuration ---
# Your GitHub details
GITHUB_TOKEN = os.getenv("GH_PAT")  # Personal Access Token
GITHUB_USER = "sullivanlb"  # Your GitHub username or organization
GITHUB_REPO = "mma-scraper"      # Your repository name

# How many hours before and after an event's start time to consider it "live"
LIVE_WINDOW_HOURS = 4 

# --- Script ---

async def is_event_live(scraper, config):
    """Checks if a UFC event is currently within the live window."""
    schema_events_urls = load_schema('./scraper/schemas/schema_events_urls.json')
    
    # Check the first page of UFC events on Tapology
    url = config.ufc_url
    data = await scraper.extract_data(url, schema_events_urls)
    
    if not data or not data[0].get("URLs"):
        print("Could not fetch event data from Tapology.")
        return False

    now_utc = datetime.now(pytz.UTC)
    
    for event in data[0]["URLs"]:
        event_date = parse_listing_date(event.get('date'))
        if not event_date:
            continue

        # Check if the event is within our "live" window
        time_difference = abs(now_utc - event_date)
        if time_difference <= timedelta(hours=LIVE_WINDOW_HOURS):
            print(f"Live event detected: {event.get('url')} (Starts at: {event_date})")
            return True
            
    print("No live events found within the configured window.")
    return False

def trigger_github_action():
    """Sends a repository_dispatch event to trigger the GitHub Actions workflow."""
    if not GITHUB_TOKEN:
        print("Error: GH_PAT environment variable not set. Cannot trigger workflow.")
        return

    url = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}/dispatches"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {GITHUB_TOKEN}",
    }
    data = {"event_type": "run-live-update"}

    print(f"Sending webhook to {url} to trigger live update...")
    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code == 204:
        print("Successfully triggered 'live_event_scrape' workflow.")
    else:
        print(f"Failed to trigger workflow. Status: {response.status_code}, Response: {response.text}")

async def main():
    config = Config()
    scraper = WebScraper(config)
    
    if await is_event_live(scraper, config):
        trigger_github_action()

if __name__ == "__main__":
    # IMPORTANT: Replace with your actual GitHub username
    if GITHUB_USER == "YOUR_GITHUB_USERNAME":
        print("Please update the GITHUB_USER variable in trigger_live_update.py before running.")
    else:
        asyncio.run(main())
