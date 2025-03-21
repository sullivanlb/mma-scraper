import json
import asyncio
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from urllib.parse import urljoin  # This helps to join base URL with relative paths
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from datetime import datetime
import hashlib

# Load schemas
with open('schema_profiles_urls.json', 'r') as file:
    schema_urls = json.load(file)

with open('schema_profiles.json', 'r') as file:
    schema_profiles = json.load(file)

BASE_URL = "https://www.tapology.com"  # The base URL of Tapology

def insert_data(supabase, table, data):
    try:
        response = supabase.table(table).upsert(data).execute()
        print(response)
        if response.status_code == 200:
            print(f"Successfully inserted {len(data)} into {table}")
        else:
            print(f"Failed to insert into {table}: {response.error_message}")
    except Exception as e:
        print(f"Error: {e}")

async def extract_fighter_urls(url):
    extraction_strategy = JsonCssExtractionStrategy(schema_urls, verbose=True)
    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        extraction_strategy=extraction_strategy,
    )

    fighter_urls = []

    async with AsyncWebCrawler(verbose=True) as crawler:
        page = 1
        while True:
            paginated_url = f"{url}?page={page}"
            result = await crawler.arun(url=paginated_url, config=config)

            if not result.success:
                print(f"Crawl failed for {paginated_url}: {result.error_message}")
                break

            data = json.loads(result.extracted_content)
            extracted_urls = data[0].get("URLs", [])

            if not extracted_urls:
                print(f"No more URLs found on page {page}, stopping.")
                break

            # Convert relative URLs to absolute URLs
            absolute_urls = [urljoin(BASE_URL, url['url']) for url in extracted_urls]
            fighter_urls.extend(absolute_urls)
            print(f"Extracted {len(extracted_urls)} URLs from {paginated_url}")
            page += 1

    return fighter_urls

async def extract_fighter_profiles(url):
    extraction_strategy = JsonCssExtractionStrategy(schema_profiles, verbose=True)
    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        extraction_strategy=extraction_strategy,
        excluded_tags=["script", "style"]
    )
 
    fighter_data = []

    async with AsyncWebCrawler(verbose=True) as crawler:
        print(f"url: {url}")
        result = await crawler.arun(url=url, config=config)

        if not result.success:
            print(f"Crawl failed for {url}: {result.error_message}")
            return

        data = json.loads(result.extracted_content)
        if data:
            fighter_data.append(data[0])
            print(f"Extracted data for {url}")
            return fighter_data

    return fighter_data

async def main():
    base_urls = [
        "https://www.tapology.com/search/mma-fighters-by-weight-class/Atomweight-105-pounds",
    ]

    # 1. Extract urls 
    # fighter_urls = await extract_fighter_urls(base_urls[0])
    # print(f"Total fighter URLs extracted: {len(fighter_urls)}")

    # if not fighter_urls:
    #     print("No fighter URLs found, exiting.")
    #     return
    
    profiles = []
    fighter_urls = [
        "https://www.tapology.com/fightcenter/fighters/15069-dustin-jacoby"
    ]

    # 2. For each urls, extract fighter data 
    for url in fighter_urls:
        fighter_profil = await extract_fighter_profiles(url)

        print(fighter_profil)

        # for fight in fighter_profil[0]["Fights"]:
        #     if fight["result"] != "":
        #         year = fight["year"]
        #         month_day = fight["monthDay"]
                
        #         last_fight_str = f"{month_day} {year}"
        #         last_fight_obj = datetime.strptime(last_fight_str, "%b %d %Y")

        #         fighter_profil[0]["Basic Infos"][0]["last_fight_date"] = last_fight_obj.isoformat()
        #         break

        profiles.append(fighter_profil)

        print(f"Total fighter profiles extracted: {len(profiles)}")

    total_fights = len([fight for fight in fighter_profil[0]["Fights"] if fight["result"] != ""])

    data_to_insert = []

    for fighter in profiles:
        fighter_info = fighter[0]['Basic Infos'][0]
        
        unique_data = (
            f"{fighter_info['pro_mma_record']}{fighter_info['weight_class']}{fighter_info['affiliation']}"
            f"{total_fights}"
        )
        
        profile_hash = hashlib.sha256(unique_data.encode()).hexdigest()

        data_to_insert.append({
            'name': fighter_info['name'],
            'nickname': fighter_info['nickname'],
            'age': fighter_info['age'],
            'date_of_birth': None if fighter_info['date_of_birth'] == 'N/A' else fighter_info['date_of_birth'],
            'height': fighter_info.get('height', ''),  # Use .get() with a default value
            'weight_class': fighter_info.get('weight_class', ''),
            'last_weight_in': fighter_info.get('last_weight_in', ''),
            'born': fighter_info.get('born', ''),
            'head_coach': fighter_info.get('head_coach', ''),
            'other_coaches': fighter_info.get('other_coaches', ''),
            'pro_mma_record': fighter_info.get('pro_mma_record', ''),
            'current_mma_streak': fighter_info.get('current_mma_streak', ''),
            'affiliation': fighter_info.get('affiliation', ''),
            'last_fight_date': fighter_info.get('last_fight_date', ''),
            'total_fights': total_fights,
            'profile_hash': profile_hash,
        })
    
    print(data_to_insert)

    # 3. Insert data
    load_dotenv()
    url: str = os.getenv("SUPABASE_URL")
    key: str = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(url, key)

    insert_data(supabase, 'fighters', data_to_insert)

if __name__ == "__main__":
    asyncio.run(main())