import scrapy
from ..items import FighterItem
from ..database import Database
from ..utils import calculate_hash, parse_listing_date
import logging
import re

class FightersSpider(scrapy.Spider):
    name = "fighters"
    allowed_domains = ["tapology.com"]

    def start_requests(self):
        # We need to manually instantiate DB because pipeline hasn't run yet or we want specific query
        db = Database(self.settings.get('SUPABASE_URL'), self.settings.get('SUPABASE_KEY'))
        fighters = db.get_fighters_to_update()

        logging.info(f"Found {len(fighters)} fighters marked for update.")
        for fighter in fighters:
             # Add random delay or just let Scrapy handle concurrency
             yield scrapy.Request(fighter['tapology_url'], callback=self.parse)

    def parse(self, response):
        def get_field(label):
             val = response.xpath(f'//div//strong[contains(text(), "{label}")]/following-sibling::span/text()').get()
             return val.strip() if val else None

        item = FighterItem()
        item['tapology_url'] = response.url

        # Basic Infos
        item['profile_img_url'] = response.css('img[src^="https://images.tapology.com/letterbox_images/"]::attr(src)').get()
        item['name'] = get_field("Given Name:") or get_field("Name:")
        item['nickname'] = get_field("Nickname:")
        item['age'] = get_field("Age:")

        dob = get_field("Date of Birth:")
        item['date_of_birth'] = parse_listing_date(dob).isoformat() if dob else None

        # Height
        height_str = get_field("Height:")
        item['height'] = height_str
        if height_str:
            m = re.search(r'\((\d+)\s*cm\)', height_str)
            if m:
                item['height'] = f"{m.group(1)}cm"

        item['weight_class'] = get_field("Weight Class:")

        lwi = get_field("Last Weigh-In:")
        item['last_weight_in'] = lwi
        if lwi:
             m = re.match(r'([\d.]+)\s*lbs', lwi, re.IGNORECASE)
             if m:
                 lbs = float(m.group(1))
                 item['last_weight_in'] = round(lbs * 0.45359237, 1)

        last_fight = get_field("Last Fight:")
        item['last_fight_date'] = parse_listing_date(last_fight).isoformat() if last_fight else None

        item['born'] = get_field("Born:")
        item['head_coach'] = get_field("Head Coach:")
        item['pro_mma_record'] = get_field("Pro MMA Record:") # Should normalize
        item['current_mma_streak'] = get_field("Current MMA Streak:")
        item['affiliation'] = get_field("Affiliation:")
        item['other_coaches'] = get_field("Other Coaches:")

        # Links
        def get_link(prefix):
            return response.xpath(f'//strong[contains(text(), "Links:")]/following-sibling::div//a[starts-with(@href, "{prefix}")]/@href').get()

        item['twitter'] = get_link("https://twitter.com/") or get_link("https://www.twitter.com/")
        item['instagram'] = get_link("https://instagram.com/")
        item['tapology_url'] = response.url

        # Hash
        item['hash'] = calculate_hash(item)

        yield item
