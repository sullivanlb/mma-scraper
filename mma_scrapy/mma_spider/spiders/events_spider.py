import scrapy
from datetime import datetime, timedelta
import pytz
from ..items import EventItem, FightItem
from ..utils import parse_listing_date, calculate_hash
import logging

class EventsSpider(scrapy.Spider):
    name = "events"
    allowed_domains = ["tapology.com"]
    base_url = "https://www.tapology.com"
    
    # Default start URL, can be overridden
    start_urls = ["https://www.tapology.com/fightcenter?page=1"]

    def __init__(self, mode='recent', days_offset=7, *args, **kwargs):
        super(EventsSpider, self).__init__(*args, **kwargs)
        self.mode = mode
        self.days_offset = int(days_offset)
        
        now = datetime.now(pytz.UTC)
        self.start_date = None
        self.end_date = None
        
        if mode == 'recent':
            self.start_date = now - timedelta(days=self.days_offset)
            self.end_date = now + timedelta(days=self.days_offset)
            logging.info(f"Targeting events between {self.start_date.date()} and {self.end_date.date()}")
        elif mode == 'upcoming':
            self.start_date = now
            logging.info("Targeting all upcoming events")
        
        # For 'all', no limits
        
    def parse(self, response):
        # 1. Scrape Event List
        events = response.css('div.promotion')
        if not events:
            logging.info("No events found on this page.")
            return

        events_processed_on_page = 0
        
        for event in events:
            url_rel = event.css('a[href^="/fightcenter/events/"]::attr(href)').get()
            date_str = event.css('span.hidden.md\\:inline::text').get() # Removed :not(:has(a)) simplification
            
            if not url_rel:
                continue

            event_date = parse_listing_date(date_str)
            
            # Filters
            if event_date:
                if self.end_date and event_date > self.end_date:
                    continue
                if self.start_date and event_date < self.start_date:
                    # If the page is ordered chronologically (upcoming -> far future), seeing older dates is unexpected
                    # But if it's ordered reverse (past), then < start_date means we can stop?
                    # Tapology fightcenter mixes or has specific ordering.
                    # For safety, we just skip the item but continue the page for now.
                    continue
            
            events_processed_on_page += 1
            yield response.follow(url_rel, callback=self.parse_event)

        # 2. Pagination
        # Logic from original scraping: continue unless we are out of range.
        # Simple infinite scroll simulation:
        current_page = 1
        if 'page=' in response.url:
            current_page = int(response.url.split('page=')[1])
        
        next_page = current_page + 1
        # Stop condition optimization could be added here if we knew the sort order strictly.
        # For now, just go to next page if we found any events or if we are just starting.
        # But to prevent infinite loops on empty pages (if tapology doesn't 404):
        if events_processed_on_page > 0:
            yield scrapy.Request(f"{self.base_url}/fightcenter?page={next_page}", callback=self.parse)
            
    def parse_event(self, response):
        # Extract Header Info
        header = response.css('#primaryDetailsContainer')
        
        # Helper for extracting text with label
        def get_header_field(label):
            # XPath: Find label span, get following sibling span's text (or link text)
            # //span[contains(text(), "Label:")]/following-sibling::span//text()
            return response.xpath(f'//div[@id="primaryDetailsContainer"]//ul//span[contains(text(), "{label}")]/following-sibling::span//text()').get()
        
        # Specific for promotion which is a link
        promotion = response.xpath('//div[@id="primaryDetailsContainer"]//ul//span[contains(text(), "Promotion:")]/following-sibling::span//a/text()').get() or \
                    response.xpath('//div[@id="primaryDetailsContainer"]//ul//span[contains(text(), "Promotion:")]/following-sibling::span/text()').get()

        datetime_str = get_header_field("Date/Time:")
        
        event_item = EventItem()
        event_item['tapology_url'] = response.url
        event_item['name'] = response.css('#eventPageMobilePromotionIcon + h2::text').get() or response.xpath('//h2/text()').get()
        event_item['datetime'] = parse_listing_date(datetime_str).isoformat() if parse_listing_date(datetime_str) else None
        event_item['broadcast'] = get_header_field("U.S. Broadcast:")
        event_item['promotion'] = promotion
        event_item['venue'] = get_header_field("Venue:")
        event_item['location'] = response.xpath('//div[@id="primaryDetailsContainer"]//ul//span[contains(text(), "Location:")]/following-sibling::span//a/text()').get()
        event_item['mma_bouts'] = get_header_field("MMA Bouts:")
        event_item['img_url'] = header.css('div:first-child img::attr(src)').get()
        
        event_item['hash'] = calculate_hash(event_item)
        
        yield event_item
        
        # Extract Fights
        fight_rows = response.css('#sectionFightCard > ul li')
        for fight in fight_rows:
            fight_item = FightItem()
            fight_item['event_tapology_url'] = response.url
            
            # Fight Details
            fight_item['fight_type'] = fight.css('div.flex.flex-col.rounded.text-tap_darkgold span.uppercase.font-bold a::text').get()
            
            # Finish details (regex parsing replacement)
            finish_text = fight.css('div.w-full.md\\:w-\\[756px\\] span.uppercase.text-sm::text').get()
            if finish_text:
                parts = finish_text.split(',', 1)
                fight_item['finish_by'] = parts[0].strip()
                fight_item['finish_by_details'] = parts[1].strip() if len(parts) > 1 else None
            
            # Rounds
            rounds_text = fight.css('div.flex.flex-col.rounded.text-tap_darkgold div.text-xs11::text').get()
            if rounds_text:
                # e.g "3 x 5"
                import re
                m = re.match(r'(\d+) x (\d+)', rounds_text)
                if m:
                    fight_item['rounds'] = m.group(1)
                    fight_item['minutes_per_round'] = m.group(2)
            
            # Fighter 1 (Left)
            # Using CSS selectors from schema as guide, adapting for Scrapy
            # [id^="fighterBoutImage"]:nth-of-type(1) img
            fight_item['fighter_1_img'] = fight.css('[id^="fighterBoutImage"]:nth-of-type(1) img::attr(src)').get()
            fight_item['fighter_1_name'] = fight.css('[id^="boutFullsize"] [id$="leftBio"] a.link-primary-red::text').get()
            fight_item['fighter_1_url'] = response.urljoin(fight.css('[id^="boutFullsize"] [id$="leftBio"] a.link-primary-red::attr(href)').get())
            fight_item['fighter_1_result'] = fight.css('[id^="boutFullsize"] [id$="leftBio"] div[class*="bg-"] span::text').get()
            fight_item['fighter_1_title'] = fight.css('#fb0TitleMatchup::text').get()
            
            # Fighter 2 (Right)
            fight_item['fighter_2_img'] = fight.css('[id^="fighterBoutImage"]:nth-of-type(2) img::attr(src)').get()
            fight_item['fighter_2_name'] = fight.css('[id^="boutFullsize"] [id$="rightBio"] a.link-primary-red::text').get()
            fight_item['fighter_2_url'] = response.urljoin(fight.css('[id^="boutFullsize"] [id$="rightBio"] a.link-primary-red::attr(href)').get())
            fight_item['fighter_2_result'] = fight.css('[id^="boutFullsize"] [id$="rightBio"] div[class*="bg-"] span::text').get()
            fight_item['fighter_2_title'] = fight.css('#fb1TitleMatchup::text').get()
            
            yield fight_item
