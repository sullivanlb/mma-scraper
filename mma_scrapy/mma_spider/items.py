# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class EventItem(scrapy.Item):
    # Key
    tapology_url = scrapy.Field()

    # Header fields
    name = scrapy.Field()
    datetime = scrapy.Field()
    broadcast = scrapy.Field()
    promotion = scrapy.Field()
    venue = scrapy.Field()
    location = scrapy.Field()
    mma_bouts = scrapy.Field()
    img_url = scrapy.Field()

    # Hash for change detection
    hash = scrapy.Field()

class FightItem(scrapy.Item):
    event_tapology_url = scrapy.Field() # Link to event

    # Fight details
    fight_type = scrapy.Field()
    finish_by = scrapy.Field()
    finish_by_details = scrapy.Field()
    rounds = scrapy.Field()
    minutes_per_round = scrapy.Field()

    # Fighter 1
    fighter_1_name = scrapy.Field()
    fighter_1_url = scrapy.Field()
    fighter_1_img = scrapy.Field()
    fighter_1_result = scrapy.Field()
    fighter_1_title = scrapy.Field()
    fighter_1_small_img = scrapy.Field()

    # Fighter 2
    fighter_2_name = scrapy.Field()
    fighter_2_url = scrapy.Field()
    fighter_2_img = scrapy.Field()
    fighter_2_result = scrapy.Field()
    fighter_2_title = scrapy.Field()
    fighter_2_small_img = scrapy.Field()

class FighterItem(scrapy.Item):
    tapology_url = scrapy.Field()
    name = scrapy.Field()
    profile_img_url = scrapy.Field()

    # Basic Infos
    nickname = scrapy.Field()
    age = scrapy.Field()
    date_of_birth = scrapy.Field()
    height = scrapy.Field()
    weight_class = scrapy.Field()
    last_weight_in = scrapy.Field()
    last_fight_date = scrapy.Field()
    born = scrapy.Field()
    head_coach = scrapy.Field()
    pro_mma_record = scrapy.Field()
    current_mma_streak = scrapy.Field()
    affiliation = scrapy.Field()
    other_coaches = scrapy.Field()

    # Social/Links
    twitter = scrapy.Field()
    instagram = scrapy.Field()
    facebook = scrapy.Field()
    sherdog = scrapy.Field()
    wikipedia = scrapy.Field()
    mixedmartialarts = scrapy.Field()
    ufc = scrapy.Field()
    ufcstats = scrapy.Field()
    bestfightodds = scrapy.Field()

    # Logic for hash
    hash = scrapy.Field()

    # Lists (kept as raw or processed dicts)
    records = scrapy.Field()
    fights = scrapy.Field()
