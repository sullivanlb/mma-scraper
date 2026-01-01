import os
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
# Ensure project root is in path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mma_spider.spiders.events_spider import EventsSpider

def main():
    # Set up settings
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'mma_spider.settings')
    settings = get_project_settings()

    process = CrawlerProcess(settings)

    # Run the spider
    print("🚀 Starting Scrapy Spider for Recent Events...")
    process.crawl(EventsSpider, mode='recent', days_offset=7)
    process.start()
    print("✅ Spider Finished")

if __name__ == '__main__':
    main()
