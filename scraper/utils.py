# =================================================================
# scraper/utils.py - Utility functions
# =================================================================

import re
from datetime import datetime
import pytz
import logging

logger = logging.getLogger(__name__)

def format_date(date_to_format):
    """Format date string to standardized format"""
    if not date_to_format or date_to_format == "N/A":
        return None

    cleaned_date = re.sub(
        r"(\n.*|inUFC|ET|PT|CT|MT|UTC|GMT| at )",
        "", 
        date_to_format.strip()
    ).strip()

    formats = [
        "%B %d, %Y %I:%M %p",
        "%B %d, %Y",
        "%A %m.%d.%Y %I:%M %p",
        "%Y-%m-%dT%H:%M:%S%z"
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned_date, fmt)
            return dt.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S%z")
        except ValueError:
            continue

    logger.warning(f"Failed to parse date: {date_to_format}")
    return None

def parse_listing_date(date_str: str) -> datetime:
    """Parse all date formats found in Tapology listings into a UTC datetime."""
    # Normalize whitespace and remove extra commas
    clean_date = re.sub(r'\s{2,}', ' ', date_str.strip()).replace(',', '').strip()
    
    # Define all possible date formats in order of specificity
    formats = [
        # Formats including year and full month name
        '%B %d %Y %I%p ET',              # "September 28 2024 6pm ET"
        '%B %d %Y %I%p',                 # "September 28 2024 6pm"
        '%B %d %Y %H:%M',                # "September 28 2024 18:00"
        '%B %d %Y',                      # "September 28 2024"
        '%a %B %d %Y %I%p ET',           # "Sat September 28 2024 6pm ET"
        '%a %B %d %Y %I%M%p ET',         # "Sat September 28 2024 6:30pm ET"
        
        # Formats including year with abbreviated month
        '%a %b %d %Y %I%p ET',          # "Sat Sep 28 2024 6pm ET"
        '%a %b %d %Y %I%M%p ET',        # "Sat Sep 28 2024 6:30pm ET"
        '%b %d %Y %I%p ET',             # "Sep 28 2024 6pm ET"
        '%b %d %Y %I%p',                # "Sep 28 2024 6pm"
        '%b %d %Y %H:%M',               # "Sep 28 2024 18:00"
        
        # Formats without year (require appending current year)
        '%a %b %d %I%p ET',             # "Sat Sep 28 6pm ET"
        '%a %b %d %I%M%p ET',           # "Sat Sep 28 6:30pm ET"
        '%b %d %I%p ET',                # "Sep 28 6pm ET"
        '%b %d %I%p',                   # "Sep 28 6pm"
        '%b %d %H:%M',                  # "Sep 28 18:00"
    ]
    
    current_year = datetime.now().year
    us_eastern = pytz.timezone('US/Eastern')
    
    # First, attempt to parse formats that include the year
    for fmt in formats:
        try:
            dt = datetime.strptime(clean_date, fmt)
            localized_dt = us_eastern.localize(dt) if '%Y' in fmt else us_eastern.localize(dt.replace(year=current_year))
            return localized_dt.astimezone(pytz.UTC)
        except ValueError:
            continue
    
    # Fallback: try appending current_year to formats without year
    formats_without_year = [
        '%a %b %d %I%p ET',
        '%a %b %d %I%M%p ET',
        '%b %d %I%p ET',
        '%b %d %I%p',
        '%b %d %H:%M',
    ]
    for fmt in formats_without_year:
        try:
            dt = datetime.strptime(f"{clean_date} {current_year}", f"{fmt} %Y")
            localized_dt = us_eastern.localize(dt)
            return localized_dt.astimezone(pytz.UTC)
        except ValueError:
            continue
    
    logger.warning(f"Failed to parse date: {date_str}")
    return None