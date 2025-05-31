# =================================================================
# scraper/utils.py - Utility functions
# =================================================================

import re
from datetime import datetime
from typing import Optional
import pytz
import logging
import re
import pendulum
from datetime import datetime

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

    logger.warning(f"format_date(): Failed to parse date: {date_to_format}")
    return None

def parse_listing_date(date_str: str) -> Optional[datetime]:
    """Parse date using pendulum library with improved year handling."""
    if not date_str:
        return None
        
    # Clean the input
    clean_date = re.sub(r'\s{2,}', ' ', date_str.strip())
    clean_date = re.sub(r'\s*,\s*', ', ', clean_date)
    clean_date = re.sub(r'\s+ET$', '', clean_date, flags=re.IGNORECASE)
    
    try:
        # First, try to parse as-is
        parsed = pendulum.parse(clean_date, tz='America/New_York', strict=False)
        return parsed.in_timezone('UTC')
        
    except Exception:
        # If that fails, try adding the current year
        current_year = pendulum.now().year
        
        # Try adding current year
        try:
            date_with_year = f"{clean_date}, {current_year}"
            parsed = pendulum.parse(date_with_year, tz='America/New_York', strict=False)
            
            # If the parsed date is more than 6 months in the past, try next year
            now = pendulum.now('America/New_York')
            if parsed < now.subtract(months=6):
                date_with_year = f"{clean_date}, {current_year + 1}"
                parsed = pendulum.parse(date_with_year, tz='America/New_York', strict=False)
            
            return parsed.in_timezone('UTC')
            
        except Exception:
            # Last resort: try manual parsing for common patterns
            return _manual_parse_fallback(clean_date)

def _manual_parse_fallback(date_str: str) -> Optional[datetime]:
    """Manual fallback parser for common date patterns."""
    
    # Pattern: "Mon DD, HH:mm[am/pm]" or "Mon DD, HHam/pm"
    pattern1 = r'(\w{3})\s+(\d{1,2}),\s+(\d{1,2}):?(\d{0,2})\s*(am|pm)?'
    match = re.match(pattern1, date_str, re.IGNORECASE)
    
    if match:
        month_str, day_str, hour_str, minute_str, ampm = match.groups()
        
        try:
            # Convert month name to number
            month_map = {
                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
            }
            month = month_map.get(month_str.lower()[:3])
            if not month:
                raise ValueError(f"Unknown month: {month_str}")
            
            day = int(day_str)
            hour = int(hour_str)
            minute = int(minute_str) if minute_str else 0
            
            # Handle am/pm
            if ampm:
                if ampm.lower() == 'pm' and hour != 12:
                    hour += 12
                elif ampm.lower() == 'am' and hour == 12:
                    hour = 0
            
            # Determine year (current or next)
            current_year = pendulum.now().year
            try:
                # Try current year first
                dt = pendulum.datetime(current_year, month, day, hour, minute, 
                                     tz='America/New_York')
                
                # If it's more than 6 months in the past, use next year
                if dt < pendulum.now('America/New_York').subtract(months=6):
                    dt = pendulum.datetime(current_year + 1, month, day, hour, minute, 
                                         tz='America/New_York')
                
                return dt.in_timezone('UTC')
                
            except ValueError:
                # Try next year if current year fails (e.g., Feb 29 on non-leap year)
                dt = pendulum.datetime(current_year + 1, month, day, hour, minute, 
                                     tz='America/New_York')
                return dt.in_timezone('UTC')
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Manual parse failed for '{date_str}': {e}")
            return None
    
    logger.warning(f"parse_listing_date(): Failed to parse date: {date_str}")
    return None

def calculate_total_fights(self, record_str: str) -> Optional[int]:
    """Calculate total fights from record string like '20-3-1' """
    if not record_str:
        return None
    try:
        # Split by '-' and sum all numbers (wins-losses-draws)
        parts = record_str.split('-')
        return sum(int(part) for part in parts if part.isdigit())
    except (ValueError, AttributeError):
        return None