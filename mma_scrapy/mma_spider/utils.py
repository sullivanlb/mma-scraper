import re
import pendulum
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)

def parse_listing_date(date_str: str) -> Optional[datetime]:
    """Parse date using pendulum library with improved error handling."""
    if not date_str:
        return None

    # Clean the input
    clean_date = re.sub(r'\s{2,}', ' ', date_str.strip())
    clean_date = re.sub(r'\s*,\s*', ', ', clean_date)
    clean_date = re.sub(r'\s+ET\b', '', clean_date, flags=re.IGNORECASE)

    # Prioritize manual parsing for tricky formats
    manual_result = _manual_parse_fallback(clean_date)
    if manual_result:
        return manual_result

    # Fallback to pendulum for standard formats
    try:
        # Try to parse as-is with explicit timezone
        parsed = pendulum.parse(clean_date, tz='America/New_York')
        return parsed.in_timezone('UTC')
    except Exception:
        # If that fails, try adding the current year for partial dates
        if not re.search(r'\b(19|20)\d{2}\b', clean_date):
            try:
                current_year = pendulum.now().year
                date_with_year = f"{clean_date}, {current_year}"
                parsed = pendulum.parse(date_with_year, tz='America/New_York')

                # If the parsed date is more than 6 months in the past, try next year
                now = pendulum.now('America/New_York')
                if parsed < now.subtract(months=6):
                    date_with_year = f"{clean_date}, {current_year + 1}"
                    parsed = pendulum.parse(date_with_year, tz='America/New_York')

                return parsed.in_timezone('UTC')
            except Exception:
                pass

    return None

def _manual_parse_fallback(date_str: str) -> Optional[datetime]:
    """Manual parsing fallback for common date patterns."""
    patterns = [
        r'(?:\w+,\s+)?(\w+)\s+(\d{1,2})\s+at\s+(\d{1,2}):(\d{2})\s+([AP]M)',
        r'(?:\w+,\s+)?(\w+)\s+(\d{1,2}),\s+(\d{1,2}):(\d{2})\s+([AP]M)',
        r'(?:\w+,\s+)?(\w+)\s+(\d{1,2})\s+at\s+(\d{1,2}):(\d{2})\s+([AP]M)\s+UTC',
        r'(?:\w+\s+)?(\w+)\s+(\d{1,2}),\s*(?:(\d{1,2})(?::(\d{2}))?([ap]m),\s*)?(\d{4})',
        r'(?:\w+\s+)?(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s+at\s+(\d{1,2}):(\d{2})\s+([AP]M))?',
        r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',
        r'(\d{4})-(\d{1,2})-(\d{1,2})',
        r'(\d{1,2})/(\d{1,2})/(\d{4})',
        r'(\w+)\s+(\d{1,2})',
    ]

    month_map = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
        'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6, 'jul': 7, 'aug': 8,
        'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }

    for i, pattern in enumerate(patterns):
        match = re.search(pattern, date_str, re.IGNORECASE)
        if not match:
            continue

        try:
            groups = match.groups()
            now = pendulum.now('America/New_York')
            parsed_date = None

            if i == 0:
                month_name, day, hour, minute, ampm = groups
                month = month_map.get(month_name.lower())
                if month:
                    hour, minute = int(hour), int(minute)
                    if ampm.upper() == 'PM' and hour != 12: hour += 12
                    if ampm.upper() == 'AM' and hour == 12: hour = 0
                    parsed_date = pendulum.datetime(now.year, month, int(day), hour, minute, tz='America/New_York')
                    if parsed_date < now.subtract(months=6): parsed_date = parsed_date.add(years=1)
            elif i == 1:
                month_name, day, hour, minute, ampm = groups
                month = month_map.get(month_name.lower())
                if month:
                    hour, minute = int(hour), int(minute)
                    if ampm.upper() == 'PM' and hour != 12: hour += 12
                    if ampm.upper() == 'AM' and hour == 12: hour = 0
                    parsed_date = pendulum.datetime(now.year, month, int(day), hour, minute, tz='America/New_York')
                    if parsed_date < now.subtract(months=6): parsed_date = parsed_date.add(years=1)
            elif i == 2:
                month_name, day, hour, minute, ampm = groups
                month = month_map.get(month_name.lower())
                if month:
                    hour, minute = int(hour), int(minute)
                    if ampm.upper() == 'PM' and hour != 12: hour += 12
                    if ampm.upper() == 'AM' and hour == 12: hour = 0
                    parsed_date = pendulum.datetime(now.year, month, int(day), hour, minute, tz='UTC')
                    if parsed_date < pendulum.now('UTC').subtract(months=6): parsed_date = parsed_date.add(years=1)
                    return parsed_date
            elif i == 3:
                month_name, day, hour, minute, ampm, year = groups
                month = month_map.get(month_name.lower())
                if month:
                    hour_24, minute_val = 0, 0
                    if hour and ampm:
                        hour_val = int(hour)
                        minute_val = int(minute) if minute else 0
                        if ampm.lower() == 'pm' and hour_val != 12: hour_24 = hour_val + 12
                        elif ampm.lower() == 'am' and hour_val == 12: hour_24 = 0
                        else: hour_24 = hour_val
                    parsed_date = pendulum.datetime(int(year), month, int(day), hour_24, minute_val, tz='America/New_York')
            elif i == 4:
                month_str, day, year, hour, minute, ampm = groups
                month = int(month_str)
                hour_24, minute_val = 0, 0
                if hour and ampm:
                    hour_val = int(hour)
                    minute_val = int(minute) if minute else 0
                    if ampm.upper() == 'PM' and hour_val != 12: hour_24 = hour_val + 12
                    elif ampm.upper() == 'AM' and hour_val == 12: hour_24 = 0
                    else: hour_24 = hour_val
                parsed_date = pendulum.datetime(int(year), month, int(day), hour_24, minute_val, tz='America/New_York')
            elif i == 5:
                month_name, day, year = groups
                month = month_map.get(month_name.lower())
                if month:
                    parsed_date = pendulum.datetime(int(year), month, int(day), tz='America/New_York')
            elif i == 6:
                year, month, day = groups
                parsed_date = pendulum.datetime(int(year), int(month), int(day), tz='America/New_York')
            elif i == 7:
                month, day, year = groups
                parsed_date = pendulum.datetime(int(year), int(month), int(day), tz='America/New_York')
            elif i == 8:
                month_name, day = groups
                month = month_map.get(month_name.lower())
                if month:
                    parsed_date = pendulum.datetime(now.year, month, int(day), tz='America/New_York')
                    if parsed_date < now.subtract(months=6):
                        parsed_date = parsed_date.add(years=1)

            if parsed_date:
                return parsed_date.in_timezone('UTC')

        except (ValueError, TypeError) as e:
            continue

    return None

def normalize_record(record_str):
    if not record_str:
        return None
    match = re.match(r'(\d+)-(\d+)-(\d+)(?:,\s*(\d+)\s*NC)?', record_str)
    if match:
        win, loss, draw, nc = match.groups()
        if nc:
            return f"{win}-{loss}-{draw}-{nc}"
        else:
            return f"{win}-{loss}-{draw}"
    return record_str

def calculate_hash(data) -> str:
    import json
    import hashlib
    # If data is a dict or list, dump it. If it's a scrapy Item, convert to dict.
    if hasattr(data, 'adapter'): # Scrapy ItemAdapter or Item
        from itemadapter import ItemAdapter
        data = ItemAdapter(data).asdict()

    json_str = json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()
