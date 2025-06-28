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
from urllib.parse import urljoin
from datetime import datetime

logger = logging.getLogger(__name__)

def parse_listing_date(date_str: str) -> Optional[datetime]:
    """Parse date using pendulum library with improved error handling."""
    if not date_str:
        return None
        
    # Clean the input
    clean_date = re.sub(r'\s{2,}', ' ', date_str.strip())
    clean_date = re.sub(r'\s*,\s*', ', ', clean_date)
    clean_date = re.sub(r'\s+ET$', '', clean_date, flags=re.IGNORECASE)
    
    # Debug: print what we're trying to parse
    # print(f"Attempting to parse: '{clean_date}'")
    
    try:
        # First, try to parse as-is with explicit timezone
        parsed = pendulum.parse(clean_date, tz='America/New_York')
        return parsed.in_timezone('UTC')
        
    except Exception as e:
        # Try without timezone specification first
        try:
            parsed = pendulum.parse(clean_date)
            # Then set timezone
            parsed = parsed.in_timezone('America/New_York')
            return parsed.in_timezone('UTC')
            
        except Exception as e2:
            # If that fails, try adding the current year for partial dates
            current_year = pendulum.now().year
            
            # Check if year is missing (common patterns)
            if not re.search(r'\b(19|20)\d{2}\b', clean_date):
                try:
                    date_with_year = f"{clean_date}, {current_year}"
                    parsed = pendulum.parse(date_with_year)
                    parsed = parsed.in_timezone('America/New_York')
                    
                    # If the parsed date is more than 6 months in the past, try next year
                    now = pendulum.now('America/New_York')
                    if parsed < now.subtract(months=6):
                        date_with_year = f"{clean_date}, {current_year + 1}"
                        parsed = pendulum.parse(date_with_year)
                        parsed = parsed.in_timezone('America/New_York')
                    
                    return parsed.in_timezone('UTC')
                    
                except Exception as e3:
                    pass
                    
            # Last resort: try manual parsing for common patterns
            return _manual_parse_fallback(clean_date)

def _manual_parse_fallback(date_str: str) -> Optional[datetime]:
    """Manual parsing fallback for common date patterns."""
    # print(f"Attempting manual fallback for: '{date_str}'")
    
    # Enhanced patterns that capture time
    patterns = [
        r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',                                          # "January 18, 2025"
        r'(\d{4})\s+(\w+)\s+(\d{1,2})',                                           # "1999 May 04"
        # FIXED: This pattern now captures the time portion
        r'(?:\w+\s+)?(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s+at\s+(\d{1,2}):(\d{2})\s+([AP]M))?',  # "Saturday 06.28.2025 at 06:30 PM"
        r'(?:\w+\s+)?(\w+)\s+(\d{1,2}),\s*(?:(\d{1,2})(?::(\d{2}))?([ap]m),\s*)?(\d{4})',     # "Sat Sep 13, 6pm, 2025"
        r'(\w+)\s+(\d{1,2}),\s*(?:(\d{1,2})(?::(\d{2}))?([ap]m),\s*)?(\d{4})',               # "Aug 16, 6pm, 2025" 
        r'(\d{1,2})/(\d{1,2})/(\d{4})',                                            # "1/18/2025"
        r'(\d{4})-(\d{1,2})-(\d{1,2})',                                            # "2025-01-18"
        r'(\w+)\s+(\d{1,2})',                                                      # "January 18" (no year)
    ]
    
    # Comprehensive month mapping
    month_map = {
        # Full names
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12,
        # Common abbreviations
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
        'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
        'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    
    for pattern in patterns:
        match = re.search(pattern, date_str)
        if match:
            try:
                groups = match.groups()
                # print(f"Pattern matched: groups: {groups}")
                
                if pattern == patterns[0]:  # Month name format
                    month_name, day, year = groups
                    month = month_map.get(month_name.lower())
                    if month:
                        parsed = pendulum.datetime(int(year), month, int(day), tz='America/New_York')
                        return parsed.in_timezone('UTC')
                        
                elif pattern == patterns[1]:  # YYYY Month DD
                    year, month_name, day = groups
                    month = month_map.get(month_name.lower())
                    if month:
                        parsed = pendulum.datetime(int(year), month, int(day), tz='America/New_York')
                        return parsed.in_timezone('UTC')
                        
                elif pattern == patterns[2]:  # MM.DD.YYYY format with optional time
                    month, day, year, hour, minute, ampm = groups
                    
                    # Default to midnight if no time specified
                    hour_24 = 0
                    minute_val = 0
                    
                    # Parse time if provided
                    if hour and ampm:
                        hour_val = int(hour)
                        minute_val = int(minute) if minute else 0
                        
                        # Convert to 24-hour format
                        if ampm.upper() == 'PM' and hour_val != 12:
                            hour_24 = hour_val + 12
                        elif ampm.upper() == 'AM' and hour_val == 12:
                            hour_24 = 0
                        else:
                            hour_24 = hour_val
                    
                    parsed = pendulum.datetime(int(year), int(month), int(day), 
                                             hour_24, minute_val, tz='America/New_York')
                    # print(f"Manual parse successful with time: {parsed}")
                    return parsed.in_timezone('UTC')
                
                elif pattern == patterns[3] or pattern == patterns[4]:  # Month name with time
                    if pattern == patterns[3]:  # "Sat Sep 13, 6pm, 2025"
                        month_name, day, hour, minute, ampm, year = groups
                    else:  # "Aug 16, 6pm, 2025"
                        month_name, day, hour, minute, ampm, year = groups
                    
                    month = month_map.get(month_name.lower())
                    if month:
                        # Default to midnight if no time
                        hour_24 = 0
                        minute_val = 0
                        
                        # Parse time if provided
                        if hour and ampm:
                            hour_val = int(hour)
                            minute_val = int(minute) if minute else 0
                            
                            # Convert to 24-hour format
                            if ampm.lower() == 'pm' and hour_val != 12:
                                hour_24 = hour_val + 12
                            elif ampm.lower() == 'am' and hour_val == 12:
                                hour_24 = 0
                            else:
                                hour_24 = hour_val
                        
                        parsed = pendulum.datetime(int(year), month, int(day),
                                                 hour_24, minute_val, tz='America/New_York')
                        return parsed.in_timezone('UTC')
                
                elif pattern == patterns[5]:  # MM/DD/YYYY
                    month, day, year = groups
                    parsed = pendulum.datetime(int(year), int(month), int(day), tz='America/New_York')
                    return parsed.in_timezone('UTC')
                
                elif pattern == patterns[6]:  # YYYY-MM-DD
                    year, month, day = groups
                    parsed = pendulum.datetime(int(year), int(month), int(day), tz='America/New_York')
                    return parsed.in_timezone('UTC')
                
                elif pattern == patterns[7] and len(groups) == 2:  # No year provided
                    month_name, day = groups
                    month = month_map.get(month_name.lower())
                    if month:
                        current_year = pendulum.now().year
                        parsed = pendulum.datetime(current_year, month, int(day), tz='America/New_York')
                        
                        # Adjust year if date seems too far in the past
                        now = pendulum.now('America/New_York')
                        if parsed < now.subtract(months=6):
                            parsed = pendulum.datetime(current_year + 1, month, int(day), tz='America/New_York')
                        
                        return parsed.in_timezone('UTC')
                        
            except Exception as e:
                # print(f"Manual parse attempt failed: {e}")
                continue
    
    # print(f"All parsing attempts failed for: '{date_str}'")
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
    
async def get_or_create_fighter(self, fighter_url: str, fighter_name: str) -> Optional[int]:
    """Get fighter ID or create new fighter if doesn't exist"""
    if not fighter_url:
        return None
        
    # Check if fighter exists
    existing_fighter = self.db.get_fighter_by_url(urljoin(self.config.base_url, fighter_url))
    if existing_fighter:
        return existing_fighter['id']
    
    # Create new fighter
    logger.info(f"🆕 Creating new fighter: {fighter_name}")
    full_url = urljoin(self.config.base_url, fighter_url)
    
    # Try to get fighter profile data
    fighter_data = await self.scraper.extract_data(full_url, self.schema_profiles)
    
    # Initialize fighter record with basic data
    fighter_record = {
        'tapology_url': full_url,
        'name': fighter_name,
        'needs_update': False,  # Set default
        'created_at': datetime.now(pytz.UTC).isoformat()
    }
    
    # Add comprehensive profile data if available
    if fighter_data and len(fighter_data) > 0 and fighter_data[0].get('Basic Infos'):
        try:
            basic_info = fighter_data[0]['Basic Infos'][0]
            
            # Map all available fields from the schema to database columns
            # Extract height in centimeters if available
            height_raw = basic_info.get('height')
            height_cm = None
            if height_raw:
                match = re.search(r'\((\d+)\s*cm\)', height_raw)
                if match:
                    height_cm = f"{match.group(1)}cm"
                else:
                    height_cm = height_raw  # fallback to original if no cm found

            # Convert last_weight_in from lbs to kg if present
            last_weight_in_kg = None
            last_weight_in_raw = basic_info.get('last_weight_in')
            if last_weight_in_raw:
                match = re.match(r'([\d.]+)\s*lbs', last_weight_in_raw, re.IGNORECASE)
                if match:
                    lbs = float(match.group(1))
                    last_weight_in_kg = round(lbs * 0.45359237, 1)
                else:
                    last_weight_in_kg = last_weight_in_raw  # fallback to original if not in expected format

            fighter_record.update({
                'nickname': '' if basic_info.get('nickname') == 'N/A' else basic_info.get('nickname'),
                'age': basic_info.get('age'),
                'date_of_birth': parse_listing_date(basic_info.get('date_of_birth')).isoformat(),
                'height': height_cm,
                'weight_class': basic_info.get('weight_class'),
                'last_weight_in': last_weight_in_kg,
                'last_fight_date': parse_listing_date(basic_info.get('last_fight_date')).isoformat(),
                'born': basic_info.get('born'),
                'head_coach': '' if basic_info.get('head_coach') == 'N/A' else basic_info.get('head_coach'),
                'pro_mma_record': normalize_record(basic_info.get('pro_mma_record')),
                'current_mma_streak': basic_info.get('current_mma_streak'),
                'affiliation': '' if basic_info.get('affiliation') == 'N/A' else basic_info.get('affiliation'),
                'other_coaches': '' if basic_info.get('other_coaches') == 'N/A' else basic_info.get('other_coaches'),
                'hash': calculate_hash(fighter_data)
            })
            
            # Add profile image URL if available
            if fighter_data[0]['profile_img_url']:
                fighter_record['profile_img_url'] = fighter_data[0]['profile_img_url']
            
            # Calculate total fights from the record if available
            if basic_info.get('pro_mma_record'):
                fighter_record['total_fights'] = calculate_total_fights(self, basic_info['pro_mma_record'])
                
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"⚠️ Could not parse complete fighter profile for {fighter_name}: {str(e)}")
    else:
        logger.warning(f"⚠️ No profile data available for {fighter_name}, creating with basic info only")
    
    try:
        result = self.db.create_fighter(fighter_record)
        return result['id'] if result else None
    except Exception as e:
        logger.error(f"❌ Failed to create fighter {fighter_name}: {str(e)}")
        return None

def calculate_hash(data) -> str:
    """Calculate hash for change detection"""
    import json
    import hashlib
    json_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()

# Normalize pro_mma_record to "Win-Loss-Draw-NoContest" format
def normalize_record(record_str):
    if not record_str:
        return None
    # Extract main record and NC if present
    match = re.match(r'(\d+)-(\d+)-(\d+)(?:,\s*(\d+)\s*NC)?', record_str)
    if match:
        win, loss, draw, nc = match.groups()
        if nc:
            return f"{win}-{loss}-{draw}-{nc}"
        else:
            return f"{win}-{loss}-{draw}"
    # Fallback: just return original
    return record_str

def parse_result(val):
    if isinstance(val, str) and val and not any(char.isalpha() for char in val):
        # Looks like a record (e.g., "16-0"), not a result
        return None
    return val