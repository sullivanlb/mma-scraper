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
        # print(f"Successfully parsed: {parsed}")
        return parsed.in_timezone('UTC')
        
    except Exception as e:
        # print(f"First parse attempt failed: {e}")
        
        # Try without timezone specification first
        try:
            parsed = pendulum.parse(clean_date)
            # Then set timezone
            parsed = parsed.in_timezone('America/New_York')
            # print(f"Successfully parsed without initial timezone: {parsed}")
            return parsed.in_timezone('UTC')
            
        except Exception as e2:
            # print(f"Second parse attempt failed: {e2}")
            
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
                    
                    # print(f"Successfully parsed with year adjustment: {parsed}")
                    return parsed.in_timezone('UTC')
                    
                except Exception as e3:
                    # print(f"Year adjustment failed: {e3}")
                    pass
                    
            # Last resort: try manual parsing for common patterns
            return _manual_parse_fallback(clean_date)

def _manual_parse_fallback(date_str: str) -> Optional[datetime]:
    """Manual parsing fallback for common date patterns."""
    # print(f"Attempting manual fallback for: '{date_str}'")
    
    # Common patterns to try
    patterns = [
        r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',                                          # "January 18, 2025" or "Aug 17, 2024"
        r'(?:\w+\s+)?(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s+at\s+.*)?',               # "Saturday 05.31.2025 at 06:30 PM"
        r'(?:\w+\s+)?(\w+)\s+(\d{1,2}),\s*(?:\d{1,2}(?::\d{2})?[ap]m,\s*)?(\d{4})', # "Sat Sep 13, 6pm, 2025"
        r'(\w+)\s+(\d{1,2}),\s*(?:\d{1,2}(?::\d{2})?[ap]m,\s*)?(\d{4})',         # "Aug 16, 6pm, 2025"
        r'(\d{1,2})/(\d{1,2})/(\d{4})',                                            # "1/18/2025"
        r'(\d{4})-(\d{1,2})-(\d{1,2})',                                            # "2025-01-18"
        r'(\w+)\s+(\d{1,2})',                                                      # "January 18" (no year)
    ]
    
    # Comprehensive month mapping (full names and abbreviations)
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
                # print(f"Pattern matched: {pattern}, groups: {groups}")
                
                if len(groups) == 3:
                    if pattern == patterns[0]:  # Month name format
                        month_name, day, year = groups
                        # Convert month name to number
                        month = month_map.get(month_name.lower())
                        # print(f"Looking up month '{month_name.lower()}' -> {month}")
                        if month:
                            parsed = pendulum.datetime(int(year), month, int(day), tz='America/New_York')
                            # print(f"Manual parse successful: {parsed}")
                            return parsed.in_timezone('UTC')
                        # else:
                            # print(f"Month '{month_name}' not found in month_map")
                    
                    elif pattern == patterns[1]:  # MM.DD.YYYY format (with optional day and time)
                        month, day, year = groups
                        parsed = pendulum.datetime(int(year), int(month), int(day), tz='America/New_York')
                        # print(f"Manual parse successful (MM.DD.YYYY): {parsed}")
                        return parsed.in_timezone('UTC')
                    
                    elif pattern == patterns[2] or pattern == patterns[3]:  # Month name with optional day prefix and time
                        month_name, day, year = groups
                        month = month_map.get(month_name.lower())
                        # print(f"Looking up month '{month_name.lower()}' -> {month}")
                        if month:
                            parsed = pendulum.datetime(int(year), month, int(day), tz='America/New_York')
                            # print(f"Manual parse successful (with time): {parsed}")
                            return parsed.in_timezone('UTC')
                        # else:
                        #     print(f"Month '{month_name}' not found in month_map")
                    
                    elif pattern == patterns[4]:  # MM/DD/YYYY
                        month, day, year = groups
                        parsed = pendulum.datetime(int(year), int(month), int(day), tz='America/New_York')
                        # print(f"Manual parse successful: {parsed}")
                        return parsed.in_timezone('UTC')
                    
                    elif pattern == patterns[5]:  # YYYY-MM-DD
                        year, month, day = groups
                        parsed = pendulum.datetime(int(year), int(month), int(day), tz='America/New_York')
                        # print(f"Manual parse successful: {parsed}")
                        return parsed.in_timezone('UTC')
                
                elif len(groups) == 2:  # No year provided
                    month_name, day = groups
                    month = month_map.get(month_name.lower())
                    # print(f"Looking up month '{month_name.lower()}' -> {month}")
                    if month:
                        current_year = pendulum.now().year
                        parsed = pendulum.datetime(current_year, month, int(day), tz='America/New_York')
                        
                        # Adjust year if date seems too far in the past
                        now = pendulum.now('America/New_York')
                        if parsed < now.subtract(months=6):
                            parsed = pendulum.datetime(current_year + 1, month, int(day), tz='America/New_York')
                        
                        # print(f"Manual parse successful (year assumed): {parsed}")
                        return parsed.in_timezone('UTC')
                    # else:
                    #     print(f"Month '{month_name}' not found in month_map")
                        
            except Exception as e:
                # print(f"Manual parse attempt failed: {e}")
                continue
    
    print(f"All parsing attempts failed for: '{date_str}'")
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
        'created_at': datetime.now(pytz.UTC).isoformat()
    }
    
    # Add comprehensive profile data if available
    if fighter_data and len(fighter_data) > 0 and fighter_data[0].get('Basic Infos'):
        try:
            basic_info = fighter_data[0]['Basic Infos'][0]
            
            # Map all available fields from the schema to database columns
            fighter_record.update({
                'nickname': basic_info.get('nickname'),
                'age': basic_info.get('age'),
                'date_of_birth': parse_listing_date(basic_info.get('date_of_birth')).isoformat(),
                'height': basic_info.get('height'),
                'weight_class': basic_info.get('weight_class'),
                'last_weight_in': basic_info.get('last_weight_in'),
                'last_fight_date': parse_listing_date(basic_info.get('last_fight_date')).isoformat(),
                'born': basic_info.get('born'),
                'head_coach': basic_info.get('head_coach'),
                'pro_mma_record': basic_info.get('pro_mma_record'),
                'current_mma_streak': basic_info.get('current_mma_streak'),
                'affiliation': basic_info.get('affiliation'),
                'other_coaches': basic_info.get('other_coaches'),
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