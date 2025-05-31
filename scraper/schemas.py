# =================================================================
# scraper/schemas.py - Schema loading
# =================================================================

import json
import logging

logger = logging.getLogger(__name__)

def load_schema(filename: str) -> dict:
    """Load JSON schema with error handling"""
    try:
        with open(filename, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        logger.error(f"Schema file {filename} not found")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {filename}: {e}")
        raise