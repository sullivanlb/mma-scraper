# api/dependencies.py
from supabase import create_client
from supabase.client import ClientOptions
from dotenv import load_dotenv
import os
import httpx

# Load environment variables
load_dotenv()

def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("Missing Supabase credentials")

    # Create custom HTTP client
    http_client = httpx.Client()

    # Create client with custom options
    options = ClientOptions(
        schema="public",
        auto_refresh_token=True,
        persist_session=True,
        httpx_client=http_client
    )

    return create_client(url, key, options)