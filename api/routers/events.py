from fastapi import APIRouter, Depends, Query, HTTPException, Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from ..dependencies import get_supabase
from pydantic import BaseModel, Field
import logging
from urllib.parse import unquote

# Configure logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/events", tags=["events"])

# Response Models
class EventResponse(BaseModel):
    id: int
    name: Optional[str] = None
    event_datetime: Optional[datetime] = Field(None, alias="datetime")
    promotion: Optional[str] = None
    venue: Optional[str] = None
    location: Optional[str] = None
    mma_bouts: Optional[int] = None
    img_url: Optional[str] = None
    broadcast: Optional[str] = None
    created_at: Optional[datetime] = None
    hash: Optional[str] = None
    tapology_url: Optional[str] = None
    fight_card: Optional[List[Dict[str, Any]]] = None
    
    class Config:
        from_attributes = True
        # Allow extra fields from database that aren't in model
        extra = "ignore"

class PaginationInfo(BaseModel):
    total: int
    page: int
    per_page: int
    total_pages: int
    has_next: bool
    has_previous: bool

class EventListResponse(BaseModel):
    data: List[EventResponse]
    pagination: Optional[PaginationInfo] = None

class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    status_code: int

# Utility Functions
def handle_database_error(e: Exception, operation: str = "database operation"):
    """Standardized error handling"""
    error_msg = f"Failed to execute {operation}"
    logger.error(f"{error_msg}: {str(e)}")
    raise HTTPException(
        status_code=500, 
        detail={"error": "Internal Server Error", "message": error_msg}
    )

def calculate_pagination(total: int, page: int, per_page: int) -> PaginationInfo:
    """Calculate pagination metadata"""
    total_pages = (total + per_page - 1) // per_page
    return PaginationInfo(
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_previous=page > 1
    )

def apply_date_filters(query, from_date: Optional[str], to_date: Optional[str]):
    """Apply date range filters to query"""
    if from_date:
        try:
            query = query.gte("datetime", f"{from_date}T00:00:00")
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid from_date format. Use YYYY-MM-DD")
    
    if to_date:
        try:
            query = query.lte("datetime", f"{to_date}T23:59:59")
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid to_date format. Use YYYY-MM-DD")
    
    return query

def apply_ordering(query, order: str):
    """Apply ordering to query"""
    valid_fields = ["datetime", "name", "created_at", "venue", "location", "promotion"]
    
    if order.startswith("-"):
        field = order[1:]
        if field not in valid_fields:
            raise HTTPException(status_code=400, detail=f"Invalid order field. Use: {', '.join(valid_fields)}")
        return query.order(field, desc=True)
    else:
        if order not in valid_fields:
            raise HTTPException(status_code=400, detail=f"Invalid order field. Use: {', '.join(valid_fields)}")
        return query.order(order)

async def get_fight_card(supabase, event_id: int) -> List[Dict[str, Any]]:
    """Get fight card for an event"""
    try:
        response = supabase.table("fights") \
            .select("*") \
            .eq("id_event", event_id) \
            .execute()
        return response.data if response.data else []
    except Exception as e:
        logger.warning(f"Failed to fetch fight card for event {event_id}: {str(e)}")
        return []

# Main Endpoints
@router.get("/")
async def get_events(
    status: Optional[str] = Query(None, description="Filter by status"),
    promotion: Optional[str] = Query(None, description="Filter by promotion (e.g., UFC, Bellator)"),
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    venue: Optional[str] = Query(None, description="Filter by venue"),
    location: Optional[str] = Query(None, description="Filter by location"),
    page: int = Query(1, description="Page number", ge=1),
    per_page: int = Query(10, description="Items per page", ge=1, le=100),
    order: str = Query("-datetime", description="Order by field (-datetime for descending)"),
    include_fights: bool = Query(False, description="Include fight cards"),
    include_pagination: bool = Query(True, description="Include pagination metadata"),
    supabase=Depends(get_supabase)
):
    """Get events with comprehensive filtering and pagination"""
    try:
        # Build base query
        query = supabase.table("events").select("*", count="exact")
        
        # Apply filters
        if promotion:
            query = query.ilike("promotion", f"%{promotion}%")
        if venue:
            query = query.ilike("venue", f"%{venue}%")  
        if location:
            query = query.ilike("location", f"%{location}%")
        
        query = apply_date_filters(query, from_date, to_date)
        query = apply_ordering(query, order)
        
        # Get total count for pagination
        if include_pagination:
            count_response = query.execute()
            total = count_response.count if hasattr(count_response, 'count') else 0
        else:
            total = 0
        
        # Apply pagination
        offset = (page - 1) * per_page
        response = query.range(offset, offset + per_page - 1).execute()
        
        events = response.data if response.data else []
        
        # Include fight cards if requested
        if include_fights and events:
            for event in events:
                event['fight_card'] = await get_fight_card(supabase, event['id'])
        
        # Prepare response
        result = {"data": events}
        if include_pagination:
            result["pagination"] = calculate_pagination(total, page, per_page)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        handle_database_error(e, "fetching events")

@router.get("/upcoming")
async def get_upcoming_events(
    days: int = Query(90, description="Lookahead window in days", ge=1, le=365),
    limit: int = Query(5, description="Number of results", ge=1, le=50),
    promotion: Optional[str] = Query(None, description="Filter by promotion"),
    include_fights: bool = Query(False, description="Include fight cards"),
    supabase=Depends(get_supabase)
):
    """Get upcoming events within specified time window"""
    try:
        now = datetime.utcnow()
        future_date = now + timedelta(days=days)
        
        query = supabase.table("events") \
            .select("*") \
            .gte("datetime", now.isoformat()) \
            .lte("datetime", future_date.isoformat()) \
            .order("datetime", desc=False) \
            .limit(limit)
        
        if promotion:
            query = query.ilike("promotion", f"%{promotion}%")
        
        response = query.execute()
        events = response.data if response.data else []
        
        if include_fights and events:
            for event in events:
                event['fight_card'] = await get_fight_card(supabase, event['id'])
        
        return events
        
    except Exception as e:
        handle_database_error(e, "fetching upcoming events")

@router.get("/recent")
async def get_recent_events(
    days: int = Query(30, description="Lookback window in days", ge=1, le=365),
    limit: int = Query(10, description="Number of results", ge=1, le=50),
    promotion: Optional[str] = Query(None, description="Filter by promotion"),
    include_fights: bool = Query(False, description="Include fight cards"),
    supabase=Depends(get_supabase)
):
    """Get recent events within specified time window"""
    try:
        now = datetime.utcnow()
        past_date = now - timedelta(days=days)
        
        query = supabase.table("events") \
            .select("*") \
            .gte("datetime", past_date.isoformat()) \
            .lte("datetime", now.isoformat()) \
            .order("datetime", desc=True) \
            .limit(limit)
        
        if promotion:
            query = query.ilike("promotion", f"%{promotion}%")
        
        response = query.execute()
        events = response.data if response.data else []
        
        if include_fights and events:
            for event in events:
                event['fight_card'] = await get_fight_card(supabase, event['id'])
        
        return events
        
    except Exception as e:
        handle_database_error(e, "fetching recent events")

# Specific URL Pattern Endpoints
@router.get("/name/{event_name}")
async def get_events_by_name(
    event_name: str = Path(..., description="Event name to search"),
    include_fights: bool = Query(False, description="Include fight cards"),
    limit: int = Query(10, description="Max results", ge=1, le=50),
    supabase=Depends(get_supabase)
):
    """Get events by name (partial matching)"""
    try:
        decoded_name = unquote(event_name)
        
        response = supabase.table("events") \
            .select("*") \
            .ilike("name", f"%{decoded_name}%") \
            .order("datetime", desc=True) \
            .limit(limit) \
            .execute()
        
        events = response.data if response.data else []
        
        if include_fights and events:
            for event in events:
                event['fight_card'] = await get_fight_card(supabase, event['id'])
        
        return events
        
    except Exception as e:
        handle_database_error(e, f"searching events by name '{event_name}'")

@router.get("/venue/{venue_name}")
async def get_events_by_venue(
    venue_name: str = Path(..., description="Venue name to search"),
    include_fights: bool = Query(False, description="Include fight cards"),
    limit: int = Query(20, description="Max results", ge=1, le=50),
    supabase=Depends(get_supabase)
):
    """Get events by venue"""
    try:
        decoded_venue = unquote(venue_name)
        
        response = supabase.table("events") \
            .select("*") \
            .ilike("venue", f"%{decoded_venue}%") \
            .order("datetime", desc=True) \
            .limit(limit) \
            .execute()
        
        events = response.data if response.data else []
        
        if include_fights and events:
            for event in events:
                event['fight_card'] = await get_fight_card(supabase, event['id'])
        
        return events
        
    except Exception as e:
        handle_database_error(e, f"searching events by venue '{venue_name}'")

@router.get("/location/{location_name}")
async def get_events_by_location(
    location_name: str = Path(..., description="Location to search"),
    include_fights: bool = Query(False, description="Include fight cards"),
    limit: int = Query(20, description="Max results", ge=1, le=50),
    supabase=Depends(get_supabase)
):
    """Get events by location"""
    try:
        decoded_location = unquote(location_name)
        
        response = supabase.table("events") \
            .select("*") \
            .ilike("location", f"%{decoded_location}%") \
            .order("datetime", desc=True) \
            .limit(limit) \
            .execute()
        
        events = response.data if response.data else []
        
        if include_fights and events:
            for event in events:
                event['fight_card'] = await get_fight_card(supabase, event['id'])
        
        return events
        
    except Exception as e:
        handle_database_error(e, f"searching events by location '{location_name}'")

@router.get("/promotion/{promotion_name}")
async def get_events_by_promotion(
    promotion_name: str = Path(..., description="Promotion name (UFC, Bellator, etc.)"),
    include_fights: bool = Query(False, description="Include fight cards"),
    limit: int = Query(20, description="Max results", ge=1, le=50),
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    supabase=Depends(get_supabase)
):
    """Get events by promotion"""
    try:
        decoded_promotion = unquote(promotion_name)
        
        query = supabase.table("events") \
            .select("*") \
            .ilike("promotion", f"%{decoded_promotion}%")
        
        query = apply_date_filters(query, from_date, to_date)
        
        response = query.order("datetime", desc=True) \
            .limit(limit) \
            .execute()
        
        events = response.data if response.data else []
        
        if include_fights and events:
            for event in events:
                event['fight_card'] = await get_fight_card(supabase, event['id'])
        
        return events
        
    except HTTPException:
        raise
    except Exception as e:
        handle_database_error(e, f"searching events by promotion '{promotion_name}'")

@router.get("/date/{event_date}")
async def get_events_by_date(
    event_date: str = Path(..., description="Event date (YYYY-MM-DD)"),
    include_fights: bool = Query(False, description="Include fight cards"),
    supabase=Depends(get_supabase)
):
    """Get events on specific date"""
    try:
        # Validate date format
        try:
            datetime.strptime(event_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        
        response = supabase.table("events") \
            .select("*") \
            .gte("datetime", f"{event_date}T00:00:00") \
            .lte("datetime", f"{event_date}T23:59:59") \
            .order("datetime", desc=False) \
            .execute()
        
        events = response.data if response.data else []
        
        if include_fights and events:
            for event in events:
                event['fight_card'] = await get_fight_card(supabase, event['id'])
        
        return events
        
    except HTTPException:
        raise
    except Exception as e:
        handle_database_error(e, f"searching events by date '{event_date}'")

@router.get("/year/{year}")
async def get_events_by_year(
    year: int = Path(..., description="Year (e.g., 2024)", ge=2000, le=2030),
    promotion: Optional[str] = Query(None, description="Filter by promotion"),
    include_fights: bool = Query(False, description="Include fight cards"),
    limit: int = Query(50, description="Max results", ge=1, le=100),
    supabase=Depends(get_supabase)
):
    """Get events by year"""
    try:
        start_date = f"{year}-01-01T00:00:00"
        end_date = f"{year}-12-31T23:59:59"
        
        query = supabase.table("events") \
            .select("*") \
            .gte("datetime", start_date) \
            .lte("datetime", end_date)
        
        if promotion:
            query = query.ilike("promotion", f"%{promotion}%")
        
        response = query.order("datetime", desc=True) \
            .limit(limit) \
            .execute()
        
        events = response.data if response.data else []
        
        if include_fights and events:
            for event in events:
                event['fight_card'] = await get_fight_card(supabase, event['id'])
        
        return events
        
    except Exception as e:
        handle_database_error(e, f"searching events by year {year}")

@router.get("/month/{year_month}")
async def get_events_by_month(
    year_month: str = Path(..., description="Year-Month (YYYY-MM)"),
    include_fights: bool = Query(False, description="Include fight cards"),
    promotion: Optional[str] = Query(None, description="Filter by promotion"),
    supabase=Depends(get_supabase)
):
    """Get events by year and month"""
    try:
        # Validate format
        try:
            year, month = year_month.split("-")
            year, month = int(year), int(month)
            if not (2000 <= year <= 2030) or not (1 <= month <= 12):
                raise ValueError()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid format. Use YYYY-MM")
        
        # Calculate date range
        start_date = f"{year_month}-01T00:00:00"
        if month == 12:
            next_year, next_month = year + 1, 1
        else:
            next_year, next_month = year, month + 1
        end_date = f"{next_year}-{next_month:02d}-01T00:00:00"
        
        query = supabase.table("events") \
            .select("*") \
            .gte("datetime", start_date) \
            .lt("datetime", end_date)
        
        if promotion:
            query = query.ilike("promotion", f"%{promotion}%")
        
        response = query.order("datetime", desc=False) \
            .execute()
        
        events = response.data if response.data else []
        
        if include_fights and events:
            for event in events:
                event['fight_card'] = await get_fight_card(supabase, event['id'])
        
        return events
        
    except HTTPException:
        raise
    except Exception as e:
        handle_database_error(e, f"searching events by month '{year_month}'")

@router.get("/{event_id}")
async def get_event_by_id(
    event_id: int = Path(..., description="Event ID"),
    include_fights: bool = Query(True, description="Include fight card"),
    supabase=Depends(get_supabase)
):
    """Get specific event by ID"""
    try:
        response = supabase.table("events") \
            .select("*") \
            .eq("id", event_id) \
            .execute()
        
        if not response.data:
            raise HTTPException(status_code=404, detail=f"Event with ID {event_id} not found")
        
        event = response.data[0]
        
        if include_fights:
            event['fight_card'] = await get_fight_card(supabase, event_id)
        
        return event
        
    except HTTPException:
        raise
    except Exception as e:
        handle_database_error(e, f"fetching event with ID {event_id}")

# Statistics endpoint
@router.get("/stats/summary")
async def get_events_summary(
    supabase=Depends(get_supabase)
):
    """Get events statistics summary"""
    try:
        # Total events
        total_response = supabase.table("events") \
            .select("*", count="exact") \
            .execute()
        total_events = total_response.count if hasattr(total_response, 'count') else 0
        
        # Upcoming events
        now = datetime.utcnow()
        upcoming_response = supabase.table("events") \
            .select("*", count="exact") \
            .gte("datetime", now.isoformat()) \
            .execute()
        upcoming_events = upcoming_response.count if hasattr(upcoming_response, 'count') else 0
        
        # Events by promotion
        promotions_response = supabase.table("events") \
            .select("promotion") \
            .not_.is_("promotion", "null") \
            .execute()
        
        promotion_counts = {}
        if promotions_response.data:
            for event in promotions_response.data:
                promo = event.get('promotion', 'Unknown')
                promotion_counts[promo] = promotion_counts.get(promo, 0) + 1
        
        return {
            "total_events": total_events,
            "upcoming_events": upcoming_events,
            "completed_events": total_events - upcoming_events,
            "events_by_promotion": promotion_counts,
            "last_updated": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        handle_database_error(e, "generating events summary")