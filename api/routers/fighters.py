from fastapi import APIRouter, Depends, Query
from ..dependencies import get_supabase

router = APIRouter(prefix="/fighters", tags=["fighters"])

@router.get("/")
async def list_fighters(
    weight_class: str = Query(None),
    limit: int = 50,
    offset: int = 0,
    db = Depends(get_supabase)
):
    query = db.table("fighters").select("*")
    
    if weight_class:
        query = query.eq("weight_class", weight_class.upper())
    
    return query.range(offset, offset + limit - 1).execute().data