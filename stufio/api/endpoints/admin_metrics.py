"""
API endpoints for database metrics collection and monitoring.
"""
from fastapi import APIRouter, HTTPException

from ...core.config import get_settings
from ...db.metrics import get_metrics_summary
from ...schemas.db_metrics import DatabaseMetricsSummary

settings = get_settings()
router = APIRouter()


@router.get(
    "/database", 
    response_model=DatabaseMetricsSummary,
    summary="Get database metrics"
)
async def get_database_metrics() -> DatabaseMetricsSummary:
    """
    Get metrics about database operations.
    
    Returns metrics about ClickHouse, MongoDB and Redis operations.
    Includes query counts, average execution times, error rates, etc.
    
    Only accessible to superusers.
    """
    if not getattr(settings, "DB_METRICS_ENABLE", False):
        raise HTTPException(
            status_code=404,
            detail="Database metrics collection is disabled. Enable it with DB_METRICS_ENABLE setting."
        )
    
    return await get_metrics_summary()
