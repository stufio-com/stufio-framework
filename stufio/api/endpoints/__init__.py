from fastapi import APIRouter

from . import (
    internal_settings,
    login,
    users,
    admin_users,
    admin_settings
    # proxy,
)
from ..admin import admin_router, internal_router
from ...core.config import get_settings

api_router = APIRouter()

# api_router.include_router(proxy.router, prefix="/proxy", tags=["proxy"])
api_router.include_router(login.router, prefix="/login", tags=["login"])
api_router.include_router(users.router, prefix="/users", tags=["users"])

# Include admin routers
admin_router.include_router(
    admin_users.router, prefix="/users", tags=["users"]
)
admin_router.include_router(
    admin_settings.router, prefix="/settings", tags=["settings"]
)

# Include internal/admin routers
internal_router.include_router(
    internal_settings.router, prefix="/settings", tags=["settings"]
)


"""
Register database metrics API endpoints with the main FastAPI application.

Args:
    app: The FastAPI application instance
    prefix: Optional API prefix to use (defaults to settings.API_ADMIN_STR)
"""

settings = get_settings()
# If metrics are enabled, include the routes
if getattr(settings, "DB_METRICS_ENABLE", False):
    from .admin_metrics import router as metrics_router
    admin_router.include_router(
        metrics_router, prefix="/metrics", tags=["metrics", "admin"]
    )
