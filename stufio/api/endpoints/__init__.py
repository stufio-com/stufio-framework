from fastapi import APIRouter

from stufio.api.endpoints import (
    internal_settings,
    login,
    users,
    admin_users,
    admin_settings
    # proxy,
)
from stufio.api.admin import admin_router, internal_router

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