from fastapi import APIRouter

from stufio.api.endpoints import (
    login,
    users,
    admin_users
    # proxy,
)
from stufio.api.admin import admin_router

api_router = APIRouter()

# api_router.include_router(proxy.router, prefix="/proxy", tags=["proxy"])
api_router.include_router(login.router, prefix="/login", tags=["login"])
api_router.include_router(users.router, prefix="/users", tags=["users"])

# Include admin routers
admin_router.include_router(
    admin_users.router, prefix="/users", tags=["users"]
)
