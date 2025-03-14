from fastapi import APIRouter

from pymongo import settings
from stufio.core.config import get_settings
from stufio.api.endpoints import (
    login,
    users,
    admin_users
    # proxy,
)

api_router = APIRouter()
api_router.include_router(login.router, prefix="/login", tags=["login"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
# api_router.include_router(proxy.router, prefix="/proxy", tags=["proxy"])

settings = get_settings()
api_router.include_router(
    admin_users.router, prefix=settings.API_ADMIN_STR + "/users", tags=["users", "admin"]
)
