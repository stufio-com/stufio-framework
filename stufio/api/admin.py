from fastapi import APIRouter, Depends
from stufio.api import deps
from stufio.core.config import get_settings

settings = get_settings()

# Create an admin router with superuser dependency built in
admin_router = APIRouter(
    prefix=settings.API_ADMIN_STR,
    dependencies=[Depends(deps.get_current_active_superuser)],
    tags=["admin"],
)

# Define a router for internal/admin endpoints
internal_router = APIRouter(
    prefix=settings.API_INTERNAL_STR,
    dependencies=[Depends(deps.get_api_secret)],
    tags=["internal"],
)
