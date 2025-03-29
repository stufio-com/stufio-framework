from typing import Any, Dict
from fastapi import APIRouter, Depends, Path
from motor.core import AgnosticDatabase

from ...api import deps
from ...models.user import User
from ...crud.crud_setting import crud_setting
from ...schemas.setting import Setting, SettingSchemaResponse, SettingUpdate

router = APIRouter()


@router.get("/schemas", response_model=SettingSchemaResponse)
async def get_settings_schemas() -> Dict[str, Dict[str, Any]]:
    """Get metadata for all settings organized by module"""
    return await crud_setting.get_settings_metadata()


@router.post("/refresh-cache", response_model=Dict[str, bool])
async def refresh_settings_cache(
    db: AgnosticDatabase = Depends(deps.get_db),
) -> Dict[str, bool]:
    """Refresh the entire settings cache"""
    await crud_setting.refresh_cache(db)
    return {"success": True}
