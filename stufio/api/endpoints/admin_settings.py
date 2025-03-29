from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, Path, Body
from motor.core import AgnosticDatabase

from ...api import deps
from ...models.user import User
from ...crud.crud_setting import crud_setting
from ...schemas.setting import Setting, SettingBase, SettingSchemaResponse, SettingUpdate, SettingsGetRequest

router = APIRouter()


@router.get("/schemas", response_model=SettingSchemaResponse)
async def get_settings_schemas(
    current_user: User = Depends(deps.get_current_active_superuser),
) -> Dict[str, Dict[str, Any]]:
    """Get metadata for all settings organized by module"""
    return await crud_setting.get_settings_metadata()


@router.post("/get", response_model=Dict[str, Any])
async def get_settings(
    request: SettingsGetRequest = Body(...),
    db: AgnosticDatabase = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_superuser),
) -> Dict[str, Any]:
    """Get settings by module and/or keys"""
    return await crud_setting.get_filtered_settings(
        db, 
        keys=request.keys
    )


@router.put("/{module}/{key}", response_model=SettingBase)
async def update_setting(
    update: SettingUpdate,
    key: str = Path(...),
    module: str = Path(...),
    db: AgnosticDatabase = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_superuser),
) -> Setting:
    """Update a setting value"""
    result = await crud_setting.create_or_update(
        db,
        key=key,
        value=update.value,
        module=module,  # Module from request body
        user_id=current_user.id if current_user else None,
    )

    return result


@router.post("/save", response_model=List[SettingBase])
async def update_setting(
    update: List[SettingBase],
    db: AgnosticDatabase = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_superuser),
) -> Setting:
    """Update the settings list"""
    results = []
    for setting in update:
        result = await crud_setting.create_or_update(
            db,
            key=setting.key,
            value=setting.value,
            module=setting.module,  # Module from request body
            user_id=current_user.id if current_user else None,
        )
        results.append(result)

    return results


@router.delete("/{key}", response_model=bool)
async def delete_setting(
    key: str = Path(...),
    db: AgnosticDatabase = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_superuser),
) -> bool:
    """Delete a setting (reset to default)"""
    return await crud_setting.delete(db, key=key)


@router.post("/refresh-cache", response_model=Dict[str, bool])
async def refresh_settings_cache(
    db: AgnosticDatabase = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_superuser),
) -> Dict[str, bool]:
    """Refresh the entire settings cache"""
    await crud_setting.refresh_cache(db)
    return {"success": True}
