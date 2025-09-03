from typing import Any, Dict, List, Optional, Union
from bson import ObjectId
from motor.core import AgnosticDatabase
from pymongo import settings
import redis
import json
from datetime import datetime, timezone

from ..crud.mongo_base import CRUDMongo
from ..models.setting import Setting, SettingHistory
from ..schemas.setting import SettingCreate, SettingUpdate
from ..core.config import get_settings
from ..core.setting_registry import settings_registry


class CRUDSetting(CRUDMongo[Setting, SettingCreate, SettingUpdate]):
    """CRUD operations for settings with Redis caching integration"""

    def __init__(self, model: Setting):
        super().__init__(model)
        settings = get_settings()
        self.redis = redis.from_url(settings.REDIS_URL)
        self.prefix = f"{settings.REDIS_PREFIX}settings:"

    async def get_by_key(self, key: str) -> Optional[Setting]:
        """Get a setting by its key"""
        return await self.get_by_field("key", key)

    async def get_module_settings(self, module: str = "core") -> List[Setting]:
        """Get all settings for a module"""
        return await self.get_by_field("module", module)

    async def create_or_update(
        self,
        key: str,
        value: Any,
        module: str = "core",
        user_id: Optional[ObjectId] = None
    ) -> Setting:
        """Create a new setting or update an existing one"""
        # Check if setting exists
        existing = await self.get_by_key(key=key)

        if existing:
            # Create history record
            history = SettingHistory(
                setting_id=existing.id,
                key=existing.key,
                module=existing.module,
                value=existing.value,
                created_by=user_id,
            )

            # Update existing setting
            existing.value = value
            existing.updated_at = datetime.now(timezone.utc).replace(microsecond=0)
            existing.updated_by = user_id

            # await self.engine.save_all([existing, history])
            setting = await self.engine.save(existing)
        else:
            # Create new setting
            setting = Setting(
                key=key,
                module=module,
                value=value,
                updated_by=user_id
            )
            setting = await self.engine.save(setting)

        # Update redis cache
        self._update_cache(key, value)

        return setting

    async def delete(self, key: str) -> bool:
        """Delete a setting (revert to default value)"""
        # Get the setting first
        setting = await self.get_by_key(key=key)
        if not setting:
            return False

        # Delete from database
        await self.engine.delete(setting)

        # Delete from cache
        self._delete_cache(key)

        return True

    def _update_cache(self, key: str, value: Any) -> None:
        """Update a setting in Redis cache"""
        redis_key = f"{self.prefix}:{key}"
        self.redis.set(redis_key, json.dumps(value))

        # Also invalidate the merged settings cache
        cache_key = f"{self.prefix}:merged"
        self.redis.delete(cache_key)

    def _delete_cache(self, key: str) -> None:
        """Delete a setting from Redis cache"""
        redis_key = f"{self.prefix}:{key}"
        self.redis.delete(redis_key)

        # Also invalidate the merged settings cache
        cache_key = f"{self.prefix}:merged"
        self.redis.delete(cache_key)

    async def refresh_cache(self) -> None:
        """Refresh the entire settings cache"""
        # Get all modules with settings
        keys = self.redis.keys(f"{self.prefix}*")
        if keys:
            self.redis.delete(*keys)

        # Update merged settings
        await self.get_merged_settings(force_refresh=True)

    async def get_merged_settings(
        self,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Get settings for a module, merging default values with DB overrides
        
        Args:
            module: Module name
            force_refresh: Whether to bypass cache and force a refresh
            
        Returns:
            Dictionary of merged settings
        """
        # Check cache first unless force_refresh is True
        # if not force_refresh:
        #     merged_key = f"{self.prefix}:merged"
        #     cached = self.redis.get(merged_key)
        #     if cached:
        #         return json.loads(cached)

        # Get default settings
        settings_obj = get_settings()

        # Initialize with empty dictionary
        default_settings = {}

        settings_list = settings_registry.get_settings()

        for setting in settings_list:
            key = setting.key
            try:
                default_settings[key] = getattr(settings_obj, key)
            except KeyError:
                if hasattr(setting, "default_value"):
                    default_settings[key] = setting.default_value

        # Get DB overrides
        db_settings = await self.get_multi(limit=None)
        db_overrides = {setting.key: setting.value for setting in db_settings}

        # Merge settings (DB overrides take precedence)
        merged = {**default_settings, **db_overrides}

        # Cache the result
        merged_key = f"{self.prefix}:merged"
        self.redis.set(merged_key, json.dumps(merged))

        return merged

    async def get_settings_metadata(
        self, module: str = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get metadata for settings, optionally filtered by module
        
        Args:
            module: Optional module to filter by
            
        Returns:
            Dictionary of settings metadata
        """
        # Get settings from registry
        settings_list = settings_registry.get_settings(module)

        settings_groups = [setting_list.group for setting_list in settings_list]
        groups = [
            group
            for group in settings_registry.get_groups()
            if group.id in settings_groups
        ]

        subgroups = {}
        for group in groups:
            subgroups[group.id] = settings_registry.get_subgroups(group.id)

        # Convert to dictionary format
        metadata = {
            "settings": {s.key: s.model_dump() for s in settings_list},
            "groups": {g.id: g.model_dump() for g in groups},
            "subgroups": {g.id: [sg.model_dump() for sg in subgroups[g.id]] for g in groups},
        }

        return metadata

    async def get_filtered_settings(
        self,
        keys: Optional[List[str]] = None,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Get filtered settings for a module, optionally filtered by keys
        
        Args:
            module: Module name
            keys: Optional list of keys to filter by
            force_refresh: Whether to bypass cache and force a refresh
            
        Returns:
            Dictionary of merged settings
        """
        # Get merged settings for the module
        merged_settings = await self.get_merged_settings(force_refresh=force_refresh)

        # If keys are provided, filter the results
        if keys:
            return {k: v for k, v in merged_settings.items() if k in keys}

        # Otherwise return all merged settings
        return merged_settings


# Create a singleton instance
crud_setting = CRUDSetting(Setting)
