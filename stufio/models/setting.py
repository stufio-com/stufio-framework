from datetime import datetime
from typing import Any, Optional
from bson import ObjectId
from odmantic import Field
from pydantic import ConfigDict
from stufio.db.mongo_base import MongoBase, datetime_now_sec
from stufio.models.user import User


class Setting(MongoBase):
    """
    Model for storing user-defined setting values that override default configuration
    """
    key: str = Field(..., index=True)
    module: str = Field(default="core", index=True)
    value: Any = Field(...)  # Stores the active value that overrides default
    updated_at: datetime = Field(default_factory=datetime_now_sec)
    updated_by: Optional[ObjectId] = Field(default=None)  # User who last modified the setting
    
    model_config = ConfigDict(
        collection="settings",
        indexes=[
            {"fields": [("key", 1), ("module", 1)], "unique": True}
        ]
    )


class SettingHistory(MongoBase):
    """
    Model for tracking historical changes to settings
    """
    setting_id: ObjectId = Field(...)  # Reference to the original setting
    key: str = Field(..., index=True)
    module: str = Field(default="core", index=True)
    value: Any = Field(...)  # Previous value
    created_at: datetime = Field(default_factory=datetime_now_sec)
    created_by: Optional[ObjectId] = Field(default=None)  # User who made the change
    
    model_config = ConfigDict(
        collection="settings_history",
        indexes=[
            {"fields": [("setting_id", 1)], "background": True},
            {"fields": [("created_at", 1)], "background": True},  # For TTL index
        ]
     )