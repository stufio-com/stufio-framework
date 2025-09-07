from datetime import datetime
from typing import Any, Optional
from bson import ObjectId
from odmantic import Field, Index
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
    
    model_config = {
        "collection": "settings",
        "indexes": lambda: [
            Index("key", "module", unique=True)
        ]
    }


class SettingHistory(MongoBase):
    """
    Model for tracking historical changes to settings
    """
    setting_id: ObjectId = Field(..., index=True)  # Reference to the original setting
    key: str = Field(..., index=True)
    module: str = Field(default="core", index=True)
    value: Any = Field(...)  # Previous value
    created_at: datetime = Field(default_factory=datetime_now_sec, index=True)
    created_by: Optional[ObjectId] = Field(default=None)  # User who made the change
    
    model_config = {
        "collection": "settings_history"
    }