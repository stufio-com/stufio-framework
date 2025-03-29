from datetime import datetime
from enum import Enum
from re import sub
from typing import Any, Dict, Optional, List, Union
from pydantic import BaseModel, Field


class SettingType(str, Enum):
    """Enumeration of supported setting types"""
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    SELECT = "select"
    MULTI_SELECT = "multi_select"
    DATE = "date"
    EMAIL = "email"
    PASSWORD = "password"
    TEXT = "text"
    URL = "url"
    COLOR = "color"
    FILE = "file"
    SWITCH = "switch"
    RADIO = "radio"
    SLIDER = "slider"


# Base metadata models
class GroupMetadataSchema(BaseModel):
    """Schema for setting group metadata"""
    id: str
    label: str
    description: Optional[str] = None
    icon: Optional[str] = None
    order: int = 100


class SubgroupMetadataSchema(BaseModel):
    """Schema for setting subgroup metadata"""
    id: str
    group_id: str
    label: str
    description: Optional[str] = None
    icon: Optional[str] = None
    order: int = 100
    module: str = "core"


class SettingMetadataSchema(BaseModel):
    """Schema for setting metadata"""
    key: str
    label: str
    description: Optional[str] = None
    group: str = "general"
    subgroup: Optional[str] = None
    type: SettingType = SettingType.STRING
    component: Optional[str] = None
    options: Optional[List[Dict[str, str]]] = None
    min: Optional[Union[int, float]] = None
    max: Optional[Union[int, float]] = None
    placeholder: Optional[str] = None
    required: bool = False
    secret: bool = False
    restart_required: bool = False
    advanced: bool = False
    order: int = 100
    depends_on: Optional[List[str]] = None
    module: str = "core"
    default_value: Optional[Any] = None
    validation: Optional[Dict[str, Any]] = None


# Settings CRUD schemas
class SettingBase(BaseModel):
    """Base schema for settings with common fields"""
    key: str
    module: str = "core"
    value: Any


class SettingCreate(SettingBase):
    """Schema for creating a new setting"""
    pass


class SettingUpdate(BaseModel):
    """Schema for updating an existing setting"""
    value: Any


class SettingInDBBase(SettingBase):
    """Base schema for settings stored in the database"""
    updated_at: datetime
    updated_by: Optional[str] = None


class Setting(SettingInDBBase):
    """Schema for returning a setting"""
    pass


class SettingHistoryBase(BaseModel):
    """Base schema for setting history records"""
    setting_id: str
    key: str
    module: str = "core"
    value: Any


class SettingHistoryCreate(SettingHistoryBase):
    """Schema for creating a setting history record"""
    created_by: Optional[str] = None


class SettingHistory(SettingHistoryBase):
    """Schema for returning a setting history record"""
    created_at: datetime
    created_by: Optional[str] = None


class SettingSchemaResponse(BaseModel):
    """Response schema for settings metadata"""
    settings: Dict[str, SettingMetadataSchema]
    groups: Dict[str, GroupMetadataSchema]
    subgroups: Dict[str, List[SubgroupMetadataSchema]]


class SettingsGetRequest(BaseModel):
    """Request schema for getting filtered settings"""
    keys: Optional[List[str]] = None
