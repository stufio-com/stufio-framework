from typing import Optional, List
from pydantic import BaseModel
from odmantic import ObjectId

class UserGroupBase(BaseModel):
    name: str
    description: Optional[str] = None
    permissions: List[str] = []
    is_active: bool = True

class UserGroupCreate(UserGroupBase):
    pass

class UserGroupUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    permissions: Optional[List[str]] = None
    is_active: Optional[bool] = None

class UserGroupInDBBase(UserGroupBase):
    id: Optional[ObjectId] = None

    class Config:
        from_attributes = True

class UserGroup(UserGroupInDBBase):
    pass