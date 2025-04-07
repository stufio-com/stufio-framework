from typing import Optional, List
from odmantic import Field
from pydantic import ConfigDict
from stufio.db.mongo_base import MongoBase

class UserGroup(MongoBase):
    """User Group model for MongoDB"""
    name: str = Field(unique=True, index=True)
    description: Optional[str] = None
    permissions: List[str] = Field(default_factory=list)
    is_active: bool = True
    
    model_config = ConfigDict(
        collection="user_groups",
        indexes=[
            {"fields": [("name", 1)], "unique": True}
        ],
    )