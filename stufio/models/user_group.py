from typing import Optional, List
from odmantic import Field
from stufio.db.mongo_base import MongoBase

class UserGroup(MongoBase):
    """User Group model for MongoDB"""
    name: str = Field(unique=True, index=True)
    description: Optional[str] = None
    permissions: List[str] = Field(default_factory=list)
    is_active: bool = True
    
    model_config = {
        "collection": "user_groups"
    }