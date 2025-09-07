from __future__ import annotations
from typing import Any, Optional, List
from datetime import datetime
from pydantic import EmailStr, model_validator
from odmantic import Field, ObjectId

from stufio.db.mongo_base import MongoBase, datetime_now_sec

class User(MongoBase):
    """User model for MongoDB"""
    created: datetime = Field(default_factory=datetime_now_sec)
    modified: datetime = Field(default_factory=datetime_now_sec)
    email: EmailStr = Field(unique=True, index=True)
    # Explicitly type as str with annotation to prevent type confusion
    hashed_password: str = Field(default="")
    full_name: str = Field(default="")
    is_active: bool = Field(default=True)
    is_superuser: bool = Field(default=False)
    email_validated: bool = Field(default=False)
    email_tokens_cnt: int = Field(default=0)
    totp_secret: str = Field(default="")
    totp_counter: int = Field(default=0)
    # Add user groups as a list of ObjectIds with index
    user_groups: List[ObjectId] = Field(default_factory=list, index=True)

    @model_validator(mode="before")
    @classmethod
    def validate_types(cls, data: dict) -> dict:
        """Validate and convert field types before model creation"""
        # Convert fields to appropriate types
        string_fields = ["hashed_password", "full_name", "totp_secret"]
        for field in string_fields:
            if field in data:
                # Force conversion to string, even if None
                data[field] = str(data[field] or "")
        
        # Ensure integer fields are properly typed
        int_fields = ["email_tokens_cnt", "totp_counter"]
        for field in int_fields:
            if field in data:
                # Force conversion to int, default to 0 if None or empty
                try:
                    if data[field] == "" or data[field] is None:
                        data[field] = 0
                    else:
                        data[field] = int(data[field])
                except (ValueError, TypeError):
                    # If conversion fails, use default
                    data[field] = 0
        
        return data

    model_config = {
        "collection": "users"
    }
