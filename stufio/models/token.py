from __future__ import annotations
from typing import Optional
from odmantic import Field, Reference
from pydantic import ConfigDict
from stufio.db.mongo_base import MongoBase, datetime_now_sec
from stufio.models import User
from datetime import datetime, timedelta
from stufio.core.config import get_settings


settings = get_settings()

def datetime_expires_sec() -> datetime:
    """Return max expiration time for a token"""
    max_expire = max(
        settings.ACCESS_TOKEN_EXPIRE_SECONDS, settings.REFRESH_TOKEN_EXPIRE_SECONDS
    )
    return datetime.now().replace(microsecond=0) + timedelta(seconds=max_expire)


class Token(MongoBase):
    token: str
    authenticates_id: User = Reference()  # Store just the ObjectId instead of a reference
    created: datetime = Field(default_factory=datetime_now_sec)
    expires: datetime = Field(default_factory=datetime_expires_sec)

    model_config = ConfigDict(
        collection="tokens",
        indexes=[
            {"fields": [("token", 1)], "unique": True},
            {"fields": [("authenticates_id", 1)]},
            {"fields": [("expires", 1)], "expireAfterSeconds": 0},
        ],
    )
