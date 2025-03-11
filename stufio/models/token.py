from __future__ import annotations
from typing import Optional
from odmantic import Reference, Field
from stufio.db.mongo_base import MongoBase, datetime_now_sec
from stufio.models.user import User
from datetime import datetime, timedelta
from stufio.core.config import settings

def datetime_expires_sec() -> datetime:
    """Return max expiration time for a token"""
    max_expire = max(
        settings.ACCESS_TOKEN_EXPIRE_SECONDS, settings.REFRESH_TOKEN_EXPIRE_SECONDS
    )
    return datetime.now().replace(microsecond=0) + timedelta(seconds=max_expire)


# Fix the reference syntax - ODMantic uses type annotation for reference
class Token(MongoBase):
    token: str
    authenticates_id: Optional[User] = Reference()
    created: datetime = Field(default_factory=datetime_now_sec)
    expires: datetime = Field(default_factory=datetime_expires_sec)
