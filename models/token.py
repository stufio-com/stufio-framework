from __future__ import annotations
from odmantic import Reference
from stufio.db.mongo_base import MongoBase, datetime_now_sec
from .user import User  
from datetime import datetime, timedelta
from odmantic import Field
from stufio.core.config import settings

def datetime_expires_sec() -> datetime:
    """Return max expiration time for a token"""
    max_expire = max(
        settings.ACCESS_TOKEN_EXPIRE_SECONDS, settings.REFRESH_TOKEN_EXPIRE_SECONDS
    )
    return datetime.now().replace(microsecond=0) + timedelta(seconds=max_expire)


# Consider reworking to consolidate information to a userId. This may not work well
class Token(MongoBase):
    token: str
    authenticates_id: User = Reference()
    created: datetime = Field(default_factory=datetime_now_sec)
    expires: datetime = Field(default_factory=datetime_expires_sec)
