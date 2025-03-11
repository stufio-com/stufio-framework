from __future__ import annotations
from typing import Any, Optional
from datetime import datetime
from pydantic import EmailStr
from odmantic import Field

from stufio.db.mongo_base import MongoBase, datetime_now_sec

class User(MongoBase):
    created: datetime = Field(default_factory=datetime_now_sec)
    modified: datetime = Field(default_factory=datetime_now_sec)
    full_name: str = Field(default="")
    email: EmailStr
    hashed_password: Any = Field(default=None)
    totp_secret: Any = Field(default=None)
    totp_counter: Optional[int] = Field(default=None)
    email_validated: bool = Field(default=False)
    email_tokens_cnt: int = Field(default=0)
    is_active: bool = Field(default=False)
    is_superuser: bool = Field(default=False)
