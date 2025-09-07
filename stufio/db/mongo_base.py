from odmantic import Model
from datetime import datetime, timezone

def datetime_now():
    return datetime.now(timezone.utc)

def datetime_now_sec() -> datetime:
    """Return current UTC datetime without microseconds for consistency"""
    return datetime.now(timezone.utc).replace(microsecond=0)

MongoBase = Model
