from odmantic import Model
from datetime import datetime


def datetime_now_sec() -> datetime:
    """Return current datetime without microseconds for Clickhouse compatibility"""
    return datetime.now().replace(microsecond=0)

MongoBase = Model
