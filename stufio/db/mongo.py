from typing import Any, Optional
import json
from bson import ObjectId
from stufio.core.config import get_settings
from stufio.__version__ import __version__
from motor.motor_asyncio import AsyncIOMotorClient
from motor import core
from odmantic import AIOEngine
from pymongo.driver_info import DriverInfo
from datetime import datetime

settings = get_settings()

DRIVER_INFO = DriverInfo(name="stufio-fastapi-mongodb", version=__version__)

class MongoJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for MongoDB objects
    Handles BSON types like ObjectId and datetime that aren't JSON serializable
    """
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO 8601 format string
        # Handle other MongoDB/BSON types here if needed
        return super().default(obj)


def serialize_mongo_doc(doc: dict) -> dict:
    """
    Convert a MongoDB document to a JSON-serializable dict
    
    Args:
        doc: MongoDB document (dict with potential ObjectId/datetime values)
        
    Returns:
        dict: JSON-serializable version of the document
    """
    if not doc:
        return {}
        
    serialized = {}
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            serialized[key] = str(value)
        elif isinstance(value, datetime):
            serialized[key] = value.isoformat()  # Convert datetime to ISO string
        elif isinstance(value, dict):
            serialized[key] = serialize_mongo_doc(value)
        elif isinstance(value, list):
            serialized[key] = [
                serialize_mongo_doc(item) if isinstance(item, dict) 
                else str(item) if isinstance(item, ObjectId)
                else item.isoformat() if isinstance(item, datetime)
                else item
                for item in value
            ]
        else:
            serialized[key] = value
    return serialized


class _MongoClientSingleton:
    mongo_client: Optional[Any] = None
    engine: AIOEngine

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(_MongoClientSingleton, cls).__new__(cls)
            cls.instance.mongo_client = AsyncIOMotorClient(
                settings.MONGO_DATABASE_URI, driver=DRIVER_INFO
            )
            cls.instance.engine = AIOEngine(client=cls.instance.mongo_client, database=settings.MONGO_DATABASE)
        return cls.instance


def MongoDatabase() -> core.AgnosticDatabase:
    return _MongoClientSingleton().mongo_client[settings.MONGO_DATABASE]


def get_engine() -> AIOEngine:
    return _MongoClientSingleton().engine


async def ping():
    await MongoDatabase().command("ping")


__all__ = [
    "MongoDatabase", 
    "ping", 
    "get_engine", 
    "MongoJSONEncoder", 
    "serialize_mongo_doc"
]
