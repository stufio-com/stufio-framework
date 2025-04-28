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
import logging

settings = get_settings()
logger = logging.getLogger(__name__)

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
    _collections_wrapped = False

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(_MongoClientSingleton, cls).__new__(cls)
            cls.instance.mongo_client = AsyncIOMotorClient(
                settings.MONGO_DATABASE_URI, driver=DRIVER_INFO
            )
            cls.instance.engine = AIOEngine(client=cls.instance.mongo_client, database=settings.MONGO_DATABASE)
            
            # Apply metrics tracking if enabled in settings
            if getattr(settings, "DB_METRICS_ENABLE", False):
                cls._apply_metrics(cls.instance)
                
        return cls.instance
    
    @classmethod
    def _apply_metrics(cls, instance):
        """Apply metrics tracking to MongoDB operations"""
        if cls._collections_wrapped:
            return
            
        try:
            # Import metrics module with new provider-based tracking
            from stufio.db.metrics import track_mongo_operation
            
            # Wrap AIOEngine methods
            original_find_one = instance.engine.find_one
            original_find = instance.engine.find
            original_save = instance.engine.save
            original_save_all = instance.engine.save_all
            original_delete = instance.engine.delete
            original_remove = instance.engine.remove
            original_count = instance.engine.count
            
            # Apply metrics tracking
            instance.engine.find_one = track_mongo_operation(original_find_one)
            instance.engine.find = track_mongo_operation(original_find)
            instance.engine.save = track_mongo_operation(original_save)
            instance.engine.save_all = track_mongo_operation(original_save_all)
            instance.engine.delete = track_mongo_operation(original_delete)
            instance.engine.remove = track_mongo_operation(original_remove) 
            instance.engine.count = track_mongo_operation(original_count)
            
            logger.debug("MongoDB engine methods wrapped with metrics tracking")
            cls._collections_wrapped = True
        except ImportError:
            logger.debug("Metrics module not available, skipping MongoDB metrics tracking")
        except Exception as e:
            logger.error(f"Error setting up MongoDB metrics tracking: {e}")


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
