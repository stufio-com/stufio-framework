"""
Base response schemas for MongoDB/ODMantic models with automatic ObjectId serialization.
"""

from typing import Any, Dict, List, Optional, Type, TypeVar, Union, Sequence
from datetime import datetime
from pydantic import BaseModel, Field
from bson import ObjectId
from odmantic import Model


ResponseType = TypeVar('ResponseType', bound='MongoBaseResponse')


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


class MongoBaseResponse(BaseModel):
    """
    Base response schema for MongoDB/ODMantic models.
    Automatically handles ObjectId to string conversion.
    """
    
    class Config:
        from_attributes = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda dt: dt.isoformat()
        }
    
    @classmethod
    def from_mongo_model(cls: Type[ResponseType], model: Optional[Model]) -> Optional[ResponseType]:
        """
        Convert ODMantic model to response schema with proper ObjectId serialization.
        
        Args:
            model: ODMantic model instance
            
        Returns:
            Response schema instance or None if model is None
        """
        if not model:
            return None
        
        # Convert model to dict and serialize MongoDB-specific types
        model_dict = model.model_dump()
        serialized_dict = serialize_mongo_doc(model_dict)
        
        return cls(**serialized_dict)
    
    @classmethod
    def from_mongo_models(cls: Type[ResponseType], models: Sequence[Model]) -> List[ResponseType]:
        """
        Convert list of ODMantic models to response schemas.
        
        Args:
            models: List of ODMantic model instances
            
        Returns:
            List of response schema instances
        """
        result = []
        for model in models:
            if model is not None:
                serialized = cls.from_mongo_model(model)
                if serialized is not None:
                    result.append(serialized)
        return result


class MongoResponseWithId(MongoBaseResponse):
    """
    Base response schema for MongoDB models that include an ID field.
    """
    id: str = Field(..., description="Unique identifier")


# Utility functions for direct use in API endpoints
def serialize_mongo_response(model: Optional[Model], response_class: Type[ResponseType]) -> Optional[ResponseType]:
    """
    Serialize a single ODMantic model to response schema.
    
    Args:
        model: ODMantic model instance
        response_class: Response schema class
        
    Returns:
        Serialized response or None
    """
    return response_class.from_mongo_model(model)


def serialize_mongo_responses(models: Sequence[Model], response_class: Type[ResponseType]) -> List[ResponseType]:
    """
    Serialize a list of ODMantic models to response schemas.
    
    Args:
        models: List of ODMantic model instances  
        response_class: Response schema class
        
    Returns:
        List of serialized responses
    """
    return response_class.from_mongo_models(models)


__all__ = [
    'MongoBaseResponse',
    'MongoResponseWithId', 
    'serialize_mongo_response',
    'serialize_mongo_responses'
]
