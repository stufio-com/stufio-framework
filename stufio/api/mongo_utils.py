"""
MongoDB utility functions for API responses.
Provides helper functions to convert ODMantic models to response schemas with proper error handling.
"""

from typing import Optional, List, Type, TypeVar
from fastapi import HTTPException, status
from odmantic import Model
from stufio.schemas.mongo_response import MongoBaseResponse

T = TypeVar('T', bound=MongoBaseResponse)

def mongo_response_or_404(model: Optional[Model], response_class: Type[T]) -> T:
    """
    Convert ODMantic model to response schema, raising 404 if model is None.
    
    Args:
        model: ODMantic model instance or None
        response_class: Response schema class
        
    Returns:
        Response schema instance
        
    Raises:
        HTTPException: 404 if model is None
    """
    if not model:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Resource not found"
        )
    
    result = response_class.from_mongo_model(model)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Resource not found"
        )
    return result


def mongo_list_response(models: List[Model], response_class: Type[T]) -> List[T]:
    """
    Convert list of ODMantic models to list of response schemas.
    
    Args:
        models: List of ODMantic model instances
        response_class: Response schema class
        
    Returns:
        List of response schema instances
    """
    return response_class.from_mongo_models(models)


__all__ = [
    'mongo_response_or_404',
    'mongo_list_response',
]
