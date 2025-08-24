"""
MongoDB-related dependencies for FastAPI endpoints.
"""

from typing import Annotated
from fastapi import Path, HTTPException, status
from odmantic import ObjectId
from pydantic import Field

def validate_object_id(value: str) -> ObjectId:
    """Validate and convert string to ObjectId."""
    try:
        return ObjectId(value)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ObjectId format"
        )

# Use Annotated with Path for FastAPI path parameter validation
ValidatedObjectId = Annotated[
    ObjectId,
    Path(..., description="MongoDB ObjectId"),
    Field(description="Valid MongoDB ObjectId")
]

__all__ = [
    'ValidatedObjectId',
    'validate_object_id'
]
