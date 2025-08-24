"""
FastAPI dependencies package.
"""

from .mongo import ValidatedObjectId, validate_object_id

__all__ = [
    'ValidatedObjectId',
    'validate_object_id'
]
