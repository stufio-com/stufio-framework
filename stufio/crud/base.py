from abc import ABC
from typing import Generic, TypeVar, Type, Optional, Dict, Any, List, Union, Callable
from pydantic import BaseModel

ModelType = TypeVar("ModelType")
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)

class BaseCRUD(ABC, Generic[ModelType, CreateSchemaType, UpdateSchemaType]):
    """Base CRUD class with common methods"""
    
    def __init__(self, model: Type[ModelType]):
        """Initialize with model class"""
        self.model = model