from typing import Any, Dict, Generic, Optional, Type, TypeVar, Union, List, Callable
from fastapi.encoders import jsonable_encoder
from httpx import get
from pydantic import BaseModel
from odmantic import AIOEngine, Model, ObjectId
from stufio.db.mongo_base import MongoBase
from stufio.db.mongo import serialize_mongo_doc
from stufio.core.config import get_settings
from .base import BaseCRUD

settings = get_settings()
ModelType = TypeVar("ModelType", bound=MongoBase)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)

class CRUDMongo(BaseCRUD[ModelType, CreateSchemaType, UpdateSchemaType]):
    """MongoDB CRUD operations using ODMantic AIOEngine"""

    def __init__(self, model: Type[ModelType], engine_factory: Callable[[], AIOEngine] = None):
        """Initialize with model class and optional engine factory"""
        super().__init__(model)
        # Store the factory, not the instance (lazy loading)
        self._engine_factory = engine_factory
        self._engine = None

    @property
    def engine(self) -> AIOEngine:
        """Get or create the engine instance"""
        if self._engine is None:
            if self._engine_factory is None:
                # Default engine factory if none provided
                from stufio.db.mongo import get_engine
                self._engine_factory = get_engine
            self._engine = self._engine_factory()
        return self._engine

    async def get(self, id: Union[ObjectId, str]) -> Optional[ModelType]:
        """Get an object by ID"""
        primary_field_name = None
        for name, field in self.model.__odm_fields__.items():
            if hasattr(field, "primary_field") and getattr(field, "primary_field", False):
                primary_field_name = name
                break

        if not primary_field_name:
            primary_field_name = 'id'

        primary_field = getattr(self.model, primary_field_name)

        if isinstance(id, str) and primary_field_name == 'id':
            id_value = ObjectId(id)
        else:
            id_value = id

        return await self.engine.find_one(self.model, primary_field == id_value)

    async def get_by_field(self, field: str, value: Any) -> Optional[ModelType]:
        """Get an object by a specific field"""
        model_field = getattr(self.model, field)
        return await self.engine.find_one(self.model, model_field == value)

    async def get_by_fields(self, **kwargs) -> Optional[ModelType]:
        """Get an object by multiple field-value pairs."""
        query = None
        for field_name, value in kwargs.items():
            field_expr = getattr(self.model, field_name) == value
            if query is None:
                query = field_expr
            else:
                query = query & field_expr
        return await self.engine.find_one(self.model, query)

    async def get_multi(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None,
        filter_expression: Optional[Any] = None,
        sort: Optional[Any] = None,
        skip: int = 0,
        limit: int = settings.MULTI_MAX,
    ) -> List[ModelType]:
        """Get multiple objects with filtering and sorting"""
        if filter_expression is not None:
            return await self.engine.find(
                self.model, filter_expression, sort=sort, skip=skip, limit=limit
            )
        elif filters:
            query = None
            for field_name, value in filters.items():
                field_expr = getattr(self.model, field_name) == value
                if query is None:
                    query = field_expr
                else:
                    query = query & field_expr
            return await self.engine.find(self.model, query, sort=sort, skip=skip, limit=limit)
        else:
            return await self.engine.find(self.model, sort=sort, skip=skip, limit=limit)

    async def create(self, obj_in: CreateSchemaType) -> ModelType:
        """Create a new object"""
        obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(**obj_in_data)
        return await self.engine.save(db_obj)
    
    async def save(self, db_obj: ModelType) -> ModelType:
        """Save (insert or update) an object in database."""
        return await self.engine.save(db_obj)

    async def update(self, db_obj: ModelType, update_data: Union[Dict[str, Any], ModelType]) -> ModelType:
        """Update a model in database."""
        obj_data = jsonable_encoder(db_obj)
        
        # If update_data is a model, convert it to dict excluding id
        if hasattr(update_data, "model_dump"):
            update_data = update_data.model_dump(exclude={"id"})
        
        # Ensure we don't update the primary key
        if "id" in update_data:
            del update_data["id"]
            
        # Update fields
        for field in obj_data:
            if field in update_data:
                setattr(db_obj, field, update_data[field])
                
        # Save to database
        await self.engine.save(db_obj)
        return db_obj

    async def remove(self, id: Union[ObjectId, str]) -> Optional[ModelType]:
        """Delete an object by ID"""
        obj = await self.get(id=id)
        if obj:
            await self.engine.delete(obj)
        return obj

    async def execute_query(self, query_func, *args, **kwargs):
        """
        For operations that need direct access to PyMongo methods

        Example usage:
        ```
        async def complex_aggregation(self, pipeline):
            return await self.execute_query(
                lambda collection: collection.aggregate(pipeline)
            )
        ```
        """
        db = self.engine.client[self.engine.database]
        collection = db[self.model.get_collection_name()]
        return await query_func(collection)

    # Enhanced methods for response serialization
    def serialize_model(self, model: Optional[ModelType]) -> Optional[Dict[str, Any]]:
        """
        Serialize a single ODMantic model with proper ObjectId conversion.
        
        Args:
            model: ODMantic model instance
            
        Returns:
            Serialized dictionary or None
        """
        if not model:
            return None
        
        model_dict = model.model_dump()
        return serialize_mongo_doc(model_dict)
    
    def serialize_models(self, models: List[ModelType]) -> List[Dict[str, Any]]:
        """
        Serialize a list of ODMantic models with proper ObjectId conversion.
        
        Args:
            models: List of ODMantic model instances
            
        Returns:
            List of serialized dictionaries
        """
        result = []
        for model in models:
            if model is not None:
                serialized = self.serialize_model(model)
                if serialized is not None:
                    result.append(serialized)
        return result
    
    async def get_serialized(self, id: Union[ObjectId, str]) -> Optional[Dict[str, Any]]:
        """Get an object by ID and return serialized dict."""
        model = await self.get(id)
        return self.serialize_model(model)
    
    async def get_multi_serialized(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None,
        filter_expression: Optional[Any] = None,
        sort: Optional[Any] = None,
        skip: int = 0,
        limit: int = settings.MULTI_MAX,
    ) -> List[Dict[str, Any]]:
        """Get multiple objects and return serialized dicts."""
        models = await self.get_multi(
            filters=filters,
            filter_expression=filter_expression,
            sort=sort,
            skip=skip,
            limit=limit
        )
        return self.serialize_models(models)
