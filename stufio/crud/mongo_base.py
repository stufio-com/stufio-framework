from typing import Any, Dict, Generic, Optional, Type, TypeVar, Union, List

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from motor.core import AgnosticDatabase
from odmantic import AIOEngine, ObjectId
from odmantic.field import ODMFieldInfo  # Changed from PrimaryField

from stufio.db.mongo_base import MongoBase
from stufio.db.mongo import get_engine
from stufio.core.config import get_settings

settings = get_settings()
ModelType = TypeVar("ModelType", bound=MongoBase)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)


class CRUDMongoBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):
    def __init__(self, model: Type[ModelType]):
        """
        CRUD object with default methods to Create, Read, Update, Delete (CRUD).
        
        Args:
            model: The Odmantic model class
        """
        self.model = model
        self.engine: AIOEngine = get_engine()

    async def get(self, db: AgnosticDatabase, id: ObjectId | str) -> Optional[ModelType]:
        """
        Get a single object by ID using the model's primary field.
        
        Args:
            db: Database connection
            id: The ID value (either ObjectId or string)
            
        Returns:
            The object if found, None otherwise
        """
        
        primary_field_name = None
        for name, field in self.model.__odm_fields__.items():
            # Check if this field has primary_field=True in its ODMFieldInfo
            if hasattr(field, "primary_field") and field.primary_field:
                primary_field_name = name
                break
        
        # Default to 'id' if no primary field found
        if not primary_field_name:
            primary_field_name = 'id'
        
        # Create query using the primary field
        primary_field = getattr(self.model, primary_field_name)
        
        # Handle ID type conversion
        if isinstance(id, str) and primary_field_name == 'id':
            id_value = ObjectId(id)
        else:
            id_value = id
            
        # Query using the primary field
        return await self.engine.find_one(self.model, primary_field == id_value)

    async def get_all(
        self,
        db: AgnosticDatabase,
        *,
        sort: Optional[Any] = None,
        skip: Optional[int] = 0,
        limit: Optional[int] = settings.MULTI_MAX,
    ) -> List[ModelType]:
        """
        Retrieve all objects.

        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            db: Database connection

        Returns:
            List of objects
        """
        return await self.engine.find(self.model, sort=sort, skip=skip, limit=limit)

    async def get_by_field(self, db: AgnosticDatabase, field: str, value: Any) -> Optional[ModelType]:
        """
        Retrieve an object by a specific field.
        """
        return await self.engine.find_one(self.model, field == value)

    async def get_multi(
        self,
        db: AgnosticDatabase,
        *,
        filters: Optional[Dict[str, Any]] = None,
        filter_expression: Optional[
            Any
        ] = None,  # Or alternatively filter by ODMantic expressions
        sort: Optional[Any] = None,
        skip: Optional[int] = 0,
        limit: Optional[int] = settings.MULTI_MAX,
    ) -> List[ModelType]:
        """
        Retrieve multiple objects by multiple fields or by a custom query expression.
        
        Args:
            db: Database connection
            fields: Dictionary of field-value pairs for filtering (legacy approach)
            query_expression: ODMantic query expression (preferred approach)
            sort: Sort criteria
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of matching objects
        """
        if filter_expression is not None:
            # Use the provided ODMantic expression directly
            return await self.engine.find(
                self.model, filter_expression, sort=sort, skip=skip, limit=limit
            )

        elif filters:
            # Convert fields dict to ODMantic expressions
            query = None
            for field_name, value in filters.items():
                field_expr = getattr(self.model, field_name) == value
                if query is None:
                    query = field_expr
                else:
                    # Combine with logical AND
                    query = query & field_expr

            return await self.engine.find(self.model, query, sort=sort, skip=skip, limit=limit)

        else:
            # No filters provided, return all
            return await self.engine.find(self.model, sort=sort, skip=skip, limit=limit)

    async def create(self, db: AgnosticDatabase, *, obj_in: CreateSchemaType) -> ModelType:
        obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(**obj_in_data)  # type: ignore
        return await self.engine.save(db_obj)

    async def update(
        self, db: AgnosticDatabase, *, db_obj: ModelType, obj_in: Union[UpdateSchemaType, Dict[str, Any]]
    ) -> ModelType:
        obj_data = jsonable_encoder(db_obj)
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)
        for field in obj_data:
            if field in update_data:
                setattr(db_obj, field, update_data[field])
                
        # TODO: Check if this saves changes with the setattr calls
        await self.engine.save(db_obj)
        return db_obj

    async def remove(self, db: AgnosticDatabase, *, id: ObjectId | str) -> Optional[ModelType]:
        obj = await self.get(db=db, id=id) 
        if obj:
            await self.engine.delete(obj)
            
        return obj
