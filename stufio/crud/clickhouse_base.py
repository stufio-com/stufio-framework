from typing import Any, Dict, Generic, Optional, Type, TypeVar, Union, List
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from clickhouse_connect.driver.asyncclient import AsyncClient

from stufio.db.clickhouse_base import ClickhouseBase
from stufio.core.config import get_settings

settings = get_settings()
ModelType = TypeVar("ModelType", bound=ClickhouseBase)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)


class CRUDClickhouseBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):

    def __init__(self, model: Type[ModelType]):
        """CRUD object with default methods"""
        self.model = model

    def get_database_name(self) -> str:
        """Get the database name"""
        return self.model.get_database_name()

    def get_table_name(self) -> str:
        """Get fully qualified table name"""
        return self.model.get_table_name()

    async def get(self, db: AsyncClient, id: Any) -> Optional[ModelType]:
        """Get a single record by id"""
        result = await db.query(
            f"""
            SELECT *
            FROM {self.get_table_name()}
            WHERE id = {{id:String}}
            LIMIT 1
            """,
            parameters={"id": str(id)},
        )
        rows = result.named_results()
        return self.model(**rows[0]) if rows else None

    async def get_by_field(
        self, db: AsyncClient, field: str, value: Any
    ) -> Optional[ModelType]:
        """
        Retrieve an object by a specific field.
        """
        result = await db.query(
            f"""
            SELECT *
            FROM {self.get_table_name()}
            WHERE {field} = {{value}}
            LIMIT 1
            """,
            parameters={"value": value},
        )
        rows = result.named_results()
        return self.model(**rows[0]) if rows else None

    async def get_multi(
        self,
        db: AsyncClient,
        *,
        filters: Optional[Dict[str, Any]] = None,
        sort: Optional[Any] = None,
        skip: Optional[int] = 0,
        limit: Optional[int] = settings.MULTI_MAX,
    ) -> List[ModelType]:
        """
        Retrieve multiple objects by multiple fields or by a custom query expression.
        
        Args:
            db: Database connection
            fields: Dictionary of field-value pairs for filtering (legacy approach)
            sort: Sort criteria
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of matching objects
        """
        query = f"""
            SELECT *
            FROM {self.get_table_name()}
            WHERE 1=1
        """

        params = {}
        if filters:
            for key, value in filters.items():
                query += f" AND {key} = {{{key}}}"
                params[key] = value

        if sort:
            if isinstance(sort, str):
                query += f" ORDER BY {sort}"
            elif isinstance(sort, list):
                query += f" ORDER BY {', '.join(sort)}"
            else:
                raise ValueError("Sort parameter must be a string or a list of strings")

        query += f" LIMIT {limit} OFFSET {skip}"

        result = await db.query(query, parameters=params)
        return [self.model(**row) for row in result.named_results()]


    async def create(self, db: AsyncClient, *, obj_in: CreateSchemaType) -> ModelType:
        """Create a new record"""
        obj_in_data = jsonable_encoder(obj_in)
        await db.insert(
            self.get_table_name(), [obj_in_data], column_names=list(obj_in_data.keys())
        )
        return self.model(**obj_in_data)

    async def update(
        self,
        db: AsyncClient,
        *,
        db_obj: ModelType,
        obj_in: Union[UpdateSchemaType, Dict[str, Any]],
    ) -> ModelType:
        """Update a record (Clickhouse doesn't support updates, so we insert new version)"""
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)

        # Use the new get_primary_fields method
        primary_fields = self.model.get_primary_fields()
        for field in primary_fields:
            update_data[field] = getattr(db_obj, field)

        await db.insert(
            self.get_table_name(), [update_data], column_names=list(update_data.keys())
        )

        # If there are multiple primary fields, we need to construct a filter
        if len(primary_fields) > 1:
            filters = {field: getattr(db_obj, field) for field in primary_fields}
            results = await self.get_multi(db, limit=1, filters=filters)
            return results[0] if results else None
        else:
            return await self.get(db, getattr(db_obj, primary_fields[0]))

    async def remove(self, db: AsyncClient, *, id: Any) -> bool:
        """Delete a record (implemented as mutation in Clickhouse)"""
        try:
            await db.query(
                f"""
                ALTER TABLE {self.get_table_name()}
                DELETE WHERE id = {{id:String}}
                """,
                parameters={"id": str(id)},
            )
            return True
        except Exception:
            return False
