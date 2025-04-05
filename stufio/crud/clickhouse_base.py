import asyncio
from typing import Generic, TypeVar, Type, Optional, Dict, Any, List, Union, Callable
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from clickhouse_connect.driver.asyncclient import AsyncClient
from stufio.db.clickhouse_base import ClickhouseBase
from stufio.db.clickhouse import ClickhouseDatabase
from stufio.core.config import get_settings
from .base import BaseCRUD

settings = get_settings()
ModelType = TypeVar("ModelType", bound=ClickhouseBase)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)

class CRUDClickhouse(BaseCRUD[ModelType, CreateSchemaType, UpdateSchemaType]):
    """ClickHouse CRUD operations"""

    def __init__(self, model: Type[ModelType], client_factory: Callable[[], AsyncClient] = None):
        """Initialize with model class and optional client factory"""
        super().__init__(model)
        # Store the factory, not the instance (lazy loading)
        self._client_factory = client_factory
        self._client = None

    @property
    async def client(self) -> AsyncClient:
        """Get or create the ClickHouse client instance"""
        if self._client is None:
            if self._client_factory is None:
                self._client = await ClickhouseDatabase()
            else:
                self._client = await self._client_factory()
                
        return self._client

    # Add helper methods to access model metadata
    def get_table_name(self) -> str:
        """Get the table name from the model"""
        return self.model.get_table_name()

    def get_database_name(self) -> str:
        """Get the database name from the model"""
        return self.model.get_database_name()

    async def get(self, id: Any) -> Optional[ModelType]:
        """Get a single record by id"""
        client = await self.client
        result = await client.query(
            f"""
            SELECT *
            FROM {self.model.get_table_name()}
            WHERE id = {{id:String}}
            LIMIT 1
            """,
            parameters={"id": str(id)},
        )
        rows = result.named_results()
        return self.model(**rows[0]) if rows else None

    async def get_by_field(self, field: str, value: Any) -> Optional[ModelType]:
        """Get a single record by field value"""
        client = await self.client
        result = await client.query(
            f"""
            SELECT *
            FROM {self.model.get_table_name()}
            WHERE {field} = {{value}}
            LIMIT 1
            """,
            parameters={"value": value},
        )
        rows = result.named_results()
        return self.model(**rows[0]) if rows else None

    async def get_multi(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None,
        sort: Optional[Any] = None,
        skip: int = 0,
        limit: int = settings.MULTI_MAX,
    ) -> List[ModelType]:
        """Get multiple records with filtering and sorting"""
        client = await self.client
        query = f"""
            SELECT *
            FROM {self.model.get_table_name()}
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

        result = await client.query(query, parameters=params)
        return [self.model(**row) for row in result.named_results()]

    async def create(self, obj_in: CreateSchemaType) -> ModelType:
        """Create a new record"""
        client = await self.client
        # obj_in_data = jsonable_encoder(obj_in)
        obj_in_data = obj_in.model_dump(exclude_unset=True)
        await client.insert(
            self.model.get_table_name(),
            [list(obj_in_data.values())],
            column_names=list(obj_in_data.keys()),
        )
        return self.model(**obj_in_data)

    async def execute_query(self, query: str, parameters: Dict[str, Any] = None) -> Any:
        """Execute a raw ClickHouse query"""
        client = await self.client
        return await client.query(query, parameters=parameters)
