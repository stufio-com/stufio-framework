from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Literal, Optional, TypeVar, Generic, Union
import time
import hashlib
import inspect
import logging

from motor.core import AgnosticDatabase
from clickhouse_connect.driver.asyncclient import AsyncClient
from stufio.models.migration import Migration

logger = logging.getLogger(__name__)

# Define database type variables
DB = TypeVar('DB')

class MigrationScript(ABC, Generic[DB]):
    """Base abstract class for all migration scripts"""

    # Migration metadata
    name: str
    description: str = ""
    migration_type: Literal["init", "schema", "data"] = "schema"
    order: int = 100  # Lower numbers run first
    version: Optional[str] = None  # Filled by migration runner

    @abstractmethod
    async def run(self, db: DB) -> None:
        """Execute the migration script"""
        pass

    def get_checksum(self) -> str:
        """Generate a checksum for this migration script"""
        # Get the source code of the run method
        source = inspect.getsource(self.run)
        return hashlib.md5(source.encode()).hexdigest()

    @property
    @abstractmethod
    def database_type(self) -> str:
        """Return the type of database this migration is for"""
        pass

    async def execute(self, db: DB, module: str, version: str) -> Migration:
        """Execute and record the migration"""
        start_time = time.time()
        error = None
        success = True

        try:
            await self.run(db)
        except Exception as e:
            error = str(e)
            success = False
            logger.error(f"❌ Migration '{self.name}' failed: {error}")
            raise e
        finally:
            end_time = time.time()
            execution_time_ms = (end_time - start_time) * 1000

            # Create migration record
            migration = Migration(
                module=module,
                version=version,
                name=self.name,
                type=self.database_type,  # Use the property instead of checking type
                migration_type=self.migration_type,
                order=self.order,
                executed_at=datetime.utcnow(),
                execution_time_ms=execution_time_ms,
                success=success,
                error=error,
                description=self.description,
                checksum=self.get_checksum()
            )

            return migration


class MongoMigrationScript(MigrationScript[AgnosticDatabase]):
    """MongoDB-specific migration script"""
    
    @property
    def database_type(self) -> str:
        return "mongodb"


class ClickhouseMigrationScript(MigrationScript[AsyncClient]):
    """ClickHouse-specific migration script"""
    
    @property
    def database_type(self) -> str:
        return "clickhouse"
