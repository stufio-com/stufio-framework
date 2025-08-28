from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Literal, Optional, TypeVar, Generic, Union
import time
import hashlib
import inspect
import logging
import re

from motor.core import AgnosticDatabase
from clickhouse_connect.driver.asyncclient import AsyncClient
from odmantic import ObjectId
from stufio.models.migration import Migration

logger = logging.getLogger(__name__)

class ClusterAwareAsyncClient:
    """Wrapper for AsyncClient that automatically transforms SQL for cluster mode"""
    
    def __init__(self, original_client: AsyncClient, transform_func, migration_name: str):
        self._original_client = original_client
        self._transform_func = transform_func
        self._migration_name = migration_name
    
    async def command(self, sql: str, *args, **kwargs):
        """Transform SQL for cluster and execute"""
        transformed_sql = self._transform_func(sql)
        if transformed_sql != sql:
            logger.debug(
                f"Auto-transformed SQL for cluster in migration '{self._migration_name}':\n"
                f"---\nOriginal:\n{sql}\n---\nTransformed:\n{transformed_sql}\n---"
            )
        return await self._original_client.command(transformed_sql, *args, **kwargs)
    
    def __getattr__(self, name):
        """Proxy all other attributes to the original client"""
        return getattr(self._original_client, name)

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
                id=ObjectId(),
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
    """
    ClickHouse-specific migration script with enhanced cluster support.

    This version automatically:
    1. Adds `ON CLUSTER` to DDL statements (`CREATE`, `DROP`, `ALTER`).
    2. Converts `MergeTree` family engines to their `Replicated*` counterparts.
    3. Generates a corresponding `Distributed` table for each replicated table created.
    """

    @property
    def database_type(self) -> str:
        return "clickhouse"

    def _get_cluster_name(self) -> str:
        """Get the configured cluster name."""
        from stufio.core.config import get_settings
        settings = get_settings()
        cluster_name = getattr(settings, 'CLICKHOUSE_CLUSTER_NAME', None)
        if not cluster_name:
            raise ValueError(
                "CLICKHOUSE_CLUSTER_NAME must be configured when using cluster mode"
            )
        return cluster_name

    def _is_cluster_enabled(self) -> bool:
        """Check if cluster support is enabled."""
        from stufio.core.config import get_settings
        settings = get_settings()
        return bool(getattr(settings, 'CLICKHOUSE_CLUSTER_DSN_LIST', None))

    def _transform_sql_for_cluster(self, sql: str) -> str:
        """
        Transforms SQL DDL statements to support a ClickHouse cluster deployment.
        """
        if not self._is_cluster_enabled():
            return sql

        cluster_name = self._get_cluster_name()
        transformed_statements = []

        # Process each SQL statement in the script individually
        for statement in self._split_sql_statements(sql):
            # --- Handle CREATE TABLE with ENGINE ---
            create_table_pattern = re.compile(
                r"^(CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)(`?[\w\.]*\.`?)?(`?[\w]+`?)",
                re.IGNORECASE
            )
            create_table_match = create_table_pattern.match(statement)
            
            if create_table_match and "ENGINE" in statement.upper():
                create_prefix = create_table_match.group(1)  # CREATE TABLE IF NOT EXISTS
                db_name = create_table_match.group(2) or ""  # database. (optional)
                table_name = create_table_match.group(3)     # table_name
                
                # Extract everything after the table name
                table_end = create_table_match.end()
                remaining_sql = statement[table_end:]
                
                # Find ENGINE clause
                engine_pattern = re.compile(r"ENGINE\s*=\s*([a-zA-Z0-9_]+)(\([^)]*\))?", re.IGNORECASE)
                engine_match = engine_pattern.search(remaining_sql)
                
                if engine_match:
                    engine_name = engine_match.group(1)
                    engine_params = engine_match.group(2) or ""
                    
                    # Get everything before and after ENGINE clause
                    before_engine = remaining_sql[:engine_match.start()]
                    after_engine = remaining_sql[engine_match.end():]
                    
                    # Build transformed CREATE statement with ON CLUSTER
                    transformed_create = f"{create_prefix} {db_name}{table_name} ON CLUSTER '{cluster_name}'{before_engine}"
                    
                    # --- Convert MergeTree to ReplicatedMergeTree ---
                    if "MergeTree" in engine_name and "Replicated" not in engine_name:
                        replicated_engine_name = f"Replicated{engine_name}"
                        
                        # Clean names for paths
                        clean_db_name = db_name.strip("`.")
                        clean_table_name = table_name.strip("`")
                        
                        # If no database specified, use default database name
                        if not clean_db_name:
                            from stufio.db.clickhouse import get_database_from_dsn
                            clean_db_name = get_database_from_dsn()
                        
                        zookeeper_path = f"'/clickhouse/tables/{{shard}}/{clean_db_name}.{clean_table_name}'"
                        
                        # Build replicated engine parameters
                        if engine_params.strip() in ["", "()"]:
                            replicated_engine_params = f"({zookeeper_path}, '{{replica}}')"
                        else:
                            replicated_engine_params = f"({zookeeper_path}, '{{replica}}', {engine_params.strip('()')})"
                        
                        # Complete the statement
                        transformed_create += f"ENGINE = {replicated_engine_name}{replicated_engine_params}{after_engine}"
                        
                        # --- Create Distributed table ---
                        distributed_table_name = f"`{clean_db_name}`.`{clean_table_name}_distributed`"
                        local_table_name = f"`{clean_db_name}`.`{clean_table_name}`"
                        
                        distributed_create = (
                            f"CREATE TABLE IF NOT EXISTS {distributed_table_name} ON CLUSTER '{cluster_name}' "
                            f"AS {local_table_name} "
                            f"ENGINE = Distributed('{cluster_name}', '{clean_db_name}', '{clean_table_name}', rand())"
                        )
                        
                        # Add both tables
                        transformed_statements.append(transformed_create)
                        transformed_statements.append(distributed_create)
                    else:
                        # Keep original engine
                        transformed_create += f"ENGINE = {engine_name}{engine_params}{after_engine}"
                        transformed_statements.append(transformed_create)
                        
                    continue

            # --- Handle CREATE DATABASE ---
            create_db_pattern = re.compile(
                r"^(CREATE\s+DATABASE(?:\s+IF\s+NOT\s+EXISTS)?\s+)(`?[\w\.]+`?)",
                re.IGNORECASE
            )
            if create_db_pattern.match(statement):
                statement = create_db_pattern.sub(
                    rf"\1\2 ON CLUSTER '{cluster_name}'", statement
                )
                transformed_statements.append(statement)
                continue

            # --- Handle DROP statements ---
            drop_pattern = re.compile(
                r"^(DROP\s+(?:TABLE|VIEW|DATABASE)\s+(?:IF\s+EXISTS\s+)?)(`?[\w\.]*\.`?)?(`?[\w]+`?)",
                re.IGNORECASE,
            )
            drop_match = drop_pattern.match(statement)
            if drop_match:
                drop_prefix = drop_match.group(1)
                db_name = drop_match.group(2) or ""
                object_name = drop_match.group(3)

                # Add ON CLUSTER
                transformed_drop = f"{drop_prefix} {db_name}{object_name} ON CLUSTER '{cluster_name}'"
                transformed_statements.append(transformed_drop)

                # Also drop the _distributed table if it's a table drop
                if "TABLE" in drop_prefix.upper():
                    clean_db_name = db_name.strip("`.")
                    clean_object_name = object_name.strip("`")
                    if not clean_db_name:
                        from stufio.db.clickhouse import get_database_from_dsn
                        clean_db_name = get_database_from_dsn()
                    distributed_table_name = f"`{clean_db_name}`.`{clean_object_name}_distributed`"
                    distributed_drop = f"DROP TABLE IF EXISTS {distributed_table_name} ON CLUSTER '{cluster_name}'"
                    transformed_statements.append(distributed_drop)
                continue

            # --- Handle ALTER TABLE statements ---
            alter_pattern = re.compile(
                r"^(ALTER\s+TABLE\s+)(`?[\w\.]*\.`?)?(`?[\w]+`?)(\s+.*)",
                re.IGNORECASE | re.DOTALL,
            )
            alter_match = alter_pattern.match(statement)
            if alter_match:
                alter_prefix = alter_match.group(1)
                db_name = alter_match.group(2) or ""
                table_name = alter_match.group(3)
                operation = alter_match.group(4)

                # Add ON CLUSTER after the table name
                transformed_alter = f"{alter_prefix} {db_name}{table_name} ON CLUSTER '{cluster_name}'{operation}"
                transformed_statements.append(transformed_alter)
                continue

            # If no pattern matched, keep original
            transformed_statements.append(statement)

        return ";\n".join(stmt for stmt in transformed_statements if stmt)

    def _split_sql_statements(self, sql: str) -> list[str]:
        """Splits a SQL script into individual statements, handling semicolons inside strings."""
        # A simple split by semicolon is often sufficient for DDL scripts
        # For more complex scripts, a more sophisticated parser might be needed.
        return [s.strip() for s in sql.split(";") if s.strip()]

    async def execute_sql(self, db: AsyncClient, sql: str, *args, **kwargs):
        """Execute SQL with automatic cluster transformation if needed."""
        transformed_sql = self._transform_sql_for_cluster(sql)
        if transformed_sql != sql:
            logger.debug(
                f"Transformed SQL for cluster:\n---\nOriginal:\n{sql}\n---\nTransformed:\n{transformed_sql}\n---"
            )

        # ClickHouse client can execute multiple statements separated by semicolons
        return await db.command(transformed_sql, *args, **kwargs)

    async def execute(self, db: AsyncClient, module: str, version: str) -> Migration:
        """Execute and record the migration with cluster-aware SQL transformation."""
        start_time = time.time()
        error = None
        success = True

        try:
            # Create a cluster-aware database wrapper if cluster mode is enabled
            if self._is_cluster_enabled():
                db_wrapper = ClusterAwareAsyncClient(db, self._transform_sql_for_cluster, self.name)
                await self.run(db_wrapper)  # type: ignore
            else:
                await self.run(db)
                
        except Exception as e:
            error = str(e)
            success = False
            logger.error(f"❌ Migration '{self.name}' failed: {error}")
            raise
        finally:
            end_time = time.time()
            execution_time_ms = (end_time - start_time) * 1000

            migration = Migration(
                id=ObjectId(),
                module=module,
                version=version,
                name=self.name,
                type="clickhouse",
                migration_type=self.migration_type,
                order=self.order,
                executed_at=datetime.utcnow(),
                execution_time_ms=execution_time_ms,
                success=success,
                error=error,
                description=self.description,
                checksum=self.get_checksum(),
            )

            return migration
