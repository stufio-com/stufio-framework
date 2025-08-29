from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Literal, Optional, TypeVar, Generic, Union
import time
import hashlib
import inspect
import logging
import re
import asyncio

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
        """Transform SQL for cluster and execute with retry logic"""
        transformed_sql = self._transform_func(sql)
        if transformed_sql != sql:
            logger.debug(
                f"Auto-transformed SQL for cluster in migration '{self._migration_name}':\n"
                f"---\nOriginal:\n{sql}\n---\nTransformed:\n{transformed_sql}\n---"
            )
        
        # Split transformed SQL by semicolons and execute separately
        # ClickHouse doesn't support multi-statements in a single command
        statements = [stmt.strip() for stmt in transformed_sql.split(';') if stmt.strip()]
        
        results = []
        for statement in statements:
            # Retry logic for connection issues
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    result = await self._original_client.command(statement, *args, **kwargs)
                    results.append(result)
                    break
                except Exception as e:
                    if attempt < max_retries - 1 and ("Connection broken" in str(e) or "IncompleteRead" in str(e)):
                        logger.warning(f"Connection error on attempt {attempt + 1}, retrying in {retry_delay}s: {e}")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                    else:
                        # Re-raise the exception if max retries exceeded or different error
                        raise
        
        # Return the last result (or first if only one)
        return results[-1] if results else None
    
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
    ClickHouse-specific migration script with simplified cluster support.

    This version automatically:
    1. Converts `MergeTree` family engines to their `Replicated*` counterparts for data replication
    2. Lets the ClickHouse cluster handle distribution automatically (no ON CLUSTER needed)
    3. Skips DML statements (SELECT, INSERT, UPDATE, DELETE) - no transformation needed
    4. Uses ReplicatedMergeTree for high availability and automatic replication
    
    Configuration:
    - CLICKHOUSE_CLUSTER_DSN_LIST: Enables cluster mode when set
    
    Benefits of this simplified approach:
    - No need for ON CLUSTER clauses - ClickHouse handles this automatically
    - No need for CLICKHOUSE_CLUSTER_NAME configuration
    - Cleaner, more maintainable SQL transformations
    - Focus solely on engine replication for data availability
    """

    @property
    def database_type(self) -> str:
        return "clickhouse"

    def _should_create_distributed_tables(self) -> bool:
        """Check if distributed tables should be created alongside replicated tables."""
        from stufio.core.config import get_settings
        settings = get_settings()
        # Default to False - ReplicatedMergeTree handles distribution automatically
        return bool(getattr(settings, 'CLICKHOUSE_CREATE_DISTRIBUTED_TABLES', False))

    def _is_cluster_enabled(self) -> bool:
        """Check if cluster support is enabled."""
        from stufio.core.config import get_settings
        settings = get_settings()
        return bool(getattr(settings, 'CLICKHOUSE_CLUSTER_DSN_LIST', None))

    def _add_on_cluster_if_needed(self, statement: str) -> str:
        """Add ON CLUSTER clause to DDL statements when cluster is configured.
        
        NOTE: ON CLUSTER is disabled because the current ClickHouse version 
        doesn't support this syntax. Instead, we rely on ReplicatedMergeTree
        engines for data replication after tables are manually created on each node.
        """
        # ON CLUSTER is not supported in this ClickHouse version, skip transformation
        return statement

    def _transform_sql_for_cluster(self, sql: str) -> str:
        """
        Transforms SQL DDL statements to support a ClickHouse cluster deployment.
        
        Two-phase Strategy:
        1. Convert MergeTree engines to ReplicatedMergeTree for automatic replication
        2. Add ON CLUSTER for DDL distribution to ensure tables exist on all nodes
        3. Skip DML statements (SELECT, INSERT, UPDATE, DELETE) - no transformation needed
        """
        if not self._is_cluster_enabled():
            return sql

        transformed_statements = []

        # Process each SQL statement in the script individually
        for statement in self._split_sql_statements(sql):
            statement_upper = statement.upper().strip()
            
            # Skip transformation for DML statements (SELECT, INSERT, UPDATE, DELETE)
            if any(statement_upper.startswith(dml) for dml in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH']):
                transformed_statements.append(statement)
                continue
            
            # --- Handle CREATE TABLE with ENGINE conversion only ---
            create_table_pattern = re.compile(
                r"^(CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)(?:(`?[\w]+`?)\.)?(`?[\w]+`?)",
                re.IGNORECASE
            )
            create_table_match = create_table_pattern.match(statement)
            
            if create_table_match and "ENGINE" in statement.upper():
                db_name = create_table_match.group(2) or ""  # database name (optional)
                table_name = create_table_match.group(3)     # table_name
                
                # Find ENGINE clause and convert MergeTree to ReplicatedMergeTree
                engine_pattern = re.compile(r"ENGINE\s*=\s*([a-zA-Z0-9_]+)(\([^)]*\))?", re.IGNORECASE)
                engine_match = engine_pattern.search(statement)
                
                if engine_match:
                    engine_name = engine_match.group(1)
                    engine_params = engine_match.group(2) or ""
                    
                    # Convert MergeTree engines to their replicated counterparts
                    if "MergeTree" in engine_name and "Replicated" not in engine_name:
                        replicated_engine_name = f"Replicated{engine_name}"
                        
                        # Clean names for ZooKeeper paths (remove backticks)
                        clean_db_name = db_name.strip("`") if db_name else ""
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
                        
                        # Replace the engine in the original statement
                        original_engine = f"ENGINE = {engine_name}{engine_params}"
                        new_engine = f"ENGINE = {replicated_engine_name}{replicated_engine_params}"
                        transformed_statement = statement.replace(original_engine, new_engine)
                        
                        # Add ON CLUSTER for DDL distribution
                        transformed_statement = self._add_on_cluster_if_needed(transformed_statement)
                        transformed_statements.append(transformed_statement)
                    else:
                        # Keep original statement (already replicated or other engine type)
                        # but still add ON CLUSTER if needed
                        transformed_statement = self._add_on_cluster_if_needed(statement)
                        transformed_statements.append(transformed_statement)
                        
                    continue

            # For all other DDL statements (CREATE VIEW, DROP, ALTER, etc.)
            # Add ON CLUSTER for distribution
            transformed_statement = self._add_on_cluster_if_needed(statement)
            transformed_statements.append(transformed_statement)

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
