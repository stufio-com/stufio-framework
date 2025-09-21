from abc import ABC, abstractmethod
from datetime import datetime, timezone
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
            # Enhanced retry logic with reconnection for connection issues
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    result = await self._original_client.command(statement, *args, **kwargs)
                    results.append(result)
                    break
                except Exception as e:
                    connection_errors = [
                        "Connection broken", "IncompleteRead", "Connection closed",
                        "Connection lost", "Connection refused", "Connection timeout",
                        "Network is unreachable", "Connection reset"
                    ]
                    
                    is_connection_error = any(error_type in str(e) for error_type in connection_errors)
                    
                    if attempt < max_retries - 1 and is_connection_error:
                        logger.warning(f"Connection error on attempt {attempt + 1} for migration '{self._migration_name}': {e}")
                        
                        # Try to force reconnection for severe connection issues
                        if "Connection broken" in str(e) or "IncompleteRead" in str(e):
                            logger.info(f"Attempting to force ClickHouse reconnection due to: {type(e).__name__}")
                            try:
                                from stufio.db.clickhouse import force_reconnect
                                reconnect_success = await force_reconnect(f"migration_retry_{self._migration_name}")
                                if reconnect_success:
                                    logger.info("ClickHouse reconnection successful, retrying statement")
                                    # Update our client reference to the new connection
                                    from stufio.db.clickhouse import ClickhouseDatabase
                                    self._original_client = await ClickhouseDatabase()
                                else:
                                    logger.warning("ClickHouse reconnection failed, will retry with existing connection")
                            except Exception as reconnect_error:
                                logger.warning(f"Reconnection attempt failed: {reconnect_error}")
                        
                        logger.info(f"Retrying statement in {retry_delay}s (attempt {attempt + 2}/{max_retries})")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                    else:
                        # Re-raise the exception if max retries exceeded or different error
                        logger.error(f"Migration '{self._migration_name}' failed after {attempt + 1} attempts: {e}")
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
                executed_at=datetime.now(timezone.utc),
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
    ClickHouse-specific migration script with cluster support.

    This version automatically:
    1. Converts `MergeTree` family engines to their `Replicated*` counterparts for data replication
    2. Adds ON CLUSTER clauses for DDL distribution when cluster is configured
    3. Skips DML statements (SELECT, INSERT, UPDATE, DELETE) - no transformation needed
    4. Uses ReplicatedMergeTree for high availability and automatic replication
    
    Configuration (both required for cluster mode):
    - CLICKHOUSE_CLUSTER_DSN_LIST: List of cluster node DSNs
    - CLICKHOUSE_CLUSTER_NAME: Name of the ClickHouse cluster
    
    Single-node mode:
    - When cluster settings are not configured, runs in single-node mode
    - No ON CLUSTER clauses or engine transformations
    - Direct execution against single ClickHouse instance
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
        """Check if cluster support is enabled.
        
        Requires both CLICKHOUSE_CLUSTER_DSN_LIST and CLICKHOUSE_CLUSTER_NAME to be properly configured.
        """
        from stufio.core.config import get_settings
        settings = get_settings()
        
        cluster_dsn_list = getattr(settings, 'CLICKHOUSE_CLUSTER_DSN_LIST', None)
        cluster_name = getattr(settings, 'CLICKHOUSE_CLUSTER_NAME', None)
        
        # Both cluster DSN list and cluster name must be configured
        return (bool(cluster_dsn_list) and 
                bool(cluster_name) and 
                cluster_name not in ['None', 'none', ''])

    def _add_on_cluster_if_needed(self, statement: str) -> str:
        """Add ON CLUSTER clause to DDL statements when cluster is configured.
        
        Only adds ON CLUSTER when both cluster DSN list AND cluster name are properly configured.
        """
        if not self._is_cluster_enabled():
            return statement

        from stufio.core.config import get_settings
        settings = get_settings()
        cluster_name = getattr(settings, 'CLICKHOUSE_CLUSTER_NAME', None)
        
        # Only proceed if cluster name is explicitly configured
        if not cluster_name or cluster_name in ['None', 'none', '']:
            return statement

        statement_upper = statement.upper().strip()
        
        # Only add ON CLUSTER to DDL statements that support it
        ddl_keywords = [
            'CREATE TABLE', 'CREATE OR REPLACE TABLE', 'CREATE VIEW', 'CREATE MATERIALIZED VIEW',
            'DROP TABLE', 'DROP VIEW', 'ALTER TABLE', 'RENAME TABLE', 'TRUNCATE TABLE'
        ]
        
        # Check if this is a DDL statement that supports ON CLUSTER
        if any(statement_upper.startswith(keyword) for keyword in ddl_keywords):
            # Check if ON CLUSTER is already present
            if 'ON CLUSTER' not in statement_upper:
                
                # Handle CREATE statements: CREATE [OR REPLACE] [MATERIALIZED] TABLE [IF NOT EXISTS] table_name ...
                if statement_upper.startswith(('CREATE TABLE', 'CREATE OR REPLACE TABLE', 'CREATE VIEW', 'CREATE MATERIALIZED VIEW')):
                    # Pattern to match: CREATE [modifiers] table_name (columns...)
                    # Table name can be: table, `table`, schema.table, `schema`.`table`
                    create_pattern = re.compile(
                        r'(CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?)(`?[^.\s]+`?(?:\.`?[^.\s]+`?)?)\s*(\(.*)',
                        re.IGNORECASE | re.DOTALL
                    )
                    match = create_pattern.match(statement)
                    if match:
                        prefix = match.group(1)  # CREATE ... part
                        table_name = match.group(2)  # table name
                        rest = match.group(3)  # column definitions and rest
                        return f"{prefix}{table_name} ON CLUSTER '{cluster_name}' {rest}"
                
                # Handle DROP statements: DROP TABLE [IF EXISTS] table_name
                elif statement_upper.startswith(('DROP TABLE', 'DROP VIEW')):
                    drop_pattern = re.compile(
                        r'(DROP\s+(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?)(`?[^.\s]+`?(?:\.`?[^.\s]+`?)?)(.*)',
                        re.IGNORECASE | re.DOTALL
                    )
                    match = drop_pattern.match(statement)
                    if match:
                        prefix = match.group(1)  # DROP ... part
                        table_name = match.group(2)  # table name
                        rest = match.group(3)  # rest of statement
                        return f"{prefix}{table_name} ON CLUSTER '{cluster_name}'{rest}"
                
                # Handle ALTER statements: ALTER TABLE table_name ...
                elif statement_upper.startswith('ALTER TABLE'):
                    alter_pattern = re.compile(
                        r'(ALTER\s+TABLE\s+)(`?[^.\s]+`?(?:\.`?[^.\s]+`?)?)(.*)',
                        re.IGNORECASE | re.DOTALL
                    )
                    match = alter_pattern.match(statement)
                    if match:
                        prefix = match.group(1)  # ALTER TABLE part
                        table_name = match.group(2)  # table name
                        rest = match.group(3)  # rest of statement
                        return f"{prefix}{table_name} ON CLUSTER '{cluster_name}'{rest}"
        
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
                executed_at=datetime.now(timezone.utc),
                execution_time_ms=execution_time_ms,
                success=success,
                error=error,
                description=self.description,
                checksum=self.get_checksum(),
            )

            return migration
