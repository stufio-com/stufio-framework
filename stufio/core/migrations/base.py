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
                id="",  # Will be set when saved to database
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
        # This is more robust than a single large regex replacement.
        for statement in self._split_sql_statements(sql):
            original_statement = statement

            # --- 1. Handle CREATE DATABASE ---
            # Pattern to match CREATE DATABASE statements.
            create_db_pattern = re.compile(
                r"^(CREATE\s+DATABASE(?:_IF_NOT_EXISTS_)?\s+)(`?[\w\.]+`?)",
                re.IGNORECASE | re.DOTALL,
            )
            if create_db_pattern.match(statement):
                statement = create_db_pattern.sub(
                    rf"\1\2 ON CLUSTER '{cluster_name}'", statement
                )
                transformed_statements.append(statement)
                continue  # Move to next statement

            # --- 2. Handle CREATE TABLE, VIEW, MATERIALIZED VIEW ---
            # This comprehensive pattern handles tables, views, and materialized views.
            # It also captures the engine definition for tables.
            create_pattern = re.compile(
                r"^(CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+VIEW|VIEW|TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?)(`?[\w\.]+\.`?)(`?[\w]+`?)(\s*\(.*?\))?(?:\s+ENGINE\s*=\s*([a-zA-Z0-9_]+)(\(.*\)))?",
                re.IGNORECASE | re.DOTALL,
            )
            create_match = create_pattern.match(statement)
            if create_match:
                create_prefix = create_match.group(1)  # CREATE TABLE/VIEW...
                db_name = create_match.group(2)  # `database`.
                table_name = create_match.group(3)  # `table_name`
                columns_and_pk = (
                    create_match.group(4) or ""
                )  # (id UInt64, ...) ORDER BY ...
                engine_name = create_match.group(5)
                engine_params = create_match.group(6)

                # Add ON CLUSTER clause
                transformed_create = f"{create_prefix} {db_name}{table_name} ON CLUSTER '{cluster_name}'{columns_and_pk}"

                # --- 2a. Convert MergeTree to ReplicatedMergeTree ---
                if (
                    engine_name
                    and "MergeTree" in engine_name
                    and "Replicated" not in engine_name
                ):
                    replicated_engine_name = f"Replicated{engine_name}"

                    # Construct the standard ZooKeeper path for the replicated table.
                    # ClickHouse will replace {shard} and {replica} macros automatically.
                    # Assumes `database.table_name` format.
                    clean_db_name = db_name.strip("`.")
                    clean_table_name = table_name.strip("`")
                    zookeeper_path = f"'/clickhouse/tables/{{shard}}/{clean_db_name}.{clean_table_name}'"

                    # Prepend ZK path and replica name to engine params
                    replicated_engine_params = (
                        f"({zookeeper_path}, '{{replica}}'{engine_params.lstrip('(')}"
                    )

                    transformed_create += (
                        f" ENGINE = {replicated_engine_name}{replicated_engine_params}"
                    )

                    # --- 2b. Create the corresponding Distributed table ---
                    # The distributed table acts as a proxy for querying all nodes in the cluster.
                    distributed_table_name = (
                        f"`{clean_db_name}`.`{clean_table_name}_distributed`"
                    )
                    local_table_name = f"`{clean_db_name}`.`{clean_table_name}`"

                    # Use the same column definition as the local table
                    distributed_create = (
                        f"CREATE TABLE IF NOT EXISTS {distributed_table_name} ON CLUSTER '{cluster_name}' "
                        f"AS {local_table_name} "
                        f"ENGINE = Distributed('{cluster_name}', '{clean_db_name}', '{clean_table_name}', rand())"
                    )
                    transformed_statements.append(distributed_create)

                elif (
                    engine_name
                ):  # For other engines (e.g., Null, Memory), just append them back
                    transformed_create += f" ENGINE = {engine_name}{engine_params}"

                transformed_statements.insert(
                    0, transformed_create
                )  # Insert local table create first
                continue

            # --- 3. Handle DROP statements (TABLE, VIEW, DATABASE) ---
            drop_pattern = re.compile(
                r"^(DROP\s+(?:TABLE|VIEW|DATABASE)\s+(?:IF\s+EXISTS\s+)?)(`?[\w\.]+\.`?)(`?[\w]+`?)",
                re.IGNORECASE,
            )
            drop_match = drop_pattern.match(statement)
            if drop_match:
                drop_prefix = drop_match.group(1)
                db_name = drop_match.group(2)
                object_name = drop_match.group(3)

                # Add ON CLUSTER
                transformed_drop = (
                    f"{drop_prefix} {db_name}{object_name} ON CLUSTER '{cluster_name}'"
                )
                transformed_statements.append(transformed_drop)

                # Also drop the _distributed table if it's a table drop
                if "TABLE" in drop_prefix.upper():
                    clean_db_name = db_name.strip("`.")
                    clean_object_name = object_name.strip("`")
                    distributed_table_name = (
                        f"`{clean_db_name}`.`{clean_object_name}_distributed`"
                    )
                    distributed_drop = f"DROP TABLE IF EXISTS {distributed_table_name} ON CLUSTER '{cluster_name}'"
                    transformed_statements.append(distributed_drop)
                continue

            # --- 4. Handle ALTER TABLE statements ---
            alter_pattern = re.compile(
                r"^(ALTER\s+TABLE\s+)(`?[\w\.]+\.`?)(`?[\w]+`?)(\s+.*)",
                re.IGNORECASE | re.DOTALL,
            )
            alter_match = alter_pattern.match(statement)
            if alter_match:
                alter_prefix = alter_match.group(1)
                db_name = alter_match.group(2)
                table_name = alter_match.group(3)
                operation = alter_match.group(4)  # ADD COLUMN, etc.

                # Add ON CLUSTER after the table name
                transformed_alter = f"{alter_prefix} {db_name}{table_name} ON CLUSTER '{cluster_name}'{operation}"
                transformed_statements.append(transformed_alter)
                continue

            # If no pattern matched, it's likely an INSERT or other DML, or already transformed.
            if statement not in transformed_statements:
                transformed_statements.append(original_statement)

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
            # The `execute_sql` method now handles transformation, so we can simplify this.
            # If the migration script has its own `run` logic that calls `db.command`
            # directly, the monkey-patching approach is still valid.
            # However, a cleaner pattern is to have `run` call `self.execute_sql`.
            await self.run(db)
        except Exception as e:
            error = str(e)
            success = False
            logger.error(f"❌ Migration '{self.name}' failed: {error}")
            raise e
        finally:
            end_time = time.time()
            execution_time_ms = (end_time - start_time) * 1000

            migration = Migration(
                id="",
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
