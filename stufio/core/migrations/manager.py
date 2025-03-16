from typing import List, Dict, Optional, Set, cast
import importlib
import inspect
import os
import logging
import re
from motor.core import AgnosticDatabase
from clickhouse_connect.driver.asyncclient import AsyncClient

from stufio.core.migrations.base import (
    MigrationScript,
    MongoMigrationScript,
    ClickhouseMigrationScript,
)

logger = logging.getLogger(__name__)

class MigrationManager:
    """Manager for discovering and running module migrations"""

    # Regular expression to match date-based version directories (v20250308)
    VERSION_PATTERN = re.compile(r'^v(\d{8})$')

    def __init__(self):
        self.migrations: Dict[str, Dict[str, List[MigrationScript]]] = {}
        self.executed_migrations: Set[str] = set()

    def discover_app_migrations(self) -> None:
        """Discover migrations in the core app"""
        migrations_dir = os.path.dirname(os.path.abspath(__file__))
        migrations_base_path = os.path.join(migrations_dir, "migrations")

        logger.debug(f"Discovering core app migrations in {migrations_base_path}")

        if not os.path.exists(migrations_base_path) or not os.path.isdir(migrations_base_path):
            logger.debug("No migrations directory found for core app")
            return

        # Use 'stufio' as the module name for app migrations
        module_name = "stufio"

        # Initialize core app migrations dict
        if module_name not in self.migrations:
            self.migrations[module_name] = {}

        # Get all version directories
        for version_dir in os.listdir(migrations_base_path):
            if version_dir.startswith('__'):
                continue
            version_path = os.path.join(migrations_base_path, version_dir)

            # Skip non-directories and special directories
            if not os.path.isdir(version_path) or version_dir.startswith('__'):
                continue

            # Check if the directory matches our date-based version pattern
            version_match = self.VERSION_PATTERN.match(version_dir)
            if not version_match:
                logger.warning(f"Skipping directory {version_dir} - doesn't match version format v[YYYYMMDD]")
                continue

            # Extract date from directory name
            version = version_match.group(1)  # Get the date part without the 'v' prefix

            # Discover migrations for this version
            self._discover_migrations(
                migrations_path=version_path,
                module_name=module_name, 
                version=version,
                import_path_generator=lambda rel_path: f"stufio.core.migrations.migrations.{version_dir}.{os.path.splitext(os.path.basename(rel_path))[0]}"
            )

    def discover_module_migrations(self, module_path: str, module_name: str, module_version: str, module_import_path: str = None) -> None:
        """
        Discover migrations in a module.
        
        Args:
            module_path: Path to the module
            module_name: Name of the module
            module_version: Module version (ignored for date-based migrations)
            module_import_path: Optional pre-calculated import path for the module
        """
        migrations_path = os.path.join(module_path, "migrations")

        if not os.path.exists(migrations_path):
            logger.debug(f"No migrations folder, skipping: {migrations_path}")  
            return

        # If no import path provided, calculate it (for backward compatibility)
        if module_import_path is None:
            # Determine the module's import path using the same logic as ModuleRegistry
            path_parts = os.path.normpath(module_path).split(os.sep)

            if len(path_parts) >= 3:
                parent2 = path_parts[-3]
                parent1 = path_parts[-2]
                module_import_path = f"{parent2}.{parent1}.{module_name}"
            else:
                module_import_path = f"stufio.modules.{module_name}"

        logger.debug(f"Using module import path: {module_import_path} for migrations")

        # For modules, look for date-based version directories like v20250308
        try:
            for version_dir in os.listdir(migrations_path):
                version_dir_path = os.path.join(migrations_path, version_dir)

                # Skip non-directories and special directories
                if not os.path.isdir(version_dir_path) or version_dir.startswith('__'):
                    continue

                # Check if the directory matches our date-based version pattern
                version_match = self.VERSION_PATTERN.match(version_dir)
                if not version_match:
                    logger.warning(f"Skipping directory {version_dir} - doesn't match version format v[YYYYMMDD]")
                    continue

                # Extract date from directory name
                version = version_match.group(1)  # Get the date part without the 'v' prefix

                # Use the shared discovery implementation
                self._discover_migrations(
                    migrations_path=version_dir_path,
                    module_name=module_name,
                    version=version,
                    import_path_generator=lambda rel_path: f"{module_import_path}.migrations.{version_dir}.{os.path.splitext(os.path.basename(rel_path))[0]}",
                )
        except Exception as e:
            logger.error(
                f"Error discovering migrations for module {module_name}: {str(e)}"
            )

    def _discover_migrations(self, migrations_path: str, module_name: str, version: str, 
                           import_path_generator: callable) -> None:
        """
        Common implementation for discovering migrations.
        
        Args:
            migrations_path: Path to scan for migration files
            module_name: Name of the module (or 'core' for app)
            version: Version string for migrations
            import_path_generator: Function that generates import path from relative path
        """
        # Safety check - ensure directory exists
        if not os.path.exists(migrations_path) or not os.path.isdir(migrations_path):
            logger.warning(f"Migration path not found or not a directory: {migrations_path}")
            return

        # Initialize module migrations dict for this version
        if module_name not in self.migrations:
            self.migrations[module_name] = {}

        if version not in self.migrations[module_name]:
            self.migrations[module_name][version] = []

        # Get migration script files
        migration_files = []
        try:
            for file in os.listdir(migrations_path):
                if file.endswith(".py") and not file.startswith("__"):
                    migration_files.append(file)
        except FileNotFoundError:
            logger.warning(f"Migration directory not found: {migrations_path}")
            return
        except PermissionError:
            logger.warning(f"Permission denied when accessing: {migrations_path}")
            return

        logger.debug(f"Discovered {len(migration_files)} migration files for {module_name} v{version}")

        # Import and register migration scripts
        for file_name in migration_files:
            # Generate import path using the provided function
            try:
                import_path = import_path_generator(file_name)
                migration_module = importlib.import_module(import_path)

                logger.debug(f"Imported migration module: {import_path}")

                # Find all MigrationScript subclasses in the module
                for name, obj in inspect.getmembers(migration_module):
                    if (inspect.isclass(obj) and 
                        issubclass(obj, MigrationScript) and 
                        obj is not MigrationScript and
                        obj is not MongoMigrationScript and
                        obj is not ClickhouseMigrationScript):

                        migration_script = obj()
                        migration_script.version = version

                        # Add migration to the registry
                        self.migrations[module_name][version].append(migration_script)
                        logger.debug(f"Registered migration {migration_script.name} ({migration_script.database_type}) for {module_name} v{version}")

            except ImportError as e:
                logger.error(f"Error importing migration {file_name}: {str(e)}")
            except Exception as e:
                logger.error(f"Error processing migration {file_name}: {str(e)}")

    async def get_executed_migrations(self, db: AgnosticDatabase) -> None:
        """Load already executed migrations from database"""
        self.executed_migrations = set()
        self.failed_migrations = set()

        # Find all migrations
        async for migration in db.migrations.find({}, {"module": 1, "version": 1, "name": 1, "success": 1}):
            migration_key = f"{migration['module']}:{migration['version']}:{migration['name']}"

            # Track successful migrations separately from failed ones
            if migration.get("success", True):
                self.executed_migrations.add(migration_key)
            else:
                self.failed_migrations.add(migration_key)

        logger.debug(f"Loaded {len(self.executed_migrations)} previously executed migrations")

    async def run_pending_migrations(self, mongodb: AgnosticDatabase, clickhouse: Optional[AsyncClient] = None) -> int:
        """Run pending migrations for core app and all modules"""

        # Load already executed migrations
        await self.get_executed_migrations(mongodb)

        # Track how many migrations we run
        executed_count = 0

        # Process each module's migrations including core app migrations
        for module_name, versions in self.migrations.items():
            # Sort versions by date (the date-based version strings will sort correctly)
            sorted_versions = sorted(versions.keys())

            for version in sorted_versions:
                migrations = versions[version]
                # Sort migrations by order
                migrations.sort(key=lambda m: m.order)

                for migration in migrations:
                    migration_key = f"{module_name}:{version}:{migration.name}"

                    # Skip if already executed
                    if migration_key in self.executed_migrations:
                        logger.debug(f"Skipping already executed migration: {migration_key}")
                        continue

                    # Check if this is a retry of a failed migration
                    is_retry = migration_key in self.failed_migrations

                    # Determine DB type and run migration
                    logger.info(f"Running migration {migration_key}")

                    try:
                        if isinstance(migration, ClickhouseMigrationScript):
                            # ClickHouse migration
                            if not clickhouse:
                                logger.error(f"Cannot run ClickHouse migration {migration_key}: ClickHouse client not provided")
                                continue

                            # Cast to ensure proper type checking
                            ch_migration = cast(ClickhouseMigrationScript, migration)
                            migration_record = await ch_migration.execute(clickhouse, module_name, version)
                            executed_count += 1
                        elif isinstance(migration, MongoMigrationScript):
                            # MongoDB migration
                            mongo_migration = cast(MongoMigrationScript, migration)
                            migration_record = await mongo_migration.execute(mongodb, module_name, version)
                            executed_count += 1
                        else:
                            # Fallback - assume MongoDB for backward compatibility
                            logger.warning(f"Migration {migration_key} does not use specific interface - assuming MongoDB")
                            migration_record = await migration.execute(mongodb, module_name, version)
                            executed_count += 1

                        if is_retry:
                            logger.info(f"Successfully retried migration {migration_key}")
                            # Update migration record
                            await mongodb.migrations.update_one(
                                {"module": module_name, "version": version, "name": migration.name},
                                {"$set": migration_record.dict()}
                            )
                        else:
                            if migration_record.success:
                                logger.info(f"Successfully executed migration {migration_key}")
                            else:
                                logger.error(f"Failed to execute migration {migration_key}: {migration_record.error}")
                            # Save migration record
                            await mongodb.migrations.insert_one(migration_record.dict())

                        # Add to executed migrations
                        self.executed_migrations.add(migration_key)

                    except Exception as e:
                        logger.error(f"Failed to execute migration {migration_key}: {e}")
                        # Stop migrations on error
                        raise

        return executed_count

# Create singleton instance
migration_manager = MigrationManager()
