import traceback
from typing import Optional
from clickhouse_connect.driver.asyncclient import AsyncClient
from motor.core import AgnosticDatabase
import logging

from stufio import crud, schemas
from stufio.core.migrations.manager import migration_manager
from stufio.core.module_registry import registry

from stufio.core.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


async def init_db(
    db: AgnosticDatabase, clickhouse: Optional[AsyncClient] = None
) -> None:
    """Initialize the database with required data and schema."""
    try:
        logger.info("Initializing database: Running migrations")
        await run_migrations(db, clickhouse)

        # Create superuser if it doesn't exist
        user = await crud.user.get_by_email(db, email=settings.FIRST_SUPERUSER)
        if not user:
            # Create user auth
            user_in = schemas.UserCreate(
                email=settings.FIRST_SUPERUSER,
                password=settings.FIRST_SUPERUSER_PASSWORD,
                is_superuser=True,
                full_name=settings.FIRST_SUPERUSER,
            )
            user = await crud.user.create(db, obj_in=user_in)
            logger.info(f"Superuser created: {settings.FIRST_SUPERUSER}")

    except Exception as e:
        logger.error(f"Database initialization error: {str(e)}")
        traceback.print_exc()
        raise


async def run_migrations(
    mongodb: AgnosticDatabase, clickhouse: Optional[AsyncClient] = None
) -> None:
    """Run all pending migrations across core app and all modules"""
    try:
        # Ensure migrations collection exists
        collection_names = await mongodb.list_collection_names()
        if "migrations" not in collection_names:
            await mongodb.create_collection("migrations")

            # Create indexes
            await mongodb.migrations.create_index(
                [("module", 1), ("version", 1), ("name", 1)], unique=True
            )
            await mongodb.migrations.create_index([("module", 1), ("success", 1)])

        # Discover core app migrations first
        migration_manager.discover_app_migrations()
        
        # First, we need to discover all modules before creating the app
        for module_name in registry.discover_modules():
            registry.load_module(module_name, discover_migrations=True)

        # Run all pending migrations
        count = await migration_manager.run_pending_migrations(mongodb, clickhouse)
        if count > 0:
            logger.info(f"Successfully executed {count} pending migrations")

    except Exception as e:
        logger.error(f"Error running migrations: {e}")
        raise
