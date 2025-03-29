from datetime import timedelta
from motor.core import AgnosticDatabase
from stufio.core.migrations.base import MongoMigrationScript


class CreateSettingsCollections(MongoMigrationScript):
    name = "create_settings_collections"
    description = "Setup settings collections and indexes"
    migration_type = "schema"
    order = 30
    
    async def run(self, db: AgnosticDatabase) -> None:
        # Create settings collection if it doesn't exist
        if "settings" not in await db.list_collection_names():
            await db.create_collection("settings")
        
        settings_collection = db["settings"]
        
        # Create index for faster setting lookups
        await settings_collection.create_index(
            [("key", 1), ("module", 1)], 
            unique=True, 
            name="settings_key_module_index", 
            background=True
        )
        
        # Create settings_history collection if it doesn't exist
        if "settings_history" not in await db.list_collection_names():
            await db.create_collection("settings_history", 
                timeseries={
                    "timeField": "created_at",
                    "metaField": "setting_id",
                    "granularity": "minutes"
                }
            )
        
        settings_history_collection = db["settings_history"]
        
        # Create index for faster history lookups
        await settings_history_collection.create_index(
            [("setting_id", 1)], 
            name="settings_history_setting_id_index", 
            background=True
        )
        
        # Create TTL index to automatically delete history older than 6 months
        # Add partialFilterExpression to satisfy time-series collection requirement
        await settings_history_collection.create_index(
            [("created_at", 1)],
            expireAfterSeconds=int(timedelta(days=180).total_seconds()),
            name="settings_history_ttl_index",
            background=True,
            partialFilterExpression={"setting_id": {"$exists": True}}
        )