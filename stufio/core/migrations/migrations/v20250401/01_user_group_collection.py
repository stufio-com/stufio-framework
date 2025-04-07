from motor.core import AgnosticDatabase
from stufio.core.migrations.base import MongoMigrationScript

class CreateUserGroupCollection(MongoMigrationScript):
    name = "create_user_group_collection"
    description = "Setup user group collection and indexes"
    migration_type = "schema"
    order = 15  # Run after user collection

    async def run(self, db: AgnosticDatabase) -> None:
        # Create collection if it doesn't exist
        if "user_groups" not in await db.list_collection_names():
            await db.create_collection("user_groups")

        user_group_collection = db["user_groups"]

        # Unique name index
        await user_group_collection.create_index(
            [("name", 1)],
            unique=True,
            name="user_group_name_unique",
            background=True
        )

        # Index for active groups
        await user_group_collection.create_index(
            [("is_active", 1)],
            name="user_group_is_active",
            background=True
        )

        # Index for permissions to quickly find groups with specific permissions
        await user_group_collection.create_index(
            [("permissions", 1)],
            name="user_group_permissions",
            background=True
        )
