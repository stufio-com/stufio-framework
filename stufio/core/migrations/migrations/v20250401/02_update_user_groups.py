from motor.core import AgnosticDatabase
from stufio.core.migrations.base import MongoMigrationScript

class UpdateUserWithGroups(MongoMigrationScript):
    name = "update_user_with_groups"
    description = "Add user_groups field to user collection and create an index"
    migration_type = "schema"
    order = 20  # Run after user_group collection
    
    async def run(self, db: AgnosticDatabase) -> None:
        user_collection = db["users"]
        
        # Add index for user groups
        await user_collection.create_index(
            [("user_groups", 1)],
            name="user_user_groups",
            background=True
        )
        
        # Update existing users to have an empty user_groups array if it doesn't exist
        await user_collection.update_many(
            {"user_groups": {"$exists": False}},
            {"$set": {"user_groups": []}}
        )