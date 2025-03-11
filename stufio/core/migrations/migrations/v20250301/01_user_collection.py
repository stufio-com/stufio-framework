from motor.core import AgnosticDatabase
from stufio.core.migrations.base import MongoMigrationScript

class CreateUserCollection(MongoMigrationScript):
    name = "create_user_collection"
    description = "Setup user collection and indexes"
    migration_type = "schema"
    order = 10
    
    async def run(self, db: AgnosticDatabase) -> None:
        # Make sure collection exists
        if "user" not in await db.list_collection_names():
            await db.create_collection("user")
        
        user_collection = db["user"]
        
        # Unique email index
        await user_collection.create_index(
            [("email", 1)],
            unique=True,
            name="user_email_unique",
            background=True
        )
        
        # Index for active users
        await user_collection.create_index(
            [("is_active", 1)],
            name="user_is_active",
            background=True
        )
            
        # Compound index for email validation status and email tokens count
        await user_collection.create_index(
            [("email_validated", 1), ("email_tokens_cnt", 1)],
            name="user_email_validation",
            background=True
        )
        
        # Add any other user-related indexes needed
        await user_collection.create_index(
            [("is_superuser", 1)],
            name="user_is_superuser",
            background=True
        )