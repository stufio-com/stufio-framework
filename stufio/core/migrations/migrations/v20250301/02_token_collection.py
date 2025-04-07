from motor.core import AgnosticDatabase
from stufio.core.migrations.base import MongoMigrationScript

class CreateTokenCollection(MongoMigrationScript):
    name = "create_token_collection"
    description = "Setup token collection and indexes"
    migration_type = "schema"
    order = 20

    async def run(self, db: AgnosticDatabase) -> None:
        # Make sure collection exists
        if "tokens" not in await db.list_collection_names():
            await db.create_collection("tokens")

        token_collection = db["tokens"]

        # Create TTL index that will automatically delete expired tokens
        await token_collection.create_index(
            [("expires", 1)],
            expireAfterSeconds=0,
            name="token_ttl_expire_index",
            background=True,
        )

        # Create index for faster token lookups
        await token_collection.create_index(
            [("token", 1)], 
            unique=True, 
            name="token_value_index", 
            background=True
        )

        # Index for faster lookup of tokens by user
        await token_collection.create_index(
            [("authenticates_id", 1)], 
            name="token_auth_id_index", 
            background=True
        )

        # Optional: Index for token types if you have different token types
        await token_collection.create_index(
            [("token_type", 1)], 
            name="token_type_index", 
            background=True
        )
