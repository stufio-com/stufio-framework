from __future__ import annotations
from motor.core import AgnosticDatabase

from stufio.crud.mongo_base import CRUDMongo
from stufio.models.user import User
from stufio.models.token import Token
from stufio.schemas import RefreshTokenCreate, RefreshTokenUpdate


class CRUDToken(CRUDMongo[Token, RefreshTokenCreate, RefreshTokenUpdate]):
    # Everything is user-dependent
    async def create(self, obj_in: str, user_obj: User) -> Token:
        db_obj = await self.engine.find_one(self.model, self.model.token == obj_in)
        if db_obj:
            if db_obj.authenticates_id != user_obj:
                raise ValueError("Token mismatch between key and user.")
            return db_obj
        else:
            new_token = self.model(token=obj_in, authenticates_id=user_obj)
            await self.engine.save_all([new_token])
            return new_token

    async def get_by_user(self, user: User, token: str) -> Token | None:
        """
        Get a token document by its string value, ensuring it belongs to the specified user
        """
        db_obj = await self.engine.find_one(self.model, (self.model.token == token))

        if db_obj and db_obj.authenticates_id == user:
            return db_obj

        return None

    async def remove(self, db_obj: Token) -> None:
        await self.engine.delete(db_obj)


token = CRUDToken(Token)
