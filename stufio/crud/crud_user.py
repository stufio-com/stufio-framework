from typing import Any, Dict, Optional, Union
from stufio.crud.mongo_base import CRUDMongo
from stufio.core.security import get_password_hash, verify_password
from stufio.models.user import User
from stufio.schemas import UserCreate, UserInDB, UserUpdate, NewTOTP

class CRUDUser(CRUDMongo[User, UserCreate, UserUpdate]):
    async def get_by_email(self, email: str) -> Optional[User]:
        return await self.get_by_field("email", email)
    
    async def create(self, obj_in: UserCreate) -> User:
        user = {
            **obj_in.model_dump(),
            "email": obj_in.email,
            "hashed_password": get_password_hash(obj_in.password) if obj_in.password is not None else None,
            "full_name": obj_in.full_name,
            "is_superuser": obj_in.is_superuser,
        }

        return await super().create(User(**user))

    async def update(self, db_obj: User, obj_in: Union[UserUpdate, Dict[str, Any]]) -> User:
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)
        if update_data.get("password"):
            hashed_password = get_password_hash(update_data["password"])
            del update_data["password"]
            update_data["hashed_password"] = hashed_password
        if update_data.get("email") and db_obj.email != update_data["email"]:
            update_data["email_validated"] = False
        return await super().update(db_obj=db_obj, obj_in=update_data)

    async def authenticate(self, email: str, password: str) -> Optional[User]:
        user = await self.get_by_email(email=email)
        if not user:
            return None
        if not verify_password(plain_password=password, hashed_password=user.hashed_password):
            return None
        return user

    async def validate_email(self, db_obj: User) -> User:
        obj_in = UserUpdate(**UserInDB.model_validate(db_obj).model_dump())
        obj_in.email_validated = True
        return await self.update(db_obj=db_obj, obj_in=obj_in)

    async def activate_totp(self, db_obj: User, totp_in: NewTOTP) -> User:
        obj_in = UserUpdate(**UserInDB.model_validate(db_obj).model_dump())
        obj_in = obj_in.model_dump(exclude_unset=True)
        obj_in["totp_secret"] = totp_in.secret
        return await self.update(db_obj=db_obj, obj_in=obj_in)

    async def deactivate_totp(self, db_obj: User) -> User:
        obj_in = UserUpdate(**UserInDB.model_validate(db_obj).model_dump())
        obj_in = obj_in.model_dump(exclude_unset=True)
        obj_in["totp_secret"] = None
        obj_in["totp_counter"] = None
        return await self.update(db_obj=db_obj, obj_in=obj_in)

    async def update_totp_counter(self, db_obj: User, new_counter: int) -> User:
        obj_in = UserUpdate(**UserInDB.model_validate(db_obj).model_dump())
        obj_in = obj_in.model_dump(exclude_unset=True)
        obj_in["totp_counter"] = new_counter
        return await self.update(db_obj=db_obj, obj_in=obj_in)

    async def increment_email_verification_counter(
        self, db_obj: User, inc_counter: int = 1
    ) -> User:
        obj_in = UserUpdate(**UserInDB.model_validate(db_obj).model_dump())
        obj_in = obj_in.model_dump(exclude_unset=True)
        obj_in["email_tokens_cnt"] = obj_in.get("email_tokens_cnt", 0) + inc_counter
        return await self.update(db_obj=db_obj, obj_in=obj_in)

    async def toggle_user_state(self, obj_in: Union[UserUpdate, Dict[str, Any]]) -> User:
        db_obj = await self.get_by_email(email=obj_in.email)
        if not db_obj:
            return None
        return await self.update(db_obj=db_obj, obj_in=obj_in)

    def has_password(self, user: User) -> bool:
        if user.hashed_password:
            return True
        return False

    def is_active(self, user: User) -> bool:
        return user.is_active

    def is_superuser(self, user: User) -> bool:
        return user.is_superuser

    def is_email_validated(self, user: User) -> bool:
        return user.email_validated


user = CRUDUser(User)
