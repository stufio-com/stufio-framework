from typing import Any, Dict, Optional, Union, List
from odmantic import ObjectId
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

    # Add new methods for user groups
    async def add_to_group(self, user: User, group_id: ObjectId) -> User:
        """Add a user to a group"""
        if group_id not in user.user_groups:
            user.user_groups.append(group_id)
            return await self.engine.save(user)
        return user
    
    async def remove_from_group(self, user: User, group_id: ObjectId) -> User:
        """Remove a user from a group"""
        if group_id in user.user_groups:
            user.user_groups.remove(group_id)
            return await self.engine.save(user)
        return user
    
    async def set_user_groups(self, user: User, group_ids: List[ObjectId]) -> User:
        """Set the user's groups (replacing existing ones)"""
        user.user_groups = group_ids
        return await self.engine.save(user)
    
    async def get_users_by_group(self, group_id: ObjectId) -> List[User]:
        """Get all users in a specific group"""
        return await self.get_multi(filters={"user_groups": group_id})
    
    async def get_user_groups(self, user: User) -> List[ObjectId]:
        """Get all groups a user belongs to"""
        return user.user_groups
    
    def is_in_group(self, user: User, group_id: ObjectId) -> bool:
        """Check if a user is in a specific group"""
        return group_id in user.user_groups
    
    def has_any_group(self, user: User, group_ids: List[ObjectId]) -> bool:
        """Check if a user is in any of the specified groups"""
        return any(group_id in user.user_groups for group_id in group_ids)
    
    def has_all_groups(self, user: User, group_ids: List[ObjectId]) -> bool:
        """Check if a user is in all of the specified groups"""
        return all(group_id in user.user_groups for group_id in group_ids)

user = CRUDUser(User)
