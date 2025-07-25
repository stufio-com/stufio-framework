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

    async def update(self, db_obj: User, update_data: Union[UserUpdate, Dict[str, Any]]) -> User:
        if isinstance(update_data, dict):
            update_dict = update_data
        else:
            update_dict = update_data.model_dump(exclude_unset=True)
        if update_dict.get("password"):
            hashed_password = get_password_hash(update_dict["password"])
            del update_dict["password"]
            update_dict["hashed_password"] = hashed_password
        if update_dict.get("email") and db_obj.email != update_dict["email"]:
            update_dict["email_validated"] = False
        return await super().update(db_obj=db_obj, update_data=update_dict)

    async def authenticate(self, email: str, password: str) -> Optional[User]:
        user = await self.get_by_email(email=email)
        if not user:
            return None
        if not verify_password(plain_password=password, hashed_password=user.hashed_password):
            return None
        return user

    async def validate_email(self, db_obj: User) -> User:
        update_data = UserUpdate(**UserInDB.model_validate(db_obj).model_dump())
        update_data.email_validated = True
        return await self.update(db_obj=db_obj, update_data=update_data)

    async def activate_totp(self, db_obj: User, totp_in: NewTOTP) -> User:
        update_obj = UserUpdate(**UserInDB.model_validate(db_obj).model_dump())
        update_data = update_obj.model_dump(exclude_unset=True)
        update_data["totp_secret"] = totp_in.secret
        return await self.update(db_obj=db_obj, update_data=update_data)

    async def deactivate_totp(self, db_obj: User) -> User:
        update_obj = UserUpdate(**UserInDB.model_validate(db_obj).model_dump())
        update_data = update_obj.model_dump(exclude_unset=True)
        update_data["totp_secret"] = None
        update_data["totp_counter"] = None
        return await self.update(db_obj=db_obj, update_data=update_data)

    async def update_totp_counter(self, db_obj: User, new_counter: int) -> User:
        update_obj = UserUpdate(**UserInDB.model_validate(db_obj).model_dump())
        update_data = update_obj.model_dump(exclude_unset=True)
        update_data["totp_counter"] = new_counter
        return await self.update(db_obj=db_obj, update_data=update_data)

    async def increment_email_verification_counter(
        self, db_obj: User, inc_counter: int = 1
    ) -> User:
        update_obj = UserUpdate(**UserInDB.model_validate(db_obj).model_dump())
        update_data = update_obj.model_dump(exclude_unset=True)
        update_data["email_tokens_cnt"] = update_data.get("email_tokens_cnt", 0) + inc_counter
        return await self.update(db_obj=db_obj, update_data=update_data)

    async def toggle_user_state(
        self, db_obj: User, is_active: bool | None = None
    ) -> User:
        update_obj = UserUpdate(**UserInDB.model_validate(db_obj).model_dump())
        if is_active is None:
            is_active = not update_obj.is_active
        update_data = update_obj.model_dump(exclude_unset=True)
        update_data["is_active"] = is_active
        return await self.update(db_obj=db_obj, update_data=update_data)

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
