from typing import List, Optional
from odmantic import ObjectId
from stufio.crud.mongo_base import CRUDMongo
from stufio.models.user_group import UserGroup
from stufio.schemas.user_group import UserGroupCreate, UserGroupUpdate

class CRUDUserGroup(CRUDMongo[UserGroup, UserGroupCreate, UserGroupUpdate]):
    async def get_by_name(self, name: str) -> Optional[UserGroup]:
        return await self.get_by_field("name", name)
    
    async def get_active_groups(self) -> List[UserGroup]:
        return await self.get_multi(filters={"is_active": True})

    async def get_groups_by_permission(self, permission: str) -> List[UserGroup]:
        query_filter = {"permissions": {"$in": [permission]}}
        groups = await self.execute_query(
            lambda collection: collection.find(query_filter).to_list(None)
        )
        return [UserGroup.parse_obj(g) for g in groups]
    
    async def get_by_ids(self, group_ids: List[ObjectId]) -> List[UserGroup]:
        query_filter = {"_id": {"$in": group_ids}}
        groups = await self.execute_query(
            lambda collection: collection.find(query_filter).to_list(None)
        )
        return [UserGroup.parse_obj(g) for g in groups]

user_group = CRUDUserGroup(UserGroup)