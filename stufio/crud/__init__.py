from .crud_user import user
from .crud_token import token


# For a new basic set of CRUD operations you could just do

# from .base import CRUDMongoBase
# from app.models.item import Item
# from app.schemas.item import ItemCreate, ItemUpdate

# item = CRUDMongoBase[Item, ItemCreate, ItemUpdate](Item)
