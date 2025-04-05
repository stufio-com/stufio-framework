from stufio.crud.crud_user import user
from stufio.crud.crud_token import token


# For a new basic set of CRUD operations you could just do

# from .base import CRUDMongo
# from app.models.item import Item
# from app.schemas.item import ItemCreate, ItemUpdate

# item = CRUDMongo[Item, ItemCreate, ItemUpdate](Item)

__all__ = ["user", "token"]
