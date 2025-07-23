from typing import Dict

from fastapi.testclient import TestClient
from motor.core import AgnosticDatabase

from stufio import crud
from stufio.core.config import get_settings
from stufio.models.user import User
from stufio.schemas.user import UserCreate, UserUpdate
from stufio.tests.utils.utils import random_email, random_lower_string

settings = get_settings()

def user_authentication_headers(*, client: TestClient, email: str, password: str) -> Dict[str, str]:
    data = {"username": email, "password": password}

    r = client.post(f"{settings.API_V1_STR}/login/oauth", data=data)
    response = r.json()
    auth_token = response["access_token"]
    headers = {"Authorization": f"Bearer {auth_token}"}
    return headers


async def authentication_token_from_email(*, client: TestClient, email: str) -> Dict[str, str]:
    """
    Return a valid token for the user with given email.

    If the user doesn't exist it is created first.
    """
    password = random_lower_string()
    user = await crud.user.get_by_email(email=email)
    if not user:
        user_in_create = UserCreate(username=email, email=email, password=password)
        user = await crud.user.create(obj_in=user_in_create)
    else:
        user_in_update = UserUpdate(password=password)
        user = await crud.user.update(db_obj=user, update_data=user_in_update)

    return user_authentication_headers(client=client, email=email, password=password)
