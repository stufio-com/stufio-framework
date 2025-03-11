import asyncio
from typing import Dict, Generator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from motor.core import AgnosticDatabase

from stufio.core.config import get_settings
from stufio.db.init_db import init_db
from stufio.db.mongo import MongoDatabase, _MongoClientSingleton
from stufio.tests.utils.user import authentication_token_from_email
from stufio.tests.utils.utils import get_superuser_token_headers

TEST_DATABASE = "test"
settings = get_settings()
settings.MONGO_DATABASE = TEST_DATABASE


@pytest_asyncio.fixture(scope="session")
async def db() -> Generator:
    db = MongoDatabase()
    _MongoClientSingleton.instance.mongo_client.get_io_loop = asyncio.get_event_loop
    await init_db(db)
    yield db


@pytest.fixture(scope="module")
def superuser_token_headers(client: TestClient) -> Dict[str, str]:
    return get_superuser_token_headers(client)


@pytest_asyncio.fixture(scope="module")
async def normal_user_token_headers(client: TestClient, db: AgnosticDatabase) -> Dict[str, str]:
    return await authentication_token_from_email(client=client, email=settings.EMAIL_TEST_USER, db=db)
