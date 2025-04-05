from typing import AsyncGenerator, Generator
from stufio.db.clickhouse import ClickhouseDatabase
from stufio.db.mongo import MongoDatabase
from stufio.db.mongo import get_engine
from odmantic import AIOEngine

async def get_clickhouse() -> AsyncGenerator:
    try:
        clickhouse = await ClickhouseDatabase()
        yield clickhouse
    finally:
        pass


def get_mongo_engine() -> Generator:
    try:
        engine = get_engine()
        yield engine
    finally:
        pass


def get_db() -> Generator:
    try:
        db = MongoDatabase()
        yield db
    finally:
        pass
