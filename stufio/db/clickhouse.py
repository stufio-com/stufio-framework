import asyncio
from typing import Optional
from venv import logger
from stufio.core.config import get_settings

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import ClickHouseError

from urllib.parse import urlparse

settings = get_settings()

def get_database_from_dsn(dsn: str = settings.CLICKHOUSE_DSN) -> str:
    """Extract database name from Clickhouse DSN"""
    parsed = urlparse(dsn)
    # Remove leading '/' from path to get database name
    return parsed.path.lstrip("/")


class ClickhouseConnectionError(Exception):
    """Raised when Clickhouse connection fails"""
    pass


class _ClickhouseClientSingleton:
    _instance = None
    clickhouse_client: Optional[AsyncClient] = None

    def __init__(self):
        self.clickhouse_client = None

    @classmethod
    async def get_instance(cls):
        if not cls._instance:
            try:
                logger.info(
                    f"Connecting to Clickhouse at {settings.CLICKHOUSE_DSN}"
                )
                clickhouse_client = await clickhouse_connect.get_async_client(
                    dsn=settings.CLICKHOUSE_DSN,
                    client_name=f"stufio.fastapi",
                )
                await clickhouse_client.ping()
                
                cls._instance = cls()
                cls._instance.clickhouse_client = clickhouse_client
            except ClickHouseError as e:
                cls._instance = None
                raise ClickhouseConnectionError(
                    f"Failed to connect to Clickhouse: {str(e)}"
                )
                
        return cls._instance


async def ClickhouseDatabase() -> AsyncClient:
    instance = await _ClickhouseClientSingleton.get_instance()
    if instance and instance.clickhouse_client:
        return instance.clickhouse_client
    raise ClickhouseConnectionError("Could not establish Clickhouse connection")


async def ping(retries: int = 3) -> bool:
    """Ping Clickhouse server with retries"""
    for attempt in range(retries):
        try:
            client = await ClickhouseDatabase()
            await client.ping()
            return True
        except (ClickhouseConnectionError, ClickHouseError) as e:
            await asyncio.sleep(0.1)
            if attempt == retries - 1:
                raise ClickhouseConnectionError(
                    f"Failed to ping Clickhouse after {retries} attempts: {str(e)} DSN: {settings.CLICKHOUSE_DSN}"
                )
    return False


__all__ = ["ClickhouseDatabase", "ping", "ClickhouseConnectionError"]
