from datetime import datetime
from typing import Any, Dict, List
from pydantic import BaseModel, ConfigDict
from stufio.db.clickhouse import get_database_from_dsn


def datetime_now_sec() -> datetime:
    """Return current datetime without microseconds for Clickhouse compatibility"""
    return datetime.now().replace(microsecond=0)

class ClickhouseBase(BaseModel):
    """Base class for Clickhouse models"""

    model_config = ConfigDict(from_attributes=True)

    @classmethod
    def get_database_name(cls) -> str:
        """Get database name from DSN or config"""
        return get_database_from_dsn()

    @classmethod
    def get_table_short_name(cls) -> str:
        """Get the table name without database prefix"""
        full_name = cls.__name__.lower()
        
        if isinstance(cls.model_config, dict):
            # Extract table name from config
            full_name = cls.model_config.get("table_name", full_name)
        elif cls.model_config and hasattr(cls.model_config, "table_name"):
            # Extract table name from full name (after last dot)
            full_name = cls.model_config.table_name
        if "." in full_name:
            return full_name.split(".")[-1]

        return full_name

    @classmethod
    def get_table_name(cls) -> str:
        """Get fully qualified Clickhouse table name (with database)"""
        if (
            hasattr(cls.model_config, "table_name")
            and "." in cls.model_config.table_name
        ):
            return cls.model_config.table_name

        return f"{cls.get_database_name()}.{cls.get_table_short_name()}"

    @classmethod
    def get_primary_fields(cls) -> List[str]:
        """Get list of primary key fields"""
        return [
            field_name
            for field_name, field in cls.model_fields.items()
            if field.json_schema_extra and field.json_schema_extra.get("primary_field")
        ]

    def dict_for_insert(self) -> Dict[str, Any]:
        """Convert model to dict format suitable for Clickhouse insert"""
        return self.model_dump(exclude_unset=True)
        # return data, list(data.values()), list(data.keys())
