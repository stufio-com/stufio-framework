from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, Literal
from datetime import datetime


class MigrationBase(BaseModel):
    module: str
    version: str
    name: str
    type: Literal["mongodb", "clickhouse"]
    migration_type: Literal["init", "schema", "data"] = "schema"
    order: int = 100
    description: Optional[str] = None

class MigrationCreate(MigrationBase):
    execution_time_ms: float
    success: bool
    error: Optional[str] = None
    checksum: Optional[str] = None
    metadata: Dict[str, Any] = {}

class MigrationRead(MigrationBase):
    id: str
    executed_at: datetime
    execution_time_ms: float
    success: bool
    error: Optional[str] = None
    checksum: Optional[str] = None
    metadata: Dict[str, Any] = {}

    class Config:
        orm_mode = True

class MigrationFilter(BaseModel):
    module: Optional[str] = None
    version: Optional[str] = None
    type: Optional[Literal["mongodb", "clickhouse"]] = None
    success: Optional[bool] = None