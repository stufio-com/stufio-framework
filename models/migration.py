from __future__ import annotations
from datetime import datetime
from typing import Optional, Dict, Any, Literal
from odmantic import Field, Index
from pydantic import ConfigDict

from stufio.db.mongo_base import MongoBase, datetime_now_sec

class Migration(MongoBase):
    """Migration record to track which migrations have been executed"""
    
    # Identifiers
    module: str = Field(index=True)  # Module name
    version: str = Field(index=True)  # Module version that added this migration
    name: str  # Migration name
    
    # Migration details
    type: Literal["mongodb", "clickhouse"] = Field(index=True)  # Database type
    migration_type: Literal["init", "schema", "data"] = Field(default="schema")  # Migration category
    order: int = Field(default=100)  # Execution order within version
    
    # Execution details
    executed_at: datetime = Field(default_factory=datetime_now_sec)
    execution_time_ms: float = Field(default=0)  # How long it took to run
    success: bool = Field(default=True)  # Was execution successful
    error: Optional[str] = None  # Error message if failed
    
    # Migration metadata
    description: Optional[str] = None  # Description of what this migration does
    checksum: Optional[str] = None  # Hash of migration content to detect changes
    metadata: Dict[str, Any] = Field(default_factory=dict)  # Additional metadata
    
    # ODMantic requires a different pattern for model configuration
    # Use model_config instead of nested Config class
    model_config = ConfigDict(
        collection="migrations",
        indexes=[
            Index(
                "module", 
                "version", 
                "name", 
                unique=True
            )
        ]
    )