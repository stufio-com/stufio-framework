"""
Pydantic schema models for database metrics.
These models represent the structure of various database performance metrics
collected through the application.
"""
from typing import Dict, List, Tuple, Any, Optional
from pydantic import BaseModel, Field


class QueryTypeStats(BaseModel):
    """Statistics about a specific query type."""
    query_type: str = Field(..., description="Type of query or command")
    count: int = Field(..., description="Number of queries of this type")


class ClickhouseMetrics(BaseModel):
    """Metrics for ClickHouse database operations."""
    total_queries: int = Field(..., description="Total number of queries executed")
    avg_execution_time_ms: float = Field(..., description="Average execution time in milliseconds")
    max_execution_time_ms: float = Field(..., description="Maximum execution time in milliseconds")
    error_rate: float = Field(..., description="Rate of errors (0.0 to 1.0)")
    queries_per_minute: int = Field(..., description="Average queries per minute")
    slow_query_percentage: float = Field(..., description="Percentage of slow queries")
    top_query_types: List[Tuple[str, int]] = Field(
        ..., description="Top query types by frequency", 
        example=[("SELECT", 100), ("INSERT", 50)]
    )


class MongoDBMetrics(BaseModel):
    """Metrics for MongoDB database operations."""
    total_queries: int = Field(..., description="Total number of queries executed")
    avg_execution_time_ms: float = Field(..., description="Average execution time in milliseconds")
    max_execution_time_ms: float = Field(..., description="Maximum execution time in milliseconds")
    error_rate: float = Field(..., description="Rate of errors (0.0 to 1.0)")
    queries_per_minute: int = Field(..., description="Average queries per minute")
    slow_query_percentage: float = Field(..., description="Percentage of slow queries")
    top_collections: List[Tuple[str, int]] = Field(
        ..., description="Top collections by query frequency",
        example=[("users", 100), ("logs", 50)]
    )


class RedisMetrics(BaseModel):
    """Metrics for Redis database operations."""
    total_operations: int = Field(..., description="Total number of operations executed")
    avg_execution_time_ms: float = Field(..., description="Average execution time in milliseconds")
    max_execution_time_ms: float = Field(..., description="Maximum execution time in milliseconds")
    error_rate: float = Field(..., description="Rate of errors (0.0 to 1.0)")
    operations_per_minute: int = Field(..., description="Average operations per minute")
    slow_operation_percentage: float = Field(..., description="Percentage of slow operations")
    top_commands: List[Tuple[str, int]] = Field(
        ..., description="Top commands by frequency",
        example=[("GET", 100), ("SET", 50)]
    )


class DatabaseMetricsSummary(BaseModel):
    """Summary of metrics for all database systems."""
    clickhouse: ClickhouseMetrics = Field(..., description="ClickHouse database metrics")
    mongo: MongoDBMetrics = Field(..., description="MongoDB database metrics")
    redis: RedisMetrics = Field(..., description="Redis database metrics")