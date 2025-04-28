"""
Database metrics collector for monitoring MongoDB, ClickHouse, and Redis query performance.

This module provides decorators and utilities to track database operations
and measure performance across different database systems.
"""
import functools
import time
import logging
import asyncio
import json
from typing import Dict, Any, Optional, Callable, TypeVar, Awaitable, List, Union, Type, ClassVar
from contextlib import asynccontextmanager
import inspect
import contextvars
from abc import ABC, abstractmethod
from datetime import datetime
from stufio.core.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

# Type definitions
F = TypeVar('F', bound=Callable[..., Awaitable[Any]])

# Registry for metrics providers
_METRICS_PROVIDERS = {}

# Context storage for request-level metrics isolation
class MetricsContextStorage:
    """Thread/task-local storage for metrics isolation between requests."""
    
    def __init__(self):
        # Create contextvars for each standard DB provider
        self.mongodb_context = contextvars.ContextVar('mongodb_metrics', default={
            "queries": 0,
            "time_ms": 0,
            "slow_queries": 0,
            "operation_types": {},
            "collection_stats": {},
            "timestamp": time.time()
        })
        self.clickhouse_context = contextvars.ContextVar('clickhouse_metrics', default={
            "queries": 0,
            "time_ms": 0,
            "slow_queries": 0,
            "query_types": {},
            "timestamp": time.time()
        })
        self.redis_context = contextvars.ContextVar('redis_metrics', default={
            "operations": 0,
            "time_ms": 0,
            "slow_operations": 0,
            "command_types": {},
            "timestamp": time.time()
        })
    
    def get_mongodb_metrics(self) -> Dict:
        """Get thread-local MongoDB metrics."""
        return self.mongodb_context.get()
    
    def set_mongodb_metrics(self, metrics: Dict) -> None:
        """Set thread-local MongoDB metrics."""
        self.mongodb_context.set(metrics)
    
    def get_clickhouse_metrics(self) -> Dict:
        """Get thread-local ClickHouse metrics."""
        return self.clickhouse_context.get()
    
    def set_clickhouse_metrics(self, metrics: Dict) -> None:
        """Set thread-local ClickHouse metrics."""
        self.clickhouse_context.set(metrics)
    
    def get_redis_metrics(self) -> Dict:
        """Get thread-local Redis metrics."""
        return self.redis_context.get()
    
    def set_redis_metrics(self, metrics: Dict) -> None:
        """Set thread-local Redis metrics."""
        self.redis_context.set(metrics)


# Create global context storage
metrics_context = MetricsContextStorage()

# Global metrics for total stats (not request-specific)
global_metrics: Dict[str, Dict[str, Any]] = {
    "clickhouse": {
        "queries": 0,
        "execution_time_ms": 0,
        "errors": 0,
        "avg_execution_time_ms": 0,
        "max_execution_time_ms": 0,
        "last_minute_queries": 0,
        "slow_queries": 0,
        "query_types": {},
        "query_paths": {},
    },
    "mongodb": {
        "queries": 0,
        "execution_time_ms": 0,
        "errors": 0,
        "avg_execution_time_ms": 0,
        "max_execution_time_ms": 0,
        "last_minute_queries": 0,
        "slow_queries": 0,
        "collection_stats": {},
        "operation_types": {},
    },
    "redis": {
        "operations": 0,
        "execution_time_ms": 0,
        "errors": 0,
        "avg_execution_time_ms": 0,
        "max_execution_time_ms": 0,
        "last_minute_operations": 0,
        "slow_operations": 0,
        "command_types": {},
    }
}

# Rolling metrics for time-based stats
minute_metrics = {
    "clickhouse": {"start_time": time.time(), "queries": 0},
    "mongodb": {"start_time": time.time(), "queries": 0},
    "redis": {"start_time": time.time(), "operations": 0},
}

# Lock for thread safety of global metrics
metrics_lock = asyncio.Lock()

class BaseMetricsProvider(ABC):
    """Base class for all metrics providers.
    
    A metrics provider is responsible for collecting metrics from a specific system
    like a database, external API, or other service. Providers are registered automatically
    and included in metrics collection.
    """
    
    # Class variable that must be overridden by subclasses
    provider_name: ClassVar[str] = None
    
    # Optional configuration parameters
    config: Dict[str, Any]
    
    def __init__(self, **config):
        """Initialize the metrics provider with optional configuration."""
        self.config = config
        
    @abstractmethod
    async def get_metrics(self) -> Dict[str, Any]:
        """Collect and return metrics for this provider.
        
        Returns:
            Dict[str, Any]: A dictionary containing the metrics collected.
                The structure is provider-specific, but should generally include:
                - Basic counters (queries, operations, calls, etc.)
                - Timing information (time_ms, avg_time_ms, etc.)
                - Error information if relevant
        """
        pass
    
    async def reset_metrics(self) -> None:
        """Reset any metrics counters for this provider.
        
        This method is called at the start of a new request or event processing
        to ensure metrics are specific to the current operation.
        """
        pass

def register_metrics_provider(provider_class: Type[BaseMetricsProvider]) -> Type[BaseMetricsProvider]:
    """Decorator to register a metrics provider class.
    
    Example:
        ```python
        @register_metrics_provider
        class OpenSearchMetricsProvider(BaseMetricsProvider):
            provider_name = "opensearch"
            
            async def get_metrics(self) -> Dict[str, Any]:
                # Implementation
                return {"queries": 10}
        ```
    """
    provider_name = getattr(provider_class, "provider_name", None)
    if not provider_name:
        raise ValueError(f"Metrics provider {provider_class.__name__} has no provider_name attribute")
        
    if provider_name in _METRICS_PROVIDERS:
        logger.warning(f"Overriding existing metrics provider for {provider_name}")
        
    _METRICS_PROVIDERS[provider_name] = provider_class
    logger.debug(f"Registered metrics provider: {provider_name}")
    return provider_class

async def get_all_metrics() -> Dict[str, Dict[str, Any]]:
    """Collect metrics from all registered providers.
    
    Returns:
        Dict[str, Dict[str, Any]]: Dictionary mapping provider names to their metrics
    """
    result = {}
    
    # Instantiate and collect metrics from each provider
    for name, provider_class in _METRICS_PROVIDERS.items():
        try:
            provider = provider_class()
            result[name] = await provider.get_metrics()
        except Exception as e:
            logger.warning(f"Error collecting metrics from provider {name}: {e}")
            result[name] = {"error": str(e)}
            
    return result

async def reset_all_metrics() -> None:
    """Reset metrics for all registered providers."""
    for name, provider_class in _METRICS_PROVIDERS.items():
        try:
            provider = provider_class()
            await provider.reset_metrics()
        except Exception as e:
            logger.warning(f"Error resetting metrics for provider {name}: {e}")

# Standard provider implementations
@register_metrics_provider
class MongoDBMetricsProvider(BaseMetricsProvider):
    """Provider for MongoDB metrics."""
    
    provider_name: ClassVar[str] = "mongodb"
    
    def __init__(self, **config):
        super().__init__(**config)
        
    async def get_metrics(self) -> Dict[str, Any]:
        """Collect MongoDB metrics."""
        return metrics_context.get_mongodb_metrics()
    
    async def reset_metrics(self) -> None:
        """Reset MongoDB metrics."""
        metrics_context.set_mongodb_metrics({
            "queries": 0,
            "time_ms": 0,
            "slow_queries": 0,
            "operation_types": {},
            "collection_stats": {},
            "timestamp": time.time()
        })

@register_metrics_provider
class ClickHouseMetricsProvider(BaseMetricsProvider):
    """Provider for ClickHouse metrics."""
    
    provider_name: ClassVar[str] = "clickhouse"
    
    def __init__(self, **config):
        super().__init__(**config)
        
    async def get_metrics(self) -> Dict[str, Any]:
        """Collect ClickHouse metrics."""
        return metrics_context.get_clickhouse_metrics()
    
    async def reset_metrics(self) -> None:
        """Reset ClickHouse metrics."""
        metrics_context.set_clickhouse_metrics({
            "queries": 0,
            "time_ms": 0,
            "slow_queries": 0,
            "query_types": {},
            "timestamp": time.time()
        })

@register_metrics_provider
class RedisMetricsProvider(BaseMetricsProvider):
    """Provider for Redis metrics."""
    
    provider_name: ClassVar[str] = "redis"
    
    def __init__(self, **config):
        super().__init__(**config)
        
    async def get_metrics(self) -> Dict[str, Any]:
        """Collect Redis metrics."""
        return metrics_context.get_redis_metrics()
    
    async def reset_metrics(self) -> None:
        """Reset Redis metrics."""
        metrics_context.set_redis_metrics({
            "operations": 0,
            "time_ms": 0,
            "slow_operations": 0,
            "command_types": {},
            "timestamp": time.time()
        })

async def reset_minute_counters():
    """Reset per-minute counters after a minute has passed"""
    async with metrics_lock:
        current_time = time.time()
        for db_type, data in minute_metrics.items():
            if current_time - data["start_time"] >= 60:
                # Set the per-minute stats in the main metrics
                if db_type == "redis":
                    global_metrics[db_type]["last_minute_operations"] = data["operations"]
                    data["operations"] = 0
                else:
                    global_metrics[db_type]["last_minute_queries"] = data["queries"]
                    data["queries"] = 0
                data["start_time"] = current_time

async def reset_request_metrics():
    """Reset per-request metrics at the start of each request"""
    await reset_all_metrics()

async def get_request_metrics():
    """Get metrics for the current request"""
    return await get_all_metrics()

async def record_operation(db_type: str, execution_time_ms: float, operation_type: str = None, is_slow: bool = False):
    """Record a database operation for the current request and global metrics
    
    Args:
        db_type: One of 'clickhouse', 'mongodb', 'redis'
        execution_time_ms: Time in milliseconds the operation took
        operation_type: Type of operation (e.g., 'find', 'insert', 'GET', 'SET')
        is_slow: Whether this operation is considered slow
    """
    if db_type not in global_metrics:
        return
    
    # Update global metrics
    async with metrics_lock:
        if db_type == "redis":
            global_metrics[db_type]["operations"] += 1
            minute_metrics[db_type]["operations"] += 1
        else:
            global_metrics[db_type]["queries"] += 1
            minute_metrics[db_type]["queries"] += 1
        
        global_metrics[db_type]["execution_time_ms"] += execution_time_ms
        
        # Update max execution time
        if execution_time_ms > global_metrics[db_type]["max_execution_time_ms"]:
            global_metrics[db_type]["max_execution_time_ms"] = execution_time_ms
        
        # Update average execution time
        total_ops = global_metrics[db_type]["operations" if db_type == "redis" else "queries"]
        if total_ops > 0:
            global_metrics[db_type]["avg_execution_time_ms"] = (
                global_metrics[db_type]["execution_time_ms"] / total_ops
            )
        
        # Track slow operations
        if is_slow:
            global_metrics[db_type]["slow_operations" if db_type == "redis" else "slow_queries"] += 1
        
        # Track operation type
        if operation_type:
            type_key = "command_types" if db_type == "redis" else "operation_types" if db_type == "mongodb" else "query_types"
            if type_key in global_metrics[db_type]:
                op_types = global_metrics[db_type][type_key]
                op_types[operation_type] = op_types.get(operation_type, 0) + 1
    
    # Update request-specific metrics
    if db_type == "mongodb":
        mongodb_metrics = metrics_context.get_mongodb_metrics()
        mongodb_metrics["queries"] = mongodb_metrics.get("queries", 0) + 1
        mongodb_metrics["time_ms"] = mongodb_metrics.get("time_ms", 0) + execution_time_ms
        if is_slow:
            mongodb_metrics["slow_queries"] = mongodb_metrics.get("slow_queries", 0) + 1
        if operation_type:
            op_types = mongodb_metrics.get("operation_types", {})
            op_types[operation_type] = op_types.get(operation_type, 0) + 1
            mongodb_metrics["operation_types"] = op_types
        metrics_context.set_mongodb_metrics(mongodb_metrics)
    
    elif db_type == "clickhouse":
        clickhouse_metrics = metrics_context.get_clickhouse_metrics()
        clickhouse_metrics["queries"] = clickhouse_metrics.get("queries", 0) + 1
        clickhouse_metrics["time_ms"] = clickhouse_metrics.get("time_ms", 0) + execution_time_ms
        if is_slow:
            clickhouse_metrics["slow_queries"] = clickhouse_metrics.get("slow_queries", 0) + 1
        if operation_type:
            query_types = clickhouse_metrics.get("query_types", {})
            query_types[operation_type] = query_types.get(operation_type, 0) + 1
            clickhouse_metrics["query_types"] = query_types
        metrics_context.set_clickhouse_metrics(clickhouse_metrics)
    
    elif db_type == "redis":
        redis_metrics = metrics_context.get_redis_metrics()
        redis_metrics["operations"] = redis_metrics.get("operations", 0) + 1
        redis_metrics["time_ms"] = redis_metrics.get("time_ms", 0) + execution_time_ms
        if is_slow:
            redis_metrics["slow_operations"] = redis_metrics.get("slow_operations", 0) + 1
        if operation_type:
            command_types = redis_metrics.get("command_types", {})
            command_types[operation_type] = command_types.get(operation_type, 0) + 1
            redis_metrics["command_types"] = command_types
        metrics_context.set_redis_metrics(redis_metrics)

def track_clickhouse_query(func: F) -> F:
    """Decorator to track ClickHouse query execution time and performance"""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if not getattr(settings, "DB_METRICS_ENABLE", False):
            return await func(*args, **kwargs)
            
        start_time = time.time()
        error = None
        query_type = "UNKNOWN"
        
        # Try to extract query type from first argument if it's a string
        if args and isinstance(args[0], str):
            query = args[0].strip().upper()
            if query.startswith("SELECT"):
                query_type = "SELECT"
            elif query.startswith("INSERT"):
                query_type = "INSERT"
            elif query.startswith("UPDATE"):
                query_type = "UPDATE"
            elif query.startswith("DELETE"):
                query_type = "DELETE"
            elif query.startswith("ALTER"):
                query_type = "ALTER"
            elif query.startswith("CREATE"):
                query_type = "CREATE"
            elif query.startswith("DROP"):
                query_type = "DROP"
            else:
                # Try to find first word
                words = query.split()
                if words:
                    query_type = words[0]
        
        # Get caller information
        frame = inspect.currentframe().f_back
        module_name = frame.f_globals["__name__"] if frame else "unknown"
        
        try:
            result = await func(*args, **kwargs)
            return result
        except Exception as e:
            error = e
            raise
        finally:
            execution_time = (time.time() - start_time) * 1000  # ms
            is_slow = execution_time > 100  # Consider queries over 100ms as slow
            
            # Record metrics
            asyncio.create_task(record_operation(
                db_type="clickhouse", 
                execution_time_ms=execution_time, 
                operation_type=query_type,
                is_slow=is_slow
            ))
            
            # Update global metrics
            async def update_global_metrics():
                async with metrics_lock:
                    # Update error count if there was an error
                    if error:
                        global_metrics["clickhouse"]["errors"] += 1
                    
                    # Track query path
                    query_paths = global_metrics["clickhouse"]["query_paths"]
                    query_paths[module_name] = query_paths.get(module_name, 0) + 1
                    
            # Update metrics asynchronously
            asyncio.create_task(update_global_metrics())
    
    return wrapper

def track_mongo_operation(func: F) -> F:
    """Decorator to track MongoDB operation execution time and performance"""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if not getattr(settings, "DB_METRICS_ENABLE", False):
            return await func(*args, **kwargs)
            
        start_time = time.time()
        error = None
        operation_type = func.__name__
        
        # Try to determine collection name if possible
        collection_name = "unknown"
        if hasattr(args[0], "collection_name") and callable(getattr(args[0], "collection_name", None)):
            try:
                collection_name = args[0].collection_name()
            except:
                pass
        elif hasattr(args[0], "_collection"):
            collection_name = getattr(args[0], "_collection", "unknown")
        
        try:
            result = await func(*args, **kwargs)
            return result
        except Exception as e:
            error = e
            raise
        finally:
            execution_time = (time.time() - start_time) * 1000  # ms
            is_slow = execution_time > 100
            
            # Record metrics
            asyncio.create_task(record_operation(
                db_type="mongodb", 
                execution_time_ms=execution_time, 
                operation_type=operation_type,
                is_slow=is_slow
            ))
            
            # Update collection-specific metrics
            async def update_collection_metrics():
                async with metrics_lock:
                    # Update error count if there was an error
                    if error:
                        global_metrics["mongodb"]["errors"] += 1
                        
                    # Track collection stats
                    collection_stats = global_metrics["mongodb"]["collection_stats"]
                    collection_stats[collection_name] = collection_stats.get(collection_name, 0) + 1
                    
                # Update request-specific collection stats
                mongodb_metrics = metrics_context.get_mongodb_metrics()
                collection_stats = mongodb_metrics.get("collection_stats", {})
                collection_stats[collection_name] = collection_stats.get(collection_name, 0) + 1
                mongodb_metrics["collection_stats"] = collection_stats
                metrics_context.set_mongodb_metrics(mongodb_metrics)
            
            # Update metrics asynchronously
            asyncio.create_task(update_collection_metrics())
    
    return wrapper

def track_redis_operation(func: F) -> F:
    """Decorator to track Redis operation execution time and performance"""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if not getattr(settings, "DB_METRICS_ENABLE", False):
            return await func(*args, **kwargs)
            
        start_time = time.time()
        error = None
        command = func.__name__
        
        try:
            result = await func(*args, **kwargs)
            return result
        except Exception as e:
            error = e
            raise
        finally:
            execution_time = (time.time() - start_time) * 1000  # ms
            is_slow = execution_time > 50  # Consider operations over 50ms as slow
            
            # Record metrics
            asyncio.create_task(record_operation(
                db_type="redis", 
                execution_time_ms=execution_time, 
                operation_type=command,
                is_slow=is_slow
            ))
            
            # Update global metrics
            async def update_global_metrics():
                async with metrics_lock:
                    # Update error count if there was an error
                    if error:
                        global_metrics["redis"]["errors"] += 1
            
            # Update metrics asynchronously
            asyncio.create_task(update_global_metrics())
    
    return wrapper

@asynccontextmanager
async def track_db_operation(db_type: str, operation_name: str):
    """
    Context manager to track custom database operations
    
    Example:
        async with track_db_operation("clickhouse", "complex_query"):
            # perform database operations here
    """
    if db_type not in global_metrics or not getattr(settings, "DB_METRICS_ENABLE", False):
        yield
        return
        
    start_time = time.time()
    error = None
    
    try:
        yield
    except Exception as e:
        error = e
        raise
    finally:
        execution_time = (time.time() - start_time) * 1000  # ms
        is_slow = execution_time > (50 if db_type == "redis" else 100)
        
        # Record metrics
        asyncio.create_task(record_operation(
            db_type=db_type, 
            execution_time_ms=execution_time, 
            operation_type=operation_name,
            is_slow=is_slow
        ))
        
        # Update global metrics
        async def update_global_metrics():
            async with metrics_lock:
                # Update error count if there was an error
                if error:
                    global_metrics[db_type]["errors"] += 1
        
        # Update metrics asynchronously
        asyncio.create_task(update_global_metrics())

async def get_metrics_summary() -> Dict[str, Any]:
    """Get a summary of all database metrics"""
    await reset_minute_counters()
    
    async with metrics_lock:
        return {
            "clickhouse": {
                "total_queries": global_metrics["clickhouse"]["queries"],
                "avg_execution_time_ms": global_metrics["clickhouse"]["avg_execution_time_ms"],
                "max_execution_time_ms": global_metrics["clickhouse"]["max_execution_time_ms"],
                "error_rate": global_metrics["clickhouse"]["errors"] / max(global_metrics["clickhouse"]["queries"], 1),
                "queries_per_minute": global_metrics["clickhouse"]["last_minute_queries"],
                "slow_query_percentage": global_metrics["clickhouse"]["slow_queries"] / max(global_metrics["clickhouse"]["queries"], 1) * 100,
                "top_query_types": sorted(
                    global_metrics["clickhouse"]["query_types"].items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5],
            },
            "mongodb": {
                "total_queries": global_metrics["mongodb"]["queries"],
                "avg_execution_time_ms": global_metrics["mongodb"]["avg_execution_time_ms"],
                "max_execution_time_ms": global_metrics["mongodb"]["max_execution_time_ms"],
                "error_rate": global_metrics["mongodb"]["errors"] / max(global_metrics["mongodb"]["queries"], 1),
                "queries_per_minute": global_metrics["mongodb"]["last_minute_queries"],
                "slow_query_percentage": global_metrics["mongodb"]["slow_queries"] / max(global_metrics["mongodb"]["queries"], 1) * 100,
                "top_collections": sorted(
                    global_metrics["mongodb"]["collection_stats"].items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5],
            },
            "redis": {
                "total_operations": global_metrics["redis"]["operations"],
                "avg_execution_time_ms": global_metrics["redis"]["avg_execution_time_ms"],
                "max_execution_time_ms": global_metrics["redis"]["max_execution_time_ms"],
                "error_rate": global_metrics["redis"]["errors"] / max(global_metrics["redis"]["operations"], 1),
                "operations_per_minute": global_metrics["redis"]["last_minute_operations"],
                "slow_operation_percentage": global_metrics["redis"]["slow_operations"] / max(global_metrics["redis"]["operations"], 1) * 100,
                "top_commands": sorted(
                    global_metrics["redis"]["command_types"].items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5],
            },
        }

async def background_metrics_reporter():
    """Background task to periodically report database metrics"""
    while True:
        try:
            # Sleep first to allow some metrics to accumulate
            await asyncio.sleep(300)  # Report every 5 minutes
            
            if not getattr(settings, "DB_METRICS_ENABLE", False):
                continue
                
            summary = await get_metrics_summary()
            
            # Format and log the metrics
            log_lines = ["Database Performance Metrics:"]
            
            # ClickHouse metrics
            ch_metrics = summary["clickhouse"]
            log_lines.append(f"ClickHouse: {ch_metrics['total_queries']} queries, " +
                           f"{ch_metrics['queries_per_minute']}/min, " +
                           f"avg={ch_metrics['avg_execution_time_ms']:.2f}ms, " +
                           f"max={ch_metrics['max_execution_time_ms']:.2f}ms, " +
                           f"slow={ch_metrics['slow_query_percentage']:.2f}%, " +
                           f"err={ch_metrics['error_rate']*100:.2f}%")
            
            # MongoDB metrics
            mg_metrics = summary["mongodb"]
            log_lines.append(f"MongoDB: {mg_metrics['total_queries']} queries, " +
                           f"{mg_metrics['queries_per_minute']}/min, " +
                           f"avg={mg_metrics['avg_execution_time_ms']:.2f}ms, " +
                           f"max={mg_metrics['max_execution_time_ms']:.2f}ms, " +
                           f"slow={mg_metrics['slow_query_percentage']:.2f}%, " +
                           f"err={mg_metrics['error_rate']*100:.2f}%")
            
            # Redis metrics
            rd_metrics = summary["redis"]
            log_lines.append(f"Redis: {rd_metrics['total_operations']} ops, " +
                           f"{rd_metrics['operations_per_minute']}/min, " +
                           f"avg={rd_metrics['avg_execution_time_ms']:.2f}ms, " +
                           f"max={rd_metrics['max_execution_time_ms']:.2f}ms, " +
                           f"slow={rd_metrics['slow_operation_percentage']:.2f}%, " +
                           f"err={rd_metrics['error_rate']*100:.2f}%")
            
            # Log the formatted metrics
            logger.info("\n".join(log_lines))
            
        except Exception as e:
            logger.error(f"Error in metrics reporter: {e}", exc_info=True)

# Background task reference
metrics_reporter_task = None

async def start_metrics_collection():
    """Start the background metrics reporter"""
    global metrics_reporter_task
    if metrics_reporter_task is None or metrics_reporter_task.done():
        metrics_reporter_task = asyncio.create_task(background_metrics_reporter())
        logger.info("Started database metrics collection")

async def stop_metrics_collection():
    """Stop the background metrics reporter"""
    global metrics_reporter_task
    if metrics_reporter_task and not metrics_reporter_task.done():
        metrics_reporter_task.cancel()
        try:
            await metrics_reporter_task
        except asyncio.CancelledError:
            pass
        logger.info("Stopped database metrics collection")

async def clear_metrics():
    """Clear all collected metrics (mainly for testing)"""
    await reset_all_metrics()
    
    async with metrics_lock:
        for db_type in global_metrics:
            # Fix the problematic line by separating the conditional logic
            if db_type == "redis":
                global_metrics[db_type]["operations"] = 0
            else:
                global_metrics[db_type]["queries"] = 0
            
            global_metrics[db_type]["execution_time_ms"] = 0
            global_metrics[db_type]["errors"] = 0
            global_metrics[db_type]["avg_execution_time_ms"] = 0
            global_metrics[db_type]["max_execution_time_ms"] = 0
            global_metrics[db_type]["slow_queries" if db_type != "redis" else "slow_operations"] = 0
            
            # Clear breakdown dictionaries
            if db_type == "clickhouse":
                global_metrics[db_type]["query_types"] = {}
                global_metrics[db_type]["query_paths"] = {}
            elif db_type == "mongodb":
                global_metrics[db_type]["collection_stats"] = {}
                global_metrics[db_type]["operation_types"] = {}
            elif db_type == "redis":
                global_metrics[db_type]["command_types"] = {}

# For compatibility with existing code
get_mongodb_metrics = MongoDBMetricsProvider().get_metrics
get_clickhouse_metrics = ClickHouseMetricsProvider().get_metrics
get_redis_metrics = RedisMetricsProvider().get_metrics

# Export all relevant functions and classes
__all__ = [
    "reset_request_metrics", 
    "get_request_metrics", 
    "track_clickhouse_query", 
    "track_mongo_operation",
    "track_redis_operation", 
    "track_db_operation", 
    "start_metrics_collection", 
    "stop_metrics_collection",
    "get_metrics_summary", 
    "clear_metrics", 
    "reset_all_metrics", 
    "get_all_metrics",
    "BaseMetricsProvider", 
    "register_metrics_provider", 
    "get_mongodb_metrics",
    "get_clickhouse_metrics", 
    "get_redis_metrics"
]