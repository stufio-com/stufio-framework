"""
Database metrics collector for monitoring MongoDB, ClickHouse, and Redis query performance.

This module provides decorators and utilities to track database operations
and measure performance across different database systems.
"""
import functools
import time
import logging
import asyncio
from typing import Dict, Any, Optional, Callable, TypeVar, Awaitable, List
from contextlib import asynccontextmanager
import inspect
from stufio.core.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

# Type definitions
F = TypeVar('F', bound=Callable[..., Awaitable[Any]])

# In-memory metrics store
metrics: Dict[str, Dict[str, Any]] = {
    "clickhouse": {
        "queries": 0,
        "execution_time_ms": 0,
        "errors": 0,
        "avg_execution_time_ms": 0,
        "max_execution_time_ms": 0,
        "last_minute_queries": 0,
        "slow_queries": 0,  # Queries taking more than 100ms
        "query_types": {},  # Breakdown by query type (SELECT, INSERT, etc)
        "query_paths": {},  # Breakdown by calling module/function
    },
    "mongo": {
        "queries": 0,
        "execution_time_ms": 0,
        "errors": 0,
        "avg_execution_time_ms": 0,
        "max_execution_time_ms": 0,
        "last_minute_queries": 0,
        "slow_queries": 0,
        "collection_stats": {},  # Queries per collection
        "operation_types": {},   # Breakdown by operation type (find, update, etc)
    },
    "redis": {
        "operations": 0,
        "execution_time_ms": 0,
        "errors": 0,
        "avg_execution_time_ms": 0,
        "max_execution_time_ms": 0,
        "last_minute_operations": 0,
        "slow_operations": 0,
        "command_types": {},     # Breakdown by command (GET, SET, etc)
    }
}

# Rolling metrics for time-based stats
minute_metrics = {
    "clickhouse": {"start_time": time.time(), "queries": 0},
    "mongo": {"start_time": time.time(), "queries": 0},
    "redis": {"start_time": time.time(), "operations": 0},
}

# Per-request metrics tracking
request_metrics = {
    "clickhouse": {"queries": 0, "time_ms": 0},
    "mongo": {"queries": 0, "time_ms": 0},
    "redis": {"operations": 0, "time_ms": 0}
}

# Lock for thread safety
metrics_lock = asyncio.Lock()

async def reset_minute_counters():
    """Reset per-minute counters after a minute has passed"""
    async with metrics_lock:
        current_time = time.time()
        for db_type, data in minute_metrics.items():
            if current_time - data["start_time"] >= 60:
                # Set the per-minute stats in the main metrics
                if db_type == "redis":
                    metrics[db_type]["last_minute_operations"] = data["operations"]
                    data["operations"] = 0
                else:
                    metrics[db_type]["last_minute_queries"] = data["queries"]
                    data["queries"] = 0
                data["start_time"] = current_time

async def reset_request_metrics():
    """Reset per-request metrics at the start of each request"""
    async with metrics_lock:
        request_metrics["clickhouse"] = {"queries": 0, "time_ms": 0}
        request_metrics["mongo"] = {"queries": 0, "time_ms": 0}
        request_metrics["redis"] = {"operations": 0, "time_ms": 0}

async def get_request_metrics():
    """Get metrics for the current request"""
    async with metrics_lock:
        # Return a copy to avoid concurrent modification
        return {
            "clickhouse": dict(request_metrics["clickhouse"]),
            "mongo": dict(request_metrics["mongo"]),
            "redis": dict(request_metrics["redis"])
        }

async def record_operation(db_type: str, execution_time_ms: float):
    """Record a database operation for the current request and global metrics
    
    Args:
        db_type: One of 'clickhouse', 'mongo', 'redis'
        execution_time_ms: Time in milliseconds the operation took
    """
    if db_type not in metrics:
        return
    
    async with metrics_lock:
        # Update global metrics
        if db_type == "redis":
            metrics[db_type]["operations"] += 1
            request_metrics[db_type]["operations"] += 1
        else:
            metrics[db_type]["queries"] += 1
            request_metrics[db_type]["queries"] += 1
        
        metrics[db_type]["execution_time_ms"] += execution_time_ms
        request_metrics[db_type]["time_ms"] += execution_time_ms

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
            
            async def update_metrics():
                async with metrics_lock:
                    # Update total query stats
                    metrics["clickhouse"]["queries"] += 1
                    metrics["clickhouse"]["execution_time_ms"] += execution_time
                    
                    # Update per-request metrics
                    request_metrics["clickhouse"]["queries"] += 1
                    request_metrics["clickhouse"]["time_ms"] += execution_time
                    
                    # Update per-minute stats
                    minute_metrics["clickhouse"]["queries"] += 1
                    
                    # Update error count if there was an error
                    if error:
                        metrics["clickhouse"]["errors"] += 1
                    
                    # Update max execution time
                    if execution_time > metrics["clickhouse"]["max_execution_time_ms"]:
                        metrics["clickhouse"]["max_execution_time_ms"] = execution_time
                    
                    # Update average execution time
                    metrics["clickhouse"]["avg_execution_time_ms"] = (
                        metrics["clickhouse"]["execution_time_ms"] / 
                        metrics["clickhouse"]["queries"]
                    )
                    
                    # Track slow queries (>100ms)
                    if execution_time > 100:
                        metrics["clickhouse"]["slow_queries"] += 1
                    
                    # Track query type
                    query_types = metrics["clickhouse"]["query_types"]
                    query_types[query_type] = query_types.get(query_type, 0) + 1
                    
                    # Track query path
                    query_paths = metrics["clickhouse"]["query_paths"]
                    query_paths[module_name] = query_paths.get(module_name, 0) + 1
            
            # Update metrics asynchronously
            asyncio.create_task(update_metrics())
    
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
        if hasattr(args[0], "collection_name") and callable(args[0].collection_name):
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
            
            async def update_metrics():
                async with metrics_lock:
                    # Update total query stats
                    metrics["mongo"]["queries"] += 1
                    metrics["mongo"]["execution_time_ms"] += execution_time
                    
                    # Update per-request metrics
                    request_metrics["mongo"]["queries"] += 1
                    request_metrics["mongo"]["time_ms"] += execution_time
                    
                    # Update per-minute stats
                    minute_metrics["mongo"]["queries"] += 1
                    
                    # Update error count if there was an error
                    if error:
                        metrics["mongo"]["errors"] += 1
                    
                    # Update max execution time
                    if execution_time > metrics["mongo"]["max_execution_time_ms"]:
                        metrics["mongo"]["max_execution_time_ms"] = execution_time
                    
                    # Update average execution time
                    metrics["mongo"]["avg_execution_time_ms"] = (
                        metrics["mongo"]["execution_time_ms"] / 
                        metrics["mongo"]["queries"]
                    )
                    
                    # Track slow queries (>100ms)
                    if execution_time > 100:
                        metrics["mongo"]["slow_queries"] += 1
                    
                    # Track collection stats
                    collection_stats = metrics["mongo"]["collection_stats"]
                    collection_stats[collection_name] = collection_stats.get(collection_name, 0) + 1
                    
                    # Track operation type
                    operation_types = metrics["mongo"]["operation_types"]
                    operation_types[operation_type] = operation_types.get(operation_type, 0) + 1
            
            # Update metrics asynchronously
            asyncio.create_task(update_metrics())
    
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
            
            async def update_metrics():
                async with metrics_lock:
                    # Update total operation stats
                    metrics["redis"]["operations"] += 1
                    metrics["redis"]["execution_time_ms"] += execution_time
                    
                    # Update per-request metrics
                    request_metrics["redis"]["operations"] += 1
                    request_metrics["redis"]["time_ms"] += execution_time
                    
                    # Update per-minute stats
                    minute_metrics["redis"]["operations"] += 1
                    
                    # Update error count if there was an error
                    if error:
                        metrics["redis"]["errors"] += 1
                    
                    # Update max execution time
                    if execution_time > metrics["redis"]["max_execution_time_ms"]:
                        metrics["redis"]["max_execution_time_ms"] = execution_time
                    
                    # Update average execution time
                    metrics["redis"]["avg_execution_time_ms"] = (
                        metrics["redis"]["execution_time_ms"] / 
                        metrics["redis"]["operations"]
                    )
                    
                    # Track slow operations (>50ms)
                    if execution_time > 50:
                        metrics["redis"]["slow_operations"] += 1
                    
                    # Track command types
                    command_types = metrics["redis"]["command_types"]
                    command_types[command] = command_types.get(command, 0) + 1
            
            # Update metrics asynchronously
            asyncio.create_task(update_metrics())
    
    return wrapper

@asynccontextmanager
async def track_db_operation(db_type: str, operation_name: str):
    """
    Context manager to track custom database operations
    
    Example:
        async with track_db_operation("clickhouse", "complex_query"):
            # perform database operations here
    """
    if db_type not in metrics or not getattr(settings, "DB_METRICS_ENABLE", False):
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
        
        async with metrics_lock:
            # Update operation count based on db_type
            if db_type == "redis":
                metrics[db_type]["operations"] += 1
                minute_metrics[db_type]["operations"] += 1
                op_key = "operations"
            else:
                metrics[db_type]["queries"] += 1
                minute_metrics[db_type]["queries"] += 1
                op_key = "queries"
            
            # Update execution time
            metrics[db_type]["execution_time_ms"] += execution_time
            
            # Update error count if there was an error
            if error:
                metrics[db_type]["errors"] += 1
            
            # Update max execution time
            if execution_time > metrics[db_type]["max_execution_time_ms"]:
                metrics[db_type]["max_execution_time_ms"] = execution_time
            
            # Update average execution time
            total_ops = metrics[db_type][op_key]
            if total_ops > 0:
                metrics[db_type]["avg_execution_time_ms"] = (
                    metrics[db_type]["execution_time_ms"] / total_ops
                )
            
            # Track slow operations
            slow_threshold = 50 if db_type == "redis" else 100
            if execution_time > slow_threshold:
                metrics[db_type]["slow_queries" if db_type != "redis" else "slow_operations"] += 1
            
            # Track operation types
            if db_type == "clickhouse":
                query_types = metrics[db_type]["query_types"]
                query_types[operation_name] = query_types.get(operation_name, 0) + 1
            elif db_type == "mongo":
                operation_types = metrics[db_type]["operation_types"]
                operation_types[operation_name] = operation_types.get(operation_name, 0) + 1
            elif db_type == "redis":
                command_types = metrics[db_type]["command_types"]
                command_types[operation_name] = command_types.get(operation_name, 0) + 1

async def get_metrics_summary() -> Dict[str, Any]:
    """Get a summary of all database metrics"""
    await reset_minute_counters()
    
    async with metrics_lock:
        return {
            "clickhouse": {
                "total_queries": metrics["clickhouse"]["queries"],
                "avg_execution_time_ms": metrics["clickhouse"]["avg_execution_time_ms"],
                "max_execution_time_ms": metrics["clickhouse"]["max_execution_time_ms"],
                "error_rate": metrics["clickhouse"]["errors"] / max(metrics["clickhouse"]["queries"], 1),
                "queries_per_minute": metrics["clickhouse"]["last_minute_queries"],
                "slow_query_percentage": metrics["clickhouse"]["slow_queries"] / max(metrics["clickhouse"]["queries"], 1) * 100,
                "top_query_types": sorted(
                    metrics["clickhouse"]["query_types"].items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5],
            },
            "mongo": {
                "total_queries": metrics["mongo"]["queries"],
                "avg_execution_time_ms": metrics["mongo"]["avg_execution_time_ms"],
                "max_execution_time_ms": metrics["mongo"]["max_execution_time_ms"],
                "error_rate": metrics["mongo"]["errors"] / max(metrics["mongo"]["queries"], 1),
                "queries_per_minute": metrics["mongo"]["last_minute_queries"],
                "slow_query_percentage": metrics["mongo"]["slow_queries"] / max(metrics["mongo"]["queries"], 1) * 100,
                "top_collections": sorted(
                    metrics["mongo"]["collection_stats"].items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5],
            },
            "redis": {
                "total_operations": metrics["redis"]["operations"],
                "avg_execution_time_ms": metrics["redis"]["avg_execution_time_ms"],
                "max_execution_time_ms": metrics["redis"]["max_execution_time_ms"],
                "error_rate": metrics["redis"]["errors"] / max(metrics["redis"]["operations"], 1),
                "operations_per_minute": metrics["redis"]["last_minute_operations"],
                "slow_operation_percentage": metrics["redis"]["slow_operations"] / max(metrics["redis"]["operations"], 1) * 100,
                "top_commands": sorted(
                    metrics["redis"]["command_types"].items(),
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
            mg_metrics = summary["mongo"]
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

def clear_metrics():
    """Clear all collected metrics (mainly for testing)"""
    global metrics
    for db_type in metrics:
        metrics[db_type]["queries"] = 0 if db_type != "redis" else metrics[db_type]["operations"]
        metrics[db_type]["execution_time_ms"] = 0
        metrics[db_type]["errors"] = 0
        metrics[db_type]["avg_execution_time_ms"] = 0
        metrics[db_type]["max_execution_time_ms"] = 0
        metrics[db_type]["slow_queries" if db_type != "redis" else "slow_operations"] = 0
        
        # Clear breakdown dictionaries
        if db_type == "clickhouse":
            metrics[db_type]["query_types"] = {}
            metrics[db_type]["query_paths"] = {}
        elif db_type == "mongo":
            metrics[db_type]["collection_stats"] = {}
            metrics[db_type]["operation_types"] = {}
        elif db_type == "redis":
            metrics[db_type]["command_types"] = {}