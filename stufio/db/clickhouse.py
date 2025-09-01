import asyncio
import random
from typing import Optional
import logging
from stufio.core.config import get_settings
import time
from datetime import datetime, timedelta

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import ClickHouseError

from urllib.parse import urlparse

settings = get_settings()
logger = logging.getLogger(__name__)

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
    _optimization_task: Optional[asyncio.Task] = None
    _last_optimization_check: Optional[datetime] = None
    _optimization_interval_minutes: int = 5  # Check every 5 minutes

    def __init__(self):
        self.clickhouse_client = None
        self._optimization_task = None
        self._last_optimization_check = None

    @classmethod
    async def get_instance(cls):
        if not cls._instance:
            # Check if we have cluster DSN list or single DSN
            cluster_dsn_list = settings.CLICKHOUSE_CLUSTER_DSN_LIST
            single_dsn = settings.CLICKHOUSE_DSN
            cluster_name = settings.CLICKHOUSE_CLUSTER_NAME

            if cluster_dsn_list and len(cluster_dsn_list) > 0:
                logger.info(f"Using cluster mode with {len(cluster_dsn_list)} nodes")
                connected_client = await cls._connect_to_cluster(cluster_dsn_list, cluster_name)
            else:
                logger.info(f"Using single node mode")
                connected_client = await cls._connect_to_single_node(single_dsn)

            if connected_client:
                cls._instance = cls()
                cls._instance.clickhouse_client = connected_client

                # Start periodic optimization if enabled
                if settings.CLICKHOUSE_CLUSTER_CONN_OPTIMIZE and cluster_dsn_list and cluster_name:
                    optimization_mode = getattr(
                        settings, "CLICKHOUSE_CONN_OPTIMIZE_MODE", "once"
                    )

                    if optimization_mode == "once":
                        # Single optimization attempt
                        logger.info("ClickHouse connection optimization enabled (once mode)")
                        asyncio.create_task(cls._instance._perform_single_optimization())
                    elif optimization_mode in ["adaptive", "periodic"]:
                        # Continuous optimization
                        cls._instance._start_optimization_task()
                        logger.info(f"ClickHouse connection optimization enabled ({optimization_mode} mode)")
                    else:
                        logger.warning(f"Unknown optimization mode: {optimization_mode}, using adaptive mode")
                        cls._instance._start_optimization_task()
                        logger.info("ClickHouse connection optimization enabled (adaptive mode)")
            else:
                raise ClickhouseConnectionError("Failed to connect to any ClickHouse instance")

        return cls._instance

    @classmethod
    def _create_pool_manager(cls) -> Optional[object]:
        """Create a custom urllib3 pool manager with optimal settings for ClickHouse connections"""
        try:
            import urllib3
            
            pool_mgr = urllib3.PoolManager(
                num_pools=settings.CLICKHOUSE_POOL_SIZE,
                maxsize=settings.CLICKHOUSE_POOL_SIZE,
                block=False,  # Don't block when pool is full, discard instead
                timeout=urllib3.Timeout(connect=30, read=180),
                retries=urllib3.Retry(
                    total=3,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504]
                )
            )
            
            logger.info(f"Created ClickHouse connection pool with size: {settings.CLICKHOUSE_POOL_SIZE}")
            return pool_mgr
            
        except Exception as e:
            logger.warning(f"Failed to create custom pool manager: {e}, using default")
            return None

    @classmethod
    def _get_connection_params(cls, dsn: str, client_name_suffix: str = "") -> dict:
        """Get standardized connection parameters for ClickHouse client"""
        
        # Create custom pool manager
        pool_mgr = cls._create_pool_manager()
        
        # Get settings with defaults for missing values
        readonly = getattr(settings, 'CLICKHOUSE_READ_ONLY', 0)
        max_threads = getattr(settings, 'CLICKHOUSE_MAX_THREADS', 4)
        
        # Base connection parameters
        connection_params = {
            "dsn": dsn,
            "settings": {
                "send_progress_in_http_headers": True,
                "readonly": int(readonly),
                "max_threads": max(1, min(8, max_threads)),
            },
            "generic_args": {
                "client_name": f"stufio.fastapi{client_name_suffix}",
                "connect_timeout": 30,
                "send_receive_timeout": 180,
                "query_retries": 3,  # Use query_retries instead of max_retries
                "compress": True,
                "verify": True,
            }
        }
        
        # Add pool manager if successfully created
        if pool_mgr:
            connection_params["generic_args"]["pool_mgr"] = pool_mgr
            
        return connection_params

    @classmethod
    async def _connect_to_single_node(cls, dsn: str) -> Optional[AsyncClient]:
        """Connect to a single ClickHouse node"""
        try:
            parsed = urlparse(dsn)
            if parsed.scheme not in ['clickhouse', 'clickhousedb', 'http', 'https', 'clickhouse+http', 'clickhouse+https']:
                raise ValueError(f"Invalid Clickhouse DSN: {dsn}")
            if not parsed.path or parsed.path == "/":
                raise ValueError(f"Missing database name in DSN: {dsn}")

            logger.info(f"Connecting to Clickhouse at {dsn}")

            # Get standardized connection parameters
            connection_params = cls._get_connection_params(dsn, client_name_suffix="")
            
            # Create the ClickHouse client
            clickhouse_client = await clickhouse_connect.get_async_client(**connection_params)

            # Test the connection
            await clickhouse_client.ping()

            # Apply metrics wrapper if enabled
            cls._apply_metrics_wrapper(clickhouse_client)

            logger.info(f"Successfully connected to ClickHouse at {dsn}")
            logger.info(f"Connection configured with compression and extended timeouts")
            return clickhouse_client

        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse at {dsn}: {str(e)}")
            return None

    @classmethod
    async def _connect_to_cluster(cls, dsn_list: list, cluster_name: Optional[str]) -> Optional[AsyncClient]:
        """Connect to ClickHouse cluster, trying nodes in order and checking cluster health"""
        last_exception = None

        for dsn in dsn_list:
            try:
                parsed = urlparse(dsn)
                if parsed.scheme not in ['clickhouse', 'clickhousedb', 'http', 'https', 'clickhouse+http', 'clickhouse+https']:
                    logger.warning(f"Invalid Clickhouse DSN: {dsn}")
                    continue
                if not parsed.path or parsed.path == "/":
                    logger.warning(f"Missing database name in DSN: {dsn}")
                    continue

                logger.info(f"Trying to connect to Clickhouse cluster node at {dsn}")

                # Get standardized connection parameters for cluster
                connection_params = cls._get_connection_params(dsn, client_name_suffix=".cluster")
                
                # Create the ClickHouse client with proper pool management
                clickhouse_client = await clickhouse_connect.get_async_client(**connection_params)

                # Test basic connection
                await clickhouse_client.ping()

                # If cluster name is provided, check cluster health
                if cluster_name:
                    cluster_healthy = await cls._check_cluster_health(clickhouse_client, cluster_name)
                    if not cluster_healthy:
                        logger.warning(f"Cluster '{cluster_name}' is not healthy via node {dsn}")
                        await clickhouse_client.close()
                        continue

                # Apply metrics wrapper if enabled
                cls._apply_metrics_wrapper(clickhouse_client)

                logger.info(f"Successfully connected to ClickHouse cluster via {dsn}")
                logger.info(f"Connection configured with compression and extended timeouts")
                if cluster_name:
                    logger.info(f"Cluster '{cluster_name}' is healthy")

                return clickhouse_client

            except Exception as e:
                last_exception = e
                logger.warning(f"Failed to connect to ClickHouse cluster node {dsn}: {str(e)}")
                continue

        # If we get here, all connections failed
        logger.error(f"Failed to connect to any ClickHouse cluster node. Last error: {str(last_exception)}")
        return None

    @classmethod
    async def _check_cluster_health(cls, client: AsyncClient, cluster_name: str) -> bool:
        """Check if the ClickHouse cluster is healthy and accessible"""
        try:
            # Check if cluster exists and nodes are accessible
            result = await client.query(
                f"SELECT host_name, port, is_local, errors_count FROM system.clusters WHERE cluster = '{cluster_name}'"
            )

            if not result.result_rows:
                logger.warning(f"Cluster '{cluster_name}' not found in system.clusters")
                return False

            # Check cluster health and connection locality
            unhealthy_nodes = []
            local_nodes = []
            remote_nodes = []

            for row in result.result_rows:
                host_name, port, is_local, errors_count = row

                # Track local vs remote nodes
                if is_local == 1:
                    local_nodes.append(f"{host_name}:{port}")
                else:
                    remote_nodes.append(f"{host_name}:{port}")

                # Check for unhealthy nodes
                if errors_count > 10:  # Threshold for acceptable errors
                    unhealthy_nodes.append(f"{host_name}:{port} (errors: {errors_count})")

            # Log connection locality information
            if local_nodes:
                logger.debug(f"Cluster '{cluster_name}' - Connected via local nodes: {local_nodes}")
            else:
                logger.warning(f"Cluster '{cluster_name}' - No local nodes found! All connections are remote: {remote_nodes}")
                logger.warning("Consider checking local ClickHouse node availability for better performance")

            if remote_nodes:
                logger.debug(f"Cluster '{cluster_name}' - Remote nodes available: {remote_nodes}")

            # Report unhealthy nodes
            if unhealthy_nodes:
                logger.warning(f"Cluster '{cluster_name}' has unhealthy nodes: {unhealthy_nodes}")
                return False

            # Additional check: If we expect local connections but only have remote ones,
            # this might indicate a performance issue
            if not local_nodes and remote_nodes:
                logger.warning(f"Performance warning: Cluster '{cluster_name}' only has remote nodes accessible")
                logger.warning("This may impact query performance. Check local ClickHouse node status.")
                # Still return True as cluster is functional, just not optimal

            logger.debug(f"Cluster '{cluster_name}' is healthy with {len(result.result_rows)} nodes ({len(local_nodes)} local, {len(remote_nodes)} remote)")
            return True

        except Exception as e:
            logger.warning(f"Failed to check cluster health for '{cluster_name}': {str(e)}")
            # If we can't check cluster health due to permissions or other issues,
            # assume the connection is healthy since basic connectivity worked
            if "ACCESS_DENIED" in str(e) or "Not enough privileges" in str(e):
                logger.info(f"Insufficient privileges to check system.clusters, assuming cluster '{cluster_name}' is healthy")
                return True
            return False

    @classmethod
    def _apply_metrics_wrapper(cls, clickhouse_client: AsyncClient):
        """Apply metrics tracking wrapper to client methods if enabled"""
        if getattr(settings, "DB_METRICS_ENABLE", False):
            try:
                from stufio.db.metrics import track_clickhouse_query

                # Store original methods
                original_query = clickhouse_client.query
                original_insert = clickhouse_client.insert
                original_query_column_block_stream = clickhouse_client.query_column_block_stream
                original_query_row_block_stream = clickhouse_client.query_row_block_stream
                original_query_rows_stream = clickhouse_client.query_rows_stream
                original_raw_query = clickhouse_client.raw_query

                # Apply metrics tracking to all query methods
                clickhouse_client.query = track_clickhouse_query(original_query)
                clickhouse_client.insert = track_clickhouse_query(original_insert)
                clickhouse_client.query_column_block_stream = track_clickhouse_query(original_query_column_block_stream)
                clickhouse_client.query_row_block_stream = track_clickhouse_query(original_query_row_block_stream)
                clickhouse_client.query_rows_stream = track_clickhouse_query(original_query_rows_stream)
                clickhouse_client.raw_query = track_clickhouse_query(original_raw_query)

                logger.debug("ClickHouse client methods wrapped with metrics tracking")
            except ImportError:
                logger.debug("Metrics module not available, skipping ClickHouse metrics tracking")

    @classmethod
    async def check_connection_locality(cls) -> dict:
        """
        Check if current connection is local and return connectivity status.
        This can be used for health monitoring and alerting.
        """
        if not cls._instance or not cls._instance.clickhouse_client:
            return {"status": "no_connection", "is_local": None, "cluster_info": None}

        try:
            client = cls._instance.clickhouse_client
            cluster_name = settings.CLICKHOUSE_CLUSTER_NAME

            if not cluster_name:
                return {"status": "single_node", "is_local": None, "cluster_info": "no_cluster_configured"}

            # Get cluster information
            result = await client.query(
                f"SELECT host_name, port, is_local, errors_count FROM system.clusters WHERE cluster = '{cluster_name}'"
            )

            if not result.result_rows:
                return {"status": "cluster_not_found", "is_local": None, "cluster_info": f"cluster_{cluster_name}_not_found"}

            local_count = sum(1 for row in result.result_rows if row[2] == 1)  # is_local = 1
            total_count = len(result.result_rows)

            status_info = {
                "status": "connected",
                "is_local": local_count > 0,
                "cluster_info": {
                    "cluster_name": cluster_name,
                    "total_nodes": total_count,
                    "local_nodes": local_count,
                    "remote_nodes": total_count - local_count,
                    "has_local_connection": local_count > 0
                }
            }

            # Log warning if no local connections
            if local_count == 0:
                logger.warning(f"Performance Alert: No local ClickHouse nodes available in cluster '{cluster_name}'")
                logger.warning(f"All {total_count} connections are remote, which may impact performance")
                status_info["performance_warning"] = True

            return status_info

        except Exception as e:
            logger.error(f"Failed to check connection locality: {str(e)}")
            return {"status": "check_failed", "is_local": None, "cluster_info": str(e)}

    @classmethod
    async def cleanup(cls):
        """Cleanup connections and tasks - should be called during app shutdown"""
        logger.info("Cleaning up ClickHouse connections...")
        
        # Stop optimization task if running
        if cls._instance and cls._instance._optimization_task:
            try:
                cls._instance._optimization_task.cancel()
                await cls._instance._optimization_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug(f"Error cancelling optimization task: {str(e)}")
            finally:
                cls._instance._optimization_task = None

        # Close existing connection if it exists
        if cls._instance and cls._instance.clickhouse_client:
            try:
                await cls._instance.clickhouse_client.close()
                logger.info("ClickHouse client connection closed")
            except Exception as e:
                logger.debug(f"Error closing ClickHouse connection: {str(e)}")
            finally:
                cls._instance.clickhouse_client = None

        # Reset singleton
        cls._instance = None
        logger.info("ClickHouse cleanup completed")

    @classmethod
    async def force_reconnect(cls, reason: str = "manual_reconnect"):
        """
        Force reconnection to ClickHouse, useful when we detect suboptimal connections.
        This will reset the singleton and attempt to reconnect.
        """
        logger.info(f"Forcing ClickHouse reconnection - Reason: {reason}")

        # Stop optimization task if running
        if cls._instance and cls._instance._optimization_task:
            cls._instance._optimization_task.cancel()
            cls._instance._optimization_task = None

        # Close existing connection if it exists
        if cls._instance and cls._instance.clickhouse_client:
            try:
                await cls._instance.clickhouse_client.close()
            except Exception as e:
                logger.debug(f"Error closing existing connection: {str(e)}")

        # Reset singleton
        cls._instance = None

        # Trigger new connection
        try:
            await cls.get_instance()
            logger.info("ClickHouse reconnection successful")
            return True
        except Exception as e:
            logger.error(f"ClickHouse reconnection failed: {str(e)}")
            return False

    def _start_optimization_task(self):
        """Start the periodic connection optimization task"""
        if self._optimization_task and not self._optimization_task.done():
            return  # Task already running

        optimization_mode = getattr(settings, 'CLICKHOUSE_CONN_OPTIMIZE_MODE', 'adaptive')

        if optimization_mode == "adaptive":
            self._optimization_task = asyncio.create_task(self._adaptive_optimization_loop())
        else:  # periodic mode
            self._optimization_task = asyncio.create_task(self._periodic_optimization_loop())

        logger.debug(f"Started ClickHouse connection optimization task ({optimization_mode} mode)")

    async def _perform_single_optimization(self):
        """Perform a single optimization check and stop"""
        try:
            logger.debug("Performing single ClickHouse connection optimization check")
            optimization_needed = await self._perform_optimization_check()

            if optimization_needed:
                logger.info("ClickHouse single optimization completed with issues detected")
            else:
                logger.info("ClickHouse single optimization completed successfully")

        except Exception as e:
            logger.error(f"Error during single ClickHouse optimization: {str(e)}")

    async def _periodic_optimization_loop(self):
        """Simple periodic loop - checks at fixed intervals regardless of success/failure"""
        base_interval = self._optimization_interval_minutes * 60

        while True:
            try:
                await asyncio.sleep(base_interval)

                # Check if optimization is still enabled
                if not settings.CLICKHOUSE_CLUSTER_CONN_OPTIMIZE:
                    logger.info("ClickHouse connection optimization disabled, stopping task")
                    break

                # Perform optimization check (ignore return value in periodic mode)
                await self._perform_optimization_check()

            except asyncio.CancelledError:
                logger.debug("ClickHouse periodic optimization task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in ClickHouse periodic optimization loop: {str(e)}")
                await asyncio.sleep(60)  # Wait a minute before retrying

    async def _adaptive_optimization_loop(self):
        """Adaptive loop to check and optimize ClickHouse connections with smart intervals"""
        consecutive_failures = 0
        max_consecutive_failures = 3
        base_interval = self._optimization_interval_minutes * 60

        while True:
            try:
                # Dynamic interval based on success/failure
                if consecutive_failures == 0:
                    # If everything is working, check less frequently
                    interval = base_interval
                elif consecutive_failures <= max_consecutive_failures:
                    # If we're having issues, check more frequently initially
                    interval = base_interval // 2
                else:
                    # After several failures, check much less frequently to avoid spam
                    interval = base_interval * 4

                await asyncio.sleep(interval)

                # Check if optimization is still enabled
                if not settings.CLICKHOUSE_CLUSTER_CONN_OPTIMIZE:
                    logger.info("ClickHouse connection optimization disabled, stopping task")
                    break

                # Perform optimization check
                optimization_needed = await self._perform_optimization_check()

                if optimization_needed:
                    consecutive_failures += 1
                    if consecutive_failures > max_consecutive_failures:
                        logger.warning(f"ClickHouse optimization failed {consecutive_failures} times, reducing check frequency")
                else:
                    # Reset failure count on success
                    if consecutive_failures > 0:
                        logger.info("ClickHouse connection optimization working normally again")
                    consecutive_failures = 0

            except asyncio.CancelledError:
                logger.debug("ClickHouse adaptive optimization task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in ClickHouse adaptive optimization loop: {str(e)}")
                consecutive_failures += 1
                # Wait a minute before retrying on exceptions
                await asyncio.sleep(60)

    async def _perform_optimization_check(self) -> bool:
        """Check connection locality and reconnect if needed
        
        Returns:
            bool: True if optimization was needed (performance issue detected), False if all is well
        """
        try:
            self._last_optimization_check = datetime.now()

            # Check current connection status
            status = await self.check_connection_locality()

            if status.get("status") != "connected":
                logger.warning("ClickHouse connection check failed during optimization")
                return True  # Connection issue detected

            cluster_info = status.get("cluster_info", {})
            has_local_connection = cluster_info.get("has_local_connection", True)

            # If no local connections are available, try to reconnect
            if not has_local_connection:
                logger.info("No local ClickHouse connections detected, attempting to reconnect for optimization")

                # Try to reconnect
                success = await self.__class__.force_reconnect("optimization_no_local_nodes")

                if success:
                    # Check again after reconnection
                    new_status = await self.check_connection_locality()
                    new_cluster_info = new_status.get("cluster_info", {})
                    new_has_local = new_cluster_info.get("has_local_connection", True)

                    if new_has_local:
                        logger.info("✅ ClickHouse connection optimization successful - now using local nodes")
                        return False  # Successfully optimized
                    else:
                        logger.warning("⚠️ ClickHouse reconnection completed but still no local nodes available")
                        return True  # Still having issues
                else:
                    logger.error("❌ ClickHouse optimization reconnection failed")
                    return True  # Optimization failed
            else:
                logger.debug("ClickHouse connection optimization check passed - local connections available")
                return False  # No optimization needed

        except Exception as e:
            logger.error(f"Error during ClickHouse connection optimization: {str(e)}")
            return True  # Error occurred, consider it an issue


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
            
            # If we have a cluster, also verify cluster health
            if settings.CLICKHOUSE_CLUSTER_NAME:
                cluster_healthy = await _ClickhouseClientSingleton._check_cluster_health(
                    client, settings.CLICKHOUSE_CLUSTER_NAME
                )
                if not cluster_healthy:
                    raise ClickhouseConnectionError(f"Cluster '{settings.CLICKHOUSE_CLUSTER_NAME}' is not healthy")
            
            return True
        except (ClickhouseConnectionError, ClickHouseError) as e:
            await asyncio.sleep(0.1)
            if attempt == retries - 1:
                dsn_info = settings.CLICKHOUSE_DSN
                if settings.CLICKHOUSE_CLUSTER_DSN_LIST:
                    dsn_info = f"cluster with {len(settings.CLICKHOUSE_CLUSTER_DSN_LIST)} nodes"
                raise ClickhouseConnectionError(
                    f"Failed to ping Clickhouse after {retries} attempts: {str(e)} DSN: {dsn_info}"
                )
    return False


async def check_connection_locality() -> dict:
    """
    Check if current ClickHouse connection is local and return connectivity status.
    Useful for monitoring and performance optimization.
    
    Returns:
        dict: Connection status with locality information
    """
    return await _ClickhouseClientSingleton.check_connection_locality()


async def force_reconnect(reason: str = "manual_reconnect") -> bool:
    """
    Force reconnection to ClickHouse cluster.
    Useful when detecting suboptimal connections (e.g., only remote nodes available).
    
    Args:
        reason: Reason for reconnection (for logging)
        
    Returns:
        bool: True if reconnection successful, False otherwise
    """
    return await _ClickhouseClientSingleton.force_reconnect(reason)


async def get_optimization_status() -> dict:
    """
    Get current status of ClickHouse connection optimization.
    
    Returns:
        dict: Status information about optimization task and last check
    """
    if not _ClickhouseClientSingleton._instance:
        return {
            "optimization_enabled": settings.CLICKHOUSE_CLUSTER_CONN_OPTIMIZE,
            "optimization_running": False,
            "last_check": None,
            "status": "no_connection"
        }
    
    instance = _ClickhouseClientSingleton._instance
    task_running = instance._optimization_task is not None and not instance._optimization_task.done()
    
    return {
        "optimization_enabled": settings.CLICKHOUSE_CLUSTER_CONN_OPTIMIZE,
        "optimization_running": task_running,
        "last_check": instance._last_optimization_check.isoformat() if instance._last_optimization_check else None,
        "check_interval_minutes": instance._optimization_interval_minutes,
        "status": "connected"
    }


async def stop_optimization():
    """
    Stop the periodic optimization task.
    Useful for graceful shutdown.
    """
    if _ClickhouseClientSingleton._instance and _ClickhouseClientSingleton._instance._optimization_task:
        _ClickhouseClientSingleton._instance._optimization_task.cancel()
        _ClickhouseClientSingleton._instance._optimization_task = None
        logger.info("ClickHouse connection optimization stopped")


async def test_connection_pool(concurrent_requests: int = 5) -> dict:
    """
    Test ClickHouse connection pool under concurrent load.
    Useful for debugging pool exhaustion issues.
    
    Args:
        concurrent_requests: Number of concurrent requests to test with
        
    Returns:
        dict: Test results with timing and success/failure info
    """
    import time
    
    async def single_ping():
        """Single ping test"""
        start_time = time.time()
        try:
            client = await ClickhouseDatabase()
            await client.ping()
            return {"success": True, "duration": time.time() - start_time}
        except Exception as e:
            return {"success": False, "duration": time.time() - start_time, "error": str(e)}
    
    logger.info(f"Testing ClickHouse connection pool with {concurrent_requests} concurrent requests...")
    
    start_time = time.time()
    
    # Run concurrent pings
    tasks = [single_ping() for _ in range(concurrent_requests)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    total_time = time.time() - start_time
    
    # Analyze results
    successful = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
    failed = len(results) - successful
    
    durations = [r.get("duration", 0) for r in results if isinstance(r, dict)]
    avg_duration = sum(durations) / len(durations) if durations else 0
    
    test_results = {
        "connection_config": "simplified",
        "concurrent_requests": concurrent_requests,
        "total_time": total_time,
        "successful_requests": successful,
        "failed_requests": failed,
        "average_duration": avg_duration,
        "success_rate": successful / len(results) if results else 0
    }
    
    logger.info(f"Pool test completed: {successful}/{len(results)} successful, avg duration: {avg_duration:.3f}s")
    
    if failed > 0:
        logger.warning(f"Connection test found {failed} failures - consider reviewing ClickHouse connection configuration")
    
    return test_results


async def get_connection_pool_status() -> dict:
    """
    Get current ClickHouse connection pool status for monitoring.
    Useful for debugging connection pool issues.
    
    Returns:
        dict: Pool status information
    """
    if not _ClickhouseClientSingleton._instance or not _ClickhouseClientSingleton._instance.clickhouse_client:
        return {
            "status": "no_connection",
            "pool_configured": False,
            "connection_type": None
        }
    
    try:
        client = _ClickhouseClientSingleton._instance.clickhouse_client
        
        # Get basic connection configuration
        pool_info = {
            "status": "connected",
            "pool_configured": "simplified",
            "connection_type": "default_pooling",
            "client_name": getattr(client, 'client_name', 'unknown')
        }
        
        # Test connection to verify pool is working
        await client.ping()
        pool_info["connection_test"] = "success"
        
        return pool_info
        
    except Exception as e:
        logger.error(f"Error getting connection pool status: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "pool_configured": "simplified",
            "connection_type": "default_pooling",
            "connection_test": "failed"
        }


async def cleanup():
    """
    Cleanup ClickHouse connections and tasks.
    Should be called during application shutdown.
    """
    await _ClickhouseClientSingleton.cleanup()


__all__ = [
    "ClickhouseDatabase", 
    "ping", 
    "ClickhouseConnectionError", 
    "get_database_from_dsn",
    "check_connection_locality",
    "force_reconnect",
    "get_optimization_status",
    "stop_optimization",
    "get_connection_pool_status",
    "test_connection_pool",
    "cleanup"
]
