import redis.asyncio as redis
from redis.asyncio.cluster import ClusterNode
from typing import Optional, Any, Union, List, Dict, Callable
import inspect
from functools import wraps
from stufio.core.config import get_settings
import logging
import asyncio

settings = get_settings()
logger = logging.getLogger(__name__)

class RedisConnectionError(Exception):
    """Raised when Redis connection fails"""
    pass

class PrefixedRedisClient:
    """
    Redis client wrapper that automatically prefixes all keys with settings.REDIS_PREFIX
    """
    def __init__(self, client: Union[redis.Redis, redis.RedisCluster], prefix: str):
        self._client = client
        self._prefix = prefix
        
        # Apply metrics tracking if enabled
        if getattr(settings, "DB_METRICS_ENABLE", False):
            try:
                from stufio.db.metrics import track_redis_operation
                # List of methods to wrap with metrics tracking
                for method_name in dir(self._client):
                    # Skip private methods and non-callable attributes
                    if method_name.startswith('_') or not callable(getattr(self._client, method_name)):
                        continue
                    
                    # Only wrap coroutine functions
                    method = getattr(self._client, method_name)
                    if asyncio.iscoroutinefunction(method):
                        setattr(self._client, method_name, track_redis_operation(method))
                
                logger.debug("Redis client methods wrapped with metrics tracking")
            except ImportError:
                logger.debug("Metrics module not available, skipping Redis metrics tracking")
            except Exception as e:
                logger.error(f"Error setting up Redis metrics tracking: {e}")

    def _prefix_key(self, key: str) -> str:
        """Add prefix to a key if not already prefixed"""
        if key.startswith(self._prefix):
            return key
        return f"{self._prefix}{key}"
        
    def _prefix_keys(self, keys: List[str]) -> List[str]:
        """Add prefix to multiple keys"""
        return [self._prefix_key(key) for key in keys]
    
    def _prefix_dict(self, mapping: Dict[str, Any]) -> Dict[str, Any]:
        """Add prefix to dictionary keys"""
        return {self._prefix_key(k): v for k, v in mapping.items()}

    def __getattr__(self, name):
        """
        Dynamically intercept Redis commands to add prefixing
        """
        attr = getattr(self._client, name)
        
        # If not callable (e.g., a property), return directly
        if not callable(attr):
            return attr
            
        @wraps(attr)
        async def wrapped(*args, **kwargs):
            # Get the signature of the method
            sig = inspect.signature(attr)
            param_names = [p.name for p in sig.parameters.values()]
            
            # Handle common Redis commands with key as first argument
            if args and isinstance(args[0], str) and args and len(param_names) > 0 and param_names[0] in ['key', 'name']:
                args = list(args)
                args[0] = self._prefix_key(args[0])
                
            # Handle commands with multiple keys
            elif args and isinstance(args[0], (list, tuple)) and len(param_names) > 0 and param_names[0] in ['keys', 'names']:
                args = list(args)
                args[0] = self._prefix_keys(args[0])
                
            # Handle commands with key-value mappings
            elif 'mapping' in kwargs and isinstance(kwargs['mapping'], dict):
                kwargs['mapping'] = self._prefix_dict(kwargs['mapping'])
                
            # Handle specific commands
            if name == 'mget':
                if args:
                    args = [self._prefix_keys(args[0])]
            elif name == 'scan_iter':
                if 'match' in kwargs and kwargs['match'] is not None:
                    kwargs['match'] = self._prefix_key(kwargs['match'])

            # Execute the Redis command
            return await attr(*args, **kwargs)
        
        return wrapped

class _RedisClientSingleton:
    """Singleton for Redis client"""
    instance = None
    redis_client: Optional[PrefixedRedisClient] = None

    def __new__(cls):
        if not cls.instance:
            cls.instance = super(_RedisClientSingleton, cls).__new__(cls)
            
            # Check if we should use Redis Cluster mode
            redis_cluster_nodes = getattr(settings, 'REDIS_CLUSTER_NODES', None)
            
            if redis_cluster_nodes:
                logger.info(f"Initializing Redis Cluster client with nodes: {redis_cluster_nodes}")
                try:
                    # Parse cluster nodes and create startup nodes list
                    startup_nodes = []
                    for node in redis_cluster_nodes.split(','):
                        host, port = node.strip().split(':')
                        startup_nodes.append(ClusterNode(host, int(port)))
                    
                    # Create Redis cluster client
                    raw_client = redis.RedisCluster(startup_nodes=startup_nodes, decode_responses=True)  # type: ignore
                    logger.info(f"Redis Cluster client initialized successfully")
                except Exception as e:
                    logger.error(f"Failed to create Redis Cluster: {e}. Falling back to single Redis client.")
                    # Fallback to single Redis client
                    raw_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
            else:
                logger.info("Initializing single Redis client")
                raw_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
            
            # Wrap the raw client with our prefixing wrapper
            cls.instance.redis_client = PrefixedRedisClient(raw_client, settings.REDIS_PREFIX)
        return cls.instance

async def RedisClient() -> PrefixedRedisClient:
    """Get Redis client instance with auto-prefixing"""
    singleton = _RedisClientSingleton()
    if singleton.redis_client is None:
        raise RedisConnectionError("Redis client not initialized")
    return singleton.redis_client

async def ping(retries: int = 3) -> bool:
    """Ping Redis server with retries"""
    for attempt in range(retries):
        try:
            client = await RedisClient()
            return await client._client.ping()  # Access the underlying client for ping
        except Exception as e:
            if attempt == retries - 1:
                raise RedisConnectionError(
                    f"Failed to ping Redis after {retries} attempts: {str(e)}"
                )
    return False

__all__ = ["RedisClient", "ping", "RedisConnectionError", "PrefixedRedisClient"]