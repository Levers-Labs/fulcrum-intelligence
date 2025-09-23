import logging

from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.backends.redis import RedisBackend
from pydantic_settings import BaseSettings
from redis import asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnErr

logger = logging.getLogger(__name__)


async def init_cache(cfg: BaseSettings) -> None:
    """Initialize cache backend with Redis fallback to InMemory."""

    redis_url = getattr(cfg, "REDIS_URL", None)
    prefix = getattr(cfg, "CACHE_PREFIX", "cache")

    if redis_url:
        try:
            redis = aioredis.from_url(redis_url, max_connections=20)
            await redis.ping()  # connectivity check
            FastAPICache.init(RedisBackend(redis), prefix=prefix)
            logger.info("FastAPICache initialized with Redis backend")
            return
        except (RedisConnErr, Exception) as err:
            logger.error("Redis unavailable (%s); falling back to InMemoryBackend", err)

    # Fallback: no REDIS_URL or Redis connection failed
    FastAPICache.init(InMemoryBackend(), prefix=prefix)
    logger.info("FastAPICache initialized with InMemoryBackend")


async def close_cache() -> None:
    """Close cache backend connections."""
    try:
        backend = FastAPICache.get_backend()
        if isinstance(backend, RedisBackend):
            await backend.redis.close()
            logger.info("Redis cache backend closed")
    except Exception as e:
        logger.warning("Error closing cache backend: %s", e)


async def invalidate_namespace(namespace: str) -> None:
    """Clear all cached items under a namespace."""
    await FastAPICache.clear(namespace=namespace)
