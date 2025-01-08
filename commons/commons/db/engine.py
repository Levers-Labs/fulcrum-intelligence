from typing import Any

from sqlalchemy import (
    Engine,
    NullPool,
    QueuePool,
    StaticPool,
)
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import create_engine

# Add pool class mapping
POOL_CLASS_MAPPING = {
    "NullPool": NullPool,
    "QueuePool": QueuePool,
    "StaticPool": StaticPool,
}


def resolve_pool_class(options: dict[str, Any]) -> dict[str, Any]:
    """Convert poolclass string to actual SQLAlchemy pool class"""
    if "poolclass" in options and isinstance(options["poolclass"], str):
        pool_class_name = options["poolclass"]
        options["poolclass"] = POOL_CLASS_MAPPING.get(pool_class_name)
    return options


# Global variables to hold engine instances
engine: Engine | None = None
async_engine: AsyncEngine | None = None


def get_engine(database_url: str, options: dict[str, Any] | None = None) -> Engine:
    global engine
    if engine is None:
        options = options or {}
        options = resolve_pool_class(options)
        engine = create_engine(database_url, **options)
    return engine


def get_async_engine(database_url: str, options: dict[str, Any] | None = None) -> AsyncEngine:
    global async_engine
    if async_engine is None:
        options = options or {}
        options = resolve_pool_class(options)
        async_engine = create_async_engine(database_url, **options)
    return async_engine
