from typing import Any

from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import create_engine

# Global variables to hold engine instances
engine: Engine | None = None
async_engine: AsyncEngine | None = None


def get_engine(database_url: str, options: dict[str, Any] | None = None) -> Engine:
    global engine
    if engine is None:
        options = options or {}
        engine = create_engine(database_url, **options)
    return engine


def get_async_engine(database_url: str, options: dict[str, Any] | None = None) -> AsyncEngine:
    global async_engine
    if async_engine is None:
        options = options or {}
        async_engine = create_async_engine(database_url, **options)
    return async_engine
