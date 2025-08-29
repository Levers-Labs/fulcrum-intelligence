"""Database utilities for query_manager scripts."""

from contextlib import asynccontextmanager
from functools import wraps

from commons.db.v2 import dispose_session_manager, get_session_manager, init_session_manager
from query_manager.config import get_settings


@asynccontextmanager
async def session_ctx():
    """Context manager for script database sessions."""
    init_session_manager(get_settings(), app_name="query_manager_script")
    try:
        session_manager = get_session_manager()
        async with session_manager.session() as session:
            yield session
    finally:
        await dispose_session_manager()


def async_db_session():
    """Decorator for async functions that need a database session."""

    def wrapper(fn):
        @wraps(fn)
        async def inner(*args, **kwargs):
            async with session_ctx() as session:
                return await fn(session, *args, **kwargs)

        return inner

    return wrapper
