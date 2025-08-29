from __future__ import annotations

import logging
from typing import Any

from pydantic_settings import BaseSettings

from commons.db.v2 import AsyncSessionManager, build_engine_options, dispose_async_engine

logger = logging.getLogger(__name__)

_manager: AsyncSessionManager | None = None
_engine_opts: dict[str, Any] | None = None
_db_url: str | None = None


def init_session_manager(settings: BaseSettings, app_name: str | None = None) -> AsyncSessionManager:
    """
    Initialize (or return) a global AsyncSessionManager using attributes of *settings*.

    Expected attributes on settings:
        DATABASE_URL: str
        SQLALCHEMY_ENGINE_OPTIONS: dict | None
        DB_PROFILE: str | None
        DB_MAX_CONCURRENT_SESSIONS: int | None

    Args:
        settings: Service settings object containing database configuration
        app_name: Name of the application.

    Returns:
        AsyncSessionManager: The global session manager instance

    Raises:
        ValueError: If called with different DATABASE_URL than previous initialization
    """
    global _manager, _engine_opts, _db_url

    # raise error if database url is not set
    if not hasattr(settings, "DATABASE_URL"):
        raise ValueError("DATABASE_URL is available in settings")

    # raise error if database url is different from previous initialization
    if _manager:  # already initialized
        if settings.DATABASE_URL != _db_url:
            raise ValueError(
                "init_session_manager() called with different DATABASE_URL "
                f"(prev={_db_url}, new={settings.DATABASE_URL})"
            )
        return _manager

    # Build engine options
    _engine_opts = build_engine_options(
        profile=getattr(settings, "DB_PROFILE", None) or "dev",
        overrides=getattr(settings, "SQLALCHEMY_ENGINE_OPTIONS", None),
        app_name=app_name,
    )

    # Set global session manager
    _manager = AsyncSessionManager(
        database_url=settings.DATABASE_URL,
        engine_options=_engine_opts,
        max_concurrent_sessions=getattr(settings, "DB_MAX_CONCURRENT_SESSIONS", None),
    )
    # Set global database url
    _db_url = settings.DATABASE_URL

    logger.info(
        "Global AsyncSessionManager initialized for %s: pool_size=%s max_overflow=%s",
        app_name,
        _engine_opts.get("pool_size"),
        _engine_opts.get("max_overflow"),
    )
    return _manager


def get_session_manager() -> AsyncSessionManager:
    """
    Get the global session manager instance.

    Returns:
        AsyncSessionManager: The global session manager instance

    Raises:
        AssertionError: If session manager not initialized
    """
    assert _manager is not None, "Session manager not initialized. Call init_session_manager() first."
    return _manager


async def dispose_session_manager() -> None:
    """Dispose the engine and reset global state (for lifespan shutdown or tests)."""
    global _manager, _engine_opts, _db_url
    if _manager and _engine_opts and _db_url:
        await dispose_async_engine(_db_url, _engine_opts)
    _manager = _engine_opts = _db_url = None
    logger.info("Global AsyncSessionManager disposed")
