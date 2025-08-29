"""Database utilities for scripts using Session Manager v2."""

import logging
from contextlib import asynccontextmanager

from analysis_manager.config import get_settings
from commons.db.v2 import AsyncSessionManager, build_engine_options
from commons.db.v2.engine import dispose_async_engine

logger = logging.getLogger(__name__)


@asynccontextmanager
async def script_session_manager():
    """Context manager for scripts to get a properly configured session manager."""
    settings = get_settings()

    # Build engine options
    engine_options = build_engine_options(
        profile=settings.DB_PROFILE,
        overrides=settings.SQLALCHEMY_ENGINE_OPTIONS,
        app_name="analysis-manager-script",
    )

    # Create session manager
    session_manager = AsyncSessionManager(
        database_url=settings.DATABASE_URL,
        engine_options=engine_options,
        max_concurrent_sessions=settings.DB_MAX_CONCURRENT_SESSIONS,
    )

    logger.info("Initialized session manager for script")

    try:
        yield session_manager
    finally:
        logger.info("Disposing session manager engine")
        await dispose_async_engine(settings.DATABASE_URL, engine_options)


@asynccontextmanager
async def script_session():
    """Context manager for scripts to get a database session directly."""
    async with script_session_manager() as manager:
        async with manager.session() as session:
            yield session
