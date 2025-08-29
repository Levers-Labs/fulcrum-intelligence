import logging
from contextlib import asynccontextmanager

from analysis_manager.config import get_settings
from commons.db.v2 import AsyncSessionManager, build_engine_options
from commons.db.v2.engine import dispose_async_engine

logger = logging.getLogger(__name__)

_manager: AsyncSessionManager | None = None


def get_session_manager() -> AsyncSessionManager:
    """Get the global session manager instance."""
    assert _manager is not None, "Session manager not initialized"
    return _manager


@asynccontextmanager
async def lifespan(app):
    """FastAPI lifespan context manager for Session v2 initialization."""
    global _manager
    settings = get_settings()

    logger.info("Initializing AsyncSessionManager with profile: %s", settings.DB_PROFILE)

    # Build engine options with LARGE profile and existing overrides
    engine_opts = build_engine_options(
        profile=settings.DB_PROFILE,  # "large"
        overrides=settings.SQLALCHEMY_ENGINE_OPTIONS,
        app_name="analysis_manager",
    )

    # Initialize the session manager
    _manager = AsyncSessionManager(
        database_url=settings.DATABASE_URL,
        engine_options=engine_opts,
        max_concurrent_sessions=settings.DB_MAX_CONCURRENT_SESSIONS,
    )

    logger.info(
        "Session manager initialized - max_concurrent_sessions: %d, pool_size: %d, max_overflow: %d",
        settings.DB_MAX_CONCURRENT_SESSIONS,
        engine_opts.get("pool_size", 0),
        engine_opts.get("max_overflow", 0),
    )

    try:
        yield
    finally:
        logger.info("Disposing AsyncSessionManager engine")
        await dispose_async_engine(settings.DATABASE_URL, engine_opts)
        _manager = None
