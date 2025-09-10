import logging
from contextlib import asynccontextmanager

from commons.cache import close_cache, init_cache
from commons.db.v2 import dispose_session_manager, init_session_manager
from insights_backend.config import get_settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    """FastAPI lifespan context manager for DB sessions and caching."""
    settings = get_settings()

    # ---------- Database session manager ----------
    logger.info("Initializing AsyncSessionManager with profile: %s", settings.DB_PROFILE)
    init_session_manager(settings, app_name="insights-backend")

    # ---------- Cache initialization ----------
    await init_cache(settings)

    try:
        yield
    finally:
        # ---------- Graceful shutdown ----------
        logger.info("Disposing AsyncSessionManager engine")
        await dispose_session_manager()
        await close_cache()
