import logging
from contextlib import asynccontextmanager

from analysis_manager.config import get_settings
from commons.db.v2 import dispose_session_manager, init_session_manager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    """FastAPI lifespan context manager for Session v2 initialization."""
    settings = get_settings()

    logger.info("Initializing AsyncSessionManager with profile: %s", settings.DB_PROFILE)

    # Initialize the session manager using commons lifecycle
    init_session_manager(settings, app_name="analysis_manager")

    try:
        yield
    finally:
        logger.info("Disposing AsyncSessionManager engine")
        await dispose_session_manager()
