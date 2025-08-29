import logging

import httpx
from fastapi import (
    APIRouter,
    Response,
    Security,
    status,
)
from pydantic import BaseModel
from sqlalchemy import select

from analysis_manager.config import get_settings
from analysis_manager.core.dependencies import oauth2_auth
from analysis_manager.db.config import AsyncSessionDep, AsyncSessionManagerDep
from commons.auth.scopes import ADMIN_READ

router = APIRouter()
logger = logging.getLogger(__name__)


class APIHealth(BaseModel):
    query_manager_is_online: bool = True


class DatabaseStats(BaseModel):
    """Database connection stats."""

    active_sessions: int
    database_is_online: bool


@router.get(
    "/health",
    response_model=APIHealth,
    responses={503: {"description": "Some or all services are unavailable", "model": APIHealth}},
)
async def check_health(response: Response):
    """Check availability of several's service to get an idea of the api health."""
    logger.info("Health Check⛑")
    health = APIHealth()
    settings = get_settings()

    # check if query_manager is online
    async with httpx.AsyncClient() as client:
        try:
            url = f"{str(settings.QUERY_MANAGER_SERVER_HOST).rstrip('/')}/v1/health"
            res = await client.get(url)
            res.raise_for_status()
        except Exception as e:
            health.query_manager_is_online = False
            logger.exception("Query Manager is offline: %s", e)

    if not all(health.dict().values()):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return health


@router.get(
    "/db-stats",
    response_model=DatabaseStats,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_READ])],  # type: ignore
)
async def get_database_stats(mgr: AsyncSessionManagerDep, session: AsyncSessionDep):
    """Get database statistics including active sessions count. Protected endpoint."""
    logger.info("Database Stats Request")
    stats = DatabaseStats(active_sessions=0, database_is_online=True)

    # Get active session count for observability
    try:
        stats.active_sessions = mgr.current_session_count
        logger.debug("Active DB sessions: %d", stats.active_sessions)
    except Exception as e:
        logger.exception("Failed to get session count: %s", e)
        stats.active_sessions = -1  # Indicate error in getting count

    # database check
    try:
        await session.exec(select(1))  # type:ignore
    except Exception as e:
        stats.database_is_online = False
        logger.exception("Database connection failed: %s", e)

    return stats
