import logging

from fastapi import (
    APIRouter,
    Response,
    Security,
    status,
)
from pydantic import BaseModel
from sqlalchemy import select

from commons.auth.scopes import ADMIN_READ
from query_manager.core.dependencies import oauth2_auth
from query_manager.db.config import AsyncSessionDep, AsyncSessionManagerDep

router = APIRouter(prefix="/health")
logger = logging.getLogger(__name__)


class APIHealth(BaseModel):
    graph_api_is_online: bool = True
    cube_api_is_online: bool = True


class DatabaseStats(BaseModel):
    """Database connection stats."""

    active_sessions: int
    database_is_online: bool


@router.get(
    "",
    response_model=APIHealth,
    responses={503: {"description": "Some or all services are unavailable", "model": APIHealth}},
)
async def check_health(response: Response):
    """Check availability of several's service to get an idea of the api health."""
    logger.info("Health Check")
    health = APIHealth()

    try:
        # todo: check metric graph api availability
        pass
    except Exception as e:
        health.graph_api_is_online = False
        logger.exception("Graph API Connection failed: %s", e)

    try:
        # todo: check cube api availability
        pass
    except Exception as e:
        health.cube_api_is_online = False
        logger.exception("Cube API Connection failed: %s", e)

    if not all(health.model_dump(mode="json").values()):
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
