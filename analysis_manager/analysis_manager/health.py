import logging

import httpx
from fastapi import APIRouter, Response, status
from pydantic import BaseModel

from analysis_manager.config import get_settings

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
