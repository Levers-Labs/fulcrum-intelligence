import logging

from fastapi import APIRouter, Response, status
from pydantic import BaseModel
from sqlalchemy import select

from insights_backend.db.config import AsyncSessionDep

router = APIRouter()
logger = logging.getLogger(__name__)


class APIHealth(BaseModel):
    database_is_online: bool = True


@router.get(
    "/health",
    response_model=APIHealth,
    responses={503: {"description": "Some or all services are unavailable", "model": APIHealth}},
)
async def check_health(response: Response, session: AsyncSessionDep):
    """Check availability of several's service to get an idea of the api health."""
    logger.info("Health Check⛑")
    health = APIHealth()

    # database check
    try:
        await session.execute(select(1))
    except Exception as e:
        health.database_is_online = False
        logger.exception("Database connection failed: %s", e)

    if not all(health.dict().values()):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return health
