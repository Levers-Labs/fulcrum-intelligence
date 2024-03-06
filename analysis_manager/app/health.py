from fastapi import APIRouter, Response, status
from pydantic import BaseModel
from tortoise.exceptions import DBConnectionError

from app.utilities.logger import logger

router = APIRouter(prefix="/health")


class APIHealth(BaseModel):
    database_is_online: bool = True


@router.get(
    "/",
    response_model=APIHealth,
    responses={
        503: {"description": "Some or all services are unavailable", "model": APIHealth}
    },
)
async def check_health(response: Response):
    """Check availability of several's service to get an idea of the api health."""
    logger.info("Health Check⛑")
    health = APIHealth()

    # database check
    try:
        # list(await User.all())
        pass
    except DBConnectionError:
        health.database_is_online = False
        logger.exception("Database connection failed")

    if not all(health.dict().values()):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return health
