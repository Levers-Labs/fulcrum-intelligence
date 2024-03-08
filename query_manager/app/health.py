import logging

from fastapi import APIRouter, Response, status
from pydantic import BaseModel

router = APIRouter(prefix="/health")
logger = logging.getLogger(__name__)


class APIHealth(BaseModel):
    graph_api_is_online: bool = True


@router.get(
    "",
    response_model=APIHealth,
    responses={
        503: {"description": "Some or all services are unavailable", "model": APIHealth}
    },
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
        logger.exception("Connection failed: %s", e)

    if not all(health.dict().values()):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return health
