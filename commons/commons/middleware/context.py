import logging
from collections.abc import Callable
from typing import Any, TypeVar

from fastapi import Request, Response

from commons.utilities.context import set_tenant_id

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


async def set_context_middleware(request: Request, call_next: F) -> Response:
    """
    clears tenant id after each request
    """
    if "tenant_id" in request.headers and request.headers["tenant_id"]:
        logger.info(f"Setting the tenant id {request.headers['tenant_id']}")
        request.state.tenant_id = int(request.headers["tenant_id"])

    response: Response = await call_next(request)
    logger.info("clearing context var of tenant_id")
    set_tenant_id("")

    return response
