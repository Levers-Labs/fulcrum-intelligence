import logging
from collections.abc import Callable
from typing import Any, TypeVar

from fastapi import Request, Response

from commons.utilities.context import reset_context, set_tenant_id

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


async def context_middleware(request: Request, call_next: F) -> Response:
    """
    clears tenant id after each request
    """
    if "X-Tenant-Id" in request.headers and request.headers["X-Tenant-Id"]:
        logger.info(f"Setting the tenant id {request.headers['X-Tenant-Id']}")
        set_tenant_id(request.headers["X-Tenant-Id"])

    response: Response = await call_next(request)
    logger.info("clearing context var of tenant_id")
    reset_context()

    return response
