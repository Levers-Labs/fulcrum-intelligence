import logging
from collections.abc import Callable
from typing import Any, TypeVar

from fastapi import Request, Response

from commons.auth.constants import TENANT_ID_HEADER, TENANT_VERIFICATION_BYPASS_ENDPOINTS
from commons.utilities.context import reset_context, set_tenant_id

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


async def tenant_context_middleware(request: Request, call_next: F) -> Response:
    """
    Sets the tenant id in the context
    Processes the request
    Resets the tenant id in the context
    """

    # Check if the request path matches any of the bypass endpoints
    if any(endpoint in request.url.path for endpoint in TENANT_VERIFICATION_BYPASS_ENDPOINTS):
        return await call_next(request)  # Skip tenant context setting

    # Set the tenant id in the context
    tenant_id_str = request.headers.get(TENANT_ID_HEADER)
    # convert the tenant id to int
    tenant_id = int(tenant_id_str) if tenant_id_str and tenant_id_str.isdigit() else None
    if tenant_id:
        logger.info("Setting the tenant id in the context: %s", tenant_id)
        set_tenant_id(tenant_id)

    # Process the request
    response: Response = await call_next(request)

    # Reset the tenant id from the context
    logger.info("Resetting the tenant id from the context")
    reset_context()

    return response
