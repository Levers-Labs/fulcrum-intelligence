import logging
from collections.abc import Callable
from typing import Any, TypeVar

from fastapi import Request, Response

from commons.utilities.context import set_tenant_id

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


async def clear_tenant_id_middleware(request: Request, call_next: F) -> Response:
    """
    clears tenant id after each request
    """
    response: Response = await call_next(request)
    set_tenant_id("")

    return response
