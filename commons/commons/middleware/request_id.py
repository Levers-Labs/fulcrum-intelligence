import logging
import uuid
from collections.abc import Callable
from typing import Any, TypeVar

from fastapi import Request, Response

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


async def request_id_middleware(request: Request, call_next: F) -> Response:
    """
    Add a unique request id to the request headers
    """
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    response: Response = await call_next(request)
    response.headers["X-Request-ID"] = request_id

    return response
