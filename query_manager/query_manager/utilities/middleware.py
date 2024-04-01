import logging
import time
import uuid
from collections.abc import Callable
from typing import Any, TypeVar

from fastapi import Request, Response

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


async def process_time_log_middleware(request: Request, call_next: F) -> Response:
    """
    Add API process time in response headers and log calls
    """
    start_time = time.time()
    response: Response = await call_next(request)
    process_time = str(round(time.time() - start_time, 3))
    response.headers["X-Process-Time"] = process_time

    logger.info(
        "Method=%s Path=%s StatusCode=%s ProcessTime=%s",
        request.method,
        request.url.path,
        response.status_code,
        process_time,
    )

    return response


async def request_id_middleware(request: Request, call_next: F) -> Response:
    """
    Add a unique request id to the request headers
    """
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    response: Response = await call_next(request)
    response.headers["X-Request-ID"] = request_id

    return response
