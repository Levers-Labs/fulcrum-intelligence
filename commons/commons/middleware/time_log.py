import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

from fastapi import Request, Response

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


async def process_time_log_middleware(request: Request, call_next: F):
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
