import asyncio
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

T = TypeVar("T", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


def retry(
    retries: int = 3, delay: float = 1.0, exceptions: tuple[type[Exception], ...] = (Exception,)
) -> Callable[[T], T]:
    """Decorator to retry a function on specified exceptions."""

    def decorator(func: T) -> T:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            for attempt in range(retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    logger.warning("Retrying %s due to %s: attempt %d/%d", func.__name__, str(e), attempt + 1, retries)
                    await asyncio.sleep(delay)
            if last_exception is not None:
                raise last_exception

        return wrapper  # type: ignore

    return decorator
