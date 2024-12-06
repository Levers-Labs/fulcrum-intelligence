import json
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

T = TypeVar("T", bound=Callable[..., Any])

logger = logging.getLogger(__name__)


def log_llm_request(func: T) -> T:
    """Decorator to log LLM requests and responses."""

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Extract relevant information
        service_name = args[0].__class__.__name__ if args else "UnknownService"

        # Log request
        logger.info(
            "LLM Request - Service: %s, Args: %s, Kwargs: %s",
            service_name,
            json.dumps(args[1:], default=str),
            json.dumps(kwargs, default=str),
        )

        try:
            result = await func(*args, **kwargs)

            # Log success
            logger.info("LLM Response - Service: %s, Status: success", service_name)

            return result

        except Exception as e:
            # Log error
            logger.error("LLM Error - Service: %s, Error: %s", service_name, str(e), exc_info=True)
            raise

    return wrapper  # type: ignore
