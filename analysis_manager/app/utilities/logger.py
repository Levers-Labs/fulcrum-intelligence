import logging
import sys
from functools import lru_cache
from typing import Union

from pydantic import BaseModel

from app.config import Environment, settings

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
LOGGER_FORMAT = "%(name)s | %(levelname)s | %(asctime)s | %(filename)s | %(funcName)s:%(lineno)d | %(message)s"



class LoggerConfig(BaseModel):
    handlers: list
    format: str
    date_format: Union[str, None] = None
    level: Union[str, int] = logging.INFO


@lru_cache
def get_logger_config():
    """Installs RichHandler (Rich library) if not in production
    environment, or use the production log configuration.
    """

    if not settings.ENV == Environment.prod:
        from rich.logging import RichHandler

        return LoggerConfig(
            handlers=[
                RichHandler(
                    rich_tracebacks=True, tracebacks_show_locals=True, show_time=False
                ),
            ],
            format=LOGGER_FORMAT,
            date_format=DATE_FORMAT,
            level=settings.LOGGING_LEVEL,
        )

    handler_format = logging.Formatter(LOGGER_FORMAT, datefmt=DATE_FORMAT)

    # Stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(handler_format)

    return LoggerConfig(
        handlers=[stdout_handler],
        format=LOGGER_FORMAT,
        date_format=DATE_FORMAT,
        level=settings.LOGGING_LEVEL,
    )


def setup_rich_logger():
    """Cycles through uvicorn root loggers to
    remove handler, then runs `get_logger_config()`
    to populate the `LoggerConfig` class with Rich
    logger parameters.
    """

    # Remove all handlers from root logger
    # and propagate to root logger.
    for name in logging.root.manager.loggerDict.keys():
        logging.getLogger(name).handlers = []
        logging.getLogger(name).propagate = True

    logger_config = get_logger_config()  # get Rich logging config

    logging.basicConfig(
        level=logger_config.level,
        format=logger_config.format,
        datefmt=logger_config.date_format,
        handlers=logger_config.handlers,
    )
