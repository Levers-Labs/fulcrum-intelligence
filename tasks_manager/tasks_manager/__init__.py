"""Tasks Manager - Prefect orchestration service."""

import logging
import os

import sentry_sdk

# Simple Sentry initialization
try:
    sentry_dsn = os.getenv("SENTRY_DSN")
    if sentry_dsn:
        sentry_sdk.init(
            dsn=sentry_dsn,
            traces_sample_rate=1.0,
            environment=os.getenv("ENV", "dev"),
        )
        logging.getLogger(__name__).info("Sentry initialized for Tasks Manager")
except ImportError:
    logging.getLogger(__name__).warning("Sentry SDK not available")
except Exception as e:
    logging.getLogger(__name__).error(f"Failed to initialize Sentry: {e}")

from .flows.alerts import *  # noqa
