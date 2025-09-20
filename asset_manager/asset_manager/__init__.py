"""Asset Manager - Dagster-based data orchestration service."""

from __future__ import annotations

import sentry_sdk

from asset_manager.resources.config import Settings

__all__ = ["__version__"]
__version__ = "0.1.0"

# ──────────────────────────────
# Sentry initialization
# covers:
#   • dagster CLI (daemon, sensors)
#   • `run.py` local runners
#   • any ad-hoc scripts importing
#     asset_manager code
# ──────────────────────────────
settings = Settings()  # type: ignore
if settings.sentry_dsn:
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        traces_sample_rate=1.0,
    )
