from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from query_manager.config import get_settings
from query_manager.core.routes import router as core_router
from query_manager.health import router as health_check_router
from query_manager.utilities.logger import setup_rich_logger
from query_manager.utilities.middleware import process_time_log_middleware


def get_application() -> FastAPI:
    settings = get_settings()

    _app = FastAPI(
        title="Query Manager",
        description="Query Manager for Fulcrum Intelligence",
        debug=settings.DEBUG,
    )
    _app.include_router(core_router, prefix="/v1")
    _app.include_router(health_check_router, prefix="/v1")
    _app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # add process time log middleware
    _app.add_middleware(BaseHTTPMiddleware, dispatch=process_time_log_middleware)

    # setup logging
    setup_rich_logger()

    return _app


app = get_application()
