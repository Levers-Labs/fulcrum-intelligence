from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from analysis_manager.config import settings
from analysis_manager.core.routes import router as core_router, user_router
from analysis_manager.health import router as health_check_router
from analysis_manager.utilities.logger import setup_rich_logger
from analysis_manager.utilities.middleware import process_time_log_middleware


def get_application() -> FastAPI:
    _app = FastAPI(
        title="Analysis Manager",
        description="Analysis Manager for Fulcrum Intelligence",
        debug=settings.DEBUG,
    )
    _app.include_router(core_router, prefix="/v1")
    _app.include_router(user_router, prefix="/v1")
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
