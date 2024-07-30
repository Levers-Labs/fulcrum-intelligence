from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from analysis_manager.config import settings
from analysis_manager.core.routes import router as core_router, user_router
from analysis_manager.health import router as health_check_router
from commons.middleware import process_time_log_middleware, request_id_middleware
from commons.utilities.docs import setup_swagger_ui
from commons.utilities.logger import setup_rich_logger


def get_application() -> FastAPI:
    _app = FastAPI(
        title="Analysis Manager",
        description="Analysis Manager for Fulcrum Intelligence",
        debug=settings.DEBUG,
        root_path=settings.OPENAPI_PREFIX,  # type: ignore
        docs_url=None,
        redoc_url=None,
    )
    _app.include_router(core_router, prefix="/v1")
    _app.include_router(user_router, prefix="/v1")
    _app.include_router(health_check_router, prefix="/v1")
    swagger_router = setup_swagger_ui("Analysis Manager", settings)
    _app.include_router(swagger_router)
    _app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # add request id middleware
    _app.add_middleware(BaseHTTPMiddleware, dispatch=request_id_middleware)

    # add process time log middleware
    _app.add_middleware(BaseHTTPMiddleware, dispatch=process_time_log_middleware)

    # setup logging
    setup_rich_logger(settings)

    return _app


app = get_application()
