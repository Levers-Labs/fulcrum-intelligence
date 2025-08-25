import sentry_sdk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from analysis_manager.config import get_settings
from analysis_manager.core.routes import router as core_router
from analysis_manager.exceptions import add_exception_handlers
from analysis_manager.health import router as health_check_router
from commons.middleware import process_time_log_middleware, request_id_middleware, tenant_context_middleware
from commons.utilities.docs import custom_openapi, setup_swagger_ui
from commons.utilities.logger import setup_rich_logger


def get_application() -> FastAPI:
    settings = get_settings()
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        traces_sample_rate=1.0,
    )
    _app = FastAPI(
        title="Analysis Manager",
        description="Analysis Manager for Fulcrum Intelligence",
        debug=settings.DEBUG,
        root_path=settings.URL_PREFIX,
        docs_url=None,
        redoc_url=None,
    )
    _app.include_router(core_router, prefix="/v1")
    _app.include_router(health_check_router, prefix="/v1")
    swagger_router = setup_swagger_ui("Analysis Manager", settings.URL_PREFIX)
    _app.include_router(swagger_router)
    _app.openapi = custom_openapi(_app, settings)  # type: ignore
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

    # add tenant context middleware
    _app.add_middleware(BaseHTTPMiddleware, dispatch=tenant_context_middleware)

    # setup logging
    setup_rich_logger(settings)

    # add exception handlers
    add_exception_handlers(_app)

    return _app


app = get_application()
