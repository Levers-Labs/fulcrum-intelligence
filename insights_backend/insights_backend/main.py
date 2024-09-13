from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from commons.middleware import process_time_log_middleware, request_id_middleware, set_context_middleware
from commons.utilities.docs import custom_openapi, setup_swagger_ui
from commons.utilities.logger import setup_rich_logger
from insights_backend.config import get_settings
from insights_backend.core.routes import tenant_config_router, tenant_router, user_router as core_router
from insights_backend.health import router as health_check_router


def get_application() -> FastAPI:
    settings = get_settings()
    _app = FastAPI(
        title="Insights Backend",
        description="Insights Backend for Fulcrum Intelligence",
        debug=settings.DEBUG,
        root_path=settings.URL_PREFIX,
        docs_url=None,
        redoc_url=None,
    )
    _app.include_router(core_router, prefix="/v1")
    _app.include_router(tenant_router, prefix="/v1")
    _app.include_router(tenant_config_router, prefix="/v1")
    _app.include_router(health_check_router, prefix="/v1")
    swagger_router = setup_swagger_ui("Insights Backend", settings.URL_PREFIX)
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
    _app.add_middleware(BaseHTTPMiddleware, dispatch=set_context_middleware)
    # setup logging
    setup_rich_logger(settings)

    return _app


app = get_application()
