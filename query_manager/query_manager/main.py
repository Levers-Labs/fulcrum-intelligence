from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from commons.middleware import process_time_log_middleware, request_id_middleware, tenant_context_middleware
from commons.utilities.docs import custom_openapi, setup_swagger_ui
from commons.utilities.logger import setup_rich_logger
from query_manager.config import get_settings
from query_manager.core.routes import router as core_router
from query_manager.exceptions import add_exception_handlers
from query_manager.health import router as health_check_router
from query_manager.semantic_manager.routes import router as semantic_router


def get_application() -> FastAPI:
    settings = get_settings()

    _app = FastAPI(
        title="Query Manager",
        description="Query Manager for Fulcrum Intelligence",
        debug=settings.DEBUG,
        root_path=settings.URL_PREFIX,
        docs_url=None,
        redoc_url=None,
    )
    _app.include_router(core_router, prefix="/v1")
    _app.include_router(health_check_router, prefix="/v1")
    _app.include_router(semantic_router, prefix="/v2")
    swagger_router = setup_swagger_ui("Query Manager", settings.URL_PREFIX)
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
