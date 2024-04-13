from fastapi import APIRouter
from fastapi.openapi.docs import get_swagger_ui_html


def setup_swagger_ui(title, settings):
    router = APIRouter(prefix="")

    @router.get("/docs", include_in_schema=False)
    async def swagger_ui_html():
        root_path = settings.OPENAPI_PREFIX.rstrip("/") if settings.OPENAPI_PREFIX else ""
        openapi_url = root_path + "/openapi.json"
        favicon_url = (
            "https://assets-global.website-files.com/65b0c52f1811c3bf08fed0b5/65bbf1d7d16c01cc7220efd8_256.png"
        )
        return get_swagger_ui_html(openapi_url=openapi_url, title=title, swagger_favicon_url=favicon_url)

    return router
