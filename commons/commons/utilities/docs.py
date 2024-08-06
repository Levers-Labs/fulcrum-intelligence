from fastapi import APIRouter, FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from pydantic_settings import BaseSettings


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


def custom_openapi(app: FastAPI, settings: BaseSettings):
    def custom_openapi_callable():
        """
        Overriding openapi method to add client cred based auth as well in swagger along with token based auth
        AuthServer schema in following code is for client cred based auth
        """
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title=app.title,
            version="3.1.0",
            description=app.description,
            routes=app.routes,
            servers=[{"url": settings.OPENAPI_PREFIX or "/dev/"}],
        )
        openapi_schema["components"]["securitySchemes"]["AuthServer"] = {
            "description": "Authentication via Cognito(OAuth2)",
            "type": "oauth2",
            "flows": {
                "clientCredentials": {
                    "tokenUrl": f"{settings.AUTH0_ISSUER.rstrip('/')}/oauth/token",
                }
            },
        }

        # Adding AuthServer security schema for all the routes of app, else they aren't working with client cred auth
        for _, path_config in openapi_schema["paths"].items():
            for _, method_config in path_config.items():
                if "security" in method_config:
                    method_config["security"].append({"AuthServer": []})

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    return custom_openapi_callable
