from __future__ import annotations

from enum import Enum
from pathlib import Path

from pydantic import AnyHttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StrEnum(str, Enum):
    pass


class Environment(StrEnum):
    dev = "dev"
    prod = "prod"


class Paths:
    # story_manager
    ROOT_DIR: Path = Path(__file__).parent.parent
    # story_manager/story_manager
    BASE_DIR: Path = ROOT_DIR / "insights_backend"


class Settings(BaseSettings):
    PATHS: Paths = Paths()

    DEBUG: bool = False
    SECRET_KEY: str
    ENV: Environment = Environment.dev
    # App as URL prefix added for each microservice
    URL_PREFIX: str = "/insights"

    LOGGING_LEVEL: str = "INFO"

    SERVER_HOST: str | AnyHttpUrl
    PAGINATION_PER_PAGE: int = 20

    DATABASE_URL: str
    SQLALCHEMY_ENGINE_OPTIONS: dict = {"pool_pre_ping": True, "pool_size": 5, "max_overflow": 80, "echo": True}

    BACKEND_CORS_ORIGINS: list[AnyHttpUrl | str] = []

    # Auth0
    AUTH0_API_AUDIENCE: str
    AUTH0_ISSUER: str
    AUTH0_CLIENT_ID: str
    AUTH0_CLIENT_SECRET: str

    # Slack OAuth
    SLACK_CLIENT_ID: str
    SLACK_CLIENT_SECRET: str
    SLACK_OAUTH_SCOPES: list[str] = ["chat:write", "chat:write.public", "channels:read", "channels:join"]
    SLACK_OAUTH_REDIRECT_PATH: str = "api/slack/oauth/callback"

    # Prefect
    PREFECT_API_URL: str
    PREFECT_API_TOKEN: str

    # Sentry
    SENTRY_DSN: str | None = None

    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    @classmethod
    def assemble_cors_origins(cls, v: str | list[str]) -> list[str] | str:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    # pydantic settings config
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


_settings: Settings | None = None


def get_settings() -> Settings:
    """
    Get the settings an object.
    A Lazy load of the settings object.
    :return: Settings
    """
    global _settings
    if _settings is None:
        _settings = Settings()  # type: ignore
    return _settings
