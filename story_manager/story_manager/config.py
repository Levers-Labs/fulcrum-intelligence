from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import (
    AnyHttpUrl,
    Field,
    field_validator,
    json,
)
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
    BASE_DIR: Path = ROOT_DIR / "story_manager"
    # story_manager/story_manager/templates
    TEMPLATES_DIR: Path = BASE_DIR / "templates"


class Settings(BaseSettings):
    PATHS: Paths = Paths()

    DEBUG: bool = False
    SECRET_KEY: str
    ENV: Environment = Environment.dev
    # App as URL prefix added for each microservice
    URL_PREFIX: str = "/story"

    LOGGING_LEVEL: str = "INFO"

    SERVER_HOST: str | AnyHttpUrl
    PAGINATION_PER_PAGE: int = 20

    DATABASE_URL: str
    SQLALCHEMY_ENGINE_OPTIONS: dict[str, Any] = Field(
        default_factory=lambda: {"pool_pre_ping": True, "pool_size": 5, "max_overflow": 80, "echo": True}
    )

    @field_validator("SQLALCHEMY_ENGINE_OPTIONS", mode="before")
    @classmethod
    def parse_engine_options(cls, v: Any) -> dict[str, Any]:
        """Parse engine options from string or dict and merge with defaults"""
        defaults = {"pool_pre_ping": True, "echo": True}

        if isinstance(v, str):
            try:
                custom_options = json.loads(v)
            except json.JSONDecodeError:
                return defaults
        elif isinstance(v, dict):
            custom_options = v
        else:
            return defaults

        return {**defaults, **custom_options}

    QUERY_MANAGER_SERVER_HOST: str | AnyHttpUrl
    ANALYSIS_MANAGER_SERVER_HOST: str | AnyHttpUrl
    INSIGHTS_BACKEND_SERVER_HOST: str | AnyHttpUrl

    BACKEND_CORS_ORIGINS: list[AnyHttpUrl | str] = []
    DSENSEI_BASE_URL: str = "http//localhost:5001"

    AUTH0_API_AUDIENCE: str
    AUTH0_ISSUER: str
    AUTH0_CLIENT_ID: str
    AUTH0_CLIENT_SECRET: str

    SENTRY_DSN: str

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
