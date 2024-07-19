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
    # query_manager
    ROOT_DIR: Path = Path(__file__).parent.parent
    BASE_DIR: Path = ROOT_DIR / "query_manager"


class Settings(BaseSettings):
    PATHS: Paths = Paths()

    DEBUG: bool = False
    SECRET_KEY: str
    ENV: Environment = Environment.dev
    OPENAPI_PREFIX: str | None = None
    LOGGING_LEVEL: str = "INFO"

    SERVER_HOST: str | AnyHttpUrl
    PAGINATION_PER_PAGE: int = 20

    CUBE_API_URL: str | AnyHttpUrl

    AWS_BUCKET: str = "fulcrum-engine-metrics"
    AWS_REGION: str = "us-east-1"

    BACKEND_CORS_ORIGINS: list[AnyHttpUrl | str] = []

    DATABASE_URL: str
    SQLALCHEMY_ENGINE_OPTIONS: dict = {"pool_pre_ping": True, "pool_size": 5, "max_overflow": 80, "echo": True}

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
