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
    # analysis_manager
    ROOT_DIR: Path = Path(__file__).parent.parent
    BASE_DIR: Path = ROOT_DIR / "analysis_manager"


class Settings(BaseSettings):
    PATHS: Paths = Paths()

    DEBUG: bool = False
    SECRET_KEY: str
    ENV: Environment = Environment.dev
    LOGGING_LEVEL: str = "INFO"

    SERVER_HOST: str | AnyHttpUrl
    PAGINATION_PER_PAGE: int = 20

    DATABASE_URL: str
    SQLALCHEMY_ENGINE_OPTIONS: dict = {"pool_pre_ping": True, "pool_size": 5, "max_overflow": 80, "echo": True}

    BACKEND_CORS_ORIGINS: list[AnyHttpUrl | str] = []

    QUERY_MANAGER_SERVER_HOST: str | AnyHttpUrl
    DSENSEI_BASE_URL: str = "http://localhost:5001"

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


settings = Settings()  # type: ignore
