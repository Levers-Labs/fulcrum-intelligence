from __future__ import annotations

from pathlib import Path
from typing import Union

from pydantic import AnyHttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    from enum import StrEnum
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):
        pass


class Environment(StrEnum):
    dev = "dev"
    prod = "prod"


class Paths:
    # query_manager
    ROOT_DIR: Path = Path(__file__).parent.parent
    BASE_DIR: Path = ROOT_DIR / "app"


class Settings(BaseSettings):
    PATHS: Paths = Paths()

    DEBUG: bool = False
    SECRET_KEY: str
    ENV: Environment = Environment.dev
    LOGGING_LEVEL: str = "INFO"

    SERVER_HOST: Union[str, AnyHttpUrl] = "http://localhost:8000"
    PAGINATION_PER_PAGE: int = 20

    BACKEND_CORS_ORIGINS: list[AnyHttpUrl] = []

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


settings = Settings()
