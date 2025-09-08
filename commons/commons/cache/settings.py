from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class CacheSettings(BaseSettings):
    CACHE_ENABLED: bool = True
    CACHE_PREFIX: str = "cache"  # overridden per service
    REDIS_URL: str | None = None  # "redis://..."
    CACHE_TTL_SECONDS: int = 300

    model_config = SettingsConfigDict(env_prefix="")


@lru_cache
def cache_settings() -> CacheSettings:
    """Get cache settings instance."""
    return CacheSettings()
