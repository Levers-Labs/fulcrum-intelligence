"""App configuration resource loaded from env variables or .env file.

This uses pydantic-settings to support loading from a local .env file as well as
process environment variables (prefixed with APP_).
"""

from dagster import ConfigurableResource
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    env: str = "dev"
    secret_key: SecretStr

    # Service endpoints
    story_manager_server_host: str
    analysis_manager_server_host: str
    query_manager_server_host: str
    insights_backend_server_host: str
    server_host: str

    # Database
    database_url: str

    # Auth0
    auth0_api_audience: str
    auth0_issuer: str
    auth0_client_secret: SecretStr
    auth0_client_id: str

    # Notifications
    sender_email: str = "Levers Insights <notifications@leverslabs.com>"
    aws_region: str = "us-west-1"

    # Dagster S3 Storage (optional - only required for prod)
    dagster_s3_bucket: str | None = Field(default=None)

    # Sentry
    sentry_dsn: str | None = Field(default=None, alias="SENTRY_DSN")


class AppConfigResource(ConfigurableResource):
    """Dagster resource providing application configuration.

    Usage:
        app_config = AppConfigResource.from_env()
        # access via app_config.settings
    """

    @property
    def settings(self) -> Settings:
        """Load settings from environment and .env lazily (not a Dagster config field)."""
        return Settings()  # type: ignore

    @classmethod
    def from_env(cls) -> "AppConfigResource":
        """Factory method retained for API parity (no Dagster config fields)."""
        return cls()
