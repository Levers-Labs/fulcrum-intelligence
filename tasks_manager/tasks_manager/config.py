import json
import os
from typing import Literal

from prefect.blocks.core import Block
from pydantic import Field, SecretStr


class AppConfig(Block):
    """Block containing application configuration."""

    _block_type_name = "AppConfig"
    _block_schema_capabilities = ["app-configuration"]

    env: Literal["dev", "stage", "prod"] = Field(default="dev", description="Environment name")
    secret_key: SecretStr = Field(..., description="Secret key")
    # Story Manager
    story_manager_server_host: str = Field(..., description="Story Manager Server Host")
    # Analysis Manager
    analysis_manager_server_host: str = Field(..., description="Analysis Manager Server Host")
    # Query Manager
    query_manager_server_host: str = Field(..., description="Query Manager Server Host")
    # Insights Backend
    insights_backend_server_host: str = Field(..., description="Insights Backend Server Host")
    # Database
    database_url: SecretStr = Field(..., description="Database URL")
    # SQLALCHEMY_ENGINE_OPTIONS
    sqlalchemy_engine_options: dict = Field(
        default={"pool_pre_ping": True, "poolclass": "NullPool", "echo": True},
        description="SQLAlchemy Engine Options",
    )
    # Auth0
    auth0_api_audience: str = Field(..., description="Auth0 API Audience")
    auth0_issuer: str = Field(..., description="Auth0 Issuer")
    auth0_client_secret: SecretStr = Field(..., description="Auth0 Client Secret")
    auth0_client_id: str = Field(..., description="Auth0 Client ID")
    # Notifiers
    sender_email: str = Field(default="Levers Insights <notifications@leverslabs.com>", description="Sender Email")
    aws_region: str = Field(default="us-west-1", description="AWS Region")

    @property
    def DATABASE_URL(self) -> str:  # noqa
        return self.database_url.get_secret_value()

    @property
    def SQLALCHEMY_ENGINE_OPTIONS(self) -> dict:  # noqa
        return self.sqlalchemy_engine_options

    def _set_env_vars(self) -> None:
        """Set environment variables from config attributes"""
        for field_name, field_value in self:
            env_var_name = field_name.upper()
            if isinstance(field_value, (str, int, float, bool)):
                os.environ[env_var_name] = str(field_value)
            elif isinstance(field_value, SecretStr):
                os.environ[env_var_name] = field_value.get_secret_value()
            elif isinstance(field_value, dict):
                os.environ[env_var_name] = json.dumps(field_value)

    @classmethod
    async def load(cls, name: str, **kwargs) -> "AppConfig":  # type: ignore
        """Load configuration and set environment variables"""
        # Your existing config loading logic here
        config = await super().load(name, **kwargs)

        # Mirror all config values to environment variables
        config._set_env_vars()  # noqa

        return config
