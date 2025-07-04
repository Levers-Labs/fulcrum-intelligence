from pydantic import field_validator
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlmodel import Field

from commons.db.models import BaseSQLModel
from commons.models import BaseModel


class Tenant(BaseSQLModel):
    # Unique identifier for the tenant
    identifier: str = Field(unique=True)
    # Display name for the tenant
    name: str
    description: str | None = None
    domains: list[str] = Field(sa_column=Column(ARRAY(String)))
    external_id: str


class CubeConnectionRead(BaseModel):
    """
    Cube connection details
    """

    cube_api_url: str
    cube_auth_type: str


class CubeConnectionConfig(CubeConnectionRead):
    """
    Cube connection details with secret key or token
    """

    cube_auth_token: str | None = None
    cube_auth_secret_key: str | None = None


class SlackConnectionRead(BaseModel):
    """
    Slack connection details
    """

    bot_user_id: str
    app_id: str
    team: dict
    authed_user: dict


class SlackConnectionConfig(SlackConnectionRead):
    """
    Slack connection details with token
    """

    bot_token: str


class TenantConfigUpdate(BaseSQLModel):
    # Cube connection details
    cube_connection_config: CubeConnectionConfig = Field(sa_type=JSONB, default_factory=dict)
    enable_story_generation: bool

    @field_validator("cube_connection_config")
    @classmethod
    def validate_cube_connection_config(cls, value: CubeConnectionConfig) -> CubeConnectionConfig:
        if value.cube_auth_type == "TOKEN":
            if not value.cube_auth_token:
                raise ValueError("cube_auth_token is required when auth_type is TOKEN")
        elif value.cube_auth_type == "SECRET_KEY":
            if not value.cube_auth_secret_key:
                raise ValueError("cube_auth_secret_key is required when auth_type is SECRET_KEY")
        return value


class TenantConfig(TenantConfigUpdate):
    # Slack connection details
    slack_connection: SlackConnectionConfig | None = Field(sa_type=JSONB, default_factory=dict, nullable=True)


class TenantConfigRead(BaseModel):
    """
    Tenant config details
    """

    cube_connection_config: CubeConnectionRead
    slack_connection: SlackConnectionRead | None = None
    enable_story_generation: bool
    enable_metric_cache: bool
