from pydantic import ConfigDict, Field, model_validator

from commons.models import BaseModel
from commons.models.slack import SlackChannel
from insights_backend.core.models import SnowflakeAuthMethod


class SlackChannelResponse(BaseModel):
    results: list[SlackChannel]
    next_cursor: str | None = None


# Snowflake Configuration Schemas
class SnowflakeConfigBase(BaseModel):
    account_identifier: str = Field(description="Snowflake account identifier (e.g., account.region)")
    username: str = Field(description="Snowflake username")
    warehouse: str | None = Field(None, description="Optional warehouse name")
    role: str | None = Field(None, description="Optional role name")
    database: str = Field(description="Database name for caching")
    db_schema: str = Field(description="Schema name for caching")
    auth_method: SnowflakeAuthMethod = Field(description="Authentication method")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "account_identifier": "myaccount.us-east-1",
                "username": "analytics_user",
                "warehouse": "ANALYTICS_WH",
                "role": "ANALYTICS_ROLE",
                "database": "FULCRUM_CACHE",
                "db_schema": "METRICS",
                "auth_method": "PASSWORD",
            }
        },
    )


class SnowflakeConfigCreate(SnowflakeConfigBase):
    password: str | None = Field(None, description="Password for password authentication")
    private_key: str | None = Field(None, description="Private key for key-based authentication")
    private_key_passphrase: str | None = Field(None, description="Passphrase for private key")

    @model_validator(mode="after")
    def validate_credentials_based_on_auth_method(self):
        """Validate that required credentials are provided based on auth method"""
        if self.auth_method == SnowflakeAuthMethod.PASSWORD:
            if not self.password:
                raise ValueError("Password is required when auth_method is 'PASSWORD'")
        elif self.auth_method == SnowflakeAuthMethod.PRIVATE_KEY:
            if not self.private_key:
                raise ValueError("Private key is required when auth_method is 'PRIVATE_KEY'")
        return self

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "account_identifier": "myaccount.us-east-1",
                "username": "analytics_user",
                "warehouse": "ANALYTICS_WH",
                "role": "ANALYTICS_ROLE",
                "database": "FULCRUM_CACHE",
                "db_schema": "METRICS",
                "auth_method": "PASSWORD",
                "password": "secure_password123",
            }
        }
    )


class SnowflakeConfigRead(SnowflakeConfigBase):
    id: int
    tenant_id: int

    model_config = ConfigDict(from_attributes=True)


class SnowflakeConfigUpdate(BaseModel):
    account_identifier: str | None = None
    username: str | None = None
    warehouse: str | None = None
    role: str | None = None
    database: str | None = None
    db_schema: str | None = None
    auth_method: SnowflakeAuthMethod | None = None
    password: str | None = Field(None, description="Password for password authentication")
    private_key: str | None = Field(None, description="Private key for key-based authentication")
    private_key_passphrase: str | None = Field(None, description="Passphrase for private key")
    model_config = ConfigDict(from_attributes=True)


class SnowflakeConnectionTest(BaseModel):
    success: bool
    message: str
    connection_details: dict | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "success": True,
                "message": "Connection successful",
                "connection_details": {
                    "warehouse": "ANALYTICS_WH",
                    "database": "FULCRUM_CACHE",
                    "db_schema": "METRICS",
                    "role": "ANALYTICS_ROLE",
                },
            }
        }
    )
