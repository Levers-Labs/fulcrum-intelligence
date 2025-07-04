import logging
from typing import Any

import snowflake.snowpark as snowpark
from cryptography.hazmat.primitives import serialization
from pydantic import BaseModel

from commons.models.enums import SnowflakeAuthMethod

logger = logging.getLogger(__name__)


class SnowflakeConfigModel(BaseModel):
    """Snowflake configuration model for the client."""

    account_identifier: str
    username: str
    password: str | None = None
    private_key: str | None = None
    private_key_passphrase: str | None = None
    database: str
    db_schema: str  # type: ignore
    warehouse: str | None = None
    role: str | None = None
    auth_method: str  # "password" or "private_key"


class SnowflakeClient:
    """
    Client for connecting to Snowflake using Snowpark Python.
    This client handles connection management and provides methods for common operations.
    """

    def __init__(self, config: SnowflakeConfigModel):
        """
        Initialize the Snowflake client with the given configuration.

        Args:
            config: SnowflakeConfigModel with connection details
        """
        self.config = config
        self._session: snowpark.Session | None = None

    def _get_connection_parameters(self) -> dict[str, Any]:
        """
        Build connection parameters dictionary from configuration.

        Returns:
            Dictionary with connection parameters for Snowpark
        """
        conn_params: dict[str, Any] = {
            "account": self.config.account_identifier,
            "user": self.config.username,
            "database": self.config.database,
            "schema": self.config.db_schema,
        }

        # Add optional parameters if they exist
        if self.config.warehouse:
            conn_params["warehouse"] = self.config.warehouse
        if self.config.role:
            conn_params["role"] = self.config.role

        # Add authentication parameters based on auth method
        if self.config.auth_method == SnowflakeAuthMethod.PASSWORD:
            if not self.config.password:
                raise ValueError("Password is required for password authentication")
            conn_params["password"] = self.config.password
        elif self.config.auth_method == SnowflakeAuthMethod.PRIVATE_KEY:
            if not self.config.private_key:
                raise ValueError("Private key is required for private key authentication")
            # Load the private key object for Snowpark
            private_key = serialization.load_pem_private_key(
                self.config.private_key.encode(),
                password=(self.config.private_key_passphrase.encode() if self.config.private_key_passphrase else None),
            )
            conn_params["private_key"] = private_key
        else:
            raise ValueError(f"Unsupported auth method: {self.config.auth_method}")

        return conn_params

    def _create_session(self) -> snowpark.Session:
        """
        Create a new Snowpark session.

        Returns:
            Snowpark Session object
        """
        conn_params = self._get_connection_parameters()
        return snowpark.Session.builder.configs(conn_params).create()

    async def test_connection(self) -> dict[str, Any]:
        """
        Test the Snowflake connection with the provided configuration.

        Returns:
            Dictionary with connection test results:
            - success: Boolean indicating if connection was successful
            - message: Status message
            - connection_details: Connection details if successful, None otherwise
            - version: Snowflake version information if successful
        """
        try:
            # Validate that all required configuration fields are present
            required_fields = [
                self.config.account_identifier,
                self.config.username,
                self.config.database,
                self.config.db_schema,
                self.config.auth_method,
            ]

            if not all(required_fields):
                return {
                    "success": False,
                    "message": "Missing required configuration fields",
                    "connection_details": None,
                }

            # Try to create a session and run a simple query
            session = self._create_session()
            version_info = session.sql("SELECT CURRENT_VERSION() AS VERSION").collect()[0]["VERSION"]

            # Close the session after testing
            session.close()

            return {
                "success": True,
                "message": "Connection successful",
                "connection_details": {
                    "warehouse": self.config.warehouse,
                    "database": self.config.database,
                    "schema": self.config.db_schema,
                    "role": self.config.role,
                    "account": self.config.account_identifier,
                },
                "version": version_info,
            }

        except Exception as e:  # noqa
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}",
                "connection_details": None,
            }

    def get_session(self) -> snowpark.Session:
        """
        Get an existing session or create a new one if none exists.

        Returns:
            Snowpark Session object
        """
        if self._session is None:
            self._session = self._create_session()
        else:
            # Try to use the existing session
            try:
                # Execute a simple query to check if session is still valid
                self._session.sql("SELECT 1").collect()
            except Exception:
                # If there's an exception, create a new session
                try:
                    # Try to close the old session if possible
                    self._session.close()
                except Exception:
                    logger.exception("Failed to close old session")
                self._session = self._create_session()

        return self._session

    def close_session(self) -> None:
        """Close the current session if it exists."""
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                logger.exception("Failed to close session")
            finally:
                self._session = None

    def __enter__(self) -> "SnowflakeClient":
        """Context manager entry point."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - ensures session is closed."""
        self.close_session()
