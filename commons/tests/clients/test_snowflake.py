# ruff: noqa: S106, S105
from unittest.mock import MagicMock, patch

import pytest

from commons.clients.snowflake import SnowflakeClient, SnowflakeConfigModel


@pytest.fixture
def mock_snowpark_session():
    """Create a mock Snowpark session for testing."""
    session_mock = MagicMock()
    # Mock the SQL query execution and result
    query_result_mock = MagicMock()
    query_result_mock.collect.return_value = [{"VERSION": "SNOWFLAKE 7.0.0"}]
    session_mock.sql.return_value = query_result_mock
    return session_mock


@pytest.fixture
def snowflake_config():
    """Create a sample Snowflake configuration for testing."""
    return SnowflakeConfigModel(
        account_identifier="my_account",
        username="test_user",
        password="test_password",
        database="test_db",
        schema="test_schema",
        warehouse="test_warehouse",
        role="test_role",
        auth_method="password",
    )


class TestSnowflakeClient:
    @patch("snowflake.snowpark.Session")
    def test_init(self, mock_session, snowflake_config):
        """Test client initialization."""
        client = SnowflakeClient(config=snowflake_config)

        assert client.config == snowflake_config
        assert client._session is None

    @patch("snowflake.snowpark.Session.builder")
    def test_get_connection_parameters(self, mock_builder, snowflake_config):
        """Test the connection parameters generation."""
        client = SnowflakeClient(config=snowflake_config)

        params = client._get_connection_parameters()

        assert params["account"] == "my_account"
        assert params["user"] == "test_user"
        assert params["password"] == "test_password"
        assert params["database"] == "test_db"
        assert params["schema"] == "test_schema"
        assert params["warehouse"] == "test_warehouse"
        assert params["role"] == "test_role"

    @patch("snowflake.snowpark.Session.builder")
    @patch("cryptography.hazmat.primitives.serialization.load_pem_private_key")
    def test_get_connection_parameters_private_key(self, mock_load_pem_key, mock_builder):
        """Test the connection parameters generation with private key auth."""
        # Create a mock private key object that will be returned by the mocked load_pem_private_key
        mock_private_key = MagicMock()
        mock_load_pem_key.return_value = mock_private_key

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            private_key="TEST_PRIVATE_KEY",
            private_key_passphrase="passphrase",
            database="test_db",
            schema="test_schema",
            auth_method="private_key",
        )
        client = SnowflakeClient(config=config)

        params = client._get_connection_parameters()

        # Verify the connection parameters
        assert params["account"] == "my_account"
        assert params["user"] == "test_user"
        assert "password" not in params
        assert params["private_key"] == mock_private_key  # Should be the mock private key object
        assert params["database"] == "test_db"
        assert params["schema"] == "test_schema"

        # Verify mock_load_pem_key was called correctly
        mock_load_pem_key.assert_called_once_with(b"TEST_PRIVATE_KEY", password=b"passphrase")

    @patch("snowflake.snowpark.Session.builder")
    def test_create_session(self, mock_builder, snowflake_config):
        """Test the session creation."""
        session_mock = MagicMock()
        mock_builder.configs.return_value.create.return_value = session_mock

        client = SnowflakeClient(config=snowflake_config)
        session = client._create_session()

        assert session == session_mock
        mock_builder.configs.assert_called_once()
        mock_builder.configs.return_value.create.assert_called_once()

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    @patch("cryptography.hazmat.primitives.serialization.load_pem_private_key")
    async def test_test_connection_success_with_private_key(
        self, mock_load_pem_key, mock_builder, mock_snowpark_session
    ):
        """Test successful connection testing with private key authentication."""
        # Create a mock private key object
        mock_private_key = MagicMock()
        mock_load_pem_key.return_value = mock_private_key

        mock_builder.configs.return_value.create.return_value = mock_snowpark_session

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            private_key="TEST_PRIVATE_KEY_PEM",
            private_key_passphrase="secret",
            database="test_db",
            schema="test_schema",
            auth_method="private_key",
        )

        client = SnowflakeClient(config=config)
        result = await client.test_connection()

        assert result["success"] is True
        assert result["message"] == "Connection successful"
        assert result["connection_details"]["database"] == "test_db"
        assert result["version"] == "SNOWFLAKE 7.0.0"

        mock_snowpark_session.sql.assert_called_with("SELECT CURRENT_VERSION() AS VERSION")
        mock_snowpark_session.close.assert_called_once()

        # Verify the private key was loaded correctly
        mock_load_pem_key.assert_called_once_with(b"TEST_PRIVATE_KEY_PEM", password=b"secret")

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    async def test_test_connection_failure(self, mock_builder, snowflake_config):
        """Test failed connection testing."""
        mock_builder.configs.return_value.create.side_effect = Exception("Connection error")

        client = SnowflakeClient(config=snowflake_config)
        result = await client.test_connection()

        assert result["success"] is False
        assert "Connection failed: Connection error" in result["message"]
        assert result["connection_details"] is None

    @patch("snowflake.snowpark.Session.builder")
    def test_get_session_new(self, mock_builder, mock_snowpark_session):
        """Test getting a new session when none exists."""
        mock_builder.configs.return_value.create.return_value = mock_snowpark_session

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            schema="test_schema",
            auth_method="password",
        )
        client = SnowflakeClient(config=config)
        session = client.get_session()

        assert session == mock_snowpark_session
        mock_builder.configs.return_value.create.assert_called_once()

    @patch("snowflake.snowpark.Session.builder")
    def test_get_session_existing(self, mock_builder, mock_snowpark_session):
        """Test getting an existing session."""
        mock_builder.configs.return_value.create.return_value = mock_snowpark_session

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            schema="test_schema",
            auth_method="password",
        )
        client = SnowflakeClient(config=config)
        client._session = mock_snowpark_session
        session = client.get_session()

        assert session == mock_snowpark_session
        # Should check if session is valid with a simple query
        mock_snowpark_session.sql.assert_called_with("SELECT 1")

    @patch("snowflake.snowpark.Session.builder")
    def test_close_session(self, mock_builder, mock_snowpark_session):
        """Test closing an existing session."""
        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            schema="test_schema",
            auth_method="password",
        )
        client = SnowflakeClient(config=config)
        client._session = mock_snowpark_session

        client.close_session()

        mock_snowpark_session.close.assert_called_once()
        assert client._session is None

    @patch("snowflake.snowpark.Session.builder")
    def test_context_manager(self, mock_builder, mock_snowpark_session):
        """Test using the client as a context manager."""
        mock_builder.configs.return_value.create.return_value = mock_snowpark_session

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            schema="test_schema",
            auth_method="password",
        )

        with SnowflakeClient(config=config) as client:
            session = client.get_session()
            assert session == mock_snowpark_session

        # After exiting context, session should be closed
        mock_snowpark_session.close.assert_called_once()

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    async def test_test_connection_success_with_password(self, mock_builder, mock_snowpark_session):
        """Test successful connection testing with password authentication."""
        mock_builder.configs.return_value.create.return_value = mock_snowpark_session

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            schema="test_schema",
            warehouse="test_warehouse",
            role="test_role",
            auth_method="password",
        )

        client = SnowflakeClient(config=config)
        result = await client.test_connection()

        assert result["success"] is True
        assert result["message"] == "Connection successful"
        assert result["connection_details"]["database"] == "test_db"
        assert result["version"] == "SNOWFLAKE 7.0.0"
        mock_snowpark_session.sql.assert_called_with("SELECT CURRENT_VERSION() AS VERSION")
        mock_snowpark_session.close.assert_called_once()

    def test_password_missing_error(self):
        """Test error when password auth method is used but no password is provided."""
        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password=None,  # No password provided
            database="test_db",
            schema="test_schema",
            auth_method="password",  # But auth method is password
        )

        client = SnowflakeClient(config=config)

        with pytest.raises(ValueError, match="Password is required for password authentication"):
            client._get_connection_parameters()

    def test_private_key_missing_error(self):
        """Test error when private_key auth method is used but no private_key is provided."""
        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            private_key=None,  # No private key provided
            database="test_db",
            schema="test_schema",
            auth_method="private_key",  # But auth method is private_key
        )

        client = SnowflakeClient(config=config)

        with pytest.raises(ValueError, match="Private key is required for private key authentication"):
            client._get_connection_parameters()

    def test_invalid_auth_method_error(self):
        """Test error when an invalid auth method is provided."""
        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            database="test_db",
            schema="test_schema",
            auth_method="invalid_method",  # Invalid auth method
        )

        client = SnowflakeClient(config=config)

        with pytest.raises(ValueError, match="Unsupported auth method: invalid_method"):
            client._get_connection_parameters()

    @pytest.mark.asyncio
    async def test_test_connection_missing_required_fields(self):
        """Test connection test with missing required fields."""
        # Create config with missing required fields
        config = SnowflakeConfigModel(
            account_identifier="",  # Empty account identifier
            username="test_user",
            database="test_db",
            schema="test_schema",
            auth_method="password",
        )

        client = SnowflakeClient(config=config)
        result = await client.test_connection()

        assert result["success"] is False
        assert result["message"] == "Missing required configuration fields"
        assert result["connection_details"] is None

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    async def test_test_connection_exception(self, mock_builder):
        """Test connection test when an exception occurs."""
        # Mock the session creation to raise an exception
        mock_builder.configs.return_value.create.side_effect = Exception("Connection error")

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            schema="test_schema",
            auth_method="password",
        )

        client = SnowflakeClient(config=config)
        result = await client.test_connection()

        assert result["success"] is False
        assert "Connection failed: Connection error" in result["message"]
        assert result["connection_details"] is None

    @patch("snowflake.snowpark.Session.builder")
    def test_get_session_existing_session(self, mock_builder, mock_snowpark_session):
        """Test getting existing session."""
        mock_builder.configs.return_value.create.return_value = mock_snowpark_session

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            schema="test_schema",
            auth_method="password",
        )

        client = SnowflakeClient(config=config)

        # First call should create a new session
        session1 = client.get_session()
        assert session1 == mock_snowpark_session
        mock_builder.configs.return_value.create.assert_called_once()

        # Second call should reuse the existing session
        session2 = client.get_session()
        assert session2 == mock_snowpark_session
        # The create method should still have been called only once
        mock_builder.configs.return_value.create.assert_called_once()

    @patch("snowflake.snowpark.Session.builder")
    def test_get_session_reconnect(self, mock_builder, mock_snowpark_session):
        """Test reconnecting when session validation fails."""
        mock_builder.configs.return_value.create.return_value = mock_snowpark_session

        # Set up the first session to fail validation
        bad_session = MagicMock()
        bad_session.sql.side_effect = Exception("Session expired")

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            schema="test_schema",
            auth_method="password",
        )

        client = SnowflakeClient(config=config)
        client._session = bad_session

        # Getting session should detect the invalid session and create a new one
        session = client.get_session()

        assert session == mock_snowpark_session
        bad_session.close.assert_called_once()  # Should try to close the old session
        mock_builder.configs.return_value.create.assert_called_once()  # Should create a new session

    @patch("snowflake.snowpark.Session.builder")
    def test_get_session_close_exception(self, mock_builder, mock_snowpark_session):
        """Test handling exception when closing old session during reconnect."""
        mock_builder.configs.return_value.create.return_value = mock_snowpark_session

        # Set up the first session to fail validation and throw error on close
        bad_session = MagicMock()
        bad_session.sql.side_effect = Exception("Session expired")
        bad_session.close.side_effect = Exception("Failed to close")

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            schema="test_schema",
            auth_method="password",
        )

        client = SnowflakeClient(config=config)
        client._session = bad_session

        # Getting session should handle the close exception and still create a new session
        session = client.get_session()

        assert session == mock_snowpark_session
        bad_session.close.assert_called_once()  # Should try to close the old session
        mock_builder.configs.return_value.create.assert_called_once()  # Should create a new session

    @patch("snowflake.snowpark.Session.builder")
    def test_close_session_exception(self, mock_builder):
        """Test handling exception when closing session in close_session."""
        # Create a session that throws an error when closed
        session_mock = MagicMock()
        session_mock.close.side_effect = Exception("Failed to close")

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            schema="test_schema",
            auth_method="password",
        )

        client = SnowflakeClient(config=config)
        client._session = session_mock

        # close_session should handle the exception and still set _session to None
        client.close_session()

        session_mock.close.assert_called_once()
        assert client._session is None
