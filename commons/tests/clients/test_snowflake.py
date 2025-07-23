# ruff: noqa: S106, S105
from unittest.mock import MagicMock, patch

import pytest

from commons.clients.snowflake import SnowflakeAuthMethod, SnowflakeClient, SnowflakeConfigModel


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
        db_schema="test_schema",
        warehouse="test_warehouse",
        role="test_role",
        auth_method=SnowflakeAuthMethod.PASSWORD,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PRIVATE_KEY,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PRIVATE_KEY,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
        )

        with SnowflakeClient(config=config) as client:
            session = client.get_session()
            assert session == mock_snowpark_session

        # After exiting context, session should be closed
        mock_snowpark_session.close.assert_called_once()

    @patch("snowflake.snowpark.Session.builder")
    def test_context_manager_entry(self, mock_builder, mock_snowpark_session):
        """Test context manager __enter__ creates session correctly."""
        mock_builder.configs.return_value.create.return_value = mock_snowpark_session

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
        )

        client = SnowflakeClient(config=config)

        with patch.object(client, "create_session") as mock_create:
            mock_create.return_value = mock_snowpark_session

            with client as context_client:
                assert context_client == client
                mock_create.assert_called_once()

    @patch("snowflake.snowpark.Session.builder")
    def test_context_manager_exit_closes_session(self, mock_builder, mock_snowpark_session):
        """Test context manager __exit__ closes session properly."""
        mock_builder.configs.return_value.create.return_value = mock_snowpark_session

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
        )

        client = SnowflakeClient(config=config)

        with patch.object(client, "create_session") as mock_create, patch.object(client, "close_session") as mock_close:
            mock_create.return_value = mock_snowpark_session

            with client:
                pass

            mock_close.assert_called_once()

    @patch("snowflake.snowpark.Session.builder")
    def test_context_manager_exit_with_exception(self, mock_builder, mock_snowpark_session):
        """Test context manager __exit__ closes session even with exception."""
        mock_builder.configs.return_value.create.return_value = mock_snowpark_session

        config = SnowflakeConfigModel(
            account_identifier="my_account",
            username="test_user",
            password="test_password",
            database="test_db",
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
        )

        client = SnowflakeClient(config=config)

        with patch.object(client, "create_session") as mock_create, patch.object(client, "close_session") as mock_close:
            mock_create.return_value = mock_snowpark_session

            try:
                with client:
                    raise ValueError("Test exception")
            except ValueError:
                pass

            mock_close.assert_called_once()

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
            db_schema="test_schema",
            warehouse="test_warehouse",
            role="test_role",
            auth_method=SnowflakeAuthMethod.PASSWORD,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,  # But auth method is password
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PRIVATE_KEY,  # But auth method is private_key
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
            db_schema="test_schema",
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
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
            db_schema="test_schema",
            auth_method=SnowflakeAuthMethod.PASSWORD,
        )

        client = SnowflakeClient(config=config)
        client._session = session_mock

        # close_session should handle the exception and still set _session to None
        client.close_session()

        session_mock.close.assert_called_once()
        assert client._session is None


class TestSnowflakeClientCacheMethods:
    """Test cache-specific methods added in the diff."""

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    async def test_create_cache_table_new_table(self, mock_builder, snowflake_config):
        """Test creating a new cache table."""
        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session

        # Mock table doesn't exist
        mock_session.sql.return_value.collect.return_value = [[0]]

        client = SnowflakeClient(config=snowflake_config)
        await client._create_cache_table("test_table", mock_session)

        # Should check if table exists and create it
        assert mock_session.sql.call_count == 2
        create_call = mock_session.sql.call_args_list[1][0][0]
        assert "CREATE TABLE test_table" in create_call
        assert "metric_id VARCHAR" in create_call
        assert "PRIMARY KEY (metric_id, grain, date)" in create_call

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    async def test_create_cache_table_existing_table(self, mock_builder, snowflake_config):
        """Test handling existing cache table."""
        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session

        # Mock table exists
        mock_session.sql.return_value.collect.return_value = [[1]]

        client = SnowflakeClient(config=snowflake_config)
        await client._create_cache_table("test_table", mock_session)

        # Should only check if table exists, not create it
        assert mock_session.sql.call_count == 1

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    @patch("pandas.DataFrame")
    async def test_create_or_update_metric_time_series_full_sync(self, mock_df, mock_builder, snowflake_config):
        """Test creating/updating metric time series with full sync."""
        test_data = [
            {
                "metric_id": "test_metric",
                "date": "2024-01-01",
                "grain": "day",
                "value": 100.0,
                "cached_at": "2024-01-01T10:00:00",
            }
        ]

        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session
        mock_session.write_pandas.return_value = MagicMock()

        client = SnowflakeClient(config=snowflake_config)

        with patch.object(client, "_create_cache_table") as mock_create_table:
            result = await client.create_or_update_metric_time_series(
                table_name="test_table", data=test_data, is_full_sync=True
            )

            # Should create table and write data
            mock_create_table.assert_called_once_with("test_table", mock_session)
            mock_session.write_pandas.assert_called_once()

            # Should truncate for full sync
            write_args = mock_session.write_pandas.call_args
            assert write_args[1]["overwrite"] is True

            assert result["status"] == "SUCCESS"
            assert result["rows_processed"] == 1

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    @patch("pandas.DataFrame")
    async def test_create_or_update_metric_time_series_incremental_sync(self, mock_df, mock_builder, snowflake_config):
        """Test creating/updating metric time series with incremental sync."""
        test_data = [
            {
                "metric_id": "test_metric",
                "date": "2024-01-01",
                "grain": "day",
                "value": 100.0,
                "cached_at": "2024-01-01T10:00:00",
            }
        ]

        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session
        mock_session.write_pandas.return_value = MagicMock()

        client = SnowflakeClient(config=snowflake_config)

        with patch.object(client, "_create_cache_table") as mock_create_table:
            result = await client.create_or_update_metric_time_series(
                table_name="test_table", data=test_data, is_full_sync=False
            )

            # Should create table and write data
            mock_create_table.assert_called_once_with("test_table", mock_session)
            mock_session.write_pandas.assert_called_once()

            # Should append for incremental sync
            write_args = mock_session.write_pandas.call_args
            assert write_args[1]["overwrite"] is False

            assert result["status"] == "SUCCESS"
            assert result["rows_processed"] == 1

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    async def test_get_table_row_count(self, mock_builder, snowflake_config):
        """Test getting table row count."""
        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session
        mock_session.sql.return_value.collect.return_value = [[1500]]

        client = SnowflakeClient(config=snowflake_config)
        count = await client.get_table_row_count("test_table")

        assert count == 1500
        mock_session.sql.assert_called_once()
        sql_query = mock_session.sql.call_args[0][0]
        assert "SELECT COUNT(*) FROM test_table" in sql_query

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    async def test_get_table_performance_metrics(self, mock_builder, snowflake_config):
        """Test getting table performance metrics."""
        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session

        # Mock responses for different queries
        mock_session.sql.return_value.collect.side_effect = [
            [[1000]],  # row count
            [["2024-01-01", "2024-01-31"]],  # date range
            [[31]],  # unique dates
        ]

        client = SnowflakeClient(config=snowflake_config)
        metrics = await client.get_table_performance_metrics("test_table")

        assert metrics["row_count"] == 1000
        assert metrics["date_range"]["min_date"] == "2024-01-01"
        assert metrics["date_range"]["max_date"] == "2024-01-31"
        assert metrics["unique_dates"] == 31

        # Should make 3 SQL queries
        assert mock_session.sql.call_count == 3

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    async def test_cleanup_old_table_data(self, mock_builder, snowflake_config):
        """Test cleaning up old table data."""
        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session
        mock_session.sql.return_value.collect.return_value = [[50]]  # rows deleted

        client = SnowflakeClient(config=snowflake_config)
        result = await client.cleanup_old_table_data(table_name="test_table", retention_days=365)

        assert result["status"] == "SUCCESS"
        assert result["rows_deleted"] == 50
        assert result["retention_days"] == 365

        # Should execute delete query
        mock_session.sql.assert_called_once()
        sql_query = mock_session.sql.call_args[0][0]
        assert "DELETE FROM test_table" in sql_query
        assert "date < DATEADD('day', -365, CURRENT_DATE())" in sql_query

    @pytest.mark.asyncio
    async def test_create_or_update_metric_time_series_empty_data(self, snowflake_config):
        """Test handling empty data."""
        client = SnowflakeClient(config=snowflake_config)
        result = await client.create_or_update_metric_time_series(table_name="test_table", data=[], is_full_sync=True)

        assert result["status"] == "SUCCESS"
        assert result["rows_processed"] == 0
        assert "No data provided" in result["message"]

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    async def test_error_handling_in_cache_methods(self, mock_builder, snowflake_config):
        """Test error handling in cache methods."""
        mock_builder.configs.return_value.create.side_effect = Exception("Connection failed")

        client = SnowflakeClient(config=snowflake_config)

        with pytest.raises(Exception) as exc_info:
            await client.get_table_row_count("test_table")

        assert "Connection failed" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    async def test_get_table_performance_metrics_error_handling(self, mock_builder, snowflake_config):
        """Test error handling in performance metrics."""
        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session
        mock_session.sql.side_effect = Exception("Query failed")

        client = SnowflakeClient(config=snowflake_config)

        with pytest.raises(Exception) as exc_info:
            await client.get_table_performance_metrics("test_table")

        assert "Query failed" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch("snowflake.snowpark.Session.builder")
    async def test_cleanup_old_table_data_error_handling(self, mock_builder, snowflake_config):
        """Test error handling in cleanup operation."""
        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session
        mock_session.sql.side_effect = Exception("Delete failed")

        client = SnowflakeClient(config=snowflake_config)

        result = await client.cleanup_old_table_data("test_table", 365)

        assert result["status"] == "ERROR"
        assert "Delete failed" in result["error"]
