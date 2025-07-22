import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timedelta
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

    @contextmanager
    def session_context(self) -> Generator[snowpark.Session, None, None]:
        """Context manager for session lifecycle management."""
        session = None
        try:
            session = self._create_session()
            yield session
        except Exception:
            logger.exception("Error during session operation")
            raise
        finally:
            if session is not None:
                try:
                    session.close()
                except Exception:
                    logger.exception("Failed to close session in context manager")

    def __enter__(self) -> "SnowflakeClient":
        """Context manager entry point."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - ensures session is closed."""
        self.close_session()

    # Cache-specific methods
    async def _create_cache_table(self, table_name: str, session: snowpark.Session) -> None:
        """Create cache table with predefined schema if it doesn't exist."""
        # Define schema for time series data - single table per tenant
        schema_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            metric_id VARCHAR,
            grain VARCHAR,
            date DATE,
            value FLOAT,
            cached_at TIMESTAMP,
            PRIMARY KEY (metric_id, grain, date)
        )
        """

        # Create table if it doesn't exist
        session.sql(schema_sql).collect()
        logger.info("Ensured cache table %s exists", table_name)

    async def create_or_update_metric_time_series(
        self,
        table_name: str,
        data: list[dict[str, Any]],
        is_full_sync: bool = False,
    ) -> None:
        """Create or update Snowflake table with cache data using DataFrame operations."""
        if not data:
            return

        with self.session_context() as session:
            # Ensure table exists
            await self._create_cache_table(table_name, session)

            # Create DataFrame from the data
            df = session.create_dataframe(data)

            if is_full_sync:
                # For full sync, overwrite all data
                df.write.mode("overwrite").save_as_table(table_name)
            else:
                # For incremental sync, use SQL MERGE for upsert
                # Create a temporary table for the new data
                temp_table_name = f"{table_name}_temp_{int(datetime.now().timestamp())}"
                df.write.mode("overwrite").save_as_table(temp_table_name)

                # Perform merge/upsert using SQL
                merge_sql = f"""
                MERGE INTO {table_name} AS target
                USING {temp_table_name} AS source
                ON target.metric_id = source.metric_id
                   AND target.grain = source.grain
                   AND target.date = source.date
                WHEN MATCHED THEN
                    UPDATE SET
                        value = source.value,
                        cached_at = source.cached_at
                WHEN NOT MATCHED THEN
                    INSERT (metric_id, grain, date, value, cached_at)
                    VALUES (source.metric_id, source.grain, source.date, source.value, source.cached_at)
                """  # noqa: S608

                session.sql(merge_sql).collect()

                # Clean up temporary table
                session.sql(f"DROP TABLE {temp_table_name}").collect()

        logger.info("Successfully cached %d records to table %s using DataFrame operations", len(data), table_name)

    async def get_table_row_count(self, table_name: str) -> int:
        """Get the number of rows in a Snowflake table."""
        try:
            with self.session_context() as session:
                count_query = f"SELECT COUNT(*) FROM {table_name}"  # noqa: S608
                result = session.sql(count_query).collect()
                return int(result[0][0]) if result else 0  # type: ignore
        except Exception as e:
            logger.error("Failed to get row count for table %s: %s", table_name, str(e))
            return 0

    async def get_table_performance_metrics(self, table_name: str) -> dict[str, Any]:
        """Get performance metrics for a Snowflake table."""
        try:
            with self.session_context() as session:
                # Get table information
                info_query = f"""
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT date),
                    MIN(date),
                    MAX(date),
                    AVG(value),
                    STDDEV(value)
                FROM {table_name}
                """  # noqa: S608

                result = session.sql(info_query).collect()
                if result:
                    row = result[0]
                    return {
                        "row_count": int(row[0]) if row[0] else 0,  # type: ignore
                        "unique_dates": int(row[1]) if row[1] else 0,  # type: ignore
                        "date_range": {
                            "min_date": str(row[2]) if row[2] else None,
                            "max_date": str(row[3]) if row[3] else None,
                        },
                        "value_statistics": {
                            "avg_value": float(row[4]) if row[4] else 0.0,  # type: ignore
                            "stddev_value": float(row[5]) if row[5] else 0.0,  # type: ignore
                        },
                        "status": "SUCCESS",
                    }
                else:
                    return {"status": "NO_DATA"}

        except Exception as e:
            logger.error("Failed to get performance metrics for table %s: %s", table_name, str(e))
            return {"status": "ERROR", "error": str(e)}

    async def cleanup_old_table_data(
        self,
        table_name: str,
        retention_days: int = 730,
    ) -> dict[str, Any]:
        """Clean up old data from a Snowflake table beyond retention period."""
        try:
            # Calculate cutoff date
            cutoff_date = datetime.now().date() - timedelta(days=retention_days)

            with self.session_context() as session:
                # Count records to be deleted
                count_query = f"SELECT COUNT(*) FROM {table_name} WHERE date < '{cutoff_date}'"  # noqa: S608
                count_result = session.sql(count_query).collect()
                records_to_delete = int(count_result[0][0]) if count_result else 0  # type: ignore

                if records_to_delete > 0:
                    # Delete old records
                    delete_query = f"DELETE FROM {table_name} WHERE date < '{cutoff_date}'"  # noqa: S608
                    session.sql(delete_query).collect()

                    logger.info("Cleaned up %d old records from table %s", records_to_delete, table_name)

                return {
                    "cutoff_date": cutoff_date.isoformat(),
                    "records_deleted": records_to_delete,
                    "status": "SUCCESS",
                }

        except Exception as e:
            logger.error("Cache cleanup failed for table %s: %s", table_name, str(e))
            return {"status": "ERROR", "error": str(e)}
