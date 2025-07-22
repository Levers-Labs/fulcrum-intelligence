"""
Snowflake Cache Manager for semantic data operations.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any

from sqlmodel.ext.asyncio.session import AsyncSession

from commons.clients.snowflake import SnowflakeClient
from commons.models.enums import Granularity
from commons.utilities.context import get_tenant_id
from query_manager.core.models import MetricCacheGrainConfig
from query_manager.semantic_manager.crud import SemanticManager
from query_manager.semantic_manager.models import (
    SyncOperation,
    SyncStatus,
    SyncType,
    TenantSyncStatus,
)

logger = logging.getLogger(__name__)


class SnowflakeSemanticCacheManager(SemanticManager):
    """Extended SemanticManager with Snowflake cache capabilities."""

    def __init__(self, session: AsyncSession, snowflake_client: SnowflakeClient | None = None):
        super().__init__(session)
        self.snowflake_client = snowflake_client

    def _ensure_snowflake_client(self) -> SnowflakeClient:
        """Ensure snowflake_client is available for methods that require it."""
        if self.snowflake_client is None:
            raise ValueError("Snowflake client is required for this operation but was not provided")
        return self.snowflake_client

    async def start_tenant_cache_operation(
        self,
        sync_operation: SyncOperation,
        grain: Granularity,
        run_info: dict | None = None,
    ) -> TenantSyncStatus:
        """Start a new tenant cache operation."""
        return await self.tenant_sync_status.start_sync(
            sync_operation=sync_operation,
            grain=grain,
            run_info=run_info,
        )

    async def end_tenant_cache_operation(
        self,
        sync_operation: SyncOperation,
        grain: Granularity,
        status: SyncStatus,
        metrics_processed: int | None = None,
        metrics_succeeded: int | None = None,
        metrics_failed: int | None = None,
        run_info: dict | None = None,
        error: str | None = None,
    ) -> TenantSyncStatus:
        """Complete a tenant cache operation."""
        return await self.tenant_sync_status.end_sync(
            sync_operation=sync_operation,
            grain=grain,
            status=status,
            metrics_processed=metrics_processed,
            metrics_succeeded=metrics_succeeded,
            metrics_failed=metrics_failed,
            run_info=run_info,
            error=error,
        )

    def _get_tenant_name(self) -> str:
        """Get tenant name from context or config."""
        # For now, we'll use tenant_id as tenant_name
        # TODO: enhance to get actual tenant name from config
        tenant_id = get_tenant_id()
        if tenant_id is None:
            raise ValueError("Tenant ID not set in context")
        return f"tenant_{tenant_id}"

    def _generate_table_name(self) -> str:
        """Generate Snowflake table name for metric cache."""
        tenant_name = self._get_tenant_name()

        # Clean tenant name to be table name safe
        clean_tenant_name = tenant_name.replace("-", "_").replace(" ", "_").lower()

        # Single table per tenant for all metrics and grains
        table_name = f"{clean_tenant_name}_metric_time_series"

        return table_name

    def _calculate_cache_date_range(
        self,
        sync_type: SyncType,
        grain_config: MetricCacheGrainConfig,
    ) -> tuple[date, date]:
        """Calculate date range for cache operation based on config."""
        end_date = date.today() - timedelta(days=1)

        if sync_type == SyncType.FULL:
            # Use initial sync period for full cache
            delta_days = grain_config.initial_sync_period
        else:
            # Use delta sync period for incremental cache
            delta_days = grain_config.delta_sync_period

        start_date = end_date - timedelta(days=delta_days)
        return start_date, end_date

    async def cache_metric_time_series(
        self,
        metric_id: str,
        grain: Granularity,
        sync_type: SyncType,
        start_date: date,
        end_date: date,
        values: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Cache metric time series data to Snowflake from provided values."""
        sync_operation = SyncOperation.SNOWFLAKE_CACHE

        try:
            # Generate table name
            table_name = self._generate_table_name()

            # Track sync status for this metric using existing sync tracking
            await self.metric_sync_status.start_sync(
                metric_id=metric_id,
                grain=grain,
                sync_operation=sync_operation,
                sync_type=sync_type,
                start_date=start_date,
                end_date=end_date,
            )

            # Convert values to dictionary format for Snowflake
            # The values are already in the correct format from fetch_metric_values
            cache_data = [
                {
                    "metric_id": value["metric_id"],
                    "date": value["date"].isoformat() if hasattr(value["date"], "isoformat") else value["date"],
                    "grain": value["grain"].value if hasattr(value["grain"], "value") else value["grain"],
                    "value": value["value"],
                    "cached_at": datetime.now().isoformat(),
                }
                for value in values
            ]

            # Create/update table in Snowflake using client method
            snowflake_client = self._ensure_snowflake_client()
            await snowflake_client.create_or_update_metric_time_series(
                table_name=table_name,
                data=cache_data,
                is_full_sync=(sync_type == SyncType.FULL),
            )

            # Calculate cache size (approximate)
            cache_size_mb = len(cache_data) * 0.1  # Rough estimate

            # End sync operation
            await self.metric_sync_status.end_sync(
                metric_id=metric_id,
                grain=grain,
                sync_operation=sync_operation,
                sync_type=sync_type,
                status=SyncStatus.SUCCESS,
                records_processed=len(cache_data),
            )

            return {
                "status": "success",
                "time_series_stats": {
                    "processed": len(cache_data),
                    "skipped": 0,
                    "failed": 0,
                    "total": len(cache_data),
                },
                "cache_size_mb": cache_size_mb,
                "table_name": table_name,
            }

        except Exception as e:
            logger.error("Failed to cache metric time series from values: %s", str(e))
            await self.metric_sync_status.end_sync(
                metric_id=metric_id,
                grain=grain,
                sync_operation=sync_operation,
                sync_type=sync_type,
                status=SyncStatus.FAILED,
                error=str(e),
            )
            return {
                "status": "failed",
                "time_series_stats": {
                    "processed": 0,
                    "skipped": 0,
                    "failed": len(values),
                    "total": len(values),
                },
                "table_name": table_name,
                "error": str(e),
            }

    async def validate_cache_integrity(
        self,
        metric_id: str,
        grain: Granularity,
    ) -> dict[str, Any]:
        """Validate cache integrity by comparing with source data."""
        try:
            # Generate table name
            table_name = self._generate_table_name()

            # Get source data count
            source_data = await self.get_metric_time_series(
                metric_id=metric_id,
                grain=grain,
            )

            source_count = len(source_data)

            # Get cached data count using client method
            snowflake_client = self._ensure_snowflake_client()
            cached_count = await snowflake_client.get_table_row_count(table_name)

            # Calculate integrity metrics
            integrity_ratio = cached_count / source_count if source_count > 0 else 0
            is_valid = integrity_ratio >= 0.95  # 95% threshold

            return {
                "table_name": table_name,
                "source_count": source_count,
                "cached_count": cached_count,
                "integrity_ratio": integrity_ratio,
                "is_valid": is_valid,
                "validation_status": "PASSED" if is_valid else "FAILED",
            }

        except Exception as e:
            logger.error("Cache validation failed: %s", str(e))
            return {
                "table_name": table_name if "table_name" in locals() else None,
                "source_count": 0,
                "cached_count": 0,
                "integrity_ratio": 0.0,
                "is_valid": False,
                "validation_status": "ERROR",
                "error": str(e),
            }

    async def get_cache_performance_metrics(self) -> dict[str, Any]:
        """Get performance metrics for cached data."""
        try:
            # Generate table name
            table_name = self._generate_table_name()

            # Get performance metrics using client method
            snowflake_client = self._ensure_snowflake_client()
            metrics = await snowflake_client.get_table_performance_metrics(table_name)

            # Add table name to the response
            metrics["table_name"] = table_name

            return metrics

        except Exception as e:
            logger.error("Failed to get cache performance metrics: %s", str(e))
            return {
                "table_name": table_name if "table_name" in locals() else None,
                "status": "ERROR",
                "error": str(e),
            }

    async def cleanup_old_cache_data(
        self,
        retention_days: int = 730,
    ) -> dict[str, Any]:
        """Clean up old cached data beyond retention period."""
        try:
            # Generate table name
            table_name = self._generate_table_name()

            # Cleanup old data using client method
            snowflake_client = self._ensure_snowflake_client()
            cleanup_result = await snowflake_client.cleanup_old_table_data(
                table_name=table_name,
                retention_days=retention_days,
            )

            # Add table name to the response
            cleanup_result["table_name"] = table_name

            return cleanup_result

        except Exception as e:
            logger.error("Cache cleanup failed: %s", str(e))
            return {
                "table_name": table_name if "table_name" in locals() else None,
                "status": "ERROR",
                "error": str(e),
            }
