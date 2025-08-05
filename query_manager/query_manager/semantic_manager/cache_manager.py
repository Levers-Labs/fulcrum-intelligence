"""
Snowflake Cache Manager for semantic data operations.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any

from fastapi import HTTPException
from sqlalchemy import and_, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.clients.snowflake import SnowflakeClient
from commons.models.enums import Granularity
from commons.utilities.context import get_tenant_id
from commons.utilities.pagination import PaginationParams
from query_manager.core.crud import CRUDMetricCacheConfig
from query_manager.core.models import MetricCacheConfig, MetricCacheGrainConfig
from query_manager.semantic_manager.crud import SemanticManager
from query_manager.semantic_manager.models import (
    MetricSyncStatus,
    SyncOperation,
    SyncStatus,
    SyncType,
    TenantSyncStatus,
)

logger = logging.getLogger(__name__)


class SnowflakeSemanticCacheManager(SemanticManager):
    """Extended SemanticManager with Snowflake cache capabilities.

    Args:
        session: Async database session
        snowflake_client: Optional Snowflake client for cache operations
        tenant_identifier: Optional tenant identifier from insights backend for table naming
    """

    def __init__(
        self,
        session: AsyncSession,
        snowflake_client: SnowflakeClient | None = None,
        tenant_identifier: str | None = None,
    ):
        super().__init__(session)
        self.snowflake_client = snowflake_client
        self.tenant_identifier = tenant_identifier

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

    def _get_tenant_identifier(self) -> str:
        """Get tenant name from insights backend or fallback to tenant_id."""
        # Use tenant name from insights backend if available
        if self.tenant_identifier:
            return self.tenant_identifier

        # Fallback to tenant_id format for backward compatibility
        tenant_id = get_tenant_id()
        if tenant_id is None:
            raise ValueError("Tenant ID not set in context")
        return f"tenant_{tenant_id}"

    def _generate_table_name(self) -> str:
        """Generate Snowflake table name for metric cache."""
        tenant_name = self._get_tenant_identifier()

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

    async def get_tenant_sync_history(
        self,
        params: PaginationParams,
        sync_operation: SyncOperation | None = None,
        grain: Granularity | None = None,
        sync_status: SyncStatus | None = None,
    ) -> tuple[list[TenantSyncStatus], int]:
        """Get paginated tenant sync history with optional filtering using ORM-based pagination."""
        # Build filter parameters
        filter_params = {}
        if sync_operation:
            filter_params["sync_operation"] = sync_operation.value
        if grain:
            filter_params["grain"] = grain.value
        if sync_status:
            filter_params["sync_status"] = sync_status.value

        # Use the built-in paginate method with proper filtering and ordering
        return await self.tenant_sync_status.paginate(params, filter_params)

    async def get_comprehensive_cache_info(self) -> dict[str, Any]:
        """Get comprehensive cache information including table info and performance metrics."""
        try:
            # Generate table name
            table_name = self._generate_table_name()

            self._ensure_snowflake_client()
            performance_metrics = await self.snowflake_client.get_table_performance_metrics(table_name)  # type: ignore
            return {
                "table_name": table_name,
                "snowflake_connected": True,
                # Table/data information from Snowflake
                "date_range_start": performance_metrics.get("date_range", {}).get("min_date"),
                "date_range_end": performance_metrics.get("date_range", {}).get("max_date"),
                "total_records": performance_metrics.get("row_count", 0),
                "unique_dates": performance_metrics.get("unique_dates", 0),
            }

        except Exception as e:
            logger.error("Failed to get comprehensive cache info: %s", str(e))
            raise HTTPException(status_code=500, detail=f"Failed to get comprehensive cache info: {str(e)}") from e

    async def get_metric_cache_configs(
        self,
        params: PaginationParams,
        metric_ids: list[str] | None = None,
        is_enabled: bool | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """Get enhanced cache configurations with sync information."""
        # Create the CRUD instance
        cache_crud = CRUDMetricCacheConfig(model=MetricCacheConfig, session=self.session)

        # Build filter parameters for ORM-based filtering
        filter_params: dict[str, Any] = {}
        if metric_ids:
            filter_params["metric_ids"] = metric_ids
        if is_enabled is not None:
            filter_params["is_enabled"] = is_enabled

        # Get cache configs using built-in paginate method
        cache_configs, count = await cache_crud.paginate(params, filter_params)

        if not cache_configs:
            return [], count

        # Get all metric IDs from the cache configs
        config_metric_ids = [config.metric_id for config in cache_configs]
        tenant_id = get_tenant_id()

        # Fetch sync data for all metrics in one query to avoid N+1 problem
        # Use a subquery to get the latest sync status for each metric
        latest_sync_subquery = (
            select(
                MetricSyncStatus.metric_id,
                MetricSyncStatus.last_sync_at,
                MetricSyncStatus.sync_status,
                func.row_number()
                .over(partition_by=MetricSyncStatus.metric_id, order_by=MetricSyncStatus.last_sync_at.desc())  # type: ignore
                .label("rn"),
            )
            .where(
                and_(
                    MetricSyncStatus.tenant_id == tenant_id,  # type: ignore
                    MetricSyncStatus.metric_id.in_(config_metric_ids),  # type: ignore
                    MetricSyncStatus.sync_operation == SyncOperation.SNOWFLAKE_CACHE,  # type: ignore
                )
            )
            .subquery()
        )

        # Query: Get snapshot date ranges (actual data date ranges across all syncs)
        snapshot_dates_query = (
            select(  # type: ignore
                MetricSyncStatus.metric_id,
                func.min(MetricSyncStatus.start_date).label("first_snapshot_date"),
                func.max(MetricSyncStatus.end_date).label("last_snapshot_date"),
            )
            .where(
                and_(
                    MetricSyncStatus.tenant_id == tenant_id,  # type: ignore
                    MetricSyncStatus.metric_id.in_(config_metric_ids),  # type: ignore
                    MetricSyncStatus.sync_operation == SyncOperation.SNOWFLAKE_CACHE,  # type: ignore
                )
            )
            .group_by(MetricSyncStatus.metric_id)
        )

        # Get only the latest sync status for each metric (rn = 1)
        latest_sync_query = select(
            latest_sync_subquery.c.metric_id,
            latest_sync_subquery.c.last_sync_at,
            latest_sync_subquery.c.sync_status,
        ).where(latest_sync_subquery.c.rn == 1)

        sync_result = await self.session.execute(latest_sync_query)
        sync_data = {row.metric_id: row for row in sync_result.mappings()}

        snapshot_result = await self.session.execute(snapshot_dates_query)
        snapshot_data = {row.metric_id: row for row in snapshot_result.mappings()}

        # Enhance cache configs with sync information
        enhanced_configs = []
        for config in cache_configs:
            sync_info = sync_data.get(config.metric_id)
            snapshot_info = snapshot_data.get(config.metric_id)

            # Convert config to dict while preserving all original fields
            config_dict = {
                "id": config.id,
                "metric_id": config.metric_id,
                "is_enabled": config.is_enabled,
                "last_sync_date": sync_info.last_sync_at if sync_info else None,
                "sync_status": sync_info.sync_status.value if sync_info else None,
                "first_snapshot_date": snapshot_info.first_snapshot_date if snapshot_info else None,
                "last_snapshot_date": snapshot_info.last_snapshot_date if snapshot_info else None,
            }
            enhanced_configs.append(config_dict)

        return enhanced_configs, count

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
