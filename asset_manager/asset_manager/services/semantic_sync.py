"""Semantic sync service for time series data.

Extracts core semantic sync logic from Prefect tasks for reuse across
daily/weekly/monthly Dagster assets. Handles sync type determination,
tenant context management, and data fetching/storage.
"""

import logging
from datetime import (
    date,
    datetime,
    timedelta,
    timezone,
)
from typing import Any

from pydantic import BaseModel

from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.semantic_loader import fetch_metric_values
from asset_manager.services.utils import get_metric
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from query_manager.core.schemas import MetricDetail
from query_manager.semantic_manager.crud import SemanticManager
from query_manager.semantic_manager.models import SyncOperation, SyncStatus, SyncType

logger = logging.getLogger(__name__)


class SemanticSyncRequest(BaseModel):
    """Request parameters for semantic sync operation."""

    tenant_id: int
    tenant_identifier: str
    metric_id: str
    grain: Granularity
    sync_date: date
    force_full_sync: bool = False

    @property
    def metric_context(self) -> str:
        """Get metric context string for partitioning."""
        return f"{self.tenant_identifier}::{self.metric_id}"


class SemanticSyncResponse(BaseModel):
    """Response from semantic sync operation."""

    tenant_id: int
    tenant_identifier: str
    metric_id: str
    grain: Granularity
    sync_date: date
    records_processed: int
    dimensions_processed: int
    start_time: datetime
    end_time: datetime
    success: bool
    error_message: str | None = None

    @property
    def duration_seconds(self) -> float:
        """Calculate sync duration in seconds."""
        return (self.end_time - self.start_time).total_seconds()


class SemanticSyncService:
    """Service for semantic time series data synchronization."""

    def __init__(self, config: AppConfigResource, db: DbResource):
        self.config = config
        self.db = db

    async def sync_metric_time_series(self, request: SemanticSyncRequest) -> SemanticSyncResponse:
        """
        Sync metric time series data for a specific date and grain.

        This is the main entry point that orchestrates:
        1. Tenant context setup
        2. Data fetching and storage
        3. Cleanup and response
        """
        start_time = datetime.now(timezone.utc)

        try:
            # Set tenant context
            set_tenant_id(request.tenant_id)

            logger.info(
                "Starting semantic sync for %s on %s (%s)",
                request.metric_context,
                request.sync_date,
                request.grain.value,
            )

            # Perform the sync
            records_processed, dimensions_processed = await self._perform_sync(request)

            end_time = datetime.now(timezone.utc)

            return SemanticSyncResponse(
                tenant_id=request.tenant_id,
                tenant_identifier=request.tenant_identifier,
                metric_id=request.metric_id,
                grain=request.grain,
                sync_date=request.sync_date,
                records_processed=records_processed,
                dimensions_processed=dimensions_processed,
                start_time=start_time,
                end_time=end_time,
                success=True,
            )

        except Exception as e:
            logger.error("Semantic sync failed for %s: %s", request.metric_context, e)
            end_time = datetime.now(timezone.utc)

            return SemanticSyncResponse(
                tenant_id=request.tenant_id,
                tenant_identifier=request.tenant_identifier,
                metric_id=request.metric_id,
                grain=request.grain,
                sync_date=request.sync_date,
                records_processed=0,
                dimensions_processed=0,
                start_time=start_time,
                end_time=end_time,
                success=False,
                error_message=str(e),
            )

        finally:
            # Always reset tenant context
            reset_context()

    async def _determine_sync_type(
        self,
        metric_id: str,
        grain: Granularity,
        force_full: bool = False,
        dimension_name: str | None = None,
    ) -> SyncType:
        """
        Determine whether to perform FULL or INCREMENTAL sync.
        """
        if force_full:
            return SyncType.FULL

        try:
            async with self.db.session() as session:
                semantic_manager = SemanticManager(session)

                # Use existing CRUD method that handles both cases
                sync_type = await semantic_manager.metric_sync_status.determine_sync_type(
                    metric_id=metric_id,
                    grain=grain,
                    sync_operation=SyncOperation.SEMANTIC_SYNC,
                    dimension_name=dimension_name,
                )

                component_type = f"dimension: {dimension_name}" if dimension_name else "metric"
                logger.info(
                    "Determined sync type for %s - Metric: %s, Grain: %s, Sync Type: %s",
                    component_type,
                    metric_id,
                    grain.value,
                    sync_type.value,
                )
                return sync_type

        except Exception as e:
            component_type = f"dimension: {dimension_name}" if dimension_name else "metric"
            logger.warning(
                "Error determining sync type for %s - Metric: %s, Grain: %s, Error: %s. Defaulting to FULL sync.",
                component_type,
                metric_id,
                grain.value,
                str(e),
            )
            return SyncType.FULL

    async def _perform_sync(self, request: SemanticSyncRequest) -> tuple[int, int]:
        """
        Perform the actual data sync operation.

        Returns tuple of (records_processed, dimensions_processed)
        """
        records_processed = 0
        dimensions_processed = 0

        # Get metric details
        async with self.db.session() as session:
            metric = await get_metric(request.metric_id, session)

        # Determine sync type for base metric
        base_metric_sync_type = await self._determine_sync_type(
            metric_id=request.metric_id,
            grain=request.grain,
            force_full=request.force_full_sync,
            dimension_name=None,
        )

        # Fetch and store base metric data
        base_stats = await self._fetch_and_store_metric_time_series(
            request.tenant_id, metric, request.sync_date, request.grain, base_metric_sync_type
        )
        records_processed += base_stats["processed"]

        # Fetch and store dimensional data for all dimensions
        dimensional_stats = await self._fetch_and_store_dimensional_data(
            request.tenant_id, metric, request.sync_date, request.grain
        )
        dimensions_processed = len(dimensional_stats)
        records_processed += sum(stats["processed"] for stats in dimensional_stats)

        logger.info(
            "Processed %d records across %d dimensions for %s",
            records_processed,
            dimensions_processed,
            request.metric_context,
        )

        return records_processed, dimensions_processed

    def _calculate_grain_lookback(self, sync_type: SyncType, grain: Granularity) -> dict[str, int]:
        """Calculate lookback period for each grain based on sync type."""
        if grain == Granularity.DAY:
            # 365 days for full sync, 90 days (3 months) for incremental sync
            return {"days": 365 if sync_type == SyncType.FULL else 90}
        elif grain == Granularity.WEEK:
            # 104 weeks for full sync, 24 weeks (6 months) for incremental sync
            return {"weeks": 104 if sync_type == SyncType.FULL else 24}
        else:  # MONTH
            # 5 years for full sync, 12 months (1 year) for incremental sync
            return {"weeks": 260 if sync_type == SyncType.FULL else 52}

    def _calculate_sync_window(self, sync_date: date, grain: Granularity, sync_type: SyncType) -> tuple[date, date]:
        """Calculate the date window for data fetching based on grain and sync type."""
        lookback = self._calculate_grain_lookback(sync_type, grain)
        start_date = sync_date - timedelta(**lookback)
        end_date = sync_date
        return start_date, end_date

    async def _fetch_and_store_metric_time_series(
        self,
        tenant_id: int,
        metric: MetricDetail,
        sync_date: date,
        grain: Granularity,
        sync_type: SyncType,
    ) -> dict[str, int]:
        """Fetch and store metric time series values."""
        # Calculate date window based on sync type
        start_date, end_date = self._calculate_sync_window(sync_date, grain, sync_type)

        logger.info(
            "Starting time series sync - Metric: %s, Tenant: %s, " "Grain: %s, Sync Type: %s, Window: %s to %s",
            metric.metric_id,
            tenant_id,
            grain.value,
            sync_type.value,
            start_date,
            end_date,
        )

        try:
            async with self.db.session() as session:
                semantic_manager = SemanticManager(session)

                # Start the sync
                await semantic_manager.metric_sync_status.start_sync(
                    metric_id=metric.metric_id,
                    grain=grain,
                    sync_operation=SyncOperation.SEMANTIC_SYNC,
                    sync_type=sync_type,
                    start_date=start_date,
                    end_date=end_date,
                )

                try:
                    if sync_type == SyncType.FULL:
                        logger.info("Performing full sync - clearing existing data")
                        await semantic_manager.clear_metric_data(
                            metric_id=metric.metric_id,
                            tenant_id=tenant_id,
                            grain=grain,
                            start_date=start_date,
                            end_date=end_date,
                        )

                    # Fetch the metric values
                    values, fetch_stats = await fetch_metric_values(
                        metric=metric,
                        start_date=start_date,
                        end_date=end_date,
                        grain=grain,
                        config=self.config,
                        dimension_id=None,
                    )

                    # Upsert the metric values
                    upsert_stats = await semantic_manager.bulk_upsert_time_series(values=values)

                    # Complete the sync
                    await semantic_manager.metric_sync_status.end_sync(
                        metric_id=metric.metric_id,
                        grain=grain,
                        sync_operation=SyncOperation.SEMANTIC_SYNC,
                        sync_type=sync_type,
                        status=SyncStatus.SUCCESS,
                        records_processed=upsert_stats["processed"],
                    )

                    # Commit the changes
                    await session.commit()

                    logger.info(
                        "Time series sync complete - Metric: %s, Grain: %s, Processed: %d, Skipped: %d, Failed: %d",
                        metric.metric_id,
                        grain.value,
                        upsert_stats["processed"],
                        fetch_stats["skipped"],
                        upsert_stats["failed"],
                    )

                    return {
                        "processed": upsert_stats["processed"],
                        "skipped": fetch_stats["skipped"],
                        "failed": upsert_stats["failed"],
                        "total": fetch_stats["total"],
                    }

                except Exception as e:
                    await semantic_manager.metric_sync_status.end_sync(
                        metric_id=metric.metric_id,
                        grain=grain,
                        sync_operation=SyncOperation.SEMANTIC_SYNC,
                        sync_type=sync_type,
                        status=SyncStatus.FAILED,
                        error=str(e),
                    )
                    raise

        except Exception as e:
            logger.error(
                "Critical failure in time series sync - Metric: %s, Grain: %s, Error: %s",
                metric.metric_id,
                grain.value,
                str(e),
                exc_info=True,
            )
            raise

    async def _fetch_and_store_dimensional_data(
        self,
        tenant_id: int,
        metric: MetricDetail,
        sync_date: date,
        grain: Granularity,
    ) -> list[dict[str, int]]:
        """Fetch and store dimensional metric data for all dimensions."""
        # if the metric has no dimensions, return an empty list
        if not metric.dimensions:
            logger.info("Metric %s has no dimensions, skipping dimensional sync", metric.metric_id)
            return []
        dimensional_stats = []

        try:
            # Process metric dimensions

            for dimension in metric.dimensions:
                dimension_id = dimension.dimension_id
                try:
                    # Determine sync type for this specific dimension
                    dimension_sync_type = await self._determine_sync_type(
                        metric_id=metric.metric_id,
                        grain=grain,
                        dimension_name=dimension_id,
                    )

                    stats = await self._fetch_and_store_metric_dimensional_time_series(
                        tenant_id=tenant_id,
                        metric=metric,
                        dimension_id=dimension_id,
                        sync_date=sync_date,
                        grain=grain,
                        sync_type=dimension_sync_type,
                    )
                    dimensional_stats.append(stats)

                except Exception as e:
                    logger.warning("Failed to sync dimension %s for %s: %s", dimension_id, metric.metric_id, e)
                    dimensional_stats.append(
                        {
                            "processed": 0,
                            "skipped": 0,
                            "failed": 1,
                            "total": 0,
                            "dimension_id": dimension_id,
                            "sync_type": dimension_sync_type.value,
                            "error": str(e),
                        }
                    )
                    continue

            return dimensional_stats

        except Exception as e:
            logger.error("Failed to fetch dimensional data for %s: %s", metric.metric_id, e)
            return []

    async def _fetch_and_store_metric_dimensional_time_series(
        self,
        tenant_id: int,
        metric: MetricDetail,
        dimension_id: str,
        sync_date: date,
        grain: Granularity,
        sync_type: SyncType,
    ) -> dict[str, Any]:
        """Fetch and store metric dimensional time series data."""
        # Calculate date window based on sync type
        start_date, end_date = self._calculate_sync_window(sync_date, grain, sync_type)

        logger.info(
            "Starting dimensional time series sync - Metric: %s, Dimension: %s, "
            "Grain: %s, Sync Type: %s, Window: %s to %s",
            metric.metric_id,
            dimension_id,
            grain.value,
            sync_type.value,
            start_date,
            end_date,
        )

        try:
            async with self.db.session() as session:
                semantic_manager = SemanticManager(session)

                # Start the sync
                await semantic_manager.metric_sync_status.start_sync(
                    metric_id=metric.metric_id,
                    grain=grain,
                    sync_operation=SyncOperation.SEMANTIC_SYNC,
                    dimension_name=dimension_id,
                    sync_type=sync_type,
                    start_date=start_date,
                    end_date=end_date,
                )

                try:
                    if sync_type == SyncType.FULL:
                        logger.info("Performing full sync - clearing existing dimensional data")
                        await semantic_manager.clear_metric_data(
                            metric_id=metric.metric_id,
                            tenant_id=tenant_id,
                            grain=grain,
                            start_date=start_date,
                            end_date=end_date,
                            dimension_name=dimension_id,
                        )

                    # Fetch the metric values
                    values, fetch_stats = await fetch_metric_values(
                        metric=metric,
                        start_date=start_date,
                        end_date=end_date,
                        grain=grain,
                        config=self.config,
                        dimension_id=dimension_id,
                    )

                    # Upsert the dimensional values
                    upsert_stats = await semantic_manager.bulk_upsert_dimensional_time_series(values=values)

                    # Complete the sync
                    await semantic_manager.metric_sync_status.end_sync(
                        metric_id=metric.metric_id,
                        grain=grain,
                        sync_operation=SyncOperation.SEMANTIC_SYNC,
                        dimension_name=dimension_id,
                        sync_type=sync_type,
                        status=SyncStatus.SUCCESS,
                        records_processed=upsert_stats["processed"],
                    )

                    # Commit the changes
                    await session.commit()

                    logger.info(
                        "Dimensional sync complete - Metric: %s, Dimension: %s, Grain: %s, "
                        "Processed: %d, Skipped: %d, Failed: %d",
                        metric.metric_id,
                        dimension_id,
                        grain.value,
                        upsert_stats["processed"],
                        fetch_stats["skipped"],
                        upsert_stats["failed"],
                    )

                    return {
                        "dimension_id": dimension_id,
                        "processed": upsert_stats["processed"],
                        "skipped": fetch_stats["skipped"],
                        "failed": upsert_stats["failed"],
                        "total": fetch_stats["total"],
                        "sync_type": sync_type.value,
                        "error": None,
                    }

                except Exception as e:
                    await semantic_manager.metric_sync_status.end_sync(
                        metric_id=metric.metric_id,
                        grain=grain,
                        sync_operation=SyncOperation.SEMANTIC_SYNC,
                        dimension_name=dimension_id,
                        status=SyncStatus.FAILED,
                        sync_type=sync_type,
                        error=str(e),
                    )
                    raise

        except Exception as e:
            logger.error(
                "Critical failure in dimensional time series sync - Metric: %s, Dimension: %s, Grain: %s, Error: %s",
                metric.metric_id,
                dimension_id,
                grain.value,
                str(e),
                exc_info=True,
            )
            raise
