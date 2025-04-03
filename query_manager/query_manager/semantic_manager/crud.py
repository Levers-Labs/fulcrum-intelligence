"""
CRUD operations for semantic data.
"""

import logging
from collections.abc import AsyncGenerator
from datetime import date, datetime
from typing import Any, TypeVar, cast

from pydantic import BaseModel
from sqlalchemy import (
    Insert,
    Select,
    and_,
    delete,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.crud import CRUDBase
from commons.db.filters import BaseFilter
from commons.db.models import BaseTimeStampedTenantModel
from commons.models.enums import Granularity
from commons.utilities.context import get_tenant_id
from query_manager.core.models import Metric
from query_manager.semantic_manager.filters import TargetFilter
from query_manager.semantic_manager.models import (
    MetricDimensionalTimeSeries,
    MetricSyncStatus,
    MetricTarget,
    MetricTimeSeries,
    SyncEvent,
    SyncStatus,
    SyncType,
)
from query_manager.semantic_manager.schemas import (
    MetricTargetOverview,
    TargetCreate,
    TargetStatus,
    TargetUpdate,
)

logger = logging.getLogger(__name__)

ModelType = TypeVar("ModelType", bound=BaseTimeStampedTenantModel)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)
FilterType = TypeVar("FilterType", bound=BaseFilter)


class CRUDSemantic(CRUDBase[ModelType, CreateSchemaType, UpdateSchemaType, FilterType]):
    """Base CRUD class for semantic models."""

    BATCH_SIZE = 1000  # Configurable batch size for bulk operations

    def _get_unique_fields(self) -> list[str]:
        """Get unique constraint fields for the model."""
        unique_fields = ["metric_id", "tenant_id"]

        if hasattr(self.model, "date") and hasattr(self.model, "grain"):
            unique_fields.extend(["date", "grain"])

        if hasattr(self.model, "dimension_name") and hasattr(self.model, "dimension_slice"):
            unique_fields.extend(["dimension_name", "dimension_slice"])

        if hasattr(self.model, "sync_type"):
            unique_fields.append("sync_type")

        return unique_fields

    def _get_update_dict(self, stmt: Insert) -> dict[str, Any]:
        """Get update dictionary for upsert operation."""
        unique_fields = self._get_unique_fields()
        update_dict = {
            c.name: stmt.excluded[c.name]  # type: ignore
            for c in self.model.__table__.columns  # type: ignore
            if c.name not in unique_fields and c.name != "id"
        }
        update_dict["updated_at"] = func.now()
        return update_dict

    async def _execute_bulk_upsert(self, batch: list[dict[str, Any]]) -> int:
        """Execute bulk upsert for a batch of records."""
        try:
            stmt = insert(self.model).values(batch)
            update_dict = self._get_update_dict(stmt)
            stmt = stmt.on_conflict_do_update(index_elements=self._get_unique_fields(), set_=update_dict)
            await self.session.execute(stmt)
            return len(batch)
        except SQLAlchemyError as e:
            logger.error("Bulk upsert failed for batch: %s", e)
            # Rollback the failed batch
            await self.session.rollback()
            raise

    async def bulk_upsert(self, objects: list[dict[str, Any]], batch_size: int | None = None) -> dict[str, Any]:
        """
        Enhanced bulk upsert with batching, error handling, and performance tracking.

        Args:
            objects: List of objects to upsert
            batch_size: Optional custom batch size for this operation

        Returns:
            Dict containing operation statistics
        """
        if not objects:
            return {"processed": 0, "failed": 0, "total": 0}

        stats = {"processed": 0, "failed": 0, "total": len(objects)}
        batch_size = batch_size or self.BATCH_SIZE

        logger.info(
            "Starting bulk upsert for %d records of %s with batch size %d",
            len(objects),
            self.model.__name__,
            batch_size,
        )

        try:
            # Process in batches
            for i in range(0, len(objects), batch_size):
                batch = objects[i : i + batch_size]
                try:
                    processed = await self._execute_bulk_upsert(batch)
                    stats["processed"] += processed

                    logger.debug(
                        "Processed batch %d: %d/%d records successful", i // batch_size + 1, processed, len(batch)
                    )
                except SQLAlchemyError as e:
                    stats["failed"] += len(batch)
                    logger.error("Failed to process batch %d: %s", i // batch_size + 1, str(e))
                    # Continue with next batch instead of failing entire operation
                    continue

            await self.session.flush()
            logger.info(
                "Bulk upsert completed: %d processed, %d failed out of %d total records",
                stats["processed"],
                stats["failed"],
                stats["total"],
            )

            return stats

        except Exception as e:
            logger.error("Bulk upsert operation failed: %s", str(e))
            await self.session.rollback()
            raise

    async def clear_data(
        self,
        tenant_id: int,
        metric_id: str,
        grain: Granularity,
        start_date: date | None = None,
        end_date: date | None = None,
        dimension_name: str | None = None,
        dimension_slice: str | None = None,
    ) -> dict[str, Any]:
        """
        Enhanced clear data with error handling and logging.

        Returns:
            Dict containing an operation result
        """
        logger.info(
            "Clearing data for metric %s tenant %d grain %s date range: %s to %s",
            metric_id,
            tenant_id,
            grain,
            start_date,
            end_date,
        )

        try:
            conditions = [
                self.model.tenant_id == tenant_id,  # type: ignore
                self.model.metric_id == metric_id,  # type: ignore
                self.model.grain == grain,  # type: ignore
            ]

            if start_date:
                conditions.append(self.model.date >= start_date)  # type: ignore
            if end_date:
                conditions.append(self.model.date <= end_date)  # type: ignore
            if dimension_name:
                conditions.append(self.model.dimension_name == dimension_name)  # type: ignore
            if dimension_slice:
                conditions.append(self.model.dimension_slice == dimension_slice)  # type: ignore

            stmt = delete(self.model).where(and_(*conditions))
            result = await self.session.execute(stmt)
            await self.session.commit()

            rows_deleted = result.rowcount  # type: ignore
            logger.info("Successfully cleared %d records", rows_deleted)

            return {
                "success": True,
                "rows_deleted": rows_deleted,
                "message": "Successfully cleared %d records" % rows_deleted,
            }

        except SQLAlchemyError as e:
            logger.error("Failed to clear data: %s", str(e))
            await self.session.rollback()
            return {"success": False, "rows_deleted": 0, "error": str(e)}


class CRUDMetricTimeSeries(CRUDSemantic[MetricTimeSeries, BaseModel, BaseModel, BaseFilter]):
    """CRUD operations for MetricTimeSeries."""

    async def get_time_series_query(
        self,
        metric_ids: list[str],
        grain: Granularity,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> Select:
        """Get a query for time series data with optional date filtering."""
        query = self.get_select_query()
        query = query.where(
            and_(self.model.metric_id.in_(metric_ids), self.model.grain == grain)  # type: ignore
        ).order_by(
            self.model.date.desc(), self.model.metric_id  # type: ignore
        )
        # Apply date filters only if they are provided
        if start_date:
            query = query.where(self.model.date >= start_date)  # type: ignore
        if end_date:
            query = query.where(self.model.date <= end_date)  # type: ignore
        return query

    async def get_time_series(
        self,
        metric_id: str,
        grain: Granularity,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[MetricTimeSeries]:
        """Get time series data for a metric with optional date filtering."""
        return await self.get_multi_metric_time_series(
            metric_ids=[metric_id],
            grain=grain,
            start_date=start_date,
            end_date=end_date,
        )

    async def get_multi_metric_time_series(
        self,
        metric_ids: list[str],
        grain: Granularity,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[MetricTimeSeries]:
        """Get time series data for multiple metrics."""
        # Get the query without executing it
        query = await self.get_time_series_query(metric_ids, grain, start_date=start_date, end_date=end_date)  # type: ignore
        # Execute the query
        result = await self.session.execute(query)
        # Return the results
        return cast(list[MetricTimeSeries], list(result.scalars().all()))


class CRUDMetricDimensionalTimeSeries(CRUDSemantic[MetricDimensionalTimeSeries, BaseModel, BaseModel, BaseFilter]):
    """CRUD operations for MetricDimensionalTimeSeries."""

    async def get_dimensional_time_series_query(
        self,
        metric_id: str,
        grain: Granularity,
        start_date: date | None = None,
        end_date: date | None = None,
        dimension_names: list[str] | None = None,
    ) -> Select:
        """Get a query for dimensional time series data with optional date and dimension filtering."""
        query = self.get_select_query()
        query = query.where(and_(self.model.metric_id == metric_id, self.model.grain == grain))  # type: ignore
        if start_date:
            query = query.where(self.model.date >= start_date)  # type: ignore
        if end_date:
            query = query.where(self.model.date <= end_date)  # type: ignore
        if dimension_names:
            query = query.where(self.model.dimension_name.in_(dimension_names))  # type: ignore
        # Add order by
        query = query.order_by(self.model.date.desc(), self.model.dimension_name, self.model.dimension_slice)  # type: ignore
        return query

    async def get_dimensional_time_series(
        self,
        metric_id: str,
        grain: Granularity,
        start_date: date | None = None,
        end_date: date | None = None,
        dimension_names: list[str] | None = None,
    ) -> list[MetricDimensionalTimeSeries]:
        """
        Get time series data for a metric with dimensions.
        By default, all dimensions values are returned.
        If dimension_names are provided, only the specified dimensions values are returned.
        """
        # Get the query without executing it
        query = await self.get_dimensional_time_series_query(
            metric_id, grain, start_date=start_date, end_date=end_date, dimension_names=dimension_names
        )
        # Execute the query
        result = await self.session.execute(query)
        # Return the results
        return cast(list[MetricDimensionalTimeSeries], list(result.scalars().all()))


class CRUDMetricSyncStatus(CRUDSemantic[MetricSyncStatus, BaseModel, BaseModel, BaseFilter]):
    """CRUD operations for MetricSyncStatus."""

    async def get_sync_status(
        self,
        tenant_id: int,
        metric_id: str,
        grain: Granularity | None = None,
        dimension_name: str | None = None,
    ) -> list[MetricSyncStatus]:
        """Get sync status for a metric."""
        conditions = [
            self.model.tenant_id == tenant_id,
            self.model.metric_id == metric_id,
        ]

        if grain:
            conditions.append(self.model.grain == grain)
        if dimension_name:
            conditions.append(self.model.dimension_name == dimension_name)

        query = select(self.model).where(and_(*conditions)).order_by(self.model.last_sync_at.desc())  # type: ignore
        result = await self.session.execute(query)
        return cast(list[MetricSyncStatus], list(result.scalars().all()))

    async def update_sync_status(
        self,
        metric_id: str,
        grain: Granularity,
        sync_status: SyncStatus,
        sync_type: SyncType,
        start_date: date,
        end_date: date,
        dimension_name: str | None = None,
        records_processed: int | None = None,
        error: str | None = None,
        add_to_history: bool = False,
    ) -> MetricSyncStatus:
        """Update sync status for a metric."""
        query = select(self.model).where(
            and_(
                self.model.metric_id == metric_id,  # type: ignore
                self.model.grain == grain,  # type: ignore
                self.model.dimension_name == dimension_name,  # type: ignore
                self.model.sync_type == sync_type,  # type: ignore
            )
        )
        result = await self.session.execute(query)
        existing = result.scalars().first()

        # Get current time
        now = datetime.now()

        if existing:
            # Prepare update data
            update_data = {
                "sync_status": sync_status,
                "start_date": start_date,
                "end_date": end_date,
                "last_sync_at": now,
                "records_processed": records_processed,
                "error": error,
                "updated_at": now,
            }
            existing = cast(MetricSyncStatus, existing)
            # Add to history only if flag is True
            if add_to_history:
                history_entry: SyncEvent = {
                    "sync_status": existing.sync_status,
                    "sync_type": existing.sync_type,
                    "start_date": existing.start_date.isoformat(),
                    "end_date": existing.end_date.isoformat(),
                    "records_processed": existing.records_processed,
                    "error": existing.error,
                    "last_sync_at": existing.last_sync_at.isoformat(),
                    "updated_at": now.isoformat(),
                }

                current_history = existing.history or []
                # Keep last 20 entries
                update_data["history"] = [history_entry] + current_history[:19]

            return await self.update(obj=existing, obj_in=update_data)
        else:
            # Create new record data
            create_data = MetricSyncStatus(
                metric_id=metric_id,
                grain=grain,
                dimension_name=dimension_name,
                sync_status=sync_status,
                sync_type=sync_type,
                start_date=start_date,
                end_date=end_date,
                last_sync_at=now,
                records_processed=records_processed,
                error=error,
                history=[],
            )
            return await self.create(obj_in=create_data)

    async def start_sync(
        self,
        metric_id: str,
        grain: Granularity,
        sync_type: SyncType,
        start_date: date,
        end_date: date,
        dimension_name: str | None = None,
    ) -> None:
        """Start a new sync."""
        await self.update_sync_status(
            metric_id=metric_id,
            grain=grain,
            sync_status=SyncStatus.RUNNING,
            sync_type=sync_type,
            start_date=start_date,
            end_date=end_date,
            dimension_name=dimension_name,
            add_to_history=True,
        )

    async def end_sync(
        self,
        metric_id: str,
        grain: Granularity,
        sync_type: SyncType,
        status: SyncStatus,
        records_processed: int | None = None,
        dimension_name: str | None = None,
        error: str | None = None,
    ) -> None:
        """Complete a sync."""
        await self.update_sync_status(
            metric_id=metric_id,
            grain=grain,
            sync_status=status,
            sync_type=sync_type,
            start_date=datetime.now().date(),
            end_date=datetime.now().date(),
            dimension_name=dimension_name,
            records_processed=records_processed,
            error=error,
            add_to_history=False,
        )


class CRUDMetricTarget(CRUDSemantic[MetricTarget, TargetCreate, TargetUpdate, TargetFilter]):
    """CRUD operations for metric targets."""

    filter_class = TargetFilter

    def _get_unique_fields(self) -> list[str]:
        """Get unique constraint fields for the model."""
        return ["metric_id", "grain", "target_date", "tenant_id"]

    async def delete_targets(
        self,
        metric_id: str,
        grain: Granularity,
        target_date: date | None = None,
        target_date_ge: date | None = None,
        target_date_le: date | None = None,
    ) -> bool:
        """Delete a target for a given metric, grain, and target date."""
        tenant_id = get_tenant_id()
        if tenant_id is None:
            raise ValueError("Tenant ID is not set")
        query = delete(self.model).where(
            and_(
                self.model.metric_id == metric_id,  # type: ignore
                self.model.grain == grain,  # type: ignore
                self.model.tenant_id == tenant_id,  # type: ignore
            )
        )

        # Apply date based filters
        if target_date:
            query = query.where(self.model.target_date == target_date)  # type: ignore
        if target_date_ge:
            query = query.where(self.model.target_date >= target_date_ge)  # type: ignore
        if target_date_le:
            query = query.where(self.model.target_date <= target_date_le)  # type: ignore

        try:
            result = await self.session.execute(query)
            await self.session.commit()
            return result.rowcount > 0  # type: ignore
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"Error deleting target: {e}")
            raise

    async def bulk_upsert_targets(self, targets: list[dict[str, Any]], batch_size: int | None = None) -> dict[str, Any]:
        """Bulk upsert targets."""
        tenant_id = get_tenant_id()
        if tenant_id is None:
            raise ValueError("Tenant ID is not set")
        # Add tenant_id to each target
        for target in targets:
            target["tenant_id"] = tenant_id
        stats = await self.bulk_upsert(targets, batch_size)
        # Commit the transaction
        await self.session.commit()
        return stats

    async def get_metrics_targets_list(self) -> tuple[list[MetricTargetOverview], int]:
        """Get list of all metrics with their target status."""
        tenant_id = get_tenant_id()
        if tenant_id is None:
            raise ValueError("Tenant ID is required")

        # Get all metrics first (we need this to show metrics even without targets)
        metrics_query = (
            select(Metric.metric_id, Metric.label, Metric.aim)  # type: ignore
            .where(Metric.tenant_id == tenant_id)
            .order_by(Metric.label)
        )

        # Get target information
        targets_query = (
            select(self.model.metric_id, self.model.grain, func.max(self.model.target_date).label("through_date"))
            .where(self.model.tenant_id == tenant_id)
            .group_by(self.model.metric_id, self.model.grain)
        )

        # Execute queries
        metrics_result = await self.session.execute(metrics_query)
        targets_result = await self.session.execute(targets_query)

        # Process metrics
        metrics = {
            row.metric_id: {
                "metric_id": row.metric_id,
                "label": row.label,
                "aim": row.aim,
                "periods": {grain: TargetStatus(has_targets=False, through_date=None) for grain in Granularity},
            }
            for row in metrics_result.mappings()
        }

        # Update with target information
        for row in targets_result.mappings():
            if row.metric_id in metrics and row.grain:
                metrics[row.metric_id]["periods"][row.grain] = TargetStatus(
                    has_targets=True, through_date=row.through_date
                )

        # Convert to list of MetricTargetOverview objects
        overviews = [MetricTargetOverview(**metric_data) for metric_data in metrics.values()]

        return overviews, len(overviews)


class SemanticManager:
    """Manager class for semantic operations."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.metric_time_series = CRUDMetricTimeSeries(MetricTimeSeries, session)
        self.metric_dimensional_time_series = CRUDMetricDimensionalTimeSeries(MetricDimensionalTimeSeries, session)
        self.metric_sync_status = CRUDMetricSyncStatus(MetricSyncStatus, session)
        self.metric_target = CRUDMetricTarget(MetricTarget, session)

    async def bulk_upsert_time_series(
        self, values: list[dict[str, Any]], batch_size: int | None = None
    ) -> dict[str, Any]:
        """Enhanced bulk upsert time series values with statistics."""
        return await self.metric_time_series.bulk_upsert(values, batch_size)

    async def bulk_upsert_dimensional_time_series(
        self, values: list[dict[str, Any]], batch_size: int | None = None
    ) -> dict[str, Any]:
        """Enhanced bulk upsert dimensional time series values with statistics."""
        return await self.metric_dimensional_time_series.bulk_upsert(values, batch_size)

    async def clear_metric_data(
        self,
        metric_id: str,
        tenant_id: int,
        grain: Granularity,
        start_date: date,
        end_date: date,
        dimension_name: str | None = None,
    ) -> dict[str, Any]:
        """Clear metric data for a given time range."""
        if dimension_name:
            return await self.metric_dimensional_time_series.clear_data(
                tenant_id=tenant_id,
                metric_id=metric_id,
                grain=grain,
                start_date=start_date,
                end_date=end_date,
                dimension_name=dimension_name,
            )
        return await self.metric_time_series.clear_data(
            tenant_id=tenant_id,
            metric_id=metric_id,
            grain=grain,
            start_date=start_date,
            end_date=end_date,
        )

    async def get_metric_time_series(
        self,
        metric_id: str,
        grain: Granularity,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[MetricTimeSeries]:
        """
        Get time series data for a metric, grain, and optional date range.
        """
        return await self.metric_time_series.get_time_series(
            metric_id=metric_id,
            grain=grain,
            start_date=start_date,
            end_date=end_date,
        )

    async def get_multi_metric_time_series(
        self,
        metric_ids: list[str],
        grain: Granularity,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[MetricTimeSeries]:
        """
        Get time series data for multiple metrics with the same grain.
        """
        return await self.metric_time_series.get_multi_metric_time_series(
            metric_ids=metric_ids,
            grain=grain,
            start_date=start_date,
            end_date=end_date,
        )

    async def stream_multi_metric_time_series(
        self,
        metric_ids: list[str],
        grain: Granularity,
        start_date: date | None = None,
        end_date: date | None = None,
        batch_size: int = 100,
    ) -> AsyncGenerator[MetricTimeSeries, None]:
        """
        Stream time series data for multiple metrics with the same grain.
        Yields results in batches to reduce memory usage.
        """
        # Get the query without executing it
        query = await self.metric_time_series.get_time_series_query(
            metric_ids=metric_ids,
            grain=grain,
            start_date=start_date,
            end_date=end_date,
        )

        # Execute and stream results
        result_proxy = await self.session.execute(query)

        while batch := result_proxy.fetchmany(batch_size):
            for row in batch:
                # Cast to MetricTimeSeries before yielding
                yield cast(MetricTimeSeries, row[0])

    async def get_dimensional_time_series(
        self,
        metric_id: str,
        grain: Granularity,
        start_date: date | None = None,
        end_date: date | None = None,
        dimension_names: list[str] | None = None,
    ) -> list[MetricDimensionalTimeSeries]:
        """
        Get dimensional time series data for a metric, grain, and dimension.
        """
        return await self.metric_dimensional_time_series.get_dimensional_time_series(
            metric_id=metric_id,
            grain=grain,
            start_date=start_date,
            end_date=end_date,
            dimension_names=dimension_names,
        )

    async def stream_dimensional_time_series(
        self,
        metric_id: str,
        grain: Granularity,
        start_date: date | None = None,
        end_date: date | None = None,
        dimension_names: list[str] | None = None,
        batch_size: int = 100,
    ) -> AsyncGenerator[MetricDimensionalTimeSeries, None]:
        """
        Stream dimensional time series data for a metric, grain, and optional dimensions.
        Yields results in batches to reduce memory usage.
        """
        # Get the query without executing it
        query = await self.metric_dimensional_time_series.get_dimensional_time_series_query(
            metric_id=metric_id,
            grain=grain,
            start_date=start_date,
            end_date=end_date,
            dimension_names=dimension_names,
        )

        # Execute and stream results
        result_proxy = await self.session.execute(query)

        while batch := result_proxy.fetchmany(batch_size):
            for row in batch:
                # Cast to MetricDimensionalTimeSeries before yielding
                yield cast(MetricDimensionalTimeSeries, row[0])
