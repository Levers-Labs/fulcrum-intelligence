"""Time series assets for semantic sync data.

Provides daily, weekly, and monthly metric time series assets that fetch
data directly from cube using the semantic sync service. Each asset is
partitioned by time and metric context for efficient backfilling.
"""

import logging
from datetime import date, datetime

from dagster import AssetExecutionContext, MaterializeResult, asset

from asset_manager.partitions import (
    MetricContext,
    daily_time_series_partitions,
    monthly_time_series_partitions,
    weekly_time_series_partitions,
)
from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.semantic_sync import SemanticSyncRequest, SemanticSyncService
from asset_manager.services.utils import get_tenant_id_by_identifier
from commons.models.enums import Granularity

logger = logging.getLogger(__name__)


# ============================================
# DAILY TIME SERIES
# ============================================
@asset(
    partitions_def=daily_time_series_partitions,
    group_name="time_series",
    description="Daily metric time series data",
    metadata={"grain": "daily", "type": "time_series", "owner": "data-platform"},
    output_required=False,
)
async def metric_time_series_daily(  # type: ignore
    context: AssetExecutionContext, app_config: AppConfigResource, db: DbResource
) -> MaterializeResult | None:
    """
    Captures daily metric data from cube.
    Idempotent - can be re-run safely for backfills.

    Partition format: "2025-07-01|tenant_a::cpu_usage"
    - date dimension: "2025-07-01"
    - metric_context dimension: "tenant_a::cpu_usage"
    """
    # Parse partition info
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    metric_context = MetricContext.from_string(partition_key.keys_by_dimension["metric_context"])

    sync_date = date.fromisoformat(date_str)

    context.log.info(f"Starting daily semantic sync for {metric_context} on {date_str}")

    # Resolve tenant_id from tenant_identifier
    tenant_id = await get_tenant_id_by_identifier(app_config, metric_context.tenant)

    # Create sync request
    request = SemanticSyncRequest(
        tenant_id=tenant_id,
        tenant_identifier=metric_context.tenant,
        metric_id=metric_context.metric,
        grain=Granularity.DAY,
        sync_date=sync_date,
        force_full_sync=False,
    )

    service = SemanticSyncService(config=app_config, db=db)

    response = await service.sync_metric_time_series(request)

    if not response.success:
        raise Exception(f"Semantic sync failed: {response.error_message}")

    # Skip materialization if no records were processed
    if response.records_processed == 0:
        context.log.warning(f"No records processed for {metric_context} on {date_str}, skipping materialization")
        return

    context.log.info(
        f"Daily sync completed for {metric_context}: "
        f"{response.records_processed} records, "
        f"{response.dimensions_processed} dimensions, "
        f"{response.duration_seconds:.2f}s"
    )
    yield MaterializeResult(metadata=response.model_dump(mode="json"))


# ============================================
# WEEKLY TIME SERIES
# ============================================
@asset(
    partitions_def=weekly_time_series_partitions,
    group_name="time_series",
    description="Weekly metric time series data",
    metadata={"grain": "weekly", "type": "time_series", "owner": "data-platform"},
    output_required=False,
)
async def metric_time_series_weekly(  # type: ignore
    context: AssetExecutionContext, app_config: AppConfigResource, db: DbResource
) -> MaterializeResult | None:
    """
    Captures weekly metric data from cube.
    No aggregation - fetches directly from cube like daily.
    Idempotent - can be re-run safely for backfills.

    Partition format: "2025-07-01|tenant_a::cpu_usage"
    - week dimension: "2025-07-01" (start date of week, Monday-based weeks)
    - metric_context dimension: "tenant_a::cpu_usage"
    """
    # Parse partition info
    partition_key = context.partition_key
    week_str = partition_key.keys_by_dimension["week"]
    metric_context = MetricContext.from_string(partition_key.keys_by_dimension["metric_context"])

    # Convert week string to date (first day of week)
    sync_date = date.fromisoformat(week_str)

    context.log.info(f"Starting weekly semantic sync for {metric_context} for week starting {sync_date}")

    # Resolve tenant_id from tenant_identifier
    tenant_id = await get_tenant_id_by_identifier(app_config, metric_context.tenant)

    # Create sync request
    request = SemanticSyncRequest(
        tenant_id=tenant_id,
        tenant_identifier=metric_context.tenant,
        metric_id=metric_context.metric,
        grain=Granularity.WEEK,
        sync_date=sync_date,
        force_full_sync=False,
    )

    service = SemanticSyncService(config=app_config, db=db)

    response = await service.sync_metric_time_series(request)

    if not response.success:
        raise Exception(f"Semantic sync failed: {response.error_message}")

    # Skip materialization if no records were processed
    if response.records_processed == 0:
        context.log.warning(
            f"No records processed for {metric_context} for week starting {sync_date}, skipping materialization"
        )
        return

    context.log.info(
        f"Weekly sync completed for {metric_context}: "
        f"{response.records_processed} records, "
        f"{response.dimensions_processed} dimensions, "
        f"{response.duration_seconds:.2f}s"
    )

    yield MaterializeResult(metadata=response.model_dump(mode="json"))


# ============================================
# MONTHLY TIME SERIES
# ============================================
@asset(
    partitions_def=monthly_time_series_partitions,
    group_name="time_series",
    description="Monthly metric time series data",
    metadata={"grain": "monthly", "type": "time_series", "owner": "data-platform"},
    output_required=False,
)
async def metric_time_series_monthly(  # type: ignore
    context: AssetExecutionContext, app_config: AppConfigResource, db: DbResource
) -> MaterializeResult | None:
    """
    Captures monthly metric data from cube.
    No aggregation - fetches directly from cube like daily.
    Idempotent - can be re-run safely for backfills.

    Partition format: "2025-07|tenant_a::cpu_usage"
    - month dimension: "2025-07"
    - metric_context dimension: "tenant_a::cpu_usage"
    """
    # Parse partition info
    partition_key = context.partition_key
    month_str = partition_key.keys_by_dimension["month"]
    metric_context = MetricContext.from_string(partition_key.keys_by_dimension["metric_context"])

    # Convert month string to date (first day of month)
    sync_date = datetime.strptime(month_str, "%Y-%m").date()

    context.log.info(f"Starting monthly semantic sync for {metric_context} on {month_str} ({sync_date})")

    # Resolve tenant_id from tenant_identifier
    tenant_id = await get_tenant_id_by_identifier(app_config, metric_context.tenant)

    # Create sync request
    request = SemanticSyncRequest(
        tenant_id=tenant_id,
        tenant_identifier=metric_context.tenant,
        metric_id=metric_context.metric,
        grain=Granularity.MONTH,
        sync_date=sync_date,
        force_full_sync=False,
    )

    service = SemanticSyncService(config=app_config, db=db)

    response = await service.sync_metric_time_series(request)

    if not response.success:
        raise Exception(f"Semantic sync failed: {response.error_message}")

    # Skip materialization if no records were processed
    if response.records_processed == 0:
        context.log.warning(
            f"No records processed for {metric_context} on {month_str} ({sync_date}), skipping materialization"
        )
        return

    context.log.info(
        f"Monthly sync completed for {metric_context}: "
        f"{response.records_processed} records, "
        f"{response.dimensions_processed} dimensions, "
        f"{response.duration_seconds:.2f}s"
    )

    yield MaterializeResult(metadata=response.model_dump(mode="json"))
