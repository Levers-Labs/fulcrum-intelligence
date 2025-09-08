"""Pattern analysis assets for daily, weekly, and monthly grains.

Provides pattern analysis assets that depend on time series assets and execute
pattern analysis for specific metric-pattern combinations. Each asset is
partitioned by time and metric pattern context for efficient execution.
"""

import logging
from datetime import date, datetime

from dagster import AssetExecutionContext, MaterializeResult, asset

from asset_manager.partitions import (
    MetricPatternContext,
    daily_pattern_partitions,
    monthly_pattern_partitions,
    weekly_pattern_partitions,
)
from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.patterns import run_patterns_for_metric
from asset_manager.services.utils import get_tenant_id_by_identifier
from commons.models.enums import Granularity

logger = logging.getLogger(__name__)


# ============================================
# DAILY PATTERN ANALYSIS
# ============================================
@asset(
    partitions_def=daily_pattern_partitions,
    group_name="patterns",
    metadata={"grain": "daily", "type": "pattern_analysis", "owner": "data-platform"},
    description="Daily pattern analysis for a specific metric and pattern",
    output_required=False,
)
async def pattern_run_daily(  # type: ignore
    context: AssetExecutionContext, app_config: AppConfigResource, db: DbResource
) -> MaterializeResult | None:
    """
    Execute daily pattern analysis for a specific metric-pattern combination.
    Fetches data directly from the database using the partition date.

    Partition format: "2025-07-01|tenant_a::cpu_usage::performance_status"
    - date dimension: "2025-07-01"
    - metric_pattern_context dimension: "tenant_a::cpu_usage::performance_status"
    """
    # Parse partition info
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    exec_ctx = MetricPatternContext.from_string(partition_key.keys_by_dimension["metric_pattern_context"])

    sync_date = date.fromisoformat(date_str)
    tenant_id = await get_tenant_id_by_identifier(app_config, exec_ctx.tenant)

    context.log.info(f"Starting daily pattern analysis for {exec_ctx} on {sync_date}")

    # Run pattern analysis for the specific pattern
    # TODO: need a fix so we can pass custom date for which the pattern should be run
    result = await run_patterns_for_metric(
        tenant_id=tenant_id,
        db=db,
        metric_id=exec_ctx.metric,
        grain=Granularity.DAY,
        pattern=exec_ctx.pattern,
        sync_date=sync_date,
    )

    runs = result.pop("runs", [])

    # Skip materialization if no pattern runs were generated
    if not runs:
        context.log.warning(f"No pattern runs generated for {exec_ctx} on {sync_date}, skipping materialization")
        return

    context.log.info(f"Daily pattern analysis completed for {exec_ctx} on {sync_date}")
    result.update({"tenant": exec_ctx.tenant, "tenant_id": tenant_id})
    yield MaterializeResult(metadata=result, value=runs)


# ============================================
# WEEKLY PATTERN ANALYSIS
# ============================================
@asset(
    partitions_def=weekly_pattern_partitions,
    group_name="patterns",
    metadata={"grain": "weekly", "type": "pattern_analysis", "owner": "data-platform"},
    description="Weekly pattern analysis for a specific metric and pattern",
    output_required=False,
)
async def pattern_run_weekly(  # type: ignore
    context: AssetExecutionContext, app_config: AppConfigResource, db: DbResource
) -> MaterializeResult | None:
    """
    Execute weekly pattern analysis for a specific metric-pattern combination.
    Fetches data directly from the database using the partition week.

    Partition format: "2025-07-07|tenant_a::cpu_usage::performance_status"
    - week dimension: "2025-07-07" (start date of week, Monday-based)
    - metric_pattern_context dimension: "tenant_a::cpu_usage::performance_status"
    """
    # Parse partition info
    partition_key = context.partition_key
    week_str = partition_key.keys_by_dimension["week"]
    exec_ctx = MetricPatternContext.from_string(partition_key.keys_by_dimension["metric_pattern_context"])

    sync_date = date.fromisoformat(week_str)
    tenant_id = await get_tenant_id_by_identifier(app_config, exec_ctx.tenant)

    context.log.info(f"Starting weekly pattern analysis for {exec_ctx} for week starting {sync_date}")

    # Run pattern analysis for the specific pattern
    # TODO: need a fix so we can pass custom date for which the pattern should be run
    result = await run_patterns_for_metric(
        tenant_id=tenant_id,
        db=db,
        metric_id=exec_ctx.metric,
        grain=Granularity.WEEK,
        pattern=exec_ctx.pattern,
        sync_date=sync_date,
    )

    runs = result.pop("runs", [])

    # Skip materialization if no pattern runs were generated
    if not runs:
        context.log.warning(
            f"No pattern runs generated for {exec_ctx} for week starting {sync_date}, skipping materialization"
        )
        return

    context.log.info(f"Weekly pattern analysis completed for {exec_ctx} for week starting {sync_date}")
    result.update({"tenant": exec_ctx.tenant, "tenant_id": tenant_id})

    yield MaterializeResult(metadata=result, value=runs)


# ============================================
# MONTHLY PATTERN ANALYSIS
# ============================================
@asset(
    partitions_def=monthly_pattern_partitions,
    group_name="patterns",
    metadata={"grain": "monthly", "type": "pattern_analysis", "owner": "data-platform"},
    description="Monthly pattern analysis for a specific metric and pattern",
    output_required=False,
)
async def pattern_run_monthly(  # type: ignore
    context: AssetExecutionContext, app_config: AppConfigResource, db: DbResource
) -> MaterializeResult | None:
    """
    Execute monthly pattern analysis for a specific metric-pattern combination.
    Fetches data directly from the database using the partition month.

    Partition format: "2025-07|tenant_a::cpu_usage::performance_status"
    - month dimension: "2025-07"
    - metric_pattern_context dimension: "tenant_a::cpu_usage::performance_status"
    """
    # Parse partition info
    partition_key = context.partition_key
    month_str = partition_key.keys_by_dimension["month"]
    exec_ctx = MetricPatternContext.from_string(partition_key.keys_by_dimension["metric_pattern_context"])

    sync_date = datetime.strptime(month_str, "%Y-%m").date()
    tenant_id = await get_tenant_id_by_identifier(app_config, exec_ctx.tenant)

    context.log.info(f"Starting monthly pattern analysis for {exec_ctx} on {month_str} ({sync_date})")

    # Run pattern analysis for the specific pattern
    # TODO: need a fix so we can pass custom date for which the pattern should be run
    result = await run_patterns_for_metric(
        tenant_id=tenant_id,
        db=db,
        metric_id=exec_ctx.metric,
        grain=Granularity.MONTH,
        pattern=exec_ctx.pattern,
        sync_date=sync_date,
    )

    runs = result.pop("runs", [])

    # Skip materialization if no pattern runs were generated
    if not runs:
        context.log.warning(
            f"No pattern runs generated for {exec_ctx} on {month_str} ({sync_date}), skipping materialization"
        )
        return

    context.log.info(f"Monthly pattern analysis completed for {exec_ctx} on {month_str} ({sync_date})")
    result.update({"tenant": exec_ctx.tenant, "tenant_id": tenant_id})

    yield MaterializeResult(metadata=result, value=runs)
