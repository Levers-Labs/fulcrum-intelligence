"""Story generation assets for daily, weekly, and monthly grains.

Provides story generation assets that depend on pattern analysis assets and generate
stories for specific metric-pattern combinations. Each asset is partitioned by time
and metric pattern context for efficient execution.
"""

import logging
from datetime import date

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    MaterializeResult,
    asset,
)

from asset_manager.partitions import (
    MetricPatternContext,
    daily_pattern_partitions,
    monthly_pattern_partitions,
    weekly_pattern_partitions,
)
from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.stories import generate_stories_for_pattern_runs
from asset_manager.services.utils import get_tenant_id_by_identifier
from commons.models.enums import Granularity

logger = logging.getLogger(__name__)


# ============================================
# DAILY STORY GENERATION
# ============================================
@asset(
    partitions_def=daily_pattern_partitions,
    group_name="stories",
    description="Generate daily stories from pattern analysis results",
    metadata={"grain": Granularity.DAY.value, "type": "story_generation", "owner": "data-platform"},
    ins={"pattern_runs": AssetIn(key=AssetKey("pattern_run_daily"))},
    output_required=False,
)
async def metric_stories_daily(  # type: ignore
    context: AssetExecutionContext,
    app_config: AppConfigResource,
    db: DbResource,
    pattern_runs: list[dict] | None,
) -> MaterializeResult | None:
    """
    Generate daily stories from pattern analysis results.

    Partition format: "2025-07-01|tenant_a::cpu_usage::performance_status"
    - date dimension: "2025-07-01"
    - metric_pattern_context dimension: "tenant_a::cpu_usage::performance_status"
    """
    # Parse partition info
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    exec_ctx = MetricPatternContext.from_string(partition_key.keys_by_dimension["metric_pattern_context"])

    # Skip materialization if no pattern runs available
    if not pattern_runs:
        context.log.warning(f"No pattern runs available for {exec_ctx} on {date_str}, skipping materialization")
        return

    context.log.info(f"Processing stories evaluation for {exec_ctx} on {date_str}")

    # Resolve tenant_id from tenant_identifier
    tenant_id = await get_tenant_id_by_identifier(app_config, exec_ctx.tenant)

    # Generate stories for all pattern runs
    result = await generate_stories_for_pattern_runs(
        tenant_id=tenant_id,
        db=db,
        metric_id=exec_ctx.metric,
        pattern=exec_ctx.pattern,
        grain=Granularity.DAY,
        analysis_date=date.fromisoformat(date_str),
        runs=pattern_runs,
    )
    stories_count = result.pop("count")
    if stories_count == 0:
        context.log.warning(f"No stories generated for {exec_ctx} on {date_str}, skipping materialization")
        return

    context.log.info(f"Generated {stories_count} total stories for stories evaluation for {exec_ctx} on {date_str}")

    yield MaterializeResult(
        metadata={
            **result,
            "grain": Granularity.DAY.value,
            "tenant": exec_ctx.tenant,
        }
    )


# ============================================
# WEEKLY STORY GENERATION
# ============================================
@asset(
    partitions_def=weekly_pattern_partitions,
    group_name="stories",
    description="Generate weekly stories from pattern analysis results",
    metadata={"grain": Granularity.WEEK.value, "type": "story_generation", "owner": "data-platform"},
    ins={"pattern_runs": AssetIn(key=AssetKey("pattern_run_weekly"))},
    output_required=False,
)
async def metric_stories_weekly(  # type: ignore
    context: AssetExecutionContext,
    app_config: AppConfigResource,
    db: DbResource,
    pattern_runs: list[dict] | None,
) -> MaterializeResult | None:
    """
    Generate weekly stories from pattern analysis results.

    Partition format: "2025-07-07|tenant_a::cpu_usage::performance_status"
    - week dimension: "2025-07-07" (start date of week, Monday-based)
    - metric_pattern_context dimension: "tenant_a::cpu_usage::performance_status"
    """
    # Parse partition info
    partition_key = context.partition_key
    week_str = partition_key.keys_by_dimension["week"]
    exec_ctx = MetricPatternContext.from_string(partition_key.keys_by_dimension["metric_pattern_context"])

    # Skip materialization if no pattern runs available
    if not pattern_runs:
        context.log.warning(
            f"No pattern runs available for {exec_ctx} for week starting {week_str}, skipping materialization"
        )
        return

    context.log.info(f"Processing stories evaluation for {exec_ctx} for week starting {week_str}")

    # Resolve tenant_id from tenant_identifier
    tenant_id = await get_tenant_id_by_identifier(app_config, exec_ctx.tenant)

    # Generate stories for all pattern runs
    result = await generate_stories_for_pattern_runs(
        tenant_id=tenant_id,
        db=db,
        metric_id=exec_ctx.metric,
        pattern=exec_ctx.pattern,
        grain=Granularity.WEEK,
        analysis_date=date.fromisoformat(week_str),
        runs=pattern_runs,
    )

    stories_count = result.pop("count")
    if stories_count == 0:
        context.log.warning(
            f"No stories generated for {exec_ctx} for week starting {week_str}, skipping materialization"
        )
        return

    context.log.info(
        f"Generated {stories_count} total stories for stories evaluation for {exec_ctx} for week starting {week_str}"
    )

    yield MaterializeResult(
        metadata={
            **result,
            "grain": Granularity.WEEK.value,
            "tenant": exec_ctx.tenant,
        }
    )


# ============================================
# MONTHLY STORY GENERATION
# ============================================
@asset(
    partitions_def=monthly_pattern_partitions,
    group_name="stories",
    description="Generate monthly stories from pattern analysis results",
    metadata={"grain": Granularity.MONTH.value, "type": "story_generation", "owner": "data-platform"},
    ins={"pattern_runs": AssetIn(key=AssetKey("pattern_run_monthly"))},
    output_required=False,
)
async def metric_stories_monthly(  # type: ignore
    context: AssetExecutionContext,
    app_config: AppConfigResource,
    db: DbResource,
    pattern_runs: list[dict] | None,
) -> MaterializeResult | None:
    """
    Generate monthly stories from pattern analysis results.

    Partition format: "2025-07|tenant_a::cpu_usage::performance_status"
    - month dimension: "2025-07"
    - metric_pattern_context dimension: "tenant_a::cpu_usage::performance_status"
    """
    # Parse partition info
    partition_key = context.partition_key
    month_str = partition_key.keys_by_dimension["month"]
    exec_ctx = MetricPatternContext.from_string(partition_key.keys_by_dimension["metric_pattern_context"])

    # Skip materialization if no pattern runs available
    if not pattern_runs:
        context.log.warning(f"No pattern runs available for {exec_ctx} on {month_str}, skipping materialization")
        return

    context.log.info(f"Processing stories evaluation for {exec_ctx} on {month_str}")

    # Resolve tenant_id from tenant_identifier
    tenant_id = await get_tenant_id_by_identifier(app_config, exec_ctx.tenant)

    # Generate stories for all pattern runs
    result = await generate_stories_for_pattern_runs(
        tenant_id=tenant_id,
        db=db,
        metric_id=exec_ctx.metric,
        pattern=exec_ctx.pattern,
        grain=Granularity.MONTH,
        analysis_date=date.fromisoformat(month_str),
        runs=pattern_runs,
    )

    stories_count = result.pop("count")
    if stories_count == 0:
        context.log.warning(f"No stories generated for {exec_ctx} on {month_str}, skipping materialization")
        return

    context.log.info(f"Generated {stories_count} total stories for stories evaluation for {exec_ctx} on {month_str}")

    yield MaterializeResult(
        metadata={
            **result,
            "grain": Granularity.MONTH.value,
            "tenant": exec_ctx.tenant,
        }
    )
