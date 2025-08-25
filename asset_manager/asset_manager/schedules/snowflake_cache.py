"""Snowflake cache schedules (daily/weekly/monthly) with shared logic."""

import asyncio

from dagster import (
    DefaultScheduleStatus,
    RunRequest,
    ScheduleEvaluationContext,
    schedule,
)

from asset_manager.jobs import snowflake_cache_job
from asset_manager.partitions import cache_tenant_grain_metric_partition, to_tenant_grain_metric_key
from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.snowflake_sync_service import get_tenant_partition_sets

DAILY_CRON_SCHEDULE = "0 4 * * *"  # 4 AM
WEEKLY_CRON_SCHEDULE = "0 6 * * 1"  # 6 AM on Monday
MONTHLY_CRON_SCHEDULE = "0 8 1 * *"  # 8 AM on the first day of the month


def _date_str(context: ScheduleEvaluationContext) -> str:
    """Extract date string from schedule context for run key."""
    ts = context.scheduled_execution_time
    return ts.strftime("%Y%m%d") if ts else "adhoc"


def _iter_cache_run_requests(
    context: ScheduleEvaluationContext,
    app_config: AppConfigResource,
    sync_db: DbResource,
    allowed_grains: set[str],
    schedule_label: str,
):
    """Generate RunRequests for tenant/metric/grain combinations filtered by allowed grains."""
    tenant_keys, tenant_metrics_map, tenant_grains_map = asyncio.run(get_tenant_partition_sets(app_config, sync_db))
    context.log.info(f"Total tenants: {len(tenant_keys)}")
    date_str = _date_str(context)

    # Pre-compute all partition keys for this evaluation
    triplets: list[tuple[str, str, str]] = []  # (tenant, metric, grain)
    for t in tenant_keys:
        metrics = tenant_metrics_map.get(t, [])
        grains = [g for g in tenant_grains_map.get(t, []) if g in allowed_grains]
        for m in metrics:
            for g in grains:
                triplets.append((t, m, g))

    partition_keys = [to_tenant_grain_metric_key(t, g, m) for (t, m, g) in triplets]

    # Ensure dynamic partitions exist before yielding RunRequests
    existing = set(context.instance.get_dynamic_partitions(cache_tenant_grain_metric_partition.name))
    missing = sorted(set(partition_keys) - existing)
    if missing:
        context.instance.add_dynamic_partitions(cache_tenant_grain_metric_partition.name, missing)
        context.log.info("Added %d missing dynamic partitions for schedule run", len(missing))

    # Yield RunRequests
    for (t, m, g), partition_key in zip(triplets, partition_keys):
        run_key = f"{t}_{m}_{g}_{date_str}"
        yield RunRequest(
            partition_key=partition_key,
            run_key=run_key,
            tags={"tenant": t, "metric": m, "grain": g, "schedule": schedule_label},
        )
        context.log.info(f"Scheduled Run: {run_key}")


@schedule(job=snowflake_cache_job, cron_schedule=DAILY_CRON_SCHEDULE, default_status=DefaultScheduleStatus.RUNNING)
def daily_snowflake_cache_schedule(
    context: ScheduleEvaluationContext, app_config: AppConfigResource, sync_db: DbResource
):
    """Daily schedule to materialize only 'day' grain partitions at 03:00."""
    yield from _iter_cache_run_requests(context, app_config, sync_db, {"day"}, "daily")


@schedule(job=snowflake_cache_job, cron_schedule=WEEKLY_CRON_SCHEDULE, default_status=DefaultScheduleStatus.RUNNING)
def weekly_snowflake_cache_schedule(
    context: ScheduleEvaluationContext, app_config: AppConfigResource, sync_db: DbResource
):
    """Weekly schedule to materialize only 'week' grain partitions every Monday at 04:00."""
    yield from _iter_cache_run_requests(context, app_config, sync_db, {"week"}, "weekly")


@schedule(job=snowflake_cache_job, cron_schedule=MONTHLY_CRON_SCHEDULE, default_status=DefaultScheduleStatus.RUNNING)
def monthly_snowflake_cache_schedule(
    context: ScheduleEvaluationContext, app_config: AppConfigResource, sync_db: DbResource
):
    """Monthly schedule to materialize only 'month' grain partitions on the 1st at 05:00."""
    yield from _iter_cache_run_requests(context, app_config, sync_db, {"month"}, "monthly")
