"""Snowflake cache schedules (daily/weekly/monthly) with shared logic."""

from dagster import (
    DefaultScheduleStatus,
    RunRequest,
    ScheduleEvaluationContext,
    schedule,
)

from asset_manager.jobs import snowflake_cache_job
from asset_manager.partitions import cache_tenant_grain_metric_partition, parse_tenant_grain_metric_key

DAILY_CRON_SCHEDULE = "0 3 * * *"  # every day at 3 am
WEEKLY_CRON_SCHEDULE = "0 6 * * 1"  # 6 AM on Monday
MONTHLY_CRON_SCHEDULE = "0 8 1 * *"  # 8 AM on the first day of the month


def _date_str(context: ScheduleEvaluationContext) -> str:
    """Extract date string from schedule context for run key."""
    ts = context.scheduled_execution_time
    return ts.strftime("%Y%m%d") if ts else "adhoc"


def _iter_cache_run_requests(context: ScheduleEvaluationContext, allowed_grains: set[str], schedule_label: str):
    """Generate RunRequests for existing partitions filtered by allowed grains.

    Note: Partition management is handled by the partition_sync_sensor.
    This function assumes partitions already exist and just filters them.
    """
    date_str = _date_str(context)

    # Get existing partitions (managed by sensor)
    existing_partitions = context.instance.get_dynamic_partitions(cache_tenant_grain_metric_partition.name)

    # Filter partitions by allowed grains
    filtered_partitions = []
    for partition_key in existing_partitions:
        try:
            tenant_id, grain, metric_id = parse_tenant_grain_metric_key(partition_key)
            if grain in allowed_grains:
                filtered_partitions.append((partition_key, tenant_id, grain, metric_id))
        except ValueError:
            context.log.warning(f"Invalid partition key format: {partition_key}")
            continue

    # Yield RunRequests for filtered partitions
    for partition_key, tenant_id, grain, metric_id in filtered_partitions:
        run_key = f"{tenant_id}_{metric_id}_{grain}_{date_str}"
        yield RunRequest(
            partition_key=partition_key,
            run_key=run_key,
            tags={"tenant": tenant_id, "metric": metric_id, "grain": grain, "schedule": schedule_label},
        )


@schedule(job=snowflake_cache_job, cron_schedule=DAILY_CRON_SCHEDULE, default_status=DefaultScheduleStatus.RUNNING)
def daily_snowflake_cache_schedule(context: ScheduleEvaluationContext):
    """Daily schedule to materialize only 'day' grain partitions at 03:00."""
    yield from _iter_cache_run_requests(context, {"day"}, "daily")


@schedule(job=snowflake_cache_job, cron_schedule=WEEKLY_CRON_SCHEDULE, default_status=DefaultScheduleStatus.RUNNING)
def weekly_snowflake_cache_schedule(context: ScheduleEvaluationContext):
    """Weekly schedule to materialize only 'week' grain partitions every Monday at 06:00."""
    yield from _iter_cache_run_requests(context, {"week"}, "weekly")


@schedule(job=snowflake_cache_job, cron_schedule=MONTHLY_CRON_SCHEDULE, default_status=DefaultScheduleStatus.RUNNING)
def monthly_snowflake_cache_schedule(context: ScheduleEvaluationContext):
    """Monthly schedule to materialize only 'month' grain partitions on the 1st at 05:00."""
    yield from _iter_cache_run_requests(context, {"month"}, "monthly")
