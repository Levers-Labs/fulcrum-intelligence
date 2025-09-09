"""Sensors for story generation assets.

Provides sensors that trigger story generation jobs when pattern analysis
assets are materialized. These sensors monitor pattern run completions and
trigger corresponding story jobs with the same partition keys.
"""

import logging

from dagster import (
    AssetKey,
    DagsterEventType,
    DailyPartitionsDefinition,
    DefaultSensorStatus,
    EventRecordsFilter,
    MonthlyPartitionsDefinition,
    MultiPartitionKey,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    WeeklyPartitionsDefinition,
    sensor,
)

from asset_manager.jobs.stories import daily_stories_job, monthly_stories_job, weekly_stories_job
from asset_manager.partitions import (
    MetricPatternContext,
    daily_partitions,
    monthly_partitions,
    weekly_partitions,
)
from commons.models.enums import Granularity

logger = logging.getLogger(__name__)


# ============================================
# COMMON SENSOR LOGIC
# ============================================


def _trigger_stories_on_pattern_runs(
    context: SensorEvaluationContext,
    pattern_asset_key: str,
    time_dimension_key: str,
    granularity: Granularity,
    time_dimension_partition: DailyPartitionsDefinition | WeeklyPartitionsDefinition | MonthlyPartitionsDefinition,
) -> SensorResult:
    """
    Common logic for triggering story generation when pattern analysis assets are materialized.
    Returns:
        SensorResult with run requests for story generation
    """
    # Convert string cursor to int (default to 0 if None/empty)
    try:
        last_cursor = int(context.cursor) if context.cursor else 0
    except (ValueError, TypeError):
        last_cursor = 0

    # Get all materialization events since last cursor
    events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=AssetKey(pattern_asset_key),
            after_cursor=last_cursor,
        ),
        limit=1000,  # Increase if you expect more events
    )

    if not events:
        context.log.info(f"No new {pattern_asset_key} materializations found")
        return SensorResult(skip_reason=f"No new {pattern_asset_key} materializations found")

    run_requests = []
    max_cursor = last_cursor

    # Process each materialization event
    for event_record in events:
        # Track the highest cursor value for next iteration
        max_cursor = max(max_cursor, event_record.storage_id)

        if not event_record.event_log_entry.dagster_event:
            continue

        dagster_event = event_record.event_log_entry.dagster_event
        if not dagster_event.partition:
            continue

        # Parse the pattern partition key
        partition_key = dagster_event.partition  # MultiPartitionKey
        time_str = partition_key.keys_by_dimension[time_dimension_key]  # type: ignore
        metric_pattern_context = partition_key.keys_by_dimension["metric_pattern_context"]  # type: ignore

        # Skip if time str is not in the partition key (discontinued partitions)
        if not time_dimension_partition.has_partition_key(time_str):
            context.log.info(f"Skipping discontinued pattern materialization for {time_str}")
            continue

        context.log.info(f"Processing pattern materialization for {metric_pattern_context} on {time_str}")

        # Create run request with the same multi-partition key
        multi_partition_key = MultiPartitionKey(
            keys_by_dimension={time_dimension_key: time_str, "metric_pattern_context": metric_pattern_context}
        )

        # Generate unique run key similar to patterns sensor format
        # this will ensure we will always re-run the story generation for the same pattern run combination
        # TODO: See if we remove cursor so that only one story will be generated for each pattern run combination
        run_key = f"{last_cursor}_{time_dimension_key}/{time_str}|metric_pattern_context/{metric_pattern_context}"

        exec_ctx = MetricPatternContext.from_string(metric_pattern_context)

        tags = {
            "grain": granularity.value,
            "metric_id": exec_ctx.metric,
            "pattern": exec_ctx.pattern,
            "tenant": exec_ctx.tenant,
            time_dimension_key: time_str,
        }

        run_requests.append(
            RunRequest(
                run_key=run_key,
                partition_key=multi_partition_key,
                tags=tags,
            )
        )

    context.log.info(f"Triggering {len(run_requests)} story generation runs for {len(events)} pattern materializations")

    return SensorResult(
        run_requests=run_requests,
        cursor=str(max_cursor),
    )


# ============================================
# STORY SENSORS FOR EACH GRAIN
# ============================================


@sensor(
    job=daily_stories_job,
    name="trigger_daily_stories_on_pattern_runs",
    default_status=DefaultSensorStatus.RUNNING,
)
def trigger_daily_stories_on_pattern_runs(context: SensorEvaluationContext) -> SensorResult:
    """
    Triggers daily story generation when daily pattern analysis is materialized.

    For each successful pattern materialization, this sensor:
    1. Triggers story generation for the same metric-pattern-date combination
    """
    return _trigger_stories_on_pattern_runs(
        context=context,
        pattern_asset_key="pattern_run_daily",
        time_dimension_key="date",
        granularity=Granularity.DAY,
        time_dimension_partition=daily_partitions,
    )


@sensor(
    job=weekly_stories_job,
    name="trigger_weekly_stories_on_pattern_runs",
    default_status=DefaultSensorStatus.RUNNING,
)
def trigger_weekly_stories_on_pattern_runs(context: SensorEvaluationContext) -> SensorResult:
    """
    Triggers weekly story generation when weekly pattern analysis is materialized.
    """
    return _trigger_stories_on_pattern_runs(
        context=context,
        pattern_asset_key="pattern_run_weekly",
        time_dimension_key="week",
        granularity=Granularity.WEEK,
        time_dimension_partition=weekly_partitions,
    )


@sensor(
    job=monthly_stories_job,
    name="trigger_monthly_stories_on_pattern_runs",
    default_status=DefaultSensorStatus.RUNNING,
)
def trigger_monthly_stories_on_pattern_runs(context: SensorEvaluationContext) -> SensorResult:
    """
    Triggers monthly story generation when monthly pattern analysis is materialized.
    """
    return _trigger_stories_on_pattern_runs(
        context=context,
        pattern_asset_key="pattern_run_monthly",
        time_dimension_key="month",
        granularity=Granularity.MONTH,
        time_dimension_partition=monthly_partitions,
    )
