"""Sensors for pattern analysis assets.

Provides sensors for dynamic partition management and event-driven pattern
execution triggered by successful time series materializations.
"""

import asyncio
import logging

from dagster import (
    AddDynamicPartitionsRequest,
    AssetKey,
    DagsterEventType,
    DailyPartitionsDefinition,
    DefaultSensorStatus,
    DeleteDynamicPartitionsRequest,
    EventRecordsFilter,
    IntMetadataValue,
    MonthlyPartitionsDefinition,
    MultiPartitionKey,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    WeeklyPartitionsDefinition,
    sensor,
)
from dagster._core.events import StepMaterializationData

from asset_manager.jobs.patterns import daily_patterns_job, monthly_patterns_job, weekly_patterns_job
from asset_manager.partitions import (
    MetricContext,
    MetricPatternContext,
    daily_partitions,
    metric_pattern_contexts,
    monthly_partitions,
    weekly_partitions,
)
from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.utils import discover_metric_contexts
from commons.models.enums import Granularity
from levers import Levers

logger = logging.getLogger(__name__)


# ============================================
# DYNAMIC PARTITION MANAGEMENT SENSOR
# ============================================


def _build_execution_context_keys(metric_contexts: list[MetricContext], patterns: list[str]) -> list[str]:
    """Build execution context keys from metric contexts and patterns."""
    return [
        MetricPatternContext(tenant=mc.tenant, metric=mc.metric, pattern=pattern).key
        for mc in metric_contexts
        for pattern in patterns
    ]


def _get_records_processed(event_specific_data: StepMaterializationData) -> int | None:
    """Get the records processed from the materialization."""

    materialization = event_specific_data.materialization
    if "records_processed" in materialization.metadata:
        records_processed = materialization.metadata["records_processed"]
        if isinstance(records_processed, IntMetadataValue):
            return records_processed.value
    return None


@sensor(
    name="sync_metric_pattern_contexts_sensor",
    minimum_interval_seconds=300,  # 5 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def sync_metric_pattern_contexts_sensor(
    context: SensorEvaluationContext, app_config: AppConfigResource, sync_db: DbResource
) -> SensorResult:
    """
    Manages dynamic metric pattern context partitions.

    Monitors for new metrics/tenants and available patterns, then adds them
    to the metric_pattern_contexts partition. This enables automatic discovery
    of new pattern execution contexts.
    """
    # Discover active metric contexts
    metric_contexts = asyncio.run(discover_metric_contexts(app_config, sync_db))

    # Get available patterns
    try:
        patterns = Levers().list_patterns()
    except Exception as e:
        context.log.warning(f"Failed to get patterns from Levers: {e}")
        patterns = []

    if not patterns:
        context.log.warning("No patterns available, skipping partition sync")
        return SensorResult(run_requests=[], dynamic_partitions_requests=[])

    # Build desired execution context keys
    desired_keys = set(_build_execution_context_keys(metric_contexts, patterns))

    # Get current partition keys
    current_keys = set(context.instance.get_dynamic_partitions(metric_pattern_contexts.name))  # type: ignore

    # Find new contexts to add
    new_keys = sorted(list(desired_keys - current_keys))

    # Find keys to remove
    keys_to_remove = sorted(list(current_keys - desired_keys))

    # Counts
    to_add = len(new_keys)
    to_remove = len(keys_to_remove)
    desired = len(desired_keys)

    partition_updates = []

    if new_keys:
        context.log.info(f"Adding {to_add} new metric pattern contexts")
        partition_updates.append(
            AddDynamicPartitionsRequest(
                partitions_def_name=metric_pattern_contexts.name,  # type: ignore
                partition_keys=new_keys,
            )
        )

    if keys_to_remove:
        context.log.info(f"Removing {to_remove} inactive metric pattern contexts")
        partition_updates.append(
            DeleteDynamicPartitionsRequest(
                partitions_def_name=metric_pattern_contexts.name,  # type: ignore
                partition_keys=keys_to_remove,
            )
        )

    context.log.info(f"Synced metric pattern contexts: To add: {to_add}, To remove: {to_remove}, Total: {desired}")

    return SensorResult(run_requests=[], dynamic_partitions_requests=partition_updates)


# ============================================
# ASSET SENSORS FOR EVENT-DRIVEN EXECUTION
# ============================================


def _trigger_patterns_on_time_series(
    context: SensorEvaluationContext,
    time_series_asset_key: str,
    time_dimension_key: str,
    time_dimension_partition: DailyPartitionsDefinition | WeeklyPartitionsDefinition | MonthlyPartitionsDefinition,
    granularity: Granularity,
    ensure_partitions: bool = False,
) -> SensorResult:
    """
    Common logic for triggering pattern analysis when time series data is materialized.

    Args:
        context: Sensor evaluation context
        time_series_asset_key: Asset key to monitor for materializations
        time_dimension_key: Key to extract time dimension ("date", "week", "month")
        granularity: Granularity enum value for tagging
        ensure_partitions: Whether to ensure dynamic partitions exist (needed for weekly/monthly)
    """
    # Convert string cursor to int (default to 0 if None/empty)
    try:
        last_cursor = int(context.cursor) if context.cursor else 0
    except (ValueError, TypeError):
        last_cursor = 0
    # Get ALL materialization events since last cursor
    events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=AssetKey(time_series_asset_key),
            after_cursor=last_cursor,
        ),
        limit=1000,  # Increase if you expect more events
    )

    if not events:
        context.log.info(f"No new {granularity.value} time series materializations found")
        return SensorResult(skip_reason=f"No new {granularity.value} time series materializations found")

    # Get available patterns
    try:
        patterns = Levers().list_patterns()
    except Exception as e:
        context.log.warning(f"Failed to get patterns: {e}")
        return SensorResult(skip_reason="Failed to get patterns")

    if not patterns:
        context.log.info("No patterns available, skipping pattern runs")
        return SensorResult(skip_reason="No patterns available")

    run_requests = []
    partition_updates = []
    all_missing_keys = set()
    current_keys = set(context.instance.get_dynamic_partitions(metric_pattern_contexts.name))  # type: ignore
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

        event_specific_data = dagster_event.event_specific_data
        if not event_specific_data or not isinstance(event_specific_data, StepMaterializationData):
            continue

        records_processed = _get_records_processed(event_specific_data)
        if records_processed is None or records_processed == 0:
            context.log.info(
                f"Skipping {granularity.value} materialization - no records processed "
                f"(records_processed={records_processed})"
            )
            continue

        # Parse the time series partition key
        partition_key = dagster_event.partition  # MultiPartitionKey
        time_str = partition_key.keys_by_dimension[time_dimension_key]  # type: ignore
        metric_context = MetricContext.from_string(partition_key.keys_by_dimension["metric_context"])  # type: ignore

        # Skip if time str in not in the partition key
        if not time_dimension_partition.has_partition_key(time_str):
            context.log.info(f"Skipping discontinued {granularity.value} time series materialization for {time_str}")
            continue

        context.log.info(
            f"Processing {granularity.value} time series materialization for {metric_context} on {time_str}"
        )

        # Collect missing partition keys for batch creation
        if ensure_partitions:
            desired_keys = [
                MetricPatternContext(tenant=metric_context.tenant, metric=metric_context.metric, pattern=p).key
                for p in patterns
            ]
            missing_keys = [k for k in desired_keys if k not in current_keys]
            all_missing_keys.update(missing_keys)

        # Build run requests for each pattern for this time series materialization
        for pattern in patterns:
            # TODO: find a better way to handle this
            # Skip forecasting pattern for non-daily granularity
            if pattern == "forecasting" and granularity != Granularity.DAY:
                context.log.info(f"Skipping forecasting pattern for non-daily granularity: {granularity}")
                continue
            pattern_context = MetricPatternContext(
                tenant=metric_context.tenant, metric=metric_context.metric, pattern=pattern
            )
            multi_partition_key = MultiPartitionKey(
                keys_by_dimension={time_dimension_key: time_str, "metric_pattern_context": pattern_context.key}
            )
            run_key = f"{last_cursor}_{time_dimension_key}/{time_str}|metric_pattern_context/{pattern_context.key}"

            # Build tags with time dimension key
            tags = {
                "grain": granularity.value,
                "metric_id": metric_context.metric,
                "pattern": pattern,
                "tenant": metric_context.tenant,
                time_dimension_key: time_str,
            }

            run_requests.append(RunRequest(run_key=run_key, partition_key=multi_partition_key, tags=tags))

    # Create missing partitions in a single batch request
    if all_missing_keys:
        partition_updates.append(
            AddDynamicPartitionsRequest(
                partitions_def_name=metric_pattern_contexts.name,  # type: ignore
                partition_keys=sorted(list(all_missing_keys)),
            )
        )

    context.log.info(
        f"Triggering {len(run_requests)} {granularity.value} pattern runs for "
        f"{len(events)} materialized time series"
    )

    return SensorResult(
        run_requests=run_requests, dynamic_partitions_requests=partition_updates, cursor=str(max_cursor)
    )


@sensor(
    job=daily_patterns_job,
    name="trigger_daily_patterns_on_time_series",
    default_status=DefaultSensorStatus.RUNNING,
)
def trigger_daily_patterns_on_time_series(context: SensorEvaluationContext) -> SensorResult:
    """
    Triggers daily pattern analysis when daily time series data is materialized.

    For each successful time series materialization, this sensor:
    1. Triggers pattern runs for all available patterns for that metric
    """
    return _trigger_patterns_on_time_series(
        context=context,
        time_series_asset_key="metric_time_series_daily",
        time_dimension_key="date",
        granularity=Granularity.DAY,
        ensure_partitions=True,
        time_dimension_partition=daily_partitions,
    )


@sensor(
    job=weekly_patterns_job,
    name="trigger_weekly_patterns_on_time_series",
    default_status=DefaultSensorStatus.RUNNING,
)
def trigger_weekly_patterns_on_time_series(context: SensorEvaluationContext) -> SensorResult:
    """
    Triggers weekly pattern analysis when weekly time series data is materialized.
    """
    return _trigger_patterns_on_time_series(
        context=context,
        time_series_asset_key="metric_time_series_weekly",
        time_dimension_key="week",
        granularity=Granularity.WEEK,
        ensure_partitions=True,
        time_dimension_partition=weekly_partitions,
    )


@sensor(
    job=monthly_patterns_job,
    name="trigger_monthly_patterns_on_time_series",
    default_status=DefaultSensorStatus.RUNNING,
)
def trigger_monthly_patterns_on_time_series(context: SensorEvaluationContext) -> SensorResult:
    """
    Triggers monthly pattern analysis when monthly time series data is materialized.
    """
    return _trigger_patterns_on_time_series(
        context=context,
        time_series_asset_key="metric_time_series_monthly",
        time_dimension_key="month",
        granularity=Granularity.MONTH,
        ensure_partitions=True,
        time_dimension_partition=monthly_partitions,
    )
