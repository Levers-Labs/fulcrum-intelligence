"""Sensors for time series assets.

Provides sensors that dynamically manage metric context partitions
and trigger backfills based on system events or manual requests.
"""

import asyncio
import logging

from dagster import (
    AddDynamicPartitionsRequest,
    DefaultSensorStatus,
    DeleteDynamicPartitionsRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

from asset_manager.partitions import MetricContext, metric_contexts_partition
from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.utils import discover_metric_contexts

logger = logging.getLogger(__name__)


# ============================================
# METRIC CONTEXT MANAGEMENT SENSOR
# ============================================


@sensor(
    name="sync_metric_contexts_partition_sensor",
    minimum_interval_seconds=900,  # 15 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def sync_metric_contexts_partition_sensor(
    context: SensorEvaluationContext, app_config: AppConfigResource, sync_db: DbResource
) -> SensorResult:
    """
    Manages dynamic metric context partitions.

    Monitors for new metrics/tenants and adds them to the metric_contexts
    partition. This enables automatic discovery of new time series to sync.

    In practice, this would:
    1. Query the database for new tenant/metric combinations
    2. Add new partition keys for discovered contexts
    3. Remove partition keys for deactivated contexts
    """
    context.log.info("Checking for new metric contexts to partition")

    # Get current partition keys
    current_partitions = context.instance.get_dynamic_partitions(metric_contexts_partition.name)  # type: ignore

    # Query database for active tenant/metric combinations
    discovered_contexts: list[MetricContext] = asyncio.run(discover_metric_contexts(app_config, sync_db))
    discovered_contexts_keys = [ctx.key for ctx in discovered_contexts]

    # Find new contexts to add
    new_context_keys = [ctx.key for ctx in discovered_contexts if ctx.key not in current_partitions]

    # Find context keys to remove (if they're no longer active)
    context_keys_to_remove = [key for key in current_partitions if key not in discovered_contexts_keys]

    partition_updates = []
    # counts
    to_add = len(new_context_keys)
    to_remove = len(context_keys_to_remove)
    desired = len(discovered_contexts)

    # Add new partitions
    if new_context_keys:
        context.log.info(f"Adding {to_add} new metric contexts")
        partition_updates.append(
            AddDynamicPartitionsRequest(
                partitions_def_name=metric_contexts_partition.name,  # type: ignore
                partition_keys=new_context_keys,
            )
        )

    # Remove inactive partitions (be cautious with this in production)
    if context_keys_to_remove:
        context.log.info(f"Removing {to_remove} inactive contexts")
        partition_updates.append(
            DeleteDynamicPartitionsRequest(
                partitions_def_name=metric_contexts_partition.name,  # type: ignore
                partition_keys=context_keys_to_remove,
            )
        )
    context.log.info(f"Synced dynamic partitions, To add: {to_add}, To remove: {to_remove}, Total: {desired}")
    return SensorResult(run_requests=[], dynamic_partitions_requests=partition_updates)
