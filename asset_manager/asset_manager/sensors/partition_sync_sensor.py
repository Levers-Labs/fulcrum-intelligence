"""Sensor to keep dynamic partitions in sync with config.

This populates a single dynamic partition dimension:
- tenant_grain_metric: "<tenant_id>::<grain>::<metric_id>"

Automatically adds missing partitions and removes stale ones.
"""

import asyncio
from datetime import datetime

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    ScheduleEvaluationContext,
    SensorEvaluationContext,
    sensor,
)

from asset_manager.jobs import snowflake_cache_job
from asset_manager.partitions import (
    cache_tenant_grain_metric_partition,
    parse_tenant_grain_metric_key,
    to_tenant_grain_metric_key,
)
from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.snowflake_sync_service import get_tenant_partition_sets


async def _compute_triplet_keys(app_config: AppConfigResource, db: DbResource) -> list[str]:
    """Compute desired combined triplet partition keys."""
    tenant_keys, tenant_metrics_map, tenant_grains_map = await get_tenant_partition_sets(app_config, db)
    return sorted(
        to_tenant_grain_metric_key(t, g, m)
        for t in tenant_keys
        for g in tenant_grains_map.get(t, [])
        for m in tenant_metrics_map.get(t, [])
    )


@sensor(
    name="partition_sync_sensor",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
)
def sync_dynamic_partitions(context: SensorEvaluationContext, app_config: AppConfigResource, sync_db: DbResource):
    """Ensure dynamic partition registry contains exactly the desired triplet keys."""
    desired = set(asyncio.run(_compute_triplet_keys(app_config, sync_db)))
    existing = set(context.instance.get_dynamic_partitions(cache_tenant_grain_metric_partition.name))

    to_add = sorted(desired - existing)
    to_delete = sorted(existing - desired)

    if to_add:
        context.instance.add_dynamic_partitions(cache_tenant_grain_metric_partition.name, to_add)
        context.log.info("Added %d dynamic partitions", len(to_add))

    if to_delete:
        for key in to_delete:
            # Delete per-key for broader Dagster version compatibility
            context.instance.delete_dynamic_partition(cache_tenant_grain_metric_partition.name, key)
        context.log.info("Deleted %d stale dynamic partitions", len(to_delete))

    context.log.info(
        "Synced dynamic partitions: tenant_grain_metric added=%d deleted=%d total=%d",
        len(to_add),
        len(to_delete),
        len(desired),
    )
    yield


@sensor(
    name="daily_snowflake_cache_sensor",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
    job=snowflake_cache_job,
)
def daily_snowflake_cache_sensor(context: ScheduleEvaluationContext):
    """Daily schedule to materialize only 'day' grain partitions at 03:00."""
    date_str = datetime.now().strftime("%Y%m%d")

    # Get existing partitions (managed by sensor)
    partitions = cache_tenant_grain_metric_partition.get_partition_keys(dynamic_partitions_store=context.instance)

    # Yield RunRequests for filtered partitions
    for partition_key in partitions:
        try:
            tenant_id, grain, metric_id = parse_tenant_grain_metric_key(partition_key)
            if grain in ["day"]:
                yield RunRequest(partition_key=partition_key, run_key=f"{tenant_id}_{metric_id}_{grain}_{date_str}")
        except ValueError:
            context.log.warning(f"Invalid partition key format: {partition_key}")
            continue
