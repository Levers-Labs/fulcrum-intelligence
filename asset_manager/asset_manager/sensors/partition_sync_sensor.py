"""Sensor to keep dynamic partitions in sync with config.

This populates a single dynamic partition dimension:
- tenant_grain_metric: "<tenant_id>::<grain>::<metric_id>"

Automatically adds missing partitions and removes stale ones.
"""

import asyncio

from dagster import DefaultSensorStatus, SensorEvaluationContext, sensor

from asset_manager.partitions import cache_tenant_grain_metric_partition, to_tenant_grain_metric_key
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
    partition_name = cache_tenant_grain_metric_partition.name
    if partition_name is None:
        context.log.error("Partition name is None, cannot sync partitions")
        return

    desired = set(asyncio.run(_compute_triplet_keys(app_config, sync_db)))
    existing = set(context.instance.get_dynamic_partitions(partition_name))

    to_add = sorted(desired - existing)
    to_delete = sorted(existing - desired)

    if to_add:
        context.instance.add_dynamic_partitions(partition_name, to_add)
        context.log.info("Added %d dynamic partitions", len(to_add))

    if to_delete:
        for key in to_delete:
            # Delete per-key for broader Dagster version compatibility
            context.instance.delete_dynamic_partition(partition_name, key)
        context.log.info("Deleted %d stale dynamic partitions", len(to_delete))

    context.log.info(
        "Synced dynamic partitions: tenant_grain_metric added=%d deleted=%d total=%d",
        len(to_add),
        len(to_delete),
        len(desired),
    )
    yield
