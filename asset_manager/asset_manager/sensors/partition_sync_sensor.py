"""Sensor to keep dynamic partitions in sync with config.

This populates two dynamic partition dimensions:
- tenant_metric: "<tenant_id>::<metric_id>"
- tenant_grain:  "<tenant_id>::<grain>"
"""

import asyncio
import logging

from dagster import DefaultSensorStatus, SensorEvaluationContext, sensor

from asset_manager.partitions import (
    tenant_grain_partition,
    tenant_metric_partition,
    to_tenant_grain_key,
    to_tenant_metric_key,
)
from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.snowflake_sync_service import get_tenant_partition_sets

logger = logging.getLogger(__name__)


async def _compute_partition_keys(app_config: AppConfigResource, db: DbResource) -> tuple[list[str], list[str]]:
    """Compute global dynamic partition keys for both dimensions."""
    tenant_keys, tenant_metrics_map, tenant_grains_map = await get_tenant_partition_sets(app_config, db)
    tenant_metric_keys = sorted(
        {to_tenant_metric_key(t, m) for t in tenant_keys for m in tenant_metrics_map.get(t, [])}
    )
    tenant_grain_keys = sorted({to_tenant_grain_key(t, g) for t in tenant_keys for g in tenant_grains_map.get(t, [])})
    return tenant_metric_keys, tenant_grain_keys


@sensor(
    name="partition_sync_sensor",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
)
def sync_dynamic_partitions(context: SensorEvaluationContext, app_config: AppConfigResource, sync_db: DbResource):
    """Ensure dynamic partition registries contain the union of keys for both dims."""
    tenant_metric_keys, tenant_grain_keys = asyncio.run(_compute_partition_keys(app_config, sync_db))
    context.instance.add_dynamic_partitions(tenant_metric_partition.name, tenant_metric_keys)
    context.instance.add_dynamic_partitions(tenant_grain_partition.name, tenant_grain_keys)
    logger.info(
        "Synced dynamic partitions: tenant_metric=%d tenant_grain=%d",
        len(tenant_metric_keys),
        len(tenant_grain_keys),
    )
    yield
