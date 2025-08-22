"""Daily schedule to materialize all active tenant/metric/grain partitions at 3 AM.

Emits runs using two dynamic dimensions: `tenant_metric` and `tenant_grain`.
Only valid combinations per tenant are scheduled (tenant-specific metrics and grains).
"""

import asyncio

from dagster import (
    DefaultScheduleStatus,
    MultiPartitionKey,
    RunRequest,
    ScheduleEvaluationContext,
    schedule,
)

from asset_manager.jobs import snowflake_cache_job
from asset_manager.partitions import to_tenant_grain_key, to_tenant_metric_key
from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.snowflake_sync_service import get_tenant_partition_sets


@schedule(
    job=snowflake_cache_job,
    cron_schedule="0 3 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_snowflake_cache_schedule(
    context: ScheduleEvaluationContext, app_config: AppConfigResource, sync_db: DbResource
):
    """Daily schedule to materialize all active tenant/metric/grain partitions at 3 AM."""
    tenant_keys, tenant_metrics_map, tenant_grains_map = asyncio.run(get_tenant_partition_sets(app_config, sync_db))
    context.log.info(f"Total tenants: {len(tenant_keys)}")
    for t in tenant_keys:
        metrics = tenant_metrics_map.get(t, [])
        grains = tenant_grains_map.get(t, [])
        for m in metrics:
            for g in grains:
                # Create unique run_key for each combination
                run_key = f"{t}_{m}_{g}_{context.scheduled_execution_time.strftime('%Y%m%d')}"

                yield RunRequest(
                    partition_key=MultiPartitionKey(
                        {
                            "tenant_metric": to_tenant_metric_key(t, m),
                            "tenant_grain": to_tenant_grain_key(t, g),
                        }
                    ),
                    run_key=run_key,
                )
                context.log.info(f"Scheduled Run: {run_key}")
