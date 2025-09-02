"""
Snowflake cache sync flows for semantic data.
"""

import os

from prefect import flow, get_run_logger, unmapped
from prefect.futures import wait

from commons.db.crud import NotFoundError
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from query_manager.semantic_manager.models import SyncOperation
from tasks_manager.config import AppConfig
from tasks_manager.tasks.common import fetch_tenants
from tasks_manager.tasks.events import send_tenant_sync_requested_event
from tasks_manager.tasks.snowflake_sync import (
    cache_tenant_metrics_to_snowflake,
    get_enabled_grain_config,
    get_enabled_grains_for_tenant,
)


@flow(  # type: ignore
    name="snowflake_cache_sync_tenant",
    flow_run_name="snowflake_cache_sync_tenant={tenant_id_str}_grain={grain}",
    description="Cache tenant metrics to Snowflake for a specific grain or all enabled grains",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=3600,
)
async def snowflake_cache_sync_for_tenant(
    tenant_id_str: str,
    grain: Granularity | None = None,
    metrics: list[str] | None = None,
) -> None:
    """
    Cache tenant metrics to Snowflake for a specific grain or all enabled grains.

    Args:
        tenant_id_str: The tenant ID as string
        grain: Optional granularity level to sync. If None, sync all enabled grains.
        metrics: Optional list of metric IDs to sync. If None, sync all enabled metrics.

    Returns:
        List of SyncSummary objects, one for each grain processed
    """

    logger = get_run_logger()
    tenant_id = int(tenant_id_str)
    logger.info("Starting cache sync for tenant %d with grain %s", tenant_id, grain.value if grain else "ALL")

    # Set tenant context
    set_tenant_id(tenant_id)

    # set the server host for the query manager
    config = await AppConfig.load("default")
    os.environ["SERVER_HOST"] = config.query_manager_server_host

    try:

        if grain is not None:
            # Get specific grain configuration
            try:
                grain_config = await get_enabled_grain_config(grain)
            except NotFoundError:
                logger.warning("Grain %s is not enabled for tenant %d", grain.value, tenant_id)
                return
            enabled_grains = [grain_config]
        else:
            # Get all enabled grains
            enabled_grains = await get_enabled_grains_for_tenant()

        if not enabled_grains:
            logger.info("No enabled grains found for tenant %d", tenant_id)
            return

        # Process all enabled grains in parallel using map
        logger.info("Processing %d grains in parallel for tenant %d", len(enabled_grains), tenant_id)

        grain_futures = cache_tenant_metrics_to_snowflake.map(
            tenant_id=unmapped(tenant_id),
            grain=[grain_config.grain for grain_config in enabled_grains],
            grain_config=enabled_grains,
            metrics=unmapped(metrics),
        )

        # Wait for all grain processing to complete
        done, not_done = wait(grain_futures, timeout=60 * 30)  # 30 minutes
        logger.info("Processed %d grains, %d grains not done", len(done), len(not_done))

        # Get results from completed futures
        summaries = [future.result() for future in done]
        logger.info(
            "Completed cache sync for %s enabled grains - Tenant: %d, Processed %d grains",
            grain.value if grain else "All",
            tenant_id,
            len(summaries),
        )

    except Exception as e:
        logger.error(
            "Cache sync failed - Tenant: %d, Grain: %s, Error: %s",
            tenant_id,
            grain.value if grain else "ALL",
            str(e),
            exc_info=True,
        )
        raise
    finally:
        reset_context()


@flow(  # type: ignore
    name="snowflake_cache_sync_all_tenants",
    flow_run_name="snowflake_cache_sync_all_tenants",
    description="Cache sync for all tenants with metric cache enabled",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=60,  # 1 minute
)
async def snowflake_cache_sync_all_tenants() -> None:
    """
    Trigger Snowflake cache sync for all tenants.
    This flow triggers tenant sync events without grain parameter,
    causing each tenant to process all their enabled grains.
    """
    logger = get_run_logger()
    logger.info("Starting cache sync for all tenants")

    try:
        # Fetch tenants with metric cache enabled
        tenants = await fetch_tenants(enable_metric_cache=True)  # type: ignore
        logger.info("Found %d tenants to trigger cache sync", len(tenants))

        # Emit event for each tenant to trigger their cache sync
        for tenant in tenants:
            send_tenant_sync_requested_event(
                tenant_id=tenant["id"],
                sync_operation=SyncOperation.SNOWFLAKE_CACHE,
                tenant_name=tenant["name"],
            )

        logger.info(
            "Successfully triggered cache sync for %d tenants with metric cache enabled",
            len(tenants),
        )

    except Exception as e:
        logger.error(
            "Failed to trigger cache sync for all tenants - Error: %s",
            str(e),
            exc_info=True,
        )
        raise
