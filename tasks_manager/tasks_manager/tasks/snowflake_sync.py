"""
Snowflake cache tasks for semantic data operations.
"""

from datetime import date, datetime, timedelta
from typing import Any

from prefect import get_run_logger, task, unmapped
from prefect.context import EngineContext, get_run_context
from prefect.futures import wait
from prefect.tasks import task_input_hash

from commons.clients.insight_backend import InsightBackendClient
from commons.clients.snowflake import SnowflakeClient, SnowflakeConfigModel
from commons.models.enums import Granularity
from commons.utilities.context import get_tenant_id
from query_manager.core.crud import CRUDMetricCacheConfig, CRUDMetricCacheGrainConfig
from query_manager.core.models import MetricCacheConfig, MetricCacheGrainConfig
from query_manager.core.schemas import MetricDetail
from query_manager.db.config import get_async_session
from query_manager.semantic_manager.cache_manager import SnowflakeSemanticCacheManager
from query_manager.semantic_manager.models import SyncOperation, SyncStatus, SyncType
from tasks_manager.config import AppConfig
from tasks_manager.tasks.artifacts import create_tenant_cache_summary_artifact
from tasks_manager.tasks.events import send_tenant_sync_finished_event, send_tenant_sync_started_event
from tasks_manager.tasks.query import fetch_metric_values, get_metric
from tasks_manager.tasks.semantic_manager import SyncSummary, determine_sync_type, get_error_summary
from tasks_manager.utils import get_client_auth_from_config


def calculate_snowflake_grain_lookback(sync_type: SyncType, grain_config: MetricCacheGrainConfig) -> dict[str, int]:
    """Calculate lookback period for Snowflake cache using grain config."""
    if sync_type == SyncType.FULL:
        # Use initial sync period for full cache
        return {"days": grain_config.initial_sync_period}
    else:
        # Use delta sync period for incremental cache
        return {"days": grain_config.delta_sync_period}


@task(
    name="get_enabled_grains_for_tenant",
    task_run_name="get_enabled_grains_for_tenant",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=3600,
    cache_key_fn=task_input_hash,
)
async def get_enabled_grains_for_tenant() -> list[MetricCacheGrainConfig]:
    """Get enabled grain cache configurations for a tenant."""
    async with get_async_session() as session:
        crud = CRUDMetricCacheGrainConfig(MetricCacheGrainConfig, session)
        enabled_grains = await crud.get_enabled_grains()
        return enabled_grains


@task(
    name="get_enabled_grain_config",
    task_run_name="get_cache_config:grain={grain}",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=3600,
    cache_key_fn=task_input_hash,
)
async def get_enabled_grain_config(grain: Granularity) -> MetricCacheGrainConfig:
    """Get enabled grain cache configurations for a tenant."""
    async with get_async_session() as session:
        crud = CRUDMetricCacheGrainConfig(MetricCacheGrainConfig, session)
        config = await crud.get_by_grain(grain)
        return config


@task(
    name="get_enabled_metrics_for_tenant",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=3600,
    cache_key_fn=task_input_hash,
)
async def get_enabled_metrics_for_tenant() -> list[MetricCacheConfig]:
    """Get enabled metric cache configurations for a tenant."""
    async with get_async_session() as session:
        crud = CRUDMetricCacheConfig(MetricCacheConfig, session)
        enabled_configs = await crud.get_enabled_metrics()
        return enabled_configs


@task(name="get_snowflake_client", retries=1, retry_delay_seconds=30, timeout_seconds=3600)
async def get_snowflake_client() -> SnowflakeClient:
    """Get Snowflake client for a tenant."""

    # Get configuration and auth
    config = await AppConfig.load("default")
    auth = get_client_auth_from_config(config)

    # Get insight backend client
    insight_client = InsightBackendClient(base_url=config.insights_backend_server_host, auth=auth)

    # Get snowflake configuration
    snowflake_config = await insight_client.get_snowflake_config()

    # Create SnowflakeConfigModel from the API response
    client_config = SnowflakeConfigModel(
        account_identifier=snowflake_config["account_identifier"],
        username=snowflake_config["username"],
        password=snowflake_config.get("password"),
        private_key=snowflake_config.get("private_key"),
        private_key_passphrase=snowflake_config.get("private_key_passphrase"),
        database=snowflake_config["database"],
        db_schema=snowflake_config["db_schema"],
        warehouse=snowflake_config.get("warehouse"),
        role=snowflake_config.get("role"),
        auth_method=snowflake_config["auth_method"],
    )

    # Create and return Snowflake client
    return SnowflakeClient(config=client_config)


@task(
    name="cache_metric_to_snowflake",
    task_run_name="cache_metric_to_snowflake:{metric_id}:{grain}",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=3600,
)
async def cache_metric_to_snowflake(
    metric_id: str,
    grain: Granularity,
    grain_config: MetricCacheGrainConfig,
) -> dict:
    """
    Cache a single metric to Snowflake by pulling data directly from the cube.

    Args:
        metric_id: The metric ID to cache
        grain: The granularity level
        grain_config: Cache configuration for the grain

    Returns:
        dict: Statistics about the cache operation
    """
    logger = get_run_logger()
    tenant_id = get_tenant_id()
    if tenant_id is None:
        raise ValueError("Tenant ID is not set")

    try:
        # Fetch metric details
        metric: MetricDetail = await get_metric(metric_id)

        # Determine sync type using the same logic as semantic_data_sync_metric
        sync_type = await determine_sync_type(metric_id, tenant_id, grain, sync_operation=SyncOperation.SNOWFLAKE_CACHE)

        # Calculate date range using Snowflake-specific grain config
        grain_lookback = calculate_snowflake_grain_lookback(sync_type, grain_config)
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(**grain_lookback)

        logger.info(
            "Caching metric to Snowflake - Metric: %s, Tenant: %d, Grain: %s, Sync Type: %s, " "Date Range: %s to %s",
            metric_id,
            tenant_id,
            grain.value,
            sync_type.value,
            start_date.isoformat(),
            end_date.isoformat(),
        )

        # Fetch metric values directly from cube (same as semantic manager does)
        values, fetch_stats = await fetch_metric_values(
            tenant_id=tenant_id,
            metric=metric,
            start_date=start_date,
            end_date=end_date,
            grain=grain,
        )

        logger.info(
            "Fetched %d values from cube for metric %s, %d skipped",
            len(values),
            metric_id,
            fetch_stats["skipped"],
        )

        # Check if no values were fetched - return early if empty
        if not values:
            logger.info("No values fetched for metric %s - skipping cache operation", metric_id)
            return {
                "tenant_id": tenant_id,
                "metric_id": metric_id,
                "grain": grain.value,
                "status": "failed",
                "sync_type": sync_type.value,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "time_series_stats": {
                    **fetch_stats,
                    "dimension_id": None,
                    "sync_type": sync_type.value,
                    "error": None,
                },
                "dimensional_stats": [],
                "failed_tasks": [],
                "error": f"No values fetched for metric {metric_id}",
            }

        # Cache the data to Snowflake
        async with get_async_session() as session:
            snowflake_client = await get_snowflake_client()
            cache_manager = SnowflakeSemanticCacheManager(session, snowflake_client)

            # Cache the metric using the fetched values
            result = await cache_manager.cache_metric_time_series(
                metric_id=metric_id,
                grain=grain,
                sync_type=sync_type,
                start_date=start_date,
                end_date=end_date,
                values=values,
            )

            # Update skipped records
            result["time_series_stats"]["skipped"] = fetch_stats["skipped"]

            logger.info(
                "Successfully cached metric %s: %d records",
                metric_id,
                result["time_series_stats"]["processed"],
            )

            return {
                "metric_id": metric_id,
                "tenant_id": tenant_id,
                "grain": grain.value,
                "status": result["status"],
                "sync_type": sync_type.value,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "time_series_stats": result["time_series_stats"],
                "dimensional_stats": [],
                "error": result.get("error"),
            }

    except Exception as e:
        logger.error(
            "Failed to cache metric %s to Snowflake: %s",
            metric_id,
            str(e),
            exc_info=True,
        )
        return {
            "metric_id": metric_id,
            "status": "failed",
            "records_processed": 0,
            "sync_type": sync_type.value if "sync_type" in locals() else SyncType.FULL.value,
            "start_date": start_date.isoformat() if "start_date" in locals() else "",
            "end_date": end_date.isoformat() if "end_date" in locals() else "",
            "error": str(e),
        }


@task(
    name="cache_tenant_metrics_to_snowflake",
    task_run_name="cache_tenant_metrics_to_snowflake:tenant={tenant_id}_grain={grain}",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=3600,
)
async def cache_tenant_metrics_to_snowflake(
    tenant_id: int,
    grain: Granularity,
    grain_config: MetricCacheGrainConfig,
    metrics: list[str] | None = None,
) -> SyncSummary:
    """Cache all enabled metrics for a tenant to Snowflake using parallel processing."""
    logger = get_run_logger()

    try:
        # Get enabled metrics using the CRUD method
        enabled_metric_configs = await get_enabled_metrics_for_tenant()
        enabled_metric_ids = [config.metric_id for config in enabled_metric_configs]
        if metrics:
            # check if all metrics are enabled
            enabled_metric_ids = [metric for metric in metrics if metric in enabled_metric_ids]
            if len(enabled_metric_ids) != len(metrics):
                logger.warning("Some metrics are not enabled for tenant %d from given metrics", tenant_id)
        if len(enabled_metric_ids) == 0:
            logger.info(
                "No enabled metrics found for tenant %d from %s metrics", tenant_id, "given" if metrics else "all"
            )
            error_summary = get_error_summary(
                tenant_id=tenant_id,
                grain=grain,
                metric_id=None,
                error="No enabled metrics found for tenant",
            )

            # Create artifact for the error summary
            await create_tenant_cache_summary_artifact(error_summary)
            logger.info("Created artifact for empty metrics summary")

            return error_summary

        async with get_async_session() as session:
            cache_manager = SnowflakeSemanticCacheManager(session)

            # Prepare run info
            run_context: EngineContext = get_run_context()  # type: ignore
            run_info = {}
            if hasattr(run_context, "task_run"):
                run_info = {
                    "flow_run_id": str(run_context.task_run.flow_run_id),
                    "task_run_id": str(run_context.task_run.id),
                    "run_name": run_context.task_run.name,
                    "start_time": (
                        run_context.start_time.strftime("%Y-%m-%dT%H:%M:%S") if run_context.start_time else None
                    ),
                }

            # Start tenant-level sync operation
            await cache_manager.start_tenant_cache_operation(
                sync_operation=SyncOperation.SNOWFLAKE_CACHE,
                grain=grain,
                run_info=run_info,
            )

            # Send tenant sync started event
            send_tenant_sync_started_event(
                tenant_id=tenant_id,
                sync_operation=SyncOperation.SNOWFLAKE_CACHE,
                grain=grain,
                metrics_count=len(enabled_metric_ids),
            )

            logger.info(
                "Starting parallel cache sync for %d enabled metrics - Tenant: %d, Grain: %s",
                len(enabled_metric_ids),
                tenant_id,
                grain.value,
            )

            # Process metrics in parallel using .map
            cache_futures = cache_metric_to_snowflake.map(
                metric_id=enabled_metric_ids,
                grain=unmapped(grain),
                grain_config=unmapped(grain_config),
            )

            # Wait for all futures and collect results
            wait(cache_futures)
            results: Any = [future.result() for future in cache_futures]

            # Analyze results
            metrics_succeeded = 0
            metrics_failed = 0
            total_records_processed = 0
            failed_tasks = []

            for result in results:
                if result["status"] == "success":
                    metrics_succeeded += 1
                    total_records_processed += result["time_series_stats"]["processed"]
                else:
                    metrics_failed += 1
                    failed_tasks.append(
                        {
                            "metric_id": result["metric_id"],
                            "task": f"cache_metric_{result['metric_id']}",
                            "error": result["error"],
                        }
                    )

            # Determine overall status and error message
            error_message = None
            if metrics_failed == 0:
                status = SyncStatus.SUCCESS
                overall_status = "success"
            elif metrics_succeeded > 0:
                status = SyncStatus.SUCCESS  # Partial success is still success for tenant level
                overall_status = "partial_success"
                error_message = f"Partial success: {metrics_failed} out of {len(enabled_metric_ids)} metrics failed"
            else:
                status = SyncStatus.FAILED
                overall_status = "failed"
                error_message = f"All {metrics_failed} metrics failed to sync"

            # End tenant-level sync operation
            await cache_manager.end_tenant_cache_operation(
                sync_operation=SyncOperation.SNOWFLAKE_CACHE,
                grain=grain,
                status=status,
                metrics_processed=len(enabled_metric_ids),
                metrics_succeeded=metrics_succeeded,
                metrics_failed=metrics_failed,
                run_info=run_info,
                error=error_message,
            )

            # Create summary
            summary: SyncSummary = {
                "metric_id": None,
                "tenant_id": tenant_id,
                "status": overall_status,  # type: ignore
                "sync_type": SyncType.FULL.value,  # Overall sync type
                "grain": grain.value,
                "start_date": datetime.now().date().isoformat(),
                "end_date": datetime.now().date().isoformat(),
                "time_series_stats": {
                    "processed": total_records_processed,
                    "failed": metrics_failed,
                    "skipped": 0,
                    "total": len(enabled_metric_ids),
                    "dimension_id": None,
                    "sync_type": SyncType.FULL.value,
                    "error": None,
                },
                "dimensional_stats": [],  # No dimensional data in simplified version
                "failed_tasks": failed_tasks,
                "error": None,
            }

            logger.info(
                "Tenant cache sync completed - Tenant: %d, Status: %s, Metrics: %d/%d succeeded, " "Total records: %d",
                tenant_id,
                overall_status,
                metrics_succeeded,
                len(enabled_metric_ids),
                total_records_processed,
            )

            # Send tenant sync finished event
            send_tenant_sync_finished_event(
                tenant_id=tenant_id,
                sync_operation=SyncOperation.SNOWFLAKE_CACHE,
                grain=grain,
                summary=summary,
                error=None if status == SyncStatus.SUCCESS else f"Failed metrics: {metrics_failed}",
            )

            # Create artifact for the sync summary
            await create_tenant_cache_summary_artifact(summary)
            logger.info("Created artifact for sync summary - Status: %s", overall_status)

            return summary

    except Exception as e:
        logger.error("Failed to cache tenant metrics: %s", str(e), exc_info=True)

        # Prepare run info for error case
        try:
            run_context: EngineContext = get_run_context()  # type: ignore
            run_info = {}
            if run_context.flow_run:
                run_info = {
                    "flow_run_id": run_context.flow_run.id,
                    "flow_run_name": run_context.flow_run.name,
                    "deployment_id": run_context.flow_run.deployment_id,
                    "start_time": (
                        run_context.flow_run.start_time.strftime("%Y-%m-%dT%H:%M:%S")
                        if run_context.flow_run.start_time
                        else None
                    ),
                }
        except Exception:
            run_info = {}

        # Update tenant sync status for error case if session is available
        try:
            async with get_async_session() as session:
                cache_manager = SnowflakeSemanticCacheManager(session)
                await cache_manager.end_tenant_cache_operation(
                    sync_operation=SyncOperation.SNOWFLAKE_CACHE,
                    grain=grain,
                    status=SyncStatus.FAILED,
                    metrics_processed=0,
                    metrics_succeeded=0,
                    metrics_failed=1,
                    run_info=run_info,
                    error=str(e),
                )
        except Exception as session_error:
            logger.error("Failed to update tenant sync status for error case: %s", str(session_error))

        # Return error summary
        error_summary = {
            "metric_id": None,
            "tenant_id": tenant_id,
            "status": "failed",
            "sync_type": SyncType.FULL.value,
            "grain": grain.value,
            "start_date": datetime.now().date().isoformat(),
            "end_date": datetime.now().date().isoformat(),
            "time_series_stats": {
                "processed": 0,
                "failed": 1,
                "skipped": 0,
                "total": 1,
                "dimension_id": None,
                "sync_type": SyncType.FULL.value,
                "error": str(e),
            },
            "dimensional_stats": [],
            "failed_tasks": [],
            "error": str(e),
        }

        # Send tenant sync finished event for error case
        send_tenant_sync_finished_event(
            tenant_id=tenant_id,
            sync_operation=SyncOperation.SNOWFLAKE_CACHE,
            grain=grain,
            summary=error_summary,
            error=str(e),
        )

        # Create artifact for the error summary
        await create_tenant_cache_summary_artifact(error_summary)
        logger.info("Created artifact for error summary")

        return error_summary
