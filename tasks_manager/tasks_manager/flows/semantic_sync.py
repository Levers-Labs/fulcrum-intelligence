from datetime import date, datetime, timedelta

from prefect import flow, get_run_logger
from prefect.events import emit_event

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from query_manager.core.schemas import MetricDetail
from query_manager.semantic_manager.models import SyncType
from tasks_manager.tasks.common import fetch_tenants
from tasks_manager.tasks.query import fetch_metrics_for_tenant, get_metric
from tasks_manager.tasks.semantic_manager import (
    SyncSummary,
    calculate_grain_lookback,
    create_sync_summary_artifact,
    determine_sync_type,
    fetch_and_store_metric_dimensional_time_series,
    fetch_and_store_metric_time_series,
    send_metric_semantic_sync_finished_event,
)


@flow(  # type: ignore
    name="semantic_data_sync_metric",
    flow_run_name="semantic_data_sync_metric:tenant={tenant_id_str}_metric={metric_id}_grain={grain}",
)
async def semantic_data_sync_metric(tenant_id_str: str, metric_id: str, grain: Granularity) -> SyncSummary:  # type: ignore
    """
    Orchestrates the semantic data sync process for a metric.

    Args:
        tenant_id_str: The tenant ID as string
        metric_id: The metric ID to sync
        grain: The granularity level to sync

    Returns:
        SyncSummary containing detailed statistics about the sync operation
    """
    logger = get_run_logger()
    tenant_id = int(tenant_id_str)

    # set context
    set_tenant_id(tenant_id)

    # Determine sync type for base metric
    base_sync_type = await determine_sync_type(metric_id, tenant_id, grain)

    # Calculate date range
    grain_lookback = calculate_grain_lookback(base_sync_type, grain)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(**grain_lookback)

    # Track if any component uses a FULL sync
    any_full_sync = base_sync_type == SyncType.FULL

    try:
        logger.info(
            "Starting semantic sync - Metric: %s, Tenant: %s, Grain: %s, Base Sync Type: %s",
            metric_id,
            tenant_id,
            grain.value,
            base_sync_type.value,
        )

        # Fetch metric details
        metric: MetricDetail = await get_metric(metric_id)  # type: ignore

        # Process base time series
        time_series_future = fetch_and_store_metric_time_series.submit(  # type: ignore
            metric_id=metric_id,
            metric=metric,
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
            grain=grain,
            sync_type=base_sync_type,
        )

        # Process dimensions in parallel if they exist
        # Store futures in a dictionary with dimension_id as key
        dimensional_futures = {}
        if metric.dimensions:
            for dim in metric.dimensions:
                dimension_id = dim.dimension_id

                # Check if full sync has been completed for this dimension
                dim_sync_type = await determine_sync_type(metric_id, tenant_id, grain, dimension_name=dimension_id)
                # Update any_full_sync flag
                any_full_sync = any_full_sync or (dim_sync_type == SyncType.FULL)

                # Calculate date range for dimension
                dim_grain_lookback = calculate_grain_lookback(dim_sync_type, grain)
                dim_start_date = end_date - timedelta(**dim_grain_lookback)

                logger.info(
                    "Scheduling dimensional sync - Dimension: %s, Sync Type: %s",
                    dimension_id,
                    dim_sync_type.value,
                )

                dimensional_futures[dimension_id] = fetch_and_store_metric_dimensional_time_series.submit(  # type: ignore
                    metric_id=metric_id,
                    metric=metric,
                    tenant_id=tenant_id,
                    dimension_id=dimension_id,
                    start_date=dim_start_date,
                    end_date=end_date,
                    grain=grain,
                    sync_type=dim_sync_type,
                )

        # Wait for all futures and collect results
        failed_tasks = []
        dimensional_stats = []
        try:
            time_series_stats = time_series_future.result()
        except Exception as e:
            logger.error(
                "Error fetching time series stats - Metric: %s, Grain: %s, Error: %s",
                metric_id,
                grain.value,
                str(e),
                exc_info=True,
            )
            time_series_stats = {  # type: ignore
                "processed": 0,
                "failed": 1,
                "skipped": 0,
                "total": 1,
                "dimension_id": None,
                "sync_type": SyncType.FULL.value,
                "error": str(e),
            }
            failed_tasks.append(
                {
                    "metric_id": metric_id,
                    "dimension_id": None,
                    "task": f"time_series_sync_for_metric_id={metric_id}",
                    "error": str(e),
                }
            )

        for dimension_id, dim_future in dimensional_futures.items():
            try:
                dim_stats = dim_future.result()
            except Exception as e:
                logger.error(
                    "Error fetching dimensional stats - Metric: %s, Dimension: %s, Grain: %s, Error: %s",
                    metric_id,
                    dimension_id,
                    grain.value,
                    str(e),
                    exc_info=True,
                )
                dim_stats = {  # type: ignore
                    "processed": 0,
                    "failed": 0,
                    "skipped": 0,
                    "total": 0,
                    "dimension_id": dimension_id,
                    "sync_type": SyncType.FULL.value,
                    "error": str(e),
                }
                failed_tasks.append(
                    {
                        "metric_id": metric_id,
                        "dimension_id": dimension_id,
                        "task": f"dimensional_sync_for_dimension_id={dimension_id}",
                        "error": str(e),
                    }
                )
            dimensional_stats.append(dim_stats)

        # For the summary, use FULL if any component used it, otherwise INCREMENTAL
        overall_sync_type = SyncType.FULL if any_full_sync else SyncType.INCREMENTAL

        summary: SyncSummary = {
            "metric_id": metric_id,
            "tenant_id": tenant_id,
            "status": (
                "success"
                if not failed_tasks
                else ("partial_success" if len(failed_tasks) < len(dimensional_stats) + 1 else "failed")
            ),
            "sync_type": overall_sync_type.value,
            "grain": grain.value,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "time_series_stats": time_series_stats,  # type: ignore
            "dimensional_stats": dimensional_stats,  # type: ignore
            "failed_tasks": failed_tasks,
            "error": None,
        }

        logger.info(
            "Sync completed, Status: %s - Metric: %s, Grain: %s\n"
            "Time Series - Processed: %d, Failed: %d, Total: %d\n"
            "Dimensions - Count: %d, Total Processed: %d",
            summary["status"],
            metric_id,
            grain.value,
            time_series_stats["processed"],  # type: ignore
            time_series_stats["failed"],  # type: ignore
            time_series_stats["total"],  # type: ignore
            len(dimensional_stats),
            sum(s["processed"] for s in dimensional_stats),  # type: ignore
        )

        # Create summary artifact
        await create_sync_summary_artifact(summary)

        # Emit completion event
        send_metric_semantic_sync_finished_event(metric_id=metric_id, tenant_id=tenant_id, grain=grain, summary=summary)
    except Exception as e:
        # For error case, default to FULL sync type
        error_summary: SyncSummary = {
            "metric_id": metric_id,
            "tenant_id": tenant_id,
            "status": "failed",
            "sync_type": SyncType.FULL.value,
            "grain": grain.value,
            "start_date": start_date.isoformat() if "start_date" in locals() else "",
            "end_date": end_date.isoformat() if "end_date" in locals() else "",
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
            "error": str(e),
            "failed_tasks": [],
        }

        logger.error("Sync failed - Metric: %s, Grain: %s, Error: %s", metric_id, grain.value, str(e), exc_info=True)

        # Create failure summary artifact
        await create_sync_summary_artifact(error_summary)

        # Emit failure event
        send_metric_semantic_sync_finished_event(
            metric_id=metric_id, tenant_id=tenant_id, grain=grain, summary=error_summary, error=str(e)
        )

        raise
    finally:
        reset_context()


@flow(  # type: ignore
    name="semantic_data_sync_tenant",
    flow_run_name="semantic_data_sync_tenant:tenant={tenant_id_str}_grain={grain}",
    description="Emits sync events for all metrics of a tenant for a specific grain",
)
async def semantic_data_sync_tenant(tenant_id_str: str, grain: Granularity) -> None:
    """
    Trigger semantic data sync events for all metrics of a tenant.

    Args:
        tenant_id_str: The tenant ID
        grain: Granularity level to sync
    """
    logger = get_run_logger()
    tenant_id = int(tenant_id_str)
    logger.info(
        "Starting to emit sync events - Tenant: %d, Grain: %s",
        tenant_id,
        grain.value,
    )

    # set context
    set_tenant_id(tenant_id)

    try:
        # Fetch all metrics for the tenant
        metrics = await fetch_metrics_for_tenant(tenant_id)  # type: ignore
        logger.info("Found %d metrics for tenant %d", len(metrics), tenant_id)

        # Emit event for each metric
        for metric in metrics:
            emit_event(
                event="metric.semantic.sync.requested",
                resource={
                    "prefect.resource.id": f"metric.{metric['metric_id']}",
                    "metric_id": metric["metric_id"],
                    "grain": grain.value,
                    "tenant_id": str(tenant_id),
                },
                payload={
                    "timestamp": datetime.now().isoformat(),
                },
            )

        logger.info(
            "Successfully emitted sync events for %d metrics - Tenant: %d, Grain: %s",
            len(metrics),
            tenant_id,
            grain.value,
        )

    except Exception as e:
        logger.error(
            "Failed to emit sync events - Tenant: %d, Grain: %s, Error: %s",
            tenant_id,
            grain.value,
            str(e),
            exc_info=True,
        )
        raise
    finally:
        reset_context()


@flow(  # type: ignore
    name="semantic_data_sync",
    flow_run_name="semantic_data_sync:grain={grain}",
    description="Emits sync events for all tenants for a specific grain",
)
async def semantic_data_sync(grain: Granularity) -> None:
    """
    Trigger semantic data sync events for all tenants.

    Args:
        grain: Granularity level to sync
    """
    logger = get_run_logger()
    logger.info(
        "Starting to emit sync events - Grain: %s",
        grain.value,
    )

    try:
        # Fetch all tenants
        tenants = await fetch_tenants()  # type: ignore
        logger.info("Found %d tenants", len(tenants))

        # Emit event for each tenant
        for tenant in tenants:
            emit_event(
                event="tenant.semantic.sync.requested",
                resource={
                    "prefect.resource.id": f"tenant.{tenant['id']}",
                    "tenant_id": str(tenant["id"]),
                    "grain": grain.value,
                },
                payload={
                    "timestamp": datetime.now().isoformat(),
                },
            )

        logger.info(
            "Successfully emitted sync events for %d tenants - Grain: %s",
            len(tenants),
            grain.value,
        )

    except Exception as e:
        logger.error(
            "Failed to emit sync events - Grain: %s, Error: %s",
            grain.value,
            str(e),
            exc_info=True,
        )
        raise
    finally:
        reset_context()
