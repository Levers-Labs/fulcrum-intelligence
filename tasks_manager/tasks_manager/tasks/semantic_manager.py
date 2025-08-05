import os
from datetime import date, datetime
from typing import Literal, TypedDict

from prefect import get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.events import Event, emit_event

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from query_manager.core.schemas import MetricDetail
from query_manager.db.config import get_async_session
from query_manager.semantic_manager.crud import SemanticManager
from query_manager.semantic_manager.models import SyncOperation, SyncStatus, SyncType
from tasks_manager.config import AppConfig
from tasks_manager.tasks.query import fetch_metric_values


class SyncStats(TypedDict):
    processed: int
    skipped: int
    failed: int
    total: int
    dimension_id: str | None
    sync_type: str
    error: str | None


class SyncSummary(TypedDict):
    # will be null for tenant sync summary
    metric_id: str | None
    tenant_id: int
    status: Literal["success", "failed", "partial_success"]
    sync_type: str
    grain: str
    start_date: str
    end_date: str
    time_series_stats: SyncStats
    dimensional_stats: list[SyncStats]
    failed_tasks: list[dict] | None
    error: str | None


def get_error_summary(
    tenant_id: int,
    grain: Granularity,
    metric_id: str | None = None,
    error: str | None = None,
    sync_type: SyncType = SyncType.FULL,
) -> SyncSummary:
    """Get a summary of the sync error."""
    return SyncSummary(
        metric_id=metric_id,
        tenant_id=tenant_id,
        status="failed",
        sync_type=sync_type.value,
        grain=grain.value,
        start_date=datetime.now().date().isoformat(),
        end_date=datetime.now().date().isoformat(),
        time_series_stats=SyncStats(
            processed=0,
            skipped=0,
            failed=0,
            total=0,
            dimension_id=None,
            sync_type=sync_type.value,
            error=error,
        ),
        dimensional_stats=[],
        failed_tasks=[],
        error=error,
    )


def send_metric_semantic_sync_finished_event(
    metric_id: str, tenant_id: int, grain: Granularity, summary: SyncSummary, error: str | None = None
) -> Event | None:
    """
    Send a metric semantic sync finished event.
    It could be a success, a failure or a partial success.
    """
    event_name = "metric.semantic.sync.success"
    if summary["status"] == "failed":
        event_name = "metric.semantic.sync.failed"
    elif summary["status"] == "partial_success":
        event_name = "metric.semantic.sync.partial_success"

    event = emit_event(
        event=event_name,
        resource={
            "prefect.resource.id": f"metric.{metric_id}",
            "metric_id": metric_id,
            "tenant_id": str(tenant_id),
            "grain": grain.value,
        },
        payload={
            "summary": summary,
            "timestamp": datetime.now().isoformat(),
            "error": error,
        },
    )
    return event


def calculate_grain_lookback(sync_type: SyncType, grain: Granularity) -> dict[str, int]:
    """Calculate lookback period for each grain."""
    if grain == Granularity.DAY:
        # 365 days for full sync, 90 days (3 months) for incremental sync
        return {"days": 365 if sync_type == SyncType.FULL else 90}
    elif grain == Granularity.WEEK:
        # 104 weeks for full sync, 24 weeks (6 months) for incremental sync
        return {"weeks": 104 if sync_type == SyncType.FULL else 24}
    else:  # MONTH
        # 5 years for full sync, 12 months (1 year) for incremental sync
        return {"weeks": 260 if sync_type == SyncType.FULL else 52}


async def create_sync_summary_artifact(summary: SyncSummary) -> None:
    """
    Create a markdown artifact summarizing the sync operation.

    Args:
        summary: Dictionary containing sync operation summary
    """
    time_series = summary["time_series_stats"]
    dim_stats = summary["dimensional_stats"]

    # Calculate dimensional aggregates
    dim_totals = {
        "processed": sum(s["processed"] for s in dim_stats),
        "failed": sum(s["failed"] for s in dim_stats),
        "skipped": sum(s["skipped"] for s in dim_stats),
        "total": sum(s["total"] for s in dim_stats),
    }

    dimensions_table = (
        "\n".join(
            [
                f"| {stat['dimension_id']} | {stat.get('sync_type', 'FULL')} | {stat['processed']} | {stat['failed']} | {stat['skipped']} | {stat['total']} |"  # noqa: E501
                for stat in dim_stats
            ]
        )
        if dim_stats
        else "No dimensional data processed"
    )

    # Create status section with color coding
    status_color = {"success": "🟢", "partial_success": "🟡", "failed": "🔴"}.get(summary["status"], "⚪")
    status_section = f"- **Status**: {status_color} {summary['status'].upper()}"

    # Create detailed sync status section
    sync_status_section = "\n## Sync Status Details\n"

    # Table headers for sync status
    sync_status_section += """| Component | Status | Sync Type | Records | Error |
|-----------|---------|-----------|----------|--------|
"""

    # Time series sync status
    ts_has_error = bool(time_series.get("error") or time_series["failed"] > 0)
    ts_status = "🔴 Failed" if ts_has_error else "🟢 Success"
    ts_error = time_series.get("error") or "-"
    ts_records = f"{time_series['processed']}/{time_series['total']}" if not ts_has_error else "-"
    ts_sync_type = time_series.get("sync_type", "FULL")
    sync_status_section += f"| Time Series | {ts_status} | {ts_sync_type} | {ts_records} | {ts_error} |\n"

    # Dimensional sync status
    if dim_stats:
        for stat in dim_stats:
            dim_has_error = bool(stat.get("error") or stat["failed"] > 0)
            dim_status = "🔴 Failed" if dim_has_error else "🟢 Success"
            dim_error = stat.get("error") or "-"
            dim_records = f"{stat['processed']}/{stat['total']}" if not dim_has_error else "-"
            dim_sync_type = stat.get("sync_type", "FULL")
            sync_status_section += f"| Dimension: {stat['dimension_id']} | {dim_status} | {dim_sync_type} | {dim_records} | {dim_error} |\n"  # noqa: E501

    # Create error section
    error_section = ""
    if summary.get("error") or summary.get("failed_tasks"):
        error_section = "\n## Errors\n"
        if summary.get("error"):
            error_section += f"""```
{summary['error']}
```\n"""

        if summary.get("failed_tasks"):
            error_section += "\n### Failed Tasks\n"
            for task in summary.get("failed_tasks", []):  # type: ignore
                task_name = task.get("task", "Unknown Task")
                task_error = task.get("error", "No error details available")
                error_section += f"""- **{task_name}**
```
{task_error}
```\n"""

    markdown = f"""# Semantic Sync Summary for metric {summary['metric_id']} ({summary['grain']})

## Overview
- **Metric ID**: {summary['metric_id']}
- **Tenant ID**: {summary['tenant_id']}
{status_section}
- **Sync Type**: {summary['sync_type']}
- **Grain**: {summary['grain']}
- **Period**: {summary['start_date']} to {summary['end_date']}
{sync_status_section}

## Time Series Statistics
| Metric | Sync Type | Processed | Failed | Skipped | Total |
|--------|-----------|-----------|---------|----------|--------|
| Time Series | {time_series.get('sync_type', 'FULL')} | {time_series['processed']} | {time_series['failed']} | {time_series['skipped']} | {time_series['total']} |

## Dimensional Statistics
| Dimension | Sync Type | Processed | Failed | Skipped | Total |
|-----------|-----------|-----------|---------|----------|--------|
{dimensions_table}

### Dimensional Totals
- Processed: {dim_totals['processed']}
- Failed: {dim_totals['failed']}
- Skipped: {dim_totals['skipped']}
- Total: {dim_totals['total']}

## Overall Statistics
- Total Records Processed: {time_series['processed'] + dim_totals['processed']}
- Total Records Failed: {time_series['failed'] + dim_totals['failed']}
- Total Records Skipped: {time_series['skipped'] + dim_totals['skipped']}
- Grand Total: {time_series['total'] + dim_totals['total']}
{error_section}"""  # noqa: E501

    await create_markdown_artifact(  # type: ignore
        key=f"metric-{summary['metric_id'].replace('_', '-').lower()}-{summary['grain'].lower()}-sync-summary",  # type: ignore
        markdown=markdown
    )


async def determine_sync_type(
    metric_id: str,
    tenant_id: int,
    grain: Granularity,
    dimension_name: str | None = None,
    sync_operation: SyncOperation = SyncOperation.SEMANTIC_SYNC,
) -> SyncType:
    """
    Determine the appropriate sync type (FULL or INCREMENTAL) for a metric component.

    For a component that has never had a successful full sync, a FULL sync will be returned.
    Otherwise, an INCREMENTAL sync will be returned.

    Args:
        metric_id: The metric ID to check
        tenant_id: The tenant ID
        grain: The granularity level to check
        dimension_name: Optional dimension name to check, if None checks the base metric
        sync_operation: Sync Operation type

    Returns:
        SyncType.FULL if no successful full sync exists, SyncType.INCREMENTAL otherwise
    """
    logger = get_run_logger()

    try:
        # set the server host for the query manager
        config = await AppConfig.load("default")
        os.environ["SERVER_HOST"] = config.query_manager_server_host

        async with get_async_session() as session:
            semantic_manager = SemanticManager(session)

            # Get sync status for the component
            sync_statuses = await semantic_manager.metric_sync_status.get_sync_status(
                tenant_id=tenant_id,
                metric_id=metric_id,
                sync_operation=sync_operation,
                grain=grain,
                dimension_name=dimension_name,
            )

            # Check if any full sync has been completed successfully
            for status in sync_statuses:
                if status.sync_type == SyncType.FULL and status.sync_status == SyncStatus.SUCCESS:
                    component_type = f"dimension: {dimension_name}" if dimension_name else "metric"
                    logger.info(
                        "Found successful full sync for %s, Metric: %s, Tenant: %d, Grain: %s, Last Sync: %s",
                        component_type,
                        metric_id,
                        tenant_id,
                        grain.value,
                        status.last_sync_at.isoformat(),
                    )
                    return SyncType.INCREMENTAL

            component_type = f"dimension: {dimension_name}" if dimension_name else "metric"
            logger.info(
                "No successful full sync found for %s, Metric: %s, Tenant: %d, Grain: %s - Will perform FULL sync",
                component_type,
                metric_id,
                tenant_id,
                grain.value,
            )
            return SyncType.FULL
    except Exception as e:
        component_type = f"dimension: {dimension_name}" if dimension_name else "base metric"
        logger.warning(
            "Error checking sync status for %s - Metric: %s, Tenant: %d, Grain: %s, Error: %s. "
            "Defaulting to FULL sync.",
            component_type,
            metric_id,
            tenant_id,
            grain.value,
            str(e),
        )
        return SyncType.FULL


@task(  # type: ignore
    name="semantic_data_sync_for_metric_grain",
    task_run_name="semantic_data_sync:metric={metric_id}:grain={grain}",
    retries=1,
    retry_delay_seconds=30,
    tags=["db-operation", "semantic-manager"],
    timeout_seconds=300,  # 5 minutes
)
async def fetch_and_store_metric_time_series(
    metric_id: str,  # noqa
    metric: MetricDetail,
    tenant_id: int,
    start_date: date,
    end_date: date,
    grain: Granularity,
    sync_type: SyncType,
) -> SyncStats:
    """
    Fetch and store metric time series values (without dimensions).

    Args:
        metric_id: ID of the metric
        metric: Metric object containing metric details
        tenant_id: ID of the tenant
        start_date: Start date for data fetching
        end_date: End date for data fetching
        grain: Granularity level for the metric data
        sync_type: Whether to perform a full sync or incremental

    Returns:
        SyncStats containing statistics about the sync operation
    """
    logger = get_run_logger()
    logger.info(
        "Starting time series sync - Metric: %s, Tenant: %s, Grain: %s, Sync Type: %s",
        metric.metric_id,
        tenant_id,
        grain,
        sync_type,
    )

    # Set the tenant ID for the context
    set_tenant_id(tenant_id)

    try:
        # set the server host for the query manager
        config = await AppConfig.load("default")
        os.environ["SERVER_HOST"] = config.query_manager_server_host

        async with get_async_session() as session:
            semantic_manager = SemanticManager(session)
            # Start the sync
            await semantic_manager.metric_sync_status.start_sync(
                metric_id=metric.metric_id,
                grain=grain,
                sync_operation=SyncOperation.SEMANTIC_SYNC,
                sync_type=sync_type,
                start_date=start_date,
                end_date=end_date,
            )

            try:
                if sync_type == SyncType.FULL:
                    logger.info("Performing full sync - clearing existing data")
                    await semantic_manager.clear_metric_data(
                        metric_id=metric.metric_id,
                        tenant_id=tenant_id,
                        grain=grain,
                        start_date=start_date,
                        end_date=end_date,
                    )

                # Fetch the metric values
                values, fetch_stats = await fetch_metric_values(
                    tenant_id=tenant_id, metric=metric, start_date=start_date, end_date=end_date, grain=grain
                )

                # Upsert the metric values
                upsert_stats = await semantic_manager.bulk_upsert_time_series(values=values)
                # Complete the sync
                await semantic_manager.metric_sync_status.end_sync(
                    metric_id=metric.metric_id,
                    grain=grain,
                    sync_operation=SyncOperation.SEMANTIC_SYNC,
                    sync_type=sync_type,
                    status=SyncStatus.SUCCESS,
                    records_processed=upsert_stats["processed"],
                )

                # Commit the changes
                await session.commit()

                # Return the stats
                result_stats = SyncStats(
                    processed=upsert_stats["processed"],
                    skipped=fetch_stats["skipped"],
                    failed=upsert_stats["failed"],
                    total=fetch_stats["total"],
                    dimension_id=None,
                    sync_type=sync_type.value,
                    error=None,
                )

                logger.info(
                    "Semantic sync complete - Metric: %s, Grain: %s, Processed: %d, Skipped: %d, Failed: %d, Total: %d",
                    metric.metric_id,
                    grain,
                    result_stats["processed"],
                    result_stats["skipped"],
                    result_stats["failed"],
                    result_stats["total"],
                )
                return result_stats

            except Exception as e:
                await semantic_manager.metric_sync_status.end_sync(
                    metric_id=metric.metric_id,
                    grain=grain,
                    sync_operation=SyncOperation.SEMANTIC_SYNC,
                    sync_type=sync_type,
                    status=SyncStatus.FAILED,
                    error=str(e),
                )
                raise

    except Exception as e:
        logger.error(
            "Critical failure in time series sync - Metric: %s, Grain: %s, Error: %s",
            metric.metric_id,
            grain,
            str(e),
            exc_info=True,
        )
        raise
    finally:
        reset_context()


@task(  # type: ignore
    name="semantic_data_sync_for_metric_grain_and_dimension",
    task_run_name="semantic_dimensional_data_sync:metric={metric_id}:dim={dimension_id}:grain={grain}",
    retries=1,
    retry_delay_seconds=30,
    tags=["db-operation", "semantic-manager"],
    timeout_seconds=300,  # 5 minutes
)
async def fetch_and_store_metric_dimensional_time_series(
    metric_id: str,  # noqa # just for run name
    metric: MetricDetail,
    tenant_id: int,
    dimension_id: str,
    start_date: date,
    end_date: date,
    grain: Granularity,
    sync_type: SyncType,
) -> SyncStats:
    """
    Fetch and store metric dimensional time series data.

    Args:
        metric_id: ID of the metric
        metric: Metric object containing metric details
        tenant_id: ID of the tenant
        dimension_id: ID of the dimension to process
        start_date: Start date for data fetching
        end_date: End date for data fetching
        grain: Granularity level for the metric data
        sync_type: Whether to perform a full sync or incremental

    Returns:
        SyncStats containing statistics about the sync operation
    """
    logger = get_run_logger()
    logger.info(
        "Starting dimensional time series sync - Metric: %s, Dimension: %s, Grain: %s, Sync Type: %s",
        metric.metric_id,
        dimension_id,
        grain,
        sync_type,
    )

    # Set the tenant ID for the context
    set_tenant_id(tenant_id)

    try:
        # set the server host for the query manager
        config = await AppConfig.load("default")
        os.environ["SERVER_HOST"] = config.query_manager_server_host

        async with get_async_session() as session:
            semantic_manager = SemanticManager(session)
            # Start the sync
            await semantic_manager.metric_sync_status.start_sync(
                metric_id=metric.metric_id,
                grain=grain,
                sync_operation=SyncOperation.SEMANTIC_SYNC,
                dimension_name=dimension_id,
                sync_type=sync_type,
                start_date=start_date,
                end_date=end_date,
            )

            try:
                if sync_type == SyncType.FULL:
                    logger.info("Performing full sync - clearing existing dimensional data")
                    await semantic_manager.clear_metric_data(
                        metric_id=metric.metric_id,
                        tenant_id=tenant_id,
                        grain=grain,
                        start_date=start_date,
                        end_date=end_date,
                        dimension_name=dimension_id,
                    )

                # Fetch the metric values
                values, fetch_stats = await fetch_metric_values(
                    tenant_id=tenant_id,
                    metric=metric,
                    start_date=start_date,
                    end_date=end_date,
                    grain=grain,
                    dimension_id=dimension_id,
                )

                upsert_stats = await semantic_manager.bulk_upsert_dimensional_time_series(values=values)
                # Complete the sync
                await semantic_manager.metric_sync_status.end_sync(
                    metric_id=metric.metric_id,
                    grain=grain,
                    sync_operation=SyncOperation.SEMANTIC_SYNC,
                    dimension_name=dimension_id,
                    sync_type=sync_type,
                    status=SyncStatus.SUCCESS,
                    records_processed=upsert_stats["processed"],
                )

                # Commit the changes
                await session.commit()

                result_stats = SyncStats(
                    dimension_id=dimension_id,
                    processed=upsert_stats["processed"],
                    skipped=fetch_stats["skipped"],
                    failed=upsert_stats["failed"],
                    total=fetch_stats["total"],
                    sync_type=sync_type.value,
                    error=None,
                )

                logger.info(
                    "Semantic sync complete - Metric: %s, Dimension: %s, Grain: %s"
                    " Processed: %d, Skipped: %d, Failed: %d, Total: %d",
                    metric.metric_id,
                    dimension_id,
                    grain,
                    result_stats["processed"],
                    result_stats["skipped"],
                    result_stats["failed"],
                    result_stats["total"],
                )
                return result_stats

            except Exception as e:
                await semantic_manager.metric_sync_status.end_sync(
                    metric_id=metric.metric_id,
                    grain=grain,
                    sync_operation=SyncOperation.SEMANTIC_SYNC,
                    dimension_name=dimension_id,
                    status=SyncStatus.FAILED,
                    sync_type=sync_type,
                    error=str(e),
                )
                raise

    except Exception as e:
        logger.error(
            "Critical failure in dimensional time series sync - Metric: %s, Dimension: %s, Grain: %s, Error: %s",
            metric.metric_id,
            dimension_id,
            grain,
            str(e),
            exc_info=True,
        )
        raise
    finally:
        reset_context()
