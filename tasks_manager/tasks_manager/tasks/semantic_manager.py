import os
from datetime import date
from typing import Literal, TypedDict

from prefect import get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from query_manager.core.schemas import MetricDetail
from query_manager.db.config import get_async_session
from query_manager.semantic_manager.crud import SemanticManager
from query_manager.semantic_manager.models import SyncStatus, SyncType
from tasks_manager.config import AppConfig
from tasks_manager.tasks.query import fetch_metric_values


class SyncStats(TypedDict):
    processed: int
    skipped: int
    failed: int
    total: int
    dimension_id: str | None


class SyncSummary(TypedDict):
    metric_id: str
    tenant_id: int
    status: Literal["success", "failed"]
    sync_type: str
    grain: str
    start_date: str
    end_date: str
    time_series_stats: SyncStats
    dimensional_stats: list[SyncStats]
    error: str | None


def calculate_grain_lookback(sync_type: SyncType, grain: Granularity, lookback_days: int = 90) -> dict[str, int]:
    """Calculate lookback period for each grain."""
    if grain == Granularity.DAY:
        # 365 days for full sync, lookback_days for incremental sync
        return {"days": 365 if sync_type == SyncType.FULL else lookback_days}
    elif grain == Granularity.WEEK:
        # 104 weeks for full sync, lookback_days // 7 for incremental sync
        return {"weeks": 104 if sync_type == SyncType.FULL else lookback_days // 7}
    else:  # MONTH
        # 5 years for full sync, lookback_days // 10 for incremental sync
        return {"weeks": 260 if sync_type == SyncType.FULL else lookback_days // 10}


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
                f"| {stat['dimension_id']} | {stat['processed']} | {stat['failed']} | {stat['skipped']} | {stat['total']} |"  # noqa: E501
                for stat in dim_stats
            ]
        )
        if dim_stats
        else "No dimensional data processed"
    )

    # Create error section separately
    error_section = ""
    if summary["error"]:
        error_section = f"""
## Error
```
{summary['error']}
```
"""

    markdown = f"""# Semantic Sync Summary for metric {summary['metric_id']} ({summary['grain']})

## Overview
- **Metric ID**: {summary['metric_id']}
- **Tenant ID**: {summary['tenant_id']}
- **Status**: {summary['status']}
- **Sync Type**: {summary['sync_type']}
- **Grain**: {summary['grain']}
- **Period**: {summary['start_date']} to {summary['end_date']}

## Time Series Statistics
| Metric | Processed | Failed | Skipped | Total |
|--------|-----------|---------|----------|--------|
| Base Time Series | {time_series['processed']} | {time_series['failed']} | {time_series['skipped']} | {time_series['total']} |

## Dimensional Statistics
| Dimension | Processed | Failed | Skipped | Total |
|-----------|-----------|---------|----------|--------|
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

    await create_markdown_artifact(
        key=f"metric-{summary['metric_id'].lower()}-{summary['grain'].lower()}-sync-summary", markdown=markdown
    )


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
        return SyncStats(processed=0, skipped=0, failed=1, total=1, dimension_id=None)
    finally:
        reset_context()


@task(  # type: ignore
    name="semantic_data_sync_for_metric_grain_and_dimension",
    task_run_name="semantic_dimensional_data_sync:metric={metric_id}:dim={dimension_id}:grain={grain}",
    retries=1,
    retry_delay_seconds=30,
    tags=["db-operation", "semantic-manager"],
    timeout_seconds=450,  # 7 minutes
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
        return SyncStats(dimension_id=dimension_id, processed=0, skipped=0, failed=1, total=1)
    finally:
        reset_context()
