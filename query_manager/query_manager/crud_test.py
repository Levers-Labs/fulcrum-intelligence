from datetime import date

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from query_manager.db.config import get_async_session
from query_manager.semantic_manager.dependencies import get_semantic_manager
from query_manager.semantic_manager.models import SyncStatus, SyncType


async def metric_time_series_test() -> None:
    set_tenant_id(1)
    metric_id = "newInqs"
    grain = Granularity.MONTH
    dimension_name = None
    sync_type = SyncType.FULL
    start_date = date(2025, 1, 1)
    end_date = date(2025, 2, 10)
    transformed_values = [
        {
            "metric_id": metric_id,
            "tenant_id": 1,
            "date": date(2025, 2, 1),
            "grain": grain,
            "value": 220,
        },
        {
            "metric_id": metric_id,
            "tenant_id": 1,
            "date": date(2025, 3, 1),
            "grain": grain,
            "value": 300,
        },
    ]
    async with get_async_session() as session:
        semantic_manager = await get_semantic_manager(session)
        await semantic_manager.metric_sync_status.start_sync(
            metric_id=metric_id,
            grain=grain,
            dimension_name=dimension_name,
            sync_type=sync_type,
            start_date=start_date,
            end_date=end_date,
        )
        # Store total values using semantic manager
        result = await semantic_manager.bulk_upsert_time_series(transformed_values)
        records_processed = result.get("processed", 0)
        await semantic_manager.metric_sync_status.end_sync(
            metric_id=metric_id,
            grain=grain,
            dimension_name=None,
            sync_type=sync_type,
            status=SyncStatus.SUCCESS,
            records_processed=records_processed,
        )
        await semantic_manager.metric_sync_status.end_sync(
            metric_id=metric_id,
            grain=grain,
            dimension_name=None,
            sync_type=sync_type,
            status=SyncStatus.FAILED,
            error="Some error occurred",
        )
        result = await semantic_manager.metric_sync_status.get_sync_status(
            tenant_id=1, metric_id=metric_id, grain=grain
        )
        return result


async def metric_dimensional_time_series_test() -> None:
    set_tenant_id(1)
    metric_id = "newInqs"
    grain = Granularity.MONTH
    dimension_name = "dim1"
    sync_type = SyncType.FULL
    start_date = date(2025, 1, 1)
    end_date = date(2025, 2, 10)
    transformed_values = [
        {
            "metric_id": metric_id,
            "tenant_id": 1,
            "date": date(2025, 1, 1),
            "grain": grain,
            "value": 100,
            "dimension_name": dimension_name,
            "dimension_slice": "value1",
        },
        {
            "metric_id": metric_id,
            "tenant_id": 1,
            "date": date(2025, 2, 1),
            "grain": grain,
            "value": 200,
            "dimension_name": dimension_name,
            "dimension_slice": "value2",
        },
    ]
    async with get_async_session() as session:
        semantic_manager = await get_semantic_manager(session)
        await semantic_manager.metric_sync_status.start_sync(
            metric_id=metric_id,
            grain=grain,
            dimension_name=dimension_name,
            sync_type=sync_type,
            start_date=start_date,
            end_date=end_date,
        )
        result = await semantic_manager.bulk_upsert_dimensional_time_series(transformed_values)
        records_processed = result.get("processed", 0)
        await semantic_manager.metric_sync_status.end_sync(
            metric_id=metric_id,
            grain=grain,
            dimension_name=dimension_name,
            sync_type=sync_type,
            status=SyncStatus.SUCCESS,
            records_processed=records_processed,
        )
        result = await semantic_manager.metric_sync_status.get_sync_status(
            tenant_id=1, metric_id=metric_id, grain=grain
        )
        return result


if __name__ == "__main__":
    import asyncio

    asyncio.run(metric_dimensional_time_series_test())
