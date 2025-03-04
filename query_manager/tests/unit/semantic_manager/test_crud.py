"""Tests for semantic manager CRUD operations."""

from datetime import date

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from query_manager.semantic_manager.crud import (
    CRUDMetricDimensionalTimeSeries,
    CRUDMetricSyncStatus,
    CRUDMetricTimeSeries,
    SemanticManager,
)
from query_manager.semantic_manager.models import (
    MetricDimensionalTimeSeries,
    MetricSyncStatus,
    MetricTimeSeries,
    SyncStatus,
    SyncType,
)

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def semantic_manager(db_session: AsyncSession) -> SemanticManager:
    """Fixture for SemanticManager instance."""
    return SemanticManager(db_session)


@pytest_asyncio.fixture
async def metric_time_series_crud(db_session: AsyncSession) -> CRUDMetricTimeSeries:
    """Fixture for CRUDMetricTimeSeries instance."""
    return CRUDMetricTimeSeries(MetricTimeSeries, db_session)


@pytest_asyncio.fixture
async def metric_dimensional_time_series_crud(db_session: AsyncSession) -> CRUDMetricDimensionalTimeSeries:
    """Fixture for CRUDMetricDimensionalTimeSeries instance."""
    return CRUDMetricDimensionalTimeSeries(MetricDimensionalTimeSeries, db_session)


@pytest_asyncio.fixture
async def metric_sync_status_crud(db_session: AsyncSession) -> CRUDMetricSyncStatus:
    """Fixture for CRUDMetricSyncStatus instance."""
    return CRUDMetricSyncStatus(MetricSyncStatus, db_session)


@pytest_asyncio.fixture
async def sample_time_series(db_session: AsyncSession) -> list[MetricTimeSeries]:
    """Create sample time series data."""
    # Set tenant ID
    set_tenant_id(1)
    data = [
        MetricTimeSeries(
            tenant_id=1,
            metric_id="test_metric",
            date=date(2024, 1, 1),
            grain=Granularity.DAY,
            value=100.0,
        ),
        MetricTimeSeries(
            tenant_id=1,
            metric_id="test_metric",
            date=date(2024, 1, 2),
            grain=Granularity.DAY,
            value=200.0,
        ),
    ]
    for item in data:
        db_session.add(item)
    await db_session.commit()
    return data


@pytest_asyncio.fixture
async def sample_dimensional_time_series(db_session: AsyncSession) -> list[MetricDimensionalTimeSeries]:
    """Create sample dimensional time series data."""
    # Set tenant ID
    set_tenant_id(1)
    data = [
        MetricDimensionalTimeSeries(
            tenant_id=1,
            metric_id="test_metric",
            date=date(2024, 1, 1),
            grain=Granularity.DAY,
            dimension_name="region",
            dimension_slice="US",
            value=100.0,
        ),
        MetricDimensionalTimeSeries(
            tenant_id=1,
            metric_id="test_metric",
            date=date(2024, 1, 1),
            grain=Granularity.DAY,
            dimension_name="region",
            dimension_slice="EU",
            value=150.0,
        ),
    ]
    for item in data:
        db_session.add(item)
    await db_session.commit()
    return data


async def test_bulk_upsert_time_series(metric_time_series_crud: CRUDMetricTimeSeries):
    """Test bulk upsert of time series data."""
    # Set tenant ID
    set_tenant_id(1)
    values = [
        {
            "tenant_id": 1,
            "metric_id": "test_metric",
            "date": date(2024, 1, 1),
            "grain": Granularity.DAY,
            "value": 100.0,
        },
        {
            "tenant_id": 1,
            "metric_id": "test_metric",
            "date": date(2024, 1, 2),
            "grain": Granularity.DAY,
            "value": 200.0,
        },
    ]

    result = await metric_time_series_crud.bulk_upsert(values)
    assert result["processed"] == 2
    assert result["failed"] == 0
    assert result["total"] == 2

    # Test upsert (update existing records)
    updated_values = [
        {
            "tenant_id": 1,
            "metric_id": "test_metric",
            "date": date(2024, 1, 1),
            "grain": Granularity.DAY,
            "value": 150.0,  # Updated value
        }
    ]
    result = await metric_time_series_crud.bulk_upsert(updated_values)
    assert result["processed"] == 1
    assert result["failed"] == 0
    assert result["total"] == 1

    # Verify the update
    query = await metric_time_series_crud.get_time_series_query(
        metric_ids=["test_metric"],
        grain=Granularity.DAY,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
    )
    result = await metric_time_series_crud.session.execute(query)
    results = result.scalars().all()
    assert len(results) == 1
    assert results[0].value == 150.0


async def test_bulk_upsert_dimensional_time_series(
    metric_dimensional_time_series_crud: CRUDMetricDimensionalTimeSeries,
):
    """Test bulk upsert of dimensional time series data."""
    # Set tenant ID
    set_tenant_id(1)
    values = [
        {
            "tenant_id": 1,
            "metric_id": "test_metric",
            "date": date(2024, 1, 1),
            "grain": Granularity.DAY,
            "dimension_name": "region",
            "dimension_slice": "US",
            "value": 100.0,
        },
        {
            "tenant_id": 1,
            "metric_id": "test_metric",
            "date": date(2024, 1, 1),
            "grain": Granularity.DAY,
            "dimension_name": "region",
            "dimension_slice": "EU",
            "value": 150.0,
        },
    ]

    result = await metric_dimensional_time_series_crud.bulk_upsert(values)
    assert result["processed"] == 2
    assert result["failed"] == 0
    assert result["total"] == 2

    # Verify the data
    query = await metric_dimensional_time_series_crud.get_dimensional_time_series_query(
        metric_id="test_metric",
        grain=Granularity.DAY,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
        dimension_names=["region"],
    )
    result = await metric_dimensional_time_series_crud.session.execute(query)
    results = sorted(result.scalars().all(), key=lambda x: x.dimension_slice)
    assert len(results) == 2
    assert results[0].dimension_slice == "EU"
    assert results[0].value == 150.0
    assert results[1].dimension_slice == "US"
    assert results[1].value == 100.0


async def test_get_metric_time_series(
    semantic_manager: SemanticManager,
    sample_time_series: list[MetricTimeSeries],
):
    """Test retrieving metric time series data."""
    # Pre-test setup
    set_tenant_id(1)
    # Act
    results = await semantic_manager.get_metric_time_series(
        metric_id="test_metric",
        grain=Granularity.DAY,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
    )
    # Assert
    assert len(results) == 2
    assert results[0].value == 200
    assert results[1].value == 100.0


async def test_get_multi_metric_time_series(
    semantic_manager: SemanticManager,
    sample_time_series: list[MetricTimeSeries],
):
    """Test retrieving multiple metric time series data."""
    # Pre-test setup
    set_tenant_id(1)
    # Act
    results = await semantic_manager.get_multi_metric_time_series(
        metric_ids=["test_metric"],
        grain=Granularity.DAY,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
    )

    # Assert
    assert len(results) == 2
    assert results[0].value == 200.0
    assert results[1].value == 100.0


async def test_get_dimensional_time_series(
    semantic_manager: SemanticManager,
    sample_dimensional_time_series: list[MetricDimensionalTimeSeries],
):
    """Test retrieving dimensional time series data."""
    # Pre-test setup
    set_tenant_id(1)
    # Act
    results = await semantic_manager.get_dimensional_time_series(
        metric_id="test_metric",
        grain=Granularity.DAY,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
        dimension_names=["region"],
    )

    # Assert
    assert len(results) == 2
    assert results[0].dimension_slice == "EU"
    assert results[0].value == 150.0
    assert results[1].dimension_slice == "US"
    assert results[1].value == 100.0


async def test_clear_metric_data(semantic_manager: SemanticManager, sample_time_series: list[MetricTimeSeries]):
    """Test clearing metric data."""
    # Pre-test setup
    set_tenant_id(1)
    # Act
    result = await semantic_manager.clear_metric_data(
        metric_id="test_metric",
        tenant_id=1,
        grain=Granularity.DAY,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
    )

    assert result["success"]
    assert result["rows_deleted"] == 2

    # Verify data is cleared
    results = await semantic_manager.get_metric_time_series(
        metric_id="test_metric",
        grain=Granularity.DAY,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
    )
    assert len(results) == 0


async def test_sync_status_operations(metric_sync_status_crud: CRUDMetricSyncStatus):
    """Test sync status operations."""
    # Pre-test setup
    set_tenant_id(1)
    # Start sync
    await metric_sync_status_crud.start_sync(
        metric_id="test_metric",
        grain=Granularity.DAY,
        sync_type=SyncType.FULL,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
    )

    # Get sync status
    status = await metric_sync_status_crud.get_sync_status(
        tenant_id=1,
        metric_id="test_metric",
        grain=Granularity.DAY,
    )
    # Assert
    assert len(status) == 1
    assert status[0].sync_status == SyncStatus.RUNNING

    # End sync successfully
    await metric_sync_status_crud.end_sync(
        metric_id="test_metric",
        grain=Granularity.DAY,
        sync_type=SyncType.FULL,
        status=SyncStatus.SUCCESS,
        records_processed=100,
    )

    # Verify final status
    status = await metric_sync_status_crud.get_sync_status(
        tenant_id=1,
        metric_id="test_metric",
        grain=Granularity.DAY,
    )
    # Assert
    assert len(status) == 1
    assert status[0].sync_status == SyncStatus.SUCCESS
    assert status[0].records_processed == 100


async def test_sync_status_history(metric_sync_status_crud: CRUDMetricSyncStatus):
    """Test sync status history tracking."""
    # Pre-test setup
    set_tenant_id(1)
    # Create initial sync status
    sync_status = await metric_sync_status_crud.update_sync_status(
        metric_id="test_metric",
        grain=Granularity.DAY,
        sync_status=SyncStatus.RUNNING,
        sync_type=SyncType.FULL,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
        add_to_history=True,
    )
    # Assert
    assert len(sync_status.history) == 0

    # Update with history
    sync_status = await metric_sync_status_crud.update_sync_status(
        metric_id="test_metric",
        grain=Granularity.DAY,
        sync_status=SyncStatus.SUCCESS,
        sync_type=SyncType.FULL,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
        records_processed=100,
        add_to_history=True,
    )

    assert len(sync_status.history) == 1
    assert sync_status.history[0]["sync_status"] == SyncStatus.RUNNING
    assert sync_status.history[0]["sync_type"] == SyncType.FULL


async def test_bulk_upsert_empty_list(metric_time_series_crud: CRUDMetricTimeSeries):
    """Test bulk upsert with empty list."""
    # Pre-test setup
    set_tenant_id(1)
    # Act
    result = await metric_time_series_crud.bulk_upsert([])
    # Assert
    assert result == {"processed": 0, "failed": 0, "total": 0}


async def test_bulk_upsert_error_handling(metric_time_series_crud: CRUDMetricTimeSeries):
    """Test bulk upsert error handling."""
    # Pre-test setup
    set_tenant_id(1)
    # Invalid data to trigger SQLAlchemyError
    invalid_values = [
        {
            "tenant_id": 1,
            "metric_id": "test_metric",
            "date": "invalid_date",  # Invalid date to trigger error
            "grain": Granularity.DAY,
            "value": 100.0,
        }
    ]

    # Act
    stats = await metric_time_series_crud.bulk_upsert(invalid_values)

    # Assert
    assert stats["processed"] == 0
    assert stats["failed"] == 1


async def test_stream_multi_metric_time_series(
    semantic_manager: SemanticManager,
    sample_time_series: list[MetricTimeSeries],
):
    """Test streaming multiple metric time series data."""
    # Pre-test setup
    set_tenant_id(1)

    # Act
    results = []
    async for item in semantic_manager.stream_multi_metric_time_series(
        metric_ids=["test_metric"],
        grain=Granularity.DAY,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
        batch_size=1,  # Small batch size to test batching
    ):
        results.append(item)

    # Assert
    assert len(results) == 2
    assert results[0].value == 200.0
    assert results[1].value == 100.0


async def test_stream_dimensional_time_series(
    semantic_manager: SemanticManager,
    sample_dimensional_time_series: list[MetricDimensionalTimeSeries],
):
    """Test streaming dimensional time series data."""
    # Pre-test setup
    set_tenant_id(1)

    # Act
    results = []
    async for item in semantic_manager.stream_dimensional_time_series(
        metric_id="test_metric",
        grain=Granularity.DAY,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
        dimension_names=["region"],
        batch_size=1,  # Small batch size to test batching
    ):
        results.append(item)

    # Assert
    assert len(results) == 2
    sorted_results = sorted(results, key=lambda x: x.dimension_slice)
    assert sorted_results[0].dimension_slice == "EU"
    assert sorted_results[0].value == 150.0
    assert sorted_results[1].dimension_slice == "US"
    assert sorted_results[1].value == 100.0


async def test_sync_status_not_found(metric_sync_status_crud: CRUDMetricSyncStatus):
    """Test sync status when no records exist."""
    # Pre-test setup
    set_tenant_id(1)

    # Act
    status = await metric_sync_status_crud.get_sync_status(
        tenant_id=1,
        metric_id="nonexistent_metric",
        grain=Granularity.DAY,
    )

    # Assert
    assert len(status) == 0


async def test_sync_status_with_filters(metric_sync_status_crud: CRUDMetricSyncStatus):
    """Test sync status with various filters."""
    # Pre-test setup
    set_tenant_id(1)

    # Create test data
    await metric_sync_status_crud.update_sync_status(
        metric_id="test_metric",
        grain=Granularity.DAY,
        sync_status=SyncStatus.SUCCESS,
        sync_type=SyncType.FULL,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
        dimension_name="region",
    )

    # Test with grain filter
    status = await metric_sync_status_crud.get_sync_status(
        tenant_id=1,
        metric_id="test_metric",
        grain=Granularity.DAY,
    )
    assert len(status) == 1

    # Test with dimension filter
    status = await metric_sync_status_crud.get_sync_status(
        tenant_id=1,
        metric_id="test_metric",
        dimension_name="region",
    )
    assert len(status) == 1

    # Test with non-matching filters
    status = await metric_sync_status_crud.get_sync_status(
        tenant_id=1,
        metric_id="test_metric",
        grain=Granularity.WEEK,  # Different grain
        dimension_name="nonexistent",  # Non-existent dimension
    )
    assert len(status) == 0


async def test_update_sync_status_create_new(metric_sync_status_crud: CRUDMetricSyncStatus):
    """Test creating new sync status when none exists."""
    # Pre-test setup
    set_tenant_id(1)

    # Act
    sync_status = await metric_sync_status_crud.update_sync_status(
        metric_id="new_metric",
        grain=Granularity.DAY,
        sync_status=SyncStatus.RUNNING,
        sync_type=SyncType.FULL,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
        records_processed=None,
        error=None,
        add_to_history=False,
    )

    # Assert
    assert sync_status.metric_id == "new_metric"
    assert sync_status.sync_status == SyncStatus.RUNNING
    assert sync_status.sync_type == SyncType.FULL
    assert sync_status.records_processed is None
    assert sync_status.error is None
    assert sync_status.history == []


async def test_update_sync_status_with_error(metric_sync_status_crud: CRUDMetricSyncStatus):
    """Test updating sync status with error information."""
    # Pre-test setup
    set_tenant_id(1)

    # Act
    sync_status = await metric_sync_status_crud.update_sync_status(
        metric_id="test_metric",
        grain=Granularity.DAY,
        sync_status=SyncStatus.FAILED,
        sync_type=SyncType.FULL,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
        error="Test error message",
        add_to_history=True,
    )

    # Assert
    assert sync_status.sync_status == SyncStatus.FAILED
    assert sync_status.error == "Test error message"
