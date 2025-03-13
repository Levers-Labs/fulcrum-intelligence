"""Tests for semantic manager streaming functionality."""

import asyncio
from datetime import date

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from query_manager.semantic_manager.crud import SemanticManager
from query_manager.semantic_manager.models import MetricDimensionalTimeSeries, MetricTimeSeries

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def semantic_manager(db_session: AsyncSession) -> SemanticManager:
    """Fixture for SemanticManager instance."""
    return SemanticManager(db_session)


@pytest_asyncio.fixture
async def large_time_series_dataset(db_session: AsyncSession) -> list[MetricTimeSeries]:
    """Create a larger dataset for testing streaming functionality."""
    set_tenant_id(1)
    data = []

    # Create 100 records for testing
    for i in range(1, 101):
        day = i % 30 + 1
        month = (i % 12) + 1
        data.append(
            MetricTimeSeries(
                tenant_id=1,
                metric_id="stream_metric_1" if i <= 50 else "stream_metric_2",
                date=date(2024, month, day),
                grain=Granularity.DAY,
                value=float(i * 10),
            )
        )

    for item in data:
        db_session.add(item)
    await db_session.commit()
    return data


@pytest_asyncio.fixture
async def large_dimensional_time_series_dataset(db_session: AsyncSession) -> list[MetricDimensionalTimeSeries]:
    """Create a larger dimensional dataset for testing streaming functionality."""
    set_tenant_id(1)
    data = []

    # Create 100 records for testing with different dimensions
    for i in range(1, 101):
        day = i % 30 + 1
        month = (i % 12) + 1
        dimension_name = "region" if i <= 50 else "product"
        dimension_slice = f"slice_{i % 5 + 1}"

        data.append(
            MetricDimensionalTimeSeries(
                tenant_id=1,
                metric_id="stream_dimensional_metric",
                date=date(2024, month, day),
                grain=Granularity.DAY,
                dimension_name=dimension_name,
                dimension_slice=dimension_slice,
                value=float(i * 10),
            )
        )

    for item in data:
        db_session.add(item)
    await db_session.commit()
    return data


async def test_stream_multi_metric_time_series(
    semantic_manager: SemanticManager,
    large_time_series_dataset: list[MetricTimeSeries],
):
    """Test streaming time series data for multiple metrics."""
    # Test streaming with small batch size
    batch_size = 10
    metric_ids = ["stream_metric_1", "stream_metric_2"]

    # Collect results from the stream
    results = []
    async for item in semantic_manager.stream_multi_metric_time_series(
        metric_ids=metric_ids,
        grain=Granularity.DAY,
        batch_size=batch_size,
    ):
        results.append(item)

    # Verify all items were streamed
    assert len(results) == 100

    # Verify correct metrics were returned
    assert all(item.metric_id in metric_ids for item in results)

    # Test with date filtering
    results = []
    async for item in semantic_manager.stream_multi_metric_time_series(
        metric_ids=["stream_metric_1"],
        grain=Granularity.DAY,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 30),
        batch_size=batch_size,
    ):
        results.append(item)

    # Verify filtered results
    assert len(results) > 0
    assert len(results) <= 50  # Should be less than or equal to the total for stream_metric_1
    assert all(item.metric_id == "stream_metric_1" for item in results)
    assert all(date(2024, 1, 1) <= item.date <= date(2024, 6, 30) for item in results)


async def test_stream_dimensional_time_series(
    semantic_manager: SemanticManager,
    large_dimensional_time_series_dataset: list[MetricDimensionalTimeSeries],
):
    """Test streaming dimensional time series data."""
    # Test streaming with small batch size
    batch_size = 10

    # Collect results from the stream
    results = []
    async for item in semantic_manager.stream_dimensional_time_series(
        metric_id="stream_dimensional_metric",
        grain=Granularity.DAY,
        batch_size=batch_size,
    ):
        results.append(item)

    # Verify all items were streamed
    assert len(results) == 100

    # Test with dimension filtering
    results = []
    async for item in semantic_manager.stream_dimensional_time_series(
        metric_id="stream_dimensional_metric",
        grain=Granularity.DAY,
        dimension_names=["region"],
        batch_size=batch_size,
    ):
        results.append(item)

    # Verify filtered results
    assert len(results) > 0
    assert all(item.dimension_name == "region" for item in results)

    # Test with date and dimension filtering
    results = []
    async for item in semantic_manager.stream_dimensional_time_series(
        metric_id="stream_dimensional_metric",
        grain=Granularity.DAY,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 30),
        dimension_names=["product"],
        batch_size=batch_size,
    ):
        results.append(item)

    # Verify filtered results
    assert all(item.dimension_name == "product" for item in results)
    assert all(date(2024, 1, 1) <= item.date <= date(2024, 6, 30) for item in results)


async def test_stream_with_empty_results(semantic_manager: SemanticManager):
    """Test streaming with no matching results."""
    # Test streaming with non-existent metric
    results = []
    async for item in semantic_manager.stream_multi_metric_time_series(
        metric_ids=["non_existent_metric"],
        grain=Granularity.DAY,
    ):
        results.append(item)

    # Verify no results were returned
    assert len(results) == 0

    # Test dimensional streaming with non-existent metric
    results = []
    async for item in semantic_manager.stream_dimensional_time_series(
        metric_id="non_existent_metric",
        grain=Granularity.DAY,
    ):
        results.append(item)

    # Verify no results were returned
    assert len(results) == 0


async def test_stream_concurrency(
    semantic_manager: SemanticManager,
    large_time_series_dataset: list[MetricTimeSeries],
):
    """Test concurrent streaming of time series data."""

    async def collect_stream_results(metric_ids, grain, batch_size=10):
        results = []
        async for item in semantic_manager.stream_multi_metric_time_series(
            metric_ids=metric_ids,
            grain=grain,
            batch_size=batch_size,
        ):
            results.append(item)
        return results

    # Run multiple streams concurrently
    tasks = [
        collect_stream_results(["stream_metric_1"], Granularity.DAY),
        collect_stream_results(["stream_metric_2"], Granularity.DAY),
        collect_stream_results(["stream_metric_1", "stream_metric_2"], Granularity.DAY, batch_size=5),
    ]

    results = await asyncio.gather(*tasks)

    # Verify results from each stream
    assert len(results[0]) == 50  # stream_metric_1 only
    assert len(results[1]) == 50  # stream_metric_2 only
    assert len(results[2]) == 100  # Both metrics

    # Verify correct metrics in each result set
    assert all(item.metric_id == "stream_metric_1" for item in results[0])
    assert all(item.metric_id == "stream_metric_2" for item in results[1])
    assert all(item.metric_id in ["stream_metric_1", "stream_metric_2"] for item in results[2])
