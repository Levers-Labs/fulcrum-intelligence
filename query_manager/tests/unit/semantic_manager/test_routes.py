"""Tests for semantic manager routes."""

from datetime import date

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from query_manager.semantic_manager.models import MetricDimensionalTimeSeries, MetricTimeSeries
from query_manager.semantic_manager.schemas import MetricDimensionalTimeSeriesResponse, MetricTimeSeriesResponse

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def sample_time_series(db_session: AsyncSession, jwt_payload: dict) -> list[MetricTimeSeries]:
    """Create sample time series data in the database."""
    set_tenant_id(jwt_payload["tenant_id"])
    data = [
        MetricTimeSeries(
            tenant_id=1,
            metric_id="test_metric",
            date=date(2024, 1, 1),
            grain=Granularity.DAY,
            value=200.0,
        ),
        MetricTimeSeries(
            tenant_id=1,
            metric_id="test_metric",
            date=date(2024, 1, 2),
            grain=Granularity.DAY,
            value=100.0,
        ),
    ]
    for item in data:
        db_session.add(item)
    await db_session.commit()
    return data


@pytest_asyncio.fixture
async def sample_dimensional_time_series(
    db_session: AsyncSession, jwt_payload: dict
) -> list[MetricDimensionalTimeSeries]:
    """Create sample dimensional time series data in the database."""
    set_tenant_id(jwt_payload["tenant_id"])
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


async def test_get_metric_time_series(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    sample_time_series: list[MetricTimeSeries],
):
    """Test getting metric time series data."""
    set_tenant_id(jwt_payload["tenant_id"])
    # Make request
    response = await async_client.get(
        "/v2/semantic/metrics/test_metric/time-series",
        params={
            "grain": "day",
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
        },
    )

    # Assert response
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert "results" in data
    assert len(data["results"]) == 2

    # Verify the response matches our schema
    response_model = MetricTimeSeriesResponse(**data)
    assert len(response_model.results) == 2
    assert response_model.results[0].value == 100.0
    assert response_model.results[1].value == 200.0


async def test_get_multi_metric_time_series(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    db_session: AsyncSession,
):
    """Test getting time series data for multiple metrics."""
    # Create test data for multiple metrics
    set_tenant_id(jwt_payload["tenant_id"])
    data = [
        MetricTimeSeries(
            tenant_id=1,
            metric_id="test_metric1",
            date=date(2024, 1, 1),
            grain=Granularity.DAY,
            value=100.0,
        ),
        MetricTimeSeries(
            tenant_id=1,
            metric_id="test_metric2",
            date=date(2024, 1, 1),
            grain=Granularity.DAY,
            value=200.0,
        ),
    ]
    for item in data:
        db_session.add(item)
    await db_session.commit()

    # Make request
    response = await async_client.get(
        "/v2/semantic/metrics/time-series",
        params={
            "metric_ids": ["test_metric1", "test_metric2"],
            "grain": "day",
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
        },
    )

    # Assert response
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert "results" in data
    assert len(data["results"]) == 2

    # Verify the response matches our schema
    response_model = MetricTimeSeriesResponse(**data)
    assert len(response_model.results) == 2
    # Sort results by value to ensure consistent comparison
    results = sorted(response_model.results, key=lambda x: x.value)
    assert results[0].value == 100.0
    assert results[0].metric_id == "test_metric1"
    assert results[1].value == 200.0
    assert results[1].metric_id == "test_metric2"


async def test_get_metric_dimensional_time_series(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    sample_dimensional_time_series: list[MetricDimensionalTimeSeries],
):
    """Test getting dimensional time series data."""
    set_tenant_id(jwt_payload["tenant_id"])
    # Make request
    response = await async_client.get(
        "/v2/semantic/metrics/test_metric/dimensional-time-series",
        params={
            "grain": "day",
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
            "dimension_names": ["region"],
        },
    )

    # Assert response
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert "results" in data
    assert len(data["results"]) == 2

    # Verify the response matches our schema
    response_model = MetricDimensionalTimeSeriesResponse(**data)
    assert len(response_model.results) == 2
    # Sort results by dimension_slice to ensure consistent comparison
    results = sorted(response_model.results, key=lambda x: x.dimension_slice)
    assert results[0].dimension_name == "region"
    assert results[0].dimension_slice == "EU"
    assert results[0].value == 150.0
    assert results[1].dimension_slice == "US"
    assert results[1].value == 100.0


async def test_get_metric_time_series_validation(app: FastAPI, async_client: AsyncClient, jwt_payload: dict):
    """Test validation for metric time series endpoint."""
    set_tenant_id(jwt_payload["tenant_id"])
    # Test invalid grain
    response = await async_client.get(
        "/v2/semantic/metrics/test_metric/time-series",
        params={
            "grain": "INVALID",
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
        },
    )
    assert response.status_code == 422

    # Test invalid date format
    response = await async_client.get(
        "/v2/semantic/metrics/test_metric/time-series",
        params={
            "grain": "DAY",
            "start_date": "invalid-date",
            "end_date": "2024-01-02",
        },
    )
    assert response.status_code == 422

    # Test end_date before start_date
    response = await async_client.get(
        "/v2/semantic/metrics/test_metric/time-series",
        params={
            "grain": "DAY",
            "start_date": "2024-01-02",
            "end_date": "2024-01-01",
        },
    )
    assert response.status_code == 422


async def test_get_multi_metric_time_series_validation(app: FastAPI, async_client: AsyncClient, jwt_payload: dict):
    """Test validation for multi-metric time series endpoint."""
    set_tenant_id(jwt_payload["tenant_id"])
    # Test empty metric_ids
    response = await async_client.get(
        "/v2/semantic/metrics/time-series",
        params={
            "metric_ids": [],
            "grain": "day",
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
        },
    )
    assert response.status_code == 422

    # Test invalid grain
    response = await async_client.get(
        "/v2/semantic/metrics/time-series",
        params={
            "metric_ids": ["test_metric"],
            "grain": "INVALID",
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
        },
    )
    assert response.status_code == 422


async def test_get_metric_time_series_no_data(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
):
    """Test getting metric time series when no data exists."""
    set_tenant_id(jwt_payload["tenant_id"])
    response = await async_client.get(
        "/v2/semantic/metrics/nonexistent_metric/time-series",
        params={
            "grain": "day",
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["results"] == []


async def test_get_metric_dimensional_time_series_no_data(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
):
    """Test getting dimensional time series when no data exists."""
    set_tenant_id(jwt_payload["tenant_id"])
    response = await async_client.get(
        "/v2/semantic/metrics/nonexistent_metric/dimensional-time-series",
        params={
            "grain": "day",
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
            "dimension_names": ["region"],
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["results"] == []


async def test_get_multi_metric_time_series_no_metrics_error(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
):
    """Test getting multi-metric time series when no metrics exist."""
    set_tenant_id(jwt_payload["tenant_id"])
    response = await async_client.get(
        "/v2/semantic/metrics/time-series",
        params={"metric_ids": []},
    )

    # Assert response
    assert response.status_code == 422
