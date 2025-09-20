"""Tests for semantic manager routes."""

from datetime import date

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from query_manager.core.enums import MetricAim
from query_manager.core.models import Metric
from query_manager.semantic_manager.models import MetricDimensionalTimeSeries, MetricTarget, MetricTimeSeries
from query_manager.semantic_manager.schemas import (
    MetricDimensionalTimeSeriesResponse,
    MetricTimeSeriesResponse,
    TargetBulkUpsertRequest,
    TargetCreate,
)

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


@pytest_asyncio.fixture
async def sample_targets(db_session: AsyncSession, jwt_payload: dict) -> list:
    """Create sample targets in the database."""
    set_tenant_id(jwt_payload["tenant_id"])
    targets = [
        MetricTarget(
            tenant_id=jwt_payload["tenant_id"],
            metric_id="test_metric",
            grain=Granularity.DAY,
            target_date=date(2024, 1, 1),
            target_value=100.0,
            target_upper_bound=110.0,
            target_lower_bound=90.0,
            yellow_buffer=5.0,
            red_buffer=10.0,
        ),
        MetricTarget(
            tenant_id=jwt_payload["tenant_id"],
            metric_id="test_metric",
            grain=Granularity.DAY,
            target_date=date(2024, 1, 2),
            target_value=200.0,
            target_upper_bound=220.0,
            target_lower_bound=180.0,
            yellow_buffer=5.0,
            red_buffer=10.0,
        ),
        MetricTarget(
            tenant_id=jwt_payload["tenant_id"],
            metric_id="another_metric",
            grain=Granularity.DAY,
            target_date=date(2024, 1, 1),
            target_value=300.0,
            target_upper_bound=330.0,
            target_lower_bound=270.0,
            yellow_buffer=5.0,
            red_buffer=10.0,
        ),
    ]
    for target in targets:
        db_session.add(target)
    await db_session.commit()

    # Refresh targets with IDs
    for target in targets:
        await db_session.refresh(target)

    return targets


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
    assert "results" in data
    assert len(data["results"]) == 2  # type: ignore

    # Verify the response matches our schema
    response_model = MetricTimeSeriesResponse(**data)  # type: ignore
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
    results = sorted(response_model.results, key=lambda x: x.dimension_slice)  # type: ignore
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


async def test_get_targets(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    sample_targets: list,
):
    """Test getting targets with pagination and filtering."""
    # Test getting all targets
    response = await async_client.get("/v2/semantic/metrics/targets")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3

    # Test filtering by metric_id
    response = await async_client.get("/v2/semantic/metrics/targets?metric_ids=test_metric")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert all(item["metric_id"] == "test_metric" for item in data)

    # Test filtering by grain
    response = await async_client.get("/v2/semantic/metrics/targets?grain=day")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    assert all(item["grain"] == "day" for item in data)

    # Test filtering by target_date
    response = await async_client.get("/v2/semantic/metrics/targets?target_date=2024-01-01")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert all(item["target_date"] == "2024-01-01" for item in data)

    # Test filtering by date range
    response = await async_client.get(
        "/v2/semantic/metrics/targets?target_date_ge=2024-01-01&target_date_le=2024-01-02"
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3


async def test_get_target_by_id(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    sample_targets: list,
):
    """Test getting a specific target by ID."""
    target_id = sample_targets[0].id

    response = await async_client.get(f"/v2/semantic/metrics/targets/{target_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == target_id
    assert data["metric_id"] == sample_targets[0].metric_id
    assert data["target_value"] == sample_targets[0].target_value


async def test_bulk_upsert_targets(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
):
    """Test bulk upserting targets."""
    # Create test data
    targets = [
        TargetCreate(
            metric_id="bulk_test_metric",
            grain=Granularity.DAY,
            target_date=date(2024, 1, 1),
            target_value=100.0,
            target_upper_bound=110.0,
            target_lower_bound=90.0,
            yellow_buffer=5.0,
            red_buffer=10.0,
        ),
        TargetCreate(
            metric_id="bulk_test_metric",
            grain=Granularity.DAY,
            target_date=date(2024, 1, 2),
            target_value=200.0,
            target_upper_bound=220.0,
            target_lower_bound=180.0,
            yellow_buffer=5.0,
            red_buffer=10.0,
        ),
    ]

    request_data = TargetBulkUpsertRequest(targets=targets)

    # Test bulk upsert
    response = await async_client.post("/v2/semantic/metrics/targets/bulk", json=request_data.model_dump(mode="json"))
    assert response.status_code == 201
    data = response.json()
    assert data["total"] == 2
    assert data["processed"] == 2
    assert data["failed"] == 0

    # Verify targets were created
    response = await async_client.get("/v2/semantic/metrics/targets?metric_ids=bulk_test_metric")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2

    # Test updating existing targets
    updated_targets = [
        TargetCreate(
            metric_id="bulk_test_metric",
            grain=Granularity.DAY,
            target_date=date(2024, 1, 1),
            target_value=150.0,  # Updated value
            target_upper_bound=110.0,
            target_lower_bound=90.0,
            yellow_buffer=5.0,
            red_buffer=10.0,
        ),
    ]

    request_data = TargetBulkUpsertRequest(targets=updated_targets)

    response = await async_client.post(
        "/v2/semantic/metrics/targets/bulk",
        json=request_data.model_dump(mode="json"),
    )
    assert response.status_code == 201

    # Verify target was updated
    response = await async_client.get("/v2/semantic/metrics/targets?metric_ids=bulk_test_metric&target_date=2024-01-01")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["target_value"] == 150.0


async def test_delete_targets(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    sample_targets: list,
):
    """Test deleting targets."""
    # Delete a specific target by date
    response = await async_client.delete("/v2/semantic/metrics/test_metric/targets?grain=day&target_date=2024-01-01")
    assert response.status_code == 204

    # Verify target was deleted
    response = await async_client.get("/v2/semantic/metrics/targets?metric_ids=test_metric")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1  # Only one target should remain
    assert data[0]["target_date"] == "2024-01-02"

    # Delete by date range
    response = await async_client.delete(
        "/v2/semantic/metrics/test_metric/targets?grain=day&target_date_ge=2024-01-01&target_date_le=2024-01-02"
    )
    assert response.status_code == 204

    # Verify all targets were deleted
    response = await async_client.get("/v2/semantic/metrics/targets?metric_ids=test_metric")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 0


async def test_get_target_not_found(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
):
    """Test getting a non-existent target."""
    response = await async_client.get("/v2/semantic/metrics/targets/999999")
    assert response.status_code == 404
    data = response.json()
    assert "detail" in data
    assert "not found" in data["detail"].lower()


@pytest_asyncio.fixture
async def sample_metrics_with_targets(db_session: AsyncSession, jwt_payload: dict) -> dict:
    """Create sample metrics and their targets for testing the stats."""
    set_tenant_id(jwt_payload["tenant_id"])

    # Create metrics with different aims
    metrics = [
        Metric(tenant_id=jwt_payload["tenant_id"], metric_id="revenue", label="Revenue", aim=MetricAim.MAXIMIZE),
        Metric(tenant_id=jwt_payload["tenant_id"], metric_id="cost", label="Cost", aim=MetricAim.MINIMIZE),
        Metric(
            tenant_id=jwt_payload["tenant_id"],
            metric_id="satisfaction",
            label="Customer Satisfaction",
            aim=MetricAim.BALANCE,
        ),
    ]

    for metric in metrics:
        db_session.add(metric)

    # Create targets for different grains
    targets = [
        # Revenue targets
        MetricTarget(
            tenant_id=jwt_payload["tenant_id"],
            metric_id="revenue",
            grain=Granularity.DAY,
            target_date=date(2025, 6, 30),
            target_value=1000.0,
        ),
        MetricTarget(
            tenant_id=jwt_payload["tenant_id"],
            metric_id="revenue",
            grain=Granularity.WEEK,
            target_date=date(2025, 5, 31),
            target_value=7000.0,
        ),
        # Cost targets
        MetricTarget(
            tenant_id=jwt_payload["tenant_id"],
            metric_id="cost",
            grain=Granularity.DAY,
            target_date=date(2025, 4, 30),
            target_value=500.0,
        ),
        # Satisfaction has no targets initially
    ]

    for target in targets:
        db_session.add(target)

    await db_session.commit()
    return {"metrics": metrics, "targets": targets}


async def test_get_metrics_targets_stats(
    app: FastAPI, async_client: AsyncClient, jwt_payload: dict, sample_metrics_with_targets: dict
):
    """Test getting the metrics targets stats."""
    response = await async_client.get("/v2/semantic/metrics/targets/stats")
    assert response.status_code == 200
    data = response.json()

    # Check pagination
    assert "count" in data
    assert "results" in data
    results = data["results"]

    # Should return all metrics
    assert len(results) == 3

    # Check revenue metric
    revenue = next(m for m in results if m["metric_id"] == "revenue")
    assert revenue["label"] == "Revenue"
    assert revenue["aim"] == "Maximize"
    assert revenue["periods"][0]["grain"] == "day"
    assert revenue["periods"][0]["target_set"] is True
    assert revenue["periods"][0]["target_till_date"] == "2025-06-30"
    assert revenue["periods"][0]["target_value"] == 1000.0
    assert revenue["periods"][1]["grain"] == "week"
    assert revenue["periods"][1]["target_set"] is True
    assert revenue["periods"][1]["target_till_date"] == "2025-05-31"
    assert revenue["periods"][1]["target_value"] == 7000.0
    assert revenue["periods"][2]["grain"] == "month"
    assert revenue["periods"][2]["target_set"] is False
    assert revenue["periods"][2]["target_till_date"] is None
    assert revenue["periods"][2]["target_value"] == 0.0

    # Check cost metric
    cost = next(m for m in results if m["metric_id"] == "cost")
    assert cost["label"] == "Cost"
    assert cost["aim"] == "Minimize"
    assert cost["periods"][0]["grain"] == "day"
    assert cost["periods"][0]["target_set"] is True
    assert cost["periods"][0]["target_till_date"] == "2025-04-30"
    assert cost["periods"][1]["grain"] == "week"
    assert cost["periods"][1]["target_set"] is False
    assert cost["periods"][2]["grain"] == "month"
    assert cost["periods"][2]["target_set"] is False
    assert cost["periods"][2]["target_value"] == 0.0
    # Check satisfaction metric (no targets)
    satisfaction = next(m for m in results if m["metric_id"] == "satisfaction")
    assert satisfaction["label"] == "Customer Satisfaction"
    assert satisfaction["aim"] == "Balance"
    assert all(not period["target_set"] for period in satisfaction["periods"])
    assert all(period["target_till_date"] is None for period in satisfaction["periods"])
    assert satisfaction["periods"][2]["target_value"] == 0.0


async def test_get_metrics_targets_stats_empty(
    app: FastAPI, async_client: AsyncClient, jwt_payload: dict, db_session: AsyncSession
):
    """Test getting the metrics targets stats when no metrics exist."""
    response = await async_client.get("/v2/semantic/metrics/targets/stats")
    assert response.status_code == 200
    data = response.json()

    assert data["count"] == 0
    assert data["results"] == []


async def test_get_metrics_targets_stats_with_filter(
    app: FastAPI, async_client: AsyncClient, jwt_payload: dict, sample_metrics_with_targets: dict
):
    """Test getting the metrics targets stats with a filter."""
    response = await async_client.get("/v2/semantic/metrics/targets/stats?metric_label=Revenue")
    assert response.status_code == 200
    data = response.json()

    # Check pagination
    assert "count" in data
    assert "results" in data
    results = data["results"]

    # Should return all metrics
    assert len(results) == 1

    # Check revenue metric
    revenue = next(m for m in results if m["metric_id"] == "revenue")
    assert revenue["label"] == "Revenue"
    assert revenue["aim"] == "Maximize"
    assert revenue["periods"][0]["grain"] == "day"
    assert revenue["periods"][0]["target_set"] is True
    assert revenue["periods"][0]["target_till_date"] == "2025-06-30"
    assert revenue["periods"][0]["target_value"] == 1000.0
    assert revenue["periods"][1]["grain"] == "week"
    assert revenue["periods"][1]["target_set"] is True
    assert revenue["periods"][1]["target_till_date"] == "2025-05-31"
    assert revenue["periods"][1]["target_value"] == 7000.0
    assert revenue["periods"][2]["grain"] == "month"
    assert revenue["periods"][2]["target_set"] is False
    assert revenue["periods"][2]["target_till_date"] is None
    assert revenue["periods"][2]["target_value"] == 0.0


async def test_get_multi_metric_time_series_large_dataset(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    db_session: AsyncSession,
):
    """Test getting time series data for multiple metrics with larger dataset."""
    # Create test data for multiple metrics with multiple dates
    set_tenant_id(jwt_payload["tenant_id"])
    data = []
    for i in range(5):  # 5 metrics
        for day in range(10):  # 10 days each
            data.append(
                MetricTimeSeries(
                    tenant_id=1,
                    metric_id=f"test_metric_{i}",
                    date=date(2024, 1, day + 1),
                    grain=Granularity.DAY,
                    value=100.0 + i * 10 + day,
                )
            )

    for item in data:
        db_session.add(item)
    await db_session.commit()

    # Make request for subset of metrics
    response = await async_client.get(
        "/v2/semantic/metrics/time-series",
        params={
            "metric_ids": ["test_metric_0", "test_metric_2", "test_metric_4"],
            "grain": "day",
            "start_date": "2024-01-05",
            "end_date": "2024-01-08",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    results = data["results"]

    # Should have 3 metrics * 4 days = 12 results
    assert len(results) == 12

    # Verify data ranges and values
    metric_ids = {result["metric_id"] for result in results}
    assert metric_ids == {"test_metric_0", "test_metric_2", "test_metric_4"}

    # Verify date range
    dates = {result["date"] for result in results}
    expected_dates = {"2024-01-05", "2024-01-06", "2024-01-07", "2024-01-08"}
    assert dates == expected_dates


async def test_get_metric_time_series_with_different_grains(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    db_session: AsyncSession,
):
    """Test getting metric time series data with different grain types."""
    set_tenant_id(jwt_payload["tenant_id"])

    # Create data with different grains
    grains_data = [
        (Granularity.DAY, date(2024, 1, 15), 100.0),
        (Granularity.WEEK, date(2024, 1, 15), 700.0),
        (Granularity.MONTH, date(2024, 1, 1), 3000.0),
        (Granularity.QUARTER, date(2024, 1, 1), 9000.0),
        (Granularity.YEAR, date(2024, 1, 1), 36000.0),
    ]

    for grain, test_date, value in grains_data:
        data = MetricTimeSeries(
            tenant_id=1,
            metric_id="grain_test_metric",
            date=test_date,
            grain=grain,
            value=value,
        )
        db_session.add(data)

    await db_session.commit()

    # Test each grain separately
    for grain, test_date, expected_value in grains_data:
        response = await async_client.get(
            "/v2/semantic/metrics/grain_test_metric/time-series",
            params={
                "grain": grain.value,
                "start_date": test_date.strftime("%Y-%m-%d"),
                "end_date": test_date.strftime("%Y-%m-%d"),
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 1
        assert data["results"][0]["value"] == expected_value
        assert data["results"][0]["grain"] == grain.value


async def test_get_metric_dimensional_time_series_multiple_dimensions(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    db_session: AsyncSession,
):
    """Test dimensional time series with multiple dimension types."""
    set_tenant_id(jwt_payload["tenant_id"])

    # Create data with multiple dimensions
    dimensional_data = [
        ("region", "US", 100.0),
        ("region", "EU", 150.0),
        ("region", "ASIA", 200.0),
        ("product_type", "Premium", 300.0),
        ("product_type", "Basic", 100.0),
        ("customer_segment", "Enterprise", 500.0),
        ("customer_segment", "SMB", 250.0),
    ]

    for dim_name, dim_slice, value in dimensional_data:
        data = MetricDimensionalTimeSeries(
            tenant_id=1,
            metric_id="multi_dim_metric",
            date=date(2024, 1, 1),
            grain=Granularity.DAY,
            dimension_name=dim_name,
            dimension_slice=dim_slice,
            value=value,
        )
        db_session.add(data)

    await db_session.commit()

    # Test filtering by specific dimensions
    response = await async_client.get(
        "/v2/semantic/metrics/multi_dim_metric/dimensional-time-series",
        params={
            "grain": "day",
            "start_date": "2024-01-01",
            "end_date": "2024-01-01",
            "dimension_names": ["region", "product_type"],
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 5  # 3 regions + 2 product types

    # Verify dimension names
    dimension_names = {result["dimension_name"] for result in data["results"]}
    assert dimension_names == {"region", "product_type"}


async def test_bulk_upsert_targets_validation_error(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
):
    """Test bulk upsert targets with validation errors."""
    # Create invalid target data (missing required fields)
    invalid_targets = [
        {
            "metric_id": "test_metric",
            # Missing grain
            "target_date": "2024-01-01",
            "target_value": 100.0,
        }
    ]

    request_data = {"targets": invalid_targets}

    response = await async_client.post("/v2/semantic/metrics/targets/bulk", json=request_data)
    assert response.status_code == 422  # Validation error


async def test_delete_targets_with_different_filters(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    db_session: AsyncSession,
):
    """Test deleting targets with various filter combinations."""
    set_tenant_id(jwt_payload["tenant_id"])

    # Create multiple targets with different dates
    targets = []
    for i in range(10):
        target = MetricTarget(
            tenant_id=jwt_payload["tenant_id"],
            metric_id="delete_test_metric",
            grain=Granularity.DAY,
            target_date=date(2024, 1, i + 1),
            target_value=100.0 + i,
            target_upper_bound=110.0 + i,
            target_lower_bound=90.0 + i,
        )
        targets.append(target)
        db_session.add(target)

    await db_session.commit()

    # Delete targets in date range
    response = await async_client.delete(
        "/v2/semantic/metrics/delete_test_metric/targets?grain=day&target_date_ge=2024-01-03&target_date_le=2024-01-07"
    )
    assert response.status_code == 204

    # Verify only targets outside the range remain
    response = await async_client.get("/v2/semantic/metrics/targets?metric_ids=delete_test_metric")
    assert response.status_code == 200
    remaining_targets = response.json()
    assert len(remaining_targets) == 5  # Should have 5 remaining (1-2, 8-10)

    # Verify the remaining dates
    remaining_dates = {target["target_date"] for target in remaining_targets}
    expected_dates = {"2024-01-01", "2024-01-02", "2024-01-08", "2024-01-09", "2024-01-10"}
    assert remaining_dates == expected_dates


async def test_get_targets_pagination(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    db_session: AsyncSession,
):
    """Test getting targets with custom pagination parameters."""
    set_tenant_id(jwt_payload["tenant_id"])

    # Create many targets for pagination testing
    targets = []
    for i in range(25):
        target = MetricTarget(
            tenant_id=jwt_payload["tenant_id"],
            metric_id="pagination_test_metric",
            grain=Granularity.DAY,
            target_date=date(2024, 1, (i % 28) + 1),
            target_value=100.0 + i,
        )
        targets.append(target)
        db_session.add(target)

    await db_session.commit()

    # Test with pagination - this endpoint doesn't seem to support pagination based on current schema
    # But we can test the basic functionality
    response = await async_client.get("/v2/semantic/metrics/targets?metric_ids=pagination_test_metric")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 25


async def test_get_target_by_id_edge_cases(
    app: FastAPI,
    async_client: AsyncClient,
    jwt_payload: dict,
    sample_targets: list,
):
    """Test edge cases for getting target by ID."""
    # Test with valid target ID
    target_id = sample_targets[0].id
    response = await async_client.get(f"/v2/semantic/metrics/targets/{target_id}")
    assert response.status_code == 200

    # Test with very large ID
    response = await async_client.get("/v2/semantic/metrics/targets/999999999")
    assert response.status_code == 404

    # Test with invalid ID format - this would be caught by FastAPI validation
    response = await async_client.get("/v2/semantic/metrics/targets/invalid_id")
    assert response.status_code == 422
