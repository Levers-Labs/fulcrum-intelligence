from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient

from commons.db.crud import NotFoundError
from commons.models.enums import Granularity
from query_manager.core.models import MetricCacheConfig, MetricCacheGrainConfig


@pytest.mark.asyncio
async def test_list_grain_configs(async_client: AsyncClient, mocker):
    """Test listing all grain cache configurations."""
    # Mock data
    mock_configs = [
        MetricCacheGrainConfig(
            id=1,
            tenant_id=1,
            grain=Granularity.DAY,
            is_enabled=True,
            initial_sync_period=730,
            delta_sync_period=90,
        ),
        MetricCacheGrainConfig(
            id=2,
            tenant_id=1,
            grain=Granularity.WEEK,
            is_enabled=True,
            initial_sync_period=1095,
            delta_sync_period=120,
        ),
    ]

    # Mock the CRUD paginate method
    mock_paginate = AsyncMock(return_value=(mock_configs, len(mock_configs)))
    mocker.patch("query_manager.core.crud.CRUDMetricCacheGrainConfig.paginate", mock_paginate)

    # Execute request
    response = await async_client.get("/v1/grains/cache-config")

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 2
    assert data["results"][0]["grain"] == "day"
    assert data["results"][1]["grain"] == "week"
    assert data["count"] == 2


@pytest.mark.asyncio
async def test_get_grain_config(async_client: AsyncClient, mocker):
    """Test getting a specific grain cache configuration."""
    # Mock data
    mock_config = MetricCacheGrainConfig(
        id=1,
        tenant_id=1,
        grain=Granularity.DAY,
        is_enabled=True,
        initial_sync_period=730,
        delta_sync_period=90,
    )

    # Mock the get_by_grain method
    mock_get_by_grain = AsyncMock(return_value=mock_config)
    mocker.patch("query_manager.core.crud.CRUDMetricCacheGrainConfig.get_by_grain", mock_get_by_grain)

    # Execute request - must use uppercase for the route since that's what the enum expects
    response = await async_client.get("/v1/grains/day/cache-config")

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert data["grain"] == "day"  # lowercase in JSON response
    assert data["is_enabled"] is True
    assert data["initial_sync_period"] == 730
    assert data["delta_sync_period"] == 90


@pytest.mark.asyncio
async def test_get_grain_config_not_found(async_client: AsyncClient, mocker):
    """Test getting a non-existent grain cache configuration."""
    # Mock the get_by_grain method to raise NotFoundError
    mock_get_by_grain = AsyncMock(side_effect=NotFoundError("Grain not found"))
    mocker.patch("query_manager.core.crud.CRUDMetricCacheGrainConfig.get_by_grain", mock_get_by_grain)

    # Execute request
    response = await async_client.get("/v1/grains/day/cache-config")

    # Assertions
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_update_grain_config(async_client: AsyncClient, mocker):
    """Test updating a grain cache configuration."""
    # Mock data
    mock_config = MetricCacheGrainConfig(
        id=1,
        tenant_id=1,
        grain=Granularity.DAY,
        is_enabled=False,  # Changed from True to False
        initial_sync_period=365,  # Changed from 730
        delta_sync_period=90,
    )

    # Mock the update_grain_config method
    mock_update = AsyncMock(return_value=mock_config)
    mocker.patch("query_manager.core.crud.CRUDMetricCacheGrainConfig.update_grain_config", mock_update)

    # Execute request with update data
    update_data = {
        "is_enabled": False,
        "initial_sync_period": 365,
    }
    response = await async_client.put("/v1/grains/day/cache-config", json=update_data)

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert data["grain"] == "day"  # lowercase in JSON
    assert data["is_enabled"] is False
    assert data["initial_sync_period"] == 365
    assert data["delta_sync_period"] == 90


@pytest.mark.asyncio
async def test_bulk_update_grain_configs(async_client: AsyncClient, mocker):
    """Test bulk updating grain cache configurations."""
    # Mock data
    mock_configs = [
        MetricCacheGrainConfig(
            id=1,
            tenant_id=1,
            grain=Granularity.DAY,
            is_enabled=False,
            initial_sync_period=365,
            delta_sync_period=30,
        ),
        MetricCacheGrainConfig(
            id=2,
            tenant_id=1,
            grain=Granularity.WEEK,
            is_enabled=False,
            initial_sync_period=1095,
            delta_sync_period=120,
        ),
    ]

    # Mock the bulk_update_grain_configs method
    mock_bulk_update = AsyncMock(return_value=mock_configs)
    mocker.patch("query_manager.core.crud.CRUDMetricCacheGrainConfig.bulk_update_grain_configs", mock_bulk_update)

    # Execute request with bulk update data
    bulk_update_data = {
        "configs": [
            {"grain": "day", "is_enabled": False, "delta_sync_period": 30},
            {"grain": "week", "is_enabled": False},
        ]
    }
    response = await async_client.post("/v1/grains/cache-config/bulk", json=bulk_update_data)

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["grain"] == "day"
    assert data[0]["is_enabled"] is False
    assert data[0]["delta_sync_period"] == 30
    assert data[1]["grain"] == "week"
    assert data[1]["is_enabled"] is False

    # Verify the method was called with correct parameters (but converted to enum)
    mock_bulk_update.assert_called_once()


@pytest.mark.asyncio
async def test_enable_all_grain_caching(async_client: AsyncClient, mocker):
    """Test enabling caching for all grain levels."""
    # Mock data
    mock_configs = [
        MetricCacheGrainConfig(
            id=1,
            tenant_id=1,
            grain=Granularity.DAY,
            is_enabled=True,
            initial_sync_period=730,
            delta_sync_period=90,
        ),
        MetricCacheGrainConfig(
            id=2,
            tenant_id=1,
            grain=Granularity.WEEK,
            is_enabled=True,
            initial_sync_period=1095,
            delta_sync_period=120,
        ),
        MetricCacheGrainConfig(
            id=3,
            tenant_id=1,
            grain=Granularity.MONTH,
            is_enabled=True,
            initial_sync_period=1825,
            delta_sync_period=180,
        ),
    ]

    # Mock the create_default_grain_configs method
    mock_create_defaults = AsyncMock(return_value=mock_configs)
    mocker.patch(
        "query_manager.core.crud.CRUDMetricCacheGrainConfig.create_default_grain_configs", mock_create_defaults
    )

    # Execute request
    response = await async_client.post("/v1/grains/cache-config/enable-all")

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    assert {config["grain"] for config in data} == {"day", "week", "month"}
    for config in data:
        assert config["is_enabled"] is True

    # Verify the method was called
    mock_create_defaults.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_metric_cache_config(async_client: AsyncClient, mocker):
    """Test getting cache configuration for a specific metric."""
    # Mock data
    mock_config = MetricCacheConfig(
        id=1,
        tenant_id=1,
        metric_id="test_metric",
        is_enabled=True,
    )

    # Mock the get_by_metric_id method
    mock_get_by_metric_id = AsyncMock(return_value=mock_config)
    mocker.patch("query_manager.core.crud.CRUDMetricCacheConfig.get_by_metric_id", mock_get_by_metric_id)

    # Execute request
    response = await async_client.get("/v1/metrics/test_metric/cache-config")

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert data["metric_id"] == "test_metric"
    assert data["is_enabled"] is True


@pytest.mark.asyncio
async def test_get_metric_cache_config_not_found(async_client: AsyncClient, mocker):
    """Test getting a non-existent metric cache configuration."""
    # Mock the get_by_metric_id method to raise NotFoundError
    mock_get_by_metric_id = AsyncMock(side_effect=NotFoundError("Metric cache config not found"))
    mocker.patch("query_manager.core.crud.CRUDMetricCacheConfig.get_by_metric_id", mock_get_by_metric_id)

    # Execute request
    response = await async_client.get("/v1/metrics/non_existent_metric/cache-config")

    # Assertions
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_update_metric_cache_config(async_client: AsyncClient, mocker):
    """Test updating cache configuration for a specific metric."""
    # Mock data
    mock_config = MetricCacheConfig(
        id=1,
        tenant_id=1,
        metric_id="test_metric",
        is_enabled=False,  # Changed from True to False
    )

    # Mock the create_or_update_metric_config method
    mock_update = AsyncMock(return_value=mock_config)
    mocker.patch("query_manager.core.crud.CRUDMetricCacheConfig.create_or_update_metric_config", mock_update)

    # Execute request with update data
    update_data = {
        "is_enabled": False,
    }
    response = await async_client.put("/v1/metrics/test_metric/cache-config", json=update_data)

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert data["metric_id"] == "test_metric"
    assert data["is_enabled"] is False

    # Verify correct parameters were passed to the update method
    mock_update.assert_awaited_once_with("test_metric", False)


@pytest.mark.asyncio
async def test_list_metric_cache_configs(async_client: AsyncClient, mocker):
    """Test listing all metric cache configurations."""
    # Mock data
    mock_configs = [
        MetricCacheConfig(
            id=1,
            tenant_id=1,
            metric_id="metric1",
            is_enabled=True,
        ),
        MetricCacheConfig(
            id=2,
            tenant_id=1,
            metric_id="metric2",
            is_enabled=False,
        ),
    ]

    # Mock the paginate method
    mock_paginate = AsyncMock(return_value=(mock_configs, len(mock_configs)))
    mocker.patch("query_manager.core.crud.CRUDMetricCacheConfig.paginate", mock_paginate)

    # Execute request
    response = await async_client.get("/v1/metrics/cache-config/all")

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 2
    assert data["results"][0]["metric_id"] == "metric1"
    assert data["results"][0]["is_enabled"] is True
    assert data["results"][1]["metric_id"] == "metric2"
    assert data["results"][1]["is_enabled"] is False
    assert data["count"] == 2


@pytest.mark.asyncio
async def test_list_metric_cache_configs_with_filters(async_client: AsyncClient, mocker):
    """Test listing metric cache configurations with filters."""
    # Mock data - filtered to only enabled metrics
    mock_configs = [
        MetricCacheConfig(
            id=1,
            tenant_id=1,
            metric_id="metric1",
            is_enabled=True,
        ),
    ]

    # Mock the paginate method with filter
    mock_paginate = AsyncMock(return_value=(mock_configs, len(mock_configs)))
    mocker.patch("query_manager.core.crud.CRUDMetricCacheConfig.paginate", mock_paginate)

    # Execute request with filters
    response = await async_client.get("/v1/metrics/cache-config/all?is_enabled=true")

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 1
    assert data["results"][0]["metric_id"] == "metric1"
    assert data["results"][0]["is_enabled"] is True
    assert data["count"] == 1

    # Verify correct filter was passed to paginate
    mock_paginate.assert_called_once()
    # Can't check exact filter contents due to async client param conversion
    # but can verify it was called


@pytest.mark.asyncio
async def test_bulk_update_metric_cache_configs(async_client: AsyncClient, mocker):
    """Test bulk updating metric cache configurations."""
    # Mock data
    mock_configs = [
        MetricCacheConfig(
            id=1,
            tenant_id=1,
            metric_id="metric1",
            is_enabled=True,
        ),
        MetricCacheConfig(
            id=2,
            tenant_id=1,
            metric_id="metric2",
            is_enabled=True,
        ),
    ]

    # Mock the bulk_update_metric_configs method
    mock_bulk_update = AsyncMock(return_value=mock_configs)
    mocker.patch("query_manager.core.crud.CRUDMetricCacheConfig.bulk_update_metric_configs", mock_bulk_update)

    # Execute request with bulk update data
    bulk_update_data = {"metric_ids": ["metric1", "metric2"], "is_enabled": True}
    response = await async_client.post("/v1/metrics/cache-config/bulk", json=bulk_update_data)

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["metric_id"] == "metric1"
    assert data[0]["is_enabled"] is True
    assert data[1]["metric_id"] == "metric2"
    assert data[1]["is_enabled"] is True

    # Verify correct parameters were passed to the bulk update method
    mock_bulk_update.assert_awaited_once_with(["metric1", "metric2"], True)


@pytest.mark.asyncio
async def test_enable_all_metric_caching(async_client: AsyncClient, mocker):
    """Test enabling caching for all metrics."""
    # Mock data
    mock_configs = [
        MetricCacheConfig(
            id=1,
            tenant_id=1,
            metric_id="metric1",
            is_enabled=True,
        ),
        MetricCacheConfig(
            id=2,
            tenant_id=1,
            metric_id="metric2",
            is_enabled=True,
        ),
        MetricCacheConfig(
            id=3,
            tenant_id=1,
            metric_id="metric3",
            is_enabled=True,
        ),
    ]

    # Mock the enable_all_metrics method
    mock_enable_all = AsyncMock(return_value=mock_configs)
    mocker.patch("query_manager.core.crud.CRUDMetricCacheConfig.enable_all_metrics", mock_enable_all)

    # Execute request
    response = await async_client.post("/v1/metrics/cache-config/enable-all")

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    assert {config["metric_id"] for config in data} == {"metric1", "metric2", "metric3"}
    for config in data:
        assert config["is_enabled"] is True

    # Verify the method was called
    mock_enable_all.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_grain_config_error(async_client: AsyncClient, mocker):
    """Test error handling when updating a grain cache configuration."""
    # Mock the update_grain_config method to raise an exception
    mock_update = AsyncMock(side_effect=Exception("Database error"))
    mocker.patch("query_manager.core.crud.CRUDMetricCacheGrainConfig.update_grain_config", mock_update)

    # Execute request
    update_data = {"is_enabled": False}
    response = await async_client.put("/v1/grains/day/cache-config", json=update_data)

    # Assertions
    assert response.status_code == 500
    assert "failed to update grain configuration" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_bulk_update_grain_configs_error(async_client: AsyncClient, mocker):
    """Test error handling when bulk updating grain cache configurations."""
    # Mock the bulk_update_grain_configs method to raise an exception
    mock_bulk_update = AsyncMock(side_effect=Exception("Database error"))
    mocker.patch("query_manager.core.crud.CRUDMetricCacheGrainConfig.bulk_update_grain_configs", mock_bulk_update)

    # Execute request
    bulk_update_data = {"configs": [{"grain": "day", "is_enabled": False}]}
    response = await async_client.post("/v1/grains/cache-config/bulk", json=bulk_update_data)

    # Assertions
    assert response.status_code == 500
    assert "failed to bulk update grain configurations" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_update_metric_cache_config_error(async_client: AsyncClient, mocker):
    """Test error handling when updating a metric cache configuration."""
    # Mock the create_or_update_metric_config method to raise an exception
    mock_update = AsyncMock(side_effect=Exception("Database error"))
    mocker.patch("query_manager.core.crud.CRUDMetricCacheConfig.create_or_update_metric_config", mock_update)

    # Execute request
    update_data = {"is_enabled": False}
    response = await async_client.put("/v1/metrics/test_metric/cache-config", json=update_data)

    # Assertions
    assert response.status_code == 500
    assert "failed to update metric cache configuration" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_bulk_update_metric_cache_configs_error(async_client: AsyncClient, mocker):
    """Test error handling when bulk updating metric cache configurations."""
    # Mock the bulk_update_metric_configs method to raise an exception
    mock_bulk_update = AsyncMock(side_effect=Exception("Database error"))
    mocker.patch("query_manager.core.crud.CRUDMetricCacheConfig.bulk_update_metric_configs", mock_bulk_update)

    # Execute request
    bulk_update_data = {"metric_ids": ["metric1", "metric2"], "is_enabled": True}
    response = await async_client.post("/v1/metrics/cache-config/bulk", json=bulk_update_data)

    # Assertions
    assert response.status_code == 500
    assert "failed to bulk update metric cache configurations" in response.json()["detail"].lower()
