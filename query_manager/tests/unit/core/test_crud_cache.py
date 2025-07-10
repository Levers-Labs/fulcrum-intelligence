import pytest
from sqlalchemy import select

from commons.db.crud import NotFoundError
from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from query_manager.core.crud import CRUDMetricCacheConfig, CRUDMetricCacheGrainConfig
from query_manager.core.models import MetricCacheConfig, MetricCacheGrainConfig


@pytest.fixture
async def metric_cache_grain_crud(db_session, jwt_payload):
    # Ensure tenant_id is set in context
    set_tenant_id(jwt_payload["tenant_id"])
    return CRUDMetricCacheGrainConfig(model=MetricCacheGrainConfig, session=db_session)


@pytest.fixture
async def metric_cache_config_crud(db_session, jwt_payload):
    # Ensure tenant_id is set in context
    set_tenant_id(jwt_payload["tenant_id"])
    return CRUDMetricCacheConfig(model=MetricCacheConfig, session=db_session)


@pytest.mark.asyncio
async def test_get_by_grain(db_session, jwt_payload):
    # Ensure tenant_id is set in context
    set_tenant_id(jwt_payload["tenant_id"])

    # Prepare test data
    grain_config = MetricCacheGrainConfig(
        tenant_id=jwt_payload["tenant_id"],
        grain=Granularity.DAY,
        is_enabled=True,
        initial_sync_period=730,
        delta_sync_period=90,
    )
    db_session.add(grain_config)
    await db_session.commit()

    # Create CRUD instance and call method directly
    grain_crud = CRUDMetricCacheGrainConfig(model=MetricCacheGrainConfig, session=db_session)

    # Test successful retrieval
    result = await grain_crud.get_by_grain(Granularity.DAY)
    assert result.grain == Granularity.DAY
    assert result.is_enabled is True
    assert result.initial_sync_period == 730
    assert result.delta_sync_period == 90

    # Test non-existent grain
    with pytest.raises(NotFoundError):
        await grain_crud.get_by_grain(Granularity.MONTH)


@pytest.mark.asyncio
async def test_create_default_grain_configs(db_session, jwt_payload):
    # Ensure tenant_id is set in context
    set_tenant_id(jwt_payload["tenant_id"])

    # Create the CRUD instance directly
    grain_crud = CRUDMetricCacheGrainConfig(model=MetricCacheGrainConfig, session=db_session)

    # Execute
    configs = await grain_crud.create_default_grain_configs()

    # Assert
    assert len(configs) == 3
    grains = [config.grain for config in configs]
    assert Granularity.DAY in grains
    assert Granularity.WEEK in grains
    assert Granularity.MONTH in grains

    # Verify database entries
    result = await db_session.execute(select(MetricCacheGrainConfig))
    db_configs = list(result.scalars().all())
    assert len(db_configs) == 3


@pytest.mark.asyncio
async def test_update_grain_config(db_session, jwt_payload):
    # Ensure tenant_id is set in context
    set_tenant_id(jwt_payload["tenant_id"])

    # Prepare test data
    grain_config = MetricCacheGrainConfig(
        tenant_id=jwt_payload["tenant_id"],
        grain=Granularity.DAY,
        is_enabled=True,
        initial_sync_period=730,
        delta_sync_period=90,
    )
    db_session.add(grain_config)
    await db_session.commit()

    # Create the CRUD instance directly
    grain_crud = CRUDMetricCacheGrainConfig(model=MetricCacheGrainConfig, session=db_session)

    # Execute update
    update_data = {"is_enabled": False, "initial_sync_period": 365}
    updated = await grain_crud.update_grain_config(Granularity.DAY, update_data)

    # Assert
    assert updated.is_enabled is False
    assert updated.initial_sync_period == 365
    assert updated.delta_sync_period == 90  # Unchanged

    # Verify database update
    result = await db_session.execute(select(MetricCacheGrainConfig).filter_by(grain=Granularity.DAY))
    db_config = result.scalar_one()
    assert db_config.is_enabled is False
    assert db_config.initial_sync_period == 365


@pytest.mark.asyncio
async def test_bulk_update_grain_configs(db_session, jwt_payload):
    # Ensure tenant_id is set in context
    set_tenant_id(jwt_payload["tenant_id"])

    # Create the CRUD instance directly
    grain_crud = CRUDMetricCacheGrainConfig(model=MetricCacheGrainConfig, session=db_session)

    # Prepare test data - create initial configs
    await grain_crud.create_default_grain_configs()

    # Execute bulk update
    configs_data = [
        {"grain": Granularity.DAY, "is_enabled": False, "delta_sync_period": 30},
        {"grain": Granularity.WEEK, "is_enabled": False},
        {"grain": Granularity.YEAR, "is_enabled": True, "initial_sync_period": 1825, "delta_sync_period": 180},
    ]
    updated_configs = await grain_crud.bulk_update_grain_configs(configs_data)

    # Assert
    assert len(updated_configs) == 3

    # Find day config in updated results
    day_config = next((c for c in updated_configs if c.grain == Granularity.DAY), None)
    assert day_config is not None
    assert day_config.is_enabled is False
    assert day_config.delta_sync_period == 30

    # Find week config in updated results
    week_config = next((c for c in updated_configs if c.grain == Granularity.WEEK), None)
    assert week_config is not None
    assert week_config.is_enabled is False

    # Find year config in updated results (should be newly created)
    year_config = next((c for c in updated_configs if c.grain == Granularity.YEAR), None)
    assert year_config is not None
    assert year_config.is_enabled is True
    assert year_config.initial_sync_period == 1825


@pytest.mark.asyncio
async def test_get_by_metric_id(db_session, jwt_payload):
    # Ensure tenant_id is set in context
    set_tenant_id(jwt_payload["tenant_id"])

    # Prepare test data
    metric_config = MetricCacheConfig(
        tenant_id=jwt_payload["tenant_id"],
        metric_id="test_metric",
        is_enabled=True,
    )
    db_session.add(metric_config)
    await db_session.commit()

    # Create the CRUD instance directly
    cache_crud = CRUDMetricCacheConfig(model=MetricCacheConfig, session=db_session)

    # Test successful retrieval
    result = await cache_crud.get_by_metric_id("test_metric")
    assert result.metric_id == "test_metric"
    assert result.is_enabled is True

    # Test non-existent metric
    with pytest.raises(NotFoundError):
        await cache_crud.get_by_metric_id("non_existent_metric")


@pytest.mark.asyncio
async def test_create_or_update_metric_config(db_session, jwt_payload):
    # Ensure tenant_id is set in context
    set_tenant_id(jwt_payload["tenant_id"])

    # Create the CRUD instance directly
    cache_crud = CRUDMetricCacheConfig(model=MetricCacheConfig, session=db_session)

    # Test creation of new config
    config = await cache_crud.create_or_update_metric_config("test_metric", True)
    assert config.metric_id == "test_metric"
    assert config.is_enabled is True

    # Test update of existing config
    updated_config = await cache_crud.create_or_update_metric_config("test_metric", False)
    assert updated_config.metric_id == "test_metric"
    assert updated_config.is_enabled is False

    # Verify in database
    result = await db_session.execute(select(MetricCacheConfig).filter_by(metric_id="test_metric"))
    db_config = result.scalar_one()
    assert db_config.is_enabled is False


@pytest.mark.asyncio
async def test_bulk_update_metric_configs(db_session, jwt_payload):
    # Ensure tenant_id is set in context
    set_tenant_id(jwt_payload["tenant_id"])

    # Create the CRUD instance directly
    cache_crud = CRUDMetricCacheConfig(model=MetricCacheConfig, session=db_session)

    # Execute bulk update
    metric_ids = ["metric1", "metric2", "metric3"]
    updated_configs = await cache_crud.bulk_update_metric_configs(metric_ids, True)

    # Assert
    assert len(updated_configs) == 3
    for config in updated_configs:
        assert config.is_enabled is True
        assert config.metric_id in metric_ids

    # Update some to disabled
    updated_configs = await cache_crud.bulk_update_metric_configs(["metric1", "metric2"], False)

    # Assert
    assert len(updated_configs) == 2
    for config in updated_configs:
        assert config.is_enabled is False

    # Verify in database that metric3 remains enabled
    result = await db_session.execute(select(MetricCacheConfig).filter_by(metric_id="metric3"))
    config3 = result.scalar_one()
    assert config3.is_enabled is True


@pytest.mark.asyncio
async def test_get_enabled_metrics(db_session, jwt_payload):
    # Ensure tenant_id is set in context
    set_tenant_id(jwt_payload["tenant_id"])

    # Prepare test data
    configs = [
        MetricCacheConfig(tenant_id=jwt_payload["tenant_id"], metric_id="metric1", is_enabled=True),
        MetricCacheConfig(tenant_id=jwt_payload["tenant_id"], metric_id="metric2", is_enabled=False),
        MetricCacheConfig(tenant_id=jwt_payload["tenant_id"], metric_id="metric3", is_enabled=True),
    ]
    for config in configs:
        db_session.add(config)
    await db_session.commit()

    # Create the CRUD instance directly
    cache_crud = CRUDMetricCacheConfig(model=MetricCacheConfig, session=db_session)

    # Execute
    enabled_configs = await cache_crud.get_enabled_metrics()

    # Assert
    assert len(enabled_configs) == 2
    metric_ids = [config.metric_id for config in enabled_configs]
    assert "metric1" in metric_ids
    assert "metric3" in metric_ids
    assert "metric2" not in metric_ids
