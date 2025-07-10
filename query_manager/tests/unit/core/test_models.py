from commons.models.enums import Granularity
from query_manager.core.models import (
    Dimension,
    Metric,
    MetricCacheConfig,
    MetricCacheGrainConfig,
)


def test_metric_schema_get_dimension(metric, dimension):
    # Prepare
    metric = Metric.model_validate(metric)
    dimension_id = dimension["dimension_id"]
    dimension_obj = Dimension(**dimension)
    metric.dimensions = [dimension_obj]

    # Execute
    result = metric.get_dimension(dimension_id)

    # Assert
    assert result == dimension_obj

    # Execute
    result = metric.get_dimension("dimension2")

    # Assert
    assert result is None

    # Prepare
    metric.dimensions = []

    # Execute
    result = metric.get_dimension(dimension_id)

    # Assert
    assert result is None


def test_metric_cache_grain_config():
    """Test MetricCacheGrainConfig model creation and properties."""
    # Test with all parameters
    config = MetricCacheGrainConfig(
        tenant_id=1,
        grain=Granularity.DAY,
        is_enabled=True,
        initial_sync_period=730,
        delta_sync_period=90,
    )

    assert config.tenant_id == 1
    assert config.grain == Granularity.DAY
    assert config.is_enabled is True
    assert config.initial_sync_period == 730
    assert config.delta_sync_period == 90

    # Test with default values
    config = MetricCacheGrainConfig(
        tenant_id=1,
        grain=Granularity.DAY,
        initial_sync_period=730,  # Provide defaults since these are required
        delta_sync_period=90,
    )
    assert config.is_enabled is True  # Default should be True
    assert config.initial_sync_period == 730
    assert config.delta_sync_period == 90


def test_metric_cache_config():
    """Test MetricCacheConfig model creation and properties."""
    # Test with explicit is_enabled
    config = MetricCacheConfig(
        tenant_id=1,
        metric_id="test_metric",
        is_enabled=True,
    )

    assert config.tenant_id == 1
    assert config.metric_id == "test_metric"
    assert config.is_enabled is True

    # Test with default is_enabled value
    config = MetricCacheConfig(tenant_id=1, metric_id="test_metric")
    assert config.is_enabled is True  # Default should be True
