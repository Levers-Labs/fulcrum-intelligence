"""Tests for the pattern manager."""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlmodel.ext.asyncio.session import AsyncSession

from analysis_manager.patterns.crud.config import PatternConfigCRUD
from analysis_manager.patterns.crud.performance_status import PerformanceStatusCRUD
from analysis_manager.patterns.manager import PatternManager
from analysis_manager.patterns.models.config import PatternConfig
from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from levers.models import PatternConfig as PatternConfigModel
from levers.models.common import AnalysisWindow
from levers.models.pattern_config import (
    AnalysisWindowConfig,
    DataSource,
    DataSourceType,
    WindowStrategy,
)
from levers.models.patterns.performance_status import MetricGVAStatus, MetricPerformance

pytestmark = pytest.mark.asyncio


@pytest.fixture
def performance_status_model():
    """Create a performance status model."""
    return MetricPerformance(
        metric_id="test_metric",
        pattern="performance_status",
        current_value=100.0,
        prior_value=90.0,
        target_value=110.0,
        status=MetricGVAStatus.ON_TRACK,
        absolute_delta_from_prior=10.0,
        pop_change_percent=0.11,
        absolute_gap=10.0,
        percent_gap=0.09,
        analysis_date=date.today(),
        analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2023-01-01", end_date="2023-01-31"),
        num_periods=31,
    )


@pytest.fixture
def sample_pattern_config_model():
    """Fixture providing a sample PatternConfigModel instance."""
    return PatternConfigModel(
        pattern_name="test_pattern",
        version="1.0",
        description="Test pattern for unit tests",
        data_sources=[DataSource(source_type=DataSourceType.METRIC_TIME_SERIES, data_key="data")],
        analysis_window=AnalysisWindowConfig(strategy=WindowStrategy.FIXED_TIME, days=90),
    )


@pytest.fixture
def sample_db_pattern_config(sample_pattern_config_model):
    """Fixture providing a sample database PatternConfig instance."""
    db_config = PatternConfig.from_pydantic(sample_pattern_config_model)
    return db_config


@pytest.fixture
def mock_session():
    """Fixture providing a mock AsyncSession."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def mock_pattern_manager(mock_session):
    """Fixture providing a PatternManager instance with mocked dependencies."""
    # Create mock for config_crud
    config_crud = MagicMock(spec=PatternConfigCRUD)

    # Patch the PatternConfigCRUD initialization
    with patch("analysis_manager.patterns.manager.PatternConfigCRUD", return_value=config_crud):
        manager = PatternManager(mock_session)
        manager.session = mock_session
        return manager


@pytest.fixture
def pattern_manager(db_session):
    """Create a pattern manager with a real database session."""
    return PatternManager(db_session)


async def test_store_pattern_result(performance_status_model, pattern_manager, jwt_payload):
    """Test the store pattern result method."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    # Act
    result = await pattern_manager.store_pattern_result("performance_status", performance_status_model)
    # Assert
    assert result is not None
    assert result.metric_id == performance_status_model.metric_id
    assert result.pattern == performance_status_model.pattern
    assert result.current_value == performance_status_model.current_value
    assert result.prior_value == performance_status_model.prior_value
    assert result.target_value == performance_status_model.target_value
    assert result.status == performance_status_model.status


async def test_get_latest_pattern_result(performance_status_model, pattern_manager, jwt_payload):
    """Test the get latest pattern result method."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    # Store the pattern result
    await pattern_manager.store_pattern_result("performance_status", performance_status_model)
    # Act
    result = await pattern_manager.get_latest_pattern_result("performance_status", performance_status_model.metric_id)
    # Assert
    assert result is not None
    assert result.metric_id == performance_status_model.metric_id
    assert result.pattern == performance_status_model.pattern
    assert result.current_value == performance_status_model.current_value


async def test_get_pattern_crud(pattern_manager):
    """Test the get pattern crud method."""
    # Act
    result = pattern_manager._get_pattern_crud("performance_status")
    # Assert
    assert result is not None
    assert result.__class__ == PerformanceStatusCRUD


async def test_get_pattern_crud_unknown_pattern(pattern_manager):
    """Test the get pattern crud method with an unknown pattern."""
    # Act
    with pytest.raises(ValueError):
        pattern_manager._get_pattern_crud("unknown_pattern")


async def test_get_pattern_config_from_db(mock_pattern_manager, sample_db_pattern_config, sample_pattern_config_model):
    """Test getting a pattern configuration from the database."""
    # Arrange
    pattern_name = "test_pattern"
    mock_pattern_manager.config_crud.get_config = AsyncMock(return_value=sample_db_pattern_config)

    # Act
    result = await mock_pattern_manager.get_pattern_config(pattern_name)

    # Assert
    mock_pattern_manager.config_crud.get_config.assert_called_once_with(pattern_name)
    assert result == sample_pattern_config_model


async def test_get_pattern_config_default(mock_pattern_manager):
    """Test getting a default pattern configuration when not in the database."""
    # Arrange
    pattern_name = "test_pattern"
    mock_pattern_manager.config_crud.get_config = AsyncMock(return_value=None)

    mock_levers = MagicMock()
    mock_default_config = MagicMock()
    mock_levers.get_pattern_default_config.return_value = mock_default_config

    with patch("analysis_manager.patterns.manager.Levers", return_value=mock_levers):
        # Act
        result = await mock_pattern_manager.get_pattern_config(pattern_name)

        # Assert
        mock_pattern_manager.config_crud.get_config.assert_called_once_with(pattern_name)
        mock_levers.get_pattern_default_config.assert_called_once_with(pattern_name)
        assert result == mock_default_config


async def test_list_pattern_configs(mock_pattern_manager, sample_db_pattern_config, sample_pattern_config_model):
    """Test listing all pattern configurations."""
    # Arrange
    db_configs = [sample_db_pattern_config]
    mock_pattern_manager.config_crud.list_configs = AsyncMock(return_value=db_configs)

    # Act
    result = await mock_pattern_manager.list_pattern_configs()

    # Assert
    mock_pattern_manager.config_crud.list_configs.assert_called_once()
    assert len(result) == 1
    assert result[0] == sample_pattern_config_model


async def test_store_pattern_config(mock_pattern_manager, sample_pattern_config_model):
    """Test storing a pattern configuration."""
    # Arrange
    mock_db_config = MagicMock()
    mock_pattern_manager.config_crud.create_or_update_config = AsyncMock(return_value=mock_db_config)

    # Act
    result = await mock_pattern_manager.store_pattern_config(sample_pattern_config_model)

    # Assert
    mock_pattern_manager.config_crud.create_or_update_config.assert_called_once_with(sample_pattern_config_model)
    assert result == mock_db_config


async def test_delete_pattern_config(mock_pattern_manager):
    """Test deleting a pattern configuration."""
    # Arrange
    pattern_name = "test_pattern"
    mock_pattern_manager.config_crud.delete_config = AsyncMock(return_value=True)

    # Act
    result = await mock_pattern_manager.delete_pattern_config(pattern_name)

    # Assert
    mock_pattern_manager.config_crud.delete_config.assert_called_once_with(pattern_name)
    assert result is True


async def test_integration_pattern_config_crud(pattern_manager, sample_pattern_config_model, jwt_payload, db_session):
    """Integration test for pattern config CRUD operations."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    pattern_name = "test_pattern"

    # Act - Store config
    await pattern_manager.store_pattern_config(sample_pattern_config_model)

    # Act - Get config
    result = await pattern_manager.get_pattern_config(pattern_name)

    # Assert - Get config
    assert result is not None
    assert result.pattern_name == pattern_name
    assert result.data_sources[0].source_type == DataSourceType.METRIC_TIME_SERIES

    # Act - List configs
    configs = await pattern_manager.list_pattern_configs()

    # Assert - List configs
    assert len(configs) >= 1
    assert any(config.pattern_name == pattern_name for config in configs)

    # Act - Delete config
    delete_result = await pattern_manager.delete_pattern_config(pattern_name)

    # Assert - Delete config
    assert delete_result is True

    # Verify it's gone
    configs_after_delete = await pattern_manager.list_pattern_configs()
    assert not any(config.pattern_name == pattern_name for config in configs_after_delete)
