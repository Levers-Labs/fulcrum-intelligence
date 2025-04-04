"""
Unit tests for pattern configuration models.
"""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from levers.exceptions import InvalidPatternConfigError
from levers.models.common import Granularity
from levers.models.pattern_config import (
    AnalysisWindowConfig,
    DataSource,
    DataSourceType,
    PatternConfig,
    WindowStrategy,
)


class TestAnalysisWindowConfig:
    """Tests for the AnalysisWindowConfig class."""

    def test_init_fixed_time(self):
        """Test initialization with FIXED_TIME strategy."""
        # Arrange & Act
        config = AnalysisWindowConfig(
            strategy=WindowStrategy.FIXED_TIME,
            days=180,
            min_days=30,
            max_days=365,
            include_today=False,
        )

        # Assert
        assert config.strategy == WindowStrategy.FIXED_TIME
        assert config.days == 180
        assert config.min_days == 30
        assert config.max_days == 365
        assert config.include_today is False

    def test_init_grain_specific_time(self):
        """Test initialization with GRAIN_SPECIFIC_TIME strategy."""
        # Arrange & Act
        grain_days = {
            Granularity.DAY: 90,
            Granularity.WEEK: 180,
            Granularity.MONTH: 365,
        }

        config = AnalysisWindowConfig(
            strategy=WindowStrategy.GRAIN_SPECIFIC_TIME,
            grain_days=grain_days,
            min_days=30,
            max_days=730,
        )

        # Assert
        assert config.strategy == WindowStrategy.GRAIN_SPECIFIC_TIME
        assert config.grain_days == grain_days
        assert config.min_days == 30
        assert config.max_days == 730

    def test_init_fixed_datapoints(self):
        """Test initialization with FIXED_DATAPOINTS strategy."""
        # Arrange & Act
        config = AnalysisWindowConfig(
            strategy=WindowStrategy.FIXED_DATAPOINTS,
            datapoints=24,
            min_days=30,
            max_days=365,
        )

        # Assert
        assert config.strategy == WindowStrategy.FIXED_DATAPOINTS
        assert config.datapoints == 24
        assert config.min_days == 30
        assert config.max_days == 365

    def test_validate_strategy_params_fixed_time_missing_days(self):
        """Test validation fails when days parameter is missing for FIXED_TIME strategy."""
        # Arrange & Act & Assert
        window_config = AnalysisWindowConfig(
            strategy=WindowStrategy.FIXED_TIME,
            # days parameter is missing
        )

        with pytest.raises(InvalidPatternConfigError) as exc_info:
            window_config.validate_strategy_params(pattern_name="test_pattern")

        assert "days parameter is required for FIXED_TIME strategy" in str(exc_info.value)
        assert exc_info.value.pattern_name == "test_pattern"

    def test_validate_strategy_params_grain_specific_time_missing_grain_days(self):
        """Test validation fails when grain_days parameter is missing for GRAIN_SPECIFIC_TIME strategy."""
        # Arrange & Act & Assert
        window_config = AnalysisWindowConfig(
            strategy=WindowStrategy.GRAIN_SPECIFIC_TIME,
            # grain_days parameter is missing
        )

        with pytest.raises(InvalidPatternConfigError) as exc_info:
            window_config.validate_strategy_params(pattern_name="test_pattern")

        assert "grain_days parameter is required for GRAIN_SPECIFIC_TIME strategy" in str(exc_info.value)
        assert exc_info.value.pattern_name == "test_pattern"

    def test_validate_strategy_params_fixed_datapoints_missing_datapoints(self):
        """Test validation fails when datapoints parameter is missing for FIXED_DATAPOINTS strategy."""
        # Arrange & Act & Assert
        window_config = AnalysisWindowConfig(
            strategy=WindowStrategy.FIXED_DATAPOINTS,
            # datapoints parameter is missing
        )

        with pytest.raises(InvalidPatternConfigError) as exc_info:
            window_config.validate_strategy_params(pattern_name="test_pattern")

        assert "datapoints parameter is required for FIXED_DATAPOINTS strategy" in str(exc_info.value)
        assert exc_info.value.pattern_name == "test_pattern"

    def test_get_date_range_fixed_time(self):
        """Test get_date_range with FIXED_TIME strategy."""
        # Arrange
        config = AnalysisWindowConfig(
            strategy=WindowStrategy.FIXED_TIME,
            days=90,
        )
        today = date.today()
        expected_end_date = today - timedelta(days=1)  # not including today
        expected_start_date = expected_end_date - timedelta(days=90)

        # Act
        start_date, end_date = config.get_date_range(Granularity.DAY)

        # Assert
        assert start_date == expected_start_date
        assert end_date == expected_end_date

    def test_get_date_range_fixed_time_include_today(self):
        """Test get_date_range with FIXED_TIME strategy including today."""
        # Arrange
        config = AnalysisWindowConfig(
            strategy=WindowStrategy.FIXED_TIME,
            days=90,
            include_today=True,
        )
        today = date.today()
        expected_end_date = today
        expected_start_date = expected_end_date - timedelta(days=90)

        # Act
        start_date, end_date = config.get_date_range(Granularity.DAY)

        # Assert
        assert start_date == expected_start_date
        assert end_date == expected_end_date

    def test_get_date_range_grain_specific_time(self):
        """Test get_date_range with GRAIN_SPECIFIC_TIME strategy."""
        # Arrange
        grain_days = {
            Granularity.DAY: 90,
            Granularity.WEEK: 180,
            Granularity.MONTH: 365,
        }

        config = AnalysisWindowConfig(
            strategy=WindowStrategy.GRAIN_SPECIFIC_TIME,
            grain_days=grain_days,
        )
        today = date.today()
        expected_end_date = today - timedelta(days=1)  # not including today

        # Act & Assert
        for grain, days in grain_days.items():
            expected_start_date = expected_end_date - timedelta(days=days)
            start_date, end_date = config.get_date_range(grain)
            assert start_date == expected_start_date
            assert end_date == expected_end_date

    def test_get_date_range_enforce_min_days(self):
        """Test get_date_range enforces minimum days constraint."""
        # Arrange
        config = AnalysisWindowConfig(
            strategy=WindowStrategy.FIXED_TIME,
            days=5,  # Less than min_days (default 7)
        )
        today = date.today()
        expected_end_date = today - timedelta(days=1)
        expected_start_date = expected_end_date - timedelta(days=7)  # Should use min_days instead of days

        # Act
        start_date, end_date = config.get_date_range(Granularity.DAY)

        # Assert
        assert start_date == expected_start_date
        assert end_date == expected_end_date

    def test_get_date_range_enforce_max_days(self):
        """Test get_date_range enforces maximum days constraint."""
        # Arrange
        config = AnalysisWindowConfig(
            strategy=WindowStrategy.FIXED_TIME,
            days=1000,  # More than max_days (default 730)
            max_days=365,  # Custom max_days
        )
        today = date.today()
        expected_end_date = today - timedelta(days=1)
        expected_start_date = expected_end_date - timedelta(days=365)  # Should use max_days instead of days

        # Act
        start_date, end_date = config.get_date_range(Granularity.DAY)

        # Assert
        assert start_date == expected_start_date
        assert end_date == expected_end_date

    def test_get_prev_period_start_date(self):
        """Test get_prev_period_start_date calculation."""
        # Arrange
        latest_start_date = date(2023, 12, 31)

        # Test cases for different grains
        test_cases = [
            (Granularity.DAY, 7, date(2023, 12, 24)),  # 7 days back
            (Granularity.WEEK, 4, date(2023, 12, 3)),  # 4 weeks back
            (Granularity.MONTH, 3, date(2023, 9, 30)),  # 3 months back
            (Granularity.QUARTER, 2, date(2023, 6, 30)),  # 2 quarters back
            (Granularity.YEAR, 1, date(2022, 12, 31)),  # 1 year back
        ]

        # Act & Assert
        for grain, period_count, expected_date in test_cases:
            result = AnalysisWindowConfig.get_prev_period_start_date(grain, period_count, latest_start_date)
            assert result == expected_date


class TestPatternConfig:
    """Tests for the PatternConfig class."""

    def test_init_minimal(self):
        """Test initialization with minimal required fields."""
        # Arrange & Act
        with patch.object(AnalysisWindowConfig, "validate_strategy_params") as mock_validate:
            config = PatternConfig(
                pattern_name="test_pattern",
                data_sources=[DataSource(source_type=DataSourceType.METRIC_TIME_SERIES, data_key="data")],
                analysis_window=AnalysisWindowConfig(strategy=WindowStrategy.FIXED_TIME, days=90),
            )

            # Assert
            assert config.pattern_name == "test_pattern"
            assert config.version == "1.0"  # Default value
            assert len(config.data_sources) == 1
            assert config.data_sources[0].source_type == DataSourceType.METRIC_TIME_SERIES
            assert config.data_sources[0].data_key == "data"
            assert config.analysis_window.strategy == WindowStrategy.FIXED_TIME
            assert config.analysis_window.days == 90
            assert config.settings == {}  # Default empty dict
            assert config.meta == {}  # Default empty dict

            # Verify pattern_name was passed to validate_strategy_params
            mock_validate.assert_called_once_with(pattern_name="test_pattern")

    def test_init_complete(self):
        """Test initialization with all fields."""
        # Arrange & Act
        with patch.object(AnalysisWindowConfig, "validate_strategy_params") as mock_validate:
            config = PatternConfig(
                pattern_name="test_pattern",
                version="2.0",
                description="Test pattern for unit testing",
                data_sources=[
                    DataSource(source_type=DataSourceType.METRIC_TIME_SERIES, data_key="main_data", is_required=True),
                    DataSource(
                        source_type=DataSourceType.METRIC_WITH_TARGETS, data_key="target_data", is_required=False
                    ),
                ],
                analysis_window=AnalysisWindowConfig(
                    strategy=WindowStrategy.GRAIN_SPECIFIC_TIME, grain_days={Granularity.DAY: 90, Granularity.WEEK: 180}
                ),
                settings={"threshold": 0.05, "min_datapoints": 10},
                meta={"created_by": "test_suite", "created_at": "2023-01-01"},
            )

            # Assert
            assert config.pattern_name == "test_pattern"
            assert config.version == "2.0"
            assert config.description == "Test pattern for unit testing"
            assert len(config.data_sources) == 2
            assert config.data_sources[0].source_type == DataSourceType.METRIC_TIME_SERIES
            assert config.data_sources[0].data_key == "main_data"
            assert config.data_sources[0].is_required is True
            assert config.data_sources[1].source_type == DataSourceType.METRIC_WITH_TARGETS
            assert config.data_sources[1].data_key == "target_data"
            assert config.data_sources[1].is_required is False
            assert config.analysis_window.strategy == WindowStrategy.GRAIN_SPECIFIC_TIME
            assert config.analysis_window.grain_days == {Granularity.DAY: 90, Granularity.WEEK: 180}
            assert config.settings == {"threshold": 0.05, "min_datapoints": 10}
            assert config.meta == {"created_by": "test_suite", "created_at": "2023-01-01"}

            # Verify pattern_name was passed to validate_strategy_params
            mock_validate.assert_called_once_with(pattern_name="test_pattern")

    def test_validate_nested_models_propagates_pattern_name(self):
        """Test that validate_nested_models properly passes pattern_name to child models."""
        # Arrange
        analysis_window = MagicMock(spec=AnalysisWindowConfig)
        analysis_window.validate_strategy_params = MagicMock()

        # Act
        PatternConfig(
            pattern_name="test_pattern",
            data_sources=[DataSource(source_type=DataSourceType.METRIC_TIME_SERIES, data_key="data")],
            analysis_window=analysis_window,
        )

        # Assert
        analysis_window.validate_strategy_params.assert_called_once_with(pattern_name="test_pattern")

    def test_validation_error_includes_pattern_name(self):
        """Test that validation errors from nested models include the pattern name."""
        # Arrange & Act & Assert
        with pytest.raises(InvalidPatternConfigError) as exc_info:
            PatternConfig(
                pattern_name="error_pattern",
                data_sources=[DataSource(source_type=DataSourceType.METRIC_TIME_SERIES, data_key="data")],
                analysis_window=AnalysisWindowConfig(
                    strategy=WindowStrategy.FIXED_TIME,
                    # days is missing, which will trigger a validation error
                ),
            )

        # Assert the error contains the pattern name
        assert exc_info.value.pattern_name == "error_pattern"
        assert "days parameter is required for FIXED_TIME strategy" in str(exc_info.value)


class TestDataSource:
    """Tests for the DataSource class."""

    def test_init_minimal(self):
        """Test initialization with minimal required fields."""
        # Arrange & Act
        data_source = DataSource(source_type=DataSourceType.METRIC_TIME_SERIES, data_key="data")

        # Assert
        assert data_source.source_type == DataSourceType.METRIC_TIME_SERIES
        assert data_source.data_key == "data"
        assert data_source.is_required is True  # Default value
        assert data_source.meta == {}  # Default empty dict

    def test_init_complete(self):
        """Test initialization with all fields."""
        # Arrange & Act
        data_source = DataSource(
            source_type=DataSourceType.DIMENSIONAL_TIME_SERIES,
            data_key="dimension_data",
            is_required=False,
            meta={"dimensions": ["region", "product"]},
        )

        # Assert
        assert data_source.source_type == DataSourceType.DIMENSIONAL_TIME_SERIES
        assert data_source.data_key == "dimension_data"
        assert data_source.is_required is False
        assert data_source.meta == {"dimensions": ["region", "product"]}
