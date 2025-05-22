"""
Unit tests for historical performance pattern models.
"""

import pytest
from pydantic import ValidationError

from levers.models import (
    AnalysisWindow,
    Granularity,
    TrendExceptionType,
    TrendType,
)
from levers.models.patterns import (
    BenchmarkComparison,
    GrowthStats,
    HistoricalPerformance,
    PeriodMetrics,
    RankSummary,
    Seasonality,
    TrendException,
    TrendInfo,
)


class TestPeriodMetrics:
    """Tests for the PeriodMetrics class."""

    def test_valid_creation(self):
        """Test creating a valid PeriodMetrics object."""
        # Arrange & Act
        metrics = PeriodMetrics(
            period_start="2023-01-01",
            period_end="2023-01-31",
            pop_growth_percent=10.5,
            pop_acceleration_percent=2.0,
            central_line=105.5,
            ucl=120.3,
            lcl=90.7,
            slope=1.5,
            slope_change_percent=0.5,
            trend_signal_detected=True,
        )

        # Assert
        assert metrics.period_start == "2023-01-01"
        assert metrics.period_end == "2023-01-31"
        assert metrics.pop_growth_percent == 10.5
        assert metrics.pop_acceleration_percent == 2.0
        assert metrics.central_line == 105.5
        assert metrics.ucl == 120.3
        assert metrics.lcl == 90.7
        assert metrics.slope == 1.5
        assert metrics.slope_change_percent == 0.5
        assert metrics.trend_signal_detected is True

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        metrics = PeriodMetrics(
            period_start="2023-01-01",
            period_end="2023-01-31",
            pop_growth_percent=None,
            pop_acceleration_percent=None,
            central_line=None,
            ucl=None,
            lcl=None,
            slope=None,
            slope_change_percent=None,
        )

        # Assert
        assert metrics.period_start == "2023-01-01"
        assert metrics.period_end == "2023-01-31"
        assert metrics.pop_growth_percent is None
        assert metrics.pop_acceleration_percent is None
        assert metrics.central_line is None
        assert metrics.ucl is None
        assert metrics.lcl is None
        assert metrics.slope is None
        assert metrics.slope_change_percent is None
        assert metrics.trend_signal_detected is False  # Default value

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            PeriodMetrics(
                period_start="2023-01-01"
                # Missing period_end
            )

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        metrics = PeriodMetrics(
            period_start="2023-01-01",
            period_end="2023-01-31",
            pop_growth_percent=10.5,
            pop_acceleration_percent=2.0,
            central_line=105.5,
            ucl=120.3,
            lcl=90.7,
            slope=1.5,
            slope_change_percent=0.5,
            trend_signal_detected=True,
        )

        # Act
        result = metrics.to_dict()

        # Assert
        assert result["period_start"] == "2023-01-01"
        assert result["period_end"] == "2023-01-31"
        assert result["pop_growth_percent"] == 10.5
        assert result["pop_acceleration_percent"] == 2.0
        assert result["central_line"] == 105.5
        assert result["ucl"] == 120.3
        assert result["lcl"] == 90.7
        assert result["slope"] == 1.5
        assert result["slope_change_percent"] == 0.5
        assert result["trend_signal_detected"] is True


class TestGrowthStats:
    """Tests for the GrowthStats class."""

    def test_valid_creation(self):
        """Test creating a valid GrowthStats object."""
        # Arrange & Act
        stats = GrowthStats(
            current_pop_growth=10.5,
            average_pop_growth=8.0,
            current_growth_acceleration=2.5,
            num_periods_accelerating=3,
            num_periods_slowing=1,
        )

        # Assert
        assert stats.current_pop_growth == 10.5
        assert stats.average_pop_growth == 8.0
        assert stats.current_growth_acceleration == 2.5
        assert stats.num_periods_accelerating == 3
        assert stats.num_periods_slowing == 1

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        stats = GrowthStats(current_pop_growth=None, average_pop_growth=None, current_growth_acceleration=None)

        # Assert
        assert stats.current_pop_growth is None
        assert stats.average_pop_growth is None
        assert stats.current_growth_acceleration is None
        assert stats.num_periods_accelerating == 0
        assert stats.num_periods_slowing == 0

    def test_default_values(self):
        """Test default values."""
        # Arrange & Act
        stats = GrowthStats()

        # Assert
        assert stats.current_pop_growth is None
        assert stats.average_pop_growth is None
        assert stats.current_growth_acceleration is None
        assert stats.num_periods_accelerating == 0
        assert stats.num_periods_slowing == 0

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        stats = GrowthStats(
            current_pop_growth=10.5,
            average_pop_growth=8.0,
            current_growth_acceleration=2.5,
            num_periods_accelerating=3,
            num_periods_slowing=1,
        )

        # Act
        result = stats.to_dict()

        # Assert
        assert result["current_pop_growth"] == 10.5
        assert result["average_pop_growth"] == 8.0
        assert result["current_growth_acceleration"] == 2.5
        assert result["num_periods_accelerating"] == 3
        assert result["num_periods_slowing"] == 1


class TestRankSummary:
    """Tests for the RankSummary class."""

    def test_valid_creation(self):
        """Test creating a valid RankSummary object."""
        # Arrange & Act
        rank = RankSummary(
            value=150.0,
            rank=1,
            duration_grains=30,
            prior_record_value=140.0,
            prior_record_date="2023-01-15",
            absolute_delta_from_prior_record=10.0,
            relative_delta_from_prior_record=7.14,
        )

        # Assert
        assert rank.value == 150.0
        assert rank.rank == 1
        assert rank.duration_grains == 30
        assert rank.prior_record_value == 140.0
        assert rank.prior_record_date == "2023-01-15"
        assert rank.absolute_delta_from_prior_record == 10.0
        assert rank.relative_delta_from_prior_record == 7.14

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        rank = RankSummary(
            value=150.0,
            rank=1,
            duration_grains=30,
            prior_record_value=None,
            prior_record_date=None,
            absolute_delta_from_prior_record=None,
            relative_delta_from_prior_record=None,
        )

        # Assert
        assert rank.value == 150.0
        assert rank.rank == 1
        assert rank.duration_grains == 30
        assert rank.prior_record_value is None
        assert rank.prior_record_date is None
        assert rank.absolute_delta_from_prior_record is None
        assert rank.relative_delta_from_prior_record is None

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            RankSummary(
                value=150.0,
                rank=1,
                # Missing duration_grains
            )

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        rank = RankSummary(
            value=150.0,
            rank=1,
            duration_grains=30,
            prior_record_value=140.0,
            prior_record_date="2023-01-15",
            absolute_delta_from_prior_record=10.0,
            relative_delta_from_prior_record=7.14,
        )

        # Act
        result = rank.to_dict()

        # Assert
        assert result["value"] == 150.0
        assert result["rank"] == 1
        assert result["duration_grains"] == 30
        assert result["prior_record_value"] == 140.0
        assert result["prior_record_date"] == "2023-01-15"
        assert result["absolute_delta_from_prior_record"] == 10.0
        assert result["relative_delta_from_prior_record"] == 7.14


class TestSeasonality:
    """Tests for the Seasonality class."""

    def test_valid_creation(self):
        """Test creating a valid Seasonality object."""
        # Arrange & Act
        seasonality = Seasonality(
            is_following_expected_pattern=True,
            expected_change_percent=10.0,
            actual_change_percent=9.5,
            deviation_percent=-0.5,
        )

        # Assert
        assert seasonality.is_following_expected_pattern is True
        assert seasonality.expected_change_percent == 10.0
        assert seasonality.actual_change_percent == 9.5
        assert seasonality.deviation_percent == -0.5

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        seasonality = Seasonality(
            is_following_expected_pattern=False,
            expected_change_percent=None,
            actual_change_percent=None,
            deviation_percent=None,
        )

        # Assert
        assert seasonality.is_following_expected_pattern is False
        assert seasonality.expected_change_percent is None
        assert seasonality.actual_change_percent is None
        assert seasonality.deviation_percent is None

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            Seasonality()  # Missing is_following_expected_pattern

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        seasonality = Seasonality(
            is_following_expected_pattern=True,
            expected_change_percent=10.0,
            actual_change_percent=9.5,
            deviation_percent=-0.5,
        )

        # Act
        result = seasonality.to_dict()

        # Assert
        assert result["is_following_expected_pattern"] is True
        assert result["expected_change_percent"] == 10.0
        assert result["actual_change_percent"] == 9.5
        assert result["deviation_percent"] == -0.5


class TestBenchmarkComparison:
    """Tests for the BenchmarkComparison class."""

    def test_valid_creation(self):
        """Test creating a valid BenchmarkComparison object."""
        # Arrange & Act
        benchmark = BenchmarkComparison(reference_period="WTD", absolute_change=10.0, change_percent=5.0)

        # Assert
        assert benchmark.reference_period == "WTD"
        assert benchmark.absolute_change == 10.0
        assert benchmark.change_percent == 5.0

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        benchmark = BenchmarkComparison(reference_period="WTD", absolute_change=10.0, change_percent=None)

        # Assert
        assert benchmark.reference_period == "WTD"
        assert benchmark.absolute_change == 10.0
        assert benchmark.change_percent is None

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            BenchmarkComparison(
                reference_period="WTD"
                # Missing absolute_change
            )

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        benchmark = BenchmarkComparison(reference_period="WTD", absolute_change=10.0, change_percent=5.0)

        # Act
        result = benchmark.to_dict()

        # Assert
        assert result["reference_period"] == "WTD"
        assert result["absolute_change"] == 10.0
        assert result["change_percent"] == 5.0


class TestTrendInfo:
    """Tests for the TrendInfo class."""

    def test_valid_creation(self):
        """Test creating a valid TrendInfo object."""
        # Arrange & Act
        trend = TrendInfo(
            trend_type=TrendType.UPWARD, start_date="2023-01-01", average_pop_growth=10.5, duration_grains=30
        )

        # Assert
        assert trend.trend_type == TrendType.UPWARD
        assert trend.start_date == "2023-01-01"
        assert trend.average_pop_growth == 10.5
        assert trend.duration_grains == 30

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        trend = TrendInfo(
            trend_type=TrendType.STABLE, start_date="2023-01-01", average_pop_growth=None, duration_grains=30
        )

        # Assert
        assert trend.trend_type == TrendType.STABLE
        assert trend.start_date == "2023-01-01"
        assert trend.average_pop_growth is None
        assert trend.duration_grains == 30

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            TrendInfo(
                trend_type=TrendType.UPWARD,
                start_date="2023-01-01",
                # Missing duration_grains
            )

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        trend = TrendInfo(
            trend_type=TrendType.UPWARD, start_date="2023-01-01", average_pop_growth=10.5, duration_grains=30
        )

        # Act
        result = trend.to_dict()

        # Assert
        assert result["trend_type"] == "upward"
        assert result["start_date"] == "2023-01-01"
        assert result["average_pop_growth"] == 10.5
        assert result["duration_grains"] == 30


class TestTrendException:
    """Tests for the TrendException class."""

    def test_valid_creation(self):
        """Test creating a valid TrendException object."""
        # Arrange & Act
        exception = TrendException(
            type=TrendExceptionType.SPIKE,
            current_value=150.0,
            normal_range_low=100.0,
            normal_range_high=130.0,
            absolute_delta_from_normal_range=20.0,
            magnitude_percent=15.38,
        )

        # Assert
        assert exception.type == TrendExceptionType.SPIKE
        assert exception.current_value == 150.0
        assert exception.normal_range_low == 100.0
        assert exception.normal_range_high == 130.0
        assert exception.absolute_delta_from_normal_range == 20.0
        assert exception.magnitude_percent == 15.38

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        exception = TrendException(
            type=TrendExceptionType.DROP,
            current_value=80.0,
            normal_range_low=100.0,
            normal_range_high=130.0,
            absolute_delta_from_normal_range=None,
            magnitude_percent=None,
        )

        # Assert
        assert exception.type == TrendExceptionType.DROP
        assert exception.current_value == 80.0
        assert exception.normal_range_low == 100.0
        assert exception.normal_range_high == 130.0
        assert exception.absolute_delta_from_normal_range is None
        assert exception.magnitude_percent is None

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            TrendException(
                type=TrendExceptionType.SPIKE,
                current_value=150.0,
                normal_range_low=100.0,
                # Missing normal_range_high
            )

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        exception = TrendException(
            type=TrendExceptionType.SPIKE,
            current_value=150.0,
            normal_range_low=100.0,
            normal_range_high=130.0,
            absolute_delta_from_normal_range=20.0,
            magnitude_percent=15.38,
        )

        # Act
        result = exception.to_dict()

        # Assert
        assert result["type"] == "Spike"
        assert result["current_value"] == 150.0
        assert result["normal_range_low"] == 100.0
        assert result["normal_range_high"] == 130.0
        assert result["absolute_delta_from_normal_range"] == 20.0
        assert result["magnitude_percent"] == 15.38


class TestHistoricalPerformance:
    """Tests for the HistoricalPerformance class."""

    def test_valid_creation(self):
        """Test creating a valid HistoricalPerformance object."""
        # Arrange & Act
        performance = HistoricalPerformance(
            pattern="historical_performance",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY),
            period_metrics=[
                PeriodMetrics(period_start="2023-01-01", period_end="2023-01-15", pop_growth_percent=10.5),
                PeriodMetrics(period_start="2023-01-16", period_end="2023-01-31", pop_growth_percent=12.0),
            ],
            growth_stats=GrowthStats(current_pop_growth=12.0, average_pop_growth=11.25),
            current_trend=TrendInfo(trend_type=TrendType.UPWARD, start_date="2023-01-01", duration_grains=31),
            high_rank=RankSummary(value=150.0, rank=1, duration_grains=31),
            low_rank=RankSummary(value=100.0, rank=31, duration_grains=31),
            benchmark_comparison=BenchmarkComparison(reference_period="WTD", absolute_change=10.0, change_percent=5.0),
            trend_exception=TrendException(
                type=TrendExceptionType.SPIKE, current_value=150.0, normal_range_low=100.0, normal_range_high=130.0
            ),
            grain=Granularity.DAY,
        )

        # Assert
        assert performance.pattern == "historical_performance"
        assert performance.metric_id == "test_metric"
        assert performance.analysis_window.start_date == "2023-01-01"
        assert performance.analysis_window.end_date == "2023-01-31"
        assert len(performance.period_metrics) == 2
        assert performance.growth_stats.current_pop_growth == 12.0
        assert performance.current_trend.trend_type == TrendType.UPWARD
        assert performance.high_rank.value == 150.0
        assert performance.low_rank.value == 100.0
        assert performance.benchmark_comparison is not None
        assert performance.trend_exception is not None

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            HistoricalPerformance(
                pattern="historical_performance",
                metric_id="test_metric",
                analysis_window=AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31"),
                period_metrics=[],
                growth_stats=GrowthStats(),
                # Missing high_rank and low_rank
            )

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        performance = HistoricalPerformance(
            pattern="historical_performance",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31"),
            growth_stats=GrowthStats(),
            high_rank=RankSummary(value=150.0, rank=1, duration_grains=31),
            low_rank=RankSummary(value=100.0, rank=31, duration_grains=31),
            grain=Granularity.DAY,
        )

        # Act
        result = performance.to_dict()

        # Assert
        assert result["pattern"] == "historical_performance"
        assert result["metric_id"] == "test_metric"
        assert result["analysis_window"]["start_date"] == "2023-01-01"
        assert result["analysis_window"]["end_date"] == "2023-01-31"
        assert "period_metrics" in result
        assert "growth_stats" in result
        assert "high_rank" in result
        assert "low_rank" in result
