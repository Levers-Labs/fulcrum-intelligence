"""
Unit tests for historical performance pattern models.
"""

from datetime import date

import pytest
from pydantic import ValidationError

from levers.models import (
    AnalysisWindow,
    ComparisonType,
    Granularity,
    TrendExceptionType,
    TrendType,
)
from levers.models.patterns import (
    Benchmark,
    BenchmarkComparison,
    GrowthStats,
    HistoricalPerformance,
    PeriodMetrics,
    RankSummary,
    Seasonality,
    TrendAnalysis,
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
        )

        # Assert
        assert metrics.period_start == "2023-01-01"
        assert metrics.period_end == "2023-01-31"
        assert metrics.pop_growth_percent == 10.5
        assert metrics.pop_acceleration_percent == 2.0

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        metrics = PeriodMetrics(
            period_start="2023-01-01",
            period_end="2023-01-31",
            pop_growth_percent=None,
            pop_acceleration_percent=None,
        )

        # Assert
        assert metrics.period_start == "2023-01-01"
        assert metrics.period_end == "2023-01-31"
        assert metrics.pop_growth_percent is None
        assert metrics.pop_acceleration_percent is None

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
        )

        # Act
        result = metrics.to_dict()

        # Assert
        assert result["period_start"] == "2023-01-01"
        assert result["period_end"] == "2023-01-31"
        assert result["pop_growth_percent"] == 10.5
        assert result["pop_acceleration_percent"] == 2.0


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


class TestBenchmark:
    """Tests for the Benchmark class."""

    def test_valid_creation(self):
        """Test creating a valid Benchmark object."""
        # Arrange & Act
        benchmark = Benchmark(
            reference_value=100.0,
            reference_date=date(2024, 1, 1),
            reference_period="Last Week",
            absolute_change=10.0,
            change_percent=5.0,
        )

        # Assert
        assert benchmark.reference_value == 100.0
        assert benchmark.reference_date == date(2024, 1, 1)
        assert benchmark.reference_period == "Last Week"
        assert benchmark.absolute_change == 10.0
        assert benchmark.change_percent == 5.0

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        benchmark = Benchmark(
            reference_value=100.0,
            reference_date=date(2024, 1, 1),
            reference_period="Last Week",
            absolute_change=10.0,
            change_percent=None,
        )

        # Assert
        assert benchmark.reference_value == 100.0
        assert benchmark.reference_date == date(2024, 1, 1)
        assert benchmark.reference_period == "Last Week"
        assert benchmark.absolute_change == 10.0
        assert benchmark.change_percent is None

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            Benchmark(
                reference_value=100.0,
                reference_date=date(2024, 1, 1),
                reference_period="Last Week",
                # Missing absolute_change
            )

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        benchmark = Benchmark(
            reference_value=100.0,
            reference_date=date(2024, 1, 1),
            reference_period="Last Week",
            absolute_change=10.0,
            change_percent=5.0,
        )

        # Act
        result = benchmark.to_dict()

        # Assert
        assert result["reference_value"] == 100.0
        assert result["reference_date"] == "2024-01-01"
        assert result["reference_period"] == "Last Week"
        assert result["absolute_change"] == 10.0
        assert result["change_percent"] == 5.0


class TestBenchmarkComparison:
    """Tests for the BenchmarkComparison class."""

    def test_valid_creation(self):
        """Test creating a valid BenchmarkComparison object."""
        # Arrange & Act
        benchmark_comparison = BenchmarkComparison(current_value=110.0, current_period="This Week")

        # Assert
        assert benchmark_comparison.current_value == 110.0
        assert benchmark_comparison.current_period == "This Week"
        assert len(benchmark_comparison.benchmarks) == 0

    def test_add_benchmark(self):
        """Test adding a benchmark to the comparison."""
        # Arrange
        benchmark_comparison = BenchmarkComparison(current_value=110.0, current_period="This Week")
        benchmark = Benchmark(
            reference_value=100.0,
            reference_date=date(2024, 1, 1),
            reference_period="Last Week",
            absolute_change=10.0,
            change_percent=10.0,
        )

        # Act
        benchmark_comparison.add_benchmark(ComparisonType.LAST_WEEK, benchmark)

        # Assert
        assert len(benchmark_comparison.benchmarks) == 1
        assert benchmark_comparison.has_benchmark(ComparisonType.LAST_WEEK)
        retrieved_benchmark = benchmark_comparison.get_benchmark(ComparisonType.LAST_WEEK)
        assert retrieved_benchmark is not None
        assert retrieved_benchmark.reference_value == 100.0

    def test_get_benchmark_not_found(self):
        """Test getting a benchmark that doesn't exist."""
        # Arrange
        benchmark_comparison = BenchmarkComparison(current_value=110.0, current_period="This Week")

        # Act & Assert
        assert benchmark_comparison.get_benchmark(ComparisonType.LAST_WEEK) is None
        assert not benchmark_comparison.has_benchmark(ComparisonType.LAST_WEEK)

    def test_get_all_benchmarks(self):
        """Test getting all benchmarks."""
        # Arrange
        benchmark_comparison = BenchmarkComparison(current_value=110.0, current_period="This Week")
        benchmark1 = Benchmark(
            reference_value=100.0,
            reference_date=date(2024, 1, 1),
            reference_period="Last Week",
            absolute_change=10.0,
            change_percent=10.0,
        )
        benchmark2 = Benchmark(
            reference_value=90.0,
            reference_date=date(2023, 12, 1),
            reference_period="Last Month",
            absolute_change=20.0,
            change_percent=22.2,
        )

        # Act
        benchmark_comparison.add_benchmark(ComparisonType.LAST_WEEK, benchmark1)
        benchmark_comparison.add_benchmark(ComparisonType.WEEK_IN_LAST_MONTH, benchmark2)
        all_benchmarks = benchmark_comparison.get_all_benchmarks()

        # Assert
        assert len(all_benchmarks) == 2
        assert ComparisonType.LAST_WEEK in all_benchmarks
        assert ComparisonType.WEEK_IN_LAST_MONTH in all_benchmarks

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            BenchmarkComparison(
                current_value=110.0
                # Missing current_period
            )

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        benchmark_comparison = BenchmarkComparison(current_value=110.0, current_period="This Week")
        benchmark = Benchmark(
            reference_value=100.0,
            reference_date=date(2024, 1, 1),
            reference_period="Last Week",
            absolute_change=10.0,
            change_percent=10.0,
        )
        benchmark_comparison.add_benchmark(ComparisonType.LAST_WEEK, benchmark)

        # Act
        result = benchmark_comparison.to_dict()

        # Assert
        assert result["current_value"] == 110.0
        assert result["current_period"] == "This Week"
        assert "benchmarks" in result
        assert "last_week" in result["benchmarks"]


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


class TestTrendAnalysis:
    """Tests for the TrendAnalysis class."""

    def test_valid_creation(self):
        """Test creating a valid TrendAnalysis object."""
        # Arrange & Act
        trend_analysis = TrendAnalysis(
            value=150.0,
            date="2023-01-31",
            central_line=120.0,
            ucl=140.0,
            lcl=100.0,
            slope=2.5,
            trend_signal_detected=True,
        )

        # Assert
        assert trend_analysis.value == 150.0
        assert trend_analysis.date == "2023-01-31"
        assert trend_analysis.central_line == 120.0
        assert trend_analysis.ucl == 140.0
        assert trend_analysis.lcl == 100.0
        assert trend_analysis.slope == 2.5
        assert trend_analysis.trend_signal_detected is True

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        trend_analysis = TrendAnalysis(
            value=150.0,
            date="2023-01-31",
            central_line=None,
            ucl=None,
            lcl=None,
            slope=None,
            trend_signal_detected=False,
        )

        # Assert
        assert trend_analysis.value == 150.0
        assert trend_analysis.date == "2023-01-31"
        assert trend_analysis.central_line is None
        assert trend_analysis.ucl is None
        assert trend_analysis.lcl is None
        assert trend_analysis.slope is None
        assert trend_analysis.trend_signal_detected is False

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            TrendAnalysis(
                date="2023-01-31",
                # Missing value
            )

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        trend_analysis = TrendAnalysis(
            value=150.0,
            date="2023-01-31",
            central_line=120.0,
            ucl=140.0,
            lcl=100.0,
            slope=2.5,
            trend_signal_detected=True,
        )

        # Act
        result = trend_analysis.to_dict()

        # Assert
        assert result["value"] == 150.0
        assert result["date"] == "2023-01-31"
        assert result["central_line"] == 120.0
        assert result["ucl"] == 140.0
        assert result["lcl"] == 100.0
        assert result["slope"] == 2.5
        assert result["trend_signal_detected"] is True


class TestHistoricalPerformance:
    """Tests for the HistoricalPerformance class."""

    def test_valid_creation(self):
        """Test creating a valid HistoricalPerformance object."""
        # Arrange
        benchmark_comparison = BenchmarkComparison(current_value=110.0, current_period="This Week")
        benchmark = Benchmark(
            reference_value=100.0,
            reference_date=date(2024, 1, 1),
            reference_period="Last Week",
            absolute_change=10.0,
            change_percent=10.0,
        )
        benchmark_comparison.add_benchmark(ComparisonType.LAST_WEEK, benchmark)

        # Act
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
            trend_analysis=[
                TrendAnalysis(
                    value=150.0,
                    date="2023-01-31",
                    central_line=120.0,
                    ucl=140.0,
                    lcl=100.0,
                    slope=2.5,
                    trend_signal_detected=True,
                )
            ],
            high_rank=RankSummary(value=150.0, rank=1, duration_grains=31),
            low_rank=RankSummary(value=100.0, rank=31, duration_grains=31),
            benchmark_comparison=benchmark_comparison,
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
        assert len(performance.trend_analysis) == 1
        assert performance.trend_analysis[0].central_line == 120.0
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
            trend_analysis=[
                TrendAnalysis(
                    value=150.0,
                    date="2023-01-31",
                    central_line=120.0,
                    ucl=140.0,
                    lcl=100.0,
                    slope=2.5,
                    trend_signal_detected=True,
                )
            ],
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
        assert "trend_analysis" in result
        assert len(result["trend_analysis"]) == 1
        assert "high_rank" in result
        assert "low_rank" in result
