"""
Historical Performance Pattern Models

This module defines the Pydantic models for historical performance analysis output.
These models define the structure of the pattern output when analyzing
a metric's historical performance over time.
"""

from datetime import date

from pydantic import Field

from levers.models import (
    BaseModel,
    BasePattern,
    ComparisonType,
    TrendExceptionType,
    TrendType,
)


class PeriodMetrics(BaseModel):
    """Growth and acceleration metrics for a specific period."""

    period_start: str  # Start date of the period
    period_end: str  # End date of the period

    # Growth metrics
    pop_growth_percent: float | None = None  # Period-over-period growth
    pop_acceleration_percent: float | None = None  # Growth acceleration


class GrowthStats(BaseModel):
    """Growth statistics for a specific period."""

    current_pop_growth: float | None = None
    average_pop_growth: float | None = None
    current_growth_acceleration: float | None = None
    num_periods_accelerating: int = 0
    num_periods_slowing: int = 0
    overall_growth: float | None = None


class RankSummary(BaseModel):
    """High or low value ranking and comparison information within the historical window."""

    value: float
    rank: int
    duration_grains: int
    prior_record_value: float | None = None
    prior_record_date: str | None = None
    absolute_delta_from_prior_record: float | None = None
    relative_delta_from_prior_record: float | None = None


class Seasonality(BaseModel):
    """Seasonality information."""

    is_following_expected_pattern: bool
    expected_change_percent: float | None = None
    actual_change_percent: float | None = None
    deviation_percent: float | None = None


class Benchmark(BaseModel):
    """Benchmark comparison information."""

    reference_value: float
    # Date against which the current value is compared
    reference_date: date
    # string representation of the reference date
    # e.g. Month Ago, Quarter Ago, Year Ago
    reference_period: str
    # Difference between the current and reference values
    absolute_change: float
    # Percentage change from the reference value to the current value
    change_percent: float | None = None


class BenchmarkComparison(BaseModel):
    """Dynamic benchmark comparison information using ComparisonType enum keys."""

    # Current value of the metric
    current_value: float
    # String representation of the current period
    # e.g. This Week, This Month, This Quarter, This Year
    current_period: str
    # Dictionary of comparisons, keyed by ComparisonType
    benchmarks: dict[ComparisonType, Benchmark] = Field(default_factory=dict)

    def get_benchmark(self, comparison_type: ComparisonType) -> Benchmark | None:
        """Get a specific comparison by type."""
        return self.benchmarks.get(comparison_type)

    def add_benchmark(self, comparison_type: ComparisonType, comparison: Benchmark) -> None:
        """Add a comparison for a specific type."""
        self.benchmarks[comparison_type] = comparison

    def get_all_benchmarks(self) -> dict[ComparisonType, Benchmark]:
        """Get all comparisons."""
        return self.benchmarks

    def has_benchmark(self, comparison_type: ComparisonType) -> bool:
        """Check if a specific comparison type exists."""
        return comparison_type in self.benchmarks


class TrendInfo(BaseModel):
    """Trend information."""

    trend_type: TrendType
    start_date: str
    average_pop_growth: float | None = None
    duration_grains: int


class TrendException(BaseModel):
    """Trend exception (spike or drop) information."""

    type: TrendExceptionType
    current_value: float
    normal_range_low: float
    normal_range_high: float
    absolute_delta_from_normal_range: float | None = None
    magnitude_percent: float | None = None


class TrendAnalysis(BaseModel):
    """Trend analysis information."""

    value: float
    date: str
    central_line: float | None = None
    ucl: float | None = None
    lcl: float | None = None
    slope: float | None = None
    slope_change_percent: float | None = None
    trend_signal_detected: bool = False


class HistoricalPerformance(BasePattern):
    """Historical performance analysis output."""

    pattern: str = "historical_performance"
    # Period metrics (growth and acceleration)
    period_metrics: list[PeriodMetrics] = []

    # Summaries for current vs. average growth
    growth_stats: GrowthStats

    # Trends
    current_trend: TrendInfo | None = None
    previous_trend: TrendInfo | None = None
    trend_analysis: list[TrendAnalysis] = []

    # Value ranking summaries
    high_rank: RankSummary
    low_rank: RankSummary

    # Seasonal analysis
    seasonality: Seasonality | None = None

    # Benchmark comparisons
    benchmark_comparison: BenchmarkComparison | None = None

    # Trend exceptions (spike/drop)
    trend_exception: TrendException | None = None
