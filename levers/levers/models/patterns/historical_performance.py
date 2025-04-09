"""
Historical Performance Pattern Models

This module defines the Pydantic models for historical performance analysis output.
These models define the structure of the pattern output when analyzing
a metric's historical performance over time.
"""

from datetime import datetime
from enum import Enum

from pydantic import Field

from levers.models.common import BaseModel, BasePattern


class TrendExceptionType(str, Enum):
    """Type of trend exception."""

    SPIKE = "Spike"
    DROP = "Drop"


class AnomalyDetectionMethod(str, Enum):
    """Method of anomaly detection."""

    VARIANCE = "variance"
    SPC = "spc"
    COMBINED = "combined"


class TrendType(str, Enum):
    """Classification of trend direction."""

    STABLE = "stable"
    UPWARD = "upward"
    DOWNWARD = "downward"
    PLATEAU = "plateau"


class PeriodMetrics(BaseModel):
    """Growth and acceleration metrics for a specific period."""

    period_start: str
    period_end: str
    pop_growth_percent: float | None = None
    pop_acceleration_percent: float | None = None


class GrowthStats(BaseModel):
    """Growth statistics for a specific period."""

    current_pop_growth: float | None = None
    average_pop_growth: float | None = None
    current_growth_acceleration: float | None = None
    num_periods_accelerating: int = 0
    num_periods_slowing: int = 0


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


class BenchmarkComparison(BaseModel):
    """Benchmark comparison information."""

    reference_period: str
    absolute_change: float
    change_percent: float | None = None


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
    z_score: float | None = None


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

    # Value ranking summaries
    high_rank: RankSummary
    low_rank: RankSummary

    # Seasonal analysis
    seasonality: Seasonality | None = None

    # Benchmark comparisons
    benchmark_comparisons: list[BenchmarkComparison] = Field(default_factory=list)

    # Trend exceptions (spike/drop)
    trend_exceptions: list[TrendException] = []
