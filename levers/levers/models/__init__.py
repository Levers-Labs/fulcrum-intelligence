"""
Models module for the levers package.

This module contains Pydantic models used throughout the package.
"""

from .enums import (
    AnomalyDetectionMethod,
    AverageGrowthMethod,
    CumulativeGrowthMethod,
    DataFillMethod,
    DataSourceType,
    Granularity,
    GrowthTrend,
    MetricGVAStatus,
    PartialInterval,
    SmoothingMethod,
    TrendExceptionType,
    TrendType,
    WindowStrategy,
    ConcentrationMethod,
    ComparisonType,
    PeriodType,
)
from .common import AnalysisWindow, BaseModel, BasePattern
from .pattern_config import AnalysisWindowConfig, DataSource, PatternConfig
from .time_series import AverageGrowth, TimeSeriesSlope
from .trend_analysis import (
    PerformancePlateau,
    RecordHigh,
    RecordLow,
    TrendAnalysis,
)
from .dimensional_analysis import (
    BaseSlice,
    SliceMetric,
    SlicePerformance,
    SliceRanking,
    SliceShare,
    SliceStrength,
    SliceComparison,
    TopSlice,
    HistoricalPeriodRanking,
)
from .forecasting import (
    ForecastVsTargetStats,
    PacingProjection,
    RequiredPerformance,
    Forecast,
    ForecastWindow,
)


__all__ = [
    # Enums
    "Granularity",
    "GrowthTrend",
    "AnomalyDetectionMethod",
    "MetricGVAStatus",
    "SmoothingMethod",
    "TrendExceptionType",
    "TrendType",
    "DataFillMethod",
    "AverageGrowthMethod",
    "PartialInterval",
    "CumulativeGrowthMethod",
    "DataSourceType",
    "WindowStrategy",
    "ConcentrationMethod",
    "ComparisonType",
    "PeriodType",
    # Common models
    "AnalysisWindow",
    "BaseModel",
    "BasePattern",
    # Pattern configuration models
    "AnalysisWindowConfig",
    "DataSource",
    "PatternConfig",
    "AverageGrowth",
    "TimeSeriesSlope",
    # Trend analysis models
    "TrendAnalysis",
    "PerformancePlateau",
    "RecordHigh",
    "RecordLow",
    # Dimensional analysis models
    "BaseSlice",
    "SliceMetric",
    "SlicePerformance",
    "SliceRanking",
    "SliceShare",
    "SliceStrength",
    "SliceComparison",
    "TopSlice",
    "HistoricalPeriodRanking",
    # Forecasting models
    "ForecastVsTargetStats",
    "PacingProjection",
    "RequiredPerformance",
    "Forecast",
    "ForecastWindow",
]
