"""
Models module for the levers package.

This module contains Pydantic models used throughout the package.
"""

from .common import AnalysisWindow, BaseModel, BasePattern
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
)
from .pattern_config import AnalysisWindowConfig, DataSource, PatternConfig
from .time_series import AverageGrowth, TimeSeriesSlope, ToDateGrowth
from .trend_analysis import (
    PerformancePlateau,
    RecordHigh,
    RecordLow,
    TrendAnalysis,
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
    # Common models
    "AnalysisWindow",
    "BaseModel",
    "BasePattern",
    # Pattern configuration models
    "AnalysisWindowConfig",
    "DataSource",
    "PatternConfig",
    "AverageGrowth",
    "ToDateGrowth",
    "TimeSeriesSlope",
    # Trend analysis models
    "TrendAnalysis",
    "PerformancePlateau",
    "RecordHigh",
    "RecordLow",
]
