"""
Models module for the levers package.

This module contains Pydantic models used throughout the package.
"""

from .common import (
    AnalysisWindow,
    BaseModel,
    BasePattern,
    Granularity,
    GrowthTrend,
)
from .pattern_config import (
    AnalysisWindowConfig,
    DataSource,
    DataSourceType,
    PatternConfig,
    WindowStrategy,
)
from .time_series import (
    AverageGrowth,
    AverageGrowthMethod,
    CumulativeGrowthMethod,
    DataFillMethod,
    PartialInterval,
    TimeSeriesSlope,
    ToDateGrowth,
)
from .trend_analysis import (
    PerformancePlateau,
    RecordHigh,
    RecordLow,
    TrendAnalysis,
)

__all__ = [
    "AnalysisWindow",
    "BaseModel",
    "BasePattern",
    "Granularity",
    "GrowthTrend",
    "DataFillMethod",
    "AverageGrowthMethod",
    "PartialInterval",
    "CumulativeGrowthMethod",
    # Pattern configuration models
    "AnalysisWindowConfig",
    "DataSource",
    "DataSourceType",
    "PatternConfig",
    "WindowStrategy",
    "AverageGrowth",
    "ToDateGrowth",
    "TimeSeriesSlope",
    # Trend analysis models
    "TrendAnalysis",
    "PerformancePlateau",
    "RecordHigh",
    "RecordLow",
]
