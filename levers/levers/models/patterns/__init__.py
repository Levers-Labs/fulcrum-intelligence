"""
Pattern model classes for various analysis patterns.

These are the output models for the pattern implementations
in the levers.patterns module.
"""

# Dimension analysis
from .dimension_analysis import DimensionAnalysis

# Forecasting models
from .forecasting import Forecasting

# Historical performance models
from .historical_performance import (
    Benchmark,
    BenchmarkComparison,
    GrowthStats,
    HistoricalPerformance,
    PerformanceTrend,
    PeriodMetrics,
    RankSummary,
    Seasonality,
    TrendAnalysis,
    TrendException,
    TrendInfo,
)

# Performance status models
from .performance_status import (
    HoldSteady,
    MetricPerformance,
    StatusChange,
    Streak,
)

__all__ = [
    # Performance status
    "StatusChange",
    "Streak",
    "HoldSteady",
    "MetricPerformance",
    # Historical performance
    "Benchmark",
    "BenchmarkComparison",
    "HistoricalPerformance",
    "PerformanceTrend",
    "PeriodMetrics",
    "RankSummary",
    "Seasonality",
    "TrendAnalysis",
    "TrendException",
    "TrendInfo",
    "GrowthStats",
    # Dimension Analysis
    "DimensionAnalysis",
    # Forecasting
    "Forecasting",
]
