"""
Pattern-specific output models.

This module directly exposes all pattern output models for easy import.
"""

# Historical performance models
from .historical_performance import (
    BenchmarkComparison,
    GrowthStats,
    HistoricalPerformance,
    PeriodMetrics,
    RankSummary,
    Seasonality,
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

# Add other patterns here

__all__ = [
    # Performance status
    "StatusChange",
    "Streak",
    "HoldSteady",
    "MetricPerformance",
    # Historical performance
    "BenchmarkComparison",
    "HistoricalPerformance",
    "PeriodMetrics",
    "RankSummary",
    "Seasonality",
    "TrendException",
    "TrendInfo",
    "GrowthStats",
]
