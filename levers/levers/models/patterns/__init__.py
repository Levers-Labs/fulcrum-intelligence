"""
Pattern-specific output models.

This module directly exposes all pattern output models for easy import.
"""

# Historical performance models
from .historical_performance import (
    AccelerationRate,
    AnomalyDetectionMethod,
    BenchmarkComparison,
    HistoricalPerformance,
    PeriodGrowthRate,
    RecordValue,
    Seasonality,
    TrendException,
    TrendExceptionType,
    TrendType,
)

# Performance status models
from .performance_status import (
    HoldSteady,
    MetricGVAStatus,
    MetricPerformance,
    SmoothingMethod,
    StatusChange,
    Streak,
)

# Add other patterns here

__all__ = [
    # Performance status
    "MetricGVAStatus",
    "SmoothingMethod",
    "StatusChange",
    "Streak",
    "HoldSteady",
    "MetricPerformance",
    # Historical performance
    "AccelerationRate",
    "BenchmarkComparison",
    "HistoricalPerformance",
    "PeriodGrowthRate",
    "RecordValue",
    "Seasonality",
    "TrendException",
    "TrendExceptionType",
    "TrendType",
    "AnomalyDetectionMethod",
]
