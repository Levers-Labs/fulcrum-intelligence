"""
Models for dimension analysis pattern.
"""

from levers.models import (
    BasePattern,
    HistoricalPeriodRanking,
    SliceComparison,
    SlicePerformance,
    SliceRanking,
    SliceShare,
    SliceStrength,
)


class DimensionAnalysis(BasePattern):
    """
    Model representing a dimension analysis result.

    This model contains the results of analyzing a metric across
    different dimension slices, comparing current vs. prior periods.
    """

    pattern: str = "dimension_analysis"

    dimension_name: str = ""
    slices: list[SlicePerformance] = []
    top_slices: list[SliceRanking] = []
    bottom_slices: list[SliceRanking] = []

    largest_slice: SliceShare | None = None
    smallest_slice: SliceShare | None = None

    strongest_slice: SliceStrength | None = None
    weakest_slice: SliceStrength | None = None

    comparison_highlights: list[SliceComparison] = []

    historical_slice_rankings: list[HistoricalPeriodRanking] = []
