"""
Models for dimension analysis pattern.
"""

from typing import Any

from pydantic import Field

from levers.models import BasePattern, enums
from levers.models.common import BaseModel


class DimensionAnalysisResult(BasePattern):
    """
    Model representing a dimension analysis result.

    This model contains the results of analyzing a metric across
    different dimension slices, comparing current vs. prior periods.
    """

    pattern_name: str = "dimension_analysis"
    schema_version: str = "1.0.0"

    metric_id: str = Field(..., description="ID of the analyzed metric")
    grain: enums.Granularity = Field(..., description="Time grain of the analysis")
    analysis_date: str = Field(..., description="Date of analysis")
    dimension_name: str = Field(..., description="Name of the dimension analyzed")
    evaluation_time: str = Field(..., description="Time when the analysis was run")

    slices: list[dict[str, Any]] = Field(..., description="List of slice data with metrics for all dimension values")

    top_slices_by_performance: list[dict[str, Any]] = Field(
        ..., description="Top performing slices ranked by current value"
    )
    bottom_slices_by_performance: list[dict[str, Any]] = Field(
        ..., description="Bottom performing slices ranked by current value"
    )

    largest_slice: dict[str, Any] = Field(..., description="Information about the slice with largest share")
    smallest_slice: dict[str, Any] = Field(..., description="Information about the slice with smallest share")

    new_strongest_slice: dict[str, Any] = Field(
        {}, description="Information about new strongest slice if changed from prior period"
    )
    new_weakest_slice: dict[str, Any] = Field(
        {}, description="Information about new weakest slice if changed from prior period"
    )

    comparison_highlights: list[dict[str, Any]] = Field(
        [], description="Highlights of interesting slice comparisons and performance gaps"
    )

    historical_slice_rankings: dict[str, Any] = Field(
        ..., description="Historical rankings of slices over multiple periods"
    )

    class Config:
        schema_extra = {
            "example": {
                "pattern_name": "dimension_analysis",
                "schema_version": "1.0.0",
                "metric_id": "revenue",
                "grain": "month",
                "analysis_date": "2023-06-01",
                "evaluation_time": "2023-06-02T10:15:30",
                "dimension_name": "region",
                "slices": [
                    {
                        "slice_value": "North America",
                        "current_value": 2500000,
                        "prior_value": 2200000,
                        "absolute_change": 300000,
                        "relative_change_percent": 13.64,
                        "current_share_of_volume_percent": 42.3,
                        "prior_share_of_volume_percent": 40.1,
                        "share_of_volume_change_percent": 2.2,
                        "absolute_marginal_impact": 300000,
                        "absolute_diff_from_avg": 1200000,
                        "absolute_diff_percent_from_avg": 92.3,
                        "rank_by_performance": 1,
                        "rank_by_share": 1,
                    }
                ],
                "top_slices_by_performance": [
                    {
                        "slice_value": "North America",
                        "metric_value": 2500000,
                        "avg_other_slices_value": 1300000,
                        "absolute_diff_from_avg": 1200000,
                        "absolute_diff_percent_from_avg": 92.3,
                        "rank": 1,
                    }
                ],
                "bottom_slices_by_performance": [],
                "largest_slice": {
                    "slice_value": "North America",
                    "current_share_of_volume_percent": 42.3,
                    "previous_slice_value": "North America",
                    "previous_share_percent": 40.1,
                },
                "smallest_slice": {},
                "new_strongest_slice": {},
                "new_weakest_slice": {},
                "comparison_highlights": [],
                "historical_slice_rankings": {"periods_analyzed": 8, "period_rankings": []},
            }
        }
