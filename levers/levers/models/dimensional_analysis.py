"""
Models for dimensional analysis outputs.
"""

from pydantic import Field

from levers.models.common import BaseModel as LeversBaseModel


class BaseSlice(LeversBaseModel):
    """Base model for representing dimension slice data"""

    slice_value: str = Field(..., description="Value of the slice/dimension")


class SliceMetric(BaseSlice):
    """Model for representing a dimension slice's metric value"""

    dimension: str = Field(..., description="Dimension name")
    metric_value: float = Field(..., description="Metric value")


class SliceRanking(SliceMetric):
    """Model for representing a slice's performance ranking"""

    rank: int = Field(..., description="Rank by performance")
    avg_other_slices_value: float | None = Field(None, description="Average value of other slices")
    absolute_diff_from_avg: float | None = Field(None, description="Difference from average of other slices")
    absolute_diff_percent_from_avg: float | None = Field(
        None, description="Percentage difference from average of other slices"
    )


class SlicePerformance(BaseSlice):
    """Model for representing a dimension slice's performance metrics"""

    current_value: float = Field(..., description="Current period value")
    prior_value: float = Field(..., description="Prior period value")
    absolute_change: float = Field(..., description="Absolute difference (current - prior)")
    relative_change_percent: float | None = Field(None, description="Percentage change relative to prior value")
    current_share_of_volume_percent: float | None = Field(None, description="Current period's share percentage")
    prior_share_of_volume_percent: float | None = Field(None, description="Prior period's share percentage")
    share_of_volume_change_percent: float | None = Field(None, description="Change in share percentage")
    absolute_marginal_impact: float | None = Field(None, description="Contribution to overall change")
    relative_marginal_impact_percent: float | None = Field(
        None, description="Percentage contribution to overall change"
    )
    avg_other_slices_value: float | None = Field(None, description="Average value of other slices")
    absolute_diff_from_avg: float | None = Field(None, description="Difference from average of other slices")
    absolute_diff_percent_from_avg: float | None = Field(
        None, description="Percentage difference from average of other slices"
    )
    consecutive_above_avg_streak: int | None = Field(None, description="Consecutive periods above average")
    rank_by_performance: int | None = Field(None, description="Rank by performance value")
    rank_by_share: int | None = Field(None, description="Rank by share percentage")


class SliceShare(BaseSlice):
    """Model for representing a slice's share metrics"""

    current_share_of_volume_percent: float = Field(..., description="Current period's share percentage")
    previous_slice_value: str | None = Field(None, description="Value of the comparable slice in the prior period")
    previous_share_percent: float | None = Field(
        None, description="Share percentage of the comparable slice in the prior period"
    )


class SliceStrength(BaseSlice):
    """Model for representing a slice's strength change metrics"""

    previous_slice_value: str = Field(..., description="Value of the comparable slice in the prior period")
    current_value: float = Field(..., description="Current period value")
    prior_value: float = Field(..., description="Prior period value")
    absolute_delta: float = Field(..., description="Absolute change")
    relative_delta_percent: float | None = Field(None, description="Percentage change")


class SliceComparison(LeversBaseModel):
    """Model for comparing two slices"""

    slice_a: str = Field(..., description="First slice value")
    current_value_a: float = Field(..., description="Current value of first slice")
    prior_value_a: float = Field(..., description="Prior value of first slice")
    slice_b: str = Field(..., description="Second slice value")
    current_value_b: float = Field(..., description="Current value of second slice")
    prior_value_b: float = Field(..., description="Prior value of second slice")
    performance_gap_percent: float | None = Field(None, description="Current performance gap percentage")
    gap_change_percent: float | None = Field(None, description="Change in performance gap")


class TopSlice(SliceMetric):
    """Model for representing a top-performing slice in a period"""


class HistoricalPeriodRanking(LeversBaseModel):
    """Model for historical period rankings"""

    start_date: str = Field(..., description="Start date of the period")
    end_date: str = Field(..., description="End date of the period")
    top_slices_by_performance: list[TopSlice] = Field(..., description="Top slices in the period")


class HistoricalSliceRankings(LeversBaseModel):
    """Model for historical slice rankings across periods"""

    periods_analyzed: int = Field(..., description="Number of periods analyzed")
    period_rankings: list[HistoricalPeriodRanking] = Field(..., description="Rankings by period")
