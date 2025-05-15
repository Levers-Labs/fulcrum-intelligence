"""
Models for dimensional analysis outputs.
"""

from levers.models.common import BaseModel


class BaseSlice(BaseModel):
    """Base model for representing dimension slice data"""

    slice_value: str


class SliceMetric(BaseSlice):
    """Model for representing a dimension slice's metric value"""

    dimension: str
    metric_value: float


class SliceRanking(SliceMetric):
    """Model for representing a slice's performance ranking"""

    rank: int
    avg_other_slices_value: float | None = None
    absolute_diff_from_avg: float | None = None
    absolute_diff_percent_from_avg: float | None = None


class SlicePerformance(BaseSlice):
    """Model for representing a dimension slice's performance"""

    current_value: float
    prior_value: float
    absolute_change: float | None = None
    relative_change_percent: float | None = None
    current_share_of_volume_percent: float | None = None
    prior_share_of_volume_percent: float | None = None
    share_of_volume_change_percent: float | None = None
    absolute_marginal_impact: float | None = None
    relative_marginal_impact_percent: float | None = None
    avg_other_slices_value: float | None = None
    absolute_diff_from_avg: float | None = None
    absolute_diff_percent_from_avg: float | None = None
    consecutive_above_avg_streak: int | None = None
    rank_by_performance: int | None = None
    rank_by_share: int | None = None


class SliceShare(BaseSlice):
    """Model for representing a slice's share metrics"""

    current_share_of_volume_percent: float
    previous_slice_value: str | None = None
    previous_share_percent: float | None = None


class SliceStrength(BaseSlice):
    """Model for representing a slice's strength change metrics"""

    previous_slice_value: str
    current_value: float
    prior_value: float
    absolute_delta: float
    relative_delta_percent: float | None = None


class SliceComparison(BaseModel):
    """Model for comparing two slices"""

    slice_a: str
    current_value_a: float
    prior_value_a: float
    slice_b: str
    current_value_b: float
    prior_value_b: float
    performance_gap_percent: float | None = None
    gap_change_percent: float | None = None


class TopSlice(SliceMetric):
    """Model for representing a top-performing slice in a period"""


class HistoricalPeriodRanking(BaseModel):
    """Model for historical period rankings"""

    start_date: str
    end_date: str
    top_slices_by_performance: list[TopSlice]
