from levers.models import BaseModel, TrendType


class TrendAnalysis(BaseModel):
    """Trend analysis for a specific period."""

    trend_type: TrendType
    trend_slope: float | None = None
    trend_confidence: float | None = None
    normalized_slope: float | None = None
    recent_trend_type: TrendType | None = None
    is_accelerating: bool = False
    is_plateaued: bool = False


class PerformancePlateau(BaseModel):
    """
    Analysis of whether a metric has plateaued (stabilized) over a period.

    Attributes:
        is_plateaued: Whether the metric is currently in a plateau state
        plateau_duration: Number of consecutive periods the metric has been plateaued
        stability_score: Score between 0-1 indicating how stable the metric is (1 = most stable)
        mean_value: Average value during the plateau period
    """

    is_plateaued: bool = False
    plateau_duration: int = 0
    stability_score: float = 0.0
    mean_value: float | None = None


class RecordValueBase(BaseModel):
    """Base model for record value analysis."""

    current_value: float
    rank: int
    periods_compared: int
    absolute_delta: float
    percentage_delta: float


class RecordHigh(RecordValueBase):
    """Record high value analysis."""

    is_record_high: bool = False
    prior_max: float
    prior_max_index: int


class RecordLow(RecordValueBase):
    """Record low value analysis."""

    is_record_low: bool = False
    prior_min: float
    prior_min_index: int
