from levers.models import BaseModel, BasePattern, MetricGVAStatus


class StatusChange(BaseModel):
    """Status change information"""

    has_flipped: bool
    old_status: str | None = None
    new_status: str
    old_status_duration_grains: int | None = None


class Streak(BaseModel):
    """Streak information"""

    length: int
    status: MetricGVAStatus
    performance_change_percent_over_streak: float | None = None
    absolute_change_over_streak: float | None = None
    average_change_percent_per_grain: float | None = None
    average_change_absolute_per_grain: float | None = None


class HoldSteady(BaseModel):
    """Hold steady scenario information"""

    is_currently_at_or_above_target: bool
    time_to_maintain_grains: int | None = None
    current_margin_percent: float | None = None


class MetricPerformance(BasePattern):
    """Performance status analysis output"""

    pattern: str = "performance_status"
    # Current vs. prior values
    current_value: float
    prior_value: float | None = None
    absolute_delta_from_prior: float | None = None
    pop_change_percent: float | None = None

    # Target-related fields
    target_value: float | None = None
    status: MetricGVAStatus
    # Applicable if status is OFF_TRACK
    absolute_gap: float | None = None
    percent_gap: float | None = None
    # Applicable if status is ON_TRACK
    absolute_over_performance: float | None = None
    percent_over_performance: float | None = None

    # Status change info
    status_change: StatusChange | None = None

    # Streak info
    streak: Streak | None = None

    # "Hold steady" scenario
    hold_steady: HoldSteady | None = None
