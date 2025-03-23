"""Performance status pattern models."""

from sqlalchemy import Enum
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, Field

from analysis_manager.patterns.models.base import BasePatternResult
from levers.models.patterns.performance_status import (
    HoldSteady,
    MetricGVAStatus,
    MetricPerformance,
    StatusChange,
    Streak,
)


class PerformanceStatus(BasePatternResult[MetricPerformance], table=True):  # type: ignore
    """Database model for performance status pattern results."""

    __tablename__ = "performance_status"

    # Core metrics
    current_value: float
    prior_value: float | None = None
    target_value: float | None = None
    status: MetricGVAStatus = Field(
        sa_column=Column(Enum(MetricGVAStatus, name="metric_gva_status", inherit_schema=True), index=True)
    )

    # Delta calculations
    absolute_delta_from_prior: float | None = None
    pop_change_percent: float | None = None
    # Gap calculations
    # Applicable if status is OFF_TRACK
    absolute_gap: float | None = None
    percent_gap: float | None = None
    # Applicable if status is ON_TRACK
    absolute_over_performance: float | None = None
    percent_over_performance: float | None = None

    # Status change info
    status_change: StatusChange | None = Field(default=None, sa_column=Column(JSONB, nullable=True))

    # Streak info
    streak: Streak | None = Field(default=None, sa_column=Column(JSONB, nullable=True))

    # "Hold steady" scenario
    hold_steady: HoldSteady | None = Field(default=None, sa_column=Column(JSONB, nullable=True))

    @property
    def pattern_model_class(self) -> type[MetricPerformance]:
        """Get the pattern model class."""
        return MetricPerformance
