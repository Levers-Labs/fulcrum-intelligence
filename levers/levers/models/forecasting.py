"""
Forecasting Models

Pydantic models for forecasting pattern outputs.
"""

from pydantic import BaseModel

from levers.models.enums import MetricGVAStatus, PeriodType


class ForecastVsTargetStats(BaseModel):
    """Model for Forecast vs Target stats."""

    period: PeriodType | None = None
    forecasted_value: float | None = None
    target_value: float | None = None
    gap_percent: float | None = None
    status: MetricGVAStatus | None = None


class PacingProjection(BaseModel):
    """Model for pacing projection information."""

    period: PeriodType | None = None
    period_elapsed_percent: float | None = None
    cumulative_value: float | None = None
    projected_value: float | None = None
    target_value: float | None = None
    gap_percent: float | None = None
    status: str | None = None


class RequiredPerformance(BaseModel):
    """Model for required performance information."""

    period: PeriodType | None = None
    remaining_periods: int | None = None
    required_pop_growth_percent: float | None = None
    previous_pop_growth_percent: float | None = None
    growth_difference: float | None = None
    previous_periods: int | None = None


class Forecast(BaseModel):
    """Model for forecast information."""

    date: str
    forecasted_value: float | None = None
    lower_bound: float | None = None
    upper_bound: float | None = None


class ForecastWindow(BaseModel):
    """Model for forecast window information."""

    start_date: str
    end_date: str
    num_periods: int
