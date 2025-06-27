"""
Forecasting Models

Pydantic models for forecasting pattern outputs.
"""

from pydantic import BaseModel

from levers.models.enums import MetricGVAStatus


class ForecastVsTargetStats(BaseModel):
    """Model for Forecast vs Target stats."""

    forecasted_value: float | None = None
    lower_bound: float | None = None
    upper_bound: float | None = None
    confidence_level: float | None = None
    target_date: str | None = None
    target_value: float | None = None
    forecasted_gap_percent: float | None = None
    forecast_status: MetricGVAStatus | None = None


class PacingProjection(BaseModel):
    """Model for pacing projection information."""

    percent_of_period_elapsed: float | None = None
    cumulative_value: float | None = None
    projected_value: float | None = None
    gap_percent: float | None = None
    status: str | None = None


class RequiredPerformance(BaseModel):
    """Model for required performance information."""

    remaining_periods_count: int | None = None
    required_pop_growth_percent: float | None = None
    past_pop_growth_percent: float | None = None
    delta_from_historical_growth: float | None = None


class Forecast(BaseModel):
    """Model for forecast information."""

    date: str
    forecasted_value: float | None = None
    lower_bound: float | None = None
    upper_bound: float | None = None
    confidence_level: float | None = None
