"""
Forecasting Models

Pydantic models for forecasting pattern outputs.
"""

from pydantic import BaseModel

from levers.models.enums import MetricGVAStatus, PeriodType


class ForecastVsTargetStats(BaseModel):
    """Model for Forecast vs Target stats."""

    # The period type (e.g. END_OF_MONTH, END_OF_QUARTER)
    period: PeriodType | None = None
    # The forecasted value for the period
    forecasted_value: float | None = None
    # The target value to compare against
    target_value: float | None = None
    # The percentage gap between forecast and target
    gap_percent: float | None = None
    # The status indicating if forecast is on/off track vs target
    status: MetricGVAStatus | None = None


class PacingProjection(BaseModel):
    """Model for pacing projection information."""

    # The period type (e.g. END_OF_MONTH, END_OF_QUARTER)
    period: PeriodType | None = None
    # Percentage of the period that has elapsed
    period_elapsed_percent: float | None = None
    # Projected final value based on current pace
    projected_value: float | None = None
    # Target value to compare against
    target_value: float | None = None
    # Percentage gap between projection and target
    gap_percent: float | None = None
    # Status indicating if pacing is on/off track
    status: MetricGVAStatus | None = None


class RequiredPerformance(BaseModel):
    """Model for required performance information."""

    # The period type (e.g. END_OF_MONTH, END_OF_QUARTER)
    period: PeriodType | None = None
    # Number of periods remaining until target date
    remaining_periods: int | None = None
    # Required growth rate to hit target
    required_pop_growth_percent: float | None = None
    # Historical growth rate from previous periods
    previous_pop_growth_percent: float | None = None
    # Difference between required and previous growth rates
    growth_difference: float | None = None
    # Number of previous periods used for growth calculation
    previous_periods: int | None = None


class Forecast(BaseModel):
    """Model for forecast information."""

    # The date of the forecast
    date: str
    # The forecasted value
    forecasted_value: float | None = None
    # Lower bound of forecast confidence interval
    lower_bound: float | None = None
    # Upper bound of forecast confidence interval
    upper_bound: float | None = None
    # Confidence level for the bounds (e.g. 0.95 for 95%)
    confidence_level: float | None = None


class ForecastWindow(BaseModel):
    """Model for forecast window information."""

    # Start date of the forecast window
    start_date: str
    # End date of the forecast window
    end_date: str
    # Number of periods in the forecast window
    num_periods: int
