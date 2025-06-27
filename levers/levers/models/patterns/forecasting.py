from levers.models import (
    BasePattern,
    Forecast,
    ForecastVsTargetStats,
    PacingProjection,
    RequiredPerformance,
)
from levers.models.enums import PeriodType


class Forecasting(BasePattern):
    """Output model for the Forecasting pattern."""

    pattern: str = "forecasting"

    period_type: PeriodType
    # Forecast results
    forecast_vs_target_stats: ForecastVsTargetStats | None = None
    pacing: PacingProjection | None = None
    required_performance: RequiredPerformance | None = None
    period_forecast: list[Forecast] | None = None
