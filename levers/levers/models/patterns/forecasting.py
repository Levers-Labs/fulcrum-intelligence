from levers.models import (
    BasePattern,
    Forecast,
    ForecastVsTargetStats,
    PacingProjection,
    RequiredPerformance,
)
from levers.models.enums import Granularity
from levers.models.forecasting import ForecastWindow


class Forecasting(BasePattern):
    """Output model for the Forecasting pattern."""

    pattern: str = "forecasting"

    forecast_period_grain: Granularity
    # Forecast results
    forecast_window: ForecastWindow | None = None
    forecast_vs_target_stats: ForecastVsTargetStats | None = None
    pacing: PacingProjection | None = None
    required_performance: RequiredPerformance | None = None
    period_forecast: list[Forecast] | None = None
