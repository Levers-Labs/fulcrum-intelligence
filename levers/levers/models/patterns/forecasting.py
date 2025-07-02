from levers.models import (
    BasePattern,
    Forecast,
    ForecastVsTargetStats,
    PacingProjection,
    RequiredPerformance,
)
from levers.models.forecasting import ForecastWindow


class Forecasting(BasePattern):
    """Output model for the Forecasting pattern."""

    pattern: str = "forecasting"

    # Forecast results
    forecast_window: ForecastWindow | None = None
    forecast_vs_target_stats: list[ForecastVsTargetStats | None] = []
    pacing: list[PacingProjection | None] = []
    required_performance: list[RequiredPerformance | None] = []
    forecast: list[Forecast] = []
