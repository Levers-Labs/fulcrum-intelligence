from levers.models import (
    BasePattern,
    DailyForecast,
    PacingProjection,
    RequiredPerformance,
    StatisticalForecast,
)


class Forecasting(BasePattern):
    """Output model for the Forecasting pattern."""

    pattern: str = "forecasting"

    period_name: str
    # Forecast results
    statistical: StatisticalForecast | None = None
    pacing: PacingProjection | None = None
    required_performance: RequiredPerformance | None = None
    daily_forecast: list[DailyForecast] = []
