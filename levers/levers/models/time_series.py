from levers.models import BaseModel


class AverageGrowth(BaseModel):
    """Average growth for a specific period."""

    average_growth: float | None = None
    total_growth: float | None = None
    periods: int


class TimeSeriesSlope(BaseModel):
    """Time series slope analysis results."""

    slope: float | None
    intercept: float | None
    r_value: float | None
    p_value: float | None
    std_err: float | None
    slope_per_day: float | None
    slope_per_week: float | None
    slope_per_month: float | None
    slope_per_year: float | None
