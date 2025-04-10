from enum import Enum

from levers.models.common import BaseModel


class DataFillMethod(str, Enum):
    """Method to fill missing data"""

    FORWARD_FILL = "ffill"
    BACKWARD_FILL = "bfill"
    INTERPOLATE = "interpolate"


class AverageGrowthMethod(str, Enum):
    """Method to calculate average growth"""

    ARITHMETIC = "arithmetic"
    CAGR = "cagr"


class PartialInterval(str, Enum):
    """Partial interval for analysis"""

    MTD = "MTD"
    QTD = "QTD"
    YTD = "YTD"
    WTD = "WTD"


class CumulativeGrowthMethod(str, Enum):
    """Method to calculate cumulative growth"""

    INDEX = "index"
    CUMSUM = "cumsum"
    CUMPROD = "cumprod"


class AverageGrowth(BaseModel):
    """Average growth for a specific period."""

    average_growth: float | None = None
    total_growth: float | None = None
    periods: int


class ToDateGrowth(BaseModel):
    """Growth from a specific date to the current date."""

    current_value: float
    prior_value: float
    abs_diff: float
    growth_rate: float | None = None


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
