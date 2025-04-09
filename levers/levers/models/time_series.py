from enum import Enum


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
