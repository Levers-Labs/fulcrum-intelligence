from enum import Enum


class Granularity(str, Enum):
    """Time grain for analysis"""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class DataSourceType(str, Enum):
    """Types of data sources that patterns can use."""

    METRIC_TIME_SERIES = "metric_time_series"
    METRIC_WITH_TARGETS = "metric_with_targets"
    DIMENSIONAL_TIME_SERIES = "dimensional_time_series"
    MULTI_METRIC = "multi_metric"
    METRIC_TARGETS = "metric_targets"


class WindowStrategy(str, Enum):
    """Strategies for determining the analysis window."""

    FIXED_TIME = "fixed_time"  # Same time window for all grains
    GRAIN_SPECIFIC_TIME = "grain_specific_time"  # Different time windows for different grains
    FIXED_DATAPOINTS = "fixed_datapoints"  # Fixed number of data points


class GrowthTrend(str, Enum):
    """Classification of growth patterns over time"""

    STABLE = "stable"  # Growth rates show minimal variation
    ACCELERATING = "accelerating"  # Growth rates are increasing over time
    DECELERATING = "decelerating"  # Growth rates are decreasing over time
    VOLATILE = "volatile"  # Growth rates show inconsistent patterns


class TrendExceptionType(str, Enum):
    """Type of trend exception."""

    SPIKE = "Spike"
    DROP = "Drop"


class AnomalyDetectionMethod(str, Enum):
    """Method of anomaly detection."""

    VARIANCE = "variance"
    SPC = "spc"
    COMBINED = "combined"


class TrendType(str, Enum):
    """Classification of trend direction."""

    STABLE = "stable"
    UPWARD = "upward"
    DOWNWARD = "downward"
    PLATEAU = "plateau"


class SmoothingMethod(str, Enum):
    """Method for calculating interim targets along a trajectory."""

    LINEAR = "linear"  # Equal changes each period
    FRONT_LOADED = "front_loaded"  # Larger changes earlier, smaller changes later
    BACK_LOADED = "back_loaded"  # Smaller changes earlier, larger changes later


class MetricGVAStatus(str, Enum):
    """Status of a metric's performance against target."""

    ON_TRACK = "on_track"  # Metric is within acceptable threshold of target
    OFF_TRACK = "off_track"  # Metric is outside acceptable threshold of target
    NO_TARGET = "no_target"  # No valid target exists for comparison


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


class ConcentrationMethod(str, Enum):
    """Method to calculate concentration"""

    GINI = "GINI"
    HHI = "HHI"


class ComparisonType(str, Enum):
    """Types of benchmark comparisons."""

    # Week-based comparisons
    LAST_WEEK = "last_week"
    WEEK_IN_LAST_MONTH = "week_in_last_month"
    WEEK_IN_LAST_QUARTER = "week_in_last_quarter"
    WEEK_IN_LAST_YEAR = "week_in_last_year"

    # Month-based comparisons
    LAST_MONTH = "last_month"
    MONTH_IN_LAST_QUARTER = "month_in_last_quarter"
    MONTH_IN_LAST_YEAR = "month_in_last_year"


class PeriodType(str, Enum):
    """Names of periods."""

    END_OF_WEEK = "endOfWeek"
    END_OF_MONTH = "endOfMonth"
    END_OF_QUARTER = "endOfQuarter"
    END_OF_YEAR = "endOfYear"
    END_OF_NEXT_MONTH = "endOfNextMonth"


class ForecastMethod(str, Enum):
    """Methods for forecasting."""

    NAIVE = "naive"
    SES = "ses"
    HOLT_WINTERS = "holtwinters"
    AUTO_ARIMA = "auto_arima"
    PROPHET = "prophet"


class SeriesScope(str, Enum):
    """Scopes of series."""

    FULL_PERIOD = "full_period"
    END_OF_PERIOD = "end_of_period"
