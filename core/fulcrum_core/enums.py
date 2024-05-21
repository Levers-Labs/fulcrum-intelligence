from enum import Enum


class Granularity(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class TargetAim(str, Enum):
    MAXIMIZE = "increasing"
    MINIMIZE = "decreasing"


class MetricChangeDirection(str, Enum):
    UPWARD = "UPWARD"
    DOWNWARD = "DOWNWARD"
    UNCHANGED = "UNCHANGED"


class AggregationMethod(str, Enum):
    DISTINCT = "unique"
    SUM = "sum"
    COUNT = "count"
