from enum import Enum


class Granularity(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class MetricAim(str, Enum):
    INCREASING = "increasing"
    DECREASING = "decreasing"


class MetricChangeDirection(str, Enum):
    UPWARD = "UPWARD"
    DOWNWARD = "DOWNWARD"
    UNCHANGED = "UNCHANGED"


class AggregationOption(str, Enum):
    DISTINCT = "nunique"
    SUM = "sum"
    COUNT = "count"


class AggregationMethod(str, Enum):
    DISTINCT = "DISTINCT"
    SUM = "SUM"
    COUNT = "COUNT"
