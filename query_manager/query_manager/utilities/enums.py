from enum import Enum


class Granularity(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class OutputFormat(str, Enum):
    JSON = "JSON"
    PARQUET = "Parquet"


class TargetAim(str, Enum):
    MAXIMIZE = "Maximize"
    MINIMIZE = "Minimize"
    BALANCE = "Balance"
