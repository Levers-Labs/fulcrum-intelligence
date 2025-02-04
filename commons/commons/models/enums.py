from enum import Enum


class StrEnum(str, Enum):
    """
    A string Enum.
    """


class Granularity(StrEnum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class GranularityOrder(Enum):
    DAY = 1
    WEEK = 2
    MONTH = 3
    QUARTER = 4
    YEAR = 5


class GranularityLabel(StrEnum):
    DAY = "Daily"
    WEEK = "Weekly"
    MONTH = "Monthly"
