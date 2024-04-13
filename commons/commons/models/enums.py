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
