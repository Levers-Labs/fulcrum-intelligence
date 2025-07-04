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


class ExecutionStatus(StrEnum):
    """Status of the notification/task execution"""

    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    CRASHED = "CRASHED"


class SnowflakeAuthMethod(StrEnum):
    """Method of authentication for Snowflake."""

    PASSWORD = "PASSWORD"  # noqa
    PRIVATE_KEY = "PRIVATE_KEY"  # noqa
