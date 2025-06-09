from enum import Enum


class OutputFormat(str, Enum):
    JSON = "JSON"
    PARQUET = "PARQUET"


class MetricAim(str, Enum):
    MAXIMIZE = "Maximize"
    MINIMIZE = "Minimize"
    BALANCE = "Balance"


class SemanticMemberType(str, Enum):
    MEASURE = "measure"
    DIMENSION = "dimension"


class Complexity(str, Enum):
    ATOMIC = "Atomic"
    COMPLEX = "Complex"


class CubeFilterOperator(str, Enum):
    """
    Enumeration of supported cube filter operators.
    Based on Cube.js and common OLAP operations.
    """

    # Equality operators
    EQUALS = "equals"
    NOT_EQUALS = "notEquals"

    # String operators
    CONTAINS = "contains"
    NOT_CONTAINS = "notContains"
    STARTS_WITH = "startsWith"
    ENDS_WITH = "endsWith"

    # Comparison operators (for numbers and dates)
    GT = "gt"  # greater than
    GTE = "gte"  # greater than or equal
    LT = "lt"  # less than
    LTE = "lte"  # less than or equal

    # Set operators
    IN = "in"
    NOT_IN = "notIn"

    # Range operators
    BETWEEN = "between"
    NOT_BETWEEN = "notBetween"

    # Date-specific operators
    IN_DATE_RANGE = "inDateRange"
    NOT_IN_DATE_RANGE = "notInDateRange"
    BEFORE_DATE = "beforeDate"
    AFTER_DATE = "afterDate"

    # Boolean operators
    IS_TRUE = "isTrue"
    IS_FALSE = "isFalse"

    # Null operators
    SET = "set"  # not null
    NOT_SET = "notSet"  # is null
