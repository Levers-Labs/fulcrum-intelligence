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
