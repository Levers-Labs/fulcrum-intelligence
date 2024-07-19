from enum import Enum


class OutputFormat(str, Enum):
    JSON = "JSON"
    PARQUET = "PARQUET"


class TargetAim(str, Enum):
    MAXIMIZE = "Maximize"
    MINIMIZE = "Minimize"
    BALANCE = "Balance"


class SemanticMemberType(str, Enum):
    MEASURE = "measure"
    DIMENSION = "dimension"


class UnitOfMeasure(str, Enum):
    QUANTITY = "Quantity"
    PERCENT = "Percent"


class Unit(str, Enum):
    QUANTITY = "n"
    PERCENT = "%"


class Complexity(str, Enum):
    ATOMIC = "Atomic"
    COMPLEX = "Complex"
