from typing import Any, TypeAlias

# Type aliases for common types
ErrorDetails: TypeAlias = dict[str, Any]
InvalidFields: TypeAlias = dict[str, Any]
DataDetails: TypeAlias = dict[str, Any]


class LeversError(Exception):
    """Base exception for all levers library errors"""

    def __init__(self, message: str, details: ErrorDetails | None = None) -> None:
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class ValidationError(LeversError):
    """Exception raised when input validation fails"""

    def __init__(self, message: str, invalid_fields: InvalidFields | None = None) -> None:
        details = {"invalid_fields": invalid_fields or {}}
        super().__init__(message, details)
        self.invalid_fields = invalid_fields or {}


class DataError(LeversError):
    """Exception raised for data-related issues"""

    def __init__(self, message: str, data_details: DataDetails | None = None) -> None:
        details = {"data_details": data_details or {}}
        super().__init__(message, details)
        self.data_details = data_details or {}


class MissingDataError(DataError):
    """Exception raised when required data is missing"""

    def __init__(self, message: str, missing_fields: list[str]) -> None:
        data_details = {"missing_fields": missing_fields}
        super().__init__(message, data_details)
        self.missing_fields = missing_fields


class InvalidDataError(DataError):
    """Exception raised when data is invalid or incompatible"""

    pass


class TimeSeriesError(DataError):
    """Exception raised for time series-specific issues"""

    pass


class TimeRangeError(TimeSeriesError):
    """Exception raised when time range is invalid or contains no data"""

    def __init__(self, message: str, start_date: Any, end_date: Any) -> None:
        data_details = {"start_date": str(start_date), "end_date": str(end_date)}
        super().__init__(message, data_details)
        self.start_date = start_date
        self.end_date = end_date


class CalculationError(LeversError):
    """Exception raised when a calculation fails"""

    pass


class DivisionByZeroError(CalculationError):
    """Exception raised for division by zero errors"""

    pass


class PatternError(LeversError):
    """Exception raised for pattern-specific errors"""

    def __init__(self, message: str, pattern_name: str, details: ErrorDetails | None = None):
        pattern_details = {"pattern_name": pattern_name, **(details or {})}
        super().__init__(message, pattern_details)
        self.pattern_name = pattern_name


class PrimitiveError(LeversError):
    """Exception raised for primitive-specific errors"""

    def __init__(self, message: str, primitive_name: str, details: ErrorDetails | None = None):
        primitive_details = {"primitive_name": primitive_name, **(details or {})}
        super().__init__(message, primitive_details)
        self.primitive_name = primitive_name
