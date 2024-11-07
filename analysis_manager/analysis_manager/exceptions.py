from enum import Enum

from fastapi import HTTPException
from starlette.responses import JSONResponse
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY, HTTP_500_INTERNAL_SERVER_ERROR


class ErrorCode(str, Enum):
    """
    An enumeration of error codes for the analysis manager.
    """

    NO_METRIC_EXPRESSION = "no_metric_expression"
    NO_METRIC_VALUES = "no_metric_values"
    COMPLEX_NUMBER_RESULT = "complex_number_result"
    UNHANDLED_ERROR = "unhandled_error"
    METRIC_NOT_FOUND = "metric_not_found"
    METRIC_VALUE_NOT_FOUND = "metric_value_not_found"


class AnalysisManagerError(HTTPException):
    def __init__(self, status_code: int, code: ErrorCode, detail: str):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code


class NoMetricExpressionError(AnalysisManagerError):
    def __init__(self, metric_id: str):
        detail = f"No metric expression found for metric_id: {metric_id}. Components do not exist."
        super().__init__(status_code=HTTP_422_UNPROCESSABLE_ENTITY, code=ErrorCode.NO_METRIC_EXPRESSION, detail=detail)


class MetricNotFoundError(AnalysisManagerError):
    def __init__(self, metric_id: str):
        self.metric_id = metric_id
        detail = f"Metric '{metric_id}' not found."
        super().__init__(status_code=HTTP_404_NOT_FOUND, detail=detail, code=ErrorCode.METRIC_NOT_FOUND)


class MetricValueNotFoundError(AnalysisManagerError):
    def __init__(self, metric_id: str):
        self.metric_id = metric_id
        detail = f"Value for metric '{metric_id}' not found."
        super().__init__(status_code=HTTP_404_NOT_FOUND, detail=detail, code=ErrorCode.METRIC_VALUE_NOT_FOUND)


class ComplexValueError(AnalysisManagerError):
    def __init__(self, metric_id: str):
        detail = f"Calculation for metric_id: {metric_id} cannot be completed due to complex number results."
        super().__init__(status_code=HTTP_422_UNPROCESSABLE_ENTITY, code=ErrorCode.COMPLEX_NUMBER_RESULT, detail=detail)


class UnhandledError(AnalysisManagerError):
    def __init__(
        self, status_code: int = HTTP_500_INTERNAL_SERVER_ERROR, detail: str = "An unexpected error occurred."
    ):
        super().__init__(status_code=status_code, detail=detail, code=ErrorCode.UNHANDLED_ERROR)


def add_exception_handlers(app):
    @app.exception_handler(AnalysisManagerError)
    async def analysis_manager_exception_handler(request, exc: AnalysisManagerError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.code, "detail": exc.detail},
        )
