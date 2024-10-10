from enum import Enum

from fastapi import HTTPException
from starlette.responses import JSONResponse
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY


class ErrorCode(str, Enum):
    """
    An enumeration of error codes for the analysis manager.
    """

    NO_METRIC_EXPRESSION = "no_metric_expression"


class AnalysisManagerError(HTTPException):
    def __init__(self, status_code: int, code: ErrorCode, detail: str):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code


class NoMetricExpressionError(AnalysisManagerError):
    def __init__(self, metric_id: str):
        detail = f"No metric expression found for metric_id: {metric_id}. Components do not exist."
        super().__init__(status_code=HTTP_422_UNPROCESSABLE_ENTITY, code=ErrorCode.NO_METRIC_EXPRESSION, detail=detail)


def add_exception_handlers(app):
    @app.exception_handler(AnalysisManagerError)
    async def analysis_manager_exception_handler(request, exc: AnalysisManagerError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.code, "detail": exc.detail},
        )
