from enum import Enum

from fastapi import HTTPException, Request
from starlette.responses import JSONResponse
from starlette.status import HTTP_404_NOT_FOUND


class ErrorCode(str, Enum):
    """
    An enumeration of error codes.
    """

    METRIC_VALUE_NOT_FOUND = "metric_value_not_found"
    METRIC_NOT_FOUND = "metric_not_found"
    DIMENSION_NOT_FOUND = "dimension_not_found"
    METRIC_METADATA_ERROR = "metric_metadata_error"
    METRIC_TARGET_ERROR = "metric_target_error"


class QueryManagerError(HTTPException):
    def __init__(self, status_code: int, code: ErrorCode, detail: str):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code


class MetricValueNotFoundError(QueryManagerError):
    def __init__(self, metric_id: str):
        self.metric_id = metric_id
        detail = f"Value for metric '{metric_id}' not found."
        super().__init__(status_code=HTTP_404_NOT_FOUND, detail=detail, code=ErrorCode.METRIC_VALUE_NOT_FOUND)


class MalformedMetricMetadataError(QueryManagerError):
    def __init__(self, metric_id: str):
        self.metric_id = metric_id
        detail = f"Malformed metadata for metric '{metric_id}', could not fetch metric values."
        super().__init__(status_code=HTTP_404_NOT_FOUND, detail=detail, code=ErrorCode.METRIC_METADATA_ERROR)


class MetricNotFoundError(QueryManagerError):
    def __init__(self, metric_id: str):
        self.metric_id = metric_id
        detail = f"Metric '{metric_id}' not found."
        super().__init__(status_code=HTTP_404_NOT_FOUND, detail=detail, code=ErrorCode.METRIC_NOT_FOUND)


class DimensionNotFoundError(QueryManagerError):
    def __init__(self, dimension_id: str):
        self.dimension_id = dimension_id
        detail = f"Dimension '{dimension_id}' not found."
        super().__init__(status_code=HTTP_404_NOT_FOUND, detail=detail, code=ErrorCode.DIMENSION_NOT_FOUND)


def add_exception_handlers(app):
    @app.exception_handler(QueryManagerError)
    async def query_manager_exception_handler(request: Request, exc: QueryManagerError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.code, "detail": exc.detail},
        )
