from enum import Enum

from fastapi import HTTPException
from starlette.responses import JSONResponse
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from commons.clients.base import HttpClientError


class ErrorCode(str, Enum):
    """
    An enumeration of error codes for the story manager.
    """

    NO_METRIC_EXPRESSION = "no_metric_expression"
    UNHANDLED_ERROR = "unhandled_error"
    METRIC_NOT_FOUND = "metric_not_found"
    METRIC_VALUE_NOT_FOUND = "metric_value_not_found"


class StoryManagerError(HTTPException):
    def __init__(self, status_code: int, code: ErrorCode, detail: str):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code


class UnhandledError(StoryManagerError):
    def __init__(
        self, status_code: int = HTTP_500_INTERNAL_SERVER_ERROR, detail: str = "An unexpected error occurred."
    ):
        super().__init__(status_code=status_code, detail=detail, code=ErrorCode.UNHANDLED_ERROR)


def add_exception_handlers(app):
    @app.exception_handler(StoryManagerError)
    async def story_manager_exception_handler(request, exc: StoryManagerError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.code, "detail": exc.detail},
        )

    @app.exception_handler(HttpClientError)
    async def http_client_error_handler(request, exc: HttpClientError):
        if exc.status_code == 404:
            raise StoryManagerError(
                exc.status_code, exc.content.get("error"), str(exc.content.get("detail"))  # type: ignore
            ) from exc
        else:
            raise UnhandledError(status_code=exc.status_code, detail=str(exc.message)) from exc
