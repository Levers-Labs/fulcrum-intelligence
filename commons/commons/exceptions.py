from enum import Enum

from fastapi import HTTPException
from starlette.status import HTTP_404_NOT_FOUND, HTTP_500_INTERNAL_SERVER_ERROR


class ErrorCode(str, Enum):
    """
    An enumeration of error codes.
    """

    INVALID_TENANT = "invalid_tenant"
    UNHANDLED_ERROR = "unhandled_error"


class ServiceError(HTTPException):
    def __init__(self, status_code: int, code: ErrorCode, detail: str):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code


class InvalidTenantError(ServiceError):
    def __init__(self, tenant_id: int | None = None):
        self.tenant_id = tenant_id
        detail = f"Tenant with id '{tenant_id}' not found."
        super().__init__(status_code=HTTP_404_NOT_FOUND, detail=detail, code=ErrorCode.INVALID_TENANT)


class UnhandledError(ServiceError):
    def __init__(
        self, status_code: int = HTTP_500_INTERNAL_SERVER_ERROR, detail: str = "An unexpected error occurred."
    ):
        super().__init__(status_code=status_code, detail=detail, code=ErrorCode.UNHANDLED_ERROR)
