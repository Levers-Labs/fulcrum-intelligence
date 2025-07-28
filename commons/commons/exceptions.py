from enum import Enum

from fastapi import HTTPException
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT, HTTP_500_INTERNAL_SERVER_ERROR


class ErrorCode(str, Enum):
    """
    An enumeration of error codes.
    """

    INVALID_TENANT = "invalid_tenant"
    PREFECT_OPERATION_FAILED = "prefect_operation_failed"
    DUPLICATE_RESOURCE = "duplicate_resource"


class ServiceError(HTTPException):
    def __init__(self, status_code: int, code: ErrorCode, detail: str):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code


class InvalidTenantError(ServiceError):
    def __init__(self, tenant_id: int | None = None):
        self.tenant_id = tenant_id
        detail = f"Tenant with id '{tenant_id}' not found."
        super().__init__(status_code=HTTP_404_NOT_FOUND, detail=detail, code=ErrorCode.INVALID_TENANT)


class ConflictError(ServiceError):
    def __init__(self, detail: str):
        super().__init__(status_code=HTTP_409_CONFLICT, detail=detail, code=ErrorCode.DUPLICATE_RESOURCE)


class PrefectOperationError(ServiceError):
    def __init__(self, operation: str, detail: str):
        self.operation = operation
        super().__init__(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Prefect {operation} failed: {detail}",
            code=ErrorCode.PREFECT_OPERATION_FAILED,
        )
