from enum import Enum

from fastapi import HTTPException
from starlette.status import HTTP_404_NOT_FOUND


class ErrorCode(str, Enum):
    """
    An enumeration of error codes.
    """

    INVALID_TENANT = "invalid_tenant"


class ServiceError(HTTPException):
    def __init__(self, status_code: int, code: ErrorCode, detail: str):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code


class InvalidTenantError(ServiceError):
    def __init__(self, tenant_id: int | None = None):
        self.tenant_id = tenant_id
        detail = f"Tenant with id '{tenant_id}' not found."
        super().__init__(status_code=HTTP_404_NOT_FOUND, detail=detail, code=ErrorCode.INVALID_TENANT)
