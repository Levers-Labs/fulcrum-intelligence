import logging
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Security,
)
from sqlalchemy.exc import IntegrityError
from starlette import status

from commons.auth.scopes import (
    ADMIN_READ,
    TENANT_READ,
    USER_READ,
    USER_WRITE,
)
from commons.db.crud import NotFoundError
from commons.models.tenant import TenantConfig
from commons.utilities.context import get_tenant_id, set_tenant_id
from commons.utilities.pagination import PaginationParams
from insights_backend.core.crud import TenantCRUD
from insights_backend.core.dependencies import TenantsCRUDDep, UsersCRUDDep, oauth2_auth
from insights_backend.core.models import (
    TenantList,
    TenantRead,
    User,
    UserCreate,
    UserList,
)
from insights_backend.core.models.users import UserRead, UserUpdate

user_router = APIRouter(prefix="/users", tags=["users"])
router = APIRouter(tags=["tenants"])
logger = logging.getLogger(__name__)


async def handle_tenant_context_from_org_id(org_id: str, tenant_crud_client: TenantCRUD):
    # Get tenant id using tenant org id
    tenant = await tenant_crud_client.get_tenant_by_external_id(org_id)
    if not tenant:
        raise HTTPException(status_code=422, detail=f"Tenant not found for org id: {org_id}")
    # Set tenant id in context
    set_tenant_id(tenant.id)


@user_router.post(
    "/",
    response_model=UserRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],  # type: ignore
)
async def create_user(user: UserCreate, user_crud_client: UsersCRUDDep, tenant_crud_client: TenantsCRUDDep):
    """
    To create a new user in DB, this endpoint will be used by Auth0 for user registration.
    """
    # Handle tenant context from org id
    await handle_tenant_context_from_org_id(user.tenant_org_id, tenant_crud_client)
    try:
        return await user_crud_client.create(obj_in=user)
    except IntegrityError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e.orig)) from e


@user_router.get(
    "/{user_id}",
    response_model=UserRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[])],  # type: ignore
)
async def get_user(user_id: int, user_crud_client: UsersCRUDDep):
    """
    Retrieve a user by ID.
    """
    try:
        user = await user_crud_client.get(user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="User not found") from e
    return user


@user_router.get(
    "/user-by-email/{email}",
    response_model=UserRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],  # type: ignore
)
async def get_user_by_email(email: str, user_crud_client: UsersCRUDDep):
    """
    Retrieve a user by email.
    """
    db_user = await user_crud_client.get_user_by_email(email)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user


@user_router.put(
    "/{user_id}",
    response_model=UserRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],  # type: ignore
)
async def update_user(user_id: int, user: UserUpdate, user_crud_client: UsersCRUDDep):
    """
    Update a user by ID.
    """
    try:
        old_user_obj: User = await user_crud_client.get(user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="User not found") from e

    return await user_crud_client.update(obj=old_user_obj, obj_in=user)


@user_router.get(
    "/",
    response_model=UserList,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],  # type: ignore
)
async def list_users(
    user_crud_client: UsersCRUDDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
) -> UserList:
    """
    Retrieve all the users in DB.
    """
    count = await user_crud_client.total_count()
    results: list[UserRead] = [UserRead.from_orm(user) for user in await user_crud_client.list_results(params=params)]
    return UserList(results=results, count=count)


@user_router.delete("/{user_id}", dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])])  # type: ignore
async def delete_user(user_id: int, user_crud_client: UsersCRUDDep):
    """
    Retrieve a user by ID.
    """
    try:
        await user_crud_client.delete(id=user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="User not found") from e


# Tenant Routes
@router.get(
    "/tenants/all",
    response_model=TenantList,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_READ])],  # type: ignore
)
async def list_tenants(
    tenant_crud_client: TenantsCRUDDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
):
    """
    Retrieve all tenants in DB.
    """
    count = await tenant_crud_client.total_count()
    results: list[TenantRead] = [
        TenantRead.model_validate(tenant) for tenant in await tenant_crud_client.list_results(params=params)
    ]
    return TenantList(results=results, count=count)


@router.get(
    "/tenant/config",
    response_model=TenantConfig,
    dependencies=[Security(oauth2_auth().verify, scopes=[TENANT_READ])],  # type: ignore
)
async def get_tenant_config(tenant_id: Annotated[int, Depends(get_tenant_id)], tenant_crud_client: TenantsCRUDDep):
    """
    Retrieve the configuration for the current tenant.
    """
    try:
        config = await tenant_crud_client.get_tenant_config(tenant_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Tenant not found") from e

    if not config:
        raise HTTPException(status_code=404, detail="Tenant configuration not found")

    return config
