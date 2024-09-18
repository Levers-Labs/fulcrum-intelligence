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

from commons.auth.scopes import USER_READ, USER_WRITE
from commons.db.crud import NotFoundError
from commons.models.tenant import Tenant as TenantCreate
from commons.utilities.pagination import PaginationParams
from insights_backend.core.dependencies import (
    TenantConfigCRUDDep,
    TenantCRUDDep,
    UsersCRUDDep,
    oauth2_auth,
)
from insights_backend.core.models import (
    Tenant,
    TenantConfig,
    User,
    UserCreate,
    UserList,
)

user_router = APIRouter(prefix="/users", tags=["users"])
tenant_router = APIRouter(prefix="/tenant", tags=["tenants"])
tenant_config_router = APIRouter(prefix="/tenantConfig", tags=["tenant_config"])
logger = logging.getLogger(__name__)


@user_router.post(
    "/",
    response_model=User,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],  # type: ignore
)
async def create_user(user: UserCreate, user_crud_client: UsersCRUDDep) -> User:
    """
    To create a new user in DB, this endpoint will be used by Auth0 for user registration.
    """
    try:
        return await user_crud_client.create(obj_in=user)
    except IntegrityError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e.orig)) from e


@user_router.get(
    "/{user_id}",
    response_model=User,
    dependencies=[Security(oauth2_auth().verify, scopes=[])],  # type: ignore
)
async def get_user(user_id: int, user_crud_client: UsersCRUDDep) -> User:
    """
    Retrieve a user by ID.
    """
    try:
        user: User = await user_crud_client.get(user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="User not found") from e
    return user


@user_router.get(
    "/user-by-email/{email}",
    response_model=User,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],  # type: ignore
)
async def get_user_by_email(email: str, user_crud_client: UsersCRUDDep) -> User:
    """
    Retrieve a user by email.
    """
    db_user = await user_crud_client.get_user_by_email(email)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user


@user_router.put(
    "/{user_id}",
    response_model=User,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],  # type: ignore
)
async def update_user(user_id: int, user: UserCreate, user_crud_client: UsersCRUDDep) -> User:
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
    results: list[User] = [User.from_orm(user) for user in await user_crud_client.list_results(params=params)]
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


@tenant_router.post("/", response_model=Tenant)
async def create_tenant(tenant: TenantCreate, tenant_crud_client: TenantCRUDDep) -> Tenant:
    """
    To create a new user in DB, this endpoint will be used by Auth0 for user registration.
    """
    try:
        return await tenant_crud_client.create(obj_in=tenant)
    except IntegrityError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e.orig)) from e


@tenant_router.get("/{tenant_id}", response_model=Tenant)
async def get_tenant(tenant_id: int, tenant_crud_client: TenantCRUDDep) -> Tenant:
    """
    Retrieve a user by ID.
    """
    try:
        tenant: Tenant = await tenant_crud_client.get(tenant_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Tenant not found") from e
    return tenant


@tenant_config_router.get("/{tenant_id}", response_model=TenantConfig)
async def get_tenant_config(tenant_id: int, tenant_config_crud_client: TenantConfigCRUDDep) -> TenantConfig:
    try:
        config = await tenant_config_crud_client.get_config_by_tenant_id(tenant_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Tenant Config not found") from e
    return config


@tenant_router.delete("/{tenant_id}")  # type: ignore
async def delete_tenant(tenant_id: int, tenant_crud_client: TenantCRUDDep):
    """
    Retrieve a user by ID.
    """
    try:
        await tenant_crud_client.delete(id=tenant_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Tenant not found") from e
