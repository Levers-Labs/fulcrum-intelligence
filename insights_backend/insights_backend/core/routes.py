import logging
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Security,
)
from slack_sdk.errors import SlackApiError
from sqlalchemy.exc import IntegrityError
from starlette import status

from commons.auth.scopes import (
    ADMIN_READ,
    ADMIN_WRITE,
    USER_READ,
    USER_WRITE,
)
from commons.db.crud import NotFoundError
from commons.models.tenant import (
    SlackConnectionConfig,
    TenantConfig,
    TenantConfigRead,
    TenantConfigUpdate,
)
from commons.utilities.context import get_tenant_id, set_tenant_id
from commons.utilities.pagination import PaginationParams
from insights_backend.core.crud import TenantCRUD
from insights_backend.core.dependencies import (
    SlackClientDep,
    SlackOAuthServiceDep,
    TenantsCRUDDep,
    UsersCRUDDep,
    oauth2_auth,
)
from insights_backend.core.filters import TenantConfigFilter
from insights_backend.core.models import (
    TenantList,
    TenantRead,
    UserCreate,
    UserList,
)
from insights_backend.core.models.tenant import SnowflakeConfig
from insights_backend.core.models.users import UserRead, UserUpdate
from insights_backend.core.schemas import (
    SlackChannel,
    SlackChannelResponse,
    SnowflakeConfigCreate,
    SnowflakeConfigRead,
    SnowflakeConfigUpdate,
    SnowflakeConnectionTest,
)

user_router = APIRouter(prefix="/users", tags=["users"])
router = APIRouter(tags=["tenants"])
slack_router = APIRouter(prefix="/slack", tags=["slack"])
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
async def upsert_user(user: UserCreate, user_crud_client: UsersCRUDDep, tenant_crud_client: TenantsCRUDDep):
    """
    To create a new user in DB, this endpoint will be used by Auth0 for user registration.
    """
    # Handle tenant context from org id
    await handle_tenant_context_from_org_id(user.tenant_org_id, tenant_crud_client)
    try:
        return await user_crud_client.upsert(obj_in=user)
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
        user_obj = await user_crud_client.get(user_id)
        return await user_crud_client.update(obj=user_obj, obj_in=user)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


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
    results: list[UserRead] = [
        UserRead.model_validate(user) for user in await user_crud_client.list_results(params=params)
    ]
    return UserList(results=results, count=count)


@user_router.delete("/{user_id}", dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])])  # type: ignore
async def delete_user(user_id: int, user_crud_client: UsersCRUDDep):
    """
    Delete a user by ID.
    """
    try:
        await user_crud_client.delete(id=user_id)
        return {"detail": "User deleted successfully"}
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
    params: Annotated[
        PaginationParams,
        Depends(PaginationParams),
    ],
    enable_story_generation: bool | None = None,
    identifier: str | None = None,
):
    """
    Retrieve all tenants in DB.
    """
    tenant_config_filter = TenantConfigFilter(enable_story_generation=enable_story_generation, identifier=identifier)
    results, count = await tenant_crud_client.paginate(
        params=params, filter_params=tenant_config_filter.model_dump(exclude_unset=True)
    )
    tenants: list[TenantRead] = [TenantRead.model_validate(tenant) for tenant in results]

    return TenantList(results=tenants, count=count)


@router.get(
    "/tenant/details",
    response_model=TenantRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_READ])],  # type: ignore
)
async def get_tenant_details(tenant_id: Annotated[int, Depends(get_tenant_id)], tenant_crud_client: TenantsCRUDDep):
    """
    Retrieve the details for the current tenant.
    """
    return await tenant_crud_client.get(tenant_id)


@router.get(
    "/tenant/config",
    response_model=TenantConfigRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_READ])],  # type: ignore
)
async def get_tenant_config(tenant_id: Annotated[int, Depends(get_tenant_id)], tenant_crud_client: TenantsCRUDDep):
    """
    Retrieve the configuration for the current tenant.
    """
    try:
        config = await tenant_crud_client.get_tenant_config(tenant_id)
        return config
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Tenant not found") from e


@router.get(
    "/tenant/config/internal",
    response_model=TenantConfig,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_READ])],  # type: ignore
)
async def get_tenant_config_internal(
    tenant_id: Annotated[int, Depends(get_tenant_id)],
    tenant_crud_client: TenantsCRUDDep,
):
    """
    Internal endpoint to retrieve the complete tenant configuration including sensitive fields.
    This endpoint should only be used by internal services.
    """
    try:
        config: TenantConfig = await tenant_crud_client.get_tenant_config(tenant_id)
        return TenantConfig.model_validate(config)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Tenant not found") from e


@router.put(
    "/tenant/config",
    response_model=TenantConfigRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[])],  # type: ignore
)
async def update_tenant_config(
    tenant_id: Annotated[int, Depends(get_tenant_id)],  # Retrieve tenant_id from the request context
    config: TenantConfigUpdate,  # The new configuration for the tenant's cube connection
    tenant_crud_client: TenantsCRUDDep,  # Dependency for the tenant CRUD operations
):
    """
    Update the configuration for a tenant's cube connection.
    """
    try:
        # Attempt to update the tenant configuration
        updated_config = await tenant_crud_client.update_tenant_config(tenant_id, config)  # type: ignore
        return updated_config
    except NotFoundError as e:
        # Raise an HTTPException if the tenant is not found
        raise HTTPException(status_code=404, detail="Tenant not found") from e


# Snowflake Configuration Routes
@router.post(
    "/tenant/snowflake-config",
    response_model=SnowflakeConfigRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_WRITE])],  # type: ignore
    tags=["snowflake"],
)
async def create_snowflake_config(config: SnowflakeConfigCreate, tenant_crud_client: TenantsCRUDDep):
    """
    Create Snowflake configuration for a tenant.
    """
    snowflake_config = await tenant_crud_client.create_snowflake_config(config)
    return snowflake_config


@router.get(
    "/tenant/snowflake-config",
    response_model=SnowflakeConfigRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_READ])],  # type: ignore
    tags=["snowflake"],
)
async def get_snowflake_config(tenant_crud_client: TenantsCRUDDep):
    """
    Retrieve Snowflake configuration for the current tenant.
    """
    try:
        config = await tenant_crud_client.get_snowflake_config()
        return config
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Snowflake configuration not found") from e


@router.get(
    "/tenant/snowflake-config/internal",
    response_model=SnowflakeConfig,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_READ])],  # type: ignore
    tags=["snowflake"],
)
async def get_snowflake_config_internal(tenant_crud_client: TenantsCRUDDep):
    """
    Internal endpoint to retrieve the complete Snowflake configuration including sensitive fields.
    This endpoint should only be used by internal services.
    """
    config: SnowflakeConfig = await tenant_crud_client.get_snowflake_config()
    return config


@router.put(
    "/tenant/snowflake-config",
    response_model=SnowflakeConfigRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_WRITE])],  # type: ignore
    tags=["snowflake"],
)
async def update_snowflake_config(config: SnowflakeConfigUpdate, tenant_crud_client: TenantsCRUDDep):
    """
    Update Snowflake configuration for a tenant.
    """
    try:
        updated_config = await tenant_crud_client.update_snowflake_config(config)
        return updated_config
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Tenant or Snowflake configuration not found") from e


@router.delete(
    "/tenant/snowflake-config",
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_WRITE])],  # type: ignore
    tags=["snowflake"],
)
async def delete_snowflake_config(tenant_crud_client: TenantsCRUDDep):
    """
    Delete Snowflake configuration for a tenant.
    """
    success = await tenant_crud_client.delete_snowflake_config()
    if success:
        return {"detail": "Snowflake configuration deleted successfully"}
    else:
        raise HTTPException(status_code=404, detail="Snowflake configuration not found")


@router.post(
    "/tenant/snowflake-config/test",
    response_model=SnowflakeConnectionTest,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_WRITE])],  # type: ignore
    tags=["snowflake"],
)
async def test_snowflake_connection(config_data: SnowflakeConfigCreate, tenant_crud_client: TenantsCRUDDep):
    """
    Test Snowflake connection with provided configuration.
    This allows testing a connection before saving the configuration.
    """
    test_result = await tenant_crud_client.test_snowflake_connection(config_data.model_dump(mode="json"))
    return test_result


@router.put(
    "/tenant/metric-cache/enable",
    response_model=TenantConfigRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_WRITE])],  # type: ignore
    tags=["snowflake"],
)
async def enable_metric_cache(tenant_crud_client: TenantsCRUDDep, enabled: bool = True):
    """
    Enable or disable metric cache feature for a tenant.
    """
    try:
        updated_config = await tenant_crud_client.enable_metric_cache(enabled)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Tenant or Snowflake configuration not found") from e
    return updated_config


# Slack OAuth Routes
@slack_router.get(
    "/oauth/authorize",
    response_model=dict[str, str],
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_WRITE])],  # type: ignore
)
async def generate_authorize_url(service: SlackOAuthServiceDep):
    """Generate Slack OAuth URL for authorization."""
    # todo: need to store tenant_id in the state for the callback context to retrieve it
    # Generate OAuth URL
    auth_url = service.generate_oauth_url()
    logger.info("Slack OAuth URL: %s", auth_url)
    return {"authorization_url": auth_url}


@slack_router.post(
    "/oauth/callback",
    response_model=TenantConfigRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_WRITE])],  # type: ignore
)
async def oauth_callback(
    code: str,
    service: SlackOAuthServiceDep,
    tenant_crud: TenantsCRUDDep,
    tenant_id: Annotated[int, Depends(get_tenant_id)],
):
    """Handle Slack OAuth callback."""

    # Authorize OAuth code
    slack_config = await service.authorize_oauth_code(code)

    # Update tenant config with Slack connection details
    tenant_config = await tenant_crud.update_slack_connection(tenant_id, slack_config)

    return tenant_config


@slack_router.post(
    "/disconnect",
    response_model=TenantConfigRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_WRITE])],  # type: ignore
)
async def disconnect_slack(
    service: SlackOAuthServiceDep,
    tenant_crud: TenantsCRUDDep,
    tenant_id: Annotated[int, Depends(get_tenant_id)],
):
    """
    Disconnect Slack integration for a tenant by clearing the Slack connection config.
    """
    tenant_config = await tenant_crud.get_tenant_config(tenant_id)
    # check if tenant config has slack connection details
    if not tenant_config.slack_connection:
        raise HTTPException(status_code=400, detail="Slack connection details not found")
    # call revoke token endpoint
    slack_connection = SlackConnectionConfig.model_validate(tenant_config.slack_connection)
    await service.revoke_oauth_token(slack_connection.bot_token)
    # clear the slack connection details
    tenant_config = await tenant_crud.revoke_slack_connection(tenant_config)
    return tenant_config


@slack_router.get(
    "/channels",
    response_model=SlackChannelResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_READ])],  # type: ignore
)
async def list_channels(
    slack_client: SlackClientDep,
    name: str | None = None,
    cursor: str | None = None,
    limit: int = 100,
):
    """
    List Slack channels with optional name filtering and pagination support.
    """
    return slack_client.list_channels(cursor=cursor, limit=limit, name=name)


@slack_router.get(
    "/channels/{channel_id}",
    response_model=SlackChannel | dict,  # noqa
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_READ])],  # type: ignore
)
async def get_channel_info(
    slack_client: SlackClientDep,
    channel_id: str,
):
    """
    Retrieve detailed information about a specific Slack channel by its ID.
    """
    try:
        return slack_client.get_channel_info(channel_id=channel_id)
    except SlackApiError as SlackErr:
        raise HTTPException(status_code=404, detail=f"Channel not found for {channel_id}") from SlackErr
