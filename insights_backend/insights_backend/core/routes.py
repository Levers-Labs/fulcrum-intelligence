import logging
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
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
from commons.models.enums import Granularity
from commons.models.tenant import (
    SlackConnectionConfig,
    TenantConfig,
    TenantConfigRead,
    TenantConfigUpdate,
)
from commons.utilities.context import get_tenant_id, set_tenant_id
from commons.utilities.pagination import Page, PaginationParams
from insights_backend.core.crud import TenantCRUD
from insights_backend.core.dependencies import (
    AlertsCRUDDep,
    NotificationCRUDDep,
    NotificationListServiceDep,
    PreviewServiceDep,
    SlackClientDep,
    SlackOAuthServiceDep,
    TenantsCRUDDep,
    UsersCRUDDep,
    oauth2_auth,
)
from insights_backend.core.filters import NotificationFilter, TenantConfigFilter
from insights_backend.core.models import (
    TenantList,
    TenantRead,
    User,
    UserCreate,
    UserList,
)
from insights_backend.core.models.users import UserRead, UserUpdate
from insights_backend.core.schemas import (
    AlertCreateRequest,
    AlertPublishRequest,
    AlertResponse,
    AlertUpdateRequest,
    NotificationList,
    NotificationType,
    PreviewRequest,
    PreviewResponse,
    SlackChannel,
    SlackChannelResponse,
)

user_router = APIRouter(prefix="/users", tags=["users"])
router = APIRouter(tags=["tenants"])
slack_router = APIRouter(prefix="/slack", tags=["slack"])
notification_router = APIRouter(prefix="/notification", tags=["notifications"])
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
):
    """
    Retrieve all tenants in DB.
    """
    tenant_config_filter = TenantConfigFilter(enable_story_generation=enable_story_generation)
    results, count = await tenant_crud_client.paginate(
        params=params, filter_params=tenant_config_filter.dict(exclude_unset=True)
    )
    tenants: list[TenantRead] = [TenantRead.model_validate(tenant) for tenant in results]

    return TenantList(results=tenants, count=count)


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
        config: TenantConfig = await tenant_crud_client.get_tenant_config(tenant_id)
        return config
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Tenant not found") from e


@router.get(
    "/tenant/config/internal",
    response_model=TenantConfig,
    dependencies=[Security(oauth2_auth().verify, scopes=[ADMIN_READ])],  # type: ignore
)
async def get_tenant_config_internal(
    tenant_id: Annotated[int, Depends(get_tenant_id)], tenant_crud_client: TenantsCRUDDep
):
    """
    Internal endpoint to retrieve the complete tenant configuration including sensitive fields.
    This endpoint should only be used by internal services.
    """
    try:
        config: TenantConfig = await tenant_crud_client.get_tenant_config(tenant_id)
        return config
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
    slack_connection = SlackConnectionConfig.parse_obj(tenant_config.slack_connection)
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
    return await slack_client.list_channels(cursor=cursor, limit=limit, name=name)


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
        return await slack_client.get_channel_info(channel_id=channel_id)
    except SlackApiError as SlackErr:
        raise HTTPException(status_code=404, detail=f"Channel not found for {channel_id}") from SlackErr


#  ALERTS APIS =========


@notification_router.post(
    "/alerts",
    status_code=201,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],
)
async def create_alert(
    alert_data: AlertCreateRequest,
    alert_crud: AlertsCRUDDep,
    notification_crud: NotificationCRUDDep,
):
    """
    Create a new alert, either as draft or published.
    If creating as draft (is_published=False), notification channels will be ignored.
    """
    try:
        alert = await alert_crud.create(
            new_alert=alert_data,
        )

        if alert_data.notification_channels:
            _ = await notification_crud.create(notification_configs=alert_data.notification_channels, alert_id=alert.id)
    except IntegrityError as e:
        # Handle any integrity errors that occur during the creation process
        raise HTTPException(
            status_code=400, detail="Alert creation failed. Possible duplicate name or invalid data."
        ) from e


@notification_router.get(
    "/alerts/tags",
    response_model=list[str],
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],
)
async def get_alert_tags(
    alert_crud: AlertsCRUDDep,
) -> list[str]:
    """Get all unique tags used across alerts"""
    return await alert_crud.get_unique_tags()


@notification_router.get(
    "/alerts/{alert_id}",
    response_model=AlertResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],  # type: ignore
)
async def get_alert(
    alert_id: int,
    alert_crud: AlertsCRUDDep,
    notification_crud: NotificationCRUDDep,
):
    """
    Retrieve an alert configuration along with its associated notification channels by ID.

    This endpoint fetches an alert by its ID and includes its notification channels in the response.
    It requires the USER_READ scope for authentication.

    :param alert_id: The ID of the alert to retrieve.
    :param alert_crud: Dependency for CRUD operations on alerts.
    :param notification_crud: Dependency for CRUD operations on notifications.
    """
    # Attempt to fetch the alert by its ID
    alert = await alert_crud.get(alert_id)
    # If the alert is not found, raise an HTTPException
    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert with ID {alert_id} not found")

    # Fetch the notification channels associated with the alert
    notification_channels = await notification_crud.get_channels_by_alert(alert_id)

    # Prepare the response by combining the alert data with its notification channels
    # Let Pydantic handle the conversion of the data to the expected response model
    return AlertResponse.model_validate(
        {**alert.model_dump(), "notification_channels": [n.model_dump() for n in notification_channels]}
    )


@notification_router.delete(
    "/alerts/{alert_id}",
    status_code=204,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],  # type: ignore
)
async def delete_alert(
    alert_id: int,
    alert_crud: AlertsCRUDDep,
    notification_crud: NotificationCRUDDep,
):
    """
    Delete an alert and its associated notification channels.

    This endpoint deletes an alert by its ID and also removes all notification channels associated with it.
    It requires the USER_WRITE scope for authentication.

    :param alert_id: The ID of the alert to delete.
    :param alert_crud: Dependency for CRUD operations on alerts.
    :param notification_crud: Dependency for CRUD operations on notifications.
    """
    alert = await alert_crud.get(alert_id)
    if not alert:
        raise HTTPException(
            status_code=404,
            detail={
                "loc": ["path", "alert_id"],
                "msg": f"Alert with id '{alert_id}' not found.",
                "type": "not_found",
            },
        )

    # Delete associated notification channels and alert
    await notification_crud.delete_by_alert_ids(alert_id)
    await alert_crud.delete(alert_id)

    return None


@notification_router.patch(
    "/alerts/{alert_id}",
    response_model=AlertResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],
)
async def update_alert(
    alert_id: int,
    update_data: AlertUpdateRequest,
    alert_crud: AlertsCRUDDep,
    notification_crud: NotificationCRUDDep,
):
    """Update alert configuration"""
    try:
        alert = await alert_crud.update(alert_id=alert_id, update_alert=update_data)

        notification_channels = []
        if update_data.notification_channels is not None:
            notification_channels = await notification_crud.update_by_alert_id(
                alert_id=alert_id, notification_configs=update_data.notification_channels
            )
        else:
            notification_channels = await notification_crud.get_channels_by_alert(alert_id)

        return AlertResponse.model_validate(
            {**alert.model_dump(), "notification_channels": [n.model_dump() for n in notification_channels]}
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail={"msg": str(e), "type": "validation_error"}) from e
    except IntegrityError as i:
        raise HTTPException(
            status_code=409, detail={"msg": "An alert with this name already exists", "type": "conflict"}
        ) from i


@notification_router.post(
    "/alerts/{alert_id}/publish",
    response_model=AlertResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],
)
async def publish_alert(
    alert_id: int,
    publish_data: AlertPublishRequest,
    alert_crud: AlertsCRUDDep,
    notification_crud: NotificationCRUDDep,
):
    """Publish a draft alert with notification channels"""
    try:
        # Ensure is_published is True
        if not publish_data.is_published:
            raise HTTPException(
                status_code=400,
                detail={"msg": "Cannot set is_published to False in publish request", "type": "validation_error"},
            )

        alert = await alert_crud.publish(alert_id=alert_id, publish_data=publish_data)

        if publish_data.notification_channels:
            notification_channels = await notification_crud.create(
                notification_configs=publish_data.notification_channels, alert_id=alert_id
            )
        else:
            notification_channels = []

        return AlertResponse.model_validate(
            {**alert.model_dump(), "notification_channels": [n.model_dump() for n in notification_channels]}
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail={"msg": str(e), "type": "validation_error"}) from e


@notification_router.post(
    "/alerts/preview",
    response_model=PreviewResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],
)
async def preview_alert(
    preview_data: PreviewRequest,
    preview_service: PreviewServiceDep,
):
    """
    Preview notification template with sample data

    Returns a preview of how the notification will appear in email or Slack,
    using sample data to demonstrate the template rendering.
    """
    try:
        return preview_service.preview_template(preview_data)

    except Exception as e:
        raise HTTPException(
            status_code=400, detail={"msg": f"Failed to generate preview: {str(e)}", "type": "preview_error"}
        ) from e


# Common ==========


@notification_router.get(
    "/notifications",
    response_model=Page[NotificationList],
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],
)
async def list_notifications(
    notification_service: NotificationListServiceDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    type: Annotated[NotificationType, Query(description="List of notification types")] = None,  # type: ignore
    grain: Annotated[Granularity, Query(description="List of grains")] = None,  # type: ignore
    tags: Annotated[str, Query(description="List of tags")] = None,  # type: ignore
):
    """
    Retrieve a paginated list of all notifications (alerts and reports).

    This endpoint fetches a paginated list of all notifications, including both alerts and reports.
    Supports filtering by status, type, grain, and tags.

    :param notification_service: Dependency injection for the NotificationListService
    :param params: Pagination parameters
    :param status: Filter by notification status
    :param type: Filter by notification type
    :param grain: Filter by granularity
    :param tags: Filter by tags
    :return: A paginated list of notifications
    """
    notification_filter = NotificationFilter(
        type=type,
        grain=grain,
        tags=tags,
    )

    notifications, total = await notification_service.get_notifications(
        params=params, filter_params=notification_filter.dict(exclude_unset=True)
    )

    return Page[NotificationList].create(items=notifications, total_count=total, params=params)


@notification_router.patch(
    "/notifications/bulk/status",
    status_code=200,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],
)
async def bulk_update_status(
    alert_ids: list[int],
    # report_ids: list[int],
    is_active: bool,  # Required parameter
    alert_crud: AlertsCRUDDep,
    # report_crud: ReportCRUDDep = Depends(),  # Will be added later
):
    """Bulk update status for alerts and reports"""
    if alert_ids:
        missing_alerts = await alert_crud.get_missing_ids(alert_ids)
        if missing_alerts:
            raise HTTPException(status_code=404, detail=f"Alerts not found: {list(missing_alerts)}")
        await alert_crud.update_active_status(alert_ids, is_active)

    # When report module is added
    # if report_ids:
    #     missing_reports = await report_crud.get_missing_ids(report_ids)
    #     if missing_reports:
    #         raise HTTPException(
    #             status_code=404,
    #             detail=f"Reports not found: {list(missing_reports)}"
    #         )
    #     await report_crud.update_active_status(report_ids, is_active)


@notification_router.delete(
    "/notifications/bulk",
    status_code=204,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],
)
async def bulk_delete_notifications(
    alert_ids: list[int],
    # report_ids: list[int],
    alert_crud: AlertsCRUDDep,
    notification_crud: NotificationCRUDDep,
    # report_crud: ReportCRUDDep = Depends(),  # Will be added later
):
    """Bulk delete notifications (alerts/reports) and their associated configurations"""
    if alert_ids:
        missing_alerts = await alert_crud.get_missing_ids(alert_ids)
        if missing_alerts:
            raise HTTPException(status_code=404, detail=f"Alerts not found: {list(missing_alerts)}")
        await notification_crud.delete_by_alert_ids(alert_ids)
        await alert_crud.delete(alert_ids)

    # When report module is added
    # if report_ids:
    #     missing_reports = await report_crud.get_missing_ids(report_ids)
    #     if missing_reports:
    #         raise HTTPException(
    #             status_code=404,
    #             detail=f"Reports not found: {list(missing_reports)}"
    #         )
    #     await report_crud.delete_recipients(report_ids)
    #     await report_crud.delete(report_ids)

    return None
