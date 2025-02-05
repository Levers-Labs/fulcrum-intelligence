import logging
import os
from typing import Annotated

from fastapi import (
    APIRouter,
    Body,
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
    SlackClientDep,
    SlackOAuthServiceDep,
    TenantsCRUDDep,
    UsersCRUDDep,
    oauth2_auth,
)
from insights_backend.core.enums import SCHEDULE_OPTIONS_MAP, ScheduleOption
from insights_backend.core.filters import TenantConfigFilter
from insights_backend.core.models import (
    TenantList,
    TenantRead,
    User,
    UserCreate,
    UserList,
)
from insights_backend.core.models.notifications import NotificationType
from insights_backend.core.models.users import UserRead, UserUpdate
from insights_backend.core.schemas import (
    AlertCreateRequest,
    AlertResponse,
    AlertUpdateRequest,
    NotificationChannelDetail,
    NotificationList,
    PreviewRequest,
    PreviewResponse,
    SlackChannel,
    SlackChannelResponse,
)

from ..services.preview_service import PreviewService

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


@notification_router.get(
    "/schedule-options/{granularity}",
    response_model=list[ScheduleOption],
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],  # type: ignore
)
async def get_schedule_options(granularity: Granularity) -> list[ScheduleOption]:
    """
    Get available schedule options for a given granularity.
    """
    return SCHEDULE_OPTIONS_MAP.get(granularity, [])


@notification_router.post(
    "/alerts",
    response_model=AlertResponse,
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
        # Attempt to create a new alert using the provided alert data
        alert = await alert_crud.create(
            new_alert=alert_data,
        )

        # Initialize an empty list to hold notification channels
        notification_channels = []
        # Check if the alert is being published and if notification channels are provided
        if alert_data.is_published and alert_data.notification_channels:
            # Create notification channels for the published alert
            notification_channels = await notification_crud.create(
                notification_configs=alert_data.notification_channels, alert_id=alert.id
            )

        # Prepare the response by combining the alert data with its notification channels
        return AlertResponse.model_validate(
            {**alert.model_dump(), "notification_channels": [n.model_dump() for n in notification_channels]}
        )
    except IntegrityError as e:
        # Handle any integrity errors that occur during the creation process
        raise HTTPException(
            status_code=400, detail="Alert creation failed. Possible duplicate name or invalid data."
        ) from e


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


@notification_router.delete(
    "/alerts/bulk",
    status_code=204,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],  # type: ignore
)
async def bulk_delete_alerts(
    alert_crud: AlertsCRUDDep,
    notification_crud: NotificationCRUDDep,
    alert_ids: list[int] = Body(..., description="List of alert IDs to delete"),
):
    """
    Bulk delete alerts and their associated notification channels.

    This endpoint allows for the bulk deletion of alerts and their associated notification channels.
    It requires the USER_WRITE scope for authentication.

    :param alert_crud: Dependency for CRUD operations on alerts.
    :param notification_crud: Dependency for CRUD operations on notifications.
    :param alert_ids: List of IDs of the alerts to delete.
    """
    failed_deletions = []
    successful_deletions = []

    for alert_id in alert_ids:
        if not await alert_crud.get(alert_id):
            failed_deletions.append(alert_id)
            continue
        successful_deletions.append(alert_id)

    if failed_deletions and not successful_deletions:
        # If all deletions failed
        raise HTTPException(
            status_code=404,
            detail={
                "loc": ["body", "alert_ids"],
                "msg": f"None of the alerts were found: {failed_deletions}",
                "type": "not_found",
            },
        )

    if successful_deletions:
        # Delete notification channels and alerts for successful IDs
        await notification_crud.delete_by_alert_ids(successful_deletions)
        await alert_crud.delete(successful_deletions)

    return None


@notification_router.patch(
    "/bulk/actions",
    status_code=200,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],  # type: ignore
)
async def bulk_toggle_notifications(
    alert_crud: AlertsCRUDDep,
    is_active: bool,
    notification_type: NotificationType,
    ids: list[int],
):
    """
    Bulk toggle the active status of notifications based on their type.

    This endpoint allows for the bulk update of the active status of notifications.
    It supports two types of notifications: ALERT and others (not explicitly handled).
    For ALERT type, it uses the alert_crud service to update the status.
    For other types, it does not perform any update.

    :param alert_crud: Dependency for CRUD operations on alerts.
    :param is_active: The new active status to set for the notifications.
    :param notification_type: The type of notifications to update.
    :param ids: List of IDs of the notifications to update.
    """
    # Determine the CRUD service to use based on the notification type
    crud_service = alert_crud if notification_type == NotificationType.ALERT else None

    failed_updates = []
    successful_updates = []

    # Iterate through the list of IDs to check which notifications exist
    for item_id in ids:
        if not await crud_service.get(item_id):  # type: ignore
            failed_updates.append(item_id)
            continue
        successful_updates.append(item_id)

    # If all updates failed, raise an HTTPException
    if failed_updates and not successful_updates:
        raise HTTPException(
            status_code=404,
            detail={
                "loc": ["body", "ids"],
                "msg": f"None of the {notification_type} were found: {failed_updates}",
                "type": "not_found",
            },
        )

    # If there are successful updates, update the active status for those IDs
    if successful_updates:
        await crud_service.update_active_status(successful_updates, is_active)  # type: ignore

    return None


@notification_router.patch(
    "/alerts/{alert_id}",
    response_model=AlertResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],
)
async def update_alert(
    alert_id: int,
    update_data: AlertUpdateRequest,
    alert_crud: AlertsCRUDDep,
    notification_crud: NotificationCRUDDep,
):
    """
    Update alert with partial or full data.
    - Draft alerts ignore notification channels
    - Publishing requires notification channels
    - Cannot transition from published to draft (use dedicated unpublish endpoint)
    """
    try:
        # Retrieve the current alert state
        current_alert = await alert_crud.get(alert_id)
        if not current_alert:
            raise HTTPException(status_code=404, detail=f"Alert with ID {alert_id} not found")

        # Check if attempting to publish an alert without specifying notification channels
        if not current_alert.is_published and update_data.is_published:
            if not update_data.notification_channels:
                raise HTTPException(
                    status_code=400, detail="Notification channels are required when publishing an alert"
                )

        # Update the alert with the provided data
        alert = await alert_crud.update(
            alert_id=alert_id,
            update_alert=update_data,
        )

        notification_channels = []

        # Handle notification channels based on the alert's state
        if alert.is_published:
            if update_data.notification_channels is not None:  # Explicitly check if field was provided
                # Update channels if provided
                notification_channels = await notification_crud.update_by_alert_id(
                    alert_id=alert_id, notification_configs=update_data.notification_channels
                )
            else:
                # Keep existing channels if not updating them
                notification_channels = await notification_crud.get_channels_by_alert(alert_id)

        # Prepare the response with the updated alert and its notification channels
        return AlertResponse.model_validate(
            {**alert.model_dump(), "notification_channels": [n.model_dump() for n in notification_channels]}
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except IntegrityError as e:
        raise HTTPException(
            status_code=400, detail="Alert update failed. Possible duplicate name or invalid data."
        ) from e


@notification_router.post(
    "/alerts/{alert_id}/publish",
    response_model=AlertResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],
)
async def publish_alert(
    alert_id: int,  # The ID of the alert to be published
    notification_configs: list[NotificationChannelDetail],  # List of notification channel configurations
    alert_crud: AlertsCRUDDep,  # Dependency for alert CRUD operations
    notification_crud: NotificationCRUDDep,  # Dependency for notification CRUD operations
):
    """
    Publish a draft alert.
    This endpoint is used to publish a draft alert. It requires notification channel configurations to be provided.
    The endpoint verifies the alert ID, checks if the alert is already published, validates the notification
    configurations,
    updates the alert to a published state, creates notification channels based on the provided configurations,
    and returns
    the updated alert with its notification channels.
    """
    try:
        # Retrieve the draft alert by its ID
        alert = await alert_crud.get(alert_id)
        if not alert:
            raise HTTPException(status_code=404, detail=f"Alert with ID {alert_id} not found")

        # Check if the alert is already published
        if alert.is_published:
            raise HTTPException(status_code=400, detail="Alert is already published")

        # Validate that notification configurations are provided
        if not notification_configs:
            raise HTTPException(status_code=400, detail="Notification channels are required for publishing an alert")

        # Update the alert to a published state
        alert = await alert_crud.update(alert_id=alert_id, update_alert={"is_published": True})  # type: ignore

        # Create notification channels based on the provided configurations
        notification_channels = await notification_crud.create(
            notification_configs=notification_configs, alert_id=alert_id
        )

        # Prepare the response with the updated alert and its notification channels
        return AlertResponse.model_validate(
            {**alert.model_dump(), "notification_channels": [n.model_dump() for n in notification_channels]}
        )

    except IntegrityError as e:
        # Handle any integrity errors that occur during the publication process
        raise HTTPException(
            status_code=400, detail="Alert publication failed. Invalid notification configuration."
        ) from e


@notification_router.post(
    "/alerts/preview",
    response_model=PreviewResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],
)
async def preview_template(
    preview_data: PreviewRequest,
):
    """
    Preview notification template with sample data.

    This endpoint generates a preview of a notification template using sample data. It requires the USER_READ scope
    for authentication.

    :param preview_data: The request data for previewing the template, including template type, story groups, metric,
     grain, and recipients.
    :return: A PreviewResponse object containing the preview HTML, raw content, and recipients.
    """
    # Define the directory path for the templates
    template_dir = os.path.join(os.path.dirname(__file__), "../templates")
    # Initialize the PreviewService with the template directory
    preview_service = PreviewService(template_dir=template_dir)

    try:
        # Call the preview_template method of the PreviewService to generate the preview
        preview_result = preview_service.preview_template(preview_data)

        # Return a PreviewResponse object with the preview HTML, raw content, and recipients
        return PreviewResponse(
            preview_html=preview_result["preview_html"],
            raw_content=preview_result["raw_content"],
            recipients=preview_result["recipients"],
        )

    except Exception as e:
        # Raise an HTTPException if any error occurs during template rendering
        raise HTTPException(status_code=400, detail=f"Template rendering failed: {str(e)}") from e


@notification_router.get(
    "/notifications",
    response_model=Page[NotificationList],
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],
)
async def list_notifications(
    notification_service: NotificationListServiceDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
):
    """
    Retrieve a paginated list of all notifications (alerts and reports).

    This endpoint fetches a paginated list of all notifications, including both alerts and reports. It requires the
    USER_READ scope for authentication.

    :param notification_service: Dependency injection for the NotificationListService.
    :param params: Pagination parameters, including limit and offset.
    :return: A paginated list of notifications, including the total count.
    """
    # Fetch notifications and total count from the notification service
    notifications, total = await notification_service.get_notifications(params=params)

    # Create and return a paginated page of notifications
    return Page[NotificationList].create(items=notifications, total_count=total, params=params)
