import logging
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Security,
)

from commons.auth.scopes import ALERT_REPORT_READ, ALERT_REPORT_WRITE
from commons.notifiers.constants import NotificationChannel
from commons.utilities.pagination import Page, PaginationParams
from insights_backend.core.dependencies import oauth2_auth
from insights_backend.notifications.dependencies import AlertPreviewServiceDep, AlertsCRUDDep, CRUDNotificationsDep
from insights_backend.notifications.filters import NotificationConfigFilter
from insights_backend.notifications.models import Alert
from insights_backend.notifications.schemas import (
    AlertDetail,
    AlertRequest,
    Granularity,
    NotificationList,
    NotificationType,
    PreviewResponse,
)

logger = logging.getLogger(__name__)

notification_router = APIRouter(prefix="/notification", tags=["notifications"])


#  ALERTS APIS =========


@notification_router.post(
    "/alerts",
    status_code=201,
    response_model=Alert,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_WRITE])],
)
async def create_alert(
    alert_create: AlertRequest,
    alert_crud: AlertsCRUDDep,
):
    """
    Creates a new alert, either as draft or published.

    This endpoint allows the creation of a new alert, which can be either in draft or published state. It requires
    the ALERT_REPORT_WRITE scope for authentication.

    :param alert_create: The request data for creating the alert.
    :param alert_crud: Dependency for CRUD operations on alerts.
    :return: The created alert object.
    """
    alert = await alert_crud.create(
        alert_create=alert_create,
    )

    return alert


@notification_router.get(
    "/alerts/{alert_id}",
    response_model=AlertDetail,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],  # type: ignore
)
async def get_alert(
    alert_id: int,
    alert_crud: AlertsCRUDDep,
):
    """
    Retrieve an alert configuration along with its associated notification channels by ID.

    This endpoint fetches an alert by its ID and includes its notification channels in the response.
    It requires the ALERT_REPORT_READ scope for authentication.

    :param alert_id: The ID of the alert to retrieve.
    :param alert_crud: Dependency for CRUD operations on alerts.
    """

    return await alert_crud.get(alert_id)


@notification_router.post(
    "/alerts/{alert_id}/publish",
    status_code=200,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],
)
async def publish_alert(
    alert_id: int,
    alert_crud: AlertsCRUDDep,
):
    """
    Publish a draft alert with notification channels.

    This endpoint publishes a draft alert, making it active and ready for distribution through its associated
    notification channels. It requires the ALERT_REPORT_READ scope for authentication.

    :param alert_id: The ID of the alert to publish.
    :param alert_crud: Dependency for CRUD operations on alerts.
    """
    try:
        return await alert_crud.publish(alert_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@notification_router.patch(
    "/alerts/{alert_id}",
    response_model=AlertDetail,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_WRITE])],
)
async def update_alert(
    alert_id: int,
    alert_update: AlertRequest,
    alert_crud: AlertsCRUDDep,
):
    """
    Update an existing alert and its notification channels.

    This endpoint updates an alert and its associated channels. Channels not included
    in the update will be deleted. It requires the ALERT_REPORT_WRITE scope for authentication.

    Args:
        alert_id: The ID of the alert to update
        alert_update: The updated alert data
        alert_crud: Dependency for CRUD operations on alerts

    Returns:
        Updated alert with its notification channels
    """
    return await alert_crud.update_alert(
        alert_id=alert_id,
        alert_update=alert_update,
    )


@notification_router.post(
    "/alerts/preview",
    response_model=PreviewResponse,
    status_code=200,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],
)
async def preview_alert(
    alert_data: AlertRequest,
    preview_service: AlertPreviewServiceDep,
) -> PreviewResponse:
    """
    Preview an alert with rendered notification templates.

    This endpoint generates a preview of how the alert notifications will look
    without actually creating the alert or sending notifications.

    :param alert_data: The alert data to preview
    :param preview_service: Service for generating previews
    :return: Preview of email and/or slack notifications
    """
    return await preview_service.preview(alert_data)  # type: ignore


# Common ==========


@notification_router.get(
    "/tags",
    response_model=list[str],
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],
)
async def get_tags(notifications_crud: CRUDNotificationsDep) -> list[str]:
    """Get all unique tags used across alerts and reports"""
    return await notifications_crud.get_unique_tags()


@notification_router.get(
    "/",
    response_model=Page[NotificationList],
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],
)
async def list_notifications(
    notification_crud: CRUDNotificationsDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    notification_type: Annotated[NotificationType | None, Query()] = None,
    channel_type: Annotated[NotificationChannel | None, Query()] = None,
    grain: Annotated[Granularity | None, Query()] = None,
    is_active: Annotated[bool | None, Query()] = None,
):
    """
    Retrieve a paginated list of all notifications (alerts and reports).

    Args:
        notification_crud: CRUD dependency for notification channels
        params: Pagination parameters
        notification_type: Filter by notification type
        channel_type: Filter by channel type
        grain: Filter by granularity
        tags: Filter by tags
        is_active: Filter by active status

    Returns:
        Paginated list of notifications
    """
    notification_filter = NotificationConfigFilter(
        notification_type=notification_type, channel_type=channel_type, grain=grain, is_active=is_active
    )

    results, count = await notification_crud.get_notifications_list(
        params=params, filter_params=notification_filter.dict(exclude_unset=True)
    )

    notifications = [NotificationList.model_validate(row) for row in results]

    return Page.create(items=notifications, total_count=count, params=params)


@notification_router.patch(
    "/bulk/status",
    status_code=200,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_WRITE])],
)
async def bulk_update_status(
    alert_ids: list[int],
    is_active: bool,
    alert_crud: AlertsCRUDDep,
):
    """
    Bulk update the status of alerts and reports.

    This endpoint updates the status of a list of alerts and reports to either active or inactive.
    It first validates the provided alert IDs to ensure they exist in the database. If any IDs are not found,
    it raises an HTTPException with a 404 status code. If all IDs are valid, it proceeds to update the status
    of the alerts and reports in bulk.

    :param alert_ids: List of alert IDs to update
    :param is_active: Boolean indicating the new status (True for active, False for inactive)
    :param alert_crud: Dependency injection for the AlertsCRUD service
    """
    if alert_ids:
        missing_alerts = await alert_crud.validate_ids(alert_ids)
        if missing_alerts:
            raise HTTPException(status_code=404, detail=f"Alerts not found: {list(missing_alerts)}")
        await alert_crud.batch_status_update(alert_ids, is_active)


@notification_router.delete(
    "/bulk",
    status_code=204,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_WRITE])],
)
async def bulk_delete_notifications(
    alert_ids: list[int],
    alert_crud: AlertsCRUDDep,
):
    """
    Bulk delete notifications (alerts/reports) and their associated configurations.

    This endpoint deletes a list of notifications (alerts or reports) and their associated configurations.
    It first validates the provided alert IDs to ensure they exist in the database. If any IDs are not found,
    it raises an HTTPException with a 404 status code. If all IDs are valid, it proceeds to delete the notifications
    and their configurations in bulk.

    :param alert_ids: List of alert IDs to delete
    :param alert_crud: Dependency injection for the AlertsCRUD service
    :return: None
    """
    if alert_ids:
        # Validate the provided alert IDs to ensure they exist in the database
        missing_alert_ids = await alert_crud.validate_ids(alert_ids)
        if missing_alert_ids:
            # If any IDs are not found, raise an HTTPException with a 404 status code
            raise HTTPException(status_code=404, detail=f"Alerts not found: {list(missing_alert_ids)}")
        # If all IDs are valid, proceed to delete the notifications and their configurations in bulk
        await alert_crud.batch_delete(alert_ids)
    return None
