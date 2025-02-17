import logging
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Security,
)

from commons.auth.scopes import USER_READ, USER_WRITE
from commons.utilities.pagination import Page, PaginationParams
from insights_backend.core.dependencies import oauth2_auth
from insights_backend.notifications.dependencies import AlertsCRUDDep, NotificationListServiceDep
from insights_backend.notifications.models import Alert
from insights_backend.notifications.schemas import AlertRequest, NotificationList

logger = logging.getLogger(__name__)

notification_router = APIRouter(prefix="/notification", tags=["notifications"])


#  ALERTS APIS =========


@notification_router.post(
    "/alerts",
    status_code=201,
    response_model=Alert,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],
)
async def create_alert(
    alert_data: AlertRequest,
    alert_crud: AlertsCRUDDep,
):
    """
    Creates a new alert, either as draft or published.

    This endpoint allows the creation of a new alert, which can be either in draft or published state. It requires
    the USER_WRITE scope for authentication.

    :param alert_data: The request data for creating the alert.
    :param alert_crud: Dependency for CRUD operations on alerts.
    :return: The created alert object.
    """
    alert = await alert_crud.create(
        alert_data=alert_data,
    )

    return alert


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
    response_model=Alert,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],  # type: ignore
)
async def get_alert(
    alert_id: int,
    alert_crud: AlertsCRUDDep,
):
    """
    Retrieve an alert configuration along with its associated notification channels by ID.

    This endpoint fetches an alert by its ID and includes its notification channels in the response.
    It requires the USER_READ scope for authentication.

    :param alert_id: The ID of the alert to retrieve.
    :param alert_crud: Dependency for CRUD operations on alerts.
    """

    return await alert_crud.get_alert_with_channels(alert_id)


@notification_router.post(
    "/alerts/{alert_id}/publish",
    status_code=200,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_READ])],
)
async def publish_alert(
    alert_id: int,
    alert_crud: AlertsCRUDDep,
):
    """
    Publish a draft alert with notification channels.

    This endpoint publishes a draft alert, making it active and ready for distribution through its associated
    notification channels. It requires the USER_READ scope for authentication.

    :param alert_id: The ID of the alert to publish.
    :param alert_crud: Dependency for CRUD operations on alerts.
    """
    try:
        return await alert_crud.publish(alert_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


# Common ==========


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
    This endpoint fetches a paginated list of all notifications, including both alerts and reports.
    Supports filtering by status, type, grain, and tags.

    :param notification_service: Dependency injection for the NotificationListService
    :param params: Pagination parameters
    :return: A paginated list of notifications
    """

    notifications, total = await notification_service.get_notifications_list(params=params)
    return Page[NotificationList].create(items=notifications, total_count=total, params=params)


@notification_router.patch(
    "/notifications/bulk/status",
    status_code=200,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],
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
    "/notifications/bulk",
    status_code=204,
    dependencies=[Security(oauth2_auth().verify, scopes=[USER_WRITE])],
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
