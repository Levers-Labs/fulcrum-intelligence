import logging
from datetime import datetime
from typing import Annotated

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Query,
    Security,
)

from commons.auth.scopes import ALERT_REPORT_READ, ALERT_REPORT_WRITE
from commons.db.signals import EventAction, EventTiming, publish_event
from commons.exceptions import PrefectOperationError
from commons.models.enums import ExecutionStatus
from commons.notifiers.constants import NotificationChannel
from commons.utilities.pagination import Page, PaginationParams
from insights_backend.core.dependencies import oauth2_auth
from insights_backend.notifications.dependencies import (
    AlertPreviewServiceDep,
    AlertsCRUDDep,
    CRUDNotificationsDep,
    ExecutionCRUDDep,
    ReportPreviewServiceDep,
    ReportsCRUDDep,
)
from insights_backend.notifications.filters import AlertFilter, NotificationConfigFilter, NotificationExecutionFilter
from insights_backend.notifications.models import Alert, NotificationExecutionCreate, NotificationExecutionRead
from insights_backend.notifications.schemas import (
    AlertDetail,
    AlertRequest,
    BatchDeleteResponse,
    BatchStatusUpdateResponse,
    Granularity,
    NotificationList,
    NotificationType,
    PreviewResponse,
    ReportDetail,
    ReportRequest,
)

logger = logging.getLogger(__name__)

notification_router = APIRouter(prefix="/notification", tags=["notifications"])


#  ALERTS APIS =========


@notification_router.post(
    "/alerts",
    status_code=201,
    response_model=AlertDetail,
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
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_WRITE])],
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
    except PrefectOperationError as e:
        raise e


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


@notification_router.get(
    "/alerts",
    response_model=Page[Alert],
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],
)
async def list_alerts(
    alert_crud: AlertsCRUDDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    grains: Annotated[list[Granularity] | None, Query()] = None,
    is_active: Annotated[bool | None, Query()] = None,
    is_published: Annotated[bool | None, Query()] = None,
    metric_ids: Annotated[list[str] | None, Query()] = None,
    story_groups: Annotated[list[str] | None, Query()] = None,
):
    """
    Retrieve a paginated list of alerts with optional filtering.

    This endpoint allows filtering alerts by:
    - metric_ids: Match alerts containing any of these metric IDs in their trigger
    - story_groups: Match alerts containing any of these story groups in their trigger
    - is_active: Match alerts by active status
    - is_published: Match alerts by published status
    - grain: Match alerts by granularity
    """
    alert_filter = AlertFilter(
        metric_ids=metric_ids,
        story_groups=story_groups,
        is_active=is_active,
        is_published=is_published,
        grains=grains,
    )

    results, count = await alert_crud.paginate(
        params=params,
        filter_params=alert_filter.dict(exclude_unset=True),
    )

    return Page.create(items=results, total_count=count, params=params)


# Common ==========


@notification_router.get(
    "/tags",
    response_model=list[str],
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],
)
async def get_tags(
    notifications_crud: CRUDNotificationsDep,
    search: str | None = Query(None, description="Search term for tag suggestions"),
) -> list[str]:
    """Get all unique tags used across alerts and reports"""
    return await notifications_crud.get_unique_tags(search)


@notification_router.get(
    "",
    response_model=Page[NotificationList],
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],
)
async def list_notifications(
    notification_crud: CRUDNotificationsDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    name: Annotated[str | None, Query()] = None,
    notification_type: Annotated[NotificationType | None, Query()] = None,
    channel_type: Annotated[NotificationChannel | None, Query()] = None,
    grain: Annotated[Granularity | None, Query()] = None,
    is_active: Annotated[bool | None, Query()] = None,
    tags: Annotated[list[str] | None, Query()] = None,
    is_published: Annotated[bool | None, Query()] = None,
):
    """
    Retrieve a paginated list of all notifications (alerts and reports).

    Args:
        notification_crud: CRUD dependency for notification channels
        params: Pagination parameters
        name: Filter by notification name
        notification_type: Filter by notification type
        channel_type: Filter by channel type
        grain: Filter by granularity
        is_active: Filter by active status
        tags: Filter by tags (matches any of the provided tags)
        is_published: Filter by published status

    Returns:
        Paginated list of notifications
    """
    notification_filter = NotificationConfigFilter(
        name=name,
        notification_type=notification_type,
        channel_type=channel_type,
        grain=grain,
        is_active=is_active,
        tags=tags,
        is_published=is_published,
    )

    notifications, count = await notification_crud.get_notifications_list(
        params=params, filter_params=notification_filter.model_dump(exclude_unset=True)
    )

    return Page.create(items=notifications, total_count=count, params=params)


@notification_router.patch(
    "/bulk/status",
    status_code=200,
    response_model=BatchStatusUpdateResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_WRITE])],
)
async def bulk_update_status(
    notification_crud: CRUDNotificationsDep,
    background_tasks: BackgroundTasks,
    is_active: bool,
    alert_ids: list[int] | None = None,
    report_ids: list[int] | None = None,
):
    """
    Bulk update the status of alerts and reports.

    This endpoint updates the status of a list of alerts and reports to either active or inactive.
    It first validates the provided alert IDs to ensure they exist in the database. If any IDs are not found,
    it raises an HTTPException with a 404 status code. If all IDs are valid, it proceeds to update the status
    of the alerts and reports in bulk.

    :param report_ids:
    :param notification_crud:
    :param alert_ids: List of alert IDs to update
    :param is_active: Boolean indicating the new status (True for active, False for inactive)
    :param background_tasks: Background task dependency for scheduling status change events
    """
    try:
        await notification_crud.batch_status_update(alert_ids, report_ids, is_active)  # type: ignore
        # Fetch all objects in a single query per type
        alerts, reports = await notification_crud.batch_get(alert_ids, report_ids)  # type: ignore

        # Schedule status change events as background task
        for obj in [*alerts, *reports]:
            background_tasks.add_task(
                publish_event,
                action=EventAction.STATUS_CHANGE,
                sender=obj.__class__,
                timing=EventTiming.AFTER,
                instance=obj,
            )

        return BatchStatusUpdateResponse(
            alerts_updated=len(alerts),
            reports_updated=len(reports),
            total_updated=len(alerts) + len(reports),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@notification_router.delete(
    "/bulk",
    status_code=200,
    response_model=BatchDeleteResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_WRITE])],
)
async def bulk_delete_notifications(
    notification_crud: CRUDNotificationsDep,
    background_tasks: BackgroundTasks,
    alert_ids: list[int] | None = None,
    report_ids: list[int] | None = None,
):
    """
    Bulk delete notifications (alerts/reports) and their associated configurations.

    This endpoint deletes a list of notifications (alerts or reports) and their associated configurations.
    It first validates the provided alert IDs to ensure they exist in the database. If any IDs are not found,
    it raises an HTTPException with a 404 status code. If all IDs are valid, it proceeds to delete the notifications
    and their configurations in bulk.

    :param notification_crud:
    :param report_ids: List of report IDs to delete
    :param alert_ids: List of alert IDs to delete
    :param background_tasks: Background task dependency for scheduling status change events
    :return: BatchDeleteResponse
    """
    try:
        # Fetch all objects in a single query per type
        alerts, reports = await notification_crud.batch_get(alert_ids, report_ids)  # type: ignore
        # Delete all objects in a single query per type
        await notification_crud.batch_delete(alert_ids, report_ids)  # type: ignore

        # Send delete events for all notifications
        for obj in [*alerts, *reports]:
            background_tasks.add_task(
                publish_event,
                action=EventAction.DELETE,
                sender=obj.__class__,
                timing=EventTiming.AFTER,
                instance=obj,
            )
        return BatchDeleteResponse(
            alerts_deleted=len(alerts),
            reports_deleted=len(reports),
            total_deleted=len(alerts) + len(reports),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


#  REPORTS APIS =========


@notification_router.post(
    "/reports",
    status_code=201,
    response_model=ReportDetail,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_WRITE])],
)
async def create_report(
    report_create: ReportRequest,
    report_crud: ReportsCRUDDep,
):
    """
    Creates a new report with its configuration.

    This endpoint creates a new report based on the provided request data. It requires the ALERT_REPORT_WRITE scope
    for authentication.
    The report creation includes setting up its configuration, such as schedule, metrics, and notification channels.

    :param report_create: The request data for creating a report.
    :param report_crud: Dependency for CRUD operations on reports.
    :return: The newly created report.
    """
    return await report_crud.create(
        report_create=report_create,
    )


@notification_router.get(
    "/reports/{report_id}",
    response_model=ReportDetail,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],  # type: ignore
)
async def get_report(
    report_id: int,
    report_crud: ReportsCRUDDep,
):
    """
    Retrieves a report by its ID.

    This endpoint fetches a report by its ID. It requires the ALERT_REPORT_READ scope for authentication.

    :param report_id: The ID of the report to retrieve.
    :param report_crud: Dependency for CRUD operations on reports.
    :return: The report details.
    """
    return await report_crud.get(report_id)


@notification_router.post(
    "/reports/{report_id}/publish",
    status_code=200,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_WRITE])],
)
async def publish_report(
    report_id: int,
    report_crud: ReportsCRUDDep,
):
    """
    Publish a draft report with notification channels.

    This endpoint publishes a draft report, making it active and ready for distribution through its associated
    notification channels. It requires the ALERT_REPORT_READ scope for authentication.

    :param report_crud: Dependency for CRUD operations on reports.
    :param report_id: The ID of the report to publish.
    """
    try:
        return await report_crud.publish(report_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except PrefectOperationError as e:
        # The PrefectOperationError is already a ServiceError subclass with proper status code and format
        raise e


@notification_router.patch(
    "/reports/{report_id}",
    response_model=ReportDetail,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_WRITE])],
)
async def update_report(
    report_id: int,
    report_update: ReportRequest,
    report_crud: ReportsCRUDDep,
):
    """
    Update an existing report and its notification configs.

    This endpoint updates an alert and its associated channels. Channels not included
    in the update will be deleted. It requires the ALERT_REPORT_WRITE scope for authentication.

    Args:
        alert_id: The ID of the alert to update
        alert_update: The updated alert data
        alert_crud: Dependency for CRUD operations on alerts

    Returns:
        Updated alert with its notification channels
        :param report_crud: Dependency for CRUD operations on reports
        :param report_update: The updated report data
        :param report_id: The ID of the report to update
    """
    return await report_crud.update_report(
        report_id=report_id,
        report_update=report_update,
    )


@notification_router.post(
    "/reports/preview",
    response_model=PreviewResponse,
    status_code=200,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],
)
async def preview_report(
    report_data: ReportRequest,
    preview_service: ReportPreviewServiceDep,
) -> PreviewResponse:
    """
    Preview an alert with rendered notification templates.

    This endpoint generates a preview of how the alert notifications will look
    without actually creating the alert or sending notifications.

    :param report_data: The report data to preview
    :param preview_service: Service for generating previews
    :return: Preview of email and/or slack notifications
    """
    return await preview_service.preview(report_data)  # type: ignore


@notification_router.post(
    "/executions",
    status_code=201,
    response_model=NotificationExecutionRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_WRITE])],
)
async def create_notification_execution(
    execution: NotificationExecutionCreate,
    execution_crud: ExecutionCRUDDep,
):
    """
    Create a new notification execution record.

    Args:
        execution: The execution details to record
        execution_crud: CRUD dependency for executions
    """
    return await execution_crud.create(obj_in=execution)


@notification_router.get(
    "/executions",
    response_model=Page[NotificationExecutionRead],
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],
)
async def list_notification_executions(
    execution_crud: ExecutionCRUDDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    notification_type: Annotated[NotificationType | None, Query()] = None,
    status: Annotated[ExecutionStatus | None, Query()] = None,
    alert_id: Annotated[int | None, Query()] = None,
    report_id: Annotated[int | None, Query()] = None,
    start_date: Annotated[datetime | None, Query()] = None,
    end_date: Annotated[datetime | None, Query()] = None,
):
    """
    Retrieve a paginated list of notification executions with optional filtering.

    Args:
        execution_crud: CRUD dependency for executions
        params: Pagination parameters
        notification_type: Filter by notification type (ALERT/REPORT)
        status: Filter by execution status
        alert_id: Filter by alert ID
        report_id: Filter by report ID
        start_date: Filter executions after this date
        end_date: Filter executions before this date
    """
    filter_instance = NotificationExecutionFilter(
        notification_type=notification_type,
        status=status,
        alert_id=alert_id,
        report_id=report_id,
        start_date=start_date,
        end_date=end_date,
    )
    executions, count = await execution_crud.paginate(
        params=params,
        filter_params=filter_instance.model_dump(exclude_unset=True),
    )

    return Page.create(items=executions, total_count=count, params=params)


@notification_router.get(
    "/executions/{execution_id}",
    response_model=NotificationExecutionRead,
    dependencies=[Security(oauth2_auth().verify, scopes=[ALERT_REPORT_READ])],
)
async def get_notification_execution(
    execution_id: int,
    execution_crud: ExecutionCRUDDep,
):
    """
    Retrieve a specific notification execution by ID.

    Args:
        execution_id: The ID of the execution to retrieve
        execution_crud: CRUD dependency for executions
    """
    return await execution_crud.get(id=execution_id)
