from datetime import datetime
from typing import Tuple

from sqlalchemy import func, select

from commons.utilities.pagination import PaginationParams
from insights_backend.core.crud import CRUDAlert, CRUDNotificationChannel
from insights_backend.core.models import Alert
from insights_backend.core.models.notifications import NotificationChannelConfig
from insights_backend.core.schemas import NotificationList


class NotificationListService:
    """
    Service to handle combined listing of notifications (alerts and reports)
    """

    def __init__(
        self,
        alert_crud: CRUDAlert,
        notification_crud: CRUDNotificationChannel,
        # TODO: Add report_crud when implemented
    ):
        """
        Initialize the NotificationListService with CRUD instances for alerts and notification channels.
        """
        self.alert_crud = alert_crud
        self.notification_crud = notification_crud
        # TODO: Add report_crud instance

    async def _get_alerts_query(self) -> select:
        """
        Build base query for alerts listing
        """
        return (
            select(
                Alert.id,
                Alert.name,
                Alert.type,
                Alert.grain,
                Alert.summary.label("trigger_schedule"),  # type: ignore
                # Alert.created_by,
                Alert.tags,
                Alert.is_active,
                func.count(NotificationChannelConfig.id).label("recipients_count"),
            )
            .outerjoin(NotificationChannelConfig, Alert.id == NotificationChannelConfig.alert_id)
            .group_by(Alert.id)
        )

    async def _get_alerts_data(self, params: PaginationParams) -> tuple[list[NotificationList], int]:
        """
        Get paginated alerts data
        """
        query = await self._get_alerts_query()

        # Get total count
        count_query = select(func.count()).select_from(query.subquery())  # type: ignore
        total = await self.alert_crud.session.scalar(count_query)

        # Get paginated results
        results = await self.alert_crud.session.execute(query.offset(params.offset).limit(params.limit))  # type: ignore

        notifications = []
        for row in results:
            notifications.append(
                NotificationList(
                    id=row.id,
                    name=row.name,
                    type=row.type,
                    grain=row.grain,
                    trigger_schedule=row.trigger_schedule,
                    # created_by=row.created_by,
                    tags=row.tags,
                    last_execution=datetime.now(),  # todo: update after execution integration
                    recipients_count=row.recipients_count,
                    status="Active" if row.is_active else "Inactive",
                )
            )

        return notifications, total or 0

    # TODO: Add methods for reports when implemented
    # async def _get_reports_query(self) -> select:
    #     """Build base query for reports listing"""
    #     pass

    # async def _get_reports_data(self, params: PaginationParams) -> Tuple[list[NotificationList], int]:
    #     """Get paginated reports data"""
    #     pass

    async def get_notifications(
        self,
        params: PaginationParams,
    ) -> tuple[list[NotificationList], int]:
        """
        Get paginated list of notifications combining alerts and reports
        """
        # Get alerts data
        notifications, total = await self._get_alerts_data(params)

        # TODO: When reports are implemented
        # 1. Get reports data
        # 2. Combine with alerts data
        # 3. Handle pagination for combined results
        # reports, reports_total = await self._get_reports_data(params)
        # notifications.extend(reports)
        # total += reports_total

        return notifications, total
