from typing import Any

from sqlalchemy import func, select, union_all

from commons.utilities.context import get_tenant_id
from commons.utilities.pagination import PaginationParams
from insights_backend.core.crud import CRUDAlert, CRUDNotificationChannel
from insights_backend.core.filters import NotificationFilter
from insights_backend.core.models import Alert
from insights_backend.core.models.notifications import NotificationChannelConfig, NotificationStatus
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

    def _build_alerts_query(self, filter_params: dict[str, Any]) -> select:
        """Build base query for alerts listing"""
        query = (
            select(
                Alert.id,
                Alert.name,
                Alert.type,
                Alert.grain,
                Alert.summary.label("trigger_schedule"),  # type: ignore
                Alert.tags,
                Alert.is_active,
                func.count(NotificationChannelConfig.id).label("recipients_count"),
            )
            .outerjoin(NotificationChannelConfig, Alert.id == NotificationChannelConfig.alert_id)
            .group_by(Alert.id, Alert.name, Alert.type, Alert.grain, Alert.summary, Alert.tags, Alert.is_active)
            .where(Alert.tenant_id == get_tenant_id())
        )

        query = NotificationFilter.apply_filters(query, filter_params)

        return query

    def _build_reports_query(self) -> select:
        """Build base query for reports listing"""
        # Will be implemented with Report model
        pass

    def _build_combined_query(self, params: PaginationParams, filter_params: dict[str, Any]) -> select:
        """Combine alerts and reports queries with pagination"""
        combined_query = union_all(
            self._build_alerts_query(filter_params),
            # self._build_reports_query()
        ).alias("notifications")

        return select(combined_query).offset(params.offset).limit(params.limit)

    async def _get_total_count(self) -> int:
        """Get total count of all notifications"""
        alerts_count = await self.alert_crud.session.scalar(select(func.count()).select_from(Alert)) or 0

        # reports_count = await self.report_crud.session.scalar(
        #     select(func.count()).select_from(Report)
        # ) or 0
        # return alerts_count + reports_count

        return alerts_count

    async def get_notifications(
        self, params: PaginationParams, filter_params: dict[str, Any]
    ) -> tuple[list[NotificationList], int]:
        """
        Get paginated list of notifications combining alerts and reports
        """
        query = self._build_combined_query(params, filter_params)
        total = await self._get_total_count()

        results = await self.alert_crud.session.execute(query)
        # notifications = results.scalars().all()

        # notifications = results.fetchall()

        notifications = [
            NotificationList(
                id=row.id,
                name=row.name,
                type=row.type,
                grain=row.grain,
                trigger_schedule=row.trigger_schedule,
                tags=row.tags,
                last_execution=None,
                recipients_count=row.recipients_count,
                status=NotificationStatus.ACTIVE if row.is_active else NotificationStatus.INACTIVE,
            )
            for row in results
        ]

        return notifications, total
