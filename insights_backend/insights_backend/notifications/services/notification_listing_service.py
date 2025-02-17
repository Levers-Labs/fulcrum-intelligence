from commons.utilities.pagination import PaginationParams
from insights_backend.notifications.crud import CRUDAlert
from insights_backend.notifications.schemas import NotificationList


class NotificationListService:
    """Service to handle combined listing of notifications (alerts and reports)"""

    def __init__(
        self,
        alert_crud: CRUDAlert,
    ):
        self.alert_crud = alert_crud

    async def get_notifications_list(self, params: PaginationParams) -> tuple[list[NotificationList], int]:
        """
        Get combined list of notifications from all sources.

        Args:
            params: Pagination parameters

        Returns:
            Tuple of (list of notifications, total count)
        """
        # Get alerts with counts
        alert_results, total_alerts = await self.alert_crud.get_alerts_list(params)

        notifications = [NotificationList.model_validate(row) for row in alert_results]

        return notifications, total_alerts
