import asyncio
from typing import Any

from sqlalchemy import (
    delete,
    func,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.crud import CRUDBase, ModelType, NotFoundError
from commons.utilities.context import get_tenant_id
from commons.utilities.pagination import PaginationParams
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import Alert, NotificationChannelConfig
from insights_backend.notifications.schemas import AlertRequest, AlertWithChannelsResponse
from insights_backend.notifications.services.template_service import TemplateService


class CRUDNotificationChannel(
    CRUDBase[NotificationChannelConfig, NotificationChannelConfig, NotificationChannelConfig, None]
):
    """
    CRUD operations for NotificationChannelConfig model.
    This class provides methods for creating, retrieving, updating, and deleting notification channels.
    """

    def __init__(self, model: type[ModelType], session: AsyncSession, template_service: TemplateService):
        super().__init__(model, session)
        self.template_service = template_service

    async def batch_create_or_update(
        self,
        *,
        notification_configs: list[NotificationChannelConfig],
    ) -> list[NotificationChannelConfig]:
        """
        Creates notification channels for alerts or reports.

        Args:
            notification_configs: List of notification channel configurations

        Returns:
            List of created NotificationChannelConfig objects
        """

        for config in notification_configs:
            config.template = self.template_service.prepare_channel_template(
                config.notification_type, config.channel_type
            )

            tenant_id = get_tenant_id()
            stmt = insert(self.model).values(**config.model_dump(exclude={"id"}), tenant_id=tenant_id)
            # Determine constraint based on notification type
            id_field = "alert_id" if config.notification_type == NotificationType.ALERT else "report_id"
            stmt = stmt.on_conflict_do_update(
                index_elements=["notification_type", "channel_type", id_field], set_=config.model_dump()
            )
            await self.session.execute(stmt)
        # Commit the session to save all created channels
        await self.session.commit()

    async def get_by_id(self, alert_id: int | None = None) -> list[NotificationChannelConfig] | None:
        """Retrieves notification channels for an alert"""
        # Build conditions for alerts and reports
        if alert_id:
            result = await self.session.execute(select(self.model).filter_by(alert_id=alert_id))
            channels = result.scalars().all()
            return channels
        return

    async def batch_delete(self, alert_ids: list[int] | None = None, report_ids: list[int] | None = None) -> None:
        """Deletes all notification channels for one or multiple alerts"""

        if alert_ids:
            await self.session.execute(delete(self.model).where(NotificationChannelConfig.alert_id.in_(alert_ids)))

        if report_ids:
            await self.session.execute(delete(self.model).where(NotificationChannelConfig.report_id.in_(report_ids)))
        # Commit the session to save the changes
        await self.session.commit()


class CRUDAlert(CRUDBase[Alert, AlertRequest, None, None]):
    """
    CRUD operations for the Alert model.
    """

    def __init__(self, model: type[ModelType], session: AsyncSession, notification_crud: CRUDNotificationChannel):
        super().__init__(model, session)
        self.notification_crud = notification_crud

    async def create(self, *, alert_data: AlertRequest) -> Any:
        """Creates a new Alert with its associated notification channels."""

        alert = Alert(
            **alert_data.model_dump(exclude={"notification_channels"}),
            type=NotificationType.ALERT,
        )

        # Add the new Alert to the session
        self.session.add(alert)
        # Flush the session to save the changes
        await self.session.flush()

        # # notification_channel = alert_data.notification_channels
        channels = alert_data.notification_channels
        # Create notification channels
        await self.notification_crud.batch_create_or_update(
            notification_configs=[
                NotificationChannelConfig(**channel.model_dump(), alert_id=alert.id, notification_type=alert.type)
                for channel in channels
            ],
        )

        # Commit the session to persist the changes
        await self.session.commit()
        await self.session.refresh(alert)
        return alert

    async def batch_delete(self, ids: list[int]) -> None:
        """Deletes one or multiple Alerts by their IDs."""

        await self.notification_crud.batch_delete(ids)
        await self.session.execute(delete(Alert).where(Alert.id.in_(ids)))
        await self.session.commit()

    async def batch_status_update(self, alert_ids: list[int], is_active: bool) -> None:
        """Updates the active status of one or multiple Alerts."""

        # Construct the SQL statement to update Alerts' active status
        await self.session.execute(update(Alert).where(Alert.id.in_(alert_ids)).values(is_active=is_active))
        # Commit the session to save the changes
        await self.session.commit()

    async def update(
        self,
        *,
        alert_id: int,
        update_data: AlertRequest,
    ) -> Alert:
        """Updates an existing Alert"""
        alert = await self.get(alert_id)
        if not alert:
            raise ValueError(f"Alert with id {alert_id} not found")

        await self.session.execute(
            update(Alert)
            .filter_by(id=alert_id)
            .values(**update_data.model_dump(exclude={"notification_channels"}, exclude_unset=True))
        )

        # Delete all existing channels
        await self.session.execute(
            delete(NotificationChannelConfig).where(NotificationChannelConfig.alert_id == alert_id)
        )

        # Create new channels
        channels = update_data.notification_channels
        # Create notification channels
        await self.notification_crud.batch_create_or_update(
            notification_configs=[
                NotificationChannelConfig(
                    **channel.model_dump(), alert_id=alert.id, notification_type=NotificationType.ALERT
                )
                for channel in channels
            ],
        )

        await self.session.refresh(alert)
        return alert

    async def publish(
        self,
        alert_id: int,
    ) -> Alert:
        """Publish a draft alert with notification channels"""
        alert = await self.get(alert_id)
        if not alert:
            raise NotFoundError(f"Alert with id {alert_id} not found")

        if not alert.is_publishable():
            raise ValueError(
                "Alert cannot be published. Ensure it has a name, trigger, and at least one notification " "channel."
            )

        if alert.is_published:
            raise ValueError(f"Alert with id {alert_id} is already published")

        alert.is_published = True
        self.session.add(alert)
        await self.session.commit()
        await self.session.refresh(alert)

        return alert

    async def get_unique_tags(self) -> list[str]:
        """
        Retrieve unique tags across all alerts.

        Returns:
            List of unique tags sorted alphabetically
        """
        # Using func.unnest to flatten the tags array
        statement = (
            select(func.unnest(Alert.tags).label("tag")).where(Alert.tags.is_not(None)).distinct().order_by("tag")
        )

        result = await self.session.execute(statement)
        return [row[0] for row in result.fetchall()]

    async def validate_ids(self, ids: list[int]) -> set[int]:
        """
        Find which IDs from the input list don't exist in the database.

        Args:
            ids: List of alert IDs to check

        Returns:
            Set of IDs that don't exist in the database
        """
        statement = select(Alert.id).where(Alert.id.in_(ids))
        result = await self.session.execute(statement)
        found_ids = {row[0] for row in result.fetchall()}
        return set(ids) - found_ids

    async def get_alerts_list(self, params: PaginationParams) -> tuple[list[Any], int]:
        """
        Get paginated alerts with notification counts and total count.

        Args:
            params: Pagination parameters

        Returns:
            Tuple of (list of results, total count)
        """
        # Build the main query with counts
        query = (
            select(
                Alert.id,
                Alert.name,
                Alert.type,
                Alert.grain,
                Alert.summary.label("summary"),  # type: ignore
                Alert.tags,
                Alert.is_active.label("status"),  # type: ignore
                func.count(NotificationChannelConfig.id).label("recipients_count"),
            )
            .outerjoin(NotificationChannelConfig, Alert.id == NotificationChannelConfig.alert_id)
            .group_by(Alert.id, Alert.name, Alert.type, Alert.grain, Alert.summary, Alert.tags, Alert.is_active)
            .offset(params.offset)
            .limit(params.limit)
        )

        # Get total count and results in parallel
        count_query = select(func.count()).select_from(Alert)
        count_result, results = await asyncio.gather(self.session.scalar(count_query), self.session.execute(query))

        return list(results), count_result or 0

    async def get_alert_with_channels(self, alert_id: int) -> AlertWithChannelsResponse:
        """Get alert by ID with its notification channels."""

        alert = await self.get(alert_id)
        if not alert:
            raise NotFoundError(f"Alert with id {alert_id} not found")

        channels = await self.notification_crud.get_by_id(alert_id)

        # Create response with alert data and channels
        return AlertWithChannelsResponse(
            **alert.model_dump(), notification_channels=[channel.model_dump() for channel in channels]
        )
