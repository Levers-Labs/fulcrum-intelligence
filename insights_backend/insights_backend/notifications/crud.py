import asyncio
from typing import Any

from sqlalchemy import (
    delete,
    func,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.crud import CRUDBase, ModelType, NotFoundError
from commons.utilities.context import get_tenant_id
from commons.utilities.pagination import PaginationParams
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.filters import NotificationConfigFilter
from insights_backend.notifications.models import Alert, NotificationChannelConfig, Report
from insights_backend.notifications.schemas import AlertDetailResponse, AlertRequest
from insights_backend.notifications.services.template_service import TemplateService


class CRUDNotifications:
    """
    Common CRUD operations for notifications (alerts and reports).
    Contains shared notification functionality.
    """

    def __init__(self, session: AsyncSession):
        self.session = session
        self.filter_class = NotificationConfigFilter

    async def get_notifications_list(
        self, params: PaginationParams, filter_params: dict[str, Any] | None = None
    ) -> tuple[list[Any], int]:
        """
        Get paginated list of notifications with filters.

        Args:
            params: Pagination parameters
            filter_params: Optional filter parameters

        Returns:
            Tuple of (list of results, total count)
        """
        # Build base query for alerts first
        base_query = (
            select(
                Alert.id,
                Alert.name,
                Alert.type,
                Alert.grain,
                Alert.summary,
                Alert.tags,
                Alert.is_active.label("status"),  # type: ignore
                func.count(NotificationChannelConfig.id).label("recipients_count"),  # type: ignore
            )
            .outerjoin(NotificationChannelConfig, Alert.id == NotificationChannelConfig.alert_id)  # type: ignore
            .group_by(Alert.id, Alert.name, Alert.type, Alert.grain, Alert.summary, Alert.tags, Alert.is_active)
        )

        # Apply filters if provided
        if filter_params:
            base_query = self.filter_class.apply_filters(base_query, filter_params)

        # Add pagination
        query = base_query.offset(params.offset).limit(params.limit)

        # Get total count from base query
        count_query = select(func.count()).select_from(base_query.subquery())

        # Execute queries
        count_result, results = await asyncio.gather(self.session.scalar(count_query), self.session.execute(query))

        return list(results), count_result or 0

    async def get_unique_tags(self, notification_type: NotificationType | None = None) -> list[str]:
        """
        Retrieve unique tags across all notifications (alerts and/or reports).

        Args:
            notification_type: Optional filter for specific notification type

        Returns:
            List of unique tags sorted alphabetically
        """
        model = Alert if notification_type == NotificationType.ALERT else Report

        statement = (
            select(func.unnest(model.tags).label("tag"))
            .where(model.tags.is_not(None))  # type: ignore
            .distinct()
            .order_by("tag")
        )

        result = await self.session.execute(statement)
        return [row[0] for row in result.fetchall()]


class CRUDNotificationChannelConfig(
    CRUDBase[NotificationChannelConfig, NotificationChannelConfig, NotificationChannelConfig, None]  # type: ignore
):
    """
    CRUD operations for NotificationChannelConfig model.
    This class provides methods for creating, retrieving, updating, and deleting notification channels.
    """

    def __init__(self, model: type[ModelType], session: AsyncSession, template_service: TemplateService):
        super().__init__(model, session)  # type: ignore
        self.template_service = template_service

    async def batch_create_or_update(
        self,
        *,
        notification_configs: list[NotificationChannelConfig],
    ):
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

    async def batch_delete(self, ids: list[int], notification_type: NotificationType) -> None:
        """Deletes all notification channels for one or multiple alerts"""
        notification_id = "alert_id" if notification_type == NotificationType.ALERT else "report_id"
        await self.session.execute(
            delete(self.model).where(getattr(NotificationChannelConfig, notification_id).in_(ids))  # type: ignore
        )


class CRUDAlert(CRUDBase[Alert, AlertRequest, None, None]):  # type: ignore
    """
    CRUD operations for the Alert model.
    """

    def __init__(
        self, model: type[ModelType], session: AsyncSession, notification_config_crud: CRUDNotificationChannelConfig
    ):
        super().__init__(model, session)  # type: ignore
        self.notification_config_crud = notification_config_crud

    def get_select_query(self) -> select:  # type: ignore
        """Base select query with all relationships loaded"""
        return select(self.model).options(selectinload(self.model.notification_channels))  # type: ignore

    async def get_alert_details(self, alert_id: int) -> AlertDetailResponse:
        """Get alert by ID with its notification channels."""
        statement = self.get_select_query().where(self.model.id == alert_id)  # type: ignore
        results = await self.session.execute(statement=statement)
        alert = results.scalar_one_or_none()
        if not alert:
            raise NotFoundError(f"Alert with id {alert_id} not found")

        return alert

    async def create(self, *, alert_create: AlertRequest) -> Any:  # type: ignore
        """Creates a new Alert with its associated notification channels."""

        alert = Alert(
            **alert_create.model_dump(exclude={"notification_channels"}),
            type=NotificationType.ALERT,
        )

        # Add the new Alert to the session
        self.session.add(alert)
        # Flush the session to save the changes
        await self.session.flush()

        # # notification_channel = alert_data.notification_channels
        channels = alert_create.notification_channels
        # Create notification channels
        await self.notification_config_crud.batch_create_or_update(
            notification_configs=[
                NotificationChannelConfig(
                    **channel.model_dump(), alert_id=alert.id, notification_type=alert.type  # type: ignore
                )
                for channel in channels
            ],
        )

        # Commit the session to persist the changes
        await self.session.commit()
        await self.session.refresh(alert)
        return alert

    async def batch_delete(self, ids: list[int]) -> None:
        """Deletes one or multiple Alerts by their IDs."""
        await self.session.execute(delete(self.model).where(Alert.id.in_(ids)))  # type: ignore
        await self.session.commit()

    async def batch_status_update(self, alert_ids: list[int], is_active: bool) -> None:
        """Updates the active status of one or multiple Alerts."""

        # Construct the SQL statement to update Alerts' active status
        await self.session.execute(
            update(Alert).where(Alert.id.in_(alert_ids)).values(is_active=is_active)  # type: ignore
        )
        # Commit the session to save the changes
        await self.session.commit()

    async def update(  # type: ignore
        self,
        *,
        alert_id: int,
        alert_update: AlertRequest,
    ) -> AlertDetailResponse:
        """Updates an existing Alert with smart channel management"""
        # Get alert with its channels
        alert = await self.get_alert_details(alert_id)

        # Update alert attributes
        await self.session.execute(
            update(Alert)
            .filter_by(id=alert_id)
            .values(**alert_update.model_dump(exclude={"notification_channels"}, exclude_unset=True))
        )

        # Map existing channels by their unique identifiers
        current_channel_map = {(c.channel_type, NotificationType.ALERT): c for c in alert.notification_channels}

        # Set of channel types in the update request
        requested_channel_types = {c.channel_type for c in alert_update.notification_channels}

        # Prepare channel configurations, preserving existing IDs where applicable
        channel_configs = [
            NotificationChannelConfig(
                **(
                    channel.model_dump()
                    | {
                        "id": current_channel_map.get(
                            (channel.channel_type, NotificationType.ALERT), {}
                        ).id,  # type: ignore
                        "alert_id": alert.id,
                        "notification_type": NotificationType.ALERT,
                    }
                )
            )
            for channel in alert_update.notification_channels
        ]

        # Batch upsert channel configurations
        if channel_configs:
            await self.notification_config_crud.batch_create_or_update(notification_configs=channel_configs)

        # Use current_channel_map that we already created earlier
        obsolete_channel_ids = [
            c.id for c in current_channel_map.values() if c.channel_type not in requested_channel_types
        ]

        if obsolete_channel_ids:
            await self.session.execute(
                delete(NotificationChannelConfig).where(
                    NotificationChannelConfig.id.in_(obsolete_channel_ids)  # type: ignore
                )
            )

        # await self.session.commit()
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
                "Alert cannot be published. Alert must be active and have: name, trigger, grain and notification "
                "channel config."
            )

        if alert.is_published:
            raise ValueError(f"Alert with id {alert_id} is already published")

        alert.is_published = True
        self.session.add(alert)
        await self.session.commit()
        await self.session.refresh(alert)

        return alert

    async def validate_ids(self, ids: list[int]) -> set[int]:
        """
        Find which IDs from the input list don't exist in the database.

        Args:
            ids: List of alert IDs to check

        Returns:
            Set of IDs that don't exist in the database
        """
        statement = select(Alert.id).where(Alert.id.in_(ids))  # type: ignore
        result = await self.session.execute(statement)
        found_ids = {row[0] for row in result.fetchall()}
        return set(ids) - found_ids
