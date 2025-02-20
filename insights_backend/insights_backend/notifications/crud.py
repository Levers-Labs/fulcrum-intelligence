import asyncio
from typing import Any

from sqlalchemy import (
    and_,
    delete,
    func,
    select,
    union,
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
from insights_backend.notifications.schemas import (
    AlertDetail,
    AlertRequest,
    ReportDetail,
    ReportRequest,
)
from insights_backend.notifications.services.template_service import TemplateService


class CRUDNotifications:
    """
    Common CRUD operations for notifications (alerts and reports).
    Contains shared notification functionality.
    """

    filter_class = NotificationConfigFilter

    def __init__(self, session: AsyncSession):
        self.session = session

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

    async def get_unique_tags(self) -> list[str]:
        """
        Retrieve unique tags across all notifications (both alerts and reports).

        Returns:
            List of unique tags sorted alphabetically
        """
        # Create subqueries for both Alert and Report tags
        alert_tags = select(func.unnest(Alert.tags).label("tag")).where(  # type: ignore
            Alert.tags.is_not(None)  # type: ignore
        )

        report_tags = select(func.unnest(Report.tags).label("tag")).where(  # type: ignore
            Report.tags.is_not(None)  # type: ignore
        )

        # Combine using UNION and add ordering
        statement = union(alert_tags, report_tags).select().distinct().order_by("tag")

        result = await self.session.execute(statement)
        return [row[0] for row in result.fetchall()]

    async def _validate_ids(self, ids: list[int], model: type[ModelType]) -> set[int]:
        """
        Find which IDs from the input list don't exist in the database.

        Args:
            ids: List of alert IDs to check

        Returns:
            Set of IDs that don't exist in the database
            :param model:
        """
        statement = select(model.id).where(model.id.in_(ids))  # type: ignore
        result = await self.session.execute(statement)
        found_ids = {row[0] for row in result.fetchall()}
        return set(ids) - found_ids

    async def _delete(self, model: type[ModelType], ids: list[int]):
        invalid_ids = await self._validate_ids(ids, model)
        if invalid_ids:
            raise ValueError(f"Data not found for {str(model.__tablename__)}: {list(invalid_ids)}")
        await self.session.execute(delete(model).where(model.id.in_(ids)))  # type: ignore

    async def batch_delete(self, alert_ids: list[int], report_ids: list[int]) -> None:
        """Deletes all notification channels for one or multiple alerts / reports"""
        if not alert_ids and not report_ids:
            return
        if alert_ids:
            await self._delete(Alert, alert_ids)
        if report_ids:
            await self._delete(Report, report_ids)
        await self.session.commit()

    async def _update_status(self, model: type[ModelType], ids: list[int], status: bool):
        invalid_ids = await self._validate_ids(ids, model)
        if invalid_ids:
            raise ValueError(f"Data not found for {str(model.__tablename__)}: {list(invalid_ids)}")
        await self.session.execute(update(model).where(model.id.in_(ids)).values(is_active=status))  # type: ignore

    async def batch_status_update(self, alert_ids: list[int], report_ids: list[int], is_active: bool) -> None:
        """Updates the active status of one or multiple Alerts."""

        if not alert_ids and not report_ids:
            return
        if alert_ids:
            await self._update_status(Alert, alert_ids, is_active)
        if report_ids:
            await self._update_status(Report, report_ids, is_active)
        # Commit the session to save the changes
        await self.session.commit()


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
        Creates or updates notification channels for alerts or reports.

        Args:
            notification_configs: List of notification channel configurations
        """
        tenant_id = get_tenant_id()

        for config in notification_configs:
            # Add template and tenant_id
            config.template = self.template_service.prepare_channel_template(
                config.notification_type, config.channel_type
            )

            # Prepare values for both insert and update
            values = config.model_dump(exclude={"id"})
            values["tenant_id"] = tenant_id

            # Determine constraint based on notification type
            id_field = "alert_id" if config.notification_type == NotificationType.ALERT else "report_id"

            stmt = (
                insert(self.model)
                .values(**values)
                .on_conflict_do_update(index_elements=["notification_type", "channel_type", id_field], set_=values)
            )

            await self.session.execute(stmt)

        await self.session.commit()


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

    async def update_alert(
        self,
        *,
        alert_id: int,
        alert_update: AlertRequest,
    ) -> AlertDetail:
        # Get alert with its channels
        alert = await self.get(alert_id)

        # Update alert attributes
        await self.session.execute(
            update(Alert)
            .filter_by(id=alert_id)
            .values(**alert_update.model_dump(exclude={"notification_channels"}, exclude_unset=True))
        )

        channels = alert_update.notification_channels
        if channels:
            # Create or Update notification channels
            channel_configs = [
                NotificationChannelConfig(
                    channel_type=channel.channel_type,
                    alert_id=alert.id,
                    notification_type=alert.type,  # type: ignore
                    recipients=channel.recipients,
                )
                for channel in channels
            ]
            await self.notification_config_crud.batch_create_or_update(notification_configs=channel_configs)

            # Get current channel types and delete the ones not in update
            upserted_channel_types = {c.channel_type for c in channel_configs}
            await self.session.execute(
                delete(NotificationChannelConfig).where(
                    and_(
                        NotificationChannelConfig.alert_id == alert_id,  # type: ignore
                        NotificationChannelConfig.channel_type.not_in(upserted_channel_types),  # type: ignore
                    )
                )
            )

        await self.session.commit()
        return await self.get(alert_id)  # type: ignore

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


class CRUDReport(CRUDBase[Report, ReportRequest, None, None]):  # type: ignore
    """
    CRUD operations for the Report model.
    """

    def __init__(
        self, model: type[ModelType], session: AsyncSession, notification_config_crud: CRUDNotificationChannelConfig
    ):
        super().__init__(model, session)  # type: ignore
        self.notification_config_crud = notification_config_crud

    def get_select_query(self) -> select:  # type: ignore
        """Base select query with all relationships loaded"""
        return select(self.model).options(selectinload(self.model.notification_channels))  # type: ignore

    async def create(self, *, report_create: ReportRequest) -> Any:  # type: ignore
        """Creates a new Report with its associated notification channels."""

        report = Report(
            **report_create.model_dump(exclude={"notification_channels"}),
            type=NotificationType.REPORT,
        )

        # Add the new Report to the session
        self.session.add(report)
        # Flush the session to save the changes
        await self.session.flush()

        channels = report_create.notification_channels
        # Create notification channels
        await self.notification_config_crud.batch_create_or_update(
            notification_configs=[
                NotificationChannelConfig(
                    **channel.model_dump(), report_id=report.id, notification_type=report.type  # type: ignore
                )
                for channel in channels
            ],
        )

        # Commit the session to persist the changes
        await self.session.commit()
        await self.session.refresh(report)
        return report

    async def publish(
        self,
        report_id: int,
    ) -> Report:
        """Publish a draft report with notification channels"""
        report = await self.get(report_id)
        if not report:
            raise NotFoundError(f"Report with id {report_id} not found")

        if not report.is_publishable():
            raise ValueError(
                "Report cannot be published. Report must be active and have: Schedule, Report Config and notification "
                "channel config."
            )

        if report.is_published:
            raise ValueError(f"Report with id {report_id} is already published")

        report.is_published = True
        self.session.add(report)
        await self.session.commit()
        await self.session.refresh(report)

        return report

    async def update_report(
        self,
        *,
        report_id: int,
        report_update: ReportRequest,
    ) -> ReportDetail:
        """Updates an existing Report with smart channel management"""

        # Get report with its channels config
        report = await self.get(report_id)

        # Update report attributes
        await self.session.execute(
            update(Report)
            .filter_by(id=report_id)
            .values(**report_update.model_dump(exclude={"notification_channels"}, exclude_unset=True))
        )

        channels = report_update.notification_channels
        if channels:
            # Create or Update notification channels
            channel_configs = [
                NotificationChannelConfig(
                    channel_type=channel.channel_type,
                    report_id=report.id,
                    notification_type=report.type,  # type: ignore
                    recipients=channel.recipients,
                )
                for channel in channels
            ]
            await self.notification_config_crud.batch_create_or_update(notification_configs=channel_configs)

            # Get current channel types and delete the ones not in update
            upserted_channel_types = {c.channel_type for c in channel_configs}
            await self.session.execute(
                delete(NotificationChannelConfig).where(
                    and_(
                        NotificationChannelConfig.report_id == report_id,  # type: ignore
                        NotificationChannelConfig.channel_type.not_in(upserted_channel_types),  # type: ignore
                    )
                )
            )

        await self.session.commit()
        return await self.get(report_id)  # type: ignore
