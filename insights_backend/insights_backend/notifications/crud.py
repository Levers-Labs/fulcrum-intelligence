import asyncio
from typing import Any, TypeVar

from sqlalchemy import (
    and_,
    column,
    delete,
    distinct,
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
from insights_backend.notifications.filters import AlertFilter, NotificationConfigFilter, NotificationExecutionFilter
from insights_backend.notifications.models import (
    Alert,
    NotificationChannelConfig,
    NotificationExecution,
    NotificationExecutionCreate,
    Report,
)
from insights_backend.notifications.schemas import (
    AlertDetail,
    AlertRequest,
    NotificationList,
    ReportDetail,
    ReportRequest,
)
from insights_backend.notifications.services.template_service import TemplateService

NotificationModel = TypeVar("NotificationModel", Alert, Report)


class CRUDNotifications:
    """
    Common CRUD operations for notifications (alerts and reports).
    Contains shared notification functionality.
    """

    filter_class = NotificationConfigFilter

    def __init__(self, session: AsyncSession):
        self.session = session

    def _create_base_query(self, model: type[NotificationModel]) -> select:  # type: ignore
        """
        Creates a base query for either Alert or Report models with all necessary joins and aggregations.

        Args:
            model: The model class (Alert or Report)

        Returns:
            SQLAlchemy select query with all required fields
        """
        # Determine the correct foreign key relationships based on model type
        model_id = model.id
        execution_fk = NotificationExecution.alert_id if model == Alert else NotificationExecution.report_id
        config_fk = NotificationChannelConfig.alert_id if model == Alert else NotificationChannelConfig.report_id
        notification_type = NotificationType.ALERT if model == Alert else NotificationType.REPORT

        return (
            select(  # type: ignore
                model.id,
                model.name,
                model.type,
                model.grain,
                model.summary,
                model.tags,
                model.is_active,
                func.max(NotificationExecution.executed_at).label("last_execution"),  # type: ignore
                func.count(distinct(NotificationChannelConfig.id)).label("channel_count"),  # type: ignore
                func.sum(func.jsonb_array_length(NotificationChannelConfig.recipients)).label("recipients_count"),
            )
            .select_from(model)
            .outerjoin(NotificationExecution, execution_fk == model_id)  # type: ignore
            .outerjoin(
                NotificationChannelConfig,
                and_(
                    config_fk == model_id,  # type: ignore
                    NotificationChannelConfig.notification_type == notification_type,  # type: ignore
                ),
            )
            .group_by(model.id, model.name, model.type, model.grain, model.summary, model.tags, model.is_active)
        )

    async def get_notifications_list(
        self, params: PaginationParams, filter_params: dict[str, Any] | None = None
    ) -> tuple[list[NotificationList], int]:
        """
        Get paginated list of notifications with their execution history and recipient counts.
        Combines data from alerts, reports, notification channels, and executions.
        """
        # Create base queries for alerts and reports
        alerts = self._create_base_query(Alert)
        reports = self._create_base_query(Report)

        if filter_params:
            filter_obj = NotificationConfigFilter.model_validate(filter_params)
            alerts = filter_obj.apply_filters(alerts, Alert)
            reports = filter_obj.apply_filters(reports, Report)

        # Combine and paginate
        combined = alerts.union(reports).order_by("name").offset(params.offset).limit(params.limit)  # type: ignore

        # Get total count for pagination
        count_query = select(func.count()).select_from(alerts.union(reports).subquery())  # type: ignore

        # Execute queries
        results, total_count = await asyncio.gather(self.session.execute(combined), self.session.scalar(count_query))
        notifications = [NotificationList.model_validate(row, from_attributes=True) for row in results]

        return notifications, total_count or 0

    async def get_unique_tags(self, search: str | None = None) -> list[str]:
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

        if search:
            statement = statement.where(func.lower(column("tag")).contains(func.lower(search)))

        result = await self.session.execute(statement)
        return [row[0] for row in result.fetchall()]

    async def _validate_ids(self, ids: list[int], model: NotificationModel) -> set[int]:
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

    async def _delete(self, model: NotificationModel, ids: list[int]):
        invalid_ids = await self._validate_ids(ids, model)
        if invalid_ids:
            raise ValueError(f"Data not found for {str(model.__tablename__)}: {list(invalid_ids)}")
        await self.session.execute(delete(model).where(model.id.in_(ids)))  # type: ignore

    async def batch_delete(self, alert_ids: list[int], report_ids: list[int]) -> None:
        """Deletes all notification channels for one or multiple alerts / reports"""
        if not alert_ids and not report_ids:
            return
        if alert_ids:
            await self._delete(Alert, alert_ids)  # type: ignore
        if report_ids:
            await self._delete(Report, report_ids)  # type: ignore
        await self.session.commit()

    async def _update_status(self, model: NotificationModel, ids: list[int], status: bool):
        invalid_ids = await self._validate_ids(ids, model)
        if invalid_ids:
            raise ValueError(f"Data not found for {str(model.__tablename__)}: {list(invalid_ids)}")
        await self.session.execute(update(model).where(model.id.in_(ids)).values(is_active=status))  # type: ignore

    async def batch_status_update(self, alert_ids: list[int], report_ids: list[int], is_active: bool) -> None:
        """Updates the active status of one or multiple Alerts."""

        if not alert_ids and not report_ids:
            return
        if alert_ids:
            await self._update_status(Alert, alert_ids, is_active)  # type: ignore
        if report_ids:
            await self._update_status(Report, report_ids, is_active)  # type: ignore
        # Commit the session to save the changes
        await self.session.commit()

    async def batch_get(self, alert_ids: list[int], report_ids: list[int]) -> tuple[list[Alert], list[Report]]:
        """
        Fetch multiple alerts and reports in bulk.

        Args:
            alert_ids: List of alert IDs to fetch
            report_ids: List of report IDs to fetch

        Returns:
            Tuple of (alerts, reports)

        Raises:
            ValueError: If any requested ID is not found
        """

        async def fetch_items(model: type[NotificationModel], ids: list[int], name: str) -> list[NotificationModel]:
            if not ids:
                return []
            result = await self.session.execute(select(model).where(model.id.in_(ids)))  # type: ignore
            items = result.scalars().all()

            found_ids = {item.id for item in items}
            missing_ids = set(ids) - found_ids
            if missing_ids:
                raise ValueError(f"{name} not found: {missing_ids}")
            return items  # type: ignore

        alerts, reports = await asyncio.gather(
            fetch_items(Alert, alert_ids, "Alerts"), fetch_items(Report, report_ids, "Reports")
        )

        return alerts, reports


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


class CRUDAlert(CRUDBase[Alert, AlertRequest, None, AlertFilter]):  # type: ignore
    """
    CRUD operations for the Alert model.
    """

    filter_class = AlertFilter

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
        for key, value in alert_update.model_dump(exclude={"notification_channels"}, exclude_unset=True).items():
            setattr(alert, key, value)
        self.session.add(alert)

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
        report_update_dict = report_update.model_dump(exclude={"notification_channels"}, exclude_unset=True)
        for key, value in report_update_dict.items():
            setattr(report, key, value)
        self.session.add(report)

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


class CRUDNotificationExecution(
    CRUDBase[
        NotificationExecution, NotificationExecutionCreate, NotificationExecutionCreate, NotificationExecutionFilter
    ]
):
    """CRUD operations for notification executions."""

    filter_class = NotificationExecutionFilter

    def get_select_query(self) -> select:  # type: ignore
        """Base select query for executions with related alerts and reports"""
        return (
            select(self.model)
            .options(selectinload(self.model.alert), selectinload(self.model.report))  # type: ignore
            .order_by(self.model.executed_at.desc())  # type: ignore
        )
