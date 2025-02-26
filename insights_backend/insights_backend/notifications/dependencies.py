from typing import Annotated

from fastapi import Depends

from insights_backend.db.config import AsyncSessionDep
from insights_backend.notifications.crud import (
    CRUDAlert,
    CRUDNotificationChannelConfig,
    CRUDNotifications,
    CRUDReport,
)
from insights_backend.notifications.models import Alert, NotificationChannelConfig, Report
from insights_backend.notifications.services.preview.alert import AlertPreviewService
from insights_backend.notifications.services.preview.report import ReportPreviewService
from insights_backend.notifications.services.template_service import TemplateService


async def get_template_service() -> TemplateService:
    return TemplateService()


TemplateServiceDep = Annotated[TemplateService, Depends(get_template_service)]


async def get_notification_channel_crud(
    session: AsyncSessionDep, template_service: TemplateServiceDep
) -> CRUDNotificationChannelConfig:
    return CRUDNotificationChannelConfig(
        model=NotificationChannelConfig, session=session, template_service=template_service
    )


NotificationChannelConfigCRUDDep = Annotated[CRUDNotificationChannelConfig, Depends(get_notification_channel_crud)]


async def get_alerts_crud(
    session: AsyncSessionDep, notification_channel_config_crud: NotificationChannelConfigCRUDDep
) -> CRUDAlert:
    return CRUDAlert(model=Alert, session=session, notification_config_crud=notification_channel_config_crud)


AlertsCRUDDep = Annotated[CRUDAlert, Depends(get_alerts_crud)]


async def get_notification_crud(session: AsyncSessionDep) -> CRUDNotifications:
    return CRUDNotifications(session=session)


CRUDNotificationsDep = Annotated[CRUDNotifications, Depends(get_notification_crud)]


async def get_alert_preview_service(
    template_service: TemplateServiceDep,
) -> AlertPreviewService:
    return AlertPreviewService(template_service)


# Type alias for cleaner route signatures
AlertPreviewServiceDep = Annotated[AlertPreviewService, Depends(get_alert_preview_service)]


async def get_report_preview_service(
    template_service: TemplateServiceDep,
) -> ReportPreviewService:
    return ReportPreviewService(template_service)


# Type alias for cleaner route signatures
ReportPreviewServiceDep = Annotated[ReportPreviewService, Depends(get_report_preview_service)]


async def get_reports_crud(
    session: AsyncSessionDep, notification_channel_config_crud: NotificationChannelConfigCRUDDep
) -> CRUDReport:
    return CRUDReport(model=Report, session=session, notification_config_crud=notification_channel_config_crud)


ReportsCRUDDep = Annotated[CRUDReport, Depends(get_reports_crud)]
