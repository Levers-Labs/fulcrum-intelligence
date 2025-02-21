from typing import Annotated

from fastapi import Depends

from insights_backend.db.config import AsyncSessionDep
from insights_backend.notifications.crud import CRUDAlert, CRUDNotificationChannelConfig, CRUDNotifications
from insights_backend.notifications.models import Alert, NotificationChannelConfig
from insights_backend.notifications.services.preview.alert import AlertPreviewService
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
    template_service: TemplateService = Depends(get_template_service),
) -> AlertPreviewService:
    return AlertPreviewService(template_service)


# Type alias for cleaner route signatures
AlertPreviewServiceDep = Annotated[AlertPreviewService, Depends(get_alert_preview_service)]
