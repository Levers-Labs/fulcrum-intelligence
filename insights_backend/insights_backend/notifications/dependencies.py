from typing import Annotated

from fastapi import Depends

from insights_backend.db.config import AsyncSessionDep
from insights_backend.notifications.crud import CRUDAlert, CRUDNotificationChannel
from insights_backend.notifications.models import Alert, NotificationChannelConfig
from insights_backend.notifications.services.preview_service import PreviewService
from insights_backend.notifications.services.template_service import TemplateService


async def get_template_service() -> TemplateService:
    return TemplateService()


TemplateServiceDep = Annotated[TemplateService, Depends(get_template_service)]


async def get_notification_channel_crud(
    session: AsyncSessionDep, template_service: TemplateServiceDep
) -> CRUDNotificationChannel:
    return CRUDNotificationChannel(model=NotificationChannelConfig, session=session, template_service=template_service)


NotificationChannelCRUDDep = Annotated[CRUDNotificationChannel, Depends(get_notification_channel_crud)]


async def get_alerts_crud(session: AsyncSessionDep, notification_channel_crud: NotificationChannelCRUDDep) -> CRUDAlert:
    return CRUDAlert(model=Alert, session=session, notification_crud=notification_channel_crud)


AlertsCRUDDep = Annotated[CRUDAlert, Depends(get_alerts_crud)]


async def get_preview_service(template_service: TemplateServiceDep) -> PreviewService:
    return PreviewService(template_service=template_service)


PreviewServiceDep = Annotated[PreviewService, Depends(get_preview_service)]
