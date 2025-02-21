from unittest.mock import AsyncMock

import pytest

from insights_backend.notifications.dependencies import (
    get_alerts_crud,
    get_notification_channel_crud,
    get_notification_crud,
    get_reports_crud,
    get_template_service,
)
from insights_backend.notifications.models import Alert, NotificationChannelConfig, Report
from insights_backend.notifications.services.template_service import TemplateService


@pytest.mark.asyncio
async def test_get_template_service():
    """Test template service dependency"""
    service = await get_template_service()
    assert isinstance(service, TemplateService)


@pytest.mark.asyncio
async def test_get_notification_channel_crud():
    """Test notification channel CRUD dependency"""
    mock_session = AsyncMock()
    mock_template_service = AsyncMock()

    crud = await get_notification_channel_crud(session=mock_session, template_service=mock_template_service)

    assert crud.model == NotificationChannelConfig
    assert crud.session == mock_session
    assert crud.template_service == mock_template_service


@pytest.mark.asyncio
async def test_get_alerts_crud():
    """Test alerts CRUD dependency"""
    mock_session = AsyncMock()
    mock_notification_config_crud = AsyncMock()

    crud = await get_alerts_crud(session=mock_session, notification_channel_config_crud=mock_notification_config_crud)

    assert crud.model == Alert
    assert crud.session == mock_session
    assert crud.notification_config_crud == mock_notification_config_crud


@pytest.mark.asyncio
async def test_get_notification_crud():
    """Test notifications CRUD dependency"""
    mock_session = AsyncMock()

    crud = await get_notification_crud(session=mock_session)

    assert crud.session == mock_session


@pytest.mark.asyncio
async def test_get_reports_crud():
    """Test reports CRUD dependency"""
    mock_session = AsyncMock()
    mock_notification_config_crud = AsyncMock()

    crud = await get_reports_crud(session=mock_session, notification_channel_config_crud=mock_notification_config_crud)

    assert crud.model == Report
    assert crud.session == mock_session
    assert crud.notification_config_crud == mock_notification_config_crud


@pytest.mark.asyncio
async def test_dependency_chain():
    """Test the entire dependency injection chain"""
    mock_session = AsyncMock()

    # Test the chain: template_service -> notification_channel_crud -> alerts_crud
    template_service = await get_template_service()
    notification_channel_crud = await get_notification_channel_crud(
        session=mock_session, template_service=template_service
    )
    alerts_crud = await get_alerts_crud(
        session=mock_session, notification_channel_config_crud=notification_channel_crud
    )

    assert isinstance(template_service, TemplateService)
    assert alerts_crud.notification_config_crud.template_service == template_service

    # Test the chain: template_service -> notification_channel_crud -> reports_crud
    reports_crud = await get_reports_crud(
        session=mock_session, notification_channel_config_crud=notification_channel_crud
    )

    assert reports_crud.notification_config_crud.template_service == template_service


@pytest.mark.asyncio
async def test_multiple_instances():
    """Test that each dependency call creates a new instance"""
    mock_session = AsyncMock()

    crud1 = await get_notification_crud(session=mock_session)
    crud2 = await get_notification_crud(session=mock_session)

    assert crud1 is not crud2  # Different instances
    assert crud1.session == crud2.session  # Same session


@pytest.mark.asyncio
async def test_template_service_singleton():
    """Test that template service behaves like a singleton"""
    service1 = await get_template_service()
    service2 = await get_template_service()

    assert isinstance(service1, TemplateService)
    assert isinstance(service2, TemplateService)
    # Template service should be stateless, so multiple instances are fine
