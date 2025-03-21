from datetime import date, datetime
from unittest.mock import AsyncMock, patch

import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGroup
from story_manager.core.models import Story
from story_manager.notifications.slack_alerts import SlackAlertsService


@pytest.fixture
def mock_query_client():
    client = AsyncMock()
    client.get_metric.return_value = {"label": "Test Metric"}
    client.get_metric_slack_notification_details.return_value = {
        "slack_enabled": True,
        "slack_channels": [{"id": "test_channel"}],
    }
    return client


@pytest.fixture
def mock_slack_connection_config():
    return {"team": "test"}


@pytest.fixture
def service(mock_query_client, mock_slack_connection_config):
    return SlackAlertsService(mock_query_client, mock_slack_connection_config)


@pytest.mark.asyncio
async def test_get_stories(service):
    mock_story = Story(
        story_group=StoryGroup.TREND_CHANGES, metric_id="test_metric", title="Test Title", detail="Test Detail"
    )

    with patch("story_manager.notifications.slack_alerts.get_async_session") as _, patch(
        "story_manager.notifications.slack_alerts.CRUDStory"
    ) as mock_crud_story:
        mock_crud = AsyncMock()
        mock_crud.get_stories.return_value = [mock_story]
        mock_crud_story.return_value = mock_crud

        stories = await service._get_stories(
            metric_id="test_metric", grain=Granularity.DAY, tenant_id=1, created_date=date.today()
        )

        assert len(stories) == 1
        assert stories[0]["story_group"] == StoryGroup.TREND_CHANGES.value
        assert stories[0]["metric_id"] == "test_metric"
        assert stories[0]["title"] == "Test Title"
        assert stories[0]["detail"] == "Test Detail"


@pytest.mark.asyncio
async def test_prepare_context(service):
    stories = [
        {
            "story_group": StoryGroup.TREND_CHANGES.value,
            "metric_id": "test_metric",
            "title": "Test Title",
            "detail": "Test Detail",
        }
    ]

    context = await service._prepare_context(stories=stories, grain=Granularity.DAY, metric_id="test_metric")

    assert context["stories"] == stories
    assert context["grain"] == Granularity.DAY.value
    assert isinstance(context["time"], datetime)
    assert context["metric"] == {"label": "Test Metric"}


@pytest.mark.asyncio
async def test_send_slack_alerts(service):
    mock_client = AsyncMock()
    mock_client.send_notification.return_value = {"ok": True}

    context = {"metric": {"label": "Test Metric"}, "stories": []}
    channel_config = {"id": "test_channel"}
    slack_config = {"team": "test"}

    response = await service._send_slack_alerts(
        client=mock_client, context=context, channel_config=channel_config, slack_config=slack_config
    )

    assert response == {"ok": True}
    mock_client.send_notification.assert_called_once_with(
        template_name=service.SLACK_MSG_TEMPLATE, config=slack_config, channel_config=channel_config, context=context
    )


@pytest.mark.asyncio
async def test_send_metric_stories_notification_no_slack_config(service):
    service.slack_connection_config = None

    await service.send_metric_stories_notification(
        grain=Granularity.DAY, tenant_id=1, created_date=date.today(), metric_id="test_metric"
    )

    # Verify that no further processing occurred
    service.query_client.get_metric_slack_notification_details.assert_not_called()


@pytest.mark.asyncio
async def test_send_metric_stories_notification_success(service):
    mock_stories = [
        {
            "story_group": StoryGroup.TREND_CHANGES.value,
            "metric_id": "test_metric",
            "title": "Test Title",
            "detail": "Test Detail",
        }
    ]

    with patch.object(service, "_get_stories", return_value=mock_stories), patch(
        "story_manager.notifications.slack_alerts.get_slack_notifier"
    ) as mock_get_notifier:
        mock_notifier = AsyncMock()
        mock_get_notifier.return_value = mock_notifier

        await service.send_metric_stories_notification(
            grain=Granularity.DAY, tenant_id=1, created_date=date.today(), metric_id="test_metric"
        )

        # Verify the notification was sent
        assert mock_notifier.send_notification.called


@pytest.mark.asyncio
async def test_send_metric_stories_notification_no_stories(service):
    with patch.object(service, "_get_stories", return_value=[]):
        await service.send_metric_stories_notification(
            grain=Granularity.DAY, tenant_id=1, created_date=date.today(), metric_id="test_metric"
        )

        # Verify that no notification was sent
        service.query_client.get_metric.assert_not_called()


@pytest.mark.asyncio
async def test_get_slack_config_success(service):
    channels = await service._get_slack_config("test_metric")

    assert channels == [{"id": "test_channel"}]
    service.query_client.get_metric_slack_notification_details.assert_called_once_with("test_metric")


@pytest.mark.asyncio
async def test_get_slack_config_disabled(service):
    service.query_client.get_metric_slack_notification_details.return_value = {"slack_enabled": False}

    with pytest.raises(ValueError, match="Slack notifications disabled"):
        await service._get_slack_config("test_metric")


@pytest.mark.asyncio
async def test_get_slack_config_no_channels(service):
    service.query_client.get_metric_slack_notification_details.return_value = {
        "slack_enabled": True,
        "slack_channels": [],
    }

    with pytest.raises(ValueError, match="No Slack channels configured"):
        await service._get_slack_config("test_metric")
