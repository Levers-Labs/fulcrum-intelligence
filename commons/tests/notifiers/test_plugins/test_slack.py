import json
from unittest.mock import MagicMock

import pytest

from commons.notifiers.plugins import SlackNotifier


@pytest.fixture
def slack_notifier(mocker, mock_slack_client):
    notifier = SlackNotifier({})
    mocker.patch.object(notifier, "get_client", return_value=mock_slack_client)
    return notifier


@pytest.mark.asyncio
async def test_slack_notifier_send_success(
    mocker, slack_notifier, mock_message, mock_config, channel_config, mock_slack_client
):
    # Prepare
    mock_template = MagicMock()
    mock_template.render.return_value = json.dumps(mock_message)
    mocker.patch.object(slack_notifier, "create_template", return_value=mock_template)
    mock_slack_client.post_message.return_value = {"MessageId": "test-id", "status": True}

    template_config = {
        "type": "SLACK_MESSAGE",
        "message": json.dumps(
            {
                "text": "Test message",
                "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}],
            }
        ),
    }

    # Act
    result = slack_notifier.send_notification(
        template_config=template_config,
        config=mock_config["slack"],
        channel_config={"id": mock_message["channel_id"]},
        context={"key": "value"},
    )

    # Assert
    mock_slack_client.post_message.assert_called_once_with(
        channel_id=mock_message["channel_id"], text=mock_message["text"], blocks=mock_message["blocks"]
    )
    assert result["status"] is True


@pytest.mark.asyncio
async def test_slack_notifier_send_default_channel(
    mocker, slack_notifier, mock_message, mock_config, channel_config, mock_slack_client
):
    # Prepare
    message_without_channel = mock_message.copy()
    del message_without_channel["channel_id"]
    mock_template = MagicMock()
    mock_template.render.return_value = json.dumps(message_without_channel)
    mocker.patch.object(slack_notifier, "create_template", return_value=mock_template)
    mock_slack_client.post_message.return_value = {"MessageId": "test-id", "status": True}

    template_config = {
        "type": "SLACK_MESSAGE",
        "message": json.dumps(
            {
                "text": "Test message",
                "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}],
            }
        ),
    }

    # Act
    result = slack_notifier.send_notification(
        template_config=template_config,
        config=mock_config["slack"],
        channel_config={"id": "test-channel"},
        context={"key": "value"},
    )

    # Assert
    mock_slack_client.post_message.assert_called_once_with(
        channel_id="test-channel", text=mock_message["text"], blocks=mock_message["blocks"]
    )
    assert result["status"] is True


@pytest.mark.asyncio
async def test_slack_notifier_send_error(
    mocker, slack_notifier, mock_message, mock_config, channel_config, mock_slack_client
):
    mock_template = MagicMock()
    mock_template.render.return_value = json.dumps(mock_message)
    mocker.patch.object(slack_notifier, "create_template", return_value=mock_template)
    mock_slack_client.post_message.side_effect = Exception("Slack API error")

    template_config = {
        "type": "SLACK_MESSAGE",
        "message": json.dumps(
            {
                "text": "Test message",
                "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}],
            }
        ),
    }

    with pytest.raises(Exception) as excinfo:
        await slack_notifier.send_notification(
            template_config=template_config,
            config=mock_config["slack"],
            channel_config={"id": mock_message["channel_id"]},
            context={"key": "value"},
        )

    assert str(excinfo.value) == "Slack API error"


def test_slack_notifier_missing_token():
    config = {"default_channel": "test-channel"}
    notifier = SlackNotifier({})

    with pytest.raises(ValueError) as excinfo:
        notifier.get_client(config)

    assert str(excinfo.value) == "Slack bot token not provided in the configuration."


def test_slack_notifier_missing_channel_id():
    config = {}
    notifier = SlackNotifier({})
    client = MagicMock()
    content = {
        "text": "Test message",
        "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}],
    }

    with pytest.raises(ValueError) as excinfo:
        notifier.send_notification_using_client(client, content, config)

    assert str(excinfo.value) == "Channel ID is not provided in the configuration."


def test_slack_notifier_get_notification_content(slack_notifier):
    template_config = {
        "type": "SLACK_MESSAGE",
        "message": json.dumps(
            {
                "text": "Test message",
                "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}],
            }
        ),
    }

    context = {"title": "Test Message", "body": "This is a test message"}

    content = slack_notifier.get_notification_content(template_config, context)
    assert isinstance(content, dict)
    assert "text" in content
    assert "blocks" in content


def test_slack_notifier_missing_message_template():
    notifier = SlackNotifier({})
    template_config = {"type": "SLACK_MESSAGE"}

    with pytest.raises(ValueError) as excinfo:
        notifier.get_notification_content(template_config, {})
    assert str(excinfo.value) == "Template configuration must include content field"


def test_slack_notifier_invalid_json_template():
    notifier = SlackNotifier({})
    template_config = {"type": "SLACK_MESSAGE", "message": "{ invalid json }"}

    mock_template = MagicMock()
    mock_template.render.return_value = "{ invalid json }"

    with pytest.raises(ValueError) as excinfo:
        notifier.get_notification_content(template_config, {})
    assert "Invalid JSON template content:" in str(excinfo.value)
