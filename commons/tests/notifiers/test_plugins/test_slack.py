import json
from unittest.mock import MagicMock

import pytest

from commons.notifiers.plugins import SlackNotifier


@pytest.fixture
def slack_notifier(mocker, mock_slack_client):
    notifier = SlackNotifier("templates")
    mocker.patch.object(notifier, "get_client", return_value=mock_slack_client)
    return notifier


@pytest.mark.asyncio
async def test_slack_notifier_send_success(
    mocker, slack_notifier, mock_message, mock_config, channel_config, mock_slack_client
):
    # Prepare
    mock_template = MagicMock()
    mock_template.render.return_value = json.dumps(mock_message)
    mocker.patch.object(slack_notifier, "get_template", return_value=mock_template)

    # Act
    slack_notifier.send_notification(mock_message, mock_config["slack"], channel_config, context={"key": "value"})

    # Assert
    mock_slack_client.post_message.assert_called_once_with(
        channel_id=mock_message["channel_id"], text=mock_message["text"], blocks=mock_message["blocks"]
    )


@pytest.mark.asyncio
async def test_slack_notifier_send_default_channel(
    mocker, slack_notifier, mock_message, mock_config, channel_config, mock_slack_client
):
    # Prepare
    message_without_channel = mock_message.copy()
    del message_without_channel["channel_id"]
    mock_template = MagicMock()
    mock_template.render.return_value = json.dumps(message_without_channel)
    mocker.patch.object(slack_notifier, "get_template", return_value=mock_template)

    # Act
    slack_notifier.send_notification(
        message_without_channel, mock_config["slack"], channel_config, context={"key": "value"}
    )
    # Assert
    mock_slack_client.post_message.assert_called_once_with(
        channel_id="test-channel", text=mock_message["text"], blocks=mock_message["blocks"]
    )


@pytest.mark.asyncio
async def test_slack_notifier_send_error(
    mocker, slack_notifier, mock_message, mock_config, channel_config, mock_slack_client
):
    mock_template = MagicMock()
    mock_template.render.return_value = json.dumps(mock_message)
    mocker.patch.object(slack_notifier, "get_template", return_value=mock_template)
    mock_slack_client.post_message.side_effect = Exception("Slack API error")

    with pytest.raises(Exception) as excinfo:
        await slack_notifier.send_notification(
            mock_message, mock_config["slack"], channel_config, context={"key": "value"}
        )

    assert str(excinfo.value) == "Slack API error"


def test_slack_notifier_missing_token():
    config = {"default_channel": "test-channel"}
    notifier = SlackNotifier("templates")

    with pytest.raises(ValueError) as excinfo:
        notifier.get_client(config)

    assert str(excinfo.value) == "Slack bot token not provided in the configuration."


def test_slack_notifier_missing_default_channel():
    config = {}
    notifier = SlackNotifier("templates")
    client = MagicMock()
    render_template_mock = MagicMock()

    with pytest.raises(ValueError) as excinfo:
        notifier.send_notification_using_client(client, render_template_mock, config)

    assert str(excinfo.value) == "Channel ID is not provided in the configuration."
