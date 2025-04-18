from unittest.mock import patch

import pytest

from commons.clients.slack import SlackClient

pytestmark = pytest.mark.asyncio


@pytest.fixture(name="mock_web_client")
def mock_web_client_fixture():
    with patch("commons.clients.slack.WebClient") as mock_client:
        yield mock_client


@pytest.fixture(name="slack_client")
def slack_client_fixture(mock_web_client):
    return SlackClient("fake-token")


def test_list_channels_basic(slack_client, mock_web_client):
    # Mock response from Slack API
    mock_response = {
        "channels": [
            {"name": "general", "is_channel": True},
            {"name": "random", "is_channel": True},
        ],
        "response_metadata": {"next_cursor": None},
    }
    mock_web_client.return_value.conversations_list.return_value = mock_response

    result = slack_client.list_channels()

    assert len(result["results"]) == 2
    assert result["next_cursor"] is None
    assert result["results"][0]["name"] == "general"
    assert result["results"][1]["name"] == "random"


def test_list_channels_with_name_filter(slack_client, mock_web_client):
    # Mock response from Slack API
    mock_response = {
        "channels": [
            {"name": "general", "is_channel": True},
            {"name": "random", "is_channel": True},
            {"name": "test-channel", "is_channel": True},
        ],
        "response_metadata": {"next_cursor": None},
    }
    mock_web_client.return_value.conversations_list.return_value = mock_response

    result = slack_client.list_channels(name="test")

    assert len(result["results"]) == 1
    assert result["results"][0]["name"] == "test-channel"


def test_list_channels_with_pagination(slack_client, mock_web_client):
    # Mock response from Slack API
    mock_response = {
        "channels": [
            {"name": "channel1", "is_channel": True},
            {"name": "channel2", "is_channel": True},
        ],
        "response_metadata": {"next_cursor": "dXNlcjpVMDYxTkZUVDI="},
    }
    mock_web_client.return_value.conversations_list.return_value = mock_response

    result = slack_client.list_channels(limit=2)

    assert len(result["results"]) == 2
    assert result["next_cursor"] == "dXNlcjpVMDYxTkZUVDI="


def test_list_channels_filters_non_channels(slack_client, mock_web_client):
    # Mock response including non-channels
    mock_response = {
        "channels": [
            {"name": "general", "is_channel": True},
            {"name": "direct-msg", "is_channel": False},
        ],
        "response_metadata": {"next_cursor": None},
    }
    mock_web_client.return_value.conversations_list.return_value = mock_response

    result = slack_client.list_channels()

    assert len(result["results"]) == 1
    assert result["results"][0]["name"] == "general"


def test_post_message_basic(slack_client, mock_web_client):
    mock_web_client.return_value.chat_postMessage.return_value = {
        "ok": True,
        "channel": "C1234567890",
        "ts": "1234567890.123456",
    }

    result = slack_client.post_message(channel_id="C1234567890", text="Hello, World!")

    assert result["ok"] is True
    assert result["channel"] == "C1234567890"
    assert result["ts"] == "1234567890.123456"
    mock_web_client.return_value.chat_postMessage.assert_called_once_with(channel="C1234567890", text="Hello, World!")


def test_post_message_with_blocks(slack_client, mock_web_client):
    mock_web_client.return_value.chat_postMessage.return_value = {
        "ok": True,
        "channel": "C1234567890",
        "ts": "1234567890.123456",
    }
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "Hello"}}]

    result = slack_client.post_message(channel_id="C1234567890", blocks=blocks)

    assert result["ok"] is True
    assert result["channel"] == "C1234567890"
    assert result["ts"] == "1234567890.123456"
    mock_web_client.return_value.chat_postMessage.assert_called_once_with(channel="C1234567890", blocks=blocks)


def test_post_message_with_attachments(slack_client, mock_web_client):
    mock_web_client.return_value.chat_postMessage.return_value = {
        "ok": True,
        "channel": "C1234567890",
        "ts": "1234567890.123456",
    }
    attachments = [{"text": "Attachment text"}]

    result = slack_client.post_message(channel_id="C1234567890", attachments=attachments)

    assert result["ok"] is True
    assert result["channel"] == "C1234567890"
    assert result["ts"] == "1234567890.123456"
    mock_web_client.return_value.chat_postMessage.assert_called_once_with(
        channel="C1234567890", attachments=attachments
    )


def test_get_channel_info(slack_client, mock_web_client):
    # Mock response from Slack API
    mock_response = {
        "channel": {
            "id": "C1234567890",
            "name": "test-channel",
            "is_channel": True,
            "created": 1234567890,
            "creator": "U0123456789",
            "is_archived": False,
            "is_general": False,
            "members": ["U0123456789"],
            "topic": {"value": "Channel topic", "creator": "U0123456789", "last_set": 1234567890},
            "purpose": {"value": "Channel purpose", "creator": "U0123456789", "last_set": 1234567890},
        }
    }
    mock_web_client.return_value.conversations_info.return_value = mock_response

    result = slack_client.get_channel_info("C1234567890")

    assert result["id"] == "C1234567890"
    assert result["name"] == "test-channel"
    assert result["is_channel"] is True
    mock_web_client.return_value.conversations_info.assert_called_once_with(channel="C1234567890")
