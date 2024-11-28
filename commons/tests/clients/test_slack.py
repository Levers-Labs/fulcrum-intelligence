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


async def test_list_channels_basic(slack_client, mock_web_client):
    # Mock response from Slack API
    mock_response = {
        "channels": [
            {"name": "general", "is_channel": True},
            {"name": "random", "is_channel": True},
        ],
        "response_metadata": {"next_cursor": None},
    }
    mock_web_client.return_value.conversations_list.return_value = mock_response

    result = await slack_client.list_channels()

    assert len(result["results"]) == 2
    assert result["next_cursor"] is None
    assert result["results"][0]["name"] == "general"
    assert result["results"][1]["name"] == "random"


async def test_list_channels_with_name_filter(slack_client, mock_web_client):
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

    result = await slack_client.list_channels(name="test")

    assert len(result["results"]) == 1
    assert result["results"][0]["name"] == "test-channel"


async def test_list_channels_with_pagination(slack_client, mock_web_client):
    # Mock response from Slack API
    mock_response = {
        "channels": [
            {"name": "channel1", "is_channel": True},
            {"name": "channel2", "is_channel": True},
        ],
        "response_metadata": {"next_cursor": "dXNlcjpVMDYxTkZUVDI="},
    }
    mock_web_client.return_value.conversations_list.return_value = mock_response

    result = await slack_client.list_channels(limit=2)

    assert len(result["results"]) == 2
    assert result["next_cursor"] == "dXNlcjpVMDYxTkZUVDI="


async def test_list_channels_filters_non_channels(slack_client, mock_web_client):
    # Mock response including non-channels
    mock_response = {
        "channels": [
            {"name": "general", "is_channel": True},
            {"name": "direct-msg", "is_channel": False},
        ],
        "response_metadata": {"next_cursor": None},
    }
    mock_web_client.return_value.conversations_list.return_value = mock_response

    result = await slack_client.list_channels()

    assert len(result["results"]) == 1
    assert result["results"][0]["name"] == "general"


async def test_post_message_basic(slack_client, mock_web_client):
    mock_web_client.return_value.chat_postMessage.return_value = {"ok": True}

    result = await slack_client.post_message(channel_id="C1234567890", text="Hello, World!")

    assert result is True
    mock_web_client.return_value.chat_postMessage.assert_called_once_with(channel="C1234567890", text="Hello, World!")


async def test_post_message_with_blocks(slack_client, mock_web_client):
    mock_web_client.return_value.chat_postMessage.return_value = {"ok": True}
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "Hello"}}]

    result = await slack_client.post_message(channel_id="C1234567890", blocks=blocks)

    assert result is True
    mock_web_client.return_value.chat_postMessage.assert_called_once_with(channel="C1234567890", blocks=blocks)


async def test_post_message_with_attachments(slack_client, mock_web_client):
    mock_web_client.return_value.chat_postMessage.return_value = {"ok": True}
    attachments = [{"text": "Attachment text"}]

    result = await slack_client.post_message(channel_id="C1234567890", attachments=attachments)

    assert result is True
    mock_web_client.return_value.chat_postMessage.assert_called_once_with(
        channel="C1234567890", attachments=attachments
    )
