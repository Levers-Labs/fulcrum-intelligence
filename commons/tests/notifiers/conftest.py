from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_slack_client():
    client = MagicMock()
    client.post_message = MagicMock()
    return client


@pytest.fixture
def mock_email_client():
    client = MagicMock()
    client.send_email = MagicMock()
    return client


@pytest.fixture
def mock_config():
    return {"slack": {"bot_token": "test-token"}}


@pytest.fixture
def channel_config():
    return {
        "id": "test-channel",
        "name": "Test Channel",
    }


@pytest.fixture
def mock_email_config():
    return {
        "region": "us-west-1",
        "to": ["test@example.com"],
        "cc": ["cc@example.com"],
    }


@pytest.fixture
def mock_message():
    return {
        "channel_id": "test-channel",
        "text": "Test message",
        "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}],
    }
