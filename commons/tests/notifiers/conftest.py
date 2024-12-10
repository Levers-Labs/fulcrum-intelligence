from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def mock_slack_client():
    client = MagicMock()
    client.post_message = AsyncMock()
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
def mock_message():
    return {
        "channel_id": "test-channel",
        "text": "Test message",
        "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}],
    }
