from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import Response

from commons.clients.story_manager import StoryManagerClient


@pytest.fixture
def story_manager_client():
    base_url = "https://example.com"
    auth = MagicMock()
    return StoryManagerClient(base_url=base_url, api_version="v1", auth=auth)


@pytest.fixture(autouse=True)
def mock_get_tenant_id():
    with patch("commons.clients.base.get_tenant_id", return_value="test_tenant_id"):
        yield


@pytest.mark.asyncio
async def test_list_stories_default_params(story_manager_client):
    """Test list_stories with default parameters."""
    response_mock = AsyncMock(spec=Response)
    response_mock.json.return_value = {"items": [{"id": 1, "title": "Test Story"}], "total": 1, "page": 1, "size": 100}

    with patch.object(story_manager_client, "get", return_value=response_mock.json.return_value) as get_mock:
        response = await story_manager_client.list_stories()

        get_mock.assert_awaited_once_with("/stories/", params={"page": 1, "size": 100})
        assert response == response_mock.json.return_value


@pytest.mark.asyncio
async def test_list_stories_with_pagination(story_manager_client):
    """Test list_stories with custom pagination parameters."""
    response_mock = AsyncMock(spec=Response)
    response_mock.json.return_value = {"items": [{"id": 1, "title": "Test Story"}], "total": 1, "page": 2, "size": 50}

    with patch.object(story_manager_client, "get", return_value=response_mock.json.return_value) as get_mock:
        response = await story_manager_client.list_stories(page=2, size=50)

        get_mock.assert_awaited_once_with("/stories/", params={"page": 2, "size": 50})
        assert response == response_mock.json.return_value


@pytest.mark.asyncio
async def test_list_stories_with_filters(story_manager_client):
    """Test list_stories with various filter parameters."""
    from enum import Enum

    class MockGrain(Enum):
        DAY = "day"
        WEEK = "week"

    filters = {
        "story_types": ["type1", "type2"],
        "story_groups": ["group1"],
        "genres": ["genre1"],
        "metric_ids": [1, 2, 3],
        "grains": [MockGrain.DAY, MockGrain.WEEK],
        "story_date_start": "2024-01-01",
        "story_date_end": "2024-02-01",
        "digest": "daily",
        "section": "news",
        "is_heuristic": True,
    }

    response_mock = AsyncMock(spec=Response)
    response_mock.json.return_value = {"items": [{"id": 1, "title": "Test Story"}], "total": 1, "page": 1, "size": 100}

    expected_params = {
        "page": 1,
        "size": 100,
        "story_types": ["type1", "type2"],
        "story_groups": ["group1"],
        "genres": ["genre1"],
        "metric_ids": [1, 2, 3],
        "grains": ["day", "week"],
        "story_date_start": "2024-01-01",
        "story_date_end": "2024-02-01",
        "digest": "daily",
        "section": "news",
        "is_heuristic": True,
    }

    with patch.object(story_manager_client, "get", return_value=response_mock.json.return_value) as get_mock:
        response = await story_manager_client.list_stories(**filters)

        get_mock.assert_awaited_once_with("/stories/", params=expected_params)
        assert response == response_mock.json.return_value


@pytest.mark.asyncio
async def test_list_stories_empty_response(story_manager_client):
    """Test list_stories when no stories are found."""
    response_mock = AsyncMock(spec=Response)
    response_mock.json.return_value = {"items": [], "total": 0, "page": 1, "size": 100}

    with patch.object(story_manager_client, "get", return_value=response_mock.json.return_value) as get_mock:
        response = await story_manager_client.list_stories()

        get_mock.assert_awaited_once_with("/stories/", params={"page": 1, "size": 100})
        assert response == response_mock.json.return_value
        assert len(response["items"]) == 0
