import pytest


@pytest.mark.asyncio
async def test_get_stories(client):
    response = client.get("/v1/stories/")
    assert response.status_code == 200
