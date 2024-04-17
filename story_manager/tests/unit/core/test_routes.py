import pytest
from starlette import status

from story_manager.core.enums import GENRE_TO_STORY_TYPE_MAPPING


@pytest.mark.asyncio
async def test_get_story_genres(client):
    response = client.get("/v1/stories/genres")
    assert response.status_code == status.HTTP_200_OK

    genres = response.json()
    assert len(genres) == len(GENRE_TO_STORY_TYPE_MAPPING)

    for genre in genres:
        assert "genre" in genre
        assert "types" in genre
        assert "label" in genre
        assert genre["label"] == genre["genre"].replace("_", " ").title()

        for story_type in genre["types"]:
            assert "type" in story_type
            assert "label" in story_type
            assert "description" in story_type


@pytest.mark.asyncio
async def test_get_stories(setup_env, client):
    response = client.get("/v1/stories/")
    assert response.status_code == 200
