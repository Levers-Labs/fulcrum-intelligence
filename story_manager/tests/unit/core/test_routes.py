import pytest
from starlette import status

from story_manager.story_manager.story_builder.core.enums import GENRE_TO_STORY_TYPE_MAPPING, StoryGenre, StoryType
from story_manager.story_manager.story_builder.core.models import Story


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
async def test_get_stories(db_session, client):
    # Create a couple of test stories
    story1 = Story(
        genre=StoryGenre.GROWTH,
        story_type=StoryType.SLOWING_GROWTH,
        metric_id="CAC",
        description="Test Story 1",
        template="Test Story 1",
        text="This is the content of Test Story 1.",
    )
    story2 = Story(
        genre=StoryGenre.GROWTH,
        story_type=StoryType.ACCELERATING_GROWTH,
        metric_id="NewMRR",
        description="Test Story 2",
        template="Test Story 2",
        text="This is the content of Test Story 2.",
        is_published=True,
    )
    db_session.add_all([story1, story2])
    db_session.commit()

    # Test listing all stories
    response = client.get("/v1/stories/")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 2

    # Test filtering by is_published
    response = client.get("/v1/stories/?genre=GROWTH")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 2
    assert data["results"][0]["genre"] == StoryGenre.GROWTH.value

    # Test filtering by metric_id
    response = client.get("/v1/stories/?metric_id=CAC")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["results"][0]["metric_id"] == "CAC"

    # Test filtering by story_type
    response = client.get("/v1/stories/?story_type=ACCELERATING_GROWTH")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["results"][0]["story_type"] == StoryType.ACCELERATING_GROWTH.value

    # Test combining multiple filters
    response = client.get("/v1/stories/?genre=GROWTH&metric_id=NewMRR")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["results"][0]["genre"] == StoryGenre.GROWTH.value
    assert data["results"][0]["metric_id"] == "NewMRR"
