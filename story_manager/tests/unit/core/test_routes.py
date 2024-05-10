import pytest
from starlette import status

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.core.models import Story


@pytest.mark.asyncio
async def test_get_stories(db_session, client):
    # Create test stories
    stories = [
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            grain=Granularity.DAY,
            story_type=StoryType.SLOWING_GROWTH,
            metric_id="CAC",
            description="Test Story 1",
            template="Test Story 1",
            text="This is the content of Test Story 1.",
        ),
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            grain=Granularity.DAY,
            story_type=StoryType.ACCELERATING_GROWTH,
            metric_id="NewMRR",
            description="Test Story 2",
            template="Test Story 2",
            text="This is the content of Test Story 2.",
            is_published=True,
        ),
    ]
    db_session.add_all(stories)
    db_session.commit()

    # Test listing all stories
    response = client.get("/v1/stories/")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == len(stories)

    # Test filtering by genre
    response = client.get("/v1/stories/?genre=GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == len(stories)
    for result in data["results"]:
        assert result["genre"] == StoryGenre.GROWTH.value
        assert result["story_group"] == StoryGroup.GROWTH_RATES.value
        assert result["grain"] == Granularity.DAY.value

    # Test filtering by metric_id
    response = client.get("/v1/stories?metric_id=CAC")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1
    assert data["results"][0]["metric_id"] == "CAC"

    # Test filtering by story_type
    response = client.get("/v1/stories?story_type=ACCELERATING_GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1
    assert data["results"][0]["story_type"] == StoryType.ACCELERATING_GROWTH.value

    # Test combining multiple filters
    response = client.get("/v1/stories?story_group=GROWTH_RATES&story_type=ACCELERATING_GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1
    assert data["results"][0]["genre"] == StoryGenre.GROWTH.value
    assert data["results"][0]["story_group"] == StoryGroup.GROWTH_RATES.value
    assert data["results"][0]["grain"] == Granularity.DAY.value
    assert data["results"][0]["metric_id"] == "NewMRR"
