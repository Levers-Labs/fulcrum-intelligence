from datetime import datetime
from unittest import mock

import pytest
from starlette import status

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.core.models import Story
from story_manager.story_builder import StoryBuilderBase, StoryFactory


class MockSecurity:
    def __init__(self, *args, **kwargs):
        self.dependency = lambda: True
        self.use_cache = False


mock.patch("fastapi.Security", MockSecurity).start()


@pytest.mark.asyncio
async def test_get_story_group_meta_success(mocker, client):
    """
    Test successful retrieval of story group metadata.
    """

    class MockStoryBuilder(StoryBuilderBase):
        supported_grains = [Granularity.DAY, Granularity.WEEK]

    # Mock the StoryFactory.get_story_builder method
    mocker.patch.object(StoryFactory, "get_story_builder", return_value=MockStoryBuilder)

    # Get the story group metadata
    response = client.get(f"/v1/stories/groups/{StoryGroup.TREND_CHANGES.value}")

    # Check the response
    assert response.status_code == 200
    result = response.json()
    assert result["group"] == StoryGroup.TREND_CHANGES.value
    assert result["grains"] == [Granularity.DAY.value, Granularity.WEEK.value]


@pytest.mark.asyncio
async def test_get_story_group_meta_not_found(mocker, client):
    """
    Test handling of non-existent story group.
    """
    # Mock the StoryFactory.get_story_builder method to return None
    mocker.patch.object(StoryFactory, "get_story_builder", return_value=None)

    # Get the story group metadata
    response = client.get(f"/stories/groups/{StoryGroup.TREND_CHANGES}")
    # Check the response
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_stories(db_session, client):
    # Create test stories
    stories = [
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            story_date=datetime(2020, 1, 1),
            grain=Granularity.DAY,
            story_type=StoryType.SLOWING_GROWTH,
            metric_id="CAC",
            title="d/d growth is slowing down",
            title_template="{{pop}} growth is slowing down",
            detail="The d/d growth rate for NewBizDeals is slowing down. It is currently 10% and down from the 15% "
            "average over the past 5 days.",
            detail_template="The {{pop}} growth rate for {{metric.label}} is slowing down. It is currently {{"
            "current_growth}}% and down from the {{reference_growth}}% average over the past {{"
            "reference_period_days}} {{grain}}s.",
        ),
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            grain=Granularity.DAY,
            story_date=datetime(2020, 1, 1),
            story_type=StoryType.ACCELERATING_GROWTH,
            metric_id="NewMRR",
            title="d/d growth is speeding up",
            title_template="{{pop}} growth is speeding up",
            detail="The d/d growth rate for NewBizDeals is speeding up. It is currently 15% and up from the 10% "
            "average over the past 11 days.",
            detail_template="The {{pop}} growth rate for {{metric.label}} is speeding up. It is currently {{"
            "current_growth}}% and up from the {{reference_growth}}% average over the past {{"
            "reference_period_days}} {{days}}s.",
            is_published=True,
        ),
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            grain=Granularity.WEEK,
            story_date=datetime(2020, 1, 1),
            story_type=StoryType.ACCELERATING_GROWTH,
            metric_id="NewBizDeals",
            title="d/d growth is slowing down",
            title_template="{{pop}} growth is slowing down",
            detail="The d/d growth rate for NewBizDeals is slowing down. It is currently 10% and down from the 15% "
            "average over the past 5 days.",
            detail_template="The {{pop}} growth rate for {{metric.label}} is slowing down. It is currently {{"
            "current_growth}}% and down from the {{reference_growth}}% average over the past {{"
            "reference_period_days}} {{grain}}s.",
        ),
        Story(
            genre=StoryGenre.TRENDS,
            story_group=StoryGroup.TREND_CHANGES,
            grain=Granularity.WEEK,
            story_date=datetime(2020, 1, 1),
            story_type=StoryType.NEW_UPWARD_TREND,
            metric_id="NewBizDeals",
            title="d/d growth is slowing down",
            title_template="{{pop}} growth is slowing down",
            detail="The d/d growth rate for NewBizDeals is slowing down. It is currently 10% and down from the 15% "
            "average over the past 5 days.",
            detail_template="The {{pop}} growth rate for {{metric.label}} is slowing down. It is currently {{"
            "current_growth}}% and down from the {{reference_growth}}% average over the past {{"
            "reference_period_days}} {{grain}}s.",
        ),
        Story(
            genre=StoryGenre.TRENDS,
            story_group=StoryGroup.TREND_EXCEPTIONS,
            grain=Granularity.MONTH,
            story_date=datetime(2020, 1, 1),
            story_type=StoryType.SPIKE,
            metric_id="NewBizDeals",
            title="d/d growth is slowing down",
            title_template="{{pop}} growth is slowing down",
            detail="The d/d growth rate for NewBizDeals is slowing down. It is currently 10% and down from the 15% "
            "average over the past 5 days.",
            detail_template="The {{pop}} growth rate for {{metric.label}} is slowing down. It is currently {{"
            "current_growth}}% and down from the {{reference_growth}}% average over the past {{"
            "reference_period_days}} {{grain}}s.",
        ),
        Story(
            genre=StoryGenre.PERFORMANCE,
            story_group=StoryGroup.LIKELY_STATUS,
            grain=Granularity.MONTH,
            story_date=datetime(2020, 1, 1),
            story_type=StoryType.LIKELY_OFF_TRACK,
            metric_id="NewBizDeals",
            title="d/d growth is slowing down",
            title_template="{{pop}} growth is slowing down",
            detail="The d/d growth rate for NewBizDeals is slowing down. It is currently 10% and down from the 15% "
            "average over the past 5 days.",
            detail_template="The {{pop}} growth rate for {{metric.label}} is slowing down. It is currently {{"
            "current_growth}}% and down from the {{reference_growth}}% average over the past {{"
            "reference_period_days}} {{grain}}s.",
        ),
        Story(
            genre=StoryGenre.PERFORMANCE,
            story_group=StoryGroup.STATUS_CHANGE,
            grain=Granularity.MONTH,
            story_date=datetime(2020, 1, 1),
            story_type=StoryType.IMPROVING_STATUS,
            metric_id="NewBizDeals",
            title="d/d growth is slowing down",
            title_template="{{pop}} growth is slowing down",
            detail="The d/d growth rate for NewBizDeals is slowing down. It is currently 10% and down from the 15% "
            "average over the past 5 days.",
            detail_template="The {{pop}} growth rate for {{metric.label}} is slowing down. It is currently {{"
            "current_growth}}% and down from the {{reference_growth}}% average over the past {{"
            "reference_period_days}} {{grain}}s.",
        ),
        Story(
            genre=StoryGenre.ROOT_CAUSES,
            story_group=StoryGroup.SEGMENT_DRIFT,
            grain=Granularity.MONTH,
            story_date=datetime(2020, 1, 1),
            story_type=StoryType.SHRINKING_SEGMENT,
            metric_id="NewBizDeals",
            title="d/d growth is slowing down",
            title_template="{{pop}} growth is slowing down",
            detail="The d/d growth rate for NewBizDeals is slowing down. It is currently 10% and down from the 15% "
            "average over the past 5 days.",
            detail_template="The {{pop}} growth rate for {{metric.label}} is slowing down. It is currently {{"
            "current_growth}}% and down from the {{reference_growth}}% average over the past {{"
            "reference_period_days}} {{grain}}s.",
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
    response = client.get("/v1/stories/?genres=GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 3
    for result in data["results"]:
        assert result["genre"] == StoryGenre.GROWTH.value
        assert result["story_group"] == StoryGroup.GROWTH_RATES.value

    # Test filtering by metric_id
    response = client.get("/v1/stories?metric_ids=CAC")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1
    assert data["results"][0]["metric_id"] == "CAC"

    # Test filtering by story_type
    response = client.get("/v1/stories?story_types=ACCELERATING_GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 2
    assert data["results"][0]["story_type"] == StoryType.ACCELERATING_GROWTH.value

    # Test combining multiple filters
    response = client.get("/v1/stories?story_groups=GROWTH_RATES&story_types=ACCELERATING_GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 2
    assert data["results"][0]["genre"] == StoryGenre.GROWTH.value
    assert data["results"][0]["story_group"] == StoryGroup.GROWTH_RATES.value
    assert data["results"][0]["grain"] == Granularity.DAY.value
    assert data["results"][0]["metric_id"] == "NewMRR"

    # Test Multiple story types based filtering
    response = client.get("/v1/stories?story_types=NEW_UPWARD_TREND&story_types=ACCELERATING_GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 3

    # Test Multiple story group based filtering
    response = client.get("/v1/stories?story_groups=TREND_CHANGES&story_groups=GROWTH_RATES")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 4

    # Test Multiple metric ids based filtering
    response = client.get("/v1/stories?metric_ids=NewBizDeals&metric_ids=CAC")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 7

    # Test multiple genre based filtering
    response = client.get("/v1/stories/?genres=GROWTH&genres=TRENDS")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 5

    # Test multiple grains based filtering
    response = client.get("/v1/stories?grains=day&grains=week")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 4

    # Testing with multiple filters
    response = client.get("/v1/stories?grains=day&grains=week&genres=GROWTH&genres=TRENDS")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 4

    # Test filtering by story_date
    response = client.get("/v1/stories?story_date=2022-01-01")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == len(stories)
    assert data["results"][0]["story_date"] == "2020-01-01T00:00:00+0000"

    response = client.get("/v1/stories?digest=PORTFOLIO&section=STATUS_CHANGES")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1

    response = client.get("/v1/stories?digest=PORTFOLIO&section=LIKELY_MISSES")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1

    response = client.get("/v1/stories?digest=PORTFOLIO&section=BIG_MOVES")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1

    response = client.get("/v1/stories?digest=PORTFOLIO&section=PROMISING_TRENDS")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 3

    response = client.get("/v1/stories?digest=PORTFOLIO&section=CONCERNING_TRENDS")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1

    response = client.get("/v1/stories?digest=METRIC&section=WHAT_IS_HAPPENING")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 5

    response = client.get("/v1/stories?digest=METRIC&section=WHY_IS_IT_HAPPENING")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1

    response = client.get("/v1/stories?digest=METRIC&section=WHAT_HAPPENS_NEXT")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1
