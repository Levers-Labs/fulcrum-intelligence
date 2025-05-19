from datetime import datetime

import pytest
from starlette import status

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.core.models import Story


@pytest.mark.asyncio
async def test_get_stories_v2(db_session, async_client, jwt_payload):
    tenant_id = jwt_payload["tenant_id"]
    # Set tenant context
    set_tenant_id(tenant_id)
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
            is_salient=False,
            in_cool_off=False,
            is_heuristic=False,
            tenant_id=tenant_id,
            version=2,
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
            is_salient=False,
            in_cool_off=False,
            is_heuristic=False,
            tenant_id=tenant_id,
            version=2,
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
            is_salient=False,
            in_cool_off=False,
            is_heuristic=False,
            tenant_id=tenant_id,
            version=2,
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
            is_salient=False,
            in_cool_off=False,
            is_heuristic=False,
            tenant_id=tenant_id,
            version=1,
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
            is_salient=False,
            in_cool_off=False,
            is_heuristic=False,
            tenant_id=tenant_id,
            version=2,
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
            is_salient=True,
            in_cool_off=False,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=2,
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
            is_salient=True,
            in_cool_off=True,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=2,
        ),
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            grain=Granularity.MONTH,
            story_date=datetime(2020, 1, 1),
            story_type=StoryType.SLOWING_GROWTH,
            metric_id="newInvoices",
            title="d/d growth is slowing down",
            title_template="{{pop}} growth is slowing down",
            detail="The d/d growth rate for NewBizDeals is slowing down. It is currently 10% and down from the 15% "
            "average over the past 5 days.",
            detail_template="The {{pop}} growth rate for {{metric.label}} is slowing down. It is currently {{"
            "current_growth}}% and down from the {{reference_growth}}% average over the past {{"
            "reference_period_days}} {{grain}}s.",
            is_salient=True,
            in_cool_off=True,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=2,
        ),
    ]
    db_session.add_all(stories)
    await db_session.flush()

    # Test listing all stories
    response = await async_client.get("/v2/stories/")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == len(stories) - 1

    # Test filtering by genre
    response = await async_client.get("/v2/stories/?genres=GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 4
    for result in data["results"]:
        assert result["genre"] == StoryGenre.GROWTH.value
        assert result["story_group"] == StoryGroup.GROWTH_RATES.value

    # Test filtering by metric_id
    response = await async_client.get("/v2/stories/?metric_ids=CAC")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1
    assert data["results"][0]["metric_id"] == "CAC"

    # Test filtering by story_type
    response = await async_client.get("/v2/stories/?story_types=ACCELERATING_GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 2
    assert data["results"][0]["story_type"] == StoryType.ACCELERATING_GROWTH.value

    # Test combining multiple filters
    response = await async_client.get("/v2/stories/?story_groups=GROWTH_RATES&story_types=ACCELERATING_GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 2
    assert data["results"][0]["genre"] == StoryGenre.GROWTH.value
    assert data["results"][0]["story_group"] == StoryGroup.GROWTH_RATES.value
    assert data["results"][0]["grain"] == Granularity.DAY.value
    assert data["results"][0]["metric_id"] == "NewMRR"
    assert data["results"][0]["version"] == 2

    # Test Multiple story types based filtering
    response = await async_client.get("/v2/stories/?story_types=NEW_UPWARD_TREND&story_types=ACCELERATING_GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 2

    # Test Multiple story group based filtering
    response = await async_client.get("/v2/stories/?story_groups=TREND_CHANGES&story_groups=GROWTH_RATES")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 4

    # Test multiple genre based filtering
    response = await async_client.get("/v2/stories/?genres=GROWTH&genres=TRENDS")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 5

    # Test multiple grains based filtering
    response = await async_client.get("/v2/stories/?grains=day&grains=week")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 3

    # Testing with multiple filters
    response = await async_client.get("/v2/stories/?grains=day&grains=week&genres=GROWTH&genres=TRENDS")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 3

    # Test filtering by story_date
    response = await async_client.get("/v2/stories/?story_date=2022-01-01")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == len(stories) - 1  # excluding version 1 story
    assert data["results"][0]["story_date"] == "2020-01-01T00:00:00+0000"

    response = await async_client.get("/v2/stories/?digest=PORTFOLIO&section=STATUS_CHANGES")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1

    response = await async_client.get("/v2/stories/?is_heuristic=True")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 3

    response = await async_client.get("/v2/stories/?is_heuristic=False")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 4  # excluding version 1 story
