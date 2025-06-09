from datetime import datetime, timedelta

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


@pytest.mark.asyncio
async def test_get_story_stats_basic_v2(db_session, async_client, jwt_payload):
    """Test basic functionality of the story stats endpoint."""
    tenant_id = jwt_payload["tenant_id"]
    # Set tenant context
    set_tenant_id(tenant_id)

    # Create test stories with different dates
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    last_week = today - timedelta(days=7)

    stories = [
        # Today's stories
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            story_date=today,
            grain=Granularity.DAY,
            story_type=StoryType.SLOWING_GROWTH,
            metric_id="CAC",
            title="Test Story 1",
            title_template="Test Template 1",
            detail="Test Detail 1",
            detail_template="Test Detail Template 1",
            is_salient=True,
            in_cool_off=False,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=2,
        ),
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            story_date=today,
            grain=Granularity.DAY,
            story_type=StoryType.ACCELERATING_GROWTH,
            metric_id="NewMRR",
            title="Test Story 2",
            title_template="Test Template 2",
            detail="Test Detail 2",
            detail_template="Test Detail Template 2",
            is_salient=True,
            in_cool_off=False,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=2,
        ),
        # Yesterday's stories
        Story(
            genre=StoryGenre.TRENDS,
            story_group=StoryGroup.TREND_CHANGES,
            story_date=yesterday,
            grain=Granularity.WEEK,
            story_type=StoryType.NEW_UPWARD_TREND,
            metric_id="NewBizDeals",
            title="Test Story 3",
            title_template="Test Template 3",
            detail="Test Detail 3",
            detail_template="Test Detail Template 3",
            is_salient=False,
            in_cool_off=False,
            is_heuristic=False,
            tenant_id=tenant_id,
            version=2,
        ),
        # Last week's stories
        Story(
            genre=StoryGenre.PERFORMANCE,
            story_group=StoryGroup.STATUS_CHANGE,
            story_date=last_week,
            grain=Granularity.MONTH,
            story_type=StoryType.IMPROVING_STATUS,
            metric_id="Revenue",
            title="Test Story 4",
            title_template="Test Template 4",
            detail="Test Detail 4",
            detail_template="Test Detail Template 4",
            is_salient=True,
            in_cool_off=True,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=2,
        ),
    ]

    db_session.add_all(stories)
    await db_session.flush()

    # Test basic stats retrieval
    response = await async_client.get("/v2/stories/stats")
    assert response.status_code == status.HTTP_200_OK

    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 3  # Should have stats for 3 different dates

    # Verify the structure of the response
    for date_stats in data:
        assert "story_date" in date_stats
        assert "count" in date_stats
        assert isinstance(date_stats["count"], int)

    # Verify the counts are correct
    date_counts = {item["story_date"].split("T")[0]: item["count"] for item in data}
    assert date_counts[today.strftime("%Y-%m-%d")] == 2
    assert date_counts[yesterday.strftime("%Y-%m-%d")] == 1
    assert date_counts[last_week.strftime("%Y-%m-%d")] == 1


@pytest.mark.asyncio
async def test_get_story_stats_with_filters_v2(db_session, async_client, jwt_payload):
    """Test story stats endpoint with various filters."""
    tenant_id = jwt_payload["tenant_id"]
    # Set tenant context
    set_tenant_id(tenant_id)

    # Create test stories with different attributes
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    stories = [
        # Growth stories
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            story_date=today,
            grain=Granularity.DAY,
            story_type=StoryType.SLOWING_GROWTH,
            metric_id="CAC",
            title="Test Story 1",
            title_template="Test Template 1",
            detail="Test Detail 1",
            detail_template="Test Detail Template 1",
            is_salient=True,
            in_cool_off=False,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=2,
        ),
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            story_date=today,
            grain=Granularity.DAY,
            story_type=StoryType.ACCELERATING_GROWTH,
            metric_id="NewMRR",
            title="Test Story 2",
            title_template="Test Template 2",
            detail="Test Detail 2",
            detail_template="Test Detail Template 2",
            is_salient=True,
            in_cool_off=False,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=2,
        ),
        # Trend stories
        Story(
            genre=StoryGenre.TRENDS,
            story_group=StoryGroup.TREND_CHANGES,
            story_date=today,
            grain=Granularity.WEEK,
            story_type=StoryType.NEW_UPWARD_TREND,
            metric_id="NewBizDeals",
            title="Test Story 3",
            title_template="Test Template 3",
            detail="Test Detail 3",
            detail_template="Test Detail Template 3",
            is_salient=False,
            in_cool_off=False,
            is_heuristic=False,
            tenant_id=tenant_id,
            version=2,
        ),
        # Performance stories
        Story(
            genre=StoryGenre.PERFORMANCE,
            story_group=StoryGroup.STATUS_CHANGE,
            story_date=today,
            grain=Granularity.MONTH,
            story_type=StoryType.IMPROVING_STATUS,
            metric_id="Revenue",
            title="Test Story 4",
            title_template="Test Template 4",
            detail="Test Detail 4",
            detail_template="Test Detail Template 4",
            is_salient=True,
            in_cool_off=True,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=2,
        ),
    ]

    db_session.add_all(stories)
    await db_session.flush()

    # Test filtering by story_group
    response = await async_client.get("/v2/stories/stats?story_groups=GROWTH_RATES")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1  # Should have stats for 1 date
    assert data[0]["count"] == 2  # Should have 2 stories for GROWTH_RATES

    # Test filtering by genre
    response = await async_client.get("/v2/stories/stats?genres=TRENDS")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1  # Should have stats for 1 date
    assert data[0]["count"] == 1  # Should have 1 story for TRENDS genre

    # Test filtering by story_type
    response = await async_client.get("/v2/stories/stats?story_types=ACCELERATING_GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1  # Should have stats for 1 date
    assert data[0]["count"] == 1  # Should have 1 story for ACCELERATING_GROWTH

    # Test filtering by grain
    response = await async_client.get("/v2/stories/stats?grains=week")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1  # Should have stats for 1 date
    assert data[0]["count"] == 1  # Should have 1 story with WEEK grain

    # Test filtering by metric_id
    response = await async_client.get("/v2/stories/stats?metric_ids=NewBizDeals")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1  # Should have stats for 1 date
    assert data[0]["count"] == 1  # Should have 1 story for NewBizDeals

    # Test filtering by is_heuristic
    response = await async_client.get("/v2/stories/stats?is_heuristic=true")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1  # Should have stats for 1 date
    assert data[0]["count"] == 3  # Should have 3 stories with is_heuristic=True

    # Test filtering by digest and section
    response = await async_client.get("/v2/stories/stats?digest=PORTFOLIO&section=STATUS_CHANGES")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1  # Should have stats for 1 date
    assert data[0]["count"] == 1  # Should have 1 story for STATUS_CHANGES section

    # Test combining multiple filters
    response = await async_client.get("/v2/stories/stats?genres=GROWTH&story_types=SLOWING_GROWTH")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1  # Should have stats for 1 date
    assert data[0]["count"] == 1  # Should have 1 story matching both filters


@pytest.mark.asyncio
async def test_get_story_stats_date_range_v2(db_session, async_client, jwt_payload):
    """Test story stats endpoint with date range filters."""
    tenant_id = jwt_payload["tenant_id"]
    # Set tenant context
    set_tenant_id(tenant_id)

    # Create test stories with different dates
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    last_week = today - timedelta(days=7)
    last_month = today - timedelta(days=30)

    stories = [
        # Today's story
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            story_date=today,
            grain=Granularity.DAY,
            story_type=StoryType.SLOWING_GROWTH,
            metric_id="CAC",
            title="Today's Story",
            title_template="Template",
            detail="Detail",
            detail_template="Detail Template",
            is_salient=True,
            in_cool_off=False,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=2,
        ),
        # Yesterday's story
        Story(
            genre=StoryGenre.TRENDS,
            story_group=StoryGroup.TREND_CHANGES,
            story_date=yesterday,
            grain=Granularity.WEEK,
            story_type=StoryType.NEW_UPWARD_TREND,
            metric_id="NewBizDeals",
            title="Yesterday's Story",
            title_template="Template",
            detail="Detail",
            detail_template="Detail Template",
            is_salient=False,
            in_cool_off=False,
            is_heuristic=False,
            tenant_id=tenant_id,
            version=2,
        ),
        # Last week's story
        Story(
            genre=StoryGenre.PERFORMANCE,
            story_group=StoryGroup.STATUS_CHANGE,
            story_date=last_week,
            grain=Granularity.MONTH,
            story_type=StoryType.IMPROVING_STATUS,
            metric_id="Revenue",
            title="Last Week's Story",
            title_template="Template",
            detail="Detail",
            detail_template="Detail Template",
            is_salient=True,
            in_cool_off=True,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=2,
        ),
        # Last month's story
        Story(
            genre=StoryGenre.ROOT_CAUSES,
            story_group=StoryGroup.SEGMENT_DRIFT,
            story_date=last_month,
            grain=Granularity.MONTH,
            story_type=StoryType.SHRINKING_SEGMENT,
            metric_id="Churn",
            title="Last Month's Story",
            title_template="Template",
            detail="Detail",
            detail_template="Detail Template",
            is_salient=True,
            in_cool_off=False,
            is_heuristic=True,
            tenant_id=tenant_id,
            version=1,
        ),
    ]

    db_session.add_all(stories)
    await db_session.flush()

    # Test with story_date_start filter
    one_week_ago = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    response = await async_client.get(f"/v2/stories/stats?story_date_start={one_week_ago}")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 3  # Should have stats for today, yesterday, and last week

    # Test with story_date_end filter
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    response = await async_client.get(f"/v2/stories/stats?story_date_end={yesterday_str}")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2  # Should have stats for yesterday, last week, and last month

    # Test with both story_date_start and story_date_end filters
    two_weeks_ago = (today - timedelta(days=14)).strftime("%Y-%m-%d")
    three_days_ago = (today - timedelta(days=3)).strftime("%Y-%m-%d")
    response = await async_client.get(
        f"/v2/stories/stats?story_date_start={two_weeks_ago}&story_date_end={three_days_ago}"
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1  # Should only have stats for last week

    # Test default behavior (past 2 months)
    response = await async_client.get("/v2/stories/stats")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 3  # Should have stats for all dates except 1


@pytest.mark.asyncio
async def test_get_story_stats_empty_results_v2(db_session, async_client, jwt_payload):
    """Test story stats endpoint when no stories match the filters."""
    tenant_id = jwt_payload["tenant_id"]
    # Set tenant context
    set_tenant_id(tenant_id)

    # Create a single story
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    story = Story(
        genre=StoryGenre.GROWTH,
        story_group=StoryGroup.GROWTH_RATES,
        story_date=today,
        grain=Granularity.DAY,
        story_type=StoryType.SLOWING_GROWTH,
        metric_id="CAC",
        title="Test Story",
        title_template="Test Template",
        detail="Test Detail",
        detail_template="Test Detail Template",
        is_salient=True,
        in_cool_off=False,
        is_heuristic=True,
        tenant_id=tenant_id,
        version=2,
    )

    db_session.add(story)
    await db_session.flush()

    # Test with a filter that won't match any stories
    response = await async_client.get("/v2/stories/stats?metric_ids=NonExistentMetric")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 0  # Should have no results

    # Test with multiple filters that won't match any stories
    response = await async_client.get("/v2/stories/stats?story_groups=RECORD_VALUES&genres=PERFORMANCE")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 0  # Should have no results
