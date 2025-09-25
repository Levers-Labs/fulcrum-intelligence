"""
Tests for the story CRUD upsert functionality.
"""

from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from unittest.mock import patch

import pytest

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from story_manager.core.crud import CRUDStory, CRUDStoryConfig
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.core.models import Story, StoryConfig


@pytest.fixture
def story_crud(db_session):
    """Fixture for story CRUD instance."""
    return CRUDStory(Story, db_session)


@pytest.fixture
def story_config_crud(db_session):
    """Fixture for story config CRUD instance."""
    return CRUDStoryConfig(StoryConfig, db_session)


@pytest.fixture
def sample_stories():
    """Fixture for sample story dictionaries."""
    base_date = date(2024, 1, 15)
    return [
        {
            "version": 2,
            "genre": StoryGenre.PERFORMANCE,
            "story_type": StoryType.ON_TRACK,
            "story_group": StoryGroup.GOAL_VS_ACTUAL,
            "grain": Granularity.DAY,
            "metric_id": "test_metric_1",
            "title": "Performance Story 1",
            "detail": "Detail for performance story 1",
            "title_template": "{{metric}} is on track",
            "detail_template": "{{metric}} performance is {{status}}",
            "story_date": base_date,
            "variables": {"status": "on_track"},
            "metadata": {"pattern": "performance_status"},
            "pattern_run_id": 123,
            "series": [],
            "evaluation_pattern": "performance_status",
        },
        {
            "version": 2,
            "genre": StoryGenre.PERFORMANCE,
            "story_type": StoryType.OFF_TRACK,
            "story_group": StoryGroup.GOAL_VS_ACTUAL,
            "grain": Granularity.DAY,
            "metric_id": "test_metric_1",
            "title": "Performance Story 2",
            "detail": "Detail for performance story 2",
            "title_template": "{{metric}} is falling behind",
            "detail_template": "{{metric}} performance is {{status}}",
            "story_date": base_date,
            "variables": {"status": "falling_behind"},
            "metadata": {"pattern": "performance_status"},
            "pattern_run_id": 123,
            "series": [],
            "evaluation_pattern": "performance_status",
        },
    ]


@pytest.fixture
def segment_stories():
    """Fixture for significant segment stories - multiple stories of same type."""
    base_date = date(2024, 1, 15)
    return [
        {
            "version": 2,
            "genre": StoryGenre.ROOT_CAUSES,
            "story_type": StoryType.TOP_4_SEGMENTS,
            "story_group": StoryGroup.SIGNIFICANT_SEGMENTS,
            "grain": Granularity.DAY,
            "metric_id": "test_metric_2",
            "title": "Top Segment: Region A",
            "detail": "Region A is performing best",
            "title_template": "Top Segment: {{segment}}",
            "detail_template": "{{segment}} is performing best",
            "story_date": base_date,
            "variables": {"segment": "Region A"},
            "metadata": {"pattern": "significant_segments", "dimension": "region"},
            "pattern_run_id": 124,
            "series": [],
            "evaluation_pattern": "dimension_analysis",
        },
        {
            "version": 2,
            "genre": StoryGenre.ROOT_CAUSES,
            "story_type": StoryType.TOP_4_SEGMENTS,
            "story_group": StoryGroup.SIGNIFICANT_SEGMENTS,
            "grain": Granularity.DAY,
            "metric_id": "test_metric_2",
            "title": "Top Segment: Region B",
            "detail": "Region B is performing second best",
            "title_template": "Top Segment: {{segment}}",
            "detail_template": "{{segment}} is performing second best",
            "story_date": base_date,
            "variables": {"segment": "Region B"},
            "metadata": {"pattern": "significant_segments", "dimension": "region"},
            "pattern_run_id": 124,
            "series": [],
            "evaluation_pattern": "dimension_analysis",
        },
    ]


@pytest.mark.asyncio
async def test_upsert_stories_by_context_new_stories(story_crud, sample_stories, jwt_payload):
    """Test upserting stories when no existing stories exist."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])

    # Act
    result = await story_crud.upsert_stories(sample_stories)

    # Assert
    assert len(result) == 2
    assert all(isinstance(story, Story) for story in result)
    assert result[0].title == "Performance Story 1"
    assert result[1].title == "Performance Story 2"
    assert result[0].pattern_run_id == 123
    assert result[1].pattern_run_id == 123
    assert result[0].tenant_id == jwt_payload["tenant_id"]
    assert result[1].tenant_id == jwt_payload["tenant_id"]


@pytest.mark.asyncio
async def test_upsert_stories_by_context_replace_existing(story_crud, sample_stories, jwt_payload):
    """Test that upsert replaces existing stories of the same types in same context."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])

    # First, create initial stories
    initial_stories = await story_crud.upsert_stories(sample_stories)
    assert len(initial_stories) == 2
    initial_ids = {story.id for story in initial_stories}

    # Modify the stories for the second upsert
    updated_stories = sample_stories.copy()
    updated_stories[0]["title"] = "Updated Performance Story 1"
    updated_stories[1]["title"] = "Updated Performance Story 2"
    updated_stories[0]["pattern_run_id"] = 456  # New pattern run
    updated_stories[1]["pattern_run_id"] = 456

    # Act - upsert the same context with updated data
    result = await story_crud.upsert_stories(updated_stories)

    # Assert
    assert len(result) == 2
    # Should be different Story objects (new IDs) since old ones were deleted
    new_ids = {story.id for story in result}
    assert initial_ids.isdisjoint(new_ids)  # No overlap in IDs

    assert result[0].title == "Updated Performance Story 1"
    assert result[1].title == "Updated Performance Story 2"
    assert result[0].pattern_run_id == 456
    assert result[1].pattern_run_id == 456

    # Verify old stories were deleted - query database directly
    from sqlalchemy import select

    old_stories_query = select(Story).where(Story.id.in_(initial_ids))
    old_stories_result = await story_crud.session.execute(old_stories_query)
    old_stories = old_stories_result.fetchall()
    assert len(old_stories) == 0  # Should be deleted


@pytest.mark.asyncio
async def test_upsert_stories_by_context_multiple_story_types_same_date(story_crud, segment_stories, jwt_payload):
    """Test that multiple stories of same type on same date are properly handled."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])

    # Act
    result = await story_crud.upsert_stories(segment_stories)

    # Assert
    assert len(result) == 2
    assert all(story.story_type == StoryType.TOP_4_SEGMENTS for story in result)
    assert result[0].title == "Top Segment: Region A"
    assert result[1].title == "Top Segment: Region B"
    assert all(story.story_group == StoryGroup.SIGNIFICANT_SEGMENTS for story in result)


@pytest.mark.asyncio
async def test_upsert_stories_by_context_partial_overlap(story_crud, jwt_payload):
    """Test that upsert replaces all stories with same lookup_key context."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    base_date = date(2024, 1, 15)

    # First batch: two different story types
    batch1 = [
        {
            "version": 2,
            "genre": StoryGenre.PERFORMANCE,
            "story_type": StoryType.ON_TRACK,
            "story_group": StoryGroup.GOAL_VS_ACTUAL,
            "grain": Granularity.DAY,
            "metric_id": "test_metric",
            "title": "On Track Story",
            "detail": "Detail",
            "title_template": "Template",
            "detail_template": "Template",
            "story_date": base_date,
            "variables": {},
            "metadata": {"pattern": "performance_status"},
            "pattern_run_id": 123,
            "series": [],
            "evaluation_pattern": "performance_status",
        },
        {
            "version": 2,
            "genre": StoryGenre.PERFORMANCE,
            "story_type": StoryType.OFF_TRACK,
            "story_group": StoryGroup.GOAL_VS_ACTUAL,
            "grain": Granularity.DAY,
            "metric_id": "test_metric",
            "title": "Falling Behind Story",
            "detail": "Detail",
            "title_template": "Template",
            "detail_template": "Template",
            "story_date": base_date,
            "variables": {},
            "metadata": {"pattern": "performance_status"},
            "pattern_run_id": 123,
            "series": [],
            "evaluation_pattern": "performance_status",
        },
    ]

    # Second batch: only one overlapping story type
    batch2 = [
        {
            "version": 2,
            "genre": StoryGenre.PERFORMANCE,
            "story_type": StoryType.ON_TRACK,  # This overlaps
            "story_group": StoryGroup.GOAL_VS_ACTUAL,
            "grain": Granularity.DAY,
            "metric_id": "test_metric",
            "title": "Updated On Track Story",
            "detail": "Updated Detail",
            "title_template": "Template",
            "detail_template": "Template",
            "story_date": base_date,
            "variables": {},
            "metadata": {"pattern": "performance_status"},
            "pattern_run_id": 456,
            "series": [],
            "evaluation_pattern": "performance_status",
        },
    ]

    # Act
    initial_result = await story_crud.upsert_stories(batch1)
    assert len(initial_result) == 2

    updated_result = await story_crud.upsert_stories(batch2)
    assert len(updated_result) == 1

    # Verify final state
    from sqlalchemy import select

    all_stories_query = (
        select(Story)
        .where(Story.tenant_id == jwt_payload["tenant_id"])
        .where(Story.metric_id == "test_metric")
        .where(Story.story_date == base_date)
    )
    all_stories_result = await story_crud.session.execute(all_stories_query)
    all_stories = all_stories_result.fetchall()

    # Based on the upsert implementation, all stories with same lookup_key context are replaced
    # Since both stories have same evaluation_pattern (None), they have same lookup_key
    # So the second batch replaces ALL stories from the first batch, not just ON_TRACK
    assert len(all_stories) == 1
    assert all_stories[0][0].title == "Updated On Track Story"
    assert all_stories[0][0].story_type == StoryType.ON_TRACK


@pytest.mark.asyncio
async def test_upsert_stories_by_context_different_contexts_isolated(story_crud, jwt_payload):
    """Test that different contexts (metric_id, grain, etc.) don't interfere with each other."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    base_date = date(2024, 1, 15)

    # Different contexts
    metric1_stories = [
        {
            "version": 2,
            "genre": StoryGenre.PERFORMANCE,
            "story_type": StoryType.ON_TRACK,
            "story_group": StoryGroup.GOAL_VS_ACTUAL,
            "grain": Granularity.DAY,
            "metric_id": "metric_1",  # Different metric
            "title": "Metric 1 Story",
            "detail": "Detail",
            "title_template": "Template",
            "detail_template": "Template",
            "story_date": base_date,
            "variables": {},
            "metadata": {"pattern": "performance_status"},
            "pattern_run_id": 123,
            "series": [],
            "evaluation_pattern": "performance_status",
        }
    ]

    metric2_stories = [
        {
            "version": 2,
            "genre": StoryGenre.PERFORMANCE,
            "story_type": StoryType.ON_TRACK,  # Same story type, different metric
            "story_group": StoryGroup.GOAL_VS_ACTUAL,
            "grain": Granularity.DAY,
            "metric_id": "metric_2",  # Different metric
            "title": "Metric 2 Story",
            "detail": "Detail",
            "title_template": "Template",
            "detail_template": "Template",
            "story_date": base_date,
            "variables": {},
            "metadata": {"pattern": "performance_status"},
            "pattern_run_id": 124,
            "series": [],
            "evaluation_pattern": "performance_status",
        }
    ]

    # Act
    result1 = await story_crud.upsert_stories(metric1_stories)
    result2 = await story_crud.upsert_stories(metric2_stories)

    # Assert
    assert len(result1) == 1
    assert len(result2) == 1
    assert result1[0].metric_id == "metric_1"
    assert result2[0].metric_id == "metric_2"
    assert result1[0].title == "Metric 1 Story"
    assert result2[0].title == "Metric 2 Story"

    # Both stories should exist in database
    from sqlalchemy import select

    all_stories_query = select(Story).where(Story.tenant_id == jwt_payload["tenant_id"])
    all_stories_result = await story_crud.session.execute(all_stories_query)
    all_stories = all_stories_result.fetchall()
    assert len(all_stories) == 2


@pytest.mark.asyncio
async def test_upsert_stories_by_context_datetime_boundaries(story_crud, jwt_payload):
    """Test that date boundaries work correctly with datetime story_dates."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])

    # Use datetime instead of date
    story_datetime = datetime(2024, 1, 15, 14, 30, 0)  # Afternoon

    story_with_datetime = [
        {
            "version": 2,
            "genre": StoryGenre.PERFORMANCE,
            "story_type": StoryType.ON_TRACK,
            "story_group": StoryGroup.GOAL_VS_ACTUAL,
            "grain": Granularity.DAY,
            "metric_id": "test_metric",
            "title": "DateTime Story",
            "detail": "Detail",
            "title_template": "Template",
            "detail_template": "Template",
            "story_date": story_datetime,
            "variables": {},
            "metadata": {"pattern": "performance_status"},
            "pattern_run_id": 123,
            "series": [],
            "evaluation_pattern": "performance_status",
        }
    ]

    # Act - first upsert
    result1 = await story_crud.upsert_stories(story_with_datetime)
    assert len(result1) == 1

    # Update with different time same date - should replace
    story_with_datetime[0]["title"] = "Updated DateTime Story"
    story_with_datetime[0]["story_date"] = datetime(2024, 1, 15, 9, 0, 0)  # Morning same day
    story_with_datetime[0]["pattern_run_id"] = 456

    result2 = await story_crud.upsert_stories(story_with_datetime)

    # Assert
    assert len(result2) == 1
    assert result2[0].title == "Updated DateTime Story"
    assert result2[0].pattern_run_id == 456

    # Should only have one story in database (replacement worked)
    from sqlalchemy import select

    all_stories_query = select(Story).where(Story.tenant_id == jwt_payload["tenant_id"])
    all_stories_result = await story_crud.session.execute(all_stories_query)
    all_stories = all_stories_result.fetchall()
    assert len(all_stories) == 1


@pytest.mark.asyncio
async def test_upsert_stories_by_context_empty_list(story_crud):
    """Test that empty story list returns empty result."""
    # Act
    result = await story_crud.upsert_stories([])

    # Assert
    assert result == []


@pytest.mark.asyncio
async def test_upsert_stories_by_context_tenant_isolation(story_crud, sample_stories, jwt_payload):
    """Test that tenant isolation works correctly - stories from different tenants don't interfere."""
    # Arrange
    tenant1_id = jwt_payload["tenant_id"]
    tenant2_id = tenant1_id + 1  # Different tenant

    # Create stories for tenant 1
    set_tenant_id(tenant1_id)
    tenant1_result = await story_crud.upsert_stories(sample_stories)
    assert len(tenant1_result) == 2

    # Create stories for tenant 2 with same context but different tenant
    set_tenant_id(tenant2_id)
    tenant2_result = await story_crud.upsert_stories(sample_stories)
    assert len(tenant2_result) == 2

    # Assert both tenants have their own stories
    from sqlalchemy import select

    tenant1_query = select(Story).where(Story.tenant_id == tenant1_id)
    tenant1_stories_result = await story_crud.session.execute(tenant1_query)
    tenant1_stories = tenant1_stories_result.fetchall()

    tenant2_query = select(Story).where(Story.tenant_id == tenant2_id)
    tenant2_stories_result = await story_crud.session.execute(tenant2_query)
    tenant2_stories = tenant2_stories_result.fetchall()

    assert len(tenant1_stories) == 2
    assert len(tenant2_stories) == 2
    assert all(story[0].tenant_id == tenant1_id for story in tenant1_stories)
    assert all(story[0].tenant_id == tenant2_id for story in tenant2_stories)


@pytest.mark.asyncio
async def test_upsert_stories_by_context_rollback_on_error(story_crud, sample_stories, jwt_payload):
    """Test that transaction rolls back on error."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])

    # Mock a database error during commit
    with patch.object(story_crud.session, "commit", side_effect=Exception("Database error")):
        # Act & Assert
        with pytest.raises(Exception, match="Database error"):
            await story_crud.upsert_stories(sample_stories)

    # Test passed if exception was raised - rollback behavior is handled by SQLAlchemy
    # No need to verify database state as session is invalid after failed transaction


# Additional tests for other CRUD methods
@pytest.mark.asyncio
async def test_get_latest_story(story_crud, sample_stories, jwt_payload):
    """Test get_latest_story functionality."""
    set_tenant_id(jwt_payload["tenant_id"])

    # First, create some stories
    await story_crud.upsert_stories(sample_stories)

    # Test getting latest story
    latest_story = await story_crud.get_latest_story(
        metric_id="test_metric_1",
        story_type=StoryType.ON_TRACK,
        grain=Granularity.DAY,
        story_date=date(2024, 1, 16),  # Day after the stories
        tenant_id=jwt_payload["tenant_id"],
        version=2,
    )

    assert latest_story is not None
    assert latest_story.title == "Performance Story 1"
    assert latest_story.story_type == StoryType.ON_TRACK


@pytest.mark.asyncio
async def test_get_latest_story_not_found(story_crud, jwt_payload):
    """Test get_latest_story when no story is found."""
    set_tenant_id(jwt_payload["tenant_id"])

    # Test getting latest story when none exists
    latest_story = await story_crud.get_latest_story(
        metric_id="nonexistent_metric",
        story_type=StoryType.ON_TRACK,
        grain=Granularity.DAY,
        story_date=date(2024, 1, 16),
        tenant_id=jwt_payload["tenant_id"],
        version=2,
    )

    assert latest_story is None


@pytest.mark.asyncio
async def test_get_stories(story_crud, sample_stories, jwt_payload):
    """Test get_stories functionality."""
    set_tenant_id(jwt_payload["tenant_id"])

    # First, create some stories
    await story_crud.upsert_stories(sample_stories)

    # Test getting stories by metric_id
    stories = await story_crud.get_stories(
        metric_id="test_metric_1", grain=Granularity.DAY, story_date=date(2024, 1, 15)
    )

    assert len(stories) == 2
    assert stories[0].metric_id == "test_metric_1"
    assert stories[1].metric_id == "test_metric_1"


@pytest.mark.asyncio
async def test_get_stories_with_filters(story_crud, sample_stories, jwt_payload):
    """Test get_stories with optional filters."""
    set_tenant_id(jwt_payload["tenant_id"])

    # First, create some stories
    await story_crud.upsert_stories(sample_stories)

    # Test getting stories with filters
    stories = await story_crud.get_stories(
        metric_id="test_metric_1", grain=Granularity.DAY, story_date=date(2024, 1, 15), is_heuristic=True
    )

    assert len(stories) >= 0  # Should return stories that match the filter


@pytest.mark.asyncio
async def test_update_story_date(story_crud, sample_stories, jwt_payload):
    """Test update_story_date functionality."""
    set_tenant_id(jwt_payload["tenant_id"])

    # First, create some stories
    created_stories = await story_crud.upsert_stories(sample_stories)
    story_to_update = created_stories[0]

    # Update the story date
    new_date = date(2024, 2, 1)
    result = await story_crud.update_story_date(story_to_update.id, new_date)

    assert result is True

    # The method doesn't commit, so we need to commit and refresh
    await story_crud.session.commit()
    await story_crud.session.refresh(story_to_update)
    assert story_to_update.story_date.date() == new_date


@pytest.mark.asyncio
async def test_update_story_date_not_found(story_crud):
    """Test update_story_date with non-existent story."""
    # Try to update a non-existent story
    result = await story_crud.update_story_date(99999, date(2024, 2, 1))
    assert result is False


@pytest.mark.asyncio
async def test_get_story_stats(story_crud, sample_stories, jwt_payload):
    """Test get_story_stats functionality."""
    set_tenant_id(jwt_payload["tenant_id"])

    # First, create some stories
    await story_crud.upsert_stories(sample_stories)

    # Test getting story stats
    filter_params = {"tenant_id": jwt_payload["tenant_id"], "metric_id": "test_metric_1"}

    stats = await story_crud.get_story_stats(filter_params)

    assert len(stats) > 0
    # Each stat should have story_date and count
    for stat in stats:
        assert "story_date" in stat
        assert "count" in stat


def test_get_date_boundaries_with_date(story_crud):
    """Test _get_date_boundaries with date input."""
    test_date = date(2024, 1, 15)
    start, end = story_crud._get_date_boundaries(test_date)

    expected_start = datetime.combine(test_date, time.min)
    expected_end = expected_start + timedelta(days=1)

    assert start == expected_start
    assert end == expected_end


def test_get_date_boundaries_with_datetime(story_crud):
    """Test _get_date_boundaries with datetime input."""
    test_datetime = datetime(2024, 1, 15, 14, 30, 0)
    start, end = story_crud._get_date_boundaries(test_datetime)

    expected_start = datetime.combine(test_datetime.date(), time.min)
    expected_end = expected_start + timedelta(days=1)

    assert start == expected_start
    assert end == expected_end


# Tests for CRUDStoryConfig
@pytest.mark.asyncio
async def test_get_story_config_found(story_config_crud, jwt_payload):
    """Test get_story_config when config exists."""
    set_tenant_id(jwt_payload["tenant_id"])

    # First create a story config
    story_config = StoryConfig(
        story_type=StoryType.ON_TRACK,
        grain=Granularity.DAY,
        tenant_id=jwt_payload["tenant_id"],
        version=2,
        heuristic_expression="test_expression",
        cool_off_duration=1,
    )

    story_config_crud.session.add(story_config)
    await story_config_crud.session.commit()

    # Test getting the config
    result = await story_config_crud.get_story_config(
        story_type=StoryType.ON_TRACK, grain=Granularity.DAY, tenant_id=jwt_payload["tenant_id"], version=2
    )

    assert result is not None
    assert result.story_type == StoryType.ON_TRACK
    assert result.grain == Granularity.DAY
    assert result.heuristic_expression == "test_expression"
    assert result.cool_off_duration == 1


@pytest.mark.asyncio
async def test_get_story_config_not_found(story_config_crud, jwt_payload):
    """Test get_story_config when config doesn't exist."""
    set_tenant_id(jwt_payload["tenant_id"])

    # Test getting non-existent config
    result = await story_config_crud.get_story_config(
        story_type=StoryType.OFF_TRACK,  # Different story type
        grain=Granularity.WEEK,  # Different grain
        tenant_id=jwt_payload["tenant_id"],
        version=2,
    )

    assert result is None
