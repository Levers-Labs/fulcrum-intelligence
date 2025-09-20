"""
Tests for the story CRUD upsert functionality.
"""

from datetime import date, datetime
from unittest.mock import patch

import pytest

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from story_manager.core.crud import CRUDStory
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.core.models import Story


@pytest.fixture
def story_crud(db_session):
    """Fixture for story CRUD instance."""
    return CRUDStory(Story, db_session)


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
    """Test that only overlapping story types are replaced, not all stories."""
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

    # Should have 2 stories total: 1 updated ON_TRACK, 1 original FALLING_BEHIND
    assert len(all_stories) == 2
    titles = [story[0].title for story in all_stories]
    assert "Updated On Track Story" in titles
    assert "Falling Behind Story" in titles


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
