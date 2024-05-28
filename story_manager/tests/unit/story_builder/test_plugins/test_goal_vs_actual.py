from datetime import date
from unittest.mock import AsyncMock

import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins.goal_vs_actual import GoalVsActualStoryBuilder

start_date = date(2023, 4, 7)
number_of_data_points = 90


@pytest.fixture
def goal_vs_actual_story_builder(
    mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, metric_values
):
    mock_query_service.get_metric = AsyncMock(return_value={"id": "metric_1", "label": "Metric 1"})
    return GoalVsActualStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.mark.asyncio
async def test_generate_goal_vs_actual_stories_no_min_data_points(goal_vs_actual_story_builder, targets_df):
    # Prepare
    goal_vs_actual_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    # Act
    result = await goal_vs_actual_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_goal_vs_actual_stories_on_track(goal_vs_actual_story_builder, targets_df):
    latest_date, _ = goal_vs_actual_story_builder._get_current_period_range(Granularity.DAY)
    latest_date = latest_date.strftime("%Y-%m-%d")
    targets_df = targets_df.copy()
    on_track_data = {
        "value": 100,
        "target": 50,
        "date": latest_date,
    }
    targets_df.loc[len(targets_df)] = on_track_data
    goal_vs_actual_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    result = await goal_vs_actual_story_builder.generate_stories("metric_1", Granularity.DAY)

    story = result[0]
    assert story["story_type"] == StoryType.ON_TRACK


@pytest.mark.asyncio
async def test_generate_goal_vs_actual_stories_off_track(goal_vs_actual_story_builder, targets_df):
    latest_date, _ = goal_vs_actual_story_builder._get_current_period_range(Granularity.DAY)
    latest_date = latest_date.strftime("%Y-%m-%d")
    targets_df = targets_df.copy()
    on_track_data = {
        "value": 0,
        "target": 50,
        "date": latest_date,
    }
    targets_df.loc[len(targets_df)] = on_track_data
    goal_vs_actual_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    result = await goal_vs_actual_story_builder.generate_stories("metric_1", Granularity.DAY)

    story = result[0]
    assert story["story_type"] == StoryType.OFF_TRACK


@pytest.mark.asyncio
async def test_generate_goal_vs_actual_stories_no_target(goal_vs_actual_story_builder, targets_df):
    latest_date, _ = goal_vs_actual_story_builder._get_current_period_range(Granularity.DAY)
    latest_date = latest_date.strftime("%Y-%m-%d")
    targets_df = targets_df.copy()
    on_track_data = {
        "value": 0,
        "target": None,
        "date": latest_date,
    }
    targets_df.loc[len(targets_df)] = on_track_data
    goal_vs_actual_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    result = await goal_vs_actual_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0
