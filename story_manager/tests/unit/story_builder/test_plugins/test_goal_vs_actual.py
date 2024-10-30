from datetime import date
from unittest.mock import AsyncMock

import numpy as np
import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins.goal_vs_actual import GoalVsActualStoryBuilder

start_date = date(2024, 3, 19)
number_of_data_points = 90


@pytest.fixture
def goal_vs_actual_story_builder(
    mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, metric_values, get_metric_resp
):
    mock_query_service.get_metric = AsyncMock(return_value=get_metric_resp)
    return GoalVsActualStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.mark.asyncio
async def test_generate_goal_vs_actual_stories_no_min_data_points(goal_vs_actual_story_builder, targets_df):
    # Prepare
    goal_vs_actual_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=pd.DataFrame())

    # Act
    result = await goal_vs_actual_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_goal_vs_actual_stories_on_track(goal_vs_actual_story_builder, targets_df):
    targets_df = targets_df.copy()
    on_track_data = {
        "value": 100,
        "target": 50,
        "date": pd.to_datetime(start_date),
    }
    targets_df.loc[len(targets_df)] = on_track_data
    goal_vs_actual_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    result = await goal_vs_actual_story_builder.generate_stories("metric_1", Granularity.DAY)

    story = result[0]
    assert story["story_type"] == StoryType.ON_TRACK


@pytest.mark.asyncio
async def test_generate_goal_vs_actual_stories_off_track(goal_vs_actual_story_builder, targets_df):
    targets_df = targets_df.copy()
    on_track_data = {
        "value": 0,
        "target": 50,
        "date": pd.to_datetime(start_date),
    }
    targets_df.loc[len(targets_df)] = on_track_data
    goal_vs_actual_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    result = await goal_vs_actual_story_builder.generate_stories("metric_1", Granularity.DAY)

    story = result[0]
    assert story["story_type"] == StoryType.OFF_TRACK


@pytest.mark.asyncio
async def test_generate_goal_vs_actual_stories_none_target(goal_vs_actual_story_builder, targets_df):
    targets_df = targets_df.copy()
    on_track_data = {
        "value": 0,
        "target": None,
        "date": start_date,
    }
    targets_df.loc[len(targets_df)] = on_track_data
    goal_vs_actual_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    result = await goal_vs_actual_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_goal_vs_actual_stories_nan_target(goal_vs_actual_story_builder, targets_df):
    targets_df = targets_df.copy()
    on_track_data = {
        "value": 0,
        "target": np.NaN,
        "date": pd.to_datetime(start_date),
    }
    targets_df.loc[len(targets_df)] = on_track_data
    goal_vs_actual_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    result = await goal_vs_actual_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_goal_vs_actual_stories_zero_target(goal_vs_actual_story_builder, targets_df):
    targets_df = targets_df.copy()
    data = {
        "value": 100,
        "target": 0,
        "date": pd.to_datetime(start_date),
    }
    targets_df.loc[len(targets_df)] = data
    goal_vs_actual_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    result = await goal_vs_actual_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0
