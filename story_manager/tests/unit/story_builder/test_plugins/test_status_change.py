from datetime import datetime
from unittest.mock import AsyncMock

import numpy as np
import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins import StatusChangeStoryBuilder

start_date = datetime(2024, 4, 17)


@pytest.fixture
def status_change_story_builder(
    mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, get_metric_resp
):
    mock_query_service.get_metric = AsyncMock(return_value=get_metric_resp)
    return StatusChangeStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.mark.asyncio
async def test_generate_status_change_stories_no_min_data_points(status_change_story_builder, targets_df):
    short_data = targets_df.iloc[:1]
    # Prepare
    status_change_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=short_data)

    # Act
    result = await status_change_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_status_change_stories_on_track_to_off_track(status_change_story_builder):
    status_change_data = [
        {"value": 100, "target": 50, "date": pd.to_datetime(start_date - pd.Timedelta(days=1))},
        {"value": 40, "target": 50, "date": pd.to_datetime(start_date)},
    ]
    status_change_df = pd.DataFrame(status_change_data)
    status_change_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=status_change_df)
    status_change_story_builder.analysis_manager.calculate_percentage_difference.return_value = 20.0

    result = await status_change_story_builder.generate_stories("metric_1", Granularity.DAY)

    story = result[0]
    assert story["story_type"] == StoryType.WORSENING_STATUS


@pytest.mark.asyncio
async def test_generate_status_change_stories_no_status_change(status_change_story_builder, targets_df):
    no_change_data = [
        {"value": 40, "target": 50, "date": pd.to_datetime(start_date - pd.Timedelta(days=1))},
        {"value": 40, "target": 50, "date": pd.to_datetime(start_date)},
    ]
    no_change_df = pd.DataFrame(no_change_data)
    status_change_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=no_change_df)

    result = await status_change_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_status_change_stories_insufficient_data(status_change_story_builder):
    insufficient_data = [
        {"value": 40, "target": 50, "date": pd.to_datetime(start_date)},
    ]
    insufficient_df = pd.DataFrame(insufficient_data)
    status_change_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=insufficient_df)

    result = await status_change_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_status_change_stories_none_target(status_change_story_builder):
    none_target_data = [
        {"value": 40, "target": 50, "date": pd.to_datetime(start_date - pd.Timedelta(days=1))},
        {"value": 40, "target": None, "date": pd.to_datetime(start_date)},
    ]
    test_df = pd.DataFrame(none_target_data)
    status_change_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=test_df)

    result = await status_change_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_status_change_stories_nan_target(status_change_story_builder):
    nan_target_data = [
        {"value": 40, "target": 50, "date": pd.to_datetime(start_date - pd.Timedelta(days=1))},
        {"value": 40, "target": np.NaN, "date": pd.to_datetime(start_date)},
    ]
    test_df = pd.DataFrame(nan_target_data)
    status_change_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=test_df)

    result = await status_change_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0


@pytest.mark.parametrize(
    "statuses, target_status, expected_duration",
    [
        ([StoryType.ON_TRACK, StoryType.OFF_TRACK, StoryType.ON_TRACK, StoryType.OFF_TRACK], StoryType.ON_TRACK, 1),
        (
            [
                StoryType.ON_TRACK,
                StoryType.OFF_TRACK,
                StoryType.OFF_TRACK,
                StoryType.ON_TRACK,
            ],
            StoryType.OFF_TRACK,
            2,
        ),
        ([StoryType.ON_TRACK, StoryType.ON_TRACK, StoryType.ON_TRACK, StoryType.ON_TRACK], StoryType.OFF_TRACK, 0),
        ([], StoryType.ON_TRACK, 0),
        ([StoryType.ON_TRACK, StoryType.ON_TRACK, StoryType.OFF_TRACK, StoryType.ON_TRACK], StoryType.OFF_TRACK, 1),
        ([StoryType.ON_TRACK, StoryType.ON_TRACK, StoryType.ON_TRACK, StoryType.OFF_TRACK], StoryType.ON_TRACK, 3),
    ],
)
def test_get_previous_duration(statuses, target_status, expected_duration):
    df = pd.DataFrame({"status": statuses})
    duration = StatusChangeStoryBuilder.get_previous_status_duration(df, target_status)
    assert duration == expected_duration
