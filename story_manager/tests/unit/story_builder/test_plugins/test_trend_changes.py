from datetime import date
from unittest.mock import AsyncMock

import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins import TrendChangesStoryBuilder

start_date = date(2023, 4, 7)
number_of_data_points = 90


@pytest.fixture
def trends_story_builder(
    mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, metric_values
):
    mock_query_service.get_metric = AsyncMock(return_value={"id": "metric_1", "label": "Metric 1"})
    mock_query_service.get_metric_time_series = AsyncMock(return_value=metric_values)
    return TrendChangesStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.mark.asyncio
async def test_generate_stories_stable_trend(mocker, trends_story_builder, process_control_df):
    # Prepare
    process_control_df = process_control_df.copy()
    process_control_df["trend_signal_detected"] = False
    process_control_df["slope"] = 1.2
    mocker.patch.object(trends_story_builder.analysis_manager, "process_control", return_value=process_control_df)

    # Act
    result = await trends_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 1
    story = result[0]
    assert story["story_type"] == StoryType.STABLE_TREND


@pytest.mark.asyncio
async def test_generate_stories_upward_trend(mocker, trends_story_builder, process_control_df):
    # Prepare
    process_control_df = process_control_df.copy()
    # set 10th from last as signal detected
    process_control_df.loc[process_control_df.index[-30], "trend_signal_detected"] = True
    process_control_df.loc[process_control_df.index[-10], "trend_signal_detected"] = True
    # set slope for them to be greater than those before
    process_control_df["slope"] = 1.8
    process_control_df.loc[process_control_df.index[-30:-10], "slope"] = 1.2
    process_control_df.loc[process_control_df.index[-10:], "slope"] = 1.5

    mocker.patch.object(trends_story_builder.analysis_manager, "process_control", return_value=process_control_df)

    # Act
    result = await trends_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 1
    story = result[0]
    assert story["story_type"] == StoryType.NEW_UPWARD_TREND
    assert story["variables"]["trend_start_date"] == process_control_df["date"].iloc[-10]
    assert story["variables"]["previous_trend_duration"] == 20


@pytest.mark.asyncio
async def test_generate_stories_downward_trend(mocker, trends_story_builder, process_control_df):
    # Prepare
    process_control_df = process_control_df.copy()
    # set 10th from last as signal detected
    process_control_df.loc[process_control_df.index[-30], "trend_signal_detected"] = True
    process_control_df.loc[process_control_df.index[-10], "trend_signal_detected"] = True
    # set slope for them to be less than those before
    process_control_df["slope"] = 1.2
    process_control_df.loc[process_control_df.index[-30:-10], "slope"] = 1.8
    process_control_df.loc[process_control_df.index[-10:], "slope"] = 1.5

    mocker.patch.object(trends_story_builder.analysis_manager, "process_control", return_value=process_control_df)

    # Act
    result = await trends_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 1
    story = result[0]
    assert story["story_type"] == StoryType.NEW_DOWNWARD_TREND
    assert story["variables"]["trend_start_date"] == process_control_df["date"].iloc[-10]
    assert story["variables"]["previous_trend_duration"] == 20


@pytest.mark.asyncio
async def test_generate_stories_performance_plateau(mocker, trends_story_builder, process_control_df):
    # Prepare
    process_control_df = process_control_df.copy()
    # set 10th from last as signal detected
    process_control_df.loc[process_control_df.index[-30], "trend_signal_detected"] = True
    process_control_df.loc[process_control_df.index[-10], "trend_signal_detected"] = True
    # set slope for them to be less than those before
    process_control_df["slope"] = 1.2
    process_control_df.loc[process_control_df.index[-30:-10], "slope"] = 1.8
    process_control_df.loc[process_control_df.index[-10:], "slope"] = 0.9

    mocker.patch.object(trends_story_builder.analysis_manager, "process_control", return_value=process_control_df)

    # Act
    result = await trends_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 2
    story_types = [story["story_type"] for story in result]
    assert StoryType.PERFORMANCE_PLATEAU in story_types


@pytest.mark.asyncio
async def test_generate_stories_no_min_data_points(mocker, trends_story_builder, metric_values):
    # Prepare
    trends_story_builder.query_service.get_metric_time_series = AsyncMock(return_value=metric_values[:5])

    # Act
    result = await trends_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0
