from datetime import date
from unittest.mock import AsyncMock

import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins import TrendExceptionsStoryBuilder

start_date = date(2023, 4, 7)
number_of_data_points = 90


@pytest.fixture
def trends_story_builder(
    mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, metric_values
):
    mock_query_service.get_metric = AsyncMock(return_value={"id": "metric_1", "label": "Metric 1"})
    mock_query_service.get_metric_time_series = AsyncMock(return_value=metric_values)
    return TrendExceptionsStoryBuilder(
        mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session
    )


@pytest.mark.asyncio
async def test_generate_stories_spike(mocker, trends_story_builder, process_control_df):
    process_control_df = process_control_df.copy()
    spike_sample = {
        "value": 100,
        "central_line": 50,
        "ucl": 70,
        "lcl": 40,
        "slope": 1.5,
        "slope_change": 0,
        "trend_signal_detected": False,
    }
    process_control_df.loc[len(process_control_df)] = spike_sample

    mocker.patch.object(trends_story_builder.analysis_manager, "process_control", return_value=process_control_df)

    result = await trends_story_builder.generate_stories("metric_1", Granularity.DAY)
    story = result[0]
    assert story["story_type"] == StoryType.SPIKE


@pytest.mark.asyncio
async def test_generate_stories_drop(mocker, trends_story_builder, process_control_df):
    process_control_df = process_control_df.copy()
    drop_sample = {
        "value": 30,
        "central_line": 50,
        "ucl": 70,
        "lcl": 40,
        "slope": 1.5,
        "slope_change": 0,
        "trend_signal_detected": False,
    }
    process_control_df.loc[len(process_control_df)] = drop_sample

    mocker.patch.object(trends_story_builder.analysis_manager, "process_control", return_value=process_control_df)

    result = await trends_story_builder.generate_stories("metric_1", Granularity.DAY)
    story = result[0]
    assert story["story_type"] == StoryType.DROP


@pytest.mark.asyncio
async def test_generate_exception_stories_no_min_data_points(mocker, trends_story_builder, metric_values):
    # Prepare
    trends_story_builder.query_service.get_metric_time_series = AsyncMock(return_value=metric_values[:5])

    # Act
    result = await trends_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0
