from datetime import date
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins import LongRangeStoryBuilder

start_date = date(2023, 4, 7)
number_of_data_points = 90
metric_id = "metric_1"
grain = Granularity.MONTH


@pytest.fixture
def long_range_story_builder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session):
    mock_query_service.get_metric = AsyncMock(return_value={"id": "metric_1", "label": "Metric 1"})
    return LongRangeStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.fixture
def sample_data():
    return pd.DataFrame({"date": pd.date_range(start="2020-01-01", periods=4, freq="M"), "value": [100, 150, 200, 250]})


@pytest.fixture
def sample_worsening_data():
    return pd.DataFrame({"date": pd.date_range(start="2020-01-01", periods=4, freq="M"), "value": [200, 150, 100, 50]})


@pytest.mark.asyncio
async def test_generate_stories_success(mocker, long_range_story_builder, sample_data):

    long_range_story_builder.query_service.get_metric = AsyncMock(
        return_value={"id": metric_id, "name": "Sample Metric"}
    )
    mocker.patch.object(
        long_range_story_builder, "_get_input_time_range", return_value=(date(2020, 1, 1), date(2020, 4, 1))
    )
    mocker.patch.object(long_range_story_builder, "_get_time_series_data", return_value=sample_data)
    mocker.patch.object(long_range_story_builder, "get_time_durations", return_value={"min": 3})
    mocker.patch.object(long_range_story_builder.analysis_manager, "cal_average_growth", return_value=50.0)
    mocker.patch.object(
        long_range_story_builder.analysis_manager, "calculate_percentage_difference", return_value=150.0
    )
    mocker.patch.object(long_range_story_builder.analysis_manager, "calculate_slope_of_time_series", return_value=1.0)
    mocker.patch.object(
        long_range_story_builder,
        "prepare_story_dict",
        return_value={
            "story_type": StoryType.IMPROVING_PERFORMANCE,
            "grain": grain,
            "metric": {"id": metric_id, "name": "Sample Metric"},
            "avg_growth": 50.0,
            "overall_growth": 150.0,
            "duration": 4,
        },
    )

    stories = await long_range_story_builder.generate_stories(metric_id, grain)
    assert len(stories) == 1
    assert stories[0]["story_type"] == StoryType.IMPROVING_PERFORMANCE


@pytest.mark.asyncio
async def test_generate_stories_insufficient_data(mocker, long_range_story_builder, sample_data):

    short_data = sample_data.iloc[:2]
    long_range_story_builder.query_service.get_metric = AsyncMock(
        return_value={"id": metric_id, "name": "Sample Metric"}
    )
    mocker.patch.object(
        long_range_story_builder, "_get_input_time_range", return_value=(date(2020, 1, 1), date(2020, 4, 1))
    )
    mocker.patch.object(long_range_story_builder, "_get_time_series_data", return_value=short_data)
    mocker.patch.object(long_range_story_builder, "get_time_durations", return_value={"min": 3})

    stories = await long_range_story_builder.generate_stories(metric_id, grain)
    assert len(stories) == 0


@pytest.mark.asyncio
async def test_generate_stories_improving_performance(mocker, long_range_story_builder, sample_data):

    long_range_story_builder.query_service.get_metric = AsyncMock(
        return_value={"id": metric_id, "name": "Sample Metric"}
    )
    mocker.patch.object(
        long_range_story_builder, "_get_input_time_range", return_value=(date(2020, 1, 1), date(2020, 4, 1))
    )
    mocker.patch.object(long_range_story_builder, "_get_time_series_data", return_value=sample_data)
    mocker.patch.object(long_range_story_builder, "get_time_durations", return_value={"min": 3})
    mocker.patch.object(
        long_range_story_builder,
        "prepare_story_dict",
        return_value={
            "story_type": StoryType.IMPROVING_PERFORMANCE,
            "grain": grain,
            "metric": {"id": metric_id, "name": "Sample Metric"},
            "avg_growth": 50.0,
            "overall_growth": 150.0,
            "duration": 4,
        },
    )

    stories = await long_range_story_builder.generate_stories(metric_id, grain)
    assert len(stories) == 1
    assert stories[0]["story_type"] == StoryType.IMPROVING_PERFORMANCE


@pytest.mark.asyncio
async def test_generate_stories_worsening_performance(mocker, long_range_story_builder, sample_worsening_data):

    long_range_story_builder.query_service.get_metric = AsyncMock(
        return_value={"id": metric_id, "name": "Sample Metric"}
    )
    mocker.patch.object(
        long_range_story_builder, "_get_input_time_range", return_value=(date(2020, 1, 1), date(2020, 4, 1))
    )
    mocker.patch.object(long_range_story_builder, "_get_time_series_data", return_value=sample_worsening_data)
    mocker.patch.object(long_range_story_builder, "get_time_durations", return_value={"min": 3})
    mocker.patch.object(
        long_range_story_builder,
        "prepare_story_dict",
        return_value={
            "story_type": StoryType.WORSENING_PERFORMANCE,
            "grain": grain,
            "metric": {"id": metric_id, "name": "Sample Metric"},
            "avg_growth": 50.0,
            "overall_growth": 150.0,
            "duration": 4,
        },
    )

    stories = await long_range_story_builder.generate_stories(metric_id, grain)
    assert len(stories) == 1
    assert stories[0]["story_type"] == StoryType.WORSENING_PERFORMANCE
