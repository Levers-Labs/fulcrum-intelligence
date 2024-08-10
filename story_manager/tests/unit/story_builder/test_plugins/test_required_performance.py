from datetime import date
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins import RequiredPerformanceStoryBuilder

start_date = date.today()
number_of_data_points = 90


@pytest.fixture
def required_performance_story_builder(
    mock_query_service,
    mock_analysis_service,
    mock_analysis_manager,
    mock_db_session,
    metric_values,
    mock_story_date,
    get_metric_resp,
):
    mock_query_service.get_metric = AsyncMock(return_value=get_metric_resp)
    return RequiredPerformanceStoryBuilder(
        mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, mock_story_date
    )


@pytest.mark.asyncio
async def test_generate_required_performance_stories_no_data_points(required_performance_story_builder, targets_df):
    # Prepare
    required_performance_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=pd.DataFrame())

    # Act
    result = await required_performance_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_req_performance_hold_steady(required_performance_story_builder, targets_df):
    steady_track_data = {
        "value": 100,
        "target": 50,
        "date": pd.to_datetime(start_date),
    }
    targets_df.loc[len(targets_df)] = steady_track_data
    required_performance_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    interval, period_end_date = required_performance_story_builder._get_end_date_of_period(Granularity.DAY)

    interval_target_df = pd.DataFrame(
        {
            "date": [pd.to_datetime(period_end_date)],
            "target": [50],
        }
    )
    required_performance_story_builder._get_time_series_for_targets = AsyncMock(return_value=interval_target_df)

    result = await required_performance_story_builder.generate_stories("metric_1", Granularity.DAY)

    story = result[0]
    assert story["story_type"] == StoryType.HOLD_STEADY


@pytest.mark.asyncio
async def test_generate_req_performance_story_req_performance(required_performance_story_builder, targets_df):
    data = {
        "value": 40,
        "target": 0,
        "date": pd.to_datetime(start_date),
    }
    targets_df.loc[len(targets_df)] = data
    required_performance_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    interval, period_end_date = required_performance_story_builder._get_end_date_of_period(Granularity.DAY)

    interval_target_df = pd.DataFrame(
        {
            "date": [pd.to_datetime(period_end_date)],
            "target": [100],
        }
    )
    required_performance_story_builder._get_time_series_for_targets = AsyncMock(return_value=interval_target_df)

    result = await required_performance_story_builder.generate_stories("metric_1", Granularity.DAY)

    story = result[0]
    assert story["story_type"] == StoryType.REQUIRED_PERFORMANCE
    assert story["variables"]["is_min_data"] is False


@pytest.mark.asyncio
async def test_generate_req_performance_story_for_min_data(required_performance_story_builder, targets_df):
    targets_df = targets_df[:5]

    required_performance_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=targets_df)

    interval, period_end_date = required_performance_story_builder._get_end_date_of_period(Granularity.DAY)

    interval_target_df = pd.DataFrame(
        {
            "date": [pd.to_datetime(period_end_date)],
            "target": [999],
        }
    )
    required_performance_story_builder._get_time_series_for_targets = AsyncMock(return_value=interval_target_df)

    result = await required_performance_story_builder.generate_stories("metric_1", Granularity.DAY)

    story = result[0]
    assert story["story_type"] == StoryType.REQUIRED_PERFORMANCE
    assert story["variables"]["is_min_data"] is True


# Test for day grain with last day of month
def test_day_grain_last_day_of_month(required_performance_story_builder):
    today = date(2024, 2, 2)  # Leap year
    required_performance_story_builder.story_date = today
    interval, end_date = required_performance_story_builder._get_end_date_of_period(Granularity.DAY)
    assert interval == Granularity.MONTH
    assert end_date == date(2024, 2, 29)


# Test for week grain with last day of month
def test_week_grain_last_day_of_month(required_performance_story_builder):
    today = date(2024, 2, 27)  # Leap year
    required_performance_story_builder.story_date = today
    interval, end_date = required_performance_story_builder._get_end_date_of_period(Granularity.WEEK)
    assert interval == Granularity.MONTH
    assert end_date == date(2024, 2, 29)


# Test for month grain with last day of quarter
def test_month_grain_last_day_of_quarter(required_performance_story_builder):
    today = date(2024, 3, 3)  # End of a quarter
    required_performance_story_builder.story_date = today
    interval, end_date = required_performance_story_builder._get_end_date_of_period(Granularity.MONTH)
    assert interval == Granularity.QUARTER
    assert end_date == date(2024, 3, 31)
