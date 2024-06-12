import datetime
import random
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins.likely_status import LikelyStatusStoryBuilder


@pytest.fixture
def likely_status_story_builder(
    mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, metric_values, mock_story_date
):
    mock_query_service.get_metric = AsyncMock(return_value={"id": "metric_1", "label": "Metric 1"})
    return LikelyStatusStoryBuilder(
        mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, mock_story_date
    )


@pytest.mark.asyncio
async def test_likely_status_stories_no_min_data_points(likely_status_story_builder):
    # Prepare
    likely_status_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=pd.DataFrame())

    # Act
    result = await likely_status_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0


@pytest.mark.asyncio
async def test_likely_status_stories_likely_on_track_off_track(likely_status_story_builder):
    # Prepare
    df = pd.DataFrame(
        {
            "date": pd.date_range(start="2021-12-1", periods=40, freq="D"),
            "value": random.sample(range(100, 200), 40),
            "target": random.sample(range(50, 100), 40),
        }
    )
    interval = Granularity.WEEK
    story_start_date = datetime.date(2022, 1, 4)
    story_end_date = datetime.date(2022, 1, 11)
    target_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2022-01-10", periods=2, freq="D"),
            "target": [130, 140],
        }
    )
    forecasted_values = [
        {"date": datetime.date(2022, 1, 10), "value": 140},
        {"date": datetime.date(2022, 1, 11), "value": 150},
    ]

    likely_status_story_builder.get_story_period = MagicMock(return_value=(interval, story_start_date, story_end_date))
    likely_status_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=df)
    likely_status_story_builder._get_time_series_for_targets = AsyncMock(return_value=target_df)
    likely_status_story_builder.analysis_manager.simple_forecast = MagicMock(return_value=forecasted_values)

    # Act
    result = await likely_status_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    story = result[0]
    assert story["story_type"] == StoryType.LIKELY_ON_TRACK

    # Prepare
    forecasted_values = [
        {"date": datetime.date(2022, 1, 10), "value": 120},
        {"date": datetime.date(2022, 1, 11), "value": 130},
    ]
    likely_status_story_builder.analysis_manager.simple_forecast = MagicMock(return_value=forecasted_values)

    # Act
    result = await likely_status_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    story = result[0]
    assert story["story_type"] == StoryType.LIKELY_OFF_TRACK


@pytest.mark.asyncio
async def test_likely_status_stories_no_target_value(likely_status_story_builder):
    # Prepare
    df = pd.DataFrame(
        {
            "date": pd.date_range(start="2021-12-1", periods=40, freq="D"),
            "value": random.sample(range(100, 200), 40),
            "target": random.sample(range(50, 100), 40),
        }
    )
    interval = Granularity.WEEK
    story_start_date = datetime.date(2022, 1, 4)
    story_end_date = datetime.date(2022, 1, 11)
    target_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2022-01-10", periods=2, freq="D"),
            "target": [130, None],
        }
    )

    likely_status_story_builder._get_story_period = MagicMock(return_value=(interval, story_start_date, story_end_date))
    likely_status_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=df)
    likely_status_story_builder._get_time_series_for_targets = AsyncMock(return_value=target_df)

    # Act
    result = await likely_status_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0


@pytest.mark.asyncio
async def test_likely_status_stories_no_forecasted_value(likely_status_story_builder):
    # Prepare
    df = pd.DataFrame(
        {
            "date": pd.date_range(start="2021-12-1", periods=40, freq="D"),
            "value": random.sample(range(100, 200), 40),
            "target": random.sample(range(50, 100), 40),
        }
    )
    interval = Granularity.WEEK
    story_start_date = datetime.date(2022, 1, 4)
    story_end_date = datetime.date(2022, 1, 11)
    target_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2022-01-10", periods=2, freq="D"),
            "target": [130, 140],
        }
    )
    forecasted_values = [
        {"date": datetime.date(2022, 1, 10), "value": 140},
        {"date": datetime.date(2022, 1, 11), "value": 150},
    ]

    likely_status_story_builder.get_story_period = MagicMock(return_value=(interval, story_start_date, story_end_date))
    likely_status_story_builder._get_time_series_data_with_targets = AsyncMock(return_value=df)
    likely_status_story_builder._get_time_series_for_targets = AsyncMock(return_value=target_df)
    likely_status_story_builder.analysis_manager.simple_forecast = MagicMock(return_value=forecasted_values)

    # Act
    result = await likely_status_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    story = result[0]
    assert story["story_type"] == StoryType.LIKELY_ON_TRACK

    # Prepare
    forecasted_values = [{"date": datetime.date(2022, 1, 10), "value": 140}]
    likely_status_story_builder.analysis_manager.simple_forecast = MagicMock(return_value=forecasted_values)

    # Act
    result = await likely_status_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0


def test_get_forecasted_value_for_date():
    # Prepare
    forecasted_values = [
        {"date": datetime.date(2022, 1, 12), "value": 100},
        {"date": datetime.date(2022, 1, 13), "value": 200},
    ]
    ref_date = datetime.date(2022, 1, 12)

    # Act
    forecasted_value = LikelyStatusStoryBuilder.get_forecasted_value_for_date(forecasted_values, ref_date)

    # Assert
    assert forecasted_value == 100

    # Prepare
    ref_date = datetime.date(2022, 2, 13)

    # Act
    forecasted_value = LikelyStatusStoryBuilder.get_forecasted_value_for_date(forecasted_values, ref_date)

    # Assert
    assert forecasted_value is None


def test_get_story_period(likely_status_story_builder):
    # Test case for Granularity.DAY
    interval, start_date, end_date = likely_status_story_builder.get_story_period(grain=Granularity.DAY)
    assert interval == Granularity.WEEK
    assert start_date == datetime.date(2023, 4, 17)
    assert end_date == datetime.date(2023, 4, 23)

    # Test case for Granularity.WEEK
    interval, start_date, end_date = likely_status_story_builder.get_story_period(grain=Granularity.WEEK)
    assert interval == Granularity.MONTH
    assert start_date == datetime.date(2023, 4, 1)
    assert end_date == datetime.date(2023, 4, 24)

    # Test case for Granularity.MONTH
    interval, start_date, end_date = likely_status_story_builder.get_story_period(grain=Granularity.MONTH)
    assert interval == Granularity.QUARTER
    assert start_date == datetime.date(2023, 4, 1)
    assert end_date == datetime.date(2023, 6, 1)


def test_get_story_period_invalid_grain(likely_status_story_builder):
    with pytest.raises(ValueError):
        likely_status_story_builder.get_story_period(grain="invalid_grain")


def test_prepare_forecasted_story_df():
    # Prepare
    df = pd.DataFrame(
        {
            "date": pd.date_range(start="2022-01-1", periods=9, freq="D"),
            "value": [50, 60, 70, 80, 90, 100, 110, 120, 130],
            "target": [40, 50, 60, 70, 80, 90, 100, 110, 120],
        }
    )
    forecasted_values = [
        {"date": datetime.date(2022, 1, 10), "value": 140},
        {"date": datetime.date(2022, 1, 11), "value": 150},
    ]
    target_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2022-01-10", periods=2, freq="D"),
            "target": [130, 140],
        }
    )
    story_start_date = datetime.date(2022, 1, 5)

    # Act
    forecasted_story_df = LikelyStatusStoryBuilder.prepare_forecasted_story_df(
        forecasted_values, df, target_df, story_start_date
    )

    # Assert
    assert len(forecasted_story_df) == 7
    assert forecasted_story_df["value"].tolist() == [90, 100, 110, 120, 130, 140, 150]
    assert forecasted_story_df["target"].tolist() == [80, 90, 100, 110, 120, 130, 140]
