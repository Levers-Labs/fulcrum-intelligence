from datetime import datetime
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins.record_values import RecordValuesStoryBuilder


@pytest.fixture
def record_values_story_builder(
    mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, metric_values
):
    mock_query_service.get_metric = AsyncMock(return_value={"id": "metric_1", "label": "Metric 1"})
    mock_query_service.get_metric_time_series = AsyncMock(return_value=metric_values)
    return RecordValuesStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.mark.asyncio
async def test_generate_stories_record_high(mocker, record_values_story_builder):
    # Prepare
    df = pd.DataFrame({"value": range(0, 100), "date": pd.date_range(start="2023-01-01", periods=100)})
    mocker.patch.object(record_values_story_builder.analysis_manager, "calculate_percentage_difference", return_value=1)
    mocker.patch.object(record_values_story_builder, "_get_time_series_data", return_value=df)

    # Act
    result = await record_values_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 1
    story = result[0]
    assert story["story_type"] == StoryType.RECORD_HIGH


@pytest.mark.asyncio
async def test_generate_stories_record_low(mocker, record_values_story_builder):
    # Prepare
    df = pd.DataFrame({"value": range(100, 0, -1), "date": pd.date_range(start="2023-01-01", periods=100)})
    mocker.patch.object(
        record_values_story_builder.analysis_manager, "calculate_percentage_difference", return_value=-1
    )
    mocker.patch.object(record_values_story_builder, "_get_time_series_data", return_value=df)

    # Act
    result = await record_values_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 1
    story = result[0]
    assert story["story_type"] == StoryType.RECORD_LOW


@pytest.mark.asyncio
async def test_generate_stories_no_min_data_points(mocker, record_values_story_builder):
    # Prepare
    df = pd.DataFrame({"value": range(1, 6), "date": pd.date_range(start="2023-01-01", periods=5)})
    mocker.patch.object(record_values_story_builder, "_get_time_series_data", return_value=df)

    # Act
    result = await record_values_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0


def test_get_rank_for_date(record_values_story_builder, sorted_df):
    # Highest value
    # ref_value = "2024-01-29"
    ref_value = pd.Timestamp(datetime(2024, 1, 29))
    rank = record_values_story_builder.get_rank_for_date(sorted_df, ref_value)
    assert rank == 1

    # second highest
    ref_value = pd.Timestamp(datetime(2024, 1, 22))
    rank = record_values_story_builder.get_rank_for_date(sorted_df, ref_value)
    assert rank == 2

    # Lowest
    ref_value = pd.Timestamp(datetime(2024, 1, 1))
    rank = record_values_story_builder.get_rank_for_date(sorted_df, ref_value)
    assert rank == len(sorted_df)

    # Second lowest
    ref_value = pd.Timestamp(datetime(2024, 2, 19))
    rank = record_values_story_builder.get_rank_for_date(sorted_df, ref_value)
    assert rank == len(sorted_df) - 1

    # random
    ref_value = pd.Timestamp(datetime(2024, 2, 5))
    rank = record_values_story_builder.get_rank_for_date(sorted_df, ref_value)
    assert rank == len(sorted_df) - 2
