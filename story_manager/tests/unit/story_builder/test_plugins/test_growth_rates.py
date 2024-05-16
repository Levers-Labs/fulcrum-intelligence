from datetime import date

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins import GrowthStoryBuilder


@pytest.fixture
def growth_story_builder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session):
    return GrowthStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.fixture
def series_df():
    df = pd.DataFrame(
        {
            "value": [100, 200, 150, 175, 158],
        },
        index=pd.date_range(start="2023-01-01", periods=5, freq="D"),
    )
    return df


def test_get_sliding_start_date_week_to_month_quarter(growth_story_builder):
    # Prepare
    curr_start_date = date(2023, 4, 10)
    reference_period = Granularity.MONTH
    grain = Granularity.WEEK
    expected_start_date = date(2023, 3, 3)

    # Act
    result = growth_story_builder._get_sliding_start_date(curr_start_date, reference_period, grain)

    # Assert
    assert result == expected_start_date

    # Prepare
    reference_period = Granularity.QUARTER
    expected_start_date = date(2023, 1, 3)

    # Act
    result = growth_story_builder._get_sliding_start_date(curr_start_date, reference_period, grain)

    # Assert
    assert result == expected_start_date


def test_get_sliding_start_date_month_to_quarter(growth_story_builder):
    # Prepare
    curr_start_date = date(2023, 4, 1)
    reference_period = Granularity.QUARTER
    grain = Granularity.MONTH
    expected_start_date = date(2022, 12, 1)

    # Act
    result = growth_story_builder._get_sliding_start_date(curr_start_date, reference_period, grain)

    # Assert
    assert result == expected_start_date


def test_get_sliding_start_date_quarter_to_year(growth_story_builder):
    # Prepare
    curr_start_date = date(2023, 1, 1)
    reference_period = Granularity.YEAR
    grain = Granularity.QUARTER
    expected_start_date = date(2021, 10, 1)

    # Act
    result = growth_story_builder._get_sliding_start_date(curr_start_date, reference_period, grain)

    # Assert
    assert result == expected_start_date


def test_get_sliding_start_date_day_to_week_month(growth_story_builder):
    # Prepare
    curr_start_date = date(2023, 4, 10)
    reference_period = Granularity.WEEK
    grain = Granularity.DAY
    expected_start_date = date(2023, 4, 2)

    # Act
    result = growth_story_builder._get_sliding_start_date(curr_start_date, reference_period, grain)

    # Assert
    assert result == expected_start_date

    # Prepare
    reference_period = Granularity.MONTH
    expected_start_date = date(2023, 3, 9)

    # Act
    result = growth_story_builder._get_sliding_start_date(curr_start_date, reference_period, grain)

    # Assert
    assert result == expected_start_date


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "grain, series_map, expected_story_types",
    [
        (
            Granularity.DAY,
            {
                Granularity.WEEK: [],
                Granularity.MONTH: [10, 20, 30, 40, 10, 20],
                Granularity.QUARTER: [None, 100, 80, 70, 77, 99],
            },
            [],
        ),
        (
            Granularity.DAY,
            {
                Granularity.WEEK: [None, 100, 80, 70, 77, 99],
                Granularity.MONTH: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
                Granularity.QUARTER: [89, 76, 65, 54, 68, 77, 89, 100],
            },
            [StoryType.SLOWING_GROWTH, StoryType.ACCELERATING_GROWTH],
        ),
        (
            Granularity.WEEK,
            {
                Granularity.WEEK: [83, 90, 100],
                Granularity.MONTH: [189, 200, 214, 230, 250],
                Granularity.QUARTER: [678, 700, 720, 750, 800, 899, 677, 678],
            },
            [StoryType.ACCELERATING_GROWTH, StoryType.ACCELERATING_GROWTH, StoryType.SLOWING_GROWTH],
        ),
        (
            Granularity.MONTH,
            {
                Granularity.MONTH: [189, 200, 202],
                Granularity.QUARTER: [678, 700, 720, 750],
            },
            [StoryType.SLOWING_GROWTH, StoryType.ACCELERATING_GROWTH],
        ),
        (
            Granularity.QUARTER,
            {
                Granularity.QUARTER: [678, 700, 720],
            },
            [StoryType.SLOWING_GROWTH],
        ),
    ],
)
async def test_generate_stories(growth_story_builder, grain, series_map, expected_story_types):
    # Prepare
    series_df_response = [
        pd.DataFrame(
            {"value": grain_series},
            index=pd.date_range(
                start="2023-01-01",
                periods=len(grain_series),
                freq=f"{grain.name[0]}E" if grain != Granularity.WEEK else "W",
            ),
        )
        for grain, grain_series in series_map.items()
    ]

    growth_story_builder.query_service.get_metric_time_series.side_effect = series_df_response
    growth_story_builder.query_service.get_metric.return_value = {"id": "test_metric", "label": "Test Metric"}

    # Act
    stories = await growth_story_builder.generate_stories("test_metric", grain)

    # Assert
    assert len(stories) == len(expected_story_types)
    for story, expected_story_type in zip(stories, expected_story_types):
        assert story["type"] == expected_story_type
