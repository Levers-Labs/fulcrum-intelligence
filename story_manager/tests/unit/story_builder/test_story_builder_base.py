from datetime import date, datetime
from unittest.mock import ANY, patch

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup
from story_manager.story_builder import StoryBuilderBase


@pytest.fixture
def story_builder(mock_query_service, mock_analysis_service, mock_db_session):
    class ConcreteStoryBuilder(StoryBuilderBase):
        genre = StoryGenre.GROWTH
        group = StoryGroup.GROWTH_RATES
        supported_grains = [Granularity.DAY]

        async def generate_stories(self, metric_id: str, grain: Granularity) -> list:
            return []

    return ConcreteStoryBuilder(mock_query_service, mock_analysis_service, mock_db_session)


@pytest.mark.asyncio
async def test_story_builder_run_unsupported_grain(story_builder):
    with pytest.raises(ValueError) as excinfo:
        await story_builder.run("metric1", Granularity.WEEK)
    assert str(excinfo.value) == "Unsupported grain 'week' for story genre 'GROWTH' of story group 'GROWTH_RATES'"


@pytest.mark.asyncio
async def test_story_builder_run_success(story_builder, mock_db_session):
    with patch.object(story_builder, "generate_stories", return_value=[{"id": 1}, {"id": 2}]):
        await story_builder.run("metric1", Granularity.DAY)
        mock_db_session.add_all.assert_called_once_with([{"id": 1}, {"id": 2}])
        mock_db_session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_story_builder_get_time_series_data(story_builder, mock_query_service):
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 3)
    time_series_df = await story_builder._get_time_series_data("metric1", Granularity.DAY, start_date, end_date)
    mock_query_service.get_metric_time_series.assert_called_once_with(
        "metric1", start_date=start_date, end_date=end_date, grain=Granularity.DAY
    )
    assert isinstance(time_series_df, pd.DataFrame)
    assert time_series_df.index.name == "date"
    assert list(time_series_df.columns) == ["value"]


@pytest.mark.asyncio
async def test_story_builder_persist_stories(story_builder, mock_db_session):
    stories = [{"id": 1}, {"id": 2}]
    await story_builder.persist_stories(stories)
    mock_db_session.add_all.assert_called_once_with(stories)
    mock_db_session.commit.assert_called_once()


def test_get_current_period_range_day(story_builder):
    grain = Granularity.DAY
    curr_date = date(2023, 4, 17)
    start_date, end_date = story_builder._get_current_period_range(grain, curr_date)
    assert start_date == date(2023, 4, 16)
    assert end_date == date(2023, 4, 16)


def test_get_current_period_range_week(story_builder):
    # prepare
    grain = Granularity.WEEK
    curr_date = date(2024, 4, 17)
    expected_start_date = date(2024, 4, 8)
    expected_end_date = date(2024, 4, 14)

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain, curr_date)

    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date


def test_get_current_period_range_month(story_builder):
    # prepare
    grain = Granularity.MONTH
    curr_date = date(2024, 4, 17)
    expected_start_date = date(2024, 3, 1)
    expected_end_date = date(2024, 3, 31)

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain, curr_date)

    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date


def test_get_current_period_range_quarter(story_builder):
    # prepare
    grain = Granularity.QUARTER
    curr_date = date(2024, 4, 17)
    expected_start_date = date(2024, 1, 1)
    expected_end_date = date(2024, 3, 31)

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain, curr_date)

    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date

    # Prepare
    curr_date = date(2024, 1, 17)
    expected_start_date = date(2023, 10, 1)
    expected_end_date = date(2023, 12, 31)

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain, curr_date)

    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date


def test_get_current_period_range_year(story_builder):
    # prepare
    grain = Granularity.YEAR
    curr_date = date(2024, 4, 17)
    expected_start_date = date(2023, 1, 1)
    expected_end_date = date(2023, 12, 31)

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain, curr_date)

    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date


def test_get_current_period_range_invalid_grain(story_builder):
    # prepare
    grain = "invalid"
    curr_date = date(2024, 4, 17)

    # Act & Assert
    with pytest.raises(ValueError):
        story_builder._get_current_period_range(grain, curr_date)  # type: ignore


def test_get_current_period_range_from_today(story_builder):
    # prepare
    grain = Granularity.DAY
    today = date.today()
    expected_start_date = (today - pd.DateOffset(days=1)).date()
    expected_end_date = expected_start_date

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain)
    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date


def test_calculate_growth_rates_of_series(story_builder):
    # prepare
    series_df = pd.DataFrame(
        {
            "value": [10, 20, 30, 40, 50],
        },
        index=pd.date_range(start="2023-01-01", periods=5, freq="D"),
    )
    series_df2 = series_df.copy()

    # Act
    series_df = story_builder._calculate_growth_rates_of_series(series_df)

    # Assert
    assert series_df["growth_rate"].tolist() == [100.0, 50.0, 33.33333333333333, 25.0]

    # Act
    series_df2 = story_builder._calculate_growth_rates_of_series(series_df2, remove_first_nan_row=False)

    # Assert
    assert series_df2["growth_rate"].tolist() == [ANY, 100.0, 50.0, 33.33333333333333, 25.0]
