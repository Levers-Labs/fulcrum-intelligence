from unittest.mock import AsyncMock

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins import GrowthStoryBuilder


@pytest.fixture
def growth_story_builder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session):
    return GrowthStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.mark.asyncio
async def test_generate_stories_accelerating_growth(mocker, growth_story_builder):
    # Prepare
    values = [938.39, 1099.37, 1290.92, 1518.85, 1789.99, 2112.39, 2495.53, 2950.49, 3600, 4900]
    values_df = pd.DataFrame(
        {"date": pd.date_range(start="2023-04-01", periods=len(values), freq="D"), "value": values}
    )
    mocker.patch.object(
        growth_story_builder.query_service, "get_metric_time_series", return_value=values_df.to_dict(orient="records")
    )
    avg_growth = growth_story_builder.analysis_manager.calculate_growth_rates_of_series(values_df["value"]).mean()
    current_growth = growth_story_builder.analysis_manager.calculate_growth_rates_of_series(values_df["value"]).iloc[-1]
    # Act
    result = await growth_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 1
    assert result[0]["story_type"] == StoryType.ACCELERATING_GROWTH
    assert result[0]["variables"]["avg_growth"] == round(avg_growth)
    assert result[0]["variables"]["current_growth"] == round(current_growth)


@pytest.mark.asyncio
async def test_generate_stories_slowing_growth(mocker, growth_story_builder):
    # Prepare
    values = [938.39, 1099.37, 1290.92, 1518.85, 1789.99, 2112.39, 2495.53, 2600, 3200, 3500]
    values_df = pd.DataFrame(
        {"date": pd.date_range(start="2023-04-01", periods=len(values), freq="D"), "value": values}
    )
    mocker.patch.object(
        growth_story_builder.query_service, "get_metric_time_series", return_value=values_df.to_dict(orient="records")
    )
    avg_growth = growth_story_builder.analysis_manager.calculate_growth_rates_of_series(values_df["value"]).mean()
    current_growth = growth_story_builder.analysis_manager.calculate_growth_rates_of_series(values_df["value"]).iloc[-1]
    # Act
    result = await growth_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 1
    assert result[0]["story_type"] == StoryType.SLOWING_GROWTH
    assert result[0]["variables"]["avg_growth"] == round(avg_growth)
    assert result[0]["variables"]["current_growth"] == round(current_growth)


@pytest.mark.asyncio
async def test_generate_stories_stable_growth(mocker, growth_story_builder):
    # Prepare
    values = [938.39, 1099.37, 1290.92, 1518.85, 1789.99, 2112.39, 2495.53, 2600, 2700, 2800]
    values_df = pd.DataFrame(
        {"date": pd.date_range(start="2023-04-01", periods=len(values), freq="D"), "value": values}
    )
    mocker.patch.object(
        growth_story_builder.query_service, "get_metric_time_series", return_value=values_df.to_dict(orient="records")
    )

    # Act
    result = await growth_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_stories_no_min_data_points(mocker, growth_story_builder, metric_values):
    # Prepare
    growth_story_builder.query_service.get_metric_time_series = AsyncMock(return_value=metric_values[:5])

    # Act
    result = await growth_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0
