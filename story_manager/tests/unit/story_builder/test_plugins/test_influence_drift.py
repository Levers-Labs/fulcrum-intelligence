from datetime import date
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins import InfluenceDriftStoryBuilder

start_date = date(2023, 4, 7)
number_of_data_points = 90


@pytest.fixture
def influence_drift_story_builder(
    mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, metric_values, get_metric_resp
):
    mock_query_service.get_metric = AsyncMock(return_value=get_metric_resp)
    mock_query_service.get_metric_time_series_df = AsyncMock(return_value=pd.DataFrame(metric_values))
    mock_query_service.get_influencers = AsyncMock(
        return_value=[{"metric_id": "influence_1"}, {"metric_id": "influence_2"}]
    )
    return InfluenceDriftStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.mark.asyncio
async def test_generate_stories_stronger_influence(mocker, influence_drift_story_builder):
    # Execute the method
    result = await influence_drift_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assertions
    assert len(result) == 4  # Two stories for each influence (Stronger/Weaker and Improving/Worsening)
    assert result[0]["story_type"] == StoryType.STRONGER_INFLUENCE
    assert result[1]["story_type"] == StoryType.IMPROVING_INFLUENCE
    assert result[2]["story_type"] == StoryType.STRONGER_INFLUENCE
    assert result[3]["story_type"] == StoryType.IMPROVING_INFLUENCE


@pytest.mark.asyncio
async def test_generate_stories_weaker_influence(mocker, influence_drift_story_builder):
    # Execute the method
    result = await influence_drift_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assertions
    assert len(result) == 2  # One story for Weaker Influence and one for Worsening Influence
    assert result[0]["story_type"] == StoryType.WEAKER_INFLUENCE
    assert result[1]["story_type"] == StoryType.WORSENING_INFLUENCE


@pytest.mark.asyncio
async def test_generate_stories_no_min_data_points(mocker, influence_drift_story_builder, metric_values):
    # Prepare
    influence_drift_story_builder.query_service.get_metric_time_series_df = AsyncMock(
        return_value=pd.DataFrame(metric_values[:5])
    )

    # Act
    result = await influence_drift_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 0


def test_extract_influencer_metric_ids(influence_drift_story_builder):
    influencers = [
        {
            "metric_id": "metric1",
            "influencers": [
                {"metric_id": "metric2"},
                {"metric_id": "metric3", "influencers": [{"metric_id": "metric4"}]},
            ],
        },
        {"metric_id": "metric5"},
    ]
    result = influence_drift_story_builder._extract_influencer_metric_ids(influencers)
    assert result == {"metric1", "metric2", "metric3", "metric4", "metric5"}


@pytest.mark.asyncio
async def test_fetch_influence_time_series(mocker, influence_drift_story_builder):
    mock_df = pd.DataFrame({"date": ["2023-01-01", "2023-01-02"], "value": [1, 2]})
    influence_drift_story_builder.query_service.get_metric_time_series_df = AsyncMock(return_value=mock_df)

    influencer = {"metric_id": "metric1", "influencers": [{"metric_id": "metric2"}, {"metric_id": "metric3"}]}
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 2)
    grain = Granularity.DAY

    result = await influence_drift_story_builder.fetch_influence_time_series(influencer, start_date, end_date, grain)

    assert len(result) == 3  # One DataFrame for each metric
    for df in result:
        assert isinstance(df, pd.DataFrame)
        assert df.equals(mock_df)


def test_calculate_output_deviation(mocker, influence_drift_story_builder):
    df = pd.DataFrame({"date": ["2023-01-01", "2023-01-02", "2023-01-03"], "value": [100, 110, 120]})
    mocker.patch.object(
        influence_drift_story_builder.analysis_manager,
        "calculate_percentage_difference",
        side_effect=[0.0909, 0.1],  # Mocked percentage differences
    )

    output_deviation, prev_output_deviation = influence_drift_story_builder._calculate_output_deviation(df)

    assert output_deviation == 0.0909
    assert prev_output_deviation == 0.1


def test_get_strength_values():
    latest = {"model": {"relative_impact": 0.8}}
    previous = {"model": {"relative_impact": 0.6}}

    result = InfluenceDriftStoryBuilder._get_strength_values(latest, previous)

    assert result == (0.8, 0.6)


def test_calculate_influence_deviation(influence_drift_story_builder, mocker):
    input_df = pd.DataFrame({"value": [100, 110, 120]})
    adjusted_input_df = pd.DataFrame({"value": [100, 110]})

    mocker.patch.object(
        influence_drift_story_builder.analysis_manager, "calculate_percentage_difference", return_value=0.0909
    )

    result = influence_drift_story_builder._calculate_influence_deviation(input_df, adjusted_input_df)

    assert result == 0.0909
    influence_drift_story_builder.analysis_manager.calculate_percentage_difference.assert_called_once_with(120, 110)


@pytest.mark.asyncio
async def test_generate_stories_empty_influencers(influence_drift_story_builder):
    influence_drift_story_builder.query_service.get_influencers = AsyncMock(return_value=[])

    result = await influence_drift_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_stories_no_data_for_influence(mocker, influence_drift_story_builder):
    mock_influence_drift = {"components": [{"metric_id": "influence_1", "model": {"relative_impact": 0.8}}]}
    mocker.patch.object(
        influence_drift_story_builder.analysis_manager,
        "influence_drift",
        side_effect=[mock_influence_drift, mock_influence_drift],
    )

    # Mock an empty DataFrame for the influence metric
    empty_df = pd.DataFrame()
    mocker.patch.object(pd, "concat", return_value=empty_df)

    result = await influence_drift_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0
