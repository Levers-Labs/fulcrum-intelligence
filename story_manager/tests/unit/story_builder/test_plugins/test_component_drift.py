from unittest.mock import AsyncMock

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.plugins import ComponentDriftStoryBuilder


@pytest.fixture
def mock_component_drift_story_builder(
    mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session
):
    mock_query_service.get_metric = AsyncMock(return_value={"id": "metric_1", "label": "Metric 1"})
    mock_analysis_service.get_component_drift = AsyncMock(
        return_value={
            "components": [
                {
                    "metric_id": "comp1",
                    "evaluation_value": 10,
                    "comparison_value": 5,
                    "drift": {"some_key": "some_value"},
                },
                {
                    "metric_id": "comp2",
                    "evaluation_value": 5,
                    "comparison_value": 8,
                    "drift": {"some_key": "some_value"},
                },
            ]
        }
    )
    return ComponentDriftStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.mark.asyncio
async def test_generate_stories(mock_component_drift_story_builder):
    # Prepare mock data
    mock_component_drift_story_builder.analysis_service.get_component_drift = AsyncMock(
        return_value={
            "components": [
                {
                    "metric_id": "comp1",
                    "evaluation_value": 10,
                    "comparison_value": 5,
                    "drift": {
                        "absolute_drift": -0.15692076470107352,
                        "percentage_drift": -0.14522821576763487,
                        "relative_impact": 0.3566784274548074,
                        "marginal_contribution": -0.12695310189267545,
                        "relative_impact_root": 0.3566784274548074,
                        "marginal_contribution_root": -0.12695310189267545,
                    },
                },
                {
                    "metric_id": "comp2",
                    "evaluation_value": 5,
                    "comparison_value": 8,
                    "drift": {
                        "absolute_drift": 0.125,
                        "percentage_drift": 0.25,
                        "relative_impact": -0.2,
                        "marginal_contribution": 0.05,
                        "relative_impact_root": -0.2,
                        "marginal_contribution_root": 0.05,
                    },
                },
            ]
        }
    )

    # Act
    result = await mock_component_drift_story_builder.generate_stories("metric_1", Granularity.DAY)

    # Assert
    assert len(result) == 2
    assert result[0]["story_type"] == StoryType.IMPROVING_COMPONENT.value
    assert result[1]["story_type"] == StoryType.WORSENING_COMPONENT.value


@pytest.mark.asyncio
async def test_generate_stories_no_min_component(mock_component_drift_story_builder):
    components = [
        {
            "metric_id": "comp1",
            "evaluation_value": 10,
            "comparison_value": 5,
            "drift": {
                "absolute_drift": -0.15692076470107352,
                "percentage_drift": -0.14522821576763487,
                "relative_impact": 0.3566784274548074,
                "marginal_contribution": -0.12695310189267545,
                "relative_impact_root": 0.3566784274548074,
                "marginal_contribution_root": -0.12695310189267545,
            },
        }
    ]
    mock_component_drift_story_builder.analysis_service.get_component_drift = AsyncMock(
        return_value={"components": components}
    )

    result = await mock_component_drift_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0


@pytest.mark.asyncio
async def test_generate_stories_empty_component_resp(mock_component_drift_story_builder):
    mock_component_drift_story_builder.analysis_service.get_component_drift = AsyncMock(return_value=None)

    result = await mock_component_drift_story_builder.generate_stories("metric_1", Granularity.DAY)

    assert len(result) == 0


def test_extract_components_data():
    # Prepare mock input
    components = [
        {"metric_id": "comp1", "evaluation_value": 10, "comparison_value": 5, "drift": {}},
        {"metric_id": "comp2", "evaluation_value": 5, "comparison_value": 8, "drift": {}},
    ]

    # Act
    df = ComponentDriftStoryBuilder.extract_components_data(components)

    # Assert
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "metric_id" in df.columns
    assert "evaluation_value" in df.columns
    assert "comparison_value" in df.columns
    assert "story_type" in df.columns
    assert "pressure" in df.columns


def test_create_ranked_df():
    # Prepare mock input DataFrame
    df = pd.DataFrame({"metric_id": ["comp1", "comp2"], "marginal_contribution_root": [50, -20]})

    # Act
    ranked_df = ComponentDriftStoryBuilder.create_ranked_df(df)

    # Assert
    assert isinstance(ranked_df, pd.DataFrame)
    assert len(ranked_df) == 2
    assert "metric_id" in ranked_df.columns
    assert "marginal_contribution_root" in ranked_df.columns


def test_get_top_components(mock_component_drift_story_builder):
    # Prepare mock DataFrame
    df = pd.DataFrame({"metric_id": ["comp1", "comp2"], "marginal_contribution_root": [50, -20]})

    # Call the instance method
    top_components = mock_component_drift_story_builder.get_top_components(df)

    # Assertions
    assert isinstance(top_components, pd.DataFrame)
    assert len(top_components) == 2
    assert "metric_id" in top_components.columns
    assert "marginal_contribution_root" in top_components.columns


def test_fetch_all_components():
    # Prepare mock input response
    response = {"components": [{"metric_id": "comp1"}, {"metric_id": "comp2"}]}

    # Act
    components_list = ComponentDriftStoryBuilder.fetch_all_components(response)

    # Assert
    assert isinstance(components_list, list)
    assert len(components_list) == 2
    assert all(isinstance(comp, dict) for comp in components_list)
    assert components_list[0]["metric_id"] == "comp1"
    assert components_list[1]["metric_id"] == "comp2"
