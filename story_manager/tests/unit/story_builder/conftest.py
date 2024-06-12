from datetime import date, datetime
from unittest.mock import AsyncMock

import pytest

from commons.models.enums import Granularity
from fulcrum_core import AnalysisManager
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType


@pytest.fixture
def mock_query_service():
    query_service = AsyncMock()
    query_service.get_metric_values.return_value = [
        {"date": "2023-01-01", "value": 100},
        {"date": "2023-01-02", "value": 200},
        {"date": "2023-01-03", "value": 300},
    ]
    query_service.get_metric_time_series.return_value = [
        {"date": "2023-01-01", "value": 100},
        {"date": "2023-01-02", "value": 200},
        {"date": "2023-01-03", "value": 300},
    ]
    query_service.list_metrics.return_value = [
        {"id": 1, "name": "metric1"},
        {"id": 2, "name": "metric2"},
        {"id": 3, "name": "metric3"},
    ]
    return query_service


@pytest.fixture
def mock_analysis_service():
    return AsyncMock()


@pytest.fixture
def mock_analysis_manager():
    return AnalysisManager()


@pytest.fixture
def mock_db_session():
    return AsyncMock()


@pytest.fixture
def mock_story_date():
    return date(2023, 4, 17)


@pytest.fixture
def mock_stories():
    stories = [
        {
            "metric_id": "metric_1",
            "genre": StoryGenre.GROWTH,
            "story_group": StoryGroup.GROWTH_RATES,
            "story_type": StoryType.ACCELERATING_GROWTH,
            "grain": Granularity.DAY,
            "series": [{"date": "2023-01-01", "value": 100}],
            "title": "Title 1",
            "detail": "Detail 1",
            "title_template": "Title Template 1",
            "detail_template": "Detail Template 1",
            "variables": {"key": "value"},
            "story_date": datetime(2023, 1, 1),
        },
        {
            "metric_id": "metric_2",
            "genre": StoryGenre.GROWTH,
            "story_group": StoryGroup.GROWTH_RATES,
            "story_type": StoryType.ACCELERATING_GROWTH,
            "grain": Granularity.DAY,
            "series": [{"date": "2023-01-02", "value": 200}],
            "title": "Title 2",
            "detail": "Detail 2",
            "title_template": "Title Template 2",
            "detail_template": "Detail Template 2",
            "variables": {"key": "value"},
            "story_date": datetime(2023, 1, 2),
        },
    ]
    return stories
