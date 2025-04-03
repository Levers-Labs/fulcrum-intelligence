"""
Common fixtures for story evaluator tests.
"""

import pytest
from pydantic import BaseModel

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType


class MockPattern(BaseModel):
    """Mock pattern for testing."""

    pattern: str = "test_pattern"
    pattern_run_id: str = "test_run_id"
    analysis_date: str = "2024-01-01"
    metric_id: str = "test_metric"


@pytest.fixture
def mock_pattern():
    """Fixture for mock pattern."""
    return MockPattern()


@pytest.fixture
def mock_metric():
    """Fixture for mock metric."""
    return {
        "label": "Test Metric",
        "value": 100.0,
        "target": 90.0,
        "trend": "up",
        "change_percent": 5.0,
        "performance_percent": 10.0,
        "gap_percent": -10.0,
        "streak_length": 3,
        "grain_label": "month",
        "performance_trend": "improving",
        "gap_trend": "widening",
        "old_status_duration": 2,
        "time_to_maintain": 1,
    }


@pytest.fixture
def mock_story_context():
    """Fixture for mock story context."""
    return {
        "metric": {"label": "Test Metric"},
        "current_value": 100.0,
        "trend_direction": "up",
        "change_percent": 5.0,
        "pop": "m/m",
        "target_value": 90.0,
        "performance_percent": 10.0,
        "streak_length": 3,
        "grain_label": "month",
        "performance_trend": "improving",
        "gap_percent": -10.0,
        "gap_trend": "widening",
        "old_status_duration": 2,
        "time_to_maintain": 1,
    }


@pytest.fixture
def mock_story():
    """Fixture for mock story."""
    return {
        "version": 2,
        "genre": StoryGenre.PERFORMANCE,
        "story_type": StoryType.ON_TRACK,
        "story_group": StoryGroup.GOAL_VS_ACTUAL,
        "grain": Granularity.DAY,
        "metric_id": "test_metric",
        "title": "Test Title",
        "detail": "Test Detail",
        "story_date": "2024-01-01",
        "variables": {"test_var": "test_value"},
        "metadata": {"pattern": "test_pattern"},
        "pattern_run_id": "test_run_id",
    }
