from unittest.mock import AsyncMock

import pytest


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
    return AsyncMock()


@pytest.fixture
def mock_db_session():
    return AsyncMock()
