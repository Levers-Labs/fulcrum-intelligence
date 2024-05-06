import json
import pathlib
from unittest.mock import MagicMock

import pandas as pd
import pytest

from story_manager.story_manager.config import Paths


@pytest.fixture
def mock_query_service():
    query_service = MagicMock()
    query_service.get_metric_values.return_value = [
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
    return MagicMock()


@pytest.fixture
def mock_db_session():
    return MagicMock()


@pytest.fixture
def mock_process_control_output(trend_type):
    trend_files = {
        "normal": "process_control_normal_trend.json",
        "upward": "process_control_upward_trend.json",
        "downward": "process_control_downward_trend.json",
        "sticky": "process_control_sticky_trend.json",
    }

    file_path = trend_files.get(trend_type)
    if file_path is None:
        raise ValueError(f"Invalid trend_type: {trend_type}")

    with open(pathlib.Path.joinpath(Paths.BASE_DIR, f"data/{file_path}")) as fr:
        process_control_output = json.loads(fr.read())
    df = pd.DataFrame(process_control_output)
    df["slope"] = 0.0
    df["has_discontinuity"] = False
    df["growth_rate"] = 0.0
    return df
