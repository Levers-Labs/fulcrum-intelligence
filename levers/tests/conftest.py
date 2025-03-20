"""
Common fixtures for all tests in the levers package.
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from levers.models.patterns import MetricGVAStatus


@pytest.fixture
def sample_numeric_values():
    """Fixture providing sample numeric values for testing."""
    return {
        "zero": 0,
        "positive_small": 0.5,
        "positive_integer": 10,
        "positive_large": 1000.5,
        "negative_small": -0.5,
        "negative_integer": -10,
        "negative_large": -1000.5,
        "non_numeric_str": "abc",
        "non_numeric_list": [1, 2, 3],
        "non_numeric_dict": {"a": 1},
    }


@pytest.fixture
def sample_time_series_data():
    """Fixture providing sample time series data for testing."""
    # Create a date range for the past 30 days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    dates = [start_date + timedelta(days=i) for i in range(31)]

    # Create sample data with actual and target values
    data = {
        "date": dates,
        "actual": [100 + i * 5 + np.random.randint(-10, 10) for i in range(31)],
        "target": [120 + i * 4 for i in range(31)],
    }

    return pd.DataFrame(data)


@pytest.fixture
def sample_status_data():
    """Fixture providing sample status data for testing."""
    # Create a date range
    dates = [datetime(2023, 1, 1) + timedelta(days=i) for i in range(30)]

    # Create sample statuses with some changes
    statuses = (
        [MetricGVAStatus.ON_TRACK] * 5
        + [MetricGVAStatus.OFF_TRACK] * 10
        + [MetricGVAStatus.NO_TARGET] * 10
        + [MetricGVAStatus.ON_TRACK] * 5
    )

    return pd.DataFrame({"date": dates, "status": statuses})


@pytest.fixture
def sample_growth_rates():
    """Fixture providing sample growth rates for testing."""
    return {
        "stable": [0.05, 0.051, 0.049, 0.05, 0.048],
        "accelerating": [0.02, 0.03, 0.05, 0.08, 0.12],
        "decelerating": [0.12, 0.08, 0.05, 0.03, 0.02],
        "volatile": [0.02, 0.08, 0.01, 0.09, 0.03],
    }
