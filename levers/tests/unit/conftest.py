"""
Fixtures specific to unit tests.
"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from levers.models import MetricGVAStatus


@pytest.fixture
def historical_gva_data():
    """Fixture providing sample data for historical GVA calculations."""
    # Create a date range
    dates = [datetime(2023, 1, 1) + timedelta(days=i) for i in range(10)]

    # Create actual values DataFrame
    actual_data = {
        "date": dates,
        "value": [100, 105, 110, 115, 120, 125, 130, 135, 140, 145],
        "metric_id": ["revenue"] * 10,
    }
    df_actual = pd.DataFrame(actual_data)

    # Create target values DataFrame
    target_data = {
        "date": dates,
        "value": [110, 115, 120, 125, 130, 135, 140, 145, 150, 155],
        "metric_id": ["revenue"] * 10,
    }
    df_target = pd.DataFrame(target_data)

    return {
        "df_actual": df_actual,
        "df_target": df_target,
    }


@pytest.fixture
def status_duration_data():
    """Fixture providing sample data for status duration tracking."""
    # Create a date range
    dates = [datetime(2023, 1, 1) + timedelta(days=i) for i in range(30)]

    # Create statuses with changes to test duration tracking
    statuses = (
        [MetricGVAStatus.ON_TRACK] * 5
        + [MetricGVAStatus.OFF_TRACK] * 10  # 5 days
        + [MetricGVAStatus.NO_TARGET] * 10  # 10 days
        + [MetricGVAStatus.ON_TRACK] * 5  # 10 days  # 5 days
    )

    # Create DataFrame
    data = {
        "date": dates,
        "status": statuses,
    }

    return pd.DataFrame(data)


@pytest.fixture
def threshold_proximity_data():
    """Fixture providing sample data for threshold proximity monitoring."""
    return [
        {"value": 105, "target": 100, "margin": 0.05, "expected": True},  # 5% above target
        {"value": 95, "target": 100, "margin": 0.05, "expected": True},  # 5% below target
        {"value": 110, "target": 100, "margin": 0.05, "expected": False},  # 10% above target
        {"value": 90, "target": 100, "margin": 0.05, "expected": False},  # 10% below target
        {"value": 100, "target": 100, "margin": 0.05, "expected": True},  # Equal to target
    ]


@pytest.fixture
def required_growth_data():
    """Fixture providing sample data for required growth calculations."""
    return [
        {"current": 100, "target": 200, "periods": 10, "expected": 7.18},  # ~7.18% growth needed
        {"current": 200, "target": 100, "periods": 10, "expected": -6.7},  # ~-6.7% growth needed (adjusted precision)
        {"current": 100, "target": 100, "periods": 10, "expected": 0.0},  # No growth needed
    ]
