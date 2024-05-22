import os
from datetime import date
from unittest.mock import ANY, AsyncMock

import pandas as pd
import pytest

from fulcrum_core import AnalysisManager
from fulcrum_core.enums import MetricChangeDirection
from fulcrum_core.modules import SegmentDriftEvaluator


def test_cal_average_growth():
    """
    Test the cal_average_growth method of AnalysisManager
    """
    # Arrange
    analysis_manager = AnalysisManager()
    values = pd.Series([10, 12, 15, 12, 18])

    # Act
    response = analysis_manager.cal_average_growth(values)

    # Assert
    assert response == 19

    # Act
    response = analysis_manager.cal_average_growth(values, 2)

    # Assert
    assert response == 18.75

    # Inf growth rate
    values = pd.Series([10, 12, 15, 12, 0, 10])

    # Act
    response = analysis_manager.cal_average_growth(values)

    # Assert
    assert response == -19


def test_correlate(correlate_df):
    """
    Test the correlate method of AnalysisManager
    """
    # Arrange
    analysis_manager = AnalysisManager()
    start_date = date(2023, 1, 1)
    end_date = date(2025, 4, 30)

    # Act
    response = analysis_manager.correlate(correlate_df, start_date=start_date, end_date=end_date)

    # Assert
    assert len(response) == 1


def test_describe(describe_data, describe_output):
    analysis_manager = AnalysisManager()

    start_date = pd.to_datetime("2023-01-01")
    end_date = pd.to_datetime("2025-01-01")
    metric_id = "ToMRR"
    dimensions = ["region", "stage_name"]

    results = analysis_manager.describe(
        describe_data, dimensions, metric_id, start_date=start_date, end_date=end_date, aggregation_function="sum"
    )

    assert sorted(results, key=lambda x: (x["metric_id"], x["dimension"], x["member"])) == sorted(
        describe_output, key=lambda x: (x["metric_id"], x["dimension"], x["member"])
    )


def test_calculate_component_drift(metric_expression, component_drift_response):
    metric_values = [
        {"metric_id": "CAC", "evaluation_value": 700, "comparison_value": 300},
        {"metric_id": "SalesDevSpend", "evaluation_value": 5000.0, "comparison_value": 4500},
        {"metric_id": "SalesSpend", "evaluation_value": 2000.0, "comparison_value": 1500},
        {"metric_id": "NewCust", "evaluation_value": 25, "comparison_value": 12},
        {"metric_id": "OldCust", "evaluation_value": 5, "comparison_value": 2},
    ]

    manager = AnalysisManager()
    result = manager.calculate_component_drift(
        "CAC",
        metric_values,
        metric_expression,
        parent_drift={
            "absolute_drift": 400,
            "percentage_drift": 133.33,
            "relative_impact": 1,
            "marginal_contribution": 1,
            "relative_impact_root": 0.75,
            "marginal_contribution_root": 0.3,
        },
    )
    # Check the overall drift values
    assert result["drift"]["absolute_drift"] == pytest.approx(-195.2380952380952)
    assert result["drift"]["percentage_drift"] == pytest.approx(-0.4555555555555555)
    assert result["drift"]["relative_impact"] == pytest.approx(1)
    assert result["drift"]["marginal_contribution"] == pytest.approx(1)
    assert round(result["drift"]["relative_impact_root"], 2) == 0.75
    assert result["drift"]["marginal_contribution_root"] == pytest.approx(0.3)

    # Check the components' drift values, particularly for SalesDevSpend
    sales_dev_spend_drift = next(
        component for component in result["components"] if component["metric_id"] == "SalesDevSpend"
    )
    assert sales_dev_spend_drift["drift"]["absolute_drift"] == pytest.approx(500.0)
    assert sales_dev_spend_drift["drift"]["percentage_drift"] == pytest.approx(0.1111111111111111)
    assert sales_dev_spend_drift["drift"]["relative_impact"] == pytest.approx(-0.12677086711605504)
    assert sales_dev_spend_drift["drift"]["marginal_contribution"] == pytest.approx(0.009625195466218992)
    assert round(sales_dev_spend_drift["drift"]["relative_impact_root"], 2) == pytest.approx(-0.1)
    assert round(sales_dev_spend_drift["drift"]["marginal_contribution_root"], 4) == pytest.approx(0.0029)


def test_calculate_growth_rates_of_series():
    analysis_manager = AnalysisManager()

    series_df = pd.DataFrame(
        {
            "value": [10, 20, 30, 40, 50],
        },
        index=pd.date_range(start="2023-01-01", periods=5, freq="D"),
    )
    series_df2 = series_df.copy()

    # Act
    growth_rates = analysis_manager.calculate_growth_rates_of_series(series_df["value"])

    # Assert
    assert growth_rates.tolist() == [ANY, 100.0, 50.0, 33.0, 25.0]

    # Act
    growth_rates2 = analysis_manager.calculate_growth_rates_of_series(series_df2["value"], 2)
    series_df2["growth_rate"] = growth_rates2

    # Assert
    assert series_df2["growth_rate"].tolist() == [ANY, 100.0, 50.0, 33.33, 25.0]


def test_calculate_percentage_change():
    analysis_manager = AnalysisManager()

    # Value greater than limit (positive deviation)
    value = 15.5
    limit = 10.0
    expected_deviation = 55.0  # ((15.5 - 10.5) / 10.0) * 100 = 55.0
    assert analysis_manager.calculate_percentage_difference(value, limit) == expected_deviation

    # Value less than limit (negative deviation)
    value = 5.0
    limit = 10.0
    expected_deviation = -50.0  # ((5 - 10) / 10) * 100 = -50.0
    assert analysis_manager.calculate_percentage_difference(value, limit) == expected_deviation

    # Value equal to limit (zero deviation)
    value = 10.0
    limit = 10.0
    expected_deviation = 0.0  # ((10 - 10) / 10) * 100 = 0.0
    assert analysis_manager.calculate_percentage_difference(value, limit) == expected_deviation

    # # Zero value
    value = 0.0
    limit = 10.0
    expected_deviation = -100.0  # ((0 - 10) / 10) * 100 = -100.0
    assert analysis_manager.calculate_percentage_difference(value, limit) == expected_deviation


def test_calculate_slope_of_time_series():
    # Create an instance of AnalysisManager
    analysis_manager = AnalysisManager()

    series_df = pd.DataFrame(
        {
            "value": [10, 20, 30, 40, 50],
            "date": pd.date_range(start="2023-01-01", periods=5, freq="D"),
        },
    )

    # Expected output based on test_df
    expected_slope = 10.0

    # Call the function
    slope = analysis_manager.calculate_slope_of_time_series(series_df)

    # Assertion
    assert slope == expected_slope


def recursive_dict_compare(dict1, dict2):
    """
    Recursively compares the values in two dictionaries.
    Args:
        dict1 (dict): First dictionary to compare.
        dict2 (dict): Second dictionary to compare.
    Returns:
        bool: True if all values match recursively, False otherwise.
    """
    # If both inputs are not dictionaries, compare their values directly
    if not isinstance(dict1, dict) or not isinstance(dict2, dict):
        return dict1 == dict2

    # If the keys of both dictionaries don't match, return False
    if set(dict1.keys()) != set(dict2.keys()):
        return False

    # Recursively compare the values for each key
    for key in dict1:
        if not recursive_dict_compare(dict1[key], dict2[key]):
            return False

    return True


def recursively_round_up_values(dict1):
    for key1, value1 in dict1.items():
        if isinstance(value1, float):
            dict1[key1] = round(value1, 4)
        elif isinstance(value1, dict):
            recursively_round_up_values(value1)
    return dict1


@pytest.mark.asyncio
async def test_segment_drift(segment_drift_data, mocker, segment_drift_output, insight_response, dsensei_csv_file_id):
    analysis_manager = AnalysisManager()

    mock_file_id = AsyncMock(return_value=dsensei_csv_file_id)
    mocker.patch.object(SegmentDriftEvaluator, "send_file_to_dsensei", mock_file_id)

    mock_insight_response = AsyncMock(return_value=insight_response)
    mocker.patch.object(SegmentDriftEvaluator, "get_insights", mock_insight_response)

    dsensei_base_url = os.environ.get("DSENSEI_BASE_URL", "http://localhost:5001")
    df = pd.json_normalize(segment_drift_data["data"])

    response = await analysis_manager.segment_drift(
        dsensei_base_url,
        df,
        segment_drift_data["evaluation_start_date"],
        segment_drift_data["evaluation_end_date"],
        segment_drift_data["comparison_start_date"],
        segment_drift_data["comparison_end_date"],
        segment_drift_data["dimensions"],
        segment_drift_data["metric_column"],
        segment_drift_data["date_column"],
        segment_drift_data["aggregation_option"],
        segment_drift_data["aggregation_method"],
        segment_drift_data["target_metric_direction"],
    )
    assert recursive_dict_compare(
        sorted(recursively_round_up_values(response)), sorted(recursively_round_up_values(segment_drift_output))
    )


def test_get_metric_change_direction():
    segment_drift_evaluator = SegmentDriftEvaluator("")
    unchanged = segment_drift_evaluator.get_metric_change_direction(0, "increasing")
    assert unchanged == MetricChangeDirection.UNCHANGED


def test_get_overall_change():
    assert SegmentDriftEvaluator("").get_overall_change(10, 0) == 0.0


def test_calculate_segment_relative_change():
    assert SegmentDriftEvaluator("").calculate_segment_relative_change("", 0, {}) == 0
