from datetime import date
from unittest.mock import ANY

import pandas as pd
import pytest

from fulcrum_core import AnalysisManager


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
    growth_rates = analysis_manager.calculate_growth_rates_of_series(series_df)

    # Assert
    assert growth_rates.tolist() == [ANY, 100.0, 50.0, 33.33333333333333, 25.0]

    # Act
    growth_rates2 = analysis_manager.calculate_growth_rates_of_series(series_df2)
    series_df2["growth_rate"] = growth_rates2

    # Assert
    assert series_df2["growth_rate"].tolist() == [ANY, 100.0, 50.0, 33.33333333333333, 25.0]


def test_calculate_deviation():
    analysis_manager = AnalysisManager()

    # Value greater than limit (positive deviation)
    value = 15.5
    limit = 10.0
    expected_deviation = 55.0  # ((15.5 - 10.5) / 10.0) * 100 = 55.0
    assert analysis_manager.calculate_deviation(value, limit) == expected_deviation

    # Value less than limit (negative deviation)
    value = 5.0
    limit = 10.0
    expected_deviation = -50.0  # ((5 - 10) / 10) * 100 = -50.0
    assert analysis_manager.calculate_deviation(value, limit) == expected_deviation

    # Value equal to limit (zero deviation)
    value = 10.0
    limit = 10.0
    expected_deviation = 0.0  # ((10 - 10) / 10) * 100 = 0.0
    assert analysis_manager.calculate_deviation(value, limit) == expected_deviation

    # # Zero value
    value = 0.0
    limit = 10.0
    expected_deviation = -100.0  # ((0 - 10) / 10) * 100 = -100.0
    assert analysis_manager.calculate_deviation(value, limit) == expected_deviation
