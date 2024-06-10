import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline

from fulcrum_core.modules import ModelAnalyzer

periods = 50


@pytest.fixture
def sample_data():
    data = {
        "metric_id": ["metric1"] * periods,
        "date": pd.date_range(start="2022-01-01", periods=periods, freq="D"),
        "value": np.random.randint(50, 100, periods),
    }
    return pd.DataFrame(data)


@pytest.fixture
def input_dfs():
    data1 = {
        "metric_id": ["metric2"] * periods,
        "date": pd.date_range(start="2022-01-01", periods=periods, freq="D"),
        "value": np.random.randint(10, 20, periods),
    }
    data2 = {
        "metric_id": ["metric3"] * periods,
        "date": pd.date_range(start="2022-01-01", periods=periods, freq="D"),
        "value": np.random.randint(30, 50, periods),
    }
    return [pd.DataFrame(data1), pd.DataFrame(data2)]


def test_merge_dataframes(sample_data, input_dfs):
    # Prepare
    analyzer = ModelAnalyzer(target_metric_id="metric1")

    # Act
    merged_df = analyzer.merge_dataframes(sample_data, input_dfs)

    # Assert
    assert not merged_df.empty
    assert "metric1" in merged_df.columns
    assert "metric2" in merged_df.columns
    assert "metric3" in merged_df.columns
    assert len(merged_df.columns) == 3


def test_validate_input(sample_data):
    # Prepare
    analyzer = ModelAnalyzer(target_metric_id="metric1")

    # Act
    analyzer.validate_input(sample_data)

    # Assert
    with pytest.raises(ValueError):
        invalid_data = sample_data.drop(columns=["metric_id"])
        analyzer.validate_input(invalid_data)

    # Act & Assert
    with pytest.raises(ValueError):
        # duplicate metric_id
        duplicate_data = sample_data.copy()
        duplicate_data["metric_id"][1] = "metric2"
        analyzer.validate_input(duplicate_data)


def test_fit_linear_regression_equation(sample_data, input_dfs):
    # Prepare
    analyzer = ModelAnalyzer(target_metric_id="metric1")
    merged_df = analyzer.merge_dataframes(sample_data, input_dfs)

    # Act
    model, equation = analyzer.fit_linear_regression_equation(merged_df)

    # Assert
    assert model is not None
    assert "terms" in equation
    assert "constant" in equation


def test_fit_polynomial_regression_equation(sample_data, input_dfs):
    # Prepare
    analyzer = ModelAnalyzer(target_metric_id="metric1")
    merged_df = analyzer.merge_dataframes(sample_data, input_dfs)

    # Act
    model, equation = analyzer.fit_polynomial_regression_equation(merged_df)

    # Assert
    assert model is not None
    assert "terms" in equation
    assert "constant" in equation


def test_calculate_rmse(sample_data, input_dfs):
    # Prepare
    analyzer = ModelAnalyzer(target_metric_id="metric1")
    merged_df = analyzer.merge_dataframes(sample_data, input_dfs)
    model, _ = analyzer.fit_linear_regression_equation(merged_df)
    features = merged_df.drop(columns=["metric1"])
    target = merged_df["metric1"]

    # Act
    rmse = analyzer.calculate_rmse(model, features, target)

    # Assert
    assert rmse >= 0


def test_analyze(sample_data, input_dfs):
    # Prepare
    analyzer = ModelAnalyzer(target_metric_id="metric1")

    # Act
    result = analyzer.analyze(sample_data, input_dfs)

    # Assert
    assert "model" in result
    assert "equation" in result


def test_fit_model_linear(sample_data, input_dfs):
    # Prepare
    analyzer = ModelAnalyzer(target_metric_id="metric1")
    # Manipulate data to favor linear model (e.g., ensure linear relationship)
    sample_data["value"] = 2 * input_dfs[0]["value"] + 3 * input_dfs[1]["value"]

    # Act
    result = analyzer.fit_model(sample_data, input_dfs)

    # Assert
    assert "model" in result
    assert "equation" in result
    assert isinstance(result["model"], LinearRegression)


def test_fit_model_polynomial(sample_data, input_dfs):
    # Prepare
    analyzer = ModelAnalyzer(target_metric_id="metric1")
    # Manipulate data to favor polynomial model (e.g., introduce non-linearity)
    sample_data["value"] = input_dfs[0]["value"] ** 2 * input_dfs[1]["value"] ** 2

    # Act
    result = analyzer.fit_model(sample_data, input_dfs)

    # Assert
    assert "model" in result
    assert "equation" in result
    assert isinstance(result["model"], Pipeline)
