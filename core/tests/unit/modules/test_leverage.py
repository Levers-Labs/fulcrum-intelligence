import pytest
import pandas as pd
import sympy as sp

from fulcrum_core.modules.leverage import LeverageCalculator


@pytest.fixture(name="metric_json")
def metric_json_fixture():
    return {
        "metric_id": "NewBizDeals",
        "metric_expression": {
            "expression_str": "AcceptOpps + SQOToWinRate",
            "expression": {
                "type": "expression",
                "operator": "+",
                "operands": [
                    {"type": "metric", "metric_id": "AcceptOpps"},
                    {"type": "metric", "metric_id": "SQOToWinRate"}
                ]
            }
        }
    }


@pytest.fixture(name="values_df")
def values_df_fixture():
    return pd.DataFrame({
        "date": ["2023-01-01", "2023-01-02"],
        "AcceptOpps": [100, 150],
        "SQOToWinRate": [50, 75]
    })


@pytest.fixture(name="max_values")
def max_values_fixture():
    return {
        "AcceptOpps": 200,
        "SQOToWinRate": 100
    }


@pytest.fixture(name="calculator")
def calculator_fixture():
    return LeverageCalculator()


def test_init(calculator):
    assert calculator.metric_json is None
    assert calculator.values_df is None
    assert calculator.max_values is None


def test_extract_expression(calculator, metric_json):
    equation = calculator._extract_expression(metric_json)
    assert equation == ["NewBizDeals = AcceptOpps + SQOToWinRate"]


def test_create_parent_and_top_parent_dicts(calculator, metric_json):
    parent_dict, top_parent_dict = calculator._create_parent_and_top_parent_dicts(metric_json)
    assert parent_dict == {"AcceptOpps": "NewBizDeals", "SQOToWinRate": "NewBizDeals"}
    assert top_parent_dict == {"AcceptOpps": "NewBizDeals", "SQOToWinRate": "NewBizDeals"}


def test_compute_y(calculator, metric_json, values_df):
    calculator.metric_json = metric_json
    calculator.values_df = values_df
    calculator.var_symbols = {var: sp.symbols(var) for var in values_df.columns if var != "date"}
    y = calculator._compute_y("AcceptOpps + SQOToWinRate", {"AcceptOpps": 100, "SQOToWinRate": 50})
    assert y == 150


def test_compute_ymax(calculator, metric_json, values_df, max_values):
    calculator.metric_json = metric_json
    calculator.values_df = values_df
    calculator.var_symbols = {var: sp.symbols(var) for var in values_df.columns if var != "date"}
    ymax = calculator._compute_ymax("AcceptOpps + SQOToWinRate", {"AcceptOpps": 100, "SQOToWinRate": 50}, "AcceptOpps", max_values["AcceptOpps"])
    assert ymax == 250


def test_analyze_parent(calculator, metric_json, values_df, max_values):
    calculator.run(metric_json, values_df, max_values)
    parent_results = calculator.analyze_parent()
    assert len(parent_results) == 4


def test_analyze_top_parent(calculator, metric_json, values_df, max_values):
    calculator.run(metric_json, values_df, max_values)
    top_parent_results = calculator.analyze_top_parent()
    assert len(top_parent_results) == 4


def test_combine_results(calculator, metric_json, values_df, max_values):
    calculator.run(metric_json, values_df, max_values)
    parent_results = calculator.analyze_parent()
    top_parent_results = calculator.analyze_top_parent()
    final_results = calculator.combine_results(parent_results, top_parent_results)
    assert len(final_results) == 4


def test_get_metric_details(calculator, metric_json, values_df, max_values):
    calculator.run(metric_json, values_df, max_values)
    parent_results = calculator.analyze_parent()
    top_parent_results = calculator.analyze_top_parent()
    final_results = calculator.combine_results(parent_results, top_parent_results)
    df = pd.DataFrame(final_results)
    aggregated_df = df.groupby("variable").agg(
        parent_metric=("parent_metric", "first"),
        top_metric=("top_metric", "first"),
        parent_percentage_difference=("parent_percentage_difference", "mean"),
        top_parent_percentage_difference=("top_parent_percentage_difference", "mean"),
    ).reset_index()
    metric_details = calculator.get_metric_details("AcceptOpps", aggregated_df)
    assert metric_details["metric_id"] == "AcceptOpps"
    assert metric_details["parent_metric"] == "NewBizDeals"


def test_build_output_structure(calculator, metric_json, values_df, max_values):
    calculator.run(metric_json, values_df, max_values)
    parent_results = calculator.analyze_parent()
    top_parent_results = calculator.analyze_top_parent()
    final_results = calculator.combine_results(parent_results, top_parent_results)
    df = pd.DataFrame(final_results)
    aggregated_df = df.groupby("variable").agg(
        parent_metric=("parent_metric", "first"),
        top_metric=("top_metric", "first"),
        parent_percentage_difference=("parent_percentage_difference", "mean"),
        top_parent_percentage_difference=("top_parent_percentage_difference", "mean"),
    ).reset_index()
    output = calculator.build_output_structure(aggregated_df)
    assert output["metric_id"] == "NewBizDeals"
    assert output["components"][0]["metric_id"] == "AcceptOpps"


def test_run(calculator, metric_json, values_df, max_values):
    output = calculator.run(metric_json, values_df, max_values)
    assert output["metric_id"] == "NewBizDeals"
    assert output["components"][0]["metric_id"] == "AcceptOpps"
