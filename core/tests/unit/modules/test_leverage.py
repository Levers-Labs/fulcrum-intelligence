import pandas as pd
import pytest
import sympy as sp

from fulcrum_core.modules import LeverageIdCalculator


@pytest.fixture(name="metric_expression")
def metric_expression_fixture():
    return {
        "metric_id": "NewBizDeals",
        "expression_str": "AcceptOpps + SQOToWinRate",
        "expression": {
            "type": "expression",
            "operator": "+",
            "operands": [
                {"type": "metric", "metric_id": "AcceptOpps"},
                {"type": "metric", "metric_id": "SQOToWinRate"},
            ],
        },
    }


@pytest.fixture(name="values_df")
def values_df_fixture():
    return pd.DataFrame(
        {
            "date": ["2023-01-01", "2023-01-02"],
            "AcceptOpps": [100, 150],
            "SQOToWinRate": [50, 75],
            "NewBizDeals": [150, 225],
        }
    )


@pytest.fixture(name="max_values")
def max_values_fixture():
    return {"AcceptOpps": 200, "SQOToWinRate": 100}


@pytest.fixture(name="calculator")
def calculator_fixture(metric_expression, max_values):
    return LeverageIdCalculator(metric_expression, max_values)


def test_extract_expression(calculator):
    equations_list = calculator._extract_expression()
    assert "NewBizDeals = AcceptOpps + SQOToWinRate" in equations_list


def test_create_parent_and_root_dicts(calculator):
    parent_dict, root_dict = calculator._create_parent_and_root_dicts()
    assert parent_dict == {"AcceptOpps": "NewBizDeals", "SQOToWinRate": "NewBizDeals"}
    assert root_dict == {"AcceptOpps": "NewBizDeals", "SQOToWinRate": "NewBizDeals"}


def test_compute_y(calculator, values_df):
    calculator.metric_symbols = {var: sp.symbols(var) for var in values_df.columns if var != "date"}
    y = calculator._compute_y("AcceptOpps + SQOToWinRate", {"AcceptOpps": 100, "SQOToWinRate": 50})
    assert y == 150


def test_analyze_parent(calculator, values_df):
    parent_dict, _ = calculator._create_parent_and_root_dicts()
    equations_list = calculator._extract_expression()
    calculator.metric_symbols = {var: sp.symbols(var) for var in values_df.columns if var != "date"}
    parent_results = calculator.analyze_parent(parent_dict, equations_list, values_df)
    assert len(parent_results) == 4


def test_analyze_root(calculator, values_df):
    equations_list = calculator._extract_expression()
    calculator.metric_symbols = {var: sp.symbols(var) for var in values_df.columns if var != "date"}
    root_results = calculator.analyze_root(equations_list, values_df)
    assert len(root_results) == 6


def test_combine_results(calculator, values_df):
    parent_dict, _ = calculator._create_parent_and_root_dicts()
    equations_list = calculator._extract_expression()
    calculator.metric_symbols = {var: sp.symbols(var) for var in values_df.columns if var != "date"}
    parent_results = calculator.analyze_parent(parent_dict, equations_list, values_df)
    root_results = calculator.analyze_root(equations_list, values_df)
    final_results = calculator.combine_results(parent_results, root_results)
    assert len(final_results) == 4


def test_get_metric_details(calculator, values_df):
    parent_dict, _ = calculator._create_parent_and_root_dicts()
    equations_list = calculator._extract_expression()
    calculator.metric_symbols = {var: sp.symbols(var) for var in values_df.columns if var != "date"}
    parent_results = calculator.analyze_parent(parent_dict, equations_list, values_df)
    root_results = calculator.analyze_root(equations_list, values_df)
    final_results = calculator.combine_results(parent_results, root_results)
    df = pd.DataFrame(final_results)
    aggregated_df = (
        df.groupby("variable")
        .agg(
            parent_metric=("parent_metric", "first"),
            top_metric=("top_metric", "first"),
            pct_diff=("pct_diff", "mean"),
            pct_diff_root=("pct_diff_root", "mean"),
        )
        .reset_index()
    )
    metric_details = calculator.get_metric_details("AcceptOpps", aggregated_df)
    assert metric_details["metric_id"] == "AcceptOpps"
    assert metric_details["parent_metric"] == "NewBizDeals"


def test_build_output_structure(calculator, values_df):
    parent_dict, _ = calculator._create_parent_and_root_dicts()
    equations_list = calculator._extract_expression()
    calculator.metric_symbols = {var: sp.symbols(var) for var in values_df.columns if var != "date"}
    parent_results = calculator.analyze_parent(parent_dict, equations_list, values_df)
    root_results = calculator.analyze_root(equations_list, values_df)
    final_results = calculator.combine_results(parent_results, root_results)
    df = pd.DataFrame(final_results)
    aggregated_df = (
        df.groupby("variable")
        .agg(
            parent_metric=("parent_metric", "first"),
            top_metric=("top_metric", "first"),
            pct_diff=("pct_diff", "mean"),
            pct_diff_root=("pct_diff_root", "mean"),
        )
        .reset_index()
    )
    output = calculator.build_output_structure(aggregated_df)
    assert output["metric_id"] == "NewBizDeals"
    assert "leverage" in output
    assert "pct_diff" in output["leverage"]
    assert "pct_diff_root" in output["leverage"]
    assert len(output["components"]) > 0
    assert output["components"][0]["metric_id"] in ["AcceptOpps", "SQOToWinRate"]
    assert "leverage" in output["components"][0]


def test_analyze(calculator, values_df):
    calculator.metric_symbols = {var: sp.symbols(var) for var in values_df.columns if var != "date"}
    output = calculator.analyze(values_df)
    assert output["metric_id"] == "NewBizDeals"
    assert "leverage" in output
    assert "pct_diff" in output["leverage"]
    assert "pct_diff_root" in output["leverage"]
    assert len(output["components"]) > 0
    assert output["components"][0]["metric_id"] in ["AcceptOpps", "SQOToWinRate"]
    assert "leverage" in output["components"][0]
