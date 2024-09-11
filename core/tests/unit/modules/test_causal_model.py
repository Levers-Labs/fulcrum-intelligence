import numpy as np
import pandas as pd
import pytest

from fulcrum_core.modules.causal_model import CausalModelAnalyzer


@pytest.fixture
def sample_data():
    dates = pd.date_range(start="2023-01-01", periods=100)
    data = {
        "date": dates,
        "NewBizDeals": np.random.rand(100) * 100,
        "AcceptOpps": np.random.rand(100) * 200,
        "SQOToWinRate": np.random.rand(100) * 0.3,
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_input_df():
    dates = pd.date_range(start="2023-01-01", periods=100)
    data = {"date": dates, "OpenNewBizOpps": np.random.rand(100) * 300, "SQORate": np.random.rand(100) * 0.5}
    return pd.DataFrame(data)


@pytest.fixture
def sample_influencers():
    return [
        {
            "metric_id": "AcceptOpps",
            "influences": [
                {"metric_id": "OpenNewBizOpps", "influences": []},
                {"metric_id": "SQORate", "influences": []},
            ],
        },
        {"metric_id": "SQOToWinRate", "influences": []},
    ]


@pytest.fixture
def result_output():
    return {
        "metric_id": "NewBizDeals",
        "model": {"coefficient": 1.0, "relative_impact": 0.0, "coefficient_root": 1.0, "relative_impact_root": 0.0},
        "components": [
            {
                "metric_id": "AcceptOpps",
                "model": {
                    "coefficient": 0.5,
                    "relative_impact": 0.0,
                    "coefficient_root": 0.5,
                    "relative_impact_root": 0.0,
                },
                "components": [],
            },
            {
                "metric_id": "SQOToWinRate",
                "model": {
                    "coefficient": 0.3,
                    "relative_impact": 0.0,
                    "coefficient_root": 0.3,
                    "relative_impact_root": 0.0,
                },
                "components": [],
            },
            {
                "metric_id": "NewProsps",
                "model": {
                    "coefficient": 0.2,
                    "relative_impact": 0.0,
                    "coefficient_root": 0.2,
                    "relative_impact_root": 0.0,
                },
                "components": [],
            },
        ],
    }


@pytest.fixture
def causal_analyzer(sample_influencers):
    return CausalModelAnalyzer("NewBizDeals", sample_influencers)


def test_init(causal_analyzer):
    assert causal_analyzer.target_metric_id == "NewBizDeals"
    assert len(causal_analyzer.influencers) == 2


def test_validate_input(causal_analyzer, sample_data):
    melted_data = pd.melt(sample_data, id_vars=["date"], var_name="metric_id", value_name="value")
    causal_analyzer.validate_input(melted_data)
    # If no exception is raised, the test passes


def test_validate_input_missing_columns(causal_analyzer):
    invalid_data = pd.DataFrame({"date": [], "value": []})
    with pytest.raises(ValueError):
        causal_analyzer.validate_input(invalid_data)


def test_integrate_yearly_seasonality(causal_analyzer, sample_data):
    result = causal_analyzer._integrate_yearly_seasonality(sample_data)
    assert "yearly_NewBizDeals" in result.columns


def test_extract_columns_from_hierarchy(causal_analyzer):
    columns, edges = causal_analyzer._extract_columns_from_hierarchy(causal_analyzer.influencers)
    assert set(columns) == {"NewBizDeals", "AcceptOpps", "OpenNewBizOpps", "SQORate", "SQOToWinRate"}
    assert len(edges) == 4


def test_add_direct_connections(causal_analyzer):
    all_columns = ["date", "NewBizDeals", "AcceptOpps", "OpenNewBizOpps", "SQORate", "SQOToWinRate", "OtherMetric"]
    selected_columns = ["NewBizDeals", "AcceptOpps", "OpenNewBizOpps", "SQORate", "SQOToWinRate"]
    connections = causal_analyzer._add_direct_connections(all_columns, selected_columns)
    assert len(connections) == 1
    assert connections[0] == "OtherMetric -> NewBizDeals"


def test_prepare_data_and_graph_edges(causal_analyzer, sample_data):
    prepared_data, columns, edges = causal_analyzer._prepare_data_and_graph_edges(sample_data)
    assert isinstance(prepared_data, pd.DataFrame)
    assert len(columns) > 0
    assert len(edges) > 0


def test_build_causal_graph(causal_analyzer):
    edges = ["AcceptOpps -> NewBizDeals", "SQOToWinRate -> NewBizDeals"]
    graph = causal_analyzer.build_causal_graph(edges)
    assert graph == "digraph { AcceptOpps -> NewBizDeals; SQOToWinRate -> NewBizDeals }"


def test_build_hierarchy(causal_analyzer):
    causal_analyzer.flipped_relation_dict = {"NewBizDeals": ["AcceptOpps", "SQOToWinRate"]}
    hierarchy = causal_analyzer._build_hierarchy("NewBizDeals")
    assert hierarchy["metric_id"] == "NewBizDeals"
    assert len(hierarchy["components"]) == 2


def test_calculate_relative_impact(causal_analyzer, result_output):
    causal_analyzer.flipped_relation_dict = {"NewBizDeals": ["AcceptOpps", "SQOToWinRate", "NewProsps"]}
    value_dict = {"NewBizDeals": 1000, "AcceptOpps": 100, "SQOToWinRate": 50, "NewProsps": 200}

    updated_result = causal_analyzer.calculate_relative_impact("NewBizDeals", value_dict, result_output)

    # Check that relative impacts are calculated
    assert all(component["model"]["relative_impact"] > 0 for component in updated_result["components"])

    # Check that the sum of relative impacts is approximately 100%
    total_relative_impact = sum(component["model"]["relative_impact"] for component in updated_result["components"])
    assert abs(total_relative_impact - 100) < 1e-6

    # Check that AcceptOpps has the highest relative impact
    assert updated_result["components"][0]["model"]["relative_impact"] == max(
        component["model"]["relative_impact"] for component in updated_result["components"]
    )

    # Check that relative_impact_root is equal to relative_impact for all components
    for component in updated_result["components"]:
        assert component["model"]["relative_impact_root"] == component["model"]["relative_impact"]


def test_find_metric(causal_analyzer):
    result_output = {
        "metric_id": "NewBizDeals",
        "components": [{"metric_id": "AcceptOpps", "model": {}}, {"metric_id": "SQOToWinRate", "model": {}}],
    }
    found_metric = causal_analyzer.find_metric(result_output, "AcceptOpps")
    assert found_metric["metric_id"] == "AcceptOpps"


def test_update_graph_definition(causal_analyzer):
    graph_definition = "digraph { AcceptOpps -> NewBizDeals; SQOToWinRate -> NewBizDeals }"
    factor_columns_dict = {"AcceptOpps": "AcceptOpps", "NewBizDeals": "NewBizDeals"}
    updated_graph = causal_analyzer._update_graph_definition(graph_definition, factor_columns_dict)
    assert "AcceptOpps -> NewBizDeals" in updated_graph


def test_merge_dataframes(causal_analyzer):
    df1 = pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=5),
            "metric_id": ["NewBizDeals"] * 5,
            "value": [100, 200, 300, 400, 500],
        }
    )
    df2 = pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=5),
            "metric_id": ["AcceptOpps"] * 5,
            "value": [10, 20, 30, 40, 50],
        }
    )
    merged_df = causal_analyzer.merge_dataframes([df1, df2])
    assert "NewBizDeals" in merged_df.columns
    assert "AcceptOpps" in merged_df.columns
    assert len(merged_df) == 5


def test_get_indirect_columns(causal_analyzer):
    graph_definition = "digraph { AcceptOpps -> OpenNewBizOpps; OpenNewBizOpps -> NewBizDeals }"
    indirect_columns = causal_analyzer.get_indirect_columns(graph_definition)
    assert "AcceptOpps" in indirect_columns
    assert "OpenNewBizOpps" not in indirect_columns


def test_analyze(sample_data, sample_input_df, sample_influencers):
    target_metric_id = "NewBizDeals"
    analyzer = CausalModelAnalyzer(target_metric_id, sample_influencers)

    # Melt the DataFrames to match the expected input format
    sample_data_melted = pd.melt(sample_data, id_vars=["date"], var_name="metric_id", value_name="value")
    sample_input_df_melted = pd.melt(sample_input_df, id_vars=["date"], var_name="metric_id", value_name="value")

    result = analyzer.analyze(sample_data_melted, input_dfs=[sample_input_df_melted])

    assert isinstance(result, dict)
    assert "metric_id" in result
    assert result["metric_id"] == target_metric_id
    assert "components" in result

    for component in result["components"]:
        assert "metric_id" in component
        assert "model" in component
        assert "coefficient" in component["model"]
        assert "relative_impact" in component["model"]

    # Check if AcceptOpps has sub-components
    accept_opps = next(comp for comp in result["components"] if comp["metric_id"] == "AcceptOpps")
    assert "components" in accept_opps
    assert len(accept_opps["components"]) == 2

    # Check if relative impacts sum up to approximately 100%
    total_impact = sum(comp["model"]["relative_impact"] for comp in result["components"])
    assert abs(total_impact - 100) < 1e-6

    # Check if the result contains data for metrics from both input dataframes
    all_metrics = set(
        [comp["metric_id"] for comp in result["components"]]
        + [subcomp["metric_id"] for comp in result["components"] for subcomp in comp.get("components", [])]
    )
    assert "OpenNewBizOpps" in all_metrics
    assert "SQORate" in all_metrics

    # Check for the absence of error key
    assert "error" not in result


def test_validate_input_empty_dataframe(causal_analyzer):
    empty_df = pd.DataFrame()
    with pytest.raises(ValueError, match="Invalid input dataframe"):
        causal_analyzer.validate_input(empty_df)


def test_analyze_with_empty_influencers():
    sample_input_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=100),
            "metric_id": ["NewBizDeals"] * 100,
            "value": [100] * 100,
        }
    )
    analyzer = CausalModelAnalyzer("NewBizDeals", influencers=[])
    with pytest.raises(ValueError, match="Influencers list is empty"):
        analyzer.validate_input(sample_input_df)


def test_merge_dataframes_with_no_common_dates(causal_analyzer):
    df1 = pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=5),
            "metric_id": ["NewBizDeals"] * 5,
            "value": [100, 200, 300, 400, 500],
        }
    )
    df2 = pd.DataFrame(
        {
            "date": pd.date_range(start="2023-06-01", periods=5),
            "metric_id": ["AcceptOpps"] * 5,
            "value": [10, 20, 30, 40, 50],
        }
    )
    merged_df = causal_analyzer.merge_dataframes([df1, df2])
    assert len(merged_df) == 0


def test_analyze_with_single_data_point(sample_influencers):
    analyzer = CausalModelAnalyzer("NewBizDeals", sample_influencers)
    single_point_df = pd.DataFrame({"date": ["2023-01-01"], "metric_id": ["NewBizDeals"], "value": [100]})
    result = analyzer.analyze(single_point_df)
    assert "error" in result


def test_analyze_with_constant_values(sample_influencers):
    analyzer = CausalModelAnalyzer("NewBizDeals", sample_influencers)
    constant_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=100),
            "metric_id": ["NewBizDeals"] * 100,
            "value": [100] * 100,
        }
    )
    result = analyzer.analyze(constant_df)
    assert "error" in result


def test_build_causal_graph_with_cycles(causal_analyzer):
    edges = ["A -> B", "B -> C", "C -> A"]
    graph = causal_analyzer.build_causal_graph(edges)
    assert "A -> B" in graph and "B -> C" in graph and "C -> A" in graph


def test_calculate_relative_impact_with_zero_values(causal_analyzer, result_output):
    causal_analyzer.flipped_relation_dict = {"NewBizDeals": ["AcceptOpps", "SQOToWinRate"]}
    value_dict = {"NewBizDeals": 0, "AcceptOpps": 0, "SQOToWinRate": 0}

    updated_result = causal_analyzer.calculate_relative_impact("NewBizDeals", value_dict, result_output)

    # Check that relative impacts are 0 when all values are 0
    assert all(component["model"]["relative_impact"] == 0 for component in updated_result["components"])
