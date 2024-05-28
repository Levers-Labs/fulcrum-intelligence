import pandas as pd
import pytest

from fulcrum_core.execptions import AnalysisError
from fulcrum_core.modules.describe import DescribeAnalyzer


@pytest.fixture
def describe_analyzer():
    return DescribeAnalyzer()


@pytest.fixture
def describe_df(describe_data):
    return pd.DataFrame(describe_data)


def test_validate_input(describe_df, describe_analyzer):
    # Prepare
    df = describe_df.copy()
    df.drop(columns=["date"], inplace=True)
    # Act
    with pytest.raises(AnalysisError):
        describe_analyzer.validate_input(df)


def test_preprocess_data(describe_df, describe_analyzer):
    # Prepare
    df = describe_df.copy()
    # Act
    result = describe_analyzer.preprocess_data(df)
    # Assert
    assert result["date"].dtype == "datetime64[ns]"
    assert result["value"].dtype == "int64"


def test_analyze(describe_df, describe_analyzer, describe_output):
    # Prepare
    df = describe_df.copy()
    start_date = pd.to_datetime("2023-01-01")
    end_date = pd.to_datetime("2025-01-01")
    metric_id = "ToMRR"
    dimensions = ["region", "stage_name"]
    aggregation_function = "sum"
    # Act
    result = describe_analyzer.run(
        df,
        start_date=start_date,
        end_date=end_date,
        metric_id=metric_id,
        dimensions=dimensions,
        aggregation_function=aggregation_function,
    )
    # Assert
    assert sorted(
        result.to_dict(orient="records"), key=lambda x: (x["metric_id"], x["dimension"], x["member"])
    ) == sorted(describe_output, key=lambda x: (x["metric_id"], x["dimension"], x["member"]))
