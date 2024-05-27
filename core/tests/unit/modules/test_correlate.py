import pandas as pd
import pytest

from fulcrum_core.execptions import AnalysisError
from fulcrum_core.modules.correlate import CorrelationAnalyzer


@pytest.fixture
def correlate_df():
    return pd.DataFrame(
        {
            "date": ["2021-01-01", "2021-01-01", "2021-01-02", "2021-01-02"],
            "metric_id": ["A", "B", "A", "B"],
            "value": [100, 200, 150, 250],
        }
    )


@pytest.fixture
def correlate_analyzer():
    return CorrelationAnalyzer(precision=3)


def test_validate_input(correlate_df, correlate_analyzer):
    # Prepare
    df = correlate_df.copy()
    df.drop(columns=["date"], inplace=True)
    # Act
    with pytest.raises(AnalysisError):
        correlate_analyzer.validate_input(df)


def test_preprocess_data(correlate_df, correlate_analyzer):
    # Prepare
    df = correlate_df.copy()
    # Act
    result = correlate_analyzer.preprocess_data(df)
    # Assert
    assert result["date"].dtype == "datetime64[ns]"
    assert result["value"].dtype == "int64"


def test_analyze(correlate_df, correlate_analyzer):
    # Prepare
    df = correlate_df.copy()
    expected_result = [{"metric_id_1": "A", "metric_id_2": "B", "correlation_coefficient": 1.0}]
    # Act
    result = correlate_analyzer.analyze(df)
    # Assert
    assert result == expected_result

    # Prepare
    df = pd.DataFrame(
        {
            "date": [
                "2021-01-01",
                "2021-01-01",
                "2021-01-01",
                "2021-01-02",
                "2021-01-02",
                "2021-01-02",
                "2021-01-03",
                "2021-01-03",
                "2021-01-03",
                "2021-01-04",
                "2021-01-04",
                "2021-01-04",
            ],
            "metric_id": ["A", "B", "C", "A", "B", "C", "A", "B", "C", "A", "B", "C"],
            "value": [100, 200, 150, 150, 250, 350, 200, 300, 400, 200, 350, 300],
        }
    )
    expected_result = [
        {
            "metric_id_1": "A",
            "metric_id_2": "B",
            "correlation_coefficient": 0.944,
        },
        {"metric_id_1": "A", "metric_id_2": "C", "correlation_coefficient": 0.806},
        {"metric_id_1": "B", "metric_id_2": "C", "correlation_coefficient": 0.598},
    ]
    # Act
    result = correlate_analyzer.run(df)
    # Assert
    assert result == expected_result
