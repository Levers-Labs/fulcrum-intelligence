from typing import Any

import pandas as pd
import pytest

from fulcrum_core.execptions import InsufficientDataError
from fulcrum_core.modules import BaseAnalyzer


@pytest.fixture
def analyzer():
    class TestAnalyzer(BaseAnalyzer):
        def analyze(self, df: pd.DataFrame, *args, **kwargs) -> dict[str, Any] | pd.DataFrame:
            df["extra"] = 1
            return df

    return TestAnalyzer(precision=3)


def test_calculate_percent_change(analyzer):
    percent_change = analyzer.calculate_percent_change(200, 100)
    assert percent_change == 100


def test_calculate_percent_change_zero_division(analyzer):
    percent_change = analyzer.calculate_percent_change(200, 0)
    assert percent_change == float("inf")


def test_validate_input(analyzer):
    with pytest.raises(InsufficientDataError):
        analyzer.validate_input(pd.DataFrame())


def test_preprocess_data(analyzer):
    df = pd.DataFrame({"date": ["2021-01-01", "2021-01-02"], "value": [100, 200]})
    result = analyzer.preprocess_data(df)
    assert result.equals(df)


def test_post_process_result(analyzer):
    result = {"date": "2021-01-01", "value": 100}
    result = analyzer.post_process_result(result)
    assert "analyzed_at" in result

    result = pd.DataFrame({"date": ["2021-01-01"], "value": [100]})
    result = analyzer.post_process_result(result)
    assert "analyzed_at" in result.columns

    result = [100, 200]
    result = analyzer.post_process_result(result)
    assert "analyzed_at" not in result


def test_round_values(analyzer):
    # Prepare
    result = {"value": 100.123456789}
    expected_result = {"value": 100.123}
    # Act
    result = analyzer.round_values(result)
    # Assert
    assert result == expected_result

    # Prepare
    result = {"value": {"sub_value": 100.123456789}}
    expected_result = {"value": {"sub_value": 100.123}}
    # Act
    result = analyzer.round_values(result)
    # Assert
    assert result == expected_result

    # Prepare
    result = [{"value": {"sub_value": 100.123456789}}]
    expected_result = [{"value": {"sub_value": 100.123}}]
    # Act
    result = analyzer.round_values(result)
    # Assert
    assert result == expected_result

    # Prepare
    result = [100.123456789, 200.123456789, 300.123456789]
    expected_result = [100.123, 200.123, 300.123]
    # Act
    result = analyzer.round_values(result)
    # Assert
    assert result == expected_result

    # Prepare
    result = pd.DataFrame(
        {"value": [100.122332, 200.123456789, 300.123456789], "date": ["2021-01-01", "2021-01-02", "2021-01-03"]}
    )
    expected_result = pd.DataFrame({"value": [100.122, 200.123, 300.123]})
    # Act
    result = analyzer.round_values(result)
    # Assert
    assert list(result["value"]) == list(expected_result["value"])


def test_run(analyzer):
    # Prepare
    df = pd.DataFrame({"date": ["2021-01-01", "2021-01-02"], "value": [100, 200]})
    # Act
    result = analyzer.run(df)
    # Assert
    assert "extra" in result.columns
    assert result["extra"].sum() == 2


def test_run_no_data(analyzer):
    # Prepare
    df = pd.DataFrame()
    # Act
    with pytest.raises(InsufficientDataError):
        analyzer.run(df)
