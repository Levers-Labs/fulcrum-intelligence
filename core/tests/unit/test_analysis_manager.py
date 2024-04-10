from datetime import date

import pandas as pd

from fulcrum_core import AnalysisManager


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


def test_process_control(process_control_df, process_control_output):
    analysis_manager = AnalysisManager()
    response = analysis_manager.process_control(process_control_df, "", "", "", "MONTH", debug=False)
    assert process_control_output == response


def test_segment_drift(segment_drift_data, segment_drift_output):
    analysis_manager = AnalysisManager()
    response = analysis_manager.segment_drift(segment_drift_data, debug=True)
    assert recursive_dict_compare(
        sorted(recursively_round_up_values(response)), sorted(recursively_round_up_values(segment_drift_output))
    )


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
