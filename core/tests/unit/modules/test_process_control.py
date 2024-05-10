import numpy as np
import pandas as pd
import pytest

from fulcrum_core import AnalysisManager
from fulcrum_core.execptions import InsufficientDataError
from fulcrum_core.modules.process_control import ProcessControlAnalyzer


@pytest.fixture(name="input_df")
def input_df_fixture():
    website_visits = [
        3332,
        3576,
        3646,
        4026,
        3841,
        3315,
        3843,
        3979,
        5270,
        4926,
        3423,
        4849,
        5728,
        5059,
        5298,
        4060,
        4086,
        4290,
        4817,
        5492,
        5680,
        4879,
        6668,
        6755,
        7629,
        7986,
        8097,
        7196,
        6866,
        7494,
        7780,
        8809,
        9276,
        8020,
        9187,
        9204,
        10734,
        10184,
        10504,
        10671,
        10780,
        9964,
        11016,
        11249,
        11332,
        9750,
    ]
    input_df = pd.DataFrame(
        {
            "date": pd.date_range("2022-01-01", periods=len(website_visits), freq="MS"),
            "value": website_visits,
        }
    )
    return input_df


def test_get_half_average_point():
    # Arrange
    analyzer = ProcessControlAnalyzer()
    start_index = 0
    end_index = 10

    # Act
    half_average_point = analyzer.get_half_average_point(start_index, end_index)

    # Assert
    assert half_average_point == 5

    # Arrange
    start_index = 0
    end_index = 7

    # Act
    half_average_point = analyzer.get_half_average_point(start_index, end_index)

    # Assert
    assert half_average_point == 3

    # Arrange
    start_index = 0
    end_index = 18

    # Act
    half_average_point = analyzer.get_half_average_point(start_index, end_index)

    # Assert
    assert half_average_point == 9


def test_compute_half_averages(input_df):
    # Arrange
    analyzer = ProcessControlAnalyzer()
    start_index = 0
    end_index = 18

    # Act
    half_averages = np.round(analyzer.compute_half_averages(input_df, start_index, end_index))

    # Assert
    assert half_averages[0] == 3870
    assert half_averages[1] == 4635

    # Arrange
    start_index = 22
    end_index = 40

    # Act
    half_averages = np.round(analyzer.compute_half_averages(input_df, start_index, end_index))

    # Assert
    assert half_averages[0] == 7386
    assert half_averages[1] == 9621


def test_compute_central_line(input_df):
    # Arrange
    analyzer = ProcessControlAnalyzer()
    half_averages = [3870, 4635]
    start_index = 0
    end_index = 18
    data_points = 36

    # Act
    central_line, slope = analyzer.compute_central_line(half_averages, start_index, end_index, data_points)

    # Assert
    assert central_line[4] == 3870
    assert slope == 85
    assert len(central_line) == data_points


def test_compute_control_limits(input_df):
    # Arrange
    analyzer = ProcessControlAnalyzer()
    central_line = [3870, 3955, 4040, 4125, 4210, 4295, 4380, 4465, 4550, 4635]
    df = input_df.iloc[:10]

    # Act
    ucl, lcl = np.round(analyzer.compute_control_limits(df, central_line))

    # Assert
    assert ucl[0] == 4965
    assert lcl[0] == 2775
    assert len(ucl) == len(central_line)
    assert len(lcl) == len(central_line)


def test_detect_signal(input_df):
    # Arrange
    analyzer = ProcessControlAnalyzer()
    central_line = [4720, 4805, 4891, 4976, 5061, 5146, 5231, 5316]
    lcl = [3335, 3420, 3506, 3591, 3676, 3761, 3846, 3931]
    ucl = [6105, 6190, 6276, 6361, 6446, 6531, 6616, 6701]
    start_index = 16
    df = input_df.iloc[start_index : len(central_line) + start_index]
    # Act
    signal_indices = analyzer.detect_signal(df, central_line, ucl, lcl)

    # Assert
    assert signal_indices == [22, 23]


def test_is_recalculation_needed():
    # Arrange
    analyzer = ProcessControlAnalyzer()
    signal_indices = [18, 24, 25, 26, 27, 28, 32, 38, 38, 16]

    # Act
    first_consecutive_index = analyzer.is_recalculation_needed(signal_indices)

    # Assert
    assert first_consecutive_index == 24

    # Arrange
    signal_indices = [18, 24, 25, 26, 28, 32, 38, 38, 16, 17]

    # Act
    first_consecutive_index = analyzer.is_recalculation_needed(signal_indices)

    # Assert
    assert first_consecutive_index is None


def test_analyze(input_df):
    # Arrange
    analyzer = ProcessControlAnalyzer()

    # Act
    res_df = analyzer.analyze(input_df)

    # Assert
    trend_detected_indices = res_df[res_df["trend_signal_detected"]].index.tolist()
    assert trend_detected_indices == [18]
    assert res_df["slope"][17] == 85.074
    assert res_df["slope"][18] == 195.42
    assert res_df["slope_change"][18] == 129.705
    assert res_df["central_line"] is not None
    assert res_df["ucl"] is not None
    assert res_df["lcl"] is not None


def test_analyze_insufficient_data():
    # Arrange
    analyzer = ProcessControlAnalyzer()
    input_df = pd.DataFrame(
        {
            "date": pd.date_range("2022-01-01", periods=5, freq="MS"),
            "value": [3332, 3576, 3646, 4026, 3841],
        }
    )

    # Act & Assert
    with pytest.raises(InsufficientDataError):
        analyzer.analyze(input_df)


def test_analyze_insufficient_data_after_signal(input_df):
    # Arrange
    analyzer = ProcessControlAnalyzer()
    input_df = input_df.copy()
    input_df = input_df.iloc[:27]

    # Act
    res_df = analyzer.analyze(input_df)

    # Assert
    # assert slope not changes at trend signal detected index
    trend_detected_index = res_df[res_df["trend_signal_detected"]].index.tolist()[0]
    assert res_df["slope"][trend_detected_index - 1] == 85.074
    assert res_df["slope"][trend_detected_index + 1] == 85.074
    assert res_df["slope_change"][trend_detected_index] == 0


def test_analysis_manager_process_control(input_df):
    # Arrange
    analysis_manager = AnalysisManager()

    # Act
    res_df = analysis_manager.process_control(input_df)

    # Assert
    assert res_df["central_line"] is not None
    assert res_df["ucl"] is not None
    assert res_df["lcl"] is not None
    assert res_df["slope"] is not None
    assert res_df["slope_change"] is not None
    assert res_df["trend_signal_detected"] is not None
    trend_detected_indices = res_df[res_df["trend_signal_detected"]].index.tolist()
    assert trend_detected_indices == [18]
    assert res_df["slope"][17] == 85.074
    assert res_df["slope"][18] == 195.42
    assert res_df["slope_change"][18] == 129.705
