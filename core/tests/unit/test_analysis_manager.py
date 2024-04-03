import math
from datetime import date

from fulcrum_core import AnalysisManager


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

    for index in range(len(response["half_average"])):
        response["half_average"][index] = (
            "nan" if math.isnan(response["half_average"][index]) else response["half_average"][index]
        )
    assert process_control_output == response
