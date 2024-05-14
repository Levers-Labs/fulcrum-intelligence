from datetime import date

import pandas as pd
import pytest

from fulcrum_core import AnalysisManager
from fulcrum_core.enums import Granularity
from fulcrum_core.execptions import AnalysisError, InsufficientDataError
from fulcrum_core.modules import SimpleForecast


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
    ]
    input_df = pd.DataFrame(
        {
            "date": pd.date_range("2022-01-01", periods=len(website_visits), freq="MS"),
            "value": website_visits,
        }
    )
    return input_df


def test_predict_till_date(input_df):
    # Prepare
    forecast = SimpleForecast(input_df, Granularity.MONTH)
    start_date = forecast._get_forecast_start_date()
    end_date = start_date + pd.DateOffset(months=12)
    expected_count = 12

    # Act
    results = forecast.predict_till_date(end_date)

    # Assert
    assert len(results) == expected_count
    assert results[-1]["date"] == date(2026, 7, 1)
    assert results[0]["value"] == pytest.approx(12675, 100)
    assert results[-1]["value"] == pytest.approx(12415, 100)
    assert "confidence_interval" in results[0]
    assert "confidence_interval" in results[-1]


def test_get_forecast_start_date(input_df):
    # Prepare
    forecast = SimpleForecast(input_df, Granularity.MONTH)
    expected_date = pd.Timestamp("2025-07-31")

    # Act
    result = forecast._get_forecast_start_date()

    # Assert
    assert result == expected_date


def test_get_min_data_points(input_df):
    # Prepare
    month_expected = 12
    week_expected = 52
    quarter_expected = 4
    day_expected = 30
    forcast = SimpleForecast(input_df, Granularity.MONTH)

    # Act
    forcast.grain = Granularity.MONTH
    month_result = forcast._get_min_data_points()
    forcast.grain = Granularity.WEEK
    week_result = forcast._get_min_data_points()
    forcast.grain = Granularity.QUARTER
    quarter_result = forcast._get_min_data_points()
    forcast.grain = Granularity.DAY
    day_result = forcast._get_min_data_points()

    # Assert
    assert month_result == month_expected
    assert week_result == week_expected
    assert quarter_result == quarter_expected
    assert day_result == day_expected


def test_get_grain_type_interval_gap(input_df):
    # Prepare
    month_expected = ("ME", pd.Timedelta(weeks=8))
    week_expected = ("W", pd.Timedelta(weeks=2))
    quarter_expected = ("Q", pd.Timedelta(weeks=24))
    day_expected = ("D", pd.Timedelta(days=2))
    forcast = SimpleForecast(input_df, Granularity.MONTH)

    # Act
    forcast.grain = Granularity.MONTH
    month_result = forcast._get_grain_type_interval_gap()
    forcast.grain = Granularity.WEEK
    week_result = forcast._get_grain_type_interval_gap()
    forcast.grain = Granularity.QUARTER
    quarter_result = forcast._get_grain_type_interval_gap()
    forcast.grain = Granularity.DAY
    day_result = forcast._get_grain_type_interval_gap()

    # Assert
    assert month_result == month_expected
    assert week_result == week_expected
    assert quarter_result == quarter_expected
    assert day_result == day_expected


def test_get_frequency(input_df):
    # Prepare
    month_expected = "MS"
    week_expected = "W-MON"
    quarter_expected = "QS"
    day_expected = "D"
    forcast = SimpleForecast(input_df, Granularity.MONTH)

    # Act
    forcast.grain = Granularity.MONTH
    month_result = forcast._get_frequency()
    forcast.grain = Granularity.WEEK
    week_result = forcast._get_frequency()
    forcast.grain = Granularity.QUARTER
    quarter_result = forcast._get_frequency()
    forcast.grain = Granularity.DAY
    day_result = forcast._get_frequency()

    # Assert
    assert month_result == month_expected
    assert week_result == week_expected
    assert quarter_result == quarter_expected
    assert day_result == day_expected


def test_preprocess_data(input_df):
    # Prepare
    forcast = SimpleForecast(input_df.copy(), Granularity.MONTH)
    expected_len = 43

    # Act
    result = forcast._preprocess_data(input_df)

    # Assert
    assert len(result) == expected_len
    assert result["value"].isnull().sum() == 0
    assert result["value"].isna().sum() == 0
    assert result["date"].dtype == "datetime64[ns]"
    assert result["value"].dtype == "float64"


def test_validate_data(input_df):
    # Prepare
    forcast = SimpleForecast(input_df.copy(), Granularity.MONTH)

    # Act
    forcast._validate_data()

    # Assert
    assert True

    # Act
    with pytest.raises(InsufficientDataError):
        SimpleForecast(input_df.copy(), Granularity.WEEK)


def test_predict_n(input_df):
    # Prepare
    forcast = SimpleForecast(input_df, Granularity.MONTH)
    expected_count = 12

    # Act
    result = forcast.predict_n(12)

    # Assert
    assert len(result) == expected_count
    assert result[0]["date"] == date(2025, 8, 1)

    # Act
    with pytest.raises(AnalysisError):
        forcast.predict_n(0)


def test_predict_till_date_error(input_df):
    # Prepare
    forcast = SimpleForecast(input_df.copy(), Granularity.MONTH)

    # Act
    with pytest.raises(AnalysisError):
        forcast.predict_till_date(date(2022, 7, 31))


def test_analysis_manager_simple_forecast(input_df):
    # Prepare
    analysis_manager = AnalysisManager()
    expected_count = 12

    # Act
    result = analysis_manager.simple_forecast(input_df.copy(), Granularity.MONTH, forecast_till_date=date(2026, 7, 1))

    # Assert
    assert len(result) == expected_count
    assert result[0]["date"] == date(2025, 8, 1)
    assert result[-1]["date"] == date(2026, 7, 1)
    assert result[0]["value"] == pytest.approx(12675, 100)
    assert result[-1]["value"] == pytest.approx(12415, 100)
    assert "confidence_interval" in result[0]
    assert "confidence_interval" in result[-1]

    # Act
    result = analysis_manager.simple_forecast(
        input_df.copy(), Granularity.MONTH, forecast_till_date=pd.Timestamp("2026-07-31"), conf_interval=90
    )

    # Assert
    assert len(result) == expected_count
    assert result[0]["value"] == pytest.approx(12675, 100)
    assert result[-1]["value"] == pytest.approx(12415, 100)


def test_analysis_manager_simple_forecast_forecast_horizon(input_df):
    # Prepare
    analysis_manager = AnalysisManager()
    expected_count = 5

    # Act
    result = analysis_manager.simple_forecast(input_df.copy(), Granularity.MONTH, forecast_horizon=5)

    # Assert
    assert len(result) == expected_count
    assert result[0]["date"] == date(2025, 8, 1)
    assert result[-1]["date"] == date(2025, 12, 1)
    assert result[0]["value"] == pytest.approx(12675, 100)
    assert result[-1]["value"] == pytest.approx(12415, 100)
    assert "confidence_interval" in result[0]


def test_analysis_manager_simple_forecast_error(input_df):
    # Prepare
    analysis_manager = AnalysisManager()

    # Act
    with pytest.raises(ValueError):
        analysis_manager.simple_forecast(input_df.copy(), Granularity.MONTH)
