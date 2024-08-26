import random
from unittest import mock
from unittest.mock import AsyncMock

import numpy as np
import pandas as pd
import pytest
from deepdiff import DeepDiff

from analysis_manager.core.services.component_drift import ComponentDriftService

# from analysis_manager.core.services.leverage import LeverageIdService
from commons.clients.query_manager import QueryManagerClient
from fulcrum_core.modules import SegmentDriftEvaluator


class MockSecurity:
    def __init__(self, *args, **kwargs):
        self.dependency = lambda: True
        self.use_cache = False


mock.patch("fastapi.Security", MockSecurity).start()


@pytest.mark.skip
def test_health(client):
    response = client.get("/v1/health")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_describe(client, mocker, metric_values, metric_sms):
    # Mock the QueryClient's list_metrics method
    values = AsyncMock(return_value=metric_values)
    metric_mock = AsyncMock(return_value=metric_sms)
    mocker.patch.object(QueryManagerClient, "get_metric", metric_mock)
    mocker.patch.object(QueryManagerClient, "get_metric_time_series", values)
    response = client.post(
        "/v1/analyze/describe",
        json={
            "metric_id": "CAC",
            "start_date": "2024-01-01",
            "end_date": "2024-04-30",
            "grain": "day",
            "dimensions": [
                {"dimension": "customer_segment", "members": ["Enterprise", "SMB"]},
                {"dimension": "channel", "members": []},
            ],
        },
    )
    expected_response = [
        {
            "metric_id": "CAC",
            "dimension": "customer_segment",
            "member": "Enterprise",
            "mean": 24745.0,
            "median": 24745.0,
            "variance": None,
            "percentile_25": 24745.0,
            "percentile_50": 24745.0,
            "percentile_75": 24745.0,
            "percentile_90": 24745.0,
            "percentile_95": 24745.0,
            "percentile_99": 24745.0,
            "min": 24745.0,
            "max": 24745.0,
            "count": 1,
            "sum": 24745.0,
            "unique": 1,
        },
        {
            "metric_id": "CAC",
            "dimension": "channel",
            "member": "Online",
            "mean": 10053.0,
            "median": 10053.0,
            "variance": None,
            "percentile_25": 10053.0,
            "percentile_50": 10053.0,
            "percentile_75": 10053.0,
            "percentile_90": 10053.0,
            "percentile_95": 10053.0,
            "percentile_99": 10053.0,
            "min": 10053.0,
            "max": 10053.0,
            "count": 1,
            "sum": 10053.0,
            "unique": 1,
        },
        {
            "metric_id": "CAC",
            "dimension": "channel",
            "member": "Retail",
            "mean": 8422.0,
            "median": 8422.0,
            "variance": None,
            "percentile_25": 8422.0,
            "percentile_50": 8422.0,
            "percentile_75": 8422.0,
            "percentile_90": 8422.0,
            "percentile_95": 8422.0,
            "percentile_99": 8422.0,
            "min": 8422.0,
            "max": 8422.0,
            "count": 1,
            "sum": 8422.0,
            "unique": 1,
        },
        {
            "metric_id": "CAC",
            "dimension": "channel",
            "member": "Wholesale",
            "mean": 6270.0,
            "median": 6270.0,
            "variance": None,
            "percentile_25": 6270.0,
            "percentile_50": 6270.0,
            "percentile_75": 6270.0,
            "percentile_90": 6270.0,
            "percentile_95": 6270.0,
            "percentile_99": 6270.0,
            "min": 6270.0,
            "max": 6270.0,
            "count": 1,
            "sum": 6270.0,
            "unique": 1,
        },
    ]
    assert DeepDiff(response.json(), expected_response, ignore_order=True) == {}


@pytest.mark.asyncio
async def test_correlate(client, mocker, metric_values_correlate):
    # Mock the QueryClient's list_metrics method

    mock_list_metrics = AsyncMock(return_value=metric_values_correlate)
    mocker.patch.object(QueryManagerClient, "get_metrics_time_series", mock_list_metrics)
    response = client.post(
        "/v1/analyze/correlate",
        json={"metric_ids": ["NewMRR", "CAC"], "start_date": "2024-01-01", "end_date": "2024-04-30", "grain": "day"},
    )
    expected_response_dict = [{"correlation_coefficient": 0.516, "metric_id_1": "CAC", "metric_id_2": "NewMRR"}]

    assert response.status_code == 200
    assert DeepDiff(response.json(), expected_response_dict, ignore_order=True) == {}


@pytest.mark.asyncio
async def test_process_control_route(client, mocker, metric_values_netmrr):
    values_df = pd.DataFrame(metric_values_netmrr[:18])
    mock_list_metrics = AsyncMock(return_value=values_df)
    mocker.patch.object(QueryManagerClient, "get_metric_time_series_df", mock_list_metrics)
    response = client.post(
        "/v1/analyze/process-control",
        json={"metric_id": "NewMRR", "start_date": "2022-09-01", "end_date": "2022-12-30", "grain": "quarter"},
    )
    expected_response = [
        {
            "central_line": 56599.815,
            "date": "2022-09-01",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": None,
            "trend_signal_detected": False,
            "ucl": 122193.819,
            "value": 50927.0,
        },
        {
            "central_line": 56293.0,
            "date": "2022-10-01",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 121887.004,
            "value": 40294.0,
        },
        {
            "central_line": 55986.185,
            "date": "2022-10-31",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 121580.189,
            "value": 67557.0,
        },
        {
            "central_line": 55679.37,
            "date": "2022-11-30",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 121273.374,
            "value": 74216.0,
        },
        {
            "central_line": 55372.556,
            "date": "2022-12-30",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 120966.56,
            "value": 58084.0,
        },
        {
            "central_line": 55065.741,
            "date": "2023-01-29",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 120659.745,
            "value": 39168.0,
        },
        {
            "central_line": 54758.926,
            "date": "2023-02-28",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 120352.93,
            "value": 20033.0,
        },
        {
            "central_line": 54452.111,
            "date": "2023-03-30",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 120046.115,
            "value": 61401.0,
        },
        {
            "central_line": 54145.296,
            "date": "2023-04-29",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 119739.3,
            "value": 86673.0,
        },
        {
            "central_line": 53838.481,
            "date": "2023-05-29",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 119432.485,
            "value": 26451.0,
        },
        {
            "central_line": 53531.667,
            "date": "2023-06-28",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 119125.671,
            "value": 47445.0,
        },
        {
            "central_line": 53224.852,
            "date": "2023-07-28",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 118818.856,
            "value": 63417.0,
        },
        {
            "central_line": 52918.037,
            "date": "2023-08-27",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 118512.041,
            "value": 63514.0,
        },
        {
            "central_line": 52611.222,
            "date": "2023-09-26",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 118205.226,
            "value": 64206.0,
        },
        {
            "central_line": 52304.407,
            "date": "2023-10-26",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 117898.411,
            "value": 78596.0,
        },
        {
            "central_line": 51997.593,
            "date": "2023-11-25",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 117591.597,
            "value": 21955.0,
        },
        {
            "central_line": 51690.778,
            "date": "2023-12-25",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 117284.782,
            "value": 30849.0,
        },
        {
            "central_line": 51383.963,
            "date": "2024-01-24",
            "lcl": 0.0,
            "slope": -306.815,
            "slope_change": 0.0,
            "trend_signal_detected": False,
            "ucl": 116977.967,
            "value": 77068.0,
        },
    ]
    diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert diff == {}


@pytest.mark.asyncio
async def test_process_control_route_insufficient_data(client, mocker, metric_values_netmrr):
    values_df = pd.DataFrame(metric_values_netmrr[:5])
    mock_list_metrics = AsyncMock(return_value=values_df)
    mocker.patch.object(QueryManagerClient, "get_metric_time_series_df", mock_list_metrics)
    response = client.post(
        "/v1/analyze/process-control",
        json={"metric_id": "NewMRR", "start_date": "2022-09-01", "end_date": "2022-12-30", "grain": "quarter"},
    )
    assert response.status_code == 400
    # case 2
    mocker.patch.object(QueryManagerClient, "get_metric_time_series_df", AsyncMock(return_value=pd.DataFrame()))
    response = client.post(
        "/v1/analyze/process-control",
        json={"metric_id": "NewMRR", "start_date": "2022-09-01", "end_date": "2022-12-30", "grain": "quarter"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_component_drift_route(client, mocker, metric_cac, metric_list, mock_drift_resp):
    mock_get_metric = AsyncMock(return_value=metric_cac)
    mock_list_metrics = AsyncMock(return_value=metric_list)
    mocker.patch.object(QueryManagerClient, "get_metric", mock_get_metric)
    mocker.patch.object(
        QueryManagerClient, "get_metric_value", side_effect=lambda *args: {"value": random.randint(100, 200)}  # noqa
    )
    mocker.patch.object(QueryManagerClient, "list_metrics", mock_list_metrics)
    mock_calculate_drift = AsyncMock(return_value=mock_drift_resp)
    mocker.patch.object(ComponentDriftService, "calculate_drift", mock_calculate_drift)

    response = client.post(
        "/v1/analyze/drift/component",
        json={
            "metric_id": "CAC",
            "evaluation_start_date": "2024-04-01",
            "evaluation_end_date": "2024-05-01",
            "comparison_start_date": "2024-03-01",
            "comparison_end_date": "2024-04-01",
        },
    )

    # assert
    assert response.status_code == 200
    assert response.json()["drift"] is not None
    assert len(response.json()["components"]) == 2
    assert response.json()["components"][0]["metric_id"] == "SalesMktSpend"
    assert response.json()["components"][0]["drift"] is not None
    assert len(response.json()["components"][0]["components"]) == 2


@pytest.mark.asyncio
async def test_simple_forecast_route(client, mocker):
    # Prepare
    input_df = pd.DataFrame(
        {
            "date": pd.date_range("2022-01-01", periods=20, freq="MS"),
            "value": random.sample(range(10000, 12000), 20),
        }
    )
    mock_get_metric_time_series_df = AsyncMock(return_value=input_df)
    mocker.patch.object(QueryManagerClient, "get_metric_time_series_df", mock_get_metric_time_series_df)

    # Act
    response = client.post(
        "/v1/analyze/forecast/simple",
        json={
            "metric_id": "NewMRR",
            "start_date": "2022-01-01",
            "end_date": "2022-12-31",
            "grain": "month",
            "forecast_horizon": 6,
            "confidence_interval": 95,
        },
    )

    # Assert
    response_data = response.json()
    assert response.status_code == 200
    assert len(response_data) == 6
    assert [res["date"] for res in response_data] == [
        "2023-09-01",
        "2023-10-01",
        "2023-11-01",
        "2023-12-01",
        "2024-01-01",
        "2024-02-01",
    ]
    # value is not none
    assert all(res["value"] is not None for res in response_data)

    # Prepare with forecast_till_date
    input_df = pd.DataFrame(
        {
            "date": pd.date_range("2022-05-01", periods=20, freq="MS"),
            "value": random.sample(range(10000, 12000), 20),
        }
    )
    mock_get_metric_time_series_df = AsyncMock(return_value=input_df)
    mocker.patch.object(QueryManagerClient, "get_metric_time_series_df", mock_get_metric_time_series_df)

    # Act
    response = client.post(
        "/v1/analyze/forecast/simple",
        json={
            "metric_id": "NewMRR",
            "start_date": "2022-01-01",
            "end_date": "2024-01-01",
            "grain": "month",
            "forecast_till_date": "2024-03-01",
            "confidence_interval": 95,
        },
    )

    # Assert
    response_data = response.json()

    assert response.status_code == 200
    assert len(response_data) == 3
    assert [res["date"] for res in response_data] == ["2024-01-01", "2024-02-01", "2024-03-01"]


@pytest.mark.asyncio
async def test_simple_forecast_route_bad_request(client, mocker):
    # Prepare
    input_df = pd.DataFrame(
        {
            "date": pd.date_range("2022-01-01", periods=20, freq="MS"),
            "value": random.sample(range(10000, 12000), 20),
        }
    )
    mock_get_metric_time_series_df = AsyncMock(return_value=input_df)
    mocker.patch.object(QueryManagerClient, "get_metric_time_series_df", mock_get_metric_time_series_df)

    # Act
    response = client.post(
        "/v1/analyze/forecast/simple",
        json={"metric_id": "NewMRR", "start_date": "2022-01-01", "end_date": "2022-12-31", "grain": "month"},
    )

    # Assert
    assert response.status_code == 400

    # Prepare for insufficient data
    input_df = pd.DataFrame(
        {
            "date": pd.date_range("2022-01-01", periods=5, freq="MS"),
            "value": random.sample(range(10000, 12000), 5),
        }
    )
    mock_get_metric_time_series_df = AsyncMock(return_value=input_df)
    mocker.patch.object(QueryManagerClient, "get_metric_time_series_df", mock_get_metric_time_series_df)

    # Act
    response = client.post(
        "/v1/analyze/forecast/simple",
        json={
            "metric_id": "NewMRR",
            "start_date": "2022-01-01",
            "end_date": "2022-12-31",
            "grain": "month",
            "forecast_horizon": 6,
        },
    )

    # Assert
    assert response.status_code == 400

    # Prepare no data
    input_df = pd.DataFrame()
    mock_get_metric_time_series_df = AsyncMock(return_value=input_df)
    mocker.patch.object(QueryManagerClient, "get_metric_time_series_df", mock_get_metric_time_series_df)

    # Act
    response = client.post(
        "/v1/analyze/forecast/simple",
        json={
            "metric_id": "NewMRR",
            "start_date": "2022-01-01",
            "end_date": "2022-12-31",
            "grain": "month",
            "forecast_horizon": 6,
        },
    )

    # Assert
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_segment_drift(
    mock_get_metric_time_series, client, dsensei_csv_file_id, get_insight_response, mocker, segment_drift_output
):
    mock_file_id = AsyncMock(return_value=dsensei_csv_file_id)
    mocker.patch.object(SegmentDriftEvaluator, "send_file_to_dsensei", mock_file_id)

    mock_insight_response = AsyncMock(return_value=get_insight_response)
    mocker.patch.object(SegmentDriftEvaluator, "get_insights", mock_insight_response)

    response = client.post(
        "/v1/analyze/drift/segment",
        json={
            "metric_id": "NewBizDeals",
            "evaluation_start_date": "2025-03-01",
            "evaluation_end_date": "2025-03-30",
            "comparison_start_date": "2024-03-01",
            "comparison_end_date": "2024-03-30",
            "dimensions": ["region", "stage_name"],
        },
    )
    assert response.status_code == 200
    assert sorted(response.json()) == sorted(segment_drift_output)


@pytest.mark.skip(reason="Skipping due to flakiness")
@pytest.mark.asyncio
async def test_influence_attribution_success(client, mocker, metric_sms):
    # Mock the QueryClient's methods
    metric_mock = AsyncMock(return_value=metric_sms)
    get_metric_time_series_df_mock = AsyncMock(
        side_effect=[
            pd.DataFrame(
                {
                    "date": pd.date_range("2024-01-01", periods=10, freq="D"),
                    "metric_id": ["SalesMktSpend"] * 10,
                    "value": np.random.randint(1000, 1200, 10),
                }
            ),
            pd.DataFrame(
                {
                    "date": pd.date_range("2024-01-01", periods=10, freq="D"),
                    "metric_id": ["influenced1"] * 10,
                    "value": np.random.randint(600, 800, 10),
                }
            ),
            pd.DataFrame(
                {
                    "date": pd.date_range("2024-01-01", periods=10, freq="D"),
                    "metric_id": ["influenced2"] * 10,
                    "value": np.random.randint(400, 600, 10),
                }
            ),
        ]
    )
    get_metrics_value_mock = AsyncMock(
        side_effect=[
            {"SalesMktSpend": 1000, "influenced1": 500, "influenced2": 300},
            {"SalesMktSpend": 1200, "influenced1": 600, "influenced2": 400},
        ]
    )
    mocker.patch.object(QueryManagerClient, "get_metric", metric_mock)
    mocker.patch.object(QueryManagerClient, "get_metric_time_series_df", get_metric_time_series_df_mock)
    mocker.patch.object(QueryManagerClient, "get_metrics_value", get_metrics_value_mock)

    response = client.post(
        "/v1/analyze/influence-attribution",
        json={
            "metric_id": "SalesMktSpend",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "grain": "day",
            "evaluation_start_date": "2024-01-01",
            "evaluation_end_date": "2024-01-31",
            "comparison_start_date": "2024-02-01",
            "comparison_end_date": "2024-02-29",
        },
    )
    assert response.status_code == 200
    assert "expression" in response.json()
    assert "influence" in response.json()


@pytest.mark.asyncio
async def test_influence_attribution_no_influenced_by(client, mocker, metric_sms):
    # Mock the QueryClient's methods
    metric_sms_copy = metric_sms.copy()
    metric_sms_copy["influenced_by"] = []
    metric_mock = AsyncMock(return_value=metric_sms_copy)
    mocker.patch.object(QueryManagerClient, "get_metric", metric_mock)

    # Act
    response = client.post(
        "/v1/analyze/influence-attribution",
        json={
            "metric_id": "SalesMktSpend",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "grain": "day",
            "evaluation_start_date": "2024-01-01",
            "evaluation_end_date": "2024-01-31",
            "comparison_start_date": "2024-02-01",
            "comparison_end_date": "2024-02-29",
        },
    )

    # Assert
    assert response.status_code == 400
    assert response.json()["detail"] == "Metric is not influenced by any other metric."


@pytest.mark.asyncio
async def test_leverage_id_route(client, mocker, leverage_id_response, metric_expression, get_metric_response):
    # Mock the QueryManagerClient methods
    mocker.patch.object(QueryManagerClient, "get_metric", new_callable=AsyncMock, return_value=get_metric_response)
    mocker.patch.object(
        QueryManagerClient,
        "get_expressions",
        new_callable=AsyncMock,
        return_value=(metric_expression, ["NewBizDeals", "AcceptOpps", "OpenNewBizOpps", "SQORate", "SQOToWinRate"]),
    )
    mocker.patch.object(
        QueryManagerClient,
        "get_metrics_max_values",
        new_callable=AsyncMock,
        return_value={"NewBizDeals": 100, "AcceptOpps": 80, "OpenNewBizOpps": 60, "SQORate": 50, "SQOToWinRate": 40},
    )
    mocker.patch.object(
        QueryManagerClient,
        "get_metrics_time_series_df",
        new_callable=AsyncMock,
        return_value=pd.DataFrame(
            {
                "date": pd.date_range("2024-02-01", periods=5, freq="MS"),
                "NewBizDeals": [100, 200, 300, 400, 500],
                "AcceptOpps": [80, 160, 240, 320, 400],
                "OpenNewBizOpps": [60, 120, 180, 240, 300],
                "SQORate": [50, 100, 150, 200, 250],
                "SQOToWinRate": [40, 80, 120, 160, 200],
            }
        ),
    )

    # Mock LeverageIdCalculator
    mock_leverage_calculator = mocker.Mock()
    mock_leverage_calculator.run.return_value = leverage_id_response
    mocker.patch("analysis_manager.core.routes.LeverageIdCalculator", return_value=mock_leverage_calculator)

    response = client.post(
        "/v1/analyze/analysis/leverage_id",
        json={"metric_id": "NewBizDeals", "start_date": "2024-02-01", "end_date": "2024-03-01", "grain": "month"},
    )

    assert response.status_code == 200
    assert response.json() == leverage_id_response


@pytest.mark.asyncio
async def test_leverage_id_route_missing_expression(client, mocker):
    # Mock the QueryManagerClient.get_metric to return a metric without an expression
    mocker.patch.object(
        QueryManagerClient, "get_metric", new_callable=AsyncMock, return_value={"metric_id": "NewBizDeals"}
    )

    # with pytest.raises(HTTPException) as exc_info:
    response = client.post(
        "/v1/analyze/analysis/leverage_id",
        json={"metric_id": "NewBizDeals", "start_date": "2024-02-01", "end_date": "2024-03-01", "grain": "month"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Metric expression not found for metric_id: NewBizDeals"
