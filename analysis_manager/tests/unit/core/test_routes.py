import random
from unittest.mock import AsyncMock

import pytest
from deepdiff import DeepDiff

from analysis_manager.services.query_manager_client import QueryManagerClient


@pytest.mark.skip
def test_health(client):
    response = client.get("/v1/health")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_describe(client, mocker, metric_values):
    # Mock the QueryClient's list_metrics method
    values = AsyncMock(return_value=metric_values)
    mocker.patch.object(QueryManagerClient, "get_metric_values", values)
    response = client.post(
        "/v1/analyze/describe",
        json={
            "metric_id": "CAC",
            "start_date": "2024-01-01",
            "end_date": "2024-04-30",
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
    mocker.patch.object(QueryManagerClient, "get_metric_values", mock_list_metrics)
    response = client.post(
        "/v1/analyze/correlate",
        json={"metric_ids": ["NewMRR", "CAC"], "start_date": "2024-01-01", "end_date": "2024-04-30"},
    )
    expected_response_dict = [
        {"correlation_coefficient": 0.5155347459793249, "metric_id_1": "CAC", "metric_id_2": "NewMRR"}
    ]

    assert response.status_code == 200
    assert DeepDiff(response.json(), expected_response_dict, ignore_order=True) == {}


@pytest.mark.asyncio
async def test_process_control_route(client, mocker, metric_values_netmrr):

    mock_list_metrics = AsyncMock(return_value=metric_values_netmrr)
    mocker.patch.object(QueryManagerClient, "get_metric_values", mock_list_metrics)
    response = client.post(
        "/v1/analyze/process-control",
        json={"metric_id": "NewMRR", "start_date": "2022-09-01", "end_date": "2022-12-30", "grains": ["quarter"]},
    )
    expected_response = [
        {
            "metric_id": "NewMRR",
            "start_date": "2022-09-01",
            "end_date": "2022-12-30",
            "grain": "quarter",
            "results": [
                {
                    "date": "2022-09-01",
                    "metric_value": 50927,
                    "central_line": 56599.81481481482,
                    "ucl": 122193.81881481482,
                    "lcl": 0,
                },
                {"date": "2022-10-01", "metric_value": 40294, "central_line": 56293, "ucl": 121887.004, "lcl": 0},
                {
                    "date": "2022-10-31",
                    "metric_value": 67557,
                    "central_line": 55986.18518518518,
                    "ucl": 121580.18918518518,
                    "lcl": 0,
                },
                {
                    "date": "2022-11-30",
                    "metric_value": 74216,
                    "central_line": 55679.370370370365,
                    "ucl": 121273.37437037037,
                    "lcl": 0,
                },
                {
                    "date": "2022-12-30",
                    "metric_value": 58084,
                    "central_line": 55372.55555555555,
                    "ucl": 120966.55955555555,
                    "lcl": 0,
                },
                {
                    "date": "2023-01-29",
                    "metric_value": 39168,
                    "central_line": 55065.74074074073,
                    "ucl": 120659.74474074073,
                    "lcl": 0,
                },
                {
                    "date": "2023-02-28",
                    "metric_value": 20033,
                    "central_line": 54758.92592592591,
                    "ucl": 120352.92992592591,
                    "lcl": 0,
                },
                {
                    "date": "2023-03-30",
                    "metric_value": 61401,
                    "central_line": 54452.111111111095,
                    "ucl": 120046.1151111111,
                    "lcl": 0,
                },
                {
                    "date": "2023-04-29",
                    "metric_value": 86673,
                    "central_line": 54145.29629629628,
                    "ucl": 119739.30029629628,
                    "lcl": 0,
                },
                {
                    "date": "2023-05-29",
                    "metric_value": 26451,
                    "central_line": 53838.48148148146,
                    "ucl": 119432.48548148146,
                    "lcl": 0,
                },
                {
                    "date": "2023-06-28",
                    "metric_value": 47445,
                    "central_line": 53531.66666666664,
                    "ucl": 119125.67066666664,
                    "lcl": 0,
                },
                {
                    "date": "2023-07-28",
                    "metric_value": 63417,
                    "central_line": 53224.851851851825,
                    "ucl": 118818.85585185183,
                    "lcl": 0,
                },
                {
                    "date": "2023-08-27",
                    "metric_value": 63514,
                    "central_line": 52918.03703703701,
                    "ucl": 118512.04103703701,
                    "lcl": 0,
                },
                {
                    "date": "2023-09-26",
                    "metric_value": 64206,
                    "central_line": 52611.22222222219,
                    "ucl": 118205.22622222219,
                    "lcl": 0,
                },
                {
                    "date": "2023-10-26",
                    "metric_value": 78596,
                    "central_line": 52304.40740740737,
                    "ucl": 117898.41140740737,
                    "lcl": 0,
                },
                {
                    "date": "2023-11-25",
                    "metric_value": 21955,
                    "central_line": 51997.592592592555,
                    "ucl": 117591.59659259256,
                    "lcl": 0,
                },
                {
                    "date": "2023-12-25",
                    "metric_value": 30849,
                    "central_line": 51690.77777777774,
                    "ucl": 117284.78177777774,
                    "lcl": 0,
                },
                {
                    "date": "2024-01-24",
                    "metric_value": 77068,
                    "central_line": 51383.96296296292,
                    "ucl": 116977.96696296292,
                    "lcl": 0,
                },
                {
                    "date": "2024-02-23",
                    "metric_value": 95685,
                    "central_line": 51077.1481481481,
                    "ucl": 116671.1521481481,
                    "lcl": 0,
                },
                {
                    "date": "2024-03-24",
                    "metric_value": 62233,
                    "central_line": 50770.333333333285,
                    "ucl": 116364.33733333329,
                    "lcl": 0,
                },
                {
                    "date": "2024-04-23",
                    "metric_value": 42173,
                    "central_line": 50463.51851851847,
                    "ucl": 116057.52251851847,
                    "lcl": 0,
                },
                {
                    "date": "2024-05-23",
                    "metric_value": 18191,
                    "central_line": 50156.70370370365,
                    "ucl": 115750.70770370365,
                    "lcl": 0,
                },
                {
                    "date": "2024-06-22",
                    "metric_value": 95453,
                    "central_line": 49849.88888888883,
                    "ucl": 115443.89288888883,
                    "lcl": 0,
                },
                {
                    "date": "2024-07-22",
                    "metric_value": 46417,
                    "central_line": 49543.074074074015,
                    "ucl": 115137.07807407402,
                    "lcl": 0,
                },
                {
                    "date": "2024-08-21",
                    "metric_value": 69306,
                    "central_line": 49236.2592592592,
                    "ucl": 114830.2632592592,
                    "lcl": 0,
                },
                {
                    "date": "2024-09-20",
                    "metric_value": 76026,
                    "central_line": 48929.44444444438,
                    "ucl": 114523.44844444438,
                    "lcl": 0,
                },
                {
                    "date": "2024-10-20",
                    "metric_value": 43884,
                    "central_line": 48622.62962962956,
                    "ucl": 114216.63362962956,
                    "lcl": 0,
                },
                {
                    "date": "2024-11-19",
                    "metric_value": 11489,
                    "central_line": 48315.814814814745,
                    "ucl": 113909.81881481475,
                    "lcl": 0,
                },
                {
                    "date": "2024-12-19",
                    "metric_value": 99466,
                    "central_line": 48008.99999999993,
                    "ucl": 113603.00399999993,
                    "lcl": 0,
                },
                {
                    "date": "2025-01-18",
                    "metric_value": 78883,
                    "central_line": 47702.18518518511,
                    "ucl": 113296.18918518511,
                    "lcl": 0,
                },
                {
                    "date": "2025-02-17",
                    "metric_value": 76167,
                    "central_line": 47395.37037037029,
                    "ucl": 112989.3743703703,
                    "lcl": 0,
                },
                {
                    "date": "2025-03-19",
                    "metric_value": 95945,
                    "central_line": 47088.555555555475,
                    "ucl": 112682.55955555548,
                    "lcl": 0,
                },
                {
                    "date": "2025-04-18",
                    "metric_value": 97288,
                    "central_line": 46781.74074074066,
                    "ucl": 112375.74474074066,
                    "lcl": 0,
                },
                {
                    "date": "2025-05-18",
                    "metric_value": 79936,
                    "central_line": 46474.92592592584,
                    "ucl": 112068.92992592584,
                    "lcl": 0,
                },
                {
                    "date": "2025-06-17",
                    "metric_value": 33299,
                    "central_line": 46168.11111111102,
                    "ucl": 111762.11511111102,
                    "lcl": 0,
                },
            ],
        }
    ]
    diff = DeepDiff(expected_response, response.json(), ignore_order=True)
    assert diff == {}


@pytest.mark.asyncio
async def test_component_drift_route(client, mocker, metric_cac, metric_list):

    mock_get_metric_details = AsyncMock(return_value=metric_cac)
    mock_get_metrics_details = AsyncMock(return_value=metric_list)
    mocker.patch.object(QueryManagerClient, "get_metric_details", mock_get_metric_details)
    mocker.patch.object(
        QueryManagerClient, "get_metric_value", side_effect=lambda *args: random.randint(100, 200)  # noqa
    )
    mocker.patch.object(QueryManagerClient, "get_metrics_details", mock_get_metrics_details)
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
    assert len(response.json()["components"]) == 3
    assert response.json()["components"][0]["metric_id"] == "SalesMktSpend"
    assert response.json()["components"][0]["drift"] is not None
    assert len(response.json()["components"][0]["components"]) == 2
