from datetime import date
from unittest.mock import AsyncMock

import pytest

from commons.clients.analysis_manager import AnalysisManagerClient
from commons.models.enums import Granularity


@pytest.mark.asyncio
async def test_perform_process_control(mocker):
    # Arrange
    metric_id = "test_metric"
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 31)
    grain = Granularity.DAY
    expected_results = [{"date": "2023-01-01", "value": 100}, {"date": "2023-01-02", "value": 200}]

    mock_post = AsyncMock(return_value=expected_results)
    client = AnalysisManagerClient(base_url="https://example.com")
    mocker.patch.object(client, "post", mock_post)

    # Act
    results = await client.perform_process_control(metric_id, start_date, end_date, grain)

    # Assert
    assert results == expected_results
    mock_post.assert_called_once_with(
        endpoint="analyze/process-control",
        data={
            "metric_id": metric_id,
            "start_date": start_date,
            "end_date": end_date,
            "grain": grain.value,
        },
    )


@pytest.mark.asyncio
async def test_perform_process_control_empty_response(mocker):
    # Arrange
    metric_id = "test_metric"
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 31)
    grain = Granularity.DAY

    mock_post = AsyncMock(return_value=None)

    client = AnalysisManagerClient(base_url="https://example.com")
    mocker.patch.object(client, "post", mock_post)

    # Act
    result = await client.perform_process_control(metric_id, start_date, end_date, grain)

    # Assert
    assert result is None
    mock_post.assert_called_once_with(
        endpoint="analyze/process-control",
        data={
            "metric_id": metric_id,
            "start_date": start_date,
            "end_date": end_date,
            "grain": grain.value,
        },
    )
