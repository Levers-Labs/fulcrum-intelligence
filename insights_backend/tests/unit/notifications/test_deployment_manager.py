from unittest.mock import AsyncMock

import pytest

from insights_backend.notifications.enums import Comparisons
from insights_backend.notifications.models import Report, ReportConfig, ScheduleConfig
from insights_backend.notifications.services.deployment_manager import PrefectDeploymentManager


@pytest.fixture
def deployment_manager(mocker):
    mock_prefect_client = AsyncMock()
    mocker.patch(
        "insights_backend.notifications.services.deployment_manager.PrefectClient", return_value=mock_prefect_client
    )
    manager = PrefectDeploymentManager()
    return manager


@pytest.fixture
def sample_report():
    report = Report(
        id=123,
        tenant_id=2,
        is_active=True,
        schedule=ScheduleConfig(minute="0", hour="0", day_of_month="*", month="*", day_of_week="*", timezone="UTC"),
        config=ReportConfig(metric_ids=["metric-123", "metric-456"], comparisons=[Comparisons.PERCENTAGE_CHANGE]),
    )
    return report


@pytest.mark.asyncio
async def test_create_deployment_success(deployment_manager, sample_report):
    # Arrange
    expected_deployment_id = "deployment-123"
    deployment_manager.prefect.create_deployment.return_value = {"id": expected_deployment_id}

    # Act
    result = await deployment_manager.create_deployment(sample_report)

    # Assert
    assert result == expected_deployment_id
    deployment_manager.prefect.create_deployment.assert_called_once()
    call_args = deployment_manager.prefect.create_deployment.call_args[0][0]
    assert call_args.name == "metric-reports-id-123"
    assert call_args.flow_name == "metric-reports"
    assert call_args.parameters == {"tenant_id": 2, "report_id": 123}
    assert call_args.schedules[0].schedule.cron == "0 0 * * *"
    assert call_args.schedules[0].schedule.timezone == "UTC"
    assert call_args.schedules[0].active


@pytest.mark.asyncio
async def test_create_deployment_failure(deployment_manager, sample_report):
    # Arrange
    deployment_manager.prefect.create_deployment.side_effect = Exception("Failed to create deployment")

    # Act
    result = await deployment_manager.create_deployment(sample_report)

    # Assert
    assert result is None
    deployment_manager.prefect.create_deployment.assert_called_once()


@pytest.mark.asyncio
async def test_create_deployment_without_schedule(deployment_manager):
    # Arrange
    report = Report(
        id=123,
        tenant_id=2,
        is_active=True,
        schedule=None,
        config=ReportConfig(metric_ids=["metric-123", "metric-456"], comparisons=[Comparisons.PERCENTAGE_CHANGE]),
    )
    # No schedule attribute
    expected_deployment_id = "deployment-123"
    deployment_manager.prefect.create_deployment.return_value = {"id": expected_deployment_id}

    # Act
    result = await deployment_manager.create_deployment(report)

    # Assert
    assert result == expected_deployment_id
    deployment_manager.prefect.create_deployment.assert_called_once()
    call_args = deployment_manager.prefect.create_deployment.call_args[0][0]
    assert call_args.schedules is None


@pytest.mark.asyncio
async def test_delete_deployment_success(deployment_manager, sample_report):
    # Arrange
    deployment_id = "deployment-123"
    deployment_manager.prefect.delete_deployment.return_value = None

    # Act
    result = await deployment_manager.delete_deployment(deployment_id)

    # Assert
    assert result is True
    deployment_manager.prefect.delete_deployment.assert_called_once_with(deployment_id)


@pytest.mark.asyncio
async def test_delete_deployment_failure(deployment_manager, sample_report):
    # Arrange
    deployment_id = "deployment-123"
    deployment_manager.prefect.delete_deployment.side_effect = Exception("Failed to delete deployment")

    # Act
    result = await deployment_manager.delete_deployment(deployment_id)

    # Assert
    assert result is False
    deployment_manager.prefect.delete_deployment.assert_called_once_with(deployment_id)
