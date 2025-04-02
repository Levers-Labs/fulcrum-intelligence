from unittest.mock import Mock

import pytest

from commons.exceptions import PrefectOperationError
from insights_backend.notifications.enums import Comparisons, ScheduleLabel
from insights_backend.notifications.models import Report, ReportConfig, ScheduleConfig
from insights_backend.notifications.services.deployment_manager import PrefectDeploymentManager


@pytest.fixture
def deployment_manager(mocker):
    mock_prefect_client = Mock()
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
        schedule=ScheduleConfig(
            minute="0",
            hour="0",
            day_of_month="*",
            month="*",
            day_of_week="*",
            timezone="UTC",
            label=ScheduleLabel.DAY,
        ),
        config=ReportConfig(metric_ids=["metric-123", "metric-456"], comparisons=[Comparisons.PERCENTAGE_CHANGE]),
    )
    return report


def test_create_deployment_success(deployment_manager, sample_report):
    # Arrange
    expected_deployment_id = "deployment-123"
    deployment_manager.prefect.create_deployment.return_value = {"id": expected_deployment_id}

    # Act
    result = deployment_manager.create_deployment(sample_report)

    # Assert
    assert result == expected_deployment_id
    deployment_manager.prefect.create_deployment.assert_called_once()
    call_args = deployment_manager.prefect.create_deployment.call_args[0][0]
    assert call_args.name == "metric-reports-id-123"
    assert call_args.flow_name == "metric-reports"
    assert call_args.parameters == {"tenant_id": 2, "report_id": 123}


async def test_create_deployment_failure(mock_deployment_manager):
    """Test handling of deployment creation failure"""
    # Configure the mock to simulate a failure
    mock_deployment_manager.create_deployment.side_effect = Exception("Failed to create deployment")

    with pytest.raises(PrefectOperationError) as exc_info:
        await mock_deployment_manager.create_deployment(Mock(id=123))

    assert "Unable to create deployment for report 123" in str(exc_info.value)


def test_create_deployment_without_schedule(deployment_manager):
    # Arrange
    report = Report(
        id=123,
        tenant_id=2,
        is_active=True,
        schedule=None,
        config=ReportConfig(metric_ids=["metric-123", "metric-456"], comparisons=[Comparisons.PERCENTAGE_CHANGE]),
    )
    expected_deployment_id = "deployment-123"
    deployment_manager.prefect.create_deployment.return_value = {"id": expected_deployment_id}

    # Act
    result = deployment_manager.create_deployment(report)

    # Assert
    assert result == expected_deployment_id
    deployment_manager.prefect.create_deployment.assert_called_once()


def test_delete_deployment_success(deployment_manager):
    # Arrange
    deployment_id = "deployment-123"
    deployment_manager.prefect.delete_deployment.return_value = None

    # Act
    result = deployment_manager.delete_deployment(deployment_id)

    # Assert
    assert result is True
    deployment_manager.prefect.delete_deployment.assert_called_once_with(deployment_id)


def test_delete_deployment_failure(deployment_manager):
    # Arrange
    deployment_id = "deployment-123"
    deployment_manager.prefect.delete_deployment.side_effect = Exception("Failed to delete deployment")

    # Act
    result = deployment_manager.delete_deployment(deployment_id)

    # Assert
    assert result is False
    deployment_manager.prefect.delete_deployment.assert_called_once_with(deployment_id)


def test_read_deployment_schedules_success(deployment_manager):
    # Arrange
    deployment_id = "deployment-123"
    expected_schedules = [{"id": "schedule-1", "cron": "0 0 * * *"}]
    deployment_manager.prefect.read_deployment_schedules.return_value = expected_schedules

    # Act
    result = deployment_manager.read_deployment_schedules(deployment_id)

    # Assert
    assert result == expected_schedules
    deployment_manager.prefect.read_deployment_schedules.assert_called_once_with(deployment_id)


def test_read_deployment_schedules_failure(deployment_manager):
    # Arrange
    deployment_id = "deployment-123"
    deployment_manager.prefect.read_deployment_schedules.side_effect = Exception("Failed to read schedules")

    # Act
    result = deployment_manager.read_deployment_schedules(deployment_id)

    # Assert
    assert result == []
    deployment_manager.prefect.read_deployment_schedules.assert_called_once_with(deployment_id)


def test_update_deployment_schedule_success(deployment_manager):
    # Arrange
    deployment_id = "deployment-123"
    schedule_id = "schedule-1"
    schedule = {"cron": "0 0 * * *"}

    # Act
    result = deployment_manager.update_deployment_schedule(deployment_id, schedule_id, schedule)

    # Assert
    assert result is True
    deployment_manager.prefect.update_deployment_schedule.assert_called_once_with(deployment_id, schedule_id, schedule)


def test_update_deployment_schedule_failure(deployment_manager):
    # Arrange
    deployment_id = "deployment-123"
    schedule_id = "schedule-1"
    schedule = {"cron": "0 0 * * *"}
    deployment_manager.prefect.update_deployment_schedule.side_effect = Exception("Failed to update schedule")

    # Act
    result = deployment_manager.update_deployment_schedule(deployment_id, schedule_id, schedule)

    # Assert
    assert result is False
    deployment_manager.prefect.update_deployment_schedule.assert_called_once_with(deployment_id, schedule_id, schedule)
