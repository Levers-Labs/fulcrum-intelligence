import logging
from unittest.mock import MagicMock

import pytest

from insights_backend.notifications.enums import ScheduleLabel
from insights_backend.notifications.models import Report, ScheduleConfig
from insights_backend.notifications.services.deployment_manager import PrefectDeploymentManager
from insights_backend.notifications.subscribers.deployment_handlers import (
    handle_creation,
    handle_report_link_deletion,
    handle_report_status_change,
    handle_update,
)


@pytest.fixture
def mock_deployment_manager(mocker):
    """Mock the PrefectDeploymentManager."""
    mock_manager = MagicMock(spec=PrefectDeploymentManager)
    mocker.patch(
        "insights_backend.notifications.subscribers.deployment_handlers.PrefectDeploymentManager",
        return_value=mock_manager,
    )
    return mock_manager


@pytest.fixture
def sample_report():
    """Create a sample report for testing."""
    return Report(
        id=123,
        tenant_id=1,
        is_published=True,
        is_active=True,
        deployment_id=None,
        schedule=ScheduleConfig(
            minute="0",
            hour="0",
            day_of_month="*",
            month="*",
            day_of_week="*",
            timezone="UTC",
            label=ScheduleLabel.DAY,
        ),
    )


# Tests for handle_creation
def test_handle_creation_unpublished(mock_deployment_manager, sample_report, caplog):
    """Test creation handler when report is not published."""
    caplog.set_level(logging.DEBUG)
    sample_report.is_published = False

    handle_creation(None, sample_report)

    mock_deployment_manager.create_deployment.assert_not_called()
    assert f"Report {sample_report.id} is not published" in caplog.text


def test_handle_creation_existing_deployment(mock_deployment_manager, sample_report, caplog):
    """Test creation handler when report already has deployment."""
    caplog.set_level(logging.DEBUG)
    sample_report.deployment_id = "existing-id"

    handle_creation(None, sample_report)

    mock_deployment_manager.create_deployment.assert_not_called()
    assert f"Report {sample_report.id} already has deployment ID" in caplog.text


def test_handle_creation_success(mock_deployment_manager, sample_report, caplog):
    """Test successful creation of deployment."""
    caplog.set_level(logging.INFO)
    expected_deployment_id = "new-deployment-123"
    mock_deployment_manager.create_deployment.return_value = expected_deployment_id

    handle_creation(None, sample_report)

    mock_deployment_manager.create_deployment.assert_called_once_with(sample_report)
    assert sample_report.deployment_id == expected_deployment_id
    assert "Created Prefect deployment" in caplog.text


# Tests for handle_update
def test_handle_update_unpublish(mock_deployment_manager, sample_report, caplog):
    """Test update handler when report is unpublished."""
    caplog.set_level(logging.INFO)
    sample_report.is_published = False
    history = {"deployment_id": {"old": "old-deployment-id"}, "is_published": {"old": True}}
    mock_deployment_manager.delete_deployment.return_value = True

    handle_update(None, sample_report, history)

    mock_deployment_manager.delete_deployment.assert_called_once_with("old-deployment-id")
    assert sample_report.deployment_id is None
    assert "Deleted Prefect deployment" in caplog.text


def test_handle_update_schedule_change(mock_deployment_manager, sample_report, caplog):
    """Test update handler when schedule changes."""
    caplog.set_level(logging.INFO)
    old_schedule = ScheduleConfig(
        minute="30", hour="*", day_of_month="*", month="*", day_of_week="*", timezone="UTC", label=ScheduleLabel.DAY
    )
    history = {"schedule": {"old": old_schedule}, "is_published": {"old": True}}
    expected_deployment_id = "new-deployment-123"
    mock_deployment_manager.create_deployment.return_value = expected_deployment_id

    handle_update(None, sample_report, history)

    mock_deployment_manager.create_deployment.assert_called_once_with(sample_report)
    assert sample_report.deployment_id == expected_deployment_id
    assert "Created new Prefect deployment" in caplog.text


def test_handle_update_newly_published(mock_deployment_manager, sample_report, caplog):
    """Test update handler when report becomes published."""
    caplog.set_level(logging.INFO)
    history = {"is_published": {"old": False}}
    expected_deployment_id = "new-deployment-123"
    mock_deployment_manager.create_deployment.return_value = expected_deployment_id

    handle_update(None, sample_report, history)

    mock_deployment_manager.create_deployment.assert_called_once_with(sample_report)
    assert sample_report.deployment_id == expected_deployment_id
    assert "Created new Prefect deployment" in caplog.text


def test_handle_update_no_changes(mock_deployment_manager, sample_report):
    """Test update handler when no relevant changes occur."""
    history = {"is_published": {"old": True}}

    handle_update(None, sample_report, history)

    mock_deployment_manager.create_deployment.assert_not_called()


# Tests for handle_report_status_change
def test_handle_report_status_change_success(mock_deployment_manager, sample_report, caplog):
    """Test successful status change handling."""
    caplog.set_level(logging.INFO)
    sample_report.deployment_id = "deployment-123"
    sample_report.is_active = True
    mock_deployment_manager.read_deployment_schedules.return_value = [{"id": "schedule-1", "active": False}]
    mock_deployment_manager.update_deployment_schedule.return_value = True

    handle_report_status_change(None, sample_report)

    mock_deployment_manager.update_deployment_schedule.assert_called_once_with(
        "deployment-123", "schedule-1", {"id": "schedule-1", "active": True}
    )
    assert "Enabled Prefect deployment schedule" in caplog.text


def test_handle_report_status_change_no_deployment(mock_deployment_manager, sample_report, caplog):
    """Test status change handling with no deployment."""
    caplog.set_level(logging.DEBUG)
    sample_report.deployment_id = None

    handle_report_status_change(None, sample_report)

    mock_deployment_manager.read_deployment_schedules.assert_not_called()
    assert "has no deployment ID" in caplog.text


def test_handle_report_status_change_no_schedules(mock_deployment_manager, sample_report, caplog):
    """Test status change handling with no schedules."""
    caplog.set_level(logging.ERROR)
    sample_report.deployment_id = "deployment-123"
    mock_deployment_manager.read_deployment_schedules.return_value = []

    handle_report_status_change(None, sample_report)

    mock_deployment_manager.update_deployment_schedule.assert_not_called()
    assert "No schedules found" in caplog.text


# Tests for handle_report_link_deletion
def test_handle_report_link_deletion_success(mock_deployment_manager, sample_report, caplog):
    """Test successful deletion handling."""
    caplog.set_level(logging.INFO)
    sample_report.deployment_id = "deployment-123"

    handle_report_link_deletion(None, sample_report)

    mock_deployment_manager.delete_deployment.assert_called_once_with("deployment-123")
    assert "Deleted Prefect deployment" in caplog.text


def test_handle_report_link_deletion_no_deployment(mock_deployment_manager, sample_report, caplog):
    """Test deletion handling with no deployment."""
    caplog.set_level(logging.DEBUG)
    sample_report.deployment_id = None

    handle_report_link_deletion(None, sample_report)

    mock_deployment_manager.delete_deployment.assert_not_called()
    assert "has no deployment ID" in caplog.text
