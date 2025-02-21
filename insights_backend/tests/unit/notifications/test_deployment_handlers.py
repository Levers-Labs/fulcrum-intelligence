import logging
from unittest.mock import AsyncMock

import pytest

from insights_backend.db.subscribers.deployment_handlers import handle_creation, handle_deletion, handle_update
from insights_backend.notifications.models import Report, ScheduleConfig
from insights_backend.notifications.services.deployment_manager import PrefectDeploymentManager


@pytest.fixture
def mock_deployment_manager(mocker):
    """Mock the PrefectDeploymentManager."""
    mock_manager = AsyncMock(spec=PrefectDeploymentManager)
    mocker.patch(
        "insights_backend.db.subscribers.deployment_handlers.PrefectDeploymentManager", return_value=mock_manager
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
        schedule=ScheduleConfig(minute="0", hour="0", day_of_month="*", month="*", day_of_week="*", timezone="UTC"),
    )


# Tests for handle_creation
@pytest.mark.asyncio
async def test_handle_creation_unpublished(mock_deployment_manager, sample_report, caplog):
    """Test creation handler when report is not published."""
    caplog.set_level(logging.DEBUG)
    sample_report.is_published = False

    await handle_creation(None, sample_report)

    mock_deployment_manager.return_value.create_deployment.assert_not_called()
    assert f"Report {sample_report.id} is not published" in caplog.text


@pytest.mark.asyncio
async def test_handle_creation_existing_deployment(mock_deployment_manager, sample_report, caplog):
    """Test creation handler when report already has deployment."""
    caplog.set_level(logging.DEBUG)
    sample_report.deployment_id = "existing-id"

    await handle_creation(None, sample_report)

    mock_deployment_manager.return_value.create_deployment.assert_not_called()
    assert f"Report {sample_report.id} already has deployment ID" in caplog.text


@pytest.mark.asyncio
async def test_handle_creation_success(mock_deployment_manager, sample_report, caplog):
    """Test successful creation of deployment."""
    caplog.set_level(logging.INFO)
    expected_deployment_id = "new-deployment-123"
    mock_deployment_manager.create_deployment.return_value = expected_deployment_id

    await handle_creation(None, sample_report)

    mock_deployment_manager.create_deployment.assert_called_once_with(sample_report)
    assert sample_report.deployment_id == expected_deployment_id
    assert "Created Prefect deployment" in caplog.text


# Tests for handle_update
@pytest.mark.asyncio
async def test_handle_update_unpublish(mock_deployment_manager, sample_report, caplog):
    """Test update handler when report is unpublished."""
    caplog.set_level(logging.INFO)
    sample_report.is_published = False
    history = {"deployment_id": {"old": "old-deployment-id"}, "is_published": {"old": True}}
    mock_deployment_manager.delete_deployment.return_value = True

    await handle_update(None, sample_report, history)

    mock_deployment_manager.delete_deployment.assert_called_once_with("old-deployment-id")
    assert sample_report.deployment_id is None
    assert "Deleted Prefect deployment" in caplog.text


@pytest.mark.asyncio
async def test_handle_update_schedule_change(mock_deployment_manager, sample_report, caplog):
    """Test update handler when schedule changes."""
    caplog.set_level(logging.INFO)
    old_schedule = ScheduleConfig(minute="30", hour="*", day_of_month="*", month="*", day_of_week="*", timezone="UTC")
    history = {"schedule": {"old": old_schedule}, "is_published": {"old": True}, "is_active": {"old": True}}
    expected_deployment_id = "new-deployment-123"
    mock_deployment_manager.create_deployment.return_value = expected_deployment_id

    await handle_update(None, sample_report, history)

    mock_deployment_manager.create_deployment.assert_called_once_with(sample_report)
    assert sample_report.deployment_id == expected_deployment_id
    assert "Created new Prefect deployment" in caplog.text


@pytest.mark.asyncio
async def test_handle_update_newly_published(mock_deployment_manager, sample_report, caplog):
    """Test update handler when report becomes published."""
    caplog.set_level(logging.INFO)
    history = {"is_published": {"old": False}, "is_active": {"old": True}}
    expected_deployment_id = "new-deployment-123"
    mock_deployment_manager.create_deployment.return_value = expected_deployment_id

    await handle_update(None, sample_report, history)

    mock_deployment_manager.create_deployment.assert_called_once_with(sample_report)
    assert sample_report.deployment_id == expected_deployment_id
    assert "Created new Prefect deployment" in caplog.text


@pytest.mark.asyncio
async def test_handle_update_active_status_change(mock_deployment_manager, sample_report, caplog):
    """Test update handler when active status changes."""
    caplog.set_level(logging.INFO)
    history = {"is_active": {"old": False}, "is_published": {"old": True}}
    expected_deployment_id = "new-deployment-123"
    mock_deployment_manager.create_deployment.return_value = expected_deployment_id

    await handle_update(None, sample_report, history)

    mock_deployment_manager.create_deployment.assert_called_once_with(sample_report)
    assert sample_report.deployment_id == expected_deployment_id
    assert "Created new Prefect deployment" in caplog.text


@pytest.mark.asyncio
async def test_handle_update_no_changes(mock_deployment_manager, sample_report):
    """Test update handler when no relevant changes occur."""
    history = {"is_active": {"old": True}, "is_published": {"old": True}}

    await handle_update(None, sample_report, history)

    mock_deployment_manager.create_deployment.assert_not_called()


# Tests for handle_deletion
@pytest.mark.asyncio
async def test_handle_deletion_with_deployment(mock_deployment_manager, sample_report, caplog):
    """Test deletion handler with existing deployment."""
    caplog.set_level(logging.INFO)
    sample_report.deployment_id = "existing-deployment-id"

    await handle_deletion(None, sample_report)

    mock_deployment_manager.delete_deployment.assert_called_once_with("existing-deployment-id")
    assert "Deleted Prefect deployment" in caplog.text


@pytest.mark.asyncio
async def test_handle_deletion_without_deployment(mock_deployment_manager, sample_report):
    """Test deletion handler without existing deployment."""
    sample_report.deployment_id = None

    await handle_deletion(None, sample_report)

    mock_deployment_manager.delete_deployment.assert_not_called()
