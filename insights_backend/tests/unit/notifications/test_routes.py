from copy import deepcopy
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from commons.db.signals import EventAction, EventTiming, publish_event
from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import Alert, NotificationChannelConfig, Report

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(name="alert_request_data")
def alert_request_data_fixture():
    # set_tenant_id(1)
    """Sample data for creating/updating an alert"""
    return {
        "tenant_id": 1,
        "name": "alert name",
        "description": "alert description",
        "grain": "day",
        "trigger": {
            "type": "METRIC_STORY",
            "condition": {"metric_ids": ["NewBizDeals"], "story_groups": ["TREND_CHANGES"]},
        },
        "summary": "summary",
        "notification_channels": [
            {
                "channel_type": "slack",
                "recipients": [
                    {
                        "id": "C1234567890",
                        "name": "#channel",
                        "is_channel": True,
                        "is_group": False,
                        "is_dm": False,
                        "is_private": False,
                    }
                ],
            },
            {"channel_type": "email", "recipients": [{"email": "user@example.com", "location": "to"}]},
        ],
    }


@pytest_asyncio.fixture(name="sample_alert")
async def sample_alert_fixture(db_session: AsyncSession, jwt_payload: dict) -> Alert:
    """Create a sample alert for testing"""
    set_tenant_id(jwt_payload["tenant_id"])

    alert = Alert(
        name="Existing Alert",
        type=NotificationType.ALERT,
        grain=Granularity.DAY,
        summary="Existing alert summary",
        tags=["revenue", "existing", "risk"],
        is_active=True,
        is_published=False,
        tenant_id=jwt_payload["tenant_id"],
        trigger={"type": "METRIC_STORY", "condition": {"metric_ids": ["revenue"]}},
    )
    db_session.add(alert)
    await db_session.flush()
    await db_session.refresh(alert)
    return alert


@pytest_asyncio.fixture(name="sample_notification_config_alert")
async def sample_notification_config_fixture(
    db_session: AsyncSession, jwt_payload: dict, sample_alert: Alert
) -> NotificationChannelConfig:
    """Create a sample notification channel config for testing"""
    set_tenant_id(jwt_payload["tenant_id"])
    channel = NotificationChannelConfig(
        alert_id=sample_alert.id,
        notification_type=NotificationType.ALERT,
        channel_type="slack",
        recipients=[
            {
                "id": "C1234567890",
                "name": "#channel",
                "is_channel": True,
                "is_group": False,
                "is_dm": False,
                "is_private": False,
            }
        ],
        tenant_id=jwt_payload["tenant_id"],
        template="my slack template",
    )
    db_session.add(channel)
    await db_session.flush()
    await db_session.refresh(channel)
    return channel


@pytest_asyncio.fixture(name="sample_notification_config_report")
async def sample_notification_config_report_fixture(
    db_session: AsyncSession, jwt_payload: dict, sample_report: Report
) -> NotificationChannelConfig:
    """Create a sample notification channel config for testing"""
    set_tenant_id(jwt_payload["tenant_id"])
    channel = NotificationChannelConfig(
        report_id=sample_report.id,
        notification_type=NotificationType.REPORT,
        channel_type="slack",
        recipients=[
            {
                "id": "C1234567890",
                "name": "#channel",
                "is_channel": True,
                "is_group": False,
                "is_dm": False,
                "is_private": False,
            }
        ],
        tenant_id=jwt_payload["tenant_id"],
        template="my slack template",
    )
    db_session.add(channel)
    await db_session.flush()
    await db_session.refresh(channel)
    return channel


async def test_create_alert(async_client: AsyncClient, alert_request_data: dict):
    """Test creating a new alert"""
    response = await async_client.post("/v1/notification/alerts", json=alert_request_data)

    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["name"] == alert_request_data["name"]
    assert data["description"] == alert_request_data["description"]
    assert data["summary"] == alert_request_data["summary"]


async def test_create_alert_duplicate_channels(async_client: AsyncClient, alert_request_data: dict):
    """Test creating alert with duplicate channel types"""
    alert_request_data["notification_channels"].append(
        {"channel_type": "slack", "recipients": [{"name": "#another-channel"}]}
    )

    response = await async_client.post("/v1/notification/alerts", json=alert_request_data)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


async def test_get_alert(async_client: AsyncClient, sample_alert):
    """Test getting a specific alert"""
    response = await async_client.get(f"/v1/notification/alerts/{sample_alert.id}")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["id"] == sample_alert.id
    assert data["name"] == sample_alert.name


async def test_create_alert_duplicate_tags(async_client: AsyncClient, alert_request_data: dict):
    """Test creating alert with duplicate tags"""
    alert_request_data["tags"] = ["revenue", "revenue", "test", "test"]

    response = await async_client.post("/v1/notification/alerts", json=alert_request_data)

    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    # Should deduplicate tags
    assert len(data["tags"]) == 2
    assert set(data["tags"]) == {"revenue", "test"}


async def test_get_tags_with_search(async_client: AsyncClient, sample_alert: Alert):
    """Test getting tags with various search parameters"""
    # Test exact match
    response = await async_client.get("/v1/notification/tags?search=revenue")
    assert response.status_code == status.HTTP_200_OK
    tags = response.json()
    assert tags == ["revenue"]

    # Test partial match starting with
    response = await async_client.get("/v1/notification/tags?search=r")
    assert response.status_code == status.HTTP_200_OK
    tags = response.json()
    assert set(tags) == {"revenue", "risk"}


async def test_create_alert_empty_name(async_client: AsyncClient, alert_request_data: dict):
    """Test creating alert with empty name"""
    alert_request_data["name"] = None

    response = await async_client.post("/v1/notification/alerts", json=alert_request_data)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    data = response.json()
    assert "name" in data["detail"][0]["loc"]


async def test_create_alert_invalid_grain(async_client: AsyncClient, alert_request_data: dict):
    """Test creating alert with invalid grain"""
    alert_request_data["grain"] = "invalid_grain"

    response = await async_client.post("/v1/notification/alerts", json=alert_request_data)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    data = response.json()
    assert "grain" in data["detail"][0]["loc"]


async def test_create_alert_invalid_trigger(async_client: AsyncClient, alert_request_data: dict):
    """Test creating alert with invalid trigger"""
    alert_request_data["trigger"] = {"type": "INVALID_TYPE", "condition": {}}

    response = await async_client.post("/v1/notification/alerts", json=alert_request_data)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    data = response.json()
    assert "trigger" in data["detail"][0]["loc"]


async def test_create_alert_missing_required_fields(async_client: AsyncClient):
    """Test creating alert with missing required fields"""
    incomplete_data = {
        "name": "Test Alert"
        # Missing required fields
    }

    response = await async_client.post("/v1/notification/alerts", json=incomplete_data)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


async def test_create_alert_with_whitespace_tags(async_client: AsyncClient, alert_request_data: dict):
    """Test creating alert with whitespace in tags"""
    alert_request_data["tags"] = ["  revenue  ", " test ", "", "  "]

    response = await async_client.post("/v1/notification/alerts", json=alert_request_data)

    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    # Should strip whitespace and remove empty tags
    assert len(data["tags"]) == 2
    assert all(tag.strip() == tag for tag in data["tags"])
    assert "" not in data["tags"]


async def test_publish_alert(async_client: AsyncClient, sample_alert: Alert):
    """Test publishing a alert"""
    # Mock is_publishable to return True
    with patch.object(Alert, "is_publishable", return_value=True):
        response = await async_client.post(f"/v1/notification/alerts/{sample_alert.id}/publish")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["is_published"] is True


async def test_publish_nonexistent_alert(async_client: AsyncClient):
    """Test publishing a non-existent alert"""

    response = await async_client.post("/v1/notification/alerts/999999/publish")

    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest_asyncio.fixture(name="multiple_alerts")
async def multiple_alerts_fixture(db_session: AsyncSession, jwt_payload: dict) -> list[Alert]:
    """Create multiple alerts for bulk operation testing"""
    set_tenant_id(jwt_payload["tenant_id"])
    alerts = []

    for i in range(3):
        alert = Alert(
            name=f"Test Alert {i}",
            type=NotificationType.ALERT,
            grain=Granularity.DAY,
            summary=f"Test summary {i}",
            tags=[f"tag{i}", "common"],
            is_active=True,
            is_published=False,
            tenant_id=jwt_payload["tenant_id"],
            trigger={"type": "METRIC_STORY", "condition": {"metric_ids": ["revenue"]}},
        )
        db_session.add(alert)
        alerts.append(alert)

    await db_session.flush()
    for alert in alerts:
        await db_session.refresh(alert)
    return alerts


async def test_bulk_delete_alerts(async_client: AsyncClient, multiple_alerts: list[Alert]):
    """Test bulk deletion of alerts"""
    alert_ids = [alert.id for alert in multiple_alerts]
    mock_background_task = patch("fastapi.BackgroundTasks.add_task").start()

    response = await async_client.request(
        "DELETE", "/v1/notification/bulk", json={"alert_ids": alert_ids, "report_ids": []}
    )

    assert response.status_code == status.HTTP_200_OK

    # Verify background task was called for each alert
    assert mock_background_task.call_count == len(alert_ids)
    for alert in multiple_alerts:
        mock_background_task.assert_any_call(
            publish_event,
            action=EventAction.DELETE,
            sender=Alert,
            timing=EventTiming.AFTER,
            instance=alert,
        )

    # Verify alerts are deleted
    for alert_id in alert_ids:
        get_response = await async_client.get(f"/v1/notification/alerts/{alert_id}")
        assert get_response.status_code == status.HTTP_404_NOT_FOUND

    patch.stopall()


async def test_bulk_update_alert_status(async_client: AsyncClient, multiple_alerts: list[Alert]):
    """Test bulk update of alert status"""
    alert_ids = [alert.id for alert in multiple_alerts]

    response = await async_client.patch(
        "/v1/notification/bulk/status?is_active=true", json={"alert_ids": alert_ids, "report_ids": []}
    )
    assert response.status_code == status.HTTP_200_OK


async def test_list_notifications(
    async_client: AsyncClient, multiple_alerts: list[Alert], sample_notification_config_alert: NotificationChannelConfig
):
    """Test listing notifications with various filters"""
    response = await async_client.get(
        "/v1/notification/?notification_type=ALERT&grain=day",
    )

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert "results" in data
    assert "count" in data
    assert len(data["results"]) > 0


async def test_get_tags(async_client: AsyncClient, multiple_alerts: list[Alert]):
    """Test getting all unique alert tags"""
    response = await async_client.get("/v1/notification/tags")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert "common" in data  # Common tag across all alerts
    assert any(tag.startswith("tag") for tag in data)  # Individual tags


async def test_list_notifications_invalid_filters(async_client: AsyncClient):
    """Test listing notifications with invalid filters"""
    response = await async_client.get(
        "/v1/notification/?notification_type=Invalid&grain=Invalid",
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest_asyncio.fixture(name="sample_report")
async def sample_report_fixture(db_session: AsyncSession, jwt_payload: dict) -> Report:
    """Create a sample report for testing"""
    set_tenant_id(jwt_payload["tenant_id"])

    report = Report(
        name="Test Report",
        type=NotificationType.REPORT,
        grain=Granularity.DAY,
        summary="Test report summary",
        tags=["revenue", "test"],
        is_active=True,
        is_published=False,
        tenant_id=jwt_payload["tenant_id"],
        schedule={
            "minute": "0",
            "hour": "9",
            "day_of_month": "*",
            "month": "*",
            "day_of_week": "MON",
            "timezone": "UTC",
            "label": "DAY",
        },
        config={"metric_ids": ["revenue"], "comparisons": ["PERCENTAGE_CHANGE"]},
    )
    db_session.add(report)
    await db_session.flush()
    await db_session.refresh(report)
    return report


@pytest_asyncio.fixture(name="multiple_reports")
async def multiple_reports_fixture(db_session: AsyncSession, jwt_payload: dict) -> list[Report]:
    """Create multiple reports for testing"""
    set_tenant_id(jwt_payload["tenant_id"])
    reports = []

    for i in range(3):
        report = Report(
            name=f"Test Report {i}",
            type=NotificationType.REPORT,
            grain=Granularity.DAY,
            summary=f"Test summary {i}",
            tags=[f"tag{i}", "common"],
            is_active=True,
            is_published=False,
            tenant_id=jwt_payload["tenant_id"],
            schedule={
                "minute": "0",
                "hour": "9",
                "day_of_month": "*",
                "month": "*",
                "day_of_week": "MON",
                "timezone": "UTC",
                "label": "DAY",
            },
            config={"metric_ids": ["revenue"], "comparisons": ["WoW"]},
        )
        db_session.add(report)
        reports.append(report)

    await db_session.flush()
    for report in reports:
        await db_session.refresh(report)
    return reports


async def test_create_report(async_client: AsyncClient):
    """Test creating a new report"""
    report_data = {
        "name": "Test Report",
        "description": "Test Description",
        "grain": "day",
        "summary": "Test Summary",
        "schedule": {
            "minute": "0",
            "hour": "9",
            "day_of_month": "*",
            "month": "*",
            "day_of_week": "MON",
            "timezone": "UTC",
            "label": "DAY",
        },
        "config": {"metric_ids": ["revenue"], "comparisons": ["PERCENTAGE_CHANGE"]},
        "notification_channels": [
            {"channel_type": "slack", "recipients": [{"id": "C1234567890", "name": "#channel", "is_channel": True}]}
        ],
    }

    response = await async_client.post("/v1/notification/reports", json=report_data)
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["name"] == report_data["name"]


async def test_get_report(async_client: AsyncClient, sample_report: Report):
    """Test getting a specific report"""
    response = await async_client.get(f"/v1/notification/reports/{sample_report.id}")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["id"] == sample_report.id
    assert data["name"] == sample_report.name


async def test_bulk_delete_reports(async_client: AsyncClient, multiple_reports: list[Report]):
    """Test bulk deletion of reports"""
    report_ids = [report.id for report in multiple_reports]
    mock_background_task = patch("fastapi.BackgroundTasks.add_task").start()

    response = await async_client.request(
        "DELETE", "/v1/notification/bulk", json={"alert_ids": [], "report_ids": report_ids}
    )

    assert response.status_code == status.HTTP_200_OK

    # Verify background task was called for each report
    assert mock_background_task.call_count == len(report_ids)
    for report in multiple_reports:
        mock_background_task.assert_any_call(
            publish_event,
            action=EventAction.DELETE,
            sender=Report,
            timing=EventTiming.AFTER,
            instance=report,
        )

    # Verify reports are deleted
    for report_id in report_ids:
        get_response = await async_client.get(f"/v1/notification/reports/{report_id}")
        assert get_response.status_code == status.HTTP_404_NOT_FOUND

    patch.stopall()


async def test_bulk_delete_mixed(
    async_client: AsyncClient, multiple_alerts: list[Alert], multiple_reports: list[Report]
):
    """Test bulk deletion of both alerts and reports"""
    alert_ids = [alert.id for alert in multiple_alerts]
    report_ids = [report.id for report in multiple_reports]
    mock_background_task = patch("fastapi.BackgroundTasks.add_task").start()

    response = await async_client.request(
        "DELETE", "/v1/notification/bulk", json={"alert_ids": alert_ids, "report_ids": report_ids}
    )

    assert response.status_code == status.HTTP_200_OK

    # Verify background task was called for each item
    assert mock_background_task.call_count == len(alert_ids) + len(report_ids)

    # Verify calls for alerts
    for alert in multiple_alerts:
        mock_background_task.assert_any_call(
            publish_event,
            action=EventAction.DELETE,
            sender=Alert,
            timing=EventTiming.AFTER,
            instance=alert,
        )

    # Verify calls for reports
    for report in multiple_reports:
        mock_background_task.assert_any_call(
            publish_event,
            action=EventAction.DELETE,
            sender=Report,
            timing=EventTiming.AFTER,
            instance=report,
        )

    # Verify all items are deleted
    for alert_id in alert_ids:
        get_response = await async_client.get(f"/v1/notification/alerts/{alert_id}")
        assert get_response.status_code == status.HTTP_404_NOT_FOUND

    for report_id in report_ids:
        get_response = await async_client.get(f"/v1/notification/reports/{report_id}")
        assert get_response.status_code == status.HTTP_404_NOT_FOUND

    patch.stopall()


async def test_publish_report(async_client: AsyncClient, sample_report: Report):
    """Test publishing a report"""
    # Mock is_publishable to return True
    with patch.object(Report, "is_publishable", return_value=True):
        response = await async_client.post(f"/v1/notification/reports/{sample_report.id}/publish")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["is_published"] is True


async def test_update_alert(async_client: AsyncClient, sample_alert: Alert):
    """Test updating an alert"""
    update_data = {
        "name": "Updated Alert Name",
        "grain": "day",
        "description": "Updated description",
        "summary": "Updated summary",
        "tags": ["new_tag", "updated"],
        "is_active": True,
        "trigger": {
            "type": "METRIC_STORY",
            "condition": {"metric_ids": ["NewBizDeals"], "story_groups": ["TREND_CHANGES"]},
        },
        "notification_channels": [
            {
                "channel_type": "slack",
                "recipients": [
                    {
                        "id": "C1234567890",
                        "name": "#updated-channel",
                        "is_channel": True,
                        "is_group": False,
                        "is_dm": False,
                        "is_private": False,
                    }
                ],
            }
        ],
    }

    response = await async_client.patch(f"/v1/notification/alerts/{sample_alert.id}", json=update_data)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == update_data["name"]
    assert data["description"] == update_data["description"]
    assert set(data["tags"]) == set(update_data["tags"])  # type: ignore


async def test_update_report(async_client: AsyncClient, sample_report: Report):
    """Test updating a report"""
    update_data = {
        "name": "Updated Report Name",
        "description": "Updated description",
        "summary": "Updated summary",
        "tags": ["new_tag", "updated"],
        "is_active": True,
        "grain": "day",
        "schedule": {
            "minute": "30",
            "hour": "10",
            "day_of_month": "*",
            "month": "*",
            "day_of_week": "TUE",
            "timezone": "UTC",
            "label": "DAY",
        },
        "notification_channels": [
            {
                "channel_type": "slack",
                "recipients": [
                    {
                        "id": "C1234567890",
                        "name": "#updated-channel",
                        "is_channel": True,
                        "is_group": False,
                        "is_dm": False,
                        "is_private": False,
                    }
                ],
            }
        ],
        "config": {"metric_ids": ["revenue"], "comparisons": ["PERCENTAGE_CHANGE"]},
    }

    response = await async_client.patch(f"/v1/notification/reports/{sample_report.id}", json=update_data)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == update_data["name"]
    assert data["description"] == update_data["description"]
    assert set(data["tags"]) == set(update_data["tags"])  # type: ignore


async def test_bulk_delete_nonexistent_ids(async_client: AsyncClient):
    """Test bulk deletion with non-existent IDs"""
    response = await async_client.request(
        "DELETE", "/v1/notification/bulk", json={"alert_ids": [99999], "report_ids": [88888]}
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND


async def test_bulk_delete_empty_ids(async_client: AsyncClient):
    """Test bulk deletion with empty ID lists"""
    response = await async_client.request("DELETE", "/v1/notification/bulk", json={"alert_ids": [], "report_ids": []})
    assert response.status_code == status.HTTP_200_OK


async def test_publish_already_published_alert(async_client: AsyncClient, sample_alert: Alert):
    """Test publishing an already published alert"""
    # First publish
    with patch.object(Alert, "is_publishable", return_value=True):
        await async_client.post(f"/v1/notification/alerts/{sample_alert.id}/publish")

    # Try to publish again
    response = await async_client.post(f"/v1/notification/alerts/{sample_alert.id}/publish")
    assert response.status_code == status.HTTP_400_BAD_REQUEST


async def test_publish_already_published_report(async_client: AsyncClient, sample_report: Report):
    """Test publishing an already published report"""
    # First publish
    with patch.object(Report, "is_publishable", return_value=True):
        await async_client.post(f"/v1/notification/reports/{sample_report.id}/publish")

    # Try to publish again
    response = await async_client.post(f"/v1/notification/reports/{sample_report.id}/publish")
    assert response.status_code == status.HTTP_400_BAD_REQUEST


async def test_bulk_update_status_nonexistent_ids(async_client: AsyncClient):
    """Test bulk status update with non-existent IDs"""
    response = await async_client.patch(
        "/v1/notification/bulk/status?is_active=true", json={"alert_ids": [99999], "report_ids": []}
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND


async def test_bulk_update_status(async_client: AsyncClient, multiple_alerts: list[Alert]):
    """Test bulk update of alert status"""
    alert_ids = [alert.id for alert in multiple_alerts]
    mock_background_task = patch("fastapi.BackgroundTasks.add_task").start()

    response = await async_client.patch(
        "/v1/notification/bulk/status?is_active=true", json={"alert_ids": alert_ids, "report_ids": []}
    )

    assert response.status_code == status.HTTP_200_OK

    # Verify background task was called for each alert
    assert mock_background_task.call_count == len(alert_ids)
    for alert in multiple_alerts:
        mock_background_task.assert_any_call(
            publish_event,
            action=EventAction.STATUS_CHANGE,
            sender=Alert,
            timing=EventTiming.AFTER,
            instance=alert,
        )

    patch.stopall()


async def test_create_alert_missing_slack_channel_id(async_client: AsyncClient, alert_request_data: dict):
    """Test creating alert with missing Slack channel ID"""
    # Create a deep copy to avoid modifying the fixture
    alert_data = deepcopy(alert_request_data)
    # Remove id from Slack channel
    del alert_data["notification_channels"][0]["recipients"][0]["id"]

    response = await async_client.post("/v1/notification/alerts", json=alert_data)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    data = response.json()
    assert any("id" in str(error).lower() for error in data["detail"])


async def test_list_alerts_with_filters(async_client: AsyncClient, multiple_alerts: list[Alert]):
    """Test listing alerts with various filter combinations"""
    # Test basic listing
    response = await async_client.get("/v1/notification/alerts")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data["results"]) == len(multiple_alerts)

    # Test is_active filter
    response = await async_client.get("/v1/notification/alerts?is_active=true")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert all(alert["is_active"] for alert in data["results"])

    # Test grains filter
    response = await async_client.get("/v1/notification/alerts?grains=day")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert all(alert["grain"] == "day" for alert in data["results"])

    # Test is_published filter
    response = await async_client.get("/v1/notification/alerts?is_published=false")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert all(not alert["is_published"] for alert in data["results"])


async def test_list_alerts_with_metric_filters(async_client: AsyncClient, db_session: AsyncSession, jwt_payload: dict):
    """Test listing alerts with metric_ids filter"""
    # Create alerts with different metric IDs
    set_tenant_id(jwt_payload["tenant_id"])
    alerts = [
        Alert(
            name=f"Alert {i}",
            type=NotificationType.ALERT,
            grain=Granularity.DAY,
            tenant_id=jwt_payload["tenant_id"],
            trigger={
                "type": "METRIC_STORY",
                "condition": {"metric_ids": [f"metric_{i}", "common_metric"]},
            },
        )
        for i in range(2)
    ]
    for alert in alerts:
        db_session.add(alert)
    await db_session.flush()

    # Test filtering by single metric ID
    response = await async_client.get("/v1/notification/alerts?metric_ids=metric_0")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data["results"]) == 1
    assert "metric_0" in data["results"][0]["trigger"]["condition"]["metric_ids"]

    # Test filtering by common metric ID
    response = await async_client.get("/v1/notification/alerts?metric_ids=common_metric")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data["results"]) == 2

    # Test filtering by multiple metric IDs
    response = await async_client.get("/v1/notification/alerts?metric_ids=metric_0&metric_ids=metric_1")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data["results"]) == 2


async def test_list_alerts_with_story_groups(async_client: AsyncClient, db_session: AsyncSession, jwt_payload: dict):
    """Test listing alerts with story_groups filter"""
    set_tenant_id(jwt_payload["tenant_id"])
    alerts = [
        Alert(
            name=f"Alert {i}",
            type=NotificationType.ALERT,
            grain=Granularity.DAY,
            tenant_id=jwt_payload["tenant_id"],
            trigger={
                "type": "METRIC_STORY",
                "condition": {
                    "metric_ids": ["metric"],
                    "story_groups": [f"group_{i}", "common_group"],
                },
            },
        )
        for i in range(2)
    ]
    for alert in alerts:
        db_session.add(alert)
    await db_session.flush()

    # Test filtering by single story group
    response = await async_client.get("/v1/notification/alerts?story_groups=group_0")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data["results"]) == 1
    assert "group_0" in data["results"][0]["trigger"]["condition"]["story_groups"]

    # Test filtering by common story group
    response = await async_client.get("/v1/notification/alerts?story_groups=common_group")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data["results"]) == 2

    # Test filtering by multiple story groups
    response = await async_client.get("/v1/notification/alerts?story_groups=group_0&story_groups=group_1")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data["results"]) == 2


async def test_list_alerts_with_combined_filters(
    async_client: AsyncClient, db_session: AsyncSession, jwt_payload: dict
):
    """Test listing alerts with multiple filters combined"""
    set_tenant_id(jwt_payload["tenant_id"])
    alerts = [
        Alert(
            name=f"Alert {i}",
            type=NotificationType.ALERT,
            grain=Granularity.DAY if i % 2 == 0 else Granularity.WEEK,
            is_active=i % 2 == 0,
            tenant_id=jwt_payload["tenant_id"],
            trigger={
                "type": "METRIC_STORY",
                "condition": {
                    "metric_ids": [f"metric_{i}", "common_metric"],
                    "story_groups": [f"group_{i}", "common_group"],
                },
            },
        )
        for i in range(4)
    ]
    for alert in alerts:
        db_session.add(alert)
    await db_session.flush()

    # Test combining multiple filters
    response = await async_client.get(
        "/v1/notification/alerts?is_active=true&grains=day&metric_ids=common_metric&story_groups=common_group"
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data["results"]) > 0
    for alert in data["results"]:
        assert alert["is_active"] is True
        assert alert["grain"] == "day"
        assert "common_metric" in alert["trigger"]["condition"]["metric_ids"]
        assert "common_group" in alert["trigger"]["condition"]["story_groups"]


async def test_list_alerts_invalid_filters(async_client: AsyncClient):
    """Test listing alerts with invalid filter values"""
    # Test invalid grains
    response = await async_client.get("/v1/notification/alerts?grains=invalid")
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    # Test invalid boolean value
    response = await async_client.get("/v1/notification/alerts?is_active=invalid")
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
