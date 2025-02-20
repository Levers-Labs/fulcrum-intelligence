import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import Alert, NotificationChannelConfig

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
        tags=["revenue", "existing"],
        is_active=True,
        is_published=False,
        tenant_id=jwt_payload["tenant_id"],
        trigger={"type": "METRIC_STORY", "condition": {"metric_ids": ["revenue"]}},
    )
    db_session.add(alert)
    await db_session.flush()
    await db_session.refresh(alert)
    return alert


@pytest_asyncio.fixture(name="sample_notification_config")
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

    response = await async_client.request(
        "DELETE", "/insights/v1/notification/bulk", json=alert_ids  # Send list of IDs directly
    )

    assert response.status_code == status.HTTP_204_NO_CONTENT

    # Verify alerts are deleted
    for alert_id in alert_ids:
        get_response = await async_client.get(f"/v1/notification/alerts/{alert_id}")
        assert get_response.status_code == status.HTTP_404_NOT_FOUND


async def test_bulk_update_alert_status(async_client: AsyncClient, multiple_alerts: list[Alert]):
    """Test bulk update of alert status"""
    alert_ids = [alert.id for alert in multiple_alerts]

    response = await async_client.patch("/v1/notification/bulk/status?is_active=true", json=alert_ids)

    assert response.status_code == status.HTTP_200_OK


async def test_list_notifications(
    async_client: AsyncClient, multiple_alerts: list[Alert], sample_notification_config: NotificationChannelConfig
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
