import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from commons.db.crud import NotFoundError
from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from commons.utilities.pagination import PaginationParams
from insights_backend.notifications.crud import CRUDNotifications
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import Alert, Report

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(name="notifications_crud")
async def notifications_crud_fixture(db_session: AsyncSession):
    """Fixture for CRUDNotifications instance"""
    return CRUDNotifications(db_session)  # type: ignore


@pytest_asyncio.fixture(name="alerts_with_tags")
async def alerts_with_tags_fixture(db_session: AsyncSession, jwt_payload: dict) -> list[Alert]:
    """Create sample alerts with tags"""
    set_tenant_id(jwt_payload["tenant_id"])
    alerts = []

    tag_sets = [
        ["daily", "metrics", "revenue"],
        ["day", "development", "testing"],
        ["monthly", "metrics", "sales"],
        ["quarterly", "finance"],
    ]

    for i, tags in enumerate(tag_sets):
        alert = Alert(
            name=f"Test Alert {i}",
            type=NotificationType.ALERT,
            grain=Granularity.DAY,
            summary=f"Test summary {i}",
            tags=tags,
            is_active=True,
            tenant_id=jwt_payload["tenant_id"],
            trigger={"type": "METRIC_STORY", "condition": {"metric_ids": ["revenue"]}},
        )
        db_session.add(alert)
        alerts.append(alert)

    await db_session.flush()
    for alert in alerts:
        await db_session.refresh(alert)
    return alerts


@pytest_asyncio.fixture(name="reports_with_tags")
async def reports_with_tags_fixture(db_session: AsyncSession, jwt_payload: dict) -> list[Report]:
    """Create sample reports with tags"""
    set_tenant_id(jwt_payload["tenant_id"])
    reports = []

    tag_sets = [
        ["weekly", "metrics", "pipeline"],
        ["monthly", "development", "progress"],
        ["quarterly", "metrics", "forecast"],
    ]

    default_schedule = {
        "minute": "0",
        "hour": "9",
        "day_of_month": "*",
        "month": "*",
        "day_of_week": "MON",
        "timezone": "UTC",
    }

    for i, tags in enumerate(tag_sets):
        report = Report(
            name=f"Test Report {i}",
            type=NotificationType.REPORT,
            grain=Granularity.DAY,
            summary=f"Test summary {i}",
            tags=tags,
            is_active=True,
            tenant_id=jwt_payload["tenant_id"],
            config={"metric_ids": ["revenue"], "comparisons": ["PERCENTAGE_CHANGE"]},
            schedule=default_schedule,
        )
        db_session.add(report)
        reports.append(report)

    await db_session.flush()
    for report in reports:
        await db_session.refresh(report)
    return reports


async def test_get_unique_tags_no_search(
    notifications_crud: CRUDNotifications, alerts_with_tags: list[Alert], reports_with_tags: list[Report]
):
    """Test getting all unique tags without search"""
    tags = await notifications_crud.get_unique_tags()

    # Collect all expected tags
    alert_tags = {tag for alert in alerts_with_tags for tag in alert.tags}
    report_tags = {tag for report in reports_with_tags for tag in report.tags}
    expected_tags = sorted(alert_tags | report_tags)

    assert tags == expected_tags
    assert len(tags) == len(expected_tags)
    assert len(tags) == len(set(tags))  # Ensure uniqueness


async def test_get_unique_tags_with_search(
    notifications_crud: CRUDNotifications, alerts_with_tags: list[Alert], reports_with_tags: list[Report]
):
    """Test getting tags with search parameter"""
    # Test exact match
    tags = await notifications_crud.get_unique_tags(search="daily")
    assert tags == ["daily"]

    # Test partial match
    tags = await notifications_crud.get_unique_tags(search="ly")
    assert set(tags) == {"daily", "monthly", "weekly", "quarterly"}

    # Test case insensitive
    tags = await notifications_crud.get_unique_tags(search="METRICS")
    assert "metrics" in tags

    # Test no matches
    tags = await notifications_crud.get_unique_tags(search="nonexistent")
    assert tags == []


async def test_get_unique_tags_empty_database(notifications_crud: CRUDNotifications):
    """Test getting tags when database is empty"""
    tags = await notifications_crud.get_unique_tags()
    assert tags == []

    tags = await notifications_crud.get_unique_tags(search="test")
    assert tags == []


async def test_get_notifications_list(
    notifications_crud: CRUDNotifications,
    alerts_with_tags: list[Alert],
    reports_with_tags: list[Report],
    jwt_payload: dict,
):
    """Test getting paginated list of notifications"""
    # Set tenant context for the query
    set_tenant_id(jwt_payload["tenant_id"])

    total_notifications = len(alerts_with_tags) + len(reports_with_tags)

    # Test without filters
    notifications, count = await notifications_crud.get_notifications_list(PaginationParams(offset=0, limit=10))

    assert count == total_notifications, f"Expected {total_notifications} notifications, got {count}"
    assert len(notifications) == total_notifications

    # Test pagination
    notifications, count = await notifications_crud.get_notifications_list(PaginationParams(offset=1, limit=2))
    assert len(notifications) == 2
    assert count == total_notifications


async def test_batch_delete(
    notifications_crud: CRUDNotifications,
    alerts_with_tags: list[Alert],
    reports_with_tags: list[Report],
):
    """Test batch deletion of notifications"""
    alert_ids = [alerts_with_tags[0].id]
    report_ids = [reports_with_tags[0].id]

    await notifications_crud.batch_delete(alert_ids=alert_ids, report_ids=report_ids)

    # Verify deletion
    notifications, count = await notifications_crud.get_notifications_list(PaginationParams(offset=0, limit=10))
    assert count == len(alerts_with_tags) + len(reports_with_tags) - 2
    assert all(n.id not in alert_ids for n in notifications)
    assert all(n.id not in report_ids for n in notifications)


async def test_batch_delete_invalid_ids(notifications_crud: CRUDNotifications):
    """Test batch deletion with invalid IDs"""
    with pytest.raises(NotFoundError) as exc_info:
        await notifications_crud.batch_delete(alert_ids=[99999], report_ids=[])
    assert str(exc_info.value) == "404: Object with id [99999] not found"


async def test_batch_status_update(
    notifications_crud: CRUDNotifications,
    alerts_with_tags: list[Alert],
    reports_with_tags: list[Report],
):
    """Test batch status update"""
    alert_ids = [alerts_with_tags[0].id]
    report_ids = [reports_with_tags[0].id]

    # Test deactivation
    await notifications_crud.batch_status_update(alert_ids=alert_ids, report_ids=report_ids, is_active=False)

    notifications, _ = await notifications_crud.get_notifications_list(PaginationParams(offset=0, limit=10))

    for notification in notifications:
        if notification.id in alert_ids or notification.id in report_ids:
            assert not notification.is_active
        else:
            assert notification.is_active

    # Test activation
    await notifications_crud.batch_status_update(alert_ids=alert_ids, report_ids=report_ids, is_active=True)

    notifications, _ = await notifications_crud.get_notifications_list(PaginationParams(offset=0, limit=10))
    assert all(n.is_active for n in notifications)


async def test_batch_status_update_invalid_ids(notifications_crud: CRUDNotifications):
    """Test batch status update with invalid IDs"""
    with pytest.raises(NotFoundError) as exc_info:
        await notifications_crud.batch_status_update(alert_ids=[99999], report_ids=[], is_active=True)
    assert str(exc_info.value) == "404: Object with id [99999] not found"


async def test_get_notifications_list_sorting(
    notifications_crud: CRUDNotifications,
    alerts_with_tags: list[Alert],
):
    """Test notification list sorting"""
    # Get all notifications sorted by name ascending
    notifications, _ = await notifications_crud.get_notifications_list(
        PaginationParams(offset=0, limit=10),
        filter_params={"type": NotificationType.ALERT.value},  # Filter only alerts for consistent testing
    )

    # Verify ascending order (default)
    names = [n.name for n in notifications]
    sorted_names = sorted(names)
    assert names == sorted_names, f"Expected {sorted_names}, got {names}"
