import pytest

from commons.models.enums import Granularity
from insights_backend.notifications.models import AlertTrigger
from insights_backend.notifications.schemas import (
    AlertRequest,
    NotificationChannelConfig,
    ReportConfig,
    ReportRequest,
    ScheduleConfig,
)
from insights_backend.notifications.services.preview.alert import AlertPreviewService
from insights_backend.notifications.services.preview.report import ReportPreviewService
from insights_backend.notifications.services.template_service import TemplateService

pytestmark = pytest.mark.asyncio


@pytest.fixture
def template_service():
    return TemplateService()


@pytest.fixture
def alert_preview_service(template_service):
    return AlertPreviewService(template_service)


@pytest.fixture
def report_preview_service(template_service):
    return ReportPreviewService(template_service)


@pytest.fixture
def sample_alert_request():
    return AlertRequest(
        name="alert name",
        description="alert description",
        grain=Granularity.DAY,
        summary="summary",
        notification_channels=[
            NotificationChannelConfig(
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
            ),
            NotificationChannelConfig(
                channel_type="email", recipients=[{"email": "user@example.com", "location": "to"}]
            ),
        ],
        trigger=AlertTrigger(
            type="METRIC_STORY", condition={"metric_ids": ["NewBizDeals"], "story_groups": ["TREND_CHANGES"]}
        ),
    )


@pytest.fixture
def sample_report_request():
    return ReportRequest(
        name="report name",
        description="report description",
        grain=Granularity.DAY,
        summary="Daily report summary",
        notification_channels=[
            NotificationChannelConfig(
                channel_type="slack",
                recipients=[
                    {
                        "name": "#daily-metrics",
                        "is_channel": True,
                        "is_group": False,
                        "is_dm": False,
                        "is_private": False,
                    }
                ],
            ),
            NotificationChannelConfig(
                channel_type="email",
                recipients=[
                    {"email": "team@example.com", "location": "to"},
                    {"email": "manager@example.com", "location": "cc"},
                ],
            ),
        ],
        schedule=ScheduleConfig(
            minute="0",
            hour="9",
            day_of_month="*",
            month="*",
            day_of_week="MON",
            timezone="America/New_York",
        ),
        config=ReportConfig(metric_ids=["NewBizDeals", "NewWins"], comparisons=["PERCENTAGE_CHANGE"]),
    )


async def test_alert_preview_generation(alert_preview_service, sample_alert_request):
    preview = await alert_preview_service.preview(sample_alert_request)

    # Check structure
    assert "email" in preview
    assert "slack" in preview

    # Check email preview
    email = preview["email"]
    assert email["to_emails"] == ["user@example.com"]
    assert email["cc_emails"] == []
    assert "New Stories Alert" in email["subject"]
    assert isinstance(email["body"], str)

    # Check slack preview
    slack = preview["slack"]
    assert slack["channels"] == ["#channel"]
    assert isinstance(slack["message"], str)


async def test_alert_preview_context(alert_preview_service, sample_alert_request):
    context = await alert_preview_service._generate_context(sample_alert_request)

    # Check required context fields
    assert "metric" in context
    assert "grain" in context
    assert "time" in context
    assert "stories" in context

    # Check story structure
    assert isinstance(context["stories"], list)
    for story in context["stories"]:
        assert "story_group" in story
        assert "title" in story
        assert "detail" in story


async def test_report_preview_generation(report_preview_service, sample_report_request):
    preview = await report_preview_service.preview(sample_report_request)

    # Check structure
    assert "email" in preview
    assert "slack" in preview

    # Check email preview
    email = preview["email"]
    assert email["to_emails"] == ["team@example.com"]
    assert email["cc_emails"] == ["manager@example.com"]
    assert "Report" in email["subject"]
    assert isinstance(email["body"], str)

    # Check slack preview
    slack = preview["slack"]
    assert slack["channels"] == ["#daily-metrics"]
    assert isinstance(slack["message"], str)


async def test_report_preview_context(report_preview_service, sample_report_request):
    context = await report_preview_service._generate_context(sample_report_request)

    # Check required context fields
    assert "report_name" in context
    assert "grain" in context
    assert "time" in context
    assert "metrics" in context

    # Check metrics structure
    assert isinstance(context["metrics"], list)
    for metric in context["metrics"]:
        assert "id" in metric
        assert "label" in metric
        assert "value" in metric


async def test_report_metrics_generation(report_preview_service, sample_report_request):
    metrics = report_preview_service._generate_metrics(sample_report_request)

    # Check metrics match configured metric_ids
    assert len(metrics) == len(sample_report_request.config.metric_ids)
    metric_ids = [metric["id"] for metric in metrics]
    for configured_id in sample_report_request.config.metric_ids:
        assert configured_id in metric_ids


async def test_report_fallback_metrics(report_preview_service, sample_report_request):
    # Test with no configured metrics
    sample_report_request.config.metric_ids = []
    metrics = report_preview_service._generate_metrics(sample_report_request)

    # Should generate default number of fake metrics
    assert len(metrics) == 3
    for metric in metrics:
        assert isinstance(metric["id"], str)
        assert isinstance(metric["label"], str)
        assert isinstance(metric["value"], int)
