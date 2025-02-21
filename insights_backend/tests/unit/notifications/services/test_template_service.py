import pytest
from jinja2 import Template

from commons.notifiers.constants import NotificationChannel
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import EmailTemplate, SlackTemplate
from insights_backend.notifications.services.template_service import TemplateService


@pytest.fixture
def template_service():
    return TemplateService()


def test_template_service_initialization(template_service: TemplateService):
    """Test template service initialization"""
    assert template_service.env is not None
    assert template_service.template_dir.exists()
    assert template_service.template_dir.is_dir()


@pytest.mark.parametrize(
    "notification_type,channel_type,expected_extension",
    [
        (NotificationType.ALERT, NotificationChannel.SLACK, "json"),
        (NotificationType.ALERT, NotificationChannel.EMAIL, "html"),
        (NotificationType.REPORT, NotificationChannel.SLACK, "json"),
        (NotificationType.REPORT, NotificationChannel.EMAIL, "html"),
    ],
)
def test_get_template(
    template_service: TemplateService,
    notification_type: NotificationType,
    channel_type: NotificationChannel,
    expected_extension: str,
):
    """Test getting templates for different notification and channel types"""
    template = template_service._get_template(notification_type, channel_type)

    assert isinstance(template, Template)
    assert template.filename.endswith(f".{expected_extension}")  # type: ignore
    assert notification_type.lower() in template.filename  # type: ignore
    assert channel_type.lower() in template.filename  # type: ignore


@pytest.mark.parametrize(
    "channel_type,expected_type",
    [
        (NotificationChannel.SLACK, SlackTemplate),
        (NotificationChannel.EMAIL, EmailTemplate),
    ],
)
def test_prepare_channel_template(
    template_service: TemplateService, channel_type: NotificationChannel, expected_type: type
):
    """Test preparing templates for different channels"""
    template = template_service.prepare_channel_template(NotificationType.ALERT, channel_type)

    assert isinstance(template, expected_type)


def test_slack_template_formatting(template_service: TemplateService):
    """Test Slack template specific formatting"""
    template = template_service.prepare_channel_template(NotificationType.REPORT, NotificationChannel.SLACK)

    assert isinstance(template, SlackTemplate)
    # Verify the template is valid JSON structure
    assert '"blocks":' in template.message


def test_email_template_structure(template_service: TemplateService):
    """Test email template structure"""
    template = template_service.prepare_channel_template(NotificationType.ALERT, NotificationChannel.EMAIL)

    assert isinstance(template, EmailTemplate)
    assert template.subject is not None
    assert template.body is not None


def test_invalid_template_path(template_service: TemplateService):
    """Test handling of invalid template paths"""
    with pytest.raises(Exception):  # noqa
        template_service._get_template("INVALID_TYPE", NotificationChannel.SLACK)  # type: ignore


def test_template_whitespace_handling(template_service: TemplateService):
    """Test template whitespace handling"""
    slack_template = template_service.prepare_channel_template(NotificationType.REPORT, NotificationChannel.SLACK)

    # Verify no excessive whitespace in Slack JSON
    assert "  " not in slack_template.message  # No double spaces
    assert "\n" not in slack_template.message  # No newlines
