from unittest.mock import ANY, MagicMock

import pytest

from commons.notifiers.constants import EMAIL_TEMPLATES, TemplateType
from commons.notifiers.plugins import EmailNotifier


@pytest.fixture
def email_notifier(mocker, mock_email_client):
    notifier = EmailNotifier("templates", sender="test@example.com")
    mocker.patch.object(notifier, "get_client", return_value=mock_email_client)
    return notifier


@pytest.mark.asyncio
async def test_email_notifier_send_success(mocker, email_notifier, mock_email_config, mock_email_client):
    # Prepare
    mock_template = MagicMock()
    mock_template.render.return_value = "<p>Test email content</p>"
    mocker.patch.object(email_notifier, "get_template", return_value=mock_template)
    mock_email_client.send_email.return_value = (True, {"MessageId": "test-id"})

    context = {
        "name": "IMP Metrics",
        "grain": "weekly",
        "period": "Nov 18, 2024 — Nov 24, 2024",
        "metrics": [
            {"name": "Signups", "value": "80.3K", "percentage_change": 5.51, "absolute_change": "+4190"},
            {"name": "Pro Trials", "value": "2.77K", "percentage_change": -12.7, "absolute_change": "-401"},
            {
                "name": "New + Reactivated Pro Customers",
                "value": "1.41K",
                "percentage_change": 0.14,
                "absolute_change": "+2",
            },
            {"name": "Churn Customers", "value": "679", "percentage_change": -7.74, "absolute_change": "-57"},
        ],
        "report_link": "https://app-dev.leverslabs.com/metrics/report",
    }

    # Act
    response = email_notifier.send_notification(
        TemplateType.METRIC_ALERT, dict(region="us-west-1"), mock_email_config, context=context
    )

    # Assert
    mock_email_client.send_email.assert_called_once_with(
        sender="test@example.com",
        recipients=mock_email_config["to"],
        cc=mock_email_config.get("cc"),
        subject=EMAIL_TEMPLATES[TemplateType.METRIC_ALERT]["subject"].format(**context),
        body_html=ANY,
    )
    assert response["status"] is True
    assert response["MessageId"] == "test-id"


@pytest.mark.asyncio
async def test_email_notifier_send_error(mocker, email_notifier, mock_email_config, mock_email_client):
    # Prepare
    mock_template = MagicMock()
    mock_template.render.return_value = "<p>Test email content</p>"
    mocker.patch.object(email_notifier, "get_template", return_value=mock_template)
    mock_email_client.send_email.return_value = (False, None)

    context = {
        "name": "IMP Metrics",
        "grain": "weekly",
        "period": "Nov 18, 2024 — Nov 24, 2024",
        "metrics": [
            {"name": "Signups", "value": "80.3K", "percentage_change": 5.51, "absolute_change": "+4190"},
            {"name": "Pro Trials", "value": "2.77K", "percentage_change": -12.7, "absolute_change": "-401"},
            {
                "name": "New + Reactivated Pro Customers",
                "value": "1.41K",
                "percentage_change": 0.14,
                "absolute_change": "+2",
            },
            {"name": "Churn Customers", "value": "679", "percentage_change": -7.74, "absolute_change": "-57"},
        ],
        "report_link": "https://app-dev.leverslabs.com/metrics/report",
    }

    # Act
    response = email_notifier.send_notification(
        TemplateType.METRIC_ALERT, dict(region="us-west-1"), mock_email_config, context=context
    )

    # Assert
    mock_email_client.send_email.assert_called_once_with(
        sender="test@example.com",
        recipients=mock_email_config["to"],
        cc=mock_email_config.get("cc"),
        subject=EMAIL_TEMPLATES[TemplateType.METRIC_ALERT]["subject"].format(**context),
        body_html=ANY,
    )
    assert response["status"] is False


def test_email_notifier_missing_region():
    config = {}
    notifier = EmailNotifier("templates", sender="test@example.com")

    with pytest.raises(ValueError) as excinfo:
        notifier.get_client(config)

    assert str(excinfo.value) == "AWS region not provided in the configuration."


def test_email_notifier_missing_recipients():
    config = {}
    notifier = EmailNotifier("templates", sender="test@example.com")
    client = MagicMock()
    content = {"subject": "Test", "html": "<p>Test</p>"}

    with pytest.raises(ValueError) as excinfo:
        notifier.send_notification_using_client(client, content, config)

    assert str(excinfo.value) == "Recipients are not provided in the configuration."


def test_email_notifier_get_notification_content(email_notifier):
    template_name = "test_template"
    context = {
        "name": "IMP Metrics",
        "grain": "weekly",
        "period": "Nov 18, 2024 — Nov 24, 2024",
        "metrics": [
            {"name": "Signups", "value": "80.3K", "percentage_change": 5.51, "absolute_change": "+4190"},
            {"name": "Pro Trials", "value": "2.77K", "percentage_change": -12.7, "absolute_change": "-401"},
            {
                "name": "New + Reactivated Pro Customers",
                "value": "1.41K",
                "percentage_change": 0.14,
                "absolute_change": "+2",
            },
            {"name": "Churn Customers", "value": "679", "percentage_change": -7.74, "absolute_change": "-57"},
        ],
        "report_link": "https://app-dev.leverslabs.com/metrics/report",
    }

    with pytest.raises(ValueError) as excinfo:
        email_notifier.get_notification_content(template_name, context)

    assert str(excinfo.value) == "Email template test_template not found"
