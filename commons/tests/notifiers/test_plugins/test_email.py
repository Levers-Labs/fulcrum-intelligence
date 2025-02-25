from unittest.mock import ANY, MagicMock

import pytest

from commons.notifiers.plugins import EmailNotifier


@pytest.fixture
def email_notifier(mocker, mock_email_client):
    notifier = EmailNotifier({"sender": "test@example.com"})
    mocker.patch.object(notifier, "get_client", return_value=mock_email_client)
    return notifier


@pytest.mark.asyncio
async def test_email_notifier_send_success(mocker, email_notifier, mock_email_config, mock_email_client):
    # Prepare
    mock_template = MagicMock()
    mock_template.render.return_value = "<p>Test email content</p>"
    mocker.patch.object(email_notifier, "create_template", return_value=mock_template)
    mock_email_client.send_email.return_value = (True, {"MessageId": "test-id", "status": True})

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

    template_config = {
        "type": "METRIC_ALERT",
        "subject": "Weekly IMP Metrics Report - {period}",
        "body": "<h1>Weekly IMP Metrics Report</h1><p>Period: {{ period }}</p>",
    }

    # Act
    response = email_notifier.send_notification(
        template_config=template_config,
        config={"region": "us-west-1"},
        channel_config=mock_email_config,
        context=context,
    )

    # Assert
    mock_email_client.send_email.assert_called_once_with(
        sender=email_notifier.sender,
        recipients=mock_email_config["to"],
        cc=mock_email_config.get("cc"),
        subject=template_config["subject"].format(**context),
        body_html=ANY,
    )
    assert response["status"] is True
    assert response["MessageId"] == "test-id"


@pytest.mark.asyncio
async def test_email_notifier_send_error(mocker, email_notifier, mock_email_config, mock_email_client):
    # Prepare
    mock_template = MagicMock()
    mock_template.render.return_value = "<p>Test email content</p>"
    mocker.patch.object(email_notifier, "create_template", return_value=mock_template)
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

    template_config = {
        "type": "METRIC_ALERT",
        "subject": "Weekly IMP Metrics Report - {period}",
        "body": "<h1>Weekly IMP Metrics Report</h1><p>Period: {{ period }}</p>",
    }

    # Act
    response = email_notifier.send_notification(
        template_config=template_config,
        config={"region": "us-west-1"},
        channel_config=mock_email_config,
        context=context,
    )

    # Assert
    mock_email_client.send_email.assert_called_once_with(
        sender=email_notifier.sender,
        recipients=mock_email_config["to"],
        cc=mock_email_config.get("cc"),
        subject=template_config["subject"].format(**context),
        body_html=ANY,
    )
    assert response["status"] is False


def test_email_notifier_missing_sender():
    with pytest.raises(ValueError) as excinfo:
        notifier = EmailNotifier({})
        notifier.get_client({})

    assert str(excinfo.value) == "Sender email not provided in the configuration."


def test_email_notifier_missing_region():
    config = {}
    notifier = EmailNotifier({"sender": "test@example.com"})

    with pytest.raises(ValueError) as excinfo:
        notifier.get_client(config)

    assert str(excinfo.value) == "AWS region not provided in the configuration."


def test_email_notifier_missing_recipients():
    config = {}
    notifier = EmailNotifier({"sender": "test@example.com"})
    client = MagicMock()
    content = {"subject": "Test", "html": "<p>Test</p>"}

    with pytest.raises(ValueError) as excinfo:
        notifier.send_notification_using_client(client, content, config)

    assert str(excinfo.value) == "Recipients are not provided in the configuration."


def test_email_notifier_get_notification_content(email_notifier):
    template_config = {
        "type": "CUSTOM",
        "subject": "Test Subject - {period}",
        "body": "<h1>Test Email</h1><p>Period: {{ period }}</p>",
    }

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

    content = email_notifier.get_notification_content(template_config, context)
    assert isinstance(content, dict)
    assert "subject" in content
    assert "html" in content


def test_email_notifier_missing_template_config():
    notifier = EmailNotifier({"sender": "test@example.com"})

    # Test missing subject
    template_config_no_subject = {
        "type": "CUSTOM",
        "body": "<h1>Test Email</h1>",
    }
    with pytest.raises(ValueError) as excinfo:
        notifier.get_notification_content(template_config_no_subject, {})
    assert str(excinfo.value) == "Template configuration must include both subject and body"

    # Test missing body
    template_config_no_body = {
        "type": "CUSTOM",
        "subject": "Test Subject",
    }
    with pytest.raises(ValueError) as excinfo:
        notifier.get_notification_content(template_config_no_body, {})
    assert str(excinfo.value) == "Template configuration must include both subject and body"
