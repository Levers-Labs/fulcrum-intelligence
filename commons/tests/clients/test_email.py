from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from commons.clients.email import EmailClient

pytestmark = pytest.mark.asyncio


@pytest.fixture(name="mock_ses_client")
def mock_ses_client_fixture():
    with patch("commons.clients.email.boto3.client") as mock_client:
        yield mock_client


@pytest.fixture(name="email_client")
def email_client_fixture(mock_ses_client):
    return EmailClient("us-east-1")


def test_send_email_basic(email_client, mock_ses_client):
    # Mock successful response from SES
    mock_ses_client.return_value.send_email.return_value = {"MessageId": "test-message-id"}

    success, response = email_client.send_email(
        sender="test@example.com", recipients=["recipient@example.com"], subject="Test Subject", body_text="Hello World"
    )

    assert success is True
    assert response["MessageId"] == "test-message-id"
    mock_ses_client.return_value.send_email.assert_called_once()


def test_send_email_with_html(email_client, mock_ses_client):
    mock_ses_client.return_value.send_email.return_value = {"MessageId": "test-message-id"}

    success, response = email_client.send_email(
        sender="test@example.com",
        recipients=["recipient@example.com"],
        subject="Test Subject",
        body_html="<p>Hello World</p>",
    )

    assert success is True
    assert response["MessageId"] == "test-message-id"


def test_send_email_with_cc(email_client, mock_ses_client):
    mock_ses_client.return_value.send_email.return_value = {"MessageId": "test-message-id"}

    success, response = email_client.send_email(
        sender="test@example.com",
        recipients=["recipient@example.com"],
        subject="Test Subject",
        body_text="Hello World",
        cc=["cc@example.com"],
    )

    assert success is True
    assert response["MessageId"] == "test-message-id"


def test_send_email_failure(email_client, mock_ses_client):
    # Mock ClientError from SES
    mock_ses_client.return_value.send_email.side_effect = ClientError(
        {"Error": {"Code": "MessageRejected", "Message": "Email address not verified"}}, "SendEmail"
    )

    success, response = email_client.send_email(
        sender="test@example.com", recipients=["recipient@example.com"], subject="Test Subject", body_text="Hello World"
    )

    assert success is False
    assert response is None


def test_send_email_no_body(email_client):
    with pytest.raises(ValueError) as exc_info:
        email_client.send_email(sender="test@example.com", recipients=["recipient@example.com"], subject="Test Subject")

    assert str(exc_info.value) == "Either body_text or body_html must be provided"


def test_get_send_quota_success(email_client, mock_ses_client):
    mock_ses_client.return_value.get_send_quota.return_value = {
        "Max24HourSend": 50000.0,
        "MaxSendRate": 14.0,
        "SentLast24Hours": 147.0,
    }

    quota = email_client.get_send_quota()

    assert quota["max_24_hour_send"] == 50000.0
    assert quota["max_send_rate"] == 14.0
    assert quota["sent_last_24_hours"] == 147.0


def test_get_send_quota_failure(email_client, mock_ses_client):
    mock_ses_client.return_value.get_send_quota.side_effect = ClientError(
        {"Error": {"Code": "Error", "Message": "Error getting quota"}}, "GetSendQuota"
    )

    quota = email_client.get_send_quota()

    assert quota == {}
