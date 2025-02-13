import boto3
from botocore.exceptions import ClientError


class EmailClient:
    """A wrapper around Amazon SES providing email functionality"""

    def __init__(self, region_name: str):
        """Initialize the SES client"""
        self.client = boto3.client("ses", region_name=region_name)

    def send_email(
        self,
        sender: str,
        recipients: list[str],
        subject: str,
        body_text: str | None = None,
        body_html: str | None = None,
        cc: list[str] | None = None,
    ) -> tuple[bool, dict | None]:
        """
        Send an email using Amazon SES

        Args:
            sender: The email address of the sender
            recipients: List of recipient email addresses
            subject: Email subject line
            body_text: Optional plain text version of the email
            body_html: Optional HTML version of the email
            cc: Optional list of CC recipients

        Returns:
            bool: True if email was sent successfully
            dict: The response from the SES service

        Raises:
            ClientError: If there's an error sending the email
        """
        # Prepare the email message
        message: dict = {"Subject": {"Data": subject, "Charset": "UTF-8"}}

        # Add body content
        body = {}
        if body_text:
            body["Text"] = {"Data": body_text, "Charset": "UTF-8"}
        if body_html:
            body["Html"] = {"Data": body_html, "Charset": "UTF-8"}

        if not body:
            raise ValueError("Either body_text or body_html must be provided")

        message["Body"] = body

        # Prepare destination
        destination = {"ToAddresses": recipients}

        if cc:
            destination["CcAddresses"] = cc

        try:
            response = self.client.send_email(Source=sender, Destination=destination, Message=message)
            return True, response
        except ClientError:
            # Log the error here if needed
            return False, None

    def get_send_quota(self) -> dict:
        """
        Get the sending limits for the Amazon SES account

        Returns:
            dict: Contains max_24_hour_send, max_send_rate, and sent_last_24_hours
        """
        try:
            response = self.client.get_send_quota()
            return {
                "max_24_hour_send": response["Max24HourSend"],
                "max_send_rate": response["MaxSendRate"],
                "sent_last_24_hours": response["SentLast24Hours"],
            }
        except ClientError:
            return {}
