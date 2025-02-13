from unittest.mock import MagicMock

import pytest

from commons.notifiers import BaseNotifier
from commons.notifiers.factory import NotifierFactory


def test_notifier_factory_create_slack(mock_config):
    class CustomNotifier(BaseNotifier):
        channel = "CUSTOM"

        def get_client(self, config):
            return MagicMock()

        def get_notification_content(self, template_name, context):
            return {"subject": "Test", "html": "Test"}

        def send_notification_using_client(self, **kwargs):
            return {"status": "success"}

    notifier = NotifierFactory.create_notifier("CUSTOM", "templates")  # type: ignore # noqa
    assert isinstance(notifier, CustomNotifier)


def test_notifier_factory_create_unsupported():
    with pytest.raises(ValueError) as excinfo:
        NotifierFactory.create_notifier("unsupported", "test")  # noqa
    assert str(excinfo.value) == "No notifier found for channel unsupported"
