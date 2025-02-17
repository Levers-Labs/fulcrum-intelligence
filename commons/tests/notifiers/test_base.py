from abc import ABC
from unittest.mock import MagicMock

import pytest

from commons.notifiers import BaseNotifier


def test_base_notifier_is_abstract():
    with pytest.raises(TypeError):
        BaseNotifier("templates")


def test_base_notifier_requires_send_implementation():
    class ConcreteNotifier(BaseNotifier, ABC):
        channel = "CONCRETE"
        pass

    with pytest.raises(TypeError):
        ConcreteNotifier("templates")


def test_base_notifier_with_implementation():
    class ConcreteNotifier(BaseNotifier):
        channel = "CONCRETE"

        def get_client(self, config):
            return MagicMock()

        def get_notification_content(self, template_name, context):
            return {"subject": "Test", "html": "Test"}

        def send_notification_using_client(self, **kwargs):
            return {"status": "success"}

    notifier = ConcreteNotifier("templates")
    assert isinstance(notifier, BaseNotifier)
