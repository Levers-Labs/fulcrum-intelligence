from unittest.mock import MagicMock

import pytest

from commons.notifiers import BaseNotifier
from commons.notifiers.factory import NotifierFactory


class TestNotifier(BaseNotifier):
    channel = "TEST"

    def get_client(self, config):
        return MagicMock()

    def get_notification_content(self, template_config, context):
        return {"subject": "Test", "html": "Test"}

    def send_notification_using_client(self, client, content, channel_config):
        return {"status": "success"}


def test_notifier_factory_create_custom(mock_config):
    class CustomNotifier(BaseNotifier):
        channel = "CUSTOM"

        def get_client(self, config):
            return MagicMock()

        def get_notification_content(self, template_config, context):
            return {"subject": "Test", "html": "Test"}

        def send_notification_using_client(self, client, content, channel_config):
            return {"status": "success"}

    notifier = NotifierFactory.create_notifier("CUSTOM", {})
    assert isinstance(notifier, CustomNotifier)


def test_notifier_factory_create_unsupported():
    with pytest.raises(ValueError) as excinfo:
        NotifierFactory.create_notifier("unsupported", {})
    assert str(excinfo.value) == "No notifier found for channel unsupported"


def test_notifier_factory_type_hints():
    """Test that the factory properly handles type hints and generics"""
    factory = NotifierFactory[TestNotifier]()
    assert factory.base_class == BaseNotifier
    assert factory.plugin_module == "plugins"
    assert isinstance(factory.bag, dict)


def test_get_channel_notifier_success():
    """Test getting a notifier for a valid channel"""
    NotifierFactory.bag = {"TEST": TestNotifier}
    notifier_class = NotifierFactory.get_channel_notifier("TEST")
    assert notifier_class == TestNotifier


def test_get_channel_notifier_invalid():
    """Test getting a notifier for an invalid channel"""
    NotifierFactory.bag = {"TEST": TestNotifier}
    with pytest.raises(ValueError) as excinfo:
        NotifierFactory.get_channel_notifier("CUSTOM")
    assert str(excinfo.value) == "No notifier found for channel CUSTOM"


def test_create_notifier_with_config():
    """Test creating a notifier instance with configuration"""
    NotifierFactory.bag = {"TEST": TestNotifier}
    config = {"api_key": "test_key"}
    notifier = NotifierFactory.create_notifier("TEST", config)
    assert isinstance(notifier, TestNotifier)
    assert notifier.config == config
