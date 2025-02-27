from unittest.mock import MagicMock

import pytest
from jinja2 import Template, TemplateError

from commons.notifiers import BaseNotifier


def test_base_notifier_is_abstract():
    with pytest.raises(TypeError):
        BaseNotifier({})


def test_base_notifier_requires_implementations():
    class ConcreteNotifier(BaseNotifier):
        channel = "CUSTOM"

    with pytest.raises(TypeError):
        ConcreteNotifier({})


def test_create_template():
    class ConcreteNotifier(BaseNotifier):
        channel = "CUSTOM"

        def get_client(self, config):
            return MagicMock()

        def get_notification_content(self, template_config, context):
            return {"subject": "Test", "html": "Test"}

        def send_notification_using_client(self, client, content, channel_config):
            return {"status": "success"}

    notifier = ConcreteNotifier({})
    template_content = "Hello {{ name }}!"
    template = notifier.create_template(template_content)
    assert isinstance(template, Template)


def test_render_template_success():
    class ConcreteNotifier(BaseNotifier):
        channel = "CUSTOM"

        def get_client(self, config):
            return MagicMock()

        def get_notification_content(self, template_config, context):
            return {"subject": "Test", "html": "Test"}

        def send_notification_using_client(self, client, content, channel_config):
            return {"status": "success"}

    notifier = ConcreteNotifier({})
    template = notifier.create_template("Hello {{ name }}!")
    result = notifier.render_template(template, {"name": "World"})
    assert result == "Hello World!"


def test_render_template_error():
    class ConcreteNotifier(BaseNotifier):
        channel = "CUSTOM"

        def get_client(self, config):
            return MagicMock()

        def get_notification_content(self, template_config, context):
            return {"subject": "Test", "html": "Test"}

        def send_notification_using_client(self, client, content, channel_config):
            return {"status": "success"}

    notifier = ConcreteNotifier({})
    with pytest.raises(TemplateError):
        template = notifier.create_template("Hello {{ name }!")  # Invalid template
        notifier.render_template(template, {"name": "World"})


def test_send_notification_flow():
    class ConcreteNotifier(BaseNotifier):
        channel = "CUSTOM"

        def get_client(self, config):
            return MagicMock()

        def get_notification_content(self, template_config, context):
            return {"subject": "Test Subject", "body": "Test Body"}

        def send_notification_using_client(self, client, content, channel_config):
            assert isinstance(client, MagicMock)
            assert content == {"subject": "Test Subject", "body": "Test Body"}
            assert channel_config == {"key": "value"}
            return {"status": "success", "message_id": "123"}

    notifier = ConcreteNotifier({"api_key": "test"})
    result = notifier.send_notification(
        template_config={"template": "test"},
        config={"api_key": "test"},
        channel_config={"key": "value"},
        context={"name": "Test"},
    )

    assert result == {"status": "success", "message_id": "123"}
