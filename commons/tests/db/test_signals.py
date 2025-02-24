from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from blinker import signal
from sqlalchemy.orm.attributes import History

from commons.db.models import BaseDBModel
from commons.db.signals import (
    EventAction,
    EventTiming,
    publish_event,
    subscribe,
)
from commons.db.signals.blinker_events import (
    base_model_after_change,
    base_model_after_delete,
    base_model_after_insert,
    base_model_before_change,
    base_model_before_delete,
    base_model_before_insert,
    get_history,
    send_blinker_event,
)


# Test Model
class TestModel(BaseDBModel):
    __tablename__ = "test_model"
    name: str
    value: int


@pytest.fixture
def test_instance():
    return TestModel(id=1, name="test", value=42)


@pytest.fixture
def mock_inspect():
    """Mock SQLAlchemy inspect functionality."""
    mock_attr = MagicMock()
    mock_attr.history = History(added=["updated"], deleted=["test"], unchanged=[])
    mock_attr.key = "name"
    mock_insp = MagicMock()
    mock_insp.attrs = [mock_attr]
    mock_insp.has_changes = True
    return mock_insp


def test_event_action_enum():
    """Test EventAction enum values."""
    assert EventAction.CREATE == "create"
    assert EventAction.UPDATE == "update"
    assert EventAction.DELETE == "delete"
    assert len(EventAction) == 4


def test_event_timing_enum():
    """Test EventTiming enum values."""
    assert EventTiming.BEFORE == "before"
    assert EventTiming.AFTER == "after"
    assert len(EventTiming) == 2


def test_publish_event():
    """Test event publishing functionality."""
    # Arrange
    mock_receiver = MagicMock()
    signal("create_after").connect(mock_receiver)

    # Act
    publish_event(
        action=EventAction.CREATE,
        sender=TestModel,
        timing=EventTiming.AFTER,
        instance=TestModel(id=1, name="test", value=42),
    )

    # Assert
    mock_receiver.assert_called_once()
    call_args = mock_receiver.call_args[1]
    assert call_args["action"] == EventAction.CREATE
    assert call_args["timing"] == EventTiming.AFTER
    assert isinstance(call_args["timestamp"], datetime)


def test_subscribe_single_action():
    """Test subscription to a single event action."""
    mock_handler = MagicMock()

    @subscribe(EventAction.CREATE, TestModel)
    def event_handler(*args, **kwargs):
        mock_handler(*args, **kwargs)

    # Trigger the event
    publish_event(EventAction.CREATE, TestModel, EventTiming.AFTER)

    # Assert
    mock_handler.assert_called_once()


def test_subscribe_multiple_actions():
    """Test subscription to multiple event actions."""
    mock_handler = MagicMock()

    @subscribe([EventAction.CREATE, EventAction.UPDATE], TestModel)
    def event_handler(*args, **kwargs):
        mock_handler(*args, **kwargs)

    # Trigger events
    publish_event(EventAction.CREATE, TestModel, EventTiming.AFTER)
    publish_event(EventAction.UPDATE, TestModel, EventTiming.AFTER)

    # Assert
    assert mock_handler.call_count == 2


def test_get_history(mocker, test_instance, mock_inspect):
    """Test history tracking for model changes."""
    mocker.patch("commons.db.signals.blinker_events.inspect", return_value=mock_inspect)
    # Simulate attribute changes
    test_instance.name = "updated"
    test_instance.value = 100

    # Get history
    history = get_history(test_instance)

    # Assert
    assert "name" in history
    assert history["name"]["old"] == "test"
    assert history["name"]["new"] == "updated"


def test_send_blinker_event_create(test_instance):
    """Test sending create event."""
    mock_publish = MagicMock()
    with patch("commons.db.signals.blinker_events.publish_event", mock_publish):
        send_blinker_event(test_instance, EventAction.CREATE)

        mock_publish.assert_called_once()
        assert mock_publish.call_args[0][0] == EventAction.CREATE
        assert mock_publish.call_args[0][1] == TestModel


def test_send_blinker_event_update_with_changes(mocker, test_instance, mock_inspect):
    """Test sending update event with changes."""
    mocker.patch("commons.db.signals.blinker_events.inspect", return_value=mock_inspect)
    mock_publish = MagicMock()

    with patch("commons.db.signals.blinker_events.publish_event", mock_publish):
        send_blinker_event(test_instance, EventAction.UPDATE)

        mock_publish.assert_called_once()
        call_kwargs = mock_publish.call_args[1]
        assert "history" in call_kwargs
        assert call_kwargs["history"]["name"]["old"] == "test"
        assert call_kwargs["history"]["name"]["new"] == "updated"


def test_send_blinker_event_update_no_changes(test_instance, mocker):
    """Test sending update event with no changes."""
    # Mock inspect to return no changes
    mock_attr = MagicMock()
    mock_attr.history = History(added=[], deleted=[], unchanged=["test"])
    mock_attr.key = "name"
    mock_attr2 = MagicMock()
    mock_attr2.history = History(added=[], deleted=[], unchanged=[42])
    mock_attr.key = "value"
    mock_insp = MagicMock()
    mock_insp.attrs = [mock_attr, mock_attr2]

    mocker.patch("commons.db.signals.blinker_events.inspect", return_value=mock_insp)

    mock_publish = MagicMock()
    with patch("commons.db.signals.blinker_events.publish_event", mock_publish):
        send_blinker_event(test_instance, EventAction.UPDATE)

        mock_publish.assert_not_called()


@pytest.mark.parametrize(
    "event_handler,timing",
    [
        (base_model_before_insert, EventTiming.BEFORE),
        (base_model_after_insert, EventTiming.AFTER),
        (base_model_before_change, EventTiming.BEFORE),
        (base_model_after_change, EventTiming.AFTER),
        (base_model_before_delete, EventTiming.BEFORE),
        (base_model_after_delete, EventTiming.AFTER),
    ],
)
def test_sqlalchemy_event_handlers(mocker, event_handler, timing, test_instance, mock_inspect):
    """Test SQLAlchemy event handlers."""
    mocker.patch("commons.db.signals.blinker_events.inspect", return_value=mock_inspect)
    mock_connection = MagicMock()
    mock_publish = MagicMock()

    with patch("commons.db.signals.blinker_events.publish_event", mock_publish):
        event_handler(None, mock_connection, test_instance)

        mock_publish.assert_called_once()
        call_args = mock_publish.call_args
        assert call_args[1]["timing"] == timing
        assert "connection" in call_args[1]


def test_event_handler_with_timing():
    """Test event handler with specific timing."""
    mock_handler = MagicMock()

    @subscribe(EventAction.CREATE, TestModel, timing=EventTiming.BEFORE)
    def event_handler(*args, **kwargs):
        mock_handler(*args, **kwargs)

    # Trigger events with different timings
    publish_event(EventAction.CREATE, TestModel, EventTiming.BEFORE)
    publish_event(EventAction.CREATE, TestModel, EventTiming.AFTER)

    # Assert only BEFORE event was handled
    assert mock_handler.call_count == 1


def test_multiple_handlers():
    """Test multiple handlers for the same event."""
    mock_handler1 = MagicMock()
    mock_handler2 = MagicMock()

    @subscribe(EventAction.CREATE, TestModel)
    def event_handler1(*args, **kwargs):
        mock_handler1(*args, **kwargs)

    @subscribe(EventAction.CREATE, TestModel)
    def event_handler2(*args, **kwargs):
        mock_handler2(*args, **kwargs)

    # Trigger event
    publish_event(EventAction.CREATE, TestModel, EventTiming.AFTER)

    # Assert both handlers were called
    mock_handler1.assert_called_once()
    mock_handler2.assert_called_once()
