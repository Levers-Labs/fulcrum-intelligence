from datetime import datetime
from typing import Any

from blinker import signal
from celery.utils.time import maybe_make_aware

from commons.db.signals import EventAction, EventTiming


def publish_event(action: EventAction, sender: Any, timing: EventTiming, **kwargs):
    """Publish an event using blinker signals.

    Args:
        action: The event action (CREATE/UPDATE/DELETE)
        sender: The model class that triggered the event
        timing: When the event occurred (BEFORE/AFTER)
        **kwargs: Additional event data
    """
    # Create a unique signal name combining action and timing
    signal_name = f"{action}_{timing}"
    pub = signal(signal_name)

    kwargs.update(action=action, timing=timing, timestamp=maybe_make_aware(datetime.utcnow()))
    return pub.send(sender, **kwargs)
