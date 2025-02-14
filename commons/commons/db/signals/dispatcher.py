from typing import Any

from blinker import signal

from commons.db.signals import EventAction, EventTiming


def subscribe(
    action: EventAction | list[EventAction] | tuple[EventAction, ...],
    sender: Any,
    timing: EventTiming = EventTiming.AFTER,
    **kwargs,
):
    """Subscribe to one or multiple model events.

    Args:
        action: Single EventAction or list/tuple of EventActions
        sender: The model class to subscribe to
        timing: When to trigger (BEFORE or AFTER, defaults to AFTER)
        **kwargs: Additional connection arguments for blinker
    """

    def _decorator(func):
        if isinstance(action, (list, tuple)):
            for _action in action:
                signal_name = f"{_action}_{timing}"
                signal(signal_name).connect(func, sender=sender, **kwargs)
        else:
            signal_name = f"{action}_{timing}"
            signal(signal_name).connect(func, sender=sender, **kwargs)
        return func

    return _decorator
