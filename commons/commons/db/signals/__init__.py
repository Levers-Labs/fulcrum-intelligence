from enum import Enum


# Enum definitions
class EventAction(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class EventTiming(str, Enum):
    BEFORE = "before"
    AFTER = "after"


# Imports
from .publisher import publish_event  # noqa
from .blinker_events import *  # noqa
from .dispatcher import subscribe  # noqa
