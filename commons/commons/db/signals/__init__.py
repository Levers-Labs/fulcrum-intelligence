from enum import Enum


# Enum definitions
class EventAction(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class EventTiming(str, Enum):
    BEFORE = "before"
    AFTER = "after"


from .blinker_events import *  # noqa
from .dispatcher import subscribe  # noqa

# Imports
from .publisher import publish_event  # noqa
