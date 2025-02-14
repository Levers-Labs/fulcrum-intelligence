import logging
from collections import defaultdict

from sqlalchemy import event, inspect

from commons.db.models import BaseDBModel
from commons.db.signals import EventAction, EventTiming, publish_event

logger = logging.getLogger(__name__)


def get_history(target):
    history = defaultdict(dict)
    for attr in inspect(target).attrs:
        if attr.history.has_changes():
            history[attr.key]["old"] = attr.history.deleted[0] if attr.history.deleted else None
            history[attr.key]["new"] = attr.history.added[0] if attr.history.added else attr.value
    return history


def send_blinker_event(target, action: EventAction, timing: EventTiming = EventTiming.AFTER, **kwargs):
    sender = target.__class__
    # check history
    history = {}
    if action == EventAction.UPDATE:
        history = get_history(target)
        # if no history for update, skip event
        if not history:
            return
    # Send signal event
    publish_event(action, sender, timing=timing, instance=target, history=history, **kwargs)
    logger.debug("Published %s model %s event for id: %s (timing: %s)", sender, action, target.id, timing)


@event.listens_for(BaseDBModel, "before_insert", propagate=True)
def base_model_before_insert(_, connection, target):
    send_blinker_event(target, EventAction.CREATE, timing=EventTiming.BEFORE, connection=connection)


@event.listens_for(BaseDBModel, "after_insert", propagate=True)
def base_model_after_insert(_, connection, target):
    send_blinker_event(target, EventAction.CREATE, timing=EventTiming.AFTER, connection=connection)


@event.listens_for(BaseDBModel, "before_update", propagate=True)
def base_model_before_change(_, connection, target):
    send_blinker_event(target, EventAction.UPDATE, timing=EventTiming.BEFORE, connection=connection)


@event.listens_for(BaseDBModel, "after_update", propagate=True)
def base_model_after_change(_, connection, target):
    send_blinker_event(target, EventAction.UPDATE, timing=EventTiming.AFTER, connection=connection)


@event.listens_for(BaseDBModel, "before_delete", propagate=True)
def base_model_before_delete(_, connection, target):
    send_blinker_event(target, EventAction.DELETE, timing=EventTiming.BEFORE, connection=connection)


@event.listens_for(BaseDBModel, "after_delete", propagate=True)
def base_model_after_delete(_, connection, target):
    send_blinker_event(target, EventAction.DELETE, timing=EventTiming.AFTER, connection=connection)
