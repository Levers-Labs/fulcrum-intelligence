import logging

from commons.db.signals import EventAction, EventTiming, subscribe
from insights_backend.notifications.models import Report

logger = logging.getLogger(__name__)


@subscribe(EventAction.CREATE, sender=Report, timing=EventTiming.BEFORE)
def handle_prefect_deployment_for_report_creation(sender, instance, history, **kwargs):  # noqa
    logger.debug("Creating prefect deployment for the report: %s", instance.id)
