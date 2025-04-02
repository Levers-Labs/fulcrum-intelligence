import logging

from commons.db.signals import EventAction, EventTiming, subscribe
from insights_backend.notifications.models import Report
from insights_backend.notifications.services.deployment_manager import PrefectDeploymentManager

logger = logging.getLogger(__name__)


@subscribe(EventAction.CREATE, sender=Report, timing=EventTiming.BEFORE)
def handle_creation(sender, instance: Report, **kwargs):
    if not instance.is_published:
        logger.debug("Report %s is not published, skipping deployment creation", instance.id)
        return

    if instance.deployment_id:
        logger.debug("Report %s already has deployment ID %s", instance.id, instance.deployment_id)
        return

    deployment_manager = PrefectDeploymentManager()

    deployment_id = deployment_manager.create_deployment(instance)
    instance.deployment_id = deployment_id
    logger.info("Created Prefect deployment for Report %s: %s", instance.id, deployment_id)


@subscribe(EventAction.UPDATE, sender=Report, timing=EventTiming.BEFORE)
def handle_update(sender, instance: Report, history: dict, **kwargs):
    old_is_published = history.get("is_published", {}).get("old", instance.is_published)
    old_schedule = history.get("schedule", {}).get("old") if hasattr(instance, "schedule") else None
    old_deployment_id = history.get("deployment_id", {}).get("old")

    deployment_manager = PrefectDeploymentManager()
    # Handle unpublishing
    if not instance.is_published and old_deployment_id:
        if deployment_manager.delete_deployment(old_deployment_id):
            instance.deployment_id = None
            logger.info("Deleted Prefect deployment %s for Report %s", old_deployment_id, instance.id)
        return

    # Handle new publication or schedule update
    if instance.is_published:
        schedule_changed = (
            hasattr(instance, "schedule") and old_schedule is not None and str(instance.schedule) != str(old_schedule)
        )
        newly_published = not old_is_published and instance.is_published

        if schedule_changed or newly_published:
            # Create/Update deployment with current settings
            deployment_id = deployment_manager.create_deployment(instance)
            if deployment_id:
                instance.deployment_id = deployment_id
                logger.info(
                    "Created new Prefect deployment for Report %s: %s (active: %s)",
                    instance.id,
                    deployment_id,
                    instance.is_active,
                )


@subscribe(EventAction.STATUS_CHANGE, sender=Report, timing=EventTiming.AFTER)
def handle_report_status_change(sender, instance: Report, **kwargs):
    """Handle activation change for a report."""
    if not instance.deployment_id:
        logger.debug("Report %s has no deployment ID, skipping activation change", instance.id)
        return
    deployment_manager = PrefectDeploymentManager()
    schedules = deployment_manager.read_deployment_schedules(instance.deployment_id)
    if not schedules:
        logger.error("No schedules found for deployment %s", instance.deployment_id)
        return
    schedule = schedules[0]
    schedule["active"] = instance.is_active
    is_success = deployment_manager.update_deployment_schedule(instance.deployment_id, schedule["id"], schedule)
    if is_success:
        logger.info(
            "%s Prefect deployment schedule %s for report %s",
            "Enabled" if instance.is_active else "Disabled",
            instance.deployment_id,
            instance.id,
        )
    else:
        logger.error("Failed to update Prefect deployment schedule %s for %s", schedule["id"], instance.deployment_id)


@subscribe(EventAction.DELETE, sender=Report, timing=EventTiming.AFTER)
def handle_report_link_deletion(sender, instance: Report, **kwargs):
    if not instance.deployment_id:
        logger.debug("Report %s has no deployment ID, skipping deletion", instance.id)
        return
    deployment_manager = PrefectDeploymentManager()
    deployment_manager.delete_deployment(instance.deployment_id)
    logger.info("Deleted Prefect deployment %s for report %s", instance.deployment_id, instance.id)
