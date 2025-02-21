import logging

from commons.db.signals import EventAction, EventTiming, subscribe
from insights_backend.notifications.models import Report
from insights_backend.notifications.services.deployment_manager import PrefectDeploymentManager

logger = logging.getLogger(__name__)


@subscribe(EventAction.CREATE, sender=Report, timing=EventTiming.BEFORE)
async def handle_creation(sender, instance: Report, **kwargs):
    if not instance.is_published:
        logger.debug("Report %s is not published, skipping deployment creation", instance.id)
        return

    if instance.deployment_id:
        logger.debug("Report %s already has deployment ID %s", instance.id, instance.deployment_id)
        return

    deployment_manager = PrefectDeploymentManager()

    deployment_id = await deployment_manager.create_deployment(instance)
    if deployment_id:
        instance.deployment_id = deployment_id
        logger.info("Created Prefect deployment for Report %s: %s", instance.id, deployment_id)


@subscribe(EventAction.UPDATE, sender=Report, timing=EventTiming.BEFORE)
async def handle_update(sender, instance: Report, history: dict, **kwargs):
    old_is_published = history.get("is_published", {}).get("old", instance.is_published)
    old_is_active = history.get("is_active", {}).get("old", instance.is_active)
    old_schedule = history.get("schedule", {}).get("old") if hasattr(instance, "schedule") else None
    old_deployment_id = history.get("deployment_id", {}).get("old")

    deployment_manager = PrefectDeploymentManager()
    # Handle unpublishing
    if not instance.is_published and old_deployment_id:
        if await deployment_manager.delete_deployment(old_deployment_id):
            instance.deployment_id = None
            logger.info("Deleted Prefect deployment %s for Report %s", old_deployment_id, instance.id)
        return

    # Handle new publication, schedule update, or active status change
    if instance.is_published:
        schedule_changed = (
            hasattr(instance, "schedule") and old_schedule is not None and str(instance.schedule) != str(old_schedule)
        )
        newly_published = not old_is_published and instance.is_published
        active_changed = old_is_active != instance.is_active

        if schedule_changed or newly_published or active_changed:
            # Create/Update deployment with current settings
            deployment_id = await deployment_manager.create_deployment(instance)
            if deployment_id:
                instance.deployment_id = deployment_id
                logger.info(
                    "Created new Prefect deployment for Report %s: %s (active: %s)",
                    instance.id,
                    deployment_id,
                    instance.is_active,
                )


@subscribe(EventAction.DELETE, sender=Report, timing=EventTiming.BEFORE)
async def handle_deletion(sender, instance: Report, **kwargs):
    if instance.deployment_id:
        deployment_manager = PrefectDeploymentManager()
        await deployment_manager.delete_deployment(instance.deployment_id)
        logger.info("Deleted Prefect deployment %s for report %s", instance.deployment_id, instance.id)
