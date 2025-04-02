import logging
from typing import Any

from commons.clients.prefect import PrefectClient, PrefectDeployment
from commons.exceptions import PrefectOperationError
from insights_backend.config import get_settings
from insights_backend.notifications.models import Report, ScheduleConfig
from insights_backend.notifications.schemas import MetricTaskParameters

logger = logging.getLogger(__name__)

FLOW_CONFIGS = {
    Report: {
        "flow_name": "metric-reports",
        "entrypoint": "tasks_manager.flows.reports:deliver_metric_reports",
        "parameter_schema": MetricTaskParameters,
    },
}


class PrefectDeploymentManager:
    """Manages Prefect deployments for notifications (alerts and reports)."""

    def __init__(self):
        settings = get_settings()
        self.prefect = PrefectClient(settings.PREFECT_API_URL, settings.PREFECT_API_TOKEN)

    def create_deployment(self, instance: Report) -> str | None:
        """Create a new Prefect deployment."""
        model_type = type(instance)
        try:
            flow_config = FLOW_CONFIGS[model_type]  # type: ignore
            schedule = None
            timezone = None
            if getattr(instance, "schedule", None) is not None:
                schedule_config = ScheduleConfig.parse_obj(instance.schedule)
                schedule = str(schedule_config)
                timezone = schedule_config.timezone

            deployment = PrefectDeployment.create_with_defaults(
                name=f"{flow_config['flow_name']}-id-{instance.id}",  # type: ignore
                flow_name=flow_config["flow_name"],  # type: ignore
                entrypoint=flow_config["entrypoint"],  # type: ignore
                schedule=schedule,
                timezone=timezone,
                is_active=instance.is_active,
                parameter_schema=flow_config["parameter_schema"],  # type: ignore
                parameters={"tenant_id": instance.tenant_id, "report_id": instance.id},
            )
            result = self.prefect.create_deployment(deployment)
            return result["id"]
        except Exception as e:
            logger.error(
                "Failed to create Prefect deployment for %s %s: %s",
                model_type.__name__.lower(),
                instance.id,
                str(e),
            )
            raise PrefectOperationError(
                operation="create",
                detail=f"Unable to create deployment for {model_type.__name__.lower()} {instance.id}: {str(e)}",
            ) from e

    def delete_deployment(self, deployment_id: str) -> bool:
        """Delete a Prefect deployment."""
        try:
            self.prefect.delete_deployment(deployment_id)
            return True
        except Exception as e:
            logger.error("Failed to delete Prefect deployment %s: %s", deployment_id, str(e))
            return False

    def read_deployment_schedules(self, deployment_id: str) -> list[dict[str, Any]]:
        """Read the schedules for a Prefect deployment."""
        try:
            return self.prefect.read_deployment_schedules(deployment_id)
        except Exception as e:
            logger.error("Failed to read Prefect deployment schedules for %s: %s", deployment_id, str(e))
            return []

    def update_deployment_schedule(self, deployment_id: str, schedule_id: str, schedule: dict[str, Any]) -> bool:
        """Update the schedule for a Prefect deployment."""
        try:
            self.prefect.update_deployment_schedule(deployment_id, schedule_id, schedule)
            return True
        except Exception as e:
            logger.error(
                "Failed to update Prefect deployment schedule %s for %s: %s", schedule_id, deployment_id, str(e)
            )
            return False
