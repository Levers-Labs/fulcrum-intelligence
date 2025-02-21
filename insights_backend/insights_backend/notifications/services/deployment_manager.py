import logging

from commons.clients.prefect import PrefectClient, PrefectDeployment
from insights_backend.config import get_settings
from insights_backend.notifications.models import Report
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

    async def create_deployment(self, instance: Report) -> str | None:
        """Create a new Prefect deployment."""
        model_type = type(instance)
        try:
            flow_config = FLOW_CONFIGS[model_type]  # type: ignore

            deployment = PrefectDeployment.create_with_defaults(
                name=f"{flow_config['flow_name']}-id-{instance.id}",  # type: ignore
                flow_name=flow_config["flow_name"],  # type: ignore
                entrypoint=flow_config["entrypoint"],  # type: ignore
                schedule=str(instance.schedule) if getattr(instance, "schedule", None) else None,
                timezone=instance.schedule.timezone if getattr(instance, "schedule", None) else None,
                is_active=instance.is_active,
                parameter_schema=flow_config["parameter_schema"],  # type: ignore
                parameters={"tenant_id": instance.tenant_id, "report_id": instance.id},
            )
            result = await self.prefect.create_deployment(deployment)
            return result["id"]
        except Exception as e:
            logger.error(
                "Failed to create Prefect deployment for %s %s: %s",
                model_type.__name__.lower(),
                instance.id,
                str(e),
            )
            return None

    async def delete_deployment(self, deployment_id: str) -> bool:
        """Delete a Prefect deployment."""
        try:
            await self.prefect.delete_deployment(deployment_id)
            return True
        except Exception as e:
            logger.error("Failed to delete Prefect deployment %s: %s", deployment_id, str(e))
            return False
