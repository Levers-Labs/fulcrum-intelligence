from datetime import date, datetime
from typing import Any

from prefect import get_run_logger

from commons.clients.query_manager import QueryManagerClient
from commons.clients.story_manager import StoryManagerClient
from commons.models.enums import Granularity
from commons.utilities.grain_utils import GrainPeriodCalculator
from tasks_manager.config import AppConfig
from tasks_manager.utils import get_client_auth_from_config


class AlertExecutionService:
    """
    Service for executing alerts based on different trigger types.
    Handles data preparation for different alert triggers and formats context for notifications.
    """

    def __init__(
        self,
        query_client: QueryManagerClient,
        story_client: StoryManagerClient,
    ):
        self.query_client = query_client
        self.story_client = story_client
        self.logger = get_run_logger()

    async def prepare_alert_data(
        self,
        alert_config: dict[str, Any],
        trigger_params: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Prepare alert data based on trigger type and parameters
        Args:
            alert_config: The alert configuration including trigger details
            trigger_params: Parameters from what triggered the alert
        Returns:
            Dictionary containing the prepared data for notifications
        """
        trigger_type = alert_config["trigger"]["type"]

        if trigger_type == "METRIC_STORY":
            return await self._prepare_story_alert_data(alert_config, trigger_params)
        elif trigger_type == "METRIC_THRESHOLD":
            return await self._prepare_threshold_alert_data(alert_config, trigger_params)
        else:
            raise ValueError(f"Unsupported trigger type: {trigger_type}")

    async def _prepare_story_alert_data(
        self,
        alert_config: dict[str, Any],
        execution_params: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Prepare data for story-based alerts
        Args:
            alert_config: The alert configuration
            execution_params: Parameters including story_date, metric_id, and story_groups
        Returns:
            Dictionary containing stories and related data for notifications
        Raises:
            ValueError: If no stories are found for the given parameters
        """
        story_date = execution_params["story_date"]
        metric_id = execution_params["metric_id"]
        story_groups = execution_params.get("story_groups")
        grain = alert_config["grain"]

        # Fetch stories from the story service
        stories = await self._fetch_stories(
            metric_id=metric_id,
            grain=Granularity(grain),
            story_date=story_date,
            story_groups=story_groups,
        )

        # Raise error if no stories found
        if not stories:
            self.logger.warning(
                "No stories found for metric %s on %s with grain %s and story groups %s",
                metric_id,
                story_date,
                grain,
                story_groups,
            )
            raise ValueError("No stories found for the given parameters")

        # Get metric details
        metric = await self.query_client.get_metric(metric_id)

        return {
            "stories": stories,
            "metric": metric,
            "fetched_at": datetime.now().strftime("%b %d, %Y"),
            "grain": grain,
            "date_label": GrainPeriodCalculator.generate_date_label(Granularity(grain), datetime.today()),
        }

    async def _prepare_threshold_alert_data(
        self,
        alert_config: dict[str, Any],
        execution_params: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Prepare data for threshold-based alerts (placeholder for future implementation)
        Args:
            alert_config: The alert configuration
            execution_params: Parameters for threshold evaluation
        Returns:
            Dictionary containing threshold evaluation data for notifications
        """
        # TODO: Implement threshold alert data preparation
        raise NotImplementedError("Threshold alerts not yet implemented")

    async def _fetch_stories(
        self,
        metric_id: str,
        grain: Granularity,
        story_date: date,
        story_groups: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch stories from the story service
        Args:
            metric_id: The metric ID
            grain: Time granularity
            story_date: The date for which to fetch stories
            story_groups: Optional list of story groups to filter by
        Returns:
            List of stories
        """
        response = await self.story_client.list_stories(
            metric_ids=[metric_id],
            grains=[grain],
            story_date_start=story_date,
            story_date_end=story_date,
            story_groups=story_groups,
        )
        return response.get("results", [])


async def get_alert_execution_service() -> AlertExecutionService:
    """Factory function to create AlertExecutionService instance"""
    config = await AppConfig.load("default")
    auth = get_client_auth_from_config(config)

    query_client = QueryManagerClient(config.query_manager_server_host, auth=auth)
    story_client = StoryManagerClient(config.story_manager_server_host, auth=auth)

    return AlertExecutionService(query_client, story_client)
