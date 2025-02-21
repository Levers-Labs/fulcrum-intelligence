from datetime import datetime
from typing import Any

from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.schemas import ReportRequest
from insights_backend.notifications.services.preview.base import BasePreviewService


class ReportPreviewService(BasePreviewService[ReportRequest]):
    """Service for previewing report notifications"""

    def __init__(self, template_service):
        super().__init__(template_service, NotificationType.REPORT)

    async def _generate_context(self, report_data: ReportRequest) -> dict[str, Any]:
        """Generate mock context data for report template rendering"""
        metrics = self._generate_metrics(report_data)

        return {
            "report_name": report_data.name,
            "grain": report_data.grain.value,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "metrics": metrics,
            "metric": (
                {"id": ",".join(report_data.config.metric_ids)}
                if report_data.config.metric_ids
                else {"id": "default_metric"}
            ),
        }

    def _generate_metrics(self, report_data: ReportRequest) -> list[dict[str, Any]]:
        """Generate metrics data based on user's configuration"""
        if report_data.config and report_data.config.metric_ids:
            # Generate data for user-selected metrics
            return [
                {
                    "id": metric_id,
                    "label": metric_id,  # Using ID as label since we don't have labels in config
                    "value": self.faker.random_int(min=-10000, max=10000),
                }
                for metric_id in report_data.config.metric_ids
            ]

        # If no metrics configured, generate some fake ones
        return [
            {
                "id": f"metric_{i}",
                "label": self.faker.word().title(),
                "value": self.faker.random_int(min=-10000, max=10000),
            }
            for i in range(3)  # Generate 3 fake metrics
        ]
