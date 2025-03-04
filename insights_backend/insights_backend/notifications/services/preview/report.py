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
        context = {
            "data": {
                "grain": report_data.grain.value.lower(),
                "interval": self.COMMON_VARIABLES[report_data.grain.value.lower()]["interval"],
                "fetched_at": datetime.now().strftime("%b %d, %Y"),
                "start_date": datetime.now().strftime("%b %d, %Y"),
                "end_date": datetime.now().strftime("%b %d, %Y"),
                "metrics": metrics,
            },
            "config": {"name": report_data.name},
        }
        return context

    def _generate_metrics(self, report_data: ReportRequest) -> list[dict[str, Any]]:
        """Generate metrics data based on user's configuration"""
        current_value = self.faker.random_int(min=-10000, max=10000)
        previous_value = self.faker.random_int(min=-10000, max=10000)
        # Generate data for user-selected metrics
        abs_change = current_value - previous_value
        pct_change = round(((current_value - previous_value) / abs(previous_value)) * 100, 2)
        if report_data.config and report_data.config.metric_ids:
            return [
                {
                    "metric_id": metric_id,
                    "metric": {"label": metric_id, "metric_id": metric_id},
                    "previous_value": previous_value,
                    "current_value": current_value,
                    "absolute_change": abs_change,
                    "percentage_change": pct_change,
                    "is_positive": current_value > previous_value,
                }
                for metric_id in report_data.config.metric_ids
            ]

        # If no metrics configured, generate some fake ones
        return [
            {
                "metric_id": f"metric_{i}",
                "metric": {"label": self.faker.word().title(), "metric_id": f"metric_{i}"},
                "previous_value": previous_value,
                "current_value": current_value,
                "absolute_change": abs_change,
                "percentage_change": pct_change,
                "is_positive": abs_change >= 0,
            }
            for i in range(3)  # Generate 3 fake metrics
        ]
