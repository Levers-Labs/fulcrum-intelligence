# from datetime import datetime
# from typing import Any
#
# from insights_backend.notifications.enums import NotificationType
# from insights_backend.notifications.schemas import ReportRequest
# from insights_backend.notifications.services.base import BasePreviewService
#
#
# class ReportPreviewService(BasePreviewService[ReportRequest]):
#     """Service for previewing report notifications"""
#
#     def _get_notification_type(self) -> NotificationType:
#         return NotificationType.REPORT
#
#     def _get_email_subject(self, context: dict[str, Any]) -> str:
#         return f"[{context['report_name']}] Report Generated"
#
#     async def _generate_context(self, report_data: ReportRequest) -> dict[str, Any]:
#         """Generate mock context data for report template rendering"""
#         return {
#             "report_name": self.faker.word().title(),
#             "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#             "summary": self._generate_summary(),
#             "metrics": self._generate_metrics(),
#             "insights": self._generate_insights(),
#         }
#
#     def _generate_summary(self) -> dict[str, Any]:
#         """Generate mock report summary"""
#         return {
#             "total_metrics": self.faker.random_int(min=5, max=20),
#             "anomalies": self.faker.random_int(min=1, max=5),
#             "period": f"{self.faker.date()} to {self.faker.date()}",
#         }
#
#     def _generate_metrics(self) -> list[dict[str, Any]]:
#         """Generate mock metrics data"""
#         return [
#             {
#                 "name": self.faker.word().title(),
#                 "value": self.faker.random_int(min=1000, max=100000),
#                 "change": f"{self.faker.random_int(min=-50, max=50)}%",
#                 "status": self.faker.random_element(["up", "down", "stable"]),
#             }
#             for _ in range(5)
#         ]
#
#     def _generate_insights(self) -> list[str]:
#         """Generate mock insights"""
#         return [
#             f"{self.faker.word().title()} increased by {self.faker.random_int(min=1, max=100)}%",
#             f"Unusual trend detected in {self.faker.word().title()}",
#             f"New pattern emerged in {self.faker.word().title()} metrics",
#         ]
