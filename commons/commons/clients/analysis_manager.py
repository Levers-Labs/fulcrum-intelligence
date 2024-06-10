from datetime import date
from typing import Any

from commons.clients.base import AsyncHttpClient
from commons.models.enums import Granularity


class AnalysisManagerClient(AsyncHttpClient):
    """
    Analysis Manager Client used to interact with the Analysis Manager API.
    """

    async def perform_process_control(
        self, metric_id: str, start_date: date, end_date: date, grain: Granularity
    ) -> list[dict[str, Any]]:
        """
        Perform process control for a given metric id and date range.
        metric_id: if of the metric
        start_date: start date of the time series
        end_date: end date of the time series
        grain: granularity of the data
        """
        params = {
            "metric_id": metric_id,
            "start_date": start_date,
            "end_date": end_date,
            "grain": grain.value,
        }
        response = await self.post(endpoint="analyze/process-control", data=params)
        return response  # type: ignore

    async def get_significant_segment(
        self,
        metric_id: str,
        evaluation_start_date: date,
        evaluation_end_date: date,
        comparison_start_date: date,
        comparison_end_date: date,
        dimensions: list[str],
    ):
        params = {
            "metric_id": metric_id,
            "evaluation_start_date": evaluation_start_date,
            "evaluation_end_date": evaluation_end_date,
            "comparison_start_date": comparison_start_date,
            "comparison_end_date": comparison_end_date,
            "dimensions": dimensions,
        }
        response = await self.post(endpoint="analyze/significant/segment", data=params)
        return response  # type: ignore
