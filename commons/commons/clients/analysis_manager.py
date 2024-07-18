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

    async def get_segment_drift(
        self,
        metric_id: str,
        evaluation_start_date: date,
        evaluation_end_date: date,
        comparison_start_date: date,
        comparison_end_date: date,
        dimensions: list[str],
    ) -> dict[str, Any]:
        """
        Fetching segment drift data for a given metric id and date range.
        metric_id: metric id for which we want to calculate drift
        evaluation_start_date: Start date of the current time series,
        evaluation_end_date: End date of the current time series,
        comparison_start_date: start date of the past time series for comparison,
        comparison_end_date:  end date of the past time series for comparison,
        dimensions: dimensions of the time series, on which we want to perform the drift calculation
        """
        params = {
            "metric_id": metric_id,
            "evaluation_start_date": evaluation_start_date,
            "evaluation_end_date": evaluation_end_date,
            "comparison_start_date": comparison_start_date,
            "comparison_end_date": comparison_end_date,
            "dimensions": dimensions,
        }
        response = await self.post(endpoint="analyze/drift/segment", data=params)
        return response

    async def get_component_drift(
        self,
        metric_id: str,
        evaluation_start_date: date,
        evaluation_end_date: date,
        comparison_start_date: date,
        comparison_end_date: date,
    ) -> dict[str, Any]:
        """
        Fetching component drift data for a given metric id and date range.
        metric_id: metric id for which we want to calculate drift
        evaluation_start_date: Start date of the current time series,
        evaluation_end_date: End date of the current time series,
        comparison_start_date: start date of the prior time series for comparison,
        comparison_end_date:  end date of the prior time series for comparison,
        """
        params = {
            "metric_id": metric_id,
            "evaluation_start_date": evaluation_start_date,
            "evaluation_end_date": evaluation_end_date,
            "comparison_start_date": comparison_start_date,
            "comparison_end_date": comparison_end_date,
        }
        response = await self.post(endpoint="analyze/drift/component", data=params)
        return response
