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
            "grains": [grain],
        }
        response = await self.post(endpoint="analyze/process-control", data=params)
        if not response:
            return []
        # get the response matching the grain
        grain_response = next((res for res in response if res["grain"] == grain), None)  # type: ignore

        return grain_response["results"]  # type: ignore
