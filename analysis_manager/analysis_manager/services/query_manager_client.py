from datetime import date
from typing import Any

from analysis_manager.core.schema import DimensionRequest
from analysis_manager.utilities.async_http_client import AsyncHttpClient


class QueryManagerClient(AsyncHttpClient):
    METRICS_VALUES_BASE_URL = "metrics/values"
    DATE_FORMAT = "%Y-%m-%d"

    async def get_metric_values(
        self,
        metric_ids: list[str],
        start_date: date,
        end_date: date,
        dimensions: list[DimensionRequest] | None = None,
    ) -> dict[str, Any]:
        """
        Get metric values.
        metric_ids: list of metric ids
        start_date: start date
        end_date: end date
        dimensions: list of dimensions

        Returns: dict
        {
            "id": "29",
            "date": "2024-03-15",
            "dimension": "Geosegmentation",
            "slice": "Americas",
            "value": 0
        }
        """
        start_date_str = start_date.strftime(self.DATE_FORMAT)
        end_date_str = end_date.strftime(self.DATE_FORMAT)
        dimensions_json = [d.model_dump(mode="json") for d in dimensions] if dimensions else None
        data = {
            "metric_ids": metric_ids,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "dimensions": dimensions_json,
        }
        return await self.post(endpoint=self.METRICS_VALUES_BASE_URL, data=data)
