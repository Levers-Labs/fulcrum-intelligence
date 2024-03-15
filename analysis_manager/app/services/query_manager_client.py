from datetime import datetime
from typing import Any

from app.core.schema import DimensionRequest
from app.utilities.async_http_client import AsyncHttpClient


class QueryManagerClient(AsyncHttpClient):
    METRICS_VALUES_BASE_URL = "/metrics/values"
    DATE_FORMAT = "%Y-%m-%d"

    async def get_metric_values(
        self,
        metric_ids: list[str],
        start_date: datetime,
        end_date: datetime,
        dimensions: list[DimensionRequest] | None = None,
    ) -> dict[str, Any]:
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
