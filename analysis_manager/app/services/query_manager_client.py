from datetime import datetime
from typing import Any

from app.core.schema import DimensionRequest
from app.utilities.async_http_client import AsyncHttpClient


class QueryManagerClient(AsyncHttpClient):
    METRICS_VALUES_BASE_URL = "metrics/values"
    DATE_FORMAT = "%Y-%m-%d"

    async def get_metric_values(
        self,
        metric_ids: list[str],
        start_date: datetime,
        end_date: datetime,
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
            "metric_id": "29",
            "dimension": "Geosegmentation",
            "slice": "Americas",
            "mean": 2.6490066225165565,
            "median": 1,
            "variance": 23.60264900662251,
            "standard_deviation": 4.858255757637973,
            "percentile_25": 1,
            "percentile_50": 1,
            "percentile_75": 2,
            "percentile_90": 5,
            "percentile_95": 8.5,
            "percentile_99": 23.5,
            "min": 1,
            "max": 44,
            "count": 151,
            "sum": 400,
            "unique": 14
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
