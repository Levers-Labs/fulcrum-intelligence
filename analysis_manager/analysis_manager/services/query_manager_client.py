from datetime import date
from typing import Any

from analysis_manager.utilities.async_http_client import AsyncHttpClient


class QueryManagerClient(AsyncHttpClient):
    METRICS_VALUES_BASE_URL = "metrics/{metric_id}/values"
    DATE_FORMAT = "%Y-%m-%d"

    async def get_metric_values(
        self,
        metric_ids: list[str],
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """
        Get metric values.
        metric_ids: list of metric ids
        start_date: start date
        end_date: end date

        Returns: list of metric values
        [{
            "id": "29",
            "date": "2024-03-15",
            "dimension": "Geosegmentation",
            "slice": "Americas",
            "value": 0
        },
        {
            "id": "29",
            "date": "2024-03-15",
            "dimension": "Geosegmentation",
            "slice": "Americas",
            "value": 0
        }]
        """
        start_date_str = start_date.strftime(self.DATE_FORMAT)
        end_date_str = end_date.strftime(self.DATE_FORMAT)
        payload = {
            "start_date": start_date_str,
            "end_date": end_date_str,
        }
        results = []
        for metric_id in metric_ids:
            url = self.METRICS_VALUES_BASE_URL.format(metric_id=metric_id)
            res = await self.post(endpoint=url, data=payload)
            if res.get("data"):
                results.extend(res["data"])
        return results
