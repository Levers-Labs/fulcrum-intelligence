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
        dimensions: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
             Get metric values.
             metric_ids: list of metric ids
             start_date: start date
             end_date: end date
             dimensions: list of dimensions

             Returns: list of metric values
             [
        {
           "metric_id":"CAC",
           "value":203,
           "date":"2022-09-01",
           "customer_segment":"Enterprise",
           "channel":"Online",
           "region":"Asia"
        },
        {
           "metric_id":"CAC",
           "value":3139,
           "date":"2022-09-01",
           "customer_segment":"Enterprise",
           "channel":"Retail",
           "region":"Asia"
        },
        {
           "metric_id":"CAC",
           "value":83,
           "date":"2022-09-01",
           "customer_segment":"Enterprise",
           "channel":"Retail",
           "region":"Europe"
        }]
        """
        start_date_str = start_date.strftime(self.DATE_FORMAT)
        end_date_str = end_date.strftime(self.DATE_FORMAT)
        payload = {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "dimensions": dimensions if dimensions else [],
        }
        results = []
        for metric_id in metric_ids:
            url = self.METRICS_VALUES_BASE_URL.format(metric_id=metric_id)
            res = await self.post(endpoint=url, data=payload)
            if res.get("data"):
                results.extend(res["data"])
        return results
