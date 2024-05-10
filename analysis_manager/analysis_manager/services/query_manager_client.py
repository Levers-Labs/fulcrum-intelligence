from datetime import date
from typing import Any

from commons.utilities.async_http_client import AsyncHttpClient


class QueryManagerClient(AsyncHttpClient):
    METRICS_VALUES_BASE_URL = "metrics/{metric_id}/values"
    DATE_FORMAT = "%Y-%m-%d"

    async def get_metric_value(
        self, metric_id: str, start_date: date | None = None, end_date: date | None = None
    ) -> Any:
        """
        Get aggregated metric value for a given metric id and date range if provided.
        metric_id: metric id
        start_date: start date
        end_date: end date
        """
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        res = await self.get(endpoint=f"metrics/{metric_id}/value", params=params)
        return res["value"]

    async def get_metrics_values(self, metric_ids: list[str], start_date: date, end_date: date) -> list[dict[str, Any]]:
        """
        Get aggregated metric values for a list of metric ids and date range.
        metric_ids: list of metric ids
        start_date: start date
        end_date: end date
        """
        results = []
        for metric_id in metric_ids:
            results.append(await self.get_metric_value(metric_id, start_date, end_date))
        return results

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

    async def get_metric_details(self, metric_id: str) -> dict[str, Any]:
        """
        Get metric details.
        metric_id: metric id
        """
        return await self.get(endpoint=f"metrics/{metric_id}")

    async def get_metrics_details(self, metric_ids: list[str]) -> list[dict[str, Any]]:
        """
        Get metrics details.
        metric_ids: list of metric ids
        """
        res = await self.get(endpoint="metrics", params={"metric_ids": metric_ids})
        return res["results"]
