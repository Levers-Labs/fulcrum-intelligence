from datetime import date
from typing import Any

import pandas as pd

from commons.clients.base import AsyncHttpClient
from commons.models.enums import Granularity


class QueryManagerClient(AsyncHttpClient):
    """
    Query Manager Client used to interact with the Query Manager API.
    """

    DATE_FORMAT = "%Y-%m-%d"

    async def get_metric_value(
        self, metric_id: str, start_date: date | None = None, end_date: date | None = None, **kwargs
    ) -> Any:
        """
        Get aggregated metric value for a given metric id and date range if provided.
        metric_id: ID of the metric
        start_date: filter by start date
        end_date: filter by end date
        kwargs: additional filters to pass to query manager values endpoint
        """

        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date
        results = await self.get_metric_values(metric_id=metric_id, **kwargs)
        return results[0] if len(results) == 1 else None

    async def get_metric_values(
        self,
        metric_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
        dimensions: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get metric values.
        metric_id: id of the metric
        start_date: start date
        end_date: end date
        dimensions: list of dimensions

        Returns: list of metric values
        """
        start_date_str = start_date.strftime(self.DATE_FORMAT) if start_date else None
        end_date_str = end_date.strftime(self.DATE_FORMAT) if end_date else None
        payload = {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "dimensions": dimensions if dimensions else [],
        }
        response = await self.post(endpoint=f"metrics/{metric_id}/values", data=payload)
        return response["data"]

    async def get_metrics_values(
        self,
        metric_ids: list[str],
        start_date: date | None = None,
        end_date: date | None = None,
        dimensions: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get aggregated metric values for a list of metric ids and date range.
        metric_ids: list of metric ids
        start_date: start date
        end_date: end date
        dimensions: list of dimensions

        Returns: list of metric values
        """
        results = []
        for metric_id in metric_ids:
            results.extend(await self.get_metric_values(metric_id, start_date, end_date, dimensions))
        return results

    async def get_metric_time_series(
        self,
        metric_id: str,
        start_date: date,
        end_date: date,
        grain: Granularity = Granularity.DAY,
        dimensions: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get metric time series.
        metric_id: id of the metric
        start_date: start date
        end_date: end date
        grain: granularity of the data (default: day)
        dimensions: list of dimensions (optional)

        Returns: list of metric time series
        """
        start_date_str = start_date.strftime(self.DATE_FORMAT)
        end_date_str = end_date.strftime(self.DATE_FORMAT)
        payload = {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "dimensions": dimensions if dimensions else [],
            "grain": grain.value,
        }
        response = await self.post(endpoint=f"metrics/{metric_id}/values", data=payload)
        return response["data"]

    async def get_metrics_time_series(
        self,
        metric_ids: list[str],
        start_date: date,
        end_date: date,
        grain: Granularity = Granularity.DAY,
        dimensions: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get metric time series for a list of metric ids.
        metric_ids: list of metric ids
        start_date: start date
        end_date: end date
        grain: granularity of the data (default: day)
        dimensions: list of dimensions (optional)

        Returns: list of metric time series
        """
        results = []
        for metric_id in metric_ids:
            results.extend(await self.get_metric_time_series(metric_id, start_date, end_date, grain, dimensions))
        return results

    async def get_metric_time_series_df(
        self,
        metric_id: str,
        start_date: date,
        end_date: date,
        grain: Granularity = Granularity.DAY,
        dimensions: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Get metric time series as a pandas DataFrame.
        metric_id: id of the metric
        start_date: start date
        end_date: end date
        grain: granularity of the data (default: day)
        dimensions: list of dimensions (optional)

        Returns: pandas DataFrame of metric time series
        """
        data = await self.get_metric_time_series(metric_id, start_date, end_date, grain, dimensions)
        return pd.DataFrame(data)

    async def get_metric(self, metric_id: str) -> dict[str, Any]:
        """
        Get metric details.
        metric_id: metric id
        """
        return await self.get(endpoint=f"metrics/{metric_id}")

    async def list_metrics(self, metric_ids: list[str] | None = None, **params) -> list[dict[str, Any]]:
        """
        List metrics.
        metric_ids: list of metric ids
        """
        if metric_ids is not None:
            params["metric_ids"] = metric_ids
        response = await self.get(endpoint="metrics", params=params)
        return response["results"]

    async def get_metric_targets(
        self,
        metric_id: str,
        grain: Granularity | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get metric targets.
        metric_id: metric id
        """
        params = {}
        if grain:
            params["grain"] = grain.value
        if start_date and end_date:
            params["start_date"] = start_date.strftime(self.DATE_FORMAT)
            params["end_date"] = end_date.strftime(self.DATE_FORMAT)
        res = await self.get(endpoint=f"metrics/{metric_id}/targets", params=params)
        return res["results"]
