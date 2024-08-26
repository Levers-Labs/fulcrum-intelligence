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

    async def get_metrics_value(
        self, metric_ids: list[str], start_date: date | None = None, end_date: date | None = None, **kwargs
    ) -> dict[str, Any]:
        """
        Get aggregated metric values for multiple metric IDs.

        Args:
            metric_ids: List of metric IDs.
            start_date: Optional start date to filter the metrics.
            end_date: Optional end date to filter the metrics.
            kwargs: Additional filters to pass to the query manager values endpoint.

        Returns:
            A dictionary with metric IDs as keys and their corresponding values.
        """
        results = {}
        for metric_id in metric_ids:
            value = await self.get_metric_value(metric_id, start_date, end_date, **kwargs)
            results[metric_id] = value["value"]
        return results

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

    async def get_metrics_time_series_df(
        self, metric_ids: list[str], start_date: date, end_date: date, grain: Granularity = Granularity.DAY
    ) -> pd.DataFrame:
        """
        Fetches time series data for multiple metrics and returns it as a pandas DataFrame.

        Args:
            metric_ids (list[str]): List of metric IDs for which the time series data is required.
            start_date (date): The start date for the time series data.
            end_date (date): The end date for the time series data.
            grain (Granularity): The granularity of the time series data (default is DAY).

        Returns:
            pd.DataFrame: A DataFrame where each column represents a metric ID with their respective
                          time series data, indexed by date.
        """
        # Initialize an empty DataFrame to store results
        combined_df = pd.DataFrame(columns=["date"])

        # Loop through each metric ID and fetch its time series data
        for metric_id in metric_ids:
            data = await self.get_metric_time_series(metric_id, start_date, end_date, grain)
            # Convert the list of dictionaries to a DataFrame
            metric_df = pd.DataFrame(data)
            # Rename the 'value' column to the current metric_id
            metric_df.rename(columns={"value": metric_id}, inplace=True)
            # Keep only the 'date' and the current metric_id columns
            metric_df = metric_df[["date", metric_id]]
            # Merge the current metric's DataFrame with the main DataFrame
            # Use an outer join to include all dates, filling missing values with NaN
            combined_df = combined_df.merge(metric_df, on="date", how="outer")

        return combined_df

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

    async def get_metric_values_df(
        self,
        metric_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
        dimensions: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Get metric values as a pandas DataFrame.
        metric_id: id of the metric
        start_date: start date
        end_date: end date
        grain: granularity of the data (default: day)
        dimensions: list of dimensions (optional)

        Returns: pandas DataFrame of metric values
        """
        data = await self.get_metric_values(metric_id, start_date, end_date, dimensions)
        return pd.DataFrame(data)

    async def get_metrics_max_values(self, metric_ids: list[str]) -> dict[str, Any]:
        """
        Get metric targets.
        metric_id: metric id
        """
        results = await self.list_metrics(metric_ids=metric_ids)

        # Create a set for faster lookup
        metric_id_set = set(metric_ids)
        return {
            metric["metric_id"]: metric["hypothetical_max"]
            for metric in results
            if metric["metric_id"] in metric_id_set and "hypothetical_max" in metric
        }

    async def get_expressions(
        self, expr: dict[str, Any] | None, metric_ids: set = set(), nested: bool = False  # noqa
    ) -> tuple[dict[str, Any], list]:
        """
        Get the nested expressions for a given metric expression.

        :param expr: Dictionary containing the metric expression.
        :param metric_ids: Set of metric IDs to keep track of all metrics involved.
        :param nested: Flag to determine if nested expressions should be processed.
        :returns: Dictionary containing the nested expressions and set of metric IDs.
        """
        if expr is None:
            return {}, list(metric_ids)

        # Initialize the result with basic metric details
        result = {
            "metric_id": expr.get("metric_id"),
            "type": expr.get("type", "metric"),
            "period": expr.get("period", 0),
        }

        # Add the expression string if it exists
        if "expression_str" in expr:
            metric_ids.add(expr["metric_id"])
            result["expression_str"] = expr["expression_str"]

        # Process nested expressions only if the nested flag is True
        if nested and "expression" in expr and expr["expression"]:
            result["expression"] = {"type": "expression", "operator": expr["expression"]["operator"], "operands": []}

            for operand in expr["expression"]["operands"]:
                if operand["type"] == "metric":
                    metric_ids.add(operand["metric_id"])
                    # Fetch nested metric details
                    nested_metric = await self.get_metric(operand["metric_id"])
                    nested_expr = nested_metric.get("metric_expression", {})
                    processed_nested_expr, nested_metric_ids = await self.get_expressions(
                        nested_expr, metric_ids, nested
                    )
                    metric_ids.update(nested_metric_ids)
                    if not processed_nested_expr:
                        processed_nested_expr = {
                            "metric_id": operand["metric_id"],
                            "type": "metric",
                            "period": operand.get("period", 0),
                        }
                    result["expression"]["operands"].append(processed_nested_expr)
                else:
                    result["expression"]["operands"].append(operand)

        return result, list(metric_ids)
