import logging
from datetime import date, datetime
from typing import Any, TypeVar

from commons.clients.query_manager import QueryManagerClient
from commons.models.enums import ExecutionStatus, Granularity
from commons.utilities.grain_utils import GRAIN_META, GrainPeriodCalculator

T = TypeVar("T", bound=float | int)


class ReportDataService:
    """Service for preparing metric data for reports, including period-based values and comparisons."""

    def __init__(self, query_client: QueryManagerClient):
        self.query_client = query_client

    def _get_report_period_dates(
        self, grain: Granularity, include_previous: bool = False
    ) -> tuple[date, date] | tuple[date, date, date, date]:
        """
        Calculate report period date ranges based on granularity.
        Args:
            grain: Time granularity for the report period
            include_previous: Whether to include previous period dates for comparison
        Returns:
            Tuple of (current_start, current_end) or (current_start, current_end, previous_start, previous_end)
        """
        current_start, current_end = GrainPeriodCalculator.get_current_period_range(grain, date.today())

        if include_previous:
            previous_start, previous_end = GrainPeriodCalculator.get_current_period_range(grain, current_start)
            return current_start, current_end, previous_start, previous_end

        return current_start, current_end

    def _calculate_metric_change(self, current_value: T, previous_value: T) -> float:
        """
        Calculate percentage change between current and previous values.

        Args:
            current_value: Current period value
            previous_value: Previous period value

        Returns:
            Percentage change as a float
        """
        if previous_value == 0:
            return round(float("inf") if current_value > 0 else float("-inf") if current_value < 0 else 0.0, 2)

        # Convert values to float to ensure a type checker understands the operations
        curr = float(current_value)
        prev = float(previous_value)
        return round(((curr - prev) / abs(prev)) * 100, 2)

    async def _prepare_comparison_data(
        self,
        metric_id: str,
        grain: Granularity,
        current_period: tuple[date, date],
        previous_period: tuple[date, date],
        include_raw_data: bool = False,
    ) -> dict[str, Any]:
        """
        Prepare metric data with comparison between current and previous periods.
        """
        try:
            metric = await self.query_client.get_metric(metric_id)

            current_start, current_end = current_period
            previous_start, _ = previous_period

            # Fetch time series data for both periods in one call
            time_series = await self.query_client.get_metric_time_series(
                metric_id=metric_id, start_date=previous_start, end_date=current_end, grain=grain
            )

            # Find values for exact period start dates
            current_value = next(
                (item["value"] for item in time_series if item["date"] == current_start.isoformat()),
                None,  # Use None if no matching date is found
            )
            previous_value = next(
                (item["value"] for item in time_series if item["date"] == previous_start.isoformat()),
                None,  # Use None if no matching date is found
            )

            # TODO: need to use comparisons
            # Calculate changes only if both values are available
            absolute_change = None
            percentage_change = None
            if current_value is not None and previous_value is not None:
                absolute_change = current_value - previous_value
                percentage_change = self._calculate_metric_change(current_value, previous_value)

            result = {
                "metric_id": metric_id,
                "metric": metric,
                "current_value": current_value,
                "previous_value": previous_value,
                "absolute_change": absolute_change,
                "percentage_change": percentage_change,
                "is_positive": absolute_change >= 0,  # type: ignore
                "status": ExecutionStatus.COMPLETED,
            }

            if include_raw_data:
                result["raw_data"] = time_series

            return result
        except Exception as e:
            logging.error(f"An exception occurred for {metric_id}: {str(e)}")
            return {}

    async def prepare_report_metrics_data(
        self,
        metric_ids: list[str],
        grain: Granularity,
        include_raw_data: bool = False,
        comparisons: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Prepare metrics data for report generation, optionally including period comparisons.

        Args:
            metric_ids: List of metric IDs to include in the report
            grain: Time granularity for the report periods
            include_raw_data: Whether to include raw time series data

        Returns:
            Dictionary containing:
                - metrics: List of formatted metric data for the report
                - fetched_at: ISO formatted timestamp
        """
        current_start, current_end, previous_start, previous_end = self._get_report_period_dates(  # type: ignore
            grain=grain, include_previous=True
        )
        metrics_data = []

        for metric_id in metric_ids:
            metric_data = await self._prepare_comparison_data(
                metric_id=metric_id,
                grain=grain,
                current_period=(current_start, current_end),
                previous_period=(previous_start, previous_end),
                include_raw_data=include_raw_data,
            )
            if metric_data:
                metrics_data.append(metric_data)

        return {
            "metrics": metrics_data,
            "start_date": current_start.strftime("%b %d, %Y"),
            "end_date": current_end.strftime("%b %d, %Y"),
            "fetched_at": datetime.now().strftime("%b %d, %Y"),
            "interval": GRAIN_META[grain]["interval"],
        }
