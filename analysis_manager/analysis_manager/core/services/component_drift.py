from datetime import date
from typing import Any

from analysis_manager.exceptions import NoMetricExpressionError
from commons.clients.query_manager import QueryManagerClient
from fulcrum_core import AnalysisManager


class ComponentDriftService:
    def __init__(self, analysis_manager: AnalysisManager, query_manager: QueryManagerClient):
        self.analysis_manager = analysis_manager
        self.query_manager = query_manager

    async def calculate_drift(
        self,
        metric: dict,
        evaluation_start_date: date,
        evaluation_end_date: date,
        comparison_start_date: date,
        comparison_end_date: date,
        parent_drift: dict | None = None,
    ) -> dict:
        """
        Calculate component drift for a given metric.

        Args:
            metric: metric dict
            evaluation_start_date: evaluation start date
            evaluation_end_date: evaluation end date
            comparison_start_date: comparison start date
            comparison_end_date: comparison end date
            parent_drift: parent component drift

        Returns:
            Component object containing the drift analysis results.
        """
        output_metric_id: str = metric["metric_id"]

        if not metric.get("metric_expression"):
            raise NoMetricExpressionError(output_metric_id)

        # Get evaluation and comparison values for the input & output metrics
        values = await self.get_drift_input_values(
            metric,
            evaluation_start_date,
            evaluation_end_date,
            comparison_start_date,
            comparison_end_date,
        )
        # calculate drift
        component = self.analysis_manager.calculate_component_drift(
            output_metric_id,
            values,
            metric_expression=metric["metric_expression"]["expression"],
            parent_drift=parent_drift,
        )

        # Setup recursive drift calculation for child components
        if not component.get("components"):
            return component
        # get all child component metric_ids
        input_metric_ids = [child_component["metric_id"] for child_component in component["components"]]
        input_metrics = await self.query_manager.list_metrics(metric_ids=input_metric_ids)
        input_metrics_map = {input_metric["metric_id"]: input_metric for input_metric in input_metrics}

        for input_component in component["components"]:
            input_metric_id = input_component["metric_id"]
            # continue if child component metric is the same as current output metric
            if input_metric_id == output_metric_id:
                continue
            # continue if a child component has no expression
            input_metric = input_metrics_map.get(input_metric_id)
            if input_metric is None or input_metric["metric_expression"] is None:
                continue
            # calculate drift for the child component
            input_metric_drift = await self.calculate_drift(
                input_metric,
                evaluation_start_date,
                evaluation_end_date,
                comparison_start_date,
                comparison_end_date,
                parent_drift=input_component["drift"],
            )
            # update the child component with the drift results
            input_component.update(components=input_metric_drift.get("components"))
        return component

    @staticmethod
    def get_shifted_dates_for_period(start_date: date, end_date: date, period: int) -> tuple[date, date]:
        """
        Get shifted dates for a given period.
        Changes the dates as per the period specified.
        E.g
        if period is -12, then delta = (end_date - start_date) * period,
        start_date = start_date + delta,
        end_date = end_date + delta
        Args:
            start_date: start date
            end_date: end date
            period: period

        Returns:
            tuple of shifted start date and end date
        """
        delta = (end_date - start_date) * period
        start_date = start_date + delta
        end_date = end_date + delta
        return start_date, end_date

    async def get_drift_input_values(
        self,
        metric: dict,
        evaluation_start_date: date,
        evaluation_end_date: date,
        comparison_start_date: date,
        comparison_end_date: date,
    ) -> list[dict[str, Any]]:
        """
        Get drift values for a given metric.

        Args:
            metric: metric dict
            evaluation_start_date: evaluation start date
            evaluation_end_date: evaluation end date
            comparison_start_date: comparison start date
            comparison_end_date: comparison end date

        Returns:
            list of evaluation and comparison values for the input & output metrics
        """
        metric_expression = metric["metric_expression"]
        input_metrics_expressions = []
        # get all input metrics from the expression
        if metric_expression is not None:
            input_metrics_expressions = self.get_input_metrics_from_expression(metric_expression["expression"])
        # values -> [{metric_id, evaluation_value, comparison_value}]
        values = []  # noqa
        # get metric values for the given metric_ids and date range
        for input_metric in [metric_expression, *input_metrics_expressions]:
            # calculate dates based on a period specified
            metric_evaluation_start_date, metric_evaluation_end_date = self.get_shifted_dates_for_period(
                evaluation_start_date, evaluation_end_date, input_metric["period"]
            )
            metric_comparison_start_date, metric_comparison_end_date = self.get_shifted_dates_for_period(
                comparison_start_date, comparison_end_date, input_metric["period"]
            )
            # get metric value
            values.append(
                {
                    "metric_id": input_metric["metric_id"],
                    "evaluation_value": (
                        await self.query_manager.get_metric_value(
                            input_metric["metric_id"], metric_evaluation_start_date, metric_evaluation_end_date
                        )
                    )["value"],
                    "comparison_value": (
                        await self.query_manager.get_metric_value(
                            input_metric["metric_id"], metric_comparison_start_date, metric_comparison_end_date
                        )
                    )["value"],
                }
            )
        return values

    def get_input_metrics_from_expression(self, expression: dict) -> list[dict]:
        """
        Get all input metrics from the expression.

        Args:
            expression: expression dict

        Returns:
            list of input metrics
        """
        input_metrics = []
        for node in expression["operands"]:
            if node["type"] == "metric":
                # add if not already present
                if node not in input_metrics:
                    input_metrics.append(node)
            else:
                input_metrics.extend(self.get_input_metrics_from_expression(node))
        return input_metrics
