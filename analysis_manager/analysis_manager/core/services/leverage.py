import logging
from typing import Any

import pandas as pd

from commons.clients.query_manager import QueryManagerClient
from fulcrum_core import AnalysisManager
from fulcrum_core.modules.leverage_id import LeverageIdCalculator

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LeverageIdService:
    def __init__(self, analysis_manager: AnalysisManager, query_manager: QueryManagerClient):
        """
        Initialize the LeverageIdService.

        :param analysis_manager: Instance of AnalysisManager.
        :param query_manager: Instance of QueryManagerClient.
        """
        self.analysis_manager = analysis_manager
        self.query_manager = query_manager

    async def calculate_leverage_id(
        self,
        metric: dict,
        values_df: pd.DataFrame,
        max_values: dict,
    ) -> dict:
        """
        Calculate leverage id for a given metric.

        :param metric: Dictionary containing metric details.
        :param values_df: DataFrame containing the values for the metrics.
        :param max_values: Dictionary containing the maximum values for the metrics.
        :returns: Dictionary containing the leverage ID results.
        :example:
            metric = {
                "metric_id": "NewBizDeals",
                "metric_expression": {
                    "expression_str": "AcceptOpps + SQOToWinRate",
                    "expression": {
                        "type": "expression",
                        "operator": "+",
                        "operands": [
                            {"type": "metric", "metric_id": "AcceptOpps"},
                            {"type": "metric", "metric_id": "SQOToWinRate"},
                        ],
                    },
                },
            }
            values_df = pd.DataFrame({
                "date": ["2023-01-01", "2023-01-02"],
                "AcceptOpps": [100, 150],
                "SQOToWinRate": [50, 75]
            })
            max_values = {"AcceptOpps": 200, "SQOToWinRate": 100}
            result = await leverage_id_service.calculate_leverage_id(metric, values_df, max_values)
        """
        # Get the nested expressions for the metric
        metric_expression = await self.get_nested_expressions(metric["metric_expression"])

        # Initialize the LeverageIdCalculator with the metric expression and max values
        leverage_calculator = LeverageIdCalculator(metric_expression, max_values)

        # Run the calculator and get the result
        result = leverage_calculator.run(values_df)
        return result

    async def get_nested_expressions(self, expr: dict[str, Any] | None) -> dict[str, Any]:
        """
        Get the nested expressions for a given metric expression.

        :param expr: Dictionary containing the metric expression.
        :returns: Dictionary containing the nested expressions.
        """
        if expr is None:
            return {}

        # Initialize the result with basic metric details
        result = {
            "metric_id": expr.get("metric_id"),
            "type": expr.get("type", "metric"),
            "period": expr.get("period", 0),
        }

        # Add the expression string if it exists
        if "expression_str" in expr:
            result["expression_str"] = expr["expression_str"]

        # Recursively process nested expressions
        if "expression" in expr and expr["expression"]:
            result["expression"] = {"type": "expression", "operator": expr["expression"]["operator"], "operands": []}

            for operand in expr["expression"]["operands"]:
                if operand["type"] == "metric":
                    # Fetch nested metric details
                    nested_metric = await self.query_manager.get_metric(operand["metric_id"])
                    nested_expr = nested_metric.get("metric_expression", {})
                    processed_nested_expr = await self.get_nested_expressions(nested_expr)
                    if not processed_nested_expr:
                        processed_nested_expr = {
                            "metric_id": operand["metric_id"],
                            "type": "metric",
                            "period": operand.get("period", 0),
                        }
                    result["expression"]["operands"].append(processed_nested_expr)
                else:
                    result["expression"]["operands"].append(operand)

        return result

    def extract_metric_ids(self, expression: dict[str, Any]) -> list[str]:
        """
        Extract all unique metric IDs from a nested expression structure.

        :param expression: The nested expression structure.
        :returns: A list of unique metric IDs.
        :example:
            expression = {
                "metric_id": "NewBizDeals",
                "expression": {
                    "type": "expression",
                    "operator": "+",
                    "operands": [
                        {"type": "metric", "metric_id": "AcceptOpps"},
                        {"type": "metric", "metric_id": "SQOToWinRate"},
                    ],
                },
            }
            metric_ids = leverage_id_service.extract_metric_ids(expression)
            # metric_ids will be ["NewBizDeals", "AcceptOpps", "SQOToWinRate"]
        """
        metric_ids: set = set()

        if not expression:
            return list(metric_ids)

        # Add the metric_id if it exists
        if "metric_id" in expression:
            metric_ids.add(expression["metric_id"])

        # Recursively extract metric IDs from nested expressions
        if "expression" in expression and expression["expression"]:
            for operand in expression["expression"].get("operands", []):
                metric_ids.update(self.extract_metric_ids(operand))

        return list(metric_ids)
