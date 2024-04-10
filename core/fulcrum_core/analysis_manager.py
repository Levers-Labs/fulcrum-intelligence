import logging
from datetime import date
from typing import Any

import pandas as pd

from fulcrum_core.correlate import correlate
from fulcrum_core.describe import describe
from fulcrum_core.modules import ComponentDriftEvaluator
from fulcrum_core.process_control import process_control

logger = logging.getLogger(__name__)


class AnalysisManager:
    """
    Core class for implementing all major functions for analysis manager
    """

    def describe(
        self,
        data: list[dict],
        dimensions: list[str],
        metric_id: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        aggregation_function: str,
    ) -> list[dict]:
        result = describe(data, dimensions, metric_id, start_date, end_date, aggregation_function)
        return result

    def correlate(self, data: pd.DataFrame, start_date: date, end_date: date) -> list[dict]:
        result = correlate(data, start_date, end_date)
        return result

    def process_control(
        self,
        data: pd.DataFrame,
        metric_id: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        grain: str,
        debug: bool = False,
    ) -> list[dict] | dict:
        result = process_control(data, metric_id, start_date, end_date, grain, debug)
        return result

    @staticmethod
    def calculate_component_drift(
        metric_id: str,
        values: list[dict[str, Any]],
        metric_expression: dict[str, Any],
        parent_drift: dict[str, Any] | None = None,
        root_drift: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Calculate the drift for the given expression.
        e.g CAC = (SalesDevSpend + MktSpend) / (NewCust - LostCust)
        expression = {
            "type": "expression",
            "operator": "/",
            "operands": [
                {
                    "type": "expression",
                    "operator": "+",
                    "operands": [
                        {"type": "metric", "metric_id": "SalesDevSpend"},
                        {"type": "metric", "metric_id": "MktSpend"}
                    ],
                },
                {
                    "type": "expression",
                    "operator": "-",
                    "operands": [
                        {"type": "metric", "metric_id": "NewCust"},
                        {"type": "metric", "metric_id": "LostCust"}
                    ],
                }
            ],
        }
        :return: drift information for the expression
        e.g. {
            "metric_id": "CAC",
            "evaluation_value": 100.0,
            "comparison_value": 90.0,
            "drift": {
                "absolute_drift": 10.0,
                "percentage_drift": 11.11,
                "relative_impact": 100,
                "marginal_contribution": 100,
                "relative_impact_root": 100,
                "marginal_contribution_root": 100
            },
            "components": [
                {
                    "metric_id": "SalesDevSpend",
                    "evaluation_value": 30.0,
                    "comparison_value": 25.0,
                    "drift": {
                        "absolute_drift": 5.0,
                        "percentage_drift": 20.0,
                        "relative_impact": 50.0,
                        "marginal_contribution": 10.0,
                        "relative_impact_root": 50.0,
                        "marginal_contribution_root": 10.0
                    }
                },
                {
                    "metric_id": "MktSpend",
                    "evaluation_value": 35.0,
                    "comparison_value": 35.0,
                    "drift": {
                        "absolute_drift": 4.0,
                        "percentage_drift": 12.9,
                        "relative_impact": 40.0,
                        "marginal_contribution": 5.2,
                        "relative_impact_root": 40.0,
                        "marginal_contribution_root": 5.2
                    }
                },
                {
                    "metric_id": "NewCust",
                    "evaluation_value": 20.0,
                    "comparison_value": 22.0,
                    "drift": {
                        "absolute_drift": -2.0,
                        "percentage_drift": -9.1,
                        "relative_impact": -20.0,
                        "marginal_contribution": -2.0,
                        "relative_impact_root": -20.0,
                        "marginal_contribution_root": -2.0
                    }
                },
                {
                    "metric_id": "LostCust",
                    "evaluation_value": 11.0,
                    "comparison_value": 9.0,
                    "drift": {
                        "absolute_drift": 2.0,
                        "percentage_drift": 22.2,
                        "relative_impact": 20.0,
                        "marginal_contribution": 4.0,
                        "relative_impact_root": 20.0,
                        "marginal_contribution_root": 4.0
                    }
                }
            ]
        }
        """
        evaluator = ComponentDriftEvaluator(values)
        expression = evaluator.resolve_expression_values(metric_expression)
        result = evaluator.calculate_drift(expression)
        # Return the drift information as expected by the API
        response = {
            "metric_id": metric_id,
            "evaluation_value": result["evaluation_value"],
            "comparison_value": result["comparison_value"],
            "drift": result.get("drift"),
            "components": [],
        }

        def add_metric_components(node):
            if node.get("type") == "metric":
                response["components"].append(
                    {
                        "metric_id": node.get("metric_id"),
                        "evaluation_value": node.get("evaluation_value"),
                        "comparison_value": node.get("comparison_value"),
                        "drift": node.get("drift"),
                    }
                )
            elif "operands" in node:
                for operand in node["operands"]:
                    add_metric_components(operand)

        add_metric_components(result)
        logger.debug(
            "Component drift calculated for metric: %s, values: %s, expression: %s & \n drift: %s",
            metric_id,
            values,
            metric_expression,
            response,
        )
        return response


if __name__ == "__main__":
    # Example usage
    metric_values = [
        {"metric_id": "CAC", "evaluation_value": 700, "comparison_value": 300},
        {"metric_id": "SalesDevSpend", "evaluation_value": 5000.0, "comparison_value": 4500},
        {"metric_id": "SalesSpend", "evaluation_value": 2000.0, "comparison_value": 1500},
        {"metric_id": "NewCust", "evaluation_value": 25, "comparison_value": 12},
        {"metric_id": "OldCust", "evaluation_value": 5, "comparison_value": 2},
    ]

    expression_ex = {
        "type": "expression",
        "operator": "/",
        "operands": [
            {
                "type": "expression",
                "operator": "+",
                "operands": [
                    {"type": "metric", "metric_id": "SalesDevSpend"},
                    {"type": "metric", "metric_id": "SalesSpend"},
                ],
            },
            {
                "type": "expression",
                "operator": "-",
                "operands": [
                    {"type": "metric", "metric_id": "NewCust"},
                    {"type": "metric", "metric_id": "OldCust"},
                ],
            },
        ],
    }
    manager = AnalysisManager()
    res = manager.calculate_component_drift("CAC", metric_values, expression_ex)
