import logging
import math
from enum import Enum
from typing import Any

# Constants
ADDITIVE_OPERATORS = {"+", "-"}
MULTIPLICATIVE_OPERATORS = {"*", "/"}
logger = logging.getLogger(__name__)


class CalculationType(str, Enum):
    ADDITIVE = "ADDITIVE"
    MULTIPLICATIVE = "MULTIPLICATIVE"


class ComponentDriftEvaluator:
    def __init__(self, values: list[dict[str, Any]]):
        self.metric_values = {metric["metric_id"]: metric for metric in values}

    @staticmethod
    def calculate_delta(
        evaluation_value: float, comparison_value: float, calculation_type: CalculationType, invert_values: bool = False
    ) -> float:
        """
        Calculate the delta between evaluation and comparison values.
        If is_multiplicative is True, the values are assumed to be in log space.
        Invert_values: a flag is used to invert the values when the value is in the denominator.
        """
        if calculation_type == CalculationType.MULTIPLICATIVE:
            if invert_values:
                return math.log(1 / evaluation_value) - math.log(1 / comparison_value)
            return math.log(evaluation_value) - math.log(comparison_value)
        else:
            return evaluation_value - comparison_value

    @staticmethod
    def calculate_percent_change(evaluation_value: float, comparison_value: float) -> float:
        """
        Calculate the percentage change between evaluation and comparison values.
        """
        return (evaluation_value - comparison_value) / comparison_value if comparison_value else float("inf")

    @staticmethod
    def get_calculation_type(operator: str) -> CalculationType:
        """
        Get the calculation type based on the operator.
        """
        if operator in MULTIPLICATIVE_OPERATORS:
            return CalculationType.MULTIPLICATIVE
        return CalculationType.ADDITIVE

    @staticmethod
    def calculate_relative_impact(delta: float, parent_delta: float) -> float:
        """
        Calculate the relative impact of the node w.r.t parent node.
        """
        return 0 if parent_delta == 0 else (delta / parent_delta)

    @staticmethod
    def calculate_marginal_contribution(relative_impact: float, percent_change: float) -> float:
        """
        Calculate the marginal contribution of the node w.r.t parent node.
        """
        return relative_impact * percent_change

    @staticmethod
    def calculate_value(operand_values: list[dict[str, float]], operator: str, key: str):
        """
        Calculate the value based on the operator and operand values.
        """
        # Initialize the result with the first operand value
        result = operand_values[0][key]
        # Apply the operator to the operand values
        for operand in operand_values[1:]:
            if operator == "+":
                result += operand[key]
            elif operator == "-":
                result -= operand[key]
            elif operator == "*":
                result *= operand[key]
            elif operator == "/":
                result /= operand[key]
        return result

    def resolve_expression_values(self, expression: dict[str, Any]) -> dict[str, Any]:
        """
        Calculate the evaluation and comparison values for the given expression
        by solving the expression tree.
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
                    "operator": "+",
                    "operands": [
                        {"type": "metric", "metric_id": "ChurnMRR"},
                        {"type": "metric", "metric_id": "CtrctnMRR"}
                    ],
                },
            ],
        }

        :return: Expression tree with evaluation and comparison values added for each expression node.
        """
        if expression["type"] == "metric":
            # Fetch evaluation and comparison values for metrics
            metric_id = expression["metric_id"]
            metric_data = self.metric_values[metric_id]
            expression["evaluation_value"] = metric_data["evaluation_value"]
            expression["comparison_value"] = metric_data["comparison_value"]
            return {
                "evaluation_value": metric_data["evaluation_value"],
                "comparison_value": metric_data["comparison_value"],
            }
        elif expression["type"] == "expression":
            # Recursively resolve values for expression operands
            operand_values = [self.resolve_expression_values(operand) for operand in expression["operands"]]
            # Calculate the expression value based on the operator
            evaluation_value = self.calculate_value(operand_values, expression["operator"], key="evaluation_value")
            comparison_value = self.calculate_value(operand_values, expression["operator"], key="comparison_value")
            # Update the expression node with calculated values
            expression["evaluation_value"] = evaluation_value
            expression["comparison_value"] = comparison_value
        return expression

    def calculate_drift_for_node(
        self,
        node: dict[str, Any],
        parent_node: dict[str, Any] | None = None,
        operator: str = "+",
        relative_impact_root: float = 1,
        marginal_contribution_root: float = 1,
        invert_values: bool = False,
    ):
        """
        Calculate the drift for the given node.
        """
        calculation_type = self.get_calculation_type(operator)
        # Calculate drift for a single node, considering cumulative impacts
        delta = self.calculate_delta(
            node["evaluation_value"], node["comparison_value"], calculation_type, invert_values=invert_values
        )
        percent_change = self.calculate_percent_change(node["evaluation_value"], node["comparison_value"])

        if parent_node:
            parent_delta = self.calculate_delta(
                parent_node["evaluation_value"], parent_node["comparison_value"], calculation_type
            )
            # calculate relative impact and marginal contribution w.r.t parent node
            relative_impact = self.calculate_relative_impact(delta, parent_delta)
            marginal_contribution = self.calculate_marginal_contribution(
                relative_impact, parent_node["drift"]["percentage_drift"]
            )
            # root level relative impact and marginal contribution
            relative_impact_root = relative_impact * relative_impact_root
            marginal_contribution_root = marginal_contribution * marginal_contribution_root
            # Update the cumulative impacts w.r.t top level node/metric
            relative_impact *= parent_node["drift"]["relative_impact"]
            marginal_contribution *= parent_node["drift"]["marginal_contribution"]
        else:
            # impact w.r.t itself
            relative_impact = 1
            marginal_contribution = 1
            relative_impact_root = relative_impact_root
            marginal_contribution_root = marginal_contribution_root

        return {
            "absolute_drift": delta,
            "percentage_drift": percent_change,
            "relative_impact": relative_impact,
            "marginal_contribution": marginal_contribution,
            "relative_impact_root": relative_impact_root,
            "marginal_contribution_root": marginal_contribution_root,
        }

    def calculate_drift(
        self,
        expression: dict[str, Any],
        parent_node: dict[str, Any] | None = None,
        operator: str = "+",
        relative_impact_root: float = 1,
        marginal_contribution_root: float = 1,
        invert_values: bool = False,
    ) -> dict[str, Any]:
        """
        # Calculations
        Calculate the drift for two expressions of /, we can store it at expression level.
        When we calculate the relative impact and marginal contribution, at the metric level we need to take all
        operator levels to drift into consideration.
        e.g.,
        If I want to know the relative impact of SalesDevSpend (relative to CAC),
        it will be = relative_impact(SalesDevSpend w.r.t "+" exp) * relative_impact("+" exp w.r.t "/" i.e. Root)
        relative_impact(NewCust) = relative_impact(NewCust w.r.t "-" exp) * relative_impact("-" exp w.r.t "/" i.e. Root)
        Same way we can calculate marginal contribution.

        For relative_impact_root and relative relative_impact_root, this is actually the impact w.r.t parent and
        parent's relative impact w.r.t topmost level of metric where it all started.
        E.g.,
        assume SalesDevSpend = SalesMktSpend + NewCustSpend
        then relative impact root(SalesMktSpend) = relative_impact(SalesMktSpend w.r.t SalesDevSpend) *
        relative_impact(SalesDevSpend w.r.t. CAC)

        :return: Expression along with drift values.
        Here the drift values, i.e., relative_impact & marginal_contribution are always w.r.t Parent metric.
        In this case against CAC
        """
        drift = self.calculate_drift_for_node(
            expression,
            parent_node,
            operator,
            relative_impact_root=relative_impact_root,
            marginal_contribution_root=marginal_contribution_root,
            invert_values=invert_values,
        )
        expression["drift"] = drift

        if "operands" in expression:
            for i, operand in enumerate(expression["operands"]):
                # Determine if values should be inverted based on operator and position
                should_invert_values = expression["operator"] == "/" and i > 0
                self.calculate_drift(
                    operand,
                    expression,
                    expression["operator"],
                    relative_impact_root=drift["relative_impact_root"],
                    marginal_contribution_root=drift["marginal_contribution_root"],
                    invert_values=should_invert_values,
                )

        return expression
