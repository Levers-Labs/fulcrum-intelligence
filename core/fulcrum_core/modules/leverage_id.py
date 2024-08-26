import logging
from typing import Any

import pandas as pd
import sympy as sp

from fulcrum_core.modules import BaseAnalyzer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class LeverageIdCalculator(BaseAnalyzer):
    """
    LeverageIdCalculator is responsible for calculating leverage IDs
    for given metrics based on their expressions and values.

    :param metric_expression: Dictionary containing the metric expression details.
    :param max_values: Dictionary containing the maximum values for the metrics.
    :param kwargs: Additional keyword arguments.
    """

    def __init__(self, metric_expression: dict, max_values: dict, **kwargs):
        super().__init__(**kwargs)
        self.metric_expression = metric_expression
        self.max_values = max_values
        self.root_metric_id = metric_expression["metric_id"]
        self.metric_symbols: dict = {}

    def analyze(self, df: pd.DataFrame, *args, **kwargs) -> dict[str, Any]:
        """
        Analyze the given DataFrame to calculate leverage IDs.

        :param df: DataFrame containing the values for the metrics.
        :param args: Additional arguments.
        :param kwargs: Additional keyword arguments.
        :returns: Dictionary containing the leverage ID results.
        """
        # Initialize variable symbols for sympy
        self.metric_symbols = {var: sp.symbols(var) for var in df.columns if var != "date"}

        # Extract equations from the metric expression
        equations_list = self._extract_expression()

        # Create parent and root_metric_id dictionaries
        parent_dict, root_dict = self._create_parent_and_root_dicts()

        logger.info(f"Extracted Equations List: {equations_list}")
        logger.info(f"Variable Symbols: {self.metric_symbols}")
        logger.info(f"Parent Dict: {parent_dict}")
        logger.info(f"Top Parent Dict: {root_dict}")

        # Analyze parent and root_metric_id metrics
        parent_results = self.analyze_parent(parent_dict, equations_list, df)
        root_results = self.analyze_root(equations_list, df)

        # Combine results and aggregate them
        final_results = self.combine_results(parent_results, root_results)
        aggregated_df = (
            pd.DataFrame(final_results)
            .groupby("variable")
            .agg(
                parent_metric=("parent_metric", "first"),
                top_metric=("top_metric", "first"),
                pct_diff=("pct_diff", "mean"),
                pct_diff_root=("pct_diff_root", "mean"),
            )
            .reset_index()
        )

        # Build the output structure
        return self.build_output_structure(aggregated_df)

    def _construct_expression_str(self, expression: dict) -> str:
        """
        Construct the expression string from the operator and operands.

        :param expression: Dictionary containing the expression details.
        :returns: Constructed expression string.
        """
        operator = expression.get("operator", "")
        operands = expression.get("operands", [])

        operand_strs = []
        for operand in operands:
            if operand["type"] == "metric":
                operand_strs.append(f"{{{operand['metric_id']}}}")
            elif operand["type"] == "constant":
                operand_strs.append(str(operand["value"]))

        return f" {operator} ".join(operand_strs)

    def _extract_expression(self, expression: dict | None = None, parent_metric_id: str | None = None) -> list[str]:
        """
        Extract equations from the metric expression.

        :param expression: Dictionary containing the metric expression (default is None).
        :param parent_metric_id: ID of the parent metric (default is None).
        :returns: List of equations as strings.
        """
        if expression is None:
            expression = self.metric_expression
            parent_metric_id = self.root_metric_id

        equations = []

        if "expression" in expression:
            constructed_expr = self._construct_expression_str(expression["expression"])
            equations.append(f"{parent_metric_id} = {constructed_expr}")

        for operand in expression.get("expression", {}).get("operands", []):
            if "expression" in operand:
                constructed_expr = self._construct_expression_str(operand["expression"])
                equations.append(f"{operand['metric_id']} = {constructed_expr}")
                equations.extend(self._extract_expression(operand, operand["metric_id"]))

        # Remove curly braces from equations only at the top level of recursion
        if parent_metric_id == self.root_metric_id:
            return [eq.replace("{", "").replace("}", "") for eq in equations]
        else:
            return equations

    def _create_parent_and_root_dicts(self) -> tuple[dict[str, str], dict[str, str]]:
        """
        Create dictionaries for parent and root metrics using an iterative approach.

        :returns: Tuple containing parent dictionary and root dictionary.
        """
        parent_dict = {}
        expression_stack = [(self.metric_expression["expression"], self.root_metric_id)]

        while expression_stack:
            expression, current_parent = expression_stack.pop()

            for operand in expression.get("operands", []):
                if "metric_id" in operand:
                    metric_id = operand["metric_id"]
                    parent_dict[metric_id] = current_parent

                    if "expression" in operand:
                        expression_stack.append((operand["expression"], metric_id))

        root_dict = {metric_id: self.root_metric_id for metric_id in parent_dict}
        return parent_dict, root_dict

    def _compute_y(self, equation_str: str, values: dict) -> float:
        """
        Compute the value of the equation with given values.

        :param equation_str: Equation as a string.
        :param values: Dictionary containing variable values.
        :returns: Computed value as float.
        """
        equation = sp.sympify(equation_str, locals=self.metric_symbols)
        return float(equation.subs({self.metric_symbols[var]: value for var, value in values.items()}))

    def _compute_percentage_diff(self, equation_str: str, current_values: dict, var: str) -> float:
        """
        Compute the percentage difference for a given equation and variable values.

        :param equation_str: Equation as a string.
        :param current_values: Dictionary containing variable values.
        :param var: Variable name as string.
        :returns: Computed percentage difference as float.
        """
        y = self._compute_y(equation_str, current_values)
        max_var_values = current_values.copy()
        max_var_values[var] = self.max_values.get(var, 0)
        ymax = self._compute_y(equation_str, max_var_values)
        return (ymax - y) / y * 100 if y != 0 else float("inf")

    def _analyze_metrics(
        self, equations_list: list, values_df: pd.DataFrame, metrics_to_analyze: dict, is_root: bool
    ) -> list[dict]:
        """
        Analyze metrics to calculate percentage differences.

        :param equations_list: List of equations as strings.
        :param values_df: DataFrame containing the values for the metrics.
        :param metrics_to_analyze: Dictionary of metrics to analyze.
        :param is_root: Boolean indicating if this is a root metric analysis.
        :returns: List of dictionaries containing analysis results.
        """
        results = []
        equation_dict = {eq.split(" = ")[0].strip(): eq.split(" = ")[1].strip() for eq in equations_list}

        for _, row in values_df.iterrows():
            current_values = row.drop("date").to_dict()
            for var, parent_metric in metrics_to_analyze.items():
                expr_str = equation_dict.get(parent_metric if not is_root else self.root_metric_id)
                if expr_str:
                    percentage_diff = self._compute_percentage_diff(expr_str, current_values, var)
                    results.append(
                        {
                            "date": row["date"],
                            "variable": var,
                            f"{'root' if is_root else 'parent'}_metric": (
                                parent_metric if not is_root else self.root_metric_id
                            ),
                            f"pct_diff{'_root' if is_root else ''}": float(percentage_diff),
                        }
                    )

        return sorted(
            results,
            key=lambda x: (
                x["date"],
                (
                    x[f"pct_diff{'_root' if is_root else ''}"]
                    if x[f"pct_diff{'_root' if is_root else ''}"] is not None
                    else float("inf")
                ),
            ),
            reverse=True,
        )

    def analyze_parent(self, parent_dict: dict, equations_list: list, values_df: pd.DataFrame) -> list[dict]:
        """
        Analyze parent metrics to calculate percentage differences.

        :param parent_dict: Dictionary containing parent metrics.
        :param equations_list: List of equations as strings.
        :param values_df: DataFrame containing the values for the metrics.
        :returns: List of dictionaries containing analysis results.
        """
        return self._analyze_metrics(equations_list, values_df, parent_dict, False)

    def analyze_root(self, equations_list: list, values_df: pd.DataFrame) -> list[dict]:
        """
        Analyze root metrics to calculate percentage differences.

        :param equations_list: List of equations as strings.
        :param values_df: DataFrame containing the values for the metrics.
        :returns: List of dictionaries containing analysis results.
        """
        root_dict = {var: self.root_metric_id for var in values_df.columns if var != "date"}
        return self._analyze_metrics(equations_list, values_df, root_dict, True)

    def combine_results(self, parent_results: list[dict], root_results: list[dict]) -> list[dict]:
        """
        Combine parent and root analysis results.

        :param parent_results: List of dictionaries containing parent analysis results.
        :param root_results: List of dictionaries containing root analysis results.
        :returns: List of combined analysis results.
        """
        root_lookup = {(result["date"], result["variable"]): result["pct_diff_root"] for result in root_results}
        return [
            {
                **parent_result,
                "top_metric": self.root_metric_id,
                "pct_diff_root": root_lookup.get((parent_result["date"], parent_result["variable"]), None),
            }
            for parent_result in parent_results
        ]

    def get_metric_details(self, metric_id: str, aggregated_df: pd.DataFrame) -> dict | None:
        """
        Get details of a specific metric from the aggregated DataFrame.

        :param metric_id: Metric ID as string.
        :param aggregated_df: Aggregated DataFrame containing analysis results.
        :returns: Dictionary containing metric details or None if not found.
        """
        # Filter the aggregated DataFrame for the specified metric ID
        metric_details = aggregated_df[aggregated_df["variable"] == metric_id]
        if not metric_details.empty:
            return {
                "metric_id": metric_id,
                "parent_metric": metric_details["parent_metric"].iloc[0],
                "top_metric": metric_details["top_metric"].iloc[0],
                "pct_diff": metric_details["pct_diff"].iloc[0],
                "pct_diff_root": metric_details["pct_diff_root"].iloc[0],
            }
        return None

    def build_output_structure(self, aggregated_df: pd.DataFrame, expression: dict | None = None) -> dict:
        """
        Build the output structure for the leverage analysis results.

        This method constructs a nested dictionary structure representing the leverage analysis results
        for the given metric expression. It recursively processes the metric expression and its operands
        to build a hierarchical representation of the leverage analysis.

        :param aggregated_df: Aggregated DataFrame containing analysis results.
        :param expression: Dictionary containing the metric expression details (default is None).
        :returns: Dictionary representing the leverage analysis results.
        """
        # If no specific expression is provided, use the root metric expression
        if expression is None:
            expression = self.metric_expression

        # Extract the metric ID from the expression
        metric_id = expression.get("metric_id")
        # Get the details of the metric from the aggregated DataFrame
        metric_details = self.get_metric_details(metric_id, aggregated_df)  # type: ignore

        # Initialize the result dictionary with the metric ID and leverage details
        result: dict = {
            "metric_id": metric_id,
            "leverage": {
                "pct_diff": metric_details["pct_diff"] if metric_details else 0.0,
                "pct_diff_root": metric_details["pct_diff_root"] if metric_details else 0.0,
            },
            "components": [],
        }

        # If the expression contains nested operands, process them recursively
        if expression.get("expression"):
            for operand in expression["expression"].get("operands", []):
                if operand.get("type") == "metric":
                    # Recursively build the output structure for each operand
                    component = self.build_output_structure(aggregated_df, operand)
                    if component:
                        result["components"].append(component)  # type: ignore

        return result
