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
        self.root = metric_expression["metric_id"]
        self.var_symbols: dict = {}

    def analyze(self, df: pd.DataFrame, *args, **kwargs) -> dict[str, Any]:
        """
        Analyze the given DataFrame to calculate leverage IDs.

        :param df: DataFrame containing the values for the metrics.
        :param args: Additional arguments.
        :param kwargs: Additional keyword arguments.
        :returns: Dictionary containing the leverage ID results.
        """
        # Initialize variable symbols for sympy
        self.var_symbols = {var: sp.symbols(var) for var in df.columns if var != "date"}

        # Extract equations from the metric expression
        equations_list = self._extract_expression()

        # Create parent and root dictionaries
        parent_dict, root_dict = self._create_parent_and_root_dicts()

        logger.info(f"Extracted Equations List: {equations_list}")
        logger.info(f"Variable Symbols: {self.var_symbols}")
        logger.info(f"Parent Dict: {parent_dict}")
        logger.info(f"Top Parent Dict: {root_dict}")

        # Analyze parent and root metrics
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

    def _extract_expression(self) -> list[str]:
        """
        Extract equations from the metric expression.

        :returns: List of equations as strings.
        """
        # Initialize the list of equations with the root metric equation
        equations = [f"{self.root} = {self.metric_expression.get('expression_str', '')}"]

        def recurse_expressions(expression: dict | None, parent_metric_id: str | None = None):
            """
            Recursively extract equations from nested expressions.

            :param expression: Dictionary containing the metric expression.
            :param parent_metric_id: ID of the parent metric.
            """
            if not expression:
                return
            if "expression_str" in expression:
                equations.append(f"{parent_metric_id} = {expression['expression_str']}")
            for operand in expression.get("operands", []):
                if "expression" in operand:
                    equations.append(f"{operand['metric_id']} = {operand['expression_str']}")
                    recurse_expressions(operand["expression"], operand["metric_id"])

        # Start recursion with the root metric expression
        recurse_expressions(self.metric_expression["expression"], self.root)
        return [eq.replace("{", "").replace("}", "") for eq in equations]

    def _create_parent_and_root_dicts(self) -> tuple[dict[str, str], dict[str, str]]:
        """
        Create dictionaries for parent and root metrics.

        :returns: Tuple containing parent dictionary and root dictionary.
        """
        parent_dict = {}

        def find_parent_metrics(expression: dict | None, current_parent: str):
            """
            Recursively find parent metrics and populate the parent dictionary.

            :param expression: Dictionary containing the metric expression.
            :param current_parent: ID of the current parent metric.
            """
            if not expression:
                return
            for operand in expression.get("operands", []):
                if "metric_id" in operand:
                    metric_id = operand["metric_id"]
                    parent_dict[metric_id] = current_parent
                    find_parent_metrics(operand.get("expression"), metric_id)

        # Start recursion with the root metric expression
        find_parent_metrics(self.metric_expression["expression"], self.root)
        return parent_dict, {metric_id: self.root for metric_id in parent_dict}

    def _compute_y(self, equation_str: str, values: dict) -> float:
        """
        Compute the value of the equation with given values.

        :param equation_str: Equation as a string.
        :param values: Dictionary containing variable values.
        :returns: Computed value as float.
        """
        # Convert the equation string to a sympy expression
        equation = sp.sympify(equation_str, locals=self.var_symbols)
        # Substitute the variable values into the equation and compute the result
        return float(equation.subs({self.var_symbols[var]: value for var, value in values.items()}))

    def _compute_ymax(self, equation_str: str, values: dict, var: str, max_value: float) -> float:
        """
        Compute the maximum value of the equation with given values and max value for a variable.

        :param equation_str: Equation as a string.
        :param values: Dictionary containing variable values.
        :param var: Variable name as string.
        :param max_value: Maximum value for the variable.
        :returns: Computed maximum value as float.
        """
        # Create a copy of the values dictionary and set the variable to its maximum value
        max_var_values = values.copy()
        max_var_values[var] = max_value
        # Compute the result with the maximum value
        return self._compute_y(equation_str, max_var_values)

    def analyze_parent(self, parent_dict: dict, equations_list: list, values_df: pd.DataFrame) -> list[dict]:
        """
        Analyze parent metrics to calculate percentage differences.

        :param parent_dict: Dictionary containing parent metrics.
        :param equations_list: List of equations as strings.
        :param values_df: DataFrame containing the values for the metrics.
        :returns: List of dictionaries containing analysis results.
        """
        parent_results = []
        # Create a dictionary of equations for quick lookup
        equation_dict = {eq.split(" = ")[0].strip(): eq.split(" = ")[1].strip() for eq in equations_list}

        for var, parent_metric in parent_dict.items():
            parent_expr_str = equation_dict.get(parent_metric)
            if parent_expr_str:
                for _, row in values_df.iterrows():
                    current_values = row.drop("date").to_dict()
                    y = self._compute_y(parent_expr_str, current_values)
                    ymax = self._compute_ymax(parent_expr_str, current_values, var, self.max_values.get(var, 0))
                    percentage_diff = (ymax - y) / y * 100 if y != 0 else float("inf")

                    parent_results.append(
                        {
                            "date": row["date"],
                            "variable": var,
                            "parent_metric": parent_metric,
                            "pct_diff": float(percentage_diff),
                        }
                    )

        # Sort the results by date and percentage difference in descending order
        return sorted(
            parent_results,
            key=lambda x: (x["date"], x["pct_diff"] if x["pct_diff"] is not None else float("inf")),
            reverse=True,
        )

    def analyze_root(self, equations_list: list, values_df: pd.DataFrame) -> list[dict]:
        """
        Analyze root metrics to calculate percentage differences.

        :param equations_list: List of equations as strings.
        :param values_df: DataFrame containing the values for the metrics.
        :returns: List of dictionaries containing analysis results.
        """
        root_results = []
        # Create a dictionary of equations for quick lookup
        equation_dict = {eq.split(" = ")[0].strip(): eq.split(" = ")[1].strip() for eq in equations_list}

        def compute_percentage_diff(equation_str, current_values, var):
            """
            Compute the percentage difference for a given equation and variable values.

            :param equation_str: Equation as a string.
            :param current_values: Dictionary containing variable values.
            :param var: Variable name as string.
            :returns: Computed percentage difference as float.
            """
            y = self._compute_y(equation_str, current_values)
            ymax = self._compute_ymax(equation_str, current_values, var, self.max_values.get(var, 0))
            return (ymax - y) / y * 100 if y != 0 else float("inf")

        for _, row in values_df.iterrows():
            current_values = row.drop("date").to_dict()
            visited_vars = set()
            equations_to_process = [equation_dict[self.root]]

            while equations_to_process:
                current_eq = equations_to_process.pop(0)
                equation_expr = sp.sympify(current_eq, locals=self.var_symbols)
                symbols = equation_expr.free_symbols

                for symbol in symbols:
                    var = str(symbol)
                    if var not in visited_vars and var != self.root:
                        visited_vars.add(var)
                        percentage_diff = compute_percentage_diff(current_eq, current_values, var)
                        root_results.append(
                            {
                                "date": row["date"],
                                "variable": var,
                                "root_metric": self.root,
                                "pct_diff_root": float(percentage_diff),
                            }
                        )
                        if var in equation_dict:
                            equations_to_process.append(equation_dict[var])

        # Sort the results by date and percentage difference in descending order
        return sorted(
            root_results,
            key=lambda x: (x["date"], x["pct_diff_root"] if x["pct_diff_root"] is not None else float("inf")),
            reverse=True,
        )

    def combine_results(self, parent_results: list[dict], root_results: list[dict]) -> list[dict]:
        """
        Combine parent and root analysis results.

        :param parent_results: List of dictionaries containing parent analysis results.
        :param root_results: List of dictionaries containing root analysis results.
        :returns: List of combined analysis results.
        """
        # Create a lookup dictionary for root results
        root_lookup = {(result["date"], result["variable"]): result["pct_diff_root"] for result in root_results}
        # Combine parent results with corresponding root results
        return [
            {
                **parent_result,
                "top_metric": self.root,
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

    def build_output_structure(self, aggregated_df: pd.DataFrame) -> dict:
        """
        Build the output structure for the leverage ID results.

        :param aggregated_df: Aggregated DataFrame containing analysis results.
        :returns: Dictionary containing the output structure.
        """

        def recursive_build(expression: dict) -> list[dict]:
            """
            Recursively build the output structure for nested expressions.

            :param expression: Dictionary containing the metric expression.
            :returns: List of dictionaries representing the output structure.
            """
            if not expression or expression.get("type", "") != "expression":
                return []

            components = []
            for operand in expression["operands"]:
                if operand.get("type", "") == "metric":
                    metric_id = operand["metric_id"]
                    metric_details = self.get_metric_details(metric_id, aggregated_df)
                    if metric_details:
                        components.append(
                            {
                                "metric_id": metric_id,
                                "leverage": {
                                    "pct_diff": metric_details["pct_diff"],
                                    "pct_diff_root": metric_details["pct_diff_root"],
                                },
                                "components": recursive_build(operand.get("expression", {})),
                            }
                        )
            return components

        # Get the details of the top metric
        top_metric_details = self.get_metric_details(self.root, aggregated_df)
        return {
            "metric_id": self.root,
            "leverage": {
                "pct_diff": top_metric_details["pct_diff"] if top_metric_details else 0,
                "pct_diff_root": top_metric_details["pct_diff_root"] if top_metric_details else 0,
            },
            "components": recursive_build(self.metric_expression["expression"]),
        }
