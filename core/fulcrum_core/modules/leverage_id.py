import logging
from typing import Any

import pandas as pd
import sympy as sp

from fulcrum_core.modules import BaseAnalyzer

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class LeverageIdCalculator(BaseAnalyzer):

    def __init__(self, metric_expression: dict, max_values: dict, **kwargs):
        """
        Initialize the LeverageCalculator class.

        :param metric_expression: Dictionary containing the metric expression.
        :param max_values: Dictionary containing the maximum values for the metrics.
        :param kwargs: Additional keyword arguments.
        """
        self.metric_expression = metric_expression
        self.max_values = max_values
        super().__init__(**kwargs)

        self.root = None
        self.var_symbols: dict = {}

    def analyze(self, df: pd.DataFrame, *args, **kwargs) -> dict[str, Any]:
        """
        Execute the entire workflow of the LeverageCalculator.

        :param df: DataFrame containing the values for the metrics.
        :param args: Additional arguments.
        :param kwargs: Additional keyword arguments.
        :returns: Dictionary containing the leverage analysis results.
        """
        # Initialize variable symbols for sympy
        self.var_symbols = {var: sp.symbols(var) for var in df.columns if var != "date"}

        # Set the root metric
        self.root = self.metric_expression["metric_id"]

        # Extract equations and create parent and root dictionaries
        equations_list = self._extract_expression()
        parent_dict, root_dict = self._create_parent_and_root_dicts()

        logger.info(f"Extracted Equations List: {equations_list}")
        logger.info(f"Variable Symbols: {self.var_symbols}")
        logger.info(f"Parent Dict: {parent_dict}")
        logger.info(f"Top Parent Dict: {root_dict}")

        # Perform parent and root analysis
        logger.info("Starting parent analysis...")
        parent_results = self.analyze_parent(parent_dict, equations_list, df)
        logger.info("Parent analysis completed.")

        logger.info("Starting top parent analysis...")
        root_results = self.analyze_root(equations_list, df)
        logger.info("Top parent analysis completed.")

        # Combine results and build the output structure
        final_results = self.combine_results(parent_results, root_results)
        df = pd.DataFrame(final_results)

        aggregated_df = (
            df.groupby("variable")
            .agg(
                parent_metric=("parent_metric", "first"),
                top_metric=("top_metric", "first"),
                pct_diff=("pct_diff", "mean"),
                pct_diff_root=("pct_diff_root", "mean"),
            )
            .reset_index()
        )

        output = self.build_output_structure(aggregated_df)
        return output

    def _extract_expression(self) -> list[str]:
        """
        Extract the list of equations from the metric JSON.

        :returns: List of equations as strings.
        """
        equations = [f"{self.metric_expression['metric_id']} = " f"{self.metric_expression.get('expression_str', '')}"]

        def recurse_expressions(expression: dict | None, parent_metric_id: str | None = None):
            if expression is None:
                return
            if "expression_str" in expression:
                equations.append(f"{parent_metric_id} = {expression['expression_str']}")
            for operand in expression.get("operands", []):
                if "expression" in operand:
                    equations.append(f"{operand['metric_id']} = {operand['expression_str']}")
                    recurse_expressions(operand["expression"], operand["metric_id"])

        expr = self.metric_expression["expression"]
        if expr:
            recurse_expressions(expr, self.metric_expression["metric_id"])
        return [eq.replace("{", "").replace("}", "") for eq in equations]

    def _create_parent_and_root_dicts(self) -> tuple[dict[str, str], dict[str, str]]:
        """
        Create dictionaries for parent and top parent metrics.

        :returns: Tuple containing parent and root dictionaries.
        """
        parent_dict = {}

        def find_parent_metrics(expression: dict | None, current_parent: str):
            if expression is None:
                return
            for operand in expression.get("operands", []):
                if "metric_id" in operand:
                    metric_id = operand["metric_id"]
                    parent_dict[metric_id] = current_parent
                    find_parent_metrics(operand.get("expression"), metric_id)

        top_metric_id = self.metric_expression["metric_id"]
        find_parent_metrics(self.metric_expression["expression"], top_metric_id)

        root_dict = {metric_id: top_metric_id for metric_id in parent_dict}
        return parent_dict, root_dict

    def _compute_y(self, equation_str: str, values: dict) -> float:
        """
        Compute the value of the equation with the given values.

        :param equation_str: The equation as a string.
        :param values: Dictionary containing the values for the variables.
        :returns: The computed value as a float.
        """
        equation = sp.sympify(equation_str, locals=self.var_symbols)
        return float(equation.subs({self.var_symbols[var]: value for var, value in values.items()}))

    def _compute_ymax(self, equation_str: str, values: dict, var: str, max_value: float) -> float:
        """
        Compute the value of the equation with the max value for a specific variable.

        :param equation_str: The equation as a string.
        :param values: Dictionary containing the values for the variables.
        :param var: The variable to be maximized.
        :param max_value: The maximum value for the variable.
        :returns: The computed value with the max variable as a float.
        """
        max_var_values = values.copy()
        max_var_values[var] = max_value
        equation = sp.sympify(equation_str, locals=self.var_symbols)
        return float(equation.subs({self.var_symbols[var]: value for var, value in max_var_values.items()}))

    def analyze_parent(self, parent_dict: dict, equations_list: list, values_df: pd.DataFrame) -> list[dict]:
        """
        Compute the leverage percentages against parent metrics.

        :param parent_dict: Dictionary mapping child metrics to parent metrics.
        :param equations_list: List of equations as strings.
        :param values_df: DataFrame containing the values for the metrics.
        :returns: List of dictionaries containing the parent analysis results.
        """
        parent_results = []

        for var, parent_metric in parent_dict.items():
            parent_equation = next((eq for eq in equations_list if eq.startswith(parent_metric)), None)
            if parent_equation:
                _, parent_expr_str = parent_equation.split(" = ")

                for _, row in values_df.iterrows():
                    date = row["date"]
                    current_values = row.drop("date").to_dict()
                    y = self._compute_y(parent_expr_str, current_values)
                    ymax = self._compute_ymax(parent_expr_str, current_values, var, self.max_values.get(var, 0))
                    percentage_diff = (ymax - y) / y * 100 if y != 0 else float("inf")

                    parent_results.append(
                        {
                            "date": date,
                            "variable": var,
                            "parent_metric": parent_metric,
                            "pct_diff": float(percentage_diff),
                        }
                    )

        return sorted(
            parent_results,
            key=lambda x: (
                x["date"],
                x["pct_diff"] if x["pct_diff"] is not None else float("inf"),
            ),
            reverse=True,
        )

    def analyze_root(self, equations_list: list, values_df: pd.DataFrame) -> list[dict]:
        """
        Compute the leverage percentages against top parent metrics.

        :param equations_list: List of equations as strings.
        :param values_df: DataFrame containing the values for the metrics.
        :returns: List of dictionaries containing the root analysis results.
        """
        root_results = []

        def extract_rhs_equations():
            return {lhs.strip(): rhs.strip() for eq in equations_list for lhs, rhs in [eq.split("=")]}

        def compute_percentage_diff(equation_str, current_values):
            without_date_values = {k: v for k, v in current_values.items() if k != "date"}
            try:
                equation_expr = sp.sympify(equation_str, locals=self.var_symbols)
                symbols = equation_expr.free_symbols
            except Exception as ex:
                logger.error(f"Exception: {ex}")
                return

            for symbol in symbols:
                var = str(symbol)
                if var == self.root or var in visited_nodes:
                    continue

                visited_nodes.add(var)
                y = self._compute_y(equation_str, without_date_values)
                ymax = self._compute_ymax(equation_str, without_date_values, var, self.max_values.get(var, 0))
                percentage_diff = (ymax - y) / y * 100 if y != 0 else float("inf")

                root_results.append(
                    {
                        "date": current_values["date"],
                        "variable": var,
                        "root_metric": self.root,
                        "pct_diff_root": float(percentage_diff),
                    }
                )

        for _, row in values_df.iterrows():
            visited_nodes: set = set()
            current_values = row.to_dict()

            rhs_equations = extract_rhs_equations()
            root_eq = equations_list[0].split("=")[1].strip()
            visited_equations = [root_eq]
            visited_vars = set()
            compute_percentage_diff(root_eq, current_values)

            while visited_equations:
                current_eq = visited_equations.pop(0)
                terms = [term.strip() for term in current_eq.split("*")]

                for term in terms:
                    if term in rhs_equations and term not in visited_vars:
                        expanded_eq = current_eq.replace(term, rhs_equations[term])
                        visited_equations.append(expanded_eq)
                        visited_vars.add(term)
                        compute_percentage_diff(expanded_eq, current_values)

        return sorted(
            root_results,
            key=lambda x: (
                x["date"],
                (x["pct_diff_root"] if x["pct_diff_root"] is not None else float("inf")),
            ),
            reverse=True,
        )

    def combine_results(self, parent_results: list[dict], root_results: list[dict]) -> list[dict]:
        """
        Combine parent and top parent results.

        :param parent_results: List of dictionaries containing the parent analysis results.
        :param root_results: List of dictionaries containing the root analysis results.
        :returns: List of dictionaries containing the combined results.
        """
        root_lookup = {(result["date"], result["variable"]): result["pct_diff_root"] for result in root_results}

        final_results = [
            {
                "date": parent_result["date"],
                "variable": parent_result["variable"],
                "parent_metric": parent_result["parent_metric"],
                "top_metric": self.root,
                "pct_diff": parent_result["pct_diff"],
                "pct_diff_root": root_lookup.get((parent_result["date"], parent_result["variable"]), None),
            }
            for parent_result in parent_results
        ]

        return final_results

    def get_metric_details(self, metric_id: str, aggregated_df: pd.DataFrame) -> dict | None:
        """
        Extract the details for a given metric_id.

        :param metric_id: The metric ID to get details for.
        :param aggregated_df: DataFrame containing the aggregated results.
        :returns: Dictionary containing the metric details or None if not found.
        """
        metric_details = aggregated_df[aggregated_df["variable"] == metric_id]
        if not metric_details.empty:
            return {
                "metric_id": metric_id,
                "parent_metric": metric_details["parent_metric"].values[0],
                "top_metric": metric_details["top_metric"].values[0],
                "pct_diff": metric_details["pct_diff"].values[0],
                "pct_diff_root": metric_details["pct_diff_root"].values[0],
            }
        return None

    def build_output_structure(self, aggregated_df: pd.DataFrame) -> dict:
        """
        Build the output structure recursively.

        :param aggregated_df: DataFrame containing the aggregated results.
        :returns: Dictionary containing the output structure.
        """

        def recursive_build(expression: dict) -> list[dict]:
            if expression is None or expression.get("type", "") != "expression":
                return []

            components = []
            for operand in expression["operands"]:
                if operand.get("type", "") == "metric":
                    metric_id = operand["metric_id"]
                    metric_details = self.get_metric_details(metric_id, aggregated_df)
                    if metric_details:
                        component = {
                            "metric_id": metric_id,
                            "leverage": {
                                "pct_diff": metric_details["pct_diff"],
                                "pct_diff_root": metric_details["pct_diff_root"],
                            },
                            "components": recursive_build(operand.get("expression", {})),
                        }
                        components.append(component)
            return components

        top_metric_id = self.metric_expression["metric_id"]
        top_metric_details = self.get_metric_details(top_metric_id, aggregated_df)

        return {
            "metric_id": top_metric_id,
            "leverage": {
                "pct_diff": top_metric_details["pct_diff"] if top_metric_details else 0,
                "pct_diff_root": top_metric_details["pct_diff_root"] if top_metric_details else 0,
            },
            "components": recursive_build(self.metric_expression["expression"]),
        }
