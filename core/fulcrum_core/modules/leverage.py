import logging

import pandas as pd
import sympy as sp

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class LeverageCalculator:
    def __init__(self):
        """
        Initialize the LeverageCalculator class.
        """
        self.metric_json = None
        self.values_df = None
        self.max_values = None
        self.top_parent = None
        self.equations_list = []
        self.parent_dict = {}
        self.top_parent_dict = {}
        self.var_symbols = {}

    def _extract_expression(self, metric_json: dict) -> list[str]:
        """
        Extract the list of equations from the metric JSON.
        """
        equations = [f"{metric_json['metric_id']} = {metric_json['metric_expression'].get('expression_str', '')}"]

        def recurse_expressions(expression: dict | None, parent_metric_id: str | None = None):
            if expression is None:
                return
            if "expression_str" in expression:
                equations.append(f"{parent_metric_id} = {expression['expression_str']}")
            for operand in expression.get("operands", []):
                if "expression" in operand:
                    equations.append(f"{operand['metric_id']} = {operand['expression_str']}")
                    recurse_expressions(operand["expression"], operand["metric_id"])

        expr = metric_json["metric_expression"]["expression"]
        if expr:
            recurse_expressions(expr, metric_json["metric_id"])
        return [eq.replace("{", "").replace("}", "") for eq in equations]

    def _create_parent_and_top_parent_dicts(self, metric_json: dict) -> tuple[dict[str, str], dict[str, str]]:
        """
        Create dictionaries for parent and top parent metrics.
        """
        parent_dict = {}
        top_parent_dict = {}

        def find_parent_metrics(expression: dict | None, current_parent: str):
            if expression is None:
                return
            for operand in expression.get("operands", []):
                if "metric_id" in operand:
                    metric_id = operand["metric_id"]
                    parent_dict[metric_id] = current_parent
                    find_parent_metrics(operand.get("expression"), metric_id)

        top_metric_id = metric_json["metric_id"]
        find_parent_metrics(metric_json["metric_expression"]["expression"], top_metric_id)

        top_parent_dict = {metric_id: top_metric_id for metric_id in parent_dict}
        return parent_dict, top_parent_dict

    def _compute_y(self, equation_str: str, values: dict) -> float:
        """
        Compute the value of the equation with the provided values.
        """
        equation = sp.sympify(equation_str, locals=self.var_symbols)
        return float(equation.subs({self.var_symbols[var]: value for var, value in values.items()}))

    def _compute_ymax(self, equation_str: str, values: dict, var: str, max_value: float) -> float:
        """
        Compute the value of the equation with the max value for a specific variable.
        """
        max_var_values = values.copy()
        max_var_values[var] = max_value
        equation = sp.sympify(equation_str, locals=self.var_symbols)
        return float(equation.subs({self.var_symbols[var]: value for var, value in max_var_values.items()}))

    def analyze_parent(self) -> list[dict]:
        """
        Compute the leverage percentages against parent metrics.
        """
        parent_results = []

        for var, parent_metric in self.parent_dict.items():
            parent_equation = next((eq for eq in self.equations_list if eq.startswith(parent_metric)), None)
            if parent_equation:
                _, parent_expr_str = parent_equation.split(" = ")

                for _, row in self.values_df.iterrows():
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
                            "parent_percentage_difference": float(percentage_diff),
                        }
                    )

        return sorted(
            parent_results,
            key=lambda x: (
                x["date"],
                x["parent_percentage_difference"] if x["parent_percentage_difference"] is not None else float("inf"),
            ),
            reverse=True,
        )

    def analyze_top_parent(self) -> list[dict]:
        """
        Compute the leverage percentages against top parent metrics.
        """
        top_parent_results = []

        def extract_rhs_equations():
            return {lhs.strip(): rhs.strip() for eq in self.equations_list for lhs, rhs in [eq.split("=")]}

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
                if var == self.top_parent or var in visited_nodes:
                    continue

                visited_nodes.add(var)
                y = self._compute_y(equation_str, without_date_values)
                ymax = self._compute_ymax(equation_str, without_date_values, var, self.max_values.get(var, 0))
                percentage_diff = (ymax - y) / y * 100 if y != 0 else float("inf")

                top_parent_results.append(
                    {
                        "date": current_values["date"],
                        "variable": var,
                        "top_parent_metric": self.top_parent,
                        "top_parent_percentage_difference": float(percentage_diff),
                    }
                )

        for _, row in self.values_df.iterrows():
            visited_nodes: set = set()
            current_values = row.to_dict()

            rhs_equations = extract_rhs_equations()
            top_parent_eq = self.equations_list[0].split("=")[1].strip()
            visited_equations = [top_parent_eq]
            visited_vars = set()
            compute_percentage_diff(top_parent_eq, current_values)

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
            top_parent_results,
            key=lambda x: (
                x["date"],
                (
                    x["top_parent_percentage_difference"]
                    if x["top_parent_percentage_difference"] is not None
                    else float("inf")
                ),
            ),
            reverse=True,
        )

    def combine_results(self, parent_results: list[dict], top_parent_results: list[dict]) -> list[dict]:
        """
        Combine parent and top parent results.
        """
        top_parent_lookup = {
            (result["date"], result["variable"]): result["top_parent_percentage_difference"]
            for result in top_parent_results
        }

        final_results = [
            {
                "date": parent_result["date"],
                "variable": parent_result["variable"],
                "parent_metric": parent_result["parent_metric"],
                "top_metric": self.top_parent,
                "parent_percentage_difference": parent_result["parent_percentage_difference"],
                "top_parent_percentage_difference": top_parent_lookup.get(
                    (parent_result["date"], parent_result["variable"]), None
                ),
            }
            for parent_result in parent_results
        ]

        return final_results

    def get_metric_details(self, metric_id: str, aggregated_df: pd.DataFrame) -> dict | None:
        """
        Extract the details for a given metric_id.
        """
        metric_details = aggregated_df[aggregated_df["variable"] == metric_id]
        if not metric_details.empty:
            return {
                "metric_id": metric_id,
                "parent_metric": metric_details["parent_metric"].values[0],
                "top_metric": metric_details["top_metric"].values[0],
                "parent_percentage_difference": metric_details["parent_percentage_difference"].values[0],
                "top_parent_percentage_difference": metric_details["top_parent_percentage_difference"].values[0],
            }
        return None

    def build_output_structure(self, aggregated_df: pd.DataFrame) -> dict:
        """
        Build the output structure recursively.
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
                            "parent_metric": metric_details["parent_metric"],
                            "top_metric": metric_details["top_metric"],
                            "parent_percentage_difference": metric_details["parent_percentage_difference"],
                            "top_parent_percentage_difference": metric_details["top_parent_percentage_difference"],
                            "components": recursive_build(operand.get("expression", {})),
                        }
                        components.append(component)
            return components

        top_metric_id = self.metric_json["metric_id"]
        top_metric_details = self.get_metric_details(top_metric_id, aggregated_df)

        return {
            "metric_id": top_metric_id,
            "parent_metric": top_metric_details["parent_metric"] if top_metric_details else None,
            "top_metric": top_metric_details["top_metric"] if top_metric_details else None,
            "parent_percentage_difference": (
                top_metric_details["parent_percentage_difference"] if top_metric_details else None
            ),
            "top_parent_percentage_difference": (
                top_metric_details["top_parent_percentage_difference"] if top_metric_details else None
            ),
            "components": recursive_build(self.metric_json["metric_expression"]["expression"]),
        }

    def run(self, metric_json: dict, values_df: pd.DataFrame, max_values: dict) -> dict:
        """
        Execute the entire workflow of the LeverageCalculator.
        """
        self.metric_json = metric_json
        self.values_df = values_df
        self.max_values = max_values
        self.top_parent = metric_json["metric_id"]

        self.equations_list = self._extract_expression(metric_json)
        self.parent_dict, self.top_parent_dict = self._create_parent_and_top_parent_dicts(metric_json)
        self.var_symbols = {var: sp.symbols(var) for var in self.values_df.columns if var != "date"}

        logger.info(f"Extracted Equations List: {self.equations_list}")
        logger.info(f"Variable Symbols: {self.var_symbols}")
        logger.info(f"Parent Dict: {self.parent_dict}")
        logger.info(f"Top Parent Dict: {self.top_parent_dict}")

        logger.info("Starting parent analysis...")
        parent_results = self.analyze_parent()
        logger.info("Parent analysis completed.")

        logger.info("Starting top parent analysis...")
        top_parent_results = self.analyze_top_parent()
        logger.info("Top parent analysis completed.")

        final_results = self.combine_results(parent_results, top_parent_results)
        df = pd.DataFrame(final_results)

        aggregated_df = (
            df.groupby("variable")
            .agg(
                parent_metric=("parent_metric", "first"),
                top_metric=("top_metric", "first"),
                parent_percentage_difference=("parent_percentage_difference", "mean"),
                top_parent_percentage_difference=("top_parent_percentage_difference", "mean"),
            )
            .reset_index()
        )

        output = self.build_output_structure(aggregated_df)
        return output
