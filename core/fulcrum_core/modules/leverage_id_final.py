import sympy as sp
import logging
import pandas as pd
from datetime import date

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LeverageCalculator:
    def __init__(self, metric_json: dict, values_df: pd.DataFrame, max_values: dict):
        """
        Initialize the LeverageCalculator class.
        :param metric_json: JSON object describing the metric expression.
        :param values_df: DataFrame with time series data of variable values.
        :param max_values: Dictionary of variable maximum values.
        :param top_parent: Top metric
        :param equations_list: List of all equations calculated from extract_expression method
        """
        self.metric_json = metric_json
        self.values_df = values_df
        self.max_values = max_values
        self.top_parent = metric_json['metric_id']

        # Parse the metric JSON to get the list of equations
        self.equations_list = self.extract_expression(metric_json)
        self.equations_list = [eq.replace("{", "").replace("}", "") for eq in self.equations_list]
        logger.info(f"Extracted Equations List: {self.equations_list}")
        
        self.parent_dict = {}
        self.top_parent_dict = {}
        
        # Variable symbols
        self.var_symbols = {var: sp.symbols(var) for var in self.values_df.columns if var != 'date'}
        logger.info(f"Variable Symbols: {self.var_symbols}")
        
        # Create parent and top parent dictionaries
        self.create_parent_and_top_parent_dicts(metric_json)
        logger.info(f"Parent Dict: {self.parent_dict}")
        logger.info(f"Top Parent Dict: {self.top_parent_dict}")

    def extract_expression(self, metric_json):
        """
        First append expression for top metric on LHS
        Recursively get all metrics and their expression 
        """
        equations = []
        equations.append(f"{metric_json['metric_id']} = {metric_json['expression_str']}")

        def recurse_expressions(expression, parent_metric_id=None):
            if expression is None:
                return
            if 'expression_str' in expression:
                equations.append(f"{parent_metric_id} = {expression['expression_str']}")
            if 'operands' in expression:
                for operand in expression['operands']:
                    if 'expression' in operand:
                        equations.append(f"{operand['metric_id']} = {operand['expression_str']}")
                        recurse_expressions(operand['expression'], operand['metric_id'])
                        
        recurse_expressions(metric_json['expression'], metric_json['metric_id'])
        return equations

    def create_parent_and_top_parent_dicts(self, metric_json):
        """
        Traverse in metric_json, For each metric, find parent for each metrics
        """
        def find_parent_metrics(expression, current_parent):
            if 'operator' in expression and 'operands' in expression:
                for operand in expression['operands']:
                    if 'metric_id' in operand:
                        metric_id = operand['metric_id']
                        self.parent_dict[metric_id] = current_parent
                        # Recursively find parents for deeper levels
                        if 'expression' in operand:
                            find_parent_metrics(operand['expression'], metric_id)

        # Starting point is the root metric
        top_metric_id = metric_json['metric_id']
        find_parent_metrics(metric_json['expression'], top_metric_id)

        # Set the top parent for all metrics as the top metric
        top_parent = metric_json['metric_id']
        for metric_id in self.parent_dict:
            self.top_parent_dict[metric_id] = top_parent

    def _compute_y(self, equation_str, values: dict) -> float:
        """
        Compute the value of the equation with the provided values.
        :param equation_str: The equation string.
        :param values: Dictionary of variable values at a specific time point.
        :return: Computed value.
        """
        equation = sp.sympify(equation_str, locals=self.var_symbols)
        res = equation.subs({self.var_symbols[var]: value for var, value in values.items()})
        return res

    def _compute_ymax(self, equation_str, values: dict, var: str, max_value: float) -> float:
        """
        Compute the value of the equation with the max value for a specific variable.
        :param equation_str: The equation string.
        :param values: Dictionary of variable values at a specific time point.
        :param var: Variable name.
        :param max_value: Maximum value of the variable.
        :return: Computed value with the max value of the variable.
        """
        max_var_values = values.copy()
        max_var_values[var] = max_value
        equation = sp.sympify(equation_str, locals=self.var_symbols)
        return float(equation.subs({self.var_symbols[var]: value for var, value in max_var_values.items()}))

    def analyze_parent(self) -> list:
        """
        Compute the leverage percentages against parent metrics.
        :return: List of dict with variable names, their leverage percentages against parent metrics.
        """
        parent_results = []

        for var, parent_metric in self.parent_dict.items():
            # Find the parent metric's equation in equations_list
            parent_equation = next((eq for eq in self.equations_list if eq.startswith(parent_metric)), None)
            
            if parent_equation:
                _, parent_expr_str = parent_equation.split(" = ")

                for _, row in self.values_df.iterrows():
                    date = row['date']
                    current_values = row.drop('date').to_dict()
                    y = self._compute_y(parent_expr_str, current_values)
                    ymax = self._compute_ymax(parent_expr_str, current_values, var, self.max_values.get(var, 0))
                    percentage_diff = (ymax - y) / y * 100 if y != 0 else float('inf')
                    # print(date)
                    # print(var)
                    # print(parent_metric)
                    # print(parent_expr_str)
                    # print(float(percentage_diff))

                    parent_results.append({
                        "date": date,
                        "variable": var,
                        "parent_metric": parent_metric,
                        # "current_value": current_values.get(var, None),
                        # "max_value": self.max_values.get(var, None),
                        "parent_percentage_difference": float(percentage_diff)
                    })

        sorted_parent_results = sorted(parent_results, key=lambda x: (x["date"], x["parent_percentage_difference"] if x["parent_percentage_difference"] is not None else float('inf')), reverse=True)

        return sorted_parent_results

    def analyze_top_parent(self) -> list:
        """
        Compute the leverage percentages against top parent metrics.
        :return: List of dict with variable names, their leverage percentages against top parent metrics.
        """
        top_parent_results = []
        

        def extract_rhs_equations():
            rhs_equations = {}
            for eq in self.equations_list:
                lhs, rhs = eq.split('=')
                rhs_equations[lhs.strip()] = rhs.strip()
            return rhs_equations

        def compute_percentage_diff(equation_str, current_values):
            """
            Compute percentage differences for a given equation.
            :param equation_str: The equation string.
            :param current_values: Dictionary of current variable values.
            :return: None
            """
            without_date_values = {k: v for k, v in current_values.items() if k != 'date'}
            
            # Create the equation
            equation_expr = sp.sympify(equation_str, locals=self.var_symbols)
            
            symbols = equation_expr.free_symbols

            for symbol in symbols:
                var = str(symbol)

                # Skip if the variable is the top parent itself or already visited
                if var == self.top_parent or var in visited_nodes:
                    continue

                visited_nodes.add(var)

                y = self._compute_y(equation_str, without_date_values)
                ymax = self._compute_ymax(equation_str, without_date_values, var, self.max_values.get(var, 0))

                percentage_diff = (ymax - y) / y * 100 if y != 0 else float('inf')
                # print(current_values['date'])
                # print(var)
                # print(equation_str)
                # print(float(percentage_diff))
                

                top_parent_results.append({
                    "date": current_values['date'],
                    "variable": var,
                    "top_parent_metric": self.top_parent,
                    "top_parent_percentage_difference": float(percentage_diff)
                })


        # Iterate through the rows of values_df and equations_list to calculate percentage differences
        for _, row in self.values_df.iterrows():
            visited_nodes = set()
            date = row['date']
            current_values = row.drop('date').to_dict()
            current_values['date'] = date

            rhs_equations = extract_rhs_equations()
            top_parent_eq = self.equations_list[0].split('=')[1].strip()
            visited_equations = [top_parent_eq]
            visited_vars = set()
            compute_percentage_diff(top_parent_eq,current_values)


            while visited_equations:
                current_eq = visited_equations.pop(0)
                terms = current_eq.split('*')

                for term in terms:
                    term = term.strip()
                    if term in rhs_equations and term not in visited_vars:
                        expanded_eq = current_eq.replace(term, rhs_equations[term])
                        visited_equations.append(expanded_eq)
                        visited_vars.add(term)
                        # print(f"Expanded {term} to {rhs_equations[term]}: {expanded_eq}")
                        compute_percentage_diff(expanded_eq,current_values)

        # Sort results based on date and percentage difference, if available
        sorted_top_parent_results = sorted(
            top_parent_results, 
            key=lambda x: (x["date"], x["top_parent_percentage_difference"] if x["top_parent_percentage_difference"] is not None else float('inf')), 
            reverse=True
        )

        return sorted_top_parent_results
    
    def combine_results(self,parent_results, top_parent_results):
        # Create a lookup dictionary for top_parent_results by date and variable
        top_parent_lookup = {
            (result["date"], result["variable"]): result["top_parent_percentage_difference"]
            for result in top_parent_results
        }

        final_results = []

        for parent_result in parent_results:
            # Check if there's a corresponding top parent result
            key = (parent_result["date"], parent_result["variable"])
            top_parent_diff = top_parent_lookup.get(key, None)

            # Combine parent and top parent results into final result
            final_results.append({
                "date": parent_result["date"],
                "variable": parent_result["variable"],
                "parent_metric": parent_result["parent_metric"],
                "top_metric": self.top_parent,
                "parent_percentage_difference": parent_result["parent_percentage_difference"],
                "top_parent_percentage_difference": top_parent_diff
            })

        return final_results
    
    def get_metric_details(self,metric_id,aggregated_df):
        # Extract the details for a given metric_id
        metric_details = aggregated_df[aggregated_df['variable'] == metric_id]
        if not metric_details.empty:
            return {
                "metric_id": metric_id,
                "parent_metric": metric_details['parent_metric'].values[0],
                "top_metric": metric_details['top_metric'].values[0],
                "parent_percentage_difference": metric_details['parent_percentage_difference'].values[0],
                "top_parent_percentage_difference": metric_details['top_parent_percentage_difference'].values[0]
            }
        return None
    
    def build_output_structure(self, aggregated_df):
        def recursive_build(expression):
            components = []
            if expression['type'] == 'expression':
                operands = expression['operands']
                for operand in operands:
                    if operand['type'] == 'metric':
                        metric_id = operand['metric_id']
                        metric_details = self.get_metric_details(metric_id, aggregated_df)
                        if metric_details:
                            component = {
                                "metric_id": metric_id,
                                "parent_metric": metric_details['parent_metric'],
                                "top_metric": metric_details['top_metric'],
                                "parent_percentage_difference": metric_details['parent_percentage_difference'],
                                "top_parent_percentage_difference": metric_details['top_parent_percentage_difference'],
                                "components": recursive_build(operand['expression']) if 'expression' in operand else []
                            }
                            components.append(component)
            return components

        top_metric_id = self.metric_json['metric_id']
        top_metric_details = self.get_metric_details(top_metric_id, aggregated_df)
        
        return {
            "metric_id": top_metric_id,
            "parent_metric": top_metric_details['parent_metric'] if top_metric_details else None,
            "top_metric": top_metric_details['top_metric'] if top_metric_details else None,
            "parent_percentage_difference": top_metric_details['parent_percentage_difference'] if top_metric_details else None,
            "top_parent_percentage_difference": top_metric_details['top_parent_percentage_difference'] if top_metric_details else None,
            "components": recursive_build(self.metric_json['expression'])
        }
    
    def run(self):
        """
        Execute the entire workflow of the LeverageCalculator.
        :return: A tuple containing two lists:
                - Parent analysis results
                - Top parent analysis results
                - Combine results
                - Aggregate 
                - Build output
        """
        logger.info("Starting parent analysis...")
        parent_results = self.analyze_parent()
        logger.info("Parent analysis completed.")
        
        logger.info("Starting top parent analysis...")
        top_parent_results = self.analyze_top_parent()
        logger.info("Top parent analysis completed.")

        final_results = self.combine_results(parent_results,top_parent_results)

        df = pd.DataFrame(final_results)

        # Group by 'variable' and calculate the mean for percentage differences
        aggregated_df = df.groupby('variable').agg(
            parent_metric=('parent_metric', 'first'),
            top_metric=('top_metric', 'first'),
            parent_percentage_difference=('parent_percentage_difference', 'mean'),
            top_parent_percentage_difference=('top_parent_percentage_difference', 'mean')
        ).reset_index()

        output = self.build_output_structure(aggregated_df)
        
        return output


# Example data and initialization
metric_json = {
  "metric_id": "XA",
  "type": "metric",
  "expression_str": "{XB} * {XC} * {XD}",
  "expression": {
    "type": "expression",
    "operator": "*",
    "operands": [
      {
        "type": "metric",
        "metric_id": "XB",
        "period": 0,
        "expression_str": "{XE} * {XF}",
        "expression": {
          "type": "expression",
          "operator": "*",
          "operands": [
            {
              "type": "metric",
              "metric_id": "XE",
              "period": 0
            },
            {
              "type": "metric",
              "metric_id": "XF",
              "period": 0,
              "expression_str": "{XG} * {XH}",
              "expression": {
                "type": "expression",
                "operator": "*",
                "operands": [
                  {
                    "type": "metric",
                    "metric_id": "XG",
                    "period": 0
                  },
                  {
                    "type": "metric",
                    "metric_id": "XH",
                    "period": 0
                  }
                ]
              }
            }
          ]
        }
      },
      {
        "type": "metric",
        "metric_id": "XC",
        "period": 0,
        "expression_str": "{XI} * {XJ}",
        "expression": {
          "type": "expression",
          "operator": "*",
          "operands": [
            {
              "type": "metric",
              "metric_id": "XI",
              "period": 0
            },
            {
              "type": "metric",
              "metric_id": "XJ",
              "period": 0
            }
          ]
        }
      },
      {
        "type": "metric",
        "metric_id": "XD",
        "period": 0,
        "expression_str": "{XK} * {XL}",
        "expression": {
          "type": "expression",
          "operator": "*",
          "operands": [
            {
              "type": "metric",
              "metric_id": "XK",
              "period": 0
            },
            {
              "type": "metric",
              "metric_id": "XL",
              "period": 0,
              "expression_str": "{XM} * {XN}",
              "expression": {
                "type": "expression",
                "operator": "*",
                "operands": [
                  {
                    "type": "metric",
                    "metric_id": "XM",
                    "period": 0
                  },
                  {
                    "type": "metric",
                    "metric_id": "XN",
                    "period": 0
                  }
                ]
              }
            }
          ]
        }
      }
    ]
  }
}

# Sample time series data
data = {
    'date': pd.date_range(start='2023-01-01', periods=5),
    "XA": [10, 20, 30, 40, 50],
    "XB": [15, 25, 30, 45, 55],
    "XC": [20, 30, 40, 50, 60],
    "XD": [25, 35, 45, 50, 65],
    'XE': [10, 20, 30, 40, 50],
    'XF': [1.5, 1.6, 1.7, 1.8, 1.9],
    'XG': [2, 3, 4, 5, 6],
    'XH': [2, 3, 4, 5, 6],
    'XI': [5, 6, 7, 8, 9],
    'XJ': [3, 4, 5, 6, 7],
    'XK': [7, 8, 9, 10, 11],
    'XL': [2, 2.5, 3, 3.5, 4],
    'XM': [1, 1.1, 1.2, 1.3, 1.4],
    'XN': [3, 3.1, 3.2, 3.3, 3.4]
}

# Maximum values for each variable
max_values = {
    "XA": 70,
    "XB": 65,
    "XC": 70,
    "XD": 75,
    'XE': 60,
    'XF': 2.0,
    'XG': 8,
    'XH': 9,
    'XI': 10,
    'XJ': 8,
    'XK': 15,
    'XL': 5,
    'XM': 2,
    'XN': 5
}

values_df = pd.DataFrame(data)

# Instantiate the LeverageCalculator
calculator = LeverageCalculator(metric_json, values_df, max_values)

final_result = calculator.run()
print(final_result)



