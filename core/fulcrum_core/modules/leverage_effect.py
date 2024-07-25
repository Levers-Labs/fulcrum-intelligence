import sympy as sp
import logging

logger = logging.getLogger(__name__)

class LeverageCalculator:
    def __init__(self, equation_str: str, variables: list[str], values: dict[str, float], max_values: dict[str, float]):
        """
        Initialize the LeverageCalculator class.
        :param equation_str: Equation as a string.
        :param variables: List of variable names.
        :param values: Dictionary of variable values.
        :param max_values: Dictionary of variable maximum values.
        """
        self.equation_str = equation_str
        self.variables = variables
        self.values = values
        self.max_values = max_values
        self.equation = sp.sympify(equation_str)
        self.var_symbols = {var: sp.symbols(var) for var in variables}
    
    def _compute_y(self) -> float:
        """
        Compute the initial value of the equation with the provided values.
        """
        return self.equation.subs({self.var_symbols[var]: value for var, value in self.values.items()})

    def _compute_ymax(self, var: str, max_value: float) -> float:
        """
        Compute the value of the equation with the max value for a specific variable.
        :param var: Variable name.
        :param max_value: Maximum value of the variable.
        """
        max_var_values = self.values.copy()
        max_var_values[var] = max_value
        return self.equation.subs({self.var_symbols[var]: value for var, value in max_var_values.items()})

    def analyze(self) -> list[dict]:
        """
        Compute the leverage percentages and return the results in a structured format.
        :return: List of dict with variable names, their leverage percentages, and the original/max values.
        """
        y = self._compute_y()
        results = []
        
        for var, max_value in self.max_values.items():
            ymax = self._compute_ymax(var, max_value)
            percentage_diff = (ymax - y) / y * 100
            results.append({
                "variable": var,
                "current_value": self.values[var],
                "max_value": max_value,
                "percentage_difference": float(percentage_diff)
            })
        
        sorted_results = sorted(results, key=lambda x: x["percentage_difference"], reverse=True)
        return sorted_results

# Example 4: Mixed
equation_str = "a * b + c * d - e"
variables = ["a", "b", "c", "d", "e"]
values = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
max_values = {"a": 3, "b": 4, "c": 5, "d": 6, "e": 7}

calculator = LeverageCalculator(equation_str, variables, values, max_values)
results = calculator.analyze()
for result in results:
    print(f"Variable: {result['variable']}, Current Value: {result['current_value']}, "
          f"Max Value: {result['max_value']}, Percentage Difference: {result['percentage_difference']:.2f}%")



