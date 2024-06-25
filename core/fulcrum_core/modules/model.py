import logging
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import PolynomialFeatures

from fulcrum_core.modules import BaseAnalyzer

logger = logging.getLogger(__name__)


class ModelAnalyzer(BaseAnalyzer):

    def __init__(self, target_metric_id: str, **kwargs):
        """
        Initialize the ModelAnalyzer class.
        """
        self.target_metric_id = target_metric_id
        super().__init__(**kwargs)
        # Flag to check if the model has been fit
        # This is a flag to check if the model has been fit.
        # It is used to prevent the model from being fit multiple times.
        self._model_fit = False
        # Model and equation
        self.model: Any | None = None
        self.equation: dict[str, Any] | None = None
        self.expression: dict[str, Any] | None = None

    def merge_dataframes(self, df: pd.DataFrame, input_dfs: list[pd.DataFrame]):
        """
        Merges multiple data files or dataframes based on the 'date' column and returns the merged dataframe.
        The dataframes are expected to have a 'date' column and a 'value' column.
        The 'value' column is renamed to the 'metric_id' column value.
        """
        dfs = [df]
        dfs.extend(input_dfs)
        # Fetching common dates in all features
        common_dates = df["date"]
        for _df in dfs[1:]:
            common_dates = common_dates[common_dates.isin(_df["date"])]
        # Renaming the value column to metric_id and dropping the metric_id column
        # Keeping only those dates that are common
        for _df in dfs:
            # convert date column to datetime
            _df["date"] = pd.to_datetime(_df["date"])
            # convert value column to numeric
            _df["value"] = pd.to_numeric(_df["value"], errors="coerce")
            metric_id = _df["metric_id"].iloc[0]
            _df.rename(columns={"value": metric_id}, inplace=True)
            # drop metric_id column
            _df.drop(columns=["metric_id"], inplace=True)
            # Keeping only those dates that are common
            _df = _df[_df["date"].isin(common_dates)]

        # Merging all dataframes
        merged_df = dfs[0]
        for _df in dfs[1:]:
            merged_df = merged_df.merge(_df, on="date")

        # drop na
        merged_df = merged_df.dropna()
        # drop date column
        merged_df = merged_df.drop("date", axis=1)
        return merged_df

    def validate_input(self, df: pd.DataFrame, **kwargs):
        # make sure it has metric_id, date and value column
        if not all(col in df.columns for col in ["metric_id", "date", "value"]):
            raise ValueError("Invalid input dataframe. It should have 'metric_id', 'date', and 'value' columns.")
        # make sure metric_id is unique
        if df["metric_id"].nunique() > 1:
            raise ValueError("Invalid input dataframe. 'metric_id' should be unique.")

    def fit_linear_regression_equation(self, df: pd.DataFrame) -> tuple[Any, dict[str, Any]]:
        """
        Fit a linear regression model and return the fitted model along with the equation.
        This function splits the data into training and testing sets, scales the features,
        fits a linear regression model, and constructs the equation from the model coefficients.
        """
        # Splitting the dataset into features and target variable
        features = df.drop(columns=[self.target_metric_id])
        target = df[self.target_metric_id]

        # Splitting data into training and testing sets
        features_train, features_test, target_train, target_test = train_test_split(
            features, target, test_size=0.2, random_state=42
        )

        # Initializing and fitting the Linear Regression model
        model = LinearRegression()
        model.fit(features_train, target_train)

        # Extracting the coefficients and intercept to form the equation
        coef = model.coef_
        intercept = model.intercept_

        # Constructing the equation dictionary with feature names and their coefficients in a structured format
        equation = {
            "terms": [
                {"feature": col, "coefficient": round(float(coef[i]), self.precision)}
                for i, col in enumerate(features_train.columns)
            ],
            "constant": round(float(intercept), self.precision),
        }

        return model, equation

    def _construct_polynomial_equation(self, model: Any, features: pd.DataFrame) -> dict[str, Any]:
        """
        Construct the equation in a structured format from the model coefficients and intercept.

        Parameters:
            model (Any): The fitted model.
            features (pd.DataFrame): The input features.

        Returns:
            dict: A dictionary representing the equation.
        """
        # Retrieve coefficients and intercept from the regression model
        model_coefficients = model.named_steps["regressor"].coef_
        model_intercept = model.named_steps["regressor"].intercept_

        # Get the polynomial feature names
        poly_features = model.named_steps["poly"].get_feature_names_out(features.columns)

        equation: dict[str, Any] = {"terms": []}

        for i, feature in enumerate(poly_features):
            coefficient = round(float(model_coefficients[i]), self.precision)
            if coefficient == 0:
                continue
            # Split the feature name by spaces to handle interaction terms
            terms = feature.split(" ")
            if len(terms) > 1:
                # Handle interaction terms
                interaction_term = "*".join(terms)
                equation["terms"].append({"coefficient": coefficient, "feature": interaction_term, "power": 1})
            else:
                # Handle single terms
                term = terms[0]
                if "^" in term:
                    feature_name, power = term.split("^")
                    equation["terms"].append({"coefficient": coefficient, "feature": feature_name, "power": int(power)})
                else:
                    equation["terms"].append({"coefficient": coefficient, "feature": term, "power": 1})

        equation["constant"] = round(float(model_intercept), self.precision)
        return equation

    def fit_polynomial_regression_equation(self, df: pd.DataFrame) -> tuple[Any, dict[str, Any]]:
        """
        Fit a polynomial regression model by performing hyperparameter tuning and feature scaling.
        Returns the best fitted model and a structured equation representation.

        Parameters:
            df (pd.DataFrame): The input data with features and target.

        Returns:
            tuple: A tuple containing the best fitted model and the equation as a dictionary.
        """
        # Define the range of degrees for polynomial features to be tested
        param_grid = {"poly__degree": [2, 3, 4, 5]}

        # Separate features and target variable
        features = df.drop(columns=[self.target_metric_id])
        target = df[self.target_metric_id]

        # Split data into training and testing sets
        features_train, features_test, target_train, target_test = train_test_split(
            features, target, test_size=0.2, random_state=42
        )

        # Create a pipeline for transforming data and fitting a regression model
        pipeline = Pipeline(
            [
                ("poly", PolynomialFeatures()),
                ("regressor", LinearRegression()),
            ]
        )
        # Cross validation
        n_samples = len(features_train)
        cv_splits = min(n_samples, 4)

        # Perform grid search to find the best polynomial degree
        grid_search = GridSearchCV(pipeline, param_grid, cv=cv_splits)
        grid_search.fit(features_train, target_train)

        # Extract the best model and its parameters
        best_model = grid_search.best_estimator_

        # Construct the equation using the new method
        equation = self._construct_polynomial_equation(best_model, features)

        return best_model, equation

    def calculate_rmse(self, model: Any, features: pd.DataFrame, targets: pd.Series) -> float:
        """
        calculate the Root Mean Squared Error (RMSE) using the linear regression model.

        Parameters:
            model (Any): The trained linear regression model.
            features (pd.DataFrame): The input features for prediction.
            targets (pd.Series): The true target values.

        Returns:
            float: The calculated RMSE.
        """
        predictions = model.predict(features)
        rmse = np.sqrt(mean_squared_error(targets, predictions))
        return rmse

    def _update_model_fit(self, model: Any, equation: dict[str, Any]):
        """
        Update the model and equation.
        """
        self._model_fit = True
        self.model = model
        self.equation = equation
        self.expression = self.get_equation_expression(equation)

    def fit_model(self, df: pd.DataFrame, input_dfs: list[pd.DataFrame], **kwargs) -> dict[str, Any]:
        """
        Fit the model and return the model and equation.
        This function merges the input dataframes and fits a linear regression model.
        It also fits a polynomial regression model and selects the best model based on the RMSE.
        """
        # Merge the input dataframes
        final_df = self.merge_dataframes(df, input_dfs=input_dfs)

        # Features and target
        features = final_df.drop(columns=[self.target_metric_id])
        target = final_df[self.target_metric_id]

        # Linear regression model and equation
        linear_model, linear_equation = self.fit_linear_regression_equation(final_df)

        # Polynomial regression model and equation
        polynomial_model, polynomial_equation = self.fit_polynomial_regression_equation(final_df)

        # Perform inference for both models
        linear_rmse = self.calculate_rmse(linear_model, features, target)
        poly_rmse = self.calculate_rmse(polynomial_model, features, target)

        # whichever model has the lowest rmse is the best model
        if linear_rmse <= poly_rmse:
            logger.info("Linear regression model has the lowest RMSE.")
            best_model = linear_model
            best_equation = linear_equation
        else:
            logger.info("Polynomial regression model has the lowest RMSE.")
            best_model = polynomial_model
            best_equation = polynomial_equation

        # save the best model and equation
        self._update_model_fit(best_model, best_equation)

        return {
            "model": self.model,
            "equation": self.equation,
            "expression": self.expression,
            "model_type": "linear" if isinstance(self.model, LinearRegression) else "polynomial",
        }

    def analyze(self, df: pd.DataFrame, input_dfs: list[pd.DataFrame], **kwargs) -> dict[str, Any]:  # noqa
        """
        Analyze the provided DataFrame and return the analysis results.
        """
        return self.fit_model(df, input_dfs, **kwargs)

    def get_equation_expression(self, equation: dict[str, Any]) -> dict[str, Any]:
        """
        Convert the equation with terms and constant to the standard expression dictionary and prepare
        the expression string.

        Parameters:
            equation (dict[str, Any]): The equation dictionary with terms and constant.

        Returns:
            dict[str, Any]: The standard expression dictionary including the expression string.
        """
        expression = {"expression_str": "", "type": "expression", "operator": "+", "operands": []}
        expression_parts: list[str] = []

        for term in equation["terms"]:
            feature = term["feature"]
            coefficient = term["coefficient"]
            power = term.get("power", 1)
            # Check if the feature is a product of multiple metrics (e.g., "A*B*C" or "A*B")
            if "*" in feature:
                metrics = feature.split("*")
                # Create a nested expression for each metric multiplication
                operand: dict[str, Any] = {
                    "type": "expression",
                    "operator": "*",
                    "operands": [
                        {"type": "metric", "metric_id": metric, "period": 0, "coefficient": 1, "power": power}
                        for metric in metrics
                    ],
                }
                # Add the coefficient to the operand as constant
                if coefficient != 1:
                    operand["operands"].append({"type": "constant", "value": coefficient})  # type: ignore

                # Prepare part of the expression string
                expression_parts.append(
                    f"{coefficient} * {' * '.join(metrics)}" if coefficient != 1 else " * ".join(metrics)
                )
            else:
                metric_id = feature
                operand = {
                    "type": "metric",
                    "metric_id": metric_id,
                    "period": 0,
                    "coefficient": coefficient,
                    "power": power,
                }
                # Prepare part of the expression string
                if power == 1:
                    expression_parts.append(f"{coefficient} * {metric_id}" if coefficient != 1 else metric_id)
                else:
                    expression_parts.append(
                        f"{coefficient} * {metric_id}^{power}" if coefficient != 1 else f"{metric_id}^{power}"
                    )

            expression["operands"].append(operand)  # type: ignore

        # Add the constant term
        if equation["constant"] != 0:
            expression["operands"].append({"type": "constant", "value": equation["constant"]})  # type: ignore
            expression_parts.append(str(equation["constant"]))

        # Join all parts to form the complete expression string
        expression["expression_str"] = " + ".join(expression_parts)

        return expression
