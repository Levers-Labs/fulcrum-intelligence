import logging
from typing import Any

import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import PolynomialFeatures, StandardScaler

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

        # Standardizing the features
        features_scaler = StandardScaler()
        features_train_scaled = features_scaler.fit_transform(features_train)

        # Standardizing the target variable
        target_scaler = StandardScaler()
        target_train_scaled = target_scaler.fit_transform(target_train.values.reshape(-1, 1))

        # Initializing and fitting the Linear Regression model
        model = LinearRegression()
        model.fit(features_train_scaled, target_train_scaled.ravel())

        # Extracting the coefficients and intercept to form the equation
        coef = model.coef_
        intercept = model.intercept_

        # Constructing the equation dictionary with feature names and their coefficients in a structured format
        equation = {
            "terms": [{"feature": col, "coefficient": float(coef[i])} for i, col in enumerate(features_train.columns)],
            "constant": float(intercept),
        }

        return model, equation

    def fit_polynomial_regression_equation(self, df: pd.DataFrame) -> tuple[Any, dict[str, Any]]:
        """
        Fit a polynomial regression model by performing hyperparameter tuning and feature scaling.
        Returns the best fitted model and a structured equation representation.

        Parameters:
            data (pd.DataFrame): The input data with features and target.

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
                ("scaler", StandardScaler()),
                ("regressor", LinearRegression()),
            ]
        )

        # Perform grid search to find the best polynomial degree
        grid_search = GridSearchCV(pipeline, param_grid, cv=5)
        grid_search.fit(features_train, target_train)

        # Extract the best model and its parameters
        optimal_degree = grid_search.best_params_["poly__degree"]
        best_model = grid_search.best_estimator_

        # Retrieve coefficients and intercept from the regression model
        model_coefficients = best_model.named_steps["regressor"].coef_
        model_intercept = best_model.named_steps["regressor"].intercept_

        # Construct the equation in a structured format
        # todo: the columns will be repeated for each degree
        equation = {
            "terms": [
                {"coefficient": float(model_coefficients[i]), "feature": col, "power": j}
                for j in range(1, optimal_degree + 1)
                for i, col in enumerate(features.columns)
            ],
            "constant": model_intercept,
        }

        return best_model, equation

    def calculate_rmse(self, model: LinearRegression | Pipeline, features: Any, targets: pd.Series) -> float:
        """
        Calculate the Root Mean Squared Error (RMSE) using the linear regression model.

        Parameters:
            model (LinearRegression): The trained linear regression model.
            features (pd.DataFrame): The input features for prediction.
            targets (pd.Series): The true target values.

        Returns:
            float: The calculated RMSE.
        """
        predictions = model.predict(features)
        # todo: do we need to take square root of the error?
        rmse = mean_squared_error(targets, predictions)
        return rmse

    def _update_model_fit(self, model: Any, equation: dict[str, Any]):
        """
        Update the model and equation.
        """
        self._model_fit = True
        self.model = model
        self.equation = equation

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
        }

    def analyze(self, df: pd.DataFrame, input_dfs: list[pd.DataFrame], **kwargs) -> dict[str, Any]:  # noqa
        """
        Analyze the provided DataFrame and return the analysis results.
        """
        return self.fit_model(df, input_dfs, **kwargs)
