import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from prophet import Prophet
from sklearn.metrics import mean_squared_error

from fulcrum_core.modules import BaseAnalyzer

logger = logging.getLogger(__name__)


class SeasonalityAnalyzer(BaseAnalyzer):
    def __init__(self, yearly_seasonality: bool = True, weekly_seasonality: bool = True):
        """
        Initialize the SeasonalityAnalyzer.

        Args:
            yearly_seasonality (bool): Whether to include yearly seasonality.
            weekly_seasonality (bool): Whether to include weekly seasonality.
        """
        self.yearly_seasonality = yearly_seasonality
        self.weekly_seasonality = weekly_seasonality
        self.model: Prophet | None = None
        self.fitted = False
        super().__init__()

    def validate_input(self, df: pd.DataFrame, **kwargs) -> None:
        """
        Validate the input DataFrame for the Seasonality Analyzer.

        Args:
            df (pd.DataFrame): The input DataFrame to validate.

        Raises:
            ValueError: If the input DataFrame is missing required columns.
        """
        required_columns = ["date", "value"]
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

    def preprocess_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Preprocess the input DataFrame for seasonality analysis.

        Args:
            df (pd.DataFrame): The input DataFrame for preprocessing.

        Returns:
            pd.DataFrame: The preprocessed DataFrame.
        """
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = df["value"].astype(float)
        df.rename(columns={"date": "ds", "value": "y"}, inplace=True)
        return df

    def fit(self, df: pd.DataFrame) -> None:
        """
        Fit the seasonality model to the provided DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame with 'date' and 'value' columns.
        """
        df = self.preprocess_data(df)

        # Fit the model with additive seasonality
        model_additive = Prophet(
            yearly_seasonality=self.yearly_seasonality,
            weekly_seasonality=self.weekly_seasonality,
            seasonality_mode="additive",
        )
        model_additive.add_seasonality(name="monthly", period=30.5, fourier_order=5)
        model_additive.add_seasonality(name="quarterly", period=91.25, fourier_order=5)
        model_additive.fit(df)
        forecast_additive = model_additive.predict(df)
        rmse_additive = np.sqrt(mean_squared_error(df["y"], forecast_additive["yhat"]))

        # Fit the model with multiplicative seasonality
        model_multiplicative = Prophet(
            yearly_seasonality=self.yearly_seasonality,
            weekly_seasonality=self.weekly_seasonality,
            seasonality_mode="multiplicative",
        )
        model_multiplicative.add_seasonality(name="monthly", period=30.5, fourier_order=5)
        model_multiplicative.add_seasonality(name="quarterly", period=91.25, fourier_order=5)
        model_multiplicative.fit(df)
        forecast_multiplicative = model_multiplicative.predict(df)
        rmse_multiplicative = np.sqrt(mean_squared_error(df["y"], forecast_multiplicative["yhat"]))

        # Decide which model to use
        if rmse_additive < rmse_multiplicative:
            self.model = model_additive
            logger.info("Additive model is better with RMSE:", rmse_additive)
        else:
            self.model = model_multiplicative
            logger.info("Multiplicative model is better with RMSE:", rmse_multiplicative)

        self.fitted = True

    def predict(self, periods: int) -> pd.DataFrame:
        """
        Predict future values using the fitted model.

        Args:
            periods (int): The number of periods to predict into the future.

        Returns:
            pd.DataFrame: The forecasted values.
        """
        if not self.fitted or not self.model:
            raise ValueError("The model must be fitted before prediction.")
        future = self.model.make_future_dataframe(periods=periods)
        forecast = self.model.predict(future)
        return forecast

    def extract_seasonal_component(self, forecast: pd.DataFrame, actual_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract the seasonal components from the forecast.

        Args:
            forecast (pd.DataFrame): The forecasted values.
            actual_df (pd.DataFrame): The actual values.

        Returns:
            pd.DataFrame: The seasonal components.
        """
        seasonal_component = forecast[["ds", "yhat", "yearly", "weekly", "monthly", "quarterly"]].copy()
        actual_df["ds"] = pd.to_datetime(actual_df["date"])
        seasonal_component = pd.merge(seasonal_component, actual_df, on="ds", how="left")

        seasonal_component["yearly_pct_impact"] = seasonal_component["yearly"] / seasonal_component["value"] * 100
        seasonal_component["weekly_pct_impact"] = seasonal_component["weekly"] / seasonal_component["value"] * 100
        seasonal_component["monthly_pct_impact"] = seasonal_component["monthly"] / seasonal_component["value"] * 100
        seasonal_component["quarterly_pct_impact"] = seasonal_component["quarterly"] / seasonal_component["value"] * 100
        return seasonal_component

    def plot_seasonal_components(self, seasonal_component: pd.DataFrame) -> None:
        """
        Plot the seasonal components.

        Args:
            seasonal_component (pd.DataFrame): The seasonal components to plot.
        """
        plt.figure(figsize=(14, 12))
        plt.subplot(511)
        plt.plot(seasonal_component["ds"], seasonal_component["yearly"], label="Yearly Seasonality")
        plt.legend()
        plt.subplot(512)
        plt.plot(seasonal_component["ds"], seasonal_component["weekly"], label="Weekly Seasonality")
        plt.legend()
        plt.subplot(513)
        plt.plot(seasonal_component["ds"], seasonal_component["monthly"], label="Monthly Seasonality")
        plt.legend()
        plt.subplot(514)
        plt.plot(seasonal_component["ds"], seasonal_component["quarterly"], label="Quarterly Seasonality")
        plt.legend()
        plt.subplot(515)
        plt.plot(seasonal_component["ds"], seasonal_component["yearly_pct_impact"], label="Yearly % Impact")
        plt.plot(seasonal_component["ds"], seasonal_component["weekly_pct_impact"], label="Weekly % Impact")
        plt.plot(seasonal_component["ds"], seasonal_component["monthly_pct_impact"], label="Monthly % Impact")
        plt.plot(seasonal_component["ds"], seasonal_component["quarterly_pct_impact"], label="Quarterly % Impact")

        plt.legend()
        plt.show()

    def analyze(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Analyze the input DataFrame for seasonality.

        Args:
            df (pd.DataFrame): The input DataFrame for analysis.

        Returns:
            pd.DataFrame: The seasonal components.
        """
        self.fit(df)
        forecast = self.predict(periods=0)
        seasonal_component = self.extract_seasonal_component(forecast, df)
        return seasonal_component
